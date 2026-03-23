"""MEXC Futures REST and WebSocket client."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import aiohttp

from circuit_breaker import CircuitBreaker
from config import (
    MEXC_KLINE_BATCH,
    MEXC_KLINE_DELAY,
    MEXC_KLINE_INTERVAL,
    MEXC_REST_BASE,
    MEXC_WS_URL,
    WS_CIRCUIT_FAIL_THRESHOLD,
    WS_CIRCUIT_OPEN_SEC,
    WS_CIRCUIT_WINDOW_SEC,
    WS_PING_INTERVAL,
    WS_RECONNECT_BACKOFF_SEC,
    WS_STALL_TIMEOUT_SEC,
)

logger = logging.getLogger(__name__)

# REST: защита от бана при сбоях сети / 510
_REST_BREAKER = CircuitBreaker(
    fail_threshold=WS_CIRCUIT_FAIL_THRESHOLD,
    window_sec=WS_CIRCUIT_WINDOW_SEC,
    open_sec=WS_CIRCUIT_OPEN_SEC,
)


# --- REST --------------------------------------------------------------------

async def fetch_server_time(session: aiohttp.ClientSession) -> int:
    """Server time in milliseconds."""
    url = f"{MEXC_REST_BASE}/api/v1/contract/ping"
    async with session.get(url) as r:
        r.raise_for_status()
        data = await r.json()
    if not data.get("success") or data.get("code") != 0:
        raise RuntimeError(f"MEXC ping error: {data}")
    return data["data"]


async def fetch_contract_list(session: aiohttp.ClientSession) -> list[str]:
    """All USDT perpetual symbols (state=0, quoteCoin=USDT)."""
    url = f"{MEXC_REST_BASE}/api/v1/contract/detail"
    async with session.get(url) as r:
        r.raise_for_status()
        raw = await r.json()
    if not raw.get("success") or raw.get("code") != 0:
        raise RuntimeError(f"MEXC contract list error: {raw}")
    data = raw["data"]
    # API can return one object or list
    if isinstance(data, dict):
        items = [data]
    elif isinstance(data, list):
        items = data
    else:
        items = []
    symbols = []
    for item in items:
        if not isinstance(item, dict):
            continue
        state = item.get("state", -1)
        quote = (item.get("quoteCoin") or "").upper()
        sym = item.get("symbol")
        if state == 0 and quote == "USDT" and sym:
            symbols.append(sym)
    return symbols


def _interval_step_sec(interval: str) -> int:
    """Duration of kline interval in seconds. MEXC: Min60, Hour4, Day1, ..."""
    if interval == "Min60":
        return 60 * 60
    if interval in ("Min240", "Hour4"):
        return 4 * 60 * 60
    if interval == "Day1":
        return 24 * 60 * 60
    return 60 * 60


async def fetch_klines(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str = MEXC_KLINE_INTERVAL,
    end_ts: int | None = None,
    count: int = 30,
) -> list[dict[str, Any]]:
    """
    Get last `count` closed 1H candles.
    end: optional end timestamp in seconds; if None, use server time.
    Returns list of {"t": sec, "o", "c", "h", "l", ...} oldest first.
    """
    await _REST_BREAKER.before_call()
    try:
        if end_ts is None:
            end_ts = int(await fetch_server_time(session) / 1000)
        step = _interval_step_sec(interval)
        # align to full interval
        end_ts = (end_ts // step) * step
        start_ts = end_ts - (count + 2) * step
        url = f"{MEXC_REST_BASE}/api/v1/contract/kline/{symbol}"
        params = {"interval": interval, "start": start_ts, "end": end_ts}
        async with session.get(url, params=params) as r:
            r.raise_for_status()
            raw = await r.json()
        if not raw.get("success") or raw.get("code") != 0:
            raise RuntimeError(f"MEXC kline error for {symbol}: {raw}")
        data = raw["data"]
        times = data.get("time") or []
        closes = data.get("close") or []
        opens = data.get("open") or []
        highs = data.get("high") or []
        lows = data.get("low") or []
        n = min(len(times), len(closes), count)
        out = []
        for i in range(-n, 0) if n else []:
            idx = i + n
            out.append({
                "t": times[idx],
                "o": opens[idx] if idx < len(opens) else closes[idx],
                "c": closes[idx],
                "h": highs[idx] if idx < len(highs) else closes[idx],
                "l": lows[idx] if idx < len(lows) else closes[idx],
            })
        # ensure oldest first
        out.sort(key=lambda x: x["t"])
        result = out[-count:] if len(out) > count else out
    except Exception:
        await _REST_BREAKER.on_failure()
        raise
    _REST_BREAKER.on_success()
    return result


async def fetch_klines_batch(
    session: aiohttp.ClientSession,
    symbols: list[str],
    interval: str = MEXC_KLINE_INTERVAL,
    count: int = 30,
    concurrency: int = MEXC_KLINE_BATCH,
    delay: float = MEXC_KLINE_DELAY,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch kline history for many symbols. Ограниченная параллельность и пауза между запросами — чтобы не получать 510 от MEXC."""
    sem = asyncio.Semaphore(concurrency)
    server_time_ms = await fetch_server_time(session)
    end_ts = int(server_time_ms / 1000)

    async def get_one(sym: str) -> tuple[str, list[dict]]:
        async with sem:
            await asyncio.sleep(delay)
            try:
                klines = await fetch_klines(session, sym, interval, end_ts, count)
                return (sym, klines)
            except Exception as e:
                logger.warning("Kline fetch %s: %s", sym, e)
                return (sym, [])

    tasks = [get_one(s) for s in symbols]
    results = await asyncio.gather(*tasks)
    return dict(results)


# --- WebSocket ---------------------------------------------------------------

def ws_subscribe_kline(symbol: str, interval: str = "Min60") -> dict:
    return {"method": "sub.kline", "param": {"symbol": symbol, "interval": interval}, "gzip": False}


def ws_ping() -> dict:
    return {"method": "ping"}


async def run_ws_kline_stream(
    symbols: list[str],
    on_kline_cb: Any,
    on_kline_ctx: Any = None,
    *,
    interval: str = "Min60",
    reconnect_delay: float = 5.0,
    stall_timeout_sec: float | None = None,
    backoff_delays: list[float] | None = None,
    health: dict[str, Any] | None = None,
) -> None:
    """
    MEXC WS kline: auto-reconnect, stall detection (нет сообщений N сек → reconnect),
    backoff 5→10→20→60 с. health["last_message_mono"] обновляется на любое TEXT-сообщение.
    """
    callback = on_kline_cb
    callback_ctx = on_kline_ctx
    deadline = float(stall_timeout_sec if stall_timeout_sec is not None else WS_STALL_TIMEOUT_SEC)
    backoffs = list(backoff_delays) if backoff_delays else list(WS_RECONNECT_BACKOFF_SEC)
    if not backoffs:
        backoffs = [5.0, 10.0, 20.0, 60.0]
    attempt = 0
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(MEXC_WS_URL) as ws:
                    logger.info("WebSocket kline connected (%s)", interval)
                    batch = 100
                    for i in range(0, len(symbols), batch):
                        for sym in symbols[i : i + batch]:
                            await ws.send_str(json.dumps(ws_subscribe_kline(sym, interval)))
                        await asyncio.sleep(0.1)
                    logger.info("Subscribed kline %d symbols (%s)", len(symbols), interval)
                    attempt = 0
                    last_ping = time.monotonic()
                    while True:
                        try:
                            msg = await asyncio.wait_for(ws.receive(), timeout=deadline)
                        except asyncio.TimeoutError:
                            logger.warning(
                                "WebSocket kline: нет данных %.0fs → reconnect (%s)",
                                deadline,
                                interval,
                            )
                            break
                        if msg.type == aiohttp.WSMsgType.CLOSE:
                            logger.info("WebSocket kline closed by server")
                            break
                        if msg.type == aiohttp.WSMsgType.ERROR:
                            raise ConnectionError(ws.exception())
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        if health is not None:
                            health["last_message_mono"] = time.monotonic()
                        try:
                            obj = json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue
                        ch = obj.get("channel")
                        if ch == "pong":
                            continue
                        if ch == "push.kline":
                            data = obj.get("data") or {}
                            sym = obj.get("symbol") or data.get("symbol")
                            if sym and data:
                                await callback(data, sym, callback_ctx)
                        now = time.monotonic()
                        if now - last_ping >= WS_PING_INTERVAL:
                            await ws.send_str(json.dumps(ws_ping()))
                            last_ping = now
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("WebSocket kline error: %s", e)
        attempt += 1
        delay = backoffs[min(attempt - 1, len(backoffs) - 1)]
        logger.info("WebSocket kline reconnect через %.0fs (попытка %d)", delay, attempt)
        await asyncio.sleep(delay)
