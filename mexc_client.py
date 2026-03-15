"""MEXC Futures REST and WebSocket client."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import aiohttp

from config import MEXC_KLINE_BATCH, MEXC_KLINE_DELAY, MEXC_KLINE_INTERVAL, MEXC_REST_BASE, MEXC_WS_URL, WS_PING_INTERVAL

logger = logging.getLogger(__name__)


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
    return out[-count:] if len(out) > count else out


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
) -> None:
    """
    Connect to MEXC WS, subscribe to 1H kline for all symbols.
    On each push.kline calls: on_kline_cb(data, symbol, on_kline_ctx).
    Reconnects on disconnect. Never returns unless cancelled.
    """
    callback = on_kline_cb
    callback_ctx = on_kline_ctx
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(MEXC_WS_URL) as ws:
                    logger.info("WebSocket connected")
                    # Subscribe in batches to avoid huge first message
                    batch = 100
                    for i in range(0, len(symbols), batch):
                        for sym in symbols[i : i + batch]:
                            await ws.send_str(json.dumps(ws_subscribe_kline(sym, interval)))
                        await asyncio.sleep(0.1)
                    logger.info("Subscribed to %d symbols", len(symbols))
                    last_ping = time.monotonic()
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.CLOSED:
                            break
                        if msg.type == aiohttp.WSMsgType.ERROR:
                            raise ConnectionError(ws.exception())
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
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
                        # Ping every 10–20 s
                        now = time.monotonic()
                        if now - last_ping >= WS_PING_INTERVAL:
                            await ws.send_str(json.dumps(ws_ping()))
                            last_ping = now
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("WebSocket error: %s", e)
        await asyncio.sleep(reconnect_delay)


def ws_subscribe_tickers() -> dict:
    """Подписка на тикеры всех контрактов (volume24, riseFallRate и т.д.)."""
    return {"method": "sub.tickers", "param": {}, "gzip": False}


async def run_ws_ticker_stream(
    on_tickers_cb: Any,
    ticker_ctx: Any = None,
    *,
    reconnect_delay: float = 5.0,
) -> None:
    """
    Подключается к MEXC WS, подписка на sub.tickers.
    При каждом push.tickers вызывает on_tickers_cb(data_list, ticker_ctx).
    data_list — список словарей {symbol, volume24, riseFallRate, lastPrice, ...}.
    """
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(MEXC_WS_URL) as ws:
                    await ws.send_str(json.dumps(ws_subscribe_tickers()))
                    logger.info("WebSocket tickers subscribed")
                    last_ping = time.monotonic()
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.CLOSED:
                            break
                        if msg.type == aiohttp.WSMsgType.ERROR:
                            raise ConnectionError(ws.exception())
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            obj = json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue
                        if obj.get("channel") == "pong":
                            pass
                        elif obj.get("channel") == "push.tickers":
                            data = obj.get("data")
                            if isinstance(data, list) and data:
                                await on_tickers_cb(data, ticker_ctx)
                        now = time.monotonic()
                        if now - last_ping >= WS_PING_INTERVAL:
                            await ws.send_str(json.dumps(ws_ping()))
                            last_ping = now
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("WebSocket tickers error: %s", e)
        await asyncio.sleep(reconnect_delay)
