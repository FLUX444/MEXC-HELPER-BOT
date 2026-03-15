"""
Main RSI scanner: load history, run WebSocket(s), on each kline update compute RSI(24).
Two timeframes:
- 1H  (Min60)  — сигнал, если RSI >= rsi_threshold_1h (по умолчанию 90)
- 4H  (Hour4) — сигнал, если RSI >= rsi_threshold_4h (по умолчанию 85)
Для каждой монеты и каждого таймфрейма — не больше одного сигнала на свечу.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import aiohttp

from config import KLINE_HISTORY_COUNT, RSI_PERIOD, RSI_THRESHOLD_1H, RSI_THRESHOLD_4H
from indicators import rsi
from mexc_client import fetch_contract_list, fetch_klines_batch, run_ws_kline_stream
from state import StateStore
from telegram_notify import send_signal, send_startup_message

logger = logging.getLogger(__name__)


async def on_kline(data: dict, symbol: str, ctx: dict) -> None:
    """Handle one push.kline: update state, compute RSI, maybe send alert."""
    store: StateStore = ctx["store"]
    tf_name: str = ctx["tf_name"]  # "1H" или "4H"
    threshold: float = ctx["threshold"]
    t_sec = int(data.get("t", 0))
    close = float(data.get("c", 0))
    if not t_sec or not close:
        return
    # Отдельное состояние для каждой пары+таймфрейма
    state_key = f"{symbol}@{tf_name}"
    state = store.get_or_create(state_key)
    # New candle?
    if state.candle_start_time and t_sec != state.candle_start_time:
        state.roll_to_new_candle(close, t_sec)
    else:
        if not state.candle_start_time:
            state.candle_start_time = t_sec
        state.current_close = close
    # Need at least RSI_PERIOD closed + current
    closes = state.closes_for_rsi()
    if len(closes) < RSI_PERIOD + 1:
        return
    rsi_val = rsi(closes, RSI_PERIOD)
    if rsi_val is None:
        return
    t0 = time.perf_counter()
    # anti‑spam: один сигнал на монету и таймфрейм на свечу
    already_sent = await store.get_alert_sent(state_key, state.candle_start_time)
    if rsi_val >= threshold and not already_sent:
        ok = await send_signal(symbol, rsi_val, close, state.candle_start_time, tf_name=tf_name)
        if ok:
            await store.set_alert_sent(state_key, state.candle_start_time)
            await store.push_last_signal(symbol, rsi_val, close, state.candle_start_time, tf_name=tf_name)
            latency_ms = (time.perf_counter() - t0) * 1000
            logger.info(
                "SIGNAL %s [%s] RSI=%.2f price=%.4g latency=%.0fms",
                symbol,
                tf_name,
                rsi_val,
                close,
                latency_ms,
            )
    if ctx.get("log_every_n"):
        ctx["log_n"] = (ctx.get("log_n") or 0) + 1
        if ctx["log_n"] % 5000 == 0:
            logger.info("Processed %d kline updates", ctx["log_n"])


async def bootstrap_symbols_for_tf(
    session: aiohttp.ClientSession,
    store: StateStore,
    symbols: list[str],
    *,
    interval: str,
    tf_name: str,
) -> None:
    """Load last closed candles for given timeframe and init state for each symbol."""
    logger.info("Loading kline history for %d symbols (%s)", len(symbols), tf_name)
    result = await fetch_klines_batch(session, symbols, interval=interval, count=KLINE_HISTORY_COUNT)
    inited = 0
    for sym in symbols:
        klines = result.get(sym)
        if not klines:
            continue
        closes = [float(k["c"]) for k in klines]
        if len(closes) < RSI_PERIOD:
            continue
        last = klines[-1]
        t_sec = int(last["t"])
        state_key = f"{sym}@{tf_name}"
        store.init_symbol(state_key, closes[:-1], t_sec, float(last["c"]))
        inited += 1
    logger.info("Bootstrapped %d symbols for %s", inited, tf_name)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    from config import REDIS_URL, SQLITE_PATH
    store = StateStore(redis_url=REDIS_URL, sqlite_path=SQLITE_PATH)
    # При Redis ждём готовности до 30 сек; при SQLite — сразу
    await store.ensure_redis(wait_seconds=30 if REDIS_URL else 0)
    await send_startup_message()
    from bot_handlers import run_bot_polling
    # Бот отвечает на /start сразу, пока грузится история
    bot_task = asyncio.create_task(run_bot_polling(store))
    async with aiohttp.ClientSession() as session:
        symbols = await fetch_contract_list(session)
        symbols = [s for s in symbols if s.endswith("_USDT")]
        logger.info("Found %d USDT perpetual contracts", len(symbols))
        if not symbols:
            logger.error("No symbols; exit")
            return
        logger.info("Loading kline history (throttled for MEXC limit)...")
        # 1H
        await bootstrap_symbols_for_tf(session, store, symbols, interval="Min60", tf_name="1H")
        # 4H (MEXC API: Hour4, не Min240)
        await bootstrap_symbols_for_tf(session, store, symbols, interval="Hour4", tf_name="4H")

    # Запускаем два WebSocket‑потока: 1H и 4H
    ctx_1h = {"store": store, "log_every_n": True, "tf_name": "1H", "threshold": RSI_THRESHOLD_1H}
    ctx_4h = {"store": store, "log_every_n": False, "tf_name": "4H", "threshold": RSI_THRESHOLD_4H}

    ws_1h = asyncio.create_task(run_ws_kline_stream(symbols, on_kline, ctx_1h, interval="Min60", reconnect_delay=5.0))
    ws_4h = asyncio.create_task(run_ws_kline_stream(symbols, on_kline, ctx_4h, interval="Hour4", reconnect_delay=5.0))
    await asyncio.gather(ws_1h, ws_4h)


if __name__ == "__main__":
    asyncio.run(main())
