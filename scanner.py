"""
Сканер RSI3(24) и маркет-муверы (асинхронно).
- Только 4H: RSI по текущей (открытой) свече. Сигнал строго если RSI3(24) >= 90.
- Маркет-муверы: только «Рост цены и высокий объём», уведомление при появлении новой пары.
Антиспам: symbol + timeframe + candle_open_time (один раз на свечу).
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import aiohttp

from config import (
    KLINE_HISTORY_COUNT,
    MARKET_MOVERS_ALERT_DELAY_SEC,
    MARKET_MOVERS_CHAT_ID,
    MARKET_MOVERS_INTERVAL_SEC,
    MARKET_MOVERS_MAX_PER_CYCLE,
    MARKET_MOVERS_MIN_RISE_PCT,
    MARKET_MOVERS_NEW_COOLDOWN_SEC,
    MARKET_MOVERS_TOP_N,
    MIN_ALERT_DELAY_4H_SEC,
    RSI_PERIOD,
    RSI_THRESHOLD_4H,
)
from indicators import rsi
from mexc_client import fetch_contract_list, fetch_klines_batch, run_ws_kline_stream, run_ws_ticker_stream
from state import StateStore
from telegram_notify import send_market_mover_alert, send_signal, send_startup_message

logger = logging.getLogger(__name__)


async def on_kline(data: dict, symbol: str, ctx: dict) -> None:
    """
    Обработка push.kline: RSI3(24) по текущей (открытой) свече.
    Уведомление только если RSI3(24) >= 90 (строго). Один раз на свечу: symbol + timeframe + candle_open_time.
    """
    store: StateStore = ctx["store"]
    tf_name: str = ctx["tf_name"]
    threshold: float = ctx["threshold"]
    t_sec = int(data.get("t", 0))
    close = float(data.get("c", 0))
    if not t_sec or not close:
        return
    state_key = f"{symbol}@{tf_name}"
    state = store.get_or_create(state_key)

    if state.candle_start_time and t_sec != state.candle_start_time:
        state.roll_to_new_candle(close, t_sec)
    else:
        if not state.candle_start_time:
            state.candle_start_time = t_sec
        state.current_close = close

    # RSI по текущей формирующейся свече (последние 25 цен: 24 закрытых + текущая)
    closes = state.closes_for_rsi()
    if len(closes) < RSI_PERIOD + 1:
        return
    rsi_val = rsi(closes, RSI_PERIOD)
    if rsi_val is None:
        return
    # Не слать в первые секунды свечи — первый тик даёт всплеск, не совпадающий с графиком MEXC
    now_sec = int(time.time())
    elapsed = now_sec - state.candle_start_time
    min_delay = MIN_ALERT_DELAY_4H_SEC
    if elapsed < min_delay:
        return
    already_sent = await store.get_alert_sent(state_key, state.candle_start_time)
    if rsi_val >= threshold and not already_sent:
        ok = await send_signal(symbol, rsi_val, close, state.candle_start_time, tf_name=tf_name)
        if ok:
            await store.set_alert_sent(state_key, state.candle_start_time)
            await store.push_last_signal(symbol, rsi_val, close, state.candle_start_time, tf_name=tf_name)
            logger.info("SIGNAL %s [%s] RSI3(24)=%.2f (открытая свеча) price=%.4g", symbol, tf_name, rsi_val, close)

    log_every = ctx.get("log_every_n")
    if log_every:
        ctx["log_n"] = (ctx.get("log_n") or 0) + 1
        if ctx["log_n"] % log_every == 0:
            logger.info("RSI scan %s: обработано %d обновлений свечей", ctx["tf_name"], ctx["log_n"])


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
        await bootstrap_symbols_for_tf(session, store, symbols, interval="Hour4", tf_name="4H")

    ctx_4h = {"store": store, "log_every_n": 10000, "tf_name": "4H", "threshold": RSI_THRESHOLD_4H}
    ws_4h = asyncio.create_task(run_ws_kline_stream(symbols, on_kline, ctx_4h, interval="Hour4", reconnect_delay=5.0))
    tasks = [ws_4h]
    logger.info(
        "RSI: сканирование 4H запущено — RSI3(24) ≥ %.0f по %d парам.",
        RSI_THRESHOLD_4H, len(symbols),
    )

    # Маркет-муверы: только вкладка «Рост цены и высокий объём» (не «Снижение цены», не «Падение», не откуда больше)
    ticker_store: dict[str, dict] = {}
    mover_new_last_sent: dict[str, float] = {}
    previous_top_symbols: set[str] = set()  # кто был в списке в прошлом цикле — шлём только при появлении
    min_rise = MARKET_MOVERS_MIN_RISE_PCT / 100.0

    async def on_tickers(data_list: list, ctx: dict) -> None:
        for item in data_list:
            if isinstance(item, dict) and item.get("symbol"):
                ticker_store[item["symbol"]] = item

    async def market_movers_loop() -> None:
        nonlocal previous_top_symbols
        while True:
            await asyncio.sleep(MARKET_MOVERS_INTERVAL_SEC)
            if not MARKET_MOVERS_CHAT_ID or not ticker_store:
                continue
            # Только эта вкладка: рост цены 24ч (>= min_rise%) + высокий объём; «Снижение цены»/«Падение» не трогаем
            candidates = [
                (sym, t)
                for sym, t in ticker_store.items()
                if sym.endswith("_USDT")
                and (t.get("riseFallRate") or 0) >= min_rise  # только рост, не падение
                and (t.get("volume24") or 0) > 0
            ]
            candidates.sort(key=lambda x: float(x[1].get("volume24") or 0), reverse=True)
            top = candidates[:MARKET_MOVERS_TOP_N]
            if not top:
                continue
            current_top = {sym for sym, _ in top}
            # Первый цикл — только запомнить список, не слать
            if not previous_top_symbols:
                previous_top_symbols = current_top
                continue
            # Шлём только пары, которые только что появились в списке (как «новые» в окне MEXC)
            newly_in_list = current_top - previous_top_symbols
            previous_top_symbols = current_top

            now = time.time()
            sent_this_cycle = 0
            for sym, t in top:
                if sym not in newly_in_list:
                    continue
                if sent_this_cycle >= MARKET_MOVERS_MAX_PER_CYCLE:
                    break
                last_sent = mover_new_last_sent.get(sym, 0)
                if now - last_sent < MARKET_MOVERS_NEW_COOLDOWN_SEC:
                    continue
                vol24 = float(t.get("volume24") or 0)
                rise_pct = float(t.get("riseFallRate") or 0) * 100
                price = float(t.get("lastPrice") or t.get("fairPrice") or 0)
                ok = await send_market_mover_alert(sym, rise_pct, vol24, price)
                if ok:
                    mover_new_last_sent[sym] = now
                    sent_this_cycle += 1
                    logger.info("Market mover new alert: %s +%.2f%% (появился в списке)", sym, rise_pct)
                    await asyncio.sleep(MARKET_MOVERS_ALERT_DELAY_SEC)

    if MARKET_MOVERS_CHAT_ID:
        logger.info("Маркет-муверы: только вкладка «Рост цены и высокий объём», только новые в канал %s",
                    MARKET_MOVERS_CHAT_ID)
        tasks.append(asyncio.create_task(run_ws_ticker_stream(on_tickers, ticker_store, reconnect_delay=5.0)))
        tasks.append(asyncio.create_task(market_movers_loop()))
    else:
        logger.info("Маркет-муверы: отключены (в config/keys.yml укажи telegram.market_movers_chat_id)")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
