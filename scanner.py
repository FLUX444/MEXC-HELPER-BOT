"""
Сканер RSI3(24) и маркет-муверы (асинхронно).
- Только 4H: RSI по текущей (открытой) свече. Сигнал строго если RSI3(24) >= 90.
- Продакшн: WS stall + backoff, REST backup по «застывшим» символам, SETNX-антидубль,
  watchdog, heartbeat-файл, ротация логов, circuit breaker на REST (mexc_client).

- PRE-ALERT: RSI в [rsi_prealert_min .. main) — 1 раз на свечу, Redis prealert:{symbol}:{candle_open}, TTL 4 ч.
- При require_redis: true нужен рабочий Redis (SETNX main + prealert).
"""
from __future__ import annotations

import asyncio
import logging
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import aiohttp

from config import (
    BACKUP_BATCH_SIZE,
    BACKUP_REST_CONCURRENCY,
    BACKUP_RSI_INTERVAL_SEC,
    BACKUP_STALE_SYMBOL_SEC,
    HEARTBEAT_FILE,
    HEARTBEAT_INTERVAL_SEC,
    KLINE_HISTORY_COUNT,
    LOG_FILE_BACKUP_COUNT,
    LOG_FILE_MAX_MB,
    LOG_DIR,
    MARKET_MOVERS_ALERT_DELAY_SEC,
    MARKET_MOVERS_CHAT_ID,
    MARKET_MOVERS_INTERVAL_SEC,
    MARKET_MOVERS_MAX_PER_CYCLE,
    MARKET_MOVERS_MIN_RISE_PCT,
    MARKET_MOVERS_NEW_COOLDOWN_SEC,
    MARKET_MOVERS_TOP_N,
    MIN_ALERT_DELAY_4H_SEC,
    MEXC_KLINE_DELAY,
    REDIS_URL,
    REQUIRE_REDIS,
    RSI_PERIOD,
    RSI_PREALERT_MIN,
    RSI_THRESHOLD_4H,
    SQLITE_PATH,
    TIME_SYNC_INTERVAL_SEC,
    WATCHDOG_INTERVAL_SEC,
    WATCHDOG_WARN_AFTER_SEC,
    WS_STALL_TIMEOUT_SEC,
)
from indicators import rsi
from mexc_client import (
    fetch_contract_list,
    fetch_klines,
    fetch_klines_batch,
    fetch_server_time,
    run_ws_kline_stream,
    run_ws_ticker_stream,
)
from state import StateStore
from telegram_notify import send_market_mover_alert, send_prealert, send_signal, send_startup_message

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """Консоль + файл с ротацией (logs/scanner.log)."""
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if not any(isinstance(h, RotatingFileHandler) for h in root.handlers):
        try:
            LOG_DIR.mkdir(parents=True, exist_ok=True)
            fh = RotatingFileHandler(
                LOG_DIR / "scanner.log",
                maxBytes=max(1, LOG_FILE_MAX_MB) * 1024 * 1024,
                backupCount=max(1, LOG_FILE_BACKUP_COUNT),
                encoding="utf-8",
            )
            fh.setFormatter(fmt)
            root.addHandler(fh)
        except OSError as e:
            logging.getLogger(__name__).warning("Не удалось создать файл логов: %s", e)


async def handle_kline_update(data: dict, symbol: str, ctx: dict) -> None:
    """
    Ядро RSI: одна точка для WS и REST backup.
    Валидация: пустые/битые значения, явные дубли (t+c), слишком старый t.
    """
    store: StateStore = ctx["store"]
    tf_name: str = ctx["tf_name"]
    threshold: float = ctx["threshold"]
    t_sec = int(data.get("t", 0))
    close = float(data.get("c", 0))
    if not t_sec or close <= 0 or close != close:
        return
    state_key = f"{symbol}@{tf_name}"
    state = store.get_or_create(state_key)

    dup = ctx.setdefault("_last_kline_tick", {})
    if len(dup) > 8000:
        dup.clear()
        logger.info("memory: очищен кэш дедупликации kline (контроль RAM)")
    if dup.get(symbol) == (t_sec, close):
        return
    dup[symbol] = (t_sec, close)

    if state.candle_start_time and t_sec < state.candle_start_time - 86400 * 7:
        logger.debug("stale kline ignored %s t=%s", symbol, t_sec)
        return

    if state.candle_start_time and t_sec != state.candle_start_time:
        state.roll_to_new_candle(close, t_sec)
    else:
        if not state.candle_start_time:
            state.candle_start_time = t_sec
        state.current_close = close

    closes = state.closes_for_rsi()
    # Ровно 25 точек: 24 закрытых + текущая (как в ТЗ pre-alert / main)
    if len(closes) != RSI_PERIOD + 1:
        return
    rsi_val = rsi(closes, RSI_PERIOD)
    if rsi_val is None:
        return
    now_sec = int(time.time())
    elapsed = now_sec - state.candle_start_time
    min_delay = MIN_ALERT_DELAY_4H_SEC
    if elapsed < min_delay:
        return

    if rsi_val >= threshold:
        acquired = await store.try_claim_alert(state_key, state.candle_start_time)
        if acquired:
            ok = await send_signal(symbol, rsi_val, close, state.candle_start_time, tf_name=tf_name)
            if ok:
                await store.push_last_signal(symbol, rsi_val, close, state.candle_start_time, tf_name=tf_name)
                logger.info("SIGNAL %s [%s] RSI3(24)=%.2f price=%.4g", symbol, tf_name, rsi_val, close)
            else:
                await store.release_alert_claim(state_key, state.candle_start_time)
        # Только main, без PRE-ALERT
        log_every = ctx.get("log_every_n")
        if log_every:
            ctx["log_n"] = (ctx.get("log_n") or 0) + 1
            if ctx["log_n"] % log_every == 0:
                logger.info("RSI scan %s: обработано %d обновлений свечей", ctx["tf_name"], ctx["log_n"])
        return

    if rsi_val >= RSI_PREALERT_MIN:
        if await store.get_alert_sent(state_key, state.candle_start_time):
            pass
        else:
            acquired = await store.try_claim_prealert(symbol, state.candle_start_time)
            if acquired:
                ok = await send_prealert(
                    symbol,
                    rsi_val,
                    close,
                    state.candle_start_time,
                    elapsed,
                    tf_name=tf_name,
                    main_threshold=threshold,
                )
                if ok:
                    logger.info(
                        "PRE-ALERT %s [%s] RSI3(24)=%.2f price=%.4g",
                        symbol,
                        tf_name,
                        rsi_val,
                        close,
                    )
                else:
                    await store.release_prealert_claim(symbol, state.candle_start_time)

    log_every = ctx.get("log_every_n")
    if log_every:
        ctx["log_n"] = (ctx.get("log_n") or 0) + 1
        if ctx["log_n"] % log_every == 0:
            logger.info("RSI scan %s: обработано %d обновлений свечей", ctx["tf_name"], ctx["log_n"])


async def on_kline(data: dict, symbol: str, ctx: dict) -> None:
    mono = time.monotonic()
    ctx["last_symbol_ws_mono"][symbol] = mono
    await handle_kline_update(data, symbol, ctx)


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


async def backup_rsi_loop(session: aiohttp.ClientSession, symbols: list[str], ctx_4h: dict) -> None:
    """REST-подстраховка: символы без WS дольше BACKUP_STALE_SYMBOL_SEC."""
    last_mono: dict[str, float] = ctx_4h["last_symbol_ws_mono"]
    while True:
        try:
            await asyncio.sleep(BACKUP_RSI_INTERVAL_SEC)
            now = time.monotonic()
            stale = [s for s in symbols if now - last_mono.get(s, 0) >= BACKUP_STALE_SYMBOL_SEC]
            stale.sort(key=lambda s: last_mono.get(s, 0))
            batch = stale[:BACKUP_BATCH_SIZE]
            if not batch:
                continue
            sem = asyncio.Semaphore(BACKUP_REST_CONCURRENCY)

            async def one(sym: str) -> None:
                async with sem:
                    await asyncio.sleep(MEXC_KLINE_DELAY)
                    try:
                        klines = await fetch_klines(session, sym, interval="Hour4", count=5)
                        if not klines:
                            return
                        last = klines[-1]
                        await handle_kline_update(
                            {"t": int(last["t"]), "c": float(last["c"])},
                            sym,
                            ctx_4h,
                        )
                        last_mono[sym] = time.monotonic()
                    except Exception as e:
                        logger.debug("REST backup %s: %s", sym, e)

            await asyncio.gather(*(one(s) for s in batch))
            logger.info("REST backup: подтянуто %d символов (застывшие WS)", len(batch))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("backup_rsi_loop: ошибка, пауза 5с")
            await asyncio.sleep(5)


async def heartbeat_loop() -> None:
    """Файл для Docker healthcheck и мониторинга."""
    path = Path(HEARTBEAT_FILE)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        pass
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
        try:
            path.write_text(str(int(time.time())), encoding="utf-8")
        except OSError as e:
            logger.debug("heartbeat: %s", e)


async def time_sync_loop(session: aiohttp.ClientSession) -> None:
    while True:
        await asyncio.sleep(TIME_SYNC_INTERVAL_SEC)
        try:
            ms = await fetch_server_time(session)
            local_ms = int(time.time() * 1000)
            drift = ms - local_ms
            if abs(drift) > 2500:
                logger.warning("TIME SYNC: дрейф MEXC − local = %d ms", drift)
            else:
                logger.info("TIME SYNC OK (дрейф %d ms)", drift)
        except Exception as e:
            logger.warning("TIME SYNC failed: %s", e)


async def watchdog_loop(health_kline: dict[str, Any], health_ticker: dict[str, Any] | None) -> None:
    while True:
        await asyncio.sleep(WATCHDOG_INTERVAL_SEC)
        now = time.monotonic()
        checks = [("kline_4h", health_kline)]
        if health_ticker is not None:
            checks.append(("tickers", health_ticker))
        for name, h in checks:
            last = h.get("last_message_mono")
            if last is None:
                continue
            age = now - last
            if age > WATCHDOG_WARN_AFTER_SEC:
                logger.error(
                    "WATCHDOG: %s — нет WS-трафика %.0f с (внутренний stall-timeout должен переподключить)",
                    name,
                    age,
                )


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    setup_logging()
    logger.info("MEXC Scanner старт (v5 production helpers), логи: %s", LOG_DIR)

    store = StateStore(redis_url=REDIS_URL, sqlite_path=SQLITE_PATH)
    if REQUIRE_REDIS:
        if not REDIS_URL:
            logger.error(
                "require_redis=true: задай Redis — в config/keys.yml db.use_redis: true и db.redis_url, "
                "либо переменную REDIS_URL (например redis://localhost:6379/0)."
            )
            return
        await store.ensure_redis(wait_seconds=90)
        if store._redis is None:
            logger.error("Redis обязателен: подключение не удалось. Запусти Redis и перезапусти бота.")
            return
        logger.info("Redis подключён (режим require_redis)")
    else:
        await store.ensure_redis(wait_seconds=30 if REDIS_URL else 0)

    await send_startup_message()
    from bot_handlers import run_bot_polling

    asyncio.create_task(run_bot_polling(store))

    health_kline: dict[str, Any] = {"last_message_mono": None}
    health_ticker: dict[str, Any] | None = (
        {"last_message_mono": None} if MARKET_MOVERS_CHAT_ID else None
    )

    async with aiohttp.ClientSession() as session:
        symbols = await fetch_contract_list(session)
        symbols = [s for s in symbols if s.endswith("_USDT")]
        logger.info("Found %d USDT perpetual contracts", len(symbols))
        if not symbols:
            logger.error("No symbols; exit")
            return

        logger.info("Loading kline history (throttled for MEXC limit)...")
        await bootstrap_symbols_for_tf(session, store, symbols, interval="Hour4", tf_name="4H")

        now_mono = time.monotonic()
        last_symbol_ws_mono = {s: now_mono for s in symbols}
        ctx_4h: dict[str, Any] = {
            "store": store,
            "log_every_n": 10000,
            "tf_name": "4H",
            "threshold": RSI_THRESHOLD_4H,
            "last_symbol_ws_mono": last_symbol_ws_mono,
        }

        tasks: list[asyncio.Task] = []
        tasks.append(
            asyncio.create_task(
                run_ws_kline_stream(
                    symbols,
                    on_kline,
                    ctx_4h,
                    interval="Hour4",
                    health=health_kline,
                )
            )
        )
        tasks.append(asyncio.create_task(backup_rsi_loop(session, symbols, ctx_4h)))
        tasks.append(asyncio.create_task(heartbeat_loop()))
        tasks.append(asyncio.create_task(time_sync_loop(session)))
        tasks.append(asyncio.create_task(watchdog_loop(health_kline, health_ticker)))

        logger.info(
            "RSI 4H: RSI3(24) ≥ %.0f, %d пар; backup REST каждые %.0fs; stall WS %.0fs",
            RSI_THRESHOLD_4H,
            len(symbols),
            BACKUP_RSI_INTERVAL_SEC,
            WS_STALL_TIMEOUT_SEC,
        )

        ticker_store: dict[str, dict] = {}
        mover_new_last_sent: dict[str, float] = {}
        previous_top_symbols: set[str] = set()
        min_rise = MARKET_MOVERS_MIN_RISE_PCT / 100.0

        async def on_tickers(data_list: list, ctx: dict) -> None:
            for item in data_list:
                if isinstance(item, dict) and item.get("symbol"):
                    ticker_store[item["symbol"]] = item

        async def market_movers_loop() -> None:
            nonlocal previous_top_symbols
            while True:
                await asyncio.sleep(MARKET_MOVERS_INTERVAL_SEC)
                try:
                    if not MARKET_MOVERS_CHAT_ID or not ticker_store:
                        continue
                    candidates = [
                        (sym, t)
                        for sym, t in ticker_store.items()
                        if sym.endswith("_USDT")
                        and (t.get("riseFallRate") or 0) >= min_rise
                        and (t.get("volume24") or 0) > 0
                    ]
                    candidates.sort(key=lambda x: float(x[1].get("volume24") or 0), reverse=True)
                    top = candidates[:MARKET_MOVERS_TOP_N]
                    if not top:
                        continue
                    current_top = {sym for sym, _ in top}
                    if not previous_top_symbols:
                        previous_top_symbols = current_top
                        continue
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
                            logger.info(
                                "Market mover new alert: %s +%.2f%% (появился в списке)",
                                sym,
                                rise_pct,
                            )
                            await asyncio.sleep(MARKET_MOVERS_ALERT_DELAY_SEC)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("market_movers_loop error; continue")
                    await asyncio.sleep(5)

        if MARKET_MOVERS_CHAT_ID:
            logger.info(
                "Маркет-муверы: канал %s",
                MARKET_MOVERS_CHAT_ID,
            )
            tasks.append(
                asyncio.create_task(
                    run_ws_ticker_stream(
                        on_tickers,
                        ticker_store,
                        health=health_ticker,
                    )
                )
            )
            tasks.append(asyncio.create_task(market_movers_loop()))
        else:
            logger.info("Маркет-муверы: отключены (telegram.market_movers_chat_id)")

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
