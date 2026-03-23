"""
Сканер RSI3(24) на 4H (открытая свеча, Уайлдер как MEXC).
Продакшн: WS stall + backoff, REST backup, SETNX-антидубли, watchdog, heartbeat, логи, circuit breaker.

Условия уведомлений (единственные пути в Telegram):
  MAIN:  wilder_ready, прошло >= min_alert_delay_4h_sec с открытия свечи, RSI >= rsi_threshold_4h,
         try_claim_alert (Redis rsi_alert:state_key:candle) — иначе не шлём.
  PRE:   та же задержка свечи; RSI < main; RSI >= rsi_prealert_min; по свече ещё не было main;
         try_claim_prealert (prealert:symbol:candle), TTL 4 ч — один раз на свечу.
  При RSI >= main ветка PRE не выполняется (return до неё).

require_redis: true — нужен Redis для SETNX.
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
from indicators import wilder_rsi_for_forming_candle
from mexc_client import (
    fetch_contract_list,
    fetch_klines,
    fetch_klines_batch,
    fetch_server_time,
    run_ws_kline_stream,
)
from state import StateStore
from telegram_notify import send_prealert, send_signal, send_startup_message

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

    # RSI Уайлдер (как на MEXC), не SMA по 24 дельтам
    if not state.wilder_ready or not state.closed_closes:
        return
    last_closed = state.closed_closes[-1]
    rsi_val = wilder_rsi_for_forming_candle(
        state.avg_gain_w,
        state.avg_loss_w,
        last_closed,
        state.current_close,
        RSI_PERIOD,
    )
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
        # Нужно ≥ period+1 закрытых + 1 формирующаяся (итого period+2 свечей в ответе)
        if len(closes) < RSI_PERIOD + 2:
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


async def watchdog_loop(health_kline: dict[str, Any]) -> None:
    while True:
        await asyncio.sleep(WATCHDOG_INTERVAL_SEC)
        now = time.monotonic()
        last = health_kline.get("last_message_mono")
        if last is not None:
            age = now - last
            if age > WATCHDOG_WARN_AFTER_SEC:
                logger.error(
                    "WATCHDOG: kline_4h — нет WS-трафика %.0f с (stall-timeout переподключит WS)",
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
        tasks.append(asyncio.create_task(watchdog_loop(health_kline)))

        logger.info(
            "RSI 4H: main ≥ %.0f, pre ≥ %.0f; %d пар; backup REST %.0fs; stall WS %.0fs",
            RSI_THRESHOLD_4H,
            RSI_PREALERT_MIN,
            len(symbols),
            BACKUP_RSI_INTERVAL_SEC,
            WS_STALL_TIMEOUT_SEC,
        )

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
