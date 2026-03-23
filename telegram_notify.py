"""Telegram: уведомления RSI 4H (main + PRE-ALERT). Тексты из config/messages.yml. Aiogram."""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import FSInputFile

from config import (
    BASE_DIR,
    MESSAGES,
    RSI_PREALERT_MIN,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_RETRY_BASE_DELAY_SEC,
    TELEGRAM_SEND_RETRIES,
)

logger = logging.getLogger(__name__)

# Очередь по смыслу: не бомбить Telegram параллельно (flood / rate limit)
_telegram_send_lock = asyncio.Lock()

# Чтобы не спамить лог при постоянных "chat not found"
_last_chat_not_found_log = 0.0
_CHAT_NOT_FOUND_INTERVAL = 60.0  # секунд между повторными сообщениями

# Ссылка на пару на MEXC Futures (нажав на монету — откроется эта пара)
MEXC_FUTURES_URL = "https://contract.mexc.com/exchange/"

# Папка для шапки старта бота (в канал и в личку): header.png / header.jpg / header.webp
ASSETS_DIR = BASE_DIR / "assets"
HEADER_NAMES = ("header.png", "header.jpg", "header.jpeg", "header.webp", "шапка.png", "шапка.jpg")


def _get_header_path() -> Path | None:
    """Путь к файлу шапки (header.* или шапка.*) для стартового сообщения."""
    if not ASSETS_DIR.is_dir():
        return None
    for name in HEADER_NAMES:
        p = ASSETS_DIR / name
        if p.is_file():
            return p
    return None


def _parse_chat_id(raw: str) -> int | str | None:
    """
    Возвращает chat_id для Telegram: число (личный чат/канал) или @username канала.
    Поддерживает: -1003755077278, 123456, @MEXCALERTS1.
    """
    s = (raw or "").strip()
    if not s or s.lower() in ("your_chat_id", "your_chat_id_here"):
        return None
    # Канал по username (например @MEXCALERTS1)
    if s.startswith("@"):
        username = s[1:].strip()
        if username and username.isalnum():
            return s
        return None
    # Числовой ID (личный чат или канал)
    if s.startswith("-"):
        if not s[1:].isdigit():
            return None
    elif not s.isdigit():
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _get_chat_id() -> int | str | None:
    """Канал/чат для уведомлений (число или @username)."""
    return _parse_chat_id(TELEGRAM_CHAT_ID)


def _fmt_utc(ts_sec: int) -> str:
    return datetime.fromtimestamp(ts_sec, tz=timezone.utc).strftime("%H:%M UTC")


def _fmt_elapsed(sec: int) -> str:
    """Человекочитаемое время от открытия свечи (например 1ч 20м)."""
    if sec < 0:
        sec = 0
    h = sec // 3600
    m = (sec % 3600) // 60
    parts: list[str] = []
    if h:
        parts.append(f"{h}ч")
    parts.append(f"{m}м")
    return " ".join(parts)


def _msg_cfg() -> dict:
    return (MESSAGES or {}).get("rsi_signal") or {}


def build_message(
    symbol: str,
    rsi_value: float,
    price: float,
    candle_start_sec: int,
    *,
    tf_name: str | None = None,
) -> str:
    """Формат по ТЗ: RSI3(24) по открытой свече, «Свеча открыта», «Свеча формируется», ссылка."""
    cfg = _msg_cfg()
    title = cfg.get("title") or "🚨 RSI SIGNAL (4H)"
    subtitle = cfg.get("subtitle") or "RSI ≥ 90"
    coin_label = cfg.get("coin_label") or "Монета"
    rsi_label = cfg.get("rsi_label") or "RSI3(24)"
    price_label = cfg.get("price_label") or "Цена"
    price_suffix = cfg.get("price_suffix") or "USDT"
    candle_open_label = cfg.get("candle_start_label") or "Свеча открыта"

    display_symbol = symbol.replace("_", "")
    if tf_name:
        link = f"{MEXC_FUTURES_URL}{symbol}?interval={tf_name}"
    else:
        link = MEXC_FUTURES_URL + symbol
    coin_link = f'<a href="{link}">#{display_symbol}</a>'

    tf_line = f"Таймфрейм: <b>{tf_name}</b>\n\n" if tf_name else ""

    return (
        f"{title}\n"
        f"{subtitle}\n\n"
        f"{coin_label}: {coin_link}\n"
        f"{rsi_label}: <b>{rsi_value:.2f}</b>\n"
        f"{tf_line}"
        f"💰 {price_label}: <b>{price:.4g}</b> {price_suffix}\n\n"
        f"🕐 {candle_open_label}: {_fmt_utc(candle_start_sec)} UTC\n"
        f"Свеча формируется\n\n"
        f"🔗 {link}"
    )


def _prealert_cfg() -> dict:
    return (MESSAGES or {}).get("rsi_prealert") or {}


def build_prealert_message(
    symbol: str,
    rsi_value: float,
    price: float,
    candle_start_sec: int,
    elapsed_sec: int,
    *,
    tf_name: str = "4H",
    main_threshold: float = 90.0,
) -> str:
    """Формат PRE-ALERT по ТЗ (диапазон 85 .. < main)."""
    cfg = _prealert_cfg()
    title = cfg.get("title") or "⚠️ RSI PRE-ALERT (4H)"
    rsi_label = cfg.get("rsi_label") or "RSI (24)"
    range_label = cfg.get("range_label") or "Диапазон"
    price_suffix = cfg.get("price_suffix") or "USDT"
    candle_label = cfg.get("candle_label") or "Свеча"
    elapsed_label = cfg.get("elapsed_label") or "Прошло"
    upper = main_threshold - 0.01
    range_str = cfg.get("range_text") or f"{RSI_PREALERT_MIN:.0f}–{upper:.2f}"

    display_symbol = symbol.replace("_", "")
    link = f"{MEXC_FUTURES_URL}{symbol}?interval={tf_name}"
    coin_link = f'<a href="{link}">#{display_symbol}</a>'

    return (
        f"{title}\n\n"
        f"Монета: {coin_link}\n"
        f"{rsi_label}: <b>{rsi_value:.2f}</b>\n"
        f"{range_label}: <b>{range_str}</b>\n\n"
        f"Цена: <b>{price:.4g}</b> {price_suffix}\n\n"
        f"{candle_label}: {_fmt_utc(candle_start_sec)} UTC\n"
        f"{elapsed_label}: {_fmt_elapsed(elapsed_sec)}\n\n"
        f"🔗 {link}"
    )


def _log_chat_not_found() -> None:
    global _last_chat_not_found_log
    now = time.monotonic()
    if now - _last_chat_not_found_log < _CHAT_NOT_FOUND_INTERVAL:
        return
    _last_chat_not_found_log = now
    logger.warning(
        "Telegram: chat not found. Добавь бота в канал как администратора с правом «Публикация сообщений» "
        "и проверь chat_id в config/keys.yml. Для канала ID вида -100xxxxxxxxxx (узнать: переслать пост из канала в @getidsbot)."
    )


async def send_startup_message() -> bool:
    """Отправляет сообщение о старте бота (чтобы убедиться, что уведомления приходят сюда)."""
    chat_id = _get_chat_id()
    if not TELEGRAM_BOT_TOKEN or chat_id is None:
        return False
    cfg = (MESSAGES or {}).get("startup") or {}
    text = cfg.get("text") or (
        "✅ MEXC RSI Scanner запущен 24/7. Только 4H: PRE-ALERT 85+, сигнал ≥90. Redis."
    )
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    header_path = _get_header_path()
    try:
        if header_path is not None:
            await bot.send_photo(chat_id=chat_id, photo=FSInputFile(header_path), caption=text)
        else:
            await bot.send_message(chat_id=chat_id, text=text)
        return True
    except TelegramBadRequest as e:
        if "chat not found" in (e.message or "").lower():
            _log_chat_not_found()
        else:
            logger.warning("Startup message failed: %s", e)
        return False
    except Exception as e:
        logger.warning("Startup message failed: %s", e)
        return False
    finally:
        await bot.session.close()


async def send_signal(symbol: str, rsi_value: float, price: float, candle_start_sec: int, *, tf_name: str | None = None) -> bool:
    chat_id = _get_chat_id()
    if not TELEGRAM_BOT_TOKEN or chat_id is None:
        logger.warning(
            "Telegram не настроен: укажи в config/keys.yml реальный telegram.chat_id (число). "
            "Сейчас указан плейсхолдер или пусто — уведомления не отправляются."
        )
        return False
    text = build_message(symbol, rsi_value, price, candle_start_sec, tf_name=tf_name)
    from aiogram.enums import ParseMode

    async with _telegram_send_lock:
        for attempt in range(TELEGRAM_SEND_RETRIES):
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            try:
                await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
                return True
            except TelegramBadRequest as e:
                if "chat not found" in (e.message or "").lower():
                    _log_chat_not_found()
                else:
                    logger.warning("Telegram send failed: %s", e)
                return False
            except Exception as e:
                logger.warning(
                    "Telegram send attempt %d/%d: %s",
                    attempt + 1,
                    TELEGRAM_SEND_RETRIES,
                    e,
                )
                if attempt + 1 < TELEGRAM_SEND_RETRIES:
                    await asyncio.sleep(TELEGRAM_RETRY_BASE_DELAY_SEC * (attempt + 1))
            finally:
                await bot.session.close()
    return False


async def send_prealert(
    symbol: str,
    rsi_value: float,
    price: float,
    candle_start_sec: int,
    elapsed_sec: int,
    *,
    tf_name: str = "4H",
    main_threshold: float = 90.0,
) -> bool:
    """PRE-ALERT 4H: один раз на свечу (SETNX в store до вызова)."""
    chat_id = _get_chat_id()
    if not TELEGRAM_BOT_TOKEN or chat_id is None:
        return False
    text = build_prealert_message(
        symbol,
        rsi_value,
        price,
        candle_start_sec,
        elapsed_sec,
        tf_name=tf_name,
        main_threshold=main_threshold,
    )
    from aiogram.enums import ParseMode

    async with _telegram_send_lock:
        for attempt in range(TELEGRAM_SEND_RETRIES):
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            try:
                await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
                return True
            except TelegramBadRequest as e:
                if "chat not found" in (e.message or "").lower():
                    _log_chat_not_found()
                else:
                    logger.warning("Telegram prealert failed: %s", e)
                return False
            except Exception as e:
                logger.warning(
                    "Telegram prealert attempt %d/%d: %s",
                    attempt + 1,
                    TELEGRAM_SEND_RETRIES,
                    e,
                )
                if attempt + 1 < TELEGRAM_SEND_RETRIES:
                    await asyncio.sleep(TELEGRAM_RETRY_BASE_DELAY_SEC * (attempt + 1))
            finally:
                await bot.session.close()
    return False


async def send_test_signal() -> tuple[bool, str]:
    """
    Отправляет тестовое уведомление в канал (тот же формат, что и реальные сигналы).
    Возвращает (успех, строка с временем отправки для отображения пользователю).
    Нужно для проверки: приходят ли уведомления, как быстро, правильный ли формат.
    """
    chat_id = _get_chat_id()
    if not TELEGRAM_BOT_TOKEN or chat_id is None:
        return False, ""
    now_sec = int(time.time())
    # Тестовое сообщение: как реальный сигнал, с кликабельной монетой
    test_symbol = "BTC_USDT"
    display_symbol = test_symbol.replace("_", "")
    link = MEXC_FUTURES_URL + test_symbol
    coin_link = f'<a href="{link}">#{display_symbol}</a>'
    text = (
        "🧪 <b>Уведомление (ТЕСТ)</b>\n\n"
        f"{coin_link}\n\n"
        f"🕐 <b>Отправлено в:</b> {datetime.fromtimestamp(now_sec, tz=timezone.utc).strftime('%H:%M:%S UTC')} "
        f"({datetime.fromtimestamp(now_sec, tz=timezone.utc).strftime('%d.%m.%Y')})\n\n"
        "Задержка = время получения минус время выше."
    )
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    try:
        from aiogram.enums import ParseMode
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
        sent_at = datetime.fromtimestamp(now_sec, tz=timezone.utc).strftime("%H:%M:%S UTC")
        return True, sent_at
    except TelegramBadRequest as e:
        if "chat not found" in (e.message or "").lower():
            _log_chat_not_found()
        else:
            logger.warning("Test signal failed: %s", e)
        return False, ""
    except Exception as e:
        logger.exception("Test signal failed: %s", e)
        return False, ""
    finally:
        await bot.session.close()
