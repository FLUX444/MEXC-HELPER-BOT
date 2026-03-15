"""Telegram notifications: RSI3(24) >= 90 (по закрытой свече), маркет-муверы. Тексты из config/messages.yml. Aiogram."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import FSInputFile

from config import BASE_DIR, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, MESSAGES, MARKET_MOVERS_CHAT_ID

logger = logging.getLogger(__name__)

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
    title = cfg.get("title") or "🚨 REAL-TIME RSI SIGNAL"
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
        f"{title}\n\n"
        f"{coin_label}: {coin_link}\n"
        f"{rsi_label}: <b>{rsi_value:.2f}</b>\n"
        f"{tf_line}"
        f"💰 {price_label}: <b>{price:.4g}</b> {price_suffix}\n\n"
        f"🕐 {candle_open_label}: {_fmt_utc(candle_start_sec)} UTC\n"
        f"Свеча формируется\n\n"
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
    text = cfg.get("text") or "✅ MEXC RSI Scanner запущен 24/7. RSI3(24) по открытой свече: 1H и 4H при ≥ 90. Маркет-муверы: только «Рост цены и высокий объём»."
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
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    try:
        from aiogram.enums import ParseMode
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
        return True
    except TelegramBadRequest as e:
        if "chat not found" in (e.message or "").lower():
            _log_chat_not_found()
        else:
            logger.warning("Telegram send failed: %s", e)
        return False
    except Exception as e:
        logger.exception("Telegram send failed: %s", e)
        return False
    finally:
        await bot.session.close()


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


def _market_mover_link(symbol: str) -> str:
    """Ссылка на пару фьючерсов MEXC."""
    return f"{MEXC_FUTURES_URL}{symbol}"


async def send_market_mover_alert(symbol: str, rise_pct: float, volume24: float, price: float) -> bool:
    """
    Отправляет одно уведомление «новое предложение» маркет-муверов в канал — с ссылкой на пару.
    """
    chat_id = _parse_chat_id(MARKET_MOVERS_CHAT_ID or "")
    if not TELEGRAM_BOT_TOKEN or not chat_id:
        return False
    display = symbol.replace("_", "")
    link = _market_mover_link(symbol)
    text = (
        f"📈 <b>MARKET MOVER</b>\n\n"
        f"Монета: <a href=\"{link}\">#{display} USDT</a> Бессрочный\n"
        f"Рост: <b>+{rise_pct:.2f}%</b>\n\n"
        f"Высокий объём торгов\n\n"
        f"🔗 {link}"
    )
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    try:
        from aiogram.enums import ParseMode
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
        return True
    except TelegramBadRequest as e:
        if "chat not found" in (e.message or "").lower():
            _log_chat_not_found()
        else:
            logger.warning("Market mover alert send failed: %s", e)
        return False
    except Exception as e:
        logger.exception("Market mover alert send failed: %s", e)
        return False
    finally:
        await bot.session.close()


async def send_market_movers(movers_lines: list[str]) -> bool:
    """
    Отправляет сводку «Маркет-муверы» (рост цены + высокий объём) в канал market_movers_chat_id.
    movers_lines — список строк (каждая — одна пара/событие).
    """
    chat_id = _parse_chat_id(MARKET_MOVERS_CHAT_ID or "")
    if not TELEGRAM_BOT_TOKEN or not chat_id or not movers_lines:
        return False
    text = "📊 <b>Маркет-муверы</b> (рост цены и высокий объём)\n\n" + "\n\n".join(movers_lines)
    if len(text) > 4000:
        text = text[:3997] + "..."
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    try:
        from aiogram.enums import ParseMode
        await bot.send_message(chat_id=chat_id, text=text, parse_mode=ParseMode.HTML)
        return True
    except TelegramBadRequest as e:
        if "chat not found" in (e.message or "").lower():
            _log_chat_not_found()
        else:
            logger.warning("Market movers send failed: %s", e)
        return False
    except Exception as e:
        logger.exception("Market movers send failed: %s", e)
        return False
    finally:
        await bot.session.close()
