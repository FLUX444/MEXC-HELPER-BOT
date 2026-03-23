"""Aiogram: /start с шапкой и inline-кнопками, последние уведомления (ссылки на MEXC), таблица БД."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import MenuButtonType, ParseMode
from aiogram.filters import Command, CommandStart
from pathlib import Path

from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    BotCommand,
    MenuButtonCommands,
)

from config import BASE_DIR, REDIS_URL, TELEGRAM_BOT_TOKEN, MESSAGES
from telegram_notify import send_test_signal

# Папка для шапки: положи сюда файл header.png / header.jpg / header.webp — при /start будет использоваться
ASSETS_DIR = BASE_DIR / "assets"
HEADER_NAMES = ("header.png", "header.jpg", "header.jpeg", "header.webp", "шапка.png", "шапка.jpg")

logger = logging.getLogger(__name__)

router = Router()

MEXC_FUTURES_URL = "https://contract.mexc.com/exchange/"

# Inline-кнопки главного меню
def get_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 Последние уведомления", callback_data="last"),
            InlineKeyboardButton(text="🗄 Просмотр БД", callback_data="db"),
        ],
    ])


def get_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀ Назад в меню", callback_data="back")],
    ])


_STORE = None


def set_store(store):
    global _STORE
    _STORE = store


def _mexc_link(symbol: str, label: str | None = None) -> str:
    """HTML-ссылка на пару на MEXC (без указания таймфрейма, открывается с последним использованным на сайте)."""
    label = label or symbol.replace("_", "")
    url = MEXC_FUTURES_URL + symbol
    return f'<a href="{url}">#{label}</a>'


def _format_last_signal_ago(ts: int) -> str:
    """Строка вида «N мин назад» или «только что»."""
    if not ts:
        return ""
    now = int(datetime.now(timezone.utc).timestamp())
    diff_sec = now - ts
    if diff_sec < 60:
        return "только что"
    if diff_sec < 3600:
        return f"{diff_sec // 60} мин назад"
    return f"{diff_sec // 3600} ч назад"


async def _last_signals_text_from_store(store) -> str:
    try:
        raw = await store.get_last_signals(10)
        if not raw:
            return "📭 <b>Последних уведомлений пока нет.</b>\n\nБудут сигналы 4H: PRE-ALERT от RSI 85, основной при RSI ≥ 90 (открытая свеча)."
        first_ts = raw[0].get("ts", 0) if raw else 0
        ago = _format_last_signal_ago(first_ts)
        header = "📋 <b>Последние уведомления (основной сигнал RSI ≥ 90)</b>\n"
        if ago:
            header += f"⏱ Последнее: <b>{ago}</b>\n\n"
        lines = [header]
        for i, d in enumerate(raw, 1):
            sym = (d.get("symbol") or "").strip()
            sym_display = sym.replace("_", "")
            rsi_v = d.get("rsi", 0)
            price = d.get("price", 0)
            ts = d.get("ts", 0)
            tf = d.get("tf") or ""
            tf_part = f" [{tf}]" if tf else ""
            time_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d.%m %H:%M UTC") if ts else ""
            link = _mexc_link(sym, sym_display)
            lines.append(f"{i}. {link}{tf_part}  RSI <b>{rsi_v}</b>  {price:.4g} USDT  {time_str}")
        return "\n".join(lines)
    except Exception as e:
        logger.warning("_last_signals_text: %s", e)
        return "❌ Ошибка чтения данных."


def _db_table(n_alert: int, n_signals: int) -> str:
    """Таблица БД для отображения в Telegram (рисуется в чате)."""
    a, b = str(n_alert).rjust(6), str(n_signals).rjust(6)
    return (
        "🗄 <b>Просмотр БД</b>\n\n"
        "<pre>"
        "+---------------------------+--------+\n"
        "| Параметр                  | Знач.  |\n"
        "+---------------------------+--------+\n"
        f"| Alert (сигнал/свеча)     | {a} |\n"
        f"| Последних уведомлений    | {b} |\n"
        "+---------------------------+--------+"
        "</pre>"
    )


async def _db_view_text_from_store(store) -> str:
    try:
        n_alert, n_signals = await store.get_db_stats()
        return _db_table(n_alert, n_signals)
    except Exception as e:
        logger.warning("_db_view_text: %s", e)
        return "❌ Ошибка чтения БД."


async def _get_redis():
    if not REDIS_URL:
        return None
    try:
        from redis.asyncio import Redis
        r = Redis.from_url(REDIS_URL, decode_responses=True)
        await r.ping()
        return r
    except Exception:
        return None


async def _last_signals_text(redis) -> str:
    if not redis:
        return "❌ БД не подключена. Укажи Redis или SQLite в config/keys.yml."
    try:
        key = "mexc:last_signals"
        raw = await redis.lrange(key, 0, 9)
        if not raw:
            return "📭 <b>Последних уведомлений пока нет.</b>\n\nБудут сигналы 4H: PRE-ALERT от RSI 85, основной при RSI ≥ 90 (открытая свеча)."
        first_ts = 0
        try:
            first_ts = json.loads(raw[0]).get("ts", 0) if raw else 0
        except Exception:
            pass
        ago = _format_last_signal_ago(first_ts)
        header = "📋 <b>Последние уведомления (основной сигнал RSI ≥ 90)</b>\n"
        if ago:
            header += f"⏱ Последнее: <b>{ago}</b>\n\n"
        lines = [header]
        for i, s in enumerate(raw, 1):
            try:
                d = json.loads(s)
                sym = (d.get("symbol") or "").strip()
                sym_display = sym.replace("_", "")
                rsi_v = d.get("rsi", 0)
                price = d.get("price", 0)
                ts = d.get("ts", 0)
                time_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d.%m %H:%M UTC") if ts else ""
                link = _mexc_link(sym, sym_display)
                lines.append(f"{i}. {link}  RSI <b>{rsi_v}</b>  {price:.4g} USDT  {time_str}")
            except Exception:
                continue
        return "\n".join(lines)
    except Exception as e:
        logger.warning("_last_signals_text: %s", e)
        return "❌ Ошибка чтения данных."


def _get_welcome_text() -> str:
    cfg = (MESSAGES or {}).get("welcome") or {}
    return cfg.get("text") or (
        "👋 Добро пожаловать!\n\n"
        "📊 4H: PRE-ALERT (RSI 85+), основной сигнал ≥ 90.\n\n"
        "🔔 Нажми на кнопки ниже — последние уведомления и просмотр БД."
    )


def _get_header_path() -> Path | None:
    """Путь к файлу шапки из папки assets/ (первый найденный header.* или шапка.*)."""
    if not ASSETS_DIR.is_dir():
        return None
    for name in HEADER_NAMES:
        p = ASSETS_DIR / name
        if p.is_file():
            return p
    return None


async def _db_view_text(redis) -> str:
    if not redis:
        return "❌ БД не подключена. Укажи Redis или SQLite в config/keys.yml."
    try:
        keys_alert = await redis.keys("rsi_alert:*")
        n_signals = await redis.llen("mexc:last_signals")
        return _db_table(len(keys_alert), n_signals)
    except Exception as e:
        logger.warning("_db_view_text: %s", e)
        return "❌ Ошибка чтения БД."


@router.message(CommandStart())
async def cmd_start(message: Message, bot: Bot) -> None:
    text = _get_welcome_text()
    keyboard = get_inline_keyboard()
    header_path = _get_header_path()
    if header_path is not None:
        try:
            await message.answer_photo(
                photo=FSInputFile(header_path),
                caption=text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard,
            )
            return
        except Exception as e:
            logger.warning("Шапка из assets/ не отправлена: %s", e)
    cfg = (MESSAGES or {}).get("welcome") or {}
    header_url = (cfg.get("header_image_url") or "").strip()
    if header_url:
        try:
            await message.answer_photo(photo=header_url, caption=text, parse_mode=ParseMode.HTML, reply_markup=keyboard)
            return
        except Exception:
            pass
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


async def _edit_to_content(message: Message, text: str) -> None:
    """Редактирует сообщение: только панель (БД или уведомления) + кнопка «Назад»."""
    keyboard = get_back_keyboard()
    if message.photo:
        # В Telegram caption не более 1024 символов
        cap = text if len(text) <= 1024 else text[:1021] + "..."
        await message.edit_caption(caption=cap, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    else:
        await message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)


@router.callback_query(F.data == "last")
async def cb_last(callback: CallbackQuery, bot: Bot) -> None:
    await callback.answer()
    if _STORE is not None:
        text = await _last_signals_text_from_store(_STORE)
    else:
        redis = await _get_redis()
        text = await _last_signals_text(redis)
    await _edit_to_content(callback.message, text)


@router.callback_query(F.data == "db")
async def cb_db(callback: CallbackQuery, bot: Bot) -> None:
    await callback.answer()
    if _STORE is not None:
        text = await _db_view_text_from_store(_STORE)
    else:
        redis = await _get_redis()
        text = await _db_view_text(redis)
    await _edit_to_content(callback.message, text)


@router.callback_query(F.data == "back")
async def cb_back(callback: CallbackQuery, bot: Bot) -> None:
    await callback.answer()
    welcome = _get_welcome_text()
    keyboard = get_inline_keyboard()
    if callback.message.photo:
        await callback.message.edit_caption(caption=welcome, parse_mode=ParseMode.HTML, reply_markup=keyboard)
    else:
        await callback.message.edit_text(welcome, parse_mode=ParseMode.HTML, reply_markup=keyboard)


@router.message(Command("last"))
@router.message(F.text.in_(["📋 Последние уведомления", "Последние уведомления"]))
async def btn_last(message: Message) -> None:
    if _STORE is not None:
        text = await _last_signals_text_from_store(_STORE)
    else:
        redis = await _get_redis()
        text = await _last_signals_text(redis)
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_back_keyboard())


@router.message(Command("test"))
async def cmd_test(message: Message) -> None:
    """Отправляет тестовое уведомление в канал — проверить, приходят ли и как быстро."""
    ok, sent_at = await send_test_signal()
    if ok:
        await message.answer(
            f"✅ Тест отправлен в канал в <b>{sent_at}</b>.\n\n"
            "Проверь канал: задержка = время получения минус время в сообщении.",
            parse_mode=ParseMode.HTML,
        )
    else:
        await message.answer(
            "❌ Не удалось отправить тест. Проверь config/keys.yml: telegram.chat_id и bot_token.",
            parse_mode=ParseMode.HTML,
        )


@router.message(Command("db"))
@router.message(F.text.in_(["🗄 Просмотр БД", "Просмотр БД"]))
async def btn_db(message: Message) -> None:
    if _STORE is not None:
        text = await _db_view_text_from_store(_STORE)
    else:
        redis = await _get_redis()
        text = await _db_view_text(redis)
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=get_back_keyboard())


@router.message(F.text)
async def delete_user_message(message: Message, bot: Bot) -> None:
    """Удаляем любое текстовое сообщение (чат только с меню). В канале/группе — удаляем сообщение пользователя; в личке бот не может удалить чужое — отправляем подсказку и через 3 сек удаляем её."""
    try:
        await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
    except Exception:
        hint = await message.answer("👆 Используйте кнопки меню или команды /start, /last, /db, /test")
        await asyncio.sleep(3)
        try:
            await hint.delete()
        except Exception:
            pass


async def setup_bot_commands(bot: Bot) -> None:
    await bot.set_my_commands([
        BotCommand(command="start", description="Старт"),
        BotCommand(command="last", description="Последние уведомления"),
        BotCommand(command="db", description="Просмотр БД"),
        BotCommand(command="test", description="Тест уведомления в канал"),
    ])
    await bot.set_chat_menu_button(menu_button=MenuButtonCommands(type=MenuButtonType.COMMANDS))


async def run_bot_polling(store=None) -> None:
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN не задан — интерактивный бот не запущен")
        return
    set_store(store)
    dp = Dispatcher()
    dp.include_router(router)
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    await setup_bot_commands(bot)
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
