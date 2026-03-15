"""SQLite-хранилище вместо Redis: alert_sent и последние сигналы. Без отдельного сервера."""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from pathlib import Path

logger = logging.getLogger(__name__)

ALERTS_TABLE = "rsi_alerts"
SIGNALS_TABLE = "last_signals"
LAST_SIGNALS_MAX = 20


def _init_conn(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    # check_same_thread=False: соединение передаётся в asyncio.to_thread(), нужен доступ из других потоков
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS {ALERTS_TABLE} (symbol TEXT, candle_start INTEGER, PRIMARY KEY (symbol, candle_start))"
    )
    conn.execute(
        f"""CREATE TABLE IF NOT EXISTS {SIGNALS_TABLE}
        (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, rsi REAL, price REAL, candle_start INTEGER, ts INTEGER)"""
    )
    try:
        conn.execute(f"ALTER TABLE {SIGNALS_TABLE} ADD COLUMN tf TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # колонка уже есть
    return conn


def _get_alert_sent_sync(conn: sqlite3.Connection, symbol: str, candle_start: int) -> bool:
    row = conn.execute(
        f"SELECT 1 FROM {ALERTS_TABLE} WHERE symbol=? AND candle_start=?",
        (symbol, candle_start),
    ).fetchone()
    return row is not None


def _set_alert_sent_sync(conn: sqlite3.Connection, symbol: str, candle_start: int) -> None:
    conn.execute(
        f"INSERT OR REPLACE INTO {ALERTS_TABLE} (symbol, candle_start) VALUES (?, ?)",
        (symbol, candle_start),
    )
    # Удалить старые (старше 2 дней)
    expire = int(time.time()) - 86400 * 2
    conn.execute(f"DELETE FROM {ALERTS_TABLE} WHERE candle_start < ?", (expire,))
    conn.commit()


def _push_last_signal_sync(
    conn: sqlite3.Connection, symbol: str, rsi_val: float, price: float, candle_start: int, tf: str | None = None
) -> None:
    ts = int(time.time())
    conn.execute(
        f"INSERT INTO {SIGNALS_TABLE} (symbol, rsi, price, candle_start, ts, tf) VALUES (?, ?, ?, ?, ?, ?)",
        (symbol, round(rsi_val, 2), price, candle_start, ts, tf or ""),
    )
    # Оставить только последние LAST_SIGNALS_MAX
    cur = conn.execute(f"SELECT id FROM {SIGNALS_TABLE} ORDER BY id DESC LIMIT 1 OFFSET ?", (LAST_SIGNALS_MAX,))
    row = cur.fetchone()
    if row:
        conn.execute("DELETE FROM {} WHERE id <= ?".format(SIGNALS_TABLE), (row[0],))
    conn.commit()


def _get_last_signals_sync(conn: sqlite3.Connection, limit: int = 10) -> list[dict]:
    cur = conn.execute(f"PRAGMA table_info({SIGNALS_TABLE})")
    has_tf = any(row[1] == "tf" for row in cur.fetchall())
    if has_tf:
        rows = conn.execute(
            f"SELECT symbol, rsi, price, ts, tf FROM {SIGNALS_TABLE} ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [{"symbol": r[0], "rsi": r[1], "price": r[2], "ts": r[3], "tf": r[4] or ""} for r in rows]
    rows = conn.execute(
        f"SELECT symbol, rsi, price, ts FROM {SIGNALS_TABLE} ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [{"symbol": r[0], "rsi": r[1], "price": r[2], "ts": r[3], "tf": ""} for r in rows]


def _get_db_stats_sync(conn: sqlite3.Connection) -> tuple[int, int]:
    n_alerts = conn.execute(f"SELECT COUNT(*) FROM {ALERTS_TABLE}").fetchone()[0]
    n_signals = conn.execute(f"SELECT COUNT(*) FROM {SIGNALS_TABLE}").fetchone()[0]
    return n_alerts, n_signals


class SQLiteStore:
    """Хранилище на SQLite для одного процесса. Блокирующие вызовы в executor."""

    def __init__(self, path: str):
        self._path = path
        self._conn: sqlite3.Connection | None = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = _init_conn(self._path)
        return self._conn

    async def ensure(self) -> None:
        self._conn = await asyncio.to_thread(_init_conn, self._path)
        logger.info("SQLite connected: %s", self._path)

    async def get_alert_sent(self, symbol: str, candle_start: int) -> bool:
        conn = self._get_conn()
        return await asyncio.to_thread(_get_alert_sent_sync, conn, symbol, candle_start)

    async def set_alert_sent(self, symbol: str, candle_start: int) -> None:
        conn = self._get_conn()
        await asyncio.to_thread(_set_alert_sent_sync, conn, symbol, candle_start)

    async def push_last_signal(
        self, symbol: str, rsi_val: float, price: float, candle_start: int, tf: str | None = None
    ) -> None:
        conn = self._get_conn()
        await asyncio.to_thread(_push_last_signal_sync, conn, symbol, rsi_val, price, candle_start, tf)

    async def get_last_signals(self, limit: int = 10) -> list[dict]:
        conn = self._get_conn()
        return await asyncio.to_thread(_get_last_signals_sync, conn, limit)

    async def get_db_stats(self) -> tuple[int, int]:
        conn = self._get_conn()
        return await asyncio.to_thread(_get_db_stats_sync, conn)

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
