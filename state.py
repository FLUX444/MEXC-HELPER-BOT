"""Per-symbol state and optional Redis or SQLite persistence for alert_sent."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from config import RSI_PERIOD

logger = logging.getLogger(__name__)


def _is_local_redis_url(url: str) -> bool:
    """Проверяет, что URL указывает на локальный Redis (localhost:6379)."""
    if not url:
        return False
    url = url.strip().lower()
    return ("localhost" in url or "127.0.0.1" in url) and "6379" in url


async def _try_start_redis_docker() -> None:
    """Пробует поднять Redis в Docker: docker start redis или docker run ... redis:7-alpine."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "start", "redis",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()
        if proc.returncode == 0:
            logger.info("Redis запущен в Docker (docker start redis)")
            await asyncio.sleep(2)
            return
    except FileNotFoundError:
        logger.debug("docker не найден, пропуск автозапуска Redis")
        return
    except Exception as e:
        logger.debug("docker start redis: %s", e)
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "run", "-d", "--name", "redis", "-p", "6379:6379", "redis:7-alpine",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode == 0:
            logger.info("Redis контейнер создан и запущен (docker run redis:7-alpine)")
            await asyncio.sleep(3)
        else:
            logger.debug("docker run redis: %s", stderr.decode() if stderr else proc.returncode)
    except Exception as e:
        logger.debug("docker run redis: %s", e)


@dataclass
class SymbolState:
    """State for one symbol: last 24 closed closes + current close + alert flag."""
    symbol: str
    closed_closes: list[float] = field(default_factory=list)  # len <= RSI_PERIOD, oldest first
    current_close: float = 0.0
    candle_start_time: int = 0  # seconds
    alert_sent: bool = False

    def closes_for_rsi(self) -> list[float]:
        """24 closed + current (order: oldest .. newest)."""
        out = list(self.closed_closes)[-RSI_PERIOD:]
        if self.current_close and (not out or out[-1] != self.current_close):
            out.append(self.current_close)
        return out

    def roll_to_new_candle(self, new_close: float, new_candle_start: int) -> None:
        """Move current close into history, set new candle, reset alert_sent."""
        if self.current_close and self.candle_start_time:
            self.closed_closes.append(self.current_close)
            if len(self.closed_closes) > RSI_PERIOD:
                self.closed_closes.pop(0)
        self.current_close = new_close
        self.candle_start_time = new_candle_start
        self.alert_sent = False


class StateStore:
    """In-memory state + Redis или SQLite для alert_sent и последних сигналов."""

    def __init__(self, redis_url: str = "", sqlite_path: str = ""):
        self._states: dict[str, SymbolState] = {}
        self._redis_url = redis_url
        self._redis = None
        self._sqlite_path = (sqlite_path or "").strip()
        self._sqlite: "SQLiteStore | None" = None

    async def ensure_redis(self, wait_seconds: float = 0):
        """Подключение к Redis или SQLite. Если Redis по localhost:6379 недоступен — пробуем поднять контейнер Docker."""
        if self._redis_url:
            pass  # используем Redis
        elif self._sqlite_path:
            if self._sqlite is None:
                from db_sqlite import SQLiteStore
                self._sqlite = SQLiteStore(self._sqlite_path)
                await self._sqlite.ensure()
            return
        if not self._redis_url or self._redis is not None:
            return
        import asyncio
        import time
        from redis.asyncio import Redis
        deadline = (time.monotonic() + wait_seconds) if wait_seconds else 0
        docker_tried = False
        while True:
            try:
                self._redis = Redis.from_url(self._redis_url, decode_responses=True)
                await self._redis.ping()
                logger.info("Redis connected")
                return
            except Exception as e:
                if wait_seconds and time.monotonic() < deadline:
                    if not docker_tried and _is_local_redis_url(self._redis_url):
                        docker_tried = True
                        await _try_start_redis_docker()
                    logger.info("Redis not ready, retry in 2s: %s", e)
                    await asyncio.sleep(2)
                else:
                    logger.warning("Redis not available: %s", e)
                    self._redis = None
                    return

    def get_or_create(self, key: str) -> SymbolState:
        """State key может быть вида 'BTC_USDT@1H' или 'BTC_USDT@4H'."""
        if key not in self._states:
            self._states[key] = SymbolState(symbol=key)
        return self._states[key]

    def init_symbol(self, key: str, closed_closes: list[float], candle_start: int, current_close: float):
        s = self.get_or_create(key)
        s.closed_closes = list(closed_closes)[-RSI_PERIOD:]
        s.candle_start_time = candle_start
        s.current_close = current_close
        s.alert_sent = False

    async def get_alert_sent(self, symbol: str, candle_start: int) -> bool:
        if self._sqlite:
            return await self._sqlite.get_alert_sent(symbol, candle_start)
        if self._redis:
            try:
                key = f"rsi_alert:{symbol}:{candle_start}"
                v = await self._redis.get(key)
                return v == "1"
            except Exception:
                pass
        s = self._states.get(symbol)
        return s.alert_sent if s else False

    async def set_alert_sent(self, symbol: str, candle_start: int) -> None:
        s = self.get_or_create(symbol)
        s.alert_sent = True
        if self._sqlite:
            await self._sqlite.set_alert_sent(symbol, candle_start)
            return
        if self._redis:
            try:
                key = f"rsi_alert:{symbol}:{candle_start}"
                await self._redis.set(key, "1", ex=86400 * 2)
            except Exception:
                pass

    LAST_SIGNALS_KEY = "mexc:last_signals"
    LAST_SIGNALS_MAX = 20

    async def push_last_signal(self, symbol: str, rsi_val: float, price: float, candle_start: int, *, tf_name: str | None = None) -> None:
        """Добавить последний сигнал в список для кнопки «Последние уведомления»."""
        if self._sqlite:
            await self._sqlite.push_last_signal(symbol, rsi_val, price, candle_start, tf_name)
            return
        if not self._redis:
            return
        import json
        import time
        try:
            item = json.dumps(
                {
                    "symbol": symbol,
                    "rsi": round(rsi_val, 2),
                    "price": price,
                    "candle_start": candle_start,
                    "tf": tf_name,
                    "ts": int(time.time()),
                },
                ensure_ascii=False,
            )
            await self._redis.lpush(self.LAST_SIGNALS_KEY, item)
            await self._redis.ltrim(self.LAST_SIGNALS_KEY, 0, self.LAST_SIGNALS_MAX - 1)
        except Exception as e:
            logger.debug("push_last_signal: %s", e)

    async def get_last_signals(self, limit: int = 10) -> list[dict]:
        """Для кнопки «Последние уведомления»."""
        if self._sqlite:
            return await self._sqlite.get_last_signals(limit)
        if self._redis:
            import json
            raw = await self._redis.lrange(self.LAST_SIGNALS_KEY, 0, limit - 1)
            out = []
            for s in raw:
                try:
                    out.append(json.loads(s))
                except Exception:
                    pass
            return out
        return []

    async def get_db_stats(self) -> tuple[int, int]:
        """(число alert-ключей, число последних сигналов)."""
        if self._sqlite:
            return await self._sqlite.get_db_stats()
        if self._redis:
            n_alert = len(await self._redis.keys("rsi_alert:*"))
            n_sig = await self._redis.llen(self.LAST_SIGNALS_KEY)
            return n_alert, n_sig
        return 0, 0

    async def close(self) -> None:
        if self._sqlite:
            self._sqlite.close()
            self._sqlite = None
        if self._redis:
            try:
                await self._redis.aclose()
            except Exception:
                pass
            self._redis = None
