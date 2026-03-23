"""Простой circuit breaker для REST MEXC: при лавине ошибок — пауза."""
from __future__ import annotations

import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class CircuitBreaker:
    def __init__(
        self,
        *,
        fail_threshold: int = 8,
        window_sec: float = 60.0,
        open_sec: float = 45.0,
    ) -> None:
        self._fail_threshold = max(1, fail_threshold)
        self._window_sec = window_sec
        self._open_sec = open_sec
        self._fail_times: list[float] = []
        self._open_until: float = 0.0
        self._lock = asyncio.Lock()

    async def before_call(self) -> None:
        async with self._lock:
            now = time.monotonic()
            if now < self._open_until:
                wait = self._open_until - now
                logger.warning("CircuitBreaker: open, sleep %.1fs", wait)
                await asyncio.sleep(wait)
            self._fail_times = [t for t in self._fail_times if now - t <= self._window_sec]

    def on_success(self) -> None:
        self._fail_times.clear()

    async def on_failure(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self._fail_times.append(now)
            self._fail_times = [t for t in self._fail_times if now - t <= self._window_sec]
            if len(self._fail_times) >= self._fail_threshold:
                self._open_until = now + self._open_sec
                logger.error(
                    "CircuitBreaker: OPEN на %.0fs после %d ошибок за %.0fs",
                    self._open_sec,
                    len(self._fail_times),
                    self._window_sec,
                )
                self._fail_times.clear()
