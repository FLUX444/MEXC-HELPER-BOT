"""RSI(24) on close prices — только RSI3(24), как на графике MEXC."""
from __future__ import annotations

import numpy as np

# Период только 24 (RSI3 на графике); RSI1(6) не используется
RSI_PERIOD = 24


def rsi(closes: list[float], period: int = 24) -> float | None:
    """
    RSI по последним 25 ценам закрытия (24 периода). Всегда RSI(24).
    period из аргумента не используется — только RSI_PERIOD = 24.
    """
    period = RSI_PERIOD
    closes_arr = np.asarray(closes, dtype=float)
    if len(closes_arr) < period + 1:
        return None
    window = closes_arr[-(period + 1) :]
    deltas = np.diff(window)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = float(np.mean(gains))
    avg_loss = float(np.mean(losses))
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))
