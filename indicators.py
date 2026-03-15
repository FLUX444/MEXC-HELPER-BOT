"""RSI(24) on close prices."""
from __future__ import annotations

import numpy as np


def rsi(closes: list[float], period: int = 24) -> float | None:
    """
    RSI from last `period+1` closes (period closed + current).
    Returns None if not enough data.
    """
    closes_arr = np.asarray(closes, dtype=float)
    if len(closes_arr) < period + 1:
        return None
    # last (period+1) values: period deltas
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
