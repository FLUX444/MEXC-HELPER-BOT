"""RSI по Уайлдеру (Wilder / RMA) — как RSI3(24) на графике MEXC.

Не использовать SMA по дельтам: оно даёт другие значения (ложные сигналы).
"""
from __future__ import annotations

# Период только 24 (RSI3 на графике MEXC)
RSI_PERIOD = 24


def wilder_process_closed_closes(closes: list[float], period: int = RSI_PERIOD) -> tuple[float, float]:
    """
    Прогон по всем полностью закрытым свечам (без текущей формирующейся).
    Возвращает (avg_gain, avg_loss) после последнего закрытия — состояние для следующей дельты.
    Нужно минимум period+1 закрытий (period дельт для первого сида).
    """
    if len(closes) < period + 1:
        return 0.0, 0.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    if len(deltas) < period:
        return 0.0, 0.0
    gains = [max(d, 0.0) for d in deltas[:period]]
    losses = [max(-d, 0.0) for d in deltas[:period]]
    ag = sum(gains) / period
    al = sum(losses) / period
    for i in range(period, len(deltas)):
        d = deltas[i]
        g = max(d, 0.0)
        l = max(-d, 0.0)
        ag = (ag * (period - 1) + g) / period
        al = (al * (period - 1) + l) / period
    return ag, al


def wilder_rsi_for_forming_candle(
    avg_gain: float,
    avg_loss: float,
    last_closed_close: float,
    current_close: float,
    period: int = RSI_PERIOD,
) -> float | None:
    """
    RSI с учётом текущей открытой свечи: одна дельта last_closed → current
    поверх состояния Уайлдера на момент после последнего закрытия.
    """
    d = current_close - last_closed_close
    g = max(d, 0.0)
    l = max(-d, 0.0)
    ag = (avg_gain * (period - 1) + g) / period
    al = (avg_loss * (period - 1) + l) / period
    if al == 0.0:
        return 100.0 if ag > 0.0 else 50.0
    rs = ag / al
    return 100.0 - (100.0 / (1.0 + rs))


def rsi(closes: list[float], period: int = 24) -> float | None:
    """
    Устаревший SMA-RSI — оставлен для совместимости; не совпадает с MEXC.
    Используй wilder_rsi_for_forming_candle + wilder_process_closed_closes.
    """
    period = RSI_PERIOD
    if len(closes) < period + 1:
        return None
    import numpy as np

    window = np.asarray(closes[-(period + 1) :], dtype=float)
    deltas = np.diff(window)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = float(np.mean(gains))
    avg_loss = float(np.mean(losses))
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))
