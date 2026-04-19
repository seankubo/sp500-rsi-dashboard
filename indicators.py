"""Technical indicator calculations."""

from __future__ import annotations

import pandas as pd


def calculate_rsi(close_prices: pd.Series, window: int = 14) -> pd.Series:
    """Calculate RSI using Wilder's smoothing."""
    if window <= 0:
        raise ValueError("window must be a positive integer")
    if close_prices.empty:
        raise ValueError("close_prices must not be empty")

    delta = close_prices.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
    avg_loss = loss.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()

    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    return rsi
