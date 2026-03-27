"""
Additional exit conditions for held positions.

These are applied BEFORE the rank-based close_positions step —
any symbol returned by these functions is removed from target_symbols,
causing close_positions to sell it.
"""
import numpy as np
import talib as ta
from scipy.stats import linregress


def trailing_stop_exits(
    full_stock_df,
    held_symbols,
    *,
    atr_multiplier=3.0,
    atr_lookback=20,
    hwm_lookback=60,
):
    """
    Return the set of held symbols whose current price has dropped more than
    `atr_multiplier * ATR` below their high-water mark (highest close over
    the last `hwm_lookback` trading days).

    This is a one-way gate: once triggered, the stock must re-enter through
    the normal buy process (rank + viability filters).
    """
    if full_stock_df.empty or not held_symbols:
        return set()

    available = set(full_stock_df.index.get_level_values("symbol").unique())
    stopped_out = set()

    for sym in held_symbols:
        if sym not in available:
            continue

        bars = full_stock_df.loc[sym].sort_index()
        if len(bars) < max(atr_lookback + 1, hwm_lookback):
            continue

        high = bars["high"].to_numpy()
        low = bars["low"].to_numpy()
        close = bars["close"].to_numpy()

        atr = ta.ATR(high, low, close, timeperiod=atr_lookback)
        current_atr = atr[-1]
        if np.isnan(current_atr) or current_atr <= 0:
            continue

        hwm = np.max(close[-hwm_lookback:])
        current_price = close[-1]
        stop_level = hwm - atr_multiplier * current_atr

        if current_price < stop_level:
            stopped_out.add(sym)
            print(f"  TRAILING STOP: {sym} at {current_price:.2f}, "
                  f"HWM={hwm:.2f}, ATR={current_atr:.2f}, "
                  f"stop={stop_level:.2f}")

    return stopped_out


def short_momentum_exits(
    full_stock_df,
    held_symbols,
    *,
    short_lookback=60,
):
    """
    Return the set of held symbols whose short-term (e.g. 60-day) momentum
    has turned negative (annualized return from log-linear regression < 0).

    This detects structural trend reversals — a 60-day regression turning
    negative requires weeks of consistent decline, not just a bad day.
    """
    if full_stock_df.empty or not held_symbols:
        return set()

    available = set(full_stock_df.index.get_level_values("symbol").unique())
    degraded = set()

    for sym in held_symbols:
        if sym not in available:
            continue

        bars = full_stock_df.loc[sym].sort_index()
        if len(bars) < short_lookback:
            continue

        # Use the last `short_lookback` days
        recent = bars.tail(short_lookback)
        high = recent["high"].to_numpy()
        low = recent["low"].to_numpy()
        close = recent["close"].to_numpy()

        daily_average = (high + low + close) / 3
        if np.any(daily_average <= 0):
            continue

        log_avg = np.log(daily_average)
        x_days = np.arange(len(log_avg))

        slope, _, r_value, _, _ = linregress(x_days, log_avg)
        annualized = np.exp(slope * 250) - 1

        if annualized < 0:
            degraded.add(sym)
            print(f"  SHORT MOMENTUM EXIT: {sym}, "
                  f"{short_lookback}d annualized={annualized:.2%}, "
                  f"R²={r_value**2:.3f}")

    return degraded
