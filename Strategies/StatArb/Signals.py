from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class PairSignal:
    signal_date: object
    zscore: float
    spread: float
    rolling_mean: float
    rolling_std: float


def build_spread_series(price_a, price_b, hedge_ratio):
    aligned = pd.concat(
        [
            pd.Series(price_a, copy=False, name="price_a"),
            pd.Series(price_b, copy=False, name="price_b"),
        ],
        axis=1,
        join="inner",
    ).dropna()
    if aligned.empty:
        return pd.Series(dtype="float64")

    log_a = np.log(aligned["price_a"].astype(float))
    log_b = np.log(aligned["price_b"].astype(float))
    spread = log_a - (float(hedge_ratio) * log_b)
    spread.name = "spread"
    return spread


def compute_rolling_zscore(spread, lookback_days):
    spread = pd.Series(spread, copy=False).dropna()
    if spread.empty:
        return pd.Series(dtype="float64")

    rolling_mean = spread.rolling(int(lookback_days)).mean()
    rolling_std = spread.rolling(int(lookback_days)).std(ddof=0)
    zscore = (spread - rolling_mean) / rolling_std.replace(0.0, np.nan)
    return zscore.dropna()


def calculate_pair_signal(price_a, price_b, *, hedge_ratio, lookback_days):
    spread = build_spread_series(price_a, price_b, hedge_ratio)
    if len(spread) < int(lookback_days):
        return None

    rolling_mean = spread.rolling(int(lookback_days)).mean()
    rolling_std = spread.rolling(int(lookback_days)).std(ddof=0)
    latest_mean = rolling_mean.iloc[-1]
    latest_std = rolling_std.iloc[-1]
    if pd.isna(latest_mean) or pd.isna(latest_std) or latest_std <= 0:
        return None

    latest_spread = float(spread.iloc[-1])
    zscore = float((latest_spread - latest_mean) / latest_std)
    return PairSignal(
        signal_date=spread.index[-1],
        zscore=zscore,
        spread=latest_spread,
        rolling_mean=float(latest_mean),
        rolling_std=float(latest_std),
    )

