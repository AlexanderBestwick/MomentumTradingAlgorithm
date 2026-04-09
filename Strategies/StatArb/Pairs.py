from dataclasses import dataclass
from itertools import combinations

import numpy as np
import pandas as pd

from Strategies.StatArb.Signals import calculate_pair_signal


@dataclass(frozen=True)
class PairSelectionResult:
    selected_pairs: pd.DataFrame
    candidate_pairs: pd.DataFrame


def _pair_id(symbol_a, symbol_b):
    return f"{symbol_a}__{symbol_b}"


def estimate_hedge_ratio(price_a, price_b):
    aligned = pd.concat(
        [
            pd.Series(price_a, copy=False, name="price_a"),
            pd.Series(price_b, copy=False, name="price_b"),
        ],
        axis=1,
        join="inner",
    ).dropna()
    if len(aligned) < 2:
        return None

    log_a = np.log(aligned["price_a"].astype(float))
    log_b = np.log(aligned["price_b"].astype(float))
    variance_b = float(log_b.var())
    if variance_b <= 0:
        return None

    covariance = float(np.cov(log_a, log_b, ddof=1)[0, 1])
    hedge_ratio = covariance / variance_b
    if not np.isfinite(hedge_ratio) or hedge_ratio <= 0:
        return None
    return float(hedge_ratio)


def estimate_half_life(spread):
    spread = pd.Series(spread, copy=False).dropna()
    if len(spread) < 3:
        return np.inf

    lagged = spread.shift(1).dropna()
    delta = spread.diff().dropna()
    aligned = pd.concat([lagged.rename("lagged"), delta.rename("delta")], axis=1).dropna()
    if len(aligned) < 3:
        return np.inf

    x = aligned["lagged"].to_numpy(dtype="float64")
    y = aligned["delta"].to_numpy(dtype="float64")
    x_mean = float(x.mean())
    y_mean = float(y.mean())
    denominator = float(((x - x_mean) ** 2).sum())
    if denominator <= 0:
        return np.inf

    beta = float(((x - x_mean) * (y - y_mean)).sum() / denominator)
    if beta >= 0:
        return np.inf

    return float(-np.log(2.0) / beta)


def _zero_crossings(series):
    centered = pd.Series(series, copy=False).dropna()
    if centered.empty:
        return 0

    centered = centered - centered.mean()
    signs = np.sign(centered.to_numpy(dtype="float64"))
    signs = signs[signs != 0]
    if len(signs) < 2:
        return 0
    return int((signs[1:] != signs[:-1]).sum())


def _selection_score(return_correlation, half_life, zero_crossings, formation_window_days):
    zero_crossing_bonus = min(0.20, zero_crossings / max(1, formation_window_days))
    half_life_penalty = min(0.25, half_life / 100.0)
    return float(return_correlation + zero_crossing_bonus - half_life_penalty)


def select_candidate_pairs(bars_df, eligible_universe, pair_config):
    if eligible_universe is None or eligible_universe.empty:
        empty = pd.DataFrame()
        return PairSelectionResult(selected_pairs=empty, candidate_pairs=empty)

    candidate_records = []
    formation_window_days = int(pair_config.formation_window_days)
    min_overlap_days = int(pair_config.min_overlap_days)

    for group_value, group_frame in eligible_universe.groupby("classification_bucket"):
        ranked_group = group_frame.sort_values(
            ["average_dollar_volume", "symbol"],
            ascending=[False, True],
        ).head(int(pair_config.max_symbols_per_group))
        symbols = ranked_group["symbol"].tolist()
        if len(symbols) < 2:
            continue

        for symbol_a, symbol_b in combinations(symbols, 2):
            closes_a = bars_df.loc[symbol_a].sort_index()["close"].tail(formation_window_days)
            closes_b = bars_df.loc[symbol_b].sort_index()["close"].tail(formation_window_days)
            aligned = pd.concat(
                [closes_a.rename("close_a"), closes_b.rename("close_b")],
                axis=1,
                join="inner",
            ).dropna()
            if len(aligned) < min_overlap_days:
                continue

            returns = np.log(aligned).diff().dropna()
            if returns.empty:
                continue

            return_correlation = float(returns["close_a"].corr(returns["close_b"]))
            if not np.isfinite(return_correlation) or return_correlation < float(pair_config.min_return_correlation):
                continue

            hedge_ratio = estimate_hedge_ratio(aligned["close_a"], aligned["close_b"])
            if hedge_ratio is None:
                continue

            signal = calculate_pair_signal(
                aligned["close_a"],
                aligned["close_b"],
                hedge_ratio=hedge_ratio,
                lookback_days=pair_config.zscore_lookback_days,
            )
            if signal is None:
                continue

            spread = np.log(aligned["close_a"]) - (hedge_ratio * np.log(aligned["close_b"]))
            half_life = estimate_half_life(spread)
            if not np.isfinite(half_life):
                continue
            if half_life < float(pair_config.min_half_life_days) or half_life > float(pair_config.max_half_life_days):
                continue

            zero_crossings = _zero_crossings(spread)
            if zero_crossings < int(pair_config.min_zero_crossings):
                continue

            candidate_records.append(
                {
                    "pair_id": _pair_id(symbol_a, symbol_b),
                    "symbol_a": symbol_a,
                    "symbol_b": symbol_b,
                    "classification_bucket": group_value,
                    "sector": ranked_group.loc[ranked_group["symbol"] == symbol_a, "sector"].iloc[0],
                    "industry": ranked_group.loc[ranked_group["symbol"] == symbol_a, "industry"].iloc[0],
                    "hedge_ratio": float(hedge_ratio),
                    "return_correlation": float(return_correlation),
                    "half_life_days": float(half_life),
                    "zero_crossings": int(zero_crossings),
                    "latest_zscore": float(signal.zscore),
                    "selection_score": _selection_score(
                        return_correlation,
                        half_life,
                        zero_crossings,
                        formation_window_days,
                    ),
                }
            )

    candidate_pairs = pd.DataFrame(candidate_records)
    if candidate_pairs.empty:
        empty = pd.DataFrame()
        return PairSelectionResult(selected_pairs=empty, candidate_pairs=empty)

    candidate_pairs = candidate_pairs.sort_values(
        ["selection_score", "return_correlation", "half_life_days", "pair_id"],
        ascending=[False, False, True, True],
    ).reset_index(drop=True)

    selected_records = []
    used_symbols = set()
    for row in candidate_pairs.itertuples(index=False):
        if row.symbol_a in used_symbols or row.symbol_b in used_symbols:
            continue
        selected_records.append(row._asdict())
        used_symbols.update({row.symbol_a, row.symbol_b})
        if len(selected_records) >= int(pair_config.max_selected_pairs):
            break

    return PairSelectionResult(
        selected_pairs=pd.DataFrame(selected_records),
        candidate_pairs=candidate_pairs,
    )

