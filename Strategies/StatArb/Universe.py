from dataclasses import dataclass
from pathlib import Path

import pandas as pd


DEFAULT_METADATA_COLUMNS = [
    "symbol",
    "sector",
    "industry",
    "shortable",
    "borrow_fee_bps",
    "enabled",
]


@dataclass(frozen=True)
class PreparedUniverse:
    eligible: pd.DataFrame
    rejected: pd.DataFrame
    metadata: pd.DataFrame


def _clean_text(value):
    if value is None:
        return ""
    normalized = str(value).strip()
    if normalized == "-":
        return ""
    return normalized


def _coerce_bool(value, *, default):
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except TypeError:
        pass

    if isinstance(value, float) and pd.isna(value):
        return default

    normalized = str(value).strip().lower()
    if normalized in {"", "<na>", "nan", "none"}:
        return default
    if not normalized:
        return default
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Cannot coerce {value!r} to bool.")


def load_symbol_metadata(metadata_path):
    metadata_path = Path(metadata_path)
    if not metadata_path.exists():
        return pd.DataFrame(columns=DEFAULT_METADATA_COLUMNS)

    try:
        metadata = pd.read_csv(metadata_path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=DEFAULT_METADATA_COLUMNS)

    metadata.columns = [str(column).strip().lower() for column in metadata.columns]
    for column in DEFAULT_METADATA_COLUMNS:
        if column not in metadata.columns:
            metadata[column] = pd.NA

    metadata = metadata.loc[:, DEFAULT_METADATA_COLUMNS].copy()
    metadata["symbol"] = metadata["symbol"].astype(str).str.strip().str.upper()
    metadata = metadata[metadata["symbol"] != ""].drop_duplicates(subset=["symbol"], keep="last")
    metadata["sector"] = metadata["sector"].map(_clean_text)
    metadata["industry"] = metadata["industry"].map(_clean_text)
    metadata["shortable"] = metadata["shortable"].map(lambda value: _coerce_bool(value, default=True))
    metadata["enabled"] = metadata["enabled"].map(lambda value: _coerce_bool(value, default=True))
    metadata["borrow_fee_bps"] = pd.to_numeric(metadata["borrow_fee_bps"], errors="coerce").fillna(0.0)
    return metadata.reset_index(drop=True)


def load_seed_universe(holdings_path):
    holdings = pd.read_csv(holdings_path, skiprows=4)
    holdings.columns = [str(column).strip() for column in holdings.columns]

    seed_universe = holdings.rename(
        columns={
            "Ticker": "symbol",
            "Name": "name",
            "Sector": "holdings_sector",
        }
    )

    seed_universe = seed_universe.loc[:, ["symbol", "name", "holdings_sector"]].copy()
    seed_universe["symbol"] = seed_universe["symbol"].astype(str).str.strip().str.upper()
    seed_universe["name"] = seed_universe["name"].astype(str).str.strip()
    seed_universe["holdings_sector"] = seed_universe["holdings_sector"].map(_clean_text)
    seed_universe = seed_universe[seed_universe["symbol"] != ""]
    return seed_universe.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)


def requested_symbols_for_backtest(universe_config):
    seed_universe = load_seed_universe(universe_config.holdings_path)
    metadata = load_symbol_metadata(universe_config.metadata_path)

    if metadata.empty:
        return seed_universe["symbol"].tolist()

    enabled_symbols = set(metadata.loc[metadata["enabled"], "symbol"])
    selected_symbols = seed_universe.loc[
        seed_universe["symbol"].isin(enabled_symbols),
        "symbol",
    ].tolist()
    return selected_symbols or seed_universe["symbol"].tolist()


def prepare_stat_arb_universe(bars_df, universe_config):
    seed_universe = load_seed_universe(universe_config.holdings_path)
    metadata = load_symbol_metadata(universe_config.metadata_path)
    joined = seed_universe.merge(metadata, on="symbol", how="left")

    joined["sector"] = joined["sector"].map(_clean_text)
    joined["industry"] = joined["industry"].map(_clean_text)
    joined["shortable"] = joined["shortable"].map(lambda value: _coerce_bool(value, default=True))
    joined["enabled"] = joined["enabled"].map(lambda value: _coerce_bool(value, default=True))
    joined["borrow_fee_bps"] = pd.to_numeric(joined["borrow_fee_bps"], errors="coerce").fillna(0.0)

    joined["sector"] = joined["sector"].mask(joined["sector"] == "", joined["holdings_sector"])
    joined["classification_bucket"] = joined["industry"]
    if universe_config.classification_level.strip().lower() == "sector":
        joined["classification_bucket"] = joined["sector"]
    else:
        joined["classification_bucket"] = joined["classification_bucket"].mask(
            joined["classification_bucket"] == "",
            joined["sector"],
        )

    records = []
    rejections = []
    available_symbols = set(bars_df.index.get_level_values("symbol")) if not bars_df.empty else set()

    for row in joined.itertuples(index=False):
        if not row.enabled:
            rejections.append({"symbol": row.symbol, "reason": "metadata_disabled"})
            continue

        if row.symbol not in available_symbols:
            rejections.append({"symbol": row.symbol, "reason": "missing_bars"})
            continue

        frame = bars_df.loc[row.symbol].sort_index()
        if len(frame) < int(universe_config.min_history_days):
            rejections.append({"symbol": row.symbol, "reason": "short_history"})
            continue

        latest_close = float(frame["close"].iloc[-1])
        if latest_close < float(universe_config.min_price):
            rejections.append({"symbol": row.symbol, "reason": "price_floor"})
            continue

        close_tail = frame["close"].tail(int(universe_config.min_history_days))
        if int(close_tail.isna().sum()) > int(universe_config.max_missing_closes):
            rejections.append({"symbol": row.symbol, "reason": "missing_closes"})
            continue

        dollar_volume = (
            frame["close"].tail(int(universe_config.liquidity_lookback_days))
            * frame["volume"].tail(int(universe_config.liquidity_lookback_days))
        )
        average_dollar_volume = float(dollar_volume.mean())
        if average_dollar_volume < float(universe_config.min_average_dollar_volume):
            rejections.append({"symbol": row.symbol, "reason": "liquidity_floor"})
            continue

        if universe_config.require_classification and not str(row.classification_bucket).strip():
            rejections.append({"symbol": row.symbol, "reason": "missing_classification"})
            continue

        if universe_config.require_shortable and not row.shortable:
            rejections.append({"symbol": row.symbol, "reason": "not_shortable"})
            continue

        records.append(
            {
                "symbol": row.symbol,
                "name": row.name,
                "sector": row.sector,
                "industry": row.industry,
                "classification_bucket": row.classification_bucket,
                "shortable": bool(row.shortable),
                "borrow_fee_bps": float(row.borrow_fee_bps),
                "latest_close": latest_close,
                "average_dollar_volume": average_dollar_volume,
                "history_days": int(len(frame)),
            }
        )

    eligible = pd.DataFrame(records)
    if not eligible.empty:
        eligible = eligible.sort_values(
            ["classification_bucket", "average_dollar_volume", "symbol"],
            ascending=[True, False, True],
        ).reset_index(drop=True)

    rejected = pd.DataFrame(rejections)
    if not rejected.empty:
        rejected = rejected.sort_values(["reason", "symbol"]).reset_index(drop=True)

    return PreparedUniverse(
        eligible=eligible,
        rejected=rejected,
        metadata=metadata,
    )
