from dataclasses import dataclass, field
from pathlib import Path

from Strategies.Momentum import DEFAULT_HOLDINGS_PATH


DEFAULT_METADATA_PATH = Path("Data/stat_arb_symbol_metadata.csv")


@dataclass(frozen=True)
class UniverseConfig:
    holdings_path: Path = Path(DEFAULT_HOLDINGS_PATH)
    metadata_path: Path = DEFAULT_METADATA_PATH
    min_history_days: int = 252
    min_price: float = 5.0
    liquidity_lookback_days: int = 20
    min_average_dollar_volume: float = 25_000_000.0
    max_missing_closes: int = 0
    require_classification: bool = True
    require_shortable: bool = False
    classification_level: str = "industry"


@dataclass(frozen=True)
class PairSelectionConfig:
    formation_window_days: int = 252
    min_overlap_days: int = 200
    zscore_lookback_days: int = 60
    min_return_correlation: float = 0.75
    min_half_life_days: float = 2.0
    max_half_life_days: float = 15.0
    min_zero_crossings: int = 4
    max_symbols_per_group: int = 12
    max_selected_pairs: int = 5


@dataclass(frozen=True)
class SignalConfig:
    entry_zscore: float = 2.0
    exit_zscore: float = 0.5
    stop_zscore: float = 3.5
    max_holding_days: int = 10


@dataclass(frozen=True)
class BacktestConfig:
    initial_capital: float = 100000.0
    max_gross_leverage: float = 1.0
    max_open_pairs: int = 5
    max_pair_gross_fraction: float = 0.20
    reselection_frequency: str = "monthly"
    trade_fee_flat: float = 1.0
    trade_fee_rate: float = 0.0005
    slippage_bps: float = 5.0
    default_borrow_fee_bps: float = 30.0
    close_deselected_pairs: bool = True


@dataclass(frozen=True)
class StatArbConfig:
    universe: UniverseConfig = field(default_factory=UniverseConfig)
    pairs: PairSelectionConfig = field(default_factory=PairSelectionConfig)
    signals: SignalConfig = field(default_factory=SignalConfig)
    backtest: BacktestConfig = field(default_factory=BacktestConfig)

