"""
Run multiple backtest configurations for exit condition experiments.
Saves results to exit_conditions_data/ for comparison.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pathlib import Path
from Backtesting import run_backtest

CONFIGS = {
    "trailing_stop_3x": {
        "trailing_stop_atr_multiplier": 3.0,
        "trailing_stop_hwm_lookback": 60,
        "short_momentum_lookback": None,
    },
    "short_momentum_60d": {
        "trailing_stop_atr_multiplier": None,
        "short_momentum_lookback": 60,
    },
    "combined_3x_60d": {
        "trailing_stop_atr_multiplier": 3.0,
        "trailing_stop_hwm_lookback": 60,
        "short_momentum_lookback": 60,
    },
    "trailing_stop_5x": {
        "trailing_stop_atr_multiplier": 5.0,
        "trailing_stop_hwm_lookback": 60,
        "short_momentum_lookback": None,
    },
    "trailing_stop_4x": {
        "trailing_stop_atr_multiplier": 4.0,
        "trailing_stop_hwm_lookback": 60,
        "short_momentum_lookback": None,
    },
    "short_momentum_40d": {
        "trailing_stop_atr_multiplier": None,
        "short_momentum_lookback": 40,
    },
    "combined_5x_60d": {
        "trailing_stop_atr_multiplier": 5.0,
        "trailing_stop_hwm_lookback": 60,
        "short_momentum_lookback": 60,
    },
}

# Pick config from CLI arg
if len(sys.argv) < 2 or sys.argv[1] not in CONFIGS:
    print(f"Usage: python {sys.argv[0]} <{'|'.join(CONFIGS.keys())}>")
    sys.exit(1)

config_name = sys.argv[1]
config = CONFIGS[config_name]

out_dir = Path("exit_conditions_data")
out_dir.mkdir(exist_ok=True)

print(f"\n{'='*60}")
print(f"  Running backtest: {config_name}")
print(f"  Config: {config}")
print(f"{'='*60}\n")

results_df = run_backtest(
    "2017-02-01",
    "2026-02-01",
    initial_cash=100000,
    benchmark_symbol="SPTM",
    results_path=out_dir / f"backtest_{config_name}.csv",
    chart_path=out_dir / f"backtest_{config_name}.png",
    cache_path=None,
    batch_size=400,
    warmup_days=260,
    run_on_schedule_only=True,
    strategy_weekday=2,
    raw_rank_consideration_limit=100,
    max_position_fraction=0.10,
    defensive_mode="treasury_bonds",
    defensive_symbol="IEI",
    trade_fee_flat=1.00,
    trade_fee_rate=0.0005,
    export_site_data=False,
    **config,
)

final = results_df["portfolio_value"].iloc[-1]
start = results_df["portfolio_value"].iloc[0]
ret = (final / start - 1) * 100
dd = ((results_df["portfolio_value"] / results_df["portfolio_value"].cummax()) - 1).min() * 100
trades = results_df["trade_count"].iloc[-1]
fees = results_df["fees_paid_cumulative"].iloc[-1]

print(f"\n{'='*60}")
print(f"  Results: {config_name}")
print(f"{'='*60}")
print(f"  Return:       {ret:.1f}%")
print(f"  Max Drawdown: {dd:.1f}%")
print(f"  Trades:       {trades:.0f}")
print(f"  Fees:         ${fees:,.0f}")
print(f"  Final Value:  ${final:,.0f}")
