import argparse
import datetime as dt
from pathlib import Path

from Backtesting import load_or_fetch_historical_bars
from Config import load_local_env
from Strategies.Momentum import calendar_days_for_trading_window
from Strategies.StatArb import (
    BacktestConfig,
    PairSelectionConfig,
    SignalConfig,
    StatArbConfig,
    UniverseConfig,
    requested_symbols_for_backtest,
    run_stat_arb_backtest,
)


load_local_env()


RUN_WITH_EDITOR_SETTINGS = True
EDITOR_START_DATE = "2020-01-01"
EDITOR_END_DATE = "2026-02-01"
EDITOR_INITIAL_CAPITAL = 100000.0
EDITOR_METADATA_PATH = Path("Data/stat_arb_symbol_metadata.csv")
EDITOR_RESULTS_DIR = Path("Data/StatArb")
EDITOR_CACHE_PATH = Path("Data/backtest_cache_20160104_20260201.pkl")
EDITOR_REQUIRE_CLASSIFICATION = True
EDITOR_REQUIRE_SHORTABLE = False


def _parse_args():
    parser = argparse.ArgumentParser(description="Run the stat-arb daily pairs backtest.")
    parser.add_argument("--start-date", required=True, help="Backtest start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", required=True, help="Backtest end date in YYYY-MM-DD format.")
    parser.add_argument("--initial-capital", type=float, default=EDITOR_INITIAL_CAPITAL)
    parser.add_argument("--metadata-path", type=Path, default=EDITOR_METADATA_PATH)
    parser.add_argument("--results-dir", type=Path, default=EDITOR_RESULTS_DIR)
    parser.add_argument("--cache-path", type=Path, default=EDITOR_CACHE_PATH)
    parser.add_argument(
        "--allow-missing-classification",
        action="store_true",
        help="Allow the backtest to use symbols without sector/industry metadata.",
    )
    parser.add_argument(
        "--require-shortable",
        action="store_true",
        help="Require symbols to be marked shortable in the local metadata file.",
    )
    return parser.parse_args()


def _build_config(
    *,
    metadata_path,
    initial_capital,
    require_classification,
    require_shortable,
):
    return StatArbConfig(
        universe=UniverseConfig(
            metadata_path=Path(metadata_path),
            require_classification=bool(require_classification),
            require_shortable=bool(require_shortable),
        ),
        pairs=PairSelectionConfig(),
        signals=SignalConfig(),
        backtest=BacktestConfig(initial_capital=float(initial_capital)),
    )


def _print_summary(summary):
    print()
    print("Stat-arb backtest summary")
    print(f"Initial capital: {summary['initial_capital']:.2f}")
    print(f"Final equity: {summary['final_equity']:.2f}")
    print(f"Total return: {summary['total_return_pct']:.2f}%")
    print(f"Max drawdown: {summary['max_drawdown_pct']:.2f}%")
    print(f"Trading days: {summary['trading_days']}")
    print(f"Trade count: {summary['trade_count']}")
    print(f"Pair reselections: {summary['pair_reselection_count']}")
    print(f"Average gross exposure: {summary['average_gross_exposure_pct']:.2f}%")
    print(f"Average net exposure: {summary['average_net_exposure_pct']:.2f}%")


def _save_result_tables(result, results_dir):
    results_dir.mkdir(parents=True, exist_ok=True)

    results_path = results_dir / "equity_curve.csv"
    trades_path = results_dir / "trades.csv"
    selected_pairs_path = results_dir / "selected_pairs.csv"
    candidate_pairs_path = results_dir / "candidate_pairs.csv"
    universe_path = results_dir / "eligible_universe.csv"
    rejections_path = results_dir / "rejected_universe.csv"

    result.results.to_csv(results_path, index=False)
    result.trades.to_csv(trades_path, index=False)
    result.selected_pairs.to_csv(selected_pairs_path, index=False)
    result.candidate_pairs.to_csv(candidate_pairs_path, index=False)
    result.universe.to_csv(universe_path, index=False)
    result.rejections.to_csv(rejections_path, index=False)

    print()
    print(f"Saved equity curve to {results_path}")
    print(f"Saved trades to {trades_path}")
    print(f"Saved selected pairs to {selected_pairs_path}")
    print(f"Saved candidate pairs to {candidate_pairs_path}")
    print(f"Saved eligible universe to {universe_path}")
    print(f"Saved universe rejections to {rejections_path}")


def run_backtest(
    *,
    start_date,
    end_date,
    metadata_path,
    initial_capital,
    results_dir,
    cache_path,
    require_classification,
    require_shortable,
):
    config = _build_config(
        metadata_path=metadata_path,
        initial_capital=initial_capital,
        require_classification=require_classification,
        require_shortable=require_shortable,
    )

    symbols = requested_symbols_for_backtest(config.universe)
    history_days = max(
        int(config.universe.min_history_days),
        int(config.pairs.formation_window_days),
    )
    history_days += int(config.pairs.zscore_lookback_days) + 10
    fetch_start_date = start_date - dt.timedelta(days=calendar_days_for_trading_window(history_days))

    print(f"Requesting historical bars for {len(symbols)} symbols")
    bars_df = load_or_fetch_historical_bars(
        symbols,
        fetch_start_date,
        end_date,
        cache_path=cache_path,
    )

    result = run_stat_arb_backtest(
        bars_df,
        config,
        start_date=start_date,
        end_date=end_date,
    )
    _print_summary(result.summary)
    _save_result_tables(result, results_dir)
    return result


def main():
    if RUN_WITH_EDITOR_SETTINGS:
        start_date = dt.date.fromisoformat(EDITOR_START_DATE)
        end_date = dt.date.fromisoformat(EDITOR_END_DATE)
        run_backtest(
            start_date=start_date,
            end_date=end_date,
            metadata_path=EDITOR_METADATA_PATH,
            initial_capital=EDITOR_INITIAL_CAPITAL,
            results_dir=EDITOR_RESULTS_DIR,
            cache_path=EDITOR_CACHE_PATH,
            require_classification=EDITOR_REQUIRE_CLASSIFICATION,
            require_shortable=EDITOR_REQUIRE_SHORTABLE,
        )
        return

    args = _parse_args()
    start_date = dt.date.fromisoformat(args.start_date)
    end_date = dt.date.fromisoformat(args.end_date)
    run_backtest(
        start_date=start_date,
        end_date=end_date,
        metadata_path=args.metadata_path,
        initial_capital=args.initial_capital,
        results_dir=args.results_dir,
        cache_path=args.cache_path,
        require_classification=not args.allow_missing_classification,
        require_shortable=args.require_shortable,
    )


if __name__ == "__main__":
    main()
