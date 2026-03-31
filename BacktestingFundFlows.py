import argparse
from collections import defaultdict
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Callable

import matplotlib.pyplot as plt
import pandas as pd

from App.LiveRebalance import RunAll
from Backtesting import (
    SimulatedDataClient,
    SimulatedTradingClient,
    _coerce_date,
    _format_elapsed_time,
    _should_run_strategy,
    load_or_fetch_historical_bars,
)
from Funds.Accounting import (
    DEFAULT_INITIAL_UNIT_PRICE,
    calculate_member_value,
    calculate_unit_price,
)
from Funds.Models import CashFlowRequest
from Strategies.Momentum.Logic.TradingDays import calendar_days_for_trading_window
from Strategies.Momentum.Logic.UniverseSelection import load_snp1500_symbols


DEFAULT_RESULTS_PATH = Path("Data/BacktestFundFlowsResults.csv")
DEFAULT_INVESTOR_RESULTS_PATH = Path("Data/BacktestFundFlowsInvestorResults.csv")
DEFAULT_CHART_PATH = Path("Data/BacktestFundFlowsResults.png")
BENCHMARK_ROLLING_WINDOW = 200

# Demo parameters for running this file directly from your IDE.
RUN_WITH_EDITOR_SETTINGS = True
EDITOR_START_DATE = "2019-10-01"
EDITOR_END_DATE = "2020-08-01"
EDITOR_INITIAL_CONTRIBUTION = 1000.0
EDITOR_MONTHLY_CONTRIBUTION = 500.0
EDITOR_WITHDRAWAL_AFTER_MONTHS = 6
EDITOR_WITHDRAWAL_AMOUNT = 2000.0
EDITOR_INITIAL_UNIT_PRICE = DEFAULT_INITIAL_UNIT_PRICE
EDITOR_REFERENCE_INITIAL_CASH = 100000.0
EDITOR_BENCHMARK_SYMBOL = "SPTM"
EDITOR_RESULTS_PATH = DEFAULT_RESULTS_PATH
EDITOR_INVESTOR_RESULTS_PATH = DEFAULT_INVESTOR_RESULTS_PATH
EDITOR_CHART_PATH = DEFAULT_CHART_PATH
EDITOR_CACHE_PATH = Path("Data/backtest_cache_20160517_20260201(Long).PKL")
EDITOR_BATCH_SIZE = 400
EDITOR_WARMUP_DAYS = 260
EDITOR_RUN_ON_SCHEDULE_ONLY = True
EDITOR_STRATEGY_WEEKDAY = 2  # Monday=0, Tuesday=1, Wednesday=2
EDITOR_RAW_RANK_CONSIDERATION_LIMIT = 100
EDITOR_MAX_POSITION_FRACTION = 0.10
EDITOR_DEFENSIVE_MODE = "cash"  # "cash" or "treasury_bonds"
EDITOR_DEFENSIVE_SYMBOL = "IEI"
EDITOR_TRADE_FEE_FLAT = 1.00
EDITOR_TRADE_FEE_RATE = 0.0005
EDITOR_FUND_ID = "demo_fund"
EDITOR_INVESTOR_ID = "demo_investor"


@dataclass(frozen=True)
class DailyCashFlowSummary:
    requested_contribution_amount: float = 0.0
    requested_withdrawal_amount: float = 0.0
    contribution_cash_added: float = 0.0
    withdrawal_cash_paid: float = 0.0
    withdrawal_cash_requested_remaining: float = 0.0
    contribution_cash_pending_outside_fund: float = 0.0
    units_purchased: float = 0.0
    units_redeemed: float = 0.0
    unit_price: float = 0.0
    requests: tuple[CashFlowRequest, ...] = ()


@dataclass(frozen=True)
class ReferenceDay:
    date: dt.date
    unit_price: float
    reference_portfolio_value: float
    strategy_ran: bool
    benchmark_close: float
    benchmark_rolling_average: float
    market_health: str | None = None


@dataclass(frozen=True)
class InvestorPlan:
    investor_id: str
    display_name: str
    initial_contribution: float = 0.0
    monthly_contribution: float = 0.0
    withdrawal_after_months: int | None = None
    withdrawal_amount: float = 0.0
    schedule_builder: Callable | None = None


@dataclass
class InvestorState:
    investor_id: str
    display_name: str
    units: float = 0.0
    pending_contribution_cash: float = 0.0
    pending_withdrawal_cash_requested: float = 0.0
    cumulative_requested_contributions: float = 0.0
    cumulative_requested_withdrawals: float = 0.0
    cumulative_effective_contributions: float = 0.0
    cumulative_effective_withdrawals: float = 0.0


def _build_editor_investor_plans():
    return [
        InvestorPlan(
            investor_id="investor_a",
            display_name="Investor A",
            initial_contribution=1000.0,
            monthly_contribution=500.0,
            withdrawal_after_months=6,
            withdrawal_amount=2000.0,
        ),
        InvestorPlan(
            investor_id="investor_b",
            display_name="Investor B",
            initial_contribution=2500.0,
            monthly_contribution=250.0,
        ),
        InvestorPlan(
            investor_id="investor_c",
            display_name="Investor C",
            initial_contribution=1500.0,
            schedule_builder=build_quarterly_dca_schedule,
        ),
    ]


def _display_unit_price(net_asset_value, total_units, initial_unit_price):
    if float(total_units) <= 0 and float(net_asset_value) <= 1e-9:
        return 0.0
    return calculate_unit_price(
        net_asset_value,
        total_units,
        initial_unit_price=initial_unit_price,
    )


def _load_cached_bars_if_available(symbols, cache_path):
    if not cache_path:
        return None

    cache_path = Path(cache_path)
    if not cache_path.exists():
        return None

    print(f"Loading historical bars from {cache_path}")
    bars_df = pd.read_pickle(cache_path)
    requested_symbols = set(symbols)
    available_symbols = set(bars_df.index.get_level_values("symbol"))
    missing_symbols = requested_symbols - available_symbols

    if missing_symbols:
        print(
            f"Cache is missing {len(missing_symbols)} requested symbols. "
            "Using the available cached subset for this backtest run."
        )

    kept_symbols = sorted(requested_symbols & available_symbols)
    if not kept_symbols:
        raise ValueError(f"Cache at {cache_path} did not contain any requested symbols.")

    return bars_df.loc[bars_df.index.get_level_values("symbol").isin(kept_symbols)].sort_index()


def _first_trading_day_by_month(trading_index):
    first_days = {}
    for timestamp in trading_index:
        month_key = (timestamp.year, timestamp.month)
        first_days.setdefault(month_key, timestamp.date())
    return first_days


def _build_dealing_dates(trading_index, *, run_on_schedule_only, strategy_weekday):
    dealing_dates = []
    for timestamp in trading_index:
        run_date = timestamp.date()
        if _should_run_strategy(
            run_date,
            run_on_schedule_only=run_on_schedule_only,
            strategy_weekday=strategy_weekday,
        ):
            dealing_dates.append(run_date)
    return dealing_dates


def _next_dealing_date(request_date, dealing_dates):
    return next((dealing_date for dealing_date in dealing_dates if dealing_date >= request_date), None)


def _add_cash_flow_request(schedule, request):
    schedule.setdefault(request.effective_at.date(), []).append(request)


def build_standard_investor_schedule(
    trading_index,
    *,
    dealing_dates,
    fund_id,
    investor_plan,
):
    schedule = {}
    if len(trading_index) == 0:
        return schedule

    initial_date = trading_index[0].date()
    month_start_dates = _first_trading_day_by_month(trading_index)
    request_sequence = 0

    investor_id = investor_plan.investor_id
    initial_contribution = float(investor_plan.initial_contribution)
    monthly_contribution = float(investor_plan.monthly_contribution)
    withdrawal_after_months = investor_plan.withdrawal_after_months
    withdrawal_amount = float(investor_plan.withdrawal_amount)

    def next_request_id():
        nonlocal request_sequence
        request_sequence += 1
        return f"{fund_id}:{investor_id}:{request_sequence:04d}"

    if float(initial_contribution) > 0:
        request_date = initial_date
        effective_date = _next_dealing_date(request_date, dealing_dates)
        if effective_date is None:
            return schedule
        requested_at = dt.datetime.combine(request_date, dt.time(9, 30))
        effective_at = dt.datetime.combine(effective_date, dt.time(9, 30))
        _add_cash_flow_request(
            schedule,
            CashFlowRequest(
                request_id=next_request_id(),
                fund_id=fund_id,
                investor_id=investor_id,
                flow_type="contribution",
                amount=float(initial_contribution),
                requested_at=requested_at,
                effective_at=effective_at,
                note="Initial contribution",
            ),
        )

    for month_key, effective_date in sorted(month_start_dates.items()):
        if effective_date == initial_date:
            continue
        if float(monthly_contribution) <= 0:
            continue

        request_date = effective_date
        dealing_date = _next_dealing_date(request_date, dealing_dates)
        if dealing_date is None:
            continue
        requested_at = dt.datetime.combine(request_date, dt.time(9, 30))
        effective_at = dt.datetime.combine(dealing_date, dt.time(9, 30))
        _add_cash_flow_request(
            schedule,
            CashFlowRequest(
                request_id=next_request_id(),
                fund_id=fund_id,
                investor_id=investor_id,
                flow_type="contribution",
                amount=float(monthly_contribution),
                requested_at=requested_at,
                effective_at=effective_at,
                note="Scheduled monthly contribution",
            ),
        )

    if withdrawal_after_months is not None and float(withdrawal_amount) > 0 and int(withdrawal_after_months) >= 0:
        withdrawal_target = (
            pd.Timestamp(initial_date) + pd.DateOffset(months=int(withdrawal_after_months))
        ).date()
        withdrawal_date = next(
            (timestamp.date() for timestamp in trading_index if timestamp.date() >= withdrawal_target),
            None,
        )

        if withdrawal_date is not None:
            dealing_date = _next_dealing_date(withdrawal_date, dealing_dates)
            if dealing_date is None:
                return schedule
            requested_at = dt.datetime.combine(withdrawal_date, dt.time(9, 30))
            effective_at = dt.datetime.combine(dealing_date, dt.time(9, 30))
            _add_cash_flow_request(
                schedule,
                CashFlowRequest(
                    request_id=next_request_id(),
                    fund_id=fund_id,
                    investor_id=investor_id,
                    flow_type="withdrawal",
                    amount=float(withdrawal_amount),
                    requested_at=requested_at,
                    effective_at=effective_at,
                    note=f"Scheduled withdrawal after {withdrawal_after_months} months",
                ),
            )

    for effective_date, requests in schedule.items():
        schedule[effective_date] = sorted(
            requests,
            key=lambda request: (request.flow_type != "contribution", request.request_id),
        )

    return schedule


def build_quarterly_dca_schedule(
    trading_index,
    *,
    dealing_dates,
    fund_id,
    investor_plan,
):
    schedule = {}
    if len(trading_index) == 0:
        return schedule

    month_start_dates = _first_trading_day_by_month(trading_index)
    request_sequence = 0

    def next_request_id():
        nonlocal request_sequence
        request_sequence += 1
        return f"{fund_id}:{investor_plan.investor_id}:quarterly:{request_sequence:04d}"

    monthly_dates = sorted(month_start_dates.items())
    for month_index, (_, request_date) in enumerate(monthly_dates):
        if month_index % 3 != 0:
            continue

        amount = investor_plan.initial_contribution if month_index == 0 else max(
            0.0,
            investor_plan.monthly_contribution or 750.0,
        )
        if amount <= 0:
            continue

        dealing_date = _next_dealing_date(request_date, dealing_dates)
        if dealing_date is None:
            continue

        requested_at = dt.datetime.combine(request_date, dt.time(9, 30))
        effective_at = dt.datetime.combine(dealing_date, dt.time(9, 30))
        _add_cash_flow_request(
            schedule,
            CashFlowRequest(
                request_id=next_request_id(),
                fund_id=fund_id,
                investor_id=investor_plan.investor_id,
                flow_type="contribution",
                amount=float(amount),
                requested_at=requested_at,
                effective_at=effective_at,
                note="Quarterly DCA contribution",
            ),
        )

    return schedule


def build_multi_investor_cash_flow_schedule(
    trading_index,
    *,
    dealing_dates,
    fund_id,
    investor_plans,
):
    combined_schedule = defaultdict(list)

    for investor_plan in investor_plans:
        schedule_builder = investor_plan.schedule_builder or build_standard_investor_schedule
        investor_schedule = schedule_builder(
            trading_index,
            dealing_dates=dealing_dates,
            fund_id=fund_id,
            investor_plan=investor_plan,
        )
        for effective_date, requests in investor_schedule.items():
            combined_schedule[effective_date].extend(requests)

    normalized = {}
    for effective_date, requests in combined_schedule.items():
        normalized[effective_date] = sorted(
            requests,
            key=lambda request: (request.flow_type != "contribution", request.investor_id, request.request_id),
        )
    return normalized


def apply_daily_cash_flows(
    *,
    current_date,
    requests,
    total_units,
    investor_units,
    unit_price,
    pending_contribution_cash,
    pending_withdrawal_cash_requested,
):
    contribution_requests = [request for request in requests if request.flow_type == "contribution"]
    withdrawal_requests = [request for request in requests if request.flow_type == "withdrawal"]

    requested_contribution_amount = sum(float(request.amount) for request in contribution_requests)
    requested_withdrawal_amount = sum(float(request.amount) for request in withdrawal_requests)

    pending_contribution_cash += requested_contribution_amount
    pending_withdrawal_cash_requested += requested_withdrawal_amount

    if not requests and pending_contribution_cash <= 1e-9 and pending_withdrawal_cash_requested <= 1e-9:
        return (
            total_units,
            investor_units,
            pending_contribution_cash,
            pending_withdrawal_cash_requested,
            DailyCashFlowSummary(unit_price=unit_price),
        )

    units_purchased = (pending_contribution_cash / unit_price) if pending_contribution_cash > 0 and unit_price > 0 else 0.0
    contribution_cash_added = pending_contribution_cash if pending_contribution_cash > 0 else 0.0
    if contribution_cash_added > 0:
        investor_units += units_purchased
        total_units += units_purchased
        pending_contribution_cash = 0.0

    requested_units_to_redeem = (
        pending_withdrawal_cash_requested / unit_price
        if pending_withdrawal_cash_requested > 0 and unit_price > 0
        else 0.0
    )
    units_redeemed = min(investor_units, requested_units_to_redeem)
    withdrawal_cash_paid = units_redeemed * unit_price
    if withdrawal_cash_paid > 0:
        investor_units = max(0.0, investor_units - units_redeemed)
        total_units = max(0.0, total_units - units_redeemed)
        pending_withdrawal_cash_requested = max(
            0.0,
            pending_withdrawal_cash_requested - withdrawal_cash_paid,
        )

    if investor_units > total_units + 1e-9:
        raise ValueError(
            f"Investor units exceeded total fund units on {current_date.isoformat()}."
        )

    return (
        total_units,
        investor_units,
        pending_contribution_cash,
        pending_withdrawal_cash_requested,
        DailyCashFlowSummary(
            requested_contribution_amount=requested_contribution_amount,
            requested_withdrawal_amount=requested_withdrawal_amount,
            contribution_cash_added=contribution_cash_added,
            withdrawal_cash_paid=withdrawal_cash_paid,
            withdrawal_cash_requested_remaining=pending_withdrawal_cash_requested,
            contribution_cash_pending_outside_fund=pending_contribution_cash,
            units_purchased=units_purchased,
            units_redeemed=units_redeemed,
            unit_price=unit_price,
            requests=tuple(requests),
        ),
    )


def build_reference_strategy_series(
    start_date,
    end_date,
    *,
    initial_unit_price,
    reference_initial_cash,
    benchmark_symbol,
    cache_path,
    batch_size,
    warmup_days,
    run_on_schedule_only,
    strategy_weekday,
    raw_rank_consideration_limit,
    max_position_fraction,
    defensive_mode,
    defensive_symbol,
    trade_fee_flat,
    trade_fee_rate,
):
    start_date = _coerce_date(start_date)
    end_date = _coerce_date(end_date)
    preload_start = start_date - dt.timedelta(days=calendar_days_for_trading_window(warmup_days))

    universe_symbols = set(load_snp1500_symbols()) | {benchmark_symbol}
    if defensive_mode == "treasury_bonds":
        universe_symbols.add(defensive_symbol)

    bars_df = _load_cached_bars_if_available(sorted(universe_symbols), cache_path)
    if bars_df is None:
        bars_df = load_or_fetch_historical_bars(
            sorted(universe_symbols),
            preload_start,
            end_date,
            cache_path=cache_path,
            batch_size=batch_size,
        )

    data_client = SimulatedDataClient(bars_df)
    trading_client = SimulatedTradingClient(
        data_client,
        initial_cash=reference_initial_cash,
        trade_fee_flat=trade_fee_flat,
        trade_fee_rate=trade_fee_rate,
    )

    benchmark_frame = data_client.get_symbol_frame(benchmark_symbol).sort_index().copy()
    if benchmark_frame.empty:
        raise ValueError(f"No benchmark data available for {benchmark_symbol}.")
    benchmark_frame["rolling_average"] = benchmark_frame["close"].rolling(
        window=BENCHMARK_ROLLING_WINDOW,
        min_periods=1,
    ).mean()

    benchmark_index = benchmark_frame.index[
        (benchmark_frame.index.date >= start_date) & (benchmark_frame.index.date <= end_date)
    ]
    if len(benchmark_index) == 0:
        raise ValueError("No trading dates found in the requested backtest range.")

    rows = []
    for step_number, timestamp in enumerate(benchmark_index, start=1):
        run_date = timestamp.date()
        trading_client.set_current_date(run_date)
        strategy_ran = _should_run_strategy(
            run_date,
            run_on_schedule_only=run_on_schedule_only,
            strategy_weekday=strategy_weekday,
        )

        run_summary = None
        if strategy_ran:
            print(f"[reference {step_number}/{len(benchmark_index)}] Running strategy for {run_date.isoformat()}")
            try:
                run_summary = RunAll(
                    trading_client=trading_client,
                    data_client=data_client,
                    run_date=run_date,
                    save_outputs=False,
                    raw_rank_consideration_limit=raw_rank_consideration_limit,
                    max_position_fraction=max_position_fraction,
                    defensive_mode=defensive_mode,
                    defensive_symbol=defensive_symbol,
                )
            except Exception as exc:
                raise RuntimeError(f"Reference strategy backtest failed on {run_date.isoformat()}") from exc
        elif step_number == 1:
            print(
                f"[reference {step_number}/{len(benchmark_index)}] Tracking strategy from {run_date.isoformat()} "
                "until the first scheduled run"
            )

        reference_portfolio_value = trading_client.portfolio_value
        unit_price = initial_unit_price * (reference_portfolio_value / float(reference_initial_cash))
        benchmark_close = float(benchmark_frame.loc[timestamp, "close"])
        benchmark_rolling_average = float(benchmark_frame.loc[timestamp, "rolling_average"])
        rows.append(
            ReferenceDay(
                date=run_date,
                unit_price=unit_price,
                reference_portfolio_value=reference_portfolio_value,
                strategy_ran=strategy_ran,
                benchmark_close=benchmark_close,
                benchmark_rolling_average=benchmark_rolling_average,
                market_health=run_summary["market_health"] if run_summary else None,
            )
        )

    reference_df = pd.DataFrame([row.__dict__ for row in rows])
    return reference_df


def _derive_investor_chart_path(chart_path):
    chart_path = Path(chart_path)
    return chart_path.with_name(f"{chart_path.stem}Investors{chart_path.suffix}")


def _rebase_series_to_100(series):
    valid = pd.Series(series).dropna()
    if valid.empty or abs(float(valid.iloc[0])) <= 1e-12:
        return pd.Series(series, dtype=float)
    return pd.Series(series, dtype=float) / float(valid.iloc[0]) * 100.0


def _plot_benchmark_reference(results_df, chart_path, benchmark_symbol):
    chart_path = Path(chart_path)
    chart_path.parent.mkdir(parents=True, exist_ok=True)

    dates = pd.to_datetime(results_df["date"])
    benchmark_close = pd.to_numeric(results_df["benchmark_close"], errors="coerce")
    benchmark_rolling_average = pd.to_numeric(results_df["benchmark_rolling_average"], errors="coerce")
    unit_price = pd.to_numeric(results_df["unit_price"], errors="coerce")

    rebased_unit_price = _rebase_series_to_100(unit_price)
    rebased_benchmark = _rebase_series_to_100(benchmark_close)
    rebased_rolling_average = _rebase_series_to_100(benchmark_rolling_average)

    fig, (ax_relative, ax_benchmark) = plt.subplots(2, 1, figsize=(15, 10), sharex=True)

    ax_relative.plot(
        dates,
        rebased_unit_price,
        label="Fund Unit Price (Rebased)",
        color="#1f77b4",
        linewidth=2.2,
    )
    ax_relative.plot(
        dates,
        rebased_benchmark,
        label=f"{benchmark_symbol} Close (Rebased)",
        color="#0b6e4f",
        linewidth=2.0,
    )
    ax_relative.plot(
        dates,
        rebased_rolling_average,
        label=f"{benchmark_symbol} {BENCHMARK_ROLLING_WINDOW}D Avg (Rebased)",
        color="#f18f01",
        linewidth=1.8,
        linestyle="--",
    )
    ax_relative.set_ylabel("Rebased to 100")
    ax_relative.set_title(f"Fund Unit Price vs {benchmark_symbol} and Its {BENCHMARK_ROLLING_WINDOW}-Day Average")
    ax_relative.grid(alpha=0.25)
    ax_relative.legend(loc="best")

    ax_benchmark.plot(
        dates,
        benchmark_close,
        label=f"{benchmark_symbol} Close",
        color="#0b6e4f",
        linewidth=2.0,
    )
    ax_benchmark.plot(
        dates,
        benchmark_rolling_average,
        label=f"{benchmark_symbol} {BENCHMARK_ROLLING_WINDOW}D Avg",
        color="#f18f01",
        linewidth=1.8,
        linestyle="--",
    )
    ax_benchmark.set_ylabel("Index Level")
    ax_benchmark.set_title(f"{benchmark_symbol} Index and {BENCHMARK_ROLLING_WINDOW}-Day Rolling Average")
    ax_benchmark.grid(alpha=0.25)
    ax_benchmark.legend(loc="best")
    ax_benchmark.set_xlabel("Date")

    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(chart_path, dpi=150)
    plt.close(fig)


def _plot_investor_summary(investor_results_df, chart_path):
    chart_path = Path(chart_path)
    chart_path.parent.mkdir(parents=True, exist_ok=True)

    investor_ids = list(investor_results_df["investor_id"].drop_duplicates())
    colors = list(plt.cm.tab10.colors) + list(plt.cm.Set2.colors) + list(plt.cm.Dark2.colors)

    fig, (ax_units, ax_returns) = plt.subplots(2, 1, figsize=(15, 10), sharex=True)

    for index, investor_id in enumerate(investor_ids):
        investor_frame = investor_results_df.loc[investor_results_df["investor_id"] == investor_id].copy()
        investor_dates = pd.to_datetime(investor_frame["date"])
        color = colors[index % len(colors)]
        label = investor_frame["display_name"].iloc[0] if not investor_frame.empty else investor_id

        ax_units.plot(
            investor_dates,
            investor_frame["units"],
            label=label,
            color=color,
            linewidth=2,
        )
        ax_returns.plot(
            investor_dates,
            investor_frame["money_weighted_return"] * 100.0,
            label=label,
            color=color,
            linewidth=2,
        )

    ax_units.set_ylabel("Units")
    ax_units.set_title("Investor Units Owned")
    ax_units.grid(alpha=0.25)
    ax_units.legend(loc="upper left", ncol=2)

    ax_returns.axhline(0.0, color="#666666", linewidth=1.0, linestyle=":")
    ax_returns.set_ylabel("Return (%)")
    ax_returns.set_title("Investor Money-Weighted Returns")
    ax_returns.grid(alpha=0.25)
    ax_returns.legend(loc="upper left", ncol=2)
    ax_returns.set_xlabel("Date")

    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(chart_path, dpi=150)
    plt.close(fig)


def plot_fund_flow_results(results_df, investor_results_df, chart_path, benchmark_symbol):
    benchmark_chart_path = Path(chart_path)
    investor_chart_path = _derive_investor_chart_path(benchmark_chart_path)
    _plot_benchmark_reference(results_df, benchmark_chart_path, benchmark_symbol)
    _plot_investor_summary(investor_results_df, investor_chart_path)
    return benchmark_chart_path, investor_chart_path


def run_fund_flow_backtest(
    start_date,
    end_date,
    *,
    initial_contribution,
    monthly_contribution,
    withdrawal_after_months,
    withdrawal_amount,
    initial_unit_price=DEFAULT_INITIAL_UNIT_PRICE,
    reference_initial_cash=100000.0,
    benchmark_symbol="SPTM",
    results_path=DEFAULT_RESULTS_PATH,
    investor_results_path=DEFAULT_INVESTOR_RESULTS_PATH,
    chart_path=DEFAULT_CHART_PATH,
    cache_path=None,
    batch_size=400,
    warmup_days=260,
    run_on_schedule_only=True,
    strategy_weekday=2,
    raw_rank_consideration_limit=80,
    max_position_fraction=0.10,
    defensive_mode="cash",
    defensive_symbol="SGOV",
    trade_fee_flat=0.0,
    trade_fee_rate=0.0,
    fund_id="demo_fund",
    investor_id="demo_investor",
    investor_plans=None,
):
    timer_start = perf_counter()
    start_date = _coerce_date(start_date)
    end_date = _coerce_date(end_date)
    reference_df = build_reference_strategy_series(
        start_date,
        end_date,
        initial_unit_price=initial_unit_price,
        reference_initial_cash=reference_initial_cash,
        benchmark_symbol=benchmark_symbol,
        cache_path=cache_path,
        batch_size=batch_size,
        warmup_days=warmup_days,
        run_on_schedule_only=run_on_schedule_only,
        strategy_weekday=strategy_weekday,
        raw_rank_consideration_limit=raw_rank_consideration_limit,
        max_position_fraction=max_position_fraction,
        defensive_mode=defensive_mode,
        defensive_symbol=defensive_symbol,
        trade_fee_flat=trade_fee_flat,
        trade_fee_rate=trade_fee_rate,
    )
    if reference_df.empty:
        raise ValueError("Reference strategy series did not contain any trading dates.")

    benchmark_index = pd.to_datetime(reference_df["date"])

    dealing_dates = _build_dealing_dates(
        benchmark_index,
        run_on_schedule_only=run_on_schedule_only,
        strategy_weekday=strategy_weekday,
    )
    if not dealing_dates:
        raise ValueError("No dealing dates were found in the requested backtest range.")

    if investor_plans is None:
        investor_plans = [
            InvestorPlan(
                investor_id=investor_id,
                display_name=investor_id.replace("_", " ").title(),
                initial_contribution=initial_contribution,
                monthly_contribution=monthly_contribution,
                withdrawal_after_months=withdrawal_after_months,
                withdrawal_amount=withdrawal_amount,
            )
        ]

    investor_plans = list(investor_plans)
    if not investor_plans:
        raise ValueError("investor_plans must contain at least one investor plan.")

    cash_flow_schedule = build_multi_investor_cash_flow_schedule(
        benchmark_index,
        dealing_dates=dealing_dates,
        fund_id=fund_id,
        investor_plans=investor_plans,
    )

    investor_states = {
        plan.investor_id: InvestorState(
            investor_id=plan.investor_id,
            display_name=plan.display_name,
        )
        for plan in investor_plans
    }

    total_units = 0.0
    cumulative_requested_contributions = 0.0
    cumulative_requested_withdrawals = 0.0
    cumulative_effective_contributions = 0.0
    cumulative_effective_withdrawals = 0.0
    rows = []
    investor_rows = []

    for step_number, timestamp in enumerate(benchmark_index, start=1):
        run_date = timestamp.date()
        reference_row = reference_df.iloc[step_number - 1]
        strategy_ran = bool(reference_row["strategy_ran"])
        unit_price = float(reference_row["unit_price"])
        reference_portfolio_value = float(reference_row["reference_portfolio_value"])
        market_health = reference_row["market_health"] if pd.notna(reference_row["market_health"]) else None

        daily_requests = cash_flow_schedule.get(run_date, [])
        requests_by_investor = defaultdict(list)
        for request in daily_requests:
            requests_by_investor[request.investor_id].append(request)

        investor_daily_summaries = {}
        if strategy_ran:
            for investor_plan in investor_plans:
                state = investor_states[investor_plan.investor_id]
                (
                    total_units,
                    state.units,
                    state.pending_contribution_cash,
                    state.pending_withdrawal_cash_requested,
                    investor_daily_summaries[investor_plan.investor_id],
                ) = apply_daily_cash_flows(
                    current_date=run_date,
                    requests=requests_by_investor.get(investor_plan.investor_id, []),
                    total_units=total_units,
                    investor_units=state.units,
                    unit_price=unit_price,
                    pending_contribution_cash=state.pending_contribution_cash,
                    pending_withdrawal_cash_requested=state.pending_withdrawal_cash_requested,
                )
        else:
            if daily_requests:
                raise ValueError(
                    f"Cash flows were scheduled for non-dealing day {run_date.isoformat()}."
                )
            for investor_plan in investor_plans:
                state = investor_states[investor_plan.investor_id]
                investor_daily_summaries[investor_plan.investor_id] = DailyCashFlowSummary(
                    contribution_cash_pending_outside_fund=state.pending_contribution_cash,
                    withdrawal_cash_requested_remaining=state.pending_withdrawal_cash_requested,
                    unit_price=unit_price,
                )

        aggregate_requested_contribution_amount = 0.0
        aggregate_requested_withdrawal_amount = 0.0
        aggregate_contribution_amount = 0.0
        aggregate_withdrawal_amount = 0.0
        aggregate_pending_contribution_cash = 0.0
        aggregate_pending_withdrawal_cash = 0.0
        aggregate_units_purchased = 0.0
        aggregate_units_redeemed = 0.0

        for investor_plan in investor_plans:
            state = investor_states[investor_plan.investor_id]
            flow_summary = investor_daily_summaries[investor_plan.investor_id]

            state.cumulative_requested_contributions += flow_summary.requested_contribution_amount
            state.cumulative_requested_withdrawals += flow_summary.requested_withdrawal_amount
            state.cumulative_effective_contributions += flow_summary.contribution_cash_added
            state.cumulative_effective_withdrawals += flow_summary.withdrawal_cash_paid

            aggregate_requested_contribution_amount += flow_summary.requested_contribution_amount
            aggregate_requested_withdrawal_amount += flow_summary.requested_withdrawal_amount
            aggregate_contribution_amount += flow_summary.contribution_cash_added
            aggregate_withdrawal_amount += flow_summary.withdrawal_cash_paid
            aggregate_pending_contribution_cash += state.pending_contribution_cash
            aggregate_pending_withdrawal_cash += state.pending_withdrawal_cash_requested
            aggregate_units_purchased += flow_summary.units_purchased
            aggregate_units_redeemed += flow_summary.units_redeemed

            investor_value = calculate_member_value(state.units, unit_price) if state.units > 0 else 0.0
            investor_net_contributions = (
                state.cumulative_effective_contributions - state.cumulative_effective_withdrawals
            )
            average_cost_per_held_unit = (
                investor_net_contributions / state.units if state.units > 0 else 0.0
            )
            investor_rows.append(
                {
                    "date": run_date,
                    "investor_id": state.investor_id,
                    "display_name": state.display_name,
                    "unit_price": unit_price,
                    "units": state.units,
                    "value": investor_value,
                    "requested_contribution_amount": flow_summary.requested_contribution_amount,
                    "requested_withdrawal_amount": flow_summary.requested_withdrawal_amount,
                    "contribution_amount": flow_summary.contribution_cash_added,
                    "withdrawal_amount": flow_summary.withdrawal_cash_paid,
                    "pending_contribution_cash_outside_fund": state.pending_contribution_cash,
                    "pending_withdrawal_requested_cash": state.pending_withdrawal_cash_requested,
                    "units_purchased": flow_summary.units_purchased,
                    "units_redeemed": flow_summary.units_redeemed,
                    "net_contributions": investor_net_contributions,
                    "net_requested_contributions": state.cumulative_requested_contributions - state.cumulative_requested_withdrawals,
                    "average_cost_per_held_unit": average_cost_per_held_unit,
                    "money_weighted_gain": investor_value - investor_net_contributions,
                    "strategy_ran": strategy_ran,
                }
            )

        cumulative_requested_contributions += aggregate_requested_contribution_amount
        cumulative_requested_withdrawals += aggregate_requested_withdrawal_amount
        cumulative_effective_contributions += aggregate_contribution_amount
        cumulative_effective_withdrawals += aggregate_withdrawal_amount

        portfolio_value = total_units * unit_price

        rows.append(
            {
                "date": run_date,
                "portfolio_value": portfolio_value,
                "reference_portfolio_value": reference_portfolio_value,
                "benchmark_close": float(reference_row["benchmark_close"]),
                "benchmark_rolling_average": float(reference_row["benchmark_rolling_average"]),
                "cash": 0.0,
                "invested_value": portfolio_value,
                "cash_percentage": 0.0,
                "positions": 0,
                "unit_price": unit_price,
                "flow_unit_price": unit_price,
                "total_units": total_units,
                "investor_count": len(investor_plans),
                "active_investors": sum(1 for state in investor_states.values() if state.units > 0),
                "requested_contribution_amount": aggregate_requested_contribution_amount,
                "requested_withdrawal_amount": aggregate_requested_withdrawal_amount,
                "contribution_amount": aggregate_contribution_amount,
                "withdrawal_amount": aggregate_withdrawal_amount,
                "pending_contribution_cash_outside_fund": aggregate_pending_contribution_cash,
                "pending_withdrawal_requested_cash": aggregate_pending_withdrawal_cash,
                "units_purchased": aggregate_units_purchased,
                "units_redeemed": aggregate_units_redeemed,
                "net_flow": aggregate_contribution_amount - aggregate_withdrawal_amount,
                "net_contributions": cumulative_effective_contributions - cumulative_effective_withdrawals,
                "net_requested_contributions": cumulative_requested_contributions - cumulative_requested_withdrawals,
                "market_health": market_health,
                "strategy_ran": strategy_ran,
                "fees_paid_cumulative": 0.0,
                "trade_count": 0,
                "defensive_mode": defensive_mode,
                "defensive_symbol": defensive_symbol if defensive_mode == "treasury_bonds" else "",
            }
        )

    results_df = pd.DataFrame(rows)
    results_df["money_weighted_gain"] = results_df["portfolio_value"] - results_df["net_contributions"]
    prior_unit_price = results_df["unit_price"].shift(1)
    results_df["unit_return"] = (
        results_df["unit_price"].div(prior_unit_price).sub(1.0).where(prior_unit_price > 0)
    )
    investor_results_df = pd.DataFrame(investor_rows)
    investor_results_df["money_weighted_return"] = (
        investor_results_df["value"].div(investor_results_df["net_contributions"]).sub(1.0)
    ).where(investor_results_df["net_contributions"] > 0)

    results_path = Path(results_path)
    results_path.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(results_path, index=False)

    investor_results_path = Path(investor_results_path)
    investor_results_path.parent.mkdir(parents=True, exist_ok=True)
    investor_results_df.to_csv(investor_results_path, index=False)
    benchmark_chart_path, investor_chart_path = plot_fund_flow_results(
        results_df,
        investor_results_df,
        chart_path,
        benchmark_symbol,
    )

    elapsed_seconds = perf_counter() - timer_start
    elapsed_label = _format_elapsed_time(elapsed_seconds)

    final_row = results_df.iloc[-1]
    print(f"Fund flow backtest results saved to {results_path}")
    print(f"Investor flow results saved to {investor_results_path}")
    print(f"Benchmark comparison chart saved to {benchmark_chart_path}")
    print(f"Investor summary chart saved to {investor_chart_path}")
    print(f"Final portfolio value: ${final_row['portfolio_value']:.2f}")
    print(f"Net contributions: ${final_row['net_contributions']:.2f}")
    print(f"Money-weighted gain: ${final_row['money_weighted_gain']:.2f}")
    print(f"Final unit price: ${final_row['unit_price']:.4f}")
    print(f"Final total fund units: {final_row['total_units']:.6f}")
    print(f"Fund flow backtest run time: {elapsed_label}")

    return results_df, investor_results_df


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run a unitized fund backtest with scheduled contributions and withdrawals."
    )
    parser.add_argument("--start", required=True, help="Backtest start date in YYYY-MM-DD format.")
    parser.add_argument("--end", required=True, help="Backtest end date in YYYY-MM-DD format.")
    parser.add_argument("--initial-contribution", type=float, default=1000.0, help="Initial cash contribution on the first trading day.")
    parser.add_argument("--monthly-contribution", type=float, default=500.0, help="Cash contribution on the first trading day of each later month.")
    parser.add_argument("--withdrawal-after-months", type=int, default=6, help="Number of months after the start date to schedule the withdrawal.")
    parser.add_argument("--withdrawal-amount", type=float, default=2000.0, help="Withdrawal cash amount.")
    parser.add_argument("--initial-unit-price", type=float, default=DEFAULT_INITIAL_UNIT_PRICE, help="Unit price to use when the fund has no existing units.")
    parser.add_argument("--reference-initial-cash", type=float, default=100000.0, help="Fixed notional used to generate the reference strategy return stream.")
    parser.add_argument("--benchmark", default="SPTM", help="Benchmark symbol used to define the trading calendar.")
    parser.add_argument("--results-path", default=str(DEFAULT_RESULTS_PATH), help="CSV output path.")
    parser.add_argument("--investor-results-path", default=str(DEFAULT_INVESTOR_RESULTS_PATH), help="Per-investor CSV output path.")
    parser.add_argument("--chart-path", default=str(DEFAULT_CHART_PATH), help="Chart output path.")
    parser.add_argument("--cache-path", default=None, help="Optional cache file for historical bars.")
    parser.add_argument("--batch-size", type=int, default=400, help="Historical data request batch size.")
    parser.add_argument("--warmup-days", type=int, default=260, help="Extra trading days to preload before the start date.")
    parser.add_argument("--raw-rank-consideration-limit", type=int, default=80, help="Shared raw-rank cutoff used for both sell decisions and post-filter buy consideration.")
    parser.add_argument("--max-position-fraction", type=float, default=0.10, help="Hard cap on how large a single stock position can be as a fraction of portfolio value.")
    parser.add_argument("--defensive-mode", default="cash", choices=["cash", "treasury_bonds"], help="How to handle idle cash during bad markets.")
    parser.add_argument("--defensive-symbol", default="SGOV", help="Treasury ETF to use when defensive mode is treasury_bonds.")
    parser.add_argument("--trade-fee-flat", type=float, default=0.0, help="Flat USD fee applied to every trade in the backtest.")
    parser.add_argument("--trade-fee-rate", type=float, default=0.0, help="Proportional fee rate applied to trade notional in the backtest.")
    parser.add_argument("--fund-id", default="demo_fund", help="Synthetic fund identifier for the backtest cash-flow schedule.")
    parser.add_argument("--investor-id", default="demo_investor", help="Synthetic investor identifier for the backtest cash-flow schedule.")
    return parser.parse_args()


def run_backtest_from_editor_settings():
    return run_fund_flow_backtest(
        EDITOR_START_DATE,
        EDITOR_END_DATE,
        initial_contribution=EDITOR_INITIAL_CONTRIBUTION,
        monthly_contribution=EDITOR_MONTHLY_CONTRIBUTION,
        withdrawal_after_months=EDITOR_WITHDRAWAL_AFTER_MONTHS,
        withdrawal_amount=EDITOR_WITHDRAWAL_AMOUNT,
        initial_unit_price=EDITOR_INITIAL_UNIT_PRICE,
        reference_initial_cash=EDITOR_REFERENCE_INITIAL_CASH,
        benchmark_symbol=EDITOR_BENCHMARK_SYMBOL,
        results_path=EDITOR_RESULTS_PATH,
        investor_results_path=EDITOR_INVESTOR_RESULTS_PATH,
        chart_path=EDITOR_CHART_PATH,
        cache_path=EDITOR_CACHE_PATH,
        batch_size=EDITOR_BATCH_SIZE,
        warmup_days=EDITOR_WARMUP_DAYS,
        run_on_schedule_only=EDITOR_RUN_ON_SCHEDULE_ONLY,
        strategy_weekday=EDITOR_STRATEGY_WEEKDAY,
        raw_rank_consideration_limit=EDITOR_RAW_RANK_CONSIDERATION_LIMIT,
        max_position_fraction=EDITOR_MAX_POSITION_FRACTION,
        defensive_mode=EDITOR_DEFENSIVE_MODE,
        defensive_symbol=EDITOR_DEFENSIVE_SYMBOL,
        trade_fee_flat=EDITOR_TRADE_FEE_FLAT,
        trade_fee_rate=EDITOR_TRADE_FEE_RATE,
        fund_id=EDITOR_FUND_ID,
        investor_id=EDITOR_INVESTOR_ID,
        investor_plans=_build_editor_investor_plans(),
    )


if __name__ == "__main__":
    if RUN_WITH_EDITOR_SETTINGS:
        run_backtest_from_editor_settings()
    else:
        args = parse_args()
        run_fund_flow_backtest(
            args.start,
            args.end,
            initial_contribution=args.initial_contribution,
            monthly_contribution=args.monthly_contribution,
            withdrawal_after_months=args.withdrawal_after_months,
            withdrawal_amount=args.withdrawal_amount,
            initial_unit_price=args.initial_unit_price,
            reference_initial_cash=args.reference_initial_cash,
            benchmark_symbol=args.benchmark,
            results_path=args.results_path,
            investor_results_path=args.investor_results_path,
            chart_path=args.chart_path,
            cache_path=args.cache_path,
            batch_size=args.batch_size,
            warmup_days=args.warmup_days,
            raw_rank_consideration_limit=args.raw_rank_consideration_limit,
            max_position_fraction=args.max_position_fraction,
            defensive_mode=args.defensive_mode,
            defensive_symbol=args.defensive_symbol,
            trade_fee_flat=args.trade_fee_flat,
            trade_fee_rate=args.trade_fee_rate,
            fund_id=args.fund_id,
            investor_id=args.investor_id,
        )
