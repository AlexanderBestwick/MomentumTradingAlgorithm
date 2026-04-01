import argparse
import datetime as dt
import os
from pathlib import Path
import sys
from time import perf_counter
from types import SimpleNamespace
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter
from matplotlib.transforms import blended_transform_factory
import pandas as pd

from Config import get_alpaca_credentials, load_local_env
from App.LiveRebalance import RunAll
from SiteData.Publisher import (
    DEFAULT_BACKTEST_HISTORY_LIMIT,
    DEFAULT_SITE_DATA_ROOT,
    publish_error_event,
    publish_backtest_run,
    upload_site_data_to_s3,
)
from Strategies.Momentum.Logic.TradingDays import calendar_days_for_trading_window
from Strategies.Momentum.Logic.UniverseSelection import load_snp1500_symbols


load_local_env()

DEFAULT_RESULTS_PATH = Path("Data/BacktestResults.csv")
DEFAULT_CHART_PATH = Path("Data/BacktestResults.png")
DEFAULT_SITE_DATA_PATH = DEFAULT_SITE_DATA_ROOT
DEFAULT_SITE_DATA_HISTORY_LIMIT = DEFAULT_BACKTEST_HISTORY_LIMIT
DEFAULT_S3_PUBLISH_ENABLED = os.getenv("S3_PUBLISH_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
DEFAULT_S3_PREFIX = os.getenv("S3_PREFIX", "")
DEFAULT_AWS_REGION = os.getenv("AWS_REGION")

# Backtest parameters for running this file directly from your IDE.
# Edit these values, then press Run on Backtesting.py.
RUN_WITH_EDITOR_SETTINGS = True
EDITOR_START_DATE = "2017-02-01"
EDITOR_END_DATE = "2026-02-01"
EDITOR_INITIAL_CASH = 100000
EDITOR_BENCHMARK_SYMBOL = "SPTM"
EDITOR_RESULTS_PATH = Path("Data/BacktestResults.csv")
EDITOR_CHART_PATH = Path("Data/BacktestResults.png")
EDITOR_CACHE_PATH = Path("Data/backtest_cache_20160517_20260201(Long).PKL")
EDITOR_BATCH_SIZE = 400
EDITOR_WARMUP_DAYS = 260  # trading days
EDITOR_RUN_ON_SCHEDULE_ONLY = True
EDITOR_STRATEGY_WEEKDAY = 2  # Monday=0, Tuesday=1, Wednesday=2
EDITOR_RAW_RANK_CONSIDERATION_LIMIT = 100
EDITOR_MAX_POSITION_FRACTION = 0.10
EDITOR_DEFENSIVE_MODE = "treasury_bonds"  # "cash" or "treasury_bonds"
EDITOR_DEFENSIVE_SYMBOL = "IEI" #'SHY'  #Short-duration Treasury ETF proxy
EDITOR_TRADE_FEE_FLAT = 1.00
EDITOR_TRADE_FEE_RATE = 0.0005
EDITOR_EXPORT_SITE_DATA = True
EDITOR_SITE_DATA_PATH = DEFAULT_SITE_DATA_PATH
EDITOR_SITE_DATA_HISTORY_LIMIT = DEFAULT_SITE_DATA_HISTORY_LIMIT
EDITOR_S3_PUBLISH_ENABLED = DEFAULT_S3_PUBLISH_ENABLED
EDITOR_S3_BUCKET_NAME = DEFAULT_S3_BUCKET_NAME
EDITOR_S3_PREFIX = DEFAULT_S3_PREFIX
EDITOR_AWS_REGION = DEFAULT_AWS_REGION


def _coerce_date(value):
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, dt.date):
        return value
    return dt.date.fromisoformat(str(value))


def _normalize_symbols(symbol_or_symbols):
    if isinstance(symbol_or_symbols, str):
        return [symbol_or_symbols]
    return list(symbol_or_symbols)


def _empty_bars_frame():
    empty_index = pd.MultiIndex.from_arrays([[], []], names=["symbol", "timestamp"])
    return pd.DataFrame(columns=["open", "high", "low", "close", "volume"], index=empty_index)


def _chunked(values, size):
    for start in range(0, len(values), size):
        yield values[start:start + size]


def _should_run_strategy(run_date, *, run_on_schedule_only=True, strategy_weekday=2):
    if not run_on_schedule_only:
        return True
    return run_date.weekday() == strategy_weekday


def _format_elapsed_time(elapsed_seconds):
    total_seconds = max(0, int(round(elapsed_seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes > 0:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _make_backtest_run_id(start_date, end_date, generated_at):
    return f"{generated_at:%Y%m%dT%H%M%S}_{start_date:%Y%m%d}_{end_date:%Y%m%d}"


def _compute_max_drawdown(values):
    series = pd.Series(values, dtype="float64")
    if series.empty:
        return 0.0
    running_peak = series.cummax()
    drawdowns = (series / running_peak) - 1.0
    return float(drawdowns.min() * 100.0)


def _build_backtest_record(
    results_df,
    *,
    generated_at,
    start_date,
    end_date,
    initial_cash,
    benchmark_symbol,
    raw_rank_consideration_limit,
    max_position_fraction,
    defensive_mode,
    defensive_symbol,
    trade_fee_flat,
    trade_fee_rate,
):
    latest = results_df.iloc[-1]
    final_portfolio_value = float(latest["portfolio_value"])
    final_benchmark_value = float(latest["sptm_value"])
    final_reserve_percentage = float(latest["reserve_percentage"])
    total_fees_paid = float(latest["fees_paid_cumulative"])
    total_trades = int(latest["trade_count"])
    strategy_run_count = int(results_df["strategy_ran"].sum())
    portfolio_return = ((final_portfolio_value / initial_cash) - 1.0) * 100.0 if initial_cash else 0.0
    benchmark_return = ((final_benchmark_value / initial_cash) - 1.0) * 100.0 if initial_cash else 0.0
    alpha_percent = portfolio_return - benchmark_return
    alpha_dollars = final_portfolio_value - final_benchmark_value
    max_drawdown = _compute_max_drawdown(results_df["portfolio_value"])
    reserve_label = "Treasury % of Portfolio" if defensive_mode == "treasury_bonds" else "Cash % of Portfolio"

    return {
        "id": _make_backtest_run_id(start_date, end_date, generated_at),
        "generated_at": generated_at.isoformat(),
        "period": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
            "label": f"{start_date.isoformat()} to {end_date.isoformat()}",
        },
        "settings": {
            "initial_cash": float(initial_cash),
            "benchmark_symbol": benchmark_symbol,
            "raw_rank_consideration_limit": int(raw_rank_consideration_limit),
            "max_position_fraction": float(max_position_fraction),
            "defensive_mode": defensive_mode,
            "defensive_symbol": defensive_symbol if defensive_mode == "treasury_bonds" else "",
            "trade_fee_flat": float(trade_fee_flat),
            "trade_fee_rate": float(trade_fee_rate),
        },
        "summary": {
            "initial_cash": float(initial_cash),
            "benchmark_symbol": benchmark_symbol,
            "final_portfolio_value": final_portfolio_value,
            "final_benchmark_value": final_benchmark_value,
            "portfolio_return_percent": portfolio_return,
            "benchmark_return_percent": benchmark_return,
            "alpha_percent": alpha_percent,
            "alpha_dollars": alpha_dollars,
            "final_reserve_percentage": final_reserve_percentage,
            "reserve_label": reserve_label,
            "positions_final": int(latest["positions"]),
            "trade_count": total_trades,
            "strategy_run_count": strategy_run_count,
            "fees_paid_cumulative": total_fees_paid,
            "max_drawdown_percent": max_drawdown,
            "defensive_mode": defensive_mode,
            "defensive_symbol": defensive_symbol if defensive_mode == "treasury_bonds" else "",
            "raw_rank_consideration_limit": int(raw_rank_consideration_limit),
            "max_position_fraction": float(max_position_fraction),
            "trade_fee_flat": float(trade_fee_flat),
            "trade_fee_rate": float(trade_fee_rate),
            "elapsed_seconds": float(results_df.attrs.get("elapsed_seconds", 0.0)),
            "elapsed_label": results_df.attrs.get("elapsed_label", ""),
        },
        "series": {
            "dates": [value.isoformat() for value in results_df["date"]],
            "portfolio_value": [round(float(value), 4) for value in results_df["portfolio_value"]],
            "benchmark_value": [round(float(value), 4) for value in results_df["sptm_value"]],
            "benchmark_200dma_value": [round(float(value), 4) for value in results_df["sptm_200dma_value"]],
            "reserve_percentage": [round(float(value), 4) for value in results_df["reserve_percentage"]],
        },
    }


class SimulatedDataClient:
    is_backtest = True

    def __init__(self, bars_df):
        cleaned = bars_df.reset_index()
        cleaned["timestamp"] = pd.to_datetime(cleaned["timestamp"]).dt.tz_localize(None)
        self._bars = cleaned.set_index(["symbol", "timestamp"]).sort_index()
        self._frames = {
            symbol: frame.droplevel("symbol").sort_index()
            for symbol, frame in self._bars.groupby(level="symbol")
        }
        self.current_date = None

    def set_current_date(self, current_date):
        self.current_date = pd.Timestamp(_coerce_date(current_date))

    def get_symbol_frame(self, symbol):
        return self._frames.get(symbol, pd.DataFrame())

    def get_latest_price(self, symbol):
        if self.current_date is None:
            raise ValueError("Current backtest date has not been set.")

        frame = self._frames.get(symbol)
        if frame is None or frame.empty:
            raise KeyError(f"No historical data loaded for {symbol}.")

        subset = frame.loc[frame.index <= self.current_date]
        if subset.empty:
            raise KeyError(f"No historical price available for {symbol} on or before {self.current_date.date()}.")

        return float(subset["close"].iloc[-1])

    def get_stock_bars(self, request):
        if self.current_date is None:
            raise ValueError("Current backtest date has not been set.")

        end_date = getattr(request, "end", None)
        end_ts = pd.Timestamp(_coerce_date(end_date)) if end_date is not None else self.current_date

        start_date = getattr(request, "start", None)
        start_ts = pd.Timestamp(_coerce_date(start_date)) if start_date is not None else None

        frames = []
        for symbol in _normalize_symbols(request.symbol_or_symbols):
            frame = self._frames.get(symbol)
            if frame is None or frame.empty:
                continue

            subset = frame.loc[frame.index <= end_ts]
            if start_ts is not None:
                subset = subset.loc[subset.index >= start_ts]
            if subset.empty:
                continue

            materialized = subset.copy()
            materialized["symbol"] = symbol
            frames.append(materialized.reset_index().set_index(["symbol", "timestamp"]))

        if not frames:
            return SimpleNamespace(df=_empty_bars_frame())

        return SimpleNamespace(df=pd.concat(frames).sort_index())

    def get_stock_latest_trade(self, request):
        latest_trades = {}
        for symbol in _normalize_symbols(request.symbol_or_symbols):
            latest_trades[symbol] = SimpleNamespace(price=self.get_latest_price(symbol))
        return latest_trades


class SimulatedTradingClient:
    is_backtest = True

    def __init__(self, data_client, *, initial_cash=100000, trade_fee_flat=0.0, trade_fee_rate=0.0):
        self.data_client = data_client
        self.cash = float(initial_cash)
        self.positions = {}
        self.current_date = None
        self.order_log = []
        self.trade_fee_flat = float(trade_fee_flat)
        self.trade_fee_rate = float(trade_fee_rate)
        self.total_fees_paid = 0.0

    def set_current_date(self, current_date):
        self.current_date = _coerce_date(current_date)
        self.data_client.set_current_date(current_date)

    @property
    def portfolio_value(self):
        total = self.cash
        for symbol, qty in self.positions.items():
            total += qty * self.data_client.get_latest_price(symbol)
        return total

    def get_account(self):
        return SimpleNamespace(
            cash=f"{self.cash:.2f}",
            portfolio_value=f"{self.portfolio_value:.2f}",
        )

    def get_position_value(self, symbol):
        qty = self.positions.get(symbol, 0.0)
        if qty <= 0:
            return 0.0
        return qty * self.data_client.get_latest_price(symbol)

    def _calculate_trade_fee(self, trade_notional):
        if trade_notional <= 0:
            return 0.0
        return self.trade_fee_flat + (trade_notional * self.trade_fee_rate)

    def get_all_positions(self):
        positions = []
        for symbol, qty in sorted(self.positions.items()):
            if qty <= 0:
                continue
            positions.append(
                SimpleNamespace(
                    symbol=symbol,
                    qty=f"{qty:.10f}",
                    market_value=f"{qty * self.data_client.get_latest_price(symbol):.2f}",
                )
            )
        return positions

    def close_position(self, symbol):
        held_qty = self.positions.get(symbol, 0.0)
        if held_qty <= 0:
            raise ValueError(f"No open position in {symbol} to close.")

        price = self.data_client.get_latest_price(symbol)
        trade_notional = held_qty * price
        fee = self._calculate_trade_fee(trade_notional)
        net_proceeds = trade_notional - fee
        self.cash += net_proceeds
        self.total_fees_paid += fee
        del self.positions[symbol]
        self.order_log.append(
            {
                "date": self.current_date,
                "symbol": symbol,
                "side": "sell",
                "qty": held_qty,
                "price": price,
                "notional": trade_notional,
                "fee": fee,
                "net_cash_flow": net_proceeds,
            }
        )

    def submit_order(self, order):
        symbol = order.symbol
        side = getattr(order.side, "value", str(order.side)).lower()
        price = self.data_client.get_latest_price(symbol)

        qty = getattr(order, "qty", None)
        notional = getattr(order, "notional", None)

        if qty is None:
            if notional is None:
                raise ValueError("Order must include qty or notional.")
            qty = float(notional) / price
        else:
            qty = float(qty)

        if side == "buy":
            trade_notional = qty * price
            fee = self._calculate_trade_fee(trade_notional)
            total_cash_needed = trade_notional + fee

            if total_cash_needed > self.cash:
                max_notional = max(0.0, (self.cash - self.trade_fee_flat) / (1.0 + self.trade_fee_rate))
                qty = max_notional / price if price > 0 else 0.0
                trade_notional = qty * price
                fee = self._calculate_trade_fee(trade_notional)
                total_cash_needed = trade_notional + fee

            if qty <= 0 or total_cash_needed > self.cash + 1e-9:
                raise ValueError(f"Insufficient cash to buy {symbol}.")
            total_cash_needed = min(total_cash_needed, self.cash)
            self.cash -= total_cash_needed
            self.total_fees_paid += fee
            self.positions[symbol] = self.positions.get(symbol, 0.0) + qty
            filled_notional = trade_notional
            net_cash_flow = -total_cash_needed
        elif side == "sell":
            held_qty = self.positions.get(symbol, 0.0)
            qty = min(qty, held_qty)
            if qty <= 0:
                raise ValueError(f"No shares available to sell for {symbol}.")
            filled_notional = qty * price
            fee = self._calculate_trade_fee(filled_notional)
            net_cash_flow = filled_notional - fee
            self.cash += net_cash_flow
            self.total_fees_paid += fee
            remaining = held_qty - qty
            if remaining <= 1e-10:
                self.positions.pop(symbol, None)
            else:
                self.positions[symbol] = remaining
        else:
            raise ValueError(f"Unsupported order side: {side}")

        self.order_log.append(
            {
                "date": self.current_date,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "price": price,
                "notional": filled_notional,
                "fee": fee,
                "net_cash_flow": net_cash_flow,
            }
        )

        return SimpleNamespace(
            symbol=symbol,
            qty=qty,
            side=side,
            filled_avg_price=price,
            notional=filled_notional,
            fee=fee,
        )


def fetch_historical_bars(
    data_client,
    symbols,
    start_date,
    end_date,
    *,
    batch_size=400,
):
    frames = []

    for batch_number, batch in enumerate(_chunked(symbols, batch_size), start=1):
        print(f"Fetching batch {batch_number}: {len(batch)} symbols")
        request = StockBarsRequest(
            symbol_or_symbols=batch,
            timeframe=TimeFrame.Day,
            start=start_date,
            end=end_date,
            adjustment="all",
        )
        bars = data_client.get_stock_bars(request)
        batch_df = bars.df
        if batch_df.empty:
            continue

        normalized = batch_df.reset_index()
        normalized["timestamp"] = pd.to_datetime(normalized["timestamp"]).dt.tz_localize(None)
        frames.append(normalized.set_index(["symbol", "timestamp"]).sort_index())

    if not frames:
        raise ValueError("No historical bars were returned for the requested backtest window.")

    return pd.concat(frames).sort_index()


def load_or_fetch_historical_bars(
    symbols,
    start_date,
    end_date,
    *,
    cache_path=None,
    batch_size=400,
):
    cache_path = Path(cache_path) if cache_path else Path(
        f"Data/backtest_cache_{start_date:%Y%m%d}_{end_date:%Y%m%d}.pkl"
    )

    if cache_path.exists():
        print(f"Loading historical bars from {cache_path}")
        bars_df = pd.read_pickle(cache_path)
        available_symbols = set(bars_df.index.get_level_values("symbol"))
        if set(symbols).issubset(available_symbols):
            return bars_df
        print(f"Cache missing {len(set(symbols) - available_symbols)} symbols. Refetching historical bars.")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    credentials = get_alpaca_credentials()
    live_data_client = StockHistoricalDataClient(credentials.key, credentials.secret)
    bars_df = fetch_historical_bars(
        live_data_client,
        symbols,
        start_date,
        end_date,
        batch_size=batch_size,
    )
    bars_df.to_pickle(cache_path)
    print(f"Saved historical bars cache to {cache_path}")
    return bars_df


def plot_backtest_results(results_df, chart_path):
    chart_path = Path(chart_path)
    chart_path.parent.mkdir(parents=True, exist_ok=True)
    defensive_mode = results_df["defensive_mode"].iloc[-1] if "defensive_mode" in results_df.columns and not results_df.empty else "cash"
    reserve_axis_label = "Treasury (% of Portfolio)" if defensive_mode == "treasury_bonds" else "Cash (% of Portfolio)"
    reserve_legend_label = "Treasury % of Portfolio" if defensive_mode == "treasury_bonds" else "Cash % of Portfolio"
    reserve_annotation_label = "Treasury" if defensive_mode == "treasury_bonds" else "Cash"
    reserve_series = results_df["reserve_percentage"] if "reserve_percentage" in results_df.columns else results_df["cash_percentage"]

    def annotate_last_value(ax, x_values, y_values, label, color, formatter):
        if len(y_values) == 0:
            return

        last_y = y_values.iloc[-1]
        if pd.isna(last_y):
            return

        transform = blended_transform_factory(ax.transAxes, ax.transData)
        ax.text(
            1.02,
            last_y,
            f"{label}: {formatter(last_y)}",
            transform=transform,
            ha="left",
            va="center",
            fontsize=9,
            color=color,
            clip_on=False,
            bbox={
                "boxstyle": "round,pad=0.25",
                "facecolor": "white",
                "edgecolor": color,
                "alpha": 0.35,
            },
        )

    fig, ax_left = plt.subplots(figsize=(14, 8))
    portfolio_line, = ax_left.plot(
        results_df["date"],
        results_df["portfolio_value"],
        label="Portfolio Value",
        linewidth=2,
    )
    benchmark_line, = ax_left.plot(
        results_df["date"],
        results_df["sptm_value"],
        label="SPTM Buy & Hold",
        linewidth=2,
    )
    ax_left.plot(
        results_df["date"],
        results_df["sptm_200dma_value"],
        linewidth=2.2,
        linestyle="-",
        color="tab:red",
        alpha=0.8,
    )
    ax_left.set_title("Momentum Strategy Backtest")
    ax_left.set_xlabel("Date")
    ax_left.set_ylabel("Value (USD)")
    ax_left.grid(True, alpha=0.3)
    ax_left.margins(x=0.12)

    ax_right = ax_left.twinx()
    reserve_line, = ax_right.plot(
        results_df["date"],
        reserve_series,
        label=reserve_legend_label,
        linewidth=2,
        linestyle="--",
        color="tab:green",
    )
    ax_right.set_ylabel(reserve_axis_label)
    ax_right.yaxis.set_major_formatter(PercentFormatter(xmax=100))
    ax_right.set_ylim(-5, 105)
    ax_right.margins(x=0.12)

    annotate_last_value(
        ax_left,
        results_df["date"],
        results_df["portfolio_value"],
        "Portfolio",
        portfolio_line.get_color(),
        lambda value: f"${value:,.0f}",
    )
    annotate_last_value(
        ax_left,
        results_df["date"],
        results_df["sptm_value"],
        "SPTM",
        benchmark_line.get_color(),
        lambda value: f"${value:,.0f}",
    )
    annotate_last_value(
        ax_right,
        results_df["date"],
        reserve_series,
        reserve_annotation_label,
        reserve_line.get_color(),
        lambda value: f"{value:.1f}%",
    )

    ax_left.legend(
        [portfolio_line, benchmark_line, reserve_line],
        ["Portfolio Value", "SPTM Buy & Hold", reserve_legend_label],
        loc="upper left",
    )
    fig.tight_layout(rect=(0, 0, 0.82, 1))
    fig.savefig(chart_path, dpi=150)
    plt.close(fig)


def run_backtest(
    start_date,
    end_date,
    *,
    initial_cash=100000,
    benchmark_symbol="SPTM",
    results_path=DEFAULT_RESULTS_PATH,
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
    export_site_data=True,
    site_data_path=DEFAULT_SITE_DATA_PATH,
    site_data_history_limit=DEFAULT_SITE_DATA_HISTORY_LIMIT,
    s3_publish_enabled=DEFAULT_S3_PUBLISH_ENABLED,
    s3_bucket_name=DEFAULT_S3_BUCKET_NAME,
    s3_prefix=DEFAULT_S3_PREFIX,
    aws_region=DEFAULT_AWS_REGION,
):
    timer_start = perf_counter()
    start_date = _coerce_date(start_date)
    end_date = _coerce_date(end_date)
    preload_start = start_date - dt.timedelta(days=calendar_days_for_trading_window(warmup_days))

    universe_symbols = set(load_snp1500_symbols()) | {benchmark_symbol}
    if defensive_mode == "treasury_bonds":
        universe_symbols.add(defensive_symbol)

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
        initial_cash=initial_cash,
        trade_fee_flat=trade_fee_flat,
        trade_fee_rate=trade_fee_rate,
    )

    benchmark_frame = data_client.get_symbol_frame(benchmark_symbol)
    if benchmark_frame.empty:
        raise ValueError(f"No benchmark data available for {benchmark_symbol}.")
    benchmark_frame = benchmark_frame.copy()
    benchmark_frame["sptm_200dma"] = benchmark_frame["close"].rolling(window=200, min_periods=1).mean()

    benchmark_index = benchmark_frame.index[(benchmark_frame.index.date >= start_date) & (benchmark_frame.index.date <= end_date)]
    if len(benchmark_index) == 0:
        raise ValueError("No trading dates found in the requested backtest range.")

    rows = []
    benchmark_start_close = None

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
            print(f"[{step_number}/{len(benchmark_index)}] Running strategy for {run_date.isoformat()}")
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
                raise RuntimeError(f"Backtest failed on {run_date.isoformat()}") from exc

        if step_number == 1 and not strategy_ran:
            print(f"[{step_number}/{len(benchmark_index)}] Tracking portfolio from {run_date.isoformat()} until the first scheduled run")

        benchmark_close = data_client.get_latest_price(benchmark_symbol)
        if benchmark_start_close is None:
            benchmark_start_close = benchmark_close

        rows.append(
            {
                "date": run_date,
                "portfolio_value": trading_client.portfolio_value,
                "cash": trading_client.cash,
                "cash_percentage": (trading_client.cash / trading_client.portfolio_value) * 100 if trading_client.portfolio_value else 0.0,
                "invested_value": trading_client.portfolio_value - trading_client.cash,
                "positions": len(trading_client.positions),
                "sptm_close": benchmark_close,
                "sptm_value": initial_cash * (benchmark_close / benchmark_start_close),
                "sptm_200dma_value": initial_cash * (benchmark_frame.at[timestamp, "sptm_200dma"] / benchmark_start_close),
                "defensive_mode": defensive_mode,
                "defensive_symbol": defensive_symbol if defensive_mode == "treasury_bonds" else "",
                "defensive_value": trading_client.get_position_value(defensive_symbol) if defensive_mode == "treasury_bonds" else 0.0,
                "reserve_percentage": (
                    (trading_client.get_position_value(defensive_symbol) / trading_client.portfolio_value) * 100
                    if defensive_mode == "treasury_bonds" and trading_client.portfolio_value
                    else (trading_client.cash / trading_client.portfolio_value) * 100
                    if trading_client.portfolio_value
                    else 0.0
                ),
                "strategy_ran": strategy_ran,
                "market_health": run_summary["market_health"] if run_summary else None,
                "fees_paid_cumulative": trading_client.total_fees_paid,
                "trade_count": len(trading_client.order_log),
                "raw_rank_consideration_limit": raw_rank_consideration_limit,
                "max_position_fraction": max_position_fraction,
            }
        )

    results_df = pd.DataFrame(rows)
    results_path = Path(results_path)
    results_path.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(results_path, index=False)
    plot_backtest_results(results_df, chart_path)

    elapsed_seconds = perf_counter() - timer_start
    elapsed_label = _format_elapsed_time(elapsed_seconds)
    results_df.attrs["elapsed_seconds"] = elapsed_seconds
    results_df.attrs["elapsed_label"] = elapsed_label

    backtest_record = None
    published_site_data = None
    if export_site_data:
        generated_at = dt.datetime.now(dt.timezone.utc)
        backtest_record = _build_backtest_record(
            results_df,
            generated_at=generated_at,
            start_date=start_date,
            end_date=end_date,
            initial_cash=initial_cash,
            benchmark_symbol=benchmark_symbol,
            raw_rank_consideration_limit=raw_rank_consideration_limit,
            max_position_fraction=max_position_fraction,
            defensive_mode=defensive_mode,
            defensive_symbol=defensive_symbol,
            trade_fee_flat=trade_fee_flat,
            trade_fee_rate=trade_fee_rate,
        )

    if export_site_data and backtest_record is not None:
        published_site_data = publish_backtest_run(
            backtest_record,
            site_data_root=site_data_path,
            chart_path=chart_path,
            results_path=results_path,
            max_runs=site_data_history_limit,
        )
        print(f"Published backtest site data at {site_data_path}")

    if s3_publish_enabled:
        if not published_site_data:
            raise RuntimeError("S3 publishing is enabled, but no site data was generated to upload.")

        uploaded_paths = upload_site_data_to_s3(
            published_site_data["paths"],
            site_data_root=site_data_path,
            bucket_name=s3_bucket_name,
            prefix=s3_prefix,
            aws_region=aws_region,
        )
        print(f"Uploaded {len(uploaded_paths)} published backtest files to s3://{s3_bucket_name}")

    print(f"Backtest results saved to {results_path}")
    print(f"Backtest chart saved to {chart_path}")
    print(f"Backtest run time: {elapsed_label}")

    return results_df


def parse_args():
    parser = argparse.ArgumentParser(description="Run a daily-bar backtest for the momentum strategy.")
    parser.add_argument("--start", required=True, help="Backtest start date in YYYY-MM-DD format.")
    parser.add_argument("--end", required=True, help="Backtest end date in YYYY-MM-DD format.")
    parser.add_argument("--initial-cash", type=float, default=100000, help="Starting portfolio cash.")
    parser.add_argument("--benchmark", default="SPTM", help="Benchmark symbol to plot against.")
    parser.add_argument("--results-path", default=str(DEFAULT_RESULTS_PATH), help="CSV output path.")
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
    parser.add_argument("--site-data-path", default=str(DEFAULT_SITE_DATA_PATH), help="Folder for website-facing JSON and artifact output.")
    parser.add_argument("--site-data-history-limit", type=int, default=DEFAULT_SITE_DATA_HISTORY_LIMIT, help="Maximum number of recent backtest runs to keep in the published site data.")
    parser.add_argument("--s3-publish-enabled", action="store_true", default=DEFAULT_S3_PUBLISH_ENABLED, help="Upload the published site data files to S3 after each run.")
    parser.add_argument("--s3-bucket-name", default=DEFAULT_S3_BUCKET_NAME, help="S3 bucket to receive published site data.")
    parser.add_argument("--s3-prefix", default=DEFAULT_S3_PREFIX, help="Optional S3 key prefix for published site data.")
    parser.add_argument("--aws-region", default=DEFAULT_AWS_REGION, help="AWS region for the S3 upload client.")
    return parser.parse_args()


def run_backtest_from_editor_settings():
    return run_backtest(
        EDITOR_START_DATE,
        EDITOR_END_DATE,
        initial_cash=EDITOR_INITIAL_CASH,
        benchmark_symbol=EDITOR_BENCHMARK_SYMBOL,
        results_path=EDITOR_RESULTS_PATH,
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
        export_site_data=EDITOR_EXPORT_SITE_DATA,
        site_data_path=EDITOR_SITE_DATA_PATH,
        site_data_history_limit=EDITOR_SITE_DATA_HISTORY_LIMIT,
        s3_publish_enabled=EDITOR_S3_PUBLISH_ENABLED,
        s3_bucket_name=EDITOR_S3_BUCKET_NAME,
        s3_prefix=EDITOR_S3_PREFIX,
        aws_region=EDITOR_AWS_REGION,
    )


if __name__ == "__main__":
    args = None
    try:
        if RUN_WITH_EDITOR_SETTINGS:
            run_backtest_from_editor_settings()
        else:
            args = parse_args()
            run_backtest(
                args.start,
                args.end,
                initial_cash=args.initial_cash,
                benchmark_symbol=args.benchmark,
                results_path=args.results_path,
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
                site_data_path=args.site_data_path,
                site_data_history_limit=args.site_data_history_limit,
                s3_publish_enabled=args.s3_publish_enabled,
                s3_bucket_name=args.s3_bucket_name,
                s3_prefix=args.s3_prefix,
                aws_region=args.aws_region,
            )
    except Exception as exc:
        try:
            if RUN_WITH_EDITOR_SETTINGS and EDITOR_EXPORT_SITE_DATA:
                generated_at = dt.datetime.now(dt.timezone.utc)
                published_error_data = publish_error_event(
                    generated_at=generated_at,
                    source="backtest_runner",
                    category="backtest_failed",
                    title="Backtest run failed",
                    message=str(exc),
                    site_data_root=EDITOR_SITE_DATA_PATH,
                    context={
                        "period_start": EDITOR_START_DATE,
                        "period_end": EDITOR_END_DATE,
                        "defensive_mode": EDITOR_DEFENSIVE_MODE,
                        "raw_rank_consideration_limit": EDITOR_RAW_RANK_CONSIDERATION_LIMIT,
                    },
                )
                if EDITOR_S3_PUBLISH_ENABLED:
                    upload_site_data_to_s3(
                        published_error_data["paths"],
                        site_data_root=EDITOR_SITE_DATA_PATH,
                        bucket_name=EDITOR_S3_BUCKET_NAME,
                        prefix=EDITOR_S3_PREFIX,
                        aws_region=EDITOR_AWS_REGION,
                    )
            elif args is not None and args.site_data_path:
                generated_at = dt.datetime.now(dt.timezone.utc)
                published_error_data = publish_error_event(
                    generated_at=generated_at,
                    source="backtest_runner",
                    category="backtest_failed",
                    title="Backtest run failed",
                    message=str(exc),
                    site_data_root=args.site_data_path,
                    context={
                        "period_start": args.start,
                        "period_end": args.end,
                        "defensive_mode": args.defensive_mode,
                        "raw_rank_consideration_limit": args.raw_rank_consideration_limit,
                    },
                )
                if args.s3_publish_enabled:
                    upload_site_data_to_s3(
                        published_error_data["paths"],
                        site_data_root=args.site_data_path,
                        bucket_name=args.s3_bucket_name,
                        prefix=args.s3_prefix,
                        aws_region=args.aws_region,
                    )
        except Exception as publish_exc:
            print(f"Backtest error publishing failed: {publish_exc}", file=sys.stderr)

        print(f"Backtest run failed: {exc}", file=sys.stderr)
        raise
