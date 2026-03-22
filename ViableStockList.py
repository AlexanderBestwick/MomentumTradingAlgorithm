
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import AssetClass
import datetime
import itertools
from pathlib import Path
import pandas as pd
import re
from Functions.TradingDays import calendar_days_for_trading_window, trim_multiindex_to_trailing_trading_days

DEFAULT_HOLDINGS_PATH = "Data/holdings-daily-us-en-sptm.csv"


def load_snp1500_symbols(csv_path=DEFAULT_HOLDINGS_PATH):
    df = pd.read_csv(csv_path, skiprows=4)
    ticker_col = next(c for c in df.columns if "Ticker" in c)
    tickers = (
        df[ticker_col]
        .dropna()
        .astype(str)
        .str.strip()
        .str.upper()
        .unique()
        .tolist()
    )
    clean = [t for t in tickers if re.compile(r"^[A-Z]+(\.[A-Z]+)?$").fullmatch(t)]
    return clean


def BuildSelectionUniverse(
    data_client,
    *,
    min_days=95,
    batch_size=1550,
    history_days=150,
    volatility_days=90,
    moving_average_days=100,
    as_of_date=None,
    save_path="Data/ApprovedStockFrame.csv",
):
    if as_of_date is None:
        as_of_date = datetime.date.today()

    symbols = load_snp1500_symbols()
    print(f"{len(symbols)} Total Stocks in S&P1500")

    def group(iterable, n):
        it = iter(iterable)
        while True:
            batch = list(itertools.islice(it, n))
            if not batch:
                break
            yield batch

    required_trading_days = max(min_days, volatility_days + 1, moving_average_days)
    requested_trading_days = max(history_days, required_trading_days)
    calendar_history_days = calendar_days_for_trading_window(requested_trading_days)

    approved_stocks = []
    full_stock_frames = []
    approved_stock_frames = []
    rejected_stocks = []
    datafail_stocks = []
    short_history_stocks = []
    volatile_stocks = []

    for batch in group(symbols, batch_size):
        req = StockBarsRequest(
            symbol_or_symbols=batch,
            timeframe=TimeFrame.Day,
            start=as_of_date - datetime.timedelta(days=calendar_history_days),
            end=as_of_date,
            adjustment="all",
        )
        bars = data_client.get_stock_bars(req)
        df = bars.df
        if df.empty:
            datafail_stocks.extend(batch)
            continue
        df = trim_multiindex_to_trailing_trading_days(df, requested_trading_days)
        if df.empty:
            datafail_stocks.extend(batch)
            continue

        df["daily_average"] = (df["high"] + df["low"] + df["close"]) / 3

        full_stock_frames.append(df)

        df["DailyCloseChange"] = df["close"].groupby(level="symbol").pct_change().abs()
        df["MovingAverage"] = (
            df["close"]
            .groupby(level="symbol")
            .transform(lambda series: series.rolling(window=moving_average_days, min_periods=moving_average_days).mean())
        )

        passing_stocks = []
        available_symbols = set(df.index.get_level_values("symbol"))

        for sym in batch:
            if sym not in available_symbols:
                datafail_stocks.append(sym)
                continue

            symbol_frame = df.loc[sym].sort_index()
            if len(symbol_frame) < required_trading_days:
                short_history_stocks.append(sym)
                continue

            moving_average = symbol_frame["MovingAverage"].iloc[-1]
            close = symbol_frame["close"].iloc[-1]
            max_gap = symbol_frame["DailyCloseChange"].tail(volatility_days).max(skipna=True)

            if pd.notna(max_gap) and max_gap > 0.15:
                volatile_stocks.append(sym)
                continue

            if pd.notna(moving_average) and close > moving_average:
                approved_stocks.append(sym)
                passing_stocks.append(sym)
            elif pd.notna(moving_average):
                rejected_stocks.append(sym)
            else:
                datafail_stocks.append(sym)

        if passing_stocks:
            approved_stock_frames.append(df.loc[passing_stocks])

    print(f"{len(approved_stocks)} Approved Stocks")
    print(f"{len(rejected_stocks)} Rejected Stocks")
    print(f"{len(short_history_stocks)} Short History Stocks")
    print(f"{len(datafail_stocks)} Data Error Stocks")
    print(f"{len(volatile_stocks)} Volatile Stocks")

    full_df = pd.concat(full_stock_frames).sort_index() if full_stock_frames else pd.DataFrame()
    final_df = pd.concat(approved_stock_frames).sort_index() if approved_stock_frames else pd.DataFrame()
    if save_path:
        final_df.reset_index().to_csv(save_path, index=False)
        print(f"Approved stock frame saved to {save_path}")

    return {
        "full_stock_df": full_df,
        "approved_stock_df": final_df,
        "approved_symbols": set(approved_stocks),
        "approved_stocks": approved_stocks,
        "rejected_stocks": rejected_stocks,
        "short_history_stocks": short_history_stocks,
        "datafail_stocks": datafail_stocks,
        "volatile_stocks": volatile_stocks,
    }


def GenerateStockList(
    data_client,
    *,
    min_days=95,
    batch_size=1550,
    history_days=150,
    volatility_days=90,
    moving_average_days=100,
    as_of_date=None,
    save_path="Data/ApprovedStockFrame.csv",
):
    selection_universe = BuildSelectionUniverse(
        data_client,
        min_days=min_days,
        batch_size=batch_size,
        history_days=history_days,
        volatility_days=volatility_days,
        moving_average_days=moving_average_days,
        as_of_date=as_of_date,
        save_path=save_path,
    )
    return selection_universe["approved_stock_df"]
