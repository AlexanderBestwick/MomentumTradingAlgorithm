
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

def GenerateStockList(data_client, *, min_days=95, batch_size=1550, history_days=150):

    def LoadSNP1500(csv_path="Data/holdings-daily-us-en-sptm.csv"):
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

    symbols = LoadSNP1500()
    print(f"{len(symbols)} Total Stocks in S&P1500")

    def group(iterable, n):
        it = iter(iterable)
        while True:
            batch = list(itertools.islice(it, n))
            if not batch:
                break
            yield batch

    anchor_date = datetime.date.today() - datetime.timedelta(days=history_days - 5)

    approved_stocks = []
    approved_stock_frames = []
    rejected_stocks = []
    datafail_stocks = []
    short_history_stocks = []
    volatile_stocks = []

    for batch in group(symbols, batch_size):
        req = StockBarsRequest(
            symbol_or_symbols=batch,
            timeframe=TimeFrame.Day,
            start=datetime.date.today() - datetime.timedelta(days=history_days),
            adjustment="all",
        )
        bars = data_client.get_stock_bars(req)
        df = bars.df
        df["daily_average"] = (df["high"] + df["low"] + df["close"]) / 3

        average = df.groupby(level="symbol")["daily_average"].mean()
        latest_close = df.groupby(level="symbol")["close"].last()
        earliest_bar = df.reset_index().groupby("symbol")["timestamp"].min()

        df["DailyCloseChange"] = df["close"].groupby(level="symbol").pct_change().abs()

        passing_stocks = []

        for sym in batch:
            first_bar = earliest_bar.get(sym)
            if pd.isna(first_bar) or first_bar.date() > anchor_date:
                short_history_stocks.append(sym)
                continue

            avg = average.get(sym)
            close = latest_close.get(sym)
            max_gap = df.loc[sym, "DailyCloseChange"].tail(90).max(skipna=True)

            if pd.notna(max_gap) and max_gap > 0.15:
                volatile_stocks.append(sym)
                continue

            if pd.notna(avg) and close > avg:
                approved_stocks.append(sym)
                passing_stocks.append(sym)
            elif pd.notna(avg):
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

    final_df = pd.concat(approved_stock_frames).sort_index() if approved_stock_frames else pd.DataFrame()
    final_df.reset_index().to_csv("Data/ApprovedStockFrame.csv", index=False)
    print(f"Approved stock frame saved to Data/ApprovedStockFrame.csv")
    return final_df