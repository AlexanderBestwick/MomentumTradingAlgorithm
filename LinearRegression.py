from alpaca.data.historical import StockHistoricalDataClient
from alpaca.trading.client import TradingClient
import datetime
import pandas as pd
import numpy as np
from scipy.stats import linregress
import matplotlib.pyplot as plt
import talib as ta
from Functions.PositionCap import capped_target_shares
from Functions.TradingDays import trim_multiindex_to_trailing_trading_days

def LoadApprovedBars(csv_path="Data/ApprovedStockFrame.csv", *, days_back=30, as_of_date=None):
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    df["timestamp"] = df["timestamp"].dt.tz_localize(None)
    if as_of_date is not None:
        cutoff = datetime.datetime.combine(as_of_date, datetime.time.max)
        df = df[df["timestamp"] <= cutoff]
    indexed = df.set_index(["symbol", "timestamp"]).sort_index()
    return trim_multiindex_to_trailing_trading_days(indexed, days_back)

def LinearRegression(
    trading_client,
    approved_stock_df=None,
    *,
    save_path="Data/MomentumResults.csv",
    trading_days=250,
    risk_factor=0.001,
    atr_lookback=20,
    max_position_fraction=0.10,
):

    account_value = float(trading_client.get_account().portfolio_value)

    if approved_stock_df is None:
        approved_stock_df = LoadApprovedBars()

    if approved_stock_df.empty:
        result_df = pd.DataFrame(
            columns=["raw_rank", "symbol", "momentum", "shares", "annualised_return", "atr"]
        )
        if save_path:
            result_df.to_csv(save_path, index=False)
            print(f"Momentum results saved to {save_path}")
        return result_df

    stock_results = []

    for symbol, stock_frame in approved_stock_df.groupby(level="symbol"):
        stock_frame = stock_frame.droplevel("symbol").sort_index()
        if len(stock_frame) < max(atr_lookback, 2):
            continue

        high = stock_frame["high"].to_numpy()
        low = stock_frame["low"].to_numpy()
        close = stock_frame["close"].to_numpy()

        atr = ta.ATR(high, low, close, timeperiod=atr_lookback)
        atr_current = atr[-1]
        if np.isnan(atr_current) or atr_current <= 0:
            continue

        daily_average = (high + low + close) / 3
        if np.any(daily_average <= 0):
            continue

        log_daily_average = np.log(daily_average)
        x_days = np.arange(len(log_daily_average))

        slope, intercept, r_value, p_value, std_err = linregress(x_days, log_daily_average)

        annualised_return = np.exp(slope * trading_days) - 1
        momentum = annualised_return * (r_value ** 2)
        risk_based_position_size = (account_value * risk_factor) / atr_current
        latest_close = close[-1]
        position_size = capped_target_shares(
            risk_based_position_size,
            account_value,
            latest_close,
            max_position_fraction,
        )
        if np.isnan(momentum) or np.isnan(position_size):
            continue

        stock_results.append(
            {
                "symbol": symbol,
                "momentum": momentum,
                "shares": position_size,
                "annualised_return": annualised_return,
                "atr": atr_current,
            }
        )

    result_df = pd.DataFrame(stock_results)
    if result_df.empty:
        result_df = pd.DataFrame(
            columns=["raw_rank", "symbol", "momentum", "shares", "annualised_return", "atr"]
        )
        if save_path:
            result_df.to_csv(save_path, index=False)
            print(f"Momentum results saved to {save_path}")
        return result_df

    result_df = result_df.sort_values("momentum", ascending=False).reset_index(drop=True)
    result_df.insert(0, "raw_rank", np.arange(1, len(result_df) + 1))

    if save_path:
        result_df.to_csv(save_path, index=False)
        print(f"Momentum results saved to {save_path}")

    return result_df
