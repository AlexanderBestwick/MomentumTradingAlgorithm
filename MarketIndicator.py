from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
import datetime
from Functions.TradingDays import calendar_days_for_trading_window, trim_single_symbol_to_trailing_trading_days


def MarketIndicator(client, *, symbol="SPTM", lookback_days=200, as_of_date=None):
    if as_of_date is None:
        as_of_date = datetime.date.today()

    request_params = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Day,
        start=as_of_date - datetime.timedelta(days=calendar_days_for_trading_window(lookback_days)),
        end=as_of_date,
    )

    bars = client.get_stock_bars(request_params)
    df = bars.df
    if df.empty:
        raise ValueError(f"No market indicator data returned for {symbol} on or before {as_of_date}.")

    if "symbol" in df.index.names:
        df = df.droplevel("symbol")

    trailing_frame = trim_single_symbol_to_trailing_trading_days(df, lookback_days)
    if len(trailing_frame) < lookback_days:
        raise ValueError(
            f"Not enough trading-day data returned for {symbol}. "
            f"Needed {lookback_days} bars, received {len(trailing_frame)}."
        )

    avg200price = trailing_frame["close"].mean()
    latest_close = trailing_frame["close"].iloc[-1]

    if latest_close > avg200price:
        print("SPTM:", f"200 day average: {avg200price:.2f}")
        print("SPTM:", f"latest close: {latest_close:.2f}")
        return True
    else:
        print("SPTM:", f"200 day average: {avg200price:.2f}")
        print("SPTM:", f"latest close: {latest_close:.2f}")
        return False
