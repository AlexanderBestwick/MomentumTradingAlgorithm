from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
import datetime


def MarketIndicator(client, *, symbol="SPTM", lookback_days=250, as_of_date=None):
    if as_of_date is None:
        as_of_date = datetime.date.today()

    request_params = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=TimeFrame.Day,
        start=as_of_date - datetime.timedelta(days=lookback_days),
        end=as_of_date,
    )

    bars = client.get_stock_bars(request_params)
    df = bars.df
    if df.empty:
        raise ValueError(f"No market indicator data returned for {symbol} on or before {as_of_date}.")

    df['daily_average'] = (df['high'] + df['low'] + df['close']) / 3
    df['200_average'] = df['daily_average'].mean()
    avg200price = df['200_average'].iloc[-1]
    latest_close = df['close'].iloc[-1]

    if latest_close > avg200price:
        print("SPTM:", f"200 day average: {avg200price:.2f}")
        print("SPTM:", f"latest close: {latest_close:.2f}")
        return True
    else:
        print("SPTM:", f"200 day average: {avg200price:.2f}")
        print("SPTM:", f"latest close: {latest_close:.2f}")
        return False
