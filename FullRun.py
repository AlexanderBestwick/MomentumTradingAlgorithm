from datetime import date
from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
import pandas as pd
import Keys
from Functions.Is2ndWeek import second_week
import MarketIndicator
import ViableStockList
import LinearRegression
import PortfolioBalancer
import RiskBalancer

trading_client = TradingClient(Keys.Key_Test, Keys.Secret_Test)
data_client = StockHistoricalDataClient(Keys.Key_Test, Keys.Secret_Test)


def RunAll():
    print()
    # 1) Market health - required before new buys.
    market_health = MarketIndicator.MarketIndicator(data_client)
    print()

    # 2) Approved stock list from indicator
    approved_df = ViableStockList.GenerateStockList(data_client)
    print()

    # 3) Momentum ranking and risk sizing.
    momentum_df = LinearRegression.LinearRegression(trading_client, approved_df)

    # 4) Close unapproved positions first.
    target_symbols = set(momentum_df.head(60)["symbol"])
    closed = PortfolioBalancer.close_positions(trading_client, target_symbols)
    print()

    # 5) Extra check every 2nd Wednesday: rebalance risk on existing positions
    if second_week(date.today()):
        print("Risk Balancing in Progress: Do not interrupt")
        overrisked = RiskBalancer.sell_overrisked(trading_client, momentum_df)
        underrisked = RiskBalancer.buy_underrisked(trading_client, data_client, momentum_df, market_health)
        print()

    # 6) Open new positions if market is healthy.
    opened = PortfolioBalancer.open_positions(trading_client, data_client, momentum_df, market_health)
    print()


if __name__ == "__main__":
    RunAll()