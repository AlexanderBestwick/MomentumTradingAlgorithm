from datetime import date
from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
import Keys
from Functions.Is2ndWeek import second_week
import MarketIndicator
import ViableStockList
import LinearRegression
import PortfolioBalancer
import RiskBalancer


def build_live_clients():
    trading_client = TradingClient(Keys.Key_Test, Keys.Secret_Test)
    data_client = StockHistoricalDataClient(Keys.Key_Test, Keys.Secret_Test)
    return trading_client, data_client


def RunAll(
    trading_client=None,
    data_client=None,
    *,
    run_date=None,
    save_outputs=True,
    defensive_mode="cash",
    defensive_symbol="SGOV",
    raw_rank_consideration_limit=80,
):
    if trading_client is None or data_client is None:
        trading_client, data_client = build_live_clients()

    if run_date is None:
        run_date = date.today()

    is_backtest = bool(getattr(trading_client, "is_backtest", False) or getattr(data_client, "is_backtest", False))
    approved_save_path = "Data/ApprovedStockFrame.csv" if save_outputs else None
    momentum_save_path = "Data/MomentumResults.csv" if save_outputs else None
    sleep_seconds = 0 if is_backtest else 2

    print()
    # 1) Market health - required before new buys.
    market_health = MarketIndicator.MarketIndicator(data_client, as_of_date=run_date)
    print()

    # 2) Build full universe once, then keep the existing filters unchanged.
    selection_universe = ViableStockList.BuildSelectionUniverse(
        data_client,
        as_of_date=run_date,
        save_path=approved_save_path,
    )
    approved_df = selection_universe["approved_stock_df"]
    approved_symbols = selection_universe["approved_symbols"]
    print()

    # 3) Rank the full index first.
    full_ranked_universe = LinearRegression.LinearRegression(
        trading_client,
        selection_universe["full_stock_df"],
        save_path=momentum_save_path,
    )

    # 4) Use one shared raw-rank cutoff for both sell and buy consideration.
    raw_sell_universe = full_ranked_universe.head(raw_rank_consideration_limit).reset_index(drop=True)
    raw_buy_universe = full_ranked_universe.head(raw_rank_consideration_limit).reset_index(drop=True)
    filtered_buy_universe = (
        raw_buy_universe.loc[lambda df: df["symbol"].isin(approved_symbols)]
        .reset_index(drop=True)
    )

    print(f"{len(full_ranked_universe)} stocks in full ranked universe")
    print(f"{len(filtered_buy_universe)} stocks in filtered buy universe")

    protected_symbols = (
        {defensive_symbol}
        if defensive_mode == "treasury_bonds" and not market_health
        else set()
    )

    # 5) Sell only when a held stock falls outside the shared raw-rank cutoff.
    target_symbols = set(raw_sell_universe["symbol"])
    closed = PortfolioBalancer.close_positions(
        trading_client,
        target_symbols,
        protected_symbols=protected_symbols,
    )
    print()

    # 6) Extra check every 2nd Wednesday: sell sizing uses raw ranking, buy sizing uses filtered buy universe.
    if second_week(run_date):
        print("Risk Balancing in Progress: Do not interrupt")
        overrisked = RiskBalancer.sell_overrisked(
            trading_client,
            raw_sell_universe,
            protected_symbols=protected_symbols,
        )
        if market_health:
            underrisked = RiskBalancer.buy_underrisked(
                trading_client,
                data_client,
                filtered_buy_universe,
                market_health,
                sleep_seconds=sleep_seconds,
                protected_symbols=protected_symbols,
            )
        else:
            print("Bad Markets: sell-only mode during risk rebalance")
            underrisked = []
        print()
    else:
        overrisked = []
        underrisked = []

    # 7) Buy only from the filtered subset of the shared raw-rank cutoff.
    if market_health:
        opened = PortfolioBalancer.open_positions(
            trading_client,
            data_client,
            filtered_buy_universe,
            market_health,
            top_n=raw_rank_consideration_limit,
            sleep_seconds=sleep_seconds,
        )
    else:
        print("Bad Markets: sell-only mode, no new positions opened")
        opened = []

    defensive_buys = PortfolioBalancer.allocate_defensive_position(
        trading_client,
        data_client,
        market_health,
        mode=defensive_mode,
        defensive_symbol=defensive_symbol,
    )
    print()

    return {
        "run_date": run_date,
        "market_health": market_health,
        "approved_count": len(approved_df.index.get_level_values("symbol").unique()) if not approved_df.empty else 0,
        "full_ranked_universe": full_ranked_universe,
        "raw_sell_universe": raw_sell_universe,
        "raw_buy_universe": raw_buy_universe,
        "filtered_buy_universe": filtered_buy_universe,
        "closed": closed,
        "overrisked": overrisked,
        "underrisked": underrisked,
        "opened": opened,
        "defensive_buys": defensive_buys,
        "defensive_mode": defensive_mode,
        "defensive_symbol": defensive_symbol,
        "raw_rank_consideration_limit": raw_rank_consideration_limit,
        "momentum": filtered_buy_universe,
    }


if __name__ == "__main__":
    RunAll()
