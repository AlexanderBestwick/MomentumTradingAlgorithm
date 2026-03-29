from datetime import date, datetime, timezone
import sys
from alpaca.trading.client import TradingClient
from alpaca.data.historical import StockHistoricalDataClient
from Config import get_alpaca_credentials
from Functions.Is2ndWeek import second_week
from Functions.LiveRunSafety import (
    begin_live_run_record,
    ensure_market_is_open,
    finish_live_run_record,
    get_live_clock_info,
)
import MarketIndicator
import ViableStockList
import LinearRegression
import PortfolioBalancer
import RiskBalancer


def _run_step(step_name, func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as exc:
        raise RuntimeError(f"{step_name} failed") from exc


def _build_rank_lookup(full_ranked_universe):
    lookup = {}
    if full_ranked_universe is None or full_ranked_universe.empty:
        return lookup

    for index, row in enumerate(full_ranked_universe.itertuples(index=False), start=1):
        lookup[row.symbol] = {
            "raw_rank": index,
            "momentum": float(getattr(row, "momentum", 0.0) or 0.0),
            "annualised_return": float(getattr(row, "annualised_return", 0.0) or 0.0),
            "atr": float(getattr(row, "atr", 0.0) or 0.0),
            "target_shares": float(getattr(row, "shares", 0.0) or 0.0),
        }
    return lookup


def _build_action_details(
    *,
    category,
    side,
    symbols,
    rank_lookup,
    raw_rank_consideration_limit,
    approved_symbols,
    filtered_symbols,
    max_position_fraction,
    defensive_symbol,
):
    details = []
    for symbol in sorted(symbols):
        context = rank_lookup.get(symbol, {})
        rank = context.get("raw_rank")
        passes_filters = symbol in approved_symbols
        buy_eligible = symbol in filtered_symbols
        momentum = context.get("momentum")
        annualised_return = context.get("annualised_return")
        atr = context.get("atr")

        reason = ""
        if category == "closed":
            if rank is None:
                reason = "Left the ranked universe entirely for the current run."
            elif rank > raw_rank_consideration_limit and not passes_filters:
                reason = (
                    f"Fell to raw rank {rank}, outside the raw top {raw_rank_consideration_limit} ranked universe, "
                    "and failed the current buy filters."
                )
            elif rank > raw_rank_consideration_limit:
                reason = (
                    f"Fell to raw rank {rank}, outside the raw top {raw_rank_consideration_limit} ranked universe."
                )
            elif not passes_filters:
                reason = (
                    f"Remained inside the raw top {raw_rank_consideration_limit} ranked universe but failed the current buy filters, "
                    "so it was removed from the weekly hold list."
                )
            else:
                reason = "Left the weekly target list for the current run."
            if not passes_filters:
                reason += " This usually points to weaker liquidity, price, or volatility characteristics."
        elif category == "capped_sells":
            reason = f"Trimmed to keep the position within the {max_position_fraction:.0%} single-stock cap."
        elif category == "overrisked":
            reason = "Reduced during the second-Wednesday rebalance because the target size fell materially."
        elif category == "underrisked":
            reason = "Added during the second-Wednesday rebalance because the target size rose materially."
        elif category == "opened":
            reason = f"Opened from the filtered buy universe while remaining inside the raw top {raw_rank_consideration_limit} names."
        elif category == "defensive_buys":
            reason = f"Shifted reserve capital into {defensive_symbol} because the market-health filter was negative."
        else:
            reason = "Strategy action taken."

        if rank is not None and category in {"opened", "underrisked"}:
            reason += f" It was ranked #{rank} on the latest momentum screen."

        if rank is not None and category in {"overrisked", "closed"}:
            reason += f" Latest rank was #{rank}."

        if momentum is not None:
            reason += f" Momentum reading: {momentum:.2f}%."

        if annualised_return is not None:
            reason += f" Annualised return estimate: {annualised_return:.2f}%."

        if atr is not None and category in {"closed", "capped_sells", "overrisked"}:
            reason += f" ATR snapshot: {atr:.2f}."

        details.append(
            {
                "symbol": symbol,
                "category": category,
                "side": side,
                "reason": reason,
                "raw_rank": rank,
                "passes_filters": passes_filters,
                "buy_eligible": buy_eligible,
                "momentum": momentum,
                "annualised_return": annualised_return,
                "atr": atr,
                "target_shares": context.get("target_shares"),
            }
        )

    return details


def build_live_clients():
    credentials = get_alpaca_credentials()
    trading_client = TradingClient(credentials.key, credentials.secret, paper=credentials.paper)
    data_client = StockHistoricalDataClient(credentials.key, credentials.secret)
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
    max_position_fraction=0.10,
    enforce_live_safeguards=True,
):
    if trading_client is None or data_client is None:
        trading_client, data_client = build_live_clients()

    is_backtest = bool(getattr(trading_client, "is_backtest", False) or getattr(data_client, "is_backtest", False))
    live_run_record_path = None
    live_clock_info = None

    if not is_backtest and enforce_live_safeguards:
        live_clock_info = get_live_clock_info(trading_client)
        ensure_market_is_open(live_clock_info)

        if run_date is None:
            run_date = live_clock_info.market_date
        elif run_date != live_clock_info.market_date:
            raise RuntimeError(
                f"Requested run_date {run_date.isoformat()} does not match the live market date "
                f"{live_clock_info.market_date.isoformat()} from Alpaca clock."
            )

        live_run_record_path = begin_live_run_record(
            run_date,
            live_clock_info=live_clock_info,
        )
    elif run_date is None:
        run_date = date.today()

    approved_save_path = "Data/ApprovedStockFrame.csv" if save_outputs else None
    momentum_save_path = "Data/MomentumResults.csv" if save_outputs else None
    sleep_seconds = 0 if is_backtest else 2

    try:
        print()
        # 1) Market health - required before new buys.
        market_health = _run_step(
            f"Market health check for {run_date.isoformat()}",
            MarketIndicator.MarketIndicator,
            data_client,
            as_of_date=run_date,
        )
        print()

        # 2) Build full universe once, then keep the existing filters unchanged.
        selection_universe = _run_step(
            f"Universe selection for {run_date.isoformat()}",
            ViableStockList.BuildSelectionUniverse,
            data_client,
            as_of_date=run_date,
            save_path=approved_save_path,
        )
        approved_df = selection_universe["approved_stock_df"]
        approved_symbols = selection_universe["approved_symbols"]
        print()

        # 3) Rank the full index first.
        full_ranked_universe = _run_step(
            f"Momentum ranking for {run_date.isoformat()}",
            LinearRegression.LinearRegression,
            trading_client,
            selection_universe["full_stock_df"],
            save_path=momentum_save_path,
            max_position_fraction=max_position_fraction,
        )

        # 4) Build one raw ranked window for context and one filtered universe for
        # actual weekly hold/buy decisions.
        raw_ranked_universe = full_ranked_universe.head(raw_rank_consideration_limit).reset_index(drop=True)
        filtered_universe = (
            raw_ranked_universe.loc[lambda df: df["symbol"].isin(approved_symbols)]
            .reset_index(drop=True)
        )

        print(f"{len(full_ranked_universe)} stocks in full ranked universe")
        print(f"{len(filtered_universe)} stocks in filtered weekly universe")

        approved_symbol_set = set(approved_symbols)
        filtered_symbol_set = set(filtered_universe["symbol"]) if not filtered_universe.empty else set()
        rank_lookup = _build_rank_lookup(full_ranked_universe)

        protected_symbols = (
            {defensive_symbol}
            if defensive_mode == "treasury_bonds" and not market_health
            else set()
        )

        # 5) Weekly sell discipline uses the filtered universe, so names that fail
        # the volatility or moving-average filters are removed even if they still
        # sit inside the raw top-ranked cutoff.
        target_symbols = filtered_symbol_set
        closed = _run_step(
            f"Position close step for {run_date.isoformat()}",
            PortfolioBalancer.close_positions,
            trading_client,
            target_symbols,
            protected_symbols=protected_symbols,
        )
        capped_sells = _run_step(
            f"Position cap enforcement step for {run_date.isoformat()}",
            RiskBalancer.sell_above_cap,
            trading_client,
            data_client,
            max_position_fraction=max_position_fraction,
            protected_symbols=protected_symbols,
        )
        print()

        # 6) Extra check every 2nd Wednesday: use the same filtered universe for
        # rebalance sizing, but ignore anything already closed earlier in this run
        # so we do not submit duplicate sell orders.
        rebalance_protected_symbols = protected_symbols | set(closed)
        if second_week(run_date):
            print("Risk Balancing in Progress: Do not interrupt")
            overrisked = _run_step(
                f"Risk reduction step for {run_date.isoformat()}",
                RiskBalancer.sell_overrisked,
                trading_client,
                filtered_universe,
                protected_symbols=rebalance_protected_symbols,
            )
            if market_health:
                underrisked = _run_step(
                    f"Risk rebalance buy step for {run_date.isoformat()}",
                    RiskBalancer.buy_underrisked,
                    trading_client,
                    data_client,
                    filtered_universe,
                    market_health,
                    sleep_seconds=sleep_seconds,
                    max_position_fraction=max_position_fraction,
                    protected_symbols=rebalance_protected_symbols,
                )
            else:
                print("Bad Markets: sell-only mode during risk rebalance")
                underrisked = []
            print()
        else:
            overrisked = []
            underrisked = []

        # 7) Buy only from the filtered weekly universe.
        if market_health:
            opened = _run_step(
                f"New position opening step for {run_date.isoformat()}",
                PortfolioBalancer.open_positions,
                trading_client,
                data_client,
                filtered_universe,
                market_health,
                top_n=raw_rank_consideration_limit,
                sleep_seconds=sleep_seconds,
                max_position_fraction=max_position_fraction,
            )
        else:
            print("Bad Markets: sell-only mode, no new positions opened")
            opened = []

        defensive_buys = _run_step(
            f"Defensive allocation step for {run_date.isoformat()}",
            PortfolioBalancer.allocate_defensive_position,
            trading_client,
            data_client,
            market_health,
            mode=defensive_mode,
            defensive_symbol=defensive_symbol,
        )
        print()

        result = {
            "run_date": run_date,
            "market_health": market_health,
            "approved_count": len(approved_df.index.get_level_values("symbol").unique()) if not approved_df.empty else 0,
            "full_ranked_universe": full_ranked_universe,
            "raw_ranked_universe": raw_ranked_universe,
            "filtered_universe": filtered_universe,
            "raw_sell_universe": raw_ranked_universe,
            "raw_buy_universe": raw_ranked_universe,
            "filtered_buy_universe": filtered_universe,
            "closed": closed,
            "capped_sells": capped_sells,
            "overrisked": overrisked,
            "underrisked": underrisked,
            "opened": opened,
            "defensive_buys": defensive_buys,
            "defensive_mode": defensive_mode,
            "defensive_symbol": defensive_symbol,
            "raw_rank_consideration_limit": raw_rank_consideration_limit,
            "max_position_fraction": max_position_fraction,
            "is_risk_rebalance_day": second_week(run_date),
            "action_details": (
                _build_action_details(
                    category="closed",
                    side="sell",
                    symbols=closed,
                    rank_lookup=rank_lookup,
                    raw_rank_consideration_limit=raw_rank_consideration_limit,
                    approved_symbols=approved_symbol_set,
                    filtered_symbols=filtered_symbol_set,
                    max_position_fraction=max_position_fraction,
                    defensive_symbol=defensive_symbol,
                )
                + _build_action_details(
                    category="capped_sells",
                    side="sell",
                    symbols=capped_sells,
                    rank_lookup=rank_lookup,
                    raw_rank_consideration_limit=raw_rank_consideration_limit,
                    approved_symbols=approved_symbol_set,
                    filtered_symbols=filtered_symbol_set,
                    max_position_fraction=max_position_fraction,
                    defensive_symbol=defensive_symbol,
                )
                + _build_action_details(
                    category="overrisked",
                    side="sell",
                    symbols=overrisked,
                    rank_lookup=rank_lookup,
                    raw_rank_consideration_limit=raw_rank_consideration_limit,
                    approved_symbols=approved_symbol_set,
                    filtered_symbols=filtered_symbol_set,
                    max_position_fraction=max_position_fraction,
                    defensive_symbol=defensive_symbol,
                )
                + _build_action_details(
                    category="underrisked",
                    side="buy",
                    symbols=underrisked,
                    rank_lookup=rank_lookup,
                    raw_rank_consideration_limit=raw_rank_consideration_limit,
                    approved_symbols=approved_symbol_set,
                    filtered_symbols=filtered_symbol_set,
                    max_position_fraction=max_position_fraction,
                    defensive_symbol=defensive_symbol,
                )
                + _build_action_details(
                    category="opened",
                    side="buy",
                    symbols=opened,
                    rank_lookup=rank_lookup,
                    raw_rank_consideration_limit=raw_rank_consideration_limit,
                    approved_symbols=approved_symbol_set,
                    filtered_symbols=filtered_symbol_set,
                    max_position_fraction=max_position_fraction,
                    defensive_symbol=defensive_symbol,
                )
                + _build_action_details(
                    category="defensive_buys",
                    side="buy",
                    symbols=defensive_buys,
                    rank_lookup=rank_lookup,
                    raw_rank_consideration_limit=raw_rank_consideration_limit,
                    approved_symbols=approved_symbol_set,
                    filtered_symbols=filtered_symbol_set,
                    max_position_fraction=max_position_fraction,
                    defensive_symbol=defensive_symbol,
                )
            ),
            "momentum": filtered_universe,
        }

        if live_run_record_path is not None:
            finish_live_run_record(
                live_run_record_path,
                status="completed",
                updated_at=datetime.now(timezone.utc).isoformat(),
                summary={
                    "market_health": market_health,
                    "approved_count": result["approved_count"],
                    "opened_count": len(opened),
                    "closed_count": len(closed),
                    "overrisked_count": len(overrisked),
                    "underrisked_count": len(underrisked),
                    "capped_sells_count": len(capped_sells),
                    "defensive_buy_count": len(defensive_buys),
                },
            )

        return result
    except Exception as exc:
        if live_run_record_path is not None:
            finish_live_run_record(
                live_run_record_path,
                status="failed",
                updated_at=datetime.now(timezone.utc).isoformat(),
                detail=str(exc),
            )
        raise


if __name__ == "__main__":
    try:
        RunAll()
    except Exception as exc:
        print(f"Strategy run failed: {exc}", file=sys.stderr)
        raise
