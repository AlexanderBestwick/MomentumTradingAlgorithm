from alpaca.data.requests import StockLatestTradeRequest
from alpaca.trading.requests import OrderRequest
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
import time
from Strategies.Momentum.Logic.PositionSizing import capped_target_shares


def close_positions(trading_client, target_symbols, protected_symbols=None):
    """Sell any position not in the target list."""
    protected_symbols = set(protected_symbols or [])
    current_shares = {p.symbol: float(p.qty) for p in trading_client.get_all_positions()}
    held_symbols = set(current_shares)
    to_sell = held_symbols - set(target_symbols) - protected_symbols

    for sym in sorted(to_sell):
        try:
            trading_client.close_position(sym)
            print(f"{sym} sold")
        except Exception as exc:
            raise RuntimeError(f"Failed to close position for {sym}") from exc

    return to_sell


def open_positions(
    trading_client,
    data_client,
    momentum_df,
    market_health,
    top_n=80,
    cash_buffer=1,
    sleep_seconds=2,
    max_position_fraction=0.10,
):
    """Open new positions from momentum list (only in healthy markets)."""
    if not market_health:
        print("Bad Markets: skipping new buys")
        print()
        return []

    target_symbols = set(momentum_df.head(top_n)["symbol"])
    current_shares = {p.symbol: float(p.qty) for p in trading_client.get_all_positions()}
    held_symbols = set(current_shares)
    to_buy = target_symbols - held_symbols

    remaining_balance = float(trading_client.get_account().cash)
    buys = []

    for row in momentum_df.head(top_n).itertuples(index=False):
        sym = row.symbol
        if sym not in to_buy:
            continue

        try:
            price = data_client.get_stock_latest_trade(StockLatestTradeRequest(symbol_or_symbols=sym))[sym].price
            portfolio_value = float(trading_client.get_account().portfolio_value)
            shares = capped_target_shares(
                row.shares,
                portfolio_value,
                price,
                max_position_fraction,
            )
            if shares <= 0:
                continue

            cost_estimate = price * shares

            if remaining_balance >= cost_estimate + 2:
                order = OrderRequest(
                    symbol=sym,
                    qty=shares,
                    side=OrderSide.BUY,
                    type=OrderType.MARKET,
                    time_in_force=TimeInForce.DAY,
                )
                trading_client.submit_order(order)
                print(f"Bought {shares:.2f} {sym}, cost estimate {cost_estimate:.2f}")
                remaining_balance -= cost_estimate
                buys.append(sym)
            else:
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
                remaining_balance = float(trading_client.get_account().cash)
                if remaining_balance <= cash_buffer:
                    break

                notional = round(remaining_balance - cash_buffer, 2)
                if notional <= 0:
                    raise RuntimeError(f"Computed non-positive notional while opening {sym}: {notional}")

                order = OrderRequest(
                    symbol=sym,
                    notional=notional,
                    side=OrderSide.BUY,
                    type=OrderType.MARKET,
                    time_in_force=TimeInForce.DAY,
                )
                trading_client.submit_order(order)
                print(f"Bought {notional:.2f} USD worth of {sym}")
                buys.append(sym)
                break
        except Exception as exc:
            raise RuntimeError(f"Failed to open position for {sym}") from exc

    return buys


def allocate_defensive_position(
    trading_client,
    data_client,
    market_health,
    *,
    mode="cash",
    defensive_symbol="SGOV",
    cash_buffer=1,
):
    if market_health or mode != "treasury_bonds":
        return []

    remaining_balance = float(trading_client.get_account().cash)
    if remaining_balance <= cash_buffer:
        print(f"No idle cash available for defensive allocation into {defensive_symbol}")
        return []

    notional = round(remaining_balance - cash_buffer, 2)
    if notional <= 0:
        print(f"Skipping defensive allocation for {defensive_symbol}: notional too small")
        return []

    try:
        order = OrderRequest(
            symbol=defensive_symbol,
            notional=notional,
            side=OrderSide.BUY,
            type=OrderType.MARKET,
            time_in_force=TimeInForce.DAY,
        )
        trading_client.submit_order(order)
        print(f"Allocated {notional:.2f} USD to defensive Treasury holding {defensive_symbol}")
        return [defensive_symbol]
    except Exception as exc:
        raise RuntimeError(f"Failed to allocate defensive position in {defensive_symbol}") from exc
