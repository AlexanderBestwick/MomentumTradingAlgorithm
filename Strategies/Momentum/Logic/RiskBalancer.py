from alpaca.data.requests import StockLatestTradeRequest
from alpaca.trading.requests import OrderRequest
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
import time
from Strategies.Momentum.Logic.PositionSizing import (
    capped_target_shares,
    max_position_shares,
    remaining_capacity_shares,
)


def sell_above_cap(trading_client, data_client, *, max_position_fraction=0.10, protected_symbols=None):
    protected_symbols = set(protected_symbols or [])
    current_positions = [p for p in trading_client.get_all_positions() if p.symbol not in protected_symbols]
    portfolio_value = float(trading_client.get_account().portfolio_value)

    trimmed = []
    for position in current_positions:
        sym = position.symbol
        current_qty = float(position.qty)
        try:
            price = data_client.get_stock_latest_trade(StockLatestTradeRequest(symbol_or_symbols=sym))[sym].price
            cap_shares = max_position_shares(portfolio_value, price, max_position_fraction)
            excess = current_qty - cap_shares
            if excess <= 1e-10:
                continue

            order = OrderRequest(
                symbol=sym,
                qty=excess,
                side=OrderSide.SELL,
                type=OrderType.MARKET,
                time_in_force=TimeInForce.DAY,
            )
            trading_client.submit_order(order)
            print(f"{sym} sold {excess:.2f} to respect the {max_position_fraction:.0%} cap")
            trimmed.append(sym)
        except Exception as exc:
            raise RuntimeError(f"Failed to trim {sym} back to the position cap") from exc

    return trimmed


def sell_overrisked(trading_client, momentum_df, threshold=2/3, protected_symbols=None):
    protected_symbols = set(protected_symbols or [])
    current_positions = [p for p in trading_client.get_all_positions() if p.symbol not in protected_symbols]
    current_shares = {p.symbol: float(p.qty) for p in current_positions}

    # Map current holdings to new ideal position sizes
    ideal_shares = {momentum_df.iloc[i]["symbol"]: momentum_df.iloc[i]["shares"] 
                    for i in range(len(momentum_df))}
    ideal_for_held = {symbol: ideal_shares.get(symbol, 0) for symbol in current_shares}

    # Find overrisked: new_ideal < threshold * current
    overrisked = {symbol: shares for symbol, shares in current_shares.items()
                  if ideal_for_held[symbol] < threshold * shares}
    
    print(f"Overrisked: {overrisked}")

    sold = []
    for sym, current_qty in overrisked.items():
        try:
            new_qty = ideal_for_held[sym]
            excess = current_qty - new_qty
            order = OrderRequest(
                symbol=sym,
                qty=excess,
                side=OrderSide.SELL,
                type=OrderType.MARKET,
                time_in_force=TimeInForce.DAY,
            )
            trading_client.submit_order(order)
            print(f"{sym} sold {excess:.2f} excess (overrisked)")
            sold.append(sym)
        except Exception as exc:
            raise RuntimeError(f"Failed to reduce overrisked position for {sym}") from exc

    return sold


def buy_underrisked(
    trading_client,
    data_client,
    momentum_df,
    market_health,
    threshold=3/2,
    cash_buffer=1,
    sleep_seconds=2,
    max_position_fraction=0.10,
    protected_symbols=None,
):
    if not market_health:
        print("Bad Markets: skipping risk-balance buys")
        return []

    protected_symbols = set(protected_symbols or [])
    current_positions = [p for p in trading_client.get_all_positions() if p.symbol not in protected_symbols]
    current_shares = {p.symbol: float(p.qty) for p in current_positions}

    # Map current holdings to new ideal position sizes
    ideal_shares = {momentum_df.iloc[i]["symbol"]: momentum_df.iloc[i]["shares"]
                    for i in range(len(momentum_df))}
    ideal_for_held = {symbol: ideal_shares.get(symbol, 0) for symbol in current_shares}

    # Find underrisked: new_ideal > threshold * current
    underrisked = {symbol: shares for symbol, shares in current_shares.items()
                   if ideal_for_held[symbol] > threshold * shares}
    
    print(f"Underrisked: {underrisked}")

    remaining_balance = float(trading_client.get_account().cash)
    bought = []

    for sym, current_qty in underrisked.items():
        try:
            new_qty = ideal_for_held[sym]

            price = data_client.get_stock_latest_trade(StockLatestTradeRequest(symbol_or_symbols=sym))[sym].price
            portfolio_value = float(trading_client.get_account().portfolio_value)
            capped_target_qty = capped_target_shares(
                new_qty,
                portfolio_value,
                price,
                max_position_fraction,
            )
            deficit = min(
                capped_target_qty - current_qty,
                remaining_capacity_shares(current_qty, portfolio_value, price, max_position_fraction),
            )
            if deficit <= 1e-10:
                continue

            cost_estimate = price * deficit

            print(
                f"{sym}, current: {current_qty:.2f}, ideal: {new_qty:.2f}, "
                f"capped target: {capped_target_qty:.2f}, deficit: {deficit:.2f}, cost est: {cost_estimate:.2f}"
            )

            if remaining_balance >= cost_estimate + cash_buffer:
                order = OrderRequest(
                    symbol=sym,
                    qty=deficit,
                    side=OrderSide.BUY,
                    type=OrderType.MARKET,
                    time_in_force=TimeInForce.DAY,
                )
                trading_client.submit_order(order)
                print(f"{sym} bought {deficit:.2f} to balance (underrisked)")
                remaining_balance = max(0.0, remaining_balance - cost_estimate)
                bought.append(sym)
            else:
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
                remaining_balance = float(trading_client.get_account().cash)
                if remaining_balance <= cash_buffer:
                    break

                notional = round(remaining_balance - cash_buffer, 2)
                if notional <= 0:
                    raise RuntimeError(f"Computed non-positive notional while balancing {sym}: {notional}")

                order = OrderRequest(
                    symbol=sym,
                    notional=notional,
                    side=OrderSide.BUY,
                    type=OrderType.MARKET,
                    time_in_force=TimeInForce.DAY,
                )
                trading_client.submit_order(order)
                print(f"{sym} bought {notional:.2f} USD to balance (underrisked, partial)")
                bought.append(sym)
                break
        except Exception as exc:
            raise RuntimeError(f"Failed to increase underrisked position for {sym}") from exc

    return bought
