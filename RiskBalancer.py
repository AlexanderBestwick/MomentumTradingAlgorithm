from alpaca.data.requests import StockLatestTradeRequest
from alpaca.trading.requests import OrderRequest
from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
import time


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

    return sold


def buy_underrisked(
    trading_client,
    data_client,
    momentum_df,
    market_health,
    threshold=3/2,
    cash_buffer=1,
    sleep_seconds=2,
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
        new_qty = ideal_for_held[sym]
        deficit = new_qty - current_qty

        price = data_client.get_stock_latest_trade(StockLatestTradeRequest(symbol_or_symbols=sym))[sym].price
        cost_estimate = price * deficit

        print(f"{sym}, current: {current_qty:.2f}, ideal: {new_qty:.2f}, deficit: {deficit:.2f}, cost est: {cost_estimate:.2f}")

        if remaining_balance >= cost_estimate + 2:
            order = OrderRequest(
                symbol=sym,
                qty=deficit,
                side=OrderSide.BUY,
                type=OrderType.MARKET,
                time_in_force=TimeInForce.DAY,
            )
            trading_client.submit_order(order)
            print(f"{sym} bought {deficit:.2f} to balance (underrisked)")
            remaining_balance -= cost_estimate
            bought.append(sym)
        else:
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
            remaining_balance = float(trading_client.get_account().cash)
            if remaining_balance <= cash_buffer:
                break

            notional = round(remaining_balance - cash_buffer, 2)
            if notional <= 0:
                print(f"Error: negative notional for {sym}")
                break

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

    return bought



