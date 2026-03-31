def max_position_shares(portfolio_value, price, max_position_fraction):
    portfolio_value = float(portfolio_value)
    price = float(price)
    max_position_fraction = float(max_position_fraction)

    if portfolio_value <= 0 or price <= 0 or max_position_fraction <= 0:
        return 0.0

    return (portfolio_value * max_position_fraction) / price


def remaining_capacity_shares(current_shares, portfolio_value, price, max_position_fraction):
    current_shares = float(current_shares)
    cap_shares = max_position_shares(portfolio_value, price, max_position_fraction)
    return max(0.0, cap_shares - current_shares)


def capped_target_shares(target_shares, portfolio_value, price, max_position_fraction):
    target_shares = float(target_shares)
    cap_shares = max_position_shares(portfolio_value, price, max_position_fraction)
    return min(target_shares, cap_shares)
