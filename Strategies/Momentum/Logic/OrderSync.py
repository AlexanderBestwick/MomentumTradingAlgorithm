import time


_TERMINAL_ORDER_STATUSES = {
    "filled",
    "done_for_day",
    "canceled",
    "expired",
    "replaced",
    "rejected",
    "suspended",
    "stopped",
    "calculated",
}


def _normalized_order_status(order):
    status = getattr(order, "status", "")
    return getattr(status, "value", str(status)).lower()


def wait_for_market_order_completion(
    trading_client,
    submitted_order,
    *,
    timeout_seconds=120.0,
    poll_seconds=0.5,
):
    if getattr(trading_client, "is_backtest", False):
        return submitted_order

    order_id = getattr(submitted_order, "id", None)
    if order_id is None:
        raise RuntimeError("Submitted live order did not return an order id.")

    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
    poll_seconds = max(0.1, float(poll_seconds))

    while True:
        order = trading_client.get_order_by_id(order_id)
        status = _normalized_order_status(order)

        if status == "filled":
            return order

        if status in _TERMINAL_ORDER_STATUSES:
            filled_qty = getattr(order, "filled_qty", None)
            symbol = getattr(order, "symbol", getattr(submitted_order, "symbol", "unknown"))
            raise RuntimeError(
                f"Live market order for {symbol} ended with status={status}, filled_qty={filled_qty}."
            )

        if time.monotonic() >= deadline:
            symbol = getattr(order, "symbol", getattr(submitted_order, "symbol", "unknown"))
            raise RuntimeError(
                f"Timed out waiting for live market order in {symbol} to fill. Last status={status}."
            )

        time.sleep(poll_seconds)
