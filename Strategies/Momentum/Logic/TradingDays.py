import math


def calendar_days_for_trading_window(trading_days, *, extra_buffer_days=30):
    trading_days = int(trading_days)
    if trading_days <= 0:
        return 0
    return int(math.ceil(trading_days * 7 / 5)) + int(extra_buffer_days)


def trim_multiindex_to_trailing_trading_days(frame, trading_days):
    if frame is None or frame.empty:
        return frame.copy()
    return (
        frame.sort_index()
        .groupby(level="symbol", group_keys=False)
        .tail(int(trading_days))
        .copy()
    )


def trim_single_symbol_to_trailing_trading_days(frame, trading_days):
    if frame is None or frame.empty:
        return frame.copy()
    return frame.sort_index().tail(int(trading_days)).copy()
