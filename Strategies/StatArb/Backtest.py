from dataclasses import dataclass

import pandas as pd

from Strategies.StatArb.Pairs import select_candidate_pairs
from Strategies.StatArb.Signals import calculate_pair_signal
from Strategies.StatArb.Universe import prepare_stat_arb_universe


@dataclass
class PairPosition:
    pair_id: str
    symbol_a: str
    symbol_b: str
    hedge_ratio: float
    qty_a: float
    qty_b: float
    entry_date: object
    entry_zscore: float
    direction: str
    classification_bucket: str
    sector: str
    industry: str
    holding_days: int = 0


@dataclass(frozen=True)
class StatArbBacktestResult:
    results: pd.DataFrame
    selected_pairs: pd.DataFrame
    candidate_pairs: pd.DataFrame
    trades: pd.DataFrame
    universe: pd.DataFrame
    rejections: pd.DataFrame
    summary: dict


def _normalize_bars(bars_df):
    normalized = bars_df.reset_index().copy()
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"]).dt.tz_localize(None)
    normalized["date"] = normalized["timestamp"].dt.date
    normalized = normalized.sort_values(["symbol", "date", "timestamp"])
    normalized = normalized.drop_duplicates(subset=["symbol", "date"], keep="last")
    normalized = normalized.set_index(["symbol", "date"]).sort_index()
    return normalized.loc[:, ["open", "high", "low", "close", "volume"]]


def _trade_fee(notional, backtest_config):
    if notional <= 0:
        return 0.0
    return float(backtest_config.trade_fee_flat) + (float(notional) * float(backtest_config.trade_fee_rate))


def _execution_price(price, side, backtest_config):
    slippage = float(backtest_config.slippage_bps) / 10000.0
    if side in {"buy", "cover"}:
        return float(price) * (1.0 + slippage)
    return float(price) * (1.0 - slippage)


def _get_day_frame(symbol_frames, symbol, trade_date):
    frame = symbol_frames.get(symbol)
    if frame is None:
        return None
    try:
        return frame.loc[trade_date]
    except KeyError:
        return None


def _get_price_on_or_before(symbol_frames, symbol, trade_date, field="close"):
    frame = symbol_frames.get(symbol)
    if frame is None:
        return None
    history = frame.loc[:trade_date, field]
    if history.empty:
        return None
    return float(history.iloc[-1])


def _portfolio_market_value(positions, close_prices):
    total = 0.0
    for position in positions.values():
        total += position.qty_a * float(close_prices[position.symbol_a])
        total += position.qty_b * float(close_prices[position.symbol_b])
    return total


def _gross_exposure(positions, close_prices):
    total = 0.0
    for position in positions.values():
        total += abs(position.qty_a) * float(close_prices[position.symbol_a])
        total += abs(position.qty_b) * float(close_prices[position.symbol_b])
    return total


def _net_exposure(positions, close_prices):
    total = 0.0
    for position in positions.values():
        total += position.qty_a * float(close_prices[position.symbol_a])
        total += position.qty_b * float(close_prices[position.symbol_b])
    return total


def _borrow_fees_lookup(eligible_universe, default_borrow_fee_bps):
    lookup = {}
    for row in eligible_universe.itertuples(index=False):
        borrow_fee_bps = float(row.borrow_fee_bps) if row.borrow_fee_bps else float(default_borrow_fee_bps)
        if borrow_fee_bps <= 0:
            borrow_fee_bps = float(default_borrow_fee_bps)
        lookup[row.symbol] = borrow_fee_bps
    return lookup


def _position_signal(position, symbol_frames, history_end_date, lookback_days):
    history_a = symbol_frames[position.symbol_a].loc[:history_end_date, "close"]
    history_b = symbol_frames[position.symbol_b].loc[:history_end_date, "close"]
    return calculate_pair_signal(
        history_a,
        history_b,
        hedge_ratio=position.hedge_ratio,
        lookback_days=lookback_days,
    )


def _candidate_signal(candidate_row, symbol_frames, history_end_date, lookback_days):
    history_a = symbol_frames[candidate_row.symbol_a].loc[:history_end_date, "close"]
    history_b = symbol_frames[candidate_row.symbol_b].loc[:history_end_date, "close"]
    return calculate_pair_signal(
        history_a,
        history_b,
        hedge_ratio=float(candidate_row.hedge_ratio),
        lookback_days=lookback_days,
    )


def _close_position(
    *,
    position,
    trade_date,
    day_a,
    day_b,
    backtest_config,
    cash,
    signal_zscore,
    reason,
):
    trade_records = []
    total_fees = 0.0

    if position.qty_a > 0:
        exec_price_a = _execution_price(day_a["open"], "sell", backtest_config)
        notional_a = position.qty_a * exec_price_a
        fee_a = _trade_fee(notional_a, backtest_config)
        cash += notional_a - fee_a
        total_fees += fee_a
        trade_records.append(
            {
                "date": trade_date,
                "pair_id": position.pair_id,
                "symbol": position.symbol_a,
                "side": "sell",
                "qty": position.qty_a,
                "price": exec_price_a,
                "notional": notional_a,
                "fee": fee_a,
                "reason": reason,
                "signal_zscore": signal_zscore,
            }
        )
    elif position.qty_a < 0:
        cover_qty = abs(position.qty_a)
        exec_price_a = _execution_price(day_a["open"], "cover", backtest_config)
        notional_a = cover_qty * exec_price_a
        fee_a = _trade_fee(notional_a, backtest_config)
        cash -= notional_a + fee_a
        total_fees += fee_a
        trade_records.append(
            {
                "date": trade_date,
                "pair_id": position.pair_id,
                "symbol": position.symbol_a,
                "side": "cover",
                "qty": cover_qty,
                "price": exec_price_a,
                "notional": notional_a,
                "fee": fee_a,
                "reason": reason,
                "signal_zscore": signal_zscore,
            }
        )

    if position.qty_b > 0:
        exec_price_b = _execution_price(day_b["open"], "sell", backtest_config)
        notional_b = position.qty_b * exec_price_b
        fee_b = _trade_fee(notional_b, backtest_config)
        cash += notional_b - fee_b
        total_fees += fee_b
        trade_records.append(
            {
                "date": trade_date,
                "pair_id": position.pair_id,
                "symbol": position.symbol_b,
                "side": "sell",
                "qty": position.qty_b,
                "price": exec_price_b,
                "notional": notional_b,
                "fee": fee_b,
                "reason": reason,
                "signal_zscore": signal_zscore,
            }
        )
    elif position.qty_b < 0:
        cover_qty = abs(position.qty_b)
        exec_price_b = _execution_price(day_b["open"], "cover", backtest_config)
        notional_b = cover_qty * exec_price_b
        fee_b = _trade_fee(notional_b, backtest_config)
        cash -= notional_b + fee_b
        total_fees += fee_b
        trade_records.append(
            {
                "date": trade_date,
                "pair_id": position.pair_id,
                "symbol": position.symbol_b,
                "side": "cover",
                "qty": cover_qty,
                "price": exec_price_b,
                "notional": notional_b,
                "fee": fee_b,
                "reason": reason,
                "signal_zscore": signal_zscore,
            }
        )

    return cash, total_fees, trade_records


def run_stat_arb_backtest(bars_df, stat_arb_config, *, start_date, end_date):
    normalized_bars = _normalize_bars(bars_df)
    symbol_frames = {
        symbol: frame.droplevel("symbol").sort_index()
        for symbol, frame in normalized_bars.groupby(level="symbol")
    }
    trading_dates = sorted(
        trade_date
        for trade_date in normalized_bars.index.get_level_values("date").unique()
        if start_date <= trade_date <= end_date
    )

    if len(trading_dates) < 2:
        raise ValueError("Not enough trading dates available for the requested stat-arb backtest window.")

    warmup_days = max(
        int(stat_arb_config.universe.min_history_days),
        int(stat_arb_config.pairs.formation_window_days),
        int(stat_arb_config.pairs.zscore_lookback_days) + 1,
    )

    cash = float(stat_arb_config.backtest.initial_capital)
    positions = {}
    trade_records = []
    results_records = []
    selected_pair_records = []
    candidate_pair_records = []
    latest_universe = pd.DataFrame()
    latest_rejections = pd.DataFrame()
    current_selected_pairs = pd.DataFrame()
    current_selection_period = None

    for date_index, trade_date in enumerate(trading_dates):
        if date_index == 0:
            continue

        history_end_date = trading_dates[date_index - 1]

        historical_bars = normalized_bars.loc[
            normalized_bars.index.get_level_values("date") <= history_end_date
        ]
        historical_bars = (
            historical_bars.sort_index()
            .groupby(level="symbol", group_keys=False)
            .tail(warmup_days)
        )

        if historical_bars.empty or historical_bars.groupby(level="symbol").size().max() < warmup_days:
            continue

        selection_period = (trade_date.year, trade_date.month)
        should_reselect = (
            current_selection_period is None
            or stat_arb_config.backtest.reselection_frequency == "monthly"
            and selection_period != current_selection_period
        )

        if should_reselect:
            prepared_universe = prepare_stat_arb_universe(historical_bars, stat_arb_config.universe)
            latest_universe = prepared_universe.eligible
            latest_rejections = prepared_universe.rejected
            selection = select_candidate_pairs(
                historical_bars,
                prepared_universe.eligible,
                stat_arb_config.pairs,
            )
            current_selected_pairs = selection.selected_pairs
            current_selection_period = selection_period

            if not selection.candidate_pairs.empty:
                selection_candidates = selection.candidate_pairs.copy()
                selection_candidates.insert(0, "selected_on", trade_date.isoformat())
                candidate_pair_records.append(selection_candidates)

            if not selection.selected_pairs.empty:
                selection_pairs = selection.selected_pairs.copy()
                selection_pairs.insert(0, "selected_on", trade_date.isoformat())
                selected_pair_records.append(selection_pairs)

            if stat_arb_config.backtest.close_deselected_pairs and positions:
                selected_pair_ids = set(current_selected_pairs["pair_id"]) if not current_selected_pairs.empty else set()
                for pair_id in list(positions):
                    if pair_id in selected_pair_ids:
                        continue
                    position = positions[pair_id]
                    day_a = _get_day_frame(symbol_frames, position.symbol_a, trade_date)
                    day_b = _get_day_frame(symbol_frames, position.symbol_b, trade_date)
                    if day_a is None or day_b is None:
                        continue
                    cash, _, closed_trades = _close_position(
                        position=position,
                        trade_date=trade_date,
                        day_a=day_a,
                        day_b=day_b,
                        backtest_config=stat_arb_config.backtest,
                        cash=cash,
                        signal_zscore=None,
                        reason="deselected",
                    )
                    trade_records.extend(closed_trades)
                    del positions[pair_id]

        for pair_id in list(positions):
            position = positions[pair_id]
            signal = _position_signal(
                position,
                symbol_frames,
                history_end_date,
                stat_arb_config.pairs.zscore_lookback_days,
            )
            if signal is None:
                continue

            position.holding_days += 1
            should_close = False
            close_reason = None
            if abs(signal.zscore) <= float(stat_arb_config.signals.exit_zscore):
                should_close = True
                close_reason = "mean_reversion_exit"
            elif abs(signal.zscore) >= float(stat_arb_config.signals.stop_zscore):
                should_close = True
                close_reason = "stop_zscore"
            elif position.holding_days >= int(stat_arb_config.signals.max_holding_days):
                should_close = True
                close_reason = "max_holding_days"

            if not should_close:
                continue

            day_a = _get_day_frame(symbol_frames, position.symbol_a, trade_date)
            day_b = _get_day_frame(symbol_frames, position.symbol_b, trade_date)
            if day_a is None or day_b is None:
                continue

            cash, _, closed_trades = _close_position(
                position=position,
                trade_date=trade_date,
                day_a=day_a,
                day_b=day_b,
                backtest_config=stat_arb_config.backtest,
                cash=cash,
                signal_zscore=signal.zscore,
                reason=close_reason,
            )
            trade_records.extend(closed_trades)
            del positions[pair_id]

        close_prices = {}
        for position in positions.values():
            price_a = _get_price_on_or_before(symbol_frames, position.symbol_a, trade_date, "close")
            price_b = _get_price_on_or_before(symbol_frames, position.symbol_b, trade_date, "close")
            if price_a is not None and price_b is not None:
                close_prices[position.symbol_a] = price_a
                close_prices[position.symbol_b] = price_b

        current_equity = cash + _portfolio_market_value(positions, close_prices)
        current_gross = _gross_exposure(positions, close_prices)
        remaining_capacity = max(0.0, (float(stat_arb_config.backtest.max_gross_leverage) * current_equity) - current_gross)
        open_symbols = {symbol for position in positions.values() for symbol in (position.symbol_a, position.symbol_b)}

        if not current_selected_pairs.empty and len(positions) < int(stat_arb_config.backtest.max_open_pairs):
            open_slots = int(stat_arb_config.backtest.max_open_pairs) - len(positions)
            for candidate in current_selected_pairs.itertuples(index=False):
                if candidate.pair_id in positions:
                    continue
                if candidate.symbol_a in open_symbols or candidate.symbol_b in open_symbols:
                    continue
                if remaining_capacity <= 0:
                    break

                signal = _candidate_signal(
                    candidate,
                    symbol_frames,
                    history_end_date,
                    stat_arb_config.pairs.zscore_lookback_days,
                )
                if signal is None:
                    continue
                if abs(signal.zscore) < float(stat_arb_config.signals.entry_zscore):
                    continue
                if abs(signal.zscore) >= float(stat_arb_config.signals.stop_zscore):
                    continue

                day_a = _get_day_frame(symbol_frames, candidate.symbol_a, trade_date)
                day_b = _get_day_frame(symbol_frames, candidate.symbol_b, trade_date)
                if day_a is None or day_b is None:
                    continue

                pair_gross_target = min(
                    float(stat_arb_config.backtest.max_pair_gross_fraction) * current_equity,
                    remaining_capacity / max(1, open_slots),
                )
                if pair_gross_target <= 0:
                    continue

                dollar_a = pair_gross_target / (1.0 + abs(float(candidate.hedge_ratio)))
                dollar_b = pair_gross_target - dollar_a
                open_price_a = float(day_a["open"])
                open_price_b = float(day_b["open"])
                if open_price_a <= 0 or open_price_b <= 0:
                    continue

                if signal.zscore > 0:
                    qty_a = -(dollar_a / open_price_a)
                    qty_b = dollar_b / open_price_b
                    direction = "short_a_long_b"
                else:
                    qty_a = dollar_a / open_price_a
                    qty_b = -(dollar_b / open_price_b)
                    direction = "long_a_short_b"

                cash_before = cash
                notional_a = abs(qty_a) * _execution_price(
                    open_price_a,
                    "sell" if qty_a < 0 else "buy",
                    stat_arb_config.backtest,
                )
                notional_b = abs(qty_b) * _execution_price(
                    open_price_b,
                    "sell" if qty_b < 0 else "buy",
                    stat_arb_config.backtest,
                )
                fee_a = _trade_fee(notional_a, stat_arb_config.backtest)
                fee_b = _trade_fee(notional_b, stat_arb_config.backtest)

                if qty_a > 0:
                    cash -= notional_a + fee_a
                    side_a = "buy"
                    exec_price_a = notional_a / abs(qty_a)
                else:
                    cash += notional_a - fee_a
                    side_a = "short"
                    exec_price_a = notional_a / abs(qty_a)

                if qty_b > 0:
                    cash -= notional_b + fee_b
                    side_b = "buy"
                    exec_price_b = notional_b / abs(qty_b)
                else:
                    cash += notional_b - fee_b
                    side_b = "short"
                    exec_price_b = notional_b / abs(qty_b)

                positions[candidate.pair_id] = PairPosition(
                    pair_id=candidate.pair_id,
                    symbol_a=candidate.symbol_a,
                    symbol_b=candidate.symbol_b,
                    hedge_ratio=float(candidate.hedge_ratio),
                    qty_a=float(qty_a),
                    qty_b=float(qty_b),
                    entry_date=trade_date,
                    entry_zscore=float(signal.zscore),
                    direction=direction,
                    classification_bucket=str(candidate.classification_bucket),
                    sector=str(candidate.sector),
                    industry=str(candidate.industry),
                )

                trade_records.extend(
                    [
                        {
                            "date": trade_date,
                            "pair_id": candidate.pair_id,
                            "symbol": candidate.symbol_a,
                            "side": side_a,
                            "qty": abs(qty_a),
                            "price": exec_price_a,
                            "notional": notional_a,
                            "fee": fee_a,
                            "reason": "entry",
                            "signal_zscore": signal.zscore,
                        },
                        {
                            "date": trade_date,
                            "pair_id": candidate.pair_id,
                            "symbol": candidate.symbol_b,
                            "side": side_b,
                            "qty": abs(qty_b),
                            "price": exec_price_b,
                            "notional": notional_b,
                            "fee": fee_b,
                            "reason": "entry",
                            "signal_zscore": signal.zscore,
                        },
                    ]
                )

                open_symbols.update({candidate.symbol_a, candidate.symbol_b})
                close_prices[candidate.symbol_a] = float(day_a["close"])
                close_prices[candidate.symbol_b] = float(day_b["close"])
                current_equity = cash + _portfolio_market_value(positions, close_prices)
                current_gross = _gross_exposure(positions, close_prices)
                remaining_capacity = max(
                    0.0,
                    (float(stat_arb_config.backtest.max_gross_leverage) * current_equity) - current_gross,
                )
                open_slots = int(stat_arb_config.backtest.max_open_pairs) - len(positions)
                if open_slots <= 0:
                    break

        borrow_fee_lookup = _borrow_fees_lookup(
            latest_universe if latest_universe is not None and not latest_universe.empty else pd.DataFrame(),
            stat_arb_config.backtest.default_borrow_fee_bps,
        )
        borrow_cost = 0.0
        for position in positions.values():
            for symbol, qty in ((position.symbol_a, position.qty_a), (position.symbol_b, position.qty_b)):
                if qty >= 0 or symbol not in close_prices:
                    continue
                annual_rate = float(borrow_fee_lookup.get(symbol, stat_arb_config.backtest.default_borrow_fee_bps)) / 10000.0
                borrow_cost += abs(qty) * float(close_prices[symbol]) * (annual_rate / 252.0)
        cash -= borrow_cost

        equity = cash + _portfolio_market_value(positions, close_prices)
        gross_exposure = _gross_exposure(positions, close_prices)
        net_exposure = _net_exposure(positions, close_prices)

        results_records.append(
            {
                "date": trade_date,
                "equity": equity,
                "cash": cash,
                "gross_exposure": gross_exposure,
                "gross_exposure_pct": 0.0 if equity == 0 else gross_exposure / equity,
                "net_exposure": net_exposure,
                "net_exposure_pct": 0.0 if equity == 0 else net_exposure / equity,
                "open_pairs": len(positions),
                "selected_pairs": 0 if current_selected_pairs is None else len(current_selected_pairs),
                "borrow_cost": borrow_cost,
            }
        )

    results = pd.DataFrame(results_records)
    if results.empty:
        raise ValueError("Stat-arb backtest produced no daily results. Check the metadata coverage and history window.")

    results["daily_return"] = results["equity"].pct_change().fillna(0.0)
    running_peak = results["equity"].cummax()
    results["drawdown_pct"] = (results["equity"] / running_peak) - 1.0

    summary = {
        "initial_capital": float(stat_arb_config.backtest.initial_capital),
        "final_equity": float(results["equity"].iloc[-1]),
        "total_return_pct": float(((results["equity"].iloc[-1] / stat_arb_config.backtest.initial_capital) - 1.0) * 100.0),
        "max_drawdown_pct": float(results["drawdown_pct"].min() * 100.0),
        "trading_days": int(len(results)),
        "average_gross_exposure_pct": float(results["gross_exposure_pct"].mean() * 100.0),
        "average_net_exposure_pct": float(results["net_exposure_pct"].mean() * 100.0),
        "trade_count": int(len(trade_records)),
        "pair_reselection_count": int(sum(1 for _ in selected_pair_records)),
    }

    selected_pairs = (
        pd.concat(selected_pair_records, ignore_index=True)
        if selected_pair_records
        else pd.DataFrame()
    )
    candidate_pairs = (
        pd.concat(candidate_pair_records, ignore_index=True)
        if candidate_pair_records
        else pd.DataFrame()
    )
    trades = pd.DataFrame(trade_records)

    return StatArbBacktestResult(
        results=results,
        selected_pairs=selected_pairs,
        candidate_pairs=candidate_pairs,
        trades=trades,
        universe=latest_universe,
        rejections=latest_rejections,
        summary=summary,
    )
