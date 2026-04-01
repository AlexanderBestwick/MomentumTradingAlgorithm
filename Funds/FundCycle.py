import copy
from datetime import date, datetime, time, timedelta, timezone
from types import SimpleNamespace

from Funds.Accounting import DEFAULT_INITIAL_UNIT_PRICE, apply_cash_flows_at_nav, calculate_member_value
from Funds.LedgerStore import LEDGER_SCHEMA_VERSION
from Funds.Models import CashFlowRequest


def _coerce_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc)

    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _isoformat(value):
    normalized = _coerce_datetime(value)
    return None if normalized is None else normalized.isoformat()


def _generate_request_id(fund_id, investor_id, flow_type, requested_at):
    requested_at = _coerce_datetime(requested_at) or datetime.now(timezone.utc)
    return (
        f"{str(fund_id).strip()}-"
        f"{str(investor_id).strip()}-"
        f"{str(flow_type).strip().lower()}-"
        f"{requested_at:%Y%m%dT%H%M%SZ}"
    )


def next_dealing_datetime(
    *,
    reference_time=None,
    weekday=2,
    hour=9,
    minute=30,
):
    reference_time = _coerce_datetime(reference_time) or datetime.now(timezone.utc)
    candidate = datetime.combine(
        reference_time.date(),
        time(hour=hour, minute=minute, tzinfo=timezone.utc),
    )
    days_ahead = (int(weekday) - candidate.weekday()) % 7
    candidate = candidate + timedelta(days=days_ahead)
    if candidate < reference_time:
        candidate = candidate + timedelta(days=7)
    return candidate


def build_cash_flow_request_record(
    *,
    fund_id,
    investor_id,
    flow_type,
    gross_amount,
    fee_amount=0.0,
    net_amount=None,
    request_id=None,
    requested_at=None,
    effective_at=None,
    display_name=None,
    external_reference=None,
    note=None,
):
    flow_type = str(flow_type).strip().lower()
    if flow_type not in {"contribution", "withdrawal"}:
        raise ValueError("flow_type must be 'contribution' or 'withdrawal'.")

    requested_at = _coerce_datetime(requested_at) or datetime.now(timezone.utc)
    effective_at = _coerce_datetime(effective_at) or next_dealing_datetime(reference_time=requested_at)
    gross_amount = float(gross_amount)
    fee_amount = float(fee_amount)
    resolved_net_amount = gross_amount - fee_amount if net_amount is None else float(net_amount)

    if gross_amount <= 0:
        raise ValueError("gross_amount must be greater than zero.")
    if fee_amount < 0:
        raise ValueError("fee_amount must be greater than or equal to zero.")
    if resolved_net_amount <= 0:
        raise ValueError("net_amount must be greater than zero.")

    return {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "kind": "fund_cash_flow_request",
        "request_id": request_id or _generate_request_id(fund_id, investor_id, flow_type, requested_at),
        "fund_id": str(fund_id).strip(),
        "investor_id": str(investor_id).strip(),
        "display_name": None if display_name is None else str(display_name).strip(),
        "flow_type": flow_type,
        "status": "confirmed",
        "gross_amount": gross_amount,
        "fee_amount": fee_amount,
        "net_amount": resolved_net_amount,
        "requested_at": requested_at.isoformat(),
        "cash_confirmed_at": requested_at.isoformat(),
        "effective_at": effective_at.isoformat(),
        "processed_at": None,
        "executed_at": None,
        "settled_at": None,
        "unit_price": None,
        "units_delta": None,
        "external_reference": None if external_reference is None else str(external_reference).strip(),
        "note": note,
    }


def build_investor_record(*, investor_id, display_name=None, metadata=None):
    payload = {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "kind": "fund_investor",
        "investor_id": str(investor_id).strip(),
        "display_name": str(display_name or investor_id).strip(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if metadata:
        payload["metadata"] = dict(metadata)
    return payload


def mark_request_settled(request_record, *, settled_at=None, note=None):
    flow_type = _request_flow_type(request_record)
    status = _request_status(request_record)
    if flow_type != "withdrawal":
        raise ValueError("Only withdrawal requests can be marked as settled.")
    if status not in {"executed", "settled"}:
        raise ValueError("Only executed withdrawal requests can be marked as settled.")

    updated = copy.deepcopy(request_record)
    updated["status"] = "settled"
    updated["settled_at"] = _isoformat(settled_at or datetime.now(timezone.utc))
    if note:
        existing_note = (updated.get("note") or "").strip()
        updated["note"] = f"{existing_note} | {note}".strip(" |")
    return updated


def _request_status(record):
    return str(record.get("status", "confirmed")).strip().lower()


def _request_flow_type(record):
    return str(record.get("flow_type", "")).strip().lower()


def _request_net_amount(record):
    if record.get("net_amount") is not None:
        return float(record["net_amount"])
    if record.get("cash_amount") is not None:
        return float(record["cash_amount"])
    if record.get("amount") is not None:
        return float(record["amount"])
    raise ValueError(f"Cash flow record {record.get('request_id')} is missing a net amount.")


def _request_effective_at(record):
    return _coerce_datetime(record.get("effective_at") or record.get("requested_at"))


def _is_executed_record(record):
    return _request_status(record) in {"executed", "settled"} and record.get("units_delta") is not None


def _is_unsettled_withdrawal(record):
    return (
        _request_flow_type(record) == "withdrawal"
        and _request_status(record) == "executed"
        and not record.get("settled_at")
    )


def _compute_investor_units(request_records):
    balances = {}
    for record in request_records:
        if not _is_executed_record(record):
            continue
        balances.setdefault(record["investor_id"], 0.0)
        balances[record["investor_id"]] += float(record.get("units_delta") or 0.0)
    return balances


def _summarize_state(request_records, *, gross_asset_value, cash_value, initial_unit_price):
    total_units = sum(float(record.get("units_delta") or 0.0) for record in request_records if _is_executed_record(record))

    pending_contributions = [
        record
        for record in request_records
        if _request_status(record) == "confirmed" and _request_flow_type(record) == "contribution"
    ]
    pending_subscription_liabilities = sum(_request_net_amount(record) for record in pending_contributions)

    unsettled_withdrawals = [record for record in request_records if _is_unsettled_withdrawal(record)]
    outstanding_withdrawal_payables = sum(_request_net_amount(record) for record in unsettled_withdrawals)

    gross_asset_value = float(gross_asset_value)
    cash_value = float(cash_value)
    liabilities_value = pending_subscription_liabilities + outstanding_withdrawal_payables
    net_asset_value = gross_asset_value - liabilities_value
    investable_cash = cash_value - liabilities_value
    cash_shortfall = max(0.0, liabilities_value - cash_value)

    if net_asset_value < -1e-9:
        raise ValueError(
            "Computed a negative fund NAV. "
            "This usually means pending contributions/withdrawals do not match the broker cash snapshot."
        )

    unit_price = DEFAULT_INITIAL_UNIT_PRICE if total_units == 0 else net_asset_value / total_units
    if total_units == 0:
        unit_price = float(initial_unit_price)

    return {
        "gross_asset_value": gross_asset_value,
        "cash_value": cash_value,
        "net_asset_value": max(0.0, net_asset_value),
        "liabilities_value": max(0.0, liabilities_value),
        "pending_subscription_liabilities": max(0.0, pending_subscription_liabilities),
        "outstanding_withdrawal_payables": max(0.0, outstanding_withdrawal_payables),
        "investable_cash": investable_cash,
        "cash_reserve": max(0.0, liabilities_value),
        "cash_shortfall": cash_shortfall,
        "total_units": max(0.0, total_units),
        "unit_price": max(0.0, float(unit_price)),
        "pending_contributions": pending_contributions,
        "unsettled_withdrawals": unsettled_withdrawals,
    }


def _investor_display_name(investor_id, investor_records, request_records):
    investor_record = investor_records.get(investor_id) or {}
    display_name = (investor_record.get("display_name") or "").strip()
    if display_name:
        return display_name

    for record in request_records:
        if record.get("investor_id") != investor_id:
            continue
        display_name = (record.get("display_name") or "").strip()
        if display_name:
            return display_name

    return str(investor_id).replace("_", " ").title()


def _build_investor_balances(request_records, investor_records, *, unit_price, total_units):
    unit_balances = _compute_investor_units(request_records)
    net_contribution_totals = {}
    pending_contributions = {}
    pending_withdrawals = {}
    outstanding_redemption_payables = {}

    investor_ids = {
        *unit_balances.keys(),
        *investor_records.keys(),
        *[record.get("investor_id") for record in request_records if record.get("investor_id")],
    }

    for investor_id in investor_ids:
        net_contribution_totals[investor_id] = 0.0
        pending_contributions[investor_id] = 0.0
        pending_withdrawals[investor_id] = 0.0
        outstanding_redemption_payables[investor_id] = 0.0

    for record in request_records:
        investor_id = record.get("investor_id")
        if not investor_id:
            continue

        amount = _request_net_amount(record)
        flow_type = _request_flow_type(record)
        status = _request_status(record)

        if status in {"executed", "settled"}:
            if flow_type == "contribution":
                net_contribution_totals[investor_id] += amount
            elif flow_type == "withdrawal":
                net_contribution_totals[investor_id] -= amount

        if status == "confirmed":
            if flow_type == "contribution":
                pending_contributions[investor_id] += amount
            elif flow_type == "withdrawal":
                pending_withdrawals[investor_id] += amount

        if _is_unsettled_withdrawal(record):
            outstanding_redemption_payables[investor_id] += amount

    investors = []
    for investor_id in sorted(investor_ids):
        units = float(unit_balances.get(investor_id, 0.0))
        value = calculate_member_value(units, unit_price) if units > 0 else 0.0
        ownership_percent = (units / total_units) if total_units > 0 else 0.0
        investors.append(
            {
                "investor_id": investor_id,
                "display_name": _investor_display_name(investor_id, investor_records, request_records),
                "units": units,
                "value": value,
                "ownership_percent": ownership_percent,
                "net_contributions": float(net_contribution_totals.get(investor_id, 0.0)),
                "pending_contribution_cash": float(pending_contributions.get(investor_id, 0.0)),
                "pending_withdrawal_cash": float(pending_withdrawals.get(investor_id, 0.0)),
                "outstanding_redemption_payable": float(outstanding_redemption_payables.get(investor_id, 0.0)),
            }
        )

    return investors


def write_latest_ledger_state(
    ledger_store,
    *,
    valued_at,
    gross_asset_value,
    cash_value,
    initial_unit_price=DEFAULT_INITIAL_UNIT_PRICE,
    source_run_id=None,
    note=None,
    request_records=None,
):
    valued_at = _coerce_datetime(valued_at) or datetime.now(timezone.utc)
    request_records = list(ledger_store.list_requests() if request_records is None else request_records)
    investor_records = {
        record["investor_id"]: record
        for record in ledger_store.list_investors()
        if record.get("investor_id")
    }

    state = _summarize_state(
        request_records,
        gross_asset_value=gross_asset_value,
        cash_value=cash_value,
        initial_unit_price=initial_unit_price,
    )
    investor_balances = _build_investor_balances(
        request_records,
        investor_records,
        unit_price=state["unit_price"],
        total_units=state["total_units"],
    )

    pending_records = [
        {
            "request_id": record["request_id"],
            "investor_id": record["investor_id"],
            "display_name": _investor_display_name(record["investor_id"], investor_records, request_records),
            "flow_type": _request_flow_type(record),
            "net_amount": _request_net_amount(record),
            "effective_at": record.get("effective_at"),
            "requested_at": record.get("requested_at"),
            "note": record.get("note"),
        }
        for record in request_records
        if _request_status(record) == "confirmed"
    ]
    unsettled_withdrawals = [
        {
            "request_id": record["request_id"],
            "investor_id": record["investor_id"],
            "display_name": _investor_display_name(record["investor_id"], investor_records, request_records),
            "net_amount": _request_net_amount(record),
            "executed_at": record.get("executed_at"),
            "note": record.get("note"),
        }
        for record in request_records
        if _is_unsettled_withdrawal(record)
    ]

    fund_nav_payload = {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "kind": "fund_nav_snapshot",
        "fund_id": ledger_store.fund_id,
        "valued_at": valued_at.isoformat(),
        "gross_asset_value": state["gross_asset_value"],
        "cash_value": state["cash_value"],
        "net_asset_value": state["net_asset_value"],
        "liabilities_value": state["liabilities_value"],
        "pending_subscription_liabilities": state["pending_subscription_liabilities"],
        "outstanding_withdrawal_payables": state["outstanding_withdrawal_payables"],
        "investable_cash": state["investable_cash"],
        "cash_reserve": state["cash_reserve"],
        "cash_shortfall": state["cash_shortfall"],
        "total_units": state["total_units"],
        "unit_price": state["unit_price"],
        "source_run_id": source_run_id,
        "note": note,
    }
    investor_balances_payload = {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "kind": "fund_investor_balances",
        "fund_id": ledger_store.fund_id,
        "valued_at": valued_at.isoformat(),
        "unit_price": state["unit_price"],
        "total_units": state["total_units"],
        "investors": investor_balances,
    }
    pending_payload = {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "kind": "fund_pending_cash_flows",
        "fund_id": ledger_store.fund_id,
        "valued_at": valued_at.isoformat(),
        "pending_request_count": len(pending_records),
        "pending_subscription_liabilities": state["pending_subscription_liabilities"],
        "outstanding_withdrawal_payables": state["outstanding_withdrawal_payables"],
        "cash_shortfall": state["cash_shortfall"],
        "confirmed_requests": pending_records,
        "unsettled_withdrawals": unsettled_withdrawals,
    }

    ledger_store.save_latest("fund_nav", fund_nav_payload)
    ledger_store.save_latest("investor_balances", investor_balances_payload)
    ledger_store.save_latest("pending_cash_flows", pending_payload)

    return {
        "fund_nav": fund_nav_payload,
        "investor_balances": investor_balances_payload,
        "pending_cash_flows": pending_payload,
        "cash_reserve": state["cash_reserve"],
    }


def process_confirmed_cash_flows(
    ledger_store,
    *,
    valued_at,
    gross_asset_value,
    cash_value,
    initial_unit_price=DEFAULT_INITIAL_UNIT_PRICE,
    source_run_id=None,
    note=None,
    ignore_effective_at=False,
):
    valued_at = _coerce_datetime(valued_at) or datetime.now(timezone.utc)
    request_records = ledger_store.list_requests()

    due_records = [
        record
        for record in request_records
        if _request_status(record) == "confirmed"
        and (
            ignore_effective_at
            or (_request_effective_at(record) or valued_at) <= valued_at
        )
    ]

    if not due_records:
        latest_state = write_latest_ledger_state(
            ledger_store,
            valued_at=valued_at,
            gross_asset_value=gross_asset_value,
            cash_value=cash_value,
            initial_unit_price=initial_unit_price,
            source_run_id=source_run_id,
            note=note,
            request_records=request_records,
        )
        return {
            "valued_at": valued_at.isoformat(),
            "executed_count": 0,
            "executed_request_ids": [],
            "latest_state": latest_state,
            "cash_reserve": latest_state["cash_reserve"],
            "ignored_effective_at": bool(ignore_effective_at),
        }

    pre_state = _summarize_state(
        request_records,
        gross_asset_value=gross_asset_value,
        cash_value=cash_value,
        initial_unit_price=initial_unit_price,
    )
    cash_flows = [
        CashFlowRequest(
            request_id=record["request_id"],
            fund_id=ledger_store.fund_id,
            investor_id=record["investor_id"],
            flow_type=_request_flow_type(record),
            amount=_request_net_amount(record),
            requested_at=_coerce_datetime(record.get("requested_at")) or valued_at,
            effective_at=_request_effective_at(record),
            note=record.get("note"),
        )
        for record in due_records
    ]
    batch_result = apply_cash_flows_at_nav(
        net_asset_value_before_flows=pre_state["net_asset_value"],
        total_units_before_flows=pre_state["total_units"],
        cash_flows=cash_flows,
        initial_unit_price=initial_unit_price,
    )

    snapshot_id = f"{ledger_store.fund_id}:{valued_at:%Y%m%dT%H%M%SZ}"
    due_by_id = {record["request_id"]: record for record in due_records}
    execution_records = []

    for execution in batch_result.executions:
        original_record = due_by_id[execution.request_id]
        updated_record = copy.deepcopy(original_record)
        updated_record["status"] = "executed"
        updated_record["processed_at"] = valued_at.isoformat()
        updated_record["executed_at"] = valued_at.isoformat()
        updated_record["unit_price"] = float(execution.unit_price)
        updated_record["units_delta"] = float(execution.units_delta)
        if execution.flow_type == "contribution":
            updated_record["settled_at"] = valued_at.isoformat()
        ledger_store.save_request(updated_record)

        execution_payload = {
            "schema_version": LEDGER_SCHEMA_VERSION,
            "kind": "fund_cash_flow_execution",
            "execution_id": f"{snapshot_id}:{execution.request_id}",
            "snapshot_id": snapshot_id,
            "request_id": execution.request_id,
            "fund_id": ledger_store.fund_id,
            "investor_id": execution.investor_id,
            "display_name": updated_record.get("display_name"),
            "flow_type": execution.flow_type,
            "cash_amount": float(execution.cash_amount),
            "unit_price": float(execution.unit_price),
            "units_delta": float(execution.units_delta),
            "executed_at": valued_at.isoformat(),
            "source_run_id": source_run_id,
            "note": updated_record.get("note"),
        }
        ledger_store.save_execution(valued_at, execution.request_id, execution_payload)
        execution_records.append(execution_payload)

    updated_requests = ledger_store.list_requests()
    latest_state = write_latest_ledger_state(
        ledger_store,
        valued_at=valued_at,
        gross_asset_value=gross_asset_value,
        cash_value=cash_value,
        initial_unit_price=initial_unit_price,
        source_run_id=source_run_id,
        note=note,
        request_records=updated_requests,
    )

    return {
        "valued_at": valued_at.isoformat(),
        "executed_count": len(execution_records),
        "executed_request_ids": [record["request_id"] for record in execution_records],
        "executions": execution_records,
        "latest_state": latest_state,
        "cash_reserve": latest_state["cash_reserve"],
        "unit_price_used": batch_result.unit_price,
        "ignored_effective_at": bool(ignore_effective_at),
    }


def raise_cash_for_shortfall(
    trading_client,
    shortfall_cash,
    *,
    oversell_buffer_percent=0.001,
    min_order_value=1.0,
):
    shortfall_cash = max(0.0, float(shortfall_cash))
    oversell_buffer_percent = max(0.0, float(oversell_buffer_percent))
    min_order_value = max(0.0, float(min_order_value))
    target_cash_to_raise = shortfall_cash * (1.0 + oversell_buffer_percent)

    if shortfall_cash <= 0:
        return {
            "requested_cash": 0.0,
            "target_cash_to_raise": 0.0,
            "oversell_buffer_percent": oversell_buffer_percent,
            "planned_cash": 0.0,
            "remaining_shortfall": 0.0,
            "submitted_orders": [],
        }

    try:
        from alpaca.trading.enums import OrderSide, OrderType, TimeInForce
        from alpaca.trading.requests import OrderRequest
    except ImportError as exc:
        raise RuntimeError(
            "alpaca-py is required to raise cash for outstanding withdrawals."
        ) from exc

    positions = []
    for position in trading_client.get_all_positions():
        qty = abs(float(getattr(position, "qty", 0.0) or 0.0))
        market_value = abs(float(getattr(position, "market_value", 0.0) or 0.0))
        current_price = getattr(position, "current_price", None)
        price = abs(float(current_price)) if current_price is not None else 0.0
        if price <= 0 and qty > 0:
            price = market_value / qty
        if qty <= 0 or price <= 0 or market_value <= min_order_value:
            continue

        positions.append(
            {
                "symbol": position.symbol,
                "qty": qty,
                "price": price,
                "market_value": market_value,
            }
        )

    total_market_value = sum(position["market_value"] for position in positions)
    if total_market_value <= 0:
        return {
            "requested_cash": shortfall_cash,
            "target_cash_to_raise": target_cash_to_raise,
            "oversell_buffer_percent": oversell_buffer_percent,
            "planned_cash": 0.0,
            "remaining_shortfall": shortfall_cash,
            "submitted_orders": [],
        }

    sell_ratio = min(1.0, target_cash_to_raise / total_market_value)
    planned_cash = 0.0
    submitted_orders = []
    remaining_target_cash = target_cash_to_raise

    for index, position in enumerate(sorted(positions, key=lambda item: item["market_value"], reverse=True)):
        if remaining_target_cash <= 1e-6:
            break

        planned_notional = position["market_value"] * sell_ratio
        if index == len(positions) - 1:
            planned_notional = min(position["market_value"], max(planned_notional, remaining_target_cash))
        else:
            planned_notional = min(position["market_value"], planned_notional, remaining_target_cash)

        if planned_notional <= min_order_value:
            continue

        qty_to_sell = min(position["qty"], round(planned_notional / position["price"], 6))
        if qty_to_sell <= 0:
            continue

        expected_notional = min(position["market_value"], qty_to_sell * position["price"])
        if expected_notional <= min_order_value:
            continue

        trading_client.submit_order(
            OrderRequest(
                symbol=position["symbol"],
                qty=qty_to_sell,
                side=OrderSide.SELL,
                type=OrderType.MARKET,
                time_in_force=TimeInForce.DAY,
            )
        )
        submitted_orders.append(
            {
                "symbol": position["symbol"],
                "qty": qty_to_sell,
                "expected_notional": expected_notional,
            }
        )
        planned_cash += expected_notional
        remaining_target_cash = max(0.0, remaining_target_cash - expected_notional)

    return {
        "requested_cash": shortfall_cash,
        "target_cash_to_raise": target_cash_to_raise,
        "oversell_buffer_percent": oversell_buffer_percent,
        "planned_cash": planned_cash,
        "remaining_shortfall": max(0.0, shortfall_cash - planned_cash),
        "submitted_orders": submitted_orders,
    }


class ReservedCashTradingClient:
    def __init__(self, trading_client, *, reserve_cash):
        self._trading_client = trading_client
        self._reserve_cash = max(0.0, float(reserve_cash))
        self.is_backtest = getattr(trading_client, "is_backtest", False)

    def get_account(self):
        account = self._trading_client.get_account()

        raw_cash = float(getattr(account, "cash", 0.0))
        raw_portfolio_value = float(getattr(account, "portfolio_value", raw_cash))
        raw_equity = float(getattr(account, "equity", raw_portfolio_value))
        raw_buying_power = getattr(account, "buying_power", None)
        raw_long_market_value = getattr(account, "long_market_value", None)

        adjusted_cash = max(0.0, raw_cash - self._reserve_cash)
        adjusted_portfolio_value = max(0.0, raw_portfolio_value - self._reserve_cash)
        adjusted_equity = max(0.0, raw_equity - self._reserve_cash)
        adjusted_buying_power = None
        if raw_buying_power is not None:
            adjusted_buying_power = max(0.0, float(raw_buying_power) - self._reserve_cash)

        return SimpleNamespace(
            cash=adjusted_cash,
            portfolio_value=adjusted_portfolio_value,
            equity=adjusted_equity,
            buying_power=adjusted_buying_power,
            long_market_value=raw_long_market_value,
        )

    def __getattr__(self, name):
        return getattr(self._trading_client, name)


def wrap_trading_client_with_cash_reserve(trading_client, reserve_cash):
    reserve_cash = max(0.0, float(reserve_cash))
    if reserve_cash <= 0:
        return trading_client
    return ReservedCashTradingClient(trading_client, reserve_cash=reserve_cash)
