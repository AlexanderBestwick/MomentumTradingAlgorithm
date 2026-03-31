from dataclasses import dataclass

from Funds.Models import CashFlowRequest, UnitLedgerEntry


DEFAULT_INITIAL_UNIT_PRICE = 100.0


@dataclass(frozen=True)
class CashFlowExecution:
    request_id: str
    fund_id: str
    investor_id: str
    flow_type: str
    cash_amount: float
    unit_price: float
    units_delta: float
    note: str | None = None


@dataclass(frozen=True)
class CashFlowBatchResult:
    unit_price: float
    starting_nav: float
    ending_nav: float
    starting_units: float
    ending_units: float
    executions: list[CashFlowExecution]


def calculate_unit_price(net_asset_value, total_units, *, initial_unit_price=DEFAULT_INITIAL_UNIT_PRICE):
    net_asset_value = float(net_asset_value)
    total_units = float(total_units)
    initial_unit_price = float(initial_unit_price)

    if net_asset_value < 0:
        raise ValueError("net_asset_value must be greater than or equal to zero.")
    if total_units < 0:
        raise ValueError("total_units must be greater than or equal to zero.")
    if initial_unit_price <= 0:
        raise ValueError("initial_unit_price must be greater than zero.")
    if total_units == 0:
        return initial_unit_price
    return net_asset_value / total_units


def issue_units_for_cash(cash_amount, unit_price):
    cash_amount = float(cash_amount)
    unit_price = float(unit_price)
    if cash_amount <= 0:
        raise ValueError("cash_amount must be greater than zero.")
    if unit_price <= 0:
        raise ValueError("unit_price must be greater than zero.")
    return cash_amount / unit_price


def redeem_units_for_cash(cash_amount, unit_price):
    return issue_units_for_cash(cash_amount, unit_price)


def calculate_member_value(unit_balance, unit_price):
    return float(unit_balance) * float(unit_price)


def calculate_ownership_percent(unit_balance, total_units):
    total_units = float(total_units)
    if total_units <= 0:
        return 0.0
    return float(unit_balance) / total_units


def apply_cash_flows_at_nav(
    *,
    net_asset_value_before_flows,
    total_units_before_flows,
    cash_flows,
    initial_unit_price=DEFAULT_INITIAL_UNIT_PRICE,
):
    starting_nav = float(net_asset_value_before_flows)
    starting_units = float(total_units_before_flows)
    unit_price = calculate_unit_price(
        starting_nav,
        starting_units,
        initial_unit_price=initial_unit_price,
    )

    ending_nav = starting_nav
    ending_units = starting_units
    executions = []

    for request in cash_flows:
        if not isinstance(request, CashFlowRequest):
            raise TypeError("cash_flows must contain CashFlowRequest items.")

        if request.flow_type == "contribution":
            units_delta = issue_units_for_cash(request.amount, unit_price)
            ending_nav += request.amount
            ending_units += units_delta
        else:
            units_delta = -redeem_units_for_cash(request.amount, unit_price)
            ending_nav -= request.amount
            ending_units += units_delta

        if ending_nav < -1e-9:
            raise ValueError("Cash flow batch would push fund NAV below zero.")
        if ending_units < -1e-9:
            raise ValueError("Cash flow batch would push total units below zero.")

        executions.append(
            CashFlowExecution(
                request_id=request.request_id,
                fund_id=request.fund_id,
                investor_id=request.investor_id,
                flow_type=request.flow_type,
                cash_amount=float(request.amount),
                unit_price=unit_price,
                units_delta=units_delta,
                note=request.note,
            )
        )

    return CashFlowBatchResult(
        unit_price=unit_price,
        starting_nav=starting_nav,
        ending_nav=max(0.0, ending_nav),
        starting_units=starting_units,
        ending_units=max(0.0, ending_units),
        executions=executions,
    )


def build_unit_ledger_entries(batch_result, *, snapshot_id, created_at):
    entries = []
    for execution in batch_result.executions:
        entries.append(
            UnitLedgerEntry(
                entry_id=f"{snapshot_id}:{execution.request_id}",
                fund_id=execution.fund_id,
                investor_id=execution.investor_id,
                snapshot_id=snapshot_id,
                created_at=created_at,
                entry_type=execution.flow_type,
                cash_amount=execution.cash_amount,
                unit_price=execution.unit_price,
                units_delta=execution.units_delta,
                request_id=execution.request_id,
                note=execution.note,
            )
        )
    return entries
