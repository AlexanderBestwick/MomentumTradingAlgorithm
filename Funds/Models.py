from dataclasses import dataclass
from datetime import datetime


def _require_non_empty(value, field_name):
    if not str(value).strip():
        raise ValueError(f"{field_name} must be a non-empty string.")
    return str(value).strip()


def _require_positive(value, field_name):
    numeric = float(value)
    if numeric <= 0:
        raise ValueError(f"{field_name} must be greater than zero.")
    return numeric


def _require_non_negative(value, field_name):
    numeric = float(value)
    if numeric < 0:
        raise ValueError(f"{field_name} must be greater than or equal to zero.")
    return numeric


@dataclass(frozen=True)
class FundDefinition:
    fund_id: str
    name: str
    base_currency: str = "USD"
    initial_unit_price: float = 100.0
    active: bool = True
    notes: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "fund_id", _require_non_empty(self.fund_id, "fund_id"))
        object.__setattr__(self, "name", _require_non_empty(self.name, "name"))
        object.__setattr__(self, "base_currency", _require_non_empty(self.base_currency, "base_currency").upper())
        object.__setattr__(self, "initial_unit_price", _require_positive(self.initial_unit_price, "initial_unit_price"))


@dataclass(frozen=True)
class InvestorDefinition:
    investor_id: str
    display_name: str
    email: str | None = None
    active: bool = True

    def __post_init__(self):
        object.__setattr__(self, "investor_id", _require_non_empty(self.investor_id, "investor_id"))
        object.__setattr__(self, "display_name", _require_non_empty(self.display_name, "display_name"))
        if self.email is not None:
            object.__setattr__(self, "email", _require_non_empty(self.email, "email").lower())


@dataclass(frozen=True)
class FundMembership:
    fund_id: str
    investor_id: str
    joined_at: datetime
    active: bool = True

    def __post_init__(self):
        object.__setattr__(self, "fund_id", _require_non_empty(self.fund_id, "fund_id"))
        object.__setattr__(self, "investor_id", _require_non_empty(self.investor_id, "investor_id"))


@dataclass(frozen=True)
class CashFlowRequest:
    request_id: str
    fund_id: str
    investor_id: str
    flow_type: str
    amount: float
    requested_at: datetime
    effective_at: datetime | None = None
    note: str | None = None

    def __post_init__(self):
        flow_type = _require_non_empty(self.flow_type, "flow_type").lower()
        if flow_type not in {"contribution", "withdrawal"}:
            raise ValueError("flow_type must be either 'contribution' or 'withdrawal'.")

        object.__setattr__(self, "request_id", _require_non_empty(self.request_id, "request_id"))
        object.__setattr__(self, "fund_id", _require_non_empty(self.fund_id, "fund_id"))
        object.__setattr__(self, "investor_id", _require_non_empty(self.investor_id, "investor_id"))
        object.__setattr__(self, "flow_type", flow_type)
        object.__setattr__(self, "amount", _require_positive(self.amount, "amount"))


@dataclass(frozen=True)
class NavSnapshot:
    snapshot_id: str
    fund_id: str
    valued_at: datetime
    net_asset_value: float
    gross_asset_value: float
    cash_value: float
    liabilities_value: float = 0.0
    total_units: float = 0.0
    unit_price: float = 0.0
    source_run_id: str | None = None
    note: str | None = None

    def __post_init__(self):
        object.__setattr__(self, "snapshot_id", _require_non_empty(self.snapshot_id, "snapshot_id"))
        object.__setattr__(self, "fund_id", _require_non_empty(self.fund_id, "fund_id"))
        object.__setattr__(self, "net_asset_value", _require_non_negative(self.net_asset_value, "net_asset_value"))
        object.__setattr__(self, "gross_asset_value", _require_non_negative(self.gross_asset_value, "gross_asset_value"))
        object.__setattr__(self, "cash_value", float(self.cash_value))
        object.__setattr__(self, "liabilities_value", _require_non_negative(self.liabilities_value, "liabilities_value"))
        object.__setattr__(self, "total_units", _require_non_negative(self.total_units, "total_units"))
        object.__setattr__(self, "unit_price", _require_non_negative(self.unit_price, "unit_price"))


@dataclass(frozen=True)
class UnitLedgerEntry:
    entry_id: str
    fund_id: str
    investor_id: str
    snapshot_id: str
    created_at: datetime
    entry_type: str
    cash_amount: float
    unit_price: float
    units_delta: float
    request_id: str | None = None
    note: str | None = None

    def __post_init__(self):
        entry_type = _require_non_empty(self.entry_type, "entry_type").lower()
        if entry_type not in {"contribution", "withdrawal", "adjustment"}:
            raise ValueError("entry_type must be contribution, withdrawal, or adjustment.")

        object.__setattr__(self, "entry_id", _require_non_empty(self.entry_id, "entry_id"))
        object.__setattr__(self, "fund_id", _require_non_empty(self.fund_id, "fund_id"))
        object.__setattr__(self, "investor_id", _require_non_empty(self.investor_id, "investor_id"))
        object.__setattr__(self, "snapshot_id", _require_non_empty(self.snapshot_id, "snapshot_id"))
        object.__setattr__(self, "entry_type", entry_type)
        object.__setattr__(self, "cash_amount", _require_non_negative(self.cash_amount, "cash_amount"))
        object.__setattr__(self, "unit_price", _require_non_negative(self.unit_price, "unit_price"))
        object.__setattr__(self, "units_delta", float(self.units_delta))
