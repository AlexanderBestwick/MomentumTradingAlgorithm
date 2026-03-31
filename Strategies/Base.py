from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Protocol


@dataclass(frozen=True)
class StrategyAllocation:
    fund_id: str
    strategy_id: str
    sleeve_weight: float
    active: bool = True
    notes: str | None = None

    def __post_init__(self):
        if not str(self.fund_id).strip():
            raise ValueError("fund_id must be a non-empty string.")
        if not str(self.strategy_id).strip():
            raise ValueError("strategy_id must be a non-empty string.")
        sleeve_weight = float(self.sleeve_weight)
        if sleeve_weight < 0:
            raise ValueError("sleeve_weight must be greater than or equal to zero.")
        object.__setattr__(self, "sleeve_weight", sleeve_weight)


@dataclass(frozen=True)
class StrategyContext:
    fund_id: str
    strategy_id: str
    run_date: date
    generated_at: datetime
    portfolio_value: float | None = None
    available_cash: float | None = None
    sleeve_weight: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyTarget:
    symbol: str
    target_weight: float
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        symbol = str(self.symbol).strip().upper()
        if not symbol:
            raise ValueError("symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "target_weight", float(self.target_weight))


@dataclass(frozen=True)
class StrategyDecision:
    strategy_id: str
    generated_at: datetime
    targets: list[StrategyTarget]
    notes: str | None = None

    def total_abs_weight(self):
        return sum(abs(target.target_weight) for target in self.targets)


class TradingStrategy(Protocol):
    strategy_id: str

    def build_targets(self, context: StrategyContext) -> StrategyDecision:
        ...
