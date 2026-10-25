"""Pure spot arithmetic over qualified inputs; acquisition and evidence bytes stay outside."""

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from decimal import Decimal, localcontext
from typing import Literal, Self

from pydantic import Field, model_validator

from tank.analysis.condition_inputs import SpotCalculationPolicy, SpotFlowObservation
from tank.contracts import DomainModel

type FlowSign = Literal[-1, 0, 1]
type NativeFlowField = Literal["buy_qty", "sell_qty", "sum_sq_qty"]


@contextmanager
def _native_arithmetic(values: Sequence[Decimal]) -> Iterator[None]:
    exponents: list[int] = []
    for value in values:
        if not value.is_finite():
            raise ValueError("Spot arithmetic requires finite native amounts")
        exponent = value.as_tuple().exponent
        if not isinstance(exponent, int):
            raise ValueError("Spot arithmetic requires finite Decimal exponents")
        exponents.append(exponent)
    span = max(value.adjusted() for value in values) - min(exponents) + 1 if values else 1
    with localcontext() as context:
        context.prec = max(128, 4 * span + 32)
        yield


class NativeFlowTotals(DomainModel):
    token_id: str = Field(min_length=1, pattern=r"\S")
    buy_qty: Decimal = Field(ge=0)
    sell_qty: Decimal = Field(ge=0)
    volume_native: Decimal = Field(ge=0)
    cvd: Decimal
    sum_sq_qty: Decimal = Field(ge=0)

    @model_validator(mode="after")
    def actual_native_totals(self) -> Self:
        with _native_arithmetic((self.buy_qty, self.sell_qty, self.volume_native, self.cvd)):
            if self.volume_native != self.buy_qty + self.sell_qty or self.cvd != self.buy_qty - self.sell_qty:
                raise ValueError("Native spot totals must preserve their actual side amounts")
        if (self.volume_native == 0) != (self.sum_sq_qty == 0):
            raise ValueError("Native spot squared quantity cannot invent or erase observed flow")
        return self


class MissingSpotFlowMetrics(DomainModel):
    reason: Literal["INCOMPLETE"]
    missing_metrics: tuple[NativeFlowField, ...] = Field(min_length=1)


def summarize_native_flow(
    token_id: str, rows: Sequence[SpotFlowObservation]
) -> NativeFlowTotals | MissingSpotFlowMetrics:
    missing: list[NativeFlowField] = []
    if not rows or any(row.buy_qty is None for row in rows):
        missing.append("buy_qty")
    if not rows or any(row.sell_qty is None for row in rows):
        missing.append("sell_qty")
    if not rows or any(row.sum_sq_qty is None for row in rows):
        missing.append("sum_sq_qty")
    if any(row.instrument.token_id != token_id for row in rows):
        raise ValueError("Native spot arithmetic cannot combine different token quantities")
    if missing:
        return MissingSpotFlowMetrics(reason="INCOMPLETE", missing_metrics=tuple(missing))
    buys: list[Decimal] = []
    sells: list[Decimal] = []
    squared: list[Decimal] = []
    for row in rows:
        if row.buy_qty is None or row.sell_qty is None or row.sum_sq_qty is None:
            raise ValueError("Unavailable native flow cannot enter observed arithmetic")
        buys.append(row.buy_qty)
        sells.append(row.sell_qty)
        squared.append(row.sum_sq_qty)
    with _native_arithmetic((*buys, *sells, *squared)):
        buy = sum(buys, Decimal(0))
        sell = sum(sells, Decimal(0))
        return NativeFlowTotals(
            token_id=token_id,
            buy_qty=buy,
            sell_qty=sell,
            volume_native=buy + sell,
            cvd=buy - sell,
            sum_sq_qty=sum(squared, Decimal(0)),
        )


def significant_flow(totals: NativeFlowTotals, policy: SpotCalculationPolicy) -> FlowSign:
    totals = NativeFlowTotals.model_validate(totals)
    policy = SpotCalculationPolicy.model_validate(policy)
    with _native_arithmetic((totals.cvd, totals.sum_sq_qty, policy.significant_flow_sigma)):
        if totals.cvd == 0:
            return 0
        if totals.cvd * totals.cvd < policy.significant_flow_sigma**2 * totals.sum_sq_qty:
            return 0
        return 1 if totals.cvd > 0 else -1
