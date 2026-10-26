"""Captured quote facts, declared valuation policy and separate receipt visibility."""

from collections.abc import Iterable
from decimal import MAX_EMAX, MIN_EMIN, Decimal, DecimalException, Inexact, localcontext
from typing import Annotated, Literal, Protocol, Self

from pydantic import BeforeValidator, Field, model_validator

from tank.contracts import DomainModel
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.scalars import require_integer
from tank.time_precision import Timestamp
from tank.util import parse_iso_time

type QuoteDenomination = Literal["QUOTE", "QUOTE_PER_NATIVE_TOKEN"]
type UsdDenomination = Literal["USD", "USD_PER_NATIVE_TOKEN"]
type QuoteMarketCollection = Literal[
    "mkt.klines_1m", "mkt.book_summaries", "mkt.oi_obs", "mkt.tickers_24h", "mkt.liq_obs", "mkt.flow_bars_1m"
]
type QuoteCalculationFailure = Literal[
    "QUOTE_ASSET_NOT_CAPTURED",
    "TARGET_CLOCK_NOT_CAPTURED",
    "QUOTE_RATE_NOT_CAPTURED",
    "QUOTE_RATE_INVALID",
    "QUOTE_RATE_AS_OF_INVALID",
    "QUOTE_RATE_AS_OF_NOT_CAPTURED",
    "QUOTE_RATE_FUTURE",
    "QUOTE_RATE_STALE",
    "NOT_ESTIMABLE",
]
type QuoteVisibilityFailure = Literal[
    "ASSEMBLY_CLOCK_NOT_CAPTURED",
    "NATIVE_RECEIPT_NOT_CAPTURED",
    "NATIVE_RECEIPT_AFTER_ASSEMBLY",
    "QUOTE_RATE_RECEIPT_NOT_CAPTURED",
    "QUOTE_RATE_RECEIPT_AFTER_ASSEMBLY",
]
type QuoteValuationFailure = QuoteCalculationFailure | QuoteVisibilityFailure


class QuoteValuationScope(DomainModel):
    kind: Literal["DECLARED_QUOTE"]
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    token_id: str = Field(min_length=1, pattern=r"\S")
    quote_asset: str = Field(min_length=1, pattern=r"\S")


class UnknownQuoteValuationScope(DomainModel):
    kind: Literal["QUOTE_NOT_CAPTURED"]
    instrument_id: str = Field(min_length=1, pattern=r"\S")
    token_id: str = Field(min_length=1, pattern=r"\S")
    quote_asset: None
    reason: Literal["QUOTE_ASSET_NOT_CAPTURED"]


type NativeQuoteScope = QuoteValuationScope | UnknownQuoteValuationScope


class QuoteRateEvidenceRef(DomainModel):
    kind: Literal["CANONICAL_STORAGE_READ"]
    collection: QuoteMarketCollection
    source_projection_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_evidence_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class NativeQuoteValue(DomainModel):
    scope: NativeQuoteScope
    denomination: QuoteDenomination
    value: Decimal
    target_at: Timestamp
    received_at: Timestamp
    evidence: QuoteRateEvidenceRef


class CapturedQuoteUsdRate(DomainModel):
    kind: Literal["CAPTURED_RATE"]
    scope: NativeQuoteScope
    rate: Decimal = Field(gt=0)
    as_of: Timestamp
    received_at: Timestamp
    evidence: QuoteRateEvidenceRef


class MissingQuoteUsdRate(DomainModel):
    kind: Literal["NO_CAPTURED_SAMPLE"]
    quote_asset: str | None = Field(min_length=1, pattern=r"\S")
    reason: Literal["QUOTE_RATE_NOT_CAPTURED"]


class UnusableCapturedQuoteUsdRate(DomainModel):
    kind: Literal["UNUSABLE_CAPTURED_RATE"]
    scope: NativeQuoteScope
    evidence: QuoteRateEvidenceRef
    reason: Literal["QUOTE_RATE_NOT_CAPTURED", "QUOTE_RATE_INVALID", "QUOTE_RATE_AS_OF_INVALID"]


type QuoteUsdRateSource = CapturedQuoteUsdRate | MissingQuoteUsdRate | UnusableCapturedQuoteUsdRate


class QuoteRateAgePolicy(DomainModel):
    versions: ResolvedVersionIdentity
    max_age_s: Annotated[Literal[21600], BeforeValidator(require_integer)]
    time_basis: Literal["REPORTED_AS_OF_AND_TARGET"]


class QuoteRateHistoryPolicy(DomainModel):
    versions: ResolvedVersionIdentity
    retention_s: Annotated[Literal[345600], BeforeValidator(require_integer)]
    time_basis: Literal["NEWEST_REPORTED_AS_OF"]


class UsdUnitValuationContext(DomainModel):
    kind: Literal["USD_UNIT"]
    versions: ResolvedVersionIdentity


class QuoteNotCapturedValuationContext(DomainModel):
    kind: Literal["QUOTE_NOT_CAPTURED"]
    versions: ResolvedVersionIdentity


class CapturedRateValuationContext(DomainModel):
    kind: Literal["CURRENT_ANALYSIS_CAPTURED_RATE"]
    policy: QuoteRateAgePolicy
    source: QuoteUsdRateSource


type QuoteValuationContext = (
    UsdUnitValuationContext | QuoteNotCapturedValuationContext | CapturedRateValuationContext
)


class QuoteValueCalculation(DomainModel):
    value: Decimal | None
    reason: QuoteCalculationFailure | None

    @model_validator(mode="after")
    def value_or_failure(self) -> Self:
        if (self.value is None) != (self.reason is not None):
            raise ValueError("A missing USD calculation requires its actual failure")
        return self


def quote_value_product(value: Decimal, rate: Decimal) -> Decimal:
    if not value.is_finite() or not rate.is_finite() or rate <= 0:
        raise ValueError("Quote valuation requires a finite value and positive finite rate")
    with localcontext() as context:
        context.prec = len(value.as_tuple().digits) + len(rate.as_tuple().digits) + 2
        context.Emax = MAX_EMAX
        context.Emin = MIN_EMIN
        context.traps[Inexact] = True
        return value * rate


def quote_usd_denomination(value: QuoteDenomination) -> UsdDenomination:
    match value:
        case "QUOTE":
            return "USD"
        case "QUOTE_PER_NATIVE_TOKEN":
            return "USD_PER_NATIVE_TOKEN"
        case _:
            raise ValueError("A quote conversion requires its actual denomination")


def _quote_calculation(native: NativeQuoteValue, context: QuoteValuationContext) -> QuoteValueCalculation:
    if isinstance(context, QuoteNotCapturedValuationContext):
        if not isinstance(native.scope, UnknownQuoteValuationScope):
            raise ValueError("QUOTE_NOT_CAPTURED requires actual unknown quote identity")
        return QuoteValueCalculation(value=None, reason="QUOTE_ASSET_NOT_CAPTURED")
    if isinstance(context, UsdUnitValuationContext):
        if not isinstance(native.scope, QuoteValuationScope) or native.scope.quote_asset != "USD":
            raise ValueError("USD_UNIT requires an actually declared USD quote")
        if native.target_at.value is None:
            return QuoteValueCalculation(value=None, reason="TARGET_CLOCK_NOT_CAPTURED")
        return QuoteValueCalculation(value=native.value, reason=None)
    if isinstance(native.scope, QuoteValuationScope) and native.scope.quote_asset == "USD":
        raise ValueError("Actual USD uses its unit definition without a dummy FX source")
    source = context.source
    source_quote = source.quote_asset if isinstance(source, MissingQuoteUsdRate) else source.scope.quote_asset
    if native.scope.quote_asset is None or source_quote is None:
        return QuoteValueCalculation(value=None, reason="QUOTE_ASSET_NOT_CAPTURED")
    if native.scope.quote_asset != source_quote:
        raise ValueError("A quote rate cannot borrow a different currency's sample")
    if native.target_at.value is None:
        return QuoteValueCalculation(value=None, reason="TARGET_CLOCK_NOT_CAPTURED")
    if isinstance(source, (MissingQuoteUsdRate, UnusableCapturedQuoteUsdRate)):
        return QuoteValueCalculation(value=None, reason=source.reason)
    if source.as_of.value is None:
        return QuoteValueCalculation(value=None, reason="QUOTE_RATE_AS_OF_NOT_CAPTURED")
    age = (parse_iso_time(native.target_at.value) - parse_iso_time(source.as_of.value)).total_seconds()
    if age < 0:
        return QuoteValueCalculation(value=None, reason="QUOTE_RATE_FUTURE")
    if age > context.policy.max_age_s:
        return QuoteValueCalculation(value=None, reason="QUOTE_RATE_STALE")
    try:
        value = quote_value_product(native.value, source.rate)
    except DecimalException:
        return QuoteValueCalculation(value=None, reason="NOT_ESTIMABLE")
    return QuoteValueCalculation(value=value, reason=None)


class QuoteValuationFacts(DomainModel):
    native: NativeQuoteValue
    context: QuoteValuationContext
    calculation: QuoteValueCalculation
    usd_denomination: UsdDenomination

    @model_validator(mode="after")
    def actual_source_and_math(self) -> Self:
        if self.usd_denomination != quote_usd_denomination(self.native.denomination):
            raise ValueError("USD conversion must preserve the native value's denominator")
        if self.calculation != _quote_calculation(self.native, self.context):
            raise ValueError(
                "USD calculation must preserve the actual source clocks, rate and declared policy"
            )
        return self

    @classmethod
    def calculate(cls, native: NativeQuoteValue, context: QuoteValuationContext) -> Self:
        point = NativeQuoteValue.model_validate(native)
        policy = type(context).model_validate(context)
        return cls(
            native=point,
            context=policy,
            calculation=_quote_calculation(point, policy),
            usd_denomination=quote_usd_denomination(point.denomination),
        )


class CapturedQuoteRateVisibility(DomainModel):
    kind: Literal["CAPTURED_RATE_RECEIPT"]
    evidence: QuoteRateEvidenceRef
    received_at: Timestamp


class UnappliedQuoteRateVisibility(DomainModel):
    kind: Literal["USD_UNIT", "QUOTE_NOT_CAPTURED", "NO_USABLE_RATE"]


type QuoteRateVisibility = CapturedQuoteRateVisibility | UnappliedQuoteRateVisibility


def _visibility_failures(
    assembled_at: Timestamp, native_received_at: Timestamp, rate: QuoteRateVisibility
) -> tuple[QuoteVisibilityFailure, ...]:
    failures: list[QuoteVisibilityFailure] = []
    assembly = None if assembled_at.value is None else parse_iso_time(assembled_at.value)
    if assembly is None:
        failures.append("ASSEMBLY_CLOCK_NOT_CAPTURED")
    if native_received_at.value is None:
        failures.append("NATIVE_RECEIPT_NOT_CAPTURED")
    elif assembly is not None and parse_iso_time(native_received_at.value) > assembly:
        failures.append("NATIVE_RECEIPT_AFTER_ASSEMBLY")
    if isinstance(rate, CapturedQuoteRateVisibility):
        if rate.received_at.value is None:
            failures.append("QUOTE_RATE_RECEIPT_NOT_CAPTURED")
        elif assembly is not None and parse_iso_time(rate.received_at.value) > assembly:
            failures.append("QUOTE_RATE_RECEIPT_AFTER_ASSEMBLY")
    return tuple(failures)


class QuoteValuationProof(DomainModel):
    assembled_at: Timestamp
    native_evidence: QuoteRateEvidenceRef
    native_received_at: Timestamp
    rate: QuoteRateVisibility
    failures: tuple[QuoteVisibilityFailure, ...]

    @model_validator(mode="after")
    def actual_visibility(self) -> Self:
        if self.failures != _visibility_failures(self.assembled_at, self.native_received_at, self.rate):
            raise ValueError("Quote visibility must preserve actual source receipts and assembly")
        return self

    @classmethod
    def assess(cls, facts: QuoteValuationFacts, *, assembled_at: Timestamp) -> Self:
        value = QuoteValuationFacts.model_validate(facts)
        assembly = Timestamp.model_validate(assembled_at)
        rate: QuoteRateVisibility
        if isinstance(value.context, UsdUnitValuationContext):
            rate = UnappliedQuoteRateVisibility(kind="USD_UNIT")
        elif isinstance(value.context, QuoteNotCapturedValuationContext):
            rate = UnappliedQuoteRateVisibility(kind="QUOTE_NOT_CAPTURED")
        elif isinstance(value.context.source, CapturedQuoteUsdRate):
            rate = CapturedQuoteRateVisibility(
                kind="CAPTURED_RATE_RECEIPT",
                evidence=value.context.source.evidence,
                received_at=value.context.source.received_at,
            )
        else:
            rate = UnappliedQuoteRateVisibility(kind="NO_USABLE_RATE")
        return cls(
            assembled_at=assembly,
            native_evidence=value.native.evidence,
            native_received_at=value.native.received_at,
            rate=rate,
            failures=_visibility_failures(assembly, value.native.received_at, rate),
        )


class QualifiedQuoteValue(DomainModel):
    facts: QuoteValuationFacts
    proof: QuoteValuationProof

    @model_validator(mode="after")
    def same_consumed_sources(self) -> Self:
        expected = QuoteValuationProof.assess(self.facts, assembled_at=self.proof.assembled_at)
        if self.proof != expected:
            raise ValueError("Quote visibility cannot borrow another source or receipt")
        return self

    @property
    def usd_value(self) -> Decimal | None:
        value = type(self).model_validate(self)
        return None if value.proof.failures else value.facts.calculation.value

    @property
    def unavailable_reasons(self) -> tuple[QuoteValuationFailure, ...]:
        value = type(self).model_validate(self)
        reason = value.facts.calculation.reason
        return (*(() if reason is None else (reason,)), *value.proof.failures)


class QuoteValuationEvidenceLookup(Protocol):
    """Reproject the original read and descriptor, including actual currency and clocks.

    Implementations preserve the requested reference, refuse conflicting/missing source facts,
    and retain original wire extensions outside these consumed-value models. A provider body
    hash cannot substitute for a canonical storage/input read and its qualified scope descriptor.
    """

    def native_quote_value(self, reference: QuoteRateEvidenceRef) -> NativeQuoteValue: ...

    def quote_usd_rate(
        self, reference: QuoteRateEvidenceRef
    ) -> CapturedQuoteUsdRate | UnusableCapturedQuoteUsdRate: ...


def require_clocked_quote_rates(
    values: Iterable[CapturedQuoteUsdRate], *, quote_asset: str
) -> tuple[CapturedQuoteUsdRate, ...]:
    if not quote_asset.strip():
        raise ValueError("A quote history requires its declared currency")
    result: list[CapturedQuoteUsdRate] = []
    for value in values:
        sample = CapturedQuoteUsdRate.model_validate(value)
        if not isinstance(sample.scope, QuoteValuationScope) or sample.scope.quote_asset != quote_asset:
            raise ValueError("A quote history must preserve its actual declared currency scope")
        if sample.as_of.value is None:
            raise ValueError("An unclocked rate cannot become a historical sample")
        result.append(sample)
    return tuple(result)
