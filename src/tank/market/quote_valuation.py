"""Pure current-analysis quote valuation and the existing retained-history policy."""

from collections.abc import Sequence
from datetime import datetime, timedelta

from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.quote_valuation import (
    CapturedQuoteUsdRate,
    CapturedRateValuationContext,
    NativeQuoteValue,
    QualifiedQuoteValue,
    QuoteNotCapturedValuationContext,
    QuoteRateAgePolicy,
    QuoteRateHistoryPolicy,
    QuoteUsdRateSource,
    QuoteValuationFacts,
    QuoteValuationProof,
    UsdUnitValuationContext,
    require_clocked_quote_rates,
)
from tank.time_precision import Timestamp
from tank.util import parse_iso_time


def current_quote_age_policy(versions: ResolvedVersionIdentity) -> QuoteRateAgePolicy:
    return QuoteRateAgePolicy(
        versions=ResolvedVersionIdentity.model_validate(versions),
        max_age_s=21600,
        time_basis="REPORTED_AS_OF_AND_TARGET",
    )


def current_quote_history_policy(versions: ResolvedVersionIdentity) -> QuoteRateHistoryPolicy:
    return QuoteRateHistoryPolicy(
        versions=ResolvedVersionIdentity.model_validate(versions),
        retention_s=345600,
        time_basis="NEWEST_REPORTED_AS_OF",
    )


def qualify_quote_facts(facts: QuoteValuationFacts, *, assembled_at: Timestamp) -> QualifiedQuoteValue:
    value = QuoteValuationFacts.model_validate(facts)
    return QualifiedQuoteValue(
        facts=value,
        proof=QuoteValuationProof.assess(value, assembled_at=assembled_at),
    )


def qualify_usd_unit(
    native: NativeQuoteValue, *, versions: ResolvedVersionIdentity, assembled_at: Timestamp
) -> QualifiedQuoteValue:
    facts = QuoteValuationFacts.calculate(
        native,
        UsdUnitValuationContext(kind="USD_UNIT", versions=versions),
    )
    return qualify_quote_facts(facts, assembled_at=assembled_at)


def qualify_unknown_quote(
    native: NativeQuoteValue, *, versions: ResolvedVersionIdentity, assembled_at: Timestamp
) -> QualifiedQuoteValue:
    facts = QuoteValuationFacts.calculate(
        native,
        QuoteNotCapturedValuationContext(kind="QUOTE_NOT_CAPTURED", versions=versions),
    )
    return qualify_quote_facts(facts, assembled_at=assembled_at)


def qualify_captured_quote_value(
    native: NativeQuoteValue,
    source: QuoteUsdRateSource,
    *,
    policy: QuoteRateAgePolicy,
    assembled_at: Timestamp,
) -> QualifiedQuoteValue:
    facts = QuoteValuationFacts.calculate(
        native,
        CapturedRateValuationContext(kind="CURRENT_ANALYSIS_CAPTURED_RATE", policy=policy, source=source),
    )
    return qualify_quote_facts(facts, assembled_at=assembled_at)


def retain_quote_rate_history(
    values: Sequence[CapturedQuoteUsdRate], *, quote_asset: str, policy: QuoteRateHistoryPolicy
) -> tuple[CapturedQuoteUsdRate, ...]:
    declared = QuoteRateHistoryPolicy.model_validate(policy)
    samples = require_clocked_quote_rates(values, quote_asset=quote_asset)
    if not samples:
        return ()
    dated: list[tuple[datetime, CapturedQuoteUsdRate]] = []
    for sample in samples:
        if sample.as_of.value is None:
            raise ValueError("Historical samples require their actual clocks")
        dated.append((parse_iso_time(sample.as_of.value), sample))
    newest = max(clock for clock, _ in dated)
    lower = newest - timedelta(seconds=declared.retention_s)
    return tuple(sample for clock, sample in sorted(dated, key=lambda entry: entry[0]) if clock >= lower)
