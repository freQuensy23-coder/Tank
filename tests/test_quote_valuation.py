"""Currency qualification preserves native units, original clocks and fresh visibility."""

from datetime import UTC, datetime, timedelta
from decimal import MAX_EMAX, Decimal
from typing import Literal

import pytest
from pydantic import ValidationError

from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.quote_valuation import (
    CapturedQuoteRateVisibility,
    CapturedQuoteUsdRate,
    MissingQuoteUsdRate,
    NativeQuoteValue,
    QualifiedQuoteValue,
    QuoteDenomination,
    QuoteRateEvidenceRef,
    QuoteValuationFacts,
    QuoteValuationScope,
    UnknownQuoteValuationScope,
    UnusableCapturedQuoteUsdRate,
)
from tank.market.quote_valuation import (
    current_quote_age_policy,
    current_quote_history_policy,
    qualify_captured_quote_value,
    qualify_quote_facts,
    qualify_usd_unit,
    retain_quote_rate_history,
)
from tank.time_precision import Timestamp
from tank.util import iso

AT = datetime(2026, 10, 26, 12, tzinfo=UTC)
VERSIONS = ResolvedVersionIdentity(config_version="synthetic-config-v1", code_version="synthetic-code-v1")


def reported(at: datetime) -> Timestamp:
    return Timestamp(value=iso(at), precision=None, precision_reason="SOURCE_PRECISION_NOT_CAPTURED")


def receipt(at: datetime) -> Timestamp:
    return Timestamp(value=iso(at), precision="COLLECTION_ONLY")


def unknown() -> Timestamp:
    return Timestamp(value=None, precision=None, precision_reason="NOT_CAPTURED")


def evidence(digit: Literal["a", "b", "c"]) -> QuoteRateEvidenceRef:
    return QuoteRateEvidenceRef(
        collection="mkt.klines_1m",
        source_projection_sha256=digit * 64,
        source_evidence_sha256=digit * 64,
    )


def native(value: Decimal, *, quote_asset: str, denomination: QuoteDenomination) -> NativeQuoteValue:
    return NativeQuoteValue(
        scope=QuoteValuationScope(
            kind="DECLARED_QUOTE", instrument_id="opaque-target", token_id="target:T", quote_asset=quote_asset
        ),
        denomination=denomination,
        value=value,
        target_at=reported(AT),
        received_at=receipt(AT + timedelta(seconds=1)),
        evidence=evidence("a"),
    )


def rate(value: Decimal, *, quote_asset: str, as_of: datetime) -> CapturedQuoteUsdRate:
    return CapturedQuoteUsdRate(
        kind="CAPTURED_RATE",
        scope=QuoteValuationScope(
            kind="DECLARED_QUOTE",
            instrument_id="different-source",
            token_id="other:Q",
            quote_asset=quote_asset,
        ),
        rate=value,
        as_of=reported(as_of),
        received_at=receipt(AT + timedelta(seconds=1)),
        evidence=evidence("b"),
    )


@pytest.mark.parametrize(
    ("denomination", "expected"), [("QUOTE", "USD"), ("QUOTE_PER_NATIVE_TOKEN", "USD_PER_NATIVE_TOKEN")]
)
def test_current_analysis_conversion_preserves_denomination_and_cross_instrument_source(
    denomination: QuoteDenomination, expected: str
) -> None:
    point = native(Decimal("12.5"), quote_asset="USDT", denomination=denomination)
    source = rate(Decimal("0.98"), quote_asset="USDT", as_of=AT - timedelta(hours=1))
    value = qualify_captured_quote_value(
        point,
        source,
        policy=current_quote_age_policy(VERSIONS),
        assembled_at=receipt(AT + timedelta(seconds=2)),
    )
    assert value.usd_value == Decimal("12.250")
    assert value.facts.usd_denomination == expected
    assert value.facts.native.scope.instrument_id == "opaque-target"
    assert value.facts.context.kind == "CURRENT_ANALYSIS_CAPTURED_RATE"
    assert value.facts.context.source == source
    assert value.facts.context.source.scope.instrument_id == "different-source"
    assert value.facts.context.source.as_of.precision is None
    assert value.unavailable_reasons == ()


@pytest.mark.parametrize("asset", ["USDT", "USDC", "opaque-USDT"])
def test_unit_valuation_requires_declared_usd_and_never_uses_a_stablecoin_or_id_suffix(asset: str) -> None:
    point = native(Decimal(10), quote_asset=asset, denomination="QUOTE")
    with pytest.raises(ValueError, match="actually declared USD"):
        qualify_usd_unit(point, versions=VERSIONS, assembled_at=receipt(AT + timedelta(seconds=2)))
    value = qualify_captured_quote_value(
        point,
        MissingQuoteUsdRate(kind="NO_CAPTURED_SAMPLE", quote_asset=asset, reason="QUOTE_RATE_NOT_CAPTURED"),
        policy=current_quote_age_policy(VERSIONS),
        assembled_at=receipt(AT + timedelta(seconds=2)),
    )
    assert value.usd_value is None and value.facts.native.value == Decimal(10)
    assert value.unavailable_reasons == ("QUOTE_RATE_NOT_CAPTURED",)


def test_actual_usd_unit_uses_no_dummy_sample_and_keeps_known_zero() -> None:
    point = native(Decimal(0), quote_asset="USD", denomination="QUOTE")
    value = qualify_usd_unit(point, versions=VERSIONS, assembled_at=receipt(AT + timedelta(seconds=2)))
    assert value.usd_value == Decimal(0)
    assert value.facts.context.kind == "USD_UNIT"
    assert value.proof.rate.kind == "USD_UNIT"
    assert value.unavailable_reasons == ()


@pytest.mark.parametrize(
    ("delta", "expected"),
    [
        (timedelta(hours=-6), None),
        (timedelta(hours=-6, microseconds=-1), "QUOTE_RATE_STALE"),
        (timedelta(microseconds=1), "QUOTE_RATE_FUTURE"),
    ],
)
def test_reported_asof_age_has_exact_six_hour_boundary(
    delta: timedelta, expected: Literal["QUOTE_RATE_STALE", "QUOTE_RATE_FUTURE"] | None
) -> None:
    point = native(Decimal(3), quote_asset="USDT", denomination="QUOTE")
    source = rate(Decimal(2), quote_asset="USDT", as_of=AT + delta)
    value = qualify_captured_quote_value(
        point,
        source,
        policy=current_quote_age_policy(VERSIONS),
        assembled_at=receipt(AT + timedelta(seconds=2)),
    )
    assert value.facts.native.value == Decimal(3)
    assert value.facts.context.kind == "CURRENT_ANALYSIS_CAPTURED_RATE"
    assert value.facts.context.source == source
    if expected is None:
        assert value.usd_value == Decimal(6)
    else:
        assert value.usd_value is None
        assert value.unavailable_reasons == (expected,)


def test_receipt_visibility_is_fresh_context_and_does_not_relabel_source_facts() -> None:
    point = native(Decimal(3), quote_asset="USDT", denomination="QUOTE")
    source = rate(Decimal(2), quote_asset="USDT", as_of=AT)
    source = CapturedQuoteUsdRate.model_validate(
        source.model_copy(update={"received_at": receipt(AT + timedelta(seconds=5))})
    )
    early = qualify_captured_quote_value(
        point,
        source,
        policy=current_quote_age_policy(VERSIONS),
        assembled_at=receipt(AT + timedelta(seconds=2)),
    )
    cached = early.facts.model_dump_json()
    assert early.facts.calculation.value == Decimal(6)
    assert early.usd_value is None
    assert early.unavailable_reasons == ("QUOTE_RATE_RECEIPT_AFTER_ASSEMBLY",)
    later = qualify_quote_facts(early.facts, assembled_at=receipt(AT + timedelta(seconds=5)))
    assert later.usd_value == Decimal(6)
    assert later.facts.model_dump_json() == cached
    assert later.proof.assembled_at != early.proof.assembled_at
    assert later.proof.rate == early.proof.rate


@pytest.mark.parametrize("missing", ["target", "native_receipt", "rate_asof", "rate_receipt", "assembly"])
def test_missing_clock_never_borrows_another_occurrence_or_processing_clock(missing: str) -> None:
    point = native(Decimal(3), quote_asset="USDT", denomination="QUOTE")
    source = rate(Decimal(2), quote_asset="USDT", as_of=AT)
    assembly = receipt(AT + timedelta(seconds=2))
    if missing == "target":
        point = NativeQuoteValue.model_validate(point.model_copy(update={"target_at": unknown()}))
        reason = "TARGET_CLOCK_NOT_CAPTURED"
    elif missing == "native_receipt":
        point = NativeQuoteValue.model_validate(point.model_copy(update={"received_at": unknown()}))
        reason = "NATIVE_RECEIPT_NOT_CAPTURED"
    elif missing == "rate_asof":
        source = CapturedQuoteUsdRate.model_validate(source.model_copy(update={"as_of": unknown()}))
        reason = "QUOTE_RATE_AS_OF_NOT_CAPTURED"
    elif missing == "rate_receipt":
        source = CapturedQuoteUsdRate.model_validate(source.model_copy(update={"received_at": unknown()}))
        reason = "QUOTE_RATE_RECEIPT_NOT_CAPTURED"
    else:
        assembly = unknown()
        reason = "ASSEMBLY_CLOCK_NOT_CAPTURED"
    value = qualify_captured_quote_value(
        point, source, policy=current_quote_age_policy(VERSIONS), assembled_at=assembly
    )
    assert value.usd_value is None
    assert value.facts.native == point
    assert value.facts.context.kind == "CURRENT_ANALYSIS_CAPTURED_RATE"
    assert value.facts.context.source == source
    assert value.unavailable_reasons == (reason,)


def test_unregistered_quote_identity_preserves_native_value_without_inference() -> None:
    point = native(Decimal(3), quote_asset="USDT", denomination="QUOTE_PER_NATIVE_TOKEN")
    point = NativeQuoteValue.model_validate(
        point.model_copy(
            update={
                "scope": UnknownQuoteValuationScope(
                    kind="QUOTE_NOT_CAPTURED",
                    instrument_id="opaque-USDT",
                    token_id="target:T",
                    quote_asset=None,
                    reason="QUOTE_ASSET_NOT_CAPTURED",
                )
            }
        )
    )
    value = qualify_captured_quote_value(
        point,
        MissingQuoteUsdRate(kind="NO_CAPTURED_SAMPLE", quote_asset=None, reason="QUOTE_RATE_NOT_CAPTURED"),
        policy=current_quote_age_policy(VERSIONS),
        assembled_at=receipt(AT + timedelta(seconds=2)),
    )
    assert value.usd_value is None and value.facts.native.value == Decimal(3)
    assert value.unavailable_reasons == ("QUOTE_ASSET_NOT_CAPTURED",)


def test_foreign_quote_sample_and_forged_math_or_receipt_refuse() -> None:
    point = native(Decimal(3), quote_asset="USDT", denomination="QUOTE")
    source = rate(Decimal(2), quote_asset="USDC", as_of=AT)
    with pytest.raises(ValueError, match="different currency"):
        qualify_captured_quote_value(
            point,
            source,
            policy=current_quote_age_policy(VERSIONS),
            assembled_at=receipt(AT + timedelta(seconds=2)),
        )
    source = rate(Decimal(2), quote_asset="USDT", as_of=AT)
    value = qualify_captured_quote_value(
        point,
        source,
        policy=current_quote_age_policy(VERSIONS),
        assembled_at=receipt(AT + timedelta(seconds=2)),
    )
    bad = value.facts.model_copy(
        update={"calculation": value.facts.calculation.model_copy(update={"value": Decimal(99)})}
    )
    with pytest.raises(ValidationError, match="actual source clocks, rate"):
        qualify_quote_facts(bad, assembled_at=receipt(AT + timedelta(seconds=2)))
    assert isinstance(value.proof.rate, CapturedQuoteRateVisibility)
    proof = value.proof.model_copy(
        update={"rate": value.proof.rate.model_copy(update={"evidence": evidence("c")})}
    )
    with pytest.raises(ValidationError, match="another source or receipt"):
        QualifiedQuoteValue.model_validate(value.model_copy(update={"proof": proof}))
    with pytest.raises(ValidationError, match="denominator"):
        QuoteValuationFacts.model_validate(
            value.facts.model_copy(update={"usd_denomination": "USD_PER_NATIVE_TOKEN"})
        )


def test_finite_decimal_product_preserves_subfloat_values_and_unrepresentable_product_has_reason() -> None:
    point = native(Decimal("1e-400"), quote_asset="USDT", denomination="QUOTE")
    source = rate(Decimal("1e-400"), quote_asset="USDT", as_of=AT)
    value = qualify_captured_quote_value(
        point,
        source,
        policy=current_quote_age_policy(VERSIONS),
        assembled_at=receipt(AT + timedelta(seconds=2)),
    )
    assert value.usd_value == Decimal("1e-800")
    extreme = Decimal((0, (1,), MAX_EMAX))
    point = NativeQuoteValue.model_validate(point.model_copy(update={"value": extreme}))
    source = CapturedQuoteUsdRate.model_validate(source.model_copy(update={"rate": extreme}))
    unavailable = qualify_captured_quote_value(
        point,
        source,
        policy=current_quote_age_policy(VERSIONS),
        assembled_at=receipt(AT + timedelta(seconds=2)),
    )
    assert unavailable.usd_value is None
    assert unavailable.facts.native.value == extreme
    assert unavailable.unavailable_reasons == ("NOT_ESTIMABLE",)


def test_history_keeps_four_days_by_quote_and_preserves_equal_clock_witnesses() -> None:
    newest = rate(Decimal(2), quote_asset="USDT", as_of=AT)
    boundary = rate(Decimal(3), quote_asset="USDT", as_of=AT - timedelta(days=4))
    duplicate = CapturedQuoteUsdRate.model_validate(newest.model_copy(update={"evidence": evidence("c")}))
    old = rate(Decimal(4), quote_asset="USDT", as_of=AT - timedelta(days=4, microseconds=1))
    retained = retain_quote_rate_history(
        (newest, boundary, old, duplicate), quote_asset="USDT", policy=current_quote_history_policy(VERSIONS)
    )
    assert retained == (boundary, newest, duplicate)
    assert newest.evidence != duplicate.evidence
    with pytest.raises(ValueError, match="actual declared currency"):
        retain_quote_rate_history(
            (newest,), quote_asset="USDC", policy=current_quote_history_policy(VERSIONS)
        )
    unclocked = CapturedQuoteUsdRate.model_validate(newest.model_copy(update={"as_of": unknown()}))
    with pytest.raises(ValueError, match="unclocked rate"):
        retain_quote_rate_history(
            (unclocked,), quote_asset="USDT", policy=current_quote_history_policy(VERSIONS)
        )


def test_bad_captured_rate_remains_an_explicit_source_failure() -> None:
    point = native(Decimal(0), quote_asset="USDT", denomination="QUOTE")
    invalid = UnusableCapturedQuoteUsdRate(
        kind="UNUSABLE_CAPTURED_RATE", scope=point.scope, evidence=evidence("b"), reason="QUOTE_RATE_INVALID"
    )
    value = qualify_captured_quote_value(
        point,
        invalid,
        policy=current_quote_age_policy(VERSIONS),
        assembled_at=receipt(AT + timedelta(seconds=2)),
    )
    assert value.facts.native.value == Decimal(0) and value.usd_value is None
    assert value.unavailable_reasons == ("QUOTE_RATE_INVALID",)
    for bad in (Decimal(0), Decimal("NaN")):
        with pytest.raises(ValidationError):
            CapturedQuoteUsdRate.model_validate(
                rate(Decimal(1), quote_asset="USDT", as_of=AT).model_copy(update={"rate": bad})
            )


def test_declared_policy_rejects_float_or_bool_substitutes_and_unresolved_versions() -> None:
    policy = current_quote_age_policy(VERSIONS)
    with pytest.raises(ValidationError):
        type(policy).model_validate(policy.model_copy(update={"max_age_s": 21600.0}))
    with pytest.raises(ValidationError):
        type(policy).model_validate(policy.model_copy(update={"max_age_s": True}))
    with pytest.raises(ValidationError):
        current_quote_age_policy(VERSIONS.model_copy(update={"code_version": "unversioned"}))
