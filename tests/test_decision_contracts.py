from decimal import Decimal

import pytest
from pydantic import ValidationError

from tank.analysis.decision_contracts import (
    ActiveDecisionTracking,
    DecisionBeneficiary,
    DecisionIdentity,
    DecisionMaterialization,
    DecisionMetadata,
    DecisionPrices,
)
from tank.contracts.provenance import GenerationProvenance, ResolvedVersionIdentity
from tank.time_precision import Timestamp, collection_clock


def decision_metadata_fixture() -> DecisionMetadata:
    versions = ResolvedVersionIdentity(config_version="declared-fixture", code_version="fixture-build")
    generation = GenerationProvenance(
        config_version=versions.config_version,
        code_version=versions.code_version,
        session_config_version=None,
        session_code_version=None,
        model_id=None,
        model_id_basis=None,
        model_id_reason=None,
        prompt_version=None,
        prompt_version_basis=None,
        prompt_version_reason=None,
        prompt_label=None,
        prompt_binding=None,
        agent_version=None,
        agent_version_basis=None,
        agent_version_reason=None,
        session_id=None,
    )
    return DecisionMetadata(
        identity=DecisionIdentity(
            decision_id="DEC-fixture",
            event_id="EVT-fixture",
            token_id="T",
            cutoff=Timestamp("2026-10-25T00:00:00Z", None, "LEGACY_PRECISION_UNAVAILABLE"),
            thresholds_version="declared-thresholds",
            versions=versions,
            generation=generation,
            assessment_id="EMS-fixture",
            market_truth_id="MTA-fixture",
        ),
        calculated_at=collection_clock("2026-10-25T00:01:00Z"),
        management="NORMAL",
        contradiction_ids=(),
        prices=None,
        tracking=ActiveDecisionTracking(kind="ACTIVE"),
    )


def test_materialized_identity_preserves_actual_unknown_cutoff_precision() -> None:
    metadata = decision_metadata_fixture()
    identity = metadata.identity
    assert identity.cutoff.value == "2026-10-25T00:00:00Z"
    assert identity.cutoff.precision is None
    assert identity.cutoff.precision_reason == "LEGACY_PRECISION_UNAVAILABLE"
    with pytest.raises(ValidationError, match="actual market cutoff"):
        DecisionIdentity.model_validate(
            identity.model_copy(update={"cutoff": Timestamp(None, None, "INCOMPLETE")})
        )


def test_materialization_refuses_relabelled_generation_and_missing_calculation_clock() -> None:
    metadata = decision_metadata_fixture()
    generation = metadata.identity.generation.model_copy(update={"code_version": "other-build"})
    with pytest.raises(ValidationError, match="does not match"):
        DecisionIdentity.model_validate(metadata.identity.model_copy(update={"generation": generation}))
    with pytest.raises(ValidationError, match="actual calculation clock"):
        DecisionMetadata.model_validate(
            metadata.model_copy(update={"calculated_at": Timestamp(None, None, "INCOMPLETE")})
        )


def test_watch_allocation_requires_explicit_unique_nonempty_identities() -> None:
    metadata = decision_metadata_fixture()
    assert DecisionMaterialization(metadata=metadata, condition_ids=()).condition_ids == ()
    for ids in (("CND-one", "CND-one"), ("",), (" ",)):
        with pytest.raises(ValidationError, match="identities"):
            DecisionMaterialization(metadata=metadata, condition_ids=ids)


def test_missing_credibility_is_not_inferred_from_role_or_other_evidence() -> None:
    with pytest.raises(ValidationError):
        DecisionBeneficiary.model_validate(
            {
                "role": "PRIMARY",
                "capture_class": "DIRECT",
                "thesis_link_credible": None,
                "exposure_score": 8,
            }
        )


def test_exact_selected_prices_refuse_nonfinite_or_inverted_protection() -> None:
    price = Decimal("100000000000000000.0000000000000000001")
    references = DecisionPrices(entry_ref_price=price, invalidation_price=price)
    assert references.entry_ref_price == price
    with pytest.raises(ValidationError):
        DecisionPrices(entry_ref_price=price, invalidation_price=Decimal("Infinity"))
    with pytest.raises(ValidationError, match="cannot exceed"):
        DecisionPrices(entry_ref_price=Decimal(1), invalidation_price=Decimal(2))
