"""Worker boundary validation refuses malformed durable commands before side effects."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import JsonValue, ValidationError

from tank.runtime import Runtime
from tank.runtime_contracts import (
    EscalateTokensJob,
    ItemJob,
    MarketTruthJob,
    ScoutReviewJob,
    parse_agent_message,
    parse_delivery_job,
)


@pytest.mark.parametrize("tokens", [[], ["T", 3], "T", None])
async def test_escalation_refuses_invalid_tokens_before_market_side_effect(tokens: JsonValue) -> None:
    runtime = object.__new__(Runtime)
    runtime.market = SimpleNamespace(escalate=AsyncMock())
    with pytest.raises(ValidationError):
        await runtime.escalate_wire({"event_id": "event", "tokens": tokens})
    runtime.market.escalate.assert_not_awaited()


def test_escalation_requires_explicit_token_set_instead_of_single_token_fallback() -> None:
    with pytest.raises(ValidationError):
        EscalateTokensJob.model_validate({"event_id": "event", "token_id": "T"})


def test_cutoff_cannot_invent_precision_when_queued_metadata_is_missing() -> None:
    with pytest.raises(ValidationError, match="explicit reason"):
        MarketTruthJob(event_id="event", token_id="T", cutoff="2026-10-20T12:00:00Z")


def test_unknown_cutoff_precision_preserves_explicit_reason() -> None:
    command = MarketTruthJob(
        event_id="event",
        token_id="T",
        cutoff="2026-10-20T12:00:00Z",
        cutoff_prec_reason="PROVIDER_PRECISION_UNAVAILABLE",
    )
    assert command.cutoff_prec is None
    assert command.cutoff_prec_reason == "PROVIDER_PRECISION_UNAVAILABLE"


def test_dependency_metadata_distinguishes_absent_and_declared_empty() -> None:
    absent = ItemJob(item_id="item")
    declared = ItemJob.model_validate({"item_id": "item", "_depends_on": []})
    assert "_depends_on" not in absent.model_dump(by_alias=True, exclude_unset=True)
    assert declared.model_dump(by_alias=True, exclude_unset=True)["_depends_on"] == []


def test_delivery_has_one_explicit_destination_and_subject() -> None:
    with pytest.raises(ValidationError):
        parse_delivery_job({"alert_id": "alert", "version": 1})
    with pytest.raises(ValidationError):
        parse_delivery_job(
            {"alert_id": "alert", "version": 1, "notice_id": "notice", "channel": "telegram:ops"}
        )


def test_agent_message_refuses_unknown_fields_and_kinds() -> None:
    with pytest.raises(ValidationError):
        parse_agent_message({"kind": "NEW_ITEM", "item_id": "item", "credentials": "never"})
    with pytest.raises(ValueError, match="Unsupported"):
        parse_agent_message({"kind": "UNREGISTERED"})


def test_durable_scout_alias_normalizes_to_one_canonical_field() -> None:
    old = ScoutReviewJob.model_validate({"prp_id": "proposal"})
    assert old.proposal_id == "proposal"
    assert old.model_dump() == {"depends_on": [], "proposal_id": "proposal"}
    with pytest.raises(ValidationError):
        ScoutReviewJob.model_validate({"prp_id": "one", "proposal_id": "two"})
