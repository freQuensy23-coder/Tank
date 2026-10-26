"""Token-free review messages retain their actual saved question through Context 3."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.agents.context import CONTEXT_TRANSITION, CONTEXT_VERSION, ModelContext
from tank.analysis.watch_contracts import TimedReviewCondition, TimedReviewExpression, WatchCondition
from tank.runtime_contracts import ConditionFiredMessage, TimedReviewFiredMessage, parse_agent_message
from tank.storage import MemoryRepository
from tank.util import jsonable


class ReviewContextConfiguration:
    def load_config(self, name: str) -> dict[str, JsonValue]:
        if name == "registry":
            return {"tokens": [], "entities": [], "instruments": []}
        if name == "sources":
            return {"sources": []}
        raise AssertionError("Unexpected context configuration read")

    def read_config_json(self, name: str) -> JsonValue:
        raise AssertionError("Review projection must not load a tool schema")


def fired_review() -> TimedReviewCondition:
    return TimedReviewCondition(
        cond_id="CND_synthetic-review",
        event_id="event",
        token_id=None,
        role="AGENT",
        agent_defined=True,
        created_by="AGENT",
        human_text="Did the regulator publish its stated decision?",
        expr=TimedReviewExpression(
            review_at="2026-10-26T10:00:00Z",
            review_question="Did the regulator publish its stated decision?",
        ),
        status="FIRED",
        created_at="2026-10-26T09:00:00Z",
        created_at_prec="COLLECTION_ONLY",
        created_at_prec_reason=None,
        expires_at="2026-10-26T11:00:00Z",
        expires_at_prec="EXACT",
        expires_at_prec_reason="Declared synthetic review expiry",
        fired_at="2026-10-26T10:05:00Z",
        fired_at_prec="COLLECTION_ONLY",
        fired_at_prec_reason=None,
        last_evaluated_at="2026-10-26T10:05:00Z",
        last_evaluated_at_prec="COLLECTION_ONLY",
        last_evaluated_at_prec_reason=None,
        config_version="synthetic-config",
        code_version="synthetic-code",
        session_config_version=None,
        session_code_version=None,
        model_id=None,
        model_id_basis=None,
        model_id_reason="Synthetic review has no observed model",
        prompt_version=None,
        prompt_version_basis=None,
        prompt_version_reason="Synthetic review has no observed prompt",
        prompt_label=None,
        prompt_binding=None,
        agent_version=None,
        agent_version_basis=None,
        agent_version_reason="Synthetic review has no observed agent version",
        session_id=None,
    )


def review_message() -> TimedReviewFiredMessage:
    condition = fired_review()
    return TimedReviewFiredMessage(
        kind="CONDITION_FIRED",
        event_id=condition.event_id,
        condition=condition,
        review_at=condition.expr.review_at,
        review_question=condition.expr.review_question,
    )


def test_runtime_decoder_admits_closed_timed_review_without_fabricating_a_token() -> None:
    message = review_message()
    wire: dict[str, JsonValue] = message.model_dump(mode="json", exclude_unset=True)
    parsed = parse_agent_message(wire)
    assert isinstance(parsed, TimedReviewFiredMessage)
    assert parsed == message
    assert "token_id" not in wire and parsed.condition.token_id is None


@pytest.mark.parametrize(
    "changes",
    [
        {"event_id": "another-event"},
        {"review_question": "An unrelated question"},
        {"review_at": "2026-10-26T10:01:00Z"},
        {"token_id": "invented-token"},
    ],
)
def test_timed_review_rejects_conflicting_or_mixed_message_facts(changes: dict[str, JsonValue]) -> None:
    wire: dict[str, JsonValue] = review_message().model_dump(mode="json", exclude_unset=True)
    with pytest.raises(ValidationError):
        parse_agent_message({**wire, **changes})


def test_unfired_snapshot_cannot_authorize_a_review_notification() -> None:
    condition: dict[str, JsonValue] = fired_review().model_dump(mode="json")
    condition.update(status="ARMED", fired_at=None, fired_at_prec=None, fired_at_prec_reason=None)
    armed = TimedReviewCondition.model_validate(condition)
    with pytest.raises(ValidationError, match="fired event snapshot"):
        TimedReviewFiredMessage(
            kind="CONDITION_FIRED",
            event_id=armed.event_id,
            condition=armed,
            review_at=armed.expr.review_at,
            review_question=armed.expr.review_question,
        )


@pytest.mark.parametrize("target", [None, "another-session"])
def test_known_review_origin_cannot_be_omitted_or_replaced(target: str | None) -> None:
    fields: dict[str, JsonValue] = fired_review().model_dump(mode="json")
    fields["session_id"] = "origin-session"
    condition = TimedReviewCondition.model_validate(fields)
    with pytest.raises(ValidationError, match="originating session"):
        TimedReviewFiredMessage(
            kind="CONDITION_FIRED",
            event_id=condition.event_id,
            condition=condition,
            review_at=condition.expr.review_at,
            review_question=condition.expr.review_question,
            session_id=target,
        )
    matched = TimedReviewFiredMessage(
        kind="CONDITION_FIRED",
        event_id=condition.event_id,
        condition=condition,
        review_at=condition.expr.review_at,
        review_question=condition.expr.review_question,
        session_id="origin-session",
    )
    assert matched.session_id == condition.session_id


def test_metric_firing_keeps_its_actual_token_and_requires_its_fired_phase() -> None:
    fields: dict[str, JsonValue] = fired_review().model_dump(mode="json")
    fields.update(
        token_id="world:WLD",
        human_text="Watch the event state",
        expr={
            "metric": "event.state",
            "window": "EVENT",
            "op": "==",
            "value": "LIVE",
            "persist": {"count": 1, "unit": "consecutive_windows"},
        },
    )
    metric = WatchCondition.model_validate(fields)
    message = ConditionFiredMessage(
        kind="CONDITION_FIRED", event_id=metric.event_id, token_id=metric.token_id, condition=metric
    )
    parsed = parse_agent_message(message.model_dump(mode="json", exclude_unset=True))
    assert isinstance(parsed, ConditionFiredMessage) and parsed.token_id == "world:WLD"
    fields.update(status="ARMED", fired_at=None, fired_at_prec=None, fired_at_prec_reason=None)
    armed = WatchCondition.model_validate(fields)
    with pytest.raises(ValidationError, match="fired snapshot"):
        ConditionFiredMessage(
            kind="CONDITION_FIRED", event_id=armed.event_id, token_id=armed.token_id, condition=armed
        )


@pytest.mark.asyncio
async def test_context_three_projects_saved_review_question_and_schedule_without_token() -> None:
    repo = MemoryRepository()
    message = review_message()
    await repo.put(
        "evt.events",
        "event",
        {"event_id": "event", "statement": "Regulatory decision pending", "lifecycle": "MONITORING"},
    )
    context = ModelContext(
        repo,
        ReviewContextConfiguration(),
        {"session_id": "session", "event_id": "event", "status": "OPEN"},
    )
    public = await context.message(jsonable(message), None)
    body = public["message"]
    assert isinstance(body, dict)
    assert body["review_question"] == message.review_question
    assert body["review_at"] == message.review_at
    condition = body["condition"]
    assert isinstance(condition, dict)
    description = condition["condition"]
    assert isinstance(description, dict)
    assert description["conditions"] == message.condition.expr.model_dump(mode="json")
    assert description["role"] == "AGENT" and "token" not in description
    assert "token_id" not in body and "token_id" not in condition
    assert "event_id" not in body and "cond_id" not in condition
    assert CONTEXT_VERSION == 3
    assert "PRELIMINARY" in CONTEXT_TRANSITION[False]
    assert "do not go directly to get_decision" in CONTEXT_TRANSITION[False]
