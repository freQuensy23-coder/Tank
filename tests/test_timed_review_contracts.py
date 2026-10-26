from pathlib import Path

import jsonschema
import pytest
from pydantic import JsonValue, ValidationError

from tank.analysis.watch_contracts import (
    MissingTimedReviewEvent,
    TimedReviewCondition,
    TimedReviewEventState,
    TimedReviewExpression,
    UnloadedExpiredReviewEvent,
    encode_watch_condition,
    parse_watch_condition,
)
from tank.analysis.watch_policy import evaluate_timed_review
from tank.contracts.json import decode_unique_json
from tank.time_precision import Timestamp


def armed_review() -> TimedReviewCondition:
    return TimedReviewCondition(
        config_version="declared-fixture-config",
        code_version="declared-fixture-code",
        session_config_version=None,
        session_code_version=None,
        model_id=None,
        model_id_basis=None,
        model_id_reason="NOT_CAPTURED",
        prompt_version=None,
        prompt_version_basis=None,
        prompt_version_reason="NOT_CAPTURED",
        prompt_label=None,
        prompt_binding=None,
        agent_version=None,
        agent_version_basis=None,
        agent_version_reason="NOT_CAPTURED",
        session_id="origin-session",
        cond_id="CND-review",
        event_id="event-review",
        token_id=None,
        role="AGENT",
        human_text="Has the counterparty confirmed the integration?",
        agent_defined=True,
        status="ARMED",
        created_by="AGENT",
        created_at="2026-10-26T12:00:00Z",
        created_at_prec="COLLECTION_ONLY",
        created_at_prec_reason=None,
        expires_at="2026-10-26T15:00:00Z",
        expires_at_prec="COLLECTION_ONLY",
        expires_at_prec_reason=None,
        expr=TimedReviewExpression(
            review_at="2026-10-26T13:00:00Z",
            review_question="Has the counterparty confirmed the integration?",
        ),
    )


def cutoff(value: str) -> Timestamp:
    return Timestamp(value=value, precision="EXACT", precision_reason=None)


def test_review_wire_roundtrip_has_real_event_scope_and_no_financial_predicate() -> None:
    review = armed_review()
    wire = encode_watch_condition(review)
    parsed = parse_watch_condition(wire)
    assert isinstance(parsed, TimedReviewCondition) and parsed == review
    assert wire["token_id"] is None and wire["role"] == "AGENT"
    assert wire["expr"] == {
        "review_at": "2026-10-26T13:00:00Z",
        "review_question": review.expr.review_question,
    }
    assert "counters" not in wire and "milestone_id" not in wire
    with pytest.raises(ValidationError):
        parse_watch_condition({**wire, "token_id": "invented-token"})
    with pytest.raises(ValidationError):
        parse_watch_condition(
            {
                **wire,
                "expr": {
                    "review_at": review.expr.review_at,
                    "review_question": review.expr.review_question,
                    "metric": "dc.class",
                },
            }
        )


@pytest.mark.parametrize(
    "due", ["2026-10-26T11:00:00Z", "2026-10-26T12:00:00Z", "2026-10-26T15:00:00Z", "2026-10-26T13:00:00"]
)
def test_review_schedule_refuses_invalid_or_unzoned_boundaries(due: str) -> None:
    review = armed_review()
    with pytest.raises(ValidationError):
        TimedReviewCondition.model_validate(
            review.model_copy(
                update={"expr": {"review_at": due, "review_question": review.expr.review_question}}
            )
        )


@pytest.mark.parametrize("value", [False, 0, 1, 1.0])
def test_agent_defined_review_marker_requires_actual_true(value: JsonValue) -> None:
    with pytest.raises(ValidationError):
        parse_watch_condition({**encode_watch_condition(armed_review()), "agent_defined": value})


def test_review_deadline_and_expiry_use_exact_inclusive_thresholds() -> None:
    review = armed_review()
    present = TimedReviewEventState(kind="PRESENT", event_id=review.event_id, lifecycle="TRACKED")
    assert (
        evaluate_timed_review(review, cutoff=cutoff("2026-10-26T12:59:59Z"), event=present).status == "ARMED"
    )
    assert (
        evaluate_timed_review(review, cutoff=cutoff(review.expr.review_at), event=present).status == "FIRED"
    )
    unloaded = UnloadedExpiredReviewEvent(
        kind="NOT_LOADED", event_id=review.event_id, reason="REVIEW_EXPIRED"
    )
    assert evaluate_timed_review(review, cutoff=cutoff(review.expires_at), event=unloaded).status == "EXPIRED"
    with pytest.raises(ValueError, match="actual event state"):
        evaluate_timed_review(review, cutoff=cutoff(review.expr.review_at), event=unloaded)


def test_review_cancels_missing_and_terminal_events_without_conflating_them() -> None:
    review = armed_review()
    missing = MissingTimedReviewEvent(kind="MISSING", event_id=review.event_id)
    action = evaluate_timed_review(review, cutoff=cutoff(review.expr.review_at), event=missing)
    assert action.status == "CANCELLED" and action.reason == "EVENT_MISSING"
    terminal = TimedReviewEventState(kind="PRESENT", event_id=review.event_id, lifecycle="MERGED")
    cancelled = evaluate_timed_review(review, cutoff=cutoff(review.expr.review_at), event=terminal)
    assert cancelled.status == "CANCELLED" and cancelled.reason == "EVENT_TERMINAL"
    with pytest.raises(ValueError, match="identity"):
        evaluate_timed_review(
            review,
            cutoff=cutoff(review.expr.review_at),
            event=terminal.model_copy(update={"event_id": "other-event"}),
        )


def test_re_evaluation_cannot_refire_a_completed_review() -> None:
    review = armed_review()
    fired = TimedReviewCondition.model_validate(
        review.model_copy(
            update={
                "status": "FIRED",
                "fired_at": review.expr.review_at,
                "fired_at_prec": "EXACT",
                "fired_at_prec_reason": None,
            }
        )
    )
    with pytest.raises(ValueError, match="Only an armed"):
        evaluate_timed_review(
            fired,
            cutoff=cutoff(review.expr.review_at),
            event=TimedReviewEventState(kind="PRESENT", event_id=review.event_id, lifecycle="TRACKED"),
        )


def test_watch_tool_schema_rejects_mixed_actions_and_uses_written_expiry_policy() -> None:
    path = (
        Path(__file__).resolve().parents[1]
        / "config/skills/tank-event-analyst/set_watch_condition.schema.json"
    )
    schema = decode_unique_json(path.read_bytes())
    assert isinstance(schema, dict)
    validator = jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker())
    timed: dict[str, JsonValue] = {
        "action": "ARM",
        "review_at": "2026-10-26T13:00:00Z",
        "review_question": "Has the counterparty confirmed?",
    }
    assert not list(validator.iter_errors(timed))
    assert list(validator.iter_errors({**timed, "token_id": "token"}))
    assert list(
        validator.iter_errors({"action": "DISARM", "cond_id": "CND-review", "review_at": timed["review_at"]})
    )
    assert not list(validator.iter_errors({"action": "DISARM", "cond_id": "CND-review"}))
    properties = schema["properties"]
    assert isinstance(properties, dict) and isinstance(properties["expires_in_hours"], dict)
    assert properties["expires_in_hours"]["default"] == 72


def test_timed_expression_cannot_hide_in_a_metric_group() -> None:
    from tank.analysis.watch_contracts import parse_timed_review_expression

    review = armed_review()
    expression: dict[str, JsonValue] = {
        "review_at": review.expr.review_at,
        "review_question": review.expr.review_question,
    }
    with pytest.raises(ValueError, match="cannot be nested"):
        parse_timed_review_expression({"all": [{"metric": "dc.class"}, {"all": [expression]}]})
    with pytest.raises(ValidationError):
        parse_timed_review_expression({**expression, "metric": "dc.class"})
    assert parse_timed_review_expression({"metric": "dc.class"}) is None
