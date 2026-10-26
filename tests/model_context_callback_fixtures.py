"""Complete authored callback facts for actual legacy migration behavior controls."""

from decimal import Decimal

from pydantic import JsonValue
from test_timed_review_contracts import armed_review

from tank.analysis.watch_contracts import WatchCall
from tank.contracts.json import decode_json
from tank.runtime_contracts import (
    AgentMessage,
    CallExpiredMessage,
    MilestoneMessage,
    ScoutMessage,
    SourceMessage,
    TimedReviewFiredMessage,
)


def migration_callback(kind: str, event_id: str, item_id: str, session_id: str) -> dict[str, JsonValue]:
    message: AgentMessage
    match kind:
        case "ITEM_ADDED":
            message = SourceMessage(
                kind="ITEM_ADDED", event_id=event_id, item_id=item_id, session_id=session_id
            )
        case "MILESTONE":
            message = MilestoneMessage(
                kind="MILESTONE",
                event_id=event_id,
                milestone="Declared protocol launch",
                verification_required=True,
                instruction="Verify the declared launch against primary evidence.",
            )
        case "CONDITION_FIRED":
            review = armed_review().model_copy(
                update={
                    "event_id": event_id,
                    "session_id": session_id,
                    "status": "FIRED",
                    "fired_at": "2026-10-26T13:00:00Z",
                    "fired_at_prec": "EXACT",
                    "fired_at_prec_reason": None,
                }
            )
            message = TimedReviewFiredMessage(
                kind="CONDITION_FIRED",
                event_id=event_id,
                session_id=session_id,
                condition=review,
                review_at=review.expr.review_at,
                review_question=review.expr.review_question,
            )
        case "CALL_EXPIRED":
            call = WatchCall(
                call_id="call-migration",
                event_id=event_id,
                token_id="T",
                opened_by_alert="alert-migration",
                opened_by_version=1,
                opened_at="2026-10-26T10:00:00Z",
                opened_at_prec="EXACT",
                opened_at_prec_reason=None,
                alert_generated_at="2026-10-26T10:00:00Z",
                alert_generated_at_prec="EXACT",
                alert_generated_at_prec_reason=None,
                entry_ref_price=Decimal("1"),
                invalidation_price=Decimal("0.5"),
                status="CLOSED",
                closed_by="EXPIRED",
                closed_at="2026-10-26T11:00:00Z",
                closed_at_prec="EXACT",
                closed_at_prec_reason=None,
            )
            message = CallExpiredMessage(kind="CALL_EXPIRED", event_id=event_id, call=call)
        case "SCOUT_REVIEW":
            message = ScoutMessage(kind="SCOUT_REVIEW", proposal_id="proposal_legacy", session_id=session_id)
        case _:
            raise ValueError("Unsupported authored migration callback")
    wire = decode_json(message.model_dump_json(by_alias=True, exclude_unset=True))
    if not isinstance(wire, dict):
        raise TypeError("Migration callback requires an object wire document")
    return wire
