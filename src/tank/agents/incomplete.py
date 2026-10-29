"""Source items whose agent turn ended without the required outcome, and their re-analysis.

A routed item is finished by the agent only through record_event (an event or an explicit
rejection) or add_observation. A turn that ends without either gets a completion reminder in the
same session; once the session's reminder budget is spent it is an agent failure, not a
rejection: the item gets extraction_status FAILED with extraction_reason AGENT_INCOMPLETE,
Tank Ops shows it as a processing error, and the operator can queue it for re-analysis. A session
that fails terminally while its item is still pending fails the item with SESSION_FAILED, which
is re-analysable the same way."""

from tank.storage import DECISION_MAX_ATTEMPTS
from tank.time_precision import collection_clock

AGENT_INCOMPLETE = "AGENT_INCOMPLETE"
LEGACY_INCOMPLETE = "NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION"
SESSION_FAILED = "SESSION_FAILED"
LEGACY_SESSION_FAILED = "AGENT_REMINDED"
UNFINISHED = {None, "PENDING", "RUNNING"}


async def fail_incomplete_turn(repo, session, item_id, **details):
    """Record that the agent ended its turn on item_id without an event, observation or rejection.

    Call it once the turn is settled and no further reminder will be sent (the reminder budget is
    exhausted). The item becomes FAILED / AGENT_INCOMPLETE; an item the agent already finished is
    left unchanged. A session that never recorded an event is flagged agent_incomplete, so the
    idle sweep archives it through the normal retention path instead of leaving it open; the
    caller persists the session. details are stored on the item with the failure, such as the
    spent reminder budget. Returns whether the item was marked."""
    item = await repo.get("ingest.source_items", item_id)
    if item is None or item.get("extraction_status") not in UNFINISHED:
        return False
    item.update(
        extraction_status="FAILED",
        extraction_reason=AGENT_INCOMPLETE,
        extraction_failed_session_id=session["session_id"],
        **details,
        **collection_clock().fields("extraction_failed_at"),
    )
    await repo.put("ingest.source_items", item_id, item)
    if not session.get("event_id"):
        session["agent_incomplete"] = True
    return True


async def fail_with_session(repo, session, item_id, failure):
    """Record that the session handling item_id failed terminally while the item was pending.

    The item becomes FAILED / SESSION_FAILED with the native error type, replacing any transient
    AGENT_REMINDED reason; an item that is no longer pending is left unchanged. Returns whether
    the item was marked."""
    item = await repo.get("ingest.source_items", item_id)
    if item is None or item.get("extraction_status") != "PENDING":
        return False
    error = failure.get("error")
    item.update(
        extraction_status="FAILED",
        extraction_reason=SESSION_FAILED,
        extraction_error_type=(error.get("type") if isinstance(error, dict) else None)
        or failure.get("event_type"),
        extraction_failed_session_id=session["session_id"],
        **collection_clock().fields("extraction_failed_at"),
    )
    await repo.put("ingest.source_items", item_id, item)
    return True


def reanalysable(item):
    """An item whose analysis ended without an outcome: an incomplete turn, a failed session, or
    their legacy forms (an unobserved native turn, a session failure that kept AGENT_REMINDED)."""
    return (item.get("extraction_status"), item.get("extraction_reason")) in {
        ("FAILED", AGENT_INCOMPLETE),
        ("FAILED", SESSION_FAILED),
        ("FAILED", LEGACY_SESSION_FAILED),
        ("SKIPPED", LEGACY_INCOMPLETE),
    }


async def retire_item_sessions(repo, item_id, attempt):
    """Close the item's open eventless sessions before it is analysed again.

    Such a session holds only this item, so the re-analysis must not continue it: it is flagged
    agent_incomplete, routing no longer joins it and the idle sweep archives it through the normal
    retention path. A session that recorded an event, a closed session and a replay session are
    left unchanged. Returns the flagged session ids."""
    ids = [
        session["session_id"]
        async for session in repo.iter_all("ops.agent_sessions", filters={"item_id": item_id})
    ]
    retired = []
    for sid in sorted(ids):
        async with repo.lock("session:" + sid):
            session = await repo.get("ops.agent_sessions", sid)
            if (
                session is None
                or session.get("event_id")
                or session.get("replay_run_id")
                or session.get("agent_incomplete")
                or session.get("status") in {"FAILED", "ENDED"}
                or session.get("archived_at")
            ):
                continue
            session.update(agent_incomplete=True, superseded_by_reanalysis=attempt)
            await repo.put("ops.agent_sessions", sid, session)
            retired.append(sid)
    return retired


async def reanalyse(repo, *, item_ids=None, limit=100, apply=False):
    """Queue incomplete items for a fresh analysis turn; without apply, only report them.

    Each queued item returns to PENDING and is routed again under a new routing attempt, so the
    route can pick an event recorded meanwhile and the agent message is a new input. The item's
    open eventless sessions are closed first, and a message routed by the item alone carries the
    attempt, so it always opens a fresh session for this attempt instead of resuming an old one."""
    candidates = []
    if item_ids:
        for item_id in item_ids:
            item = await repo.get("ingest.source_items", item_id)
            if item is not None and reanalysable(item):
                candidates.append(item)
    else:
        for status in ("FAILED", "SKIPPED"):
            async for item in repo.iter_all("ingest.source_items", filters={"extraction_status": status}):
                if reanalysable(item):
                    candidates.append(item)
    candidates.sort(key=lambda row: (str(row.get("observed_at") or ""), row["item_id"]))
    candidates = candidates[:limit]
    rows = [
        {
            "item_id": item["item_id"],
            "extraction_status": item.get("extraction_status"),
            "extraction_reason": item.get("extraction_reason"),
            "observed_at": item.get("observed_at"),
            "reanalysis_attempt": item.get("reanalysis_attempt", 0) + 1,
        }
        for item in candidates
    ]
    if apply:
        for row in rows:
            async with repo.lock("routing:" + row["item_id"]):
                item = await repo.get("ingest.source_items", row["item_id"])
                if item is None or not reanalysable(item):
                    row["queued"] = False
                    continue
                attempt = item.get("reanalysis_attempt", 0) + 1
                item.setdefault("reanalysis_history", []).append(
                    {
                        "attempt": attempt,
                        "previous_status": item.get("extraction_status"),
                        "previous_reason": item.get("extraction_reason"),
                        "previous_route": item.get("route"),
                        **collection_clock().fields("requested_at"),
                    }
                )
                for key in (
                    "routing_message",
                    "route",
                    "routing_scores",
                    "route_event_id",
                    "extraction_reminders",
                    "extraction_reminder_limit",
                ):
                    item.pop(key, None)
                item.update(extraction_status="PENDING", extraction_reason=None, reanalysis_attempt=attempt)
                await repo.put("ingest.source_items", item["item_id"], item)
                row["retired_sessions"] = await retire_item_sessions(repo, item["item_id"], attempt)
                await repo.enqueue(
                    "ROUTE_ITEM",
                    {"item_id": item["item_id"]},
                    f"{item['item_id']}:reanalysis:{attempt}",
                    max_attempts=DECISION_MAX_ATTEMPTS,
                )
                row["queued"] = True
    return {"apply": apply, "count": len(rows), "items": rows}
