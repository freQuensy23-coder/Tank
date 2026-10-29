"""Source items whose agent turn ended without the required outcome, and their re-analysis.

A routed item is finished by the agent only through record_event (an event or an explicit
rejection) or add_observation. A turn that ends without either is an agent failure, not a
rejection: the item gets extraction_status FAILED with extraction_reason AGENT_INCOMPLETE,
Tank Ops shows it as a processing error, and the operator can queue it for re-analysis."""

from tank.time_precision import collection_clock

AGENT_INCOMPLETE = "AGENT_INCOMPLETE"
LEGACY_INCOMPLETE = "NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION"
UNFINISHED = {None, "PENDING", "RUNNING"}


async def fail_incomplete_turn(repo, session, item_id):
    """Record that the agent ended its turn on item_id without an event, observation or rejection.

    Call it once the turn is settled and no further reminder will be sent (the reminder budget is
    exhausted). The item becomes FAILED / AGENT_INCOMPLETE; an item the agent already finished is
    left unchanged. A session that never recorded an event is flagged agent_incomplete, so the
    idle sweep archives it through the normal retention path instead of leaving it open; the
    caller persists the session. Returns whether the item was marked."""
    item = await repo.get("ingest.source_items", item_id)
    if item is None or item.get("extraction_status") not in UNFINISHED:
        return False
    item.update(
        extraction_status="FAILED",
        extraction_reason=AGENT_INCOMPLETE,
        extraction_failed_session_id=session["session_id"],
        **collection_clock().fields("extraction_failed_at"),
    )
    await repo.put("ingest.source_items", item_id, item)
    if not session.get("event_id"):
        session["agent_incomplete"] = True
    return True


def reanalysable(item):
    """An item whose agent turn ended without an outcome: the current failure or its legacy form."""
    return (item.get("extraction_status"), item.get("extraction_reason")) in {
        ("FAILED", AGENT_INCOMPLETE),
        ("SKIPPED", LEGACY_INCOMPLETE),
    }


async def reanalyse(repo, *, item_ids=None, limit=100, apply=False):
    """Queue incomplete items for a fresh analysis turn; without apply, only report them.

    Each queued item returns to PENDING and is routed again under a new routing attempt, so the
    route can pick an event recorded meanwhile and the agent message is a new input."""
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
                for key in ("routing_message", "route", "routing_scores", "route_event_id"):
                    item.pop(key, None)
                item.update(extraction_status="PENDING", extraction_reason=None, reanalysis_attempt=attempt)
                await repo.put("ingest.source_items", item["item_id"], item)
                await repo.enqueue(
                    "ROUTE_ITEM",
                    {"item_id": item["item_id"]},
                    f"{item['item_id']}:reanalysis:{attempt}",
                    max_attempts=2,
                )
                row["queued"] = True
    return {"apply": apply, "count": len(rows), "items": rows}
