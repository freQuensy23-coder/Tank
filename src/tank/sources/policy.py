"""Source ingestion policy as explicit typed relevance decisions."""

from tank.contracts.source_items import AlwaysRelevance, PendingRelevance, SourceRelevance


def is_filtered_source(source_id: str, subtype: str | None = None, author_role: str | None = None) -> bool:
    return (
        source_id in {"S30", "S31", "S32", "S33", "S34"}
        or (source_id == "S25" and subtype == "commits")
        or (source_id == "S29" and author_role != "ORGANIZATION")
    )


def initial_relevance(
    source_id: str | None, subtype: str | None = None, author_role: str | None = None
) -> SourceRelevance:
    """Replays reset only ingestion policy; uncaptured policy remains explicit unknown."""
    if source_id is None or source_id not in {f"S{number:02d}" for number in range(1, 35)}:
        return PendingRelevance(decision=None, pending_reason="SOURCE_INGESTION_POLICY_NOT_CAPTURED")
    if is_filtered_source(source_id, subtype, author_role):
        return PendingRelevance(decision=None, pending_reason="AWAITING_FILTER")
    return AlwaysRelevance(decision="ALWAYS")
