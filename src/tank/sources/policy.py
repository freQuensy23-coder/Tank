"""Shared ingestion policy; no model classification or network calls."""


def is_filtered_source(source_id: object, subtype: object = None, author_role: object = None) -> bool:
    return (
        source_id in {"S30", "S31", "S32", "S33", "S34"}
        or (source_id == "S25" and subtype == "commits")
        or (source_id == "S29" and author_role != "ORGANIZATION")
    )


def initial_relevance(
    source_id: object, subtype: object = None, author_role: object = None
) -> dict[str, str | None]:
    """Reset only ingestion policy when a fresh replay drops old classifier outputs."""
    if source_id not in {f"S{number:02d}" for number in range(1, 35)}:
        return {"decision": None, "pending_reason": "SOURCE_INGESTION_POLICY_NOT_CAPTURED"}
    if is_filtered_source(source_id, subtype, author_role):
        return {"decision": None, "pending_reason": "AWAITING_FILTER"}
    return {"decision": "ALWAYS"}
