"""Lossless domain values with a referenced redundant metadata companion.

The persisted tool result remains authoritative and complete. This projection is
only the JSON sent to OpenMA for get_market_truth; it never caps arrays, strings,
venues, windows, evidence, or confidence records.
"""


def tool_wire_result(tool, result, *, session_id, call_id):
    if tool != "get_market_truth":
        return result
    omitted = []
    record_ids = set()
    if not isinstance(result, dict) or not isinstance(result.get("market_truth"), list):
        return result
    # Only the explicit producer shape contains the redundant companion. A
    # similarly named evidence/raw field is canonical data and stays untouched.
    projected = dict(result)
    truths = []
    for index, truth in enumerate(result["market_truth"]):
        if not isinstance(truth, dict) or "measurement_metadata" not in truth:
            truths.append(truth)
            continue
        clean = {key: value for key, value in truth.items() if key != "measurement_metadata"}
        truths.append(clean)
        omitted.append(f"/market_truth/{index}/measurement_metadata")
        if truth.get("mta_id"):
            record_ids.add(truth["mta_id"])
    projected["market_truth"] = truths
    if not omitted:
        return result
    access = {
        "omitted_companion": "measurement_metadata",
        "omitted_paths": omitted,
        "reason": "Redundant per-leaf measurement index; canonical values and their inline context are unchanged.",
        "records": [
            {
                "collection": "ana.market_truth",
                "mta_id": mta_id,
                "query": "SELECT document->'measurement_metadata' AS measurement_metadata FROM ana.market_truth WHERE mta_id = %s",
                "parameters": [mta_id],
            }
            for mta_id in sorted(record_ids)
        ],
        "full_tool_result": {
            "collection": "ops.tool_calls",
            "session_id": session_id,
            "call_id": call_id,
            "storage_key": session_id + ":" + call_id,
            "query": "SELECT document->'result' AS full_tool_result FROM ops.tool_calls WHERE session_id = %s AND call_id = %s",
            "parameters": [session_id, call_id],
        },
    }
    # All Tank tools return objects; refuse an unexpected shape rather than
    # silently wrapping/replacing any canonical result fields.
    if not isinstance(projected, dict) or "measurement_metadata_access" in projected:
        raise ValueError("Unexpected get_market_truth wire projection shape")
    projected["measurement_metadata_access"] = access
    return projected
