"""Read historical recovery receipts without changing captured source evidence."""

from copy import deepcopy

from tank.util import parse_time


def source_record_at(record, at):
    """Project captured source evidence without leaking later parsing availability."""
    result = deepcopy(record)
    while result.get("body_recovery"):
        recovery = result["body_recovery"]
        if parse_time(at) >= parse_time(recovery["recovered_at"]):
            break
        result = deepcopy(recovery["original_record"])
    from .legacy import source_metadata_at

    return source_metadata_at(result, at)
