"""Return public X snapshots; the caller owns deduplication and delivery state."""

import re
import time
from datetime import UTC, datetime
from typing import Any

from public_profile import collect

HANDLE = re.compile(r"[a-z0-9_]{1,15}")
POST_ID = re.compile(r"[0-9]{1,20}")
FIELDS = {"handles", "mode", "concurrency", "cursors", "requestId"}
CREDENTIAL_FIELDS = {"xSession", "cookies", "auth_token", "ct0"}


def canonical_handle(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("INVALID_MONITORED_HANDLE")
    result = value.strip().removeprefix("@").lower()
    if not HANDLE.fullmatch(result):
        raise ValueError("INVALID_MONITORED_HANDLE")
    return result


def validate_input(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError("INPUT_OBJECT_REQUIRED")
    if any(key in value for key in CREDENTIAL_FIELDS):
        raise ValueError("X_ACCOUNT_CREDENTIALS_NOT_ACCEPTED")
    if set(value) - FIELDS:
        raise ValueError("UNSUPPORTED_INPUT_FIELD")
    if value.get("mode", "fetch") != "fetch":
        raise ValueError("FETCH_MODE_REQUIRED")
    handles = value.get("handles")
    if not isinstance(handles, list) or not 1 <= len(handles) <= 200:
        raise ValueError("HANDLES_REQUIRED_MAX_200")
    names = list(dict.fromkeys(canonical_handle(handle) for handle in handles))
    concurrency = value.get("concurrency", 6)
    if type(concurrency) is not int or not 1 <= concurrency <= 8:
        raise ValueError("CONCURRENCY_MUST_BE_1_TO_8")
    supplied = value.get("cursors", {})
    if not isinstance(supplied, dict) or len(supplied) > 200:
        raise ValueError("CURSORS_OBJECT_REQUIRED")
    cursors: dict[str, str] = {}
    for handle, post_id in supplied.items():
        name = canonical_handle(handle)
        if name not in names:
            raise ValueError("CURSOR_FOR_UNWATCHED_ACCOUNT")
        if not isinstance(post_id, str) or not POST_ID.fullmatch(post_id) or int(post_id) <= 0:
            raise ValueError("CURSOR_POST_ID_STRING_REQUIRED")
        if name in cursors and cursors[name] != post_id:
            raise ValueError("CONFLICTING_CURSOR_VALUES")
        cursors[name] = post_id
    request_id = value.get("requestId")
    if request_id is not None and (not isinstance(request_id, str) or len(request_id) > 128):
        raise ValueError("INVALID_REQUEST_ID")
    return {
        "handles": names,
        "concurrency": concurrency,
        "cursors": cursors,
        "requestId": request_id,
    }


async def fetch_snapshot(value: object) -> dict[str, Any]:
    started = time.monotonic()
    config = validate_input(value)
    cursors = config["cursors"]
    collection_config = {
        "handles": config["handles"],
        "concurrency": config["concurrency"],
        "_accountState": {handle: {"lastMaxId": post_id} for handle, post_id in cursors.items()},
    }
    posts, report = await collect(collection_config, set(), 0)
    accounts = report["accountResults"]
    next_cursors = dict(cursors)
    for account in accounts:
        if account["status"] == "ok":
            next_cursors[account["handle"]] = account["lastMaxId"]
    health = (
        "failed"
        if report["error"]
        else ("degraded" if report["failedAccounts"] or report["coverageGaps"] else "healthy")
    )
    return {
        "schemaVersion": 1,
        "mode": "fetch",
        "requestId": config["requestId"],
        "source": "x-public-profile",
        "checkedAt": datetime.now(UTC).isoformat(),
        "ok": health == "healthy",
        "health": health,
        "posts": posts,
        "accounts": accounts,
        "nextCursors": next_cursors,
        "failedAccounts": report["failedAccounts"],
        "coverageGaps": report["coverageGaps"],
        "error": report["error"],
        "stats": {
            "durationSeconds": round(time.monotonic() - started, 3),
            "watchedAccounts": report["watchedAccounts"],
            "accountsRead": report["accountsRead"],
            "postCount": len(posts),
            "wireBytes": report["wireBytes"],
        },
    }
