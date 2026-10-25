import time
from datetime import UTC, datetime
from typing import Literal

from actor_contracts import (
    CREDENTIAL_FIELDS,
    ActorRequest,
    ActorSnapshot,
    SnapshotStats,
)
from public_profile import collect
from pydantic import JsonValue


def validate_input(value: JsonValue) -> ActorRequest:
    if isinstance(value, dict) and any(key in value for key in CREDENTIAL_FIELDS):
        raise ValueError("X_ACCOUNT_CREDENTIALS_NOT_ACCEPTED")
    if isinstance(value, dict) and set(value) - ActorRequest.model_fields.keys():
        raise ValueError("UNSUPPORTED_INPUT_FIELD")
    return ActorRequest.model_validate(value)


async def fetch_snapshot(value: JsonValue) -> dict[str, JsonValue]:
    started = time.monotonic()
    config = validate_input(value)
    posts, report = await collect(config)
    next_cursors = dict(config.cursors)
    for account in report.accountResults:
        if account.status == "ok":
            next_cursors[account.handle] = account.lastMaxId
    health: Literal["failed", "degraded", "healthy"] = (
        "failed"
        if report.error is not None
        else ("degraded" if report.failedAccounts or report.coverageGaps else "healthy")
    )
    result = ActorSnapshot(
        schemaVersion=1,
        mode="fetch",
        requestId=config.requestId,
        source="x-public-profile",
        checkedAt=datetime.now(UTC).isoformat(),
        ok=health == "healthy",
        health=health,
        posts=posts,
        accounts=report.accountResults,
        nextCursors=next_cursors,
        failedAccounts=report.failedAccounts,
        coverageGaps=report.coverageGaps,
        error=report.error,
        stats=SnapshotStats(
            durationSeconds=round(time.monotonic() - started, 3),
            watchedAccounts=report.watchedAccounts,
            accountsRead=report.accountsRead,
            postCount=len(posts),
            wireBytes=report.wireBytes,
        ),
    )
    return result.model_dump(mode="json", exclude_unset=True)
