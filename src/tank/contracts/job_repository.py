"""Inward queue storage capabilities; concrete persistence belongs to adapters."""

from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager
from typing import Protocol, Self

from pydantic import JsonValue

from tank.contracts.jobs import (
    InvalidJobClaim,
    JobClaimMutation,
    JobClaimScope,
    JobEnvelope,
)
from tank.time_precision import Timestamp
from tank.util import JsonInput, TimeInput


class QueueStore(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...

    async def put(
        self,
        collection: str,
        key: str,
        record: dict[str, JsonValue],
        *,
        immutable: bool = False,
    ) -> dict[str, JsonValue]: ...

    def transaction(self) -> AbstractAsyncContextManager[Self]: ...

    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...

    async def enqueue(
        self,
        kind: str,
        payload: Mapping[str, JsonInput],
        idem_key: str,
        *,
        priority: int = 1,
        run_after: TimeInput | Timestamp | None = None,
        expires_at: TimeInput | Timestamp | None = None,
        max_attempts: int = 3,
    ) -> dict[str, JsonValue]: ...

    async def claim_job(self) -> dict[str, JsonValue] | None: ...

    async def transition_claimed_job(
        self, scope: JobClaimScope, transform: Callable[[JobEnvelope], JobClaimMutation]
    ) -> JobEnvelope | None: ...

    async def quarantine_job_claim(self, invalid: InvalidJobClaim) -> bool: ...

    async def finish_job(self, job_id: str) -> None: ...

    async def fail_job(self, job_id: str, error: str | Exception, *, retry_after: float = 1) -> None: ...
