"""Guarded queue failure effects, sparse receipt writes and durable host refusals."""

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from typing import Protocol, Self

from psycopg import Error as DriverError
from psycopg import InterfaceError, OperationalError
from pydantic import JsonValue

from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.question_retirement import (
    QuestionFailureRefusalLedger,
    QuestionFailureRefusalReceipt,
    QuestionFailureRefusalRequest,
)
from tank.contracts.session_followups import FollowupRefusalTarget
from tank.runtime_contracts import (
    FailedAnswerNotification,
    FailedQuestionNotification,
    FailureJobHeader,
    FailureJournalRejected,
    FailureJournalScopeChanged,
    FailureJournalUnavailable,
    FailureReconciliationItem,
    FailureRecordIdentity,
    SuppliedFailureReason,
    TerminalFailureOutcome,
    TerminalFailureOutcomeRecord,
    TerminalFailureSkipped,
    TerminalJobFailure,
    UserQuestionMessage,
    read_failure_marker,
    recorded_failure_reason,
    replace_job_clock,
)
from tank.time_precision import Timestamp
from tank.util import JsonInput, jsonable


class FailureStore(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...

    async def put(
        self, collection: str, key: str, record: Mapping[str, JsonInput], *, immutable: bool = False
    ) -> dict[str, JsonValue]: ...

    def lock(self, key: str) -> AbstractAsyncContextManager[Self]: ...

    def iter_all(
        self, collection: str, *, filters: dict[str, JsonValue] | None = None, batch_size: int = 1000
    ) -> AsyncIterator[dict[str, JsonValue]]: ...


def read_question_refusal_ledger(job_id: str, record: dict[str, JsonValue]) -> QuestionFailureRefusalLedger:
    if record["job_id"] != job_id:
        raise FailureJournalScopeChanged()
    return QuestionFailureRefusalLedger.model_validate(
        {"job_id": job_id, "entries": record.get("host_question_failure_refusals", {})}
    )


def same_failure(record: dict[str, JsonValue], failure: TerminalJobFailure) -> bool:
    identity = FailureRecordIdentity.from_record(record)
    if identity.job_id != failure.header.job_id:
        raise FailureJournalScopeChanged()
    if identity.job_type != failure.header.job_type or identity.status != failure.header.status:
        return False
    header = FailureJobHeader.from_record(record)
    reason = (
        failure.reason
        if isinstance(failure.reason, SuppliedFailureReason)
        else recorded_failure_reason(header, record)
    )
    current = TerminalJobFailure.from_record(
        record,
        reason=reason,
        progress_requested=failure.report_progress,
        notifications_requested=failure.notify_delivery,
    )
    return current == failure


class RepositoryRuntimeFailures:
    def __init__(
        self, store: FailureStore, versions: ResolvedVersionIdentity, clock: Callable[[], Timestamp]
    ) -> None:
        self.store = store
        self.versions = ResolvedVersionIdentity.model_validate(versions)
        self.clock = clock

    async def execute(
        self, failure: TerminalJobFailure, operation: Callable[[], Awaitable[TerminalFailureOutcome]]
    ) -> TerminalFailureOutcome:
        failure = TerminalJobFailure.model_validate(failure)
        try:
            async with self.store.lock("job:" + failure.header.job_id):
                record = await self.store.get("ops.jobs", failure.header.job_id)
                if record is None:
                    return TerminalFailureSkipped(status="SKIPPED", reason="JOB_MISSING")
                if not same_failure(record, failure):
                    return TerminalFailureSkipped(status="SKIPPED", reason="JOB_CHANGED")
                return TerminalFailureOutcomeRecord.model_validate(await operation()).root
        except (OperationalError, InterfaceError, OSError):
            raise FailureJournalUnavailable() from None
        except DriverError:
            raise FailureJournalRejected() from None

    async def record(self, request: QuestionFailureRefusalRequest) -> QuestionFailureRefusalReceipt:
        request = QuestionFailureRefusalRequest.model_validate(request)
        try:
            async with self.store.lock("job:" + request.job_id):
                record = await self.store.get("ops.jobs", request.job_id)
                if record is None:
                    raise FailureJournalScopeChanged()
                header = FailureJobHeader.from_record(record)
                if (
                    header.job_id != request.job_id
                    or header.job_type != "AGENT_MESSAGE"
                    or header.status != request.job_status
                ):
                    raise FailureJournalScopeChanged()
                payload = record["payload"]
                if not isinstance(payload, dict):
                    raise ValueError("Host question refusal requires its actual queue message")
                message = UserQuestionMessage.model_validate(payload)
                target = FollowupRefusalTarget(question_id=message.question_id, session_id=message.session_id)
                if target != request.witness.target:
                    raise FailureJournalScopeChanged()
                ledger = read_question_refusal_ledger(request.job_id, record)
                prior = ledger.entries.get(request.key())
                if prior is not None:
                    if prior.request != request:
                        raise FailureJournalScopeChanged()
                    return prior
                receipt = QuestionFailureRefusalReceipt(
                    request=request, controller=self.versions, recorded_at=self.clock()
                )
                updated = QuestionFailureRefusalLedger(
                    job_id=request.job_id, entries={**ledger.entries, request.key(): receipt}
                )
                record["host_question_failure_refusals"] = jsonable(updated.entries)
                await self.store.put("ops.jobs", request.job_id, record)
                return receipt
        except (OperationalError, InterfaceError, OSError):
            raise FailureJournalUnavailable() from None
        except DriverError:
            raise FailureJournalRejected() from None

    async def mark(self, item: FailureReconciliationItem) -> bool:
        item = FailureReconciliationItem.model_validate(item)
        try:
            async with self.store.lock("job:" + item.failure.header.job_id):
                record = await self.store.get("ops.jobs", item.failure.header.job_id)
                if record is None or not same_failure(record, item.failure):
                    return False
                marker = read_failure_marker(record, item.marker_name)
                if marker is not None and marker.value is not None:
                    return True
                await self.store.put(
                    "ops.jobs",
                    item.failure.header.job_id,
                    replace_job_clock(record, item.marker_name, self.clock()),
                )
                return True
        except (OperationalError, InterfaceError, OSError):
            raise FailureJournalUnavailable() from None
        except DriverError:
            raise FailureJournalRejected() from None

    async def pending(self, *, progress_requested: bool) -> AsyncIterator[FailureReconciliationItem]:
        try:
            for status in ("FAILED", "EXPIRED"):
                for kind in (
                    "FILTER_ITEM",
                    "ROUTE_ITEM",
                    "AGENT_MESSAGE",
                    "RUN_MARKET_TRUTH",
                    "ESCALATE_TOKENS",
                ):
                    async for record in self.store.iter_all(
                        "ops.jobs", filters={"type": kind, "status": status}
                    ):
                        marker = read_failure_marker(record, "ops_progress_recorded_at")
                        if marker is None or marker.value is None:
                            yield FailureReconciliationItem.from_record(
                                record,
                                marker_name="ops_progress_recorded_at",
                                progress_requested=progress_requested,
                                notifications_requested=False,
                            )
                async for record in self.store.iter_all(
                    "ops.jobs", filters={"type": "DELIVER", "status": status}
                ):
                    marker = read_failure_marker(record, "delivery_failure_recorded_at")
                    if marker is None or marker.value is None:
                        yield FailureReconciliationItem.from_record(
                            record,
                            marker_name="delivery_failure_recorded_at",
                            progress_requested=progress_requested,
                            notifications_requested=True,
                        )
                for kind in ("AGENT_MESSAGE", "TELEGRAM_ANSWER"):
                    async for record in self.store.iter_all(
                        "ops.jobs", filters={"type": kind, "status": status}
                    ):
                        marker = read_failure_marker(record, "followup_failure_recorded_at")
                        if marker is not None and marker.value is not None:
                            continue
                        item = FailureReconciliationItem.from_record(
                            record,
                            marker_name="followup_failure_recorded_at",
                            progress_requested=progress_requested,
                            notifications_requested=True,
                        )
                        if isinstance(
                            item.failure.notification, (FailedQuestionNotification, FailedAnswerNotification)
                        ):
                            yield item
        except (OperationalError, InterfaceError, OSError):
            raise FailureJournalUnavailable() from None
        except DriverError:
            raise FailureJournalRejected() from None
