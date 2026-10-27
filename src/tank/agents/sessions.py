"""OpenMA managed-agents v1 adapter, verified at upstream 85d5361f535c.

Durable event-list reconciliation complements SSE: upstream's strict managed
stream ignores Last-Event-ID, while the native Node transport honors it.
"""

import asyncio
import copy
import logging
import time
from collections.abc import Mapping, Sequence
from contextlib import AsyncExitStack, suppress
from datetime import timedelta
from typing import Literal
from urllib.parse import quote

import httpx
from httpx_sse import aconnect_sse
from pydantic import JsonValue, ValidationError

from tank.agents.admission import SessionAdmission
from tank.agents.admission_repository import admission_candidate, build_admission
from tank.agents.answer_settlement import (
    AnswerHistoryEvent,
    AnswerHistoryPage,
    HistoryRunning,
    NativeAnswerQuery,
    NativeTurnAnswer,
    QuestionSettlementSession,
    collect_turn_answers,
)
from tank.agents.archive_contracts import (
    ArchiveDelivery,
    archive_card,
    terminal_archive_card,
)
from tank.agents.context import CONTEXT_TRANSITION, CONTEXT_VERSION, ModelContext
from tank.agents.context_binding import ContextBinding
from tank.agents.input_ownership import (
    JournalInputLineage,
    NativeInputOwner,
    NativeOwnershipSnapshot,
    ToolInputLineage,
    UnprovedInputOwnership,
)
from tank.agents.message_dispatch import (
    DeclinedQuestionDispatch,
    MessageDispatchOutcome,
    MessageDispatchRequest,
    PendingMessageDispatch,
    find_message_dispatch,
    observe_message_dispatch,
)
from tank.agents.native_file_preparation import NativeFilePreparationService, NativeFileTransport
from tank.agents.native_inputs import (
    AcceptedNativeInput,
    NativeInput,
    NativeInputAcknowledgment,
    NativeInputRequest,
    observe_native_input,
    parse_native_input,
)
from tank.agents.native_output_files import (
    OutputReadPolicy,
    SessionFileSender,
    SessionOutputFiles,
    output_request,
)
from tank.agents.native_readback import NativeSessionMissing, NativeSessionReader
from tank.agents.native_retry import (
    PendingNativeRetry,
    ReadyRetryState,
    UnknownNativeRetryState,
    UnknownRetryState,
    retry_state,
)
from tank.agents.native_sessions import (
    NativeOpeningSource,
    NativeSessionCreate,
    NativeSessionDeployment,
    NativeSessionDiscovery,
    NativeSessionIdentity,
    NativeSessionIdentityMetadata,
    NativeSessionResolution,
    NativeStoredSessionBinding,
)
from tank.agents.native_transport import (
    ManagedEventCursor,
    NativeEventCursor,
    NativeEventObserved,
    NativeEventOutcome,
    NativeEventRetryRefused,
    NativeFrameIdentity,
    NativeRetryRefusalJournal,
    NativeStoredCursor,
    NodeEventCursor,
    decode_native_page,
    decode_native_stream,
    native_event_sequence,
    next_native_cursor,
)
from tank.agents.native_usage import record_native_usage
from tank.agents.outbound_inputs import (
    AcceptedOutboundInput,
    MessageIdentity,
    initial_input_definition,
    outbound_message_record,
    parse_outbound_input,
    recoverable_outbound_inputs,
    retained_outbound_acceptance,
)
from tank.agents.pipeline import ConfigurationError
from tank.agents.provenance import resolve_agent_provenance
from tank.agents.security import redact_tool_arguments
from tank.agents.session_contracts import (
    ExecutionPoll,
    HistoryPoll,
    LegacyToolContext,
    NativeToolSyncResult,
    PromptRecoveryRecord,
    ScoutProposalIdentity,
    ScoutSessionReference,
    SessionPromptState,
    SessionToolBoundaryState,
)
from tank.agents.session_following import (
    FollowedSession,
    FollowerObservation,
    FollowerReady,
    FollowerUnavailable,
    NativeFollowerUnavailable,
    RecordedEventDecision,
    native_runtime_status,
)
from tank.agents.session_ownership import put_owned_session, require_owned_session_identity
from tank.agents.session_reopening import reopen_question_session
from tank.agents.tool_contracts import (
    CapturedToolResult,
    FailedToolResponse,
    FailureCategory,
    FailureStatus,
    NativeText,
    NativeToolResult,
    ToolFailure,
)
from tank.agents.wire import tool_wire_result
from tank.agents.workflow import SOURCE_COMPLETION_VERSION
from tank.contracts.json import decode_json, decode_unique_json
from tank.contracts.native_readback import NativeSessionReference
from tank.contracts.native_usage import NativeUsageSession, NativeUsageSpan
from tank.contracts.openma import (
    ManagedAgentName,
    ManagedToolManifest,
    NativeSessionToolOverride,
    NativeSessionToolUpdate,
    OpenMAContractViolation,
    openma_tools_match,
)
from tank.contracts.provenance import AgentProvenance, ResolvedVersionIdentity
from tank.contracts.question_retirement import (
    ActiveQuestionIntent,
    QueuedQuestionSession,
    RetiredQuestionMessages,
    UnknownQuestionAcceptance,
    UnprovedQuestionRetirement,
    UnretainedQuestionJournal,
)
from tank.contracts.session_admission import AdmissionCandidate
from tank.contracts.session_completion import (
    CompletionAttemptOwner,
    CompletionMessage,
    CompletionSessionSnapshot,
    CompletionToolInput,
    FailedToolAttempt,
    HostExecutionObservation,
    HostReplayAbortReceipt,
    NativeExecutionBarrier,
    NativeExecutionPage,
    NativeExecutionQuery,
    NativeExecutionState,
    PriorToolInputReceipt,
    ReplayAbortFailure,
    ReplayAbortFinalized,
    ReplayAbortJournal,
    ReplayAbortOutcome,
    ReplayAbortOwnership,
    ReplayAbortRefused,
    ReplayAbortTarget,
    SessionCompletionStatus,
    SourceCompletionEntry,
    SourceCompletionSnapshot,
)
from tank.contracts.session_completion import (
    execution_barrier as derive_execution_barrier,
)
from tank.contracts.session_followups import (
    FollowupCandidate,
    FollowupEligibility,
    FollowupRefusalSink,
    FollowupRefusalTarget,
    FollowupSession,
    NoSession,
    followup_eligibility,
)
from tank.contracts.session_retention import (
    ArchiveCandidate,
    ArchiveDenied,
    ArchiveEventBlocker,
    FollowupAlertScope,
    FollowupDeliveryScope,
    FollowupPublicationScope,
    FollowupQuestionScope,
    InvalidatedArchiveCandidate,
    PendingArchiveJob,
    QuestionActivity,
    RecordedActivity,
    RetainedSession,
    RetentionPolicy,
    SessionActivity,
    SessionSweepEntry,
    TerminalArchiveCandidate,
    archive_candidate,
    archive_tool,
    latest_activity,
)
from tank.contracts.source_items import (
    FailedExtraction,
    SkippedExtraction,
)
from tank.contracts.telegram_state import QuestionPrompt
from tank.delivery import DeliveryDeferred
from tank.ops_progress import session_progress
from tank.question_answers import record_answer, validate_question_identity
from tank.question_prompts import question_message, question_prompt
from tank.runtime_contracts import (
    ScoutMessage,
    ScoutReviewJob,
    SourceMessage,
    UserQuestionMessage,
)
from tank.source_repository import SourceExtractionRepository
from tank.telegram_inbox import DOCUMENT_LIMIT
from tank.time_precision import (
    Timestamp,
    collection_clock,
    offset_schedule,
    propagate_timestamp,
    timestamp_owned_fields,
)
from tank.util import canonical_json, jsonable, new_id, parse_time, sha256, utcnow

log = logging.getLogger(__name__)

REST_TIMEOUT_S = 30


ADMISSION_SLEEP_S = 60


MATERIAL_EVENT_TYPES = {
    "agent.custom_tool_use",
    "user.custom_tool_result",
    "session.status_running",
    "session.status_idle",
    "session.status_rescheduled",
    "session.status_terminated",
    "session.error",
    "agent.refusal",
    "execution.status_failed",
    "span.model_request_end",
    "host.analysis_incomplete",
}

IDLE_RECHECK_TICKS = 60
SESSION_FOLLOW_FIELDS = (
    "session_id",
    "status",
    "runtime_status",
    "active_message_id",
    "followups_only",
    "archive_after",
)


def seen_key(event: dict[str, JsonValue]) -> str:
    """Exact native identity and optional execution correlation, without delimiter collisions."""
    return NativeFrameIdentity.from_wire(event).cache_key()


def material_event(event: dict[str, JsonValue]) -> bool:
    """Whether a native event can change state, including a refusal inside a message."""
    kind = event.get("type")
    if (
        isinstance(kind, str)
        and kind in MATERIAL_EVENT_TYPES
        or event.get("finish_reason") == "content-filter"
    ):
        return True
    content = event.get("content")
    if not isinstance(content, list):
        return False
    return any(isinstance(block, dict) and block.get("type") == "refusal" for block in content)


class SessionService:
    def __init__(
        self,
        repo,
        settings,
        tool_dispatch,
        *,
        versions: ResolvedVersionIdentity,
        refusals: FollowupRefusalSink,
        http=None,
        stream_http=None,
    ):
        self.versions = ResolvedVersionIdentity.model_validate(versions)
        self.repo, self.settings, self.tool_dispatch = repo, settings, tool_dispatch
        self.refusals = refusals
        self.source_extraction = SourceExtractionRepository(repo)
        self.retry_refusals = NativeRetryRefusalJournal(repo, self.versions, self.clock_timestamp)
        self.http = httpx.AsyncClient(timeout=REST_TIMEOUT_S) if http is None else http
        self.owns_http = http is None

        self.stream_http = (
            stream_http
            if stream_http is not None
            else (
                http
                if http is not None
                else httpx.AsyncClient(
                    timeout=httpx.Timeout(30, read=None),
                    limits=httpx.Limits(max_connections=None, max_keepalive_connections=20),
                )
            )
        )
        self.owns_stream_http = stream_http is None and http is None
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.event_locks: dict[str, asyncio.Lock] = {}
        self.send_locks: dict[str, asyncio.Lock] = {}
        self.execution_polls: dict[str, ExecutionPoll] = {}
        self.history_polls: dict[str, HistoryPoll] = {}
        self.pending_seq: dict[str, int] = {}
        self.seen_events: dict[str, set[str]] = {}
        self.closed = False
        self.telegram: SessionFileSender | None = None

    def clock_timestamp(self) -> Timestamp:
        return collection_clock()

    def event_timestamp(self, event):
        return (
            propagate_timestamp(event, "processed_at")
            if event.get("processed_at") is not None
            else self.clock_timestamp()
        )

    def agent_provenance(
        self, remote: dict[str, JsonValue], config: dict[str, JsonValue], agent_id: str | None
    ) -> AgentProvenance:
        return resolve_agent_provenance(
            remote, config, declared_agent_id=agent_id, declared_model_id=self.settings.openma_model
        )

    def config(self, *, role: Literal["ANALYST", "SCOUT"], replay: bool) -> NativeSessionDeployment:
        return NativeSessionDeployment.from_wire(
            self.settings.load_config("openma"),
            role=role,
            replay=replay,
        )

    def headers(self, idem: str | None = None) -> dict[str, str]:
        headers = {"anthropic-beta": "managed-agents-2026-04-01"}
        if self.settings.openma_api_key:
            headers["x-api-key"] = self.settings.openma_api_key
        if idem:
            headers["Idempotency-Key"] = idem
        return headers

    def url(self, path: str) -> str:
        return self.settings.openma_url.rstrip("/") + "/v1/" + path

    async def request(
        self,
        method: Literal["GET", "POST"],
        path: str,
        *,
        json: JsonValue = None,
        params: Mapping[str, str | int] | Sequence[tuple[str, str]] | None = None,
        files: Mapping[str, tuple[str, bytes, str]] | None = None,
        idem: str | None = None,
    ) -> dict[str, JsonValue]:
        query = (
            None
            if params is None
            else httpx.QueryParams(params if isinstance(params, Mapping) else list(params))
        )
        response = await self.http.request(
            method,
            self.url(path),
            headers=self.headers(idem),
            json=json,
            params=query,
            files=files,
            timeout=REST_TIMEOUT_S,
        )
        response.raise_for_status()
        payload = decode_unique_json(response.content)
        if not isinstance(payload, dict):
            raise ValueError("Native endpoint returned a non-object JSON response")
        return payload

    async def resolve(self, request: NativeSessionResolution) -> dict[str, JsonValue]:
        target = request.target
        replay_run_id = request.replay.run_id if request.replay is not None else None
        if target.session_id is not None:
            session = await self.repo.get("ops.agent_sessions", target.session_id)
            if session is None:
                raise KeyError(target.session_id)
            NativeStoredSessionBinding.from_record(session).verify(request)
            return session
        event_id, item_id = target.event_id, target.item_id
        identifiers = [
            ("event_id", event_id),
            ("item_id", item_id if event_id is None else None),
            ("proposal_id", target.proposal_id),
        ]
        for field, value in identifiers:
            if value is not None:
                async for session in self.repo.iter_all(
                    "ops.agent_sessions",
                    filters={
                        field: value,
                        **({"replay_run_id": replay_run_id} if replay_run_id is not None else {}),
                    },
                ):
                    cached = NativeStoredSessionBinding.from_record(session)
                    if request.replay is None and cached.replay_run_id is not None:
                        continue
                    cached.verify(request)
                    return session
        deployment = self.config(role=request.role, replay=request.replay is not None)
        scout = request.role == "SCOUT"
        if item_id is None and event_id is not None and not scout:
            async for observation in self.repo.iter_range(
                "evt.event_observations", "observed_at", filters={"event_id": event_id}
            ):
                source = NativeOpeningSource.model_validate(
                    {"item_id": observation.get("item_id"), "opened_by_item": None}
                )
                if source.item_id is not None:
                    item_id = source.item_id
                    break
            if item_id is None:
                async for origin in self.repo.iter_all("ops.agent_sessions", filters={"event_id": event_id}):
                    if not NativeStoredSessionBinding.from_record(origin).matches_origin_scope(request):
                        continue
                    source = NativeOpeningSource.model_validate(
                        {"item_id": origin.get("item_id"), "opened_by_item": origin.get("opened_by_item")}
                    )
                    item_id = source.opening_item()
                    if item_id is not None:
                        break
            if item_id is None:
                raise ValueError("Event session has no durable opening source item")
        identity = event_id if event_id is not None else item_id
        if identity is None:
            identity = target.proposal_id
        if identity is None:
            raise ValueError("Session needs event, item or proposal identity")
        marker = NativeSessionIdentity(
            identity=identity,
            scout=scout,
            model_context_version=CONTEXT_VERSION,
            replay_run_id=replay_run_id,
        ).marker()
        wanted = NativeSessionCreate(
            agent=deployment.agent_id,
            environment_id=deployment.environment_id,
            metadata=NativeSessionIdentityMetadata.from_binding(marker, request.replay),
        )
        remote = await NativeSessionDiscovery(self, deployment).discover_or_create(
            wanted, idem="create:" + marker
        )
        sid = remote.session_id
        opened = self.clock_timestamp()
        session = {
            "session_id": sid,
            "model_context_version": CONTEXT_VERSION,
            "event_id": event_id,
            "replay_run_id": replay_run_id,
            "replay_source_session_id": request.replay.source_session_id
            if request.replay is not None
            else None,
            "replay_evidence_sha256": request.replay.evidence_sha256 if request.replay is not None else None,
            "replay_cutoff": request.replay.cutoff if request.replay is not None else None,
            "item_id": item_id,
            "proposal_id": target.proposal_id,
            "opened_by_item": item_id,
            **opened.fields("opened_at"),
            **remote.provenance.model_dump(mode="json"),
            "billing_basis": "SUBSCRIPTION",
            "configured_effort": deployment.effort,
            "effort": remote.effort,
            "effort_basis": "RESOLVED_SESSION_AGENT" if remote.effort is not None else None,
            "effort_reason": (
                "Native resolved agent did not report reasoning effort" if remote.effort is None else None
            ),
            "status": "OPEN",
            "last_event_seq": 0,
            **propagate_timestamp(
                {}, "last_item_at", missing_reason="No accepted source item message"
            ).fields("last_item_at"),
            "config_version": self.versions.config_version,
            "code_version": self.versions.code_version,
            **opened.fields("created_at"),
            "messages": [],
        }
        session["missing_fields"] = {
            key: reason
            for key, reason in {
                "opened_by_item": "Proposal or event follow-up has no opening source item",
                "last_item_at": "No source item message accepted yet",
                "prompt_version": "INCOMPLETE: remote resolved prompt snapshot unavailable",
            }.items()
            if session.get(key) is None
        }
        session["last_item_at_reason"] = session["missing_fields"].get("last_item_at")
        await put_owned_session(self.repo, sid, session)
        return session

    async def continuable(self, request: UserQuestionMessage) -> bool:
        target = FollowupRefusalTarget(question_id=request.question_id, session_id=request.session_id)
        session = await self.repo.get("ops.agent_sessions", request.session_id)
        if session is not None:
            require_owned_session_identity(request.session_id, session)
        eligibility: FollowupEligibility
        if self.settings.replay:
            eligibility = NoSession(status="NO_SESSION", reason="REPLAY")
        else:
            eligibility = followup_eligibility(
                FollowupSession.from_record(session) if session is not None else None
            )
        if not isinstance(eligibility, FollowupCandidate):
            await self.refusals.record(target, eligibility)
            return False
        observed = await NativeSessionReader(self).state(request.session_id)
        if isinstance(observed, NativeSessionMissing) or (
            observed.status == "terminated" or observed.archived_at is not None
        ):
            async with self.repo.lock("session:" + request.session_id):
                current = await self.repo.get("ops.agent_sessions", request.session_id)
                if current is not None:
                    current["remote_state"] = {
                        **observed.model_dump(mode="json"),
                        **self.clock_timestamp().fields("checked_at"),
                    }
                    await put_owned_session(self.repo, request.session_id, current)
            eligibility = followup_eligibility(
                FollowupSession.from_record(current) if current is not None else None
            )
            if isinstance(eligibility, FollowupCandidate):
                raise ValueError("Observed closed native session has no follow-up closure")
            await self.refusals.record(target, eligibility)
            return False
        return True

    def reopen_for_question(
        self,
        session: dict[str, JsonValue],
        request: UserQuestionMessage,
        message_id: str,
    ) -> None:
        """Apply the typed reopening policy while the caller owns the session lock."""
        reopen_question_session(session, request, message_id, self.clock_timestamp())

    def analyst_tools(self, *, scout: bool) -> ManagedToolManifest:
        name: ManagedAgentName = "tank-scout-reviewer" if scout else "tank-event-analyst"
        return ManagedToolManifest.from_definitions(
            name, self.settings.read_config_json(f"skills/{name}/tools.json")
        )

    @staticmethod
    def legacy_contract(session: dict[str, JsonValue]) -> bool:
        return LegacyToolContext.from_record(session).has_legacy_turn(CONTEXT_VERSION)

    async def sync_tools(self, sid: str, *, resume: bool = False) -> NativeToolSyncResult:
        if self.settings.replay:
            return NativeToolSyncResult(status="READ_ONLY")
        session = await self.repo.get("ops.agent_sessions", sid)
        if session is None:
            raise KeyError(sid)
        state = SessionToolBoundaryState.from_record(session)
        manifest = self.analyst_tools(scout=state.proposal_id is not None)
        tools_sha256 = sha256(manifest.tools)
        migrating = state.context.needs_migration(CONTEXT_VERSION)
        if not migrating and state.tools_sha256 == tools_sha256:
            return NativeToolSyncResult(status="UNCHANGED")
        if not resume and state.is_busy():
            return NativeToolSyncResult(status="BUSY")
        reader = NativeSessionReader(self)
        observed = await reader.snapshot(sid, prompt_label=state.prompt.recovery_label())
        if isinstance(observed, NativeSessionMissing):
            raise KeyError(sid)
        if observed.state.status != "idle" or observed.state.archived_at is not None:
            if not resume:
                return NativeToolSyncResult(status="BUSY")
            raise ValueError("Wait for the native tool boundary before updating its tools")
        if not openma_tools_match(observed.observation.tools, manifest.tools):
            request = NativeSessionToolUpdate(agent=NativeSessionToolOverride(tools=manifest.tools))
            await self.request(
                "POST", f"sessions/{quote(sid, safe='')}", json=request.model_dump(mode="json")
            )
            observed = await reader.snapshot(sid, prompt_label=state.prompt.recovery_label())
            if isinstance(observed, NativeSessionMissing):
                raise OpenMAContractViolation("Native session disappeared before tool confirmation")
            if not openma_tools_match(observed.observation.tools, manifest.tools):
                raise OpenMAContractViolation("Native session did not confirm the requested tool contract")
        descriptor = observed.observation.provenance
        if descriptor.prompt_version is None:
            raise OpenMAContractViolation("Native session did not confirm its resolved prompt snapshot")
        async with self.repo.lock("session:" + sid):
            current = await self.repo.get("ops.agent_sessions", sid)
            if current is None:
                raise KeyError(sid)
            current_state = SessionToolBoundaryState.from_record(current)
            if current_state != state:
                raise ValueError("Native tool ownership changed during readback confirmation")
            version = current_state.context.version
            if version is not None and version > CONTEXT_VERSION:
                raise ValueError("Native tool context advanced during readback confirmation")
            current.update(descriptor.model_dump(mode="json"))
            current["tools_sha256"] = tools_sha256
            if current_state.context.needs_migration(CONTEXT_VERSION):
                current["legacy_model_message_ids"] = current_state.legacy_inputs(CONTEXT_VERSION)
                current["model_context_version"] = CONTEXT_VERSION
                current.pop("model_catalog", None)
            current.update(self.clock_timestamp().fields("tools_synced_at"))
            await put_owned_session(self.repo, sid, current)
        return NativeToolSyncResult(status="UPDATED")

    async def transition_note(self, sid: str) -> str | None:
        async with self.repo.lock("session:" + sid):
            current = await self.repo.get("ops.agent_sessions", sid)
            if current is None:
                raise KeyError(sid)
            state = SessionToolBoundaryState.from_record(current)
            if state.context_note_version == CONTEXT_VERSION:
                return None
            current["context_note_version"] = CONTEXT_VERSION
            await put_owned_session(self.repo, sid, current)
        return CONTEXT_TRANSITION[state.proposal_id is not None]

    async def prepare_attachments(self, target: FollowupRefusalTarget) -> QuestionPrompt:
        return await NativeFilePreparationService(
            self.repo,
            NativeFileTransport(self.http, self.settings.openma_url, self.settings.openma_api_key),
            self.versions,
            clock=self.clock_timestamp,
        ).prepare_question(target)

    async def send_file(
        self,
        session: dict[str, JsonValue],
        arguments: dict[str, JsonValue],
        call_id: str,
    ) -> dict[str, JsonValue]:
        request = output_request(session, arguments, call_id)
        if self.telegram is None:
            raise ValueError("send_telegram_file is unavailable without its Telegram sender")
        outcome = await SessionOutputFiles(
            self.repo,
            self.telegram,
            self.http,
            self.settings.openma_url,
            self.headers,
            OutputReadPolicy(
                max_bytes=DOCUMENT_LIMIT, attempts=10, retry_delay_s=1, timeout_s=REST_TIMEOUT_S
            ),
        ).send(request)
        serialized = decode_unique_json(outcome.model_dump_json())
        if not isinstance(serialized, dict):
            raise TypeError("Native file tool outcome requires an object response")
        return serialized

    async def validate_new_question_input(self, request: UserQuestionMessage, message_id: str) -> None:
        """Admit actual question scope before any native read or fresh text/file effect."""
        target = FollowupRefusalTarget(question_id=request.question_id, session_id=request.session_id)
        async with self.send_locks.setdefault(request.session_id, asyncio.Lock()):
            async with self.repo.lock("session:" + request.session_id):
                current: dict[str, JsonValue] | None = await self.repo.get(
                    "ops.agent_sessions", request.session_id
                )
                if current is not None:
                    require_owned_session_identity(request.session_id, current)
                    ContextBinding.from_session(current)
                    retained = find_message_dispatch(current, message_id)
                    if retained is not None:
                        if isinstance(retained, PendingMessageDispatch):
                            row = outbound_message_record(current, message_id)
                            original = row["payload"]
                            if not isinstance(original, dict):
                                raise ValueError(
                                    "Retained question input requires its actual structured target"
                                )
                            owner = UserQuestionMessage.model_validate(original)
                            if (
                                owner.session_id != request.session_id
                                or owner.question_id != request.question_id
                                or sha256(original) != message_id
                            ):
                                raise ValueError(
                                    "Retained question input differs from its actual requested owner"
                                )
                        return
            async with self.repo.lock("question:" + request.question_id):
                question: dict[str, JsonValue] | None = await self.repo.get(
                    "out.user_questions", request.question_id
                )
                if question is None:
                    raise KeyError(request.question_id)
                validate_question_identity(question, target)
                question_prompt(question)

    async def send_message(self, payload: dict[str, JsonValue]) -> dict[str, JsonValue] | None:
        """Admit original caller wire and return the authoritative persisted session."""
        outcome = await self.dispatch_message(MessageDispatchRequest.from_wire(payload))
        if isinstance(outcome, DeclinedQuestionDispatch):
            return None
        session: dict[str, JsonValue] | None = await self.repo.get("ops.agent_sessions", outcome.session_id)
        if session is None:
            raise KeyError(outcome.session_id)
        require_owned_session_identity(outcome.session_id, session)
        return session

    async def dispatch_message(self, request: MessageDispatchRequest) -> MessageDispatchOutcome:
        request = MessageDispatchRequest.model_validate(request, by_name=True)
        message = request.message
        payload = request.original_wire
        key = request.message_id
        if isinstance(message, UserQuestionMessage):
            await self.validate_new_question_input(message, key)
            if not await self.continuable(message):
                return DeclinedQuestionDispatch(
                    phase="NOT_CONTINUABLE",
                    target=FollowupRefusalTarget(
                        question_id=message.question_id, session_id=message.session_id
                    ),
                )
        item = (
            await self.repo.get("ingest.source_items", message.item_id)
            if isinstance(message, SourceMessage)
            else None
        )
        resolution = NativeSessionResolution.from_message(message, replay=self.settings.replay)
        async with self.repo.lock("session-mapping:" + resolution.mapping_identity()):
            session = await self.resolve(resolution)
            binding = ContextBinding.from_session(session)
        sid = binding.session_id
        migrating = LegacyToolContext.from_record(session).needs_migration(CONTEXT_VERSION)
        if migrating or isinstance(message, UserQuestionMessage):
            if (await self.sync_tools(sid)).status == "BUSY":
                raise DeliveryDeferred(offset_schedule(collection_clock(), timedelta(seconds=30)))
            session = await self.repo.get("ops.agent_sessions", sid)
            if session is None:
                raise KeyError(sid)
        content = await ModelContext(self.repo, self.settings, session).message(request, item)
        note = CONTEXT_TRANSITION[bool(session.get("proposal_id"))]
        if migrating:
            content["context_update"] = note
        async with self.send_locks.setdefault(sid, asyncio.Lock()):
            async with self.repo.lock("session:" + sid):
                session = await self.repo.get("ops.agent_sessions", sid)
                if isinstance(message, UserQuestionMessage):
                    if session is not None:
                        require_owned_session_identity(sid, session)
                    eligibility = followup_eligibility(
                        FollowupSession.from_record(session) if session is not None else None
                    )
                    if not isinstance(eligibility, FollowupCandidate):
                        await self.refusals.record(
                            FollowupRefusalTarget(question_id=message.question_id, session_id=sid),
                            eligibility,
                        )
                        return DeclinedQuestionDispatch(
                            phase="NOT_CONTINUABLE",
                            target=FollowupRefusalTarget(question_id=message.question_id, session_id=sid),
                        )
                    self.reopen_for_question(session, message, key)
                if session is None:
                    raise KeyError(sid)
                retained = find_message_dispatch(session, key)
                if retained is not None and not isinstance(retained, PendingMessageDispatch):
                    self.start_stream(sid)
                    return retained
                intent = next(
                    (row for row in session.get("message_log", []) if row["message_id"] == key), None
                )
                if intent is None:
                    text = canonical_json(content)
                    if isinstance(message, UserQuestionMessage):
                        text = question_message(
                            await self.prepare_attachments(
                                FollowupRefusalTarget(question_id=message.question_id, session_id=sid)
                            )
                        )
                        if migrating:
                            text = note + "\n\n" + text
                    body = {
                        "type": "user.message",
                        "content": [{"type": "text", "text": text}],
                    }
                    intent = {
                        "message_id": key,
                        "body": body,
                        "payload": copy.deepcopy(payload),
                        "model_context_version": CONTEXT_VERSION,
                        **self.clock_timestamp().fields("requested_at"),
                        "turn_status": "ACCEPTING",
                        "retry_count": 0,
                        "remote_event_id": None,
                        **(
                            propagate_timestamp(item, "observed_at").fields("item_observed_at")
                            if item
                            else {}
                        ),
                    }
                    session.setdefault("message_log", []).append(intent)
                    if migrating:
                        session["context_note_version"] = CONTEXT_VERSION
                session["last_message"] = copy.deepcopy(intent["body"])
                session["last_payload"] = copy.deepcopy(intent["payload"])
                if not session.get("active_message_id"):
                    session["active_message_id"] = key
                if session.get("runtime_status") != "RUNNING":
                    session["runtime_status"] = "PENDING"
                await put_owned_session(self.repo, sid, session)

            session = await self.post_message_intent(sid, key)
        self.start_stream(sid)
        return observe_message_dispatch(session, key)

    async def post_message_intent(self, sid: str, key: str) -> dict[str, JsonValue]:
        session = await self.repo.get("ops.agent_sessions", sid)
        if session is None:
            raise KeyError(sid)
        require_owned_session_identity(sid, session)
        ContextBinding.from_session(session)
        pending = parse_outbound_input(outbound_message_record(session, key))
        if isinstance(pending, AcceptedOutboundInput):
            return session
        receipt = await self.post_input(sid, pending.body, idem="message:" + key)
        accepted_at = self.clock_timestamp()
        accepted = pending.accepted(receipt, accepted_at)
        async with self.repo.lock("session:" + sid):
            current = await self.repo.get("ops.agent_sessions", sid)
            if current is None:
                raise KeyError(sid)
            require_owned_session_identity(sid, current)
            intent = outbound_message_record(current, key)
            durable = parse_outbound_input(intent)
            if isinstance(durable, AcceptedOutboundInput):
                if (
                    durable.event_id is not None
                    and accepted.event_id is not None
                    and durable.event_id != accepted.event_id
                ):
                    raise ValueError("Idempotent message acceptance changed native input identity")
                if durable.event_id is None and accepted.event_id is not None:
                    if initial_input_definition(intent) != pending.definition():
                        raise ValueError("Outbound input advanced before acknowledgment refinement")
                    lineage = JournalInputLineage.from_record(intent)
                    intent["native_input_attempts"] = {
                        **(lineage.attempts if lineage.attempts is not None else {}),
                        accepted.event_id: pending.attempt,
                    }
                    intent["remote_event_id"] = accepted.event_id
                    intent["initial_input_acceptance"] = jsonable(receipt)
                    await put_owned_session(self.repo, sid, current)
                return current
            if durable != pending:
                raise ValueError("Outbound input intent changed before acceptance committed")
            identity = accepted.event_id
            if identity is not None:
                attempts = JournalInputLineage.from_record(intent).attempts
                intent["native_input_attempts"] = {
                    **(attempts if attempts is not None else {}),
                    identity: pending.attempt,
                }
            intent.update(
                **accepted_at.fields("sent_at"),
                remote_event_id=identity,
                initial_input_acceptance=jsonable(receipt),
                turn_status="PENDING",
            )
            messages = current.setdefault("messages", [])
            if not isinstance(messages, list):
                raise ValueError("Accepted session message identities must be an array")
            if key not in messages:
                messages.append(key)
            current["status"] = "OPEN"
            if pending.source.item_id is not None:
                current.update(accepted_at.fields("last_item_at"))
                current.update(pending.source.observed_at.fields("last_item_observed_at"))
                missing = current.get("missing_fields")
                if isinstance(missing, dict):
                    missing.pop("last_item_at", None)
                elif missing is not None:
                    raise ValueError("Session missing-field provenance must be an object")
                current["last_item_at_reason"] = None
            await put_owned_session(self.repo, sid, current)
            await session_progress(self.repo, self.settings, current)
            return current

    async def recover_outbound_inputs(self, sid: str) -> None:
        async with self.send_locks.setdefault(sid, asyncio.Lock()):
            session = await self.repo.get("ops.agent_sessions", sid)
            if session is None:
                raise KeyError(sid)
            require_owned_session_identity(sid, session)
            for message in recoverable_outbound_inputs(session):
                await self.post_message_intent(sid, message.message_id)
        session = await self.repo.get("ops.agent_sessions", sid)
        if session is None:
            raise KeyError(sid)
        require_owned_session_identity(sid, session)
        await session_progress(self.repo, self.settings, session)
        pending_retry = session.get("pending_retry_event")
        if pending_retry is not None:
            if not isinstance(pending_retry, dict):
                raise ValueError("Retained native retry event must be an object")
            await self.observe_native_event(sid, pending_retry, native_event_sequence(pending_retry))

    async def post_input(self, sid: str, body: NativeInput, *, idem: str) -> NativeInputAcknowledgment:
        receiver = NativeSessionReference(session_id=sid)
        response = await self.request(
            "POST",
            f"sessions/{quote(receiver.session_id, safe='')}/events",
            json=decode_json(NativeInputRequest(events=[body]).model_dump_json(exclude_none=True)),
            idem=idem,
        )
        return observe_native_input(response, body)

    @staticmethod
    def accepted_input_id(response: NativeInputAcknowledgment) -> str | None:
        """Only a validated reported input establishes native execution ownership."""
        return response.event_id if isinstance(response, AcceptedNativeInput) else None

    async def native_input_owner(
        self, session: NativeOwnershipSnapshot, execution_id: str | None
    ) -> NativeInputOwner | None:
        """Resolve only actual recorded accepted-input lineage, including tool continuations."""
        session = NativeOwnershipSnapshot.model_validate(session)
        if execution_id is None:
            return None
        if not execution_id:
            raise ValueError("Native execution identity must be nonempty")
        if session.lineages is None:
            return None
        owners = {owner for lineage in session.lineages if (owner := lineage.owner(execution_id)) is not None}
        sid = session.session_id
        message_ids = frozenset(lineage.message_id for lineage in session.lineages)
        async for call in self.repo.iter_all("ops.tool_calls", filters={"session_id": sid}):
            if call.get("remote_event_id") != execution_id:
                continue
            if call.get("native_turn_message_id") in session.unselected_message_ids:
                continue
            owner = ToolInputLineage.from_record(call).owner(execution_id, message_ids)
            if owner is not None:
                owners.add(owner)
        if len(owners) > 1:
            raise ValueError("Native input has conflicting journal ownership")
        return next(iter(owners)) if owners else None

    async def completion_record(self, sid: str) -> dict[str, JsonValue]:
        identity = NativeSessionReference(session_id=sid)
        current: dict[str, JsonValue] | None = await self.repo.get("ops.agent_sessions", identity.session_id)
        if current is None:
            raise KeyError(identity.session_id)
        require_owned_session_identity(identity.session_id, current)
        CompletionSessionSnapshot.from_record(current)
        return current

    async def execution_barrier(
        self, session: CompletionSessionSnapshot, *, verify_remote: bool = False
    ) -> NativeExecutionBarrier:
        """Correlate accepted inputs with admitted native execution receipts."""
        snapshot = CompletionSessionSnapshot.model_validate(session)
        calls = [
            CompletionToolInput.from_record(row)
            async for row in self.repo.iter_all("ops.tool_calls", filters={"session_id": snapshot.session_id})
        ]
        observed = derive_execution_barrier(snapshot, calls)
        signature = tuple(observed.accepted_input_ids)
        poll = self.execution_polls.get(snapshot.session_id)
        should_read = poll is None or poll.signature != signature or time.monotonic() >= poll.deadline
        if verify_remote and observed.accepted_input_ids and not observed.unidentified_inputs and should_read:
            states: dict[str, NativeExecutionState] = {}
            pending_read = observed.pending_input_ids
            for offset in range(0, len(pending_read), 64):
                query = NativeExecutionQuery(execution_ids=pending_read[offset : offset + 64])
                response = await self.request(
                    "GET",
                    f"sessions/{quote(snapshot.session_id, safe='')}/executions",
                    params=[("ids[]", identity) for identity in query.execution_ids],
                )
                page = NativeExecutionPage.model_validate(response)
                for observed_state in page.correlate(query):
                    states[observed_state.execution_id] = observed_state
            async with self.repo.lock("session:" + snapshot.session_id):
                current = await self.completion_record(snapshot.session_id)
                fresh = CompletionSessionSnapshot.from_record(current)
                calls = [
                    CompletionToolInput.from_record(row)
                    async for row in self.repo.iter_all(
                        "ops.tool_calls", filters={"session_id": snapshot.session_id}
                    )
                ]
                derive_execution_barrier(fresh, calls)
                matches = (
                    fresh.execution_observation == snapshot.execution_observation
                    and fresh.execution_states == snapshot.execution_states
                )
                if states and matches:
                    merged = {**fresh.execution_states, **states}
                    current["native_execution_states"] = {
                        key: state.model_dump(mode="json") for key, state in merged.items()
                    }
                    owners = {message.message_id: message for message in fresh.journal}
                    retired = dict(fresh.superseded_inputs)
                    for call in calls:
                        identity = call.input_id
                        state = merged.get(identity) if identity is not None else None
                        owner = owners.get(call.message_id) if call.message_id is not None else None
                        if identity is None or state is None or state.state != "FAILED" or owner is None:
                            continue
                        previous = FailedToolAttempt(
                            input_id=identity, message_id=call.message_id, attempt=call.attempt
                        )
                        replacement = CompletionAttemptOwner(
                            message_id=owner.message_id,
                            attempt=owner.retry_attempt,
                            replacement_input_id=owner.native_input_id,
                        )
                        if previous.is_prior_to(replacement):
                            retired[identity] = PriorToolInputReceipt(
                                reason="FAILED_PRIOR_TURN_ATTEMPT",
                                queue_state=state,
                                replacement_input_id=owner.native_input_id,
                                message_id=owner.message_id,
                            )
                    if retired != fresh.superseded_inputs:
                        current["superseded_native_inputs"] = {
                            key: receipt.model_dump(mode="json", exclude_unset=True)
                            for key, receipt in retired.items()
                        }
                    observation = HostExecutionObservation(
                        source="HOST_MANAGED_EXECUTION_STATUS",
                        observation_id=new_id("native_status_observation"),
                        session_id=snapshot.session_id,
                        execution_ids=list(states),
                        recorded_at=self.clock_timestamp(),
                        versions=self.versions,
                    )
                    current["native_execution_observation"] = observation.model_dump(mode="json")
                    await put_owned_session(self.repo, snapshot.session_id, current)
                    snapshot = CompletionSessionSnapshot.from_record(current)
                else:
                    snapshot = fresh
            delay = min(poll.delay * 2, 1.0) if poll is not None and poll.signature == signature else 0.05
            self.execution_polls[snapshot.session_id] = ExecutionPoll(
                deadline=time.monotonic() + delay, delay=delay, signature=signature
            )
        return derive_execution_barrier(snapshot, calls)

    async def source_completion_snapshot(self, session: dict[str, JsonValue]) -> SourceCompletionSnapshot:
        journal = session.get("message_log", [])
        if not isinstance(journal, list):
            raise ValueError("Completion message journal must be an array")
        outcomes: dict[str, SourceCompletionEntry] = {}
        for raw in journal:
            if not isinstance(raw, dict):
                raise ValueError("Completion journal message must be an object")
            message = CompletionMessage.from_record(raw)
            if message.message_id in outcomes:
                raise ValueError("Completion journal repeats a message identity")
            item = (
                await self.repo.get("ingest.source_items", message.item_id)
                if message.source_message is True and message.item_id is not None
                else None
            )
            outcome = (
                item.get("extraction_status")
                if item is not None
                else "MISSING_SOURCE_ITEM"
                if message.source_message is True
                else "NOT_APPLICABLE"
                if message.source_message is False
                else None
            )
            outcomes[message.message_id] = SourceCompletionEntry.model_validate(
                {
                    "version": SOURCE_COMPLETION_VERSION,
                    "item_id": message.item_id,
                    "native_input_id": message.native_input_id,
                    "retry_attempt": message.retry_attempt,
                    "outcome": outcome,
                }
            )
        return SourceCompletionSnapshot(entries=outcomes)

    async def completed_turn(self, sid: str) -> NativeExecutionBarrier:
        result = await self.settled_turn(sid)
        if result.complete:
            await self.deliver_answers(sid)
        return result

    async def turn_answers(
        self, session: NativeOwnershipSnapshot, inputs: Sequence[str] | None = None
    ) -> dict[str, NativeTurnAnswer]:
        """Read actual descending managed history back to the requested accepted inputs."""
        query = NativeAnswerQuery(ownership=session, input_ids=None if inputs is None else tuple(inputs))
        session = query.ownership
        if session.lineages is None:
            raise ValueError("Native answers require their actual retained input journal")
        wanted = set(query.input_ids) if query.input_ids is not None else set()
        recent: list[AnswerHistoryEvent] = []
        events: dict[str, AnswerHistoryEvent] = {}
        page: str | None = None
        visited: set[str] = set()
        while True:
            params: dict[str, str | int] = {"limit": 100, "order": "desc"}
            if page is not None:
                params["page"] = page
            decoded = AnswerHistoryPage.from_wire(
                await self.request(
                    "GET", f"sessions/{quote(session.session_id, safe='')}/events", params=params
                )
            )
            for event in decoded.events:
                previous = events.get(event.event_id)
                if previous is not None:
                    if previous != event:
                        raise ValueError("Native answer history changed an observed event identity")
                    continue
                events[event.event_id] = event
                recent.append(event)
                wanted.discard(event.event_id)
            if decoded.next_page is None or (
                query.input_ids is not None and len(query.input_ids) > 0 and not wanted
            ):
                break
            if decoded.next_page in visited:
                raise ValueError("Native answer history did not advance its managed cursor")
            visited.add(decoded.next_page)
            page = decoded.next_page
        owners: dict[str, NativeInputOwner] = {}
        for event in reversed(recent):
            if isinstance(event, HistoryRunning) and event.execution_id is not None:
                owner = await self.native_input_owner(session, event.execution_id)
                if owner is not None:
                    owners[event.execution_id] = owner
        return collect_turn_answers(list(reversed(recent)), owners)

    async def deliver_answers(self, sid: str) -> None:
        """Settle each bound question from actual journal phase and observed owned history."""
        stored: dict[str, JsonValue] | None = await self.repo.get("ops.agent_sessions", sid)
        if stored is None:
            return
        require_owned_session_identity(sid, stored)
        snapshot = QuestionSettlementSession.from_record(stored)
        if snapshot.questions is None:
            return
        pending = [question for question in snapshot.questions if question.answer_recorded is not True]
        if not pending:
            return
        history_messages = frozenset(
            question.message_id for question in pending if question.turn_status != "FAILED"
        )
        answers: dict[str, NativeTurnAnswer] = {}
        expected_lineages: dict[str, JournalInputLineage] = {}
        if history_messages:
            ownership = NativeOwnershipSnapshot.from_record(stored, message_ids=history_messages)
            if ownership.lineages is not None:
                expected_lineages = {lineage.message_id: lineage for lineage in ownership.lineages}
                inputs = [
                    lineage.current_input_id
                    for lineage in ownership.lineages
                    if lineage.current_input_id is not None
                ]
                if inputs:
                    answers = await self.turn_answers(ownership, inputs)
        for question in pending:
            async with self.repo.lock("session:" + sid):
                current: dict[str, JsonValue] | None = await self.repo.get("ops.agent_sessions", sid)
                if current is None:
                    return
                require_owned_session_identity(sid, current)
                latest = QuestionSettlementSession.from_record(current)
                if latest.questions is None:
                    continue
                current_questions = {row.message_id: row for row in latest.questions}
                candidate = current_questions.get(question.message_id)
                if candidate is None:
                    continue
                if candidate.question_id != question.question_id:
                    raise ValueError("Question settlement journal owner changed its question binding")
                if candidate.answer_recorded is True:
                    continue
                answer = answers.get(question.message_id)
                ended = answer is not None and answer.ended
                stopped = latest.status in {"FAILED", "ENDED"}
                if candidate.turn_status == "FAILED" or (stopped and not ended):
                    receipt = await record_answer(self.repo, candidate.target(sid), None, status="FAILED")
                elif candidate.turn_status == "DONE" or ended:
                    if answer is not None:
                        ownership = NativeOwnershipSnapshot.from_record(
                            current, message_ids=frozenset({candidate.message_id})
                        )
                        if ownership.lineages is None or len(ownership.lineages) != 1:
                            raise ValueError("Question answer lost its actual retained journal owner")
                        if ownership.lineages[0] != expected_lineages[candidate.message_id]:
                            continue
                    receipt = await record_answer(
                        self.repo,
                        candidate.target(sid),
                        answer.text if answer is not None else None,
                        status="ANSWERED",
                    )
                else:
                    continue
                if receipt is None:
                    continue
                journal = current["message_log"]
                if not isinstance(journal, list):
                    raise ValueError("Native question journal must be an array")
                for row in journal:
                    if not isinstance(row, dict):
                        raise ValueError("Native question journal message must be an object")
                    if row["message_id"] == candidate.message_id:
                        row["answer_recorded"] = True
                await put_owned_session(self.repo, sid, current)

    async def abandon_question(self, session_id: str, question_id: str) -> bool:
        """Authorize host failure only for a missing or actually cancellable local intent."""
        target = FollowupRefusalTarget(question_id=question_id, session_id=session_id)
        async with (
            self.send_locks.setdefault(target.session_id, asyncio.Lock()),
            self.repo.lock("session:" + target.session_id),
        ):
            session: dict[str, JsonValue] | None = await self.repo.get(
                "ops.agent_sessions", target.session_id
            )
            if session is None:
                return True
            require_owned_session_identity(target.session_id, session)
            async with self.repo.lock("question:" + target.question_id):
                answered: dict[str, JsonValue] | None = await self.repo.get(
                    "out.user_questions", target.question_id
                )
                if (
                    answered is not None
                    and validate_question_identity(answered, target).answer_status is not None
                ):
                    await record_answer(self.repo, target, None, status="FAILED")
                    return False
            snapshot = QuestionSettlementSession.from_record(session)
            if snapshot.questions is None:
                raise UnprovedQuestionRetirement(
                    UnretainedQuestionJournal(target=target, reason="INPUT_JOURNAL_NOT_RETAINED")
                )
            questions = [
                question for question in snapshot.questions if question.question_id == target.question_id
            ]
            records: list[dict[str, JsonValue]] = []
            for question in questions:
                if question.answer_recorded is True or question.turn_status == "DONE":
                    return False
                row = outbound_message_record(session, question.message_id)
                if question.turn_status == "FAILED":
                    records.append(row)
                    continue
                if retained_outbound_acceptance(row) is not None:
                    return False
                records.append(row)
            for question in questions:
                if question.turn_status not in {"ACCEPTING", "FAILED"}:
                    raise UnprovedQuestionRetirement(
                        UnknownQuestionAcceptance.model_validate(
                            {
                                "target": target,
                                "message_id": question.message_id,
                                "turn_status": question.turn_status,
                                "reason": "INITIAL_ACCEPTANCE_NOT_RETAINED",
                            }
                        )
                    )
            if not records:
                return True
            retired = set(
                RetiredQuestionMessages.model_validate(
                    {"known_ids": session.get("retired_message_ids", [])}
                ).known_ids
            )
            for row in records:
                row["turn_status"] = "FAILED"
                row["answer_recorded"] = True
                identity = MessageIdentity.model_validate({"message_id": row["message_id"]}).message_id
                retired.add(identity)
            retired_wire: list[JsonValue] = [identity for identity in sorted(retired)]
            session["retired_message_ids"] = retired_wire
            active = ActiveQuestionIntent.model_validate({"message_id": session.get("active_message_id")})
            if active.message_id in retired:
                session["active_message_id"] = None
            await put_owned_session(self.repo, target.session_id, session)
            return True

    async def reconcile_answers(self) -> None:
        """Reconcile queued questions only through their actual stored session binding."""
        sessions: set[str] = set()
        async for record in self.repo.iter_all("out.user_questions", filters={"status": "QUEUED"}):
            question = QueuedQuestionSession.model_validate(
                {
                    "session_id": record.get("session_id"),
                    "status": record["status"],
                }
            )
            sid = question.session_id
            if sid is not None and not (sid in self.tasks and not self.tasks[sid].done()):
                sessions.add(sid)
        for sid in sorted(sessions):
            try:
                await self.deliver_answers(sid)
            except (httpx.HTTPError, ValueError, KeyError):
                continue

    async def settled_turn(self, sid: str) -> NativeExecutionBarrier:
        """Read committed queue states, drain history, and recheck actual accepted inputs."""
        await self.completion_record(sid)
        await self.recover_outbound_inputs(sid)
        session = await self.completion_record(sid)
        local = await self.execution_barrier(CompletionSessionSnapshot.from_record(session))
        source_outcomes = await self.source_completion_snapshot(session)
        if SessionCompletionStatus.from_record(session).status == "FAILED":
            return local.incomplete()
        if (
            local.complete
            and session.get("verified_native_completion") == local.storage_record()
            and session.get("verified_source_completion") == source_outcomes.storage_record()
        ):
            return local
        if not local.accepted_input_ids and not local.unidentified_inputs:
            return local
        if local.unidentified_inputs or not any(
            receipt.type == "session.status_idle" for receipt in local.terminal_receipts.values()
        ):
            poll = self.history_polls.get(sid)
            if poll is not None and time.monotonic() < poll.deadline:
                return local.incomplete()
            delay = poll.delay if poll is not None else 0.05
            await self.catch_up(sid)
            self.history_polls[sid] = HistoryPoll(
                deadline=time.monotonic() + delay, delay=min(delay * 2, 1.0)
            )
            session = await self.completion_record(sid)
        before = await self.execution_barrier(
            CompletionSessionSnapshot.from_record(session), verify_remote=True
        )
        if not before.complete:
            for identity in before.failed_input_ids:
                state = before.execution_states[identity]
                if state.state == "FAILED":
                    await self.process_event(
                        sid,
                        {
                            "id": "execution-status-failed:" + identity,
                            "type": "execution.status_failed",
                            "execution_id": identity,
                            "source": "MANAGED_EXECUTION_STATUS",
                            "queue_status": state.model_dump(mode="json"),
                            **self.clock_timestamp().fields("processed_at"),
                        },
                    )
            return before
        await self.catch_up(sid)
        current = await self.completion_record(sid)
        drained = await self.execution_barrier(CompletionSessionSnapshot.from_record(current))
        if not drained.complete or drained.accepted_input_ids != before.accepted_input_ids:
            return drained.incomplete()
        async with self.repo.lock("session:" + sid):
            current = await self.completion_record(sid)
            snapshot = CompletionSessionSnapshot.from_record(current)
            after = await self.execution_barrier(snapshot)
            if not after.complete or after.accepted_input_ids != before.accepted_input_ids:
                return after.incomplete()
            for message in snapshot.journal:
                record = outbound_message_record(current, message.message_id)
                if message.turn_status != "FAILED":
                    record["turn_status"] = "DONE"
                if message.source_message is True and message.item_id is not None:
                    await self.source_extraction.transition(
                        message.item_id,
                        frozenset({"PENDING", "RUNNING"}),
                        SkippedExtraction(
                            status="SKIPPED",
                            decision=None,
                            reason="NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION",
                        ),
                    )
            current["active_message_id"] = None
            current["runtime_status"] = "IDLE"
            outcomes = await self.source_completion_snapshot(current)
            for identity, outcome in outcomes.entries.items():
                record = outbound_message_record(current, identity)
                record["source_completion_verified"] = outcome.model_dump(mode="json")
                if "workflow_completion_verified" in record:
                    record["legacy_workflow_completion_verified"] = record.pop("workflow_completion_verified")
            current["verified_source_completion"] = outcomes.storage_record()
            current["verified_native_completion"] = after.storage_record()
            await put_owned_session(self.repo, sid, current)
            await session_progress(self.repo, self.settings, current)
            return after

    def start_stream(self, sid: str) -> None:
        sid = NativeSessionReference(session_id=sid).session_id
        if not self.closed and (sid not in self.tasks or self.tasks[sid].done()):
            self.tasks[sid] = asyncio.create_task(self.follow(sid), name="openma:" + sid)

    async def stop_followers(self) -> None:
        """Retire this controller's followers before finalizing owned local state."""
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.tasks.clear()

    async def finalize_replay_abort(
        self, target: ReplayAbortTarget, failure: ReplayAbortFailure | None
    ) -> ReplayAbortOutcome:
        """Record an owned host replay abort without inventing native or source outcomes."""
        target = ReplayAbortTarget.model_validate(target)
        sid, run_id = target.session_id, target.replay_run_id
        if run_id is None:
            return ReplayAbortRefused(session_id=sid)
        async with self.event_locks.setdefault(sid, asyncio.Lock()), self.repo.lock("session:" + sid):
            session: dict[str, JsonValue] | None = await self.repo.get("ops.agent_sessions", sid)
            if session is None:
                return ReplayAbortRefused(session_id=sid)
            ownership = ReplayAbortOwnership.from_record(session, sid)
            if not ownership.matches(target):
                return ReplayAbortRefused(session_id=sid)
            if failure is None:
                raise ValueError("Owned replay abort requires its actual observed failure")
            failure = ReplayAbortFailure.model_validate(failure)
            journal = ReplayAbortJournal.from_record(session)
            receipt = HostReplayAbortReceipt(
                replay_run_id=run_id,
                error_type=failure.error_type,
                reason=failure.reason,
                recorded_at=self.clock_timestamp(),
            ).storage_record()
            unfinished: list[str] = []
            if journal.messages is not None:
                for message in journal.messages:
                    if not message.unfinished:
                        continue
                    row = outbound_message_record(session, message.message_id)
                    row["turn_status"] = "FAILED"
                    row["replay_abort"] = copy.deepcopy(receipt)
                    unfinished.append(message.message_id)
            session.setdefault("terminal_failure", copy.deepcopy(receipt))
            session["replay_abort"] = copy.deepcopy(receipt)
            session["status"] = "FAILED"
            session["runtime_status"] = "IDLE"
            session["active_message_id"] = None
            session["pending_retry_event"] = None
            await put_owned_session(self.repo, sid, session)
            return ReplayAbortFinalized(session_id=sid, failed_message_ids=tuple(unfinished))

    def tool_context(self, session, call_id):
        """Every dispatch/recovery is attributed to the immutable agent session."""
        context = {
            key: session.get(key)
            for key in (
                "session_id",
                "event_id",
                "item_id",
                "proposal_id",
                "model_id",
                "prompt_version",
                "prompt_version_basis",
                "prompt_version_reason",
                "prompt_label",
                "prompt_binding",
                "model_id_basis",
                "model_id_reason",
                "agent_version_basis",
                "agent_version_reason",
                "agent_version",
                "config_version",
                "code_version",
            )
        }
        context["session_config_version"] = context["config_version"]
        context["session_code_version"] = context["code_version"]
        context["config_version"] = self.versions.config_version
        context["code_version"] = self.versions.code_version
        context.update(call_id=call_id, role="scout" if session.get("proposal_id") else "analyst")
        return context

    async def hydrate_prompt_provenance(self, session: dict[str, JsonValue]) -> None:
        state = SessionPromptState.from_record(session)
        if state.descriptor.has_resolved_snapshot():
            return
        native = await NativeSessionReader(self).prompt(
            state.session_id, prompt_label=state.descriptor.recovery_label()
        )
        if isinstance(native, NativeSessionMissing) or native.observation.provenance.prompt_version is None:
            raise ConfigurationError(
                "Resolved session prompt provenance unavailable; versioned outputs refused"
            )
        recovery = PromptRecoveryRecord(
            provenance=native.observation.provenance,
            recovered_at=self.clock_timestamp(),
            legacy=(
                state.descriptor
                if state.descriptor.prompt_version is not None and not state.has_legacy_descriptor
                else None
            ),
        )
        descriptor_data = recovery.provenance_document()
        recovered = recovery.recovery_document()
        async with self.repo.lock("session:" + state.session_id):
            current = await self.repo.get("ops.agent_sessions", state.session_id)
            if current is None:
                raise KeyError(state.session_id)
            if SessionPromptState.from_record(current).descriptor != state.descriptor:
                raise ValueError("Stored prompt provenance changed during native recovery")
            current.update(descriptor_data)
            current["recovered_prompt_descriptor"] = recovered
            if recovery.legacy is not None and "legacy_prompt_descriptor" not in current:
                current["legacy_prompt_descriptor"] = recovery.legacy.document()
            await put_owned_session(self.repo, state.session_id, current)
        session.update(descriptor_data)
        session["recovered_prompt_descriptor"] = recovered
        if recovery.legacy is not None and "legacy_prompt_descriptor" not in session:
            session["legacy_prompt_descriptor"] = recovery.legacy.document()

    async def invoke_tool(
        self,
        session,
        tool,
        arguments,
        call_id,
        *,
        decision_market_refresh_only=False,
        decision_market_refresh_completed=False,
        decision_prepared_condition_ids=None,
        resolved_arguments=None,
    ):
        if tool in {
            "record_event",
            "add_observation",
            "assess_event",
            "set_assessment",
            "set_beneficiaries",
            "merge_into",
            "schedule_milestone",
            "propose_pattern",
            "get_market_truth",
            "get_decision",
            "publish_card",
            "publish_update",
            "record_review",
            "set_watch_condition",
        }:
            await self.hydrate_prompt_provenance(session)
        if tool == "send_telegram_file" and not session.get("proposal_id"):
            return await self.send_file(session, arguments, call_id)
        if session.get("proposal_id") and tool not in ("web_search", "search_tank_items", "record_review"):
            raise ValueError("Scout tool is outside reviewer permissions")
        if getattr(self.settings, "replay", False) and tool in ("web_search", "fetch_page"):
            source_session_id = session.get("replay_source_session_id")
            if not source_session_id:
                raise ValueError("Replay external reads require explicit replay_source_session_id")
            candidates = [
                row
                async for row in self.repo.iter_all(
                    "ops.tool_calls",
                    filters={"tool": tool, "session_id": source_session_id},
                )
                if row.get("session_id") != session["session_id"]
                and row.get("status") in {"DONE", "ERROR", "REFUSED"}
                and row.get("arguments", row.get("input")) == arguments
            ]
            captures = [CapturedToolResult.from_record(row) for row in candidates]
            if len({capture.signature() for capture in captures}) != 1:
                raise ValueError("Replay requires one unambiguous stored external tool result")
            return captures[0]
        context = self.tool_context(session, call_id)
        if decision_market_refresh_only:
            context["_decision_market_refresh_only"] = True
        if decision_market_refresh_completed:
            context["_decision_market_refresh_completed"] = True
            context["_decision_prepared_condition_ids"] = decision_prepared_condition_ids
        fresh = await self.repo.get("ops.agent_sessions", session["session_id"]) or session
        arguments = await ModelContext(self.repo, self.settings, fresh).arguments(tool, arguments)
        if resolved_arguments is not None:
            resolved_arguments.update(copy.deepcopy(arguments))
        return await self.tool_dispatch(tool, arguments, context)

    def tool_failure(self, error: Exception) -> tuple[ToolFailure, FailureStatus]:
        """Return a bounded audited error; never persist provider URLs/secrets."""
        if isinstance(error, ConfigurationError):
            return ToolFailure(
                type="ConfigurationError",
                reason="Resolved session prompt provenance unavailable; versioned outputs refused",
                category="PROVENANCE",
            ), "REFUSED"
        if isinstance(error, ValidationError):
            return ToolFailure(
                type="ValidationError",
                reason="Tool data violates its declared schema; inspect the tool definitions.",
                category="VALIDATION",
            ), "REFUSED"
        if isinstance(error, (ValueError, KeyError)):
            safe, rejected = redact_tool_arguments({"reason": str(error)}, self.settings.configured_secrets())
            reason = (
                "Tool input rejected; credential-bearing diagnostic withheld" if rejected else safe["reason"]
            )
            if not isinstance(reason, str):
                raise TypeError("Redacted tool diagnostic must remain text")
            return ToolFailure(type=type(error).__name__, reason=reason, category="VALIDATION"), "REFUSED"
        category: FailureCategory
        if isinstance(error, httpx.HTTPStatusError):
            reason = f"Source request failed (HTTP {error.response.status_code})"
            category = "SOURCE_HTTP"
        elif isinstance(error, httpx.HTTPError):
            reason, category = "Source transport failed", "SOURCE_TRANSPORT"
        else:
            reason, category = (
                "Tool execution failed; inspect the audited call and implementation",
                "TOOL_EXECUTION",
            )
        return ToolFailure(type=type(error).__name__, reason=reason, category=category), "ERROR"

    async def tool(self, session, event):
        sid, call_id = session["session_id"], event["id"]
        requested_at = self.clock_timestamp()
        key = sid + ":" + call_id
        prior = await self.repo.get("ops.tool_calls", key)
        if prior and prior.get("returned_at"):
            return
        original_hash = sha256(event["input"])
        safe_arguments, credential_reason = redact_tool_arguments(
            event["input"], self.settings.configured_secrets()
        )
        event = {**event, "input": safe_arguments}
        resolved_arguments = {}
        slow_ready = bool(credential_reason)
        slow_result = None
        slow_error = (
            {"type": "CredentialRejected", "reason": credential_reason, "category": "VALIDATION"}
            if credential_reason
            else None
        )
        slow_error_status = "REFUSED"
        decision_market_refresh_completed = False
        decision_prepared_condition_ids = None
        if (
            not credential_reason
            and event["name"]
            in {"get_market_truth", "get_decision", "web_search", "fetch_page", "send_telegram_file"}
            and (not prior or prior.get("status") not in ("DONE", "REFUSED", "ERROR"))
        ):
            try:
                if event["name"] == "get_decision":
                    prepared = await self.invoke_tool(
                        session,
                        event["name"],
                        event["input"],
                        call_id,
                        decision_market_refresh_only=True,
                        resolved_arguments=resolved_arguments,
                    )
                    decision_prepared_condition_ids = prepared.get("prepared_condition_ids")
                    decision_market_refresh_completed = True
                else:
                    slow_result = await self.invoke_tool(
                        session, event["name"], event["input"], call_id, resolved_arguments=resolved_arguments
                    )
            except Exception as error:
                slow_error, slow_error_status = self.tool_failure(error)

            slow_ready = not decision_market_refresh_completed
        async with self.repo.lock("tool:" + key):
            stored = await self.repo.get("ops.tool_calls", key)
            if not stored:
                context = self.tool_context(session, call_id)
                async with self.repo.lock("session:" + sid):
                    sequencing = await self.repo.get("ops.agent_sessions", sid)
                    ordinal = int(sequencing.get("next_tool_call_ordinal", 0))
                    sequencing["next_tool_call_ordinal"] = ordinal + 1
                    await put_owned_session(self.repo, sid, sequencing)
                stored = {
                    "native_turn_call_index": ordinal,
                    "call_id": call_id,
                    "session_id": sid,
                    "event_id": session.get("event_id"),
                    "item_id": session.get("item_id"),
                    "tool": event["name"],
                    "native_source_execution_id": event.get("execution_id"),
                    "native_turn_message_id": event.get(
                        "_host_turn_message_id", session.get("active_message_id")
                    ),
                    "native_turn_retry_count": event.get(
                        "_host_turn_retry_count",
                        next(
                            (
                                row.get("retry_count", 0)
                                for row in session.get("message_log", [])
                                if row["message_id"] == session.get("active_message_id")
                            ),
                            0,
                        ),
                    ),
                    "arguments": event["input"],
                    "model_arguments": event["input"],
                    "input": event["input"],
                    "input_sha256": original_hash,
                    "arguments_redacted": bool(credential_reason),
                    "status": "PENDING",
                    **requested_at.fields("requested_at"),
                    **propagate_timestamp(event, "processed_at").fields("processed_at"),
                    **{
                        key: context[key]
                        for key in (
                            "model_id",
                            "prompt_version",
                            "prompt_version_basis",
                            "prompt_version_reason",
                            "prompt_label",
                            "agent_version",
                            "config_version",
                            "code_version",
                            "session_config_version",
                            "session_code_version",
                        )
                    },
                }
                await self.repo.put("ops.tool_calls", key, stored)
            if stored["status"] not in ("DONE", "REFUSED", "ERROR"):
                refusal = slow_error
                refusal_status = slow_error_status
                captured = None
                try:
                    async with AsyncExitStack() as mutation:
                        if stored["tool"] in {
                            "record_event",
                            "add_observation",
                            "set_assessment",
                            "set_beneficiaries",
                            "merge_into",
                            "schedule_milestone",
                            "propose_pattern",
                            "get_decision",
                            "publish_card",
                            "record_review",
                        }:
                            await mutation.enter_async_context(self.repo.lock("session:" + sid))
                        await mutation.enter_async_context(self.repo.transaction())
                        result = (
                            slow_result
                            if slow_ready
                            else await self.invoke_tool(
                                session,
                                stored["tool"],
                                stored.get("model_arguments", stored["input"]),
                                call_id,
                                resolved_arguments=resolved_arguments,
                                **(
                                    {
                                        "decision_market_refresh_completed": True,
                                        "decision_prepared_condition_ids": decision_prepared_condition_ids,
                                    }
                                    if decision_market_refresh_completed
                                    else {}
                                ),
                            )
                        )
                except Exception as error:
                    refusal, refusal_status = self.tool_failure(error)
                if not refusal and isinstance(result, CapturedToolResult):
                    captured = result
                    result = copy.deepcopy(captured.result)
                elif refusal:
                    result = FailedToolResponse(ok=False, error=refusal).model_dump(mode="json")
                context = self.tool_context(session, call_id)
                stored.update(
                    {
                        key: context[key]
                        for key in (
                            "model_id",
                            "model_id_basis",
                            "model_id_reason",
                            "prompt_version",
                            "prompt_version_basis",
                            "prompt_version_reason",
                            "prompt_label",
                            "prompt_binding",
                            "agent_version",
                            "agent_version_basis",
                            "agent_version_reason",
                        )
                    }
                )
                if resolved_arguments:
                    stored["arguments"] = copy.deepcopy(resolved_arguments)
                completed = self.clock_timestamp()
                stored.update(
                    result=result,
                    status=captured.status if captured else refusal_status if refusal else "DONE",
                    is_error=captured.is_error if captured else bool(refusal),
                    **completed.fields("answered_at"),
                    **completed.fields("completed_at"),
                )
                await self.repo.put("ops.tool_calls", key, stored)
        current = await self.repo.get("ops.agent_sessions", sid) or session
        if current.get("model_context_version", 0) < CONTEXT_VERSION:
            await self.sync_tools(sid, resume=True)
            current = await self.repo.get("ops.agent_sessions", sid) or session
        projected = await ModelContext(self.repo, self.settings, current).result(
            stored["tool"], stored["result"]
        )
        note = await self.transition_note(sid) if self.legacy_contract(session) else None
        if note:
            projected = (
                dict(projected, context_update=note)
                if isinstance(projected, dict)
                else {"result": projected, "context_update": note}
            )
        body = NativeToolResult(
            type="user.custom_tool_result",
            custom_tool_use_id=call_id,
            content=[
                NativeText(
                    type="text",
                    text=canonical_json(tool_wire_result(projected)),
                )
            ],
            is_error=True if stored.get("is_error") else None,
        ).model_dump(mode="json", exclude_none=True)
        async with self.repo.lock("tool:" + key):
            if (
                stored.get("native_result_body") is None
                or stored.get("model_context_version") != CONTEXT_VERSION
            ):
                if (
                    stored.get("native_result_body")
                    and stored.get("model_context_version") != CONTEXT_VERSION
                ):
                    stored["legacy_native_result_body"] = stored["native_result_body"]
                stored["model_context_version"] = CONTEXT_VERSION
                stored["native_result_body"] = copy.deepcopy(body)
                await self.repo.put("ops.tool_calls", key, stored)
            body = copy.deepcopy(stored["native_result_body"])
        accepted = await self.post_input(sid, parse_native_input(body), idem="tool:" + sha256(key))
        async with self.repo.lock("tool:" + key):
            stored.update(
                remote_event_id=self.accepted_input_id(accepted),
                **self.clock_timestamp().fields("returned_at"),
            )
            await self.repo.put("ops.tool_calls", key, stored)

    async def usage(self, session: NativeUsageSession, span: NativeUsageSpan) -> None:
        await record_native_usage(
            self.repo,
            session,
            span,
            versions=self.versions,
            collected_at=self.clock_timestamp(),
        )

    async def process_event(self, sid, event, seq=None):
        if event.get("type") in ("event_start", "event_delta") or not event.get("id"):
            return
        if not material_event(event) or seen_key(event) in self.seen_events.get(sid, ()):
            if seq is not None:
                self.pending_seq[sid] = max(int(seq), self.pending_seq.get(sid, 0))
            return
        key = sid + ":" + event["id"]
        async with self.event_locks.setdefault(sid, asyncio.Lock()):
            session = await self.repo.get("ops.agent_sessions", sid)
            if session is None:
                raise KeyError(sid)
            require_owned_session_identity(sid, session)
            if session.get("status") in ("FAILED", "ENDED"):
                return
            before = copy.deepcopy(session)
            kind = event["type"]
            if kind == "host.analysis_incomplete":
                async with self.repo.lock("session:" + sid):
                    current = await self.repo.get("ops.agent_sessions", sid)
                    current.setdefault("host_analysis_outcomes", {}).setdefault(
                        event["id"],
                        {**copy.deepcopy(event), "disposition": "IGNORED_LEGACY_SEMANTIC_GATE"},
                    )
                    pending = current.get("pending_retry_event") or {}
                    if pending.get("type") == kind and pending.get("id") == event["id"]:
                        current["pending_retry_event"] = None
                    if event["id"] not in current.setdefault("processed_events", []):
                        current["processed_events"].append(event["id"])
                    if seq is not None:
                        current["last_event_seq"] = max(int(current.get("last_event_seq", 0)), int(seq))
                    await put_owned_session(self.repo, sid, current)
                return
            duplicate = event["id"] in session.get("processed_events", [])
            if kind == "user.custom_tool_result" and isinstance(event.get("custom_tool_use_id"), str):
                tool_key = sid + ":" + event["custom_tool_use_id"]
                call = await self.repo.get("ops.tool_calls", tool_key)
                if (
                    call
                    and call.get("status") in {"DONE", "ERROR", "REFUSED"}
                    and not call.get("remote_event_id")
                ):
                    expected = call.get("native_result_body") or {
                        "type": "user.custom_tool_result",
                        "custom_tool_use_id": call["call_id"],
                        "content": [
                            {
                                "type": "text",
                                "text": canonical_json(tool_wire_result(call["result"])),
                            }
                        ],
                    }
                    if event.get("content") == expected.get("content") and bool(
                        event.get("is_error")
                    ) == bool(call.get("is_error")):
                        async with self.repo.lock("tool:" + tool_key):
                            call = await self.repo.get("ops.tool_calls", tool_key)
                            if not call.get("remote_event_id"):
                                call.update(
                                    remote_event_id=event["id"],
                                    native_result_body=copy.deepcopy(expected),
                                    native_ack_recovery="EXACT_ACCEPTED_EVENT_HISTORY",
                                )
                                if not call.get("returned_at"):
                                    call.update(self.clock_timestamp().fields("returned_at"))
                                await self.repo.put("ops.tool_calls", tool_key, call)
            if (
                kind
                in {
                    "session.status_running",
                    "session.status_idle",
                    "session.status_rescheduled",
                    "session.status_terminated",
                }
                and isinstance(event.get("execution_id"), str)
                and event["execution_id"]
            ):
                receipt = {
                    key: copy.deepcopy(event[key])
                    for key in ("id", "type", "execution_id", "processed_at", "stop_reason")
                    if key in event
                }
                prior = session.setdefault("native_execution_receipts", {}).get(event["id"])
                if prior and prior.get("execution_id") != event["execution_id"]:
                    raise ValueError("Native event execution correlation changed")
                session["native_execution_receipts"][event["id"]] = receipt
                if session.get("native_idle_receipt", {}).get("id") == event["id"]:
                    session["native_idle_receipt"].update(receipt)
            if duplicate:
                if session != before:
                    async with self.repo.lock("session:" + sid):
                        current = await self.repo.get("ops.agent_sessions", sid)
                        current.setdefault("native_execution_receipts", {}).update(
                            session.get("native_execution_receipts", {})
                        )
                        if current.get("native_idle_receipt", {}).get("id") == event["id"]:
                            current["native_idle_receipt"].update(
                                session["native_execution_receipts"][event["id"]]
                            )
                        await put_owned_session(self.repo, sid, current)
                self.seen_events.setdefault(sid, set()).add(seen_key(event))
                return
            if kind in {
                "session.status_running",
                "session.status_idle",
                "session.status_rescheduled",
                "session.status_terminated",
            }:
                self.execution_polls.pop(sid, None)
            if event.get("execution_id") and kind in {
                "agent.custom_tool_use",
                "session.status_running",
                "session.status_rescheduled",
                "session.error",
                "execution.status_failed",
            }:
                owner = await self.native_input_owner(
                    NativeOwnershipSnapshot.from_record(session), event["execution_id"]
                )
                if owner:
                    session["active_message_id"] = owner.message_id
                    ContextBinding.from_session(session)
                    event = dict(
                        event, _host_turn_message_id=owner.message_id, _host_turn_retry_count=owner.attempt
                    )
                elif kind in {
                    "agent.custom_tool_use",
                    "session.error",
                    "execution.status_failed",
                }:
                    raise ValueError("Native execution has no acknowledged accepted-input owner")
            ContextBinding.from_session(session)
            if kind == "agent.custom_tool_use":
                await self.tool(session, event)
                session = await self.repo.get("ops.agent_sessions", sid)
                call = await self.repo.get("ops.tool_calls", key)
                if (
                    event["name"] == "record_event"
                    and call
                    and call.get("result", {}).get("is_event") is False
                ):
                    session["no_event"] = True
            elif kind == "session.status_running":
                session["runtime_status"] = "RUNNING"
                active = next(
                    (
                        row
                        for row in session.get("message_log", [])
                        if row["message_id"] == session.get("active_message_id")
                    ),
                    None,
                )
                if active:
                    active["turn_status"] = "RUNNING"
            elif kind == "session.status_idle":
                session["native_idle_receipt"] = {
                    key: copy.deepcopy(event[key])
                    for key in ("id", "execution_id", "processed_at", "stop_reason")
                    if key in event
                }
                session["runtime_status"] = (
                    "IDLE"
                    if (event.get("stop_reason") or {}).get("type") == "end_turn"
                    else "REQUIRES_ACTION"
                )
            elif kind == "span.model_request_end":
                span = NativeUsageSpan.from_wire(event)
                if span.counters is not None:
                    await self.usage(NativeUsageSession.from_record(session), span)
            superseded_failure = bool(
                event.get("_host_turn_retry_count", 0)
                < next(
                    (
                        row.get("retry_count", 0)
                        for row in session.get("message_log", [])
                        if row["message_id"] == event.get("_host_turn_message_id")
                    ),
                    0,
                )
                or event.get("execution_id")
                and event["execution_id"] in session.get("superseded_native_inputs", {})
            )
            native_error = event.get("error")
            native_retry_status = native_error.get("retry_status") if isinstance(native_error, dict) else None
            native_retrying = (
                kind == "session.error"
                and isinstance(native_retry_status, dict)
                and native_retry_status.get("type") == "retrying"
            )
            if not superseded_failure and (native_retrying or kind == "session.status_rescheduled"):
                session["runtime_status"] = "PENDING"
            if not superseded_failure and (
                (kind == "session.error" and not native_retrying)
                or kind in ("agent.refusal", "execution.status_failed")
                or event.get("finish_reason") == "content-filter"
                or any(
                    block.get("type") == "refusal"
                    for block in event.get("content", [])
                    if isinstance(block, dict)
                )
            ):
                failure_binding = ContextBinding.from_session(session)
                active = next(
                    (
                        row
                        for row in session.get("message_log", [])
                        if row["message_id"] == session.get("active_message_id")
                    ),
                    None,
                )
                state = retry_state(session, active)
                if isinstance(state, UnknownRetryState):
                    raise UnknownNativeRetryState(state.reason)
                retry = state.attempt
                if isinstance(state, ReadyRetryState):
                    if state.body is None:
                        raise UnknownNativeRetryState("RETRY_BODY_NOT_RETAINED")
                    async with self.repo.lock("session:" + sid):
                        durable = await self.repo.get("ops.agent_sessions", sid)
                        if durable is None:
                            raise KeyError(sid)
                        plan = PendingNativeRetry.model_validate(
                            {
                                "phase": "PENDING",
                                "owner": state.owner,
                                "body": state.body,
                                "previous_input_id": state.previous_input_id,
                                "previous_attempt": retry,
                                "next_attempt": 1,
                                "failure_event_id": event["id"],
                            }
                        )
                        retained = durable.get("pending_retry_intent")
                        if retained is not None and PendingNativeRetry.model_validate(retained) != plan:
                            raise ValueError("Pending native retry intent changed before posting")
                        event = dict(event)
                        durable["pending_retry_event"] = copy.deepcopy(event)
                        durable["pending_retry_intent"] = jsonable(plan)
                        await put_owned_session(self.repo, sid, durable)
                    accepted = await self.post_input(sid, plan.body, idem=plan.idempotency_key(sid))
                    receipt = plan.accepted(accepted, self.clock_timestamp())
                    if active:
                        previous_input = active.get("remote_event_id")
                        if previous_input:
                            active.setdefault("native_input_attempts", {})[previous_input] = retry
                        replacement_input = self.accepted_input_id(accepted)
                        if replacement_input:
                            active.setdefault("native_input_attempts", {})[replacement_input] = retry + 1
                        if previous_input is not None:
                            active.setdefault("superseded_native_input_ids", []).append(previous_input)
                        if previous_input:
                            session.setdefault("superseded_native_inputs", {})[previous_input] = {
                                "reason": "TURN_FAILED_AND_RETRIED",
                                "failure_event_id": event["id"],
                                "replacement_input_id": self.accepted_input_id(accepted),
                            }
                        active.update(
                            retry_count=retry + 1,
                            turn_status="RUNNING",
                            remote_event_id=self.accepted_input_id(accepted),
                            retry_acceptance=jsonable(receipt),
                        )
                    else:
                        session["retry_count"] = retry + 1
                        session["retry_acceptance"] = jsonable(receipt)
                    session["pending_retry_event"] = None
                    session["pending_retry_intent"] = None
                    session["runtime_status"] = "RUNNING"
                else:
                    session["status"] = "FAILED"
                    terminal_failure = {
                        "event_id": event["id"],
                        "event_type": kind,
                        **{
                            key: copy.deepcopy(event[key])
                            for key in (
                                "execution_id",
                                "reason",
                                "error",
                                "message",
                                "finish_reason",
                                "source",
                                "queue_status",
                            )
                            if key in event
                        },
                        **self.clock_timestamp().fields("recorded_at"),
                    }
                    session["terminal_failure"] = terminal_failure
                    session["runtime_status"] = "IDLE"
                    session["active_message_id"] = None
                    session["pending_retry_event"] = None
                    if active:
                        active["turn_status"] = "FAILED"
                        active["terminal_failure"] = copy.deepcopy(terminal_failure)
                    item_id = failure_binding.observation_source()
                    if item_id:
                        await self.source_extraction.transition(
                            item_id,
                            frozenset({"PENDING"}),
                            FailedExtraction(status="FAILED", reason="NATIVE_TURN_FAILED"),
                        )
            elif kind == "session.status_terminated":
                session["status"] = "ENDED"
                session["runtime_status"] = "IDLE"
                session["active_message_id"] = None
                session["pending_retry_event"] = None
                termination = {
                    "event_id": event["id"],
                    "event_type": kind,
                    **{key: copy.deepcopy(event[key]) for key in ("reason", "execution_id") if key in event},
                    **self.clock_timestamp().fields("recorded_at"),
                }
                session["termination_receipt"] = termination
                for message in session.get("message_log", []):
                    if message.get("turn_status") not in {"DONE", "FAILED"}:
                        message["turn_status"] = "FAILED"
                        message["terminal_failure"] = copy.deepcopy(termination)
            async with self.repo.lock("session:" + sid):
                current = await self.repo.get("ops.agent_sessions", sid)
                current.update(
                    {
                        k: v
                        for k, v in session.items()
                        if k != "message_log" and (k not in before or v != before[k])
                    }
                )
                prior_messages = {row["message_id"]: row for row in before.get("message_log", [])}
                live_messages = {row["message_id"]: row for row in current.get("message_log", [])}
                for row in session.get("message_log", []):
                    if row["message_id"] in live_messages:
                        old = prior_messages.get(row["message_id"], {})
                        live_messages[row["message_id"]].update(
                            {key: value for key, value in row.items() if key not in old or old[key] != value}
                        )
                current.setdefault("processed_events", []).append(event["id"])
                cursor = max(int(seq) if seq is not None else 0, self.pending_seq.pop(sid, 0))
                if cursor:
                    current["last_event_seq"] = max(int(current.get("last_event_seq", 0)), cursor)
                current["last_event_id"] = event["id"]
                current.update(self.event_timestamp(event).fields("last_event_at"))
                current.update(propagate_timestamp(event, "processed_at").fields("processed_at"))
                await put_owned_session(self.repo, sid, current)
                if current.get("status") in {"FAILED", "ENDED"}:
                    await session_progress(self.repo, self.settings, current)
            self.seen_events.setdefault(sid, set()).add(seen_key(event))
            if current.get("status") in ("FAILED", "ENDED") or (
                kind == "session.status_idle"
                and (event.get("stop_reason") or {}).get("type") == "end_turn"
                and any(
                    row.get("payload", {}).get("kind") == "USER_QUESTION" and not row.get("answer_recorded")
                    for row in current.get("message_log", [])
                )
            ):
                await self.deliver_answers(sid)

    async def observe_native_event(
        self, sid: str, event: dict[str, JsonValue], sequence: int | None = None
    ) -> NativeEventOutcome:
        sequence = NativeStoredCursor.model_validate({"value": sequence}).value
        if (
            event.get("type") in ("event_start", "event_delta")
            or not event.get("id")
            or not material_event(event)
        ):
            await self.process_event(sid, event, sequence)
            return NativeEventObserved(status="OBSERVED")
        frame = NativeFrameIdentity.from_wire(event)
        recorded = await self.retry_refusals.find(sid, frame)
        if recorded is not None:
            self.seen_events.setdefault(sid, set()).add(seen_key(event))
            return NativeEventRetryRefused(status="RETRY_REFUSED", receipt=recorded)
        try:
            await self.process_event(sid, event, sequence)
        except (UnknownNativeRetryState, UnprovedInputOwnership) as refused:
            receipt = await self.retry_refusals.record(sid, frame, refused.reason, sequence)
            self.seen_events.setdefault(sid, set()).add(seen_key(event))
            log.warning("Native host input effect refused: %s", receipt.reason)
            return NativeEventRetryRefused(status="RETRY_REFUSED", receipt=receipt)
        return NativeEventObserved(status="OBSERVED")

    async def catch_up(self, sid: str) -> None:
        cursor: NativeEventCursor | None = None
        visited: set[str] = set()
        while True:
            params: dict[str, str | int] = {"limit": 100, "order": "asc"}
            if isinstance(cursor, ManagedEventCursor):
                params["page"] = cursor.page
            elif isinstance(cursor, NodeEventCursor):
                params["after_seq"] = cursor.after_seq
            page = decode_native_page(
                await self.request("GET", f"sessions/{quote(sid, safe='')}/events", params=params)
            )
            following = next_native_cursor(page, cursor)
            if following is not None:
                key = canonical_json(following)
                if key in visited:
                    raise ValueError("Native event paging repeated its cursor")
                visited.add(key)
            for event in page.data:
                await self.observe_native_event(sid, event, native_event_sequence(event))
            if following is None:
                return
            cursor = following

    async def stream(self, sid: str) -> None:
        session = await self.repo.get("ops.agent_sessions", sid)
        if session is None:
            raise KeyError(sid)
        require_owned_session_identity(sid, session)
        headers = self.headers()
        cursor = session.get("last_event_seq")
        if cursor is not None:
            observed = NativeStoredCursor.model_validate({"value": cursor}).value
            if observed is not None:
                headers["Last-Event-ID"] = str(observed)
        async with aconnect_sse(
            self.stream_http,
            "GET",
            self.url(f"sessions/{quote(sid, safe='')}/events/stream"),
            headers=headers,
        ) as source:
            source.response.raise_for_status()
            async for message in source.aiter_sse():
                if self.closed:
                    return
                if message.event == "ping" or message.event == "message" and message.data == "":
                    continue
                frame = decode_native_stream(message.data, message.id)
                await self.observe_native_event(sid, frame.event, frame.sequence)

    async def can_archive(self, candidate: ArchiveCandidate) -> bool:
        """Terminal domain state cannot preempt a pending denial/EXIT turn."""
        if isinstance(candidate, ArchiveDenied):
            ArchiveDenied.model_validate(candidate)
            return False
        if isinstance(candidate, InvalidatedArchiveCandidate):
            candidate = InvalidatedArchiveCandidate.model_validate(candidate)
        else:
            candidate = TerminalArchiveCandidate.model_validate(candidate)
        eid, sid = candidate.event_id, candidate.session_id
        async for row in self.repo.iter_all("ana.calls", filters={"event_id": eid, "status": "OPEN"}):
            blocker = ArchiveEventBlocker.model_validate(
                {"event_id": row["event_id"], "status": row["status"]}
            )
            if blocker.event_id != eid or blocker.status != "OPEN":
                raise ValueError("Archive call differs from the requested event or phase")
            return False
        async for row in self.repo.iter_all(
            "ana.watch_conditions", filters={"event_id": eid, "status": "ARMED"}
        ):
            blocker = ArchiveEventBlocker.model_validate(
                {"event_id": row["event_id"], "status": row["status"]}
            )
            if blocker.event_id != eid or blocker.status != "ARMED":
                raise ValueError("Archive watch differs from the requested event or phase")
            return False
        async for row in self.repo.iter_all("ops.tool_calls", filters={"session_id": sid}):
            if archive_tool(row, sid).pending:
                return False
        alerts: list[dict[str, JsonValue]] = [
            row async for row in self.repo.iter_all("out.alerts", filters={"event_id": eid})
        ]
        alert_ids: set[str] = set()
        for row in alerts:
            alert = FollowupAlertScope.model_validate(
                {"alert_id": row["alert_id"], "event_id": row["event_id"]}
            )
            if alert.event_id != eid:
                raise ValueError("Archive publication belongs to another event")
            alert_ids.add(alert.alert_id)
        for status in ("QUEUED", "RUNNING"):
            async for row in self.repo.iter_all("ops.jobs", filters={"status": status}):
                job = PendingArchiveJob.from_record(row)
                if job.status != status:
                    raise ValueError("Archive queue read differs from its requested phase")
                if job.blocks(session_id=sid, event_id=eid, alert_ids=alert_ids):
                    return False
        if isinstance(candidate, InvalidatedArchiveCandidate) and alerts:
            if candidate.session_finalized_at is None:
                return False
            latest = terminal_archive_card([archive_card(alert) for alert in alerts])
            if latest is None:
                return False
            async for row in self.repo.iter_all(
                "out.deliveries",
                filters={"alert_id": latest.alert_id, "version": latest.version, "status": "SENT"},
            ):
                delivery = ArchiveDelivery.model_validate(
                    {"alert_id": row["alert_id"], "version": row["version"], "status": row["status"]}
                )
                if delivery.alert_id != latest.alert_id or delivery.version != latest.version:
                    raise ValueError("Archive delivery differs from the actual final publication")
                return True
            return False
        return True

    async def finish_if_idle(self, sid: str) -> bool:
        """Return owned turn completion even when policy keeps the session unarchived."""
        verified = await self.completed_turn(sid)
        if not verified.complete:
            return False
        async with self.repo.lock("session:" + sid):
            session: dict[str, JsonValue] | None = await self.repo.get("ops.agent_sessions", sid)
            if session is None:
                return False
            require_owned_session_identity(sid, session)
            snapshot = CompletionSessionSnapshot.from_record(session)
            current_barrier = await self.execution_barrier(snapshot)
            if (
                not current_barrier.complete
                or current_barrier.accepted_input_ids != verified.accepted_input_ids
            ):
                return False
            state = RetainedSession.from_record(session)
            event: dict[str, JsonValue] | None = (
                await self.repo.get("evt.events", state.bound_event_id)
                if state.bound_event_id is not None
                else None
            )
            if event is not None and event.get("event_id") != state.bound_event_id:
                raise ValueError("Archive event identity differs from its actual lookup")
            archive = (
                await self.can_archive(archive_candidate(session, event)) if event is not None else False
            )
            if event is None and state.known_eventless:
                unfinished = [
                    archive_tool(row, sid)
                    async for row in self.repo.iter_all("ops.tool_calls", filters={"session_id": sid})
                ]
                archive = not any(tool.pending for tool in unfinished) and not any(
                    row.turn_status == "PENDING" for row in snapshot.journal
                )
            replay = self.settings.replay
            if (
                archive
                and not replay
                and await self.followable(FollowupPublicationScope.from_record(session))
            ):
                archive = False
                if state.followups_only is not True:
                    session["followups_only"] = True
                    await put_owned_session(self.repo, sid, session)
            elif archive and not replay:
                policy = RetentionPolicy(keep_days=self.settings.thresholds().session_keep_days)
                last = await self.last_message_at(SessionActivity.from_record(session))
                due = policy.deadline(last)
                if utcnow() < parse_time(due.value):
                    archive = False
                    retained: dict[str, JsonValue] = {
                        **last.fields("last_message_at"),
                        **due.fields("archive_after"),
                    }
                    owned = timestamp_owned_fields("last_message_at") | timestamp_owned_fields(
                        "archive_after"
                    )
                    if any(session.get(key) != value for key, value in retained.items()) or any(
                        key in session for key in owned if key not in retained
                    ):
                        for key in owned:
                            session.pop(key, None)
                        session.update(retained)
                        await put_owned_session(self.repo, sid, session)
                else:
                    session["archive_reason"] = "RETENTION"
                    session["archive_keep_days"] = policy.keep_days
                    for key in timestamp_owned_fields("last_message_at"):
                        session.pop(key, None)
                    session.update(last.fields("last_message_at"))
            elif not archive and state.archive_after is not None:
                for key in timestamp_owned_fields("archive_after"):
                    session.pop(key, None)
                await put_owned_session(self.repo, sid, session)
            if archive:
                await self.request("POST", f"sessions/{sid}/archive", json={})
                session.update(status="ENDED", **self.clock_timestamp().fields("archived_at"))
                session.setdefault("archive_reason", "REPLAY" if replay else "RETENTION")
                await put_owned_session(self.repo, sid, session)
            return True

    async def last_message_at(self, session: SessionActivity) -> RecordedActivity:
        """Select the latest actual session, input, native event or question clock."""
        session = SessionActivity.model_validate(session)
        questions = [
            QuestionActivity.from_record(row, session.session_id)
            async for row in self.repo.iter_all(
                "out.user_questions", filters={"session_id": session.session_id}
            )
        ]
        return latest_activity(session, questions)

    async def followable(self, scope: FollowupPublicationScope) -> bool:
        """A session whose card or answer reached Telegram stays open for reader follow-ups."""
        scope = FollowupPublicationScope.model_validate(scope)
        async for row in self.repo.iter_all("out.user_questions", filters={"session_id": scope.session_id}):
            question = FollowupQuestionScope.model_validate({"session_id": row["session_id"]})
            if question.session_id != scope.session_id:
                raise ValueError("Follow-up question belongs to another session")
            return True
        if scope.event_id is None:
            return False
        async for row in self.repo.iter_all("out.alerts", filters={"event_id": scope.event_id}):
            alert = FollowupAlertScope.model_validate(
                {"alert_id": row["alert_id"], "event_id": row["event_id"]}
            )
            if alert.event_id != scope.event_id:
                raise ValueError("Follow-up publication belongs to another event")
            async for row in self.repo.iter_all(
                "out.deliveries",
                filters={"alert_id": alert.alert_id, "channel": "telegram:alerts", "status": "SENT"},
            ):
                delivery = FollowupDeliveryScope.model_validate(
                    {name: row[name] for name in FollowupDeliveryScope.model_fields}
                )
                if delivery.alert_id != alert.alert_id:
                    raise ValueError("Follow-up delivery belongs to another publication")
                return True
        return False

    async def follower_state(self, sid: str) -> FollowerObservation | None:
        """Resolve only unreported runtime state from actual identity-bound native facts."""
        session: dict[str, JsonValue] | None = await self.repo.get("ops.agent_sessions", sid)
        if session is None:
            return None
        initial = FollowedSession.from_record(session, sid)
        if initial.status in {"FAILED", "ENDED"} or initial.runtime_status is not None:
            return FollowerReady(phase="READY", state=initial)
        observed = await NativeSessionReader(self).state(sid)
        async with self.repo.lock("session:" + sid):
            current: dict[str, JsonValue] | None = await self.repo.get("ops.agent_sessions", sid)
            if current is None:
                return None
            state = FollowedSession.from_record(current, sid)
            if state != initial:
                return FollowerReady(phase="READY", state=state)
            current["remote_state"] = {
                **observed.model_dump(mode="json"),
                **self.clock_timestamp().fields("checked_at"),
            }
            if isinstance(observed, NativeSessionMissing) or observed.archived_at is not None:
                await put_owned_session(self.repo, sid, current)
                return FollowerUnavailable(phase="UNAVAILABLE", observation=observed)
            current["runtime_status"] = native_runtime_status(observed.status)
            if observed.status == "terminated":
                current["status"] = "ENDED"
            if state.event_id is None:
                async for row in self.repo.iter_all("ops.tool_calls", filters={"session_id": sid}):
                    if (
                        row.get("tool") == "record_event"
                        and RecordedEventDecision.from_record(row, sid).is_event is False
                    ):
                        current["no_event"] = True
                        break
            await put_owned_session(self.repo, sid, current)
            return FollowerReady(phase="READY", state=FollowedSession.from_record(current, sid))

    async def follow(self, sid: str) -> None:
        delay = 1
        while not self.closed:
            stream_task: asyncio.Task[None] | None = None
            try:
                stream_task = asyncio.create_task(self.stream(sid))
                await self.catch_up(sid)
                observation = await self.follower_state(sid)
                if observation is None:
                    return
                if isinstance(observation, FollowerUnavailable):
                    raise NativeFollowerUnavailable(observation)
                if observation.state.status in ("FAILED", "ENDED") or await self.finish_if_idle(sid):
                    return
                while not self.closed:
                    done, _ = await asyncio.wait([stream_task], timeout=10)
                    if done:
                        await stream_task
                        if await self.finish_if_idle(sid):
                            return
                        break
                    await self.catch_up(sid)
                    session = await self.repo.get("ops.agent_sessions", sid)
                    if session is None:
                        return
                    state = FollowedSession.from_record(session, sid)
                    if state.status in ("FAILED", "ENDED") or await self.finish_if_idle(sid):
                        return
                delay = 1
            except asyncio.CancelledError:
                raise
            except (httpx.HTTPError, ValueError, KeyError, RuntimeError):
                delay = min(delay * 2, 30)
            finally:
                if stream_task is not None and not stream_task.done():
                    stream_task.cancel()
                    await asyncio.gather(stream_task, return_exceptions=True)
            await asyncio.sleep(delay)

    def admission(self) -> SessionAdmission:
        return build_admission(
            self.repo,
            self.settings.load_config("openma"),
            self.clock_timestamp,
            timedelta(seconds=ADMISSION_SLEEP_S),
        )

    def running_session_cap(self) -> int:
        return self.admission().policy.max_running_sessions

    async def admit(self, job: dict[str, JsonValue]) -> None:
        await self.admit_candidate(admission_candidate(job))

    async def admit_candidate(self, candidate: AdmissionCandidate) -> None:
        await self.admission().admit(AdmissionCandidate.model_validate(candidate))

    async def wake_waiting(self) -> None:
        await self.admission().wake()

    async def scout_review(self, payload: ScoutReviewJob) -> ScoutSessionReference:
        proposal = await self.repo.get("scout.expansion_proposals", payload.proposal_id)
        if proposal is None:
            raise KeyError(payload.proposal_id)
        identity = ScoutProposalIdentity.model_validate(
            {field: proposal[field] for field in ("prp_id",) if field in proposal}
        )
        if identity.prp_id != payload.proposal_id:
            raise ValueError("Scout proposal identity conflicts with its storage key")
        message = ScoutMessage(kind="SCOUT_REVIEW", proposal_id=payload.proposal_id)
        outcome = await self.dispatch_message(
            MessageDispatchRequest.from_wire(
                message.model_dump(mode="json", exclude_unset=True, by_alias=True)
            )
        )
        if isinstance(outcome, DeclinedQuestionDispatch):
            raise ValueError("Scout dispatch cannot be a question refusal")
        session: dict[str, JsonValue] | None = await self.repo.get("ops.agent_sessions", outcome.session_id)
        if session is None:
            raise KeyError(outcome.session_id)
        require_owned_session_identity(outcome.session_id, session)
        reference = ScoutSessionReference.model_validate(
            {field: session[field] for field in ("session_id", "proposal_id") if field in session}
        )
        if reference.proposal_id != payload.proposal_id:
            raise ValueError("Scout session belongs to another proposal")
        return reference

    async def run(self, stop: asyncio.Event) -> None:
        tick = 0
        while not stop.is_set() and not self.closed:
            async for record in self.repo.iter_metadata(
                "ops.agent_sessions", SESSION_FOLLOW_FIELDS, filters={"status": "OPEN"}
            ):
                session = SessionSweepEntry.from_record(record)
                sid = session.session_id
                following = sid in self.tasks and not self.tasks[sid].done()
                if not session.should_probe(
                    following=following, periodic_probe=tick % IDLE_RECHECK_TICKS == 0, now=utcnow()
                ):
                    continue
                if await self.finish_if_idle(sid):
                    continue
                self.start_stream(sid)
            if tick % IDLE_RECHECK_TICKS == 0:
                await self.reconcile_answers()
            tick += 1
            await self.wake_waiting()
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=5)

    async def close(self) -> None:
        self.closed = True
        await self.stop_followers()
        if self.owns_stream_http:
            await self.stream_http.aclose()
        if self.owns_http:
            await self.http.aclose()
