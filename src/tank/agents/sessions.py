"""OpenMA managed-agents v1 adapter, verified at upstream 85d5361f535c.

Durable event-list reconciliation complements SSE: upstream's strict managed
stream ignores Last-Event-ID, while the native Node transport honors it.
"""

import asyncio
import base64
import copy
import json
import logging
import time
from contextlib import AsyncExitStack, suppress
from dataclasses import dataclass
from datetime import timedelta
from urllib.parse import quote

import httpx
from httpx_sse import aconnect_sse

from tank.agents import workflow
from tank.agents.context import CONTEXT_TRANSITION, CONTEXT_VERSION, ModelContext
from tank.agents.incomplete import UNFINISHED, fail_incomplete_turn, fail_with_session
from tank.agents.pipeline import ConfigurationError
from tank.agents.security import redact_tool_arguments
from tank.agents.wire import tool_wire_result
from tank.delivery import DeliveryDeferred
from tank.openma_sync import agent_tools
from tank.ops_progress import SOURCE_MESSAGE_KINDS, item_progress, session_progress
from tank.settings import threshold
from tank.sources.capture_fetch import CaptureRateLimited
from tank.telegram_inbox import DOCUMENT_LIMIT, closed_reason, question_message, record_answer
from tank.time_precision import collection_clock, offset_schedule, propagate_timestamp
from tank.util import canonical_json, iso, parse_time, sha256, utcnow

REST_TIMEOUT_S = 30

DEFAULT_MAX_RUNNING_SESSIONS = 2
DEFAULT_COMPLETION_REMINDERS = 20
ADMITTED_JOB_TYPES = ("AGENT_MESSAGE", "SCOUT_REVIEW")

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
FOLLOW_FAILURE_LOG_INTERVAL_S = 300
log = logging.getLogger(__name__)


def seen_key(event):
    """Identity of a handled stream event, including the execution correlation it carried."""
    return event["id"] + "|" + str(event.get("execution_id") or "")


def material_event(event):
    """Whether a stream event can change Tank state (a refusal can arrive in any message)."""
    if event.get("type") in MATERIAL_EVENT_TYPES or event.get("finish_reason") == "content-filter":
        return True
    return any(
        isinstance(block, dict) and block.get("type") == "refusal" for block in event.get("content") or []
    )


SESSION_ADMISSION_FIELDS = (
    "session_id",
    "status",
    "runtime_status",
    "active_message_id",
    "followups_only",
    "archive_after",
    "archived_at",
    "remote_state",
    "agent_incomplete",
    "reanalysis_attempt",
)
JOB_ADMISSION_FIELDS = ("job_id", "status", "priority", "payload", "slot_waiting_since", "slot_admitted")
BUSY_RUNTIME_STATES = {"PENDING", "RUNNING", "REQUIRES_ACTION"}


class AdmissionDeferred(Exception):
    """The running-session cap is full: the message waits in order, no attempt is spent."""

    def __init__(self, not_before):
        self.not_before = not_before
        super().__init__("Running-session cap reached; the message waits for a free slot")


@dataclass(frozen=True)
class CapturedToolResult:
    """An external capture's complete outcome, not a newly successful fetch."""

    result: object
    status: str
    is_error: bool

    @classmethod
    def from_record(cls, record):
        status = record.get("status")
        if status not in {"DONE", "ERROR", "REFUSED"}:
            raise ValueError("External replay capture is not durably complete")
        is_error = record.get("is_error", status != "DONE")
        if not isinstance(is_error, bool) or is_error != (status != "DONE"):
            raise ValueError("External replay capture has inconsistent outcome metadata")
        return cls(copy.deepcopy(record["result"]), status, is_error)

    def signature(self):
        return canonical_json({"result": self.result, "status": self.status, "is_error": self.is_error})


EVENT_INPUTS = frozenset({"ITEM_ADDED", "CONDITION_FIRED", "MILESTONE", "CALL_EXPIRED"})


class SessionService:
    def __init__(self, repo, settings, tool_dispatch, *, http=None, stream_http=None):
        self.repo, self.settings, self.tool_dispatch = repo, settings, tool_dispatch
        self.http = http or httpx.AsyncClient(timeout=REST_TIMEOUT_S)
        self.owns_http = http is None

        self.stream_http = stream_http or (
            http
            if http is not None
            else httpx.AsyncClient(
                timeout=httpx.Timeout(30, read=None),
                limits=httpx.Limits(max_connections=None, max_keepalive_connections=20),
            )
        )
        self.owns_stream_http = stream_http is None and http is None
        self.tasks = {}
        self.event_locks = {}
        self.send_locks = {}
        self.execution_polls = {}
        self.history_polls = {}
        self.pending_seq = {}
        self.seen_events = {}
        self.closed = False
        self.telegram = None

    def clock_timestamp(self):

        return collection_clock(iso())

    def event_timestamp(self, event):
        return (
            propagate_timestamp(event, "processed_at")
            if event.get("processed_at") is not None
            else self.clock_timestamp()
        )

    def agent_provenance(self, remote, config, agent_id):
        """Bind generation to the actual resolved native session-agent snapshot."""
        snapshot = remote.get("agent") if isinstance(remote.get("agent"), dict) else {}
        version = snapshot.get("version")
        basis = "RESOLVED_SESSION_AGENT"
        if version is None:
            version, basis = config.get("agent_version"), "DEPLOYMENT_DECLARATION"
        pinned_skills = all(
            isinstance(skill, dict) and skill.get("version") not in (None, "latest")
            for skill in snapshot.get("skills", [])
        )
        complete = all(field in snapshot for field in ("system", "skills", "tools")) and pinned_skills
        prompt = {
            field: snapshot.get(field)
            for field in ("system", "skills", "tools", "model", "mcp_servers", "multiagent", "_oma")
        }
        return {
            "agent_id": snapshot.get("id") or agent_id,
            "model_id": (snapshot.get("model") or {}).get("id") or self.settings.openma_model,
            "model_id_basis": "RESOLVED_SESSION_AGENT"
            if (snapshot.get("model") or {}).get("id")
            else "DEPLOYMENT_DECLARATION",
            "agent_version": str(version) if version is not None else None,
            "agent_version_basis": basis if version is not None else None,
            "prompt_label": config.get("prompt_version"),
            "prompt_version": sha256(prompt) if complete else None,
            "prompt_version_basis": "RESOLVED_SESSION_AGENT_SNAPSHOT" if complete else None,
            "prompt_version_reason": None
            if complete
            else "INCOMPLETE: remote resolved prompt snapshot unavailable",
            "prompt_binding": {
                "snapshot_fields": sorted(prompt),
                "resolved_skill_versions": [
                    {key: skill[key] for key in ("type", "skill_id", "version") if key in skill}
                    for skill in snapshot.get("skills", [])
                    if isinstance(skill, dict)
                ],
            }
            if complete
            else None,
        }

    def config(self):
        config = self.settings.load_config("openma")
        if not config.get("agent_id") or not config.get("environment_id"):
            raise ConfigurationError("openma.yaml requires deployed agent_id and environment_id")
        return config

    def headers(self, idem=None):
        headers = {"anthropic-beta": "managed-agents-2026-04-01"}
        if self.settings.openma_api_key:
            headers["x-api-key"] = self.settings.openma_api_key
        if idem:
            headers["Idempotency-Key"] = idem
        return headers

    def url(self, path):
        return self.settings.openma_url.rstrip("/") + "/v1/" + path

    async def request(self, method, path, **kwargs):
        headers = self.headers(kwargs.pop("idem", None))

        kwargs.setdefault("timeout", REST_TIMEOUT_S)
        response = await self.http.request(method, self.url(path), headers=headers, **kwargs)
        response.raise_for_status()
        return response.json()

    async def resolve(self, payload):
        replay_run_id = payload.get("replay_run_id") if getattr(self.settings, "replay", False) else None
        if getattr(self.settings, "replay", False) and not replay_run_id:
            raise ValueError("Replay requires a fresh explicit replay_run_id")
        if payload.get("session_id"):
            session = await self.repo.get("ops.agent_sessions", payload["session_id"])
            if session is None:
                raise KeyError(payload["session_id"])
            if replay_run_id and session.get("replay_run_id") != replay_run_id:
                raise ValueError("Replay cannot reuse an original session")
            return session
        event_id = payload.get("event_id")
        item_id = payload.get("item_id")
        reanalysis = None if event_id else payload.get("reanalysis_attempt")
        identifiers = [
            ("event_id", event_id),
            ("item_id", item_id if not event_id else None),
            ("proposal_id", payload.get("proposal_id")),
        ]
        closed = {}
        for field, value in identifiers:
            if value:
                async for session in self.repo.iter_all(
                    "ops.agent_sessions",
                    filters={field: value, **({"replay_run_id": replay_run_id} if replay_run_id else {})},
                ):
                    if self.routable_session(session):
                        if self.joins_attempt(session, field, payload):
                            return session
                        continue
                    closed[value] = closed.get(value, 0) + 1
        config = self.config()
        scout = payload.get("kind") == "SCOUT_REVIEW"
        agent = config.get("scout_agent_id") if scout else config["agent_id"]
        if not agent:
            raise ConfigurationError("scout_agent_id required")
        if not item_id and event_id and not scout:
            async for observation in self.repo.iter_range(
                "evt.event_observations", "observed_at", filters={"event_id": event_id}
            ):
                item_id = observation.get("item_id")
                if item_id:
                    break
            if not item_id:
                async for origin in self.repo.iter_all("ops.agent_sessions", filters={"event_id": event_id}):
                    item_id = origin.get("opened_by_item") or origin.get("item_id")
                    if item_id:
                        break
            if not item_id:
                raise ValueError("Event session has no durable opening source item")
        identity = event_id or item_id or payload.get("proposal_id")
        if not identity:
            raise ValueError("Session needs event, item or proposal identity")

        marker = sha256(
            {
                "identity": identity,
                "scout": scout,
                "model_context_version": CONTEXT_VERSION,
                **({"replay_run_id": replay_run_id} if replay_run_id else {}),
                **({"reanalysis_attempt": reanalysis} if reanalysis else {}),
                **({"generation": closed[identity]} if closed.get(identity) else {}),
            }
        )
        replay_metadata = (
            {
                "tank_replay_run_id": replay_run_id,
                **(
                    {
                        "tank_replay_evidence_sha256": payload["replay_evidence_sha256"],
                        "tank_replay_cutoff": payload["replay_cutoff"],
                    }
                    if payload.get("replay_evidence_sha256")
                    else {}
                ),
            }
            if replay_run_id
            else {}
        )
        remote = None
        page = None
        while True:
            params = {"limit": 100}
            if page:
                params["page"] = page
            listing = await self.request("GET", "sessions", params=params)
            for row in listing.get("data", []):
                if row.get("metadata", {}).get("tank_identity") == marker:
                    remote = row
                    break
            page = listing.get("next_page")
            if remote or not page:
                break
        if remote is None:
            environment = (
                config.get("replay_environment_id")
                if getattr(self.settings, "replay", False)
                else config["environment_id"]
            )
            if not environment:
                raise ConfigurationError("replay_environment_id required for replay")
            remote = await self.request(
                "POST",
                "sessions",
                json={
                    "agent": agent,
                    "environment_id": environment,
                    "metadata": {"tank_identity": marker, **replay_metadata},
                },
                idem="create:" + marker,
            )
        if (
            replay_metadata
            and remote.get("metadata")
            and any(remote["metadata"].get(key) != value for key, value in replay_metadata.items())
        ):
            raise ValueError("Replay session evidence binding does not match requested run")
        sid = remote["id"]
        opened = self.clock_timestamp()
        tools = self.analyst_tools(scout=scout)
        session = {
            "session_id": sid,
            "model_context_version": CONTEXT_VERSION,
            **({"tools_sha256": sha256(tools)} if tools else {}),
            "event_id": event_id,
            "replay_run_id": replay_run_id,
            "replay_source_session_id": payload.get("replay_source_session_id"),
            "replay_evidence_sha256": payload.get("replay_evidence_sha256"),
            "replay_cutoff": payload.get("replay_cutoff"),
            "item_id": item_id,
            "proposal_id": payload.get("proposal_id"),
            "opened_by_item": item_id,
            **({"reanalysis_attempt": reanalysis} if reanalysis else {}),
            **opened.fields("opened_at"),
            **self.agent_provenance(remote, config, agent),
            "billing_basis": "SUBSCRIPTION",
            "effort": config.get("reasoning_effort"),
            "status": "OPEN",
            "last_event_seq": 0,
            **propagate_timestamp(
                {}, "last_item_at", missing_reason="No accepted source item message"
            ).fields("last_item_at"),
            "config_version": self.settings.config_version,
            "code_version": self.settings.code_version,
            **opened.fields("created_at"),
            "messages": [],
        }
        session["missing_fields"] = {
            key: reason
            for key, reason in {
                "opened_by_item": "Proposal or event follow-up has no opening source item",
                "last_item_at": "No source item message accepted yet",
                "agent_version": "Deployed agent version not exposed by remote snapshot or configuration",
                "prompt_version": "INCOMPLETE: remote resolved prompt snapshot unavailable",
            }.items()
            if session.get(key) is None
        }
        session["agent_version_reason"] = session["missing_fields"].get("agent_version")
        session["last_item_at_reason"] = session["missing_fields"].get("last_item_at")
        await self.repo.put("ops.agent_sessions", sid, session)
        return session

    async def continuable(self, payload):
        """A follow-up continues only a live session; otherwise the reader gets just the reason."""
        question_id = payload["question_id"]
        session = await self.repo.get("ops.agent_sessions", payload.get("session_id") or "")
        if getattr(self.settings, "replay", False):
            session = None
        closed = closed_reason(session)
        if closed is not None:
            await record_answer(self.repo, question_id, closed[1], status=closed[0])
            return False
        try:
            remote = await self.request("GET", f"sessions/{session['session_id']}")
        except httpx.HTTPStatusError as error:
            if error.response.status_code != 404:
                raise
            remote = {"status": "terminated", "missing": True}
        remote = remote.get("data", remote)
        if remote.get("status") == "terminated" or remote.get("archived_at"):
            async with self.repo.lock("session:" + session["session_id"]):
                current = await self.repo.get("ops.agent_sessions", session["session_id"])
                current["remote_state"] = {
                    "status": remote.get("status"),
                    "archived_at": remote.get("archived_at"),
                    **self.clock_timestamp().fields("checked_at"),
                }
                await self.repo.put("ops.agent_sessions", session["session_id"], current)
            status, text = closed_reason(current) or ("ARCHIVED", None)
            await record_answer(self.repo, question_id, text, status=status)
            return False
        return True

    def reopen_for_question(self, session, payload):
        if session.get("status") == "FAILED":
            session.setdefault("reopened", []).append(
                {
                    "question_id": payload["question_id"],
                    "previous_status": session["status"],
                    **self.clock_timestamp().fields("at"),
                }
            )
            session["status"] = "OPEN"

    def analyst_tools(self, *, scout=False):
        """The current analyst toolset: built-ins plus the custom tools of config/skills."""
        reader = getattr(self.settings, "read_config_json", None)
        if reader is None:
            return None
        return agent_tools(self.settings, "tank-scout-reviewer" if scout else "tank-event-analyst")

    def tools_changed(self, session):
        """Whether the session's tool definitions differ from the current skill's.

        A session records the digest of the tools it was created or last synchronized with; a
        changed schema (for example optional fields made nullable) reaches a retained session at its
        next idle input boundary, without a context-version migration. A busy session keeps its
        tools until a later input finds it idle."""
        tools = self.analyst_tools(scout=bool(session.get("proposal_id")))
        return bool(tools) and session.get("tools_sha256") != sha256(tools)

    @staticmethod
    def legacy_contract(session):
        return session.get("model_context_version", 0) < CONTEXT_VERSION or session.get(
            "active_message_id"
        ) in session.get("legacy_model_message_ids", [])

    async def sync_tools(self, sid, *, resume=False):
        """Migrate tools at an idle input boundary without replacing the session or its history."""
        if getattr(self.settings, "replay", False):
            return
        session = await self.repo.get("ops.agent_sessions", sid)
        if session is None:
            return None
        tools = self.analyst_tools(scout=bool(session.get("proposal_id")))
        migrating = session.get("model_context_version", 0) < CONTEXT_VERSION
        if not tools or (not migrating and session.get("tools_sha256") == sha256(tools)):
            return None
        if not resume and (
            session.get("active_message_id") or session.get("runtime_status") in BUSY_RUNTIME_STATES
        ):
            return "BUSY"
        try:
            if resume:
                remote = await self.request("GET", f"sessions/{sid}")
                if remote.get("data", remote).get("status") != "idle":
                    raise ValueError("Wait for the native tool boundary before updating its tools")
            await self.request("POST", f"sessions/{sid}", json={"agent": {"tools": tools}})
            remote = await self.request("GET", f"sessions/{sid}")
        except httpx.HTTPStatusError as error:
            if migrating or error.response.status_code == 429 or error.response.status_code >= 500:
                raise
            return
        remote = remote.get("data", remote)
        descriptor = self.agent_provenance(remote, {}, session.get("agent_id"))
        async with self.repo.lock("session:" + sid):
            current = await self.repo.get("ops.agent_sessions", sid)
            if descriptor.get("prompt_version"):
                current.update(
                    {
                        key: value
                        for key, value in descriptor.items()
                        if key not in ("agent_id", "prompt_label")
                    }
                )
            current["tools_sha256"] = sha256(tools)
            if migrating:
                current["legacy_model_message_ids"] = [
                    row["message_id"]
                    for row in current.get("message_log", [])
                    if row.get("model_context_version", 0) < CONTEXT_VERSION
                ]
                current["model_context_version"] = CONTEXT_VERSION
                current.pop("model_catalog", None)
            current.update(self.clock_timestamp().fields("tools_synced_at"))
            await self.repo.put("ops.agent_sessions", sid, current)

    async def transition_note(self, sid):
        """The ID-contract note for a turn that began before its session switched: delivered once, because
        a switched session keeps its earlier skill text and learns the lookups only from this note."""
        async with self.repo.lock("session:" + sid):
            current = await self.repo.get("ops.agent_sessions", sid)
            if current is None or current.get("context_note_version") == CONTEXT_VERSION:
                return None
            current["context_note_version"] = CONTEXT_VERSION
            await self.repo.put("ops.agent_sessions", sid, current)
        return CONTEXT_TRANSITION[bool(current.get("proposal_id"))]

    async def prepare_attachments(self, session, question):
        question = copy.deepcopy(question)
        for index, item in enumerate(question.get("attachments") or [], 1):
            key = item["file_key"]
            metadata = await self.repo.get("ops.telegram_files", key)
            payload = await self.repo.get("ops.telegram_file_payloads", key)
            if metadata is None or payload is None:
                raise ValueError("The attached file is unavailable")
            mount = (metadata.get("model_mounts") or {}).get(session["session_id"])
            if not mount:
                content = base64.b64decode(payload["content_base64"], validate=True)
                uploaded = await self.request(
                    "POST",
                    "files",
                    files={
                        "file": (item["filename"], content, item.get("mime") or "application/octet-stream")
                    },
                )
                uploaded = uploaded.get("data", uploaded)
                stamp = str(
                    question.get("created_at")
                    or question.get("received_at")
                    or metadata.get("stored_at")
                    or "current-question"
                ).replace(":", "-")
                path = f"/workspace/uploads/{stamp}/attachment-{index}/{item['filename']}"
                await self.request(
                    "POST",
                    f"sessions/{session['session_id']}/resources",
                    json={"type": "file", "file_id": uploaded["id"], "mount_path": path},
                    idem="mount:" + sha256({"session": session["session_id"], "file": key}),
                )
                mount = {"path": path, "file_id": uploaded["id"]}
                metadata.setdefault("model_mounts", {})[session["session_id"]] = mount
                await self.repo.put("ops.telegram_files", key, metadata)
            item["path"] = mount["path"]
        return question

    async def send_file(self, session, arguments, call_id):
        """send_telegram_file: read an output of this session and reply with it to the active follow-up."""
        active = next(
            (
                row
                for row in session.get("message_log", [])
                if row["message_id"] == session.get("active_message_id")
            ),
            None,
        )
        payload = (active or {}).get("payload") or {}
        if payload.get("kind") != "USER_QUESTION" or getattr(self, "telegram", None) is None:
            raise ValueError("send_telegram_file is available only while answering a Telegram follow-up")
        filename = arguments.get("filename")
        if (
            not isinstance(filename, str)
            or filename in ("", ".", "..")
            or "/" in filename
            or "\\" in filename
        ):
            raise ValueError("filename must be a plain name inside /mnt/session/outputs")
        content = None
        url = self.url(f"sessions/{session['session_id']}/outputs/{quote(filename, safe='')}")
        for _ in range(10):
            async with self.http.stream("GET", url, headers=self.headers()) as response:
                if response.status_code == 200:
                    received = bytearray()
                    async for block in response.aiter_bytes():
                        received.extend(block)
                        if len(received) > DOCUMENT_LIMIT:
                            raise ValueError("File exceeds the 45 MB Telegram bot limit")
                    content = bytes(received)
                    break
                if response.status_code != 404:
                    response.raise_for_status()
            await asyncio.sleep(1)
        if content is None:
            raise ValueError(f"/mnt/session/outputs/{filename} was not found in this session")
        if len(content) > DOCUMENT_LIMIT:
            raise ValueError("File exceeds the 45 MB Telegram bot limit")
        return await self.telegram.send_session_file(
            payload["question_id"], call_id, filename, content, arguments.get("caption")
        )

    async def send_message(self, payload):
        if payload.get("kind") == "USER_QUESTION" and not await self.continuable(payload):
            return None
        payload = await self.readdressed(payload)
        item = (
            await self.repo.get("ingest.source_items", payload["item_id"]) if payload.get("item_id") else None
        )
        mapping_identity = (
            payload.get("session_id")
            or payload.get("event_id")
            or payload.get("proposal_id")
            or payload.get("item_id")
        )
        async with self.repo.lock("session-mapping:" + str(mapping_identity)):
            session = await self.resolve(payload)
        sid, key = session["session_id"], sha256(payload)
        migrating = session.get("model_context_version", 0) < CONTEXT_VERSION
        if migrating or payload.get("kind") == "USER_QUESTION":
            if await self.sync_tools(sid) == "BUSY":
                raise DeliveryDeferred(offset_schedule(collection_clock(), timedelta(seconds=30)))
            session = await self.repo.get("ops.agent_sessions", sid)
        elif self.tools_changed(session):
            await self.sync_tools(sid)
            session = await self.repo.get("ops.agent_sessions", sid)
        content = await ModelContext(self.repo, self.settings, session).message(payload, item)
        note = CONTEXT_TRANSITION[bool(session.get("proposal_id"))]
        if migrating:
            content["context_update"] = note
        async with self.send_locks.setdefault(sid, asyncio.Lock()):
            async with self.repo.lock("session:" + sid):
                session = await self.repo.get("ops.agent_sessions", sid)
                if payload.get("kind") == "USER_QUESTION":
                    closed = closed_reason(session)
                    if closed is not None:
                        await record_answer(self.repo, payload["question_id"], closed[1], status=closed[0])
                        return None
                    self.reopen_for_question(session, payload)
                if key in session.get("messages", []):
                    self.start_stream(sid)
                    return session
                intent = next(
                    (row for row in session.get("message_log", []) if row["message_id"] == key), None
                )
                if intent is None:
                    text = canonical_json(content)
                    if payload.get("question_id"):
                        question = await self.repo.get("out.user_questions", payload["question_id"])
                        if question is None:
                            raise KeyError(payload["question_id"])
                        text = question_message(await self.prepare_attachments(session, question))
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
                await self.repo.put("ops.agent_sessions", sid, session)

            session = await self.post_message_intent(sid, key)
        self.start_stream(sid)
        return session

    async def post_message_intent(self, sid, key, *, recovering=False):
        session = await self.repo.get("ops.agent_sessions", sid)
        intent = next(row for row in session["message_log"] if row["message_id"] == key)
        initial_status = session.get("status")
        remote = session.get("remote_state") or {}
        if (
            intent.get("remote_event_id")
            or intent.get("turn_status") in {"DONE", "FAILED"}
            or key in session.get("retired_message_ids", [])
            or intent.get("native_input_attempts", {}) != {}
            or type(intent.get("retry_count", 0)) is not int
            or intent.get("retry_count", 0) != 0
            or initial_status not in {"OPEN", "FAILED"}
            or session.get("archived_at")
            or remote.get("status") == "terminated"
            or remote.get("archived_at")
            or (recovering and not self.recoverable_session(session))
        ):
            return session
        accepted = await self.request(
            "POST",
            f"sessions/{sid}/events",
            json={"events": [copy.deepcopy(intent["body"])]},
            idem="message:" + key,
        )
        accepted_at, identity = self.clock_timestamp(), self.accepted_input_id(accepted)
        async with self.repo.lock("session:" + sid):
            session = await self.repo.get("ops.agent_sessions", sid)
            intent = next(row for row in session["message_log"] if row["message_id"] == key)
            if intent.get("remote_event_id") and intent["remote_event_id"] != identity:
                raise ValueError("Idempotent message acceptance changed native input identity")
            if identity:
                intent.setdefault("native_input_attempts", {})[identity] = intent.get("retry_count", 0)
            intent.update(
                **accepted_at.fields("sent_at"),
                remote_event_id=identity,
                turn_status="PENDING"
                if intent.get("turn_status") == "ACCEPTING"
                else intent.get("turn_status", "PENDING"),
            )
            if key not in session.setdefault("messages", []):
                session["messages"].append(key)
            remote = session.get("remote_state") or {}
            if (
                session.get("status") == initial_status
                and intent.get("turn_status") not in {"DONE", "FAILED"}
                and key not in session.get("retired_message_ids", [])
                and not session.get("archived_at")
                and remote.get("status") != "terminated"
                and not remote.get("archived_at")
            ):
                session["status"] = "OPEN"
            if intent["payload"].get("item_id"):
                session.update(accepted_at.fields("last_item_at"))
                session.update(
                    propagate_timestamp(intent, "item_observed_at").fields("last_item_observed_at")
                )
                session.get("missing_fields", {}).pop("last_item_at", None)
                session["last_item_at_reason"] = None
            await self.repo.put("ops.agent_sessions", sid, session)
            await session_progress(self.repo, self.settings, session)
            return session

    async def readdressed(self, payload):
        """An event input addressed to a session that has since closed goes to its event instead.

        Timed reviews and agent-defined conditions carry the session that scheduled them, and a
        merge hands its observations to the target event's recorded session. When that session is
        FAILED, ENDED or archived, the input drops the stale session_id and is routed by its
        event_id, so it joins the event's open session or opens a fresh generation."""
        if (
            payload.get("kind") not in EVENT_INPUTS
            or not payload.get("session_id")
            or not payload.get("event_id")
        ):
            return payload
        session = await self.repo.get("ops.agent_sessions", payload["session_id"])
        if session is None or self.routable_session(session):
            return payload
        return {key: value for key, value in payload.items() if key != "session_id"}

    @staticmethod
    def routable_session(session):
        """A session new messages may join: not failed, ended or archived locally or remotely, and not
        an eventless session whose item failed as AGENT_INCOMPLETE after its reminder budget.

        A closed session never takes another input, so routing to it would strand the message;
        the next message for the same event, item or proposal opens a fresh session instead, and a
        re-analysed item starts with a full reminder budget."""
        remote = session.get("remote_state") or {}
        return (
            session.get("status") not in {"FAILED", "ENDED"}
            and not session.get("agent_incomplete")
            and not session.get("archived_at")
            and remote.get("status") != "terminated"
            and not remote.get("archived_at")
        )

    @staticmethod
    def joins_attempt(session, field, payload):
        """A re-analysis message routed by its item alone joins only the session opened for the
        same re-analysis attempt; the item's earlier sessions, including one that has since
        recorded an event, never take it, so every attempt starts in a fresh session."""
        attempt = payload.get("reanalysis_attempt")
        return field != "item_id" or not attempt or session.get("reanalysis_attempt") == attempt

    @staticmethod
    def recoverable_session(session):
        remote = session.get("remote_state") or {}
        return (
            session.get("status") == "OPEN"
            and not session.get("archived_at")
            and remote.get("status") != "terminated"
            and not remote.get("archived_at")
        )

    @staticmethod
    def recorded_message_input(session, message, calls):
        attempts = message.get("native_input_attempts")
        attempt = message.get("retry_count", 0)
        if (
            not isinstance(attempts, dict)
            or type(attempt) is not int
            or attempt < 0
            or any(
                not isinstance(identity, str)
                or not identity.strip()
                or identity != identity.strip()
                or type(ordinal) is not int
                or ordinal < 0
                for identity, ordinal in attempts.items()
            )
        ):
            return None
        candidates = [identity for identity, ordinal in attempts.items() if ordinal == attempt]
        if len(candidates) != 1:
            return None
        identity = candidates[0]
        if (
            message.get("remote_event_id") not in (None, identity)
            or identity in message.get("superseded_native_input_ids", [])
            or identity in session.get("superseded_native_inputs", {})
        ):
            return None
        for other in session.get("message_log", []):
            if other is message:
                continue
            if other.get("message_id") == message["message_id"]:
                return None
            other_attempts = other.get("native_input_attempts", {})
            if not isinstance(other_attempts, dict):
                return None
            if (
                other.get("remote_event_id") == identity
                or identity in other_attempts
                or identity in other.get("superseded_native_input_ids", [])
            ):
                return None
        if any(call.get("remote_event_id") == identity for call in calls):
            return None
        return identity

    async def repair_accepted_inputs(self, sid):
        async with self.repo.lock("session:" + sid):
            session = await self.repo.get("ops.agent_sessions", sid)
            if session is None or not self.recoverable_session(session):
                return
            calls = [row async for row in self.repo.iter_all("ops.tool_calls", filters={"session_id": sid})]
            changed = False
            for message in session.get("message_log", []):
                if message["message_id"] in session.get("retired_message_ids", []) or message.get(
                    "turn_status"
                ) not in {"ACCEPTING", "PENDING", "RUNNING"}:
                    continue
                identity = self.recorded_message_input(session, message, calls)
                if not identity:
                    continue
                if not message.get("remote_event_id"):
                    message["remote_event_id"] = identity
                    changed = True
                if message.get("turn_status") == "ACCEPTING":
                    message["turn_status"] = "PENDING"
                    changed = True
                if message["message_id"] not in session.setdefault("messages", []):
                    session["messages"].append(message["message_id"])
                    changed = True
            if changed:
                await self.repo.put("ops.agent_sessions", sid, session)

    async def recover_outbound_inputs(self, sid):
        async with self.send_locks.setdefault(sid, asyncio.Lock()):
            await self.repair_accepted_inputs(sid)
            session = await self.repo.get("ops.agent_sessions", sid)
            if session is None or not self.recoverable_session(session):
                return
            for message in session.get("message_log", []):
                if (
                    message.get("turn_status") == "ACCEPTING"
                    and not message.get("sent_at")
                    and message.get("native_input_attempts", {}) == {}
                    and type(message.get("retry_count", 0)) is int
                    and message.get("retry_count", 0) == 0
                    and message["message_id"] not in session.get("retired_message_ids", [])
                ):
                    await self.post_message_intent(sid, message["message_id"], recovering=True)
        session = await self.repo.get("ops.agent_sessions", sid)
        await session_progress(self.repo, self.settings, session)
        if session.get("pending_retry_event"):
            await self.process_event(sid, session["pending_retry_event"])

    @staticmethod
    def accepted_input_id(response):
        """Tank sends one executable input per POST; native execution uses its ID."""
        events = response.get("data") if isinstance(response, dict) else None
        if (
            isinstance(events, list)
            and len(events) == 1
            and isinstance(events[0], dict)
            and isinstance(events[0].get("id"), str)
            and events[0]["id"]
        ):
            return events[0]["id"]
        return None

    async def native_input_owner(self, session, execution_id, calls=None):
        """Resolve exact accepted-input lineage, including tool-result continuations."""
        if (
            not isinstance(execution_id, str)
            or not execution_id.strip()
            or execution_id != execution_id.strip()
        ):
            return None
        messages = {row["message_id"]: row for row in session.get("message_log", [])}
        calls = (
            calls
            if calls is not None
            else [
                row
                async for row in self.repo.iter_all(
                    "ops.tool_calls", filters={"session_id": session["session_id"]}
                )
            ]
        )
        claims = []
        for message in messages.values():
            attempts = message.get("native_input_attempts", {})
            if not isinstance(attempts, dict):
                return None
            if (
                execution_id == message.get("remote_event_id")
                or execution_id in message.get("superseded_native_input_ids", [])
                or execution_id in attempts
            ):
                recorded = self.recorded_message_input(session, message, calls)
                if execution_id in attempts:
                    attempt = attempts[execution_id]
                    if type(attempt) is not int or attempt < 0:
                        return None
                    if attempt == message.get("retry_count", 0) and recorded != execution_id:
                        return None
                elif execution_id == message.get("remote_event_id"):
                    if attempts:
                        return None
                    attempt = message.get("retry_count", 0)
                elif execution_id in message.get("superseded_native_input_ids", []):
                    attempt = message["superseded_native_input_ids"].index(execution_id)
                else:
                    return None
                if type(attempt) is not int or attempt < 0:
                    return None
                claims.append((message["message_id"], attempt))
        for call in calls:
            if call.get("remote_event_id") == execution_id and call.get("native_turn_message_id") in messages:
                attempt = call.get("native_turn_retry_count", 0)
                if type(attempt) is not int or attempt < 0:
                    return None
                claims.append((call["native_turn_message_id"], attempt))
        return claims[0] if len(claims) == 1 else None

    async def handled_terminal_inputs(self, session, calls, states, superseded):
        if not self.recoverable_session(session) or session.get("pending_retry_event"):
            return set()
        handled = set()
        messages = session.get("message_log", [])
        retired = set(session.get("retired_message_ids", []))
        for index, message in enumerate(messages):
            if message.get("turn_status") != "FAILED" or message["message_id"] in retired:
                continue
            failure = message.get("terminal_failure", session.get("terminal_failure"))
            if not isinstance(failure, dict):
                continue
            identity = failure.get("execution_id")
            attempt = message.get("retry_count", 0)
            if (
                not isinstance(identity, str)
                or not identity
                or not isinstance(failure.get("event_id"), str)
                or not failure["event_id"]
                or failure["event_id"] not in session.get("processed_events", [])
                or (
                    failure.get("event_type") == "execution.status_failed"
                    and failure["event_id"] != "execution-status-failed:" + identity
                )
                or failure.get("event_type")
                not in {"execution.status_failed", "session.error", "agent.refusal"}
                or states.get(identity, {}).get("state") != "FAILED"
                or await self.native_input_owner(session, identity, calls) != (message["message_id"], attempt)
            ):
                continue
            continuation = False
            for later in messages[index + 1 :]:
                if (
                    later["message_id"] not in retired
                    and later.get("turn_status") in {"PENDING", "RUNNING", "DONE"}
                    and later.get("remote_event_id")
                    and await self.native_input_owner(session, later["remote_event_id"], calls)
                    == (later["message_id"], later.get("retry_count", 0))
                ):
                    continuation = True
                    break
            if not continuation:
                continue
            owned = [call for call in calls if call.get("native_turn_message_id") == message["message_id"]]
            superseded_tools = {
                input_id
                for input_id, proof in superseded.items()
                if isinstance(proof, dict)
                and proof.get("reason") == "FAILED_PRIOR_TURN_ATTEMPT"
                and proof.get("message_id") == message["message_id"]
                and proof.get("replacement_input_id") == message.get("remote_event_id")
                and states.get(input_id, {}).get("state") == "FAILED"
                and proof.get("queue_state") == states[input_id]
                and (owner := await self.native_input_owner(session, input_id, calls)) is not None
                and owner[0] == message["message_id"]
                and owner[1] < attempt
            }
            if any(
                not call.get("returned_at")
                or not call.get("remote_event_id")
                or (
                    states.get(call["remote_event_id"], {}).get("state")
                    != ("FAILED" if call["remote_event_id"] == identity else "COMPLETED")
                    and call["remote_event_id"] not in superseded_tools
                )
                for call in owned
            ):
                continue
            message_input = message.get("remote_event_id")
            if not message_input or states.get(message_input, {}).get("state") != (
                "FAILED" if message_input == identity else "COMPLETED"
            ):
                continue
            returned = {call["call_id"] for call in owned}
            if any(
                receipt.get("execution_id") == identity
                and (receipt.get("stop_reason") or {}).get("type") == "requires_action"
                and any(call_id not in returned for call_id in receipt["stop_reason"].get("event_ids", []))
                for receipt in session.get("native_execution_receipts", {}).values()
            ):
                continue
            handled.add(identity)
        return handled

    async def execution_barrier(self, session, calls=None, *, verify_remote=False):
        """Correlate accepted inputs with native execution receipts, never idle timing."""
        calls = (
            calls
            if calls is not None
            else [
                row
                async for row in self.repo.iter_all(
                    "ops.tool_calls", filters={"session_id": session["session_id"]}
                )
            ]
        )
        required = set()
        unknown = []
        retired = set(session.get("retired_message_ids", []))
        calls = [call for call in calls if call.get("native_turn_message_id") not in retired]
        for message in session.get("message_log", []):
            if message["message_id"] in retired:
                continue
            identity = message.get("remote_event_id")
            owner = await self.native_input_owner(session, identity, calls) if identity else None
            if identity and owner == (message["message_id"], message.get("retry_count", 0)):
                required.add(identity)
            else:
                unknown.append("message:" + message["message_id"])
        for call in calls:
            if call.get("returned_at"):
                identity = call.get("remote_event_id")
                if identity:
                    required.add(identity)
                else:
                    unknown.append("tool:" + call["call_id"])
            else:
                unknown.append("unreturned-tool:" + call["call_id"])
        receipts = session.get("native_execution_receipts", {})
        superseded = session.get("superseded_native_inputs", {})
        required.difference_update(superseded)
        returned_tools = {call["call_id"] for call in calls if call.get("returned_at")}
        pending_actions = sorted(
            {
                identity
                for receipt in receipts.values()
                if receipt.get("execution_id") in required
                and receipt.get("stop_reason", {}).get("type") == "requires_action"
                for identity in receipt["stop_reason"].get("event_ids", [])
                if identity not in returned_tools
            }
        )
        states = copy.deepcopy(session.get("native_execution_states", {}))
        signature = tuple(sorted(required))
        poll = self.execution_polls.get(session["session_id"], (0, 0.05, None))
        should_read = poll[2] != signature or time.monotonic() >= poll[0]
        if verify_remote and required and not unknown and should_read:
            pending_read = sorted(
                identity for identity in required if states.get(identity, {}).get("state") != "COMPLETED"
            )
            for offset in range(0, len(pending_read), 64):
                requested = pending_read[offset : offset + 64]
                response = await self.request(
                    "GET",
                    f"sessions/{session['session_id']}/executions",
                    params=[("ids[]", identity) for identity in requested],
                )
                rows = response.get("data") if isinstance(response, dict) else None
                if (
                    not isinstance(rows, list)
                    or len(rows) != len(requested)
                    or [row.get("execution_id") for row in rows] != requested
                ):
                    raise ValueError("Invalid native execution status correlation")
                for row in rows:
                    state = row.get("state")
                    expected = state if state in {"COMPLETED", "FAILED", "CANCELLED"} else None
                    if (
                        state not in {"QUEUED", "RUNNING", "COMPLETED", "FAILED", "CANCELLED", "UNKNOWN"}
                        or row.get("settled_outcome") != expected
                    ):
                        raise ValueError("Invalid native execution settlement projection")
                    states[row["execution_id"]] = copy.deepcopy(row)
            async with self.repo.lock("session:" + session["session_id"]):
                current = await self.repo.get("ops.agent_sessions", session["session_id"])
                self.merge_event_changes(
                    current.setdefault("native_execution_states", {}),
                    session.get("native_execution_states", {}),
                    states,
                )
                states = copy.deepcopy(current["native_execution_states"])
                owners = {row["message_id"]: row for row in current.get("message_log", [])}
                for call in calls:
                    identity = call.get("remote_event_id")
                    owner = owners.get(call.get("native_turn_message_id"))
                    if (
                        identity
                        and states.get(identity, {}).get("state") == "FAILED"
                        and owner
                        and call.get("native_turn_retry_count", 0) < owner.get("retry_count", 0)
                    ):
                        current.setdefault("superseded_native_inputs", {})[identity] = {
                            "reason": "FAILED_PRIOR_TURN_ATTEMPT",
                            "queue_state": copy.deepcopy(states[identity]),
                            "replacement_input_id": owner.get("remote_event_id"),
                            "message_id": owner["message_id"],
                        }
                superseded = current.get("superseded_native_inputs", {})
                required.difference_update(superseded)
                await self.repo.put("ops.agent_sessions", session["session_id"], current)
            delay = min(poll[1] * 2, 1.0) if poll[2] == signature else 0.05
            self.execution_polls[session["session_id"]] = (time.monotonic() + delay, delay, signature)
        required.difference_update(await self.handled_terminal_inputs(session, calls, states, superseded))
        final_receipt = any(
            receipt.get("execution_id") in required
            and receipt.get("type") == "session.status_idle"
            and receipt.get("stop_reason", {}).get("type") == "end_turn"
            for receipt in receipts.values()
        )
        failed = sorted(
            identity
            for identity in required
            if states.get(identity, {}).get("state") in {"FAILED", "CANCELLED"}
        )
        pending = sorted(
            identity for identity in required if states.get(identity, {}).get("state") != "COMPLETED"
        )
        return {
            "complete": bool(required)
            and not unknown
            and not pending
            and not pending_actions
            and final_receipt,
            "accepted_input_ids": sorted(required),
            "pending_input_ids": pending,
            "failed_input_ids": failed,
            "unidentified_inputs": unknown,
            "pending_action_ids": pending_actions,
            "terminal_receipts": receipts,
            "execution_states": states,
        }

    async def source_completion_snapshot(self, session):
        outcomes = {}
        for message in session.get("message_log", []):
            payload = message.get("payload", {})
            item_id = payload.get("item_id")
            source_message = payload.get("kind") in {"NEW_ITEM", "ITEM_ADDED"}
            item = await self.repo.get("ingest.source_items", item_id) if source_message and item_id else None
            outcomes[message["message_id"]] = {
                "version": workflow.SOURCE_COMPLETION_VERSION,
                "item_id": item_id,
                "native_input_id": message.get("remote_event_id"),
                "retry_attempt": message.get("retry_count", 0),
                "outcome": item.get("extraction_status")
                if item
                else "MISSING_SOURCE_ITEM"
                if source_message
                else "NOT_APPLICABLE",
            }
        return outcomes

    async def completed_turn(self, sid):
        result = await self.settled_turn(sid)
        if result["complete"]:
            await self.deliver_answers(sid)
        return result

    async def turn_answers(self, session, inputs=None):
        """Per accepted message: the last agent text of its executions and whether its turn ended.

        A turn ends when the newest execution owned by the message went idle with end_turn. History
        is read newest first and only back to the oldest wanted input, never the whole session.
        """
        wanted = set(inputs or ())
        recent, page = [], None
        while True:
            params = {"limit": 100, "order": "desc"}
            if page:
                params["page"] = page
            data = await self.request("GET", f"sessions/{session['session_id']}/events", params=params)
            for event in data.get("data", []):
                recent.append(event)
                wanted.discard(event.get("id"))
            page = data.get("next_page")
            if not page or (inputs and not wanted):
                break
        texts, ends, order, current = {}, {}, [], None
        for event in reversed(recent):
            kind = event.get("type")
            if kind == "session.status_running":
                current = event.get("execution_id")
                if current and current not in order:
                    order.append(current)
            elif kind == "agent.message" and current:
                text = "".join(
                    block.get("text", "")
                    for block in event.get("content", [])
                    if isinstance(block, dict) and block.get("type") == "text"
                ).strip()
                if text:
                    texts[current] = text
            elif kind == "session.status_idle" and event.get("execution_id"):
                ends[event["execution_id"]] = (event.get("stop_reason") or {}).get("type") == "end_turn"
        answers, attempts = {}, {}
        for execution_id in order:
            owner = await self.native_input_owner(session, execution_id)
            if not owner:
                continue
            message_id, attempt = owner
            if attempt < attempts.get(message_id, attempt):
                continue
            if attempt > attempts.get(message_id, attempt):
                answers.pop(message_id, None)
            attempts[message_id] = attempt
            answer = answers.setdefault(message_id, {"text": None, "ended": False})
            answer["ended"] = ends.get(execution_id, False)
            if execution_id in texts:
                answer["text"] = texts[execution_id]
        return answers

    async def deliver_answers(self, sid):
        """Hand each settled follow-up question its answer, or its failure, exactly once.

        Questions settle independently: a later queued turn never holds back an earlier answer.
        """
        session = await self.repo.get("ops.agent_sessions", sid)
        if session is None:
            return
        stopped = session.get("status") in ("FAILED", "ENDED")
        pending = [
            row
            for row in session.get("message_log", [])
            if row.get("payload", {}).get("kind") == "USER_QUESTION" and not row.get("answer_recorded")
        ]
        if not pending:
            return
        live = any(row.get("remote_event_id") and row.get("turn_status") != "FAILED" for row in pending)
        answers = (
            await self.turn_answers(
                session, [row["remote_event_id"] for row in pending if row.get("remote_event_id")]
            )
            if live
            else {}
        )
        recorded = set()
        for row in pending:
            question_id = row["payload"]["question_id"]
            answer = answers.get(row["message_id"], {})
            if row.get("turn_status") == "FAILED" or (stopped and not answer.get("ended")):
                await record_answer(self.repo, question_id, None, status="FAILED")
            elif row.get("turn_status") == "DONE" or answer.get("ended"):
                await record_answer(self.repo, question_id, answer.get("text"), status="ANSWERED")
            else:
                continue
            recorded.add(row["message_id"])
        if not recorded:
            return
        async with self.repo.lock("session:" + sid):
            current = await self.repo.get("ops.agent_sessions", sid)
            for row in current.get("message_log", []):
                if row["message_id"] in recorded:
                    row["answer_recorded"] = True
            await self.repo.put("ops.agent_sessions", sid, current)

    async def abandon_question(self, session_id, question_id):
        """A question job gave up: retire its unsent intent so recovery never posts it later.

        Returns False when the question already reached OpenMA; its answer will still arrive.
        """
        async with self.repo.lock("session:" + str(session_id)):
            session = await self.repo.get("ops.agent_sessions", str(session_id))
            if session is None:
                return True
            rows = [
                row
                for row in session.get("message_log", [])
                if (row.get("payload") or {}).get("question_id") == question_id
            ]
            if any(row.get("remote_event_id") for row in rows):
                return False
            for row in rows:
                row["turn_status"] = "FAILED"
                row["answer_recorded"] = True
            if rows:
                retired = set(session.get("retired_message_ids", []))
                retired.update(row["message_id"] for row in rows)
                session["retired_message_ids"] = sorted(retired)
                if session.get("active_message_id") in retired:
                    session["active_message_id"] = None
                await self.repo.put("ops.agent_sessions", session["session_id"], session)
            return True

    async def reconcile_answers(self):
        """Questions still waiting after a restart or a lost terminal hand-off get their answer."""
        sessions = set()
        async for question in self.repo.iter_all("out.user_questions", filters={"status": "QUEUED"}):
            sid = question.get("session_id")
            if sid and not (sid in self.tasks and not self.tasks[sid].done()):
                sessions.add(sid)
        for sid in sorted(sessions):
            try:
                await self.deliver_answers(sid)
            except (httpx.HTTPError, ValueError, KeyError):
                continue

    async def settled_turn(self, sid):
        """Read committed queue states, drain every history page, then recheck inputs."""
        await self.recover_outbound_inputs(sid)
        session = await self.repo.get("ops.agent_sessions", sid)
        local = await self.execution_barrier(session)
        source_outcomes = await self.source_completion_snapshot(session)
        if not self.recoverable_session(session):
            return dict(local, complete=False)
        if (
            local["complete"]
            and session.get("verified_native_completion") == local
            and session.get("verified_source_completion") == source_outcomes
        ):
            return local
        if not local["accepted_input_ids"] and not local["unidentified_inputs"]:
            return local
        if local["unidentified_inputs"] or not any(
            receipt.get("type") == "session.status_idle" for receipt in local["terminal_receipts"].values()
        ):
            deadline, delay = self.history_polls.get(sid, (0, 0.05))
            if time.monotonic() < deadline:
                return dict(local, complete=False)
            await self.catch_up(sid)
            self.history_polls[sid] = (time.monotonic() + delay, min(delay * 2, 1))
            session = await self.repo.get("ops.agent_sessions", sid)
        before = await self.execution_barrier(session, verify_remote=True)
        if not before["complete"]:
            for identity in before.get("failed_input_ids", []):
                if before["execution_states"][identity]["state"] == "FAILED":
                    await self.process_event(
                        sid,
                        {
                            "id": "execution-status-failed:" + identity,
                            "type": "execution.status_failed",
                            "execution_id": identity,
                            "source": "MANAGED_EXECUTION_STATUS",
                            "queue_status": before["execution_states"][identity],
                            **self.clock_timestamp().fields("processed_at"),
                        },
                    )
            return before

        await self.catch_up(sid)

        current = await self.repo.get("ops.agent_sessions", sid)
        drained = await self.execution_barrier(current)
        if not drained["complete"] or drained["accepted_input_ids"] != before["accepted_input_ids"]:
            return dict(drained, complete=False)
        async with self.repo.lock("session:" + sid):
            current = await self.repo.get("ops.agent_sessions", sid)
            after = await self.execution_barrier(current)
            if (
                not self.recoverable_session(current)
                or not after["complete"]
                or after["accepted_input_ids"] != before["accepted_input_ids"]
            ):
                return dict(after, complete=False)
            for message in current.get("message_log", []):
                if message.get("turn_status") != "FAILED":
                    message["turn_status"] = "DONE"
            await self.remind_incomplete(current)
            current["active_message_id"] = None
            current["runtime_status"] = "IDLE"
            outcomes = await self.source_completion_snapshot(current)
            for message in current.get("message_log", []):
                if message["message_id"] in outcomes:
                    message["source_completion_verified"] = copy.deepcopy(outcomes[message["message_id"]])

                    if "workflow_completion_verified" in message:
                        message["legacy_workflow_completion_verified"] = message.pop(
                            "workflow_completion_verified"
                        )
            current["verified_source_completion"] = outcomes
            current["verified_native_completion"] = copy.deepcopy(after)
            await self.repo.put("ops.agent_sessions", sid, current)
            await session_progress(self.repo, self.settings, current)
            return after

    async def remind_incomplete(self, session):
        """A settled turn that left its source item without an outcome reminds the agent of its task.

        Each source item whose turn ended while it was still unprocessed gets one COMPLETION_REMINDER
        message in the same session; the reminder turn is checked the same way. The session's durable
        reminder budget bounds the loop: once spent, fail_incomplete_turn marks the item FAILED /
        AGENT_INCOMPLETE, which Ops shows as a processing error and tank reanalyse can queue again;
        it is never shown as a rejection. A replay sends the original inputs only and no reminder:
        its budget is zero, so a replayed turn without an outcome fails the item at once and the
        replay reports it as incomplete.
        """
        sid = session["session_id"]
        limit = (
            0
            if getattr(self.settings, "replay", False)
            else int(
                (self.settings.load_config("thresholds") or {}).get(
                    "agent_completion_reminders_per_session", DEFAULT_COMPLETION_REMINDERS
                )
            )
        )
        unchecked = {}
        for message in session.get("message_log", []):
            payload = message.get("payload") or {}
            if (
                payload.get("kind") in SOURCE_MESSAGE_KINDS
                and payload.get("item_id")
                and not message.get("completion_checked")
            ):
                message["completion_checked"] = True
                unchecked[payload["item_id"]] = message
        for item_id, message in unchecked.items():
            item = await self.repo.get("ingest.source_items", item_id)
            if not item or item.get("extraction_status") not in UNFINISHED:
                continue
            payload = message["payload"]
            sent = int(session.get("completion_reminders_sent", 0))
            attempts = session.setdefault("completion_reminder_attempts", {})
            if sent >= limit:
                await fail_incomplete_turn(
                    self.repo, session, item_id, extraction_reminders=sent, extraction_reminder_limit=limit
                )
                continue
            attempt = int(attempts.get(item_id, 0)) + 1
            attempts[item_id] = attempt
            session["completion_reminders_sent"] = sent + 1
            await self.repo.enqueue(
                "AGENT_MESSAGE",
                {
                    "kind": "COMPLETION_REMINDER",
                    "session_id": sid,
                    "item_id": item_id,
                    "reminded_kind": payload.get("reminded_kind", payload["kind"]),
                    "attempt": attempt,
                    "reminder": sent + 1,
                    "reminder_limit": limit,
                    "turn_message_id": message["message_id"],
                },
                f"{sid}:{item_id}:COMPLETION_REMINDER:{attempt}",
                priority=1,
            )
            item.update(
                extraction_status="PENDING",
                extraction_reason="AGENT_REMINDED",
                extraction_reminders=sent + 1,
                extraction_reminder_limit=limit,
            )
            await self.repo.put("ingest.source_items", item_id, item)
            await item_progress(
                self.repo,
                self.settings,
                item_id,
                "AGENT_REMINDED",
                f"Агент не вернул результат, напоминание {sent + 1}/{limit}",
                event_id=session.get("event_id"),
                key=f"{sid}:{item_id}:{attempt}",
            )

    def start_stream(self, sid):
        if not self.closed and (sid not in self.tasks or self.tasks[sid].done()):
            self.tasks[sid] = asyncio.create_task(self.follow(sid), name="openma:" + sid)

    async def stop_followers(self):
        """Retire this controller's followers before finalizing owned local state."""
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.tasks.clear()

    async def finalize_replay_abort(self, sid, replay_run_id, failure):
        """Record a host replay abort without inventing domain or source outcomes."""
        async with self.event_locks.setdefault(sid, asyncio.Lock()), self.repo.lock("session:" + sid):
            session = await self.repo.get("ops.agent_sessions", sid)
            if not replay_run_id or not session or session.get("replay_run_id") != replay_run_id:
                return {"session_id": sid, "finalized": False, "reason": "NOT_OWNED_REPLAY_RUN"}
            receipt = dict(
                origin="HOST_REPLAY_ABORT",
                replay_run_id=replay_run_id,
                **copy.deepcopy(failure),
                **self.clock_timestamp().fields("recorded_at"),
            )
            unfinished = []
            for message in session.get("message_log", []):
                if message.get("turn_status") not in {"DONE", "FAILED"}:
                    message["turn_status"] = "FAILED"
                    message["replay_abort"] = copy.deepcopy(receipt)
                    unfinished.append(message["message_id"])
            session.setdefault("terminal_failure", copy.deepcopy(receipt))
            session["replay_abort"] = copy.deepcopy(receipt)
            session["status"] = "FAILED"
            session["runtime_status"] = "IDLE"
            session["active_message_id"] = None
            session["pending_retry_event"] = None
            await self.repo.put("ops.agent_sessions", sid, session)
            return {"session_id": sid, "finalized": True, "failed_message_ids": unfinished}

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
        context["config_version"] = self.settings.config_version
        context["code_version"] = self.settings.code_version
        context.update(call_id=call_id, role="scout" if session.get("proposal_id") else "analyst")
        return context

    async def hydrate_prompt_provenance(self, session):
        """Recover only the native resolved session snapshot, never latest agent bytes."""
        value = session.get("prompt_version")
        if (
            session.get("prompt_version_basis") == "RESOLVED_SESSION_AGENT_SNAPSHOT"
            and isinstance(value, str)
            and len(value) == 64
            and all(character in "0123456789abcdef" for character in value)
        ):
            return
        remote = await self.request("GET", f"sessions/{session['session_id']}")
        remote = remote.get("data", remote)
        descriptor = self.agent_provenance(remote, {}, session.get("agent_id"))
        if not descriptor.get("prompt_version"):
            raise ConfigurationError(
                "Resolved session prompt provenance unavailable; versioned outputs refused"
            )
        if value and "legacy_prompt_descriptor" not in session:
            session["legacy_prompt_descriptor"] = {
                key: session.get(key)
                for key in (
                    "prompt_version",
                    "prompt_version_basis",
                    "prompt_version_reason",
                    "prompt_label",
                    "agent_version",
                    "model_id",
                )
            }
        descriptor["prompt_label"] = session.get("prompt_label") or value
        recovered = {**descriptor, **self.clock_timestamp().fields("recovered_at")}
        session.update(descriptor)
        session["recovered_prompt_descriptor"] = recovered

        async with self.repo.lock("session:" + session["session_id"]):
            current = await self.repo.get("ops.agent_sessions", session["session_id"])
            current.update(descriptor)
            current["recovered_prompt_descriptor"] = recovered
            if session.get("legacy_prompt_descriptor") and "legacy_prompt_descriptor" not in current:
                current["legacy_prompt_descriptor"] = copy.deepcopy(session["legacy_prompt_descriptor"])
            await self.repo.put("ops.agent_sessions", session["session_id"], current)

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

    def tool_failure(self, error):
        """Return a bounded audited error; never persist provider URLs/secrets."""
        if isinstance(error, ConfigurationError):
            return {
                "type": "ConfigurationError",
                "reason": "Resolved session prompt provenance unavailable; versioned outputs refused",
                "category": "PROVENANCE",
            }, "REFUSED"
        if isinstance(error, (ValueError, KeyError)):
            safe, rejected = redact_tool_arguments({"reason": str(error)}, self.settings)
            reason = (
                "Tool input rejected; credential-bearing diagnostic withheld" if rejected else safe["reason"]
            )
            return {"type": type(error).__name__, "reason": reason, "category": "VALIDATION"}, "REFUSED"
        retry_after = None
        if isinstance(error, CaptureRateLimited):
            retry_after = error.retry_after
        elif isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 429:
            retry_after = error.response.headers.get("retry-after", "")
        if retry_after is not None:
            reason = "Source rate-limited the request (HTTP 429)" + (
                f"; Retry-After {retry_after} s" if retry_after.isdigit() else ""
            )
            category = "SOURCE_RATE_LIMITED"
        elif isinstance(error, httpx.HTTPStatusError):
            status = error.response.status_code
            if status in {401, 403}:
                reason = f"Source refused access to the page (HTTP {status})"
                category = "SOURCE_ACCESS_DENIED"
            else:
                reason = f"Source request failed (HTTP {status})"
                category = "SOURCE_HTTP"
        elif isinstance(error, httpx.TimeoutException):
            reason, category = "Source did not answer within the fetch timeout", "SOURCE_TIMEOUT"
        elif isinstance(error, httpx.TooManyRedirects):
            reason, category = "Source redirected too many times", "SOURCE_REDIRECT"
        elif isinstance(error, httpx.HTTPError):
            reason, category = "Source transport failed", "SOURCE_TRANSPORT"
        else:
            reason, category = (
                "Tool execution failed; inspect the audited call and implementation",
                "TOOL_EXECUTION",
            )
        return {"type": type(error).__name__, "reason": reason, "category": category}, "ERROR"

    async def tool(self, session, event):
        sid, call_id = session["session_id"], event["id"]
        requested_at = self.clock_timestamp()
        key = sid + ":" + call_id
        prior = await self.repo.get("ops.tool_calls", key)
        if prior and prior.get("returned_at"):
            return
        original_hash = sha256(event["input"])
        safe_arguments, credential_reason = redact_tool_arguments(event["input"], self.settings)
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
                    await self.repo.put("ops.agent_sessions", sid, sequencing)
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
                    result = {"ok": False, "error": refusal}
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
        body = {
            "type": "user.custom_tool_result",
            "custom_tool_use_id": call_id,
            "content": [
                {
                    "type": "text",
                    "text": canonical_json(
                        tool_wire_result(stored["tool"], projected, session_id=sid, call_id=call_id)
                    ),
                }
            ],
        }
        if stored.get("is_error"):
            body["is_error"] = True
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
        accepted = await self.request(
            "POST", f"sessions/{sid}/events", json={"events": [body]}, idem="tool:" + sha256(key)
        )
        async with self.repo.lock("tool:" + key):
            stored.update(
                remote_event_id=self.accepted_input_id(accepted),
                **self.clock_timestamp().fields("returned_at"),
            )
            await self.repo.put("ops.tool_calls", key, stored)

    async def usage(self, session, event):
        usage = event.get("model_usage")
        if not usage:
            return
        model = event.get("model") or session["model_id"]
        key = session["session_id"] + ":" + event["id"]
        optional = {
            "input_tokens": usage.get("input_tokens"),
            "output_tokens": usage.get("output_tokens"),
            "cache_read_tokens": usage.get("cache_read_input_tokens"),
            "cache_write_tokens": usage.get("cache_creation_input_tokens"),
            "effort": session.get("effort"),
            "stop_reason": event.get("finish_reason"),
            "web_searches": usage.get("server_tool_use", {}).get(
                "web_search_requests", event.get("web_searches")
            ),
        }
        missing_fields = {
            key: "Provider span or deployed configuration did not supply this measurement"
            for key, value in optional.items()
            if value is None
        }
        measured_at = self.event_timestamp(event)
        await self.repo.put(
            "ops.llm_usage",
            key,
            {
                "usage_id": key,
                "session_id": session["session_id"],
                "event_id": session.get("event_id"),
                "call": "AGENT",
                "model_id": model,
                "prompt_version": session["prompt_version"],
                "prompt_version_basis": session.get("prompt_version_basis"),
                "prompt_version_reason": session.get("prompt_version_reason"),
                "prompt_label": session.get("prompt_label"),
                "agent_version": session.get("agent_version"),
                "cache_read_tokens": usage.get("cache_read_input_tokens"),
                "cache_write_tokens": usage.get("cache_creation_input_tokens"),
                "effort": session.get("effort"),
                "stop_reason": event.get("finish_reason"),
                "web_searches": usage.get("server_tool_use", {}).get(
                    "web_search_requests", event.get("web_searches")
                ),
                "usage": usage,
                "missing_fields": missing_fields,
                **usage,
                "cost_usd": "0",
                "billing_basis": "SUBSCRIPTION",
                "cost_reason": "ChatGPT subscription: no per-token charge",
                "model": model,
                "subject_id": session.get("event_id") or session["session_id"],
                **measured_at.fields("at"),
                **self.clock_timestamp().fields("created_at"),
                **propagate_timestamp(event, "processed_at").fields("processed_at"),
                "config_version": self.settings.config_version,
                "code_version": self.settings.code_version,
                "session_config_version": session.get("config_version"),
                "session_code_version": session.get("code_version"),
            },
            immutable=True,
        )

    @staticmethod
    def merge_event_changes(current, before, after):
        missing = object()
        for key, value in after.items():
            old = before.get(key, missing)
            if value == old:
                continue
            live = current.get(key, missing)
            if isinstance(value, dict) and isinstance(live, dict):
                SessionService.merge_event_changes(live, old if isinstance(old, dict) else {}, value)
            elif isinstance(value, list) and isinstance(old, list) and isinstance(live, list):
                if live == old:
                    current[key] = copy.deepcopy(value)
                else:
                    live.extend(copy.deepcopy(item) for item in value if item not in old and item not in live)
            elif live == old:
                current[key] = copy.deepcopy(value)

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
                    await self.repo.put("ops.agent_sessions", sid, current)
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
                                "text": canonical_json(
                                    tool_wire_result(
                                        call["tool"], call["result"], session_id=sid, call_id=call["call_id"]
                                    )
                                ),
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
                        self.merge_event_changes(
                            current.setdefault("native_execution_receipts", {}),
                            before.get("native_execution_receipts", {}),
                            session.get("native_execution_receipts", {}),
                        )
                        if current.get("native_idle_receipt", {}).get("id") == event["id"]:
                            current["native_idle_receipt"].update(
                                session["native_execution_receipts"][event["id"]]
                            )
                        await self.repo.put("ops.agent_sessions", sid, current)
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
                owner = await self.native_input_owner(session, event["execution_id"])
                if owner:
                    session["active_message_id"] = owner[0]
                    event = dict(event, _host_turn_message_id=owner[0], _host_turn_retry_count=owner[1])
                elif kind in {
                    "agent.custom_tool_use",
                    "session.error",
                    "execution.status_failed",
                }:
                    raise ValueError("Native execution has no acknowledged accepted-input owner")
            if kind == "agent.custom_tool_use":
                await self.tool(session, event)
                session = await self.repo.get("ops.agent_sessions", sid)
                before = copy.deepcopy(session)
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
                await self.usage(session, event)
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
                active = next(
                    (
                        row
                        for row in session.get("message_log", [])
                        if row["message_id"] == session.get("active_message_id")
                    ),
                    None,
                )
                body = active["body"] if active else session.get("last_message")
                retry = active.get("retry_count", 0) if active else session.get("retry_count", 0)
                if retry < 1 and body:
                    message_id = active["message_id"] if active else "legacy"

                    async with self.repo.lock("session:" + sid):
                        durable = await self.repo.get("ops.agent_sessions", sid)
                        event = dict(event)
                        event.setdefault(
                            "_host_retry_call_ordinal_cutoff", durable.get("next_tool_call_ordinal", 0)
                        )
                        durable["pending_retry_event"] = copy.deepcopy(event)
                        await self.repo.put("ops.agent_sessions", sid, durable)
                        before["pending_retry_event"] = copy.deepcopy(event)
                    accepted = await self.request(
                        "POST",
                        f"sessions/{sid}/events",
                        json={"events": [body]},
                        idem="turn-retry:" + sid + ":" + message_id,
                    )
                    if active:
                        active.setdefault("retry_call_ordinal_cutoffs", {})[str(retry)] = event[
                            "_host_retry_call_ordinal_cutoff"
                        ]
                        previous_input = active.get("remote_event_id")
                        if previous_input:
                            active.setdefault("native_input_attempts", {})[previous_input] = retry
                        replacement_input = self.accepted_input_id(accepted)
                        if replacement_input:
                            active.setdefault("native_input_attempts", {})[replacement_input] = retry + 1
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
                        )
                    else:
                        session["retry_count"] = retry + 1
                    session["pending_retry_event"] = None
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
                    failed_payload = (active or {}).get("payload") or session.get("last_payload") or {}
                    item_id = failed_payload.get("item_id") or session.get("item_id")
                    if item_id:
                        await fail_with_session(self.repo, session, item_id, terminal_failure)
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
                self.merge_event_changes(
                    current,
                    {key: value for key, value in before.items() if key != "message_log"},
                    {key: value for key, value in session.items() if key != "message_log"},
                )
                prior_messages = {row["message_id"]: row for row in before.get("message_log", [])}
                live_messages = {row["message_id"]: row for row in current.get("message_log", [])}
                for row in session.get("message_log", []):
                    if row["message_id"] in live_messages and row["message_id"] in prior_messages:
                        self.merge_event_changes(
                            live_messages[row["message_id"]], prior_messages[row["message_id"]], row
                        )
                if event["id"] not in current.setdefault("processed_events", []):
                    current["processed_events"].append(event["id"])
                cursor = max(int(seq) if seq is not None else 0, self.pending_seq.pop(sid, 0))
                if cursor:
                    current["last_event_seq"] = max(int(current.get("last_event_seq", 0)), cursor)
                current["last_event_id"] = event["id"]
                current.update(self.event_timestamp(event).fields("last_event_at"))
                current.update(propagate_timestamp(event, "processed_at").fields("processed_at"))
                await self.repo.put("ops.agent_sessions", sid, current)
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

    async def catch_up(self, sid):

        page = None
        while True:
            params = {"limit": 100, "order": "asc"}
            if page:
                params["page"] = page
            data = await self.request("GET", f"sessions/{sid}/events", params=params)
            for event in data.get("data", []):
                await self.process_event(sid, event)
            page = data.get("next_page")
            if not page:
                break

    async def stream(self, sid):
        session = await self.repo.get("ops.agent_sessions", sid)
        headers = self.headers()
        headers["Last-Event-ID"] = str(session.get("last_event_seq", 0))
        async with aconnect_sse(
            self.stream_http, "GET", self.url(f"sessions/{sid}/events/stream"), headers=headers
        ) as source:
            source.response.raise_for_status()
            async for message in source.aiter_sse():
                if self.closed:
                    return
                if message.event == "ping":
                    continue
                data = json.loads(message.data)

                event = data.get("event", data) if "seq" in data else data
                seq = data.get("seq")
                if seq is None and message.id and message.id.isdigit():
                    seq = int(message.id)
                await self.process_event(sid, event, seq)

    async def can_archive(self, session, event):
        """Terminal domain state cannot preempt a pending denial/EXIT turn."""
        state = event.get("lifecycle", event.get("status"))
        if state not in ("CLOSED", "INVALIDATED", "MERGED"):
            return False
        if session.get("runtime_status") != "IDLE":
            return False
        eid, sid = event["event_id"], session["session_id"]
        calls = [
            row async for row in self.repo.iter_all("ana.calls", filters={"event_id": eid, "status": "OPEN"})
        ]
        if any(call.get("status") == "OPEN" for call in calls):
            return False
        async for _ in self.repo.iter_all(
            "ana.watch_conditions", filters={"event_id": eid, "status": "ARMED"}
        ):
            return False
        tools = [row async for row in self.repo.iter_all("ops.tool_calls", filters={"session_id": sid})]
        if any(tool.get("status") == "PENDING" or not tool.get("returned_at") for tool in tools):
            return False
        alerts = [row async for row in self.repo.iter_all("out.alerts", filters={"event_id": eid})]
        alert_ids = {alert["alert_id"] for alert in alerts}
        jobs = []
        for status in ("QUEUED", "RUNNING"):
            jobs.extend([row async for row in self.repo.iter_all("ops.jobs", filters={"status": status})])
        for job in jobs:
            if job.get("status") not in ("QUEUED", "RUNNING"):
                continue
            payload = job.get("payload", {})
            if (
                payload.get("event_id") == eid
                or payload.get("session_id") == sid
                or payload.get("alert_id") in alert_ids
            ):
                return False
        ordinary_alerts = [alert for alert in alerts if alert.get("kind") != "PRELIMINARY"]
        chain = ordinary_alerts or alerts
        if state == "INVALIDATED" and chain:
            if not event.get("session_finalized_at"):
                return False
            latest = max(chain, key=lambda alert: alert.get("version", 0))
            if latest.get("kind") != ("UPDATE" if ordinary_alerts else "PRELIMINARY") or (
                not ordinary_alerts and not latest.get("terminal_correction")
            ):
                return False
            deliveries = [
                row
                async for row in self.repo.iter_all(
                    "out.deliveries",
                    filters={
                        "alert_id": latest["alert_id"],
                        "version": latest.get("version"),
                        "status": "SENT",
                    },
                )
            ]
            if not any(
                row.get("alert_id") == latest["alert_id"]
                and row.get("version") == latest.get("version")
                and row.get("status") == "SENT"
                for row in deliveries
            ):
                return False
        return True

    async def finish_if_idle(self, sid):
        verified = await self.completed_turn(sid)
        if not verified["complete"]:
            return False
        async with self.repo.lock("session:" + sid):
            session = await self.repo.get("ops.agent_sessions", sid)
            current_barrier = await self.execution_barrier(session)
            if (
                not current_barrier["complete"]
                or current_barrier["accepted_input_ids"] != verified["accepted_input_ids"]
            ):
                return False
            event = (
                await self.repo.get("evt.events", session["event_id"]) if session.get("event_id") else None
            )
            archive = bool(event and await self.can_archive(session, event))
            if not event and await self.eventless_finished(session):
                unfinished = [
                    row
                    async for row in self.repo.iter_all("ops.tool_calls", filters={"session_id": sid})
                    if not row.get("returned_at")
                ]
                archive = not unfinished and not any(
                    row.get("turn_status") == "PENDING" for row in session.get("message_log", [])
                )
            await self.retain_or_archive(sid, session, archive)
            return True

    async def eventless_finished(self, session):
        """Whether a session without an event has nothing left to do.

        Such a session holds only its source item or its scout proposal, so it is finished once the
        agent rejected the material (no_event), its item failed as AGENT_INCOMPLETE, or every source
        item it received is no longer unfinished (DONE, SKIPPED or FAILED, including the legacy
        SKIPPED / NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION of a silent turn). A scout review session
        has no source item and is finished once its turns settle."""
        if session.get("no_event") or session.get("agent_incomplete"):
            return True
        items = {session.get("item_id"), session.get("opened_by_item")}
        items |= {(row.get("payload") or {}).get("item_id") for row in session.get("message_log", [])}
        for item_id in sorted(item for item in items if item):
            item = await self.repo.get("ingest.source_items", item_id)
            if item is not None and item.get("extraction_status") in UNFINISHED:
                return False
        return True

    async def finish_failed(self, sid):
        """Archive a FAILED session in OpenMA under the same retention as a finished one.

        A FAILED session takes no further message, so nothing but a reader follow-up needs it: one
        whose card was delivered or that received a question is kept (followups_only), any other is
        archived session_keep_days after its last message. Its status stays FAILED."""
        async with self.repo.lock("session:" + sid):
            session = await self.repo.get("ops.agent_sessions", sid)
            if not session or session.get("status") != "FAILED" or session.get("archived_at"):
                return False
            await self.retain_or_archive(sid, session, True)
            return True

    async def retain_or_archive(self, sid, session, archive):
        """Archive a finished session once its retention has passed; record when it will be due."""
        replay = getattr(self.settings, "replay", False)
        if archive and not replay and await self.followable(session):
            archive = False
            if not session.get("followups_only"):
                session["followups_only"] = True
                await self.repo.put("ops.agent_sessions", sid, session)
        elif archive and not replay:
            keep = threshold(self.settings, "session_keep_days", 7)
            last = await self.last_message_at(session)
            due = last + timedelta(days=keep)
            if utcnow() < due:
                archive = False
                retained = {
                    **collection_clock(last).fields("last_message_at"),
                    **collection_clock(due).fields("archive_after"),
                }
                if any(session.get(key) != value for key, value in retained.items()):
                    session.update(retained)
                    await self.repo.put("ops.agent_sessions", sid, session)
            else:
                session.update(
                    archive_reason="RETENTION",
                    archive_keep_days=keep,
                    **collection_clock(last).fields("last_message_at"),
                )
        elif not archive and session.get("archive_after"):
            for key in ("archive_after", "archive_after_prec", "archive_after_prec_reason"):
                session.pop(key, None)
            await self.repo.put("ops.agent_sessions", sid, session)
        if archive:
            await self.request("POST", f"sessions/{sid}/archive", json={})
            session.update(
                status="FAILED" if session.get("status") == "FAILED" else "ENDED",
                **self.clock_timestamp().fields("archived_at"),
            )
            session.setdefault("archive_reason", "REPLAY" if replay else "RETENTION")
            await self.repo.put("ops.agent_sessions", sid, session)

    async def last_message_at(self, session):
        """When the session last had a message: Tank's input, the agent's output or a reader's question.

        The latest of the session opening, every message_log input (requested and sent), the last
        stream event from the agent, and each follow-up question (asked, answered, delivered).
        """
        stamps = [session.get("opened_at"), session.get("last_event_at")]
        for row in session.get("message_log", []):
            stamps += [row.get("requested_at"), row.get("sent_at")]
        async for question in self.repo.iter_all(
            "out.user_questions", filters={"session_id": session["session_id"]}
        ):
            stamps += [question.get("asked_at"), question.get("answered_at"), question.get("delivered_at")]
        known = [parse_time(value) for value in stamps if value]
        if not known:
            raise ValueError("Session has no recorded message time")
        return max(known)

    async def followable(self, session):
        """A session whose card or answer reached Telegram stays open for reader follow-ups."""
        async for _ in self.repo.iter_all(
            "out.user_questions", filters={"session_id": session["session_id"]}
        ):
            return True
        if not session.get("event_id"):
            return False
        async for alert in self.repo.iter_all("out.alerts", filters={"event_id": session["event_id"]}):
            async for _ in self.repo.iter_all(
                "out.deliveries",
                filters={"alert_id": alert["alert_id"], "channel": "telegram:alerts", "status": "SENT"},
            ):
                return True
        return False

    async def follow(self, sid):
        delay = 1
        failures = 0
        logged_at = None

        def recovered():
            nonlocal failures, logged_at
            if failures:
                log.info("Agent session %s follower recovered after %d failures", sid, failures)
                failures, logged_at = 0, None

        while not self.closed:
            stream_task = None
            try:
                stream_task = asyncio.create_task(self.stream(sid))
                await self.catch_up(sid)
                session = await self.repo.get("ops.agent_sessions", sid)
                if not session.get("runtime_status"):
                    remote = await self.request("GET", f"sessions/{sid}")
                    if remote.get("status") == "terminated":
                        session["status"] = "ENDED"
                    session["runtime_status"] = {
                        "idle": "IDLE",
                        "running": "RUNNING",
                        "rescheduling": "PENDING",
                        "terminated": "ENDED",
                    }.get(remote.get("status"), "RUNNING")
                    if not session.get("event_id"):
                        session["no_event"] = any(
                            [
                                row.get("tool") == "record_event"
                                and row.get("result", {}).get("is_event") is False
                                async for row in self.repo.iter_all(
                                    "ops.tool_calls", filters={"session_id": sid}
                                )
                            ]
                        )
                    await self.repo.put("ops.agent_sessions", sid, session)
                if session.get("status") in ("FAILED", "ENDED") or await self.finish_if_idle(sid):
                    recovered()
                    return
                while not self.closed:
                    done, _ = await asyncio.wait([stream_task], timeout=10)
                    if done:
                        await stream_task
                        if await self.finish_if_idle(sid):
                            recovered()
                            return
                        break
                    await self.catch_up(sid)
                    recovered()
                    session = await self.repo.get("ops.agent_sessions", sid)
                    if session.get("status") in ("FAILED", "ENDED") or await self.finish_if_idle(sid):
                        return
                delay = 1
                recovered()
            except asyncio.CancelledError:
                raise
            except (httpx.HTTPError, ValueError, KeyError, RuntimeError) as exc:
                delay = min(delay * 2, 30)
                failures += 1
                now = time.monotonic()
                if logged_at is None or now - logged_at >= FOLLOW_FAILURE_LOG_INTERVAL_S:
                    logged_at = now
                    log.warning(
                        "Agent session %s follower failed: %s (%s); failure %d, retrying in %ss",
                        sid,
                        type(exc).__name__,
                        exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else "-",
                        failures,
                        delay,
                    )
            finally:
                if stream_task and not stream_task.done():
                    stream_task.cancel()
                    await asyncio.gather(stream_task, return_exceptions=True)
            await asyncio.sleep(delay)

    def running_session_cap(self):
        cap = self.settings.load_config("openma").get("max_running_sessions", DEFAULT_MAX_RUNNING_SESSIONS)
        if isinstance(cap, bool) or not isinstance(cap, int) or cap < 1:
            raise ConfigurationError("openma.yaml max_running_sessions must be a positive integer")
        return cap

    @staticmethod
    def running(session):
        return session.get("status") == "OPEN" and (
            session.get("runtime_status") in BUSY_RUNTIME_STATES or bool(session.get("active_message_id"))
        )

    async def existing_session(self, payload):
        """The admission fields of the session a message would go to, without creating one.

        It selects the same generation as resolve: a closed session is skipped, so a message for
        an event whose replacement session is running is admitted to that running session."""
        payload = await self.readdressed(payload)
        event_id = payload.get("event_id")
        for field, value in (
            ("session_id", payload.get("session_id")),
            ("event_id", event_id),
            ("item_id", payload.get("item_id") if not event_id else None),
            ("proposal_id", payload.get("proposal_id") or payload.get("prp_id")),
        ):
            if value:
                async for session in self.repo.iter_metadata(
                    "ops.agent_sessions", SESSION_ADMISSION_FIELDS, filters={field: value}
                ):
                    if field == "session_id" or (
                        self.routable_session(session) and self.joins_attempt(session, field, payload)
                    ):
                        return session
        return None

    async def slot_state(self, exclude_job=None):
        """Running turns (busy sessions plus admitted jobs) and the queued waiting jobs."""
        running = {
            session["session_id"]
            async for session in self.repo.iter_metadata(
                "ops.agent_sessions", SESSION_ADMISSION_FIELDS, filters={"status": "OPEN"}
            )
            if self.running(session)
        }
        waiting = []
        for kind in ADMITTED_JOB_TYPES:
            async for other in self.repo.iter_metadata(
                "ops.jobs", JOB_ADMISSION_FIELDS, filters={"type": kind, "status": "RUNNING"}
            ):
                if other["job_id"] != exclude_job and other.get("slot_admitted"):
                    running.add("job:" + other["job_id"])
            async for other in self.repo.iter_metadata(
                "ops.jobs", JOB_ADMISSION_FIELDS, filters={"type": kind, "status": "QUEUED"}
            ):
                if other["job_id"] != exclude_job and other.get("slot_waiting_since"):
                    waiting.append(other)
        waiting.sort(key=lambda row: (row["priority"], row["slot_waiting_since"], row["job_id"]))
        return running, waiting

    async def admit(self, job):
        """Admit a turn under the running-session cap or raise AdmissionDeferred.

        A message to a session already running a turn is never blocked: OpenMA
        queues it behind the current turn. Otherwise the turn starts only while
        fewer than the cap are running and no older waiting message (by priority,
        then first enqueue) is still queued, so waiting messages keep their order.
        A deferred job sleeps; wake_waiting() schedules it when a slot frees, so
        waiting turns never occupy the job workers.
        """
        target = await self.existing_session(job["payload"])
        if target and self.running(target):
            return
        cap = self.running_session_cap()
        own = (job["priority"], job.get("slot_waiting_since") or job["created_at"])
        async with self.repo.lock("session-admission"):
            running, waiting = await self.slot_state(exclude_job=job["job_id"])
            older = any((row["priority"], row["slot_waiting_since"]) < own for row in waiting)
            if older or len(running) >= cap:
                raise AdmissionDeferred(
                    offset_schedule(collection_clock(), timedelta(seconds=ADMISSION_SLEEP_S))
                )
            current = await self.repo.get("ops.jobs", job["job_id"])
            await self.repo.put("ops.jobs", job["job_id"], {**current, "slot_admitted": True})

    async def wake_waiting(self):
        """Schedule the oldest waiting turns for the free slots, and any turn whose session runs."""
        cap = self.running_session_cap()
        async with self.repo.lock("session-admission"):
            running, waiting = await self.slot_state()
            free = max(0, cap - len(running))
            now = collection_clock()
            for row in waiting:
                target = await self.existing_session(row["payload"])
                if free > 0 or (target and self.running(target)):
                    if not (target and self.running(target)):
                        free -= 1
                    current = await self.repo.get("ops.jobs", row["job_id"])
                    if current and current["status"] == "QUEUED":
                        await self.repo.put("ops.jobs", row["job_id"], {**current, **now.fields("run_after")})

    async def scout_review(self, payload):
        proposal_id = payload.get("proposal_id") or payload.get("prp_id")
        if not proposal_id:
            raise ValueError("SCOUT_REVIEW needs proposal_id")
        proposal = await self.repo.get("scout.expansion_proposals", proposal_id)
        if proposal is None:
            raise KeyError(proposal_id)
        return await self.send_message({"kind": "SCOUT_REVIEW", "proposal_id": proposal_id})

    async def sweep_failed(self):
        """Archive FAILED sessions whose retention has passed; kept and retained ones are skipped."""
        async for session in self.repo.iter_metadata(
            "ops.agent_sessions", SESSION_ADMISSION_FIELDS, filters={"status": "FAILED"}
        ):
            kept = bool(session.get("archive_after")) and parse_time(session["archive_after"]) > utcnow()
            if session.get("archived_at") or session.get("followups_only") or kept:
                continue
            await self.finish_failed(session["session_id"])

    async def run(self, stop):
        tick = 0
        while not stop.is_set() and not self.closed:
            async for session in self.repo.iter_metadata(
                "ops.agent_sessions", SESSION_ADMISSION_FIELDS, filters={"status": "OPEN"}
            ):
                sid = session["session_id"]
                settled = session.get("runtime_status") == "IDLE" and not session.get("active_message_id")
                following = sid in self.tasks and not self.tasks[sid].done()
                kept = bool(session.get("archive_after")) and parse_time(session["archive_after"]) > utcnow()
                if (
                    settled
                    and not following
                    and (session.get("followups_only") or kept or tick % IDLE_RECHECK_TICKS)
                ):
                    continue
                if await self.finish_if_idle(sid):
                    continue
                self.start_stream(sid)
            if tick % IDLE_RECHECK_TICKS == 0:
                await self.sweep_failed()
                await self.reconcile_answers()
            tick += 1
            await self.wake_waiting()
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=5)

    async def close(self):
        self.closed = True
        await self.stop_followers()
        if self.owns_stream_http:
            await self.stream_http.aclose()
        if self.owns_http:
            await self.http.aclose()
