"""OpenMA managed-agents v1 adapter, verified at upstream 85d5361f535c.

Durable event-list reconciliation complements SSE: upstream's strict managed
stream ignores Last-Event-ID, while the native Node transport honors it.
"""

import asyncio
import copy
import json
import time
from contextlib import AsyncExitStack, suppress
from dataclasses import dataclass
from datetime import timedelta

import httpx
from httpx_sse import aconnect_sse

from tank.agents import workflow
from tank.agents.pipeline import ConfigurationError
from tank.agents.security import redact_tool_arguments
from tank.agents.wire import tool_wire_result
from tank.telegram_inbox import QUESTION_INSTRUCTIONS, record_answer
from tank.time_precision import collection_clock, offset_schedule, propagate_timestamp
from tank.util import canonical_json, iso, sha256

REST_TIMEOUT_S = 30

DEFAULT_MAX_RUNNING_SESSIONS = 2
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


SESSION_ADMISSION_FIELDS = ("session_id", "status", "runtime_status", "active_message_id", "followups_only")
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
        identifiers = [
            ("event_id", event_id),
            ("item_id", item_id if not event_id else None),
            ("proposal_id", payload.get("proposal_id")),
        ]
        for field, value in identifiers:
            if value:
                async for session in self.repo.iter_all(
                    "ops.agent_sessions",
                    filters={field: value, **({"replay_run_id": replay_run_id} if replay_run_id else {})},
                ):
                    return session
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
                **({"replay_run_id": replay_run_id} if replay_run_id else {}),
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
        session = {
            "session_id": sid,
            "event_id": event_id,
            "replay_run_id": replay_run_id,
            "replay_source_session_id": payload.get("replay_source_session_id"),
            "replay_evidence_sha256": payload.get("replay_evidence_sha256"),
            "replay_cutoff": payload.get("replay_cutoff"),
            "item_id": item_id,
            "proposal_id": payload.get("proposal_id"),
            "opened_by_item": item_id,
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
        """A follow-up question reopens its own session; a natively terminated one cannot continue."""
        question_id = payload["question_id"]
        session = await self.repo.get("ops.agent_sessions", payload.get("session_id") or "")
        if (
            session is None
            or session.get("termination_receipt")
            or session.get("replay_run_id")
            or getattr(self.settings, "replay", False)
        ):
            await record_answer(self.repo, question_id, None, status="NO_SESSION")
            return False
        if session.get("status") in ("FAILED", "ENDED"):
            async with self.repo.lock("session:" + session["session_id"]):
                current = await self.repo.get("ops.agent_sessions", session["session_id"])
                if current.get("status") in ("FAILED", "ENDED"):
                    retired = set(current.get("retired_message_ids", []))
                    retired.update(
                        row["message_id"]
                        for row in current.get("message_log", [])
                        if row.get("turn_status") != "DONE"
                    )
                    current.setdefault("reopened", []).append(
                        {
                            "question_id": question_id,
                            "previous_status": current["status"],
                            **self.clock_timestamp().fields("at"),
                        }
                    )
                    current.update(
                        status="OPEN",
                        runtime_status="IDLE",
                        active_message_id=None,
                        pending_retry_event=None,
                        retired_message_ids=sorted(retired),
                    )
                    await self.repo.put("ops.agent_sessions", current["session_id"], current)
        return True

    async def question_context(self, question_id):
        question = await self.repo.get("out.user_questions", question_id)
        if question is None:
            raise KeyError(question_id)
        return {
            "instructions": QUESTION_INSTRUCTIONS,
            "question_id": question_id,
            "text": question.get("text", ""),
            "asked_at": question.get("asked_at"),
            "in_reply_to": question.get("reference"),
        }

    async def send_message(self, payload):
        if payload.get("kind") == "USER_QUESTION" and not await self.continuable(payload):
            return None
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
        async with self.send_locks.setdefault(sid, asyncio.Lock()):
            async with self.repo.lock("session:" + sid):
                session = await self.repo.get("ops.agent_sessions", sid)
                if key in session.get("messages", []):
                    self.start_stream(sid)
                    return session
                intent = next(
                    (row for row in session.get("message_log", []) if row["message_id"] == key), None
                )
                if intent is None:
                    content = {"message": payload, "item": item}
                    if payload.get("question_id"):
                        content["question"] = await self.question_context(payload["question_id"])
                    if payload.get("proposal_id"):
                        content["proposal"] = await self.repo.get(
                            "scout.expansion_proposals", payload["proposal_id"]
                        )
                        content["open_proposals"] = [
                            row
                            async for row in self.repo.iter_all("scout.expansion_proposals")
                            if not row.get("human_action")
                        ]
                    body = {
                        "type": "user.message",
                        "content": [{"type": "text", "text": canonical_json(content)}],
                    }
                    intent = {
                        "message_id": key,
                        "body": body,
                        "payload": copy.deepcopy(payload),
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

    async def post_message_intent(self, sid, key):
        session = await self.repo.get("ops.agent_sessions", sid)
        intent = next(row for row in session["message_log"] if row["message_id"] == key)
        if intent.get("remote_event_id"):
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
            session["status"] = "OPEN"
            if intent["payload"].get("item_id"):
                session.update(accepted_at.fields("last_item_at"))
                session.update(
                    propagate_timestamp(intent, "item_observed_at").fields("last_item_observed_at")
                )
                session.get("missing_fields", {}).pop("last_item_at", None)
                session["last_item_at_reason"] = None
            await self.repo.put("ops.agent_sessions", sid, session)
            return session

    async def recover_outbound_inputs(self, sid):
        async with self.send_locks.setdefault(sid, asyncio.Lock()):
            session = await self.repo.get("ops.agent_sessions", sid)
            for message in session.get("message_log", []):
                if message.get("turn_status") == "ACCEPTING" and not message.get("sent_at"):
                    await self.post_message_intent(sid, message["message_id"])
        session = await self.repo.get("ops.agent_sessions", sid)
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

    async def native_input_owner(self, session, execution_id):
        """Resolve exact accepted-input lineage, including tool-result continuations."""
        if not execution_id:
            return None
        messages = {row["message_id"]: row for row in session.get("message_log", [])}
        for message in messages.values():
            if execution_id == message.get("remote_event_id") or execution_id in message.get(
                "superseded_native_input_ids", []
            ):
                attempt = message.get("native_input_attempts", {}).get(execution_id)
                if attempt is None:
                    attempt = (
                        message.get("retry_count", 0)
                        if execution_id == message.get("remote_event_id")
                        else message.get("superseded_native_input_ids", []).index(execution_id)
                    )
                return message["message_id"], attempt
        async for call in self.repo.iter_all("ops.tool_calls", filters={"session_id": session["session_id"]}):
            if call.get("remote_event_id") == execution_id and call.get("native_turn_message_id") in messages:
                return call["native_turn_message_id"], call.get("native_turn_retry_count", 0)
        return None

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
            if identity:
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
        final_receipt = any(
            receipt.get("execution_id") in required
            and receipt.get("type") == "session.status_idle"
            and receipt.get("stop_reason", {}).get("type") == "end_turn"
            for identity, receipt in receipts.items()
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
                current.setdefault("native_execution_states", {}).update(states)
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
                required.difference_update(current.get("superseded_native_inputs", {}))
                await self.repo.put("ops.agent_sessions", session["session_id"], current)
            delay = min(poll[1] * 2, 1.0) if poll[2] == signature else 0.05
            self.execution_polls[session["session_id"]] = (time.monotonic() + delay, delay, signature)
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

    async def turn_answers(self, session):
        """The last agent text of each accepted input's executions, keyed by owning message."""
        texts, current, page = {}, None, None
        while True:
            params = {"limit": 100, "order": "asc"}
            if page:
                params["page"] = page
            data = await self.request("GET", f"sessions/{session['session_id']}/events", params=params)
            for event in data.get("data", []):
                kind = event.get("type")
                if kind == "session.status_running":
                    current = event.get("execution_id")
                elif kind == "agent.message" and current:
                    text = "".join(
                        block.get("text", "")
                        for block in event.get("content", [])
                        if isinstance(block, dict) and block.get("type") == "text"
                    ).strip()
                    if text:
                        texts[current] = text
            page = data.get("next_page")
            if not page:
                break
        answers = {}
        for execution_id, text in texts.items():
            owner = await self.native_input_owner(session, execution_id)
            if owner:
                answers[owner[0]] = text
        return answers

    async def deliver_answers(self, sid):
        """Hand each settled follow-up question its answer, or its failure, exactly once."""
        session = await self.repo.get("ops.agent_sessions", sid)
        stopped = session.get("status") in ("FAILED", "ENDED")
        pending = [
            row
            for row in session.get("message_log", [])
            if row.get("payload", {}).get("kind") == "USER_QUESTION"
            and not row.get("answer_recorded")
            and (row.get("turn_status") in ("DONE", "FAILED") or stopped)
        ]
        if not pending:
            return
        answers = (
            await self.turn_answers(session)
            if any(row.get("turn_status") == "DONE" for row in pending)
            else {}
        )
        for row in pending:
            question_id = row["payload"]["question_id"]
            if row.get("turn_status") == "DONE":
                await record_answer(self.repo, question_id, answers.get(row["message_id"]), status="ANSWERED")
            else:
                await record_answer(self.repo, question_id, None, status="FAILED")
        recorded = {row["message_id"] for row in pending}
        async with self.repo.lock("session:" + sid):
            current = await self.repo.get("ops.agent_sessions", sid)
            for row in current.get("message_log", []):
                if row["message_id"] in recorded:
                    row["answer_recorded"] = True
            await self.repo.put("ops.agent_sessions", sid, current)

    async def settled_turn(self, sid):
        """Read committed queue states, drain every history page, then recheck inputs."""
        await self.recover_outbound_inputs(sid)
        session = await self.repo.get("ops.agent_sessions", sid)
        local = await self.execution_barrier(session)
        source_outcomes = await self.source_completion_snapshot(session)
        if session.get("status") == "FAILED":
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
            if not after["complete"] or after["accepted_input_ids"] != before["accepted_input_ids"]:
                return dict(after, complete=False)
            for message in current.get("message_log", []):
                if message.get("turn_status") != "FAILED":
                    message["turn_status"] = "DONE"
                payload = message.get("payload", {})
                if payload.get("kind") in {"NEW_ITEM", "ITEM_ADDED"} and payload.get("item_id"):
                    item = await self.repo.get("ingest.source_items", payload["item_id"])
                    if item and item.get("extraction_status") in {None, "PENDING", "RUNNING"}:
                        item.update(
                            extraction_status="SKIPPED",
                            extraction_reason="NATIVE_TURN_COMPLETED_WITHOUT_OBSERVATION",
                        )
                        await self.repo.put("ingest.source_items", item["item_id"], item)
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
            return after

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
        }:
            await self.hydrate_prompt_provenance(session)
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
            and event["name"] in {"get_market_truth", "get_decision", "web_search", "fetch_page"}
            and (not prior or prior.get("status") not in ("DONE", "REFUSED", "ERROR"))
        ):
            try:
                if event["name"] == "get_decision":
                    prepared = await self.invoke_tool(
                        session, event["name"], event["input"], call_id, decision_market_refresh_only=True
                    )
                    decision_prepared_condition_ids = prepared.get("prepared_condition_ids")
                    decision_market_refresh_completed = True
                else:
                    slow_result = await self.invoke_tool(session, event["name"], event["input"], call_id)
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
                                stored["arguments"],
                                call_id,
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
                completed = self.clock_timestamp()
                stored.update(
                    result=result,
                    status=captured.status if captured else refusal_status if refusal else "DONE",
                    is_error=captured.is_error if captured else bool(refusal),
                    **completed.fields("answered_at"),
                    **completed.fields("completed_at"),
                )
                await self.repo.put("ops.tool_calls", key, stored)
        body = {
            "type": "user.custom_tool_result",
            "custom_tool_use_id": call_id,
            "content": [
                {
                    "type": "text",
                    "text": canonical_json(
                        tool_wire_result(stored["tool"], stored["result"], session_id=sid, call_id=call_id)
                    ),
                }
            ],
        }
        if stored.get("is_error"):
            body["is_error"] = True
        async with self.repo.lock("tool:" + key):
            if stored.get("native_result_body") is None:
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
                        current.setdefault("native_execution_receipts", {}).update(
                            session.get("native_execution_receipts", {})
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
                        item = await self.repo.get("ingest.source_items", item_id)
                        if item and item.get("extraction_status") == "PENDING":
                            item["extraction_status"] = "FAILED"
                            await self.repo.put("ingest.source_items", item_id, item)
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
                await self.repo.put("ops.agent_sessions", sid, current)
            self.seen_events.setdefault(sid, set()).add(seen_key(event))
            if current.get("status") in ("FAILED", "ENDED"):
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
        if state == "INVALIDATED" and alerts:
            if not event.get("session_finalized_at"):
                return False
            latest = max(alerts, key=lambda alert: alert.get("version", 0))
            if latest.get("kind") != "UPDATE":
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
            if not event and session.get("no_event"):
                unfinished = [
                    row
                    async for row in self.repo.iter_all("ops.tool_calls", filters={"session_id": sid})
                    if not row.get("returned_at")
                ]
                archive = not unfinished and not any(
                    row.get("turn_status") == "PENDING" for row in session.get("message_log", [])
                )
            if archive and not getattr(self.settings, "replay", False) and await self.followable(session):
                archive = False
                if not session.get("followups_only"):
                    session["followups_only"] = True
                    await self.repo.put("ops.agent_sessions", sid, session)
            if archive:
                await self.request("POST", f"sessions/{sid}/archive", json={})
                session["status"] = "ENDED"
                await self.repo.put("ops.agent_sessions", sid, session)

            return True

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
                    if session.get("status") in ("FAILED", "ENDED") or await self.finish_if_idle(sid):
                        return
                delay = 1
            except asyncio.CancelledError:
                raise
            except (httpx.HTTPError, ValueError, KeyError, RuntimeError):
                delay = min(delay * 2, 30)
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
        """The admission fields of the session a message would go to, without creating one."""
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

    async def run(self, stop):
        tick = 0
        while not stop.is_set() and not self.closed:
            async for session in self.repo.iter_metadata(
                "ops.agent_sessions", SESSION_ADMISSION_FIELDS, filters={"status": "OPEN"}
            ):
                sid = session["session_id"]
                settled = session.get("runtime_status") == "IDLE" and not session.get("active_message_id")
                following = sid in self.tasks and not self.tasks[sid].done()

                if settled and not following and (session.get("followups_only") or tick % IDLE_RECHECK_TICKS):
                    continue
                if await self.finish_if_idle(sid):
                    continue
                self.start_stream(sid)
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
