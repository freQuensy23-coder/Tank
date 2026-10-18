"""OpenMA managed-agents v1 adapter, verified at upstream 85d5361f535c.

Durable event-list reconciliation complements SSE: upstream's strict managed
stream ignores Last-Event-ID, while the native Node transport honors it.
"""

import asyncio
import copy
import json

import httpx
from httpx_sse import aconnect_sse

from tank.util import canonical_json, iso, sha256

from .pipeline import ConfigurationError
from .security import redact_tool_arguments
from .wire import tool_wire_result


class SessionService:
    def __init__(self, repo, settings, tool_dispatch, *, http=None):
        self.repo, self.settings, self.tool_dispatch = repo, settings, tool_dispatch
        self.http = http or httpx.AsyncClient(timeout=httpx.Timeout(30, read=None))
        self.owns_http = http is None
        self.tasks = {}
        self.event_locks = {}
        self.closed = False

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
        reservation_id = payload.get("reservation_id")
        reservation = (
            await self.repo.get("ops.routing_reservations", reservation_id) if reservation_id else None
        )
        if reservation_id and reservation is None:
            raise KeyError(reservation_id)
        event_id = payload.get("event_id") or (reservation or {}).get("event_id")
        item_id = payload.get("item_id")
        identifiers = [
            ("event_id", event_id),
            ("reservation_id", reservation_id),
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
        identity = reservation_id or event_id or item_id or payload.get("proposal_id")
        if not identity:
            raise ValueError("Session needs event, item or proposal identity")
        # Session creation does not promise idempotency. Reconcile the durable
        # metadata marker before creating, including recovery after a lost reply.
        marker = sha256(
            {
                "identity": identity,
                "scout": scout,
                **({"replay_run_id": replay_run_id} if replay_run_id else {}),
            }
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
                json={"agent": agent, "environment_id": environment, "metadata": {"tank_identity": marker}},
                idem="create:" + marker,
            )
        sid = remote["id"]
        session = {
            "session_id": sid,
            "event_id": event_id,
            "reservation_id": reservation_id,
            "replay_run_id": replay_run_id,
            "replay_source_session_id": payload.get("replay_source_session_id"),
            "item_id": (reservation or {}).get("first_item_id", item_id),
            "proposal_id": payload.get("proposal_id"),
            "opened_by_item": item_id,
            "opened_at": iso(),
            "agent_version": (
                remote.get("agent", {}).get("version") if isinstance(remote.get("agent"), dict) else None
            )
            or config.get("agent_version"),
            "billing_basis": "SUBSCRIPTION",
            "effort": config.get("reasoning_effort"),
            "status": "OPEN",
            "last_event_seq": 0,
            "last_item_at": None,
            "model_id": self.settings.openma_model,
            "prompt_version": config.get("prompt_version", "unversioned"),
            "config_version": self.settings.config_version,
            "code_version": self.settings.code_version,
            "created_at": iso(),
            "messages": [],
        }
        session["missing_fields"] = {
            key: reason
            for key, reason in {
                "opened_by_item": "Proposal or event follow-up has no opening source item",
                "last_item_at": "No source item message accepted yet",
                "agent_version": "Deployed agent version not exposed by remote snapshot or configuration",
            }.items()
            if session.get(key) is None
        }
        session["agent_version_reason"] = session["missing_fields"].get("agent_version")
        session["last_item_at_reason"] = session["missing_fields"].get("last_item_at")
        await self.repo.put("ops.agent_sessions", sid, session)
        if reservation:
            async with self.repo.lock("routing"):
                reservation = await self.repo.get("ops.routing_reservations", reservation_id)
                reservation["session_id"] = sid
                await self.repo.put("ops.routing_reservations", reservation_id, reservation)
        return session

    async def send_message(self, payload):
        item = (
            await self.repo.get("ingest.source_items", payload["item_id"]) if payload.get("item_id") else None
        )
        mapping_identity = (
            payload.get("session_id")
            or payload.get("reservation_id")
            or payload.get("event_id")
            or payload.get("proposal_id")
            or payload.get("item_id")
        )
        async with self.repo.lock("session-mapping:" + str(mapping_identity)):
            session = await self.resolve(payload)
        sid = session["session_id"]
        key = sha256(payload)
        async with self.repo.lock("session:" + sid):
            session = await self.repo.get("ops.agent_sessions", sid)
            if key in session.get("messages", []):
                self.start_stream(sid)
                return session
            content = {"message": payload, "item": item}
            if payload.get("proposal_id"):
                content["proposal"] = await self.repo.get("scout.expansion_proposals", payload["proposal_id"])
                content["open_proposals"] = [
                    row
                    async for row in self.repo.iter_all("scout.expansion_proposals")
                    if not row.get("human_action")
                ]
            event_body = {
                "type": "user.message",
                "content": [{"type": "text", "text": canonical_json(content)}],
            }
            accepted = await self.request(
                "POST", f"sessions/{sid}/events", json={"events": [event_body]}, idem="message:" + key
            )
            session.setdefault("messages", []).append(key)
            session.setdefault("message_log", []).append(
                {
                    "message_id": key,
                    "body": event_body,
                    "payload": payload,
                    "sent_at": iso(),
                    "turn_status": "PENDING",
                    "retry_count": 0,
                    "remote_event_id": (accepted.get("data") or [{}])[0].get("id"),
                }
            )
            session["last_message"] = event_body
            session["last_payload"] = payload
            if not session.get("active_message_id"):
                session["active_message_id"] = key
            session["status"] = "OPEN"
            if session.get("runtime_status") != "RUNNING":
                session["runtime_status"] = "PENDING"
            if item:
                session["last_item_at"] = iso()
                session.get("missing_fields", {}).pop("last_item_at", None)
                session["last_item_at_reason"] = None
            await self.repo.put("ops.agent_sessions", sid, session)
        self.start_stream(sid)
        return session

    def start_stream(self, sid):
        if not self.closed and (sid not in self.tasks or self.tasks[sid].done()):
            self.tasks[sid] = asyncio.create_task(self.follow(sid), name="openma:" + sid)

    def tool_context(self, session, call_id):
        """Every dispatch/recovery is attributed to the immutable agent session."""
        context = {
            key: session.get(key)
            for key in (
                "session_id",
                "event_id",
                "item_id",
                "proposal_id",
                "reservation_id",
                "model_id",
                "prompt_version",
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

    async def invoke_tool(self, session, tool, arguments, call_id):
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
                    filters={"tool": tool, "session_id": source_session_id, "status": "DONE"},
                )
                if row.get("session_id") != session["session_id"]
                and row.get("arguments", row.get("input")) == arguments
            ]
            results = {canonical_json(row["result"]) for row in candidates}
            if len(results) != 1:
                raise ValueError("Replay requires one unambiguous stored external tool result")
            return json.loads(next(iter(results)))
        return await self.tool_dispatch(tool, arguments, self.tool_context(session, call_id))

    async def tool(self, session, event):
        sid, call_id = session["session_id"], event["id"]
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
            {"type": "CredentialRejected", "reason": credential_reason} if credential_reason else None
        )
        if (
            not credential_reason
            and event["name"] == "get_market_truth"
            and (not prior or prior.get("status") not in ("DONE", "REFUSED"))
        ):
            # Its durable queue jobs must commit before separate workers run.
            try:
                slow_result = await self.invoke_tool(session, event["name"], event["input"], call_id)
            except (ValueError, KeyError) as error:
                slow_error = {"type": type(error).__name__, "reason": str(error)}
            slow_ready = True
        async with self.repo.lock("tool:" + key):
            stored = await self.repo.get("ops.tool_calls", key)
            if not stored:
                context = self.tool_context(session, call_id)
                stored = {
                    "call_id": call_id,
                    "session_id": sid,
                    "event_id": session.get("event_id"),
                    "item_id": session.get("item_id"),
                    "tool": event["name"],
                    "arguments": event["input"],
                    "input": event["input"],
                    "input_sha256": original_hash,
                    "arguments_redacted": bool(credential_reason),
                    "status": "PENDING",
                    "requested_at": iso(),
                    **{
                        key: context[key]
                        for key in (
                            "model_id",
                            "prompt_version",
                            "agent_version",
                            "config_version",
                            "code_version",
                            "session_config_version",
                            "session_code_version",
                        )
                    },
                }
                await self.repo.put("ops.tool_calls", key, stored)
            if stored["status"] not in ("DONE", "REFUSED"):
                refusal = slow_error
                try:
                    # A rejected mutation rolls back before its refusal ledger
                    # is committed, allowing the agent to correct its arguments.
                    async with self.repo.transaction():
                        result = (
                            slow_result
                            if slow_ready
                            else await self.invoke_tool(session, stored["tool"], stored["arguments"], call_id)
                        )
                except (ValueError, KeyError) as error:
                    refusal = {"type": type(error).__name__, "reason": str(error)}
                if refusal:
                    result = {"ok": False, "error": refusal}
                stored.update(
                    result=result,
                    status="REFUSED" if refusal else "DONE",
                    is_error=bool(refusal),
                    answered_at=iso(),
                    completed_at=iso(),
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
        await self.request(
            "POST", f"sessions/{sid}/events", json={"events": [body]}, idem="tool:" + sha256(key)
        )
        async with self.repo.lock("tool:" + key):
            stored.update(returned_at=iso())
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
                "at": event.get("processed_at") or iso(),
                "created_at": event.get("processed_at") or iso(),
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
        key = sid + ":" + event["id"]
        async with self.event_locks.setdefault(sid, asyncio.Lock()):
            # Full durable payload is kept in the tool ledger; marker is stored
            # in session's log only after every side effect and response succeeds.
            session = await self.repo.get("ops.agent_sessions", sid)
            if session.get("status") in ("FAILED", "ENDED"):
                return
            before = copy.deepcopy(session)
            if event["id"] in session.get("processed_events", []):
                return
            kind = event["type"]
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
                session["runtime_status"] = (
                    "IDLE"
                    if (event.get("stop_reason") or {}).get("type") == "end_turn"
                    else "REQUIRES_ACTION"
                )
                if session["runtime_status"] == "IDLE":
                    for row in session.get("message_log", []):
                        if row["message_id"] == session.get("active_message_id"):
                            row["turn_status"] = "DONE"
                    following = next(
                        (
                            row
                            for row in session.get("message_log", [])
                            if row.get("turn_status") == "PENDING"
                        ),
                        None,
                    )
                    session["active_message_id"] = following["message_id"] if following else None
                    if following:
                        session["runtime_status"] = "PENDING"
            elif kind == "span.model_request_end":
                await self.usage(session, event)
            if (
                kind in ("session.error", "agent.refusal")
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
                    await self.request(
                        "POST",
                        f"sessions/{sid}/events",
                        json={"events": [body]},
                        idem="turn-retry:" + sid + ":" + message_id,
                    )
                    if active:
                        active.update(retry_count=retry + 1, turn_status="RUNNING")
                    else:
                        session["retry_count"] = retry + 1
                    session["runtime_status"] = "RUNNING"
                else:
                    session["status"] = "FAILED"
                    if active:
                        active["turn_status"] = "FAILED"
                    if session.get("reservation_id"):
                        async with self.repo.lock("routing"):
                            reservation = await self.repo.get(
                                "ops.routing_reservations", session["reservation_id"]
                            )
                            if reservation and reservation.get("status") == "PENDING":
                                reservation["status"] = "CLOSED"
                                await self.repo.put(
                                    "ops.routing_reservations", reservation["reservation_id"], reservation
                                )
                    if session.get("event_id"):
                        record = await self.repo.get("evt.events", session["event_id"])
                        if record:
                            record.update(failure_reason="AGENT_FAILED", status="FAILED", lifecycle="FAILED")
                            await self.repo.put("evt.events", record["event_id"], record)
            elif kind == "session.status_terminated":
                session["status"] = "ENDED"
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
                if current.get("runtime_status") == "IDLE":
                    pending = next(
                        (
                            row
                            for row in current.get("message_log", [])
                            if row.get("turn_status") == "PENDING"
                        ),
                        None,
                    )
                    if pending:
                        current.update(runtime_status="PENDING", active_message_id=pending["message_id"])
                current.setdefault("processed_events", []).append(event["id"])
                if seq is not None:
                    current["last_event_seq"] = max(int(current.get("last_event_seq", 0)), int(seq))
                current["last_event_id"] = event["id"]
                current["last_event_at"] = event.get("processed_at", iso())
                await self.repo.put("ops.agent_sessions", sid, current)

    async def catch_up(self, sid):
        # Scan from the beginning: provider page tokens may be ephemeral, while
        # dedup ids are durable. Never advance past an unfinished tool.
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
            self.http, "GET", self.url(f"sessions/{sid}/events/stream"), headers=headers
        ) as source:
            source.response.raise_for_status()
            async for message in source.aiter_sse():
                if self.closed:
                    return
                if message.event == "ping":
                    continue
                data = json.loads(message.data)
                # Native Node streams wrap {seq,event}; strict managed API emits
                # flat durable event bodies. Numeric SSE ids are native cursors.
                event = data.get("event", data) if "seq" in data else data
                seq = data.get("seq")
                if seq is None and message.id and message.id.isdigit():
                    seq = int(message.id)
                await self.process_event(sid, event, seq)
                latest = await self.repo.get("ops.agent_sessions", sid)
                if latest.get("runtime_status") == "IDLE":
                    return

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
        if any(
            tool.get("status") == "PENDING" or (tool.get("status") == "DONE" and not tool.get("returned_at"))
            for tool in tools
        ):
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
        async with self.repo.lock("session:" + sid):
            session = await self.repo.get("ops.agent_sessions", sid)
            if session.get("runtime_status") != "IDLE":
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
            if archive:
                await self.request("POST", f"sessions/{sid}/archive", json={})
                session["status"] = "ENDED"
                await self.repo.put("ops.agent_sessions", sid, session)
            # Open idle sessions remain durable; their next message starts a new
            # stream. No-event sessions finish after their final tool response.
            return True

    async def follow(self, sid):
        delay = 1
        while not self.closed:
            stream_task = None
            try:
                # Subscribe before list reconciliation to close the history/live
                # gap even when the strict API ignores Last-Event-ID.
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
                    event = (
                        await self.repo.get("evt.events", session["event_id"])
                        if session.get("event_id")
                        else None
                    )
                    if event and await self.can_archive(session, event):
                        await self.request("POST", f"sessions/{sid}/archive", json={})
                        session["status"] = "ENDED"
                        await self.repo.put("ops.agent_sessions", sid, session)
                        return
                delay = 1
            except asyncio.CancelledError:
                raise
            except (httpx.HTTPError, ValueError, KeyError, RuntimeError):
                # No provider errors, bodies or headers enter logs (may include
                # credentials). Durable state remains for reconciliation.
                delay = min(delay * 2, 30)
            finally:
                if stream_task and not stream_task.done():
                    stream_task.cancel()
                    await asyncio.gather(stream_task, return_exceptions=True)
            await asyncio.sleep(delay)

    async def scout_review(self, payload):
        proposal_id = payload.get("proposal_id") or payload.get("prp_id")
        if not proposal_id:
            raise ValueError("SCOUT_REVIEW needs proposal_id")
        proposal = await self.repo.get("scout.expansion_proposals", proposal_id)
        if proposal is None:
            raise KeyError(proposal_id)
        return await self.send_message({"kind": "SCOUT_REVIEW", "proposal_id": proposal_id})

    async def run(self, stop):
        inspected = set()
        while not stop.is_set() and not self.closed:
            async for session in self.repo.iter_all("ops.agent_sessions", filters={"status": "OPEN"}):
                sid = session["session_id"]
                if session.get("runtime_status") == "IDLE":
                    await self.finish_if_idle(sid)
                elif sid not in inspected or session.get("runtime_status") in (
                    "PENDING",
                    "RUNNING",
                    "REQUIRES_ACTION",
                ):
                    self.start_stream(sid)
                inspected.add(sid)
            try:
                await asyncio.wait_for(stop.wait(), timeout=5)
            except TimeoutError:
                pass

    async def close(self):
        self.closed = True
        for task in self.tasks.values():
            task.cancel()
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        if self.owns_http:
            await self.http.aclose()
