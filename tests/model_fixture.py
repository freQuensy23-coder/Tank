"""Adapt scripted actors to the public contract: the host binds the current event, incoming item and proposal."""

import inspect
import json
from contextvars import ContextVar

import httpx

from tank.event_tools import SCOPED_TOOLS

DECLARED_EVENT = ContextVar("declared_fixture_event", default=None)


class DeclaredNativeProtocol:
    def __init__(self, handler, initial=None):
        self.handler = handler
        self.current = dict(initial or {})
        self.calls = {}
        self.states = {identity: "RUNNING" for identity in self.current.values()}
        self.accepted = {}

    def project(self, sid, event):
        kind = event.get("type")
        if kind == "agent.custom_tool_use":
            key = (sid, event["id"])
            if key not in self.calls:
                execution = event.get("execution_id") or self.current.get(sid)
                if not execution:
                    raise AssertionError("Declared fixture call requires an accepted native input")
                event = dict(event, execution_id=execution)
                event.setdefault("processed_at", "2026-10-18T23:00:00Z")
                self.calls[key] = {
                    "declaration_id": "native-declaration-" + event["id"],
                    "event": event,
                }
                self.states[execution] = "COMPLETED"
            use = self.calls[key]
            event = use["event"]
            return [
                event,
                {
                    "id": use["declaration_id"],
                    "type": "session.status_idle",
                    "execution_id": event["execution_id"],
                    "processed_at": event["processed_at"],
                    "stop_reason": {"type": "requires_action", "event_ids": [event["id"]]},
                },
            ]
        if kind == "session.status_idle":
            execution = event.get("execution_id") or self.current.get(sid)
            if execution:
                event = dict(event, execution_id=execution)
                self.states[execution] = "COMPLETED"
        return [event]

    async def __call__(self, request):
        path = request.url.path
        native = "/sessions/" in path
        sid = path.split("/sessions/", 1)[1].split("/", 1)[0] if native else None
        if native and path.endswith("/executions") and sid in self.current:
            identities = request.url.params.get_list("ids[]")
            assert identities and len(identities) <= 64
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "execution_id": identity,
                            "state": self.states.get(identity, "UNKNOWN"),
                            "settled_outcome": "COMPLETED"
                            if self.states.get(identity) == "COMPLETED"
                            else None,
                            "custom_tool_uses": [
                                use
                                for (session, _), use in self.calls.items()
                                if session == sid and use["event"]["execution_id"] == identity
                            ],
                        }
                        for identity in identities
                    ]
                },
            )
        response = self.handler(request)
        if inspect.isawaitable(response):
            response = await response
        if not native or response.status_code >= 400:
            return response
        if request.method == "POST" and path.endswith("/events"):
            body = response.json()
            rows = body.get("data") if isinstance(body, dict) else None
            key = (sid, request.headers.get("idempotency-key", ""), request.content)
            if key not in self.accepted:
                identity = (
                    rows[0]["id"] if rows and rows[0].get("id") else f"native-input-{len(self.accepted)}"
                )
                self.accepted[key] = identity
                self.current[sid] = identity
                self.states[identity] = "RUNNING"
            identity = self.accepted[key]
            return httpx.Response(response.status_code, json={"data": [{"id": identity}]})
        if path.endswith("/events/stream"):
            protocol = self
            source = response.stream

            class DeclaredStream(httpx.AsyncByteStream):
                async def __aiter__(self):
                    pending = b""
                    async for chunk in source:
                        pending += chunk
                        while b"\n\n" in pending:
                            frame, pending = pending.split(b"\n\n", 1)
                            lines = frame.split(b"\n")
                            data = next(
                                (line[5:].strip() for line in lines if line.startswith(b"data:")), None
                            )
                            if not data:
                                yield frame + b"\n\n"
                                continue
                            envelope = json.loads(data)
                            wrapped = isinstance(envelope.get("event"), dict)
                            event = envelope["event"] if wrapped else envelope
                            for index, projected in enumerate(protocol.project(sid, event)):
                                payload = (
                                    dict(envelope, event=projected) if wrapped and index == 0 else projected
                                )
                                prefix = (
                                    [line for line in lines if not line.startswith(b"data:")]
                                    if index == 0
                                    else []
                                )
                                yield (
                                    b"\n".join(prefix + [b"data: " + json.dumps(payload).encode()]) + b"\n\n"
                                )
                    if pending:
                        yield pending

                async def aclose(self):
                    await source.aclose()

            return httpx.Response(
                response.status_code, headers={"content-type": "text/event-stream"}, stream=DeclaredStream()
            )
        if request.method == "GET" and path.endswith("/events"):
            body = response.json()
            if isinstance(body.get("data"), list):
                body["data"] = [projected for event in body["data"] for projected in self.project(sid, event)]
                return httpx.Response(response.status_code, json=body)
        return response


IMPLICIT = {
    **{tool: {"event_id"} for tool in SCOPED_TOOLS},
    "record_event": {"item_id"},
    "record_review": {"proposal_id"},
}


def model_arguments(tool, arguments):
    return {key: value for key, value in arguments.items() if key not in IMPLICIT.get(tool, set())}


async def audited_result(repo, event):
    """Check the public reply, then read host-only lineage for scripted replay bookkeeping."""
    import json

    from tank.agents.context import identifier_field

    public = json.loads(event["content"][0]["text"])

    def check(value):
        if isinstance(value, dict):
            for key, item in value.items():
                assert not identifier_field(key), key
                check(item)
        elif isinstance(value, list):
            for item in value:
                check(item)

    check(public)
    rows = [
        row
        async for row in repo.iter_all("ops.tool_calls", filters={"call_id": event["custom_tool_use_id"]})
        if row.get("native_result_body", {}).get("content") == event["content"]
    ]
    assert rows, "Reply must match the frozen projected result of a durable call"
    return max(rows, key=lambda row: row.get("requested_at", ""))["result"]


async def process_declared_event(service, sid, event, seq=None):
    if event.get("type") != "agent.custom_tool_use":
        return await service.process_event(sid, event, seq)
    session = await service.repo.get("ops.agent_sessions", sid)
    execution_id = event.get("execution_id")
    owner_id = session.get("active_message_id")
    messages = session.setdefault("message_log", [])
    owner = next((row for row in messages if row["message_id"] == owner_id), None)
    if owner is None and messages:
        owner = messages[-1]
    if (
        owner
        and owner["message_id"].startswith("fixture-message-")
        and owner.get("payload") != session.get("last_payload", {})
    ):
        owner = None
    if owner is None:
        owner = {
            "message_id": "fixture-message-" + event["id"],
            "payload": session.get("last_payload", {}),
            "body": {"type": "user.message", "content": []},
            "retry_count": 0,
            "turn_status": "RUNNING",
        }
        messages.append(owner)
    if execution_id is None:
        execution_id = owner.get("remote_event_id") or "fixture-input-" + sid + ":" + owner["message_id"]
    if not owner.get("remote_event_id"):
        owner["remote_event_id"] = execution_id
        owner.setdefault("native_input_attempts", {})[execution_id] = owner.get("retry_count", 0)
    session["active_message_id"] = owner["message_id"]
    await service.repo.put("ops.agent_sessions", sid, session)
    event = dict(event, execution_id=execution_id)
    if not hasattr(service, "fixture_native_request"):
        request = service.request

        async def declared(method, path, **kwargs):
            context = DECLARED_EVENT.get()
            if context is None or context[0] is not service:
                return await request(method, path, **kwargs)
            _, declared_sid, declared_event = context
            if method == "GET" and path == f"sessions/{declared_sid}/executions":
                identities = [identity for key, identity in kwargs["params"] if key == "ids[]"]
                return {
                    "data": [
                        {
                            "execution_id": identity,
                            "state": "COMPLETED",
                            "settled_outcome": "COMPLETED",
                            "custom_tool_uses": [
                                {
                                    "declaration_id": "fixture-declaration-" + declared_event["id"],
                                    "event": declared_event,
                                }
                            ]
                            if identity == declared_event["execution_id"]
                            else [],
                        }
                        for identity in identities
                    ]
                }
            return await request(method, path, **kwargs)

        service.fixture_native_request = request
        service.request = declared
    token = DECLARED_EVENT.set((service, sid, event))
    try:
        return await service.process_event(sid, event, seq)
    finally:
        DECLARED_EVENT.reset(token)
