import asyncio
import json

import httpx
from model_fixture import model_arguments


class CommittedPhaseProvider:
    def __init__(self):
        self.events = []
        self.executions = {}
        self.declarations = {}
        self.pending_callback = None
        self.changed = asyncio.Event()

    def append(self, event):
        self.events.append(event)
        self.changed.set()

    def accept(self, event):
        if event["type"] == "user.custom_tool_result":
            assert event["custom_tool_use_id"] == self.pending_callback
            self.pending_callback = None
        else:
            assert event["type"] == "user.message" and self.pending_callback is None
        identity = f"accepted-{len(self.executions) + 1}"
        self.executions[identity] = "RUNNING"
        self.append(dict(event, id=identity))
        return identity

    async def emit(self, event, *, project_arguments=True):
        identity = event["execution_id"]
        assert self.executions[identity] == "RUNNING"
        if event["type"] == "agent.custom_tool_use":
            if project_arguments:
                event = dict(event, input=model_arguments(event["name"], event["input"]))
            self.append(event)
            call_id = event["id"]
            assert self.pending_callback is None and call_id not in self.declarations.values()
            self.append(
                {
                    "id": f"requires-action-{call_id}",
                    "type": "session.status_idle",
                    "execution_id": identity,
                    "stop_reason": {"type": "requires_action", "event_ids": [call_id]},
                }
            )
            self.declarations[identity] = call_id
            self.pending_callback = call_id
        else:
            assert event["type"] == "session.status_idle" and event["stop_reason"]["type"] == "end_turn"
            assert self.pending_callback is None
            self.append(event)
        self.executions[identity] = "COMPLETED"

    def status(self, request):
        assert request.method == "GET"
        ids = request.url.params.get_list("ids[]")
        assert ids and len(ids) <= 64 and len(ids) == len(set(ids)) and set(ids) <= self.executions.keys()
        rows = []
        for identity in ids:
            state = self.executions[identity]
            row = {
                "execution_id": identity,
                "state": state,
                "settled_outcome": state if state == "COMPLETED" else None,
            }
            if request.url.params.get("include") == "tool_readiness":
                call_id = self.declarations.get(identity)
                row["ready_tool_call_ids"] = (
                    [call_id] if state == "COMPLETED" and call_id and call_id == self.pending_callback else []
                )
            rows.append(row)
        return httpx.Response(200, json={"data": rows})

    def history(self, request):
        assert request.url.params["order"] == "asc"
        start = int(request.url.params.get("page", "0"))
        end = start + int(request.url.params["limit"])
        return httpx.Response(
            200,
            json={"data": self.events[start:end], "next_page": str(end) if end < len(self.events) else None},
        )

    def stream(self, request):
        provider = self
        after = int(request.headers["Last-Event-ID"])

        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                index = after
                while True:
                    while index < len(provider.events):
                        event = provider.events[index]
                        index += 1
                        yield f"id: {index}\ndata: {json.dumps(event)}\n\n".encode()
                    provider.changed.clear()
                    await provider.changed.wait()

        return httpx.Response(200, stream=Stream(), headers={"content-type": "text/event-stream"})
