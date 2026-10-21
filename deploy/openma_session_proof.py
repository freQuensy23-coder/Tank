#!/usr/bin/env python3
"""Deployment proof: read back registered OpenMA resources, then run one real session.

The session must load the registered skills (the agent answers from text that exists only
in the inlined tank-event-analyst and tank-data SKILL.md bodies, without reading files) and make one schema-bound custom tool call
(get_registry), answered with Tank's own EventToolService result. Read-only for Tank state.
Usage: openma_session_proof.py OUT_DIR OPENMA_BASE_URL PROPOSED_OPENMA_YAML CONFIG_DIR
"""

import json
import os
import sys
import time
from pathlib import Path

import httpx
import yaml

sys.path.insert(0, "/srv/tank/src")
from tank.agents.wire import tool_wire_result
from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.util import canonical_json

out, base, proposed_path, config_dir = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4])
out.mkdir(parents=True, exist_ok=True)
proposed = yaml.safe_load(proposed_path.read_text())
key = Path("/etc/tank/secrets/oma_api_key").read_text().strip()
client = httpx.Client(
    base_url=base.rstrip("/") + "/v1/",
    headers={"x-api-key": key, "anthropic-beta": "managed-agents-2026-04-01,skills-2025-10-02"},
    timeout=60,
)


def save(name, value):
    (out / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def get(path, **params):
    response = client.get(path, params=params or None)
    response.raise_for_status()
    body = response.json()
    return body.get("data", body) if isinstance(body, dict) and "data" in body and len(body) <= 2 else body


agent = get("agents/" + proposed["agent_id"])
save("agent-readback.json", agent)
local_tools = json.loads((config_dir / "skills/tank-event-analyst/tools.json").read_text())
remote_custom = {t["name"]: t for t in agent.get("tools", []) if t.get("type") == "custom"}
tool_check = {
    "local_count": len(local_tools),
    "remote_custom_count": len(remote_custom),
    "names_match": sorted(remote_custom) == sorted(t["name"] for t in local_tools),
    "schemas_match": all(
        canonical_json(remote_custom.get(t["name"], {}).get("input_schema"))
        == canonical_json(t.get("input_schema"))
        for t in local_tools
    ),
}
skills = []
for binding in agent.get("skills", []):
    meta = get(f"skills/{binding['skill_id']}")
    skills.append({"binding": binding, "skill": meta})
save("skills-readback.json", skills)
environments = {
    name: get("environments/" + proposed[key_])
    for name, key_ in [("tank-analysis", "environment_id"), ("tank-replay", "replay_environment_id")]
}
save("environments-readback.json", environments)
save("tool-check.json", tool_check)
assert tool_check["local_count"] == 18 and tool_check["names_match"] and tool_check["schemas_match"], (
    tool_check
)

marker = "tank-platform-deploy-proof-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
session = client.post(
    "sessions",
    json={
        "agent": proposed["agent_id"],
        "environment_id": proposed["environment_id"],
        "metadata": {"tank_identity": marker},
    },
)
session.raise_for_status()
session = session.json()
sid = session["id"]
save("session-created.json", session)
READ_SKILLS = os.environ.get("TANK_PROOF_READ_SKILLS") == "1"
prompt = (
    "Deployment check by the Tank operator, not a news item and not an event. Do exactly this: "
    '1) call the tool get_registry once with entity_or_token set to "ETH"; '
    + (
        "2) then read the SKILL.md files of your tank-event-analyst and tank-data skills at the paths listed in your "
        "instructions (only those files), and answer in one line from them: "
        if READ_SKILLS
        else "2) then, without reading any file or calling any other tool, answer in one line from the skill text already "
        "in your instructions: "
    )
    + "(a) which claim rule number your tank-event-analyst skill says is not defined, "
    + "(b) which reference file your tank-data skill says to read before queries, "
    + "(c) how many entries the tool returned under tokens. Do not record anything."
)
posted = client.post(
    f"sessions/{sid}/events",
    json={"events": [{"type": "user.message", "content": [{"type": "text", "text": prompt}]}]},
)
posted.raise_for_status()
tools = EventToolService(None, Settings(config_dir=config_dir), None, None)
seen, events, answered, deadline = set(), [], [], time.monotonic() + 900
finished = None
while time.monotonic() < deadline and not finished:
    page = None
    while True:
        params = {"limit": 100, "order": "asc", **({"page": page} if page else {})}
        data = client.get(f"sessions/{sid}/events", params=params).json()
        for event in data.get("data", []):
            if event["id"] in seen:
                continue
            seen.add(event["id"])
            events.append(event)
            kind = event.get("type")
            if kind == "agent.custom_tool_use":
                import asyncio

                result = asyncio.run(getattr(tools, event["name"])(event.get("input") or {}, {}))
                if event["name"] != "get_registry":
                    result = {"error": "deployment check permits only get_registry"}
                body = {
                    "type": "user.custom_tool_result",
                    "custom_tool_use_id": event["id"],
                    "content": [
                        {
                            "type": "text",
                            "text": canonical_json(
                                tool_wire_result(event["name"], result, session_id=sid, call_id=event["id"])
                            ),
                        }
                    ],
                }
                if event["name"] != "get_registry":
                    body["is_error"] = True
                client.post(f"sessions/{sid}/events", json={"events": [body]}).raise_for_status()
                answered.append(
                    {
                        "tool": event["name"],
                        "input": event.get("input"),
                        "tokens": len(result.get("tokens", [])),
                    }
                )
            if kind == "session.status_idle" and (event.get("stop_reason") or {}).get("type") == "end_turn":
                finished = event
            if kind in {"session.error", "session.status_terminated", "execution.status_failed"}:
                finished = event
        page = data.get("next_page")
        if not page:
            break
    time.sleep(3)
save("session-events.json", events)
messages = [
    b.get("text", "") for e in events if e.get("type") == "agent.message" for b in e.get("content", [])
]
summary = {
    "session_id": sid,
    "marker": marker,
    "tool_check": tool_check,
    "skills": [
        {"name": s["skill"].get("name") or s["skill"].get("display_title"), **s["binding"]} for s in skills
    ],
    "tool_calls_answered": answered,
    "final_event": finished,
    "agent_text": messages,
    "analyst_skill_applied": any("V6" in m for m in messages),
    "data_skill_applied": any("DATA_GUIDE.md" in m for m in messages),
    "other_tools_used": sorted(
        {e.get("name") for e in events if e.get("type") in {"agent.tool_use", "agent.mcp_tool_use"}} - {None}
    ),
    "read_skills_mode": READ_SKILLS,
    "first_request_input_tokens": next(
        (
            e.get("model_usage", {}).get("input_tokens")
            for e in events
            if e.get("type") == "span.model_request_end"
        ),
        None,
    ),
    "builtin_tool_inputs": [e.get("input") for e in events if e.get("type") == "agent.tool_use"],
    "real_tool_call": any(a["tool"] == "get_registry" for a in answered),
}
summary["status"] = (
    "PASS"
    if summary["analyst_skill_applied"]
    and summary["data_skill_applied"]
    and summary["real_tool_call"]
    and (READ_SKILLS or not summary["other_tools_used"])
    and finished
    and (READ_SKILLS or (summary["first_request_input_tokens"] or 0) >= 15000)
    and finished.get("type") == "session.status_idle"
    else "FAIL"
)
save("result.json", summary)
print(
    json.dumps(
        {
            k: summary[k]
            for k in (
                "status",
                "session_id",
                "analyst_skill_applied",
                "data_skill_applied",
                "real_tool_call",
                "other_tools_used",
                "first_request_input_tokens",
                "agent_text",
            )
        }
    )
)
