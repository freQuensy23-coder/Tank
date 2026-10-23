"""Execute exact pinned codec excerpts with Node's native TS stripping, offline."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.node, pytest.mark.process]

ROOT = Path(__file__).resolve().parents[1]


def run_codec(tmp_path, patched):
    source = (ROOT / "tests/fixtures/openma/runtime-codec.ts").read_text()
    if patched:
        for patch in json.loads((ROOT / "tests/fixtures/openma/opaque-input-fixes.json").read_text()):
            assert source.count(patch["old"]) == patch["count"]
            source = source.replace(patch["old"], patch["new"])
    source += """
const opaque = {item_id:"real-item", itemId:"distinct-key", HTTP_ID:"CAPITAL", entity_or_token:"COW", camelCaseUserKey: 1, nested_array:[{token_id:"cow:COW", userMixedKey:true, UPPER_CASE: {inner_key:1}}]};
const raw = {id:"call_1",type:"agent.custom_tool_use",processed_at:"2026-10-18T00:00:00Z",name:"get_item",input:opaque,metadata:opaque,arguments:opaque,input_schema:{type:"object",properties:{item_id:{type:"string"},itemId:{type:"number"},UPPER_CASE:{type:"string"}},required:["item_id"],additionalProperties:false},provider_options:{ProviderID:opaque}};
const decoded=decodeRuntimeEvent(raw,new Set())[0];
const encoded=encodeRuntimeHistoryEvent({...decoded,input:opaque});
const customResult={id:"result_1",type:"user.custom_tool_result",customToolUseId:"call_1",isError:false,content:[{type:"text",text:JSON.stringify(opaque)}],metadata:opaque};
const resultHistory=encodeRuntimeHistoryEvent(customResult);
const resultDecoded=decodeRuntimeEvent(resultHistory,new Set())[0];
const realTool=decodeRuntimeEvent({...raw,input:{item_id:"real-item"}},new Set())[0];
console.log(JSON.stringify({opaque,raw,decoded,encoded,resultHistory,resultDecoded,realTool}));
"""
    path = tmp_path / "codec.ts"
    path.write_text(source)
    node = shutil.which("node")
    if not node:
        pytest.skip("Node required for actual pinned TypeScript codec execution")
    result = subprocess.run(
        [node, "--experimental-strip-types", str(path)], check=True, text=True, capture_output=True
    )
    return json.loads(result.stdout)


def test_guarded_codec_preserves_opaque_input_but_transforms_envelope(tmp_path):
    result = run_codec(tmp_path, True)
    assert result["decoded"]["input"] == result["opaque"] == result["encoded"]["input"]
    assert result["decoded"]["processedAt"] == "2026-10-18T00:00:00Z"
    assert result["encoded"]["processed_at"] == "2026-10-18T00:00:00Z"
    for key in ("metadata", "arguments"):
        assert result["decoded"][key] == result["opaque"] == result["encoded"][key]
    assert (
        result["decoded"]["inputSchema"] == result["raw"]["input_schema"] == result["encoded"]["input_schema"]
    )
    assert result["decoded"]["providerOptions"] == result["raw"]["provider_options"]
    assert result["resultDecoded"]["metadata"] == result["opaque"]
    assert result["resultHistory"]["custom_tool_use_id"] == "call_1"
    assert result["resultHistory"]["content"][0]["text"] == json.dumps(
        result["opaque"], separators=(",", ":")
    )


async def test_patched_actual_tool_uses_host_bound_item(tmp_path):
    import httpx

    from tank.agents.sessions import SessionService
    from tank.event_tools import EventToolService
    from tank.settings import Settings
    from tank.storage import MemoryRepository

    result = run_codec(tmp_path, True)
    repo = MemoryRepository()
    await repo.put(
        "ingest.source_items", "real-item", {"item_id": "real-item", "text": "actual fixture evidence"}
    )
    session = {
        "session_id": "s",
        "item_id": "real-item",
        "config_version": "config",
        "code_version": "code",
        "model_id": "gpt-6.1-sol",
        "prompt_version": "test",
        "agent_version": "test",
    }
    await repo.put("ops.agent_sessions", "s", session)
    opts = Settings(_env_file=None)
    tools = EventToolService(repo, opts, None, None)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={}))
    ) as http:
        service = SessionService(repo, opts, tools.dispatch, http=http)
        await service.tool(session, {**result["realTool"], "input": {}})
    ledger = await repo.get("ops.tool_calls", "s:call_1")
    assert ledger["status"] == "DONE" and ledger["returned_at"]
    assert ledger["model_arguments"] == {}
    assert ledger["arguments"] == {"item_id": "real-item"}
    assert ledger["result"]["item_id"] == "real-item"
