"""Independent gateway regressions. Synthetic auth; MockTransport; loopback only."""

import asyncio
import base64
import json
import shutil
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from tank.codex_gateway import RESPONSES_URL, TOKEN_URL, CodexGateway


def fake_jwt(exp=4102444800):
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).decode().rstrip("=")
    return f"AUDIT.{payload}.FAKE_SIGNATURE"


@asynccontextmanager
async def gateway(tmp_path, handler, *, expired=False):
    path = tmp_path / "synthetic-auth.json"
    original = {
        "auth_mode": "chatgpt",
        "tokens": {
            "access_token": fake_jwt(1 if expired else 4102444800),
            "refresh_token": "AUDIT_REFRESH_OLD",
            "account_id": "AUDIT_ACCOUNT",
        },
    }
    path.write_text(json.dumps(original))
    settings = SimpleNamespace(
        codex_auth_path=path,
        codex_gateway_key="AUDIT_INTERNAL",
        codex_gateway_host="127.0.0.1",
        codex_gateway_port=0,
    )
    service = CodexGateway(settings, upstream_transport=httpx.MockTransport(handler))
    await service.start()
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{service.bound_port}",
            trust_env=False,
            headers={"Authorization": "Bearer AUDIT_INTERNAL"},
        ) as client:
            yield service, client, path, original
    finally:
        await service.close()


PAYLOAD = {"model": "gpt-6.1-sol", "input": "audit", "stream": True}


@pytest.mark.integration
@pytest.mark.parametrize("authorization", [b"Bearer WRONG", b"Bearer \xff"])
async def test_auth_rejected_before_unfinished_request_body(tmp_path, authorization):
    def forbidden(request):
        pytest.fail("unauthorized call touched provider")

    async with gateway(tmp_path, forbidden) as (service, _, path, _):
        path.unlink()
        reader, writer = await asyncio.open_connection("127.0.0.1", service.bound_port)
        try:
            writer.write(
                b"POST /v1/responses HTTP/1.1\r\nHost: localhost\r\nContent-Length: 999999\r\n"
                b"Authorization: " + authorization + b"\r\n\r\n"
            )
            await writer.drain()
            headers = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 1)
            assert headers.startswith(b"HTTP/1.1 401")
        finally:
            writer.close()
            await writer.wait_closed()


@pytest.mark.integration
async def test_concurrent_stale_401_does_not_rotate_new_refresh_again(tmp_path):
    old = fake_jwt()
    new = fake_jwt(4102444801)
    old_requests = 0
    refreshes = 0
    all_old = asyncio.Event()
    refreshed = asyncio.Event()

    async def provider(request):
        nonlocal old_requests, refreshes
        if str(request.url) == TOKEN_URL:
            refreshes += 1
            refreshed.set()
            return httpx.Response(200, json={"access_token": new, "refresh_token": "AUDIT_REFRESH_NEW"})
        assert str(request.url) == RESPONSES_URL
        if request.headers["authorization"] == "Bearer " + old:
            old_requests += 1
            ordinal = old_requests
            if old_requests == 8:
                all_old.set()
            await all_old.wait()
            if ordinal != 8:
                await refreshed.wait()
            return httpx.Response(401)
        assert request.headers["authorization"] == "Bearer " + new
        return httpx.Response(200, json={"id": "audit"})

    async with gateway(tmp_path, provider) as (_, client, _, _):
        replies = await asyncio.wait_for(
            asyncio.gather(*(client.post("/v1/responses", json=PAYLOAD) for _ in range(8))), 3
        )
        assert all(reply.status_code == 200 for reply in replies)
        assert refreshes == 1


@pytest.mark.integration
async def test_cancelled_refresh_caller_still_persists_provider_rotation(tmp_path):
    exchange_started, exchange_finish = asyncio.Event(), asyncio.Event()

    async def provider(request):
        assert str(request.url) == TOKEN_URL
        exchange_started.set()
        await exchange_finish.wait()
        return httpx.Response(
            200, json={"access_token": fake_jwt(4102444801), "refresh_token": "AUDIT_REFRESH_ROTATED"}
        )

    async with gateway(tmp_path, provider, expired=True) as (service, _, path, _):
        caller = asyncio.create_task(service._credentials())
        await asyncio.wait_for(exchange_started.wait(), 1)
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        exchange_finish.set()
        # close is the generation-drain boundary and must await persistence.
        await asyncio.wait_for(service.close(), 1)
        assert json.loads(path.read_text())["tokens"]["refresh_token"] == "AUDIT_REFRESH_ROTATED"


@pytest.mark.integration
@pytest.mark.parametrize("expiry", ["bad", True, -1, 1, float("nan"), float("inf")])
async def test_bad_refresh_expiry_cannot_overwrite_valid_auth_document(tmp_path, expiry):
    def provider(request):
        assert str(request.url) == TOKEN_URL
        return httpx.Response(200, json={"access_token": fake_jwt(expiry), "refresh_token": "AUDIT_BAD_NEW"})

    async with gateway(tmp_path, provider, expired=True) as (_, client, path, original):
        reply = await client.post("/v1/responses", json=PAYLOAD)
        assert reply.status_code == 503
        assert json.loads(path.read_text()) == original


@pytest.mark.integration
async def test_refresh_redirect_does_not_forward_credentials_or_location(tmp_path):
    calls = []

    def provider(request):
        calls.append(request)
        return httpx.Response(
            307, headers={"location": "https://attacker.invalid/", "set-cookie": "AUDIT_COOKIE"}
        )

    async with gateway(tmp_path, provider, expired=True) as (_, client, path, original):
        reply = await client.post("/v1/responses", json=PAYLOAD)
        assert reply.status_code == 503 and len(calls) == 1
        assert str(calls[0].url) == TOKEN_URL
        assert "location" not in reply.headers and "set-cookie" not in reply.headers
        assert json.loads(path.read_text()) == original


@pytest.mark.integration
async def test_provider_cookie_jar_and_hop_headers_never_cross_boundary(tmp_path, monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://FAKE_PROXY_CREDENTIAL:FAKE_PASSWORD@127.0.0.1:1")

    def provider(request):
        assert "cookie" not in request.headers and "proxy-authorization" not in request.headers
        return httpx.Response(
            429,
            json={"error": {"code": "usage_limit_reached", "resets_at": 1800000000}},
            headers={
                "set-cookie": "AUDIT_COOKIE",
                "connection": "x-audit-hop",
                "x-audit-hop": "secret",
                "retry-after": "120",
                "x-codex-secondary-reset-at": "1800000000",
            },
        )

    async with gateway(tmp_path, provider) as (service, client, _, _):
        assert service._client.trust_env is False and service._client.follow_redirects is False
        service._client.cookies.set("AUDIT_OLD_COOKIE", "AUDIT_SECRET", domain="chatgpt.com")
        reply = await client.post(
            "/v1/responses",
            json=PAYLOAD,
            headers={"Cookie": "AUDIT_INCOMING_COOKIE", "Proxy-Authorization": "AUDIT_PROXY"},
        )
        assert reply.status_code == 429
        assert reply.headers["retry-after"] == "120"
        assert reply.headers["x-codex-secondary-reset-at"] == "1800000000"
        assert "set-cookie" not in reply.headers and "x-audit-hop" not in reply.headers


@pytest.mark.integration
@pytest.mark.parametrize("data", [b"123", b'"AUDIT_REFRESH_OLD"', b"{", b"\xff"])
async def test_malformed_nonstream_sse_fails_without_credentials_in_logs(tmp_path, caplog, data):
    def provider(request):
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, content=b"data: " + data + b"\n\n"
        )

    async with gateway(tmp_path, provider) as (_, client, _, _):
        reply = await client.post("/v1/responses", json={**PAYLOAD, "stream": False})
        assert reply.status_code == 502
        assert "AUDIT_REFRESH_OLD" not in reply.text + caplog.text


@pytest.mark.integration
async def test_upstream_close_exception_cannot_print_oauth_credentials(tmp_path, caplog):
    class BrokenClose(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'event: response.completed\ndata: {"type":"response.completed","response":{}}\n\n'

        async def aclose(self):
            raise RuntimeError("AUDIT_REFRESH_OLD provider close failed")

    def provider(request):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=BrokenClose())

    async with gateway(tmp_path, provider) as (_, client, _, _):
        reply = await client.post("/v1/responses", json={**PAYLOAD, "stream": False})
        await asyncio.sleep(0)
        assert "AUDIT_REFRESH_OLD" not in reply.text + caplog.text


@pytest.mark.integration
async def test_supervisor_reload_rebinds_actual_gateway_and_shutdown_closes_port(tmp_path):
    from tank.runtime import RuntimeSupervisor

    auth = tmp_path / "synthetic-auth.json"
    auth.write_text(
        json.dumps(
            {"auth_mode": "chatgpt", "tokens": {"access_token": fake_jwt(), "account_id": "AUDIT_ACCOUNT"}}
        )
    )
    first = SimpleNamespace(
        codex_auth_path=auth,
        codex_gateway_key="AUDIT_OLD_INTERNAL",
        codex_gateway_host="127.0.0.1",
        codex_gateway_port=0,
        config_version="old",
    )
    generations = []

    def forbidden(request):
        pytest.fail("lifecycle preflight/start reached external provider")

    class Generation:
        def __init__(self, repo, settings):
            self.gateway = CodexGateway(settings, upstream_transport=httpx.MockTransport(forbidden))
            generations.append(self)

        def validate(self):
            pass

        async def prepare(self):
            await self.gateway.preflight()

        async def run(self, stop):
            await self.gateway.run(stop)

        async def close(self):
            await self.gateway.close()

    candidate = None
    supervisor = RuntimeSupervisor(
        None, first, factory=Generation, validator=lambda settings: None, candidate_loader=lambda: candidate
    )
    stop, reload_requested = asyncio.Event(), asyncio.Event()
    running = asyncio.create_task(supervisor.run(stop, reload_requested))
    try:
        while not generations:
            await asyncio.sleep(0)
        await asyncio.wait_for(generations[0].gateway.ready.wait(), 1)
        port = generations[0].gateway.bound_port
        candidate = SimpleNamespace(**{**vars(first), "codex_gateway_key": "", "config_version": "invalid"})
        reload_requested.set()

        async def rejected_candidate_prepared():
            while len(generations) != 2:
                await asyncio.sleep(0)

        await asyncio.wait_for(rejected_candidate_prepared(), 1)
        assert generations[0].gateway.bound_port == port
        assert generations[1].gateway.bound_port is None
        candidate = SimpleNamespace(
            **{
                **vars(first),
                "codex_gateway_port": port,
                "codex_gateway_key": "AUDIT_NEW_INTERNAL",
                "config_version": "new",
            }
        )
        reload_requested.set()

        async def replacement_ready():
            while len(generations) != 3:
                await asyncio.sleep(0)
            await generations[2].gateway.ready.wait()

        await asyncio.wait_for(replacement_ready(), 1)
        assert generations[0].gateway.bound_port is None
        assert generations[2].gateway.bound_port == port
        async with httpx.AsyncClient(trust_env=False) as client:
            reply = await client.post(
                f"http://127.0.0.1:{port}/v1/responses",
                json=PAYLOAD,
                headers={"Authorization": "Bearer AUDIT_OLD_INTERNAL"},
            )
            assert reply.status_code == 401
    finally:
        stop.set()
        await asyncio.wait_for(running, 1)
    with pytest.raises(OSError):
        await asyncio.open_connection("127.0.0.1", port)


@pytest.mark.integration
@pytest.mark.node
@pytest.mark.process
async def test_actual_pinned_pi_library_through_gateway_offline(tmp_path):
    """Locked pi-ai1.1.0 transport against the synthetic, loopback-only gateway."""
    package = Path(__file__).resolve().parent / "node-gateway/node_modules/@earendil-works/pi-ai"
    assert (package / "package.json").is_file(), "Run npm ci --prefix tests/node-gateway --ignore-scripts"
    node = shutil.which("node")
    assert node is not None, "The actual pi transport integration requires Node.js22"
    assert json.loads((package / "package.json").read_text())["version"] == "1.1.0"
    observed = []

    def provider(request):
        assert str(request.url) == RESPONSES_URL
        assert request.headers["authorization"] == "Bearer " + fake_jwt()
        assert request.headers["chatgpt-account-id"] == "AUDIT_ACCOUNT"
        payload = json.loads(request.content)
        observed.append(payload)
        assert payload["instructions"] == "AUDIT_SYSTEM"
        assert payload["reasoning"]["effort"] == "medium"
        assert payload["tools"][0]["name"] == "audit_tool"
        assert payload["include"] == ["reasoning.encrypted_content"]
        replay = {item.get("type"): item for item in payload["input"]}
        assert replay["reasoning"]["encrypted_content"] == "AUDIT_ENCRYPTED_REASONING"
        assert replay["function_call"]["name"] == "audit_tool"
        assert replay["function_call"]["call_id"] == replay["function_call_output"]["call_id"]
        assert replay["function_call_output"]["output"] == "AUDIT_TOOL_RESULT"
        assert not {"max_output_tokens", "temperature", "prompt_cache_retention"} & payload.keys()
        assert payload["stream"] is True and payload["store"] is False
        terminal = {
            "type": "response.completed",
            "response": {
                "id": "resp_AUDIT",
                "status": "completed",
                "output": [],
                "usage": {
                    "input_tokens": 3,
                    "output_tokens": 0,
                    "total_tokens": 3,
                    "input_tokens_details": {"cached_tokens": 0},
                    "output_tokens_details": {"reasoning_tokens": 0},
                },
            },
        }
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=("event: response.completed\ndata: " + json.dumps(terminal) + "\n\n").encode(),
        )

    async with gateway(tmp_path, provider) as (service, _, _, _):
        base = f"http://127.0.0.1:{service.bound_port}/v1"
        code = """
const {stream} = await import(process.argv[2]+'/dist/api/openai-responses.js');
const {normalizeContext} = await import(process.argv[2]+'/dist/utils/transcript.js');
const base = process.argv[3];
const model = {id:'gpt-6.1-sol',name:'Tank',api:'openai-responses',provider:'tank-codex',baseUrl:base,
 reasoning:true,input:['text'],contextWindow:200000,maxTokens:32000,
 cost:{input:0,output:0,cacheRead:0,cacheWrite:0},compat:{supportsMaxOutputTokens:false}};
const context = normalizeContext({systemPrompt:'AUDIT_SYSTEM',messages:[{role:'user',content:'AUDIT_USER',timestamp:0},
 {role:'assistant',api:'openai-responses',provider:'tank-codex',model:'gpt-6.1-sol',timestamp:1,stopReason:'toolUse',
  content:[{type:'thinking',thinking:'',thinkingSignature:JSON.stringify({type:'reasoning',id:'rs_AUDIT',
    encrypted_content:'AUDIT_ENCRYPTED_REASONING',summary:[]})},
   {type:'toolCall',id:'call_AUDIT|fc_AUDIT',name:'audit_tool',arguments:{}}]},
 {role:'toolResult',toolCallId:'call_AUDIT|fc_AUDIT',toolName:'audit_tool',content:[{type:'text',text:'AUDIT_TOOL_RESULT'}],
  isError:false,timestamp:2}],
 tools:[{name:'audit_tool',description:'Synthetic only',parameters:{type:'object',properties:{},required:[]}}]});
let calls=0;const types=[];
const result=stream(model,context,{apiKey:'AUDIT_INTERNAL',reasoningEffort:'medium',cacheRetention:'long',
 sessionId:'AUDIT_SESSION',fetch:async(url,init)=>{
 if(String(url)!==base+'/responses')throw Error('External request forbidden');
 if(new Headers(init.headers).get('authorization')!=='Bearer AUDIT_INTERNAL')throw Error('Wrong internal key');
 const body=JSON.parse(init.body);if(body.input[0].role!=='developer')throw Error('Missing developer instruction');
 if(body.prompt_cache_retention!=='24h')throw Error('Missing platform field for gateway translation');
 calls++;return fetch(url,init);
}});
for await(const event of result)types.push(event.type);
if(calls!==1||!types.includes('done'))throw Error('Incomplete actual pi transport: '+JSON.stringify(types));
console.log(JSON.stringify({status:'PASS',calls,event_types:types}));
"""
        script = tmp_path / "actual-pi-probe.mjs"
        script.write_text(code)
        process = await asyncio.create_subprocess_exec(
            node,
            str(script),
            str(package),
            base,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), 10)
        assert process.returncode == 0, stderr.decode()
        assert json.loads(stdout)["status"] == "PASS" and len(observed) == 1
