"""Every credential and provider response in this file is synthetic and offline."""

import asyncio
import base64
import json
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest

from tank.codex_gateway import CLIENT_ID, RESPONSES_URL, TOKEN_URL, CodexGateway, GatewayError


def jwt(exp=4102444800, **extra):
    claims = {"exp": exp, **extra}
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"FAKE.{payload}.FAKE_SIGNATURE"


def auth_document(access=None, refresh="FAKE-refresh"):
    return {
        "auth_mode": "chatgpt",
        "OPENAI_API_KEY": None,
        "operator_extra": {"keep": True},
        "tokens": {
            "access_token": access or jwt(),
            "refresh_token": refresh,
            "id_token": jwt(),
            "account_id": "FAKE-account",
            "extra": "preserve",
        },
        "last_refresh": "2026-10-17T00:00:00Z",
    }


@pytest.fixture
def gateway_factory(tmp_path):
    @asynccontextmanager
    async def factory(handler, document=None):
        path = tmp_path / "FAKE-auth.json"
        path.write_text(json.dumps(document or auth_document()))
        settings = SimpleNamespace(
            codex_gateway_key="FAKE-internal",
            codex_auth_path=path,
            codex_gateway_host="127.0.0.1",
            codex_gateway_port=0,
        )
        gateway = CodexGateway(settings, upstream_transport=httpx.MockTransport(handler))
        await gateway.start()
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{gateway.bound_port}",
            trust_env=False,
            headers={"Authorization": "Bearer FAKE-internal"},
        ) as client:
            try:
                yield gateway, client, path
            finally:
                await gateway.close()

    return factory


def body(**extra):
    return {"model": "gpt-6.1-sol", "input": [{"role": "user", "content": "test"}], "stream": True, **extra}


async def test_internal_key_required_before_auth_or_provider(gateway_factory):
    def forbidden(request):
        pytest.fail("unauthorized request reached provider")

    async with gateway_factory(forbidden) as (_, client, path):
        path.unlink()  # Unauthorized requests must not even try to read OAuth.
        for supplied in (None, "wrong", "Bearer FAKE-interna", "Bearer FAKE-internal-long"):
            response = await client.post(
                "/v1/responses", json=body(), headers={"Authorization": supplied or ""}
            )
            assert response.status_code == 401
            assert response.json()["error"]["code"] == "unauthorized"


async def test_signing_translation_and_header_isolation(gateway_factory):
    seen = []

    def provider(request):
        seen.append(request)
        assert str(request.url) == RESPONSES_URL
        assert request.headers["authorization"] == "Bearer " + jwt()
        assert request.headers["chatgpt-account-id"] == "FAKE-account"
        assert request.headers["originator"] == "codex_cli_rs"
        assert "cookie" not in request.headers and "x-untrusted" not in request.headers
        payload = json.loads(request.content)
        assert payload["instructions"] == "existing\n\nsystem\n\ndeveloper"
        assert payload["input"] == [{"role": "user", "content": "test"}]
        assert payload["store"] is False and payload["stream"] is True
        assert "max_output_tokens" not in payload and "temperature" not in payload
        return httpx.Response(
            200,
            json={"id": "resp_FAKE"},
            headers={
                "set-cookie": "secret=value",
                "Authorization": "Bearer FAKE-provider-secret",
                "x-untrusted": "no",
            },
        )

    async with gateway_factory(provider) as (_, client, _):
        payload = body(instructions="existing", max_output_tokens=100, temperature=0.2, store=True)
        payload["input"][:0] = [
            {"role": "system", "content": "system"},
            {"role": "developer", "content": [{"type": "input_text", "text": "developer"}]},
        ]
        response = await client.post(
            "/v1/responses", json=payload, headers={"Cookie": "FAKE-client-cookie", "x-untrusted": "no"}
        )
        assert response.json() == {"id": "resp_FAKE"}
        assert "set-cookie" not in response.headers and "authorization" not in response.headers
        assert "x-untrusted" not in response.headers
        assert len(seen) == 1


@pytest.mark.parametrize("status", [400, 403, 429, 500, 503])
async def test_provider_errors_and_reset_headers_preserved(gateway_factory, status):
    error = {"error": {"code": "usage_limit_reached", "message": "Try later", "resets_at": 1800000000}}

    def provider(request):
        return httpx.Response(
            status,
            json=error,
            headers={
                "Retry-After": "120",
                "x-codex-primary-reset-at": "1800000000",
                "x-codex-primary-used-percent": "100",
                "x-ratelimit-reset-tokens": "120s",
                "x-request-id": "req_fake",
            },
        )

    async with gateway_factory(provider) as (_, client, _):
        response = await client.post("/v1/responses", json=body())
        assert response.status_code == status
        assert response.json() == error
        assert response.headers["retry-after"] == "120"
        assert response.headers["x-codex-primary-reset-at"] == "1800000000"
        assert response.headers["x-ratelimit-reset-tokens"] == "120s"


async def test_expired_refresh_singleflight_atomic_preservation_and_no_cookie(gateway_factory):
    calls = []
    new = jwt(exp=4102444801)

    async def provider(request):
        calls.append(request)
        if str(request.url) == TOKEN_URL:
            payload = json.loads(request.content)
            assert payload == {
                "grant_type": "refresh_token",
                "client_id": CLIENT_ID,
                "refresh_token": "FAKE-refresh",
                "refresh_reason": "proactive_expiry",
            }
            assert "authorization" not in request.headers and "cookie" not in request.headers
            await asyncio.sleep(0.02)
            return httpx.Response(
                200,
                json={"access_token": new, "refresh_token": "FAKE-rotated", "id_token": jwt()},
                headers={"set-cookie": "oauthsecret=never-forward"},
            )
        assert request.headers["authorization"] == "Bearer " + new
        assert "cookie" not in request.headers
        return httpx.Response(200, json={"id": "resp_FAKE"})

    async with gateway_factory(provider, auth_document(jwt(exp=1))) as (_, client, path):
        responses = await asyncio.gather(*(client.post("/v1/responses", json=body()) for _ in range(20)))
        assert all(response.status_code == 200 for response in responses)
        assert sum(str(request.url) == TOKEN_URL for request in calls) == 1
        stored = json.loads(path.read_text())
        assert stored["operator_extra"] == {"keep": True} and stored["tokens"]["extra"] == "preserve"
        assert stored["tokens"]["refresh_token"] == "FAKE-rotated"
        assert stored["tokens"]["access_token"] == new
        assert stored["last_refresh"] != "2026-10-17T00:00:00Z"
        assert path.stat().st_mode & 0o777 == 0o600
        assert list(path.parent.glob(".auth-*")) == []


async def test_401_refresh_once_then_forward_provider_auth_failure(gateway_factory):
    calls = []

    def provider(request):
        calls.append(str(request.url))
        if str(request.url) == TOKEN_URL:
            assert json.loads(request.content)["refresh_reason"] == "unauthorized_recovery"
            return httpx.Response(200, json={"access_token": jwt(exp=4102444801)})
        return httpx.Response(401, json={"error": {"code": "unauthorized"}})

    async with gateway_factory(provider) as (_, client, path):
        response = await client.post("/v1/responses", json=body())
        assert response.status_code == 401
        assert calls == [RESPONSES_URL, TOKEN_URL, RESPONSES_URL]
        assert json.loads(path.read_text())["tokens"]["refresh_token"] == "FAKE-refresh"


async def test_external_auth_replacement_during_refresh_wins(gateway_factory):
    path_holder = []
    replacement = auth_document(jwt(exp=4102444802), refresh="FAKE-external-refresh")

    async def provider(request):
        if str(request.url) == TOKEN_URL:
            path_holder[0].write_text(json.dumps(replacement))
            return httpx.Response(
                200, json={"access_token": jwt(exp=4102444801), "refresh_token": "FAKE-discard"}
            )
        assert request.headers["authorization"] == "Bearer " + replacement["tokens"]["access_token"]
        return httpx.Response(200, json={"id": "resp_FAKE"})

    async with gateway_factory(provider, auth_document(jwt(exp=1))) as (_, client, path):
        path_holder.append(path)
        assert (await client.post("/v1/responses", json=body())).status_code == 200
        assert json.loads(path.read_text()) == replacement


async def test_reload_new_auth_without_refresh(gateway_factory):
    wanted = []

    def provider(request):
        assert str(request.url) == RESPONSES_URL
        assert request.headers["authorization"] == "Bearer " + wanted[0]
        return httpx.Response(200, json={"id": "resp_FAKE"})

    async with gateway_factory(provider) as (_, client, path):
        wanted.append(jwt(exp=4102444802))
        path.write_text(json.dumps(auth_document(wanted[0])))
        assert (await client.post("/v1/responses", json=body())).status_code == 200


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(400, json={"error": "FAKE-refresh"}),
        httpx.Response(401),
        httpx.Response(500, text="FAKE-refresh"),
        httpx.Response(200, text="invalid"),
        httpx.Response(200, json={"access_token": "malformed"}),
        httpx.Response(200, json={"refresh_token": "FAKE-new"}),
    ],
)
async def test_refresh_failures_static_safe_and_auth_unmodified(gateway_factory, response):
    calls = []

    async def provider(request):
        calls.append(request)
        await asyncio.sleep(0.02)
        return response

    document = auth_document(jwt(exp=1))
    async with gateway_factory(provider, document) as (_, client, path):
        replies = await asyncio.gather(*(client.post("/v1/responses", json=body()) for _ in range(8)))
        assert all(reply.status_code == 503 for reply in replies)
        assert all("FAKE-refresh" not in reply.text and "malformed" not in reply.text for reply in replies)
        assert json.loads(path.read_text()) == document
        assert len(calls) == 1


@pytest.mark.parametrize("access,refresh", [("malformed", "FAKE-refresh"), (jwt(exp=1), None)])
async def test_preflight_rejects_invalid_or_unrefreshable_auth_offline(tmp_path, access, refresh):
    path = tmp_path / "FAKE-auth.json"
    path.write_text(json.dumps(auth_document(access, refresh)))
    gateway = CodexGateway(SimpleNamespace(codex_gateway_key="FAKE-internal", codex_auth_path=path))
    with pytest.raises(GatewayError):
        await gateway.preflight()
    assert gateway._client is None and gateway._runner is None


async def test_no_refresh_token_valid_access_but_401_requires_login(gateway_factory):
    calls = []

    def provider(request):
        calls.append(request)
        return httpx.Response(401)

    async with gateway_factory(provider, auth_document(refresh=None)) as (_, client, _):
        response = await client.post("/v1/responses", json=body())
        assert response.status_code == 503 and response.json()["error"]["code"] == "codex_login_required"
        assert len(calls) == 1


async def test_atomic_write_failure_keeps_old_auth(gateway_factory, monkeypatch):
    document = auth_document(jwt(exp=1))

    def provider(request):
        return httpx.Response(200, json={"access_token": jwt(exp=4102444801), "refresh_token": "FAKE-new"})

    async with gateway_factory(provider, document) as (_, client, path):

        def failure(*args):
            raise OSError("FAKE-refresh secret must not reach error")

        monkeypatch.setattr(os, "replace", failure)
        response = await client.post("/v1/responses", json=body())
        assert response.json()["error"]["code"] == "codex_auth_write_failed"
        assert "FAKE-refresh" not in response.text
        assert json.loads(path.read_text()) == document
        assert not list(path.parent.glob(".auth-*"))


@pytest.mark.parametrize("location", ["https://evil.invalid/steal", "https://auth.openai.com/oauth/token"])
async def test_provider_redirect_never_followed_or_exposed(gateway_factory, location):
    calls = []

    def provider(request):
        calls.append(request)
        return httpx.Response(307, headers={"location": location, "set-cookie": "FAKE-secret"})

    async with gateway_factory(provider) as (_, client, _):
        response = await client.post("/v1/responses", json=body())
        assert response.status_code == 502 and response.json()["error"]["code"] == "codex_redirect_refused"
        assert len(calls) == 1 and "location" not in response.headers


class Chunks(httpx.AsyncByteStream):
    def __init__(self, chunks, failure=False):
        self.chunks, self.failure, self.closed = chunks, failure, False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk
        if self.failure:
            raise httpx.ReadError("FAKE-refresh upstream exception")

    async def aclose(self):
        self.closed = True


async def test_sse_failure_and_usage_reset_preserved(gateway_factory):
    frame = b'event: response.failed\ndata: {"type":"response.failed","response":{"error":{"code":"usage_limit_reached","resets_at":1800000000}}}\n\n'
    stream = Chunks([frame[:39], frame[39:]])

    def provider(request):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream)

    async with gateway_factory(provider) as (_, client, _):
        response = await client.post("/v1/responses", json=body())
        assert response.content == frame
        assert stream.closed


async def test_sse_redacts_secrets_split_at_every_byte_and_rotated_tokens(gateway_factory):
    new = jwt(exp=4102444801)

    def provider(request):
        if str(request.url) == TOKEN_URL:
            return httpx.Response(200, json={"access_token": new, "refresh_token": "FAKE-rotated-secret"})
        frame = (
            "event: error\ndata: " + json.dumps({"error": "FAKE-refresh FAKE-rotated-secret " + new}) + "\n\n"
        ).encode()
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=Chunks([bytes([part]) for part in frame]),
        )

    async with gateway_factory(provider, auth_document(jwt(exp=1))) as (_, client, _):
        response = await client.post("/v1/responses", json=body())
        assert "FAKE-refresh" not in response.text and "FAKE-rotated-secret" not in response.text
        assert new not in response.text and response.text.count("[REDACTED]") == 3


async def test_stream_interruption_visible_and_closed(gateway_factory):
    stream = Chunks([b'event: response.created\ndata: {"type":"response.created"}\n\n'], failure=True)

    def provider(request):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream)

    async with gateway_factory(provider) as (_, client, _):
        response = await client.post("/v1/responses", json=body())
        assert "codex_stream_interrupted" in response.text and "FAKE-refresh" not in response.text
        assert stream.closed


async def test_stream_is_incremental_and_downstream_disconnect_closes_upstream(gateway_factory):
    released = asyncio.Event()
    closed = asyncio.Event()
    frame = b'event: response.created\ndata: {"type":"response.created"}\n\n'

    class Blocking(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield frame
            await released.wait()
            yield frame

        async def aclose(self):
            closed.set()

    def provider(request):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Blocking())

    async with gateway_factory(provider) as (_, client, _):
        async with client.stream("POST", "/v1/responses", json=body()) as response:
            first = await asyncio.wait_for(anext(response.aiter_bytes()), timeout=1)
            assert first == frame and not released.is_set()
        await asyncio.wait_for(closed.wait(), timeout=1)


async def test_nonstream_collects_terminal_response(gateway_factory):
    result = {
        "id": "resp_FAKE",
        "output": [{"type": "message", "content": "שלום"}],
        "usage": {"input_tokens": 3},
    }
    frame = (
        "event: response.completed\ndata: "
        + json.dumps({"type": "response.completed", "response": result}, ensure_ascii=False)
        + "\n\n"
    ).encode()

    def provider(request):
        assert json.loads(request.content)["stream"] is True
        return httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=Chunks([bytes([c]) for c in frame])
        )

    async with gateway_factory(provider) as (_, client, _):
        response = await client.post("/v1/responses", json=body(stream=False))
        assert response.json() == result


@pytest.mark.parametrize(
    "payload",
    [
        {},
        body(model="evil-model"),
        body(input=7),
        body(stream="true"),
        body(instructions=[]),
        body(input=[{"role": "developer", "content": 12}]),
    ],
)
async def test_invalid_request_never_reaches_provider(gateway_factory, payload):
    def provider(request):
        pytest.fail("invalid request reached provider")

    async with gateway_factory(provider) as (_, client, _):
        assert (await client.post("/v1/responses", json=payload)).status_code == 400


async def test_concurrent_401_refreshes_once(gateway_factory):
    calls = []
    new = jwt(exp=4102444801)

    async def provider(request):
        calls.append(request)
        if str(request.url) == TOKEN_URL:
            await asyncio.sleep(0.02)
            return httpx.Response(200, json={"access_token": new, "refresh_token": "FAKE-new"})
        if request.headers["authorization"] == "Bearer " + jwt():
            return httpx.Response(401)
        return httpx.Response(200, json={"id": "resp_FAKE"})

    async with gateway_factory(provider) as (_, client, _):
        responses = await asyncio.gather(*(client.post("/v1/responses", json=body()) for _ in range(12)))
        assert all(response.status_code == 200 for response in responses)
        assert sum(str(request.url) == TOKEN_URL for request in calls) == 1


async def test_refresh_redirect_refused_no_provider_call(gateway_factory):
    seen = []

    def provider(request):
        seen.append(request)
        return httpx.Response(302, headers={"location": "https://evil.invalid/steal"})

    async with gateway_factory(provider, auth_document(jwt(exp=1))) as (_, client, _):
        response = await client.post("/v1/responses", json=body())
        assert response.status_code == 503 and "location" not in response.headers
        assert len(seen) == 1 and str(seen[0].url) == TOKEN_URL


@pytest.mark.parametrize(
    "expired", [jwt(exp=1), jwt(exp="not-a-number"), jwt(exp=True), jwt(exp=float("inf"))]
)
async def test_invalid_refresh_expiry_never_replaces_auth(gateway_factory, expired):
    document = auth_document(jwt(exp=1))

    def provider(request):
        return httpx.Response(200, json={"access_token": expired, "refresh_token": "FAKE-new"})

    async with gateway_factory(provider, document) as (_, client, path):
        assert (await client.post("/v1/responses", json=body())).status_code == 503
        assert json.loads(path.read_text()) == document


async def test_account_from_id_token_when_explicit_account_absent(gateway_factory):
    document = auth_document()
    document["tokens"].pop("account_id")
    document["tokens"]["id_token"] = jwt(
        **{"https://api.openai.com/auth": {"chatgpt_account_id": "FAKE-derived"}}
    )

    def provider(request):
        assert request.headers["chatgpt-account-id"] == "FAKE-derived"
        return httpx.Response(200, json={"id": "resp_FAKE"})

    async with gateway_factory(provider, document) as (_, client, _):
        assert (await client.post("/v1/responses", json=body())).status_code == 200


async def test_unknown_provider_exception_never_logs_credential(gateway_factory, caplog):
    def provider(request):
        raise RuntimeError("FAKE-refresh FAKE-internal FAKE-secret-exception")

    async with gateway_factory(provider) as (_, client, _):
        response = await client.post("/v1/responses", json=body())
        assert response.status_code == 502
        assert "FAKE-secret-exception" not in response.text + caplog.text
        assert "FAKE-refresh" not in response.text + caplog.text


async def test_nonstream_truncated_response_is_failure(gateway_factory):
    def provider(request):
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=Chunks([b'event: response.created\ndata: {"type":"response.created"}\n\n']),
        )

    async with gateway_factory(provider) as (_, client, _):
        response = await client.post("/v1/responses", json=body(stream=False))
        assert response.status_code == 502
        assert response.json()["error"]["code"] == "codex_stream_incomplete"


async def test_run_start_close_without_provider_traffic(tmp_path):
    path = tmp_path / "FAKE-auth.json"
    path.write_text(json.dumps(auth_document()))

    def provider(request):
        pytest.fail("lifecycle must not invoke upstream")

    gateway = CodexGateway(
        SimpleNamespace(
            codex_gateway_key="FAKE-key",
            codex_auth_path=path,
            codex_gateway_host="127.0.0.1",
            codex_gateway_port=0,
        ),
        upstream_transport=httpx.MockTransport(provider),
    )
    stop = asyncio.Event()
    task = asyncio.create_task(gateway.run(stop))
    await asyncio.wait_for(gateway.ready.wait(), timeout=1)
    assert gateway.bound_port > 0
    stop.set()
    await task
    await gateway.close()
    assert not gateway.ready.is_set() and gateway.bound_port is None


async def test_external_unrelated_token_metadata_edit_preserved_during_refresh(gateway_factory):
    paths = []
    new = jwt(exp=4102444801)

    def provider(request):
        if str(request.url) == TOKEN_URL:
            updated = json.loads(paths[0].read_text())
            updated["tokens"]["extra"] = "operator-edited-during-refresh"
            updated["operator_extra"]["new"] = "keep-this-too"
            paths[0].write_text(json.dumps(updated))
            return httpx.Response(200, json={"access_token": new, "refresh_token": "FAKE-rotated"})
        assert request.headers["authorization"] == "Bearer " + new
        return httpx.Response(200, json={"id": "resp_FAKE"})

    async with gateway_factory(provider, auth_document(jwt(exp=1))) as (_, client, path):
        paths.append(path)
        assert (await client.post("/v1/responses", json=body())).status_code == 200
        stored = json.loads(path.read_text())
        assert stored["tokens"]["extra"] == "operator-edited-during-refresh"
        assert stored["operator_extra"]["new"] == "keep-this-too"
        assert stored["tokens"]["refresh_token"] == "FAKE-rotated"


async def test_connection_nominated_headers_not_forwarded(gateway_factory):
    def provider(request):
        return httpx.Response(
            429,
            json={"error": {"code": "usage_limit_reached"}},
            headers={
                "Connection": "Retry-After, X-Request-ID",
                "Retry-After": "120",
                "X-Request-ID": "FAKE-hop",
                "x-codex-primary-reset-at": "1800000000",
            },
        )

    async with gateway_factory(provider) as (_, client, _):
        response = await client.post("/v1/responses", json=body())
        assert response.status_code == 429
        assert "retry-after" not in response.headers and "x-request-id" not in response.headers
        assert response.headers["x-codex-primary-reset-at"] == "1800000000"


async def test_gateway_shutdown_transport_error_is_static(tmp_path):
    path = tmp_path / "FAKE-auth.json"
    path.write_text(json.dumps(auth_document()))

    class BadClose(httpx.MockTransport):
        async def aclose(self):
            raise RuntimeError("FAKE-refresh transport close failure")

    gateway = CodexGateway(
        SimpleNamespace(
            codex_gateway_key="FAKE-key",
            codex_auth_path=path,
            codex_gateway_host="127.0.0.1",
            codex_gateway_port=0,
        ),
        upstream_transport=BadClose(lambda request: None),
    )
    await gateway.start()
    with pytest.raises(GatewayError, match="^codex_gateway_close_failed$") as caught:
        await gateway.close()
    assert "FAKE-refresh" not in str(caught.value)
    assert gateway._client is None and gateway.bound_port is None
    await gateway.close()


@pytest.mark.parametrize("content_type", [None, "application/json", "text/plain; charset=utf-8"])
async def test_nonstream_recognizes_sse_body_despite_content_type(gateway_factory, content_type):
    result = {"id": "resp_FAKE", "status": "completed", "output": []}
    frame = (
        "event: response.completed\ndata: "
        + json.dumps({"type": "response.completed", "response": result})
        + "\n\n"
    ).encode()

    def provider(request):
        headers = {"content-type": content_type} if content_type else {}
        return httpx.Response(200, headers=headers, stream=Chunks([bytes([part]) for part in frame]))

    async with gateway_factory(provider) as (_, client, _):
        response = await client.post("/v1/responses", json=body(stream=False))
        assert response.status_code == 200 and response.json() == result
        assert response.headers["content-type"].startswith("application/json")


async def test_nonstream_preserves_actual_json_success_response(gateway_factory):
    result = {"id": "resp_FAKE", "status": "completed", "output": [], "text": "שלום"}

    def provider(request):
        raw = json.dumps(result, ensure_ascii=False).encode()
        return httpx.Response(
            200, headers={"content-type": "application/json"}, stream=Chunks([bytes([part]) for part in raw])
        )

    async with gateway_factory(provider) as (_, client, _):
        response = await client.post("/v1/responses", json=body(stream=False))
        assert response.status_code == 200 and response.json() == result
