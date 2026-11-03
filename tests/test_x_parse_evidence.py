import base64
import gzip
import hashlib
import importlib
import json

import httpx
import pytest
from test_x_diagnostics import diagnostics
from test_x_reader import BodyStream, sequence
from test_x_reader import clock as clock
from test_x_reader import reader as reader

from tank.sources.x_diagnostics import sanitize
from tank.util import sha256


@pytest.mark.parametrize("kind", ["text", "binary", "gzip"])
async def test_parse_failure_retains_original_bounded_response(reader, monkeypatch, clock, kind):
    raw = b"\xff\x00" + b"x" * 3000 if kind == "binary" else b"<title>blocked</title>" + b"x" * 3000
    wire = gzip.compress(raw) if kind == "gzip" else raw
    headers = {"Content-Length": str(len(wire))}
    if kind == "gzip":
        headers["Content-Encoding"] = "gzip"
    stream = BodyStream(wire)
    attempts = sequence(reader, monkeypatch, [httpx.Response(202, headers=headers, stream=stream)], clock)
    with pytest.raises(reader.ProfileFailure, match="^PUBLIC_PROFILE_NOT_READABLE$") as failure:
        await reader.fetch_profile("alice", 100, evidence_limit=1200)
    evidence = failure.value.diagnostics["attempts"][0]["response"]
    body = (
        base64.b64decode(evidence["body"])
        if evidence["bodyEncoding"] == "base64"
        else evidence["body"].encode()
    )
    assert body == raw[:200]
    assert evidence["bodySha256"] == sha256(raw[:200])
    assert evidence["status"] == 202 and evidence["bodyBytes"] == 200
    assert evidence["captureLimitBytes"] == evidence["serializedBodyLimitBytes"] == 1200
    assert evidence["bodyTruncated"] and len(json.dumps(evidence["body"])) <= 1200
    assert evidence["finalUrl"] == "https://x.com/alice"
    assert len(attempts) == 1 and stream.closed and not clock.waits


@pytest.mark.parametrize("field", ["finalUrl", "title"])
@pytest.mark.parametrize("prefix", ["1.https", "+.-1.https", "https"])
@pytest.mark.parametrize("query_key", ["token", "api_token", "api-token", "apiToken", "auth", "AuTh"])
def test_parse_metadata_is_redacted_bounded_jsonb_safe_and_allowlisted(field, prefix, query_key):
    value = diagnostics("body")
    response = value["attempts"][0]["response"]
    response.update(
        {
            field: f"{prefix}://reader:db-password@host/path?{query_key}=query-secret configured-secret\x00\ud800"
            + "x" * 9000,
            "articleCount": 3,
            "statusLinkCount": 2,
            "textCount": 1,
            "scriptCount": 4,
            "unknownPageMetadata": "must-not-survive",
        }
    )
    clean = sanitize(value, "configured-secret")
    captured = clean["attempts"][0]["response"]
    assert captured[field].startswith(
        f"{prefix}://reader:[REDACTED]@host/path?{query_key}=[REDACTED] [REDACTED][NUL]"
    )
    assert len(captured[field]) <= 8192
    assert all(
        secret not in json.dumps(clean)
        for secret in ("db-password", "query-secret", "configured-secret", "must-not-survive")
    )
    counts = {key: captured[key] for key in ("articleCount", "statusLinkCount", "textCount", "scriptCount")}
    assert counts == {"articleCount": 3, "statusLinkCount": 2, "textCount": 1, "scriptCount": 4}
    json.dumps(clean, ensure_ascii=False).encode()
    assert f"{field}[1]" in clean["invalidEvidence"]


@pytest.mark.parametrize("delimiter", [",", ";"])
@pytest.mark.parametrize(
    "query_key",
    [
        "auth",
        "AuTh",
        "api_token",
        "api-token",
        "apiToken",
        "api_key",
        "api-key",
        "apiKey",
        "access_token",
        "token",
        "password",
        "secret",
    ],
)
def test_consumer_query_credentials_remove_full_value_and_preserve_safe_parameters(query_key, delimiter):
    raw_url = f"https://host/path?{query_key}=query-prefix{delimiter}private-suffix&view=public"
    safe_url = f"https://host/path?{query_key}=[REDACTED]&view=public"
    raw = f"<title>{raw_url}</title>".encode()
    expected = f"<title>{safe_url}</title>".encode()
    value = diagnostics(raw.decode())
    value["attempts"][0]["response"].update(finalUrl=raw_url, title=raw_url)
    clean = sanitize(value, "configured-secret")
    response = clean["attempts"][0]["response"]
    assert response["finalUrl"].encode() == response["title"].encode() == safe_url.encode()
    assert response["body"].encode() == expected
    assert response["bodyBytes"] == len(expected)
    assert response["bodySha256"] == hashlib.sha256(expected).hexdigest()
    assert response["consumerCapturedBytes"] == len(raw)
    assert response["redacted"] and not response["consumerTransformTruncated"]
    assert all(part not in json.dumps(clean) for part in ("query-prefix", "private-suffix"))


@pytest.mark.parametrize("bad", [True, -1, 2_000_001, "3", {}, []])
@pytest.mark.parametrize("field", ["articleCount", "statusLinkCount", "textCount", "scriptCount"])
def test_invalid_parse_counts_are_not_persisted(field, bad):
    value = diagnostics("body")
    value["attempts"][0]["response"][field] = bad
    clean = sanitize(value)
    assert clean["attempts"][0]["response"][field] is None
    assert f"{field}[1]" in clean["invalidEvidence"]


@pytest.mark.parametrize("prefix", ["1.https", "+.-1.https", "https"])
@pytest.mark.parametrize("query_key", ["token", "api_token", "api-token", "apiToken", "auth", "AuTh"])
def test_actor_parse_metadata_redacts_numbered_urls_and_bounds_title(reader, monkeypatch, prefix, query_key):
    monkeypatch.setenv("ACTOR_TOKEN", "configured-secret")
    raw = (
        f"<title>{prefix}://reader:db-password@host/path?{query_key}=query-secret configured-secret "
        + "x" * 1000
        + "</title><script>page</script>"
    ).encode()
    response = httpx.Response(
        200,
        request=httpx.Request("GET", f"https://reader:db-password@x.com/alice?{query_key}=query-secret"),
    )
    evidence = reader.response_evidence(response, raw)
    assert evidence["finalUrl"] == f"https://reader:[REDACTED]@x.com/alice?{query_key}=[REDACTED]"
    assert len(evidence["title"]) == 512 and evidence["scriptCount"] == 1
    assert evidence["title"].startswith(
        f"{prefix}://reader:[REDACTED]@host/path?{query_key}=[REDACTED] [REDACTED] "
    )
    assert all(
        secret not in json.dumps(evidence) for secret in ("db-password", "query-secret", "configured-secret")
    )


@pytest.mark.integration
@pytest.mark.parametrize("kind", ["text", "binary"])
async def test_many_parse_failures_share_existing_retention_budget(reader, monkeypatch, clock, kind):
    raw = (b"\xff\x00" if kind == "binary" else b"<title>blocked</title>") + b"x" * 30_000
    sequence(reader, monkeypatch, [raw] * 200, clock)
    output = await importlib.import_module("direct_fetch").fetch_snapshot(
        {"handles": [f"account{index}" for index in range(200)]}
    )
    assert output["health"] == "failed" and output["posts"] == [] and output["nextCursors"] == {}
    assert len(output["failedAccounts"]) == 200
    total = 0
    for account in output["accounts"]:
        assert account["error"] == "PUBLIC_PROFILE_NOT_READABLE"
        evidence = account["diagnostics"]["attempts"][0]["response"]
        body = (
            base64.b64decode(evidence["body"])
            if evidence["bodyEncoding"] == "base64"
            else evidence["body"].encode()
        )
        assert body == raw[:3333] and evidence["bodyTruncated"]
        assert evidence["bodySha256"] == sha256(raw[:3333])
        assert evidence["serializedBodyLimitBytes"] == 20_000
        total += len(json.dumps(evidence["body"], ensure_ascii=True))
    assert total <= 12_000_000
