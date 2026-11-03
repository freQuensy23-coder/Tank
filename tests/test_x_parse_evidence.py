import base64
import json

import pytest
from test_x_diagnostics import diagnostics
from test_x_reader import clock as clock
from test_x_reader import reader as reader
from test_x_reader import sequence

from tank.sources.x_diagnostics import sanitize
from tank.util import sha256


@pytest.mark.parametrize("kind", ["text", "binary", "gzip"])
async def test_parse_failure_retains_original_bounded_response(reader, monkeypatch, clock, kind):
    import gzip

    import httpx

    from test_x_reader import BodyStream

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
    body = base64.b64decode(evidence["body"]) if evidence["bodyEncoding"] == "base64" else evidence["body"].encode()
    assert body == raw[:200]
    assert evidence["bodySha256"] == sha256(raw[:200])
    assert evidence["status"] == 202 and evidence["bodyBytes"] == 200
    assert evidence["captureLimitBytes"] == evidence["serializedBodyLimitBytes"] == 1200
    assert evidence["bodyTruncated"] and len(json.dumps(evidence["body"])) <= 1200
    assert evidence["finalUrl"] == "https://x.com/alice"
    assert len(attempts) == 1 and stream.closed and not clock.waits


@pytest.mark.parametrize("field", ["finalUrl", "title"])
def test_parse_metadata_is_redacted_bounded_jsonb_safe_and_allowlisted(field):
    value = diagnostics("body")
    response = value["attempts"][0]["response"]
    response.update(
        {
            field: "1.https://reader:db-password@host/path?token=query-secret configured-secret\x00\ud800" + "x" * 9000,
            "articleCount": 3,
            "statusLinkCount": 2,
            "textCount": 1,
            "scriptCount": 4,
            "unknownPageMetadata": "must-not-survive",
        }
    )
    clean = sanitize(value, "configured-secret")
    captured = clean["attempts"][0]["response"]
    assert captured[field].startswith("1.https://reader:[REDACTED]@host/path?token=[REDACTED] [REDACTED][NUL]")
    assert len(captured[field]) <= 8192
    assert all(secret not in json.dumps(clean) for secret in ("db-password", "query-secret", "configured-secret", "must-not-survive"))
    assert {key: captured[key] for key in ("articleCount", "statusLinkCount", "textCount", "scriptCount")} == {
        "articleCount": 3, "statusLinkCount": 2, "textCount": 1, "scriptCount": 4
    }
    json.dumps(clean, ensure_ascii=False).encode()
    assert f"{field}[1]" in clean["invalidEvidence"]


@pytest.mark.parametrize("bad", [True, -1, 2_000_001, "3", {}, []])
@pytest.mark.parametrize("field", ["articleCount", "statusLinkCount", "textCount", "scriptCount"])
def test_invalid_parse_counts_are_not_persisted(field, bad):
    value = diagnostics("body")
    value["attempts"][0]["response"][field] = bad
    clean = sanitize(value)
    assert clean["attempts"][0]["response"][field] is None
    assert f"{field}[1]" in clean["invalidEvidence"]
