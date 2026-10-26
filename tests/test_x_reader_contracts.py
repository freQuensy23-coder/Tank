import base64
import hashlib
import json
import sys
from pathlib import Path

import pytest
from pydantic import JsonValue, ValidationError

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "apify" / "x-reader"))

from actor_contracts import (
    ActorRequest,
    DiagnosticResponse,
    ProfileDiagnostics,
    ReferenceReadFailure,
    ReferenceReadSuccess,
)
from direct_fetch import fetch_snapshot
from public_profile import decode_embedded_json, parse_profile, run_deadline


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"handles": None},
        {"handles": ["alice"], "concurrency": True},
        {"handles": ["alice"], "concurrency": 1.0},
        {"handles": ["alice"], "concurrency": None},
        {"handles": ["alice"], "cursors": None},
        {"handles": ["alice"], "cursors": {"bob": "123"}},
        {"handles": ["alice"], "cursors": {"alice": 123}},
        {"handles": ["alice"], "cursors": {"alice": "0"}},
        {"handles": ["alice"], "mode": "historical"},
        {"handles": ["alice"], "ignoredTelemetry": "private-test-value"},
        {"handles": ["alice"], "xSession": "private-test-value"},
    ],
)
async def test_invalid_actor_request_refuses_before_profile_collection(
    payload: dict[str, JsonValue],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def forbidden_collect(config: ActorRequest) -> None:
        raise AssertionError("Invalid request reached profile collection")

    monkeypatch.setattr("direct_fetch.collect", forbidden_collect)
    with pytest.raises(ValueError) as failure:
        await fetch_snapshot(payload)
    assert "private-test-value" not in str(failure.value)


def test_declared_request_defaults_and_normalized_cursor_ownership() -> None:
    request = ActorRequest.model_validate(
        {
            "handles": ["@Alice", "alice", "bob"],
            "cursors": {"@ALICE": "123"},
        }
    )
    assert request.handles == ["alice", "bob"]
    assert request.cursors == {"alice": "123"}
    assert request.mode == "fetch" and request.concurrency == 6 and request.requestId is None
    with pytest.raises(ValidationError, match="CONFLICTING_CURSOR_VALUES"):
        ActorRequest.model_validate(
            {
                "handles": ["alice"],
                "cursors": {"Alice": "123", "alice": "124"},
            }
        )


@pytest.mark.parametrize("binary", [False, True])
def test_response_integrity_rejects_changed_body_and_invented_capture_count(binary: bool) -> None:
    payload = b"diagnostic-prefix" if not binary else b"\x00\xffdiagnostic"
    document: dict[str, JsonValue] = {
        "status": 503,
        "headers": {},
        "body": base64.b64encode(payload).decode() if binary else payload.decode(),
        "bodyEncoding": "base64" if binary else "utf-8",
        "bodyBytes": len(payload),
        "bodySha256": hashlib.sha256(payload).hexdigest(),
        "wireBytes": len(payload),
        "captureLimitBytes": len(payload),
        "serializedBodyLimitBytes": None,
        "bodyTruncated": False,
        "omittedWireBytes": None,
        "readError": None,
        "decodeError": None,
        "redacted": False,
    }
    response = DiagnosticResponse.model_validate(document)
    assert response.model_dump(mode="json") == document
    with pytest.raises(ValidationError, match="DIAGNOSTIC_BODY_INTEGRITY_MISMATCH"):
        DiagnosticResponse.model_validate({**document, "bodySha256": "0" * 64})
    with pytest.raises(ValidationError, match="DIAGNOSTIC_WIRE_CAPTURE_LIMIT_EXCEEDED"):
        DiagnosticResponse.model_validate({**document, "wireBytes": len(payload) + 1})


def test_missing_response_and_attempt_facts_cannot_be_synthesized() -> None:
    document: dict[str, JsonValue] = {
        "request": {"method": "GET", "url": "https://x.com/alice", "body": None},
        "attempts": [
            {"attempt": 1, "at": "2026-10-25T00:00:00Z", "error": "connection failed", "retryable": True}
        ],
        "attemptCount": 1,
        "outcome": "exhausted",
    }
    diagnostics = ProfileDiagnostics.model_validate(document)
    assert diagnostics.model_dump(mode="json", exclude_unset=True) == document
    with pytest.raises(ValidationError, match="DIAGNOSTIC_ATTEMPT_COUNT_MISMATCH"):
        ProfileDiagnostics.model_validate({**document, "attemptCount": 0})
    with pytest.raises(ValidationError, match="DIAGNOSTIC_RESPONSE_REQUIRED"):
        ProfileDiagnostics.model_validate(
            {
                **document,
                "attempts": [
                    {
                        "attempt": 1,
                        "at": "2026-10-25T00:00:00Z",
                        "error": "connection failed",
                        "retryable": True,
                        "status": 503,
                    }
                ],
            }
        )


@pytest.mark.parametrize("declaration", ["", "not-a-clock", "2026-10-25T00:00:00"])
def test_declared_actor_deadline_cannot_silently_use_local_fallback(
    declaration: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ACTOR_TIMEOUT_AT", declaration)
    with pytest.raises(ValueError):
        run_deadline()


def test_offset_deadline_uses_declared_instant_and_shutdown_margin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ACTOR_TIMEOUT_AT", "1970-01-01T01:00:10+01:00")
    monkeypatch.setattr("public_profile.time.time", lambda: 0.0)
    monkeypatch.setattr("public_profile.time.monotonic", lambda: 100.0)
    assert run_deadline() == 108.0


def embedded_page(wire: str) -> str:
    return (
        '<title>Alice (@alice) / X</title><article><a href="/alice/status/101">t</a>'
        '<div data-testid="tweetText">Visible original</div></article>'
        '<script>kind:"Result.Ok",value:' + json.dumps(wire) + "</script>"
    )


@pytest.mark.parametrize(
    "wire",
    [
        '{"__typename":"Tweet","rest_id":"999","rest_id":"101","details":{"full_text":"ambiguous"}}',
        '{"__typename":"Tweet","rest_id":"101","details":{"full_text":"first"},"details":{"full_text":"ambiguous"}}',
        '{"__typename":"Tweet","rest_id":"101","details":{"full_text":"first","full_\\u0074ext":"ambiguous"}}',
        '{"__typename":"Tweet","rest_id":"101","quoted_tweet_results":{"rest_id":"999","rest_id":"2000"}}',
    ],
)
def test_ambiguous_embedded_objects_never_author_context(wire: str) -> None:
    with pytest.raises(ValueError, match="AMBIGUOUS_EMBEDDED_JSON_OBJECT"):
        decode_embedded_json(wire)
    [post] = parse_profile(embedded_page(wire), "alice")
    assert post.id == "101" and post.author == "alice"
    assert post.text == "Visible original" and post.referencedPosts == []


def test_unique_enrichment_and_opaque_json_looking_post_text_remain_observed() -> None:
    opaque = '{"same":1,"same":2}'
    wire = json.dumps(
        {
            "__typename": "Tweet",
            "rest_id": "101",
            "details": {"full_text": opaque},
            "quoted_tweet_results": {"rest_id": "2000"},
        }
    )
    [post] = parse_profile(embedded_page(wire), "alice")
    assert post.text == opaque and len(post.referencedPosts) == 1
    assert post.referencedPosts[0].id == "2000"
    assert post.referencedPosts[0].author is None and post.referencedPosts[0].text is None


def test_duplicate_wire_error_does_not_echo_untrusted_key_or_value() -> None:
    secret = "synthetic-private-embedded-key"
    with pytest.raises(ValueError) as caught:
        decode_embedded_json(json.dumps({secret: 1})[:-1] + ',"' + secret + '":"' + secret + '"}')
    assert str(caught.value) == "AMBIGUOUS_EMBEDDED_JSON_OBJECT"
    assert secret not in str(caught.value)


@pytest.mark.parametrize(
    "payload",
    [
        {"post_id": "2000", "error": None},
        {"post_id": "2000", "error": ""},
        {
            "post_id": "2000",
            "error": "PUBLIC_POST_HTTP_404",
            "content": {"author": None, "text": "", "publishedAt": None},
        },
    ],
)
def test_reference_failure_requires_only_its_actual_failure(payload: dict[str, JsonValue]) -> None:
    with pytest.raises(ValidationError):
        ReferenceReadFailure.model_validate(payload)


@pytest.mark.parametrize(
    "payload",
    [
        {"post_id": "2000", "content": None},
        {
            "post_id": "2000",
            "content": {"author": None, "text": "", "publishedAt": None},
            "error": "PUBLIC_POST_HTTP_404",
        },
    ],
)
def test_reference_success_requires_only_its_actual_body(payload: dict[str, JsonValue]) -> None:
    with pytest.raises(ValidationError):
        ReferenceReadSuccess.model_validate(payload)


def test_ambiguous_enrichment_keeps_only_the_actually_visible_quote() -> None:
    wire = '{"__typename":"Tweet","rest_id":"101","quoted_tweet_results":{"rest_id":"999","rest_id":"2000"}}'
    nested = (
        '<article><a href="/bob/status/3000">t</a><div data-testid="tweetText">Observed quote</div></article>'
    )
    page = embedded_page(wire).replace("</div></article>", "</div>" + nested + "</article>", 1)
    [post] = parse_profile(page, "alice")
    assert post.text == "Visible original" and len(post.referencedPosts) == 1
    assert post.referencedPosts[0].id == "3000" and post.referencedPosts[0].author == "bob"
    assert post.referencedPosts[0].text == "Observed quote"


def test_known_embedded_author_conflict_never_becomes_monitored_author_text() -> None:
    wire = json.dumps(
        {
            "__typename": "Tweet",
            "rest_id": "101",
            "core": {"user_results": {"result": {"core": {"screen_name": "bob"}}}},
            "details": {"full_text": "Other author body"},
        }
    )
    with pytest.raises(RuntimeError, match="PUBLIC_POST_AUTHOR_CONFLICT"):
        parse_profile(embedded_page(wire), "alice")


def test_unknown_core_and_case_matching_author_preserve_dom_authority() -> None:
    cores: list[JsonValue] = [[], {"user_results": {"result": {"core": {"screen_name": "ALICE"}}}}]
    for core in cores:
        wire = json.dumps({"__typename": "Tweet", "rest_id": "101", "core": core})
        [post] = parse_profile(embedded_page(wire), "alice")
        assert post.author == "alice" and post.text == "Visible original"
