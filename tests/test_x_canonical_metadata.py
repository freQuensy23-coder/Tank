import copy
import hashlib
import json
from unittest.mock import patch

import pytest
from test_x_diagnostics import diagnostics
from test_x_legacy_diagnostics import assert_refused, canonical_manifest, legacy_notice, reordered

from tank.sources import x_diagnostic_manifest
from tank.sources.x_diagnostics import BODY_LIMIT, documents, sanitize

TEXT_FIELDS = ("finalUrl", "title")
COUNT_FIELDS = ("articleCount", "statusLinkCount", "textCount", "scriptCount")
SAFE_FIELDS = {
    "finalUrl": "https://x.com/example?view=public",
    "title": "Public page",
    "articleCount": 0,
    "statusLinkCount": 1,
    "textCount": BODY_LIMIT,
    "scriptCount": None,
}


def current_notice(fields):
    notice = legacy_notice()
    evidence = diagnostics()
    evidence["attempts"][0]["response"].update(fields)
    notice["entry_errors"][0]["diagnostics"] = sanitize(evidence)
    return notice


@pytest.mark.parametrize(
    "fields",
    [{}, SAFE_FIELDS, {"finalUrl": SAFE_FIELDS["finalUrl"]}, {"title": "x" * 8192}]
    + [{field: None} for field in COUNT_FIELDS]
    + [{field: BODY_LIMIT} for field in COUNT_FIELDS],
)
def test_current_optional_metadata_keeps_original_canonical_bytes(fields):
    notice = current_notice(fields)
    expected = canonical_manifest(notice)
    original = copy.deepcopy(notice)
    with patch.object(x_diagnostic_manifest, "_legacy_metadata", side_effect=AssertionError("historical")):
        actual = x_diagnostic_manifest.verified_documents(reordered(notice))
    assert actual == expected
    metadata = json.loads(actual[-1]["content"])["attempts"][0]["response"]
    assert {field: metadata[field] for field in fields} == fields
    assert all(field not in metadata for field in SAFE_FIELDS.keys() - fields.keys())
    assert notice == original
    assert [
        {
            "filename": row["filename"],
            "sha256": hashlib.sha256(row["content"]).hexdigest(),
            "bytes": len(row["content"]),
        }
        for row in actual
    ] == original["document_manifest"]


def test_current_generated_metadata_paths_for_all_three_attempts_keep_canonical_bytes():
    notice = legacy_notice()
    evidence = diagnostics()
    evidence["attempts"] = [copy.deepcopy(evidence["attempts"][0]) for _ in range(3)]
    for index, attempt in enumerate(evidence["attempts"], 1):
        attempt["attempt"] = index
        attempt["response"].update(
            finalUrl=None,
            title="Public\x00page",
            articleCount=True,
            statusLinkCount=-1,
            textCount=BODY_LIMIT + 1,
            scriptCount="0",
        )
    clean = sanitize(evidence)
    assert clean["invalidEvidence"] == sorted(
        f"{field}[{index}]" for field in SAFE_FIELDS for index in range(1, 4)
    )
    notice["entry_errors"][0]["diagnostics"] = clean
    expected = canonical_manifest(notice)
    with patch.object(x_diagnostic_manifest, "_legacy_metadata", side_effect=AssertionError("historical")):
        actual = x_diagnostic_manifest.verified_documents(reordered(notice))
    assert actual == expected
    metadata = json.loads(actual[-1]["content"])
    assert metadata["invalidEvidence"] == clean["invalidEvidence"]
    for attempt in metadata["attempts"]:
        response = attempt["response"]
        assert response["finalUrl"] == "INVALID_DIAGNOSTIC_TEXT"
        assert response["title"] == "Public[NUL]page"
        assert {field: response[field] for field in COUNT_FIELDS} == dict.fromkeys(COUNT_FIELDS)


@pytest.mark.parametrize("field", COUNT_FIELDS)
@pytest.mark.parametrize("value", [True, 1.5, "1", {}, -1, BODY_LIMIT + 1])
async def test_direct_current_count_metadata_refuses_before_materialization(field, value):
    notice = current_notice(SAFE_FIELDS)
    notice["entry_errors"][0]["diagnostics"]["attempts"][0]["response"][field] = value
    canonical_manifest(notice)
    with patch.object(x_diagnostic_manifest, "documents", wraps=documents) as materialize:
        await assert_refused(notice)
    materialize.assert_not_called()


@pytest.mark.parametrize("field", TEXT_FIELDS)
@pytest.mark.parametrize("value", [None, 1, {}, "Public\x00page", "Public\ud800page", "x" * 8193])
async def test_direct_current_text_metadata_refuses_before_materialization(field, value):
    notice = current_notice(SAFE_FIELDS)
    notice["entry_errors"][0]["diagnostics"]["attempts"][0]["response"][field] = value
    if not isinstance(value, str) or "\ud800" not in value:
        canonical_manifest(notice)
    with patch.object(x_diagnostic_manifest, "documents", wraps=documents) as materialize:
        await assert_refused(notice)
    materialize.assert_not_called()


@pytest.mark.parametrize(
    "path", ["finalUrl[0]", "title[4]", "response.title[1]", "unknown[1]", "articleCount[100000]"]
)
async def test_direct_current_metadata_rejects_nonproducer_paths_before_materialization(path):
    notice = current_notice(SAFE_FIELDS)
    notice["entry_errors"][0]["diagnostics"]["invalidEvidence"] = [path]
    canonical_manifest(notice)
    with patch.object(x_diagnostic_manifest, "documents", wraps=documents) as materialize:
        await assert_refused(notice)
    materialize.assert_not_called()


async def test_direct_current_metadata_unknown_field_refuses_before_materialization():
    notice = current_notice(SAFE_FIELDS)
    notice["entry_errors"][0]["diagnostics"]["attempts"][0]["response"]["otherCount"] = 0
    canonical_manifest(notice)
    with patch.object(x_diagnostic_manifest, "documents", wraps=documents) as materialize:
        await assert_refused(notice)
    materialize.assert_not_called()


@pytest.mark.parametrize("field", TEXT_FIELDS)
@pytest.mark.parametrize("separator", [",", ";"])
@pytest.mark.parametrize(
    "key",
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
async def test_direct_current_metadata_query_credentials_refuse_original_hashes(field, separator, key):
    notice = current_notice(SAFE_FIELDS)
    value = f"https://host/?{key}=private-prefix{separator}private-suffix&view=public"
    notice["entry_errors"][0]["diagnostics"]["attempts"][0]["response"][field] = value
    canonical_manifest(notice)
    with patch.object(x_diagnostic_manifest, "documents", wraps=documents) as materialize:
        await assert_refused(notice)
    materialize.assert_not_called()


@pytest.mark.parametrize("field", TEXT_FIELDS)
async def test_direct_current_metadata_configured_secret_refuses_original_hashes(field):
    notice = current_notice(SAFE_FIELDS)
    notice["entry_errors"][0]["diagnostics"]["attempts"][0]["response"][field] = "metadata-private-value"
    canonical_manifest(notice)
    with patch.object(x_diagnostic_manifest, "documents", wraps=documents) as materialize:
        await assert_refused(notice, secrets=("metadata-private-value",))
    materialize.assert_not_called()
