"""S03 replay uses the ordinary API item, retaining its source and date bounds."""

import copy
import json
from pathlib import Path

import pytest

from tank.fixture_reference import project_original_source, verify_derivation
from tank.sources import parsers
from tank.util import sha256

KIND = "FEDERAL_REGISTER_JSON_ITEM_V1"


def inputs():
    article = {
        "document_number": "2026-12345",
        "title": "Proposed custody rule",
        "type": "Proposed Rule",
        "abstract": "The Commission proposes a custody rule for crypto assets.",
        "excerpts": "A different search excerpt.",
        "html_url": "https://www.federalregister.gov/documents/2026/10/01/2026-12345/rule",
        "pdf_url": "https://www.govinfo.gov/content/pkg/FR-2026-10-01/pdf/2026-12345.pdf",
        "publication_date": "2026-10-01",
        "agencies": [{"name": "Securities and Exchange Commission"}],
        "unrelated_response_metadata": "not an authored item field",
    }
    later = {**article, "document_number": "2026-99999", "publication_date": "2026-11-01"}
    raw = json.dumps({"results": [article, later]}).encode()
    projected = project_original_source(KIND, raw, {"document_number": "2026-12345"})
    item = (
        parsers.json_items(projected, "S03")
        .discoveries[0]
        .discovery.model_dump(mode="json", exclude_unset=True)
    )
    proof = {"derivation": {"kind": KIND, "module_sha256": sha256(Path(parsers.__file__).read_bytes())}}
    return raw, projected, item, proof


def test_exact_normal_parser_body_and_unique_document_only() -> None:
    raw, projected, item, proof = inputs()
    assert item["text"] == "The Commission proposes a custody rule for crypto assets."
    assert len(json.loads(projected)["results"]) == 1
    assert b"2026-99999" not in projected
    assert b"unrelated_response_metadata" not in projected
    assert json.loads(raw)["results"][0]["abstract"] == item["text"]
    verify_derivation(item, proof, projected)


@pytest.mark.parametrize(
    "field,value",
    [
        ("external_id", "2026-99999"),
        ("title", "Approved custody rule"),
        ("document_type", "Rule"),
        ("published_at", "2026-10-02T00:00:00Z"),
        ("published_at_precision", "EXACT"),
        ("text", "The Commission approves a custody rule for crypto assets."),
        ("source_id", "S05"),
    ],
)
def test_source_content_identity_and_date_cannot_change(field, value) -> None:
    _, projected, item, proof = inputs()
    item[field] = value
    with pytest.raises(ValueError):
        verify_derivation(item, proof, projected)


@pytest.mark.parametrize("variant", ["missing", "duplicate", "selector"])
def test_projection_refuses_ambiguous_or_missing_selection(variant) -> None:
    raw, _, _, _ = inputs()
    data = json.loads(raw)
    selector = {"document_number": "2026-12345"}
    if variant == "missing":
        data["results"] = []
    elif variant == "duplicate":
        data["results"].append(copy.deepcopy(data["results"][0]))
    else:
        selector = {"index": 0}
    with pytest.raises(ValueError):
        project_original_source(KIND, json.dumps(data).encode(), selector)


@pytest.mark.parametrize("module_hash", [None, "wrong"])
def test_source_parser_version_is_bound(module_hash) -> None:
    _, projected, item, proof = inputs()
    proof["derivation"]["module_sha256"] = module_hash
    with pytest.raises(ValueError):
        verify_derivation(item, proof, projected)
