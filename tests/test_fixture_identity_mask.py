import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tank import fixture_reference as m
from tank.replay import ReplayError, load_inputs
from tank.util import canonical_json, sha256


def fixture(tmp_path):
    outside = tmp_path / "private"
    outside.mkdir()
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    original = {
        "registry": {"entities": [{"names": ["Robinhood"]}], "tokens": [{"symbol": "LIT"}]},
        "fixture_reference": {
            "fixture_id": "F-24",
            "authoritative_input_sha256": "canonical",
            "cutoff_spec": "unchanged",
        },
        "cutoff": "2026-01-01",
        "records": [
            {
                "collection": "ingest.source_items",
                "key": "one",
                "record": {"text": "Robinhood to list LIT", "author": "Robinhood"},
                "as_of_evidence": {
                    "kind": "DECLARED_SYNTHETIC_SOURCE",
                    "derivation": {"kind": "DECLARED_UTF8_SOURCE_V1"},
                    "version_id": "one",
                    "source_url": "fixture://F24",
                },
            }
        ],
    }
    raw = outside / "raw.txt"
    raw.write_text("Robinhood to list LIT")
    original["records"][0]["as_of_evidence"].update(raw_ref=str(raw), raw_sha256=sha256(raw.read_bytes()))
    (outside / "original.json").write_text(json.dumps(original))
    (outside / "registry.json").write_text(json.dumps(original["registry"]))
    private = {
        "source_sha256": sha256(canonical_json(original).encode()),
        "replacements": {"Robinhood": "IssuerSeven", "LIT": "ASSETK"},
        "original_packet_path": str(outside / "original.json"),
        "original_registry_path": str(outside / "registry.json"),
    }
    packet = copy.deepcopy(original)
    packet["records"][0]["record"].update(text="IssuerSeven to list ASSETK", author="IssuerSeven")
    masked_raw = inputs / "raw.txt"
    masked_raw.write_text("IssuerSeven to list ASSETK")
    packet["records"][0]["as_of_evidence"].update(
        raw_ref=str(masked_raw), raw_sha256=sha256(masked_raw.read_bytes())
    )
    packet["masking"] = {"applied": True, "map_sha256": sha256(canonical_json(private).encode())}
    path = outside / "map.json"
    path.write_text(json.dumps(private))
    return packet, private, path, inputs / "packet.json"


def test_identity_transform_and_unmasked_behavior(tmp_path):
    p, _, mp, ip = fixture(tmp_path)
    assert m._masked_synthetic_leads(p, {"source_leads": ["Robinhood to list LIT"]}, mp, ip) == [
        "IssuerSeven to list ASSETK"
    ]
    p.pop("masking")
    assert m._masked_synthetic_leads(p, {"source_leads": ["Robinhood to list LIT"]}, None, ip) == [
        "Robinhood to list LIT"
    ]


@pytest.mark.parametrize(
    "bad",
    [
        "no-map",
        "wrong-map",
        "meaning",
        "authority",
        "time",
        "source",
        "registry",
        "nonreversible",
        "map-in-inputs",
    ],
)
def test_fail_closed(tmp_path, bad):
    p, private, mp, ip = fixture(tmp_path)
    if bad == "no-map":
        mp = None
    elif bad == "wrong-map":
        p["masking"]["map_sha256"] = "wrong"
    elif bad == "meaning":
        private["replacements"]["list"] = "cancel"
    elif bad == "authority":
        p["fixture_reference"]["authoritative_input_sha256"] = "changed"
    elif bad == "time":
        p["cutoff"] = "2027-01-01"
    elif bad == "source":
        p["records"][0]["record"]["text"] = "IssuerSeven will not list ASSETK"
    elif bad == "registry":
        Path(private["original_registry_path"]).write_text("{}")
    elif bad == "nonreversible":
        private["replacements"]["LIT"] = "IssuerSeven"
    elif bad == "map-in-inputs":
        mp = ip.parent / "private.json"
    if mp is not None:
        mp.write_text(json.dumps(private))
    if bad != "wrong-map":
        p["masking"]["map_sha256"] = sha256(canonical_json(private).encode())
    with pytest.raises(ValueError):
        m._masked_synthetic_leads(p, {"source_leads": ["Robinhood to list LIT"]}, mp, ip)


@pytest.mark.parametrize("bad", ["raw", "alias-prose", "unknown-identity", "author", "outside-scope"])
def test_additional_boundaries(tmp_path, bad):
    p, private, mp, ip = fixture(tmp_path)
    if bad == "raw":
        Path(p["records"][0]["as_of_evidence"]["raw_ref"]).write_text("IssuerSeven cancels ASSETK")
    elif bad == "alias-prose":
        private["replacements"]["Robinhood"] = "IssuerSeven cancels listings"
    elif bad == "unknown-identity":
        private["replacements"]["to"] = "not"
    elif bad == "author":
        p["records"][0]["record"]["author"] = "UnrelatedAuthor"
    elif bad == "outside-scope":
        p["fixture_reference"]["fixture_id"] = "F-05"
    mp.write_text(json.dumps(private))
    p["masking"]["map_sha256"] = sha256(canonical_json(private).encode())
    with pytest.raises(ValueError):
        m._masked_synthetic_leads(p, {"source_leads": ["Robinhood to list LIT"]}, mp, ip)


@pytest.mark.parametrize(
    "field,value",
    [
        ("author_role", "FOUNDER_C_LEVEL"),
        ("subtype", "CONFIRMATION"),
        ("source_class", "PRIMARY"),
        ("published_at_prec", "EXACT"),
        ("known_to_us_at_prec", "EXACT"),
        ("known_to_us_at", "2030-01-01T00:00:00Z"),
    ],
)
def test_no_unbound_role_or_clock_fields(tmp_path, field, value):
    p, _, mp, ip = fixture(tmp_path)
    p["records"][0]["record"][field] = value
    with pytest.raises(ValueError, match="metadata"):
        m._masked_synthetic_leads(p, {"source_leads": ["Robinhood to list LIT"]}, mp, ip)


def signed_fixture(tmp_path, fixture_id="F-17"):
    import base64

    p, private, mp, ip = fixture(tmp_path)
    op = Path(private["original_packet_path"])
    original = json.loads(op.read_text())
    original["fixture_reference"]["fixture_id"] = fixture_id
    original["records"][0]["record"].update(text="Robinhood to list LIT", published_at="2026-01-01T00:00:00Z")
    payload = {
        "hash": "proposal-one",
        "data": {"message": {"body": "Robinhood to list LIT", "timestamp": 1767225600}},
    }
    raw = json.dumps(payload).encode()
    raw_path = op.parent / "signed.json"
    raw_path.write_bytes(raw)
    cid = "b" + base64.b32encode(
        bytes.fromhex("01551220") + bytes.fromhex(sha256(raw))
    ).decode().lower().rstrip("=")
    proof = original["records"][0]["as_of_evidence"]
    proof.update(
        kind="IMMUTABLE_ORIGINAL_PUBLICATION",
        derivation={"kind": "SNAPSHOT_SIGNED_IPFS_V1"},
        version_id=cid,
        proposal_id="proposal-one",
        raw_ref=str(raw_path),
        raw_sha256=sha256(raw),
    )
    op.write_text(json.dumps(original))
    private["source_sha256"] = sha256(canonical_json(original).encode())
    p = copy.deepcopy(original)
    p["records"][0]["record"].update(text="IssuerSeven to list ASSETK", author="IssuerSeven")
    masked_raw = raw.replace(b"Robinhood", b"IssuerSeven").replace(b"LIT", b"ASSETK")
    masked_path = ip.parent / "signed-masked.json"
    masked_path.write_bytes(masked_raw)
    p["records"][0]["as_of_evidence"].update(raw_ref=str(masked_path), raw_sha256=sha256(masked_raw))
    p["masking"] = {"applied": True, "map_sha256": sha256(canonical_json(private).encode())}
    mp.write_text(json.dumps(private))
    return p, private, mp, ip


@pytest.mark.parametrize("fixture_id", ["F-01", "F-17"])
def test_signed_original_cid_then_exact_masked_view(tmp_path, fixture_id):
    p, _, mp, ip = signed_fixture(tmp_path, fixture_id)
    context = m._host_identity_mask(p, mp, ip)
    row = p["records"][0]
    raw = Path(row["as_of_evidence"]["raw_ref"]).read_bytes()
    candidate = m
    with pytest.raises(ValueError, match="CID"):
        candidate.verify_derivation(row["record"], row["as_of_evidence"], raw)
    candidate.verify_derivation(
        row["record"], row["as_of_evidence"], raw, signed_original=context["signed_originals"]["one"]
    )
    candidate.verify_derivation(*context["signed_originals"]["one"])


@pytest.mark.parametrize("bad", ["original-cid", "masked-body", "original-role", "precision"])
@pytest.mark.parametrize("fixture_id", ["F-01", "F-17"])
def test_signed_derivative_negatives(tmp_path, bad, fixture_id):
    p, private, mp, ip = signed_fixture(tmp_path, fixture_id)
    if bad == "original-cid":
        op = Path(private["original_packet_path"])
        original = json.loads(op.read_text())
        original["records"][0]["as_of_evidence"]["version_id"] = "bwrong"
        p["records"][0]["as_of_evidence"]["version_id"] = "bwrong"
        op.write_text(json.dumps(original))
        private["source_sha256"] = sha256(canonical_json(original).encode())
    elif bad == "masked-body":
        rp = Path(p["records"][0]["as_of_evidence"]["raw_ref"])
        rp.write_bytes(rp.read_bytes().replace(b"to list", b"will not list"))
        p["records"][0]["as_of_evidence"]["raw_sha256"] = sha256(rp.read_bytes())
    elif bad == "original-role":
        p["records"][0]["record"]["author_role"] = "FOUNDER_C_LEVEL"
    else:
        p["records"][0]["record"]["published_at_precision"] = "DAY"
    mp.write_text(json.dumps(private))
    p["masking"]["map_sha256"] = sha256(canonical_json(private).encode())
    with pytest.raises(ValueError):
        m._host_identity_mask(p, mp, ip)


def test_f25_explicit_already_anonymous_noop(tmp_path):
    p, private, mp, ip = fixture(tmp_path)
    op = Path(private["original_packet_path"])
    original = json.loads(op.read_text())
    original["fixture_reference"]["fixture_id"] = "F-25"
    op.write_text(json.dumps(original))
    private.update(
        replacements={}, already_anonymous=True, source_sha256=sha256(canonical_json(original).encode())
    )
    p = copy.deepcopy(original)
    p["masking"] = {
        "applied": True,
        "already_anonymous": True,
        "map_sha256": sha256(canonical_json(private).encode()),
    }
    mp.write_text(json.dumps(private))
    assert m._masked_synthetic_leads(p, {"source_leads": ["Robinhood to list LIT"]}, mp, ip) == [
        "Robinhood to list LIT"
    ]
    p["records"][0]["record"]["author_role"] = "PRIMARY"
    with pytest.raises(ValueError):
        m._host_identity_mask(p, mp, ip)


def test_explicit_exact_case_variants_are_reversible(tmp_path):
    p, private, mp, ip = fixture(tmp_path)
    private["replacements"] = {"Robinhood": "IssuerSeven", "LIT": "ASSETK", "Lit": "AssetName"}
    private["case_sensitive_names"] = ["LIT", "Lit"]
    mp.write_text(json.dumps(private))
    p["masking"]["map_sha256"] = sha256(canonical_json(private).encode())
    context = m._host_identity_mask(p, mp, ip)
    assert context["transform"]("LIT Lit") == "ASSETK AssetName"
    assert context["transform"]("ASSETK AssetName", True) == "LIT Lit"


@pytest.mark.parametrize("bad", [None, "contract", "body", "symbol"])
@pytest.mark.parametrize("symbol", ["USAT", "USDQ"])
def test_structured_signed_token_identity(tmp_path, bad, symbol):
    import base64

    p, private, mp, ip = signed_fixture(tmp_path)
    op = Path(private["original_packet_path"])
    original = json.loads(op.read_text())
    row = original["records"][0]
    address = "0x07041776f5007ACa2A54844F50503a18A72A8b68"
    body = f"Onboard {symbol}. Token Address: [{address}](https://etherscan.io/address/{address})"
    row["record"].update(text=body, title=f"[ARFC] Onboard {symbol} on Ethereum")
    payload = {"hash": "proposal-one", "data": {"message": {"body": body, "timestamp": 1767225600}}}
    raw = json.dumps(payload).encode()
    raw_path = Path(row["as_of_evidence"]["raw_ref"])
    raw_path.write_bytes(raw)
    cid = "b" + base64.b32encode(
        bytes.fromhex("01551220") + bytes.fromhex(sha256(raw))
    ).decode().lower().rstrip("=")
    row["as_of_evidence"].update(raw_sha256=sha256(raw), version_id=cid)
    op.write_text(json.dumps(original))
    private["source_sha256"] = sha256(canonical_json(original).encode())
    private["replacements"][symbol] = "AssetZ"
    private["source_token_identity"] = {
        "item_id": row["record"].get("item_id"),
        "body_sha256": sha256(body.encode()),
        "symbol": symbol,
        "chain": "Ethereum",
        "contract_address": address,
    }

    row["record"]["item_id"] = "original-item"
    private["source_token_identity"]["item_id"] = "original-item"
    op.write_text(json.dumps(original))
    private["source_sha256"] = sha256(canonical_json(original).encode())
    p = copy.deepcopy(original)
    p["records"][0]["record"].update(
        text=body.replace(symbol, "AssetZ"), title="[ARFC] Onboard AssetZ on Ethereum", author="IssuerSeven"
    )
    masked_path = ip.parent / "token-masked.json"
    masked_path.write_bytes(raw.replace(symbol.encode(), b"AssetZ"))
    p["records"][0]["as_of_evidence"].update(
        raw_ref=str(masked_path), raw_sha256=sha256(masked_path.read_bytes())
    )
    if bad == "contract":
        private["source_token_identity"]["contract_address"] = "0x" + "0" * 40
    elif bad == "body":
        private["source_token_identity"]["body_sha256"] = "wrong"
    elif bad == "symbol":
        private["source_token_identity"]["symbol"] = "list"
    p["masking"] = {"applied": True, "map_sha256": sha256(canonical_json(private).encode())}
    mp.write_text(json.dumps(private))
    if bad:
        with pytest.raises(ValueError):
            m._host_identity_mask(p, mp, ip)
    else:
        context = m._host_identity_mask(p, mp, ip)
        assert context["signed_originals"]["one"][1]["version_id"] == cid


def test_private_map_shape_does_not_ban_public_replacements(tmp_path):
    path = tmp_path / "input.json"
    packet = {
        "cutoff": "2026-09-24T12:00:00Z",
        "config_version": "test",
        "code_version": "test",
        "registry": {},
        "config": {},
        "records": [],
        "tool_results": [],
        "provider_response": {"replacements": ["public provider field"]},
    }
    settings = SimpleNamespace(config_version="test", code_version="test")
    path.write_text(json.dumps(packet))
    assert load_inputs(path, settings).cutoff == packet["cutoff"]
    for private in (
        {"replacements": {}, "source_sha256": "private original binding"},
        {"original_packet_path": "/private/original.json"},
    ):
        packet["provider_response"] = private
        path.write_text(json.dumps(packet))
        with pytest.raises(ReplayError, match="private name masking maps"):
            load_inputs(path, settings)
