import copy
import json
from datetime import UTC
from pathlib import Path

import httpx
import pytest
from model_fixture import audited_result, model_arguments
from native_input_fixtures import acknowledge_native_input
from native_session_fixtures import created_native_session
from source_support import fixture_versions

from tank.replay import ReplayError, digest, load_inputs, original_replay_messages, recorded_page_access
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.util import parse_time, sha256


@pytest.fixture
def reference_packet(tmp_path: Path):
    body = "Synthetic original official publication text; fixture evidence only."
    raw = ("<html><time>21 Sep 2022</time><p>" + body + "</p></html>").encode()
    path = tmp_path / "original-publication.html"
    path.write_bytes(raw)
    actual = "2026-10-19T00:00:00Z"
    publication = "2022-09-21T00:00:00Z"
    cutoff = "2022-09-22T00:00:00Z"
    record = {
        "item_id": "reference-source",
        "source_id": "S01",
        "external_id": "original-v1",
        "source_class": "PRIMARY",
        "text": body,
        "title": "Original official title",
        "feed": "https://fixture.invalid/original",
        "raw_ref": str(path),
        "raw_sha256": sha256(raw),
        "content_hash": sha256(body),
        "observed_at": actual,
        "observed_at_prec": "COLLECTION_ONLY",
        "acquired_at": actual,
        "published_at": publication,
        "published_at_precision": "BUCKET(1d)",
        "relevance": {"decision": "ALWAYS"},
        "extraction_status": "PENDING",
    }
    packet = {
        "replay_mode": "FIXTURE_REFERENCE_RECONSTRUCTION",
        "fixture_reference": {
            "schema": "tank.fixture-reference.v1",
            "fixture_id": "F-18",
            "preregistered_at": actual,
            "cutoff_spec": "21 Sep 2022",
            "cutoff_policy": "UTC_DAY_END_EXCLUSIVE",
            "cutoff_day": "2022-09-21",
            "cutoff_timezone": "UTC",
            "historical_live_run": False,
        },
        "cutoff": cutoff,
        "assembled_at": actual,
        "config_version": "test",
        "code_version": "test",
        "registry": {},
        "config": {
            "replay_messages": [
                {
                    "kind": "NEW_ITEM",
                    "item_id": record["item_id"],
                    "origin": "FIXTURE_AUTHORED_REFERENCE",
                    "historical": False,
                    "replay_at": cutoff,
                }
            ]
        },
        "tool_results": [],
        "records": [
            {
                "collection": "ingest.source_items",
                "key": record["item_id"],
                "available_at": actual,
                "simulated_available_at": cutoff,
                "simulated_availability_basis": "CONSERVATIVE_PUBLICATION_UPPER_BOUND",
                "as_of_evidence": {
                    "kind": "IMMUTABLE_ORIGINAL_PUBLICATION",
                    "version_id": "original-v1",
                    "source_url": record["feed"],
                    "raw_ref": str(path),
                    "raw_sha256": sha256(raw),
                    "body_sha256": sha256(body.encode()),
                },
                "record": record,
            }
        ],
    }
    return tmp_path / "reference.json", packet


def save(path, packet) -> None:
    packet.pop("input_sha256", None)
    packet["input_sha256"] = digest(packet)
    path.write_text(json.dumps(packet))


def settings():
    return Settings(replay=True, config_version="test", code_version="test", _env_file=None)


def test_explicit_first_reference_preserves_actual_receipts_and_day_bucket(reference_packet) -> None:
    path, packet = reference_packet
    frozen = copy.deepcopy(packet)
    save(path, packet)
    inputs = load_inputs(path, settings())
    assert inputs.evidence_mode == "FIXTURE_REFERENCE_RECONSTRUCTION"
    assert not inputs.records("ingest.source_items", cutoff="2022-09-21T12:00:00Z")
    visible = inputs.records("ingest.source_items", cutoff=packet["cutoff"])[0]
    assert visible["observed_at"] == "2026-10-19T00:00:00Z"
    assert visible["published_at_precision"] == "BUCKET(1d)"
    assert original_replay_messages(inputs)[0]["origin"] == "FIXTURE_AUTHORED_REFERENCE"
    assert packet["records"] == frozen["records"]
    assert inputs.record_visible(inputs._records[0], parse_time(packet["cutoff"]))


def test_captured_unlock_calendar_uses_observation_time_not_future_release(reference_packet) -> None:
    path, packet = reference_packet
    observed = "2022-09-21T12:00:00Z"
    schedule = {
        "collection": "mkt.unlocks",
        "key": "asset-k:vesting",
        "available_at": observed,
        "record": {
            "token_id": "asset-k",
            "date": "2022-09-25T00:00:00Z",
            "date_prec": "BUCKET(1d)",
            "observed_at": observed,
            "observed_at_prec": "COLLECTION_ONLY",
            "label": "Contributors",
            "qty": "1000",
            "kind": "POTENTIAL_SUPPLY",
            "sale_inferred": False,
        },
    }
    packet["records"].append(schedule)
    save(path, packet)
    inputs = load_inputs(path, settings())
    assert inputs.records("mkt.unlocks", cutoff="2022-09-21T11:59:59Z") == []
    assert inputs.records("mkt.unlocks") == [schedule["record"]]

    late = packet["assembled_at"]
    schedule["available_at"] = schedule["record"]["observed_at"] = late
    save(path, packet)
    with pytest.raises(ReplayError, match="late unlock capture must be declared HISTORICAL_RECONSTRUCTION"):
        load_inputs(path, settings())


@pytest.mark.parametrize("bad", ["backdated", "simulated", "analysis_seed"])
def test_unlock_calendar_cannot_backdate_or_seed_a_verdict(reference_packet, bad) -> None:
    path, packet = reference_packet
    observed = "2022-09-21T12:00:00Z"
    row = {
        "collection": "mkt.unlocks",
        "key": "asset-k:vesting",
        "available_at": observed,
        "record": {"token_id": "asset-k", "date": "2022-09-25T00:00:00Z", "observed_at": observed},
    }
    if bad == "backdated":
        row["record"]["observed_at"] = packet["assembled_at"]
    elif bad == "simulated":
        row["simulated_available_at"] = observed
    else:
        row["record"]["expected_action"] = "WAIT"
    packet["records"].append(row)
    save(path, packet)
    with pytest.raises(ReplayError):
        load_inputs(path, settings())


@pytest.mark.parametrize(
    "change",
    [
        "no_mode",
        "early_bucket",
        "receipt_after_assembly",
        "bad_raw_hash",
        "bad_body_hash",
        "event_seed",
        "historical_message",
        "changed_cutoff_literal",
        "missing_hash",
    ],
)
def test_reference_exception_is_explicit_frozen_and_as_of_only(reference_packet, change) -> None:
    path, packet = reference_packet
    row = packet["records"][0]
    if change == "no_mode":
        packet.pop("replay_mode")
    elif change == "early_bucket":
        row["simulated_available_at"] = "2022-09-21T00:00:00Z"
    elif change == "receipt_after_assembly":
        row["record"]["observed_at"] = "2026-10-20T00:00:00Z"
    elif change == "bad_raw_hash":
        row["as_of_evidence"]["raw_sha256"] = "a" * 64
    elif change == "bad_body_hash":
        row["as_of_evidence"]["body_sha256"] = "a" * 64
    elif change == "event_seed":
        packet["records"].append(
            {
                "collection": "evt.events",
                "key": "old",
                "available_at": packet["cutoff"],
                "record": {"event_id": "old"},
            }
        )
    elif change == "historical_message":
        packet["config"]["replay_messages"][0]["historical"] = True
    elif change == "changed_cutoff_literal":
        packet["fixture_reference"]["cutoff_spec"] = "20 Sep 2022"
    save(path, packet)
    if change == "missing_hash":
        packet.pop("input_sha256")
        path.write_text(json.dumps(packet))
    with pytest.raises(ReplayError):
        load_inputs(path, settings())


def test_exact_reference_cutoff_keeps_original_human_literal(reference_packet) -> None:
    path, packet = reference_packet
    chosen = "2022-09-21T15:02:07Z"
    packet["fixture_reference"].update(
        cutoff_spec="21 Sep2022 15:02:07", cutoff_policy="EXACT", cutoff_instant=chosen
    )
    packet["cutoff"] = chosen
    packet["records"][0]["record"].update(published_at="2022-09-21T15:01:00Z", published_at_precision="EXACT")
    packet["records"][0]["simulated_available_at"] = chosen
    packet["config"]["replay_messages"][0]["replay_at"] = chosen
    save(path, packet)
    inputs = load_inputs(path, settings())
    assert inputs.fixture_reference["cutoff_spec"] == "21 Sep2022 15:02:07"
    assert inputs.cutoff == chosen


def test_trusted_reference_clock_binds_body_and_preserves_physical_observation(reference_packet) -> None:
    from tank.fixture_reference import TrustedFixtureClock

    path, packet = reference_packet
    save(path, packet)
    inputs = load_inputs(path, settings())
    clock = TrustedFixtureClock(inputs)
    source = inputs.records("ingest.source_items")[0]
    assert clock.known_at(source).value == packet["cutoff"]
    assert clock.known_at(source).precision == "EXACT"
    provenance = clock.provenance(source)
    assert provenance["clock_domain"] == "FIXTURE_SIMULATED_AS_OF"
    assert provenance["actual_observed_at"] == source["observed_at"] == "2026-10-19T00:00:00Z"
    assert provenance["actual_observed_at_prec"] == "COLLECTION_ONLY"
    assert provenance["preregistered_input_sha256"] == packet["input_sha256"]
    assert provenance["historical_live_performance_claim"] is False
    with pytest.raises(AttributeError):
        clock._fixture_id = "changed"
    with pytest.raises(ValueError, match="differs"):
        clock.known_at({**source, "text": "Different or future publication body"})
    with pytest.raises(ValueError, match="unique"):
        clock.known_at({**source, "item_id": "unknown"})


def test_minute_only_fixture_cutoff_remains_insufficient(reference_packet) -> None:
    path, packet = reference_packet
    packet["fixture_reference"].update(
        cutoff_spec="11 Oct2026 03:30", cutoff_policy="EXACT", cutoff_instant="2026-10-11T03:30:00Z"
    )
    packet["cutoff"] = "2026-10-11T03:30:00Z"
    save(path, packet)
    with pytest.raises(ReplayError, match="Minute-only.*insufficient"):
        load_inputs(path, settings())


def test_declared_raw_derivation_rejects_independently_matching_false_body(reference_packet) -> None:
    from pathlib import Path

    from tank.fixture_reference import verify_derivation
    from tank.sources import parsers

    row = reference_packet[1]["records"][0]
    raw = Path(row["record"]["raw_ref"]).read_bytes()
    proof = {
        **row["as_of_evidence"],
        "derivation": {
            "kind": "GOVINFO_HTML_TANK_TEXT_V1",
            "module_sha256": sha256(Path(parsers.__file__).read_bytes()),
        },
    }
    record = {**row["record"], "text": parsers.text(raw)}
    verify_derivation(record, proof, raw)
    record["text"] = "Unrelated independently hashed text"
    proof["body_sha256"] = sha256(record["text"].encode())
    with pytest.raises(ValueError, match="does not derive"):
        verify_derivation(record, proof, raw)


async def test_operational_runner_injects_only_exact_preregistered_reference_clock(
    reference_packet, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tank import replay
    from tank.fixture_reference import TrustedFixtureClock
    from tank.storage import MemoryRepository

    path, packet = reference_packet
    packet["config"]["thresholds"] = Settings(_env_file=None).load_config("thresholds")
    save(path, packet)
    options = settings().model_copy(update={"data_dir": tmp_path / "runtime"})
    called = []

    async def terminal(bundle, repo, settings, tools, **kwargs):
        clock = tools.reference_clock
        assert type(clock) is TrustedFixtureClock
        assert clock.input_sha256 == packet["input_sha256"]
        source = packet["records"][0]["record"]
        assert clock.known_at(source).value == packet["cutoff"]
        assert source["observed_at"] == "2026-10-19T00:00:00Z"
        called.append(True)
        return {"reference": True}

    monkeypatch.setattr(replay, "replay_agent_session", terminal)
    result = await replay.run_operational_replay(path, MemoryRepository(), options, http=object())
    assert result == {"reference": True}
    assert called == [True]


def test_minute_selected_start_keeps_later_source_ineligible(reference_packet) -> None:
    path, packet = reference_packet
    ref = packet["fixture_reference"]
    ref.update(
        cutoff_spec="21 Sep 2022 03:30",
        cutoff_policy="MINUTE_START",
        cutoff_instant="2022-09-21T03:30:00Z",
        original_call_precision="BUCKET(1m)",
        chosen_boundary_basis="CALLER_SELECTED_INTERVAL_START",
    )
    packet["cutoff"] = ref["cutoff_instant"]
    row = packet["records"][0]
    row["record"].update(published_at="2022-09-21T03:29:00Z", published_at_precision="EXACT")
    row["simulated_available_at"] = "2022-09-21T03:29:00Z"
    packet["config"]["replay_messages"][0]["replay_at"] = packet["cutoff"]
    save(path, packet)
    assert load_inputs(path, settings()).fixture_reference["original_call_precision"] == "BUCKET(1m)"
    row["record"]["published_at"] = "2022-09-21T03:30:08.368Z"
    row["simulated_available_at"] = row["record"]["published_at"]
    save(path, packet)
    with pytest.raises(ReplayError, match="publication precision interval"):
        load_inputs(path, settings())


def test_manifest_bound_synthetic_relative_source(reference_packet) -> None:
    from pathlib import Path

    path, packet = reference_packet
    manifest = (
        Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "authoritative-definitions-v1.json"
    )
    ref = packet["fixture_reference"]
    ref.update(
        fixture_id="F-24",
        cutoff_spec="post + 1 min",
        cutoff_policy="RELATIVE_DECLARED",
        relative_anchor="2022-09-21T00:00:00Z",
        relative_anchor_prec="EXACT",
        relative_anchor_origin="DECLARED_SYNTHETIC_COORDINATE",
        offset_seconds=60,
        cutoff_instant="2022-09-21T00:01:00Z",
        authoritative_manifest_sha256=sha256(manifest.read_bytes()),
        authoritative_input_sha256=sha256(
            (
                Path(__file__).resolve().parents[1] / "src/tank/resources/fixture-input-authority-v1.json"
            ).read_bytes()
        ),
    )
    packet["cutoff"] = ref["cutoff_instant"]
    row = packet["records"][0]
    body = 'Declared synthetic @tier10k: "Robinhood to list LIT"'
    raw_path = Path(row["record"]["raw_ref"])
    raw_path.write_text(body)
    row["record"].update(
        text=body,
        raw_sha256=sha256(body.encode()),
        published_at=ref["relative_anchor"],
        published_at_precision="EXACT",
    )
    row["as_of_evidence"].update(
        kind="DECLARED_SYNTHETIC_SOURCE",
        source_url="fixture://F-24/source",
        raw_sha256=sha256(body.encode()),
        body_sha256=sha256(body.encode()),
        derivation={"kind": "DECLARED_UTF8_SOURCE_V1"},
    )
    row["simulated_available_at"] = ref["relative_anchor"]
    packet["config"]["replay_messages"][0]["replay_at"] = packet["cutoff"]
    save(path, packet)
    assert load_inputs(path, settings()).fixture_reference["fixture_id"] == "F-24"
    ref["fixture_id"] = "F-22"
    save(path, packet)
    with pytest.raises(ReplayError, match="declared synthetic"):
        load_inputs(path, settings())


def test_signed_snapshot_cid_binds_body_identity_and_publication() -> None:
    import base64
    import hashlib

    from tank.fixture_reference import verify_derivation

    raw = json.dumps(
        {"hash": "proposal-id", "data": {"message": {"body": "Original proposal", "timestamp": 1663718400}}}
    ).encode()
    cid = "b" + base64.b32encode(
        bytes.fromhex("01551220") + hashlib.sha256(raw).digest()
    ).decode().lower().rstrip("=")
    proof = {
        "version_id": cid,
        "proposal_id": "proposal-id",
        "derivation": {"kind": "SNAPSHOT_SIGNED_IPFS_V1"},
    }
    record = {"text": "Original proposal", "published_at": "2022-09-21T00:00:00Z"}
    verify_derivation(record, proof, raw)
    with pytest.raises(ValueError, match="CID does not match"):
        verify_derivation(record, proof, raw + b" ")
    with pytest.raises(ValueError, match="timestamp mismatch"):
        verify_derivation({**record, "published_at": "2022-09-22T00:00:00Z"}, proof, raw)


@pytest.mark.parametrize(
    "kind,selector,original",
    [
        (
            "DISCOURSE_FIRST_POST_V1",
            "/post_stream/posts/0",
            {
                "post_stream": {
                    "posts": [
                        {
                            "cooked": "<p>Original</p>",
                            "created_at": "2022-01-01",
                            "updated_at": "2022-01-01",
                            "version": 1,
                            "username": "author",
                        },
                        {"cooked": "FUTURE_CANARY"},
                    ]
                },
                "latest_activity": "FUTURE_CANARY",
            },
        ),
        (
            "BINANCE_CMS_ORIGINAL_V1",
            "/data",
            {
                "data": {
                    "body": "Original",
                    "version": "1",
                    "lastUpdateTime": 0,
                    "relatedArticles": ["FUTURE_CANARY"],
                }
            },
        ),
        (
            "BYBIT_LIST_SELECTED_V1",
            "/result/list/0",
            {"result": {"list": [{"title": "Original", "description": "Lead"}, {"title": "FUTURE_CANARY"}]}},
        ),
    ],
)
def test_selected_original_transport_excludes_future_canary(kind, selector, original) -> None:
    from tank.fixture_reference import project_original_source

    raw = json.dumps(original).encode()
    projected = project_original_source(kind, raw, selector)
    assert b"FUTURE_CANARY" not in projected
    assert b"Original" in projected
    assert b"FUTURE_CANARY" in raw
    assert project_original_source(kind, raw, selector) == projected


def test_packaged_fixture_authority_contains_inputs_only() -> None:
    from importlib.resources import files

    authority = json.loads(files("tank").joinpath("resources/fixture-input-authority-v1.json").read_bytes())
    assert authority["schema"] == "tank.fixture-input-authority.v1"
    assert all(
        set(case) <= {"fixture_id", "synthetic", "cutoff_spec", "source_leads"} for case in authority["cases"]
    )
    assert len(authority["cases"]) == 27
    assert not any(
        word in json.dumps(authority).lower()
        for word in ("assertion_spec", "expected_action", "expected_grade")
    )


def test_insufficient_catalog_refuses_before_configuration_or_session(tmp_path: Path) -> None:
    path = tmp_path / "missing.json"
    path.write_text(
        json.dumps(
            {
                "fixture_status": "INSUFFICIENT_FIXTURE",
                "fixture_id": "F-22",
                "missing": ["No immutable pre-cutoff source"],
                "cutoff_spec": "03:30",
            }
        )
    )
    with pytest.raises(ReplayError, match="F-22: No immutable pre-cutoff source"):
        load_inputs(path, settings())


def test_declared_empty_synthetic_lookup_is_not_a_fabricated_source(reference_packet) -> None:

    from tank.fixture_reference import validate

    _path, packet = reference_packet

    packet["tool_results"] = [
        {"origin": "DECLARED_SYNTHETIC_TOOL_RESPONSE", "tool": "fetch_page", "result": {"items": []}}
    ]
    packet["input_sha256"] = digest(packet)
    with pytest.raises(ValueError, match="packaged input authority"):
        validate(packet, lambda row: False)


def test_retrospective_day_schedule_is_narrow_to_authorized_tch_cases(reference_packet) -> None:
    path, packet = reference_packet
    ref = packet["fixture_reference"]
    ref.update(
        fixture_id="F-08",
        cutoff_spec="21 Sep 2022 03:30",
        cutoff_policy="MINUTE_START",
        cutoff_instant="2022-09-21T03:30:00Z",
        original_call_precision="BUCKET(1m)",
        chosen_boundary_basis="CALLER_SELECTED_INTERVAL_START",
    )
    packet["cutoff"] = ref["cutoff_instant"]
    row = packet["records"][0]
    row["as_of_evidence"]["kind"] = "RETROSPECTIVE_BEHAVIOR_REFERENCE"
    row["simulated_available_at"] = packet["cutoff"]
    row["simulated_availability_basis"] = "FIXTURE_CASE_REFERENCE_SCHEDULE"
    packet["config"]["replay_messages"][0]["replay_at"] = packet["cutoff"]
    save(path, packet)
    assert load_inputs(path, settings()).fixture_reference["fixture_id"] == "F-08"
    row["record"].update(published_at="2022-09-21T03:30:08.368Z", published_at_precision="EXACT")
    save(path, packet)
    with pytest.raises(ReplayError, match="publication precision interval"):
        load_inputs(path, settings())


async def test_reference_missing_market_is_unknown_not_zero_or_captured(
    reference_packet, tmp_path: Path
) -> None:
    from importlib.resources import files

    from tank.analysis.truth import compute_market_truth
    from tank.fixture_reference import MISSING_MARKET_FIELDS
    from tank.replay import ReplayMarketReader

    path, packet = reference_packet
    authority_raw = files("tank").joinpath("resources/fixture-input-authority-v1.json").read_bytes()
    packet["fixture_reference"].update(
        authoritative_input_sha256=sha256(authority_raw),
        authoritative_manifest_sha256=json.loads(authority_raw)["original_manifest_sha256"],
    )
    missing = ["flows", "books", "open_interest", "funding", "prices"]
    actual = packet["assembled_at"]
    inventory = tmp_path / "retained-inventory.json"
    inventory.write_text(
        json.dumps(
            {
                "token_id": "uniswap:UNI",
                "retained_observation_count": 0,
                "missing_inputs": missing,
                "actual_checked_at": actual,
                "inventory_scope": "Approved retained fixture corpus at selected cutoff, not production database",
            }
        )
    )
    row = {
        "collection": "replay.market_snapshots",
        "key": "missing-uni",
        "available_at": actual,
        "simulated_available_at": packet["cutoff"],
        "missing_market_provenance": {
            "inventory_ref": str(inventory),
            "inventory_sha256": sha256(inventory.read_bytes()),
        },
        "record": {
            "artifact_kind": "REFERENCE_MISSING_MARKET_INPUTS",
            "token_id": "uniswap:UNI",
            "cutoff": packet["cutoff"],
            "source_capture_claim": False,
            "actual_declaration_at": actual,
            "actual_declaration_at_prec": "COLLECTION_ONLY",
            "missing_inputs": missing,
            **{field: [] for field in MISSING_MARKET_FIELDS},
        },
    }
    packet["records"].append(row)
    save(path, packet)
    inputs = load_inputs(path, settings())
    snapshot = await ReplayMarketReader(inputs).snapshot("uniswap:UNI", packet["cutoff"])
    truth = compute_market_truth(snapshot)
    assert truth["move_class"] == "UNKNOWN"
    assert snapshot["flow_bars"] == []
    assert snapshot["source_capture_claim"] is False
    assert snapshot["unavailable"]["flows"] == "NO_RETAINED_REFERENCE_OBSERVATIONS"
    row["record"]["flow_bars"] = [{"buy_qty": 0, "sell_qty": 0}]
    save(path, packet)
    with pytest.raises(ReplayError, match="cannot contain observations"):
        load_inputs(path, settings())
    row["record"]["flow_bars"] = []
    packet.pop("replay_mode")
    packet.pop("fixture_reference")
    save(path, packet)
    with pytest.raises(ReplayError, match="explicit fixture reference"):
        load_inputs(path, settings())


def phased_packet(reference_packet):
    from importlib.resources import files

    path, packet = reference_packet
    reference = packet["fixture_reference"]
    reference.update(
        fixture_id="F-24",
        cutoff_spec="post + 1 min",
        cutoff_policy="RELATIVE_DECLARED",
        relative_anchor="2026-10-16T12:00:00Z",
        relative_anchor_prec="EXACT",
        relative_anchor_origin="DECLARED_SYNTHETIC_COORDINATE",
        offset_seconds=60,
        cutoff_instant="2026-10-16T12:01:00Z",
        authoritative_input_sha256=sha256(
            files("tank").joinpath("resources/fixture-input-authority-v1.json").read_bytes()
        ),
        authoritative_manifest_sha256="86cda04f53b7b8032655ed40ce53e78e2d123f9a146131b0afb91201121c334c",
        phases=[
            {"phase_id": "initial", "at": "2026-10-16T12:01:00Z", "at_prec": "EXACT", "message_indexes": [0]},
            {
                "phase_id": "confirmation",
                "at": "2026-10-16T12:05:00Z",
                "at_prec": "EXACT",
                "message_indexes": [1],
            },
        ],
    )
    packet["cutoff"] = "2026-10-16T12:05:00Z"
    packet["records"] = []
    for item, source, at in [
        ("lead", "S29", "2026-10-16T12:00:00Z"),
        ("confirmation", "S13", "2026-10-16T12:05:00Z"),
    ]:
        body = "Robinhood to list LIT" + (" FUTURE_CONFIRMATION_CANARY" if item == "confirmation" else "")
        raw_path = path.parent / (item + ".txt")
        raw_path.write_text(body)
        record = {
            "item_id": item,
            "source_id": source,
            "feed": "fixture://F-24/" + item,
            "external_id": item + "-v1",
            "source_class": "SOCIAL_LEAD" if item == "lead" else "PRIMARY",
            "declared_synthetic": True,
            "text": body,
            "title": body,
            "url": "fixture://F-24/" + item,
            "raw_ref": str(raw_path),
            "raw_path": str(raw_path),
            "raw_sha256": sha256(body),
            "content_hash": sha256(body),
            "observed_at": "2026-10-19T00:00:00Z",
            "observed_at_prec": "COLLECTION_ONLY",
            "published_at": at,
            "published_at_prec": "EXACT",
            "relevance": {"decision": "ALWAYS"},
            "extraction_status": "PENDING",
        }
        packet["records"].append(
            {
                "collection": "ingest.source_items",
                "key": item,
                "record": record,
                "available_at": record["observed_at"],
                "simulated_available_at": at,
                "simulated_availability_basis": "CONSERVATIVE_PUBLICATION_UPPER_BOUND",
                "as_of_evidence": {
                    "kind": "DECLARED_SYNTHETIC_SOURCE",
                    "version_id": item + "-v1",
                    "source_url": record["url"],
                    "raw_ref": str(raw_path),
                    "raw_sha256": sha256(body),
                    "body_sha256": sha256(body),
                    "derivation": {"kind": "DECLARED_UTF8_SOURCE_V1"},
                },
            }
        )
    packet["config"]["replay_messages"] = [
        {
            "kind": "NEW_ITEM" if i == 0 else "ITEM_ADDED",
            "origin": "FIXTURE_AUTHORED_REFERENCE",
            "historical": False,
            "item_id": item,
            "replay_at": reference["phases"][i]["at"],
            "replay_at_prec": "EXACT",
        }
        for i, item in enumerate(["lead", "confirmation"])
    ]
    return path, packet


def test_f24_declared_phases_preserve_initial_boundary_and_hide_future(reference_packet) -> None:

    path, packet = phased_packet(reference_packet)
    save(path, packet)
    inputs = load_inputs(path, settings())
    first = inputs.records("ingest.source_items", cutoff="2026-10-16T12:01:00Z")
    assert [r["item_id"] for r in first] == ["lead"]
    assert "FUTURE_CONFIRMATION_CANARY" not in json.dumps(first)
    assert [r["item_id"] for r in inputs.records("ingest.source_items")] == ["lead", "confirmation"]
    assert inputs.fixture_reference["cutoff_instant"] == "2026-10-16T12:01:00Z"
    assert inputs.cutoff == "2026-10-16T12:05:00Z"


@pytest.mark.parametrize("mutation", ["initial", "reverse", "case", "unassigned", "outcome", "early_source"])
def test_f24_phase_admission_refuses_invalid_timeline(reference_packet, mutation) -> None:
    path, p = phased_packet(reference_packet)
    if mutation == "initial":
        p["fixture_reference"]["cutoff_instant"] = "2026-10-16T12:02:00Z"
    elif mutation == "reverse":
        p["fixture_reference"]["phases"].reverse()
    elif mutation == "case":
        p["fixture_reference"]["fixture_id"] = "F-26"
        p["fixture_reference"]["cutoff_spec"] = "post + 1 min"
    elif mutation == "unassigned":
        p["fixture_reference"]["phases"][1]["message_indexes"] = []
    elif mutation == "outcome":
        p["fixture_reference"]["phases"][0]["expected_class"] = "MAJOR"
    else:
        p["records"][1]["simulated_available_at"] = "2026-10-16T12:01:00Z"
        p["records"][1]["record"]["published_at"] = "2026-10-16T12:01:00Z"
    save(path, p)
    with pytest.raises(ReplayError):
        load_inputs(path, settings())


@pytest.mark.parametrize(
    "tool,argument,kind,value",
    [
        ("fetch_page", "url", "EMPTY_OFFICIAL_FEED_LOOKUP", "https://robinhood.com/newsroom/"),
        ("web_search", "query", "EMPTY_OFFICIAL_SEARCH_LOOKUP", "Issuer Seven Asset K official announcement"),
    ],
)
async def test_phased_capture_and_market_cannot_advance_to_future(
    reference_packet, tool, argument, kind, value
) -> None:
    from tank.replay import ReplayMarketReader

    path, p = phased_packet(reference_packet)
    p["tool_results"] = [
        {
            "origin": "DECLARED_SYNTHETIC_TOOL_RESPONSE",
            "declared_response_kind": kind,
            "tool": tool,
            "arguments": {argument: value},
            "result": {"items": []},
            "status": "DONE",
            "is_error": False,
            "http_acquisition_claim": False,
            "actual_authored_at": p["assembled_at"],
            "available_at": p["assembled_at"],
            "simulated_available_at": "2026-10-16T12:01:00Z",
            "valid_until": "2026-10-16T12:05:00Z",
        }
    ]
    save(path, p)
    inputs = load_inputs(path, settings())
    args = {argument: value}
    assert inputs.reference_capture(tool, args, parse_time("2026-10-16T12:01:00Z"))["result"] == {"items": []}
    with pytest.raises(ReplayError, match="eligible"):
        inputs.reference_capture(tool, args, parse_time("2026-10-16T12:00:59Z"))
    with pytest.raises(ReplayError, match="eligible"):
        inputs.reference_capture(tool, args, parse_time("2026-10-16T12:05:00Z"))
    with pytest.raises(ReplayError, match="eligible"):
        inputs.reference_capture(tool, {argument: value + " different"}, parse_time("2026-10-16T12:01:00Z"))
    reader = ReplayMarketReader(inputs)
    reader.active_cutoff = lambda: parse_time("2026-10-16T12:01:00Z")
    with pytest.raises(ReplayError, match="active reference phase"):
        await reader.snapshot("T", "2026-10-16T12:05:00Z")


def test_uniform_minute_reference_convention_preserves_all_original_literals(reference_packet) -> None:
    import re
    from datetime import datetime, timedelta
    from pathlib import Path

    manifest = json.loads((Path(__file__).parent / "fixtures/manifest.json").read_text())
    cases = [c for c in manifest["fixtures"] if re.search(r"\b\d{1,2}:\d{2}(?: UTC)?$", c["cutoff_spec"])]
    assert len(cases) == 20
    for case in cases:
        path, p = copy.deepcopy(reference_packet)
        literal = case["cutoff_spec"]
        start = datetime.strptime(literal.removesuffix(" UTC"), "%d %b %Y %H:%M").replace(tzinfo=UTC)
        end = start + timedelta(minutes=1)
        boundary = end.isoformat().replace("+00:00", "Z")
        p["fixture_reference"].update(
            fixture_id=case["fixture_id"],
            cutoff_spec=literal,
            cutoff_policy="UTC_MINUTE_END_EXCLUSIVE",
            cutoff_instant=boundary,
            original_call_precision="BUCKET(1m)",
            chosen_boundary_basis="WHOLE_NAMED_MINUTE_END_EXCLUSIVE",
        )
        p["cutoff"] = boundary
        p["config"]["replay_messages"][0]["replay_at"] = boundary

        p["records"][0]["simulated_available_at"] = (
            (start + timedelta(seconds=8.368)).isoformat().replace("+00:00", "Z")
        )
        p["records"][0]["record"]["published_at"] = "2022-09-21T00:00:00Z"
        save(path, p)
        inputs = load_inputs(path, settings())
        assert inputs.fixture_reference["cutoff_spec"] == literal
        assert len(inputs.records("ingest.source_items")) == 1
        point = copy.deepcopy(p["records"][0])
        point["simulated_available_at"] = boundary
        assert not inputs.record_visible(point, parse_time(boundary))
        oi = {
            "collection": "mkt.oi_obs",
            "available_at": "2026-10-19T00:00:00Z",
            "record": {"ts": boundary, "ts_prec": "EXACT"},
        }
        assert not inputs.record_visible(oi, parse_time(boundary))
        bar = {
            "collection": "mkt.klines_1m",
            "available_at": "2026-10-19T00:00:00Z",
            "record": {"open_time": start.isoformat(), "open_time_prec": "EXACT", "interval_s": 60},
        }
        assert inputs.record_visible(bar, parse_time(boundary))


@pytest.mark.integration
@pytest.mark.parametrize("publication", [False, True])
async def test_actual_f24_phased_producers_keep_session_and_immutable_checkpoints(
    reference_packet, tmp_path: Path, publication, repository=None, fail_later=False, missing_lookup=None
) -> None:
    """Actual source/observation producers; MATERIAL stop isolates timeline mechanics from economic fixtures."""
    import asyncio

    from test_event_tools import unknown_assessment

    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import MarketTruthService
    from tank.event_tools import EventToolService
    from tank.fixture_reference import TrustedFixtureClock
    from tank.replay import ReplayMarketReader, replay_agent_session
    from tank.storage import MemoryRepository

    path, p = phased_packet(reference_packet)
    cfg = {
        "thresholds": Settings(_env_file=None).load_config("thresholds"),
        "semantics": {},
        "dependency_map": {},
    }
    cfg["registry"] = {"tokens": [], "entities": [], "instruments": []}
    cfg["sources"] = {"sources": []}
    if publication:
        baseline = Settings(_env_file=None)
        cfg = {
            name: baseline.load_config(name)
            for name in ("registry", "sources", "thresholds", "semantics", "dependency_map")
        }
        p["records"][0]["record"].update(author="@tier10k", author_label="@tier10k")
        p["records"][1]["record"].update(author_organization="Robinhood")
    cfg["openma"] = {
        "agent_id": "analyst",
        "environment_id": "analysis",
        "replay_environment_id": "replay",
        "prompt_version": "synthetic-phased-replay-prompt",
        "reasoning_effort": "medium",
    }
    if publication:
        p["tool_results"] = [
            {
                "origin": "DECLARED_SYNTHETIC_TOOL_RESPONSE",
                "declared_response_kind": "EMPTY_OFFICIAL_FEED_LOOKUP",
                "tool": "fetch_page",
                "arguments": {"url": "https://robinhood.com/newsroom/"},
                "result": {"items": []},
                "status": "DONE",
                "is_error": False,
                "http_acquisition_claim": False,
                "actual_authored_at": p["assembled_at"],
                "available_at": p["assembled_at"],
                "simulated_available_at": "2026-10-16T12:01:00Z",
                "valid_until": "2026-10-16T12:05:00Z",
            }
        ]
    p["registry"] = cfg["registry"]
    p["config"].update(cfg)
    p["config"].pop("registry")
    save(path, p)

    class Config(Settings):
        def load_config(self, name):
            return copy.deepcopy(cfg[name]) if name in cfg else super().load_config(name)

    config = Config(replay=True, code_version="test", config_version="test", _env_file=None)
    inputs = load_inputs(path, config)
    from tank.replay_evidence import ReplayEvidenceView
    from tank.time_precision import Timestamp

    view = ReplayEvidenceView(
        inputs,
        "attempt",
        tmp_path / "guest-prefix",
        tmp_path / "private-prefixes",
        input_sha256=sha256(path.read_bytes()),
    )
    capture = view.advance(Timestamp("2026-10-16T12:01:00Z", "EXACT"))
    repo = repository if repository is not None else MemoryRepository()
    queue = asyncio.Queue()
    accepted = []
    settled = set()
    sent = []
    created = []
    seq = 0
    stage = None
    item = None
    eid = None
    claim_id = None
    publications = []

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            while True:
                yield await queue.get()

    async def emit(event) -> None:
        if event.get("type") == "agent.custom_tool_use":
            event = dict(event, input=model_arguments(event["name"], event["input"]))
        nonlocal seq
        seq += 1
        if event["type"] == "session.status_idle":
            settled.update(accepted)
        await queue.put(("data: " + json.dumps(event) + "\n\n").encode())

    async def tool(name, args, execution) -> None:
        await emit(
            {
                "id": "call-" + str(seq),
                "type": "agent.custom_tool_use",
                "execution_id": execution,
                "name": name,
                "input": args,
            }
        )

    native_records = {}

    async def transport_body(request: httpx.Request) -> httpx.Response:
        nonlocal stage, item, eid, claim_id
        route = request.url.path
        if route.endswith("/executions"):
            ids = request.url.params.get_list("ids[]")
            assert set(ids) <= set(accepted)
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "execution_id": x,
                            "state": "COMPLETED" if x in settled else "RUNNING",
                            "settled_outcome": "COMPLETED" if x in settled else None,
                        }
                        for x in ids
                    ]
                },
            )
        if route.endswith("/sessions"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            created.append(json.loads(request.content))
            native_records["phase-session"] = created_native_session(
                request,
                "phase-session",
                agent_snapshot={
                    "version": 1,
                    "system": "Synthetic phased transport actor",
                    "skills": [],
                    "tools": [],
                    "model": {"id": "fixture-model"},
                },
            )
            return httpx.Response(201, json=native_records["phase-session"])
        if route.endswith("/sessions/phase-session"):
            if request.method == "POST":
                metadata = json.loads(request.content)["metadata"]
                assert metadata["tank_replay_cutoff"] == view.capture.manifest.cutoff
                assert metadata["tank_replay_evidence_sha256"] == view.capture.manifest_sha256
                native_records["phase-session"]["metadata"] = metadata
                return httpx.Response(200, json=native_records["phase-session"])
            return httpx.Response(200, json=native_records["phase-session"])
        if route.endswith("/archive"):
            return httpx.Response(200, json={})
        if route.endswith("/events/stream"):
            return httpx.Response(200, stream=Stream(), headers={"content-type": "text/event-stream"})
        if route.endswith("/events"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": [], "next_page": None})
            event = json.loads(request.content)["events"][0]
            identity = "accepted-" + str(len(accepted))
            accepted.append(identity)
            if event["type"] == "user.message":
                assert "item_id" not in json.loads(event["content"][0]["text"])["message"]
                item = "lead" if not sent else "confirmation"
                sent.append(item)
                stage = "get"
                assert all(
                    b"FUTURE_CONFIRMATION_CANARY" not in f.read_bytes()
                    for f in view.destination.rglob("*")
                    if f.is_file()
                )
                if item == "lead":
                    assert view.capture.manifest.cutoff == "2026-10-16T12:01:00Z"
                    assert await repo.get("ingest.source_items", "confirmation") is None
                    with pytest.raises(ValueError, match="Missing"):
                        await tools.get_item({"item_id": "confirmation"}, {})
                    search = await tools.search_tank_items(
                        {"query": "FUTURE_CONFIRMATION_CANARY", "days": 90}, {}
                    )
                    assert search == {"items": []}
                else:
                    assert await repo.get("ingest.source_items", "confirmation") is not None
                    first = json.loads(next(tmp_path.glob("attempt-phase-initial.json")).read_text())
                    assert first["state"]["ingest.source_items"][0]["item_id"] == "lead"
                    assert "FUTURE_CONFIRMATION_CANARY" not in json.dumps(first)
                if fail_later and item == "confirmation":
                    return httpx.Response(200, json=acknowledge_native_input(request, identity))
                await tool("get_item", {"item_id": item}, identity)
            else:
                result = await audited_result(repo, event)
                if stage in {"get", "lookup"}:
                    if stage == "get":
                        assert result["item_id"] == item
                        if publication and item == "lead":
                            stage = "lookup"
                            lookup = missing_lookup or "fetch_page"
                            arguments = (
                                {"query": "A fresh query with no recorded response"}
                                if lookup == "web_search"
                                else {
                                    "url": "https://robinhood.com/newsroom/"
                                    + ("uncaptured/" if missing_lookup else "")
                                }
                            )
                            await tool(lookup, arguments, identity)
                            return httpx.Response(200, json=acknowledge_native_input(request, identity))
                    else:
                        if missing_lookup:
                            assert event["is_error"] is True
                            assert result["ok"] is False
                            assert result["error"]["type"] == "ReplayEvidenceUnavailable"
                            assert "REPLAY_EVIDENCE_UNAVAILABLE" in result["error"]["reason"]
                            assert "items" not in result
                        else:
                            assert result == {"items": []}
                    stage = "origin" if item == "lead" else "observation"
                    args = (
                        {
                            "item_id": item,
                            "is_event": True,
                            "statement": "Robinhood to list LIT",
                            "certainty": "MEDIUM",
                            "certainty_reason": "Declared synthetic transport actor",
                            "entities": [],
                            "token_ids": [],
                            "event_time": {"value": None, "precision": "UNKNOWN", "quote": None},
                            "public_at": {
                                "value": "2026-10-16T12:00:00Z",
                                "precision": "EXACT",
                                "basis": "Declared synthetic coordinate",
                                "quote": None,
                            },
                            "freshness": "FRESH",
                            "claims": [],
                            "next_milestone": None,
                            "checked_sources": [],
                        }
                        if item == "lead"
                        else {
                            "item_id": item,
                            "event_id": eid,
                            "link": "CORROBORATION",
                            "certainty": "MEDIUM",
                            "certainty_reason": "Declared synthetic transport actor",
                            "claims": [],
                            "public_at": {
                                "value": "2026-10-16T12:00:00Z",
                                "precision": "EXACT",
                                "basis": "Original synthetic post",
                                "quote": None,
                            },
                            "freshness": "FRESH",
                        }
                    )
                    if publication:
                        claim = {
                            "type": "FACT" if item == "lead" else "STATE",
                            "subject": "Robinhood",
                            "predicate": "will list",
                            "object": "LIT",
                            "state_claim": None if item == "lead" else "APPROVED",
                            "verified": item != "lead",
                            "directionality": "BOTH",
                            "evidence_class": "SOCIAL_LEAD" if item == "lead" else "PRIMARY_FACT",
                            "quote": "Robinhood to list LIT",
                        }
                        args["claims"] = [claim]
                        if item == "lead":
                            args["token_ids"] = ["lighter:LIT"]
                            args["entities"] = [{"entity_id": "robinhood", "role": "ACTOR"}]
                            args["checked_sources"] = [
                                "S13 lookup unavailable (missing replay evidence)"
                                if missing_lookup
                                else "S13 lookup empty (declared fixture response)"
                            ]
                        else:
                            args["link"] = "STATE_UPDATE"
                            args["certainty"] = "HIGH"
                            args["public_at"] = {
                                "value": "2026-10-16T12:05:00Z",
                                "precision": "EXACT",
                                "basis": "Declared synthetic confirmation coordinate",
                                "quote": None,
                            }
                    await tool("record_event" if item == "lead" else "add_observation", args, identity)
                elif stage == "origin":
                    eid = result["event_id"]
                    if publication:
                        claim_id = result["accepted_claims"][0]["claim_id"]
                    stage = "assessment"
                    await tool(
                        "set_assessment",
                        {
                            "event_id": eid,
                            "assessment": dict(
                                unknown_assessment(criterion="M1", klass="MAJOR", claim=claim_id),
                                criterion_venue_id="robinhood",
                            )
                            if publication
                            else unknown_assessment(klass="MATERIAL"),
                        },
                        identity,
                    )
                elif publication and stage == "observation":
                    assert result.get("link_type") in {"STATE_UPDATE", "CORROBORATION"}, result
                    event_row = await repo.get("evt.events", eid)
                    assert event_row["certainty"] == "HIGH", event_row
                    stage = "assessment"
                    await tool(
                        "set_assessment",
                        {
                            "event_id": eid,
                            "assessment": dict(
                                unknown_assessment(criterion="M1", klass="MAJOR", claim=claim_id),
                                criterion_venue_id="robinhood",
                            ),
                        },
                        identity,
                    )
                elif publication and stage == "assessment":
                    from tank.event_tools import LAYERS

                    assert result.get("class") == "MAJOR", result
                    stage = "map"
                    await tool(
                        "set_beneficiaries",
                        {
                            "event_id": eid,
                            "edges": [
                                {
                                    "entity_id": "lighter",
                                    "token_id": "lighter:LIT",
                                    "role": "PRIMARY",
                                    "layer": "ISSUER_PRODUCT",
                                    "direction": "POSITIVE",
                                    "causal_chain": "Reported listing concerns this token",
                                    "directionality": "BOTH",
                                    "exposure_score": 4,
                                    "edge_certainty": "MEDIUM",
                                    "evidence": [claim_id],
                                }
                            ],
                            "layer_reasons": {
                                layer: "No supported secondary established in the declared fixture"
                                for layer in LAYERS
                            },
                            "layer_statuses": {layer: "NONE" for layer in LAYERS},
                            "no_credible_secondary_reason": "No additional intermediary asserted",
                        },
                        identity,
                    )
                elif publication and stage == "map":
                    assert result.get("analysis_set") == ["lighter:LIT"], result
                    stage = "truth"
                    await tool("get_market_truth", {"event_id": eid, "token_ids": ["lighter:LIT"]}, identity)
                elif publication and stage == "truth":
                    assert not result.get("error"), result
                    stage = "decision"
                    await tool("get_decision", {"event_id": eid, "token_id": "lighter:LIT"}, identity)
                elif publication and stage == "decision":
                    assert result.get("action") == "WAIT", result
                    stage = "publication"
                    await tool(
                        "publish_card",
                        {
                            "event_id": eid,
                            "kind": "FULL" if item == "lead" else "UPDATE",
                            "what": "Robinhood listing reported; token market coverage unavailable",
                            "verdict": "Robinhood listing reported; token market coverage unavailable",
                            "why": [
                                "Listing report concerns a registered token",
                                "No complete market observations retained in this declared input",
                            ],
                        },
                        identity,
                    )
                elif publication and stage == "publication":
                    assert result.get("publication_receipt"), result
                    publications.append(result)
                    await emit(
                        {
                            "id": "idle-" + str(seq),
                            "type": "session.status_idle",
                            "execution_id": identity,
                            "stop_reason": {"type": "end_turn"},
                        }
                    )
                else:
                    assert (
                        result.get("class") in {"LOW", "MATERIAL"}
                        or result.get("link_type") == "CORROBORATION"
                    ), result
                    await emit(
                        {
                            "id": "idle-" + str(seq),
                            "type": "session.status_idle",
                            "execution_id": identity,
                            "stop_reason": {"type": "end_turn"},
                        }
                    )
            return httpx.Response(200, json=acknowledge_native_input(request, identity))
        raise AssertionError(route)

    async def transport(request: httpx.Request):
        return await transport_body(request)

    reader = ReplayMarketReader(inputs)
    if publication:

        class EmptyMarket:
            async def snapshot(self, token_id, cutoff, anchor=None):
                assert parse_time(cutoff) <= self.active_cutoff()
                return {
                    "token_id": token_id,
                    "cutoff": cutoff,
                    "flow_bars": [],
                    "books": [],
                    "oi": [],
                    "funding": [],
                    "klines": [],
                    "liquidations": [],
                    "tickers": [],
                }

        reader = EmptyMarket()
    reaction_truth = MarketTruthService(repo, config, reader, versions=fixture_versions())
    reaction_scheduler = Scheduler(
        repo,
        config,
        reaction_truth.market,
        reaction_truth,
        thresholds=config.thresholds(),
        versions=fixture_versions(),
    )
    tools = EventToolService(
        repo,
        config,
        reaction_truth,
        DecisionEngine(),
        reference_clock=TrustedFixtureClock(inputs),
        versions=fixture_versions(),
        scheduler=reaction_scheduler,
        pages=recorded_page_access(inputs),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        if fail_later:
            with pytest.raises(TimeoutError):
                await replay_agent_session(
                    path,
                    repo,
                    config,
                    tools,
                    http=http,
                    initial_message=p["config"]["replay_messages"][0],
                    subsequent_messages=p["config"]["replay_messages"][1:],
                    replay_run_id="attempt",
                    output_path=tmp_path / "result.json",
                    timeout=1,
                    evidence_capture=capture,
                    evidence_view=view,
                )
            failed = json.loads((tmp_path / "result.json").read_text())
            assert failed["status"] == "FAILED"
            assert [phase["phase_id"] for phase in failed["phase_checkpoints"]] == ["initial"]
            assert (tmp_path / "attempt-phase-initial.json").exists()
            assert not (tmp_path / "attempt-phase-confirmation.json").exists()
            assert len(failed["cleanup_sessions"]) == 1
            cleanup = failed["cleanup_sessions"][0]
            assert cleanup["session_id"] == "phase-session" and cleanup["archived"] is True
            assert cleanup["local_finalization"]["finalized"] is True
            assert len(cleanup["local_finalization"]["failed_message_ids"]) == 1
            terminal = await repo.get("ops.agent_sessions", "phase-session")
            assert terminal["status"] == "FAILED" and terminal["runtime_status"] == "IDLE"
            assert terminal["active_message_id"] is None
            assert [message["turn_status"] for message in terminal["message_log"]] == ["DONE", "FAILED"]
            assert (await repo.get("ingest.source_items", "lead"))["extraction_status"] == "DONE"
            assert (
                json.loads((tmp_path / "attempt-phase-initial.json").read_text())
                == failed["phase_checkpoints"][0]
            )
            return
        result = await replay_agent_session(
            path,
            repo,
            config,
            tools,
            http=http,
            initial_message=p["config"]["replay_messages"][0],
            subsequent_messages=p["config"]["replay_messages"][1:],
            replay_run_id="attempt",
            output_path=tmp_path / "result.json",
            timeout=20,
            evidence_capture=capture,
            evidence_view=view,
        )
    assert result["status"] == "COMPLETED"
    assert sent[:2] == ["lead", "confirmation"]
    assert all(item == "confirmation" for item in sent[2:])
    assert len(sent) <= 3
    assert len(created) == 1
    checkpoints = result["phase_checkpoints"]
    assert [x["phase_id"] for x in checkpoints] == ["initial", "confirmation"]
    assert {x["session_id"] for x in checkpoints} == {"phase-session"}
    assert all(x["execution_barrier"]["complete"] for x in checkpoints)
    assert [x["at"] for x in checkpoints] == [entry.cutoff for entry in view.receipts]
    assert checkpoints[0]["evidence_manifest_sha256"] != checkpoints[1]["evidence_manifest_sha256"]
    assert view.destination.stat().st_ino
    assert not list(view.destination.glob("*phase-*.json"))
    for checkpoint in checkpoints:
        frozen = json.loads((tmp_path / ("attempt-phase-" + checkpoint["phase_id"] + ".json")).read_text())
        assert frozen == checkpoint
        assert frozen["sha256"] == digest({k: v for k, v in frozen.items() if k != "sha256"})
    assert len(checkpoints[0]["state"]["ingest.source_items"]) == 1
    assert len(checkpoints[1]["state"]["ingest.source_items"]) == 2
    assert (await repo.get("ingest.source_items", "confirmation"))["extraction_status"] == "DONE"
    assert [row["tool"] for row in await repo.list("ops.tool_calls")].count("add_observation") == len(
        sent
    ) - 1
    assert result["cleanup_sessions"] == [{"session_id": "phase-session", "archived": True}]
    assert result["requested_external_captures_complete"] is (not missing_lookup)
    assert len(result["external_capture_gaps"]) == (1 if missing_lookup else 0)
    if missing_lookup:
        gap = result["external_capture_gaps"][0]
        call = await repo.get("ops.tool_calls", result["session_id"] + ":" + gap["call_id"])
        assert call["tool"] == gap["tool"] == missing_lookup
        assert call["is_error"] is True
        assert gap["origin"] == "LOCAL_REPLAY_DIAGNOSTIC"
        assert gap["requested_at"] == "2026-10-16T12:01:00Z"
        assert call["arguments"]
        assert all(not phase["requested_external_captures_complete"] for phase in checkpoints)
        assert all(phase["external_capture_gaps"] == [gap] for phase in checkpoints)
    if publication:
        assert checkpoints[0]["state"]["evt.events"][0]["certainty"] == "MEDIUM"
        assert checkpoints[1]["state"]["evt.events"][0]["certainty"] == "HIGH"
        assert publications[0].get("published") is True
        assert publications[1]["publication_receipt"]["disposition"] == "PUBLISHED"
        assert publications[1]["publication_receipt"]["kind"] == "UPDATE"
        assert publications[1]["publication_receipt"]["previous_alert_id"] == publications[0]["alert_id"]
        assert [a["kind"] for a in await repo.list("out.alerts")] == ["FULL", "UPDATE"]
        assert len(result["external_capture_receipts"]) == (0 if missing_lookup else 1)
        assert [call["tool"] for call in await repo.list("ops.tool_calls")].count(
            missing_lookup or "fetch_page"
        ) == 1
        assert result["delivery_status"] == "NOT_RUN"
        assert not await repo.list("ana.calls")
        assert all(job["status"] == "QUEUED" for job in result["delivery_intents"])
        assert next(a["kind"] for a in await repo.list("out.alerts")) == "FULL"


@pytest.mark.integration
@pytest.mark.parametrize("tool", ["web_search", "fetch_page"])
async def test_reference_session_reports_missing_lookup_without_network_or_abort(
    reference_packet, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, tool, repository=None
) -> None:
    from tank.event_tools import EventToolService

    async def forbidden(*args, **kwargs) -> None:
        raise AssertionError("A missing replay answer must never make a live lookup")

    monkeypatch.setattr(EventToolService, "web_search", forbidden)
    monkeypatch.setattr(EventToolService, "fetch_page", forbidden)
    await test_actual_f24_phased_producers_keep_session_and_immutable_checkpoints(
        reference_packet, tmp_path, True, missing_lookup=tool, repository=repository
    )


@pytest.mark.postgres
@pytest.mark.integration
async def test_reference_missing_lookup_postgres(
    reference_packet, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, postgres_template
) -> None:
    from test_session_binding_concurrency import fixture_repo

    async with fixture_repo("postgres", postgres_template) as repo:
        await test_reference_session_reports_missing_lookup_without_network_or_abort(
            reference_packet, tmp_path, monkeypatch, "web_search", repository=repo
        )


def test_ambiguous_reference_capture_still_aborts(reference_packet) -> None:
    from dataclasses import replace

    from tank.replay import missing_capture_error

    path, packet = reference_packet
    save(path, packet)
    inputs = load_inputs(path, settings())
    request = {"query": "ambiguous retained responses"}
    conflicting = tuple(
        {
            "tool": "web_search",
            "arguments": request,
            "available_at": inputs.cutoff,
            "result": {"items": result},
            "status": "DONE",
            "is_error": False,
        }
        for result in ([], [{"title": "Conflicting result"}])
    )
    inputs = replace(inputs, _tool_results=inputs._tool_results + conflicting)
    with pytest.raises(ReplayError, match="unambiguous") as raised:
        inputs.reference_capture("web_search", request, parse_time(inputs.cutoff))
    assert type(raised.value) is ReplayError
    assert missing_capture_error(
        {"result": {"error": {"type": type(raised.value).__name__, "reason": str(raised.value)}}}
    )


@pytest.mark.postgres
@pytest.mark.integration
@pytest.mark.parametrize("publication", [False, True])
async def test_actual_f24_phases_postgres(
    reference_packet, tmp_path: Path, publication, postgres_template
) -> None:
    from test_session_binding_concurrency import fixture_repo

    async with fixture_repo("postgres", postgres_template) as repo:
        await test_actual_f24_phased_producers_keep_session_and_immutable_checkpoints(
            reference_packet, tmp_path, publication, repository=repo
        )


@pytest.mark.integration
async def test_failed_second_phase_preserves_committed_first_checkpoint(
    reference_packet, tmp_path: Path
) -> None:
    await test_actual_f24_phased_producers_keep_session_and_immutable_checkpoints(
        reference_packet, tmp_path, False, fail_later=True
    )


@pytest.mark.parametrize(
    "tool,argument,kind,value",
    [
        ("fetch_page", "url", "EMPTY_OFFICIAL_FEED_LOOKUP", "https://robinhood.com/newsroom/"),
        ("web_search", "query", "EMPTY_OFFICIAL_SEARCH_LOOKUP", "Issuer Seven Asset K official announcement"),
    ],
)
def test_phased_empty_lookup_requires_explicit_expiry(reference_packet, tool, argument, kind, value) -> None:
    path, packet = phased_packet(reference_packet)
    packet["tool_results"] = [
        {
            "origin": "DECLARED_SYNTHETIC_TOOL_RESPONSE",
            "declared_response_kind": kind,
            "tool": tool,
            "arguments": {argument: value},
            "result": {"items": []},
            "status": "DONE",
            "is_error": False,
            "http_acquisition_claim": False,
            "actual_authored_at": packet["assembled_at"],
            "available_at": packet["assembled_at"],
            "simulated_available_at": "2026-10-16T12:01:00Z",
        }
    ]
    save(path, packet)
    with pytest.raises(ReplayError, match="expire"):
        load_inputs(path, settings())


@pytest.mark.parametrize(
    "mutation", ["http_claim", "result", "arguments_type", "empty_query", "extra_argument", "tool"]
)
def test_synthetic_search_never_claims_acquisition_or_expands_its_contract(
    reference_packet, mutation
) -> None:
    path, packet = phased_packet(reference_packet)
    capture = {
        "origin": "DECLARED_SYNTHETIC_TOOL_RESPONSE",
        "declared_response_kind": "EMPTY_OFFICIAL_SEARCH_LOOKUP",
        "tool": "web_search",
        "arguments": {"query": "Issuer Seven Asset K official announcement"},
        "result": {"items": []},
        "status": "DONE",
        "is_error": False,
        "http_acquisition_claim": False,
        "actual_authored_at": packet["assembled_at"],
        "available_at": packet["assembled_at"],
        "simulated_available_at": "2026-10-16T12:01:00Z",
        "valid_until": "2026-10-16T12:05:00Z",
    }
    if mutation == "http_claim":
        capture["http_acquisition_claim"] = True
    elif mutation == "result":
        capture["result"] = {"items": [{"text": "fabricated announcement"}]}
    elif mutation == "arguments_type":
        capture["arguments"] = ["query"]
    elif mutation == "empty_query":
        capture["arguments"]["query"] = " "
    elif mutation == "extra_argument":
        capture["arguments"]["fallback"] = True
    else:
        capture["tool"] = "fetch_page"
    packet["tool_results"] = [capture]
    save(path, packet)
    with pytest.raises(ReplayError, match="exact declared empty response"):
        load_inputs(path, settings())


def _discourse_topic(post5_version=1, created="2022-09-21T10:48:47.970Z"):
    def post(number, cooked, at, version=1):
        return {
            "id": 1000 + number,
            "post_number": number,
            "cooked": cooked,
            "created_at": at,
            "updated_at": at,
            "version": version,
            "last_edited_at": None,
            "username": "author" + str(number),
        }

    return {
        "id": 830,
        "post_stream": {
            "posts": [
                post(1, "<p>Proposal put to a tokenholder vote.</p>", "2022-09-01T09:00:00.000Z"),
                post(5, "<p>The tokenholder vote has now passed.</p>", created, post5_version),
                post(6, "<p>FUTURE_CANARY reply after the cutoff.</p>", "2022-10-12T14:43:46.086Z"),
            ]
        },
    }


def _topic_post_packet(reference_packet, topic, post_number=5):
    from pathlib import Path

    import tank.fixture_reference as fixture_reference
    from tank.sources import parsers

    path, packet = reference_packet
    original = json.dumps(topic).encode()
    original_path = path.parent / "topic-original.json"
    original_path.write_bytes(original)
    selector = {"post_number": post_number}
    view = fixture_reference.project_original_source("DISCOURSE_TOPIC_POST_V1", original, selector)
    view_path = path.parent / "topic-post-view.json"
    view_path.write_bytes(view)
    post = next(p for p in topic["post_stream"]["posts"] if p["post_number"] == post_number)
    row = packet["records"][0]
    record = row["record"]
    record.update(
        text=parsers.text(post["cooked"]),
        raw_ref=str(view_path),
        raw_sha256=sha256(view),
        published_at=post["created_at"],
        published_at_precision="EXACT",
    )
    row["simulated_available_at"] = post["created_at"]
    row["as_of_evidence"] = {
        "kind": "IMMUTABLE_ORIGINAL_PUBLICATION",
        "version_id": "discourse:830:post:" + str(post["id"]) + ":version:1",
        "source_url": record["feed"],
        "raw_ref": str(view_path),
        "raw_sha256": sha256(view),
        "body_sha256": sha256(record["text"].encode()),
        "derivation": {
            "kind": "SOURCE_ORIGINAL_FIELD_PROJECTION_V1",
            "source_kind": "DISCOURSE_TOPIC_POST_V1",
            "selector": selector,
        },
        "projection_provenance": {
            "original_raw_ref": str(original_path),
            "original_raw_sha256": sha256(original),
            "projection_module_sha256": sha256(Path(fixture_reference.__file__).read_bytes()),
        },
    }
    return packet, view


def test_discourse_topic_post_projection_admits_one_unedited_reply(reference_packet) -> None:
    packet, view = _topic_post_packet(reference_packet, _discourse_topic())
    save(reference_packet[0], packet)
    inputs = load_inputs(reference_packet[0], settings())
    selected = json.loads(view)["post_stream"]["posts"]
    assert [p["post_number"] for p in selected] == [5]
    assert b"FUTURE_CANARY" not in view
    assert inputs.records("ingest.source_items")[0]["published_at"] == "2022-09-21T10:48:47.970Z"


def test_discourse_topic_post_refuses_edited_reply(reference_packet) -> None:
    packet, _ = _topic_post_packet(reference_packet, _discourse_topic(post5_version=2))
    save(reference_packet[0], packet)
    with pytest.raises(ReplayError, match="version is unproven"):
        load_inputs(reference_packet[0], settings())


def test_discourse_topic_post_refuses_missing_post_number() -> None:
    from tank.fixture_reference import project_original_source

    raw = json.dumps(_discourse_topic()).encode()
    with pytest.raises(ValueError, match="selection must be unique"):
        project_original_source("DISCOURSE_TOPIC_POST_V1", raw, {"post_number": 7})
    with pytest.raises(ValueError, match="explicit integer post_number"):
        project_original_source("DISCOURSE_TOPIC_POST_V1", raw, {"post_number": "5"})


def test_discourse_topic_post_refuses_publication_after_cutoff(reference_packet) -> None:
    packet, _ = _topic_post_packet(reference_packet, _discourse_topic(created="2022-09-23T08:00:00.000Z"))
    save(reference_packet[0], packet)
    with pytest.raises(ReplayError, match="publication precision interval"):
        load_inputs(reference_packet[0], settings())
