from copy import deepcopy
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from review_typed_fixtures import put_boundary, put_flow
from scheduling_support import RecordingReactionScheduling
from score_fixtures import unassessed_score_dimensions
from source_support import fixture_versions

from tank.review import ReviewService, validate_checks
from tank.storage import MemoryRepository
from tank.util import iso, parse_time, sha256

START = "2026-10-01T00:00:00Z"
END = "2026-10-01T01:00:00Z"


@pytest.mark.asyncio
async def test_hash_corruption_and_pending() -> None:
    repo = MemoryRepository()
    service = ReviewService(repo)
    await repo.put("out.evidence_bundles", "bad", {"sha256": "bad", "payload": {"event": {}}})
    assert (await service.evidence({"evidence_bundle_id": "bad"}))["valid"] is False
    result = await service.outcome(
        {"generated_at": START}, {"cutoff": START, "outlook": {"best_horizon": "H1"}}, {}, now=START
    )
    assert result["status"] == "PENDING" and result["realized_return"] is None


@pytest.mark.asyncio
async def test_no_borrowed_future_bar_or_partial_boundary() -> None:
    repo = MemoryRepository()
    service = ReviewService(repo)
    await put_flow(
        repo,
        "future",
        {"instrument_id": "i", "open_time": END, "c": 999, "h": 999, "l": 999, "complete": True},
    )
    decision = {"cutoff": START, "outlook": {"best_horizon": "H1"}}
    r = await service.outcome(
        {"generated_at": START, "event_id": "e"},
        decision,
        {"relative": {"ref_venue": "i", "entry_ref_price": 10}},
        now=END,
    )
    assert r["status"] == "UNAVAILABLE" and r["realized_return"] is None
    assert r["coverage"]["complete_minutes"] == 0
    decision["cutoff"] = "2026-10-01T00:00:30Z"
    r = await service.outcome(
        {"generated_at": START, "event_id": "e"},
        decision,
        {"relative": {"ref_venue": "i", "entry_ref_price": 10}},
        now="2026-10-01T02:00:00Z",
    )
    assert r["coverage"]["partial_boundary"] is True


@pytest.mark.asyncio
async def test_empty_report_and_unverified_human_checks() -> None:
    repo = MemoryRepository()
    service = ReviewService(repo)
    report = await service.report(START, END)
    assert report["metrics"]["S3"]["value"] is None
    assert report["metrics"]["S9"]["status"] == "UNVERIFIED"
    with pytest.raises(ValueError):
        validate_checks([{"alert_id": "a", "version": 1, "state_overstated": "false"}])


@pytest.mark.asyncio
async def test_outcome_immutable_revision_and_frozen_beta() -> None:
    repo = MemoryRepository()
    service = ReviewService(repo)
    alert = {"alert_id": "a", "version": 1, "event_id": "e", "generated_at": START}
    decision = {
        "dec_id": "d",
        "token_id": "t",
        "cutoff": START,
        "outlook": {"best_horizon": "H1", "direction": "BULLISH"},
    }
    truth = {
        "relative": {
            "ref_venue": "i",
            "entry_ref_price": 10,
            "beta_btc": 2,
            "beta_flags": {"btc": None, "eth": None},
            "windows": {"W1H": {"normal_range": [-0.01, 0.01], "relative_measure": "R_BTC_ADJ"}},
            "outcome_btc_reference": {"instrument_id": "btc", "entry_ref_price": 100},
        }
    }
    for minute in range(60):
        await put_flow(
            repo,
            str(minute),
            {
                "instrument_id": "i",
                "open_time": iso(parse_time(START) + timedelta(minutes=minute)),
                "c": 12,
                "h": 13,
                "l": 9,
                "complete": True,
            },
        )
    for minute in range(60):
        await put_flow(
            repo,
            "btc" + str(minute),
            {
                "instrument_id": "btc",
                "open_time": iso(parse_time(START) + timedelta(minutes=minute)),
                "c": 101,
                "complete": True,
            },
        )
    a = await service.outcome(alert, decision, truth, now=END)
    b = await service.outcome(alert, decision, truth, now="2026-10-02T00:00:00Z")

    def stable(row):
        return {key: value for key, value in row.items() if not key.startswith("computed_at")}

    assert stable(a) == stable(b) and "revision" not in a
    assert a["realized_return"] == pytest.approx(0.2)
    assert a["btc_adjusted_return"] == pytest.approx(0.18) and a["direction_correct"] is True
    assert a["mfe"] == pytest.approx(0.3) and a["mae"] == pytest.approx(-0.1)

    for reason in ("INSUFFICIENT_HISTORY", "NOT_ESTIMABLE", "NOT_CAPTURED"):
        unavailable = deepcopy(truth)
        unavailable["relative"]["beta_btc"] = 1.0
        if reason == "NOT_CAPTURED":
            unavailable["relative"].pop("beta_flags")
        else:
            unavailable["relative"]["beta_flags"]["btc"] = reason
        unknown = await service.outcome(alert, decision, unavailable, now=END)
        assert unknown["status"] == "COMPLETED"
        assert unknown["realized_return"] == pytest.approx(0.2)
        assert unknown["btc_adjusted_return"] is None
        assert unknown["direction_correct"] is None
        assert unknown["direction_reason"] == "FROZEN_BETA_UNAVAILABLE"

    zero = deepcopy(truth)
    zero["relative"]["beta_btc"] = 0.0
    zero_result = await service.outcome(alert, decision, zero, now=END)
    assert zero_result["btc_adjusted_return"] == pytest.approx(0.2)
    assert zero_result["direction_correct"] is True

    raw = deepcopy(truth)
    raw["relative"]["beta_btc"] = None
    raw["relative"]["beta_flags"]["btc"] = "INSUFFICIENT_HISTORY"
    raw["relative"]["windows"]["W1H"]["relative_measure"] = "R_RAW"
    raw["relative"]["outcome_btc_reference"] = {}
    measured = await service.outcome(alert, decision, raw, now=END)
    assert measured["direction_correct"] is True
    assert measured["direction_return"] == pytest.approx(0.2)
    assert measured["btc_adjusted_return"] is None
    assert "btc_coverage" not in measured
    corrected = await repo.get("mkt.flow_bars_1m", "59")
    await put_flow(repo, "59", dict(corrected, c=11))
    c = await service.outcome(alert, decision, truth, now=END)
    assert c["realized_return"] == pytest.approx(0.1)


@pytest.mark.asyncio
async def test_subminute_boundaries_require_typed_segments_and_exclude_outside_prices() -> None:
    repo = MemoryRepository()
    service = ReviewService(repo)
    start = parse_time("2026-10-01T00:00:30Z")
    end = start + timedelta(hours=1)
    for minute in range(1, 60):
        await put_flow(
            repo,
            str(minute),
            {
                "instrument_id": "i",
                "open_time": iso(start.replace(second=0) + timedelta(minutes=minute)),
                "c": 11,
                "h": 12,
                "l": 9,
                "complete": True,
            },
        )
    await put_boundary(
        repo,
        "boundary-start",
        "i",
        start.replace(second=0),
        start,
        start.replace(second=0) + timedelta(minutes=1),
        start + timedelta(seconds=10),
        13,
    )
    await put_boundary(
        repo,
        "boundary-end",
        "i",
        end.replace(second=0),
        end.replace(second=0),
        end,
        end.replace(second=0) + timedelta(seconds=10),
        999,
    )
    result = await service.horizon_series("i", start, end, end + timedelta(minutes=1))
    assert result["coverage"]["exact"] is True
    assert result["endpoint"] == 999
    assert result["coverage"]["complete_minutes"] == 59
    row = await repo.get("mkt.flow_bars_1m", "boundary-end")
    await repo.put("mkt.flow_bars_1m", "boundary-end", dict(row, boundary_segments=[]))
    result = await service.horizon_series("i", start, end, end + timedelta(minutes=1))
    assert result["coverage"]["exact"] is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_truth_decision_publication_bundle_to_completed_review(tmp_path: Path) -> None:
    from test_truth_start_fixture import synthetic_start_snapshot

    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import compute_market_truth
    from tank.event_tools import EventToolService
    from tank.settings import Settings
    from tank.util import utcnow

    snapshot = synthetic_start_snapshot()
    snapshot["instruments"].append(
        {"instrument_id": "A:spot:BTC", "token_id": "BTC", "venue": "A", "market_type": "SPOT"}
    )
    truth = compute_market_truth(snapshot)
    truth.update(mta_id="actual-review-truth", event_id="actual-review-event")
    repo = MemoryRepository()
    settings = Settings(data_dir=tmp_path)
    tools = EventToolService(
        repo,
        settings,
        None,
        DecisionEngine(),
        versions=fixture_versions(),
        scheduler=RecordingReactionScheduling(),
    )
    now = iso(utcnow())
    eid = "actual-review-event"
    event = {
        "event_id": eid,
        "statement": "Access approved",
        "current_ems_id": "review-ems",
        "event_state": "APPROVED",
        "state_verified": True,
        "freshness": "FRESH",
        "certainty": "HIGH",
        "lifecycle": "MONITORING",
        "public_at": now,
        "public_at_prec": "EXACT",
        "known_to_us_at": now,
        "event_time": {"value": None, "precision": "UNKNOWN"},
    }
    assessment = {
        "dims": unassessed_score_dimensions(),
        "ems_id": "review-ems",
        "event_id": eid,
        "class": "MAJOR",
        "criterion": "M2",
        "certainty": "HIGH",
        "impact_horizon": {"impact_start": "NOW"},
    }
    edge = {
        "edge_id": "review-edge",
        "event_id": eid,
        "map_version": 1,
        "entity_id": "world",
        "token_id": "T",
        "role": "PRIMARY",
        "tradable": True,
        "credible": True,
        "capture_class": "DIRECT",
        "exposure_score": 8,
    }
    for table, key, row in [
        ("evt.events", eid, event),
        ("evt.ems_assessments", "review-ems", assessment),
        ("evt.beneficiary_maps", "review-map", {"event_id": eid, "map_version": 1, "analysis_set": ["T"]}),
        ("evt.beneficiary_edges", "review-edge", edge),
        ("ana.market_truth", truth["mta_id"], truth),
    ]:
        await repo.put(table, key, row)
    decision = await tools.dispatch("get_decision", {"event_id": eid, "token_id": "T"}, {})
    published = await tools.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "FULL",
            "what": "Access approved",
            "verdict": "Access approved",
            "why": ["Defined access", "Primary event confirmed"],
        },
        {},
    )
    assert published["published"], published
    start = parse_time(decision["cutoff"])
    hours = __import__("tank.review", fromlist=["HOURS"]).HOURS[decision["outlook"]["best_horizon"]]
    for minute in range(hours * 60):
        stamp = iso(start + timedelta(minutes=minute))
        await put_flow(
            repo,
            f"t-{minute}",
            {
                "instrument_id": truth["relative"]["ref_venue"],
                "open_time": stamp,
                "h": 105,
                "l": 100,
                "c": 104,
                "complete": True,
            },
        )
        await put_flow(
            repo,
            f"b-{minute}",
            {"instrument_id": "A:spot:BTC", "open_time": stamp, "c": 100, "complete": True},
        )
    result = await ReviewService(repo).show(eid, now=iso(start + timedelta(hours=hours)))
    frozen = result["versions"][0]
    assert frozen["evidence"]["valid"] is True
    assert frozen["outcomes"][0]["status"] == "COMPLETED"
    assert frozen["outcomes"][0]["btc_adjusted_return"] is not None
    assert frozen["outcomes"][0]["direction_reason"] is None
    report = await ReviewService(repo).report(
        iso(parse_time(now) - timedelta(minutes=1)),
        iso(parse_time(now) + timedelta(minutes=1)),
        checks=[
            {
                "alert_id": published["alert_id"],
                "version": 1,
                "reviewer": "offline-test",
                "reason": "Structured audit fixture",
                "state_overstated": False,
                "usd_oi_new_positions": False,
                "accumulation_without_checks": False,
                "secondary_exists": False,
                "reproducible_verdict": True,
            }
        ],
    )
    assert report["metrics"]["S3"]["status"] == "PASS"
    assert report["metrics"]["S3"]["denominator"] == 1
    assert report["metrics"]["S9"]["status"] == "PASS"

    from tank.delivery import DeliveryService

    settings.telegram_token = "offline"
    settings.telegram_alerts_chat = "1"

    async def telegram(request: httpx.Request):
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 123}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        await DeliveryService(repo, settings, http=http).deliver(
            {"channel": "telegram:alerts", "alert_id": published["alert_id"], "version": 1}
        )
    verified = await ReviewService(repo).report(
        iso(parse_time(now) - timedelta(minutes=1)),
        iso(parse_time(now) + timedelta(minutes=1)),
        checks=[
            {
                "alert_id": published["alert_id"],
                "version": 1,
                "reviewer": "offline-test",
                "reason": "Frozen bundle reproduced offline",
                "reproducible_verdict": True,
            }
        ],
    )
    assert verified["metrics"]["S9"]["status"] == "PASS"
    assert report["shadow_acceptance"] == "UNVERIFIED"


@pytest.mark.asyncio
async def test_metrics_version_labels_shown_tokens_and_uncarded_event_audit() -> None:
    repo = MemoryRepository()
    service = ReviewService(repo)
    for version, label in [(1, "NOISE"), (2, "CATCH")]:
        payload = {
            "event": {"public_at": START, "public_at_prec": "EXACT", "known_to_us_at": START},
            "ems": {"class": "MAJOR"},
            "decisions": [{"dec_id": "shown", "token_id": "T"}, {"dec_id": "hidden", "token_id": "X"}],
            "market_truth": [
                {
                    "token_id": "T",
                    "move_class": "ORGANIC_MIXED",
                    "decisive_facts": ["flow and price checked"],
                },
                {"token_id": "X", "move_class": "UNKNOWN", "decisive_facts": []},
            ],
        }
        digest = sha256(payload)
        await repo.put("out.evidence_bundles", digest, {"sha256": digest, "payload": payload}, immutable=True)
        await repo.put(
            "out.alerts",
            f"a:{version}",
            {
                "alert_id": "a",
                "version": version,
                "event_id": "carded",
                "generated_at": START,
                "evidence_bundle_id": digest,
                "decisions": ["shown"],
            },
            immutable=True,
        )
        await repo.put(
            "qa.reviews",
            f"l{version}",
            {
                "review_id": f"l{version}",
                "subject": "a",
                "version": version,
                "at": iso(parse_time(START) + timedelta(seconds=version)),
                "label": label,
            },
            immutable=True,
        )
        await repo.put(
            "out.deliveries",
            f"d{version}",
            {"alert_id": "a", "version": version, "at": START, "status": "PENDING"},
            immutable=True,
        )
    await repo.put(
        "evt.events",
        "uncarded",
        {"event_id": "uncarded", "known_to_us_at": START, "public_at": START, "current_ems_id": "ems"},
    )
    await repo.put("evt.ems_assessments", "ems", {"ems_id": "ems", "event_id": "uncarded", "class": "MAJOR"})
    checks = [
        {"event_id": "carded", "secondary_exists": False, "reviewer": "r", "reason": "none exists"},
        {
            "event_id": "uncarded",
            "secondary_exists": True,
            "secondary_accepted": True,
            "reviewer": "r",
            "reason": "reviewed beneficiary",
        },
    ]
    checks.extend(
        {"alert_id": "a", "version": v, "reviewer": "r", "reason": "reproduced", "reproducible_verdict": True}
        for v in (1, 2)
    )
    report = await service.report(START, END, checks=checks, now=END)
    assert report["metrics"]["S8_major_noise"]["value"] == 0.5
    assert report["metrics"]["S8_major_noise"]["denominator"] == 2
    assert report["metrics"]["S6"]["value"] == 1
    assert report["metrics"]["S6"]["denominator"] == 2
    assert report["metrics"]["S7"]["status"] == "PASS" and report["metrics"]["S7"]["denominator"] == 1
    assert report["metrics"]["S9"]["status"] == "PASS"


@pytest.mark.asyncio
async def test_date_only_latency_and_stale_boundary_keep_precision_unknown() -> None:
    repo = MemoryRepository()
    service = ReviewService(repo)
    known = "2026-10-04T12:00:00Z"
    cases = [
        ("exact", known, "EXACT"),
        ("straddles", "2026-10-02T00:00:00Z", "BUCKET_1D"),
        ("old", "2026-10-01T00:00:00Z", "BUCKET_1D"),
        ("recent", "2026-10-03T00:00:00Z", "BUCKET_1D"),
        ("legacy", known, None),
    ]
    for name, public, precision in cases:
        event = {"event_id": name, "known_to_us_at": known, "public_at": public}
        if precision:
            event["public_at_prec"] = precision
        await repo.put("evt.events", name, event)
        await repo.put("ingest.source_items", name, {"item_id": name, "source_id": "S01"})
        await repo.put(
            "evt.event_observations",
            name,
            {"obs_id": name, "event_id": name, "item_id": name, "observed_at": known},
        )
        payload = {"event": event, "ems": {"class": "MAJOR"}}
        digest = sha256(payload)
        await repo.put("out.evidence_bundles", digest, {"sha256": digest, "payload": payload}, immutable=True)
        await repo.put(
            "out.alerts",
            name,
            {
                "alert_id": name,
                "version": 1,
                "event_id": name,
                "generated_at": known,
                "evidence_bundle_id": digest,
            },
            immutable=True,
        )
        await repo.put(
            "out.deliveries",
            name,
            {"alert_id": name, "version": 1, "at": known, "status": "SENT"},
            immutable=True,
        )
    result = await service.report("2026-10-04T00:00:00Z", "2026-10-05T00:00:00Z", now="2026-10-05T00:00:00Z")
    latency = result["metrics"]["S1"]["public_to_known"]
    assert latency["seconds"] == 0 and latency["denominator"] == 1 and latency["eligible"] == 5
    assert latency["status"] == "UNVERIFIED"
    assert latency["precision_counts"] == {
        "EXACT": 1,
        "BUCKET(1d)": 3,
        "BUCKET(1h)": 0,
        "COLLECTION_ONLY": 0,
        "UNKNOWN": 1,
    }
    assert result["metrics"]["S1"]["known_to_sent"]["status"] == "PASS"
    stale = result["metrics"]["S2"]
    assert stale["value"] == 1 and stale["denominator"] == 3 and stale["eligible"] == 5
    assert stale["status"] == "UNVERIFIED" and stale["precision_counts"]["UNKNOWN"] == 1


@pytest.mark.asyncio
async def test_typed_boundaries_require_adjacent_pieces_and_never_slice_whole_bar() -> None:
    class TypedOnlyRepository(MemoryRepository):
        def iter_range(self, collection, *args, **kwargs):
            assert collection not in {"mkt.trades", "mkt.klines_1m"}
            return super().iter_range(collection, *args, **kwargs)

    repo = TypedOnlyRepository()
    service = ReviewService(repo)
    start = parse_time("2026-10-01T00:00:30Z")
    end = start + timedelta(hours=1)
    for minute in range(1, 60):
        await put_flow(
            repo,
            str(minute),
            {
                "instrument_id": "i",
                "open_time": iso(start.replace(second=0) + timedelta(minutes=minute)),
                "c": "11",
                "h": "12",
                "l": "9",
                "complete": True,
            },
        )
    await put_boundary(
        repo,
        "start",
        "i",
        start.replace(second=0),
        start,
        start.replace(second=0) + timedelta(minutes=1),
        start + timedelta(seconds=5),
        "13",
    )
    end_floor = end.replace(second=0)
    await put_boundary(repo, "end", "i", end_floor, end_floor, end, end_floor + timedelta(seconds=20), "14")
    row = await repo.get("mkt.flow_bars_1m", "end")
    piece = row["boundary_segments"][0]
    first = dict(
        piece,
        end=iso(end_floor + timedelta(seconds=10)),
        last_trade=dict(piece["last_trade"], ts=iso(end_floor + timedelta(seconds=5))),
        first_trade=dict(piece["first_trade"], ts=iso(end_floor + timedelta(seconds=5))),
    )
    second = dict(piece, start=iso(end_floor + timedelta(seconds=10)))
    await repo.put("mkt.flow_bars_1m", "end", dict(row, boundary_segments=[first, second]))
    result = await service.horizon_series("i", start, end, end + timedelta(minutes=1))
    assert result["coverage"]["exact"] and result["endpoint"] == 14 and result["high"] == 14
    assert result["coverage"]["boundary_segments"][-1]["pieces"] == 2

    oversized = dict(piece, start=iso(end_floor), end=iso(end_floor + timedelta(minutes=1)))
    await repo.put("mkt.flow_bars_1m", "end", dict(row, boundary_segments=[oversized]))
    result = await service.horizon_series("i", start, end, end + timedelta(minutes=1))
    assert result["coverage"]["exact"] is False


@pytest.mark.asyncio
async def test_list_and_show_page_with_next_after_cursor() -> None:
    repo = MemoryRepository()
    service = ReviewService(repo)
    for version in (1, 2, 3):
        await repo.put(
            "out.alerts",
            f"a:{version}",
            {
                "alert_id": "a",
                "version": version,
                "event_id": "e",
                "generated_at": f"2026-10-01T00:0{version}:00Z",
            },
            immutable=True,
        )
    first = await service.list(START, END, limit=2)
    assert [r["version"] for r in first["cards"]] == [1, 2]
    assert first["pagination"]["has_more"] and first["pagination"]["next_after"][1] == "a:2"
    second = await service.list(START, END, limit=2, after=first["pagination"]["next_after"])
    assert [r["version"] for r in second["cards"]] == [3]
    assert second["pagination"] == {"limit": 2, "returned": 1, "has_more": False, "next_after": None}
    shown = await service.show("a", limit=2, after=first["pagination"]["next_after"], now=END)
    assert [v["alert"]["version"] for v in shown["versions"]] == [3]


def test_cli_after_takes_the_printed_cursor() -> None:
    import click

    from tank.cli import page_cursor

    assert page_cursor('["2026-10-01T00:02:00Z", "a:2"]') == ["2026-10-01T00:02:00Z", "a:2"]
    assert page_cursor(None) is None
    for bad in ("2026-10-01T00:02:00Z", "[1, 2]", '["x"]'):
        with pytest.raises(click.BadParameter):
            page_cursor(bad)
