from datetime import timedelta

import pytest

from tank.review import ReviewService, validate_checks
from tank.storage import MemoryRepository
from tank.util import iso, parse_time, sha256

START = "2026-10-01T00:00:00Z"
END = "2026-10-01T01:00:00Z"


@pytest.mark.asyncio
async def test_hash_corruption_and_pending():
    repo = MemoryRepository()
    service = ReviewService(repo)
    await repo.put("out.evidence_bundles", "bad", {"sha256": "bad", "payload": {"event": {}}})
    assert (await service.evidence({"evidence_bundle_id": "bad"}))["valid"] is False
    result = await service.outcome(
        {"generated_at": START}, {"cutoff": START, "outlook": {"best_horizon": "H1"}}, {}, now=START
    )
    assert result["status"] == "PENDING" and result["realized_return"] is None


@pytest.mark.asyncio
async def test_no_borrowed_future_bar_or_partial_boundary():
    repo = MemoryRepository()
    service = ReviewService(repo)
    await repo.put(
        "mkt.klines_1m",
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
async def test_empty_report_and_unverified_human_checks():
    repo = MemoryRepository()
    service = ReviewService(repo)
    report = await service.report(START, END)
    assert report["metrics"]["S3"]["value"] is None
    assert report["metrics"]["S9"]["status"] == "UNVERIFIED"
    with pytest.raises(ValueError):
        validate_checks([{"alert_id": "a", "version": 1, "state_overstated": "false"}])


@pytest.mark.asyncio
async def test_outcome_immutable_revision_and_frozen_beta():
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
            "windows": {"W1H": {"normal_range": [-0.01, 0.01]}},
            "outcome_btc_reference": {"instrument_id": "btc", "entry_ref_price": 100},
        }
    }
    for minute in range(60):
        await repo.put(
            "mkt.klines_1m",
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
        await repo.put(
            "mkt.klines_1m",
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
    assert a == b and a["revision"] == 1
    assert a["realized_return"] == pytest.approx(0.2)
    assert a["btc_adjusted_return"] == pytest.approx(0.18) and a["direction_correct"] is True
    assert a["mfe"] == pytest.approx(0.3) and a["mae"] == pytest.approx(-0.1)
    corrected = await repo.get("mkt.klines_1m", "59")
    await repo.put("mkt.klines_1m", "59", dict(corrected, c=11))
    c = await service.outcome(alert, decision, truth, now=END)
    assert c["revision"] == 2 and c["realized_return"] == pytest.approx(0.1)
    assert (await repo.get("qa.outcomes", a["outcome_id"]))["realized_return"] == pytest.approx(0.2)


@pytest.mark.asyncio
async def test_postgres_outcome_restart_immutability():
    import os

    from tank.storage import ImmutableRecordError, PostgresRepository
    from tank.util import new_id

    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.skip("requires isolated TANK_TEST_DATABASE_URL")
    repo = PostgresRepository(url)
    await repo.open()
    await repo.migrate()
    key = new_id("OUT")
    record = {
        "outcome_id": key,
        "alert_id": new_id("alert"),
        "version": 1,
        "dec_id": "d",
        "computed_at": END,
        "revision": 1,
        "input_hash": "a" * 64,
        "realized_return": 0.2,
    }
    try:
        await repo.put("qa.outcomes", key, record, immutable=True)
        await repo.close()
        await repo.open()
        assert (await repo.get("qa.outcomes", key))["realized_return"] == 0.2
        with pytest.raises(ImmutableRecordError):
            await repo.put("qa.outcomes", key, dict(record, realized_return=0.3), immutable=True)
    finally:
        await repo.close()


@pytest.mark.asyncio
async def test_subminute_boundaries_require_enumeration_and_exclude_outside_prices():
    repo = MemoryRepository()
    service = ReviewService(repo)
    start = parse_time("2026-10-01T00:00:30Z")
    end = start + timedelta(hours=1)
    for minute in range(1, 60):
        await repo.put(
            "mkt.klines_1m",
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
    for minute in (0, 60):
        stamp = start.replace(second=0) + timedelta(minutes=minute)
        for second, price in [(10, 999), (40, 13)]:
            await repo.put(
                "mkt.trades",
                f"{minute}-{second}",
                {
                    "instrument_id": "i",
                    "trade_id": f"{minute}-{second}",
                    "ts": iso(stamp + timedelta(seconds=second)),
                    "price": price,
                },
            )
        await repo.put(
            "mkt.flow_bars_1m",
            str(minute),
            {"instrument_id": "i", "minute_start": iso(stamp), "complete": True, "n_buy": 1, "n_sell": 1},
        )
    result = await service.horizon_series("i", start, end, end + timedelta(minutes=1))
    assert result["coverage"]["exact"] is True
    assert result["endpoint"] == 999  # Actual last trade before 01:00:30, not the 01:00:40 trade.
    assert result["coverage"]["complete_minutes"] == 59
    await repo.delete("mkt.trades", "60-10")
    result = await service.horizon_series("i", start, end, end + timedelta(minutes=1))
    assert result["coverage"]["exact"] is False


@pytest.mark.asyncio
async def test_real_truth_decision_publication_bundle_to_completed_review(tmp_path):
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
    tools = EventToolService(repo, settings, None, DecisionEngine())
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
    decision = await tools.dispatch("get_decision", {"event_id": eid, "token": "T"}, {})
    published = await tools.dispatch(
        "publish_card",
        {
            "event_id": eid,
            "kind": "FULL",
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
        await repo.put(
            "mkt.klines_1m",
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
        await repo.put(
            "mkt.klines_1m",
            f"b-{minute}",
            {"instrument_id": "A:spot:BTC", "open_time": stamp, "c": 100, "complete": True},
        )
    result = await ReviewService(repo).show(eid, now=iso(start + timedelta(hours=hours)))
    frozen = result["versions"][0]
    assert frozen["evidence"]["valid"] is True
    assert frozen["outcomes"][0]["status"] == "COMPLETED"
    assert frozen["outcomes"][0]["btc_adjusted_return"] is not None
    assert frozen["outcomes"][0]["direction_reason"] is None
    assert await repo.list("qa.outcomes")
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
    assert (
        report["metrics"]["S9"]["status"] == "PASS"
    )  # The spec requires a ledger record, including pending delivery.
    import httpx

    from tank.delivery import DeliveryService

    settings.telegram_token = "offline"
    settings.telegram_alerts_chat = "1"

    async def telegram(request):
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 123}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(telegram)) as http:
        await DeliveryService(repo, settings, http=http).deliver(
            {"alert_id": published["alert_id"], "version": 1}
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
async def test_metrics_version_labels_shown_tokens_and_uncarded_event_audit():
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
    assert report["metrics"]["S6"]["denominator"] == 2  # One shown token per version.
    assert report["metrics"]["S7"]["status"] == "PASS" and report["metrics"]["S7"]["denominator"] == 1
    assert report["metrics"]["S9"]["status"] == "PASS"  # A pending ledger is still a delivery record.


@pytest.mark.asyncio
async def test_date_only_latency_and_stale_boundary_keep_precision_unknown():
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
    assert latency["precision_counts"] == {"EXACT": 1, "BUCKET_1D": 3, "BUCKET_1H": 0, "UNKNOWN": 1}
    assert result["metrics"]["S1"]["known_to_sent"]["status"] == "PASS"
    stale = result["metrics"]["S2"]
    assert stale["value"] == 1 and stale["denominator"] == 3 and stale["eligible"] == 5
    assert stale["status"] == "UNVERIFIED" and stale["precision_counts"]["UNKNOWN"] == 1
