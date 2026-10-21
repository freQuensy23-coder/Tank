"""03 §11.4 formula test vectors T1–T9, each expected value pinned as written in the spec.

Owned by tank-acceptance. These call production code only; they do not restate formulas.
"""

from datetime import timedelta
from decimal import ROUND_DOWN, Decimal

import pytest

from tank.analysis.truth import decomposition, enrich_evidence, oi_class, spot_truth
from tank.util import iso, parse_time

END = parse_time("2026-10-16T12:00:00Z")


def pct(a, b):
    return (Decimal(str(b)) - Decimal(str(a))) / Decimal(str(a))


def test_t1_oi_decomposition_real_native():
    n0, n1, p0, p1 = 100, 108.18, 1, 1.0705
    d = decomposition(n0, n1, p0, p1)
    usd = pct(n0 * p0, Decimal("108.18") * Decimal("1.0705"))
    assert round(usd * 100, 2) == Decimal("15.81")
    assert d["position"] == Decimal("8.18")
    assert d["markup"] == Decimal("7.05")
    assert round(d["interaction"], 3) == Decimal("0.577")

    assert Decimal(str(d["pos_share"])).quantize(Decimal("0.001"), rounding=ROUND_DOWN) == Decimal("0.517")
    assert d["pos_share"] > 0.5
    assert oi_class(0.0818, float(usd), d, [-0.03, 0.035], [-0.04, 0.05]) == "REAL_NATIVE"


def test_t2_price_markup():
    d = decomposition(100, 100.5, 1, 1.10)
    usd = pct(100, Decimal("100.5") * Decimal("1.10"))
    assert round(usd * 100, 2) == Decimal("10.55")
    assert pct(100, 100.5) == Decimal("0.005")
    assert oi_class(0.005, float(usd), d, [-0.03, 0.035], [-0.04, 0.05]) == "PRICE_MARKUP"


def flow_case(values, qs):
    start = END - timedelta(minutes=1)
    rows = [
        dict(
            instrument_id=f"{v}:spot:T",
            minute_start=iso(start),
            buy_qty=(vol + cvd) / 2,
            sell_qty=(vol - cvd) / 2,
            sum_sq_qty=q,
            complete=True,
        )
        for (v, vol, cvd), q in zip(values, qs, strict=False)
    ]
    return spot_truth(rows, [], [], start, END)


def test_t3_broad_sell_small_venues_buying():
    s = flow_case([("A", 600, -40), ("B", 250, -10), ("C", 100, 5), ("D", 25, 6), ("E", 25, 3)], [1] * 5)
    assert s["composite_max"]["cvd"] == -36
    assert s["direction"] == "SELL"
    assert round(Decimal(str(s["composite_max"]["buy_share"])), 3) == Decimal("0.482")
    assert s["breadth"]["pct_pos"] == 0.6 and s["breadth"]["pct_neg"] == 0.4
    assert s["cw_breadth"]["sell"] == 0.85
    assert s["concentration"]["top_volume_venue"] == "A"
    assert round(Decimal(str(s["concentration"]["top_contrib_share"])), 2) == Decimal("0.80")
    assert s["concentration"]["others_cwb_dir"] == 0.625
    assert s["classification"] == "BROAD"


def test_t4_single_venue_spike():
    s = flow_case([("A", 300, 60), ("B", 300, -2), ("C", 300, 1), ("D", 100, 0)], [1, 4, 4, 4])
    assert s["composite_max"]["cvd"] == 59
    assert s["direction"] == "BUY"
    assert round(Decimal(str(s["concentration"]["top_contrib_share"])), 2) == Decimal("0.98")
    assert s["concentration"]["others_cwb_dir"] == 0
    assert s["classification"] == "CONCENTRATED"


def test_t5_constant_cohort():
    def truth(values):
        return dict(
            venues=[dict(venue=k, volume_native=v, cvd=0, sum_sq_qty=1, sign=0) for k, v in values.items()],
            composite_max={"volume": sum(values.values())},
            cohort_consistent={},
            direction="NEUTRAL",
            composite_cohort=None,
        )

    spots = {
        "W24H": truth({"binance": 500, "okx": 200, "coinbase": 250, "kraken": 50}),
        "W1H": truth({"binance": 500, "okx": 200, "kraken": 50, "bybit": 250}),
    }
    rel = dict(
        ref_venue=None,
        windows={"W15M": {"normal_range": None}, "W1H": {"r_raw": None}},
        context_inputs={"rv24": None, "rv_normal_range": None},
    )
    enrich_evidence(
        {"cutoff": iso(END)}, END - timedelta(hours=1), END, spots, "W1H", {}, rel, {"per_venue": []}
    )
    comparison = spots["W1H"]["cohort_comparisons"]["W24H"]
    assert comparison["cohort"] == ["binance", "kraken", "okx"]
    assert comparison["share_of_max"] == 0.75
    assert spots["W1H"]["cohort_consistent"]["W24H"] is True


def test_t6_funding_normalization_no_market_average():
    from tank.analysis.truth import derivatives_truth

    funding = [
        dict(instrument_id=f"{v}:perp:T", kind="SETTLED", settlement_time=iso(END), rate=r, interval_h=h)
        for v, r, h in [("X", 0.0001, 8), ("Y", 0.00005, 4), ("Z", 0.00002, 1)]
    ]
    oi = [dict(instrument_id=f"{v}:perp:T", ts=iso(END), oi_native=100, oi_usd=100) for v in ("X", "Y", "Z")]
    d = derivatives_truth(dict(oi=oi, funding=funding), END - timedelta(hours=4), END)
    assert [v["funding_8h_eq"] for v in d["per_venue"]] == [
        Decimal(".0001"),
        Decimal(".0001"),
        Decimal(".00016"),
    ]
    assert d["funding_dispersion_8h"] == Decimal(".00006")

    def keys(value):
        if isinstance(value, dict):
            for k, v in value.items():
                yield k
                yield from keys(v)
        elif isinstance(value, list):
            for v in value:
                yield from keys(v)

    assert not [k for k in keys(d) if "funding" in k and ("market" in k or "avg" in k or "mean" in k)]


@pytest.fixture
async def service(tmp_path):
    from tank.event_tools import EventToolService
    from tank.settings import Settings
    from tank.storage import MemoryRepository

    repo = MemoryRepository()
    await repo.open()
    return EventToolService(repo, Settings(data_dir=tmp_path), None, None)


async def recorded(service, state):
    now = iso()
    await service.repo.put(
        "ingest.source_items",
        "t7",
        {
            "item_id": "t7",
            "source_id": "S30",
            "source_class": "PRIMARY",
            "text": "primary artifact",
            "observed_at": now,
        },
    )
    quote = "The selection makes this settlement infrastructure required for the route."
    result = await service.record_event(
        {
            "item_id": "t7",
            "is_event": True,
            "statement": "T7 vector",
            "certainty": "HIGH",
            "certainty_reason": "Primary artifact",
            "entities": [{"name": "World", "entity_id": None, "role": "ACTOR"}],
            "tokens": ["world:WLD"],
            "event_time": {"value": now, "precision": "EXACT", "quote": "time"},
            "public_at": {"value": now, "precision": "EXACT", "basis": "primary", "quote": "time"},
            "freshness": "FRESH",
            "next_milestone": None,
            "checked_sources": ["primary artifact"],
            "claims": [
                {
                    "type": "STATE",
                    "subject": "World",
                    "predicate": "selected",
                    "object": "required infrastructure",
                    "state_claim": state,
                    "verified": True,
                    "directionality": "BOTH",
                    "evidence_class": "PRIMARY_FACT",
                    "quote": quote,
                }
            ],
        },
        {},
    )
    proof = result["accepted_claims"][0]["claim_id"]
    claim = await service.repo.get("evt.claims", proof)
    claim["evidence"][0]["quote"] = quote
    await service.repo.put("evt.claims", proof, claim)
    return result["event_id"], proof, quote


def assessment(proof, klass, criterion, quote, gate=None):
    a = {
        "class": klass,
        "criterion": criterion,
        "criterion_quote": quote,
        "dims": {
            n: {"value": 8, "rationale": "vector", "evidence": [proof]}
            for n in [
                "economic_directness",
                "magnitude",
                "permanence",
                "distribution_expansion",
                "mechanical_flows",
                "surprise",
                "implementation",
            ]
        },
        "impact_horizon": {
            "impact_start": "WEEKS",
            "timing_confidence": "HIGH",
            "ramp_shape": "STEP",
            "dependencies": [],
            "time_to_materiality": "Weeks",
        },
    }
    if gate:
        a["structural_gates"] = [{"gate": gate, "satisfied": True, "evidence": [proof]}]
    return a


@pytest.mark.parametrize(
    "klass,criterion,gate",
    [("STRUCTURAL_SHOCK", "M4", None), ("MAJOR", "M4", None), ("STRUCTURAL_SHOCK", "G4", "G4")],
)
async def test_t7_fee_burn_put_to_binding_vote_is_major_never_structural(service, klass, criterion, gate):
    eid, proof, quote = await recorded(service, "PROPOSED")
    out = await service.set_assessment(
        {"event_id": eid, "assessment": assessment(proof, klass, criterion, quote, gate)}, {}
    )
    assert out["class"] != "STRUCTURAL_SHOCK"
    if criterion == "M4":
        assert out["class"] == "MAJOR"


async def test_t7_verified_required_infrastructure_selection_is_structural_g4(service):
    eid, proof, quote = await recorded(service, "LIVE")
    out = await service.set_assessment(
        {"event_id": eid, "assessment": assessment(proof, "STRUCTURAL_SHOCK", "G4", quote, "G4")}, {}
    )
    assert out["class"] == "STRUCTURAL_SHOCK" and out["criterion"] == "G4"


def _three_venue_snapshot_with_stale_funding():
    import copy

    from test_truth_start_fixture import synthetic_start_snapshot

    snap = synthetic_start_snapshot()
    cutoff = parse_time(snap["cutoff"])

    for collection in ("instruments", "flow_bars", "klines", "books", "semantics"):
        extra = []
        for row in snap[collection]:
            if row.get("instrument_id") == "A:spot:T" or row.get("metric_key", "").startswith("A.spot."):
                clone = copy.deepcopy(row)
                if "instrument_id" in clone:
                    clone["instrument_id"] = "C:spot:T"
                if "venue" in clone:
                    clone["venue"] = "C"
                if "metric_key" in clone:
                    clone["metric_key"] = clone["metric_key"].replace("A.spot.", "C.spot.")
                extra.append(clone)
        snap[collection].extend(extra)

    snap["funding"] = [
        r for r in snap["funding"] if parse_time(r["settlement_time"]) < cutoff - timedelta(hours=9)
    ]
    return snap


def test_t8_data_confidence_medium_funding_freshness_blocks_start():
    from copy import deepcopy

    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import compute_market_truth

    truth = compute_market_truth(_three_venue_snapshot_with_stale_funding())
    dc = truth["data_confidence"]
    assert truth["spot"][truth["primary_window"]]["coverage"]["n_complete"] == 3
    assert dc["derivatives_native_coverage_ready"] is True
    assert dc["class"] == "MEDIUM", dc
    assert dc["limiting"]["condition"] == "freshness" and dc["limiting"]["collection"] == "funding"

    e, a, b = (
        {"event_id": "e", "event_state": "APPROVED", "state_verified": True, "freshness": "FRESH"},
        {"class": "MAJOR", "certainty": "HIGH", "impact_horizon": {"impact_start": "NOW"}},
        {"token_id": "T", "role": "PRIMARY", "credible": True, "capture_class": "DIRECT"},
    )
    m = {
        "token_id": "T",
        "move_class": "SPOT_LED_ACCUMULATION",
        "data_confidence": {"class": "HIGH"},
        "relative": {"repricing_band": "EARLY", "market_context": "NEUTRAL", "anchor": {"ref_price": "10"}},
        "liquidity": {"liquidity_state": "NORMAL"},
        "spot": {},
        "derivatives": {},
    }
    engine = DecisionEngine()
    assert engine.evaluate(e, a, b, m)["action"] == "START"
    n = deepcopy(m)
    n["data_confidence"] = dc
    assert engine.evaluate(e, a, b, n)["action"] != "START"


def test_t9_disconnected_minutes_are_incomplete_and_exclude_the_venue():
    end = parse_time("2026-10-16T13:00:00Z")
    start = end - timedelta(hours=1)
    rows = []
    for venue, vol in (("A", 100), ("B", 80), ("C", 60)):
        for minute in range(60):
            t = start + timedelta(minutes=minute)
            if venue == "A" and 5 <= t.minute < 9:
                rows.append(
                    dict(
                        instrument_id="A:spot:T",
                        minute_start=iso(t),
                        buy_qty=None,
                        sell_qty=None,
                        sum_sq_qty=None,
                        complete=False,
                    )
                )
                continue
            rows.append(
                dict(
                    instrument_id=f"{venue}:spot:T",
                    minute_start=iso(t),
                    buy_qty=vol * 0.6,
                    sell_qty=vol * 0.4,
                    sum_sq_qty=1,
                    complete=True,
                )
            )
    s = spot_truth(rows, [], [], start, end)
    excluded = {x["venue"]: x for x in s["coverage"]["excluded"]}
    assert excluded["A"]["reason"] == "INCOMPLETE"
    assert {x["venue"] for x in s["venues"]} == {"B", "C"}
    assert s["composite_max"]["volume"] == 60 * (80 + 60)
