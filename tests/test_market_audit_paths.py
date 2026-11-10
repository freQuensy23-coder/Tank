"""Independent synthetic raw-input audit of the current written §5.8 rules.

No network, credentials, production calls, or historical fixture expectations.
Market qualifiers in scoring cases are explicit branch preconditions; every
support signal asserted below comes from raw bars/books/funding/prints.
"""

from copy import deepcopy
from datetime import timedelta
from decimal import Decimal

import pytest
from flow_helpers import flow_evidence, spot_truth
from funding_helpers import confirmation, rate, start
from funding_helpers import evidence as funding_evidence
from oi_helpers import evidence as oi_evidence
from volume_helpers import buckets, evidence

from tank.analysis.truth import (
    derivatives_truth,
    enrich_evidence,
    liquidity_truth,
    select_move,
    strict_majority,
    supply_truth,
    support_features,
)
from tank.util import iso, parse_time

END = parse_time("2026-10-16T12:00:00Z")
START = END - timedelta(minutes=15)


def raw_packet():
    packet = dict(
        token_id="T",
        cutoff=iso(END),
        _primary="EVENT",
        _has_event=True,
        _lead_start=START,
        _event_anchor=START,
        klines=[],
        flow_bars=[],
        oi=[],
        funding=[],
        books=[],
        supply=[],
        semantics=[],
        instruments=[],
        volume={},
    )
    for venue, market in [("A", "spot"), ("B", "spot"), ("P", "perp"), ("Q", "perp")]:
        iid = f"{venue}:{market}:T"
        packet["instruments"].append(
            dict(
                instrument_id=iid,
                token_id="T",
                venue=venue,
                market_type="SPOT" if market == "spot" else "PERP_LINEAR",
            )
        )
        packet["semantics"].append(
            dict(
                metric_key=iid + ":trade",
                venue=venue,
                instrument_id=iid,
                metric_family="aggressor",
                semantic_confidence="VERIFIED",
                market_type="SPOT" if market == "spot" else "PERP_LINEAR",
                trade_aggregation="PER_FILL",
            )
        )

        for index in range(9 * 96):
            t = START - timedelta(minutes=15 * (index + 1))
            packet["klines"].append(
                dict(
                    instrument_id=iid,
                    token_id="T",
                    open_time=iso(t),
                    interval_s=900,
                    complete=True,
                    o="100",
                    c="100",
                )
            )
        for minute in range(-15, 15):
            t = START + timedelta(minutes=minute)
            packet["flow_bars"].append(
                dict(
                    instrument_id=iid,
                    token_id="T",
                    minute_start=iso(t),
                    buy_qty="20",
                    sell_qty="10",
                    sum_sq_qty="1",
                    complete=True,
                    buy_notional="100000",
                    sell_notional="50000",
                    low="100",
                    size_buckets={
                        "B4": dict(
                            buy_notional="80000" if minute >= 0 else "90000",
                            sell_notional="20000" if minute >= 0 else "10000",
                        ),
                        "B5": dict(buy_notional="0", sell_notional="0"),
                    },
                )
            )
        for minute in range(15):
            t = START + timedelta(minutes=minute)
            packet["klines"].append(
                dict(
                    instrument_id=iid,
                    token_id="T",
                    open_time=iso(t),
                    interval_s=60,
                    complete=True,
                    o="100",
                    c="102" if market == "spot" or minute >= 1 else "100",
                )
            )
        if market == "perp":
            for index in range(9 * 96):
                t = START - timedelta(minutes=15 * (index + 1))
                n = Decimal(100 + 10 * (index % 3))
                packet["oi"].append(
                    dict(
                        instrument_id=iid,
                        ts=iso(t),
                        oi_native=str(n),
                        oi_usd=str(n * Decimal("100.1")),
                        mark_price="100.1",
                        index_price="100",
                    )
                )
            for t, n in [(START, 110), (END, 111)]:
                packet["oi"].append(
                    dict(
                        instrument_id=iid,
                        ts=iso(t),
                        oi_native=str(n),
                        oi_usd=str(Decimal(n) * Decimal("100.1")),
                        mark_price="100.1",
                        index_price="100",
                    )
                )
            for index in range(28):
                packet["funding"].append(
                    dict(
                        instrument_id=iid,
                        kind="SETTLED",
                        settlement_time=iso(START - timedelta(hours=8 * index)),
                        rate=".0001",
                        interval_h=8,
                    )
                )
            packet["funding"].append(
                dict(instrument_id=iid, kind="SETTLED", settlement_time=iso(END), rate=".0001", interval_h=8)
            )
        else:
            for seconds in range(-3600, 901, 10):
                packet["books"].append(
                    dict(
                        instrument_id=iid,
                        token_id="T",
                        ts=iso(START + timedelta(seconds=seconds)),
                        integrity_ok=True,
                        mid="100",
                        spread_bps="2",
                        quote_asset="USD",
                        range_covered_pct=dict(bid=2, ask=2),
                        depth=dict(
                            bid=dict(p1=dict(usd="1000000000")),
                            ask=dict(p1=dict(usd="1000000000" if seconds <= 0 else "1100000000")),
                        ),
                    )
                )
    packet["supply"] = [
        dict(
            kind="FLOW",
            ts=iso(START),
            class_="ignored",
            **{"class": "TREASURY_INTERNAL", "to_exchange": True},
        )
    ]
    return packet


def attach_volumes(packet):
    """The volume records the statements return for the raw packet: each market's native volume is 600 in
    the window against 300 before it and a 90-day range of 300, each instrument's USD volume 30000 against
    15000; `packet["volume"]` changes them (None: no volume evidence at all)."""
    changes = packet["volume"]
    packet["volume_sql"] = (
        None
        if changes is None
        else evidence(
            [i["instrument_id"] for i in packet["instruments"]],
            buckets=buckets(END, ["A", "B"], volume=2_250_000.0, count=2),
            **changes,
        )
    )
    return packet


def attach_oi(packet):
    """The open-interest records the statements return for the raw packet: each perp's open interest went
    from 110 to 111 (or `end_native` and `end_usd` of `packet["oi_state"]`) against a 90-day range of
    -9.09% .. +20%, its basis is 10 bps (or `basis`) against a range of exactly 10 bps and 10 bps at the
    window start."""
    state = dict(basis=10.0, end_native=111.0, end_usd=11111.1) | packet.get("oi_state", {})
    perps = [i["instrument_id"] for i in packet["instruments"] if ":perp:" in i["instrument_id"]]
    packet["oi_sql"] = oi_evidence(
        END,
        perps,
        lengths={"EVENT": 900},
        venue={
            None: dict(
                oi_native_start=110.0,
                oi_native_end=state["end_native"],
                oi_usd_start=11011.0,
                oi_usd_end=state["end_usd"],
                basis_bps=state["basis"],
                basis_start_bps=10.0,
            )
        },
        venue_history={
            None: dict(
                native_low=-0.0909,
                native_high=0.2,
                usd_low=-0.0909,
                usd_high=0.2,
                basis_samples=864,
                basis_low=10.0,
                basis_high=10.0,
                basis_first=START - timedelta(days=9),
                basis_last=START - timedelta(minutes=15),
            )
        },
    )
    return packet


def attach_funding(packet):
    """The funding and liquidation records the statements return for the raw packet: each perp's settled rate
    is 0.01% per 8 hours (`packet["funding_state"]["rates"]` gives a rate of an instrument, `current` of every
    one) at the window start as now (`start`) against a 90-day range of exactly that rate (`normal`), with no
    liquidation (`liquidations` gives the records of instruments) and no confirmed estimate
    (`confirmations` gives pairs of an instrument and a rate)."""
    state = dict(current=".0001", start=".0001", normal=0.0001, rates={}, liquidations={}, confirmations=[])
    state |= packet.get("funding_state", {})
    perps = [i["instrument_id"] for i in packet["instruments"] if ":perp:" in i["instrument_id"]]
    packet["funding_sql"] = funding_evidence(
        END,
        perps,
        lengths={"EVENT": 900},
        venue={
            None: rate(state["current"])
            | start(state["start"])
            | dict(normal_low=state["normal"], normal_high=state["normal"])
            | {iid: rate(value) for iid, value in state["rates"].items()}
        },
        liquidations={None: state["liquidations"]},
        confirmations=[confirmation(iid, value, END) for iid, value in state["confirmations"]],
    )
    return packet


def derive(packet, start=START, end=END):
    attach_volumes(packet)
    attach_oi(packet)
    attach_funding(packet)
    seconds = (end - start).total_seconds()
    packet["flow_sql"] = flow_evidence(packet, windows={"EVENT": seconds, "W15M": 900})
    spot = spot_truth(packet["flow_bars"], [], packet["instruments"], start, end)

    spot["composite_cohort"] = deepcopy(spot["composite_max"])
    relative = dict(
        ref_venue="A:spot:T",
        context_inputs=dict(rv24=None, rv_normal_range=None),
        windows={
            "EVENT": dict(r_raw=0.02, r_btc_adj=0.02, normal_range=[-0.01, 0.01]),
            "W15M": dict(r_raw=0.02, r_btc_adj=0.02, normal_range=[-0.01, 0.01]),
        },
    )
    deriv = derivatives_truth(packet, start, end)
    liquidity = liquidity_truth(packet, start, end, spot, relative)
    supply = supply_truth(packet, start, end)
    enrich_evidence(packet, start, end, {"EVENT": spot, "W15M": spot}, "EVENT", deriv, relative, liquidity)
    features = support_features(packet, start, end, spot, deriv, liquidity, supply)
    deriv["support_features"] = features
    return spot, deriv, liquidity, supply, relative


def test_raw_native_volume_and_basis_support_require_complete_own_history():
    packet = raw_packet()
    _, deriv, _, _, _ = derive(packet)
    f = deriv["support_features"]
    assert f["spot_volume_up_beyond_normal"] is True
    assert f["perp_volume_up_beyond_normal"] is True
    assert f["basis_within_normal"] is True
    assert f["perp_taker_flow_buy"] is True

    packet["volume"] = dict(native={market: dict(value=300.0) for market in ("SPOT", "PERP")})
    f = derive(packet)[1]["support_features"]
    assert f["spot_volume_up_beyond_normal"] is False and f["spot_volume_within_normal"] is True
    assert f["perp_volume_up_beyond_normal"] is False
    packet["volume"] = None
    f = derive(packet)[1]["support_features"]
    assert f["spot_volume_up_beyond_normal"] is None and f["perp_volume_up_beyond_normal"] is None


def test_raw_perp_taker_significance_and_basis_widening_have_negative_controls():
    packet = raw_packet()
    packet["oi_state"] = dict(basis=20.0)
    f = derive(packet)[1]["support_features"]
    assert f["basis_widening"] is True and f["basis_within_normal"] is False
    for row in packet["flow_bars"]:
        if ":perp:" in row["instrument_id"]:
            row.update(buy_qty="10", sell_qty="10")
    assert derive(packet)[1]["support_features"]["perp_taker_flow_buy"] is False
    packet["semantics"] = [s for s in packet["semantics"] if s["market_type"] == "SPOT"]
    assert derive(packet)[1]["support_features"]["perp_taker_flow_buy"] is None


def test_raw_lead_increment_ticket_and_depth_support_values():
    spot, deriv, liq, supply, _ = derive(raw_packet())
    assert deriv["lead_lag"]["first_mover"] == "SPOT"
    assert spot["size_profile"]["large_ticket_buy_majority"] is True
    assert spot["size_profile"]["sell_share_rising"] is True
    assert deriv["support_features"]["ask_depth_building"] is True
    assert deriv["support_features"]["exchange_inflows"] is True

    assert deriv["incremental_share"]["spot"] == 0.5
    packet = raw_packet()
    packet["volume"] = dict(usd={"SPOT": dict(current_usd=45000.0)})
    assert derive(packet)[1]["incremental_share"]["spot"] > 0.5
    assert liq["depth_drop"] <= 0


def test_raw_large_ticket_minority_and_ask_refill_include_missing_and_failed_controls():
    packet = raw_packet()
    for row in packet["flow_bars"]:
        row["size_buckets"]["B4"] = dict(buy_notional="1000", sell_notional="1000")
    assert derive(packet)[1]["support_features"]["large_tickets_minority"] is True

    for row in packet["books"]:
        row["depth"]["ask"]["p1"]["usd"] = "100"
    assert derive(packet)[1]["support_features"]["ask_replenishment"] is True
    for row in packet["books"]:
        if parse_time(row["ts"]) > START:
            row["depth"]["ask"]["p1"]["usd"] = "1"
    for row in packet["flow_bars"]:
        if parse_time(row["minute_start"]) > START:
            row["buy_notional"] = row["sell_notional"]
    assert derive(packet)[1]["support_features"]["ask_replenishment"] is False
    packet["books"] = []
    assert derive(packet)[1]["support_features"]["ask_replenishment"] is None


def liquidated(first, second):
    """The liquidation record of P:perp:T for a short liquidation of `first` USD in the first half of the window
    and a long one of `second` in the second."""
    return {
        "P:perp:T": dict(
            observations=2,
            long_count=1,
            long_usd=second,
            short_count=1,
            short_usd=first,
            total_usd=first + second,
            first_half_count=1,
            first_half_usd=first,
        )
    }


def test_raw_first_half_liquidation_majority_is_not_event_count_majority():
    packet = raw_packet()

    def feature():
        spot, d, liquidity, s, _ = derive(packet)
        d["per_venue"][0]["liq_coverage"] = "COMPLETE"
        return support_features(packet, START, END, spot, d, liquidity, s)["liquidations_first_half_majority"]

    packet["funding_state"] = dict(liquidations=liquidated(60.0, 40.0))
    assert feature() is True
    packet["funding_state"] = dict(liquidations=liquidated(40.0, 60.0))
    assert feature() is False
    packet["funding_state"] = dict(liquidations={})
    assert feature() is None
    packet["funding_state"] = dict(
        liquidations={"P:perp:T": dict(observations=1, short_count=1, total_usd=None, total_usd_missing=1)}
    )
    assert feature() is None


def test_raw_funding_dispersion_fragmentation_needs_independent_complete_path():
    packet = raw_packet()
    packet["funding_state"] = dict(
        rates={"Q:perp:T": ".01"}, confirmations=[("P:perp:T", ".0001"), ("Q:perp:T", ".01")]
    )
    spot, deriv, liq, _, _ = derive(packet)
    assert liq["fragmentation"]["price_path_validated"] is False
    assert deriv["funding_dispersion_normal_range_8h"] == [Decimal(0), Decimal(0)]
    assert liq["fragmentation"]["funding_path_validated"] is True
    assert liq["fragmentation"]["validated"] is True

    _, _, _, supply, relative = derive(packet)
    assert (
        select_move(spot, deriv, liq, supply, relative, "HIGH", "EVENT")["move_class"]
        == "REAL_MARKET_FRAGMENTATION"
    )
    packet["funding_state"]["confirmations"] = [("P:perp:T", ".0001")]
    assert derive(packet)[2]["fragmentation"]["funding_path_validated"] is False
    packet["funding_state"]["confirmations"] = [("P:perp:T", ".0001"), ("Q:perp:T", ".0001")]
    assert derive(packet)[2]["fragmentation"]["funding_path_validated"] is False
    packet["funding_state"]["confirmations"] = [("P:perp:T", ".0001"), ("Q:perp:T", ".01")]
    next(
        r for r in packet["flow_bars"] if r["instrument_id"] == "Q:perp:T" and r["minute_start"] == iso(START)
    )["complete"] = False
    assert derive(packet)[2]["fragmentation"]["funding_path_validated"] is False


def test_raw_alternating_top_venue_signs_do_not_invent_second_path():
    packet = raw_packet()
    start = END - timedelta(minutes=45)
    packet["flow_bars"] = []
    packet["instruments"].append(dict(instrument_id="C:spot:T", token_id="T", venue="C", market_type="SPOT"))
    for minute in range(45):
        packet["flow_bars"].append(
            dict(
                instrument_id="C:spot:T",
                minute_start=iso(start + timedelta(minutes=minute)),
                buy_qty="0",
                sell_qty="14",
                sum_sq_qty="1",
                complete=True,
                buy_notional="0",
                sell_notional="1400",
            )
        )
        sign = 1 if minute // 15 in (0, 2) else -1
        for venue in ("A", "B"):
            s = sign if venue == "A" else -sign
            packet["flow_bars"].append(
                dict(
                    instrument_id=f"{venue}:spot:T",
                    minute_start=iso(start + timedelta(minutes=minute)),
                    buy_qty="20" if s > 0 else "1",
                    sell_qty="1" if s > 0 else "20",
                    sum_sq_qty="1",
                    complete=True,
                    buy_notional="2000" if s > 0 else "100",
                    sell_notional="100" if s > 0 else "2000",
                )
            )
    spot, d, liquidity, s, r = derive(packet, start, END)
    assert spot["classification"] == "DIVERGENT" and spot["direction"] == "SELL"
    assert liquidity["fragmentation"]["top_sign_flips"] is True
    assert liquidity["fragmentation"]["validated"] is False
    classified = select_move(spot, d, liquidity, s, r, "HIGH", "EVENT")
    candidate = next(c for c in classified["divergence"]["candidates"] if c["state"] == "FRAGMENTATION")
    assert candidate["eligible"] is True and candidate["refuted"] is False
    assert classified["move_class"] != "REAL_MARKET_FRAGMENTATION"

    for row in packet["flow_bars"]:
        if row["minute_start"] == iso(start):
            row["complete"] = False
    assert derive(packet, start, END)[2]["fragmentation"]["top_sign_flips"] is not True


def test_exact_majority_flags_survive_float_presentation_rounding():
    assert strict_majority(Decimal("2"), Decimal("3.9999999999999999999999999999999999999999")) is True
    assert strict_majority(Decimal("2"), Decimal("4")) is False


def scoring_qualifiers(spot, deriv, liq, supply, relative, cls, qualifier_native=None):
    """Explicit required-rule preconditions, separate from raw-derived support.

    This tests counting/wiring, not eligibility qualification or OI capture. No
    support feature, lead, funding classification, volume share, or depth trend
    is overwritten here. Those are raw-derived by derive().
    """

    deriv["synchronized"] = {
        key: deriv["per_venue"][0].get(key)
        for key in ("oi_native_chg_pct", "native_normal_range", "oi_class")
    }
    if qualifier_native is not None:
        deriv["synchronized"] = deepcopy(qualifier_native)
    if cls in ("SHORT_SQUEEZE", "PERP_LED_CHASE"):
        deriv["synchronized"] = dict(
            oi_native_chg_pct=-0.5 if cls == "SHORT_SQUEEZE" else 0.5,
            native_normal_range=[-0.1, 0.1],
            oi_class="MIXED",
        )
    if cls in ("SPOT_LED_ACCUMULATION", "ORGANIC_MIXED"):
        relative["windows"]["EVENT"]["r_btc_adj"] = 0
    if cls == "SELL_SIDE_ABSORPTION":
        liq["elasticity"]["weak_response"] = True
    if cls == "DISTRIBUTION":
        supply["supply_in_evidence"] = "TRUE"

        spot["cohort_comparisons"] = {
            "W24H": dict(current=dict(sign=-1), other=dict(sign=1), share_majority=True)
        }
        relative["windows"]["EVENT"]["start"] = iso(START)
        relative["windows"]["EVENT"]["cutoff"] = iso(END)
    if cls == "LIQUIDITY_VACUUM":
        liq["depth_majority_lost"] = True
        liq["elasticity"]["excess_response"] = True
    return select_move(spot, deriv, liq, supply, relative, "HIGH", "EVENT")[
        "eligible_support_before_divergence"
    ]


def positive_packet(cls):
    packet = raw_packet()
    if cls == "SPOT_LED_ACCUMULATION":
        packet["volume"] = dict(usd={"SPOT": dict(current_usd=45000.0)})
    if cls == "PERP_LED_CHASE":
        for row in packet["flow_bars"]:
            if ":spot:" in row["instrument_id"]:
                row.update(buy_qty="10", sell_qty="10")
        packet["volume"] = dict(usd={"PERP": dict(current_usd=45000.0)})
        packet["funding_state"] = dict(current=".01")
        packet["oi_state"] = dict(basis=20.0)
    if cls == "SHORT_SQUEEZE":
        packet["funding_state"] = dict(
            current="-.0001",
            start="-.0001",
            normal=-0.0001,
            liquidations={
                "P:perp:T": dict(
                    observations=2,
                    short_count=2,
                    short_usd=100.0,
                    total_usd=100.0,
                    first_half_count=1,
                    first_half_usd=60.0,
                )
            },
        )
    if cls == "SELL_SIDE_ABSORPTION":
        for row in packet["books"]:
            row["depth"]["ask"]["p1"]["usd"] = "100"
    if cls == "LIQUIDITY_VACUUM":
        for row in packet["flow_bars"]:
            row["size_buckets"]["B4"] = dict(buy_notional="1000", sell_notional="1000")
        packet["volume"] = dict(native={market: dict(value=300.0) for market in ("SPOT", "PERP")})
    if cls == "REAL_MARKET_FRAGMENTATION":
        for row in packet["flow_bars"]:
            if row["instrument_id"].startswith("B:"):
                row.update(buy_qty="1", sell_qty="10")
        for row in packet["books"]:
            row["mid"] = (
                "105" if row["instrument_id"].startswith("B:") and parse_time(row["ts"]) < END else "100"
            )
            row["second_path_confirmed"] = True
            row["independent_rest"] = dict(source="PUBLIC_REST", ts=row["ts"], mid=row["mid"], spread_bps="2")
    return packet


def raw_scores(packet, cls, qualifier_native=None):
    spot, deriv, liquidity, supply, relative = derive(packet)
    if cls == "SHORT_SQUEEZE":
        for row in deriv["per_venue"]:
            row["liq_coverage"] = "COMPLETE"
        deriv["support_features"] = support_features(packet, START, END, spot, deriv, liquidity, supply)
    native = {
        key: deriv["per_venue"][0].get(key)
        for key in ("oi_native_chg_pct", "native_normal_range", "oi_class")
    }
    scores = scoring_qualifiers(spot, deriv, liquidity, supply, relative, cls, qualifier_native)
    return scores, native


@pytest.mark.parametrize(
    "cls,expected",
    [
        ("SPOT_LED_ACCUMULATION", 5),
        ("ORGANIC_MIXED", 2),
        ("PERP_LED_CHASE", 4),
        ("SHORT_SQUEEZE", 2),
        ("SELL_SIDE_ABSORPTION", 3),
        ("DISTRIBUTION", 3),
        ("LIQUIDITY_VACUUM", 2),
        ("REAL_MARKET_FRAGMENTATION", 2),
    ],
)
def test_each_section58_class_counts_all_raw_derived_support_terms(cls, expected):
    scores, _ = raw_scores(positive_packet(cls), cls)
    assert scores[cls] == expected


def test_raw_support_missing_evidence_never_counts_as_zero_or_positive():
    packet = dict(token_id="T", _primary="EVENT", klines=[], flow_bars=[], oi=[], books=[], liquidations=[])
    d = dict(per_venue=[])
    f = support_features(packet, START, END, dict(size_profile={}), d, {}, dict(flows=[]))
    for key in [
        "spot_volume_up_beyond_normal",
        "perp_volume_up_beyond_normal",
        "perp_taker_flow_buy",
        "liquidations_first_half_majority",
        "large_ticket_buy_majority",
        "large_ticket_sell_share_rising",
        "large_tickets_minority",
        "ask_depth_building",
        "ask_replenishment",
        "exchange_inflows",
    ]:
        assert f[key] is None, key

    assert f["basis_within_normal"] is False


SUPPORT_TERMS = [
    ("SPOT_LED_ACCUMULATION", "spot_volume_beyond", 5),
    ("SPOT_LED_ACCUMULATION", "spot_leads", 5),
    ("SPOT_LED_ACCUMULATION", "spot_increment_majority", 5),
    ("SPOT_LED_ACCUMULATION", "large_ticket_buy_majority", 5),
    ("SPOT_LED_ACCUMULATION", "depth_not_down", 5),
    ("ORGANIC_MIXED", "both_volumes_beyond", 2),
    ("ORGANIC_MIXED", "basis_within", 2),
    ("PERP_LED_CHASE", "funding_above", 4),
    ("PERP_LED_CHASE", "basis_widening", 4),
    ("PERP_LED_CHASE", "spot_sell_or_neutral", 4),
    ("PERP_LED_CHASE", "perp_taker_buy", 4),
    ("SHORT_SQUEEZE", "first_half_liquidation_majority", 2),
    ("SHORT_SQUEEZE", "perp_taker_buy", 2),
    ("SELL_SIDE_ABSORPTION", "ask_replenishment", 3),
    ("SELL_SIDE_ABSORPTION", "supply_evidence", 3),
    ("SELL_SIDE_ABSORPTION", "native_not_up", 3),
    ("DISTRIBUTION", "large_ticket_sell_rising", 3),
    ("DISTRIBUTION", "ask_depth_building", 3),
    ("DISTRIBUTION", "exchange_inflows", 3),
    ("LIQUIDITY_VACUUM", "large_tickets_minority", 2),
    ("LIQUIDITY_VACUUM", "spot_volume_within", 2),
    ("REAL_MARKET_FRAGMENTATION", "venue_signs_disagree", 2),
    ("REAL_MARKET_FRAGMENTATION", "gap_closes_quickly", 2),
]


@pytest.mark.parametrize("cls,term,maximum", SUPPORT_TERMS, ids=[f"{c}-{t}" for c, t, _ in SUPPORT_TERMS])
def test_each_support_term_loses_exactly_one_point_when_raw_evidence_is_removed(cls, term, maximum):
    packet = positive_packet(cls)
    scores, native = raw_scores(packet, cls)
    assert scores[cls] == maximum
    if term in ("spot_volume_beyond", "both_volumes_beyond", "spot_volume_within"):
        packet["volume"] = dict(
            packet["volume"], native=dict(SPOT=dict(value=3000.0 if term == "spot_volume_within" else 300.0))
        )
    elif term == "spot_leads":
        for row in packet["klines"]:
            if ":spot:" in row["instrument_id"] and START <= parse_time(row["open_time"]) < START + timedelta(
                minutes=3
            ):
                row["c"] = "100"
    elif term == "spot_increment_majority":
        packet["volume"] = dict(
            packet["volume"], usd=dict(packet["volume"]["usd"], PERP=dict(current_usd=60000.0))
        )
    elif term in ("large_ticket_buy_majority", "large_ticket_sell_rising", "large_tickets_minority"):
        for row in packet["flow_bars"]:
            if ":spot:" in row["instrument_id"] and parse_time(row["minute_start"]) >= START:
                row["size_buckets"]["B4"] = dict(
                    buy_notional="20000"
                    if term == "large_ticket_buy_majority"
                    else "90000"
                    if term == "large_ticket_sell_rising"
                    else "80000",
                    sell_notional="20000" if term != "large_ticket_sell_rising" else "10000",
                )
    elif term in ("depth_not_down", "ask_depth_building"):
        for row in packet["books"]:
            if parse_time(row["ts"]) > START:
                row["depth"]["ask"]["p1"]["usd"] = "900000000" if term == "depth_not_down" else "1000000000"
    elif term == "basis_within":
        packet["oi_state"] = dict(basis=20.0)
    elif term == "basis_widening":
        packet["oi_state"] = dict(basis=10.0)
    elif term == "funding_above":
        packet["funding_state"] = dict(current=".0001")
    elif term == "spot_sell_or_neutral":
        for row in packet["flow_bars"]:
            if row["instrument_id"].startswith("A:"):
                row.update(buy_qty="40", sell_qty="10")
            elif row["instrument_id"].startswith("B:"):
                row.update(buy_qty="10", sell_qty="20")
    elif term == "perp_taker_buy":
        for row in packet["flow_bars"]:
            if ":perp:" in row["instrument_id"]:
                row.update(buy_qty="10", sell_qty="10")
    elif term == "first_half_liquidation_majority":
        packet["funding_state"]["liquidations"]["P:perp:T"]["first_half_usd"] = 40.0
    elif term == "ask_replenishment":
        for row in packet["books"]:
            if parse_time(row["ts"]) > START:
                row["depth"]["ask"]["p1"]["usd"] = "1"
        for row in packet["flow_bars"]:
            if parse_time(row["minute_start"]) > START:
                row["buy_notional"] = row["sell_notional"]
    elif term in ("supply_evidence", "exchange_inflows"):
        packet["supply"] = []
    elif term == "native_not_up":
        packet["oi_state"] = dict(end_native=1000.0, end_usd=100100.0)
    elif term == "venue_signs_disagree":
        for row in packet["flow_bars"]:
            if ":spot:" in row["instrument_id"]:
                row.update(buy_qty="20", sell_qty="10")
    elif term == "gap_closes_quickly":
        for row in packet["books"]:
            if row["instrument_id"].startswith("B:") and parse_time(row["ts"]) == END:
                row["mid"] = "105"
                row["independent_rest"]["mid"] = "105"
    else:
        raise AssertionError("Unmapped written support term " + term)

    reduced, _ = raw_scores(packet, cls, None if term == "native_not_up" else native)
    assert reduced[cls] == maximum - 1
