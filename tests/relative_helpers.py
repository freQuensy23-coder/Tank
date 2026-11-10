"""Hand-built records of the price-relative statements (analysis/sql/relative_baselines.sql,
reference_prices.sql, sector_returns.sql) for tests of the rules that read them."""

import dataclasses
import json
from datetime import timedelta
from decimal import Decimal

from tank.analysis import truth_sql
from tank.analysis.truth import WINDOWS
from tank.util import iso, parse_time

HORIZON_DEFAULTS = dict(
    beta_btc_pairs=2000,
    beta_btc_ols=1.2,
    beta_btc=1.1,
    beta_eth_pairs=2000,
    beta_eth_ols=1.0,
    beta_eth=1.0,
    raw_return=None,
    btc_return=None,
    eth_return=None,
    btc_adjusted_return=None,
    eth_adjusted_return=None,
    measure=None,
    sample_count=1000,
    history_q025=-0.02,
    history_q25=-0.005,
    history_q75=0.005,
    history_q975=0.02,
    percentile=0.5,
    history_first_end=parse_time("2026-07-20T00:00:00Z"),
    history_last_end=parse_time("2026-10-15T00:00:00Z"),
    price_first_close=parse_time("2026-07-20T00:00:00Z"),
    price_last_close=parse_time("2026-10-15T00:00:00Z"),
    history_resolution_s=60.0,
    btc_first_close=parse_time("2026-07-20T00:00:00Z"),
    btc_last_close=parse_time("2026-10-16T12:00:00Z"),
    btc_realized_vol_24h=0.005,
    btc_return_24h_q025=-0.03,
    btc_return_24h_q25=-0.008,
    btc_return_24h_q975=0.06,
    btc_realized_vol_24h_q025=0.001,
    btc_realized_vol_24h_q975=0.007,
    btc_return_24h_samples=2000,
    btc_realized_vol_24h_samples=2000,
    ref_realized_vol_24h=0.01,
    ref_realized_vol_24h_q025=0.004,
    ref_realized_vol_24h_q975=0.02,
    ref_realized_vol_24h_samples=2000,
    ref_hourly_first=parse_time("2026-07-20T00:00:00Z"),
    ref_hourly_last=parse_time("2026-10-16T12:00:00Z"),
)


def horizon(name, length_s=None, **changes):
    length = (
        length_s
        if length_s is not None
        else dict(WINDOWS, W12H=43200, W3D=259200, W7D=604800, PRE24H=86400)[name]
    )
    return truth_sql.RelativeHorizon(name=name, length_s=float(length), **(HORIZON_DEFAULTS | changes))


def trade(price, ts, trade_id="1"):
    return json.dumps(
        {"price": str(price), "trade_id": trade_id, "ts": ts, "ts_prec": "EXACT", "ts_prec_reason": None}
    )


def reference(iid, **changes):
    values = dict(
        instrument_id=iid,
        last_close=Decimal("100"),
        last_close_time=parse_time("2026-10-16T12:00:00Z"),
        last_close_prec="BUCKET(60s)",
        last_close_prec_reason=None,
        hourly_close=Decimal("99"),
        hourly_close_time=parse_time("2026-10-16T12:00:00Z"),
        hourly_close_prec="BUCKET(60s)",
        hourly_close_prec_reason=None,
        flow_volume_native=1000.0,
        flow_detail=None,
        anchor_trade=None,
        entry_trade=None,
    )
    return truth_sql.ReferencePrice(**(values | changes))


def evidence(ref="A:spot:T", references=(), horizons=(), peers=(), btc=None, eth=None):
    return truth_sql.RelativeEvidence(
        ref=ref,
        btc=btc,
        eth=eth,
        references=tuple(references),
        horizons=tuple(horizons),
        peers=tuple(peers),
    )


def standard_horizons(end, start, include_outcome=True, **by_name):
    """One usable horizon per name; `by_name` maps a name to changes."""
    names = [*WINDOWS, "EVENT", "PRE24H"] + (["W12H", "W3D", "W7D"] if include_outcome else [])
    length = {"EVENT": (end - start).total_seconds()}
    return [horizon(name, length.get(name), **by_name.get(name, {})) for name in names]


def start_evidence(end, anchor):
    """The records the statements return for tests/test_truth_start_fixture.py's synthetic START
    packet: the token T on venues A and B (A is the reference, both with complete 24 h of flow), BTC
    flat at 100, a rise of 1.1 % between the anchor and the cutoff and a 90-day history around it."""
    rise = 0.011
    entry = trade("101.1", iso(end - timedelta(microseconds=1)), "SYNTHETIC-END")
    anchor_trade = trade(100, iso(anchor - timedelta(microseconds=1)), "SYNTHETIC-ANCHOR-A")
    references = [
        reference(
            f"{venue}:spot:T",
            last_close=Decimal("101.1"),
            hourly_close=Decimal("101.1"),
            flow_volume_native=216000.0,
            anchor_trade=anchor_trade,
            entry_trade=entry,
        )
        for venue in ("A", "B")
    ] + [
        reference(
            "A:spot:BTC", last_close=Decimal("100"), hourly_close=Decimal("100"), flow_volume_native=None
        )
    ]
    horizons = []
    for name in (*WINDOWS, "EVENT", "PRE24H", "W12H", "W3D", "W7D"):
        seconds = (end - anchor).total_seconds() if name == "EVENT" else None
        r = rise if name == "EVENT" else 0.002
        measure = r if name != "PRE24H" else 0.0
        quantiles = (
            dict(history_q025=-0.05, history_q25=-0.02, history_q75=0.005, history_q975=0.06)
            if name == "EVENT"
            else dict(history_q025=-0.04, history_q25=-0.01, history_q75=0.02, history_q975=0.046)
        )
        horizons.append(
            horizon(
                name,
                seconds,
                raw_return=r,
                btc_return=0.0,
                btc_adjusted_return=r,
                eth_adjusted_return=None,
                measure=measure,
                beta_btc=1.0,
                beta_btc_ols=None,
                **quantiles,
            )
        )
    return evidence(ref="A:spot:T", btc="A:spot:BTC", references=references, horizons=horizons)


def with_references(evidence, instruments=("A:spot:T", "B:spot:T"), **changes):
    """The evidence with `changes` applied to the reference rows of `instruments`."""
    return dataclasses.replace(
        evidence,
        references=tuple(
            dataclasses.replace(row, **changes) if row.instrument_id in instruments else row
            for row in evidence.references
        ),
    )


def registry_evidence(snapshot):
    """Evidence for a snapshot whose registry decides the peers: the first spot instrument of the
    token is the reference, every instrument the registry chose for a peer token returns a distinct
    return per horizon (0.001 times the position of the peer token in the registry)."""
    from tank.analysis.truth import relative_registry

    end, start = parse_time(snapshot["cutoff"]), parse_time(snapshot["anchor"]["ts"])
    registry = relative_registry(snapshot)
    order = [row["token_id"] for row in snapshot.get("tokens", [])]
    ref = registry.candidates[0] if registry.candidates else None
    names = [*WINDOWS, "EVENT"]
    peers = [
        truth_sql.SectorReturn(instrument_id=iid, name=name, peer_return=0.001 * (order.index(identity) + 1))
        for identity, iid in registry.peers.items()
        if iid
        for name in names
    ]
    references = [
        reference(
            iid,
            anchor_trade=trade(100, iso(start - timedelta(microseconds=1))),
            entry_trade=trade("101.1", iso(end - timedelta(microseconds=1))),
        )
        for iid in registry.candidates
    ]
    horizons = standard_horizons(
        end, start, **{name: dict(raw_return=0.011, btc_adjusted_return=0.01, measure=0.01) for name in names}
    )
    return evidence(
        ref=ref, btc=registry.btc, eth=registry.eth, references=references, horizons=horizons, peers=peers
    )
