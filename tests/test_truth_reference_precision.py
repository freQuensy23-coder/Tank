"""TDD §0 decimal monetary values, §2 Call entry price, §7 exact invalidation boundary."""

from decimal import Decimal
from pathlib import Path
from runpy import run_path

from truth_support import fixture_truth_config

from tank.analysis.truth import compute_market_truth

synthetic_start_snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
    "synthetic_start_snapshot"
]


def test_raw_trade_references_survive_truth_json_and_call_freeze_boundary() -> None:
    packet = synthetic_start_snapshot()
    anchor = "10000000000000000.01"
    entry = "10000000000000000.02"
    for bar in packet["flow_bars"]:
        for point in bar.get("reference_points", []):
            point["last_trade"]["price"] = anchor if point["boundary"] == packet["anchor"]["ts"] else entry
    result = compute_market_truth(packet, config=fixture_truth_config())
    relative = result["relative"]
    assert Decimal(relative["anchor"]["ref_price"]) == Decimal(anchor)
    assert Decimal(relative["entry_ref_price"]) == Decimal(entry)
    assert relative["windows"]["EVENT"]["r_raw"] > 0


def test_kline_fallback_saved_entry_and_hourly_close_remain_exact() -> None:
    packet = synthetic_start_snapshot()
    exact = "100.00000000000000000000000000001"
    for row in packet["klines"]:
        row["c"] = exact
    for bar in packet["flow_bars"]:
        if any(point["boundary"] == packet["cutoff"] for point in bar.get("reference_points", [])):
            bar.pop("last_trade", None)
            bar["reference_points"] = []
    result = compute_market_truth(packet, config=fixture_truth_config())
    relative = result["relative"]
    assert Decimal(relative["entry_ref_price"]) == Decimal(exact)
    assert Decimal(relative["close_1h_price"]) == Decimal(exact)
    assert Decimal(relative["outcome_btc_reference"]["entry_ref_price"]) == Decimal(exact)


def test_resilience_does_not_round_a_real_new_low_into_an_equal_low() -> None:
    from tank.util import parse_time

    audit = run_path(str(Path(__file__).with_name("test_market_audit_paths.py")))
    START, derive, raw_packet = (audit[name] for name in ("START", "derive", "raw_packet"))

    packet = raw_packet()
    original = "100.00000000000000000000000000000000000001"
    lower = "100.00000000000000000000000000000000000000"
    for row in packet["flow_bars"]:
        if ":spot:" not in row["instrument_id"]:
            continue
        stamp = parse_time(row["minute_start"])
        row["low"] = lower
        if stamp == START:
            row.update(sell_notional="2000000000", buy_notional="0", low=original)
    liquidity = derive(packet)[2]
    observations = liquidity["replenishment"]["observations"]
    assert observations
    assert all(row["resilient"] is False for row in observations)
