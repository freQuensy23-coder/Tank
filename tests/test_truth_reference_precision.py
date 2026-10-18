"""TDD §0 decimal monetary values, §2 Call entry price, §7 exact invalidation boundary."""

from decimal import Decimal
from pathlib import Path
from runpy import run_path

from tank.analysis.truth import compute_market_truth

synthetic_start_snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
    "synthetic_start_snapshot"
]


def test_raw_trade_references_survive_truth_json_and_call_freeze_boundary():
    packet = synthetic_start_snapshot()
    anchor = "10000000000000000.01"
    entry = "10000000000000000.02"
    for row in packet["anchor_trades"]:
        row["price"] = anchor
    packet["cutoff_trades"] = [
        dict(row, price=entry, ts="2026-10-16T11:59:59Z") for row in packet["anchor_trades"]
    ]
    result = compute_market_truth(packet)
    relative = result["relative"]
    assert Decimal(relative["anchor"]["ref_price"]) == Decimal(anchor)
    assert Decimal(relative["entry_ref_price"]) == Decimal(entry)
    assert relative["windows"]["EVENT"]["r_raw"] > 0
    # event_tools call freeze uses exactly this Decimal min and string serialization.
    assert str(min(Decimal(relative["anchor"]["ref_price"]), Decimal(relative["entry_ref_price"]))) == anchor


def test_kline_fallback_saved_entry_and_hourly_close_remain_exact():
    packet = synthetic_start_snapshot()
    exact = "100.00000000000000000000000000001"
    for row in packet["klines"]:
        row["c"] = exact
    result = compute_market_truth(packet)
    relative = result["relative"]
    assert Decimal(relative["entry_ref_price"]) == Decimal(exact)
    assert Decimal(relative["close_1h_price"]) == Decimal(exact)
    assert Decimal(relative["outcome_btc_reference"]["entry_ref_price"]) == Decimal(exact)


def test_resilience_does_not_round_a_real_new_low_into_an_equal_low():
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
