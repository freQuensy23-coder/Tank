"""TDD §0 decimal monetary values, §2 Call entry price, §7 exact invalidation boundary."""

from decimal import Decimal
from pathlib import Path
from runpy import run_path

from relative_helpers import trade, with_references

from tank.analysis.truth import compute_market_truth

synthetic_start_snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
    "synthetic_start_snapshot"
]


def test_raw_trade_references_survive_truth_json_and_call_freeze_boundary():
    packet = synthetic_start_snapshot()
    anchor = "10000000000000000.01"
    entry = "10000000000000000.02"
    packet["relative_sql"] = with_references(
        packet["relative_sql"],
        anchor_trade=trade(anchor, packet["anchor"]["ts"]),
        entry_trade=trade(entry, packet["cutoff"]),
    )
    result = compute_market_truth(packet)
    relative = result["relative"]
    assert Decimal(relative["anchor"]["ref_price"]) == Decimal(anchor)
    assert Decimal(relative["entry_ref_price"]) == Decimal(entry)
    assert relative["anchor"]["evidence"]["price"] == anchor
    assert relative["windows"]["EVENT"]["r_raw"] > 0


def test_kline_fallback_saved_entry_and_hourly_close_remain_exact():
    packet = synthetic_start_snapshot()
    exact = Decimal("100.00000000000000000000000000001")
    packet["relative_sql"] = with_references(
        packet["relative_sql"],
        ("A:spot:T", "B:spot:T", "A:spot:BTC"),
        last_close=exact,
        hourly_close=exact,
        entry_trade=None,
    )
    result = compute_market_truth(packet)
    relative = result["relative"]
    assert Decimal(relative["entry_ref_price"]) == exact
    assert relative["entry_price_source"] == "KLINE_CLOSE"
    assert Decimal(relative["close_1h_price"]) == exact
    assert Decimal(relative["outcome_btc_reference"]["entry_ref_price"]) == exact


def test_resilience_does_not_round_a_real_new_low_into_an_equal_low():
    """At float resolution (03 §0): a low one part in 1e11 lower is a new low."""
    from tank.util import parse_time

    audit = run_path(str(Path(__file__).with_name("test_market_audit_paths.py")))
    START, derive, raw_packet = (audit[name] for name in ("START", "derive", "raw_packet"))

    packet = raw_packet()
    original = "100.000000001"
    lower = "100.000000000"
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
