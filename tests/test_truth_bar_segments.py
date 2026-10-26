"""TDD §14 typed bars carry exact requested boundary aggregates, not individual PG trades."""

from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from runpy import run_path

from truth_support import fixture_truth_config

from tank.analysis.truth import compute_market_truth
from tank.util import iso, parse_time


def fractional_packet():
    snapshot = run_path(str(Path(__file__).with_name("test_truth_start_fixture.py")))[
        "synthetic_start_snapshot"
    ]()
    end = parse_time(snapshot["cutoff"]) + timedelta(seconds=17)
    start = parse_time(snapshot["anchor"]["ts"]) + timedelta(seconds=17)
    snapshot.update(cutoff=iso(end), assembled_at=iso(end + timedelta(seconds=1)))
    snapshot["anchor"]["ts"] = iso(start)
    boundaries = {
        start,
        end - timedelta(days=1),
        end - timedelta(hours=4),
        end - timedelta(hours=1),
        end - timedelta(minutes=15),
    }
    for iid in ("A:spot:T", "B:spot:T"):
        for boundary in boundaries:
            minute = boundary.replace(second=0, microsecond=0)
            parent = next(
                row
                for row in snapshot["flow_bars"]
                if row["instrument_id"] == iid and parse_time(row["minute_start"]) == minute
            )
            parent.setdefault("boundary_segments", []).append(
                dict(
                    start=iso(boundary),
                    end=iso(minute + timedelta(minutes=1)),
                    complete=True,
                    observed_at=iso(end),
                    received_ts=iso(end),
                    received_ts_prec="COLLECTION_ONLY",
                    buy_qty="2",
                    sell_qty="1",
                    sum_sq_qty="5",
                    buy_notional="200",
                    sell_notional="100",
                    n_buy=1,
                    n_sell=1,
                )
            )
            if boundary == start:
                parent.setdefault("reference_points", []).append(
                    dict(
                        boundary=iso(start),
                        complete=True,
                        last_trade=dict(
                            trade_id="ANCHOR",
                            ts=iso(start - timedelta(microseconds=1)),
                            price="100.00000000000000000001",
                        ),
                    )
                )

        snapshot["flow_bars"].append(
            dict(
                instrument_id=iid,
                token_id="T",
                minute_start=iso(end.replace(second=0)),
                complete=False,
                boundary_segments=[
                    dict(
                        start=iso(end.replace(second=0)),
                        end=iso(end),
                        complete=True,
                        observed_at=iso(end),
                        received_ts=iso(end),
                        received_ts_prec="COLLECTION_ONLY",
                        buy_qty="3",
                        sell_qty="1",
                        sum_sq_qty="10",
                        buy_notional="300",
                        sell_notional="100",
                        n_buy=1,
                        n_sell=1,
                    )
                ],
                reference_points=[
                    dict(
                        boundary=iso(end),
                        complete=True,
                        last_trade=dict(
                            trade_id="END", ts=iso(end - timedelta(microseconds=1)), price="101.1"
                        ),
                    )
                ],
            )
        )
    return snapshot


def test_exact_bar_segments_support_fractional_window_and_current_cutoff_without_raw_trades() -> None:
    snapshot = fractional_packet()
    assert not any(key in snapshot for key in ("trades", "boundary_trades", "anchor_trades", "cutoff_trades"))
    result = compute_market_truth(snapshot, config=fixture_truth_config())
    event = result["spot"]["EVENT"]
    assert event["coverage"]["n_complete"] == 2
    assert Decimal(event["composite_max"]["cvd"]) == Decimal(2 * (239 * 50 + 1 + 2))
    assert Decimal(result["relative"]["anchor"]["ref_price"]) == Decimal("100.00000000000000000001")
    assert Decimal(result["relative"]["entry_ref_price"]) == Decimal("101.1")
    assert len(event["venues"][0]["boundary_segments"]) == 2


def test_unregistered_boundary_is_incomplete_and_raw_trade_arrays_cannot_override_bars() -> None:
    snapshot = fractional_packet()
    for row in snapshot["flow_bars"]:
        row["boundary_segments"] = []
    snapshot["boundary_trades"] = [
        dict(instrument_id="A:spot:T", ts=snapshot["anchor"]["ts"], side="BUY", qty_native="9999999")
    ]
    result = compute_market_truth(snapshot, config=fixture_truth_config())
    assert result["spot"]["EVENT"]["coverage"]["n_complete"] == 0
    assert {row["detail"] for row in result["spot"]["EVENT"]["coverage"]["excluded"]} == {
        "BOUNDARY_AGGREGATE_UNAVAILABLE"
    }
