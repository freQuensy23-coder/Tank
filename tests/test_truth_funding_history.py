"""TDD §0 weekly-cycle sufficiency belongs to the actual pre-window history."""

from datetime import timedelta

import pytest

from tank.analysis.truth import derivatives_truth
from tank.util import iso, parse_time


@pytest.mark.parametrize("history_count,expected", [(18, None), (22, "ABOVE")])
def test_current_settlement_cannot_extend_training_history_into_a_week(history_count, expected) -> None:
    start = parse_time("2026-10-10T00:00:00Z")
    end = start + timedelta(days=1)
    iid = "binance:perp:T"
    funding = [
        dict(
            instrument_id=iid,
            kind="SETTLED",
            settlement_time=iso(start - timedelta(hours=8 * i)),
            rate=".0001",
            interval_h=8,
        )
        for i in range(1, history_count + 1)
    ]
    funding.append(
        dict(instrument_id=iid, kind="SETTLED", settlement_time=iso(end), rate=".01", interval_h=8)
    )

    funding.append(
        dict(
            instrument_id=iid,
            kind="SETTLED",
            settlement_time=iso(start - timedelta(days=30)),
            rate=None,
            interval_h=8,
        )
    )
    snapshot = dict(
        token_id="T",
        funding=funding,
        instruments=[dict(instrument_id=iid, token_id="T", market_type="PERP_LINEAR")],
    )
    venue = derivatives_truth(snapshot, start, end)["per_venue"][0]
    assert venue["funding_vs_normal"] == expected
    assert venue["funding_8h_eq"] is not None


def test_settled_and_estimated_funding_preserve_their_own_intervals() -> None:
    from decimal import Decimal

    start = parse_time("2026-10-10T00:00:00Z")
    end = start + timedelta(hours=1)
    iid = "binance:perp:T"
    packet = dict(
        token_id="T",
        funding=[
            dict(instrument_id=iid, kind="SETTLED", settlement_time=iso(start), rate=".001", interval_h="8"),
            dict(instrument_id=iid, kind="CURRENT_ESTIMATE", ts=iso(end), rate=".002", interval_h="4"),
        ],
    )
    venue = derivatives_truth(packet, start, end)["per_venue"][0]
    assert venue["funding_settled_last"] == Decimal(".001")
    assert venue["funding_settled_interval_h"] == Decimal("8")
    assert venue["funding_settled_last_observed_at"] == iso(start)
    assert venue["funding_estimate"] == Decimal(".002")
    assert venue["funding_estimate_interval_h"] == Decimal("4")
    assert venue["funding_estimate_observed_at"] == iso(end)
    assert venue["funding_settled_8h_eq"] == Decimal(".001")
    assert venue["funding_estimate_8h_eq"] == Decimal(".004")
    assert venue["funding_8h_kind"] == "CURRENT_ESTIMATE"
    assert venue["interval_h"] == venue["funding_estimate_interval_h"]
