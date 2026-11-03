from decimal import Decimal, localcontext

import pytest

from tank.analysis.truth import derivatives_truth, oi_class, strict_majority
from tank.util import jsonable, parse_time


@pytest.mark.parametrize("sign", ["", "-"])
def test_funding_wire_retains_128_digit_rounded_nonterminating_conversion(sign):
    start = parse_time("2026-10-10T00:00:00Z")
    end = parse_time("2026-10-10T01:00:00Z")
    snapshot = {
        "token_id": "T",
        "funding": [
            {
                "instrument_id": "binance:perp:T",
                "kind": "SETTLED",
                "settlement_time": "2026-10-10T00:00:00Z",
                "rate": sign + "0.25",
                "interval_h": "3",
            }
        ],
    }

    result = jsonable(derivatives_truth(snapshot, start, end))

    expected = sign + "0." + "6" * 127 + "7"
    assert result["per_venue"][0]["funding_8h_eq"] == expected
    assert result["per_venue"][0]["funding_settled_8h_eq"] == expected


def test_position_classification_retains_majority_below_decimal256_scale():
    position = Decimal("0.5" + "0" * 98 + "1")
    terms = {
        "position": position,
        "markup": Decimal("0.5"),
        "interaction": Decimal(0),
        "pos_share": 0.5,
    }

    with localcontext():
        result = oi_class(0.08, 0.1, terms, [-0.01, 0.01], [-0.01, 0.01])

        assert result == "REAL_NATIVE"
        assert strict_majority(position, Decimal(1)) is True
        assert strict_majority(Decimal("0.5"), Decimal(1)) is False
