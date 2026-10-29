from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext

import pytest

from tank.analysis import truth


@pytest.mark.parametrize("large", [False, True])
def test_composite_history_matches_independent_scalar_quantities_and_flags(large):
    start = datetime(2026, 10, 20, tzinfo=UTC)
    times = [start + timedelta(hours=index) for index in range(193)]
    histories = []
    for venue in range(2):
        rows = []
        for index in range(193):
            amount = (10**40 if large else 100) + (venue + 1) * 10 + index % 19
            rows.append({"oi_native": f"{amount}.00", "oi_usd": f"{3 * amount}.00"})
        histories.append((times, rows))
    with localcontext() as context:
        context.prec = 128
        context.clear_flags()
        expected = []
        for field in ("oi_native", "oi_usd"):
            series = []
            for index in range(1, 193):
                previous = sum((Decimal(rows[index - 1][field]) for _, rows in histories), Decimal(0))
                current = sum((Decimal(rows[index][field]) for _, rows in histories), Decimal(0))
                series.append(float(current / previous - 1))
            expected.append(series)
        expected_flags = dict(context.flags)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            result = truth._composite_history_vector(histories, timedelta(hours=1))
            assert result is not None
            assert [[value.hex() for value in part] for part in result] == [
                [value.hex() for value in part] for part in expected
            ]
            assert dict(context.flags) == expected_flags
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
