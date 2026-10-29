from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal, Rounded, localcontext

import pytest

from tank.analysis import truth


@pytest.mark.parametrize("derived", [False, True])
@pytest.mark.parametrize("sticky", [False, True])
def test_combined_timeline_reuses_only_stored_usd_history_columns(monkeypatch, derived, sticky):
    times = [datetime(2026, 10, 20, tzinfo=UTC) + timedelta(minutes=index) for index in range(256)]
    indexed = [
        (
            times,
            [
                {
                    "oi_native": f"{100 + 100 * venue + index}.00",
                    "oi_usd": f"{300 + 300 * venue + 3 * index}.00",
                    "oi_usd_source": "OI_NATIVE_X_ROW_MARK" if derived and venue == 0 else None,
                }
                for index in range(len(times))
            ],
        )
        for venue in range(2)
    ]
    before = deepcopy(indexed)
    original, prepared_values = truth._history_decimal_arrays, []

    def observed(columns, **kwargs):
        prepared_values.append(sum(map(len, columns)))
        return original(columns, **kwargs)

    monkeypatch.setattr(truth, "_history_decimal_arrays", observed)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            context.flags[Rounded] = sticky
            flags = dict(context.flags)
            for _, rows in indexed:
                assert truth._oi_history_arrays(rows) is not None
            combined = truth._oi_combined_timeline(indexed, 60)
            assert len(combined) == len(times)
            for index, row in enumerate(combined):
                for field in ("oi_native", "oi_usd"):
                    expected = sum((Decimal(rows[index][field]) for _, rows in indexed), Decimal(0))
                    assert row[field].as_tuple() == expected.as_tuple()
            assert sum(prepared_values) == (6 if derived else 4) * len(times)
            assert context.prec == 128 and dict(context.flags) == flags
            assert indexed == before
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


def test_unsupported_context_preserves_custom_row_extraction_failure():
    class RefusingRow(dict):
        def get(self, name, default=None):
            if name == "oi_native":
                raise RuntimeError("native getter refused")
            return super().get(name, default)

    first = datetime(2026, 10, 20, tzinfo=UTC)
    indexed = [
        ([first], [{"oi_native": "10", "oi_usd": "100"}]),
        ([first + timedelta(days=1)], [RefusingRow(oi_native="20", oi_usd="200")]),
    ]
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 32
            with pytest.raises(RuntimeError, match="native getter refused"):
                truth._oi_combined_timeline(indexed, 60)
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
