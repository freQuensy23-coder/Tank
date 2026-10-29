from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext

from tank.analysis import truth


def test_venue_windows_reuse_validated_columns_instead_of_repreparing_each_history(monkeypatch):
    first = datetime(2026, 10, 1, tzinfo=UTC)
    rows = [
        {"ts": first + timedelta(hours=index), "oi_native": "100.00", "oi_usd": "300.000"}
        for index in range(193)
    ]
    original, prepared_values = truth._history_decimal_arrays, []

    def observed(columns, **kwargs):
        prepared_values.append(sum(map(len, columns)))
        return original(columns, **kwargs)

    monkeypatch.setattr(truth, "_history_decimal_arrays", observed)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            admissible = truth._oi_split(rows)[0]
            assert truth._oi_history_arrays(admissible) is not None
            prepared_values.clear()
            end = rows[-1]["ts"]
            for hours in (1, 2):
                result = truth._oi_stats(rows, end - timedelta(hours=hours), end)
                assert result is not None
                assert result["oi_native_start"].as_tuple() == Decimal("100.00").as_tuple()
                assert result["oi_usd_end"].as_tuple() == Decimal("300.000").as_tuple()
                assert result["oi_native_chg_pct"] == result["oi_usd_chg_pct"] == 0.0
                assert result["native_normal_range"] == result["usd_normal_range"] == (0.0, 0.0)
            assert not prepared_values
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
