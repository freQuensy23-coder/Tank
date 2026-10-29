from datetime import UTC, datetime, timedelta
from decimal import Decimal, localcontext

from tank.analysis import truth


def test_composite_stats_do_not_revisit_unused_historical_rows(monkeypatch):
    start = datetime(2026, 10, 20, tzinfo=UTC)
    times = [start + timedelta(hours=index) for index in range(193)]
    histories = []
    for venue in range(2):
        histories.append(
            (
                times,
                [
                    {
                        "oi_native": f"{100 + 100 * venue + index % 19}.00",
                        "oi_usd": f"{300 + 300 * venue + 3 * (index % 19)}.00",
                    }
                    for index in range(193)
                ],
            )
        )
    rows = [
        {"ts": at, "oi_native": f"{300 + 2 * (index % 19)}.00", "oi_usd": f"{900 + 6 * (index % 19)}.00"}
        for index, at in enumerate(times)
    ]
    original, visited = truth._time, []

    def observed(row):
        visited.append(row)
        return original(row)

    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            truth._oi_split(rows)
            monkeypatch.setattr(truth, "_time", observed)
            result = truth._oi_stats(rows, times[-2], times[-1], venue_history=histories)
            assert result is not None
            assert result["oi_native_start"] == Decimal(rows[-2]["oi_native"])
            assert result["oi_native_end"] == Decimal(rows[-1]["oi_native"])
            assert result["native_normal_range"] is not None
            assert len(visited) == 4
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)
