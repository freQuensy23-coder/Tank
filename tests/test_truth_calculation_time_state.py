from datetime import UTC, datetime

from tank.analysis import truth


def test_owned_time_scope_does_not_reload_assessment(monkeypatch):
    class NoRead:
        def get(self):
            raise AssertionError("repeated assessment lookup")

    rows = [{"ts": "2026-10-28T00:00:00Z"}, {}]
    expected = [datetime(2026, 10, 28, tzinfo=UTC), None]

    def compute(snapshot, *, config):
        inputs = truth._ASSESSMENT_INPUTS.get()
        with monkeypatch.context() as patch:
            patch.setattr(truth, "_ASSESSMENT_INPUTS", NoRead())
            for _ in range(3):
                assert [truth._time(row) for row in rows] == expected
        assert len(inputs["row_times"]) == 2
        assert all(inputs["row_owners"][id(row)] is row for row in rows)
        return {"checked": True}

    monkeypatch.setattr(truth, "_compute_market_truth", compute)
    before = truth._TIMES.get(), truth._ASSESSMENT_INPUTS.get()
    assert truth._compute_market_truth_slot({}) == {"checked": True}
    assert (truth._TIMES.get(), truth._ASSESSMENT_INPUTS.get()) == before
