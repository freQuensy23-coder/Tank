from tank.analysis import truth


def test_native_metadata_boundaries_have_compiled_dispatch():
    assert {"_row_time", "_row_available", "_scalar_decimal", "_fixed_decimal_column"} <= set(
        truth.__pyx_capi__
    )


def test_hot_native_callers_have_guarded_c_entry_points():
    assert {"_metadata_time", "_metadata_decimal", "_metadata_available"} <= set(truth.__pyx_capi__)


def test_guarded_metadata_dispatch_preserves_public_callbacks(monkeypatch):
    end = truth.parse_time("2026-10-19T00:00:00Z")
    row = {"instrument_id": "A:perp:T", "ts": end, "recorded_at": end}
    snapshot = {"cutoff": end, "oi": [row]}
    expected = truth.bounded_market_snapshot(snapshot)
    seen = []

    def observe(name, function):
        def call(*args, **kwargs):
            seen.append(name)
            return function(*args, **kwargs)

        return call

    for name in ("decimal", "_time", "_available"):
        monkeypatch.setattr(truth, name, observe(name, getattr(truth, name)))
    assert truth.ratio("1", "2") == 0.5
    assert truth.bounded_market_snapshot(snapshot) == expected
    assert {"decimal", "_time", "_available"} <= set(seen)
