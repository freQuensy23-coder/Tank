from datetime import timedelta

import httpx
import pytest

from tank.market.service import MarketService
from tank.market_schema import MARKET_COLUMNS
from tank.util import iso, utcnow


@pytest.mark.parametrize("producer", ["projection", "history", "book_baseline", "series"])
async def test_condition_history_larger_than_cache_budget_is_returned_but_not_retained(
    setup, monkeypatch, producer
):
    repo, settings, instrument = setup
    monkeypatch.setattr("tank.market.service.WINDOW_ROWS", 3)
    cutoff = utcnow().replace(second=0, microsecond=0) - timedelta(hours=2)
    start = cutoff - timedelta(minutes=5)
    collection, field = (
        ("mkt.book_summaries", "exchange_ts")
        if producer == "book_baseline"
        else ("mkt.oi_obs", "ts")
        if producer == "history"
        else ("mkt.klines_1m", "open_time")
    )
    original = []
    for index in range(4):
        row = {
            "instrument_id": instrument["instrument_id"],
            "token_id": instrument["token_id"],
            field: iso(start + timedelta(minutes=index)),
            "recorded_at": iso(),
            "complete": True,
            "interval_s": 60,
            "open_time_prec": "BUCKET(60s)",
            "c": str(index + 1),
            "volume_native": str(index + 1),
            "oi_native": str(index + 1),
        }
        row = {key: value for key, value in row.items() if key in MARKET_COLUMNS[collection]}
        original.append(row)
        await repo.put(collection, f"row-{index}", row)
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        if producer == "projection":
            rows, _ = await service._condition_projected_history(
                collection,
                field,
                start,
                cutoff,
                iso(),
                fields=["instrument_id", "c"],
                filters={"instrument_id": instrument["instrument_id"]},
            )
            assert [row["c"] for row in rows] == ["1", "2", "3", "4"]
        elif producer == "history":
            rows = await service._condition_history(
                collection, field, instrument["instrument_id"], cutoff, utcnow()
            )
            assert [row["oi_native"] for row in rows] == ["1", "2", "3", "4"]
        elif producer == "book_baseline":
            rows = await service._condition_book_baseline(instrument["instrument_id"], cutoff, iso())
            assert [row["exchange_ts"] for row in rows] == [row[field] for row in original]
        else:
            series = await service._condition_kline_series(
                instrument, start, cutoff, iso(), kind="price", coverage_count=4
            )
            assert series["values"] == ["1", "2", "3", "4"]
        caches = [
            service.__dict__.get(name, {})
            for name in (
                "_condition_histories",
                "_condition_book_baselines",
                "_condition_series",
                "_condition_projections",
            )
        ]
        assert all(not cache for cache in caches)


def test_condition_cache_budget_is_shared_and_eviction_does_not_mutate_snapshots(setup, monkeypatch):
    repo, settings, _ = setup
    monkeypatch.setattr("tank.market.service.WINDOW_ROWS", 3)
    monkeypatch.setattr("tank.market.service.time.monotonic", lambda: 20)
    service = MarketService(repo, settings, http=object())
    older = [{"value": "old-1"}, {"value": "old-2"}]
    newer = [{"value": "new-1"}, {"value": "new-2"}]
    service._condition_projections = {"old": {"rows": older, "used": 18}}
    service._condition_histories = {"new": {"points": dict(enumerate(newer)), "used": 19}}
    service._bound_condition_caches()
    assert service._condition_projections == {}
    assert list(service._condition_histories) == ["new"]
    assert older == [{"value": "old-1"}, {"value": "old-2"}]
    monkeypatch.setattr("tank.market.service.time.monotonic", lambda: 20 + 1801)
    service._bound_condition_caches()
    assert service._condition_histories == {}
    assert newer == [{"value": "new-1"}, {"value": "new-2"}]
