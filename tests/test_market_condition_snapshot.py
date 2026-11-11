"""A condition evaluation reads the registry and the capture state, never a series: the numbers come from the statements."""

from datetime import timedelta

import httpx
import pytest

from tank.market.service import MarketService
from tank.util import iso, utcnow

SERIES = (
    "mkt.flow_bars_1m",
    "mkt.klines_1m",
    "mkt.book_summaries",
    "mkt.oi_obs",
    "mkt.funding_obs",
    "mkt.liq_obs",
    "mkt.tickers_24h",
)
METRICS = (
    "spot.cohort.direction",
    "deriv.native_oi_chg_pct",
    "deriv.funding_vs_normal",
    "price.close_vs_level",
    "dc.class",
    "liq.depth_p1_vs_pre",
    "mt.move_class",
    "price.repricing_band",
)


def spy(repo, name, reads, *, generator=False):
    original = getattr(repo, name)
    if generator:

        async def wrapped(collection, *args, **kwargs):
            reads.append(collection)
            async for row in original(collection, *args, **kwargs):
                yield row

    else:

        async def wrapped(collection, *args, **kwargs):
            reads.append(collection)
            return await original(collection, *args, **kwargs)

    setattr(repo, name, wrapped)


@pytest.mark.asyncio
@pytest.mark.parametrize("metric", METRICS)
async def test_the_condition_snapshot_of_a_metric_reads_no_series(setup, metric):
    repo, settings, inst = setup
    end = utcnow().replace(second=0, microsecond=0) - timedelta(hours=1)
    reads = []
    for name in ("latest_market_records", "latest_before"):
        spy(repo, name, reads)
    spy(repo, "iter_range", reads, generator=True)
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        packet = await service.condition_snapshot(
            inst["token_id"], end, requirements=[{"metric": metric, "window": "W1H"}]
        )
    assert not [collection for collection in reads if collection in SERIES]
    assert not {"flow_bars", "klines", "books", "oi", "funding", "liquidations", "tickers"} & set(packet)
    assert packet["token_id"] == inst["token_id"] and packet["instruments"]
    assert {"semantics", "capture_metadata", "tokens", "sector_baskets", "series_coverage_counts"} <= set(
        packet
    )


@pytest.mark.asyncio
async def test_the_assessment_snapshot_reads_no_series(setup):
    repo, settings, inst = setup
    end = utcnow().replace(second=0, microsecond=0) - timedelta(hours=1)
    reads = []
    for name in ("latest_market_records", "latest_before"):
        spy(repo, name, reads)
    spy(repo, "iter_range", reads, generator=True)
    anchor = {"type": "T0", "ts": iso(end - timedelta(hours=3))}
    async with httpx.AsyncClient() as http:
        service = MarketService(repo, settings, http=http)
        packet = await service.snapshot(inst["token_id"], end, anchor=anchor)
    assert not [collection for collection in reads if collection in SERIES]
    assert not {"flow_bars", "klines", "books", "oi", "funding", "liquidations", "tickers"} & set(packet)
    assert packet["instruments"] and "supply" in packet and "capture_status" in packet
