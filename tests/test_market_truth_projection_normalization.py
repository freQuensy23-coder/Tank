import json
import logging
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from types import SimpleNamespace

import httpx
import pytest

from tank import clickhouse_market as chm
from tank import market_storage, util
from tank.market import service as market
from tank.market.service import MarketService
from tank.storage import MemoryRepository


@pytest.fixture
def projection_probe(monkeypatch):
    active = ContextVar("projection_normalization_active", default=False)
    probe = SimpleNamespace(parses=[], spans=[], pages=[])
    original_parse = util.parse_time
    original_span = chm.timing_span
    original_page = chm.projection_page

    def parse(value):
        if active.get():
            probe.parses.append(value)
        return original_parse(value)

    @contextmanager
    def span(stage, *args, **kwargs):
        if stage != "clickhouse.projection_convert":
            with original_span(stage, *args, **kwargs) as result:
                yield result
            return
        probe.spans.append(dict(kwargs, thread=threading.get_ident()))
        token = active.set(True)
        try:
            with original_span(stage, *args, **kwargs) as result:
                yield result
        finally:
            active.reset(token)

    async def page(*args, **kwargs):
        result = await original_page(*args, **kwargs)
        probe.pages.append(result)
        return result

    monkeypatch.setattr(util, "parse_time", parse)
    monkeypatch.setattr(chm, "parse_time", parse)
    monkeypatch.setattr(chm, "timing_span", span)
    monkeypatch.setattr(chm, "projection_page", page)
    return probe


async def read_projection(
    batches,
    fields,
    *,
    collection="mkt.klines_1m",
    time_field="open_time",
    batch_size=2000,
):
    requests = []

    def respond(request):
        batch = batches[len(requests)]
        requests.append(request)
        return httpx.Response(200, content="\n".join(json.dumps(row) for row in batch))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        repo = MemoryRepository()
        repo.clickhouse = chm.ClickHouse("http://clickhouse.test", http=http)
        with chm.timing_span("test.projection_normalization", root=True):
            rows = [
                row
                async for row in repo.iter_market_projection(
                    collection,
                    fields,
                    time_field,
                    start="2026-10-28T00:00:00Z",
                    end="2026-10-28T01:00:00Z",
                    assembled_at="2026-10-29T00:00:00Z",
                    instrument_ids=["test:spot:T"],
                    batch_size=batch_size,
                )
            ]
    return rows, requests


@pytest.mark.parametrize("count", [1, 1000, 1001])
async def test_dense_public_projection_normalizes_each_timestamp_once(count, projection_probe, caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    main_thread = threading.get_ident()
    raw = [
        {
            "open_time": "2026-10-28T03:00:00.123456+03:00",
            "recorded_at": "2026-10-28 00:01:00.654321" if index % 2 else None,
            "c": "1.2345678901234567890123456789",
            "volume_native": "12345678901234567890.12345678901234567890",
            "complete": index % 3 != 0,
            "open_time_prec": "BUCKET(60s)" if index % 2 else None,
            "storage_key": f"bar-{index:04d}",
            "_cursor_time": "2026-10-28T03:00:00.123456+03:00",
            "_cursor_key": f"bar-{index:04d}",
        }
        for index in range(count)
    ]
    fields = [
        "open_time",
        "recorded_at",
        "c",
        "volume_native",
        "complete",
        "open_time_prec",
        "storage_key",
    ]
    result, requests = await read_projection([raw], fields)
    assert result == [
        {
            "open_time": "2026-10-28T00:00:00.123456Z",
            "recorded_at": "2026-10-28T00:01:00.654321Z" if index % 2 else None,
            "c": "1.2345678901234567890123456789",
            "volume_native": "12345678901234567890.12345678901234567890",
            "complete": index % 3 != 0,
            "open_time_prec": "BUCKET(60s)" if index % 2 else None,
            "storage_key": f"bar-{index:04d}",
        }
        for index in range(count)
    ]
    assert [entry[:2] for entry in projection_probe.pages[0]] == [
        (datetime(2026, 10, 28, 0, 0, 0, 123456, tzinfo=UTC), f"bar-{index:04d}")
        for index in range(count)
    ]
    assert len(projection_probe.spans) == 1
    conversion = projection_probe.spans[0]
    assert conversion["rows"] == count
    assert (conversion["thread"] == main_thread) is (count <= 1000)
    events = [
        json.loads(record.getMessage())
        for record in caplog.records
        if record.name == "tank.market_timing"
    ]
    ended = [event for event in events if event["event"] == "stage_end"]
    converted = [event for event in ended if event["stage"] == "clickhouse.projection_convert"]
    assert len(converted) == 1
    assert converted[0]["query_id"] == conversion["query_id"] == requests[0].url.params["query_id"]
    assert len([event for event in ended if event["stage"] == "threadpool.work"]) == int(count > 1000)
    nonnull_timestamp_cells = count + count // 2
    assert len(projection_probe.parses) <= 2 * nonnull_timestamp_cells


@pytest.mark.parametrize("include_time", [False, True])
async def test_public_projection_aliases_preserve_keyset_ties(include_time, projection_probe):
    fields = ["rate", "settlement_time_prec", "recorded_at", "storage_key"]
    if include_time:
        fields += ["settlement_time", "funding_time"]
    raw = []
    for key in ("a", "b", "c"):
        row = {
            "rate": "0.00000000000000000000001",
            "settlement_time_prec": None,
            "recorded_at": None,
            "storage_key": key,
            "_cursor_time": "2026-10-28 00:00:00.123456",
            "_cursor_key": key,
        }
        if include_time:
            row.update(
                settlement_time="2026-10-28 00:00:00.123456",
                funding_time="2026-10-28 00:00:00.123456",
            )
        raw.append(row)
    result, requests = await read_projection(
        [raw[:2], raw[2:]],
        fields,
        collection="mkt.funding_obs",
        time_field="settlement_time",
        batch_size=2,
    )
    expected = [
        {
            "rate": "1E-23",
            "settlement_time_prec": None,
            "recorded_at": None,
            "storage_key": key,
        }
        for key in ("a", "b", "c")
    ]
    if include_time:
        for row in expected:
            row.update(
                settlement_time="2026-10-28T00:00:00.123456Z",
                funding_time="2026-10-28T00:00:00.123456Z",
            )
    assert result == expected
    assert [entry[:2] for page in projection_probe.pages for entry in page] == [
        (datetime(2026, 10, 28, 0, 0, 0, 123456, tzinfo=UTC), key) for key in ("a", "b", "c")
    ]
    query = requests[1].content.decode()
    assert "`funding_time` AS _cursor_time" in query
    assert "ORDER BY `funding_time`, storage_key LIMIT 2" in query
    assert "`funding_time` >" in query and "storage_key >" in query
    assert "2026-10-28 00:00:00.123456" in requests[1].url.params.values()
    assert "b" in requests[1].url.params.values()
    assert len(projection_probe.parses) <= 2 * len(raw)


class ProjectionRows:
    def __init__(self, rows):
        self.raw = rows

    async def rows(self, query, params):
        return self.raw


async def convert_row(row, selected):
    return await chm.projection_page(
        ProjectionRows([row]),
        "mkt.klines_1m",
        selected,
        "open_time",
        start=None,
        end=None,
        filters=None,
        instrument_ids=None,
        assembled_at=None,
        after=None,
        limit=2,
    )


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("2026-10-28T03:00:00.123456+03:00", "2026-10-28T00:00:00.123456Z"),
        ("2026-10-28 00:00:00.123456", "2026-10-28T00:00:00.123456Z"),
        (datetime(2026, 10, 28, 0, 0, 0, 123456), "2026-10-28T00:00:00.123456Z"),
        (datetime(2026, 10, 28, tzinfo=UTC), "2026-10-28T00:00:00Z"),
        (
            datetime.fromisoformat("2026-10-28T03:00:00.123456+03:00"),
            "2026-10-28T00:00:00.123456Z",
        ),
        (1793145600, "2026-10-28T00:00:00Z"),
        (1793145600000, "2026-10-28T00:00:00Z"),
        (1793145600000000, "2026-10-28T00:00:00Z"),
        ("1793145600", "2026-10-28T00:00:00Z"),
        (Decimal("1793145600"), "2026-10-28T00:00:00Z"),
        (True, "1970-01-01T00:00:01Z"),
    ],
)
async def test_projection_preserves_existing_timestamp_input_policy(raw, expected):
    page = await convert_row(
        {"open_time": raw, "_cursor_time": raw, "_cursor_key": "bar"}, {"open_time": "open_time"}
    )
    assert page == [(datetime.fromisoformat(expected.replace("Z", "+00:00")), "bar", {"open_time": expected})]


@pytest.mark.parametrize("field", ["open_time", "recorded_at", "_cursor_time"])
@pytest.mark.parametrize("invalid", ["not-a-timestamp", {}, []])
async def test_projection_does_not_skip_invalid_timestamp_fields(field, invalid):
    row = {
        "open_time": "2026-10-28T00:00:00Z",
        "recorded_at": "2026-10-28T00:00:00Z",
        "_cursor_time": "2026-10-28T00:00:00Z",
        "_cursor_key": "bar",
    }
    row[field] = invalid
    with pytest.raises(ValueError):
        await convert_row(row, {"open_time": "open_time", "recorded_at": "recorded_at"})


async def test_projection_validates_a_distinct_cursor_and_preserves_native_decimals():
    row = {
        "open_time": "2026-10-28T00:00:00.123456Z",
        "recorded_at": None,
        "c": Decimal("1.2345678901234567890123456789"),
        "volume_native": Decimal("12345678901234567890.12345678901234567890"),
        "_cursor_time": "2026-10-28T00:01:00.654321Z",
        "_cursor_key": "bar",
    }
    before = deepcopy(row)
    with localcontext() as context:
        context.prec = 7
        context.clear_flags()
        page = await convert_row(row, {key: key for key in ("open_time", "recorded_at", "c", "volume_native")})
        assert context.prec == 7 and not any(context.flags.values())
    assert page == [
        (
            datetime(2026, 10, 28, 0, 1, 0, 654321, tzinfo=UTC),
            "bar",
            {
                "open_time": "2026-10-28T00:00:00.123456Z",
                "recorded_at": None,
                "c": "1.2345678901234567890123456789",
                "volume_native": "12345678901234567890.12345678901234567890",
            },
        )
    ]
    assert row == before
    row["c"] = None
    row["volume_native"] = "not-a-decimal"
    with localcontext() as context:
        context.traps[InvalidOperation] = True
        with pytest.raises(InvalidOperation):
            await convert_row(row, {"c": "c", "volume_native": "volume_native"})
    row["volume_native"] = None
    assert (await convert_row(row, {"c": "c", "volume_native": "volume_native"}))[0][2] == {
        "c": None,
        "volume_native": None,
    }
    row["_cursor_time"] = None
    with pytest.raises(ValueError):
        await convert_row(row, {"c": "c"})


@pytest.mark.parametrize("kind", ["price", "volume"])
async def test_series_reuses_normalized_opens_without_changing_vectors_or_fingerprints(kind, setup, monkeypatch):
    repo, settings, instrument = setup
    start = datetime(2026, 10, 28, tzinfo=UTC)
    opened = [start + timedelta(minutes=index, microseconds=123456) for index in range(1, 9)]
    closes = ["12.125", "0", "NaN", "-1", "13", "14", None, "15"]
    volumes = [
        "12345678901234567890.12345678901234567890",
        "0",
        None,
        "-1",
        "2.00",
        "3.00000000000000000001",
        "4",
        "5",
    ]
    rows = [
        {
            "open_time": at.isoformat(),
            "recorded_at": None if index == 0 else "2026-10-28T01:00:00Z",
            "interval_s": 60,
            "complete": index != 4,
            "open_time_prec": "EXACT" if index == 1 else None if index == 2 else "BUCKET(60s)",
            "c": closes[index],
            "volume_native": volumes[index],
        }
        for index, at in enumerate(opened)
    ]
    rows.append(
        dict(rows[0], open_time="2026-10-28T03:01:00.123456+03:00", c="16.5", volume_native="9.0001")
    )
    rows.extend(
        [
            dict(rows[0], open_time="2026-10-27T23:59:00Z", c="99", volume_native="99"),
            dict(rows[0], open_time="2026-10-28T00:09:30Z", c="99", volume_native="99"),
            dict(rows[0], interval_s=300, c="99", volume_native="99"),
            dict(rows[0], recorded_at="2026-10-29T00:00:00Z", c="99", volume_native="99"),
        ]
    )
    before = deepcopy(rows)
    calls = []
    original_parse = util.parse_time
    watched = set(opened)

    def parse(value):
        result = original_parse(value)
        if result in watched:
            calls.append(value)
        return result

    monkeypatch.setattr(util, "parse_time", parse)
    monkeypatch.setattr(market, "parse_time", parse)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(500))) as http:
        service = MarketService(repo, settings, http=http)
        result = await service._condition_kline_series(
            instrument,
            start,
            start + timedelta(minutes=10),
            "2026-10-28T02:00:00Z",
            kind=kind,
            coverage_count=7,
            fixed_window=True,
            captured_rows=rows,
        )
    if kind == "price":
        assert list(result["times_us"]) == [1793145720123456, 1793146020123456, 1793146140123456]
        assert list(result["values"]) == [16.5, 14.0, 15.0]
        assert list(result["complete"]) == [1, 1, 1]
        assert result["time_basis"] == "CLOSE_TIME"
        assert result["dependency_fingerprint"] == (
            "0222802e0452f6d4e6e135444b254be9439718338a99512189118f942c0b26a0"
        )
    else:
        assert list(result["times_us"]) == [
            1793145660123456,
            1793145720123456,
            1793145780123456,
            1793145840123456,
            1793145900123456,
            1793145960123456,
            1793146020123456,
            1793146080123456,
        ]
        assert result["values"] == ["9.0001", "0", None, "-1", "2.00", "3.00000000000000000001", "4", "5"]
        assert list(result["complete"]) == [1, 1, 0, 0, 0, 1, 1, 1]
        assert result["time_basis"] == "OPEN_TIME"
        assert result["dependency_fingerprint"] == (
            "c7a15384fcacef9afe880aa2778645c1eb75212a3bc25f82d94d95f3b48d224b"
        )
    assert result["precision"] is None and result["precision_reason"] == "INCOMPLETE"
    assert result["coverage_count"] == 7
    assert result["previous_dependency_fingerprint"] is None
    assert result["append_only"] is False
    assert result["changed_from_us"] == 1793145660123456
    assert rows == before
    checked_open_inputs = 10
    assert len(calls) <= 2 * checked_open_inputs


async def test_series_cache_reuses_equivalent_times_and_invalidates_corrected_values(setup, monkeypatch):
    repo, settings, instrument = setup
    start = datetime(2026, 10, 28, tzinfo=UTC)
    monkeypatch.setattr(market_storage, "utcnow", lambda: datetime(2026, 10, 28, 2, 30, tzinfo=UTC))
    reads = []
    original_projection = repo.iter_market_projection

    async def projection(*args, **kwargs):
        reads.append(kwargs)
        async for row in original_projection(*args, **kwargs):
            yield row

    monkeypatch.setattr(repo, "iter_market_projection", projection)
    rows = [
        {
            "instrument_id": instrument["instrument_id"],
            "token_id": instrument["token_id"],
            "open_time": "2026-10-28T03:01:00.123456+03:00",
            "open_time_prec": "BUCKET(60s)",
            "recorded_at": "2026-10-28T01:00:00Z",
            "interval_s": 60,
            "complete": True,
            "volume_native": "12345678901234567890.12345678901234567890",
        }
    ]
    await repo.put_market_batch("mkt.klines_1m", rows, keys=["one"])
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(500))) as http:
        service = MarketService(repo, settings, http=http)

        async def series(coverage_count, captured_rows=None, assembled_at="2026-10-28T02:00:00Z"):
            return await service._condition_kline_series(
                instrument,
                start,
                start + timedelta(minutes=3),
                assembled_at,
                kind="volume",
                coverage_count=coverage_count,
                fixed_window=True,
                captured_rows=captured_rows,
            )

        first = await series(1)
        repeated = await series(2)
        assert len(reads) == 1
        captured = await series(1, [dict(rows[0], open_time="2026-10-28 00:01:00.123456")])
        assert list(first["times_us"]) == [1793145660123456]
        assert first["values"] == ["12345678901234567890.12345678901234567890"]
        assert repeated["coverage_count"] == 2
        assert first["dependency_fingerprint"] == repeated["dependency_fingerprint"]
        assert first["dependency_fingerprint"] == captured["dependency_fingerprint"]
        corrected = dict(rows[0], volume_native="9.0001", recorded_at="2026-10-28T02:30:00Z")
        await repo.put_market_batch("mkt.klines_1m", [corrected], keys=["one"])
        changed = await series(3, assembled_at="2026-10-28T03:00:00Z")
    assert changed["values"] == ["9.0001"]
    assert len(reads) == 1
    assert changed["dependency_fingerprint"] != first["dependency_fingerprint"]
    assert changed["append_only"] is False
    assert changed["previous_dependency_fingerprint"] == first["dependency_fingerprint"]
    assert first["values"] == ["12345678901234567890.12345678901234567890"]
