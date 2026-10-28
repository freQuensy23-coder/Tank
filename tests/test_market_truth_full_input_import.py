import gzip
import hashlib
import json
from decimal import Decimal

import httpx
import market_truth_full_inputs as full_inputs
import pytest

from tank.clickhouse_market import ClickHouse
from tank.market_schema import MARKET_COLUMNS


@pytest.fixture
async def native_import():
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, content=b"")

    http = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    ch = ClickHouse("http://localhost:8123", http=http, database="mkt_tank_test_fixture_" + "a" * 32)
    try:
        yield ch, requests
    finally:
        await ch.close()


def retained_batch(tmp_path, lines, *, schema=None, recursive=None):
    table = "mkt.klines_1m"
    payload = "\n".join(lines).encode() + b"\n"
    path = tmp_path / "rows.jsonl.gz"
    path.write_bytes(gzip.compress(payload))
    columns = sorted({key for line in lines for key in json.loads(line)})
    entry = {
        "table": table,
        "name": path.name,
        "columns": columns,
        "rows": len(lines),
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    privacy = {
        "top_level_allowlist": {table: columns},
        "recursive_key_allowlist": {table: recursive} if recursive else {},
    }
    schema = MARKET_COLUMNS[table] if schema is None else schema
    return full_inputs.retained_rows(tmp_path, entry, privacy, schema), entry, privacy


def captured_line(**changes):
    row = {
        "storage_key": "actual:key:2026-10-28T12:01:00.123456Z",
        "instrument_id": "binance:spot:ENAUSDT",
        "open_time": "2026-10-28 12:01:00.123456",
        "recorded_at": "2026-10-28 12:02:00.654321",
        "c": "0.123456789012345678901234567890123456789000",
        "h": None,
        "interval_s": 60,
        "complete": True,
        "n_trades": 9007199254740993,
    }
    row.update(changes)
    return json.dumps(row, separators=(",", ":"))


async def test_native_import_reuses_validated_json_without_changing_typed_payload(tmp_path, native_import):
    ch, requests = native_import
    line = captured_line()
    retained, _, _ = retained_batch(tmp_path, [line])
    batch = list(retained)
    await full_inputs.insert_clickhouse(ch, "mkt.klines_1m", batch)
    assert len(requests) == 1
    query, payload = requests[0].content.decode().split("\n", 1)
    assert query == f"INSERT INTO {ch.database}.klines_1m FORMAT JSONEachRow"
    assert payload == line[:-1] + ',"_version":1}'
    assert json.loads(payload) == {
        "storage_key": "actual:key:2026-10-28T12:01:00.123456Z",
        "instrument_id": "binance:spot:ENAUSDT",
        "open_time": "2026-10-28 12:01:00.123456",
        "recorded_at": "2026-10-28 12:02:00.654321",
        "c": "0.123456789012345678901234567890123456789000",
        "h": None,
        "interval_s": 60,
        "complete": True,
        "n_trades": 9007199254740993,
        "_version": 1,
    }
    assert "o" not in json.loads(payload)
    await ch.insert("mkt.klines_1m", [{**batch[0][0], "_version": 1}])
    assert json.loads(requests[1].content.decode().split("\n", 1)[1]) == json.loads(payload)
    assert requests[0].url.params == requests[1].url.params.copy_with(
        query_id=requests[0].url.params["query_id"]
    )
    assert {key: value for key, value in requests[0].headers.items() if key != "content-length"} == {
        key: value for key, value in requests[1].headers.items() if key != "content-length"
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"capture_sources": ["PUBLIC_REST", "PUBLIC_WS"]},
        {"semantic_fingerprint": "escaped\\value"},
        {"semantic_fingerprint": "literal snowman ☃ mkt"},
        {"semantic_fingerprint": "escaped mkt"},
    ],
)
async def test_native_import_falls_back_to_product_serializer(tmp_path, native_import, changes):
    ch, requests = native_import
    line = captured_line(**changes)
    if changes == {"semantic_fingerprint": "escaped mkt"}:
        line = line.replace("escaped mkt", "escaped \\u006dkt")
    schema = {**MARKET_COLUMNS["mkt.klines_1m"], "capture_sources": "text[]"}
    retained, _, _ = retained_batch(tmp_path, [line], schema=schema)
    batch = list(retained)
    await full_inputs.insert_clickhouse(ch, "mkt.klines_1m", batch)
    await ch.insert("mkt.klines_1m", [{**batch[0][0], "_version": 1}])
    assert len(requests) == 2
    assert requests[0].content == requests[1].content
    await ch.close()


async def test_native_import_preserves_decimal_serializer_refusal(tmp_path, native_import):
    ch, requests = native_import
    line = captured_line(c="decimal-marker").replace('"decimal-marker"', "0.123456789012345678901")
    retained, _, _ = retained_batch(tmp_path, [line])
    batch = list(retained)
    assert batch[0][0]["c"] == Decimal("0.123456789012345678901")
    with pytest.raises(TypeError):
        await full_inputs.insert_clickhouse(ch, "mkt.klines_1m", batch)
    assert requests == []
    await ch.close()


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"storage_key": None}, "Missing actual storage key"),
        ({"storage_key": ""}, "Missing actual storage key"),
        ({"instrument_id": None}, "Missing actual market identity"),
        ({"open_time": None}, "Missing actual market identity"),
    ],
)
async def test_native_import_refuses_missing_identities(tmp_path, native_import, changes, message):
    ch, requests = native_import
    retained, _, _ = retained_batch(tmp_path, [captured_line(**changes)])
    with pytest.raises(ValueError, match=message):
        await full_inputs.insert_clickhouse(ch, "mkt.klines_1m", list(retained))
    assert requests == []
    await ch.close()


@pytest.mark.parametrize(
    ("line", "message"),
    [
        (captured_line(c="Infinity"), "Nonfinite captured numeric"),
        (captured_line(c="NaN"), "Nonfinite captured numeric"),
        (captured_line(semantic_fingerprint="https://private.invalid"), "Private public-fixture value"),
        (captured_line(semantic_fingerprint="person@example.invalid"), "Private public-fixture value"),
        (captured_line(source_archive=None), "Source archive was not captured"),
        (captured_line()[:-1] + ',"c":"1"}', "Duplicate JSON field"),
    ],
)
async def test_native_import_keeps_per_row_validation_refusals(tmp_path, native_import, line, message):
    ch, requests = native_import
    retained, _, _ = retained_batch(tmp_path, [line])
    with pytest.raises(ValueError, match=message):
        await full_inputs.insert_clickhouse(ch, "mkt.klines_1m", list(retained))
    assert requests == []
    await ch.close()


@pytest.mark.parametrize("corruption", ["hash", "rows", "bytes", "union", "schema", "privacy"])
async def test_native_import_keeps_complete_stream_validation(tmp_path, native_import, corruption):
    ch, requests = native_import
    retained, entry, privacy = retained_batch(tmp_path, [captured_line()])
    if corruption == "hash":
        entry["sha256"] = "0" * 64
    elif corruption in {"rows", "bytes"}:
        entry[corruption] += 1
    elif corruption == "union":
        entry["columns"].append("o")
        privacy["top_level_allowlist"][entry["table"]] = entry["columns"]
    elif corruption == "schema":
        entry["columns"].append("unknown_column")
        privacy["top_level_allowlist"][entry["table"]] = entry["columns"]
    else:
        privacy["top_level_allowlist"][entry["table"]] = []
    with pytest.raises(ValueError):
        await full_inputs.insert_clickhouse(ch, "mkt.klines_1m", list(retained))
    assert requests == []
    await ch.close()


async def test_native_import_preserves_batch_order_and_empty_noop(tmp_path, native_import):
    ch, requests = native_import
    lines = [captured_line(storage_key=f"actual:{n}", c=str(n)) for n in (3, 1, 2)]
    retained, _, _ = retained_batch(tmp_path, lines)
    await full_inputs.insert_clickhouse(ch, "mkt.klines_1m", list(retained))
    payload = requests[0].content.decode().split("\n", 1)[1]
    assert [json.loads(line)["storage_key"] for line in payload.splitlines()] == [
        "actual:3",
        "actual:1",
        "actual:2",
    ]
    await full_inputs.insert_clickhouse(ch, "mkt.klines_1m", [])
    assert len(requests) == 1
    await ch.close()
