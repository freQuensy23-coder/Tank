import json

import httpx
import pytest

from tank.clickhouse_market import ClickHouse


async def fetch(payload):
    queries = []

    def reply(request):
        queries.append(request.content.decode())
        return httpx.Response(200, content=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as http:
        rows = await ClickHouse("http://clickhouse.invalid:8123", http=http).rows("SELECT 1")
    return rows, queries


async def test_compact_wire_keeps_all_typed_values_and_shares_column_names():
    names = ["instrument_id", "oi_native", "metadata", "nullable", "count"]
    values = [
        ["venue:ENA", "-0.000", {"label": "λ\nquoted"}, None, 2**80],
        ["venue:BTC", "1E-18", {"label": "other"}, True, -3],
    ]
    payload = b"\n".join(json.dumps(row, ensure_ascii=False).encode() for row in [names, *values])
    rows, queries = await fetch(payload)
    assert rows == [dict(zip(names, row, strict=True)) for row in values]
    assert queries == ["SELECT 1 FORMAT JSONCompactEachRowWithNames"]
    assert next(iter(rows[0])) is next(iter(rows[1]))
    rows[0]["metadata"]["label"] = "caller-local"
    assert rows[1]["metadata"] == {"label": "other"}


async def test_compact_wire_empty_result_does_not_return_a_header_as_data():
    rows, _ = await fetch(b'["instrument_id","value"]\n')
    assert rows == []


@pytest.mark.parametrize(
    "payload",
    [b"42\n[1]\n", b'["value",1]\n[1,2]\n', b'["value"]\n{"value":1}\n', b'["value"]\n[1,2]\n'],
)
async def test_compact_wire_rejects_invalid_headers_or_row_width(payload):
    with pytest.raises(ValueError):
        await fetch(payload)


async def test_compact_wire_keeps_strict_invalid_utf8_failure():
    with pytest.raises(UnicodeDecodeError):
        await fetch(b'["value"]\n["\xff"]\n')
