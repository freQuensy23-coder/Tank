import json
import math
import random
import struct

import httpx
import pytest

from tank import clickhouse_market as chm


def wire_body(values):
    padding = json.dumps(["shared immutable column scalar"] * 40000).encode()
    body = b'{"padding":' + padding + b',"values":' + values + b"}"
    assert len(body) > 1_000_000
    return body


def scalar_signature(value):
    if type(value) is float:
        return (float, value.hex())
    if isinstance(value, dict):
        return {key: scalar_signature(item) for key, item in value.items()}
    if isinstance(value, list):
        return [scalar_signature(item) for item in value]
    return type(value), value


async def columns(body):
    def handler(request):
        assert request.content.endswith(b"FORMAT JSONColumns")
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        return await chm.ClickHouse("http://127.0.0.1:8123", http=http).columns("SELECT value")


async def test_large_finite_columns_skip_python_decoding_without_changing_scalar_representations(monkeypatch):
    generator = random.Random(7152)
    values = ["3.000", "1e-900", "-0.000", "Курс ₪", None, True, False, 0, 2**80, -0.0]
    for _ in range(2048):
        value = struct.unpack("!d", generator.getrandbits(64).to_bytes(8))[0]
        if math.isfinite(value):
            values.append(value)
    body = wire_body(json.dumps(values, ensure_ascii=True, allow_nan=False).encode())
    expected = scalar_signature(json.loads(body))
    decode = json.loads
    calls = []

    def observed(*args, **kwargs):
        calls.append(None)
        return decode(*args, **kwargs)

    monkeypatch.setattr(chm.json, "loads", observed)
    assert scalar_signature(await columns(body)) == expected
    assert not calls


@pytest.mark.parametrize(
    "values",
    [
        b'[1e999,NaN,Infinity,-Infinity,"\\ud800"]',
        b"[1.00000000000000011102230246251565404236316680908203125,4.9406564584124654e-324]",
        b'["\\ud83d\\ude00", "duplicate", "duplicate", -0.0]',
    ],
)
async def test_large_column_json_keeps_standard_decoder_value_semantics(values):
    body = wire_body(values)
    assert scalar_signature(await columns(body)) == scalar_signature(json.loads(body))


@pytest.mark.parametrize("values", [b"[01]", b"[+1]", b"[1,,2]", b'["unterminated]'])
async def test_large_invalid_columns_keep_standard_decoder_failure_semantics(values):
    with pytest.raises(json.JSONDecodeError):
        await columns(wire_body(values))


async def compact_rows(body):
    def handler(request):
        assert request.content.endswith(b"FORMAT JSONCompactEachRowWithNames")
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        return await chm.ClickHouse("http://127.0.0.1:8123", http=http).rows("SELECT value")


async def test_large_compact_population_reuses_qualified_json_decoder(monkeypatch):
    generator = random.Random(7152)
    values = ["3.000", "1e-900", "-0.000", "Курс ₪", None, True, False, 0, 2**80, -0.0]
    for _ in range(2048):
        value = struct.unpack("!d", generator.getrandbits(64).to_bytes(8))[0]
        if math.isfinite(value):
            values.append(value)
    population = wire_body(json.dumps(values, ensure_ascii=True, allow_nan=False).encode())
    body = b'["population"]\n[' + population + b"]\n"
    expected = scalar_signature([{"population": json.loads(population)}])
    decode = json.loads
    large_calls = []

    def observed(value, *args, **kwargs):
        if len(value) > 1_000_000:
            large_calls.append(None)
        return decode(value, *args, **kwargs)

    monkeypatch.setattr(chm.json, "loads", observed)
    assert scalar_signature(await compact_rows(body)) == expected
    assert not large_calls


@pytest.mark.parametrize(
    "values",
    [
        b'[1e999,NaN,Infinity,-Infinity,"\\ud800"]',
        b"[1.00000000000000011102230246251565404236316680908203125,4.9406564584124654e-324]",
        b'["\\ud83d\\ude00", "duplicate", "duplicate", -0.0]',
    ],
)
async def test_large_compact_population_keeps_standard_decoder_value_semantics(values):
    population = wire_body(values)
    body = b'["population"]\n[' + population + b"]\n"
    expected = scalar_signature([{"population": json.loads(population)}])
    assert scalar_signature(await compact_rows(body)) == expected


@pytest.mark.parametrize("values", [b"[01]", b"[+1]", b"[1,,2]", b'["unterminated]'])
async def test_large_invalid_compact_population_keeps_standard_decoder_failure_semantics(values):
    body = b'["population"]\n[' + wire_body(values) + b"]\n"
    with pytest.raises(json.JSONDecodeError):
        await compact_rows(body)
