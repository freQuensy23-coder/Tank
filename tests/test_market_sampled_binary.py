import json
import struct
from decimal import getcontext, localcontext

import httpx
import pytest

from tank import clickhouse_market as chm

FINGERPRINT = "A" * 64
KINDS = ("String", "Nullable(String)", "Bool", "Int64", "UInt64", chm.DT, "Array(String)")


def varuint(number):
    encoded = bytearray()
    while number >= 128:
        encoded.append((number & 127) | 128)
        number >>= 7
    encoded.append(number)
    return bytes(encoded)


def string(value):
    body = value if isinstance(value, bytes) else value.encode("utf-8")
    return varuint(len(body)) + body


def scalar(kind, value):
    if kind.startswith("Nullable("):
        return b"\x01" if value is None else b"\x00" + scalar(kind[9:-1], value)
    if kind == "String":
        return string(value)
    if kind == "Bool":
        return bytes([int(value)])
    if kind in {"Int64", chm.DT}:
        return struct.pack("<q", value)
    if kind == "UInt64":
        return struct.pack("<Q", value)
    if kind == "Array(String)":
        return varuint(len(value)) + b"".join(string(item) for item in value)
    raise AssertionError(kind)


def population(kinds, records):
    return (
        string(FINGERPRINT)
        + varuint(len(records))
        + b"".join(
            scalar(kind, value) for record in records for kind, value in zip(kinds, record, strict=True)
        )
    )


def test_sampled_binary_decodes_typed_population_without_changing_decimal_state():
    records = [
        ["1.23000000", None, True, -(2**63), 2**64 - 1, -1, ["α", "", "a\x00b"]],
        ["9" * 100 + ".00000000", "0E-8", False, 2**63 - 1, 0, 1_000_001, []],
    ]
    expected = [list(record) for record in records]
    expected[0][5] = "1969-12-31T23:59:59.999999Z"
    expected[1][5] = "1970-01-01T00:00:01.000001Z"
    with localcontext() as context:
        context.prec = 9
        context.clear_flags()
        before = context.copy()
        result = chm._sampled_binary(population(KINDS, records), KINDS)
        assert result == [{"fingerprint": FINGERPRINT, "records": expected}]
        assert type(result[0]["records"][0][2]) is bool
        assert type(result[0]["records"][0][3]) is int
        assert context.flags == before.flags
        assert context.traps == before.traps
        assert context.prec == before.prec


def test_sampled_binary_empty_population():
    assert chm._sampled_binary(population(KINDS, []), KINDS) == [
        {"fingerprint": FINGERPRINT, "records": []},
    ]


@pytest.mark.parametrize("value", ["0E-8", "1E-100", "NaN", "-0.00000000", "9" * 100, "é\x00", 'a"b\\c'])
def test_sampled_binary_preserves_string_bytes_as_text(value):
    assert chm._sampled_binary(population(["String"], [[value]]), ["String"]) == [
        {"fingerprint": FINGERPRINT, "records": [[value]]},
    ]


@pytest.mark.parametrize("length", [0, 1, 63, 64, 65, 66, 67])
def test_sampled_binary_rejects_truncation(length):
    body = population(["String"], [["PRIVATE_BINARY_SENTINEL"]])
    with pytest.raises(ValueError) as error:
        chm._sampled_binary(body[:length], ["String"])
    assert "PRIVATE_BINARY_SENTINEL" not in str(error.value)


@pytest.mark.parametrize("tail", [b"\xff" * 10, b"\x01\x02", b"\x01\x00extra-private-data"])
def test_sampled_binary_rejects_malformed_lengths_flags_and_trailing_bytes(tail):
    with pytest.raises(ValueError) as error:
        chm._sampled_binary(string(FINGERPRINT) + tail, ["Bool"])
    assert "extra-private-data" not in str(error.value)


def test_sampled_binary_invalid_utf8_requests_json_compatibility_after_validating_entire_frame():
    body = population(["String", "Array(String)"], [[b"\xff", [b"\xfe"]]])
    assert chm._sampled_binary(body, ["String", "Array(String)"]) is None
    with pytest.raises(ValueError):
        chm._sampled_binary(body + b"private-trailer", ["String", "Array(String)"])


def test_sampled_binary_rejects_an_invalid_nullable_marker():
    with pytest.raises(ValueError):
        chm._sampled_binary(string(FINGERPRINT) + b"\x01\x02", ["Nullable(Bool)"])


def test_sampled_binary_validates_later_fields_and_rows_after_unsupported_utf8():
    kinds = ["String", "Nullable(Bool)"]
    body = population(kinds, [[b"\xff", True]])
    with pytest.raises(ValueError):
        chm._sampled_binary(body[:-2] + b"\x02", kinds)
    body = population(["String"], [[b"\xff"], ["unfinished-second-row"]])
    with pytest.raises(ValueError):
        chm._sampled_binary(body[:-1], ["String"])


@pytest.mark.parametrize("incompatible", [False, True])
async def test_sampled_state_client_negotiates_binary_and_keeps_exact_json_fallback(incompatible):
    queries = []
    records = [["replacement-from-json" if incompatible else "1.23000000"], ["second-json-value"]]
    binary_records = [[b"\xff"], ["binary-only-value"]] if incompatible else records
    encoded = population(["String"], binary_records)
    expected_fingerprint = "B" * 64 if incompatible else FINGERPRINT
    sql = "SELECT {sample:String} AS fixture"
    params = {"sample": ("exact-value-β", "String")}

    def reply(request):
        query = request.content.decode()
        queries.append(query)
        assert request.url.params["param_sample"] == params["sample"][0]
        if query.endswith(" FORMAT RowBinary"):
            return httpx.Response(200, content=encoded)
        assert query.endswith(" FORMAT JSONCompactEachRowWithNames")
        header = json.dumps(["fingerprint", "records"])
        content = header + "\n" + json.dumps([expected_fingerprint, records]) + "\n"
        return httpx.Response(200, content=content.encode())

    with localcontext() as context:
        context.clear_flags()
        before = getcontext().copy()
        async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as http:
            client = chm.ClickHouse("http://clickhouse.invalid", http=http)
            result = await client.sampled_state_rows(sql, params, ["String"])
        assert result == [{"fingerprint": expected_fingerprint, "records": records}]
        expected_queries = [sql + " FORMAT RowBinary"]
        if incompatible:
            expected_queries.append(sql + " FORMAT JSONCompactEachRowWithNames")
        assert queries == expected_queries
        assert context.flags == before.flags


async def test_sampled_state_client_preserves_a_replaced_rows_callback():
    calls = []
    client = chm.ClickHouse("http://clickhouse.invalid")

    async def rows(query, params=None):
        calls.append((query, params))
        return [{"fingerprint": FINGERPRINT, "records": []}]

    client.rows = rows
    try:
        result = await client.sampled_state_rows("SELECT fixture", {"p0": ("value", "String")}, ["String"])
    finally:
        await client.close()
    assert result == [{"fingerprint": FINGERPRINT, "records": []}]
    assert calls == [("SELECT fixture", {"p0": ("value", "String")})]


async def test_sampled_state_client_uses_json_for_an_unsupported_schema():
    queries = []

    def reply(request):
        queries.append(request.content.decode())
        content = json.dumps(["fingerprint", "records"]) + "\n" + json.dumps([FINGERPRINT, []]) + "\n"
        return httpx.Response(200, content=content.encode())

    async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as http:
        result = await chm.ClickHouse("http://clickhouse.invalid", http=http).sampled_state_rows(
            "SELECT fixture", {}, ["Decimal(38, 18)"]
        )
    assert result == [{"fingerprint": FINGERPRINT, "records": []}]
    assert queries == ["SELECT fixture FORMAT JSONCompactEachRowWithNames"]
