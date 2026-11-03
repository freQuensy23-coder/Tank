import email.parser
import email.policy
import hashlib
import os

import httpx
import pytest
from truth_sql_failure_evidence import EvidenceClickHouse

from tank.clickhouse_market import ClickHouse


SETTINGS = {
    "date_time_output_format": "iso",
    "output_format_json_quote_64bit_integers": "0",
    "output_format_json_quote_decimals": "0",
    "do_not_merge_across_partitions_select_final": "1",
    "use_skip_indexes_if_final": "0",
    "max_query_size": str(256 << 20),
}


def fields(request):
    message = email.parser.BytesParser(policy=email.policy.default).parsebytes(
        b"Content-Type: " + request.headers["content-type"].encode() + b"\r\n\r\n" + request.content
    )
    parts = list(message.iter_parts())
    assert all(part.get_filename() is None for part in parts)
    names = [part.get_param("name", header="content-disposition") for part in parts]
    assert len(names) == len(set(names))
    return {name: part.get_payload(decode=True) for name, part in zip(names, parts, strict=True)}


@pytest.mark.parametrize("large", [False, True])
@pytest.mark.parametrize("database", ["mkt", "tank_test_transport"])
async def test_parameter_bytes_auth_database_and_format(large, database):
    text = "α😀'\\\n\t\r\x00&=+%" * (7000 if large else 1)
    parameters = {
        "text": (text, "String"),
        "empty": ("", "String"),
        "none": (None, "String"),
        "true": (True, "Bool"),
        "false": (False, "Bool"),
        "number": (123, "Int64"),
        "null": ("\\N", "Nullable(Int64)"),
    }
    expected = {"param_" + key: value for key, value in {
        "text": text, "empty": "", "none": "", "true": "true", "false": "false", "number": "123", "null": "\\N"
    }.items()}
    query = "SELECT {text:String} AS text FROM mkt.flow_bars_1m"
    rewritten = query.replace("mkt.", database + ".") + " FORMAT JSONEachRow"

    def respond(request):
        assert request.method == "POST"
        assert request.url.host == "localhost"
        assert request.url.path == "/"
        assert request.headers["X-ClickHouse-User"] == "transport-user"
        assert request.headers["X-ClickHouse-Key"] == "transport-key"
        assert "transport-user" not in str(request.url)
        assert "transport-key" not in str(request.url)
        if large:
            assert dict(request.url.params) == SETTINGS
            assert fields(request) == {"query": rewritten.encode(), **{key: value.encode() for key, value in expected.items()}}
        else:
            assert dict(request.url.params) == SETTINGS | expected
            assert request.content == rewritten.encode()
            assert "multipart" not in request.headers.get("content-type", "")
        return httpx.Response(200, content=b'{"value":"independent-result"}\n')

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = ClickHouse("http://transport-user@localhost:8123", password="transport-key", http=http, database=database)
        assert client.http is http
        assert await client.rows(query, parameters) == [{"value": "independent-result"}]


@pytest.mark.parametrize("large", [False, True])
async def test_parameter_transport_preserves_errors(large):
    def respond(request):
        assert request.content
        return httpx.Response(400, text="Code: 43. intended native error")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = ClickHouse("http://localhost:8123", http=http)
        with pytest.raises(RuntimeError, match=r"ClickHouse 400: Code: 43\. intended native error"):
            await client.rows("SELECT {value:String}", {"value": ("x" * (70000 if large else 1), "String")})


async def test_execute_and_insert_retain_original_body_protocol():
    expected = [b"SELECT 1", b'INSERT INTO mkt.flow_bars_1m FORMAT JSONEachRow\n{"instrument_id": "T"}']

    def respond(request):
        assert dict(request.url.params) == SETTINGS
        assert request.content == expected.pop(0)
        assert "multipart" not in request.headers.get("content-type", "")
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = ClickHouse("http://localhost:8123", http=http)
        await client.execute("SELECT 1")
        await client.insert("mkt.flow_bars_1m", [{"instrument_id": "T"}])
    assert not expected


@pytest.mark.integration
async def test_real_native_large_multipart_values(request):
    url = os.environ.get("CLICKHOUSE_TEST_URL")
    assert url, "CLICKHOUSE_TEST_URL must identify the owned native CI service"
    ascii_text = "A0&=+%?'\\\n\t\r" * 7000
    unicode_text = "שלום α😀漢字\\\n'\t\r" * 5000
    values = {"ascii": ascii_text, "unicode": unicode_text, "empty": "", "null": "\\N"}
    parameters = {key: (value, "Nullable(String)" if key == "null" else "String") for key, value in values.items()}
    query = "SELECT " + ", ".join(
        expression
        for key in ("ascii", "unicode", "empty")
        for expression in (
            f"{{{key}:String}} AS {key}_value",
            f"length({{{key}:String}}) AS {key}_bytes",
            f"lengthUTF8({{{key}:String}}) AS {key}_characters",
            f"lower(hex(SHA256({{{key}:String}}))) AS {key}_hash",
            f"substringUTF8({{{key}:String}}, 2, 9) AS {key}_substring",
        )
    ) + ", {null:Nullable(String)} AS nullable_value"
    client = EvidenceClickHouse(url, request.node.nodeid)
    try:
        rows = await client.rows(query, parameters)
    finally:
        await client.close()
    assert len(rows) == 1
    for key in ("ascii", "unicode", "empty"):
        value = values[key]
        assert rows[0][key + "_value"] == value
        assert rows[0][key + "_bytes"] == len(value.encode())
        assert rows[0][key + "_characters"] == len(value)
        assert rows[0][key + "_hash"] == hashlib.sha256(value.encode()).hexdigest()
        assert rows[0][key + "_substring"] == value[1:10]
    assert rows[0]["nullable_value"] is None
