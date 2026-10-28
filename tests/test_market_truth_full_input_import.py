import gzip
import hashlib
import json
import re
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


def retained_batch(tmp_path, lines, *, table="mkt.klines_1m", schema=None, recursive=None):
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
    assert {key: value for key, value in requests[0].url.params.items() if key != "query_id"} == {
        key: value for key, value in requests[1].url.params.items() if key != "query_id"
    }
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
    if "☃" in changes.get("semantic_fingerprint", ""):
        line = line.replace("\\u2603", "☃")
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


@pytest.mark.parametrize("private", [False, True])
async def test_native_import_keeps_recursive_json_validation_and_text(tmp_path, native_import, private):
    ch, requests = native_import
    document = {"value": "0.12345678901234567890123456789000", "missing": None}
    if private:
        document["value"] = "https://private.invalid"
    row = {
        "storage_key": "actual:funding:key",
        "instrument_id": "binance:perp_linear:ENAUSDT",
        "kind": "CURRENT_ESTIMATE",
        "as_of": "2026-10-28 12:01:00.123456",
        "source_observations": json.dumps(document, separators=(",", ":")),
    }
    retained, _, _ = retained_batch(
        tmp_path,
        [json.dumps(row)],
        table="mkt.funding_obs",
        recursive=["value", "missing"],
    )
    if private:
        with pytest.raises(ValueError, match="Private public-fixture value"):
            await full_inputs.insert_clickhouse(ch, "mkt.funding_obs", list(retained))
        assert requests == []
    else:
        await full_inputs.insert_clickhouse(ch, "mkt.funding_obs", list(retained))
        payload = json.loads(requests[0].content.decode().split("\n", 1)[1])
        assert payload["source_observations"] == (
            '{"value":"0.12345678901234567890123456789000","missing":null}'
        )
        assert payload == {**row, "_version": 1}


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
        (captured_line(_version=9), "Selected package schema mismatch"),
        (captured_line(open_time="not-a-timestamp"), "Invalid isoformat string"),
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


async def test_native_import_preserves_product_serializer_for_mixed_batch(tmp_path, native_import):
    ch, requests = native_import
    lines = [captured_line(storage_key="actual:first"), captured_line(storage_key="actual:escaped\\key")]
    retained, _, _ = retained_batch(tmp_path, lines)
    batch = list(retained)
    await full_inputs.insert_clickhouse(ch, "mkt.klines_1m", batch)
    await ch.insert("mkt.klines_1m", [{**row, "_version": 1} for row, _ in batch])
    assert len(requests) == 2
    assert requests[0].content == requests[1].content


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


ORIGINAL_PRIVATE_CONTENT = re.compile(
    r"://|[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}|-----BEGIN|"
    r"(?:password|passwd|api_key|access_token|authorization|telegram_id|chat_id|user_id|email)\s*[\"=:]",
    re.IGNORECASE,
)


@pytest.fixture
def full_privacy_searches(monkeypatch):
    matcher = full_inputs.PRIVATE_CONTENT
    assert matcher.pattern == ORIGINAL_PRIVATE_CONTENT.pattern
    assert matcher.flags == ORIGINAL_PRIVATE_CONTENT.flags
    searches = []

    class CountingMatcher:
        def search(self, text):
            searches.append(text)
            return matcher.search(text)

    monkeypatch.setattr(full_inputs, "PRIVATE_CONTENT", CountingMatcher())
    return searches


@pytest.mark.parametrize(
    ("text", "private"),
    [
        ("", False),
        ("public:key:2026-10-28T12:01:00.123456Z", False),
        ("a" * 64, False),
        ("b" * 111, False),
        ("public\nidentifier", False),
        ("escaped\\value", False),
        ("snowman ☃ İstanbul ı ſ K", False),
        ("@", False),
        ("not-an-email@", False),
        ("@example.invalid", False),
        ("person@example", False),
        ("person@example.c", False),
        ("person＠example.invalid", False),
        ("person example.invalid", False),
        ("person@example.invalid", True),
        ("PERSON+TAG@EXAMPLE.INVALID", True),
        ("prefix person@example.invalid suffix", True),
        ("ı@ſ.KK", True),
        ("person@example.İı", True),
        ("a@b..cc", True),
        ("://", True),
        ("HTTPS://PUBLIC.INVALID", True),
        ("https://public.invalid/@", True),
        ("-----BEGIN PUBLIC KEY-----", True),
        ("-----begin marker@", True),
        ("password", False),
        ("password = public", True),
        ("PASSWD\t:public", True),
        ("api_key=public", True),
        ("ACCESS_TOKEN : public", True),
        ('authorization"public', True),
        ("telegram_id:public", True),
        ("chat_id=public", True),
        ("USER_ID\n:public", True),
        ("email : public", True),
        ("API_KEY=public", True),
        ("acceſſ_token=public", True),
        ("password=public@", True),
        ("-----BEGIN https://person@example.invalid", True),
    ],
)
def test_public_privacy_preserves_decisions_and_omits_impossible_email_search(
    text, private, full_privacy_searches
):
    assert bool(ORIGINAL_PRIVATE_CONTENT.search(text)) is private
    for value, message in (
        (text, "Private public-fixture value"),
        ({text: None}, "Private public-fixture key"),
    ):
        full_privacy_searches.clear()
        if private:
            with pytest.raises(ValueError, match=f"^{message}$"):
                full_inputs.public_value(value)
        else:
            assert full_inputs.public_value(value) is None
        assert full_privacy_searches == ([text] if "@" in text else [])


def test_public_privacy_checks_nested_keys_and_values_without_full_search(full_privacy_searches):
    value = {"outer": [{"inner": "a" * 64}, {"@": "not-an-email@"}], "tag": "b" * 111}
    assert full_inputs.public_value(value, {"outer", "inner", "@", "tag"}) is None
    assert full_privacy_searches == ["@", "not-an-email@"]


@pytest.mark.parametrize(
    "text",
    ["person@example.invalid", "https://private.invalid", "-----BEGIN", "api_key=public"],
)
@pytest.mark.parametrize("location", ["key", "value"])
def test_public_privacy_refuses_nested_content(text, location, full_privacy_searches):
    child = {text: None} if location == "key" else {"inner": [text]}
    with pytest.raises(ValueError, match=f"^Private public-fixture {location}$"):
        full_inputs.public_value({"outer": [child]})
    assert full_privacy_searches == ([text] if "@" in text else [])


@pytest.mark.parametrize(
    ("document", "message", "searches"),
    [
        (r'{"value":["public","@","not-an-email@"]}', None, ["@", "not-an-email@"]),
        (r'{"value":["public",null]}', None, []),
        (
            r'{"value":["person\u0040example.invalid"]}',
            "Private public-fixture value",
            ["person@example.invalid"],
        ),
        (
            r'{"person\u0040example.invalid":"public"}',
            "Private public-fixture key",
            ["person@example.invalid"],
        ),
        (r'{"value":["https://private.invalid"]}', "Private public-fixture value", []),
        (r'{"https://private.invalid":"public"}', "Private public-fixture key", []),
        (r'{"value":["-----BEGIN"]}', "Private public-fixture value", []),
        (r'{"value":["authorization=public"]}', "Private public-fixture value", []),
        (r'{"value":{"unexpected":"public"}}', "Unexpected recursive public field", []),
    ],
)
async def test_native_import_keeps_decoded_privacy_boundaries_without_impossible_email_search(
    tmp_path, native_import, full_privacy_searches, document, message, searches
):
    ch, requests = native_import
    line = captured_line(source_observations=document)
    schema = {**MARKET_COLUMNS["mkt.klines_1m"], "source_observations": "jsonb"}
    retained, _, _ = retained_batch(
        tmp_path,
        [line],
        schema=schema,
        recursive=["value", "person@example.invalid", "https://private.invalid"],
    )
    if message:
        with pytest.raises(ValueError, match=f"^{message}$"):
            await full_inputs.insert_clickhouse(ch, "mkt.klines_1m", list(retained))
        assert requests == []
    else:
        await full_inputs.insert_clickhouse(ch, "mkt.klines_1m", list(retained))
        assert len(requests) == 1
        query, payload = requests[0].content.decode().split("\n", 1)
        assert query == f"INSERT INTO {ch.database}.klines_1m FORMAT JSONEachRow"
        assert json.loads(payload) == {**json.loads(line), "_version": 1}
        assert json.loads(payload)["source_observations"] == document
    assert full_privacy_searches == searches


@pytest.mark.parametrize(
    ("fingerprint", "independent"),
    [
        ("public", True),
        ("mkt_value", True),
        ("literal mkt", False),
        ("escaped \\u006dkt", True),
        ("snowman ☃ mkt", False),
        ("émkt", True),
    ],
)
async def test_copy_admission_matches_actual_insert_wire(tmp_path, native_import, fingerprint, independent):
    ch, requests = native_import
    line = captured_line(semantic_fingerprint=fingerprint)
    retained, _, _ = retained_batch(tmp_path, [line])
    batch = list(retained)
    assert full_inputs.clickhouse_wire_independent(batch) is independent
    await full_inputs.insert_clickhouse(ch, "mkt.klines_1m", batch)
    payload = requests[0].content.decode().split("\n", 1)[1]
    assert (ch.database not in payload) is independent


async def test_copy_admission_checks_decoded_escaped_database_token(tmp_path, native_import):
    ch, requests = native_import
    line = captured_line(semantic_fingerprint="escaped mkt").replace("escaped mkt", "escaped \\u006dkt")
    retained, _, _ = retained_batch(tmp_path, [line])
    batch = list(retained)
    assert full_inputs.clickhouse_wire_independent(batch) is False
    await full_inputs.insert_clickhouse(ch, "mkt.klines_1m", batch)
    assert json.loads(requests[0].content.decode().split("\n", 1)[1])["semantic_fingerprint"] == (
        "escaped " + ch.database
    )
