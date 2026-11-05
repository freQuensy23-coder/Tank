import asyncio
import email.parser
import email.policy
import hashlib
import itertools
import json
import os
import re
from pathlib import Path

import httpx
import pytest
import truth_sql_failure_evidence
from truth_sql_failure_evidence import EvidenceClickHouse

from tank.clickhouse_market import ClickHouse, parameter_string

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
    names = [part.get_param("name", header="content-disposition") for part in parts]
    assert len([name for name in names if name != "query"]) == len(set(names) - {"query"})
    assert all(len(part.get_payload(decode=True)) <= 32768 for part in parts if part.get_filename() is None)
    result = {}
    for name, part in zip(names, parts, strict=True):
        if part.get_filename() is not None:
            assert name == "_tank_http_parameters"
            assert part.get_filename() == "parameters.tsv"
        result[name] = result.get(name, b"") + part.get_payload(decode=True)
    return result


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
    expected = {
        "param_" + key: value
        for key, value in {
            "text": text,
            "empty": "",
            "none": "",
            "true": "true",
            "false": "false",
            "number": "123",
            "null": "\\N",
        }.items()
    }
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
            assert fields(request) == {
                "query": rewritten.replace(
                    "{text:String}", "(SELECT tuple(c0) FROM _tank_http_parameters).1"
                ).encode(),
                **{key: value.encode() for key, value in expected.items() if key != "param_text"},
                "_tank_http_parameters_structure": b"c0 String",
                "_tank_http_parameters_format": b"TabSeparated",
                "_tank_http_parameters": (text + "\n").encode(),
            }
        else:
            assert dict(request.url.params) == SETTINGS | expected
            assert request.content == rewritten.encode()
            assert "multipart" not in request.headers.get("content-type", "")
        return httpx.Response(200, content=b'{"value":"independent-result"}\n')

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = ClickHouse(
            "http://transport-user@localhost:8123", password="transport-key", http=http, database=database
        )
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


@pytest.mark.parametrize("aggregate_only", [False, True])
async def test_large_query_chunks_preserve_literals_comments_and_parameter_bytes(aggregate_only):
    literal = "Ω😀漢字" * 20000
    query = (
        f"SELECT '{literal}' AS original, {{value:String}} AS value, "
        "'{value:String}' AS quoted, $q${value:String}$q$ AS heredoc, 1 AS `quoted{value:String}` "
        "/* {value:String} */ -- {value:String}\n# {value:String}\n"
    )
    wire = "already\\\\escaped\\n'\"" * (1 if aggregate_only else 5000)
    parameters = {"value": (wire, "String")}
    if aggregate_only:
        parameters.update({f"extra{i}": ("x" * 1000, "String") for i in range(5)})

    def respond(request):
        received = fields(request)
        assert dict(request.url.params) == SETTINGS
        if aggregate_only:
            assert received == {
                "query": (query + " FORMAT JSONEachRow").encode(),
                **{"param_" + name: value.encode() for name, (value, _) in parameters.items()},
            }
        else:
            assert received == {
                "query": (
                    query.replace(
                        "{value:String} AS value",
                        "(SELECT tuple(c0) FROM _tank_http_parameters).1 AS value",
                    )
                    + " FORMAT JSONEachRow"
                ).encode(),
                "_tank_http_parameters_structure": b"c0 String",
                "_tank_http_parameters_format": b"TabSeparated",
                "_tank_http_parameters": (wire + "\n").encode(),
            }
        return httpx.Response(200, content=b'{"value":1}\n')

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        await ClickHouse("http://localhost:8123", http=http).rows(query, parameters)


async def test_transport_preserves_http_exception_identity():
    error = httpx.ReadTimeout("original transport failure")

    def respond(request):
        raise error

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        with pytest.raises(httpx.ReadTimeout) as raised:
            await ClickHouse("http://localhost:8123", http=http).rows(
                "SELECT {value:String}", {"value": ("x" * 70000, "String")}
            )
    assert raised.value is error


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


async def test_complete_error_evidence_is_sanitized_before_client_truncation(tmp_path, monkeypatch):
    secret = "private-payload-value"
    text = "Code: 46. DB::Exception: " + secret * 100 + " Function with name sup_missing does not exist"
    query = "SELECT sup_missing({payload:String})"
    receipts = []
    evidence_directory = tmp_path / "evidence"
    original_retain = truth_sql_failure_evidence.retain_first_failure

    def retain(directory, receipt):
        receipts.append(receipt)
        original_retain(evidence_directory, receipt)

    monkeypatch.setattr(truth_sql_failure_evidence, "retain_first_failure", retain)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(400, text=text))
    ) as http:
        client = EvidenceClickHouse("http://localhost:8123", "tests/test_transport.py::test_error", http=http)
        with pytest.raises(RuntimeError) as raised:
            await client.rows(query, {"payload": (secret, "String")})
    assert str(raised.value) == "ClickHouse 400: " + text[:500]
    persisted = list(evidence_directory.iterdir())
    assert len(persisted) == 1
    receipt = json.loads(persisted[0].read_text())
    assert receipt == receipts[0]
    assert receipt["native_error_code"] == 46
    assert "sup_missing" in receipt["server_message"]
    assert "does not exist" in receipt["server_message"]
    assert secret not in persisted[0].read_text()
    assert query not in persisted[0].read_text()


def test_transport_qualification_receipt_refuses_unbounded_fields(tmp_path):
    directory = tmp_path / "evidence"
    checks = {
        key: {"bytes": 0, "characters": 0, "sha256": hashlib.sha256(b"").hexdigest()}
        for key in ("ascii", "unicode", "empty")
    }
    truth_sql_failure_evidence.retain_transport_qualification(directory, "test_case", checks)
    files = list(directory.iterdir())
    assert len(files) == 1
    before = files[0].read_bytes()
    assert json.loads(before) == {"nodeid": "test_case", "status": "PASS", "checks": checks}
    checks["ascii"]["raw_value"] = "private-value"
    with pytest.raises(ValueError, match="fields"):
        truth_sql_failure_evidence.retain_transport_qualification(directory, "test_case", checks)
    assert list(directory.iterdir()) == files
    assert files[0].read_bytes() == before
    assert b"private-value" not in before


def test_native_diagnostic_vocabulary_drops_unknown_error_words():
    receipt = truth_sql_failure_evidence.failure_receipt(
        RuntimeError(
            "ClickHouse 400: Code: 36. Maximum parse depth exceeded privatecredential arbitrarypayload in scope SELECT hidden_snapshot"
        ),
        "test_case",
        "SELECT 1",
    )
    assert receipt["native_error_code"] == 36
    assert all(word in receipt["server_message"] for word in ("maximum", "parse", "depth", "exceeded"))
    serialized = json.dumps(receipt)
    assert all(
        word not in serialized for word in ("privatecredential", "arbitrarypayload", "hidden_snapshot")
    )


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("Illegal type Nullable(Float64) of argument of function _cast", {"_CAST", "Float64", "Nullable"}),
        ("Illegal type Float64 of argument of function PLUS", {"plus", "Float64"}),
        (
            "Nested type Array(Float64) cannot be inside Nullable type of function arrayelement",
            {"arrayElement", "Array", "Float64", "Nullable", "cannot be inside Nullable"},
        ),
        (
            "Cannot convert NULL value to non-Nullable type in function tupleelement",
            {"tupleElement", "Nullable", "Cannot convert NULL value to non-Nullable type"},
        ),
    ],
)
def test_static_native_diagnostics_preserve_desugared_functions(message, expected, tmp_path):
    receipt = truth_sql_failure_evidence.failure_receipt(
        RuntimeError("ClickHouse 500: Code: 43. " + message), "test_static_diagnostic", "SELECT 1"
    )
    truth_sql_failure_evidence.retain_first_failure(tmp_path / "evidence", receipt)
    files = list((tmp_path / "evidence").iterdir())
    assert len(files) == 1
    actual = json.loads(files[0].read_text())
    assert actual["native_error_code"] == 43
    assert actual["http_status"] == 500
    assert expected <= set(actual["server_message"].split(" | "))
    assert len(actual["server_message"]) <= 512


def test_native_diagnostics_do_not_copy_unknown_functions_or_private_values(tmp_path):
    query = "SELECT unknownPrivateFunction(private_column, 'credentialPrefix,credentialSuffix')"
    receipt = truth_sql_failure_evidence.failure_receipt(
        RuntimeError(
            "ClickHouse 500: Code: 43. Illegal type Float64 of argument of function unknownPrivateFunction "
            "private_column credentialPrefix,credentialSuffix https://private-host/private-path "
            "_CASTprivateSuffix privatePrefix_CAST While processing plus Nullable(String) private_scope"
        ),
        "test_private_diagnostic",
        query,
    )
    truth_sql_failure_evidence.retain_first_failure(tmp_path / "evidence", receipt)
    files = list((tmp_path / "evidence").iterdir())
    assert len(files) == 1
    serialized = files[0].read_text()
    assert json.loads(serialized)["server_message"] == "Illegal type | type | argument | function | Float64"
    for forbidden in (
        "unknownPrivateFunction",
        "private_column",
        "credentialPrefix",
        "credentialSuffix",
        "private-host",
        "private-path",
        "_CASTprivateSuffix",
        "privatePrefix_CAST",
        "private_scope",
        "plus",
        "Nullable",
        query,
    ):
        assert forbidden not in serialized


def test_array_aggregation_diagnostic_keeps_only_static_native_scope_functions(tmp_path):
    receipt = truth_sql_failure_evidence.failure_receipt(
        RuntimeError(
            "ClickHouse 500: Code: 43. array aggregation function cannot be performed on type "
            "Nullable(Float64): In scope values -> arraySum(unknownPrivateFunction(private_column, "
            "'credentialPrefix,credentialSuffix')) AS private_alias"
        ),
        "test_array_diagnostic",
        "SELECT 1",
    )
    truth_sql_failure_evidence.retain_first_failure(tmp_path / "evidence", receipt)
    files = list((tmp_path / "evidence").iterdir())
    assert len(files) == 1
    serialized = files[0].read_text()
    actual = json.loads(serialized)
    assert actual["native_error_code"] == 43
    assert {
        "arraySum",
        "array aggregation function cannot be performed on type",
        "Float64",
        "Nullable",
    } <= set(actual["server_message"].split(" | "))
    for forbidden in (
        "unknownPrivateFunction",
        "private_column",
        "credentialPrefix",
        "credentialSuffix",
        "private_alias",
        "values",
    ):
        assert forbidden not in serialized


@pytest.mark.parametrize(
    "phase",
    [
        "tier_values",
        "book_ordinals",
        "venue_books",
        "venue_depth",
        "venue_precision",
        "venue_walk_t1",
        "venue_walk_t2",
        "venue_walk_t3",
    ],
)
def test_liquidity_phase_receipts_are_bounded_and_reject_unknown_labels(tmp_path, phase):
    directory = tmp_path / "evidence"
    for status in ("STARTED", "PASS"):
        truth_sql_failure_evidence.retain_liquidity_phase(directory, "test_phases", phase, status=status)
    receipts = sorted(
        (json.loads(path.read_text()) for path in directory.iterdir()), key=lambda r: r["status"]
    )
    assert receipts == [
        {"nodeid": "test_phases::" + phase, "phase": phase, "status": "PASS"},
        {"nodeid": "test_phases::" + phase, "phase": phase, "status": "STARTED"},
    ]
    before = {path.name: path.read_bytes() for path in directory.iterdir()}
    with pytest.raises(ValueError, match="phase"):
        truth_sql_failure_evidence.retain_liquidity_phase(directory, "test_phases", "private_payload")
    with pytest.raises(ValueError, match="status"):
        truth_sql_failure_evidence.retain_liquidity_phase(
            directory, "test_phases", "state", status="private_secret"
        )
    assert {path.name: path.read_bytes() for path in directory.iterdir()} == before
    assert all(
        b"private_payload" not in content and b"private_secret" not in content for content in before.values()
    )


@pytest.fixture
def native_observer_receipts(tmp_path, monkeypatch):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("CLICKHOUSE_TEST_URL", "http://localhost:8123")
    directory = tmp_path / "native-evidence"
    directory.mkdir()
    retain = truth_sql_failure_evidence._retain_first_receipt
    monkeypatch.setattr(
        truth_sql_failure_evidence,
        "_retain_first_receipt",
        lambda original_directory, receipt, prefix: retain(directory, receipt, prefix),
    )
    ticks = itertools.count(0, 10)
    monkeypatch.setattr(truth_sql_failure_evidence.time, "monotonic_ns", lambda: next(ticks))
    return directory


def synthetic_phase_request():
    return (
        "tests/test_truth_sql_liquidity.py::test_native_liquidity_absence_relational_phase_contracts::venue_stats",
        "WITH \n{lq_snapshot:String} AS lq_snapshot, synthetic_fixture AS unused "
        "SELECT arraySort(groupArray(tuple(venue, pre_spread))) AS value FROM lq_venue_stats",
    )


def native_receipts(directory):
    return [json.loads(path.read_text()) for path in directory.glob("truth-sql-native-*.json")]


@pytest.mark.parametrize("index", [1, 2, 3, "book_ordinals", "venue_books"])
def test_ci_native_observer_walk_phase_selection_scope(index):
    phase = f"venue_walk_t{index}"
    nodeid, _ = synthetic_phase_request()
    if index == "book_ordinals":
        phase = "book_ordinals"
        selection = "SELECT length(ordered_books) AS value FROM lq_book_ordinals"
    elif index == "venue_books":
        phase = "venue_books"
        selection = "SELECT arraySort(groupArray(tuple(venue, length(series)))) AS value FROM lq_venue_books"
    else:
        selection = (
            f"SELECT arraySort(groupArray(tuple(venue, lq_walk_book(current.3, lq_tiers[{index}], 'ask')))) "
            "AS value FROM lq_venue_stats"
        )
    nodeid = nodeid.rsplit("::", 1)[0] + "::" + phase
    query = "WITH \n{lq_snapshot:String} AS lq_snapshot, synthetic_fixture AS unused " + selection
    observe = truth_sql_failure_evidence._liquidity_observation
    assert observe(nodeid, query) == phase
    assert observe(nodeid, query + " CROSS JOIN lq_tier_values") is None
    assert observe(nodeid.rsplit("::", 1)[0] + "::venue_stats", query) is None
    assert observe(nodeid, query + " trailing") is None


async def test_ci_native_observer_preserves_wire_and_records_inclusive_boundaries(native_observer_receipts):
    nodeid, query = synthetic_phase_request()
    calls = []

    def respond(request):
        calls.append(request)
        assert request.content == (query + " FORMAT JSONEachRow").encode()
        assert dict(request.url.params) == SETTINGS | {"param_value": "wire\\n"}
        assert len(request.headers["X-ClickHouse-Query-Id"]) == 64
        return httpx.Response(
            200,
            content=b'{"value":1}\n',
            headers={
                "X-ClickHouse-Summary": json.dumps(
                    {
                        "elapsed_ns": "100",
                        "read_rows": "0",
                        "result_rows": "1",
                        "privateHeaderKey": "credentialPrefix,credentialSuffix",
                    }
                ),
                "Location": "https://private-host/private-path?password=credentialPrefix;credentialSuffix",
                "X-Private": "privateHeaderValue",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = EvidenceClickHouse("http://localhost:8123", nodeid, http=http)
        for _ in range(2):
            assert await client.rows(query, {"value": ("wire\\n", "String")}) == [{"value": 1}]
        assert client.http.post == client._original_http_post
    receipts = native_receipts(native_observer_receipts)
    assert len(calls) == len(receipts) == 2
    assert len({r["query_id"] for r in receipts}) == 2
    for receipt in receipts:
        assert receipt["outcome"] == "decoded"
        assert receipt["own_query"] == {"status": "not_requested"}
        assert receipt["clocks"] == {
            "http_start_offset_ns": 10,
            "headers_offset_ns": 20,
            "http_end_offset_ns": 30,
            "decode_start_offset_ns": 40,
            "rows_end_offset_ns": 50,
        }
        assert receipt["summary"]["elapsed_ns"] == 100
        assert type(receipt["summary"]["elapsed_ns"]) is int
        assert receipt["summary"]["read_rows_present"] == 1
        assert receipt["summary"]["read_rows"] == 0
        assert receipt["summary"]["written_rows_present"] == 0
        assert "written_rows" not in receipt["summary"]
    persisted = "".join(path.read_text() for path in native_observer_receipts.iterdir())
    for forbidden in (
        "privateHeaderKey",
        "privateHeaderValue",
        "private-host",
        "private-path",
        "credentialPrefix",
        "credentialSuffix",
        query,
        "wire\\n",
        "Location",
    ):
        assert forbidden not in persisted


@pytest.mark.parametrize("diagnostic", ["live", "absent", "schema", "timeout", "cancelled", "invalid"])
async def test_ci_native_timeout_snapshot_is_single_bounded_and_preserves_original(
    native_observer_receipts, diagnostic
):
    nodeid, query = synthetic_phase_request()
    original = httpx.ReadTimeout("credentialPrefix;credentialSuffix https://private-host/private-path")
    calls = []
    query_ids = []

    def respond(request):
        calls.append(request)
        if len(calls) == 1:
            query_ids.append(request.headers["X-ClickHouse-Query-Id"])
            raise original
        if len(calls) == 3:
            assert request.url.params["param_observer_query_id"] == query_ids[0]
            assert "FROM system.trace_log WHERE query_id = {observer_query_id:String}" in (
                request.content.decode()
            )
            assert "ordered_classes" not in request.content.decode()
            return httpx.Response(200, content=b"")
        assert len(calls) == 2
        assert "X-ClickHouse-Query-Id" not in request.headers
        assert request.url.params["param_observer_query_id"] == query_ids[0]
        assert request.headers["X-ClickHouse-User"] == "default"
        assert all(value <= 4 for value in request.extensions["timeout"].values())
        text = request.content.decode()
        assert "FROM system.processes WHERE query_id = {observer_query_id:String} LIMIT 1" in text
        assert "mapContains(ProfileEvents, 'FunctionExecute')" in text
        assert "SELECT *" not in text
        assert "query," not in text and "user," not in text
        if diagnostic == "schema":
            return httpx.Response(400, text="private schema exception credentialPrefix,credentialSuffix")
        if diagnostic == "timeout":
            raise httpx.ReadTimeout("private diagnostic exception")
        if diagnostic == "cancelled":
            raise asyncio.CancelledError("private cancellation")
        if diagnostic == "absent":
            return httpx.Response(200, content=b"")
        return httpx.Response(
            200,
            json={
                "elapsed": 120.125,
                "read_rows": 3,
                "read_bytes": 30,
                "memory_usage": 2048,
                "peak_memory_usage": 4096,
                "is_cancelled": 0,
                "is_all_data_sent": 0,
                "FunctionExecute_present": 1,
                "FunctionExecute": True if diagnostic == "invalid" else 17,
                "ScalarSubqueriesGlobalCacheHit_present": 0,
                "ScalarSubqueriesGlobalCacheHit": 0,
                "ScalarSubqueriesLocalCacheHit_present": 1,
                "ScalarSubqueriesLocalCacheHit": 0,
                "ScalarSubqueriesCacheMiss_present": 1,
                "ScalarSubqueriesCacheMiss": 2,
                "private_unknown_profile_key": "credentialPrefix;credentialSuffix",
                "url": "https://private-host/private-path",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = EvidenceClickHouse("http://localhost:8123", nodeid, http=http)
        with pytest.raises(httpx.ReadTimeout) as raised:
            await client.rows(query)
        assert raised.value is original
        assert client._observation is None and client._evidence_query is None
        assert client.http.post == client._original_http_post
    assert len(calls) == 3
    (receipt,) = native_receipts(native_observer_receipts)
    assert receipt["own_trace_log"] == {"status": "NoCounter"}
    assert receipt["outcome"] == "read_timeout"
    assert receipt["summary"] == {"status": "no_response"}
    assert "decode_start_offset_ns" not in receipt["clocks"]
    snapshot = receipt["own_query"]
    if diagnostic == "live":
        assert snapshot["status"] == "observed"
        assert snapshot["elapsed"] == 120.125 and type(snapshot["elapsed"]) is float
        assert snapshot["FunctionExecute"] == 17 and type(snapshot["FunctionExecute"]) is int
        assert snapshot["ScalarSubqueriesGlobalCacheHit_present"] == 0
        assert "ScalarSubqueriesGlobalCacheHit" not in snapshot
        assert snapshot["ScalarSubqueriesLocalCacheHit_present"] == 1
        assert snapshot["ScalarSubqueriesLocalCacheHit"] == 0
    else:
        assert (
            snapshot["status"]
            == {
                "absent": "absent",
                "schema": "http_error",
                "timeout": "error",
                "cancelled": "error",
                "invalid": "error",
            }[diagnostic]
        )
    persisted = "".join(path.read_text() for path in native_observer_receipts.iterdir())
    for forbidden in (
        "credentialPrefix",
        "credentialSuffix",
        "private_unknown_profile_key",
        "private-host",
        "private-path",
        "private diagnostic",
        "private cancellation",
        query,
    ):
        assert forbidden not in persisted


@pytest.mark.parametrize(
    "reason", ["not_ci", "unowned", "unrelated_node", "unknown_phase", "unrelated_sql", "baseline_sql"]
)
async def test_native_observer_disabled_outside_exact_owned_cases(
    native_observer_receipts, monkeypatch, reason
):
    nodeid, query = synthetic_phase_request()
    if reason == "not_ci":
        monkeypatch.delenv("GITHUB_ACTIONS")
    elif reason == "unowned":
        monkeypatch.setenv("CLICKHOUSE_TEST_URL", "http://127.0.0.1:8123")
    elif reason == "unrelated_node":
        nodeid = "tests/test_clickhouse_parameter_transport.py::test_real_native_large_multipart_values"
    elif reason == "unknown_phase":
        nodeid += "private_suffix"
    elif reason == "baseline_sql":
        nodeid = truth_sql_failure_evidence._LIQUIDITY_BASELINE
    else:
        query = "SELECT 1"
    calls = []

    def respond(request):
        calls.append(request)
        assert "X-ClickHouse-Query-Id" not in request.headers
        return httpx.Response(200, content=b"{}\n")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        await EvidenceClickHouse("http://localhost:8123", nodeid, http=http).rows(query)
    assert len(calls) == 1
    assert not list(native_observer_receipts.iterdir())


@pytest.mark.parametrize(
    ("digest", "admitted"),
    [
        ("1111111111111111111111111111111111111111111111111111111111111111", True),
        ("9759dc8ae3dc941c4dae074b86124194de1b46f685aae172506e8edaff144e7c", False),
        ("00c6fb8895af64256de6ed5c0a8f41344503096029c9cb5517f7cbff56bdf485", False),
        ("df5b18d0b5110c3e1a3009b53f1d5319de95212e50b4e171d371f3b1195ce35a", False),
        ("70dc6c3374e1fb8dac7c00d478d7bef5afe187bb35229bc5f8df4fdc08221bb5", False),
        ("dfe0cbea3b2e3dc1bfd3001912cbe31120b47628d427a89e1eb44766959c8a4d", False),
        ("d04c31b10582cede47e47414f4fb93866ed7a541087ba872a552e11891d1eb1e", False),
        ("23fea68086a60d64f3e9e051af50f13046e4fd0eb005f71e317a1d0461ff32a7", False),
    ],
)
async def test_ci_native_baseline_digest_seam_preserves_timeout_and_private_invalid_metrics(
    native_observer_receipts, monkeypatch, digest, admitted
):
    query = "SELECT 'synthetic-baseline-digest-seam'"
    original_hash = hashlib.sha256

    class QueryDigest:
        def hexdigest(self):
            return digest

    monkeypatch.setattr(
        truth_sql_failure_evidence.hashlib,
        "sha256",
        lambda value: QueryDigest() if value == query.encode() else original_hash(value),
    )
    original = httpx.ReadTimeout("privateBaselineCredential;privateBaselineBody")
    calls = []
    query_ids = []

    def respond(request):
        calls.append(request)
        if len(calls) == 1:
            assert ("X-ClickHouse-Query-Id" in request.headers) is admitted
            if admitted:
                query_ids.append(request.headers["X-ClickHouse-Query-Id"])
            raise original
        if len(calls) == 3:
            assert admitted
            assert request.url.params["param_observer_query_id"] == query_ids[0]
            assert "FROM system.trace_log WHERE query_id = {observer_query_id:String}" in (
                request.content.decode()
            )
            return httpx.Response(200, content=b"")
        assert admitted and len(calls) == 2
        assert request.url.params["param_observer_query_id"] == query_ids[0]
        assert "X-ClickHouse-Query-Id" not in request.headers
        assert "FROM system.processes WHERE query_id = {observer_query_id:String} LIMIT 1" in (
            request.content.decode()
        )
        assert "system.stack_trace" not in request.content.decode()
        return httpx.Response(
            200,
            json={"read_rows": True, "unknownPrivateField": "privateBaselineCredential"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = EvidenceClickHouse(
            "http://localhost:8123",
            truth_sql_failure_evidence._LIQUIDITY_BASELINE,
            http=http,
            baseline_query=query if admitted else query + " current-root",
        )
        with pytest.raises(httpx.ReadTimeout) as raised:
            await client.rows(query)
        assert raised.value is original
        assert client._observation is None and client._evidence_query is None
        assert client.http.post == client._original_http_post
    receipts = native_receipts(native_observer_receipts)
    if admitted:
        assert len(calls) == 3
        (receipt,) = receipts
        assert receipt["own_trace_log"] == {"status": "NoCounter"}
        assert receipt["phase"] == "baseline"
        assert receipt["query_digest"] == digest
        assert receipt["query_id"] == query_ids[0]
        assert receipt["outcome"] == "read_timeout"
        assert receipt["own_query"] == {"status": "error", "exception_class": "ValueError"}
        assert receipt["summary"] == {"status": "no_response"}
    else:
        assert len(calls) == 1
        assert receipts == []
    persisted = "".join(path.read_text() for path in native_observer_receipts.iterdir())
    for forbidden in (query, "privateBaselineCredential", "privateBaselineBody", "unknownPrivateField"):
        assert forbidden not in persisted


@pytest.mark.parametrize(
    "value", [True, -1, 1.5, float("nan"), float("inf"), None, [], {}, "-1", "NaN", "1e3", 1 << 64]
)
def test_summary_rejects_invalid_typed_metrics(value):
    response = httpx.Response(200, headers={"X-ClickHouse-Summary": json.dumps({"read_rows": value})})
    assert truth_sql_failure_evidence._summary_metrics(response) == {"status": "invalid"}


def test_summary_rejects_duplicate_keys_and_ignores_unknown_keys():
    response = httpx.Response(200, headers={"X-ClickHouse-Summary": '{"read_rows":"1","read_rows":"2"}'})
    assert truth_sql_failure_evidence._summary_metrics(response) == {"status": "invalid"}
    response = httpx.Response(
        200, headers={"X-ClickHouse-Summary": '{"unknownSecretName":"unknownSecretValue"}'}
    )
    metrics = truth_sql_failure_evidence._summary_metrics(response)
    assert metrics["status"] == "observed"
    assert all(metrics[name + "_present"] == 0 for name in truth_sql_failure_evidence._SUMMARY_FIELDS)
    assert "unknownSecret" not in json.dumps(metrics)


@pytest.mark.parametrize(
    "scenario",
    [
        "samples",
        "real_only",
        "unsymbolized",
        "empty",
        "denied",
        "missing",
        "timeout",
        "cancelled",
        "boolean",
        "unknown_category",
        "too_many_samples",
        "too_many_per_type",
        "too_many_frames",
        "unknown_type",
        "duplicate",
        "oversized",
        "full_stack",
        "vector_short",
        "vector_long",
        "vector_boolean",
        "vector_float",
        "vector_negative",
        "vector_overflow",
        "vector_sum",
        "symbol_mismatch",
        "extra_field",
        "vector_object",
        "vector_duplicate",
        "maximum_stack",
        "hash_full_stack",
        "hash_vector_short",
        "hash_vector_long",
        "hash_vector_boolean",
        "hash_vector_negative",
        "hash_vector_float",
        "hash_vector_overflow",
        "hash_vector_sum",
        "hash_vector_object",
        "hash_vector_missing",
        "hash_vector_private",
    ],
)
async def test_owned_trace_log_protocol_privacy_and_refusal(native_observer_receipts, monkeypatch, scenario):
    nodeid, query = synthetic_phase_request()
    original = httpx.ReadTimeout("privateOriginalCredential")
    calls = []
    deadlines = []
    timeout_at = asyncio.timeout_at

    def bounded_timeout(deadline):
        deadlines.append(deadline)
        return timeout_at(deadline)

    monkeypatch.setattr(truth_sql_failure_evidence.asyncio, "timeout_at", bounded_timeout)
    sample = {
        "trace_type": "CPU",
        "frame_count": 2,
        "categories": ["ExpressionActions", "Other"],
        "symbol_count": 2,
        "class_counts": [0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        "hash_class_counts": [0, 0, 0, 0, 0, 2],
    }

    def respond(request):
        calls.append(request)
        if len(calls) == 1:
            raise original
        if len(calls) == 2:
            assert "FROM system.processes" in request.content.decode()
            return httpx.Response(200, content=b"")
        assert len(calls) == 3
        assert dict(request.url.params) == {
            "param_observer_query_id": calls[0].headers["X-ClickHouse-Query-Id"],
            "output_format_json_quote_64bit_integers": "0",
        }
        assert request.extensions["timeout"] == {"connect": 1, "read": 4, "write": 4, "pool": 4}
        assert "X-ClickHouse-Query-Id" not in request.headers
        assert request.headers["X-ClickHouse-User"] == "default"
        sql = request.content.decode()
        assert (
            "WHERE query_id = {observer_query_id:String} AND trace_type IN ('CPU', 'Real') "
            "ORDER BY event_time_microseconds DESC LIMIT 8 BY trace_type LIMIT 16 FORMAT JSONEachRow"
        ) in sql
        assert "arraySlice(symbols, 1, 32)" in sql
        for forbidden in ("system.stack_trace", "addressTo", "SYSTEM", "SETTINGS", "SELECT *", "lines"):
            assert forbidden not in sql
        if scenario in {"denied", "missing"}:
            return httpx.Response(403 if scenario == "denied" else 404, text="privateNativeSymbol credential")
        if scenario == "timeout":
            raise httpx.ReadTimeout("privateTraceCredential")
        if scenario == "cancelled":
            raise asyncio.CancelledError("privateTraceCredential")
        if scenario == "empty":
            return httpx.Response(200, content=b"")
        if scenario == "duplicate":
            body = json.dumps(sample)[:-1] + ', "trace_type": "CPU"}'
            return httpx.Response(200, content=body.encode())
        if scenario == "vector_duplicate":
            body = json.dumps(sample)[:-1] + ', "class_counts": ' + json.dumps(sample["class_counts"]) + "}"
            return httpx.Response(200, content=body.encode())
        if scenario == "oversized":
            return httpx.Response(200, content=b"privateTraceCredential" * 1000)
        samples = [
            sample,
            {
                "trace_type": "Real",
                "frame_count": 1,
                "categories": ["Unresolved"],
                "symbol_count": 1,
                "class_counts": [0] * 13 + [1, 0],
                "hash_class_counts": [0, 0, 0, 0, 1, 0],
            },
        ]
        if scenario == "real_only":
            samples = samples[1:]
        elif scenario == "unsymbolized":
            samples = [
                {
                    "trace_type": "CPU",
                    "frame_count": 2,
                    "categories": [],
                    "symbol_count": 0,
                    "class_counts": [0] * 15,
                    "hash_class_counts": [0] * 6,
                }
            ]
        elif scenario == "full_stack":
            sample.update(
                frame_count=43,
                symbol_count=43,
                categories=["Other"] * 32,
                class_counts=[1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 38],
                hash_class_counts=[0, 0, 0, 0, 0, 43],
            )
        elif scenario == "maximum_stack":
            sample.update(
                frame_count=255,
                symbol_count=255,
                categories=["Other"] * 32,
                class_counts=[0] * 14 + [255],
                hash_class_counts=[0, 0, 0, 0, 0, 255],
            )
            samples = [sample] * 8 + [sample | {"trace_type": "Real"}] * 8
        elif scenario == "vector_short":
            sample["class_counts"].pop()
        elif scenario == "vector_long":
            sample["class_counts"].append(0)
        elif scenario in {"vector_boolean", "vector_float", "vector_negative", "vector_overflow"}:
            sample["class_counts"][0] = {
                "vector_boolean": False,
                "vector_float": 0.0,
                "vector_negative": -1,
                "vector_overflow": 256,
            }[scenario]
            if scenario == "vector_negative":
                sample["class_counts"][-1] += 1
        elif scenario == "vector_sum":
            sample["class_counts"] = [0] * 15
        elif scenario == "symbol_mismatch":
            sample["symbol_count"] = 1
            sample["categories"] = ["ExpressionActions"]
            sample["class_counts"][-1] = 0
        elif scenario == "extra_field":
            sample["privateNativeSymbol"] = "privateTraceCredential"
        elif scenario == "vector_object":
            sample["class_counts"] = {"privateNativeSymbol": 2}
        elif scenario == "boolean":
            sample["frame_count"] = True
        elif scenario == "unknown_category":
            sample["categories"] = ["privateNativeSymbol credential"]
        elif scenario == "too_many_samples":
            samples = [sample] * 17
        elif scenario == "too_many_per_type":
            samples = [sample] * 9 + [sample | {"trace_type": "Real"}] * 7
        elif scenario == "too_many_frames":
            sample.update(
                frame_count=33,
                symbol_count=33,
                categories=["Other"] * 33,
                class_counts=[0] * 14 + [33],
                hash_class_counts=[0, 0, 0, 0, 0, 33],
            )
        elif scenario == "unknown_type":
            sample["trace_type"] = "Memory"
        elif scenario == "hash_full_stack":
            sample.update(
                frame_count=45,
                symbol_count=45,
                categories=["Other"] * 32,
                class_counts=[0] * 14 + [45],
                hash_class_counts=[1, 2, 1, 2, 1, 38],
            )
        elif scenario == "hash_vector_short":
            sample["hash_class_counts"].pop()
        elif scenario == "hash_vector_long":
            sample["hash_class_counts"].append(0)
        elif scenario == "hash_vector_missing":
            del sample["hash_class_counts"]
        elif scenario == "hash_vector_sum":
            sample["hash_class_counts"] = [0] * 6
        elif scenario == "hash_vector_object":
            sample["hash_class_counts"] = {"privateNativeSymbol": 2}
        elif scenario.startswith("hash_vector_"):
            sample["hash_class_counts"][0] = {
                "hash_vector_boolean": False,
                "hash_vector_negative": -1,
                "hash_vector_float": 0.0,
                "hash_vector_overflow": 3,
                "hash_vector_private": "privateNativeSymbol credential",
            }[scenario]
        return httpx.Response(200, content="\n".join(json.dumps(row) for row in samples).encode())

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = EvidenceClickHouse("http://localhost:8123", nodeid, http=http)
        with pytest.raises(httpx.ReadTimeout) as raised:
            await client.rows(query)
        assert raised.value is original
        assert client.http.post == client._original_http_post
        assert client._observation is None and client._evidence_query is None
    assert len(calls) == 3
    assert len(deadlines) == 2 and deadlines[0] == deadlines[1]
    diagnostic = calls[2]
    assert dict(diagnostic.url.params) == {
        "param_observer_query_id": calls[0].headers["X-ClickHouse-Query-Id"],
        "output_format_json_quote_64bit_integers": "0",
    }
    assert diagnostic.extensions["timeout"] == {"connect": 1, "read": 4, "write": 4, "pool": 4}
    assert "X-ClickHouse-Query-Id" not in diagnostic.headers
    sql = diagnostic.content.decode()
    assert (
        "WHERE query_id = {observer_query_id:String} AND trace_type IN ('CPU', 'Real') "
        "ORDER BY event_time_microseconds DESC LIMIT 8 BY trace_type LIMIT 16 FORMAT JSONEachRow"
    ) in sql
    assert "arraySlice(symbols, 1, 32)" in sql and "arraySlice(symbols, 1, 255)" in sql
    for forbidden in ("system.stack_trace", "addressTo", "SYSTEM", "SETTINGS", "SELECT *", "lines"):
        assert forbidden not in sql
    (receipt,) = native_receipts(native_observer_receipts)
    trace = receipt["own_trace_log"]
    if scenario in {
        "samples",
        "real_only",
        "unsymbolized",
        "full_stack",
        "maximum_stack",
        "hash_full_stack",
    }:
        assert trace["status"] == "observed"
        assert trace["unit"] == "retained_samples_not_cpu_seconds"
        assert trace["hash_classification_version"] == 1
        assert trace["hash_class_order"] == [
            "ConstantHash",
            "StringHash",
            "TableHash",
            "GetScalar",
            "Unresolved",
            "Other",
        ]
        if scenario == "hash_full_stack":
            assert trace["samples"][0] == sample | {"symbol_status": "observed"}
        assert trace["class_order"] == [
            "AnalyzerResolve",
            "ScalarEvaluation",
            "ConstantArguments",
            "TreeClone",
            "TreeHash",
            "ExpressionActions",
            "ActionsDAG",
            "ArrayFunction",
            "Function",
            "WideInteger",
            "PipelineExecutor",
            "ReadFromMergeTree",
            "ParserExpression",
            "Unresolved",
            "Other",
        ]
        if scenario == "full_stack":
            assert trace["samples"][0] == sample | {"symbol_status": "Unavailable"}
        if scenario == "maximum_stack":
            assert len(trace["samples"]) == 16
            assert trace["samples"] == (
                [sample | {"symbol_status": "Unavailable"}] * 8
                + [sample | {"trace_type": "Real", "symbol_status": "Unavailable"}] * 8
            )
        assert trace["CPU"] == (
            {"status": "NoCounter"}
            if scenario == "real_only"
            else {"status": "observed", "sample_count": 8 if scenario == "maximum_stack" else 1}
        )
        assert trace["Real"] == (
            {"status": "NoCounter"}
            if scenario == "unsymbolized"
            else {"status": "observed", "sample_count": 8 if scenario == "maximum_stack" else 1}
        )
        if scenario == "samples":
            assert trace["samples"][0] == sample | {"symbol_status": "observed"}
        if scenario == "unsymbolized":
            assert trace["samples"][0]["categories"] == []
            assert trace["samples"][0]["symbol_status"] == "Unavailable"
    elif scenario == "empty":
        assert trace == {"status": "NoCounter"}
    else:
        assert trace["status"] == "Unavailable"
        assert "samples" not in trace and "CPU" not in trace and "Real" not in trace
    persisted = "".join(path.read_text() for path in native_observer_receipts.iterdir())
    for forbidden in (query, "privateOriginalCredential", "privateTraceCredential", "privateNativeSymbol"):
        assert forbidden not in persisted


def test_owned_trace_hash_classifier_uses_only_pinned_public_methods():
    expected = (
        ("ConstantHash", ("DB::ConstantNode::updateTreeHashImpl(",)),
        (
            "StringHash",
            ("DB::ColumnString::updateHashWithValue(", "DB::ColumnString::updateHashFast("),
        ),
        ("TableHash", ("DB::TableNode::updateTreeHashImpl(",)),
        (
            "GetScalar",
            (
                "DB::(anonymous namespace)::FunctionGetScalar::getReturnTypeImpl(",
                "DB::(anonymous namespace)::FunctionGetScalar::executeImpl(",
            ),
        ),
        ("Unresolved", ()),
        ("Other", ()),
    )
    classes = truth_sql_failure_evidence._TRACE_HASH_CLASSES
    assert [name for name, _ in classes] == [name for name, _ in expected]
    for (_, predicate), (_, prefixes) in zip(classes[:-2], expected[:-2], strict=True):
        assert predicate == " OR ".join(f"startsWith(symbol, '{prefix}')" for prefix in prefixes)
    assert classes[-2:] == (("Unresolved", "empty(symbol)"), ("Other", "1"))
    synthetic_public_pinned_symbols = (
        ["DB::ColumnString::filter() const"] * 32
        + [prefix + ") const" for _, prefixes in expected for prefix in prefixes]
        + [
            "DB::ColumnString::updateHashFastPrivate()",
            "DB::ConstantNode::updateTreeHashImplPrivate()",
            "DB::TableNode::updateTreeHashImplPrivate()",
            "DB::(anonymous namespace)::FunctionGetScalar::privateMethod()",
            "private::DB::ColumnString::updateHashFast()",
            "privateSymbol 0x1234 payload",
            "",
        ]
    )
    counts = [0] * len(classes)
    for symbol in synthetic_public_pinned_symbols:
        for index, (_, predicate) in enumerate(classes):
            prefixes = re.findall(r"startsWith\(symbol, '([^']+)'\)", predicate)
            if (
                any(symbol.startswith(prefix) for prefix in prefixes)
                or (predicate == "empty(symbol)" and not symbol)
                or predicate == "1"
            ):
                counts[index] += 1
                break
    assert counts == [1, 2, 1, 2, 1, 38]
    sql = truth_sql_failure_evidence._own_trace_sql()
    projection = (
        "multiIf("
        + ", ".join(predicate + ", '" + name + "'" for name, predicate in classes[:-1])
        + ", 'Other')"
    )
    assert "arrayMap(symbol -> " + projection + ", arraySlice(symbols, 1, 255))" in sql
    assert "AS hash_class_counts" in sql
    assert "['ConstantHash', 'StringHash', 'TableHash', 'GetScalar', 'Unresolved', 'Other']" in sql
    assert "privateSymbol" not in sql and "payload" not in sql and "0x1234" not in sql


def test_parameter_string_preserves_native_escaped_spelling():
    value = "α😀\\\t\n\r\x00'\""
    assert parameter_string(value) == "α😀\\\\\\t\\n\\r\\0'\""
    assert parameter_string("") == ""


@pytest.mark.integration
async def test_real_native_domain_parameter_serializers_preserve_complete_json(request):
    from tank.analysis import (
        truth_sql_derivatives,
        truth_sql_liquidity,
        truth_sql_relative,
        truth_sql_result,
        truth_sql_spot,
        truth_sql_support,
    )

    url = os.environ.get("CLICKHOUSE_TEST_URL")
    assert url, "CLICKHOUSE_TEST_URL must identify the owned native CI service"
    marker = "שלום α😀\\\n\t\r\x00\"'" * 4000
    observation = {
        "kind": "FUNDAMENTALS",
        "identity_evidence": {"marker": marker},
        "ts": "2026-01-01T00:00:00Z",
    }
    snapshot = {
        "token_id": "T",
        "cutoff": "2026-01-01T01:00:00Z",
        "anchor": {"ts": "2026-01-01T00:00:00Z"},
        "supply": [observation],
        "stat_price_series": {"z-first": {"marker": marker}, "a-last": {}},
    }
    config = {"transport_marker": marker}
    domain = {"marker": marker}
    result_domains = ("spots", "derivatives", "liquidity", "supply", "relative", "confidence")
    builders = (
        (truth_sql_spot.query, {}, {"spot_snapshot": snapshot}, "canonical"),
        (
            truth_sql_derivatives.query,
            {},
            {"deriv_snapshot": snapshot, "deriv_config": config},
            "canonical",
        ),
        (truth_sql_relative.query, {}, {"rel_snapshot": snapshot}, "ordered_compact"),
        (
            truth_sql_liquidity.query,
            {"spot": domain, "relative": domain},
            {"lq_snapshot": snapshot, "lq_config": config, "lq_spot": domain, "lq_relative": domain},
            "canonical",
        ),
        (
            truth_sql_support.query,
            {name: domain for name in ("spots", "derivatives", "liquidity", "relative")},
            {
                "sup_snapshot": snapshot,
                "sup_config": config,
                **{"sup_" + name: domain for name in ("spots", "derivatives", "liquidity", "relative")},
            },
            "ordered",
        ),
        (
            truth_sql_result.query,
            {name: domain for name in result_domains},
            {
                "res_flow_snapshot": snapshot,
                "res_snapshot": snapshot,
                "res_config": config,
                **{"res_" + name: domain for name in result_domains},
            },
            "canonical",
        ),
    )
    checks = {}
    client = EvidenceClickHouse(url, request.node.nodeid)
    try:
        for builder, kwargs, originals, spelling in builders:
            _, parameters = builder(snapshot, config, **kwargs)
            expected = {
                name: json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=spelling == "canonical",
                    separators=None if spelling == "ordered" else (",", ":"),
                )
                for name, value in originals.items()
            }
            if "sup_snapshot" in expected:
                expected["sup_supply_preimages"] = json.dumps([json.dumps(observation, sort_keys=True)])
            selected = {name: parameters[name] for name in expected}
            query = "SELECT " + ", ".join(
                expression
                for name in expected
                for expression in (
                    f"{{{name}:String}} AS {name}_value",
                    f"length({{{name}:String}}) AS {name}_bytes",
                    f"lengthUTF8({{{name}:String}}) AS {name}_characters",
                    f"lower(hex(SHA256({{{name}:String}}))) AS {name}_hash",
                    f"substringUTF8({{{name}:String}}, 2, 9) AS {name}_substring",
                )
            )
            rows = await client.rows(query, selected)
            assert len(rows) == 1
            for name, text in expected.items():
                assert rows[0][name + "_value"] == text
                assert rows[0][name + "_bytes"] == len(text.encode())
                assert rows[0][name + "_characters"] == len(text)
                assert rows[0][name + "_hash"] == hashlib.sha256(text.encode()).hexdigest()
                assert rows[0][name + "_substring"] == text[1:10]
                checks[name] = {
                    "bytes": len(text.encode()),
                    "characters": len(text),
                    "sha256": hashlib.sha256(text.encode()).hexdigest(),
                }
            if "rel_snapshot" in expected:
                decoded = json.loads(rows[0]["rel_snapshot_value"])
                assert list(decoded["stat_price_series"]) == ["z-first", "a-last"]
            if "sup_snapshot" in expected:
                decoded = json.loads(rows[0]["sup_snapshot_value"])
                assert list(decoded["stat_price_series"]) == ["z-first", "a-last"]
                assert json.loads(rows[0]["sup_supply_preimages_value"]) == [
                    json.dumps(observation, sort_keys=True)
                ]
    finally:
        await client.close()
    truth_sql_failure_evidence.retain_transport_qualification(
        Path(__file__).resolve().parents[1] / ".test-results", request.node.nodeid, checks
    )


@pytest.mark.integration
async def test_real_native_large_multipart_values(request):
    url = os.environ.get("CLICKHOUSE_TEST_URL")
    assert url, "CLICKHOUSE_TEST_URL must identify the owned native CI service"
    ascii_text = "A0&=+%?'\"\\\n\t\r\x00\\N" * 7000
    unicode_text = "שלום α😀漢字\\\n'\t\r" * 5000
    query_text = "Ω😀漢字" * 20000
    array_values = ["α\\\n\t\r\x00'\"" * 8000, "", "\\N"]
    array_wire = (
        "["
        + ",".join(
            "'"
            + text.replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\n", "\\n")
            .replace("\t", "\\t")
            .replace("\r", "\\r")
            .replace("\x00", "\\0")
            + "'"
            for text in array_values
        )
        + "]"
    )
    values = {"ascii": ascii_text, "unicode": unicode_text, "empty": "", "null": "\\N"}
    parameters: dict[str, tuple[object, str]] = {
        key: (
            value
            if key == "null"
            else value.replace("\\", "\\\\")
            .replace("\n", "\\n")
            .replace("\t", "\\t")
            .replace("\r", "\\r")
            .replace("\0", "\\0"),
            "Nullable(String)" if key == "null" else "String",
        )
        for key, value in values.items()
    }
    parameters.update(
        array=(array_wire, "Array(String)"),
        nullable=(parameters["ascii"][0], "Nullable(String)"),
        truth=(True, "Bool"),
        number=(-123, "Int64"),
        decimal=("123.125", "Decimal(12, 3)"),
    )
    for key in ("ascii", "unicode", "empty"):
        assert parameter_string(values[key]) == parameters[key][0]
    query = (
        "SELECT "
        + ", ".join(
            expression
            for key in ("ascii", "unicode", "empty")
            for expression in (
                f"{{{key}:String}} AS {key}_value",
                f"length({{{key}:String}}) AS {key}_bytes",
                f"lengthUTF8({{{key}:String}}) AS {key}_characters",
                f"lower(hex(SHA256({{{key}:String}}))) AS {key}_hash",
                f"substringUTF8({{{key}:String}}, 2, 9) AS {key}_substring",
            )
        )
        + ", {null:Nullable(String)} AS nullable_value, {array:Array(String)} AS array_value"
        + ", {nullable:Nullable(String)} AS nonnull_nullable_value"
        + ", toTypeName({ascii:String}) AS string_type"
        + ", toTypeName({array:Array(String)}) AS array_type"
        + ", toTypeName({nullable:Nullable(String)}) AS nullable_type"
        + ", {truth:Bool} AS truth_value, {number:Int64} AS number_value"
        + ", toString({decimal:Decimal(12, 3)}) AS decimal_value"
        + f", '{query_text}' AS query_value"
        + ", 'complete-query-tail' AS query_tail"
    )
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
    assert rows[0]["array_value"] == array_values
    assert rows[0]["nonnull_nullable_value"] == ascii_text
    assert rows[0]["string_type"] == "String"
    assert rows[0]["array_type"] == "Array(String)"
    assert rows[0]["nullable_type"] == "Nullable(String)"
    assert rows[0]["truth_value"] is True
    assert rows[0]["number_value"] == -123
    assert rows[0]["decimal_value"] == "123.125"
    assert rows[0]["query_value"] == query_text
    assert rows[0]["query_tail"] == "complete-query-tail"
    truth_sql_failure_evidence.retain_transport_qualification(
        Path(__file__).resolve().parents[1] / ".test-results",
        request.node.nodeid,
        {
            key: {
                "bytes": len(values[key].encode()),
                "characters": len(values[key]),
                "sha256": hashlib.sha256(values[key].encode()).hexdigest(),
            }
            for key in ("ascii", "unicode", "empty")
        },
    )


@pytest.mark.parametrize("status", [200, 500])
@pytest.mark.parametrize("decoder_failure", [False, True])
async def test_ci_native_deep_summary_preserves_original_response(
    native_observer_receipts, monkeypatch, status, decoder_failure
):
    nodeid, query = synthetic_phase_request()
    summary = '{"unknownSecretName":' + "[" * 2000 + '"credentialPrefix;credentialSuffix"' + "]" * 2000 + "}"
    assert len(summary) < 16384
    original_loads = json.loads
    try:
        original_loads(summary)
    except RecursionError:
        expected_summary = {"status": "invalid"}
    else:
        expected_summary = {
            "status": "observed",
            "read_rows_present": 0,
            "read_bytes_present": 0,
            "written_rows_present": 0,
            "written_bytes_present": 0,
            "total_rows_to_read_present": 0,
            "result_rows_present": 0,
            "result_bytes_present": 0,
            "elapsed_ns_present": 0,
        }
    if decoder_failure:

        def decode(value, *args, **kwargs):
            if value == summary:
                raise RecursionError("privateRecursionPrefix;privateRecursionSuffix")
            return original_loads(value, *args, **kwargs)

        monkeypatch.setattr(json, "loads", decode)
        expected_summary = {"status": "invalid"}
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(
            status,
            content=b'{"value":7}\n' if status == 200 else b"owned failure body",
            headers={"X-ClickHouse-Summary": summary},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = EvidenceClickHouse("http://localhost:8123", nodeid, http=http)
        if status == 200:
            assert await client.rows(query) == [{"value": 7}]
        else:
            with pytest.raises(RuntimeError) as caught:
                await client.rows(query)
            assert type(caught.value) is RuntimeError
            assert str(caught.value) == "ClickHouse 500: owned failure body"
        assert client.http.post == client._original_http_post
        assert client._observation is None
        assert client._evidence_query is None
    receipts = native_receipts(native_observer_receipts)
    assert len(calls) == len(receipts) == 1
    assert receipts[0]["summary"] == expected_summary
    assert receipts[0]["outcome"] == ("decoded" if status == 200 else "exception")
    assert receipts[0]["own_query"] == {"status": "not_requested"}
    persisted = "".join(path.read_text() for path in native_observer_receipts.iterdir())
    for forbidden in (
        "unknownSecretName",
        "credentialPrefix",
        "credentialSuffix",
        "privateRecursionPrefix",
        "privateRecursionSuffix",
        summary,
        query,
    ):
        assert forbidden not in persisted


@pytest.mark.parametrize(
    "scenario",
    [
        "ordered", "unsymbolized", "maximum", "missing", "short", "long", "private", "boolean", "float",
        "negative", "unknown", "object", "inconsistent", "extra", "hash_unresolved", "category_unresolved",
        "category_recognized", "category_short", "category_counts", "hash_overlap", "legitimate_overlap",
        "boundary", "oversize", "duplicate", "lambda_only", "large_categories", "shared_suffix",
        "flexible_collision", "flexible_valid", "shared_suffix_valid",
    ],
)
async def test_owned_baseline_ordered_stack_privacy_and_validation(native_observer_receipts, scenario):
    nodeid = truth_sql_failure_evidence._LIQUIDITY_BASELINE
    query = "private SQL payload"
    original = httpx.ReadTimeout("privateOriginalCredential")
    calls = []
    sample = {
        "trace_type": "CPU",
        "frame_count": 4,
        "symbol_count": 4,
        "categories": ["Other", "Other", "Other", "Unresolved"],
        "class_counts": [1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1],
        "hash_class_counts": [0, 0, 0, 0, 1, 3],
        "ordered_classes": [3, 0, 1, 5],
    }

    def respond(request):
        calls.append(request)
        if len(calls) == 1:
            raise original
        if len(calls) == 2:
            return httpx.Response(200, content=b"")
        assert len(calls) == 3
        assert dict(request.url.params)["param_observer_query_id"] == calls[0].headers["X-ClickHouse-Query-Id"]
        sql = request.content.decode()
        assert "AS ordered_classes" in sql
        assert "DB::QueryAnalyzer::resolveFunction(" in sql
        assert "DB::IQueryTreeNode::getTreeHash(" in sql
        assert "arraySlice(symbols, 1, 255)" in sql
        assert query not in sql and "privateNativeSymbol" not in sql
        if scenario == "unsymbolized":
            sample.update(
                frame_count=4, symbol_count=0, categories=[], class_counts=[0] * 15,
                hash_class_counts=[0] * 6, ordered_classes=[],
            )
        elif scenario == "maximum":
            sample.update(
                frame_count=255, symbol_count=255, categories=["Other"] * 32,
                class_counts=[0] * 14 + [255], hash_class_counts=[0] * 5 + [255],
                ordered_classes=[0] * 255,
            )
        elif scenario == "missing":
            del sample["ordered_classes"]
        elif scenario == "short":
            sample["ordered_classes"].pop()
        elif scenario == "long":
            sample["ordered_classes"].append(0)
        elif scenario == "object":
            sample["ordered_classes"] = {"privateNativeSymbol": "privateTraceCredential"}
        elif scenario == "extra":
            sample["privateNativeSymbol"] = "privateTraceCredential"
        elif scenario == "inconsistent":
            sample["ordered_classes"][0] = 0
        elif scenario == "hash_unresolved":
            sample["hash_class_counts"] = [0, 0, 0, 0, 0, 4]
        elif scenario == "category_unresolved":
            sample["categories"][3] = "Other"
        elif scenario == "category_recognized":
            sample["categories"][0] = "Function"
        elif scenario == "category_short":
            sample["categories"].pop()
        elif scenario == "category_counts":
            sample["categories"][1] = "ExpressionActions"
        elif scenario == "hash_overlap":
            sample["hash_class_counts"] = [2, 0, 0, 0, 1, 1]
        elif scenario == "shared_suffix":
            sample.update(
                frame_count=33, symbol_count=33, categories=["Other"] * 32,
                class_counts=[0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 31],
                hash_class_counts=[0, 0, 0, 0, 0, 33], ordered_classes=[0] * 33,
            )
        elif scenario == "flexible_collision":
            sample.update(
                frame_count=33, symbol_count=33, categories=["Other"] * 31 + ["Function"],
                class_counts=[0, 0, 0, 0, 0, 1, 1, 0, 1, 0, 0, 0, 0, 0, 30],
                hash_class_counts=[0, 0, 0, 0, 0, 33], ordered_classes=[0] * 33,
            )
        elif scenario == "flexible_valid":
            sample.update(
                frame_count=3, symbol_count=3, categories=["Other", "Function", "Other"],
                class_counts=[0] * 8 + [2] + [0] * 5 + [1],
                hash_class_counts=[1, 0, 0, 0, 0, 2], ordered_classes=[0] * 3,
            )
        elif scenario == "shared_suffix_valid":
            sample.update(
                frame_count=34, symbol_count=34, categories=["Other"] * 32,
                class_counts=[0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 32],
                hash_class_counts=[1, 0, 0, 0, 0, 33], ordered_classes=[0] * 34,
            )
        elif scenario == "lambda_only":
            sample.update(
                frame_count=1, symbol_count=1, categories=["Other"],
                class_counts=[0] * 14 + [1], hash_class_counts=[0] * 5 + [1], ordered_classes=[6],
            )
        elif scenario == "large_categories":
            sample.update(
                frame_count=255, symbol_count=255, categories=["ExpressionActions"] * 32,
                class_counts=[0] * 5 + [255] + [0] * 9, hash_class_counts=[0] * 5 + [255],
                ordered_classes=[0] * 255,
            )
        elif scenario == "legitimate_overlap":
            sample.update(
                frame_count=3, symbol_count=3, categories=["Other"] * 3,
                class_counts=[0] * 14 + [3], hash_class_counts=[1, 0, 0, 0, 0, 2],
                ordered_classes=[0, 6, 0],
            )
        elif scenario in {"private", "boolean", "float", "negative", "unknown"}:
            sample["ordered_classes"][0] = {
                "private": "privateNativeSymbol 0x1234 privateTraceCredential",
                "boolean": True, "float": 3.0, "negative": -1, "unknown": 7,
            }[scenario]
        samples = [sample]
        if scenario in {"maximum", "large_categories"}:
            samples = [sample] * 8 + [sample | {"trace_type": "Real"}] * 8
        content = "\n".join(json.dumps(row, separators=(",", ":")) for row in samples).encode()
        if scenario == "large_categories":
            assert len(content) > 16384
        if scenario == "duplicate":
            content = content.replace(b'{"trace_type":', b'{"trace_type":"CPU","trace_type":', 1)
        if scenario in {"boundary", "oversize"}:
            size = 16384 if scenario == "boundary" else 16385
            assert len(content) < size
            content += b" " * (size - len(content))
            assert len(content) == size
        return httpx.Response(200, content=content)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = EvidenceClickHouse("http://localhost:8123", nodeid, http=http, baseline_query=query)
        with pytest.raises(httpx.ReadTimeout) as raised:
            await client.rows(query)
        assert raised.value is original
        assert client.http.post == client._original_http_post
        assert client._observation is None and client._evidence_query is None
    assert len(calls) == 3
    (receipt,) = native_receipts(native_observer_receipts)
    assert receipt["phase"] == "baseline"
    assert receipt["query_digest"] == hashlib.sha256(query.encode()).hexdigest()
    assert receipt["query_id"] == calls[0].headers["X-ClickHouse-Query-Id"]
    trace = receipt["own_trace_log"]
    if scenario in {
        "ordered", "unsymbolized", "maximum", "legitimate_overlap", "boundary", "lambda_only",
        "flexible_valid", "shared_suffix_valid",
    }:
        assert trace["status"] == "observed"
        assert trace["ordered_classification_version"] == 1
        assert trace["ordered_class_order"] == [
            "Other", "ResolveFunction", "ResolveExpression", "TreeHash", "TreeClone", "Unresolved", "ResolveLambda"
        ]
        assert trace["ordered_stack_direction"] == "as_stored_in_trace_log"
        assert trace["samples"][0]["ordered_classes"] == sample["ordered_classes"]
        if scenario == "maximum":
            assert len(trace["samples"]) == 16
        if scenario == "unsymbolized":
            assert trace["samples"][0]["symbol_status"] == "Unavailable"
        if scenario in {"ordered", "legitimate_overlap", "boundary", "lambda_only"}:
            assert trace["samples"][0]["symbol_status"] == "observed"
    else:
        assert trace == {"status": "Unavailable", "reason": "read_or_validation_error"}
    persisted = "".join(path.read_text() for path in native_observer_receipts.iterdir())
    for forbidden in (query, "privateOriginalCredential", "privateNativeSymbol", "privateTraceCredential", "0x1234"):
        assert forbidden not in persisted


def test_ordered_classifier_is_fixed_and_preserves_stack_order():
    classes = truth_sql_failure_evidence._TRACE_ORDERED_CLASSES
    assert [name for name, _ in classes] == [
        "Other", "ResolveFunction", "ResolveExpression", "TreeHash", "TreeClone", "Unresolved", "ResolveLambda"
    ]
    symbols = [
        "DB::IQueryTreeNode::getTreeHash()",
        "privateNativeSymbol 0x1234 payload",
        "DB::QueryAnalyzer::resolveFunction()",
        "DB::QueryAnalyzer::resolveExpressionNode()",
        "DB::IQueryTreeNode::clone()",
        "",
        "DB::QueryAnalyzer::resolveLambda()",
    ]
    observed = []
    for symbol in symbols:
        code = 0
        for index, (_, predicate) in enumerate(classes[1:], 1):
            prefixes = re.findall(r"startsWith\(symbol, '([^']+)'\)", predicate)
            if any(symbol.startswith(prefix) for prefix in prefixes) or (predicate == "empty(symbol)" and not symbol):
                code = index
                break
        observed.append(code)
    assert observed == [3, 0, 1, 2, 4, 5, 6]
    assert list(reversed(observed)) != observed
    sql = truth_sql_failure_evidence._own_trace_sql(ordered=True)
    expected = (
        ", arrayMap(symbol -> multiIf("
        "startsWith(symbol, 'DB::QueryAnalyzer::resolveFunction('), 1, "
        "startsWith(symbol, 'DB::QueryAnalyzer::resolveExpressionNode('), 2, "
        "startsWith(symbol, 'DB::IQueryTreeNode::getTreeHash('), 3, "
        "startsWith(symbol, 'DB::IQueryTreeNode::clone(') OR "
        "startsWith(symbol, 'DB::IQueryTreeNode::cloneAndReplace('), 4, "
        "empty(symbol), 5, "
        "startsWith(symbol, 'DB::QueryAnalyzer::resolveLambda('), 6, 0), "
        "arraySlice(symbols, 1, 255)) AS ordered_classes"
    )
    phase_sql = truth_sql_failure_evidence._own_trace_sql()
    suffix = " FROM system.trace_log WHERE query_id = {observer_query_id:String} "
    assert phase_sql.count(suffix) == 1
    assert sql == phase_sql.replace(suffix, expected + suffix, 1)
    assert "ordered_classes" not in phase_sql
    assert "privateNativeSymbol" not in sql and "0x1234" not in sql and "payload" not in sql
