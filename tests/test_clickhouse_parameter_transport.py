import asyncio
import email.parser
import email.policy
import hashlib
import itertools
import json
import os
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
    ["tier_values", "venue_depth", "venue_precision", "venue_walk_t1", "venue_walk_t2", "venue_walk_t3"],
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
    assert len(calls) == 2
    (receipt,) = native_receipts(native_observer_receipts)
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
