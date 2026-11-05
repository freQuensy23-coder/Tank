import asyncio
import hashlib
import ipaddress
import json
import os
from pathlib import Path
from urllib.parse import quote, urlencode
from uuid import uuid4

import pytest
from postgres_helpers import _create_database, _drop_database, _finish, _template_name
from psycopg.conninfo import conninfo_to_dict, make_conninfo

pytestmark = [pytest.mark.integration, pytest.mark.postgres, pytest.mark.node, pytest.mark.process]


def _unique_postgres_fields(pairs):
    fields = {}
    for key, value in pairs:
        if key in fields:
            raise ValueError
        fields[key] = value
    return fields


def _decode_postgres_frame(stdout, marker):
    markers = (b"TANK_OPENMA_POSTGRES_FAILURE_V1 ", b"TANK_OPENMA_POSTGRES_SUCCESS_V1 ")
    if marker not in markers or len(stdout) > 65536:
        return None
    if sum(stdout.count(tag) for tag in markers) != 1 or stdout.count(marker) != 1:
        return None
    frames = [line for line in stdout.split(b"\n") if line.startswith(marker)]
    if len(frames) != 1 or not stdout.endswith(b"\n"):
        return None
    payload = frames[0][len(marker) :]
    if len(payload) > 4096:
        return None
    try:
        return json.loads(payload, object_pairs_hook=_unique_postgres_fields)
    except (ValueError, UnicodeError, RecursionError):
        return None


def _decode_postgres_success(stdout):
    receipt = _decode_postgres_frame(stdout, b"TANK_OPENMA_POSTGRES_SUCCESS_V1 ")
    flags = {
        "shippingEntrypointExercised",
        "nativeVmExercised",
        "retainedHistory",
        "callbackIdempotence",
        "executionAndResourceFences",
        "settlementAndReopen",
    }
    if (
        isinstance(receipt, dict)
        and set(receipt) == {"qualification", *flags}
        and receipt["qualification"] == "postgres-source-only"
        and all(type(receipt[key]) is bool for key in flags)
    ):
        return receipt
    return None


def _decode_postgres_failure(stdout):
    evidence = _decode_postgres_frame(stdout, b"TANK_OPENMA_POSTGRES_FAILURE_V1 ")
    stages = {
        "imports",
        "input",
        "connection",
        "migration",
        "fixture",
        "original-history",
        "replacement",
        "callback",
        "continuation",
        "execution-fences",
        "settlement",
        "callback-refusal",
        "resource-fences",
        "model-history",
        "model-error-state",
        "cleanup",
    }
    reasons = {
        "UNKNOWN",
        "assertion",
        "rejected-error-state",
        "ERR_MODULE_NOT_FOUND",
        "MODULE_NOT_FOUND",
        "ERR_PACKAGE_PATH_NOT_EXPORTED",
        "ERR_UNKNOWN_FILE_EXTENSION",
        "ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX",
    }
    if (
        isinstance(evidence, dict)
        and set(evidence) == {"qualification", "status", "stage", "reason"}
        and evidence["qualification"] == "postgres-source-only"
        and evidence["status"] == "FAIL"
        and isinstance(evidence["stage"], str)
        and evidence["stage"] in stages
        and isinstance(evidence["reason"], str)
        and evidence["reason"] in reasons
        and (evidence["reason"] != "rejected-error-state" or evidence["stage"] == "model-error-state")
    ):
        return {"stage": evidence["stage"], "reason": evidence["reason"]}
    return None


def _local_postgres_params(admin_dsn):
    try:
        params = conninfo_to_dict(admin_dsn)
        if set(params) - {"host", "port", "dbname", "user", "password", "sslmode", "connect_timeout"}:
            raise ValueError
        if any(key.startswith("PG") for key in os.environ):
            raise ValueError
        host = params.get("host", "")
        if host not in {"127.0.0.1", "::1"} or not ipaddress.ip_address(host).is_loopback:
            raise ValueError
        _template_name(params.get("dbname"))
        port = params.get("port", "5432")
        if not port.isascii() or not port.isdecimal() or not 1 <= int(port) <= 65535:
            raise ValueError
        if not params.get("user"):
            raise ValueError
        params["port"] = str(int(port))
        params["connect_timeout"] = "5"
    except (ValueError, TypeError):
        raise ValueError(
            "PostgreSQL qualification requires an explicit numeric loopback test target"
        ) from None
    return params


@pytest.mark.parametrize(
    "suffix",
    [
        "hostaddr=192.0.2.1",
        "service=production",
        "host=127.0.0.1,192.0.2.1",
        "port=0",
        "dbname=production",
    ],
)
def test_openma_postgres_target_refusal(suffix, monkeypatch):
    for key in tuple(os.environ):
        if key.startswith("PG"):
            monkeypatch.delenv(key)
    with pytest.raises(ValueError, match="explicit numeric loopback test target"):
        _local_postgres_params(f"host=127.0.0.1 port=5432 user=postgres dbname=tank_test_admin {suffix}")


@pytest.mark.parametrize(
    "stage, reason", [("migration", "UNKNOWN"), ("model-error-state", "rejected-error-state")]
)
def test_openma_postgres_failure_frame_projection(stage, reason):
    envelope = json.dumps(
        {"qualification": "postgres-source-only", "status": "FAIL", "stage": stage, "reason": reason}
    )
    stdout = (
        b'{"msg":"db.boundary"}\n' * 200 + b"TANK_OPENMA_POSTGRES_FAILURE_V1 " + envelope.encode() + b"\n"
    )
    diagnostic = _decode_postgres_failure(stdout)
    assert diagnostic == {"stage": stage, "reason": reason}
    assert diagnostic is not None
    diagnostic["stage"] = "changed"
    assert _decode_postgres_failure(stdout) == {"stage": stage, "reason": reason}


def test_openma_postgres_failure_frame_exact_limits():
    payload = (
        b'{"qualification":"postgres-source-only","status":"FAIL","stage":"migration","reason":"UNKNOWN"}'
    )
    frame = b"TANK_OPENMA_POSTGRES_FAILURE_V1 " + payload.ljust(4096) + b"\n"
    stdout = b"x" * (65536 - len(frame) - 1) + b"\n" + frame
    assert _decode_postgres_failure(stdout) == {"stage": "migration", "reason": "UNKNOWN"}


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"not-json",
        b"[]",
        b"null",
        b'{"qualification":"postgres-source-only","status":"PASS","stage":"migration","reason":"UNKNOWN"}',
        b'{"qualification":"other","status":"FAIL","stage":"migration","reason":"UNKNOWN"}',
        b'{"qualification":"postgres-source-only","status":"FAIL","stage":"migration"}',
        b'{"qualification":"postgres-source-only","status":"FAIL","stage":"migration","reason":"UNKNOWN","message":"private"}',
        b'{"qualification":"postgres-source-only","status":"FAIL","stage":"private","reason":"UNKNOWN"}',
        b'{"qualification":"postgres-source-only","status":"FAIL","stage":[],"reason":"UNKNOWN"}',
        b'{"qualification":"postgres-source-only","status":"FAIL","stage":"migration","reason":{}}',
        b'{"qualification":"postgres-source-only","status":"FAIL","stage":"migration","reason":"private"}',
        b'{"qualification":"postgres-source-only","status":"FAIL","stage":"migration","reason":"rejected-error-state"}',
        b'{"qualification":"postgres-source-only","status":"FAIL","stage":"private","stage":"migration","reason":"UNKNOWN"}',
        b"\xff",
        b"[" * 1500 + b"]" * 1500,
        b" " * 4097,
    ],
)
def test_openma_postgres_failure_frame_payload_refusal(payload):
    assert _decode_postgres_failure(b"TANK_OPENMA_POSTGRES_FAILURE_V1 " + payload + b"\n") is None


@pytest.mark.parametrize(
    "shape", ["absent", "embedded", "duplicate", "malformed-duplicate", "unterminated", "oversized"]
)
def test_openma_postgres_failure_frame_boundary_refusal(shape):
    payload = (
        b'{"qualification":"postgres-source-only","status":"FAIL","stage":"migration","reason":"UNKNOWN"}'
    )
    frame = b"TANK_OPENMA_POSTGRES_FAILURE_V1 " + payload + b"\n"
    stdout = {
        "absent": payload,
        "embedded": b"log " + frame,
        "duplicate": frame + frame,
        "malformed-duplicate": frame + b"TANK_OPENMA_POSTGRES_FAILURE_V1 invalid\n",
        "unterminated": frame[:-1],
        "oversized": b"x" * 65536 + b"\n" + frame,
    }[shape]
    assert _decode_postgres_failure(stdout) is None


@pytest.fixture
def postgres_success_receipt():
    return {
        "qualification": "postgres-source-only",
        "shippingEntrypointExercised": False,
        "nativeVmExercised": False,
        "retainedHistory": True,
        "callbackIdempotence": True,
        "executionAndResourceFences": True,
        "settlementAndReopen": True,
    }


@pytest.mark.parametrize("position", ["before", "after", "both"])
def test_openma_postgres_success_frame_projection(position, postgres_success_receipt):
    payload = json.dumps(postgres_success_receipt).encode()
    frame = b"TANK_OPENMA_POSTGRES_SUCCESS_V1 " + payload + b"\n"
    logs = b'{"msg":"db.boundary"}\n' * 200 + payload + b"\n"
    stdout = (logs if position != "after" else b"") + frame + (logs if position != "before" else b"")
    assert _decode_postgres_success(stdout) == postgres_success_receipt
    assert _decode_postgres_failure(stdout) is None


def test_openma_postgres_success_frame_exact_limits(postgres_success_receipt):
    payload = json.dumps(postgres_success_receipt).encode().ljust(4096)
    frame = b"TANK_OPENMA_POSTGRES_SUCCESS_V1 " + payload + b"\n"
    stdout = b"x" * (65536 - len(frame) - 1) + b"\n" + frame
    assert _decode_postgres_success(stdout) == postgres_success_receipt


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"not-json",
        b"[]",
        b"null",
        b"true",
        b"1",
        b"\xff",
        b"[" * 1500 + b"]" * 1500,
        b" " * 4097,
    ],
)
def test_openma_postgres_success_frame_payload_refusal(payload):
    assert _decode_postgres_success(b"TANK_OPENMA_POSTGRES_SUCCESS_V1 " + payload + b"\n") is None


@pytest.mark.parametrize(
    "field, value",
    [
        ("qualification", "other"),
        ("qualification", None),
        ("shippingEntrypointExercised", 0),
        ("nativeVmExercised", 0.0),
        ("retainedHistory", 1),
        ("callbackIdempotence", "true"),
        ("executionAndResourceFences", []),
        ("settlementAndReopen", {}),
        ("retainedHistory", None),
    ],
)
def test_openma_postgres_success_frame_type_refusal(field, value, postgres_success_receipt):
    postgres_success_receipt[field] = value
    payload = json.dumps(postgres_success_receipt).encode()
    assert _decode_postgres_success(b"TANK_OPENMA_POSTGRES_SUCCESS_V1 " + payload + b"\n") is None


@pytest.mark.parametrize(
    "shape",
    [
        "absent",
        "embedded",
        "duplicate",
        "embedded-duplicate",
        "malformed-duplicate",
        "conflicting-before",
        "conflicting-after",
        "unterminated",
        "joined-log",
        "wrong-marker",
        "oversized",
        "oversized-payload",
        "missing-key",
        "extra-key",
        "duplicate-key",
        "escaped-duplicate-key",
        "nested-duplicate-key",
        "trailing-json",
    ],
)
def test_openma_postgres_success_frame_boundary_refusal(shape, postgres_success_receipt):
    payload = json.dumps(postgres_success_receipt).encode()
    marker = b"TANK_OPENMA_POSTGRES_SUCCESS_V1 "
    frame = marker + payload + b"\n"
    failure = (
        b'TANK_OPENMA_POSTGRES_FAILURE_V1 {"qualification":"postgres-source-only",'
        b'"status":"FAIL","stage":"migration","reason":"UNKNOWN"}\n'
    )
    missing = dict(postgres_success_receipt)
    del missing["retainedHistory"]
    stdout = {
        "absent": payload + b"\n",
        "embedded": b"log " + frame,
        "duplicate": frame + frame,
        "embedded-duplicate": frame + b"log " + frame,
        "malformed-duplicate": frame + marker + b"invalid\n",
        "conflicting-before": failure + frame,
        "conflicting-after": frame + failure,
        "unterminated": frame[:-1],
        "joined-log": frame[:-1] + b"log\n",
        "wrong-marker": frame.replace(b"SUCCESS_V1 ", b"SUCCESS_V2 "),
        "oversized": b"x" * 65536 + b"\n" + frame,
        "oversized-payload": marker + payload.ljust(4097) + b"\n",
        "missing-key": marker + json.dumps(missing).encode() + b"\n",
        "extra-key": marker + payload[:-1] + b',"message":"private"}\n',
        "duplicate-key": marker + payload[:-1] + b',"retainedHistory":true}\n',
        "escaped-duplicate-key": marker + payload[:-1] + b',"retained\\u0048istory":true}\n',
        "nested-duplicate-key": marker + payload[:-1] + b',"extra":{"x":1,"x":2}}\n',
        "trailing-json": marker + payload + b" {}\n",
    }[shape]
    assert _decode_postgres_success(stdout) is None
    if shape in {"conflicting-before", "conflicting-after"}:
        assert _decode_postgres_failure(stdout) is None


async def test_openma_retained_history_and_ownership_on_postgres(record_property):
    params = _local_postgres_params(os.environ["TANK_TEST_DATABASE_URL"])
    admin_dsn = make_conninfo(**params)
    root = Path(__file__).resolve().parents[1]
    source = root / "tests/openma/lifecycle/retained-postgres.mjs"
    name = "tank_test_fixture_" + uuid4().hex
    creation = None
    spawning = None
    communication = None
    process = None
    primary_failure = None
    try:
        creation = asyncio.create_task(_create_database(admin_dsn, name=name))
        await asyncio.shield(creation)
        host = params["host"]
        if ":" in host:
            host = f"[{host}]"
        authority = quote(params["user"], safe="")
        if "password" in params:
            authority += ":" + quote(params["password"], safe="")
        url = f"postgresql://{authority}@{host}:{params['port']}/{name}"
        options = {key: params[key] for key in ("sslmode",) if key in params}
        if options:
            url += "?" + urlencode(options)
        spawning = asyncio.create_task(
            asyncio.create_subprocess_exec(
                "node",
                "--import",
                str(root / "scripts/node-typescript-loader.mjs"),
                "--experimental-transform-types",
                str(source),
                cwd=root,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        )
        process = await asyncio.shield(spawning)
        communication = asyncio.create_task(process.communicate(json.dumps({"url": url}).encode()))
        stdout, stderr = await asyncio.wait_for(asyncio.shield(communication), 60)
        if process.returncode != 0:
            diagnostic = _decode_postgres_failure(stdout)
            if diagnostic is None:
                diagnostic = {"stage": "UNKNOWN", "reason": "UNKNOWN"}
                for code in (
                    "ERR_MODULE_NOT_FOUND",
                    "MODULE_NOT_FOUND",
                    "ERR_PACKAGE_PATH_NOT_EXPORTED",
                    "ERR_UNKNOWN_FILE_EXTENSION",
                    "ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX",
                ):
                    if f"code: '{code}'".encode() in stderr or f"Error [{code}]".encode() in stderr:
                        diagnostic = {"stage": "node-startup", "reason": code}
                        break
            record_property("openma_postgres_failure", json.dumps(diagnostic, sort_keys=True))
            raise AssertionError(
                f"PostgreSQL source qualification child failed: STAGE={diagnostic['stage']} REASON={diagnostic['reason']}"
            )
        receipt = _decode_postgres_success(stdout)
        if receipt is None:
            raise AssertionError("PostgreSQL source qualification receipt is invalid") from None
        expected = {
            "qualification": "postgres-source-only",
            "shippingEntrypointExercised": False,
            "nativeVmExercised": False,
            "retainedHistory": True,
            "callbackIdempotence": True,
            "executionAndResourceFences": True,
            "settlementAndReopen": True,
        }
        if receipt != expected:
            raise AssertionError("PostgreSQL source qualification receipt does not match")
        record_property("openma_postgres_receipt", json.dumps(expected, sort_keys=True))
        record_property("source_sha", os.environ.get("GITHUB_SHA", "unavailable"))
        record_property("run_id", os.environ.get("GITHUB_RUN_ID", "unavailable"))
        record_property("qualification_sha256", hashlib.sha256(source.read_bytes()).hexdigest())
    except BaseException as error:
        primary_failure = error
        raise
    finally:

        async def cleanup():
            nonlocal process
            failures = []
            if spawning is not None and process is None:
                try:
                    process = await spawning
                except BaseException as error:
                    failures.append(error)
            joined = process is None
            if process is not None:
                drain = asyncio.create_task(process.communicate()) if communication is None else communication
                if process.returncode is None:
                    try:
                        process.terminate()
                    except ProcessLookupError:
                        pass
                    except BaseException as error:
                        failures.append(error)
                try:
                    await asyncio.wait_for(asyncio.shield(drain), 5)
                    await asyncio.wait_for(process.wait(), 5)
                    joined = True
                except BaseException as error:
                    failures.append(error)
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    except BaseException as error:
                        failures.append(error)
                    try:
                        await asyncio.wait_for(asyncio.shield(drain), 5)
                        await asyncio.wait_for(process.wait(), 5)
                        joined = True
                    except BaseException as error:
                        failures.append(error)
            if creation is not None:
                try:
                    await creation
                except BaseException as error:
                    failures.append(error)
                if joined:
                    try:
                        await asyncio.wait_for(_drop_database(admin_dsn, name), 10)
                    except BaseException as error:
                        failures.append(error)
            if failures and primary_failure is None:
                raise AssertionError("PostgreSQL qualification cleanup failed") from None

        try:
            await _finish(cleanup())
        except BaseException:
            if primary_failure is None:
                raise
