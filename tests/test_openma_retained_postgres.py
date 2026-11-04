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
        raise ValueError("PostgreSQL qualification requires an explicit numeric loopback test target") from None
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
            diagnostic = {"stage": "UNKNOWN", "reason": "UNKNOWN"}
            stages = {
                "imports", "input", "connection", "migration", "fixture", "original-history",
                "replacement", "callback", "continuation", "execution-fences", "settlement",
                "callback-refusal", "resource-fences", "model-history", "model-error-state", "cleanup",
            }
            reasons = {
                "UNKNOWN", "assertion", "rejected-error-state", "ERR_MODULE_NOT_FOUND", "MODULE_NOT_FOUND",
                "ERR_PACKAGE_PATH_NOT_EXPORTED", "ERR_UNKNOWN_FILE_EXTENSION", "ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX",
            }
            try:
                evidence = json.loads(stdout) if len(stdout) <= 4096 else None
            except (ValueError, UnicodeError):
                evidence = None
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
                diagnostic = {"stage": evidence["stage"], "reason": evidence["reason"]}
            else:
                for code in (
                    "ERR_MODULE_NOT_FOUND", "MODULE_NOT_FOUND", "ERR_PACKAGE_PATH_NOT_EXPORTED",
                    "ERR_UNKNOWN_FILE_EXTENSION", "ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX",
                ):
                    if f"code: '{code}'".encode() in stderr or f"Error [{code}]".encode() in stderr:
                        diagnostic = {"stage": "node-startup", "reason": code}
                        break
            record_property("openma_postgres_failure", json.dumps(diagnostic, sort_keys=True))
            raise AssertionError(
                f"PostgreSQL source qualification child failed: STAGE={diagnostic['stage']} REASON={diagnostic['reason']}"
            )
        try:
            receipt = json.loads(stdout)
        except (ValueError, UnicodeError):
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
