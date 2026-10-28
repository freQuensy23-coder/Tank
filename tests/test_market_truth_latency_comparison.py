import asyncio
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tarfile
from contextlib import AsyncExitStack
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import pytest
from postgres_helpers import _finish

pytestmark = [pytest.mark.integration, pytest.mark.postgres, pytest.mark.process]

ORIGINAL = "ff6512a4ac2107319effc8e7745ee898a47c6212"
ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "market-truth-latency-full"
QUALIFICATION = "PUBLIC_CURRENT_FINAL_RECONSTRUCTION_WARM_NATIVE_DAY_COUNTS_COLD_DENSE_AND_ANALYSIS"
ORDER = (("original", "cold"), ("candidate", "cold"), ("candidate", "warm"), ("original", "warm"))


def git_bytes(*arguments):
    return subprocess.check_output(["git", *arguments], cwd=ROOT, timeout=20)


def file_digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def archive_source(revision, destination):
    archive = git_bytes(
        "archive", "--format=tar", revision, "src/tank", "config", "pyproject.toml", "uv.lock"
    )
    destination.mkdir()
    with tarfile.open(fileobj=io.BytesIO(archive)) as source:
        source.extractall(destination, filter="data")
    manifest = {
        str(path.relative_to(destination)): file_digest(path)
        for path in sorted(destination.rglob("*"))
        if path.is_file()
    }
    return {
        "commit": revision,
        "git_tree": git_bytes("rev-parse", revision + "^{tree}").decode().strip(),
        "package_git_tree": git_bytes("rev-parse", revision + ":src/tank").decode().strip(),
        "archive_sha256": hashlib.sha256(archive).hexdigest(),
        "files_sha256": hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest(),
        "files": manifest,
    }


def retained_policy():
    return {
        "manifest_sha256": file_digest(FIXTURES / "manifest.json"),
        "privacy_sha256": file_digest(FIXTURES / "privacy.json"),
        "loader_sha256": file_digest(ROOT / "tests" / "market_truth_full_inputs.py"),
    }


PUBLIC_FILE_LIMIT = 8_000_000
PUBLIC_TOTAL_LIMIT = 64_000_000
FORBIDDEN_KEY = re.compile(
    r"(?:password|passwd|secret|api_key|access_token|refresh_token|authorization|cookie|"
    r"environment|config|headers|credentials|private_key|database_url|dsn|telegram_id|chat_id|user_id|"
    r"email|phone|stderr|application_log)"
    r"|.*(?:_password|_secret|_api_key|_access_token|_dsn)$",
    re.IGNORECASE,
)
FORBIDDEN_VALUE = re.compile(
    r"://|(?<![A-Z0-9._%+-])[A-Z0-9._%+-]{1,128}@[A-Z0-9.-]{1,253}\.[A-Z]{2,}|-----BEGIN|\bBearer\s|"
    r"/(?:home|srv|tmp|root|run)/|\b(?:password|passwd|api_key|access_token|authorization)\s*[\"=:]",
    re.IGNORECASE,
)


def validate_public(value):
    remaining = 300_000

    def visit(item, depth):
        nonlocal remaining
        remaining -= 1
        if remaining < 0 or depth > 48:
            raise ValueError("Public evidence structure exceeds its bound")
        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str) or FORBIDDEN_KEY.fullmatch(key):
                    raise ValueError("Forbidden public evidence key")
                visit(key, depth + 1)
                visit(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                visit(child, depth + 1)
        elif isinstance(item, str):
            if len(item) > PUBLIC_FILE_LIMIT or FORBIDDEN_VALUE.search(item):
                raise ValueError("Forbidden public evidence value")
            if item.lstrip().startswith(("{", "[")):
                try:
                    embedded = json.loads(item)
                except ValueError:
                    pass
                else:
                    visit(embedded, depth + 1)
        elif item is not None and type(item) not in (int, float, bool):
            raise ValueError("Unsupported public evidence type")

    visit(value, 0)


def public_bytes(value):
    validate_public(value)
    payload = json.dumps(value, sort_keys=True, allow_nan=False).encode()
    if len(payload) > PUBLIC_FILE_LIMIT:
        raise ValueError("Public evidence file exceeds its bound")
    return payload


def read_public_file(source):
    if source.is_symlink() or source.parent.is_symlink() or source.stat().st_size > PUBLIC_FILE_LIMIT:
        raise ValueError("Unbounded or indirect public evidence source")
    with source.open("rb") as stream:
        raw = stream.read(PUBLIC_FILE_LIMIT + 1)
    if len(raw) > PUBLIC_FILE_LIMIT:
        raise ValueError("Public evidence source exceeds its bound")
    value = json.loads(raw)
    public_bytes(value)
    return raw, value


def export_evidence(workspace, destination, identities, provenance, lifecycles):
    payloads, inventory = {}, {}
    for name in ("original", "candidate"):
        allowed = ["ownership.json"] + [
            f"{state}-{kind}.json" for state in ("cold", "warm") for kind in ("result", "projection", "http")
        ]
        for filename in allowed:
            source = workspace / (name + "-run") / filename
            if not source.exists():
                continue
            raw, value = read_public_file(source)
            output = name + "-" + filename
            payloads[output] = public_bytes(value)
            if sum(map(len, payloads.values())) > PUBLIC_TOTAL_LIMIT:
                raise ValueError("Public evidence bundle exceeds its bound")
            inventory[output] = {
                "source_bytes_sha256": hashlib.sha256(raw).hexdigest(),
                "export_sha256": hashlib.sha256(payloads[output]).hexdigest(),
                "bytes": len(payloads[output]),
            }
    payloads["receipts.json"] = public_bytes(lifecycles)
    inventory["receipts.json"] = {
        "export_sha256": hashlib.sha256(payloads["receipts.json"]).hexdigest(),
        "bytes": len(payloads["receipts.json"]),
    }
    payloads["manifest.json"] = public_bytes(
        {
            "identities": identities,
            "sources": provenance,
            "files": inventory,
            "qualification": QUALIFICATION,
            "export_policy": "Only validated derived results, projections, HTTP JSON, ownership and receipts; no raw stderr, environment, config or application logs",
            "trace_limit": "Phase/query receipts retained; raw application telemetry deliberately not exported",
        }
    )
    if sum(map(len, payloads.values())) > PUBLIC_TOTAL_LIMIT:
        raise ValueError("Public evidence bundle exceeds its bound")
    destination.mkdir(parents=True, exist_ok=False)
    for filename, payload in payloads.items():
        (destination / filename).write_bytes(payload)
    return {"files": len(payloads), "manifest_sha256": file_digest(destination / "manifest.json")}


def retain_message(message, lifecycle):
    payload = public_bytes(message)
    total = lifecycle.get("receipt_bytes", 0) + len(payload)
    if len(lifecycle["receipts"]) >= 32 or total > PUBLIC_FILE_LIMIT:
        raise ValueError("Worker receipts exceed their bound")
    lifecycle["receipt_bytes"] = total
    lifecycle["receipts"].append(message)
    if message.get("kind") in {"ownership", "shutdown"}:
        lifecycle["ownership"] = message


async def worker_message(process, lifecycle):
    assert process.stdout is not None
    while line := await process.stdout.readline():
        message = json.loads(line)
        retain_message(message, lifecycle)
        if message["kind"] != "ownership":
            return message
    raise AssertionError("Latency worker exited without its expected safe receipt")


async def close_worker(process, lifecycle):
    primary = sys.exception()

    async def drain():
        assert process.stdout is not None
        while line := await process.stdout.readline():
            retain_message(json.loads(line), lifecycle)

    async def shutdown():
        draining = asyncio.create_task(drain())
        try:
            if process.returncode is None:
                lifecycle["shutdown_actions"].append("CLOSE_AND_STDIN_EOF")
                try:
                    assert process.stdin is not None
                    process.stdin.write(b'{"operation":"close"}\n')
                    await process.stdin.drain()
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    if process.stdin is not None:
                        process.stdin.close()
                try:
                    async with asyncio.timeout(10):
                        await process.wait()
                except TimeoutError:
                    if process.returncode is None:
                        lifecycle["shutdown_actions"].append("SIGTERM_COOPERATIVE_LAST_RESORT")
                        process.terminate()
                        try:
                            async with asyncio.timeout(5):
                                await process.wait()
                        except TimeoutError:
                            if process.returncode is None:
                                lifecycle["shutdown_actions"].append("SIGKILL_CLEANUP_NOT_CONFIRMED")
                                process.kill()
                                await process.wait()
            await draining
        finally:
            if not draining.done():
                draining.cancel()
            await asyncio.gather(draining, return_exceptions=True)
            lifecycle["exit_code"] = process.returncode
            lifecycle["pid"] = process.pid
            lifecycle["cleanup_confirmed"] = (
                lifecycle.get("ownership", {}).get("repository_cleanup") == "CONTEXT_EXIT_COMPLETED"
                and "SIGKILL_CLEANUP_NOT_CONFIRMED" not in lifecycle["shutdown_actions"]
            )

    try:
        await _finish(shutdown())
    except BaseException as error:
        lifecycle["shutdown_failure_type"] = type(error).__name__
        if primary is None:
            raise


async def start_worker(stack, source, run_dir, policy_path, provenance, schema_template, lifecycle):
    started = perf_counter()
    run_dir.mkdir()
    stderr = stack.enter_context((run_dir / "stderr.log").open("wb"))
    environment = dict(os.environ)
    environment.update(
        PYTHONPATH=os.pathsep.join([str(source / "src"), str(ROOT / "tests")]),
        PYTHONHASHSEED="0",
        PYTHONDONTWRITEBYTECODE="1",
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(ROOT / "tests" / "market_truth_latency_worker.py"),
        str(source),
        str(run_dir),
        str(FIXTURES),
        str(policy_path),
        provenance["commit"],
        schema_template,
        cwd=source,
        env=environment,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=stderr,
        limit=4_000_000,
    )

    lifecycle["pid"] = process.pid
    stack.push_async_callback(close_worker, process, lifecycle)
    ready = await worker_message(process, lifecycle)
    assert ready["kind"] == "ready", ready
    ready["startup_including_imports_wall_seconds"] = perf_counter() - started
    return process, ready


def compare_full_result(actual, expected, path=""):
    if isinstance(expected, dict):
        assert isinstance(actual, dict) and actual.keys() == expected.keys(), path
        for key in expected:
            compare_full_result(actual[key], expected[key], path + "/" + key)
    elif isinstance(expected, list):
        assert isinstance(actual, list) and len(actual) == len(expected), path
        for index, (left, right) in enumerate(zip(actual, expected, strict=True)):
            compare_full_result(left, right, path + "/" + str(index))
    elif isinstance(expected, float):
        assert isinstance(actual, (int, float)) and not isinstance(actual, bool), path
        assert actual == pytest.approx(expected, rel=1e-12, abs=1e-12), path
    else:
        assert type(actual) is type(expected) and actual == expected, path


async def test_original_and_candidate_full_input_market_truth_same_worker(
    tmp_path,
    record_property,
    pytestconfig,
):
    candidate = git_bytes("rev-parse", "HEAD").decode().strip()
    assert os.environ.get("TANK_TEST_DATABASE_URL"), "Owned disposable PostgreSQL route is required"
    assert os.environ.get("CLICKHOUSE_TEST_URL"), "Owned loopback ClickHouse route is required"
    schema_template = pytestconfig.getoption("postgres_schema_template")
    assert schema_template, "Permanent automatic CI sealed schema seed is required"
    for path in ("uv.lock", "src/tank/storage.py", "src/tank/market_schema.py", "src/tank/migrations"):
        assert git_bytes("rev-parse", ORIGINAL + ":" + path) == git_bytes(
            "rev-parse", candidate + ":" + path
        ), "Original package requires its own schema/dependencies if migration or lock inputs change: " + path
    policy_path = tmp_path / "public-policy.json"
    policy_path.write_text(json.dumps(retained_policy(), sort_keys=True))
    provenance = {
        name: archive_source(revision, tmp_path / name)
        for name, revision in (("original", ORIGINAL), ("candidate", candidate))
    }
    identities = {
        "candidate_commit": candidate,
        "original_commit": ORIGINAL,
        "harness_sha256": file_digest(Path(__file__)),
        "worker_sha256": file_digest(ROOT / "tests" / "market_truth_latency_worker.py"),
        "full_input_loader_sha256": file_digest(ROOT / "tests" / "market_truth_full_inputs.py"),
        "postgres_adapter_sha256": file_digest(ROOT / "tests" / "postgres_helpers.py"),
        "fixture_manifest_sha256": file_digest(FIXTURES / "manifest.json"),
        "public_policy_sha256": file_digest(policy_path),
        "python": sys.version,
        "run": {
            name: os.environ.get(name)
            for name in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "PYTEST_XDIST_WORKER")
        },
        "comparison_id": uuid4().hex,
        "completion": "INCOMPLETE_NO_LATENCY_OR_PARITY_OUTCOME",
    }
    public_bytes(identities)
    lifecycles = {name: {"receipts": [], "shutdown_actions": []} for name in provenance}
    evidence_dir = ROOT / ".test-results" / "market-truth-latency" / identities["comparison_id"]
    record_property("latency_source_provenance", json.dumps(provenance, sort_keys=True))
    record_property(
        "latency_scope",
        QUALIFICATION
        + ". Matched full-dense-path seeded request, not an exact historical reproduction. "
        "All 16 projections / 784665 current FINAL rows, including nine native-volume paths; "
        "original 85-instrument config and 81-peer counts retained. Later Coinbase FINAL lacks 453 "
        "underlying / 906 projected historical rows. Same pytest worker, owned PostgreSQL16/ClickHouse25.8; "
        "ABBA original-cold, candidate-cold, candidate-warm, original-warm is one balanced pair per state, "
        "not repeated statistical confidence. Independent package/process/DB/cache lifetimes per version. "
        "Cold dense/analysis caches, warm native 7209 whole-day aggregates with actual later saved_at, "
        "native SQL invalidation and real edges; NOT all-application/OS/server caches cold. Warm uses "
        "new event/jobs at identical anchor/cutoff and retained application caches, not stored results; "
        "zero repeated dense SQL projections is permitted. Fixture-owned COMPLETE and replay=True "
        "avoid live capture/repair waits; the later matching-anchor PARTIAL receipt is not historical COMPLETE. "
        "Session.tool -> durable worker -> snapshot -> full compute -> durable SQL -> loopback HTTP "
        "receipt -> returned audit. No actual OpenMA/model continuation, deployment, historical incident "
        "latency or automatic performance acceptance. Real funding/tickers/supply/tier/gaps are loaded; "
        "books/liquidations are confirmed empty. Genuine NULLs remain; uncaptured wide columns, original "
        "mutable streams and privacy/source_archive omissions remain explicit in the pinned manifest. "
        "Reduced subset component goldens and single-venue flow sums do not describe the expanded cohort; "
        "complete independent original-source output and snapshot comparisons replace them.",
    )
    record_property("latency_harness_sha256", file_digest(Path(__file__)))
    record_property("latency_worker_sha256", file_digest(ROOT / "tests" / "market_truth_latency_worker.py"))
    record_property("latency_version_order", json.dumps(ORDER))
    record_property(
        "latency_run_identity",
        json.dumps(
            {
                name: os.environ.get(name)
                for name in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_SHA", "PYTEST_XDIST_WORKER")
            },
            sort_keys=True,
        ),
    )
    workers, reports, setups = {}, {}, {}
    try:
        async with asyncio.timeout(180), AsyncExitStack() as stack:
            for name in ("original", "candidate"):
                process, ready = await start_worker(
                    stack,
                    tmp_path / name,
                    tmp_path / (name + "-run"),
                    policy_path,
                    provenance[name],
                    schema_template,
                    lifecycles[name],
                )
                workers[name] = process
                setups[name] = ready["setup"]
                record_property("latency_setup_" + name, json.dumps(ready, sort_keys=True))
            assert workers["original"].pid != workers["candidate"].pid
            for field in (
                "inputs",
                "registry_sha256",
                "semantics_sha256",
                "thresholds_sha256",
                "manifest_sha256",
                "loader_sha256",
                "expected_coverage",
                "expected_dense_projections",
                "native_day_counts",
            ):
                assert setups["original"][field] == setups["candidate"][field], field
            for ordinal, (name, state) in enumerate(ORDER):
                process = workers[name]
                assert process.stdin is not None
                process.stdin.write(json.dumps({"operation": state}).encode() + b"\n")
                await process.stdin.drain()
                report = await worker_message(process, lifecycles[name])
                record_property(
                    f"latency_sample_{ordinal}_{name}_{state}", json.dumps(report, sort_keys=True)
                )
                assert report["kind"] == "sample" and report["state"] == state
                assert report["pid"] == process.pid
                reports[name, state] = report
            for state in ("cold", "warm"):
                original = reports["original", state]
                revised = reports["candidate", state]
                assert original["snapshot_invariants"] == revised["snapshot_invariants"]
                assert original["decimal_semantics"] == revised["decimal_semantics"]
                assert original["series_requests"] == revised["series_requests"]
                assert original["dense_projection_rows"] == revised["dense_projection_rows"]
                assert original["dense_projection_dimensions"] == revised["dense_projection_dimensions"]
                for output in ("result", "projection"):
                    _, left = read_public_file(tmp_path / "original-run" / f"{state}-{output}.json")
                    _, right = read_public_file(tmp_path / "candidate-run" / f"{state}-{output}.json")
                    compare_full_result(right, left)
                record_property(
                    "latency_observed_ratio_" + state,
                    json.dumps(
                        {
                            "candidate_over_original_wall": revised["request"]["wall_seconds"]
                            / original["request"]["wall_seconds"],
                            "candidate_over_original_cpu": revised["request"]["process_cpu_seconds"]
                            / original["request"]["process_cpu_seconds"],
                            "samples_per_version_per_state": 1,
                            "interpretation": "Balanced descriptive observation; not a significance test or speedup gate",
                            "full_result_and_projection": "MATCH_ALL_FIELDS_EXCEPT_ROOT_MTA_AND_CODE_IDENTITY",
                            "float_tolerance": {"relative": 1e-12, "absolute": 1e-12},
                            "decimal_strings_nulls_precision_and_reasons": "EXACT",
                        },
                        sort_keys=True,
                    ),
                )
        for name in ("original", "candidate"):
            assert reports[name, "cold"]["series_requests"] == reports[name, "warm"]["series_requests"]
        assert all(process.returncode == 0 for process in workers.values()), "Latency worker cleanup failed"
        assert all(item.get("cleanup_confirmed") for item in lifecycles.values()), (
            "Owned cleanup not confirmed"
        )
        identities["completion"] = (
            "MATCHED_FULL_DENSE_PATH_RECONSTRUCTION_COMPARISON_COMPLETED_NOT_PERFORMANCE_ACCEPTANCE"
        )
    except TimeoutError:
        record_property(
            "latency_timeout",
            json.dumps(
                {
                    "origin": "parent_harness",
                    "limit_seconds": 180,
                    "completion": "INCOMPLETE_NO_LATENCY_OR_PARITY_OUTCOME",
                }
            ),
        )
        raise
    finally:
        primary = sys.exception()
        unchanged = {}
        for name in provenance:
            source = tmp_path / name
            observed = {
                str(path.relative_to(source)): file_digest(path)
                for path in sorted(source.rglob("*"))
                if path.is_file()
            }
            unchanged[name] = observed == provenance[name]["files"]
        identities["source_archives_unchanged"] = unchanged
        if not all(unchanged.values()):
            identities["completion"] = "INCOMPLETE_NO_LATENCY_OR_PARITY_OUTCOME"
        for name, lifecycle in lifecycles.items():
            record_property("latency_worker_lifecycle_" + name, json.dumps(lifecycle, sort_keys=True))
        try:
            exported = export_evidence(tmp_path, evidence_dir, identities, provenance, lifecycles)
        except Exception as error:
            record_property(
                "latency_public_export", json.dumps({"status": "REFUSED", "type": type(error).__name__})
            )
            if primary is None:
                raise
        else:
            record_property(
                "latency_public_export", json.dumps({"path": str(evidence_dir.relative_to(ROOT)), **exported})
            )
        for name, process in workers.items():
            record_property(
                "latency_worker_exit_" + name, json.dumps({"pid": process.pid, "code": process.returncode})
            )
        if primary is None:
            assert all(unchanged.values()), "Archived source/config was changed during qualification"
