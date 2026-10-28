import asyncio
import ast
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
from contextlib import AsyncExitStack
from pathlib import Path
from time import perf_counter

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.postgres, pytest.mark.process]

ORIGINAL = "ff6512a4ac2107319effc8e7745ee898a47c6212"
ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "market-truth-latency"
ORDER = (("original", "cold"), ("candidate", "cold"), ("candidate", "warm"), ("original", "warm"))


def git_bytes(*arguments):
    return subprocess.check_output(["git", *arguments], cwd=ROOT, timeout=20)


def file_digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def archive_source(revision, destination):
    archive = git_bytes("archive", "--format=tar", revision, "src/tank", "config", "pyproject.toml", "uv.lock")
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
    source = ROOT / "tests" / "test_market_truth_incident_profile.py"
    assignments = {
        node.targets[0].id: node.value
        for node in ast.parse(source.read_text()).body
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
    }
    fields = {
        name: sorted(ast.literal_eval(assignments[name]))
        for name in ("PRICE_FIELDS", "FLOW_FIELDS", "OI_FIELDS")
    }
    nested = assignments["NESTED_FIELDS"]
    assert isinstance(nested, ast.BinOp)
    fields["NESTED_FIELDS"] = sorted(set(fields["FLOW_FIELDS"]) | ast.literal_eval(nested.right))
    return {
        **fields,
        "ARCHIVE_HASHES": ast.literal_eval(assignments["ARCHIVE_HASHES"]),
        "policy_source_sha256": file_digest(source),
    }


async def worker_message(process):
    assert process.stdout is not None
    line = await process.stdout.readline()
    assert line, "Latency worker exited without its expected receipt; inspect worker_exit JUnit property"
    return json.loads(line)


async def start_worker(stack, source, run_dir, policy_path, provenance):
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
        cwd=source,
        env=environment,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=stderr,
        limit=4_000_000,
    )

    async def close():
        if process.returncode is None:
            try:
                assert process.stdin is not None
                process.stdin.write(b'{"operation":"close"}\n')
                await process.stdin.drain()
                async with asyncio.timeout(10):
                    await process.wait()
            except (TimeoutError, BrokenPipeError, ConnectionResetError):
                if process.returncode is None:
                    process.terminate()
                    try:
                        async with asyncio.timeout(5):
                            await process.wait()
                    except TimeoutError:
                        process.kill()
                        await process.wait()

    stack.push_async_callback(close)
    ready = await worker_message(process)
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


async def test_original_and_candidate_reduced_input_market_truth_same_worker(tmp_path, record_property):
    candidate = git_bytes("rev-parse", "HEAD").decode().strip()
    assert os.environ.get("TANK_TEST_DATABASE_URL"), "Owned disposable PostgreSQL route is required"
    assert os.environ.get("CLICKHOUSE_TEST_URL"), "Owned loopback ClickHouse route is required"
    assert os.environ.get("TANK_CI_SCHEMA_TEMPLATE"), "Permanent automatic CI sealed schema seed is required"
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
    record_property("latency_source_provenance", json.dumps(provenance, sort_keys=True))
    record_property(
        "latency_scope",
        "REDUCED_INPUT_DIAGNOSTIC_NOT_FULL_ENA_ACCEPTANCE. Complete code path, incomplete ENA inputs. "
        "Same pytest worker and owned PostgreSQL16/ClickHouse25.8 services; ABBA original-cold, "
        "candidate-cold, candidate-warm, original-warm. Independent long-lived subprocess/package/DB/cache "
        "per version. Cold means fresh dense/analysis caches, NOT cold OS/server caches. Native day-count "
        "cache state is separately recorded; a native warm-count fixture is NOT all-application-caches cold. "
        "Warm is a new "
        "event/job at identical anchor/cutoff with original application caches retained, NOT a stored-result "
        "replay. Seeded COMPLETE capture plus replay=True: repair returns normally without contacting a "
        "collector. Full Session.tool -> durable worker -> snapshot -> compute_market_truth -> SQL result "
        "-> loopback HTTP receipt -> returned audit. No live capture, repair wait, actual OpenMA/model "
        "continuation, historical incident latency, deployment, or statistical speedup acceptance. "
        "Retained price projections omit OHLC/volume; NULL stays NULL. Three explicitly synthetic "
        "next-minute exclusion controls are NOT captured historical data. "
        "Funding/books/supply/liquidations and other venue histories are absent.",
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
                    stack, tmp_path / name, tmp_path / (name + "-run"), policy_path, provenance[name]
                )
                workers[name] = process
                setups[name] = ready["setup"]
                record_property("latency_setup_" + name, json.dumps(ready, sort_keys=True))
            assert workers["original"].pid != workers["candidate"].pid
            for field in ("inputs", "registry_sha256", "semantics_sha256", "thresholds_sha256"):
                assert setups["original"][field] == setups["candidate"][field], field
            for ordinal, (name, state) in enumerate(ORDER):
                process = workers[name]
                assert process.stdin is not None
                process.stdin.write(json.dumps({"operation": state}).encode() + b"\n")
                await process.stdin.drain()
                report = await worker_message(process)
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
                for output in ("result", "projection"):
                    left = json.loads((tmp_path / "original-run" / f"{state}-{output}.json").read_text())
                    right = json.loads((tmp_path / "candidate-run" / f"{state}-{output}.json").read_text())
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
        assert all(process.returncode == 0 for process in workers.values()), "Latency worker cleanup failed"
    finally:
        for name, process in workers.items():
            record_property(
                "latency_worker_exit_" + name, json.dumps({"pid": process.pid, "code": process.returncode})
            )
        for name in provenance:
            source = tmp_path / name
            observed = {
                str(path.relative_to(source)): file_digest(path)
                for path in sorted(source.rglob("*"))
                if path.is_file()
            }
            assert observed == provenance[name]["files"], "Archived source/config was changed during qualification"
