import ast
import asyncio
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
from contextlib import AsyncExitStack
from importlib.machinery import EXTENSION_SUFFIXES
from pathlib import Path

import pytest
import pytest_asyncio
from market_truth_request_data import DATA_REVISION, ORIGINAL, stage_data

pytestmark = [pytest.mark.integration, pytest.mark.postgres, pytest.mark.process]
ROOT = Path(__file__).parents[1]


def repository_contract(path):
    source = ast.parse(path.read_text())
    nodes = []
    for node in source.body:
        if isinstance(node, ast.ClassDef):
            if node.name == "PostgresRepository":
                for member in node.body:
                    if isinstance(member, ast.AsyncFunctionDef) and member.name == "put":
                        member.body = [ast.Pass()]
            nodes.append(node)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name != "_same_json_value":
            nodes.append(node)
    return ast.dump(ast.Module(body=nodes, type_ignores=[]))


def stage_native_module(checkout, archived, binary, module):
    relative = Path("src", *module.split(".")).with_suffix(".py")
    assert (checkout / relative).read_bytes() == (archived / relative).read_bytes()
    assert (checkout / "pyproject.toml").read_bytes() == (archived / "pyproject.toml").read_bytes()
    assert binary.is_file() and not binary.is_symlink()
    assert binary.resolve().is_relative_to(checkout.resolve())
    assert any(binary.name == relative.stem + suffix for suffix in EXTENSION_SUFFIXES)
    destination = archived / relative.with_name(binary.name)
    assert destination.parent.resolve().is_relative_to(archived.resolve())
    assert not destination.exists() and not destination.is_symlink()
    shutil.copyfile(binary, destination)
    digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    assert hashlib.sha256(destination.read_bytes()).hexdigest() == digest
    return {"path": str(destination.relative_to(archived)), "sha256": digest}


def archive(revision, destination, paths):
    destination.mkdir()
    with subprocess.Popen(["git", "archive", revision, *paths], cwd=ROOT, stdout=subprocess.PIPE) as process:
        with tarfile.open(fileobj=process.stdout, mode="r|") as source:
            source.extractall(destination, filter="data")
        assert process.wait() == 0


async def stop(process):
    if process.returncode is None:
        process.terminate()
        try:
            async with asyncio.timeout(10):
                await process.wait()
        except TimeoutError:
            process.kill()
            await process.wait()


async def receive(process):
    async with asyncio.timeout(110):
        line = await process.stdout.readline()
    assert line, "Isolated request worker exited without a receipt"
    receipt = json.loads(line)
    assert "failure" not in receipt, receipt
    return receipt


async def send(process, value):
    process.stdin.write(json.dumps(value).encode() + b"\n")
    await process.stdin.drain()
    return await receive(process)


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def full_request_comparison(tmp_path_factory, pytestconfig, request):
    from postgres_helpers import PostgresTemplate
    from psycopg import AsyncConnection
    from psycopg.conninfo import conninfo_to_dict

    from tank.clickhouse_market import ClickHouse

    tmp_path = tmp_path_factory.mktemp("market-truth-request")
    evidence_case = next(item for item in request.session.items if item.path == request.path)

    def record_property(name, value):
        evidence_case.user_properties.append((name, value))

    assert os.environ.get("TANK_TEST_DATABASE_URL"), "Disposable PostgreSQL is required"
    assert os.environ.get("CLICKHOUSE_TEST_URL"), "Disposable ClickHouse is required"
    candidate = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    fixtures = tmp_path / "inputs"
    archive(candidate, fixtures, ["tests/fixtures/market-truth-latency-full"])
    fixtures = fixtures / "tests/fixtures/market-truth-latency-full"
    manifest = stage_data(fixtures)
    template = PostgresTemplate(schema_template=pytestconfig.getoption("postgres_schema_template"))
    workers, repositories, receipts, compiled = {}, [], {}, {}
    try:
        async with AsyncExitStack() as resources:
            for label, revision in (("original", ORIGINAL), ("candidate", candidate)):
                root = tmp_path / label
                archive(revision, root, ["src", "config", "uv.lock", "pyproject.toml"])
                if label == "candidate":
                    for filename in ("uv.lock", "src/tank/market_schema.py", "src/tank/models.py"):
                        assert (root / filename).read_bytes() == (
                            tmp_path / "original" / filename
                        ).read_bytes()
                    assert repository_contract(root / "src/tank/storage.py") == repository_contract(
                        tmp_path / "original/src/tank/storage.py"
                    )
                    from tank.analysis import truth

                    compiled[label] = stage_native_module(
                        ROOT, root, Path(truth.__file__), "tank.analysis.truth"
                    )
                repo = await resources.enter_async_context(
                    template.repository(os.environ["TANK_TEST_DATABASE_URL"])
                )
                repositories.append(repo)
                directory = root / "request-output"
                directory.mkdir()
                environment = dict(
                    os.environ,
                    PYTHONPATH=os.pathsep.join((str(root / "src"), str(ROOT / "tests"))),
                    PYTHONHASHSEED="0",
                    PYTHONDONTWRITEBYTECODE="1",
                )
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    str(ROOT / "tests/market_truth_request_worker.py"),
                    str(root),
                    str(directory),
                    str(fixtures),
                    revision,
                    cwd=root,
                    env=environment,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                workers[label] = process
                resources.push_async_callback(stop, process)
                process.stdin.write(
                    json.dumps(
                        {
                            "database_url": repo.database_url,
                            "clickhouse_database": repo.clickhouse.database,
                            "input_config_dir": str(tmp_path / "original" / "config"),
                        }
                    ).encode()
                    + b"\n"
                )
                await process.stdin.drain()
            for label, process in workers.items():
                receipts[label] = await receive(process)
                assert receipts[label]["kind"] == "ready"
                record_property(label + "_setup", json.dumps(receipts[label], sort_keys=True))
            assert repositories[0].database_url != repositories[1].database_url
            assert repositories[0].clickhouse.database != repositories[1].clickhouse.database
            samples = {}
            try:
                for label, state in (
                    ("original", "cold"),
                    ("candidate", "cold"),
                    ("candidate", "warm"),
                    ("original", "warm"),
                ):
                    result = await send(workers[label], {"sample": state})
                    samples[label, state] = result
                    record_property(label + "_" + state, json.dumps(result, sort_keys=True))
                assert receipts["original"]["native"] == receipts["candidate"]["native"]
                diagnostic = await send(workers["candidate"], {"diagnose": True})
                assert diagnostic["kind"] == "post_measurement_profile"
                assert diagnostic["cold_snapshot"] and diagnostic["snapshot"]
                record_property("post_measurement_profile", json.dumps(diagnostic, sort_keys=True))
                evidence = ROOT / ".test-results/market-truth-request.json"
                evidence.parent.mkdir(exist_ok=True)
                evidence.write_text(
                    json.dumps(
                        {
                            "source_heads": {"original": ORIGINAL, "candidate": candidate},
                            "compiled_artifacts": compiled,
                            "post_measurement_profile": diagnostic,
                            "samples": {
                                label + "_" + state: value for (label, state), value in samples.items()
                            },
                            "manifest_sha256": hashlib.sha256(
                                (fixtures / "manifest.json").read_bytes()
                            ).hexdigest(),
                            "qualification": manifest["qualification"],
                            "limitations": manifest["limitations"],
                        },
                        sort_keys=True,
                    )
                )
                (tmp_path / "comparison.json").write_text(evidence.read_text())
                record_property("qualification", manifest["qualification"])
                record_property("dataset_revision", DATA_REVISION)
                record_property("limitations", json.dumps(manifest["limitations"]))
                record_property(
                    "comparison",
                    "ABBA complete; six cold/warm result, projection and snapshot cases enforce exact parity",
                )
                record_property(
                    "candidate_minus_original_seconds",
                    json.dumps(
                        {
                            state: samples["candidate", state]["wall_seconds"]
                            - samples["original", state]["wall_seconds"]
                            for state in ("cold", "warm")
                        }
                    ),
                )
            finally:
                failed = sys.exception() is not None
                for process in workers.values():
                    if process.returncode is None:
                        await send(process, {"stop": True})
                        async with asyncio.timeout(10):
                            await process.wait()
                    if not failed:
                        assert process.returncode == 0
    finally:
        for process in workers.values():
            await stop(process)
        await template.close()
    async with await AsyncConnection.connect(os.environ["TANK_TEST_DATABASE_URL"], autocommit=True) as admin:
        for repo in repositories:
            name = conninfo_to_dict(repo.database_url)["dbname"]
            assert (
                await (await admin.execute("SELECT 1 FROM pg_database WHERE datname=%s", (name,))).fetchone()
                is None
            )
    check = ClickHouse(os.environ["CLICKHOUSE_TEST_URL"])
    try:
        for repo in repositories:
            assert (
                await check.rows(
                    "SELECT name FROM system.databases WHERE name={name:String}",
                    {"name": (repo.clickhouse.database, "String")},
                )
                == []
            )
    finally:
        await check.close()
    record_property("source_heads", json.dumps({"original": ORIGINAL, "candidate": candidate}))
    record_property("manifest_sha256", hashlib.sha256((fixtures / "manifest.json").read_bytes()).hexdigest())
    return tmp_path


def first_difference(original, candidate, path="$"):
    if original == candidate:
        return None
    if isinstance(original, dict) and isinstance(candidate, dict):
        if original.keys() != candidate.keys():
            return {"path": path, "keys": [sorted(original), sorted(candidate)]}
        for key in original:
            found = first_difference(original[key], candidate[key], path + "/" + key)
            if found is not None:
                return found
    elif isinstance(original, list) and isinstance(candidate, list):
        if len(original) != len(candidate):
            return {"path": path, "lengths": [len(original), len(candidate)]}
        for index, (left, right) in enumerate(zip(original, candidate, strict=True)):
            found = first_difference(left, right, path + "/" + str(index))
            if found is not None:
                return found
    return {"path": path, "original": repr(original)[:160], "candidate": repr(candidate)[:160]}


@pytest.mark.parametrize("state", ["cold", "warm"])
@pytest.mark.parametrize("contract", ["result", "projection", "snapshot"])
def test_full_market_truth_original_candidate_same_worker(
    full_request_comparison, state, contract, record_property
):
    directory = full_request_comparison
    left = json.loads((directory / "original/request-output" / f"{state}-{contract}.json").read_bytes())
    right = json.loads((directory / "candidate/request-output" / f"{state}-{contract}.json").read_bytes())
    mismatch = first_difference(left, right)
    record_property("contract", state + "/" + contract)
    record_property("first_difference", json.dumps(mismatch))
    assert mismatch is None, (state, contract, mismatch)


@pytest.mark.parametrize("state", ["cold", "warm"])
def test_full_market_truth_under_four_seconds(full_request_comparison, state, record_property):
    evidence = json.loads((full_request_comparison / "comparison.json").read_text())
    sample = evidence["samples"]["candidate_" + state]
    record_property("full_request_seconds", sample["wall_seconds"])
    record_property("compute_seconds", sample["compute_seconds"])
    assert 0 < sample["compute_seconds"] < 4.0, sample
    assert 0 < sample["wall_seconds"] < 4.0, sample
