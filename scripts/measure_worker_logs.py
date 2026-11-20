import asyncio
import json
import secrets
import subprocess
import sys
import time
from collections import Counter
from contextlib import contextmanager, redirect_stderr
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import yaml

from tank.logging import configure_logging
from tank.runtime import Runtime
from tank.storage import PostgresRepository
from tank.util import utcnow
from tank.worker_time_report import events, instant, report

IMAGE = "postgres:16-bookworm"
DOCKER = ["docker", "--host", "unix:///var/run/docker.sock"]
JOBS = 2796
SECONDS = 300


def docker(*args, **kwargs):
    return subprocess.check_output([*DOCKER, *args], **kwargs)


@contextmanager
def container(*args):
    name = f"tank-worker-log-test-{uuid4().hex}"
    docker("create", "--name", name, *args)
    try:
        yield name
    finally:
        docker("rm", "-fv", name)


async def workload(repo):
    runtime = object.__new__(Runtime)
    runtime.repo = repo

    async def handle(payload):
        await asyncio.sleep(0.04)

    class Sessions:
        admit = staticmethod(handle)

    runtime.sessions = Sessions()
    runtime.handlers = dict(SCOUT_REVIEW=handle, DELIVER=handle)
    expected = set()
    stop = asyncio.Event()
    clock = asyncio.get_running_loop().time
    origin = clock()
    start = utcnow()
    async with asyncio.TaskGroup() as tasks:
        for w in range(8):
            tasks.create_task(runtime.worker(stop, w))
        for wave in range(60):
            await asyncio.sleep(origin + wave * 5 - clock())
            for i in range(wave * JOBS // 60, (wave + 1) * JOBS // 60):
                kind = "DELIVER" if i % 2 else "SCOUT_REVIEW"
                job = await repo.enqueue(kind, {}, str(i), run_after=utcnow() - timedelta(seconds=30))
                expected.add(job["job_id"])
        await asyncio.sleep(origin + SECONDS - clock())
        stop.set()
    return start, expected


def capture(stream):
    password = secrets.token_hex(16)
    with container(
        "-p", "127.0.0.1::5432", "-e", f"POSTGRES_PASSWORD={password}", "-e", "POSTGRES_DB=tank_test", IMAGE
    ) as pg:
        docker("start", pg)
        port = docker("port", pg, "5432/tcp", text=True).strip().split(":")[-1]
        for _ in range(60):
            if (
                subprocess.run(
                    [*DOCKER, "exec", pg, "pg_isready", "-h", "127.0.0.1"], capture_output=True
                ).returncode
                == 0
            ):
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("local PostgreSQL did not start")

        async def run():
            repo = PostgresRepository(f"postgresql://postgres:{password}@127.0.0.1:{port}/tank_test")
            await repo.open()
            try:
                await repo.migrate()
                return await workload(repo)
            finally:
                await repo.close()

        with redirect_stderr(stream):
            configure_logging()
            return asyncio.run(run())


def log_sink(options):
    opts = [v for k, value in options.items() for v in ("--log-opt", f"{k}={value}")]
    return container(
        "-i", "--network", "none", "--log-driver", "json-file", *opts, IMAGE, "sh", "-c", "cat >&2"
    )


def main():
    config = yaml.safe_load((Path(__file__).resolve().parents[1] / "compose.yaml").read_text())
    logging = config["services"]["tank"]["logging"]
    assert logging["driver"] == "json-file"
    options = logging["options"]
    with log_sink(options) as source:
        with subprocess.Popen(
            [*DOCKER, "start", "-ai", source],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        ) as sink:
            assert sink.stdin is not None
            with sink.stdin:
                start, expected = capture(sink.stdin)
        assert sink.returncode == 0
        exported = docker("logs", source, stderr=subprocess.STDOUT)
        log_path = docker("inspect", "--format", "{{.LogPath}}", source, text=True).strip()
        raw = docker(
            "run",
            "--rm",
            "--network",
            "none",
            "--log-driver",
            "none",
            "-v",
            f"{log_path}:/log:ro",
            IMAGE,
            "cat",
            "/log",
        )
    end = start + timedelta(seconds=SECONDS)
    sizes = Counter()
    counts = Counter()
    messages = bytearray()
    for line in raw.splitlines(keepends=True):
        envelope = json.loads(line)
        assert envelope["stream"] == "stderr"
        messages.extend(envelope["log"].encode())
        category = "accounting" if " INFO tank.worker_time worker_time " in envelope["log"] else "other"
        sizes[category] += len(line)
        counts[category] += 1
    assert messages == exported
    records = list(events(exported.decode().splitlines()))
    result = report(records, start, end)
    attempts = result["attempts"]
    assert len(expected) == len(attempts) == JOBS
    assert {a["job_id"] for a in attempts} == expected
    starts = {r["acquired"]["job_id"]: r["at"] for r in records if r["event"] == "claim_start"}
    ends = {r["active"]["job_id"]: r["at"] for r in records if r["event"] == "claim_end"}
    assert all((a["start"], a["end"]) == (starts[a["job_id"]], ends[a["job_id"]]) for a in attempts)
    assert all(
        a["outcome"] == "completed" and instant(a["start"]) < instant(a["end"]) < end for a in attempts
    )
    for worker in result["workers"].values():
        totals = worker["total_us"]
        assert sum(totals.values()) == SECONDS * 10**6
        assert not any(span["phase"] == "unknown" for span in worker["spans"][1:])
        assert all(
            totals[k] > 0
            for k in ("prepare", "handler", "admission", "settle", "queue_select", "reconcile", "empty_wait")
        )
    assert sum(r["event"] == "sample" for r in records) >= 8 * 9
    assert counts["other"] >= JOBS // 2
    assert sizes["accounting"] <= 4_500_000 and sizes["other"] <= 1_000_000, sizes
    assert options["max-size"] == "20m" and (int(options["max-file"]) - 1) * 20_000_000 >= 24 * 5_500_000
    with log_sink({"max-size": "256k", "max-file": "3"}) as source:
        subprocess.run(
            [*DOCKER, "start", "-ai", source],
            input=exported,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
        suffix = docker("logs", source, stderr=subprocess.STDOUT)
        assert len(suffix) < len(exported) and exported.endswith(suffix)
        retained = list(events(suffix.decode().splitlines()))
    cutoff = max(instant(next(r for r in retained if r["worker"] == w)["at"]) for w in range(8))
    assert cutoff < end and report(retained, cutoff, end) == report(records, cutoff, end)
    partial = json.loads(
        subprocess.check_output(
            [sys.executable, "-m", "tank.worker_time_report", start.isoformat(), end.isoformat()],
            input=suffix,
        )
    )
    assert partial["pool_us"]["unknown"] > result["pool_us"]["unknown"]
    assert len(partial["attempts"]) < JOBS
    print(
        json.dumps(
            dict(
                jobs=JOBS,
                records=counts,
                docker_bytes=sizes,
                formatted_bytes=len(exported),
                samples=sum(r["event"] == "sample" for r in records),
                empty_wait_spans=sum(
                    s["phase"] == "empty_wait" for w in result["workers"].values() for s in w["spans"]
                ),
                retained_jobs=len(partial["attempts"]),
                phase_us=result["pool_us"],
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
