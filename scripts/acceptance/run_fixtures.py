"""Native fixture acceptance runs (03 §11.1, 01 V0-C/V0-D) through the single deployed openma.

One fixture at a time:
  1. verify the packet / private map / sealed outcomes against their pinned sha256 (entries JSON from
     the assembler, or tests/fixtures/manifest.json);
  2. reset the replay database `tank_replay` with tank-sessions' reset script (drop, create, migrate,
     read-only role, provisioning, emptiness check);
  3. copy the decompressed packet to /data/replay/inputs/F-xx.json and the private map to a path
     outside /data (TANK_REPLAY_PRIVATE_MASK_PATH); sealed outcomes never enter the container;
  4. `tank replay F-xx` in its own `compose run` process with the fixture config tree at /config: a fresh tank-replay session;
  5. copy the frozen outputs.json + attempt log out, write acceptance-run.json (started_at, frozen_output),
     and only then evaluate the sealed predicates (tests/fixture_acceptance.check_completed).
Manual clauses get a reviewer-signed manual-review.json afterwards (`review` step), which writes
acceptance-run-with-manual.json for the manifest.

The replay DSN is built inside the container by tank-sessions' helper from the mounted secret (@REPLAY_DSN@);
it is never printed or passed on argv.

Live traffic has priority (lead rule): each fixture waits until no live AGENT_MESSAGE/FILTER/ROUTE job is
queued or running, and its window is appended to /srv/tank/data/qa/fixture-windows.jsonl so latency can be
read with and without fixture load (latency.py --fixture-windows).

Usage (never in parallel):
  python3 run_fixtures.py run --entries entries.json --fixtures F-17 --out /srv/tank/docs/verification/native-fixtures-2026-10-21
  python3 run_fixtures.py review --out DIR --fixture F-17 --review manual-review.json
"""

import argparse
import hashlib
import json
import os
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import iso, utcnow, write_json

ROOT = Path("/srv/tank")
DEPLOYED = Path("/srv/tank-deploy/deployed")
if DEPLOYED.is_file():
    os.environ.setdefault("TANK_VERSION", DEPLOYED.read_text().strip())
COMPOSE = shlex.split(
    os.environ.get(
        "TANK_COMPOSE",
        "docker compose -p tank --project-directory /srv/tank-deploy -f /srv/tank-deploy/compose.yaml",
    )
)

RESET = os.environ.get("TANK_REPLAY_RESET", "/srv/tank-work/ts/reset_tank_replay.sh")
DSN_HELPER = os.environ.get(
    "TANK_REPLAY_DSN_HELPER", str(Path(__file__).resolve().parent / "replay_dsn_exec.py")
)
PRIVATE_IN_CONTAINER = "/replay-private/map.json"


def sh(args, *, input=None, check=True, timeout=None):
    out = subprocess.run(
        args, input=input, capture_output=True, text=isinstance(input, str) or input is None, timeout=timeout
    )
    if check and out.returncode != 0:
        raise RuntimeError(f"{' '.join(args[:6])}… failed ({out.returncode}): {str(out.stderr)[-800:]}")
    return out


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def resolve(p):
    p = Path(p)
    return p if p.is_absolute() else ROOT / p


def entries(path, fixtures):
    data = json.load(open(path))
    if isinstance(data, dict) and all(isinstance(v, dict) and "manifest_entry" in v for v in data.values()):
        rows = [
            {**v["manifest_entry"], "fixture_id": k, "container": v.get("container", {})}
            for k, v in data.items()
        ]
    else:
        rows = data["fixtures"] if isinstance(data, dict) and "fixtures" in data else data
    found = {r["fixture_id"]: r for r in rows if r.get("fixture_id") in fixtures}
    missing = [f for f in fixtures if f not in found]
    if missing:
        raise SystemExit(f"no entry for {missing}")
    for f, e in found.items():
        for key in ("acceptance_input", "private_name_map", "sealed_outcomes"):
            if sha(resolve(e[key]["path"])) != e[key]["sha256"]:
                raise SystemExit(f"{f}: {key} sha256 mismatch")
    return found


def container(service):
    out = sh(COMPOSE + ["ps", "-q", service]).stdout.strip()
    if not out or "\n" in out:
        raise SystemExit(f"expected one running {service} container")
    return out


def reset_replay_db():
    out = sh([RESET], check=False, timeout=600)
    if out.returncode != 0:
        raise SystemExit("tank_replay reset failed: " + str(out.stderr)[-500:])


WINDOWS = Path(os.environ.get("TANK_FIXTURE_WINDOWS", "/srv/tank/data/qa/fixture-windows.jsonl"))
LIVE_BUSY = """
SELECT (SELECT count(*) FROM ops.jobs WHERE type = 'AGENT_MESSAGE' AND status IN ('QUEUED', 'RUNNING')) AS agent_jobs,
       (SELECT count(*) FROM ops.jobs WHERE type IN ('FILTER_ITEM', 'ROUTE_ITEM') AND status IN ('QUEUED', 'RUNNING')) AS intake_jobs
"""


def wait_for_idle_live(max_wait_s):
    """Lead rule: live traffic first. Start a fixture only when no live agent work is queued or running."""
    import time

    from common import query

    deadline = time.monotonic() + max_wait_s
    while True:
        row = query(LIVE_BUSY)[0]
        if row["agent_jobs"] == 0 and row["intake_jobs"] == 0:
            return row
        if time.monotonic() > deadline:
            raise SystemExit(f"live queue never drained within {max_wait_s}s: {row}")
        time.sleep(60)


def mark_window(record):
    WINDOWS.parent.mkdir(parents=True, exist_ok=True)
    with open(WINDOWS, "a") as fh:
        fh.write(json.dumps(record) + "\n")


REPLAY_DB_EVIDENCE = {
    "ops.jobs": "SELECT job_id, type, status, payload, run_after, created_at, finished_at FROM ops.jobs ORDER BY created_at",
    "evt.capture_chains": "SELECT document FROM evt.capture_chains",
    "ops.tool_calls": "SELECT call_id, tool, status, requested_at, answered_at FROM ops.tool_calls ORDER BY requested_at",
}


def dump_replay_db(d):
    evidence = {}
    for name, sql in REPLAY_DB_EVIDENCE.items():
        out = sh(
            [
                "docker",
                "exec",
                "-i",
                "-u",
                "postgres",
                "tank-postgres-1",
                "psql",
                "-d",
                "tank_replay",
                "-X",
                "-q",
                "-At",
                "-v",
                "ON_ERROR_STOP=1",
            ],
            input=f"BEGIN TRANSACTION READ ONLY;\nSELECT coalesce(json_agg(t), '[]'::json) FROM ({sql}) t;\nCOMMIT;\n",
            check=False,
        )
        evidence[name] = (
            json.loads(out.stdout)
            if out.returncode == 0 and out.stdout.strip()
            else {"error": out.stderr[-300:]}
        )
    write_json(
        d / "replay-db-evidence.json", {"captured_at": iso(utcnow()), "database": "tank_replay", **evidence}
    )


def run_one(fid, entry, out_dir, timeout):
    """One fresh native replay in its own process (`compose run`), never inside the live runtime.

    A masked fixture pins the masked copy of the config tree (its registry is masked), so the replay
    process mounts that tree read-only at /config; the private map is mounted outside /data.
    """
    d = Path(out_dir) / fid
    d.mkdir(parents=True, exist_ok=False)
    hints = entry.get("container") or {}
    config_dir = Path(hints["TANK_CONFIG_PATH"]) if hints.get("TANK_CONFIG_PATH") else ROOT / "config"
    raw = sh(["zstd", "-dc", str(resolve(entry["acceptance_input"]["path"]))]).stdout
    packet = json.loads(raw)
    tank = container("tank")

    deployed = sh(COMPOSE + ["exec", "-T", "tank", "cat", "/app/config/openma.yaml"]).stdout
    if (config_dir / "openma.yaml").read_text() != deployed:
        raise SystemExit(
            f"{fid}: fixture config openma.yaml differs from the deployed /app/config/openma.yaml"
        )
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / f"{fid}.json"
        p.write_text(raw)
        if (
            sh(
                COMPOSE + ["exec", "-T", "tank", "test", "-e", f"/data/replay/inputs/{fid}.json"], check=False
            ).returncode
            == 0
        ):
            stamp = utcnow().strftime("%Y%m%dT%H%M%SZ")
            sh(
                COMPOSE
                + [
                    "exec",
                    "-T",
                    "tank",
                    "mv",
                    f"/data/replay/inputs/{fid}.json",
                    f"/data/replay/inputs/{fid}.json.superseded-{stamp}",
                ]
            )
        sh(COMPOSE + ["exec", "-T", "tank", "mkdir", "-p", "/data/replay/inputs"])
        sh(["docker", "cp", str(p), f"{tank}:/data/replay/inputs/{fid}.json"])
    wait_for_idle_live(int(os.environ.get("TANK_FIXTURE_MAX_WAIT_S", "21600")))
    reset_replay_db()
    private = resolve(entry["private_name_map"]["path"])
    mask_path = hints.get("TANK_REPLAY_PRIVATE_MASK_PATH") or PRIVATE_IN_CONTAINER
    mounts = ["-v", f"{config_dir}:/config:ro"]
    if hints.get("TANK_REPLAY_PRIVATE_MASK_PATH"):
        for path in hints.get("bind_same_path_read_only", []):
            mounts += ["-v", f"{path}:{path}:ro"]
        if not any(str(private).startswith(b) for b in hints.get("bind_same_path_read_only", [])):
            mounts += ["-v", f"{private}:{mask_path}:ro"]
    else:
        mounts += ["-v", f"{private}:{mask_path}:ro"]
    if mask_path.startswith("/data/"):
        raise SystemExit("private map must stay outside /data")
    started = iso(utcnow())

    res = sh(
        COMPOSE
        + [
            "run",
            "--rm",
            "--no-deps",
            "-T",
            *mounts,
            "-e",
            f"TANK_REPLAY_PRIVATE_MASK_PATH={mask_path}",
            "-v",
            f"{DSN_HELPER}:/opt/replay_dsn_exec.py:ro",
            "tank",
            "/opt/tank/venv/bin/python",
            "/opt/replay_dsn_exec.py",
            "tank",
            "replay",
            fid,
            "--database-url",
            "@REPLAY_DSN@",
        ],
        check=False,
        timeout=timeout,
    )
    finished = iso(utcnow())
    mark_window({"fixture_id": fid, "event": "END", "at": finished, "exit_code": res.returncode})
    dump_replay_db(d)
    (d / "replay-stdout.json").write_text(res.stdout)
    (d / "replay-stderr.log").write_text(res.stderr[-20000:])

    sys.path.insert(0, str(ROOT / "src"))
    from tank.replay import digest

    bundle = f"/data/replay/bundles/{digest(packet)}/outputs.json"
    if sh(COMPOSE + ["exec", "-T", "tank", "test", "-f", bundle], check=False).returncode != 0:
        bundle = None
    receipt = {
        "fixture_id": fid,
        "started_at": started,
        "finished_at": finished,
        "exit_code": res.returncode,
        "acceptance_input": entry["acceptance_input"],
        "config_dir": str(config_dir),
        "evidence_mode": "FRESH_NATIVE_TANK_REPLAY",
        "command": f"compose run tank tank replay {fid} --database-url <tank_replay> (config {config_dir})",
    }
    if bundle and res.returncode == 0:
        sh(["docker", "cp", f"{tank}:{bundle}", str(d / "outputs.json")])
        receipt["frozen_output"] = {
            "path": os.path.relpath(d / "outputs.json", ROOT),
            "sha256": sha(d / "outputs.json"),
        }
        receipt["container_bundle"] = bundle
    write_json(d / "acceptance-run.json", receipt)
    return evaluate(fid, entry, d)


def evaluate(fid, entry, d, run_name="acceptance-run.json"):
    sys.path.insert(0, str(ROOT / "tests"))
    sys.path.insert(0, str(ROOT / "src"))
    import fixture_acceptance as fa

    spec = next(
        (
            x
            for x in json.load(open(ROOT / "tests/fixtures/manifest.json"))["fixtures"]
            if x["fixture_id"] == fid
        ),
        {},
    )
    case = {**{k: v for k, v in spec.items() if k not in ("acceptance_run",)}, **entry}
    run_path = d / run_name
    case["acceptance_run"] = {"path": os.path.relpath(run_path, ROOT), "sha256": sha(run_path)}
    errors = (
        fa.check_completed(ROOT, case)
        if json.load(open(run_path)).get("frozen_output")
        else ["no frozen output"]
    )
    result = {
        "fixture_id": fid,
        "evaluated_at": iso(utcnow()),
        "passed": not errors,
        "errors": errors,
        "manifest_acceptance_run": case["acceptance_run"],
    }
    write_json(d / "evaluation.json", result)
    print(json.dumps(result, indent=2))
    return result


def review(out_dir, fid, review_path, entries_path):
    d = Path(out_dir) / fid
    run = json.load(open(d / "acceptance-run.json"))
    rv = json.load(open(review_path))
    if rv.get("frozen_output_sha256") != run["frozen_output"]["sha256"]:
        raise SystemExit("review is not bound to this frozen output")
    target = d / "manual-review.json"
    target.write_text(json.dumps(rv, indent=2) + "\n")
    run["manual_review"] = {"path": os.path.relpath(target, ROOT), "sha256": sha(target)}
    write_json(d / "acceptance-run-with-manual.json", run)
    return evaluate(fid, entries(entries_path, [fid])[fid], d, "acceptance-run-with-manual.json")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["run", "review", "evaluate"])
    ap.add_argument("--entries", default=str(ROOT / "tests/fixtures/manifest.json"))
    ap.add_argument("--fixtures", default="")
    ap.add_argument("--fixture")
    ap.add_argument("--out", required=True)
    ap.add_argument("--review")
    ap.add_argument("--timeout", type=int, default=3600)
    a = ap.parse_args()
    if a.step == "run":
        fids = [f for f in a.fixtures.split(",") if f]
        es = entries(a.entries, fids)
        summary = [run_one(f, es[f], a.out, a.timeout) for f in fids]
        write_json(Path(a.out) / f"summary-{utcnow().strftime('%Y%m%dT%H%M%SZ')}.json", summary)
    elif a.step == "review":
        review(a.out, a.fixture, a.review, a.entries)
    else:
        evaluate(a.fixture, entries(a.entries, [a.fixture])[a.fixture], Path(a.out) / a.fixture)
