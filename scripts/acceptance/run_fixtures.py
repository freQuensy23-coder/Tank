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

Usage (only after tank-platform says openma is live; never in parallel):
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
from common import iso, utcnow, write_json  # noqa: E402

ROOT = Path("/srv/tank")
COMPOSE = shlex.split(os.environ.get(
    "TANK_COMPOSE",
    "docker compose -p tank --project-directory /srv/tank -f /srv/tank/compose.yaml -f /etc/tank/compose.server.yaml"
    " -f /srv/tank/docs/verification/server-recovery-bootstrap-2026-10-20/pg.resources.yaml --env-file /etc/tank/compose.env"))
# tank-sessions owns the per-run tank_replay reset and the DSN helper (password never on argv or stdout).
RESET = os.environ.get("TANK_REPLAY_RESET", "/srv/tank-work/ts/reset_tank_replay.sh")
DSN_HELPER = os.environ.get("TANK_REPLAY_DSN_HELPER", "/srv/tank-work/ts/replay_dsn_exec.py")
PRIVATE_IN_CONTAINER = "/replay-private/map.json"  # read-only bind mount outside /data


def sh(args, *, input=None, check=True, timeout=None):
    out = subprocess.run(args, input=input, capture_output=True, text=isinstance(input, str) or input is None,
                         timeout=timeout)
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
        # assemble_packets.py output: {F-xx: {manifest_entry, container, ...}}
        rows = [{**v["manifest_entry"], "fixture_id": k, "container": v.get("container", {})} for k, v in data.items()]
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
    tank = container("tank")  # the running service must be up: openma's model card calls tank:8790
    # load_inputs refuses unless the fixture's openma.yaml is byte-identical to the deployed one.
    deployed = sh(COMPOSE + ["exec", "-T", "tank", "cat", "/config/openma.yaml"]).stdout
    if (config_dir / "openma.yaml").read_text() != deployed:
        raise SystemExit(f"{fid}: fixture config openma.yaml differs from the deployed /config/openma.yaml")
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / f"{fid}.json"
        p.write_text(raw)
        if sh(COMPOSE + ["exec", "-T", "tank", "test", "-e", f"/data/replay/inputs/{fid}.json"], check=False).returncode == 0:
            stamp = utcnow().strftime("%Y%m%dT%H%M%SZ")
            sh(COMPOSE + ["exec", "-T", "tank", "mv", f"/data/replay/inputs/{fid}.json",
                          f"/data/replay/inputs/{fid}.json.superseded-{stamp}"])
        sh(COMPOSE + ["exec", "-T", "tank", "mkdir", "-p", "/data/replay/inputs"])
        sh(["docker", "cp", str(p), f"{tank}:/data/replay/inputs/{fid}.json"])
    reset_replay_db()
    private = resolve(entry["private_name_map"]["path"])
    mask_path = hints.get("TANK_REPLAY_PRIVATE_MASK_PATH") or PRIVATE_IN_CONTAINER
    mounts = ["-v", f"{config_dir}:/config:ro"]
    if hints.get("TANK_REPLAY_PRIVATE_MASK_PATH"):
        # The map and packet reference absolute /srv/tank paths: bind them read-only at the same paths.
        for path in hints.get("bind_same_path_read_only", []):
            mounts += ["-v", f"{path}:{path}:ro"]
        if not any(str(private).startswith(b) for b in hints.get("bind_same_path_read_only", [])):
            mounts += ["-v", f"{private}:{mask_path}:ro"]
    else:
        mounts += ["-v", f"{private}:{mask_path}:ro"]
    if mask_path.startswith("/data/"):
        raise SystemExit("private map must stay outside /data")
    started = iso(utcnow())
    # No --use-aliases: the one-off replay process must not take the `tank` alias. Default volumes keep
    # /data on tank-data, where evidence for the tank-replay sandbox lives (/data/replay/evidence/<RPL-id>).
    res = sh(COMPOSE + ["run", "--rm", "--no-deps", "-T",
                        *mounts,
                        "-e", f"TANK_REPLAY_PRIVATE_MASK_PATH={mask_path}",
                        "-v", f"{DSN_HELPER}:/opt/replay_dsn_exec.py:ro",
                        "tank", "/opt/tank/venv/bin/python", "/opt/replay_dsn_exec.py",
                        "tank", "replay", fid, "--database-url", "@REPLAY_DSN@"],
             check=False, timeout=timeout)
    finished = iso(utcnow())
    (d / "replay-stdout.json").write_text(res.stdout)
    (d / "replay-stderr.log").write_text(res.stderr[-20000:])
    # replay_fixture freezes under bundles/<digest of the packet JSON> (src/tank/replay.py replay_fixture).
    sys.path.insert(0, str(ROOT / "src"))
    from tank.replay import digest

    bundle = f"/data/replay/bundles/{digest(packet)}/outputs.json"
    if sh(COMPOSE + ["exec", "-T", "tank", "test", "-f", bundle], check=False).returncode != 0:
        bundle = None
    receipt = {"fixture_id": fid, "started_at": started, "finished_at": finished, "exit_code": res.returncode,
               "acceptance_input": entry["acceptance_input"], "config_dir": str(config_dir),
               "evidence_mode": "FRESH_NATIVE_TANK_REPLAY",
               "command": f"compose run tank tank replay {fid} --database-url <tank_replay> (config {config_dir})"}
    if bundle and res.returncode == 0:
        sh(["docker", "cp", f"{tank}:{bundle}", str(d / "outputs.json")])
        receipt["frozen_output"] = {"path": os.path.relpath(d / "outputs.json", ROOT), "sha256": sha(d / "outputs.json")}
        receipt["container_bundle"] = bundle
    write_json(d / "acceptance-run.json", receipt)
    return evaluate(fid, entry, d)


def evaluate(fid, entry, d, run_name="acceptance-run.json"):
    sys.path.insert(0, str(ROOT / "tests"))
    sys.path.insert(0, str(ROOT / "src"))
    import fixture_acceptance as fa

    case = dict(entry)
    run_path = d / run_name
    case["acceptance_run"] = {"path": os.path.relpath(run_path, ROOT), "sha256": sha(run_path)}
    errors = fa.check_completed(ROOT, case) if json.load(open(run_path)).get("frozen_output") else ["no frozen output"]
    result = {"fixture_id": fid, "evaluated_at": iso(utcnow()), "passed": not errors, "errors": errors,
              "manifest_acceptance_run": case["acceptance_run"]}
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
        summary = [run_one(f, es[f], a.out, a.timeout) for f in fids]  # strictly sequential
        write_json(Path(a.out) / f"summary-{utcnow().strftime('%Y%m%dT%H%M%SZ')}.json", summary)
    elif a.step == "review":
        review(a.out, a.fixture, a.review, a.entries)
    else:
        evaluate(a.fixture, entries(a.entries, [a.fixture])[a.fixture], Path(a.out) / a.fixture)
