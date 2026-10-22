#!/usr/bin/env python3
"""Measure the Tank stack's RAM/disk with N concurrent agent sessions, each running work in its sandbox VM.

Read-only for Tank state: the agent only runs a sandbox script (pandas + a read-only psql query) and replies.
Usage: stack_sizing.py OUT_DIR OPENMA_BASE_URL OPENMA_YAML N
"""

import json
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
import yaml

out, base, config_path, count = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3]), int(sys.argv[4])
out.mkdir(parents=True, exist_ok=True)
config = yaml.safe_load(config_path.read_text())
key = Path("/etc/tank/secrets/oma_api_key").read_text().strip()
client = httpx.Client(
    base_url=base.rstrip("/") + "/v1/",
    timeout=60,
    headers={"x-api-key": key, "anthropic-beta": "managed-agents-2026-04-01"},
)
VOLUME = "/var/lib/docker/volumes/tank_openma-data/_data"
PROMPT = (
    "Sizing check by the Tank operator, not an event. Do not call any Tank tool and do not record anything. "
    "In your sandbox, run exactly this bash command once and wait for it to finish: "
    'python3 -c "import numpy as np, pandas as pd, time; a=np.random.rand(3000,3000); t=time.time(); '
    '[(a@a).sum() for _ in range(8)]; df=pd.DataFrame(np.random.rand(2_000_000,8)); print(df.describe().shape, round(time.time()-t,1))" '
    "&& psql \"$TANK_DB_URL\" -Atc 'select count(*) from pg_tables' && sleep 45. "
    "Then reply with one line: the command output."
)


def container_mem(name):
    raw = (
        subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", name],
            capture_output=True,
            text=True,
        )
        .stdout.strip()
        .split(" / ")[0]
    )
    unit = {"KiB": 1 / 1024, "MiB": 1, "GiB": 1024}
    for suffix, factor in unit.items():
        if raw.endswith(suffix):
            return round(float(raw[: -len(suffix)]) * factor, 1)
    return None


def host():
    info = {
        line.split(":")[0]: int(line.split()[1]) for line in Path("/proc/meminfo").read_text().splitlines()
    }
    shims = subprocess.run(
        ["pgrep", "-c", "-f", "boxlite-shim"], capture_output=True, text=True
    ).stdout.strip()
    rss = subprocess.run(
        ["sh", "-c", "ps -eo rss=,args= | grep '[b]oxlite-shim' | awk '{s+=$1} END {print s+0}'"],
        capture_output=True,
        text=True,
    ).stdout.strip()
    return {
        "mem_available_mib": info["MemAvailable"] // 1024,
        "mem_used_mib": (info["MemTotal"] - info["MemAvailable"]) // 1024,
        "disk_free_gib": round(shutil.disk_usage("/").free / 2**30, 2),
        "vm_processes": int(shims or 0),
        "vm_rss_mib": round(int(rss or 0) / 1024, 1),
    }


samples, stop = [], threading.Event()


def sample():
    t0 = time.monotonic()
    while not stop.is_set():
        samples.append(
            {
                "t": round(time.monotonic() - t0, 1),
                **host(),
                "openma_mib": container_mem("tank-openma-1"),
                "postgres_mib": container_mem("tank-postgres-1"),
                "tank_mib": container_mem("tank-tank-1"),
            }
        )
        time.sleep(3)


baseline = {
    **host(),
    "openma_mib": container_mem("tank-openma-1"),
    "openma_data_gib": round(
        int(subprocess.run(["du", "-sb", VOLUME], capture_output=True, text=True).stdout.split()[0]) / 2**30,
        2,
    ),
}
sampler = threading.Thread(target=sample, daemon=True)
sampler.start()
sessions = []
for i in range(count):
    created = client.post(
        "sessions",
        json={
            "agent": config["agent_id"],
            "environment_id": config["environment_id"],
            "metadata": {"tank_identity": f"tank-platform-sizing-{count}-{i}-{int(time.time())}"},
        },
    )
    created.raise_for_status()
    sid = created.json()["id"]
    client.post(
        f"sessions/{sid}/events",
        json={"events": [{"type": "user.message", "content": [{"type": "text", "text": PROMPT}]}]},
    ).raise_for_status()
    sessions.append(sid)
done, deadline = {}, time.monotonic() + 900
while len(done) < len(sessions) and time.monotonic() < deadline:
    for sid in sessions:
        if sid in done:
            continue
        events = (
            client.get(f"sessions/{sid}/events", params={"limit": 100, "order": "asc"}).json().get("data", [])
        )
        for e in events:
            if e.get("type") in {"session.status_terminated", "session.error"} or (
                e.get("type") == "session.status_idle"
                and (e.get("stop_reason") or {}).get("type") == "end_turn"
            ):
                text = [
                    b.get("text", "")
                    for x in events
                    if x.get("type") == "agent.message"
                    for b in x.get("content", [])
                ]
                done[sid] = {
                    "final": e.get("type"),
                    "agent_text": text[-1:] if text else [],
                    "tools": [
                        x.get("name")
                        for x in events
                        if x.get("type") in {"agent.tool_use", "agent.custom_tool_use"}
                    ],
                }
    time.sleep(5)
time.sleep(20)
stop.set()
sampler.join()
after = {
    **host(),
    "openma_mib": container_mem("tank-openma-1"),
    "openma_data_gib": round(
        int(subprocess.run(["du", "-sb", VOLUME], capture_output=True, text=True).stdout.split()[0]) / 2**30,
        2,
    ),
}
peak = {
    k: max(s[k] for s in samples if s.get(k) is not None)
    for k in ("mem_used_mib", "vm_processes", "vm_rss_mib", "openma_mib", "postgres_mib")
}
peak["min_mem_available_mib"] = min(s["mem_available_mib"] for s in samples)
peak["min_disk_free_gib"] = min(s["disk_free_gib"] for s in samples)
result = {
    "concurrent_sessions": count,
    "sessions": done,
    "baseline": baseline,
    "peak": peak,
    "after": after,
    "all_finished": len(done) == count,
}
(out / f"sizing-{count}.json").write_text(json.dumps({**result, "samples": samples}, indent=2) + "\n")
print(json.dumps(result))
