"""DELIVER pickup latency under a loaded event loop (evidence for r115).

8 workers poll claim_job every 0.25 s like Runtime.worker; a REACTION_CAPTURE backlog
is queued and the one claimed capture keeps running; a pressure task blocks the event
loop for `--stall` seconds out of every `--period` (prod showed multi-second stalls).
Each round enqueues one DELIVER (commit = t0) and records when a worker claims it.
Usage: TANK_TEST_DATABASE_URL=... python claim_latency.py --rounds 20
"""

import argparse
import asyncio
import json
import os
import statistics
import time

from tank.storage import PostgresRepository
from tank.util import new_id


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=20)
    ap.add_argument("--stall", type=float, default=0.3)
    ap.add_argument("--period", type=float, default=0.4)
    ap.add_argument("--holders", type=int, default=0, help="tasks that keep pooled transactions open")
    ap.add_argument("--hold", type=float, default=1.0)
    args = ap.parse_args()
    repo = await PostgresRepository(os.environ["TANK_TEST_DATABASE_URL"]).open()
    await repo.migrate()
    async with repo.transaction():
        await repo._conn().execute("DELETE FROM ops.jobs WHERE status IN ('QUEUED','RUNNING')")
    for n in range(40):
        await repo.enqueue("REACTION_CAPTURE", {"n": n}, new_id("idem"), priority=2)
    claimed_at, stop = {}, asyncio.Event()

    async def worker():
        while not stop.is_set():
            job = await repo.claim_job()
            if job is None:
                await asyncio.sleep(0.25)
                continue
            if job["type"] == "DELIVER":
                claimed_at[job["job_id"]] = time.monotonic()
                await repo.finish_job(job["job_id"])
            else:
                await asyncio.sleep(30)  # a capture keeps running

    async def pressure():
        while not stop.is_set():
            time.sleep(args.stall)  # blocks the loop, like CPU-heavy work on the loop thread
            await asyncio.sleep(args.period - args.stall)

    async def holder():
        # Like prod's idle-in-transaction work: a transaction open across awaits.
        while not stop.is_set():
            async with repo.transaction():
                await repo._conn().execute("SELECT 1")
                await asyncio.sleep(args.hold)
            await asyncio.sleep(0.05)

    tasks = [asyncio.create_task(worker()) for _ in range(8)] + [asyncio.create_task(pressure())]
    tasks += [asyncio.create_task(holder()) for _ in range(args.holders)]
    await asyncio.sleep(2)
    latencies = []
    for _ in range(args.rounds):
        job = await repo.enqueue("DELIVER", {}, new_id("idem"), priority=0)
        t0 = time.monotonic()
        while job["job_id"] not in claimed_at:
            await asyncio.sleep(0.01)
        latencies.append(claimed_at[job["job_id"]] - t0)
        await asyncio.sleep(0.5)
    stop.set()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await repo.close()
    latencies.sort()
    print(json.dumps({
        "rounds": len(latencies),
        "p50_s": round(statistics.median(latencies), 3),
        "p90_s": round(latencies[int(0.9 * len(latencies)) - 1], 3),
        "max_s": round(latencies[-1], 3),
        "stall_s": args.stall, "period_s": args.period, "holders": args.holders, "hold_s": args.hold,
    }))


asyncio.run(main())
