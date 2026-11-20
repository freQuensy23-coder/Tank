import asyncio
import json
import logging
import re
from datetime import timedelta
from time import monotonic_ns
from uuid import uuid4

from tank.util import iso, parse_time, utcnow

log = logging.getLogger(__name__)
JOB_ID = re.compile(r"JOB-[0-7][0-9A-HJKMNP-TV-Z]{25}")


class WorkerTime:
    def __init__(self, worker):
        self.worker = worker
        self.run = uuid4().hex
        self.claim = 0
        self.active = None
        self.loop = asyncio.get_running_loop()
        self.last = iso(utcnow())
        self.last_ns = monotonic_ns()
        self.emit("worker_start", self.last, self.last_ns)
        self.timer = self.loop.call_later(30, self.sample)

    def emit(self, event, at, tick, **fields):
        record = {
            "event": event,
            "worker": self.worker,
            "run": self.run,
            "since": self.last,
            "at": at,
            "interval_ns": tick - self.last_ns,
            "active": self.active,
            **fields,
        }
        log.info("worker_time %s", json.dumps(record, separators=(",", ":")))
        self.last, self.last_ns = at, tick

    def sample(self):
        self.emit("sample", iso(utcnow()), monotonic_ns())
        self.timer = self.loop.call_later(30, self.sample)

    def start_claim(self, job, kind):
        now, tick = utcnow(), monotonic_ns()
        self.claim += 1
        try:
            wait_us = max(0, (now - parse_time(job.get("run_after"))) // timedelta(microseconds=1))
        except (ValueError, TypeError, OverflowError, OSError):
            wait_us = None
        job_id = job.get("job_id")
        claim = {
            "claim": self.claim,
            "job_id": job_id if isinstance(job_id, str) and JOB_ID.fullmatch(job_id) else None,
            "kind": kind,
            "attempt": job.get("attempts") if type(job.get("attempts")) is int else None,
            "eligible_wait_us": wait_us,
            "start": iso(now),
        }
        self.emit("claim_start", claim["start"], tick, acquired=claim)
        self.active = claim
        self.claim_ns = tick

    def end_claim(self, outcome):
        tick = monotonic_ns()
        self.emit("claim_end", iso(utcnow()), tick, outcome=outcome, elapsed_ns=tick - self.claim_ns)
        self.active = None

    def stop(self):
        self.timer.cancel()
        self.emit("worker_stop", iso(utcnow()), monotonic_ns())
