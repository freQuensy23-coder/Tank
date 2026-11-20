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
US = timedelta(microseconds=1)


class WorkerTime:
    def __init__(self, worker):
        self.worker = worker
        self.run = uuid4().hex
        self.claim = self.poll = 0
        self.active = None
        self.loop = asyncio.get_running_loop()
        self.current = "loop"
        self.since = self.boundary = utcnow()
        self.parts = []
        self.emit("worker_start", self.since)
        self.timer = self.loop.call_later(30, self.sample)

    def advance(self, now):
        duration = (now - self.boundary) // US
        if duration:
            part = [self.current, duration]
            if self.current == "queue_select":
                part.append(self.poll)
            self.parts.append(part)
        self.boundary = now

    def phase(self, name):
        now = utcnow()
        self.advance(now)
        self.current = name
        if len(self.parts) >= 384:
            self.emit("sample", now)

    def select(self):
        self.phase("queue_select")
        self.poll += 1
        self.selection_start = self.boundary
        self.selection_ns = monotonic_ns()

    def emit(self, event, now, **fields):
        self.advance(now)
        record = {
            "event": event,
            "worker": self.worker,
            "run": self.run,
            "since": iso(self.since),
            "at": iso(now),
            "parts": self.parts,
            "active": self.active,
            **fields,
        }
        log.info("worker_time %s", json.dumps(record, separators=(",", ":")))
        self.since = now
        self.parts.clear()

    def sample(self):
        self.emit("sample", utcnow())
        self.timer = self.loop.call_later(30, self.sample)

    def start_claim(self, job, kind):
        now, tick = utcnow(), monotonic_ns()
        self.claim += 1
        try:
            wait_us = max(0, (now - parse_time(job.get("run_after"))) // US)
        except (ValueError, TypeError, OverflowError, OSError):
            wait_us = None
        job_id = job.get("job_id")
        claim = {
            "claim": self.claim,
            "job_id": job_id if isinstance(job_id, str) and JOB_ID.fullmatch(job_id) else None,
            "kind": kind,
            "attempt": job.get("attempts") if type(job.get("attempts")) is int else None,
            "eligible_wait_us": wait_us,
            "poll": self.poll,
            "select_start": iso(self.selection_start),
            "select_ns": tick - self.selection_ns,
            "start": iso(now),
        }
        self.emit("claim_start", now, acquired=claim)
        self.active = claim
        self.current = "prepare"
        self.claim_ns = tick

    def end_claim(self, outcome):
        now, tick = utcnow(), monotonic_ns()
        self.emit("claim_end", now, outcome=outcome, elapsed_ns=tick - self.claim_ns)
        self.active = None
        self.current = "loop"

    def stop(self):
        self.timer.cancel()
        self.emit("worker_stop", utcnow())
