"""Durable Jev rate-limit deferral and explicit recovery of failed classification jobs."""

import random
import re
from datetime import timedelta

from tank.agents.jev import JevRateLimited
from tank.time_precision import Timestamp, collection_clock, offset_schedule
from tank.util import parse_time, utcnow

JEV_JOB_TYPES = frozenset({"FILTER_ITEM", "ROUTE_ITEM"})
JEV_RATE_LIMIT_ERROR = "JEV_RATE_LIMITED: HTTP 429"


async def defer_jev_job(repo, job_id: str, error: JevRateLimited):
    async with repo.lock("job:" + job_id), repo.transaction():
        job = await repo.get("ops.jobs", job_id)
        if job is None or job["status"] != "RUNNING":
            return None
        if job["type"] not in JEV_JOB_TYPES or not job.get("expires_at"):
            raise ValueError("Jev deferral requires a classification job with an expiry")
        now = utcnow()
        deadline = parse_time(job["expires_at"])
        count = job.get("jev_rate_limit_count", 0) + 1
        backoff = min(300, 2 ** min(count, 9))
        delay = max(error.retry_after_s or 0, backoff + random.uniform(0, backoff / 10))
        remaining = max(0, (deadline - now).total_seconds())
        stamp = collection_clock(now)
        expired = remaining == 0
        return await repo.put(
            "ops.jobs",
            job_id,
            {
                **job,
                "status": "EXPIRED" if expired else "QUEUED",
                "attempts": max(0, job["attempts"] - 1),
                "last_error": JEV_RATE_LIMIT_ERROR,
                "jev_rate_limit_count": count,
                "jev_rate_limit": {**error.diagnostics(), **stamp.fields("at")},
                **offset_schedule(stamp, timedelta(seconds=min(delay, remaining))).fields("run_after"),
                **(stamp if expired else Timestamp(None, None, "PENDING")).fields("finished_at"),
            },
        )


async def retry_jev_job(repo, job_id: str):
    """Requeue one explicitly selected failed job; never infer 429 from a legacy error."""
    async with repo.lock("job:" + job_id), repo.transaction():
        job = await repo.get("ops.jobs", job_id)
        if job is None:
            raise ValueError("Job does not exist")
        if job["type"] not in JEV_JOB_TYPES or job["status"] != "FAILED":
            raise ValueError("Only failed FILTER_ITEM or ROUTE_ITEM jobs can be recovered")
        error = job.get("last_error") or ""
        if error not in {"HTTPStatusError", "JevRateLimited", JEV_RATE_LIMIT_ERROR} and not re.fullmatch(
            r"JevHTTPError: HTTP [1-5][0-9]{2}", error
        ):
            raise ValueError("The selected job did not fail with a Jev HTTP error")
        now = utcnow()
        if not job.get("expires_at") or parse_time(job["expires_at"]) <= now:
            raise ValueError("The job has expired; recovery cannot extend its original deadline")
        item = await repo.get("ingest.source_items", job["payload"].get("item_id"))
        if item is None:
            raise ValueError("The job's source item does not exist")
        if job["type"] == "FILTER_ITEM" and item.get("relevance", {}).get("decision") is not None:
            raise ValueError("The item already has a filter decision")
        if job["type"] == "ROUTE_ITEM" and (item.get("route") or item.get("routing_message")):
            raise ValueError("The item already has a routing result")
        stamp = collection_clock(now)
        history = {
            **stamp.fields("requested_at"),
            "status": job["status"],
            "attempts": job["attempts"],
            "last_error": job["last_error"],
            "finished_at": job.get("finished_at"),
            "jev_rate_limit": job.get("jev_rate_limit"),
        }
        recovered = {
            **job,
            "status": "QUEUED",
            "attempts": 0,
            "last_error": None,
            "manual_retries": [*job.get("manual_retries", []), history],
            **stamp.fields("run_after"),
            **Timestamp(None, None, "PENDING").fields("finished_at"),
        }
        for name in tuple(recovered):
            if name.startswith("ops_progress_recorded_at"):
                recovered.pop(name)
        await repo.put("ops.jobs", job_id, recovered)
        return {"job_id": job_id, "status": "QUEUED", "expires_at": job["expires_at"]}
