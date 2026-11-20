import argparse
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta

MICROSECOND = timedelta(microseconds=1)


def instant(value):
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is None:
        raise ValueError("timezone required")
    return stamp.astimezone(UTC)


def events(lines):
    for line in lines:
        _, marker, data = line.partition(" INFO tank.worker_time worker_time ")
        if marker:
            yield json.loads(data)


def report(records, start, end):
    if end <= start:
        raise ValueError("empty time window")
    spans = defaultdict(list)
    attempts = {}
    for record in records:
        worker, run = record["worker"], record["run"]
        since, at = instant(record["since"]), instant(record["at"])
        if at < since:
            raise ValueError("worker UTC clock moved backwards")
        if start < at and since < end and since < at:
            active = record["active"]
            state = "claimed" if active else "unclaimed"
            spans[worker].append((since, at, state, run, active["claim"] if active else None))
        claim = record["active"] or record.get("acquired")
        if claim is None:
            continue
        key = worker, run, claim["claim"]
        attempt = attempts.setdefault(
            key,
            {
                **{k: claim[k] for k in ("job_id", "kind", "attempt", "eligible_wait_us", "start")},
                "last_seen": instant(claim["start"]),
                "end": None,
                "elapsed_ns": None,
                "outcome": None,
            },
        )
        attempt["last_seen"] = max(attempt["last_seen"], at)
        if record["event"] == "claim_end":
            attempt.update(end=record["at"], elapsed_ns=record["elapsed_ns"], outcome=record["outcome"])

    details = []
    for (worker, run, claim_number), attempt in attempts.items():
        left, right = instant(attempt["start"]), attempt["last_seen"]
        if left >= end or right < start or (right == start and left < right):
            continue
        details.append(
            {"worker": worker, "run": run, "claim": claim_number, **attempt, "last_seen": right.isoformat()}
        )

    workers = {}
    for worker in range(8):
        cursor = start
        timeline = []
        for left, right, state, run, claim_number in sorted(spans[worker]):
            left, right = max(start, left), min(end, right)
            if right <= left:
                continue
            if left < cursor:
                raise ValueError(f"overlapping evidence for worker {worker}; select one instance")
            if cursor < left:
                timeline.append((cursor, left, "unknown", None, None))
            if state == "claimed" and attempts[worker, run, claim_number]["end"] is None:
                state = "unfinished"
            timeline.append((left, right, state, run, claim_number))
            cursor = right
        if cursor < end:
            timeline.append((cursor, end, "unknown", None, None))
        totals = dict.fromkeys(("claimed", "unfinished", "unclaimed", "unknown"), 0)
        rows = []
        for left, right, state, run, claim_number in timeline:
            totals[state] += (right - left) // MICROSECOND
            rows.append(
                dict(start=left.isoformat(), end=right.isoformat(), state=state, run=run, claim=claim_number)
            )
        workers[worker] = {"total_us": totals, "spans": rows}
    return {"workers": workers, "attempts": details}


def main():
    parser = argparse.ArgumentParser(description="Account for five minutes of Tank worker time from stdin")
    parser.add_argument("start", type=instant, help="start timestamp including UTC offset")
    args = parser.parse_args()
    print(json.dumps(report(events(sys.stdin), args.start, args.start + timedelta(minutes=5)), indent=2))


if __name__ == "__main__":
    main()
