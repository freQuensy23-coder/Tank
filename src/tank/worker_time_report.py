import argparse
import json
import sys
from collections import Counter, defaultdict
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
    winners = {}
    for record in records:
        worker, run = record["worker"], record["run"]
        if worker not in range(8):
            raise ValueError(f"unexpected worker {worker}")
        left, at = instant(record["since"]), instant(record["at"])
        active = record["active"]
        for phase, duration, *poll in record["parts"]:
            if duration < 0:
                raise ValueError("worker UTC clock moved backwards")
            right = left + duration * MICROSECOND
            if left < end and start < right:
                spans[worker].append(
                    (
                        max(left, start),
                        min(right, end),
                        phase,
                        run,
                        active and active["claim"],
                        poll[0] if poll else None,
                    )
                )
            left = right
        if left != at:
            raise ValueError("inconsistent phase durations")
        claim = active or record.get("acquired")
        if claim is None or at < start or instant(claim["select_start"]) >= end:
            continue
        key = worker, run, claim["claim"]
        winners[worker, run, claim["poll"]] = claim["claim"]
        attempt = attempts.setdefault(
            key,
            {
                **claim,
                "worker": worker,
                "run": run,
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
    for attempt in attempts.values():
        left, right = instant(attempt["start"]), attempt["last_seen"]
        if instant(attempt["select_start"]) < end and (right > start or left == right == start):
            details.append({**attempt, "last_seen": right.isoformat()})
    details.sort(key=lambda a: (instant(a["start"]), a["worker"], a["run"], a["claim"]))

    workers = {}
    pool = Counter()
    for worker in range(8):
        cursor = start
        timeline = []
        for left, right, phase, run, number, poll in sorted(spans[worker]):
            if poll is not None:
                number = winners.get((worker, run, poll))
            if left < cursor:
                raise ValueError(f"overlapping evidence for worker {worker}; select one instance")
            if cursor < left:
                timeline.append((cursor, left, "unknown", None, None))
            timeline.append((left, right, phase, run, number))
            cursor = right
        if cursor < end:
            timeline.append((cursor, end, "unknown", None, None))
        totals = Counter()
        rows = []
        for left, right, phase, run, number in timeline:
            totals[phase] += (right - left) // MICROSECOND
            rows.append(
                dict(start=left.isoformat(), end=right.isoformat(), phase=phase, run=run, claim=number)
            )
        workers[worker] = {"total_us": totals, "spans": rows}
        pool.update(totals)
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "workers": workers,
        "pool_us": pool,
        "attempts": details,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Account for Tank worker wall time from application logs on stdin"
    )
    parser.add_argument("start", type=instant, help="inclusive timestamp including UTC offset")
    parser.add_argument(
        "end", nargs="?", type=instant, help="exclusive timestamp; default: start + 5 minutes"
    )
    args = parser.parse_args()
    print(
        json.dumps(
            report(events(sys.stdin), args.start, args.end or args.start + timedelta(minutes=5)), indent=2
        )
    )


if __name__ == "__main__":
    main()
