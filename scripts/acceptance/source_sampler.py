"""V0-A read ledger: sample ops.source_checkpoints and append every new read attempt/failure.

The runtime keeps only the latest checkpoint per feed (last_attempt_at, last_success_at,
last_failure_at). Every attempt writes a new last_attempt_at; a failed read writes a new
last_failure_at. Source cadence is never faster than 15 s (03 §9), so sampling every 5 s sees
every distinct value. Read-only; appends JSONL to --out. Run detached for the whole 72 h window:

  nohup python3 source_sampler.py --out /srv/tank/data/qa/v0a-<start>/source-reads.jsonl \
      > /srv/tank-work/logs/tank-acceptance/source-sampler.log 2>&1 &
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import iso, query, utcnow  # noqa: E402

SQL = """
SELECT storage_key AS key, document->>'source_id' AS source_id, document->>'status' AS status,
       document->>'last_attempt_at' AS attempt, document->>'last_success_at' AS success,
       document->>'last_failure_at' AS failure, left(coalesce(document->>'error', ''), 300) AS error,
       document->>'http_status' AS http_status, (document ? 'entry_errors') AS entry_errors,
       (document ? 'article_errors') AS article_errors
  FROM ops.source_checkpoints
"""


def main(out, interval, until):
    seen = {}
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, "a", buffering=1) as fh:
        fh.write(json.dumps({"kind": "SAMPLER_START", "at": iso(utcnow()), "interval_s": interval}) + "\n")
        while until is None or time.time() < until:
            started = time.monotonic()
            try:
                rows = query(SQL)
            except Exception as exc:  # noqa: BLE001 -- a missed sample is recorded, never hidden
                fh.write(json.dumps({"kind": "SAMPLE_ERROR", "at": iso(utcnow()), "error": str(exc)[:300]}) + "\n")
                time.sleep(interval)
                continue
            for r in rows:
                prev = seen.get(r["key"], {})
                for field, kind in (("attempt", "ATTEMPT"), ("failure", "FAILURE"), ("success", "SUCCESS")):
                    if r[field] and r[field] != prev.get(field):
                        if prev or kind != "SUCCESS":
                            fh.write(json.dumps({"kind": kind, "key": r["key"], "source_id": r["source_id"],
                                                 "at": r[field], "status": r["status"],
                                                 "error": r["error"] if kind == "FAILURE" else None,
                                                 "http_status": r["http_status"],
                                                 "entry_errors": r["entry_errors"],
                                                 "article_errors": r["article_errors"],
                                                 "baseline": not prev}) + "\n")
                seen[r["key"]] = r
            time.sleep(max(0.0, interval - (time.monotonic() - started)))
        fh.write(json.dumps({"kind": "SAMPLER_STOP", "at": iso(utcnow())}) + "\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval", type=float, default=5.0)
    ap.add_argument("--hours", type=float, default=None, help="stop after this many hours")
    a = ap.parse_args()
    main(a.out, a.interval, time.time() + a.hours * 3600 if a.hours else None)
