"""V0-H weekly miss audit and outcome view (03 §11.5).

Writes <run>/weeks/<week-start>/:
  tank-events.md          every non-merged event of the week by class, with card/delivery status and source ids
  miss-audit-template.json  rows the reviewer fills: each major event seen in the covered source buckets,
                          the matching Tank event (or none) and, for a MISS, one reason of 03 §11.5
  outcomes.jsonl          `tank review show <alert_id>` for every card of the week (realized move, MFE/MAE,
                          conditions fired; descriptive only, never calibration)
Usage: python3 shadow_weekly.py --run /srv/tank/data/qa/v0h-<start> --week-start 2026-10-21
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import iso, query, utcnow, write_json
from shadow_daily import tank_container

MISS_REASONS = [
    "LATE",
    "WRONG_BENEFICIARY",
    "BAD_DATA",
    "OLD_NEWS",
    "STATE_ERROR",
    "MARKET_TRUTH_ERROR",
    "DELIVERY_FAILURE",
    "SOURCE_NOT_COVERED",
]

EVENTS = """
SELECT e.event_id, e.statement, e.known_to_us_at, e.public_at, e.event_state, e.freshness, e.lifecycle,
       (SELECT a.class FROM evt.ems_assessments a WHERE a.ems_id = e.current_ems_id) AS class,
       array(SELECT DISTINCT i.source_id FROM evt.event_observations o JOIN ingest.source_items i ON i.item_id = o.item_id
              WHERE o.event_id = e.event_id) AS sources,
       array(SELECT DISTINCT al.alert_id FROM out.alerts al WHERE al.event_id = e.event_id) AS alerts,
       (SELECT count(*) FROM out.deliveries d JOIN out.alerts al ON al.alert_id = d.alert_id AND al.version = d.version
         WHERE al.event_id = e.event_id AND d.status = 'SENT') AS sent
  FROM evt.events e
 WHERE e.known_to_us_at >= :'since' AND e.known_to_us_at < :'until' AND e.merged_into IS NULL
 ORDER BY e.known_to_us_at
"""


def main(run, week_start, with_outcomes) -> None:
    since, until = (
        week_start.isoformat() + "T00:00:00Z",
        (week_start + timedelta(days=7)).isoformat() + "T00:00:00Z",
    )
    d = os.path.join(run, "weeks", week_start.isoformat())
    os.makedirs(d, exist_ok=True)
    events = query(EVENTS, {"since": since, "until": until})
    order = ["STRUCTURAL_SHOCK", "MAJOR", "MATERIAL", "LOW", None]
    lines = [f"# Tank events {since} → {until}", "", f"Events: {len(events)}", ""]
    for cls in order:
        group = [e for e in events if e["class"] == cls]
        if not group:
            continue
        lines += [
            f"## {cls or 'no assessment'} ({len(group)})",
            "",
            "| known_to_us_at | event | state | freshness | sources | cards | sent | statement |",
            "|---|---|---|---|---|---:|---:|---|",
        ]
        lines += [
            f"| {e['known_to_us_at']} | {e['event_id']} | {e['event_state']} | {e['freshness']} | {','.join(e['sources'] or [])} | "
            f"{len(e['alerts'] or [])} | {e['sent']} | {(e['statement'] or '')[:160]} |"
            for e in group
        ]
        lines.append("")
    open(os.path.join(d, "tank-events.md"), "w").write("\n".join(lines))
    write_json(
        os.path.join(d, "miss-audit-template.json"),
        {
            "week": [since, until],
            "instructions": "List every major event of the week from the covered source buckets (01 §4.2). For each, set "
            "tank_event_id to the matching Tank event or null; verdict CAUGHT if that event is MAJOR+ with a card, "
            "otherwise MISS with one miss_reason. Label MISS events that exist in Tank with "
            "`tank review label <event_id> MISS --miss-reason R`.",
            "miss_reasons": MISS_REASONS,
            "rows": [
                {
                    "reviewer": None,
                    "external_event": None,
                    "source_bucket": None,
                    "public_at": None,
                    "tank_event_id": None,
                    "verdict": None,
                    "miss_reason": None,
                    "notes": None,
                }
            ],
            "tank_major_plus": [
                {
                    "event_id": e["event_id"],
                    "class": e["class"],
                    "sent": e["sent"],
                    "statement": e["statement"],
                }
                for e in events
                if e["class"] in ("MAJOR", "STRUCTURAL_SHOCK")
            ],
        },
    )
    if with_outcomes:
        c = tank_container()
        with open(os.path.join(d, "outcomes.jsonl"), "w") as fh:
            for e in events:
                for alert_id in e["alerts"] or []:
                    out = subprocess.run(
                        ["docker", "exec", c, "tank", "review", "show", alert_id],
                        capture_output=True,
                        text=True,
                    )
                    fh.write(
                        json.dumps(
                            {
                                "alert_id": alert_id,
                                "exit": out.returncode,
                                "show": json.loads(out.stdout) if out.returncode == 0 else None,
                                "error": out.stderr[-300:] if out.returncode else None,
                            }
                        )
                        + "\n"
                    )
    print(
        json.dumps(
            {"week": week_start.isoformat(), "events": len(events), "dir": d, "written_at": iso(utcnow())}
        )
    )


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--week-start", type=date.fromisoformat, required=True)
    ap.add_argument("--no-outcomes", action="store_true")
    a = ap.parse_args()
    main(a.run, a.week_start, not a.no_outcomes)
