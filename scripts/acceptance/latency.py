"""Speed acceptance (01 §9, 03 §3.9) measured from timestamps the deployed stack stored.

Read-only. Targets:
  item -> event                    p95 <= 7 min (observed_at -> record_event answered / ROUTE_ITEM ADDED finished)
  STRUCTURAL SHOCK first card      <= 180 s      (known_to_us_at -> first alert generated_at; sent also reported)
  MAJOR+ FULL card                 <= 480 s      (known_to_us_at -> first FULL alert generated_at; sent also reported)
  update after decisive condition  <= 60 s       (watch_conditions.fired_at -> next UPDATE alert generated_at)
  delivery after generation        <= 10 s       (alert generated_at -> first SENT ledger row)

Usage: python3 latency.py --since 2026-10-21T00:00:00Z --until 2026-10-24T00:00:00Z --out DIR
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import json  # noqa: E402

from common import iso, parse, query, seconds, summary, utcnow, write_json  # noqa: E402

MAJOR_PLUS = ("MAJOR", "STRUCTURAL_SHOCK")

ITEMS = """
SELECT i.item_id, i.source_id, i.observed_at, i.relevance_decision, i.route_decision, i.route_event_id,
       s.session_id, s.status AS session_status,
       (SELECT min(c.answered_at) FROM ops.tool_calls c
         WHERE c.session_id = s.session_id AND c.tool = 'record_event' AND c.status = 'DONE') AS recorded_at,
       (SELECT max(c.answered_at) FROM ops.tool_calls c WHERE c.session_id = s.session_id) AS last_tool_at,
       (SELECT min(j.finished_at) FROM ops.jobs j
         WHERE j.type = 'ROUTE_ITEM' AND j.payload->>'item_id' = i.item_id AND j.status = 'DONE') AS routed_at,
       (SELECT min(j.finished_at) FROM ops.jobs j
         WHERE j.type = 'FILTER_ITEM' AND j.payload->>'item_id' = i.item_id AND j.status = 'DONE') AS filtered_at
  FROM ingest.source_items i
  LEFT JOIN ops.agent_sessions s ON s.opened_by_item = i.item_id
 WHERE i.observed_at >= :'since' AND i.observed_at < :'until'
   AND i.relevance_decision IN ('ALWAYS', 'SEND')
"""

EVENTS = """
SELECT e.event_id, e.known_to_us_at, e.lifecycle,
       array(SELECT DISTINCT a.class FROM evt.ems_assessments a WHERE a.event_id = e.event_id) AS classes,
       (SELECT min(generated_at) FROM out.alerts al WHERE al.event_id = e.event_id) AS first_alert_at,
       (SELECT min(generated_at) FROM out.alerts al WHERE al.event_id = e.event_id AND al.kind = 'FULL') AS first_full_at,
       (SELECT min(d.at) FROM out.deliveries d JOIN out.alerts al ON al.alert_id = d.alert_id AND al.version = d.version
         WHERE al.event_id = e.event_id AND d.status = 'SENT' AND d.channel = 'telegram:alerts') AS first_sent_at,
       (SELECT min(d.at) FROM out.deliveries d JOIN out.alerts al ON al.alert_id = d.alert_id AND al.version = d.version
         WHERE al.event_id = e.event_id AND al.kind = 'FULL' AND d.status = 'SENT'
           AND d.channel = 'telegram:alerts') AS first_full_sent_at
  FROM evt.events e
 WHERE e.known_to_us_at >= :'since' AND e.known_to_us_at < :'until' AND e.merged_into IS NULL
"""

CONDITIONS = """
SELECT w.cond_id, w.event_id, w.token_id, w.fired_at,
       (SELECT min(al.generated_at) FROM out.alerts al
         WHERE al.event_id = w.event_id AND al.kind = 'UPDATE' AND al.generated_at >= w.fired_at) AS update_at,
       (SELECT count(*) FROM ops.jobs j
         WHERE j.type = 'AGENT_MESSAGE' AND j.payload->>'event_id' = w.event_id
           AND j.created_at < w.fired_at AND coalesce(j.finished_at, now()) > w.fired_at
           AND coalesce(j.payload->>'kind', '') <> 'CONDITION_FIRED') AS turns_running_at_fire
  FROM ana.watch_conditions w
 WHERE w.status = 'FIRED' AND w.fired_at >= :'since' AND w.fired_at < :'until'
"""

DELIVERIES = """
SELECT al.alert_id, al.version, al.kind, al.event_id, al.generated_at,
       (SELECT min(d.at) FROM out.deliveries d WHERE d.alert_id = al.alert_id AND d.version = al.version
         AND d.status = 'SENT') AS sent_at,
       (SELECT count(*) FROM out.deliveries d WHERE d.alert_id = al.alert_id AND d.version = al.version
         AND d.status = 'FAILED') AS failed_attempts
  FROM out.alerts al
 WHERE al.generated_at >= :'since' AND al.generated_at < :'until'
"""


def fixture_windows(path):
    """[(start, end)] of native fixture runs (run_fixtures.py), to read latency with and without fixture load."""
    if not path or not os.path.exists(path):
        return []
    starts, out = {}, []
    for line in open(path):
        r = json.loads(line)
        if r["event"] == "START":
            starts[r["fixture_id"]] = r["at"]
        elif r["event"] == "END" and r["fixture_id"] in starts:
            out.append((starts.pop(r["fixture_id"]), r["at"]))
    out += [(a, iso(utcnow())) for a in starts.values()]
    return out


def overlaps(start, end, windows):
    return any(start is not None and end is not None and parse(start) < parse(b) and parse(a) < parse(end)
               for a, b in windows)


def measure(since, until, windows=()):
    params = {"since": since, "until": until}
    items = query(ITEMS, params)
    events = query(EVENTS, params)
    conditions = query(CONDITIONS, params)
    alerts = query(DELIVERIES, params)

    item_rows, event_lat, dismissed, pending = [], [], [], []
    for it in items:
        if it["route_decision"] == "ADDED":
            lat, path = seconds(it["routed_at"], it["observed_at"]), "ADDED"
        elif it["route_decision"] == "NEW_SESSION" and it["recorded_at"]:
            lat, path = seconds(it["recorded_at"], it["observed_at"]), "NEW_SESSION"
        elif it["route_decision"] == "NEW_SESSION" and it["session_status"] in ("ENDED", "FAILED"):
            dismissed.append({"item_id": it["item_id"], "source_id": it["source_id"],
                              "verdict_s": seconds(it["last_tool_at"], it["observed_at"]),
                              "session_status": it["session_status"]})
            continue
        else:
            pending.append({"item_id": it["item_id"], "source_id": it["source_id"],
                            "route_decision": it["route_decision"], "session_status": it["session_status"],
                            "filter_s": seconds(it["filtered_at"], it["observed_at"]),
                            "age_s": seconds(until, it["observed_at"])})
            continue
        event_lat.append(lat)
        item_rows.append({"item_id": it["item_id"], "source_id": it["source_id"], "path": path,
                          "during_fixture": overlaps(it["observed_at"], it["recorded_at"] or it["routed_at"], windows),
                          "observed_at": it["observed_at"], "seconds": lat,
                          "filter_s": seconds(it["filtered_at"], it["observed_at"]),
                          "route_s": seconds(it["routed_at"], it["observed_at"])})

    structural, major = [], []
    for ev in events:
        classes = set(ev["classes"] or [])
        row = {"event_id": ev["event_id"], "known_to_us_at": ev["known_to_us_at"], "classes": sorted(classes),
               "first_card_generated_s": seconds(ev["first_alert_at"], ev["known_to_us_at"]),
               "first_card_sent_s": seconds(ev["first_sent_at"], ev["known_to_us_at"]),
               "full_card_generated_s": seconds(ev["first_full_at"], ev["known_to_us_at"]),
               "full_card_sent_s": seconds(ev["first_full_sent_at"], ev["known_to_us_at"])}
        if "STRUCTURAL_SHOCK" in classes:
            structural.append(row)
        if classes & set(MAJOR_PLUS):
            major.append(row)

    cond_rows = [{**c, "update_s": seconds(c["update_at"], c["fired_at"]),
                  "session_idle_at_fire": c["turns_running_at_fire"] == 0} for c in conditions]
    idle = [c for c in cond_rows if c["session_idle_at_fire"]]
    deliveries = [{**a, "delivery_s": seconds(a["sent_at"], a["generated_at"])} for a in alerts]

    metrics = {
        "item_to_event": summary(event_lat, 420, "p95"),
        "item_to_event_without_fixture_load": summary([r["seconds"] for r in item_rows if not r["during_fixture"]], 420, "p95"),
        # Items without an event yet count with their age at the window end: a lower bound, never hidden.
        "item_to_event_with_pending_lower_bound": summary(event_lat + [p["age_s"] for p in pending], 420, "p95"),
        "structural_first_card_generated": summary([r["first_card_generated_s"] for r in structural], 180, "max"),
        "structural_first_card_sent": summary([r["first_card_sent_s"] for r in structural], 190, "max"),
        "major_full_card_generated": summary([r["full_card_generated_s"] for r in major], 480, "max"),
        "major_full_card_sent": summary([r["full_card_sent_s"] for r in major], 490, "max"),
        "update_after_condition_idle": summary([c["update_s"] for c in idle], 60, "max"),
        "update_after_condition_busy": summary([c["update_s"] for c in cond_rows if not c["session_idle_at_fire"]]),
        "delivery_after_generation": summary([d["delivery_s"] for d in deliveries], 10, "max"),
    }
    return {
        "window": {"since": since, "until": until},
        "computed_at": iso(utcnow()),
        "definitions": __doc__.strip().splitlines()[2:8],
        "metrics": metrics,
        "items_dismissed_by_agent": {"count": len(dismissed), "verdict": summary([d["verdict_s"] for d in dismissed])},
        "items_pending_or_unrouted": len(pending),
        "rows": {"items": item_rows, "dismissed": dismissed, "pending": pending, "structural": structural,
                 "major": major, "conditions": cond_rows, "deliveries": deliveries},
    }


def render(result):
    lines = [f"# Tank speed — {result['window']['since']} → {result['window']['until']}", "",
             "| Metric | n | measured | median s | p95 s | max s | target s | status |",
             "|---|---:|---:|---:|---:|---:|---:|---|"]
    for name, m in result["metrics"].items():
        def f(v):
            return "—" if v is None else f"{v:.1f}"
        lines.append(f"| {name} | {m['n']} | {m['measured']} | {f(m['median'])} | {f(m['p95'])} | {f(m['max'])} | "
                     f"{m.get('target_s') or '—'} ({m['statistic']}) | {m.get('status', '—')} |")
    lines += ["", f"Items dismissed by the agent (no event): {result['items_dismissed_by_agent']['count']}; "
              f"still pending/unrouted at computation: {result['items_pending_or_unrouted']}.",
              "Card latencies use `out.alerts.generated_at` and the first SENT row of `out.deliveries`;"
              " `sent` targets add the 10 s delivery budget."]
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=True)
    ap.add_argument("--until", default=None)
    ap.add_argument("--out", default=None, help="directory for latency.json and latency.md")
    ap.add_argument("--fixture-windows", default="/srv/tank/data/qa/fixture-windows.jsonl")
    a = ap.parse_args()
    res = measure(a.since, a.until or iso(utcnow()), fixture_windows(a.fixture_windows))
    md = render(res)
    if a.out:
        write_json(os.path.join(a.out, "latency.json"), res)
        with open(os.path.join(a.out, "latency.md"), "w") as fh:
            fh.write(md)
    print(md)
