"""V0-G end-to-end trace: one live event from its source item to the Telegram card, from stored rows only.

Read-only. Picks the event (or takes --event-id), collects every stored row of the chain
source item -> FILTER_ITEM -> ROUTE_ITEM -> agent session -> tool calls -> Market Truth -> decision
-> alert (card) -> delivery ledger, builds a timeline against the 01 §9 / 03 §3.9 limits and writes an
evidence bundle directory. It never sends, retries or edits anything.

Usage:
  python3 e2e_trace.py --since 2026-10-21T00:00:00Z [--event-id EVT-...] --out /srv/tank/docs/verification/e2e-source-telegram-2026-10-21
"""
import argparse
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import iso, query, seconds, utcnow, write_json  # noqa: E402

CANDIDATES = """
SELECT e.event_id, e.known_to_us_at, min(d.at) AS first_sent_at
  FROM evt.events e
  JOIN out.alerts al ON al.event_id = e.event_id
  JOIN out.deliveries d ON d.alert_id = al.alert_id AND d.version = al.version
 WHERE d.status = 'SENT' AND d.channel = 'telegram:alerts' AND e.known_to_us_at >= :'since'
 GROUP BY e.event_id, e.known_to_us_at ORDER BY bool_or(al.kind = 'FULL') DESC, min(d.at)
"""

ROWS = {
    "event": "SELECT document FROM evt.events WHERE event_id = :'eid'",
    "assessments": "SELECT document FROM evt.ems_assessments WHERE event_id = :'eid' ORDER BY version",
    "observations": "SELECT document FROM evt.event_observations WHERE event_id = :'eid' ORDER BY observed_at",
    "state_changes": "SELECT document FROM evt.state_changes WHERE event_id = :'eid' ORDER BY changed_at",
    "claims": "SELECT document FROM evt.claims WHERE obs_id IN (SELECT obs_id FROM evt.event_observations WHERE event_id = :'eid')",
    "beneficiary_edges": "SELECT document FROM evt.beneficiary_edges WHERE event_id = :'eid'",
    "market_truth": "SELECT document FROM ana.market_truth WHERE event_id = :'eid' ORDER BY cutoff",
    "decisions": "SELECT document FROM ana.decisions WHERE event_id = :'eid' ORDER BY cutoff",
    "calls": "SELECT document FROM ana.calls WHERE event_id = :'eid'",
    "watch_conditions": "SELECT document FROM ana.watch_conditions WHERE event_id = :'eid'",
    "alerts": "SELECT document FROM out.alerts WHERE event_id = :'eid' ORDER BY generated_at, version",
    "deliveries": """SELECT d.document FROM out.deliveries d JOIN out.alerts al
                       ON al.alert_id = d.alert_id AND al.version = d.version
                     WHERE al.event_id = :'eid' ORDER BY d.at, d.id""",
    "sessions": "SELECT document - 'message_log' AS document FROM ops.agent_sessions WHERE event_id = :'eid'",
    "hot_tokens": "SELECT to_jsonb(h) AS document FROM mkt.hot_tokens h WHERE event_id = :'eid'",
}
ITEMS = """
SELECT i.item_id, i.source_id, i.feed, i.url, i.title, i.published_at, i.published_prec, i.observed_at,
       i.raw_sha256, i.content_hash, i.relevance_decision, i.relevance_p, i.route_decision, i.route_event_id, i.route_p
  FROM ingest.source_items i
 WHERE i.item_id IN (SELECT item_id FROM evt.event_observations WHERE event_id = :'eid')
    OR i.item_id IN (SELECT opened_by_item FROM ops.agent_sessions WHERE event_id = :'eid')
 ORDER BY i.observed_at
"""
JOBS = """
SELECT job_id, type, status, attempts, payload, created_at, run_after, finished_at, last_error
  FROM ops.jobs
 WHERE payload->>'event_id' = :'eid'
    OR payload->>'item_id' IN (SELECT item_id FROM evt.event_observations WHERE event_id = :'eid')
    OR payload->>'alert_id' IN (SELECT alert_id FROM out.alerts WHERE event_id = :'eid')
 ORDER BY created_at
"""
TOOLS = """
SELECT call_id, session_id, tool, status, requested_at, answered_at,
       left(input::text, 400) AS input_head, length(result::text) AS result_len
  FROM ops.tool_calls
 WHERE session_id IN (SELECT session_id FROM ops.agent_sessions WHERE event_id = :'eid')
 ORDER BY requested_at
"""
# out.evidence_bundles.bundle_id is "EVB-<sha>"; alerts reference the bare sha (also the storage key).
BUNDLES = """SELECT storage_key, bundle_id, document->'payload' AS payload, document->>'sha256' AS sha FROM out.evidence_bundles
 WHERE storage_key IN (SELECT evidence_bundle_id FROM out.alerts WHERE event_id = :'eid')
    OR bundle_id IN (SELECT 'EVB-' || evidence_bundle_id FROM out.alerts WHERE event_id = :'eid')"""


def synthetic_item_ids():
    """Items the lead injected for synthetic E2E runs (listed in docs/verification/e2e-synthetic-*/README.md).

    V0-G needs a live event, so a chain opened by one of these is reported as synthetic, never as a pass."""
    import glob
    import re

    ids = set()
    for readme in glob.glob("/srv/tank/docs/verification/e2e-synthetic-*/README.md"):
        ids |= set(re.findall(r"ITM-[0-9A-Z]{26}", open(readme).read()))
    return ids


def canonical_sha(value):
    try:
        sys.path.insert(0, "/srv/tank/src")
        from tank.util import sha256  # same canonical JSON as the producer
        return sha256(value)
    except Exception:  # noqa: BLE001 -- fall back to a plain canonical form, flagged below
        return "UNAVAILABLE:" + hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def trace(event_id):
    p = {"eid": event_id}
    rows = {k: [r["document"] for r in query(sql, p)] for k, sql in ROWS.items()}
    rows["items"] = query(ITEMS, p)
    rows["jobs"] = query(JOBS, p)
    rows["tool_calls"] = query(TOOLS, p)
    bundles = query(BUNDLES, p)
    event = rows["event"][0]
    known = event["known_to_us_at"]
    classes = {a.get("class") for a in rows["assessments"]}
    opening = rows["items"][0] if rows["items"] else None

    timeline = []

    def at(ts, step, detail=""):
        if ts:
            timeline.append({"at": ts, "step": step, "detail": detail})

    for it in rows["items"]:
        at(it["published_at"], "published", f"{it['source_id']} {it['url']}")
        at(it["observed_at"], "observed", f"{it['source_id']} {it['item_id']} relevance={it['relevance_decision']} route={it['route_decision']}")
    for j in rows["jobs"]:
        at(j["finished_at"], f"job {j['type']} {j['status']}", j["job_id"])
    for c in rows["tool_calls"]:
        at(c["answered_at"] or c["requested_at"], f"tool {c['tool']} {c['status']}", c["call_id"])
    for m in rows["market_truth"]:
        at(m.get("computed_at") or m.get("cutoff"), "market_truth", f"{m.get('token_id')} move_class={m.get('move_class')}")
    for d in rows["decisions"]:
        at(d.get("cutoff"), "decision", f"{d.get('token_id')} action={d.get('action')} thesis={d.get('thesis_grade')} trade={d.get('trade_grade')}")
    for a in rows["alerts"]:
        at(a["generated_at"], f"alert {a['kind']} v{a['version']}", a["alert_id"])
    for d in rows["deliveries"]:
        at(d["at"], f"delivery {d['status']} {d['channel']}", f"message_ref={d.get('message_ref')} attempt={d.get('attempt')}")
    timeline.sort(key=lambda r: r["at"])
    for r in timeline:
        r["since_known_s"] = seconds(r["at"], known)

    record_event = next((c for c in rows["tool_calls"] if c["tool"] == "record_event" and c["status"] == "DONE"), None)
    first_alert = rows["alerts"][0] if rows["alerts"] else None
    full = next((a for a in rows["alerts"] if a["kind"] == "FULL"), None)
    sent = [d for d in rows["deliveries"] if d["status"] == "SENT" and d["channel"] == "telegram:alerts"]

    def sent_for(alert):
        hits = [d["at"] for d in sent if alert and d["alert_id"] == alert["alert_id"] and d["version"] == alert["version"]]
        return min(hits) if hits else None

    bundle_checks = []
    for b in bundles:
        digest = canonical_sha(b["payload"]) if b["payload"] is not None else None
        referenced = {a.get("evidence_bundle_id") for a in rows["alerts"]}
        bundle_checks.append({"bundle_id": b["bundle_id"], "stored_sha": b["sha"], "recomputed": digest,
                              "valid": digest == b["sha"] and b["sha"] in referenced})

    stages = {
        "source_item_relevant": bool(opening and opening["relevance_decision"] in ("ALWAYS", "SEND")),
        "filter_job_done": any(j["type"] == "FILTER_ITEM" and j["status"] == "DONE" for j in rows["jobs"])
        or bool(opening and opening["relevance_decision"] == "ALWAYS"),
        "route_job_done": any(j["type"] == "ROUTE_ITEM" and j["status"] == "DONE" for j in rows["jobs"]),
        "agent_session": bool(rows["sessions"]),
        "record_event": record_event is not None,
        "assessment": bool(rows["assessments"]),
        "market_truth_tool_done": any(c["tool"] == "get_market_truth" and c["status"] == "DONE" for c in rows["tool_calls"]),
        "market_truth_row": bool(rows["market_truth"]),
        "decision": bool(rows["decisions"]),
        "card": first_alert is not None,
        "delivery_sent_with_message_ref": any(d.get("message_ref") for d in sent),
        "evidence_bundle_valid": bool(bundle_checks) and all(b["valid"] for b in bundle_checks),
    }
    timings = {
        "item_to_event_s": seconds(record_event and record_event["answered_at"], opening and opening["observed_at"]),
        "known_to_first_card_generated_s": seconds(first_alert and first_alert["generated_at"], known),
        "known_to_first_card_sent_s": seconds(sent_for(first_alert), known),
        "known_to_full_card_generated_s": seconds(full and full["generated_at"], known),
        "known_to_full_card_sent_s": seconds(sent_for(full), known),
        "delivery_after_generation_s": [seconds(sent_for(a), a["generated_at"]) for a in rows["alerts"]],
        "public_to_known_s": seconds(known, event.get("public_at")),
    }
    limits = {
        "item_to_event_s": 420,
        "known_to_first_card_generated_s": 180 if "STRUCTURAL_SHOCK" in classes else None,
        "known_to_full_card_generated_s": 480 if classes & {"MAJOR", "STRUCTURAL_SHOCK"} else None,
    }
    within = {k: (timings[k] is not None and timings[k] <= v) for k, v in limits.items() if v is not None}
    within["delivery_after_generation_s"] = all(v is not None and v <= 10 for v in timings["delivery_after_generation_s"]) \
        if timings["delivery_after_generation_s"] else False
    # An operator step is an agent message an operator injected (kind RECOVERY / note "Operator …"), or a job
    # payload that names an operator. Automatic reasons (e.g. late REACTION_CAPTURE) are not manual steps.
    def operator_job(j):
        p = j.get("payload") or {}
        return bool(p.get("operator")) or (j["type"] == "AGENT_MESSAGE" and (
            p.get("kind") == "RECOVERY" or str(p.get("note", "")).lower().startswith("operator")))
    manual = [j for j in rows["jobs"] if operator_job(j)]
    agent_rules = sorted({(a.get("model_id"), a.get("prompt_version")) for a in rows["alerts"]}, key=str)
    return {
        "event_id": event_id,
        "statement": event.get("statement"),
        "classes": sorted(c for c in classes if c),
        "known_to_us_at": known,
        "stages": stages,
        "chain_complete": all(stages.values()),
        "timings": timings,
        "limits_s": limits,
        "within_limits": within,
        "operator_or_manual_jobs": [j["job_id"] for j in manual],
        "recovered_by_operator": bool(manual),
        "synthetic_injection": bool(opening and opening["item_id"] in synthetic_item_ids()),
        "operator_steps": [{"job_id": j["job_id"], "type": j["type"], "kind": (j.get("payload") or {}).get("kind"),
                            "created_at": j["created_at"], "note": str((j.get("payload") or {}).get("note", ""))[:300]} for j in manual],
        "agent_rules_in_force": [{"model_id": m, "prompt_version": pv} for m, pv in agent_rules],
        "evidence_bundles": bundle_checks,
        "timeline": timeline,
        "rows": rows,
    }


def render(t):
    out = [f"# E2E trace {t['event_id']}", "", f"Statement: {t['statement']}", f"Classes: {', '.join(t['classes'])}",
           f"known_to_us_at: {t['known_to_us_at']}", "", "## Chain", ""]
    out += [f"- {'OK ' if ok else 'MISSING'} {k}" for k, ok in t["stages"].items()]
    out += ["", "## Timings (s)", ""]
    out += [f"- {k}: {v}" + (f" (limit {t['limits_s'][k]})" if t["limits_s"].get(k) else "") for k, v in t["timings"].items()]
    out += ["", f"Within limits: {json.dumps(t['within_limits'])}",
            f"Operator/manual steps in chain: {t['operator_or_manual_jobs'] or 'none'}"
            + (" — RECOVERED BY OPERATOR (not a no-manual-step V0-G pass)" if t["recovered_by_operator"] else ""),
            "Opening item: " + ("SYNTHETIC injection (lead E2E test), not a live event" if t["synthetic_injection"] else "live source item"),
            f"Agent rules in force (model, prompt version): {t['agent_rules_in_force']}", "", "## Timeline", "",
            "| at | +s from known | step | detail |", "|---|---:|---|---|"]
    out += [f"| {r['at']} | {r['since_known_s']:.1f} | {r['step']} | {r['detail']} |" for r in t["timeline"]]
    return "\n".join(out) + "\n"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=True)
    ap.add_argument("--event-id")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    ids = [a.event_id] if a.event_id else [r["event_id"] for r in query(CANDIDATES, {"since": a.since})]
    if not ids:
        print(json.dumps({"status": "NO_EVENT_WITH_SENT_CARD_SINCE", "since": a.since}))
        sys.exit(2)
    if not a.event_id:  # watcher mode: trace every new candidate once
        ids = [i for i in ids if not os.path.exists(os.path.join(a.out, i, "trace.json"))] or []
        if not ids:
            print(json.dumps({"status": "NO_NEW_EVENT_WITH_SENT_CARD", "since": a.since}))
            sys.exit(2)
    passed = False
    for event_id in ids:
        t = trace(event_id)
        d = os.path.join(a.out, event_id)
        write_json(os.path.join(d, "trace.json"), {k: v for k, v in t.items() if k != "rows"})
        for name, rows in t["rows"].items():
            with open(os.path.join(d, f"{name}.jsonl"), "w") as fh:
                for r in rows:
                    fh.write(json.dumps(r, sort_keys=True, default=str) + "\n")
        with open(os.path.join(d, "trace.md"), "w") as fh:
            fh.write(render(t))
        write_json(os.path.join(d, "receipt.json"), {"written_at": iso(utcnow()), "files": {
            f: hashlib.sha256(open(os.path.join(d, f), "rb").read()).hexdigest() for f in sorted(os.listdir(d))}})
        print(render(t)[:2000])
        passed = passed or (t["chain_complete"] and all(t["within_limits"].values()) and not t["recovered_by_operator"]
                            and not t["synthetic_injection"])
    sys.exit(0 if passed else 1)
