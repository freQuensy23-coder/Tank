"""V0-H daily review (03 §11.5, 01 §10): freeze the previous UTC day's cards and prepare the review.

Steps for day D (reviewing cards generated in [D-1 00:00, D 00:00)):
  1. freeze: copy every alert version (card text, struct, evidence bundle id) and its delivery ledger rows
     into <run>/days/<D-1>/cards.jsonl with a sha256 receipt (the DB rows are append-only already;
     the frozen copy is what the reviewer signs off).
  2. sheet: write review.md (one block per card with the §7.5 wording checklist) and checks-template.json
     (one row per alert version / MAJOR+ event with the review fields of `tank review metrics`, all null).
  3. merge: after the reviewer fills checks-template.json (booleans + reviewer + reason), `merge` appends
     the filled rows to <run>/checks.json, rejecting nulls and duplicates.
  4. metrics: cumulative S1–S9 from the shadow start with `tank review metrics` inside the runtime container.

Usage:
  python3 shadow_daily.py prepare --run /srv/tank/data/qa/v0h-<start> --day 2026-10-22
  python3 shadow_daily.py merge   --run ... --day 2026-10-22
  python3 shadow_daily.py metrics --run ... --start 2026-10-21T00:00:00Z --until 2026-10-22T00:00:00Z
Labels are written with the product CLI (printed by `prepare`): tank review label <alert_id> CATCH|LATE|FALSE_ALARM|NOISE --reviewer NAME
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import iso, query, utcnow, write_json

ALERT_FIELDS = [
    "state_overstated",
    "usd_oi_new_positions",
    "accumulation_without_checks",
    "reproducible_verdict",
]
EVENT_FIELDS = ["secondary_exists", "secondary_accepted"]
WORDING = [
    "L1 no market-wide/broad/unqualified buying-selling unless spot class BROAD with >= 2 complete venues; single venue named",
    "L2 no leverage/new-position/OI-build language unless oi_class REAL_NATIVE; PRICE_MARKUP stated as price markup",
    "L3 live/launched/adopted/integrated only at stage >= LIVE, approved only >= APPROVED, unverified stages say unverified",
    "L4 token-economics claims only with capture DIRECT or INDIRECT with mechanism evidence; no volume/TVL as capture",
    "L5 liquidation totals market-wide only if every venue COMPLETE",
    "L6 no cross-window trend claim without the cohort-consistent flag",
    "L7 no probability language, no price range in a scenario",
    "L8 every number has a window and unit",
    "L9 no missing value printed as 0 (n/a with reason)",
    "L10 new/fresh/just/breaking only for freshness FRESH or NORMAL",
    "L11 accumulation only for move class SPOT_LED_ACCUMULATION",
    "L12 price up/flat with spot SELL: no weak/bearish, no absorption unless ABSORPTION CONFIRMED, alternatives printed",
    "VERIFIED / NOT VERIFIED line matches the stored claims (verified with source, unverified with what is missing)",
    "<= 3 token blocks, <= 3,500 characters; an UPDATE replies to the first card's message",
    "Delivery ledger: GENERATED -> SENT with message_ref; FAILED after retries has a DELIVERY_FAILED ops notice",
]

ALERTS = """
SELECT al.document AS alert, e.statement, e.known_to_us_at, e.public_at, e.freshness, e.event_state,
       (SELECT a.class FROM evt.ems_assessments a WHERE a.ems_id = e.current_ems_id) AS class,
       (SELECT coalesce(json_agg(d.document ORDER BY d.at, d.id), '[]'::json) FROM out.deliveries d
         WHERE d.alert_id = al.alert_id AND d.version = al.version) AS deliveries,
       (SELECT coalesce(json_agg(r.document ORDER BY r.at), '[]'::json) FROM qa.reviews r
         WHERE r.subject = al.alert_id) AS reviews
  FROM out.alerts al JOIN evt.events e ON e.event_id = al.event_id
 WHERE al.generated_at >= :'since' AND al.generated_at < :'until'
 ORDER BY al.generated_at, al.alert_id, al.version
"""
SILENT_MAJOR = """
SELECT e.event_id, e.statement, e.known_to_us_at, a.class
  FROM evt.events e JOIN evt.ems_assessments a ON a.ems_id = e.current_ems_id
 WHERE e.known_to_us_at >= :'since' AND e.known_to_us_at < :'until' AND e.merged_into IS NULL
   AND a.class IN ('MAJOR', 'STRUCTURAL_SHOCK')
   AND NOT EXISTS (SELECT 1 FROM out.alerts al WHERE al.event_id = e.event_id)
"""


def day_dir(run, d):
    path = os.path.join(run, "days", d.isoformat())
    os.makedirs(path, exist_ok=True)
    return path


def prepare(run, day):
    reviewed = day - timedelta(days=1)
    since, until = reviewed.isoformat() + "T00:00:00Z", day.isoformat() + "T00:00:00Z"
    rows = query(ALERTS, {"since": since, "until": until})
    silent = query(SILENT_MAJOR, {"since": since, "until": until})
    d = day_dir(run, reviewed)
    cards = os.path.join(d, "cards.jsonl")
    with open(cards, "w") as fh:
        for r in rows:
            fh.write(json.dumps(r, sort_keys=True, default=str) + "\n")
    template, sheet, label_cmds, majors = (
        [],
        [
            f"# Daily review — cards of {reviewed} (UTC)",
            "",
            f"Cards/updates: {len(rows)}. MAJOR+ events without a card: {len(silent)}.",
            "",
            "Checklist per card (03 §7.5, §7.6):",
            "",
        ]
        + [f"- {w}" for w in WORDING]
        + [""],
        [],
        {},
    )
    for r in rows:
        a = r["alert"]
        sent = [x for x in r["deliveries"] if x.get("status") == "SENT"]
        failed = [x for x in r["deliveries"] if x.get("status") == "FAILED"]
        labelled = [x.get("label") for x in r["reviews"] if (x.get("version") or 1) == a["version"]]
        sheet += [
            f"## {a['alert_id']} v{a['version']} {a['kind']} — {r['class']} — {r['statement']}",
            "",
            f"- event {a['event_id']} state {r['event_state']} freshness {r['freshness']} public_at {r['public_at']} known_to_us_at {r['known_to_us_at']}",
            f"- generated {a['generated_at']} · sent {[x.get('at') for x in sent]} message_ref {[x.get('message_ref') for x in sent]} · failed attempts {len(failed)}",
            f"- evidence bundle {a.get('evidence_bundle_id')} · model {a.get('model_id')} · prompt {a.get('prompt_version')} · config {a.get('config_version')}",
            f"- existing labels: {labelled or 'none'}",
            "",
            "```",
            a.get("card_text") or "",
            "```",
            "",
        ]
        template.append(
            {
                "alert_id": a["alert_id"],
                "version": a["version"],
                "reviewer": None,
                "reason": None,
                **{k: None for k in ALERT_FIELDS},
            }
        )
        if not labelled:
            label_cmds.append(
                f"tank review label {a['alert_id']} <CATCH|LATE|FALSE_ALARM|NOISE> --version {a['version']} --reviewer <name> --notes '<why>'"
            )
        if r["class"] in ("MAJOR", "STRUCTURAL_SHOCK"):
            majors[a["event_id"]] = r["statement"]
    for event_id in majors:
        template.append(
            {"event_id": event_id, "reviewer": None, "reason": None, **{k: None for k in EVENT_FIELDS}}
        )
    if silent:
        sheet += ["## MAJOR+ events with no card (label CORRECT_SILENCE or MISS)", ""]
        for s in silent:
            sheet.append(f"- {s['event_id']} {s['class']} {s['known_to_us_at']} — {s['statement']}")
            label_cmds.append(
                f"tank review label {s['event_id']} <CORRECT_SILENCE|MISS> [--miss-reason R] --reviewer <name> --notes '<why>'"
            )
        sheet.append("")
    sheet += ["## Label commands (run in the tank container)", "", "```"] + label_cmds + ["```", ""]
    open(os.path.join(d, "review.md"), "w").write("\n".join(sheet))
    write_json(os.path.join(d, "checks-template.json"), template)
    receipt = {
        "day": reviewed.isoformat(),
        "frozen_at": iso(utcnow()),
        "cards": len(rows),
        "silent_major": len(silent),
        "files": {
            f: hashlib.sha256(open(os.path.join(d, f), "rb").read()).hexdigest()
            for f in ("cards.jsonl", "review.md", "checks-template.json")
        },
    }
    write_json(os.path.join(d, "freeze-receipt.json"), receipt)
    print(json.dumps(receipt, indent=2))


def merge(run, day):
    reviewed = day - timedelta(days=1)
    d = day_dir(run, reviewed)
    filled = json.load(open(os.path.join(d, "checks-template.json")))
    path = os.path.join(run, "checks.json")
    cumulative = json.load(open(path)) if os.path.exists(path) else []
    keys = {(r.get("event_id", r.get("alert_id")), r.get("version")) for r in cumulative}
    added, skipped = [], []
    for row in filled:
        clean = {k: v for k, v in row.items() if v is not None}
        fields = set(clean) & set(ALERT_FIELDS + EVENT_FIELDS)
        key = (row.get("event_id", row.get("alert_id")), row.get("version"))
        if not fields or not clean.get("reviewer") or not clean.get("reason") or key in keys:
            skipped.append(key)
            continue
        if any(type(clean[k]) is not bool for k in fields):
            raise SystemExit(f"non-boolean review value in {key}")
        cumulative.append(clean)
        keys.add(key)
        added.append(key)
    write_json(path, cumulative)
    print(
        json.dumps(
            {"added": len(added), "skipped_unfilled_or_duplicate": [list(k) for k in skipped]}, indent=2
        )
    )


def tank_container():
    out = subprocess.run(
        [
            "docker",
            "ps",
            "--filter",
            "label=com.docker.compose.project=tank",
            "--filter",
            "label=com.docker.compose.service=tank",
            "--format",
            "{{.Names}}",
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    if len(out) != 1:
        raise SystemExit(f"expected exactly one running tank runtime container, found {out}")
    return out[0]


def metrics(run, start, until):
    c = tank_container()
    checks = os.path.join(run, "checks.json")
    args = ["docker", "exec", c, "tank", "review", "metrics", "--since", start, "--until", until]
    if os.path.exists(checks):
        subprocess.run(["docker", "cp", checks, f"{c}:/tmp/v0h-checks.json"], check=True)
        args += ["--checks-file", "/tmp/v0h-checks.json"]
    out = subprocess.run(args, capture_output=True, text=True, check=False)
    if out.returncode != 0:
        raise SystemExit("tank review metrics failed: " + out.stderr[-800:])
    result = json.loads(out.stdout)
    path = os.path.join(run, "metrics", f"S1-S9-until-{until[:10]}.json")
    write_json(path, result)
    lines = [
        f"# S1–S9 {start} → {until}",
        "",
        "| Metric | value | eligible | coverage | status |",
        "|---|---:|---:|---:|---|",
    ]
    for name, m in result.get("metrics", {}).items():
        if name == "S1":
            for sub, v in m.items():
                lines.append(
                    f"| S1 {sub} | {v.get('seconds')} s | {v.get('eligible')} | {v.get('coverage')} | {v.get('status')} |"
                )
        else:
            lines.append(
                f"| {name} | {m.get('value')} | {m.get('eligible')} | {m.get('coverage')} | {m.get('status')} |"
            )
    open(path.replace(".json", ".md"), "w").write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["prepare", "merge", "metrics"])
    ap.add_argument("--run", required=True)
    ap.add_argument("--day", type=date.fromisoformat, default=None)
    ap.add_argument("--start")
    ap.add_argument("--until")
    a = ap.parse_args()
    if a.step == "metrics":
        metrics(a.run, a.start, a.until or iso(utcnow()))
    else:
        (prepare if a.step == "prepare" else merge)(a.run, a.day or utcnow().date())
