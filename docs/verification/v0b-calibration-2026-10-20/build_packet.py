"""Rebuild the frozen 1403-item V0-B set in historical-clock order (evidence tooling, not product code).

Inputs (frozen, unchanged): labels/scored dataset GLOBAL-SOURCE-1403-20261019 (provenance-corrected v1),
raw items (sha e4165435...), reviewed state catalog (sha c6edc592...), clocks.json, authored-statements.json.
"""
import json, pickle, hashlib, sys
W = "/srv/tank-work/logs/tank-calibration/"
out = sys.argv[1]
d = pickle.load(open(W + "ds.pkl", "rb"))
lab = {r["item_id"]: r for r in d["items"]}
raw = {json.loads(l)["item_id"]: json.loads(l) for l in open(W + "inputs/raw-items.publisher-enriched.1403.jsonl")}
CL = json.load(open(W + "inputs/clocks.json"))
R = pickle.load(open(W + "rebuild.pkl", "rb"))
C = pickle.load(open(W + "cands.pkl", "rb"))
AUTH = json.load(open(W + "inputs/authored-statements.json"))
cat = json.load(open(W + "inputs/state-catalog.frozen.v1.json"))["catalogs"]
ev = {e["event_id"]: e for e in cat["events"].values()}
KEEP = ("item_id", "source_id", "source", "source_name", "author", "author_role", "author_organization",
        "monitored_author", "reposted_by", "subtype", "title", "text", "quoted", "url", "published_at",
        "observed_at", "raw_sha256", "content_hash", "external_id")
act = {a["item_id"]: a["event_id"] for a in R["activity"]}
events, unscored_placeholder = [], []
for eid, e in R["events"].items():
    g = e["group"]
    if g.startswith("SOLO-"):
        continue  # no extracted event in the reviewed catalog: the session opens, nothing becomes a candidate
    base = ev[g]
    if e["kind"] == "CATALOG":
        statement, basis = base["statement"], "REVIEWED_CATALOG"
    elif eid in AUTH:
        statement, basis = AUTH[eid], "AUTHORED_FROM_OPENING_ITEM"
    else:
        statement, basis = raw[e["opened_by"]]["title"], "OPENING_ITEM_TITLE_NEVER_CANDIDATE"
        unscored_placeholder.append(eid)
    events.append({"event_id": eid, "statement": statement, "statement_basis": basis,
                   "entities": base["entities"], "tokens_mentioned": base.get("tokens_mentioned", []),
                   "event_state": base["event_state"], "reviewed_group_event_id": g, "opened_by": e["opened_by"]})
event_ids = {e["event_id"] for e in events}
items = []
for r in sorted(d["items"], key=lambda r: r["raw_index"]):
    if not r["relation_scorable"]:
        continue  # two items masked by the prior routing adjudication
    i = r["item_id"]
    eid = act.get(i)
    items.append({"item_id": i, "source_id": r["source_id"], "clock": CL[i]["clock"], "clock_basis": CL[i]["basis"],
                  "relevant": r["relevant"], "event_id": eid if eid in event_ids else None,
                  "label_rationale": r.get("rationale"),
                  "source_item": {k: raw[i].get(k) for k in KEEP if raw[i].get(k) is not None}})
packet = {
    "dataset_id": "V0B-HISTORICAL-CLOCK-1401-20261020",
    "status": "FROZEN",
    "labeler": d["labeler"] + "; historical-clock rebuild and 72 opening-item statements by tank-calibration (Claude) 2026-10-20",
    "labeled_at": "2026-10-20T00:00:00Z",
    "routing_window_days": 3,
    "provenance": {
        "successor_of": {"dataset_id": d["dataset_id"], "path": "docs/verification/current-jev-quality-audit-2026-10-20/raw/scored-dataset.provenance-corrected.frozen.v1.json",
                          "raw_sha256": d["score_provenance"]["raw_sha256"], "labels_sha256": d["score_provenance"]["labels_sha256"],
                          "history_sha256": d["score_provenance"]["history_sha256"]},
        "relevance_labels": "unchanged",
        "event_groups": "unchanged reviewed groups (catalog first_known_item_id + expected_event_id)",
        "corrections": [
            "Clock = publication time (published_at; S25 atom entry <updated>; S05 press-release URL date), not the 18-19 Oct backfill observation time, so the 3-day routing window (FR-E-02) means what it means online.",
            "History replayed in clock order: the earliest item of a reviewed group opens its event; later items within 3 days of the event's last item join it; after a longer gap the next item opens a new event (FR-E-02).",
            "Events whose opening item changed get a statement written from that opening item only; stage, entities and tokens stay those of the reviewed group event.",
            "Items without a reviewed event (no extracted fact) open a session but create no candidate event (03 §3.1).",
            "Two items masked by the prior routing adjudication are excluded.",
        ],
        "never_candidate_placeholder_events": len(unscored_placeholder),
    },
    "items": items,
    "events": events,
}
blob = json.dumps(packet, ensure_ascii=False, indent=0).encode()
open(out, "wb").write(blob)
json.dump(unscored_placeholder, open(out + ".placeholders.json", "w"))
print(len(items), len(events), len(unscored_placeholder), hashlib.sha256(blob).hexdigest())
