import json
import pickle

W = "/srv/tank-work/logs/tank-calibration/"
raw = {
    json.loads(l)["item_id"]: json.loads(l)
    for l in open(W + "inputs/raw-items.publisher-enriched.1403.jsonl")
}
R = pickle.load(open(W + "rebuild.pkl", "rb"))
C = pickle.load(open(W + "cands.pkl", "rb"))
CL = json.load(open(W + "inputs/clocks.json"))
cat = json.load(open(W + "inputs/state-catalog.frozen.v1.json"))["catalogs"]
ev = {e["event_id"]: e for e in cat["events"].values()}
out = []
for eid in sorted(C["newneeded"]):
    e = R["events"][eid]
    r = raw[e["opened_by"]]
    g = ev.get(e["group"])
    out.append(
        {
            "event_id": eid,
            "opened_by": e["opened_by"],
            "source": r["source_id"] + " " + (r.get("source") or ""),
            "author": r.get("author"),
            "clock": CL[e["opened_by"]]["clock"],
            "title": r.get("title"),
            "text": (r.get("text") or "")[:700],
            "group_catalog_statement": g and g["statement"],
            "group_stage": g and g["event_state"],
        }
    )
json.dump(out, open(W + "need_statements.json", "w"), indent=1)
print(len(out))
