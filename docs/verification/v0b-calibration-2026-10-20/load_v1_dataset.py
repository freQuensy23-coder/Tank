import collections
import json
import pickle

P = "/srv/tank/docs/verification/current-jev-quality-audit-2026-10-20/raw/scored-dataset.provenance-corrected.frozen.v1.json"
d = json.load(open(P))
pickle.dump(d, open("/srv/tank-work/logs/tank-calibration/ds.pkl", "wb"))
print(json.dumps(d["score_provenance"], indent=1)[:3000])
print(json.dumps(d["routing_adjudication"], indent=1)[:2000])
it = d["items"]
C = collections.Counter
print("relevant", C(r["relevant"] for r in it))
print("expected", C(r["expected_event_id"] is not None for r in it))
print("xsrc", C(r["cross_source_repeat"] for r in it))
print("scorable", C(r["relation_scorable"] for r in it))
print("split", C(r["split"] for r in it))
print("origin", C(r["origin_dataset"] for r in it))
print("source", sorted(C(r["source_id"] for r in it).items()))
print("policy", C(json.dumps(r["ingest_policy"].get("decision")) for r in it))
print("ncand", C(min(len(r["candidates"]), 10) for r in it))
c = next(r for r in it if r["candidates"])
print(json.dumps({k: v for k, v in c.items() if k not in ("candidates",)}, indent=0)[:2500])
print(json.dumps(c["candidates"][0], indent=0)[:4000])
