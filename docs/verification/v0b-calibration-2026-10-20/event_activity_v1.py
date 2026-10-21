import json, pickle, collections
from datetime import datetime, timedelta
W="/srv/tank-work/logs/tank-calibration/"
d=pickle.load(open(W+"ds.pkl","rb")); it=d["items"]; by={r["item_id"]:r for r in it}
cat=json.load(open(W+"inputs/state-catalog.frozen.v1.json"))["catalogs"]
ev=cat["events"]; ref=cat["reference_evidence"]; ses=cat["sessions"]
def T(s): return datetime.fromisoformat(s.replace("Z","+00:00")) if s else None
print("pub missing",sum(not r.get("published_at") for r in it))
print("sources missing pub",collections.Counter(r["source_id"] for r in it if not r.get("published_at")))
# event -> origin item
origin={}
for k,e in ev.items():
    rr=ref.get(k) or ref.get(e["event_id"])
    origin[e["event_id"]]=rr["first_known_item_id"] if rr else None
print("events",len(ev),"origin missing",sum(v is None for v in origin.values()),"origin not in ds",sum(v not in by for v in origin.values() if v))
# event activity items: origin + repeats labeled to it
act=collections.defaultdict(list)
for eid,o in origin.items():
    if o in by: act[eid].append(o)
for r in it:
    if r["expected_event_id"]: act[r["expected_event_id"]].append(r["item_id"])
def clock(r): return T(r.get("published_at")) or T(r["known_to_us_at"])
def pop(r): return r["relevant"] or not r["ingest_policy"]["requires_filter"]
rows=[r for r in it if pop(r) and r["relation_scorable"]]
def inwin(r,eid,days=3):
    t=clock(r); times=[clock(by[i]) for i in act[eid] if i!=r["item_id"]]
    prior=[x for x in times if x<=t]
    if not prior: return False
    return max(prior)>=t-timedelta(days=days)
lost_true=0; nrows=[]
for r in rows:
    cs=[c for c in r["candidates"] if inwin(r,c["event_id"])]
    t=r["expected_event_id"]
    if t and t in [c["event_id"] for c in r["candidates"]] and t not in [c["event_id"] for c in cs]: lost_true+=1
    nrows.append((r,cs))
print("true event dropped by published-time window",lost_true)
print("cands before",sum(len(r["candidates"]) for r in rows),"after",sum(len(c) for _,c in nrows))
def metrics(c, relabel):
    tp=fp=fn=0
    for r,cs in nrows:
        best=None;bs=-1
        for x in cs:
            if x["score"]>=c and x["score"]>bs: best,bs=x["event_id"],x["score"]
        t=r["expected_event_id"]
        if relabel and t and not inwin(r,t): t=None
        if best is not None:
            if best==t: tp+=1
            else: fp+=1
        if t is not None and best!=t: fn+=1
    return tp,fp,fn
for relabel in (False,True):
  print("relabel",relabel)
  for c in [0.5,0.6,0.67,0.7,0.75,0.8,0.85,0.9]:
    tp,fp,fn=metrics(c,relabel); print(" ",c,tp,fp,fn, round(tp/(tp+fp),3) if tp+fp else None, round(tp/(tp+fn),3))
pickle.dump((origin,dict(act)),open(W+"evact.pkl","wb"))
