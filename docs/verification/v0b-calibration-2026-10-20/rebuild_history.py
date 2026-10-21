import json, pickle, collections, asyncio
from datetime import datetime, timedelta
W="/srv/tank-work/logs/tank-calibration/"
d=pickle.load(open(W+"ds.pkl","rb")); it=d["items"]; lab={r["item_id"]:r for r in it}
raw={json.loads(l)["item_id"]:json.loads(l) for l in open(W+"inputs/raw-items.publisher-enriched.1403.jsonl")}
CL=json.load(open(W+"inputs/clocks.json"))
cat=json.load(open(W+"inputs/state-catalog.frozen.v1.json"))["catalogs"]
ev={e["event_id"]:e for e in cat["events"].values()}
ref={}
for k,e in cat["events"].items():
    rr=cat["reference_evidence"].get(k); ref[e["event_id"]]=rr["first_known_item_id"]
def T(s): return datetime.fromisoformat(s.replace("Z","+00:00"))
def pop(r): return r["relevant"] or not r["ingest_policy"]["requires_filter"]
group_of={}
for eid,o in ref.items(): group_of.setdefault(o,eid)
for r in it:
    if r["expected_event_id"]: group_of[r["item_id"]]=r["expected_event_id"]
# Label rule applied uniformly (the reviewers' own rule in 6 of 9 PRs): a merged PR and its commits are one event.
import collections as _c
_pr=_c.defaultdict(list)
for c in json.load(open(W+"inputs/s25-commits.json")):
    for p in c["pulls"]: _pr[(c["repo"],p)].append(c["item_id"])
pr_merges=[]
for k,members in _pr.items():
    if len(members)<2: continue
    members=sorted(members,key=lambda i:(T(CL[i]["clock"]),lab[i]["raw_index"]))
    target=group_of[members[0]]
    for m in members[1:]:
        if group_of.get(m)!=target: pr_merges.append((k,m,group_of.get(m),target)); group_of[m]=target
print("PR relabels",len(pr_merges))
order=sorted([r for r in it if pop(r) and r["relation_scorable"]],key=lambda r:(T(CL[r["item_id"]]["clock"]),r["raw_index"]))
print("route pop",len(order),"without group",sum(r["item_id"] not in group_of for r in order))
seg={}  # group -> current event dict
events={}; activity=[]; labels={}
for seq,r in enumerate(order):
    i=r["item_id"]; t=T(CL[i]["clock"]); g=group_of.get(i) or ("SOLO-"+i)
    cur=seg.get(g)
    if cur and t-cur["last_at"]<=timedelta(days=3):
        labels[i]={"expected_event_id":cur["event_id"],"cross_source_repeat":r["source_id"] not in cur["sources"]}
        cur["last_at"]=t; cur["sources"].add(r["source_id"]); cur["members"].append(i)
    else:
        n=cur["n"]+1 if cur else 0
        if g in ev and n==0 and ref[g]==i: eid=g; kind="CATALOG"
        else: eid=f"V2EV-{(g if g in ev else i)[-24:]}-{n}"; kind="NEW_OPENER"
        seg[g]={"event_id":eid,"last_at":t,"sources":{r["source_id"]},"n":n,"members":[i]}
        events[eid]={"event_id":eid,"opened_by":i,"group":g,"kind":kind,"known_to_us_at":CL[i]["clock"]}
        labels[i]={"expected_event_id":None,"cross_source_repeat":False}
    activity.append({"seq":seq,"item_id":i,"event_id":seg[g]["event_id"],"at":CL[i]["clock"]})
print(collections.Counter(e["kind"] for e in events.values()))
print("joiners",sum(l["expected_event_id"] is not None for l in labels.values()),"xsrc",sum(l["cross_source_repeat"] for l in labels.values()))
pickle.dump(dict(pr_merges=pr_merges,order=[r["item_id"] for r in order],events=events,activity=activity,labels=labels,group_of=group_of),open(W+"rebuild.pkl","wb"))
