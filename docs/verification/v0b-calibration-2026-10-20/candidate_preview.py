import json, pickle, collections, asyncio, sys
from datetime import datetime, timedelta
sys.path.insert(0,"/srv/tank-work/wt/tank-calibration/src")
from tank.settings import Settings
from tank.agents.pipeline import Pipeline
W="/srv/tank-work/logs/tank-calibration/"
d=pickle.load(open(W+"ds.pkl","rb")); lab={r["item_id"]:r for r in d["items"]}
raw={json.loads(l)["item_id"]:json.loads(l) for l in open(W+"inputs/raw-items.publisher-enriched.1403.jsonl")}
R=pickle.load(open(W+"rebuild.pkl","rb"))
cat=json.load(open(W+"inputs/state-catalog.frozen.v1.json"))["catalogs"]; ev={e["event_id"]:e for e in cat["events"].values()}
s=Settings(config_dir="/srv/tank-work/wt/tank-calibration/config")
import functools
_l=functools.lru_cache(None)(s.load_config)
object.__setattr__(s,"load_config",_l)
p=Pipeline(None,s,http=object())
print("solo relevant",collections.Counter(lab[e["opened_by"]]["relevant"] for e in R["events"].values() if e["group"].startswith("SOLO")))
def ent_ids(e): return {x["entity_id"] if isinstance(x,dict) else x for x in e.get("entities",[])}|set(e.get("tokens_mentioned",[]))
evids={}
for eid,e in R["events"].items():
    g=e["group"]
    if g in ev: evids[eid]=ent_ids(ev[g])
    else: evids[eid]=p.identities(raw[e["opened_by"]])
def T(x): return datetime.fromisoformat(x.replace("Z","+00:00"))
last={}  # event->last at (by seq)
needed=collections.Counter(); pairs=0; per_item={}
act=R["activity"]
for a in act:
    i=a["item_id"]; t=T(a["at"])
    ids=p.identities(raw[i])
    cands=[eid for eid,la in last.items() if la>=t-timedelta(days=3) and ids & evids[eid]]
    per_item[i]=cands; pairs+=len(cands)
    for c in cands: needed[c]+=1
    last[a["event_id"]]=t
print("pairs",pairs,"items with cands",sum(bool(c) for c in per_item.values()))
newneeded=[e for e in needed if R["events"][e]["kind"]=="NEW_OPENER"]
print("new events needing statements",len(newneeded))
lbl=R["labels"]
print("true not in candidates",sum(1 for i,l in lbl.items() if l["expected_event_id"] and l["expected_event_id"] not in per_item[i]))
pickle.dump(dict(evids=evids,per_item=per_item,newneeded=newneeded),open(W+"cands.pkl","wb"))
