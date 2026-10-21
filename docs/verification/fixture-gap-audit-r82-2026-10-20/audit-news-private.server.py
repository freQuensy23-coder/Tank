"""Read-only current source proof and exact retained equivalent audit."""
from pathlib import Path
import hashlib, importlib.util, json, re
ROOT=Path("/srv/tank");HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location("helper",HERE/"prepare-market-packets.server-v2.py")
helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def walk(x,at,fid,kind,missing):
 if isinstance(x,dict):
  for k,v in x.items():walk(v,at+[k],fid,kind,missing)
 elif isinstance(x,list):
  for k,v in enumerate(x):walk(v,at+[k],fid,kind,missing)
 elif isinstance(x,str):
  if x.startswith(helper.OLD_ROOT):rel=x[len(helper.OLD_ROOT):]
  elif x.startswith("/data/"):rel=x[1:]
  else:rel=x
  if rel in missing:missing[rel]["occurrences"].append({"fixture_id":fid,"document":kind,"json_path":at})
def main():
 portfolio=helper.read_json(HERE/"portfolio.candidate.json")
 missing={r["path"]:dict(r,occurrences=[]) for r in helper.read_json(HERE/"news-source-recovery-receipt.json")["remaining_missing_both"]}
 contexts=[];targets={}
 for case in portfolio["fixtures"]:
  fid=case["fixture_id"]
  if fid in {f"F-{i:02}" for i in range(11,17)}:continue
  packet=helper.read_json(case["acceptance_input"]["path"]);private=helper.read_json(case["private_name_map"]["path"])
  sources=[row for row in packet["records"] if row["collection"]=="ingest.source_items"]
  subset={k:v for k,v in packet.items() if k not in ["records","registry"]}
  subset["source_wrappers"]=sources
  walk(subset,[],fid,"acceptance_input",missing);walk(private,[],fid,"private_name_map",missing)
  needs_private=fid in {"F-24","F-25","F-26","F-27"} or any(row.get("as_of_evidence",{}).get("derivation",{}).get("kind")=="SNAPSHOT_SIGNED_IPFS_V1" for row in sources)
  parent={k:{"declared":private.get(k),"exists_in_server_projection":helper.host_path(private[k]).is_file()} for k in ["original_packet_path","original_registry_path"] if private.get(k)}
  if needs_private and any(not v["exists_in_server_projection"] for v in parent.values()):
   targets[fid]=private["source_sha256"]
  contexts.append({"fixture_id":fid,"current_source_derivations":[row.get("as_of_evidence",{}).get("derivation",{}).get("kind") for row in sources],"runtime_host_private_proof_required_by_current_validator":needs_private,"private_parent_paths":parent,"private_source_sha256":private.get("source_sha256"),"sources_currently_missing":[r["path"] for r in missing.values() if any(o["fixture_id"]==fid and o["document"]=="acceptance_input" for o in r["occurrences"])]})
  print(fid,"PRIVATE_REQUIRED="+str(needs_private),flush=True)
 matches={fid:[] for fid in targets}; examined=[];errors=[]
 candidates=[]
 for folder in [ROOT/"data/replay",ROOT/"docs/verification"]:
  for p in folder.rglob("*"):
   if not p.is_file() or p.suffix not in {".json",".zst"}:continue
   rel=str(p.relative_to(ROOT))
   if "fixture-gap-audit-r82" in rel:continue
   if not (any(fid in rel for fid in targets) or "original" in p.name):continue
   if "/raw/" in rel or p.name.endswith(".txt.zst"):continue
   candidates.append(p)
 for p in candidates:
  try:
   packet=helper.read_json(p)
   if not isinstance(packet,dict) or not {"registry","records"}.issubset(packet):continue
   fid=packet.get("fixture_id") or packet.get("fixture_reference",{}).get("fixture_id")
   if fid not in targets:continue
   h=helper.stream_digest(packet)
   row={"path":str(p.relative_to(ROOT)),"sha256":sha(p),"canonical_sha256":h,"fixture_id":fid}
   examined.append(row)
   if h==targets[fid]:matches[fid].append(row)
  except (ValueError,KeyError,TypeError,OSError) as e:errors.append({"path":str(p.relative_to(ROOT)),"error":str(e)})
 for row in missing.values():
  row["classification"]="CURRENT_RUNTIME_PRIVATE_PARENT_MISSING" if any(len(o["json_path"])==1 for o in row["occurrences"]) else "HISTORICAL_PRIVATE_TRANSFORMATION_PROVENANCE_ONLY"
 result={"scope":"READONLY_PATH_PROJECTION_NO_SOURCE_FIELDS_CHANGED","authority":{"generic_readiness_gate":"tests/fixture_acceptance.py:326-433","actual_private_derivative_gate":"src/tank/fixture_reference.py:181-199,885-932,1089-1103"},"missing_paths":list(missing.values()),"current_contexts":contexts,"exact_equivalent_packet_search":{"required_canonical_sha256":targets,"matched":matches,"examined":examined,"errors":errors},"limits":"Existing transferred replay packets only, no external/native/PG or synthesized parents; an absent older artifact remains absent."}
 (HERE/"news-private-reference-audit.json").write_text(json.dumps(result,indent=2)+"\n")
 print(json.dumps({"runtime_missing_parents":{k:v for k,v in targets.items()},"exact_matches":matches,"examined_packets":len(examined),"missing":len(missing),"current_sources_missing":sum(len(x["sources_currently_missing"]) for x in contexts)},indent=2))
if __name__=="__main__":main()
