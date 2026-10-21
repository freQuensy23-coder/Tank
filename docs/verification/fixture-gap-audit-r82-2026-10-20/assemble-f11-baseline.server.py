"""Offline F11 component successor: add authentic pre-window Binance baseline only."""
from pathlib import Path
import collections, copy, datetime, hashlib, importlib.util, json, shutil
import zstandard
from tank.market.archive import verify_binance_zip
from tank.market.normalize import oi
from tank.replay import digest
from tank.util import parse_time, iso
ROOT=Path("/srv/tank")
HERE=Path(__file__).resolve().parent
OUT=HERE/"f11-component-with-baseline"
START=parse_time("2026-10-16T08:30:00Z")
LOWER=START-datetime.timedelta(days=90)
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 assert shutil.disk_usage(ROOT).free>=8*1024**3
 OUT.mkdir(exist_ok=False)
 transfer=[]
 for line in (HERE/"wld-sidecar-original-sha256.txt").read_text().splitlines():
  expected,path=line.split(None,1); f=ROOT/path
  assert sha(f)==expected,(path,"transfer mismatch")
  transfer.append({"path":path,"sha256":expected})
 base_path=HERE/"f11-component/snapshot.json"
 base_manifest=json.loads((HERE/"f11-component/manifest.json").read_text())
 assert sha(base_path)==base_manifest["snapshot_sha256"]
 base=json.loads(base_path.read_text())
 snapshot=copy.deepcopy(base)
 inst=next(i for i in snapshot["instruments"] if i["venue"]=="binance" and i["market_type"]=="PERP_LINEAR" and i["token_id"]==snapshot["token_id"])
 receipts={}
 for line in (ROOT/"docs/verification/retained-metrics-checksums-2026-10-18/records.jsonl").read_text().splitlines():
  row=json.loads(line)
  if row["symbol"]=="WLDUSDT": receipts[row["date"]]=row
 rows=[];sources=[]
 for p in sorted((ROOT/"fixture_check/data/metrics").glob("WLDUSDT-*.zip")):
  day=p.name[len("WLDUSDT-"):-len(".zip")]
  if not LOWER <= parse_time(day) < START.replace(hour=0,minute=0,second=0): continue
  official="WLDUSDT-metrics-"+day+".zip"
  check=ROOT/"docs/verification/retained-metrics-checksums-2026-10-18/responses/WLDUSDT"/(official+".CHECKSUM")
  if not check.exists(): continue
  receipt=receipts[day]
  assert sha(p)==receipt["local"]["sha256_after"]==receipt["official_sha256"]
  assert sha(check)==receipt["sidecar_sha256"]
  verified=verify_binance_zip(p.read_bytes(),check.read_text(),filename=official,kind="metrics",expected_unit="MS")
  added=[]
  for values in verified["rows"]:
   raw=dict(zip(verified["header"],values)); ts=parse_time(raw["create_time"])
   assert ts<START
   point=oi(inst,{"sumOpenInterest":raw["sum_open_interest"],"sumOpenInterestValue":raw["sum_open_interest_value"],"timestamp":iso(ts)},bucket_size_s=300)
   point.update(received_ts=None,received_ts_prec=None,received_ts_prec_reason="INCOMPLETE",source_archive="ARCH-"+verified["sha256"])
   added.append(point)
  rows.extend(added)
  sources.append({"path":str(p.relative_to(ROOT)),"sha256":sha(p),"checksum_sidecar":str(check.relative_to(ROOT)),"checksum_sidecar_sha256":sha(check),"receipt_file":"docs/verification/retained-metrics-checksums-2026-10-18/records.jsonl","receipt_date":day,"original_acquired_at":receipt["local"]["original_acquired_at"],"original_acquisition_reason":receipt["local"]["original_acquisition_reason"],"verified_complete_5m_day":verified["complete"],"rows":len(added),"first_ts":min(p["ts"] for p in added),"last_ts":max(p["ts"] for p in added),"scope":"BASELINE_ONLY_NEVER_WINDOW_ENDPOINT"})
 assert len(rows)>=7*288
 original_oi=digest(base["oi"]);original_flow=digest(base["flow_bars"])
 snapshot["oi"]=rows+snapshot["oi"]
 snapshot["assembled_at"]=iso()
 snapshot.setdefault("provenance",{}).setdefault("sources",[]).extend(sources)
 assert snapshot["oi"][len(rows):]==base["oi"]
 assert digest(snapshot["flow_bars"])==original_flow
 for field in ["cutoff","anchor","token_id","instruments","tokens","semantics"]:
  assert snapshot.get(field)==base.get(field),field
 target=OUT/"snapshot.json.zst"
 with target.open("xb") as dest:
  with zstandard.ZstdCompressor(level=6).stream_writer(dest,closefd=False) as writer:
   for piece in json.JSONEncoder(separators=(",",":")).iterencode(snapshot):writer.write(piece.encode())
 manifest={"artifact_kind":"HOST_ONLY_AUTHENTIC_F11_BASELINE_SUCCESSOR_NOT_ACCEPTANCE","snapshot_path":str(target.relative_to(ROOT)),"snapshot_sha256":sha(target),"assembled_at":snapshot["assembled_at"],"original_component":{"path":str(base_path.relative_to(ROOT)),"sha256":sha(base_path)},"counts":{"original_oi":len(base["oi"]),"added_baseline_oi":len(rows),"total_oi":len(snapshot["oi"]),"unchanged_flow_bars":len(base["flow_bars"]),"complete_5m_days":sum(s["verified_complete_5m_day"] for s in sources),"archive_days":len(sources)},"baseline_window":{"requested_lower_inclusive":iso(LOWER),"strict_upper_exclusive":iso(START),"actual_first":min(x["ts"] for x in rows),"actual_last":max(x["ts"] for x in rows),"policy":"Only complete preceding UTC days selected; actual original archive row/bucket semantics retained."},"preservation":{"original_oi_tail_digest":original_oi,"flow_digest":original_flow,"original_cutoff":base["cutoff"],"received_ts_unknown_preserved":True,"no_catalyst_or_expected_fields":True},"sources":sources,"transferred_source_hashes":transfer,"native_or_financial_acceptance_claimed":False}
 (OUT/"manifest.json").write_text(json.dumps(manifest,indent=2)+"\n")
 print(json.dumps({"counts":manifest["counts"],"baseline_window":manifest["baseline_window"],"snapshot_sha256":manifest["snapshot_sha256"]},indent=2))
if __name__=="__main__":main()
