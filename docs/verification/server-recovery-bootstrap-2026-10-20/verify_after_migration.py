"""Read-only final guarded-retirement receipt; no source/event/queue mutation."""
import hashlib,io,json,shutil,re
from pathlib import Path
import psycopg,zstandard
from psycopg import sql
from tank.settings import Settings
from tank.market_migration import verify_legacy_table_bundle
from verify_restore import verify_live

root=Path("/data/bundles")
out=Path("/qa")
baseline=json.loads((out/"proof/tank.json").read_text())
archive={}
for name,record in baseline["tables"].items():
 if name.startswith("archive.") and record["rows"]:
  parent=re.sub(r"_(?:[0-9]{8}|historical|default)$","",name)
  archive[parent]=archive.get(parent,0)+record["rows"]
manifests=[]
for path in root.glob("*.json.zst"):
 with path.open("rb") as source,zstandard.ZstdDecompressor().stream_reader(source) as decoded,io.TextIOWrapper(decoded,encoding="utf-8") as rows:
  line=rows.readline(65537).encode()
 if len(line)>65536:continue
 if line.endswith(b',"rows":[\n'):continue
 record=json.loads(line)
 if record.get("format")!="LEGACY_SQL_ARCHIVE_MANIFEST_V1":continue
 assert hashlib.sha256(line).hexdigest()==path.name.removesuffix(".json.zst"),"Manifest content checksum mismatch"
 manifests.append(record)
assert len(manifests)==4,"Expected four retirement manifests"
exports={}
for manifest in manifests:
 for entry in manifest["tables"]:
  arguments={key:value for key,value in entry.items() if key!="path"}
  with Path(entry["path"]).open("rb") as stream:
   assert hashlib.file_digest(stream,"sha256").hexdigest()==entry["sha256"],"Permanent file checksum mismatch"
  name=entry["collection"]
  assert name not in exports and archive[name]==entry["row_count"],"Archive original row count mismatch"
  exports[name]={"rows":entry["row_count"],"sha256":entry["sha256"],"path":entry["path"],"bytes":Path(entry["path"]).stat().st_size,"guarded_pre_drop_full_readback":True,"post_file_checksum_verified":True}
assert set(exports)==set(archive),"Missing original nonempty archive table export"
domain={name:dict(record) for name,record in baseline["tables"].items() if not name.startswith(("archive.","mkt.")) and name not in {"ops.schema_migrations","ops.market_migration_exports"}}
assert domain["out.alerts"]["rows"]==0
domain["out.alerts"]["columns"]=[name for name in domain["out.alerts"]["columns"] if name!="linter"]
domain["out.alerts"]["hash_columns"]=domain["out.alerts"]["columns"]
mismatches=verify_live(domain,{name:state for name,state in baseline["sequences"].items() if not name.startswith("archive.")})
(out/"post-domain-mismatches.json").write_text(json.dumps(mismatches,indent=2)+"\n")
assert mismatches==[],"Domain/job/session/sequence state changed"
with psycopg.connect(Settings().database_url,autocommit=True) as connection:
 connection.execute("SET default_transaction_read_only=on")
 versions=[row[0]for row in connection.execute("SELECT version FROM ops.schema_migrations ORDER BY version")]
 expected=[p.name for p in sorted(Path("/app/src/tank/migrations").glob("*.sql"))]
 assert versions==expected and len(versions)==35,"Migration ledger mismatch"
 assert connection.execute("SELECT to_regnamespace('archive')").fetchone()[0]is None,"Archive schema remains"
 assert connection.execute("SELECT count(*) FROM information_schema.columns WHERE table_schema='out' AND table_name='alerts' AND column_name='linter'").fetchone()[0]==0,"Retired projection remains"
 market={}
 expected_market={}
 for name,record in baseline["tables"].items():
  if name.startswith("mkt."):
   parent=re.sub(r"_(?:[0-9]{8}|historical|default)$","",name)
   expected_market[parent]=expected_market.get(parent,0)+record["rows"]
 for name,expected_count in expected_market.items():
  schema,table=name.split(".")
  count=connection.execute(sql.SQL("SELECT count(*) FROM {}.{}").format(sql.Identifier(schema),sql.Identifier(table))).fetchone()[0]
  assert count==expected_count,"Typed market count changed"
  market[name]=count
 assert baseline["tables"]["ops.market_migration_exports"]["rows"]==0
 receipts=[row[0] for row in connection.execute("SELECT doc FROM ops.market_migration_exports")]
 assert len(receipts)==4
 for receipt in receipts:
  matched=[m for m in manifests if m["tables"]==receipt["tables"] and m["row_count"]==receipt["row_count"]]
  assert len(matched)==1 and receipt["format"]=="LEGACY_SQL_ARCHIVE_MANIFEST_V1"
  assert receipt["export_id"]==receipt["manifest_sha256"]==Path(receipt["manifest_path"]).name.removesuffix(".json.zst")
 size=connection.execute("SELECT pg_database_size(current_database())").fetchone()[0]
 assert connection.execute("SHOW max_locks_per_transaction").fetchone()[0]=="64"
report={"passed":True,"ledger":versions,"archive_absent":True,"linter_column_absent":True,"exported_tables":exports,"exported_rows":sum(r["rows"]for r in exports.values()),"all_nonempty_archive_originals_preserved":True,"full_row_readback_stage":"Normal guarded CLI before DROP; no redundant full JSON scan repeated","domain_jobs_sessions_fingerprints_unchanged":True,"new_immutable_archive_export_receipts":4,"remaining_nonarchive_sequences_unchanged":True,"retired_archive_sequence_state_retained_in_preproof":True,"typed_market_table_counts_unchanged":True,"market_counted_tables":len(market),"pre_migration_market_full_row_hash_proof":True,"post_migration_market_full_row_hash_repeated":False,"new_raw_market_frames_path_exists":Path("/data/raw/market_frames").exists(),"pg_database_bytes":size,"free_bytes":shutil.disk_usage("/data").free,"runtime_started":False,"market_warming_started":False}
assert not report["new_raw_market_frames_path_exists"]
p=out/"post-migration-proof.json";p.write_text(json.dumps(report,indent=2)+"\n");p.chmod(0o600)
print(json.dumps({key:value for key,value in report.items()if key not in {"ledger","exported_tables"}}),flush=True)
