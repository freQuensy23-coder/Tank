"""Fresh replay schema via normal CLI; no copying/resetting primary history."""
import json,os,shutil,subprocess,time
from pathlib import Path
OUT=Path("/srv/tank/docs/verification/server-recovery-bootstrap-2026-10-20")
def admin(q,db="postgres"):
 return subprocess.check_output(["docker","exec","-u","postgres","tank-postgres-1","psql","-XAt","-v","ON_ERROR_STOP=1","-U","postgres","-d",db,"-c",q],text=True).strip()
assert admin("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%' AND c.relkind IN ('r','p','v','m','S','f')","tank_replay")=="0","Replay is not empty; preserve and stop"
assert shutil.disk_usage("/var/lib/docker").free>8*1024**3
COMPOSE=["docker","compose","--env-file","/etc/tank/compose.env","-p","tank","-f","/srv/tank/compose.yaml","-f","/etc/tank/compose.server.yaml","-f",str(OUT/"pg.resources.yaml"),"-f",str(OUT/"tank.image.yaml")]
derive="import os; from urllib.parse import urlsplit,urlunsplit; u=urlsplit(os.environ['TANK_DATABASE_URL']); os.environ['TANK_DATABASE_URL']=urlunsplit((u.scheme,u.netloc,'/tank_replay',u.query,u.fragment)); os.execvp('tank',['tank','db','migrate'])"
start=time.monotonic()
with (OUT/"replay-migration.private.log").open("wb") as log:
 Path(log.name).chmod(0o600)
 process=subprocess.Popen(COMPOSE+["run","--rm","--no-deps","--pull","never","-T","--name","tank-recovery-replay-migrate","tank","python","-c",derive],stdout=log,stderr=log)
 while process.poll() is None:
  time.sleep(10)
  free=shutil.disk_usage("/var/lib/docker").free
  available=int(next(line.split()[1]for line in Path("/proc/meminfo").read_text().splitlines()if line.startswith("MemAvailable:")))*1024
  if free<8*1024**3 or available<2*1024**3:
   subprocess.run(["docker","pause","tank-recovery-replay-migrate"],check=True,capture_output=True)
   raise RuntimeError("Owned replay migration paused for capacity")
  print(json.dumps({"scope":"EMPTY_REPLAY_NORMAL_MIGRATION","elapsed_seconds":round(time.monotonic()-start),"free_bytes":free,"mem_available_bytes":available}),flush=True)
 code=process.wait()
assert code==0,"Replay migration failed; preserve private log and database"
for source,db in (("/srv/tank/deploy/provision_replay_role.sh",None),("/srv/tank/deploy/read_only_role.sql","tank_replay")):
 with Path(source).open() as stream:
  command=["docker","exec","-i","-u","postgres","tank-postgres-1"]
  command+=["sh"] if db is None else ["psql","-X","-v","ON_ERROR_STOP=1","-U","postgres","-d",db]
  result=subprocess.run(command,stdin=stream,capture_output=True,text=True)
  assert result.returncode==0,"Supported replay role provisioning failed"
report={"passed":True,"scope":"Fresh initially empty tank_replay, normal CLI; no primary history copied","elapsed_seconds":round(time.monotonic()-start,2),"ledger_count":int(admin("SELECT count(*) FROM ops.schema_migrations","tank_replay")),"archive_absent":admin("SELECT to_regnamespace('archive') IS NULL","tank_replay")=="t","read_only_sql_applied":True,"primary_event_count":int(admin("SELECT count(*) FROM evt.events","tank")),"replay_event_count":int(admin("SELECT count(*) FROM evt.events","tank_replay")),"free_bytes":shutil.disk_usage("/var/lib/docker").free}
assert report["ledger_count"]==35 and report["archive_absent"] and report["replay_event_count"]==0
p=OUT/"replay-setup-proof.json";p.write_text(json.dumps(report,indent=2)+"\n");p.chmod(0o600)
print(json.dumps(report),flush=True)
