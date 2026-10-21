"""Only normal CLI migration, after complete pre-retirement row proof."""
import json,os,shutil,subprocess,time
from pathlib import Path
OUT=Path("/srv/tank/docs/verification/server-recovery-bootstrap-2026-10-20")
assert json.loads((OUT/"semantic-proof-complete.json").read_text())=={"tank":True,"oma":True,"migrations_not_started":True}
for name in ("tank","oma"):
 assert json.loads((OUT/"proof"/f"{name}.json").read_text())["passed"]
COMPOSE=["docker","compose","--env-file","/etc/tank/compose.env","-p","tank","-f","/srv/tank/compose.yaml","-f","/etc/tank/compose.server.yaml","-f",str(OUT/"pg.resources.yaml"),"-f",str(OUT/"tank.image.yaml")]
container="tank-recovery-migrate"
start=time.monotonic()
with (OUT/"migration.private.log").open("wb") as log:
 os.chmod(log.name,0o600)
 command=COMPOSE+["run","--rm","--no-deps","--pull","never","-T","--name",container,"tank","tank","db","migrate"]
 process=subprocess.Popen(command,stdout=log,stderr=log)
 while process.poll() is None:
  time.sleep(10)
  free=shutil.disk_usage("/var/lib/docker").free
  available=int(next(line.split()[1]for line in Path("/proc/meminfo").read_text().splitlines()if line.startswith("MemAvailable:")))*1024
  if free<8*1024**3 or available<2*1024**3:
   subprocess.run(["docker","pause",container],check=True,capture_output=True)
   raise RuntimeError("Owned migration paused for capacity; originals/bundles/PG preserved")
  print(json.dumps({"state":"NORMAL_CLI_MIGRATING","elapsed_seconds":round(time.monotonic()-start),"free_bytes":free,"mem_available_bytes":available}),flush=True)
 code=process.wait()
receipt={"exit_code":code,"elapsed_seconds":round(time.monotonic()-start,2),"normal_cli":True,"image":"sha256:1c828c59acad147aa14f4f62c9c3932f4d25442721077f33c5a6089b0b7a9c9a","pre_retirement_proof":True,"free_bytes":shutil.disk_usage("/var/lib/docker").free}
(OUT/"migration-result.json").write_text(json.dumps(receipt,indent=2)+"\n")
assert code==0,"Migration failed; private logs, committed versions and evidence preserved"
print(json.dumps(receipt),flush=True)
