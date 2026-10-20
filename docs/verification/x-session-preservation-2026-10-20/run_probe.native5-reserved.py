"""One temporary X reader, bounded to1CPU/1GiB; never restarts or modifies a service."""
import json
from pathlib import Path
import re
import subprocess
import time

BASE=Path('/srv/tank/docs/verification/x-session-preservation-2026-10-20')
PRIVATE=Path('/etc/tank/private/x-connection-20261020')
NAME='tank-x-session-readonly-20261020'
IMAGE='sha256:1c828c59acad147aa14f4f62c9c3932f4d25442721077f33c5a6089b0b7a9c9a'
GIB=1024**3

def call(args,timeout=15):
    return subprocess.run(args,capture_output=True,text=True,check=True,timeout=timeout).stdout.strip()

def resources():
    mem=int(re.search(r'^MemAvailable:\s+(\d+)',Path('/proc/meminfo').read_text(),re.M)[1])*1024
    disk=int(call(['df','-Pk','/srv/tank']).splitlines()[-1].split()[3])*1024
    return {'epoch_ms':int(time.time()*1000),'mem_available_bytes':mem,'disk_available_bytes':disk}

def save(name,row):
    (BASE/name).write_text(json.dumps(row,indent=2)+'\n')

assert not (PRIVATE/'probe-report.json').exists(),'One probe only; preserve any earlier outcome'
assert not call(['docker','ps','-a','--filter','name=^/'+NAME+'$','--format','{{.Names}}'])
assert call(['docker','image','inspect',IMAGE,'--format','{{.Id}}'])==IMAGE
initial=resources()
warm=call(['docker','stats','--no-stream','--format','{{.MemUsage}}','tank-financial-warm-20261020'])
value,unit=re.match(r'([0-9.]+)([A-Za-z]+)',warm).groups()
usage=int(float(value)*{'MiB':1024**2,'GiB':GIB,'KiB':1024,'B':1}[unit])
projected=initial['mem_available_bytes']-GIB-5*GIB-max(0,GIB-usage)
save('preflight.json',{'resources':initial,'probe_limit_bytes':GIB,'future_native_limit_reserved_bytes':5*GIB,'warm_remaining_reserved_bytes':max(0,GIB-usage),'projected_reserve_bytes':projected,'minimum_reserve_bytes':2*GIB,'disk_floor_bytes':8*GIB,'state_file_read_or_hashed':False})
if projected<2*GIB or initial['disk_available_bytes']<8*GIB:
    print(json.dumps({'status':'WAIT_RESOURCE_RESERVE','browser_started':False,'authenticated_requests':0}));raise SystemExit(3)
command=['docker','run','--pull','never','--rm','--init','--name',NAME,'--user','1000:1000','--cpus','1','--memory','1g','--memory-swap','1g','--pids-limit','256','--read-only','--tmpfs','/tmp:rw,size=256m,mode=1777','--tmpfs','/home/tank/.cache:rw,size=64m,uid=1000,gid=1000,mode=0700','--mount','type=bind,src=/etc/tank/secrets/x_session,dst=/run/secrets/x_session,readonly','--mount','type=bind,src='+str(BASE/'probe.py')+',dst=/audit/probe.py,readonly','--mount','type=bind,src=/srv/tank/01_Tank_V0_PRD.md,dst=/prd/01_Tank_V0_PRD.md,readonly','--mount','type=bind,src='+str(PRIVATE)+',dst=/out','--entrypoint','/opt/tank/venv/bin/python',IMAGE,'/audit/probe.py']
save('command.json',{'command':command,'no_provider_keys':True,'full_runtime_started':False,'source_session_readonly':True,'browser_context_persistent':False,'probe_seconds':90,'outer_seconds':120})
started=time.monotonic();reason=None
with (BASE/'probe.log').open('wb') as log:
    child=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
    samples=[]
    while child.poll() is None:
        row=resources();samples.append(row)
        if row['mem_available_bytes']<2*GIB or row['disk_available_bytes']<8*GIB:
            reason='RESOURCE_FLOOR';break
        if time.monotonic()-started>120:
            reason='OUTER_TIMEOUT';break
        time.sleep(3)
    if reason:
        subprocess.run(['docker','stop','--time','5',NAME],capture_output=True,timeout=15)
    try:code=child.wait(timeout=10)
    except subprocess.TimeoutExpired:
        subprocess.run(['docker','kill',NAME],capture_output=True,timeout=10);code=child.wait(timeout=10)
left=call(['docker','ps','-a','--filter','name=^/'+NAME+'$','--format','{{.Names}}'])
save('runner.json',{'exit_code':code,'elapsed_seconds':time.monotonic()-started,'abort_reason':reason,'samples':samples,'owned_container_absent':not left,'source_state_overwritten':False,'login_attempted':False,'full_runtime_started':False})
if (PRIVATE/'probe-report.json').exists():
    private=json.loads((PRIVATE/'probe-report.json').read_text())
    public={key:private[key] for key in ['complete','status','authenticated','challenge','account_identity_readback','list_inventory_readback','list_found','exact_86_members_verified','login_attempted','cookies_saved_or_overwritten','list_mutations']}
    public['private_identity_report']='/etc/tank/private/x-connection-20261020/probe-report.json'
    save('health.json',public)
    print(json.dumps(public))
else:
    print(json.dumps({'status':'NO_PROBE_REPORT','exit_code':code,'owned_container_absent':not left}))
