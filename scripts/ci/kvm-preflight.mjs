import assert from 'node:assert/strict';
import { mkdir, writeFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { join } from 'node:path';

const directory = process.env.TANK_QUALIFICATION_EVIDENCE;
assert.ok(directory && process.env.GITHUB_ACTIONS === 'true', 'Qualification requires an isolated Actions runner');
assert.ok(process.env.RUNNER_TEMP, 'Existing executor scratch path required');
await mkdir(directory, { recursive: true });
const probe = spawnSync('/usr/bin/sudo', ['-n', '/usr/bin/python3', '-c', `import fcntl,json,os,platform,shutil,sys
result={'architecture':platform.machine(),'kvmDevice':os.path.exists('/dev/kvm'),'probeUid':os.geteuid(),'freeBytes':shutil.disk_usage(sys.argv[1]).free}
try:
 fd=os.open('/dev/kvm',os.O_RDWR|os.O_CLOEXEC)
 result['apiVersion']=fcntl.ioctl(fd,0xAE00,0)
 vm=fcntl.ioctl(fd,0xAE01,0)
 os.close(vm)
 os.close(fd)
 result['createVm']='PASS'
except OSError as error:
 result['createVm']='FAIL'
 result['errno']=getattr(error,'errno',None)
 result['error']=str(error)
print(json.dumps(result))
`, process.env.RUNNER_TEMP], { encoding: 'utf8', timeout: 15000 });
assert.equal(probe.status, 0, probe.stderr);
const result = { schema: 'tank.isolated-kvm-preflight.v1', sourceSha: process.env.GITHUB_SHA, runId: process.env.GITHUB_RUN_ID, access: 'existing-sudo-process-no-host-mutation', runnerUid: process.getuid(), ...JSON.parse(probe.stdout) };
await writeFile(join(directory, 'kvm-preflight.json'), JSON.stringify(result, null, 2) + '\n');
console.log(JSON.stringify(result));
assert.equal(result.architecture, 'x86_64', 'Actual amd64 executor required');
assert.equal(result.probeUid, 0, 'KVM prerequisite uses only the existing privileged probe process');
assert.equal(result.apiVersion, 12, 'KVM API version 12 required');
assert.equal(result.createVm, 'PASS', 'Real KVM_CREATE_VM failed; no emulation fallback');
assert.ok(result.freeBytes >= 20 * 1024 ** 3, 'Qualification requires 20 GiB free scratch disk');
