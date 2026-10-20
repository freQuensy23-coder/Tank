import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RetainedSessionLifecycle } from './retained-session.ts';

const fingerprint = 'sha256:' + 'a'.repeat(64);
const signal = () => new AbortController().signal;
const permitted = async () => {};
import { setup } from './fakes.mjs';

test('same native session rootfs/workspace/profile survive idle and controller replacement via fresh handles',async()=>{
  const s=setup(),first=s.controller();
  await first.begin(permitted,signal());
  await first.use(async runtime=>{const {disk,env}=await runtime.exec('write', {OPENROUTER_API_KEY:'FAKE_PER_EXEC_SECRET'});
    disk.files['/workspace/analysis.txt']='durable workspace';disk.files['/root/rootfs-marker']='durable rootfs';
    disk.profile.cookie='session cookie';assert.equal(env.OPENROUTER_API_KEY,'FAKE_PER_EXEC_SECRET');});
  await first.idle(permitted);
  assert.equal([...s.disks.values()][0].running,false);
  assert.equal(JSON.stringify([...s.rows.values()]).includes('FAKE_PER_EXEC_SECRET'),false);
  const replacement=s.controller({ownerId:'controller_B'});
  await replacement.begin(permitted,signal());
  await replacement.use(async runtime=>{const {disk,env}=await runtime.exec('read',{OPENROUTER_API_KEY:'FAKE_ROTATED_SECRET'});
    assert.equal(disk.files['/workspace/analysis.txt'],'durable workspace');assert.equal(disk.files['/root/rootfs-marker'],'durable rootfs');
    assert.equal(disk.profile.cookie,'session cookie');assert.equal(env.OPENROUTER_API_KEY,'FAKE_ROTATED_SECRET');});
  assert.equal(s.creates,1);assert.equal(s.nativeHandles.length,2);
  await replacement.idle(permitted);
});

test('idle waits active commands, rejects new work, and checks current execution before stop',async()=>{
  const s=setup(),c=s.controller();await c.begin(permitted,signal());
  let release;const blocked=new Promise(resolve=>{release=resolve;});
  const command=c.use(async runtime=>{await runtime.exec('long');await blocked;});
  await new Promise(resolve=>setImmediate(resolve));
  const idle=c.idle(permitted);await assert.rejects(c.use(async()=>{}),/retained_not_active/);
  await new Promise(resolve=>setImmediate(resolve));assert.equal(s.calls.some(x=>x[0]==='stop'),false);
  release();await command;await idle;
  assert.equal(s.calls.filter(x=>x[0]==='stop').length,1);
});

test('missing retained disk never falls back to creation',async()=>{
  const s=setup(),c=s.controller();await c.begin(permitted,signal());await c.idle(permitted);s.disks.clear();
  await assert.rejects(s.controller().begin(permitted,signal()),/owned retained disk missing/);assert.equal(s.creates,1);
});

test('changed configuration cannot resume or destroy the retained rootfs',async()=>{
  const s=setup(),c=s.controller();await c.begin(permitted,signal());await c.idle(permitted);
  const mismatch=s.controller({fingerprint:'sha256:'+'b'.repeat(64)});
  await assert.rejects(mismatch.begin(permitted,signal()),/retained_configuration_changed/);
  assert.equal(s.disks.size,1);assert.equal(s.calls.some(x=>x[0]==='fresh-get'),false);
});

test('competing controllers cannot acquire the active retained session',async()=>{
  const s=setup(),first=s.controller(),second=s.controller();await first.begin(permitted,signal());
  await assert.rejects(second.begin(permitted,signal()),/retained_resource_busy/);
  assert.equal(s.creates,1);await first.idle(permitted);
});

test('stale execution or resource fence cannot execute or stop current VM',async()=>{
  const s=setup(),c=s.controller();await c.begin(permitted,signal());
  await assert.rejects(c.idle(async()=>{throw Error('execution lease lost');}),/execution lease lost/);
  assert.equal(s.calls.some(x=>x[0]==='stop'),false);
  const row=[...s.rows.values()][0];row.active=false;
  await assert.rejects(c.use(runtime=>runtime.exec('stale')),/retained_fence_lost/);
  await assert.rejects(c.idle(permitted),/retained_fence_lost/);
  await assert.rejects(c.idle(permitted),/retained_fence_lost/);
  const replacement=s.controller();await replacement.begin(permitted,signal());await replacement.idle(permitted);
});

test('cold native final archive removes exact published resource without resume and remains terminal',async()=>{
  const s=setup(),c=s.controller();await c.begin(permitted,signal());await c.idle(permitted);
  const archived=s.controller();await archived.archive(permitted,signal());
  assert.equal(s.disks.size,0);assert.equal(s.calls.some(x=>x[0]==='fresh-get'),false);
  await s.controller().archive(permitted,signal());
  await assert.rejects(s.controller().begin(permitted,signal()),/retained_session_archived/);
  assert.equal(s.calls.filter(x=>x[0]==='remove-owned').length,1);
});

test('unauthorized or active archive cannot mutate resources',async()=>{
  const s=setup(),c=s.controller();await c.begin(permitted,signal());
  await assert.rejects(c.archive(permitted,signal()),/retained_archive_while_active/);
  await c.idle(permitted);
  await assert.rejects(s.controller().archive(async()=>{throw Error('not native final archive');},signal()),/not native final archive/);
  assert.equal(s.disks.size,1);
});

test('failed initial pointer publication cleans only that unpublished owned allocation',async()=>{
  const s=setup(),publish=s.fences.publish.bind(s.fences);s.fences.publish=async input=>{if(input.workspaceCandidate.metadata.state==='retained')throw Error('durable store unavailable');return publish(input);};
  await assert.rejects(s.controller().begin(permitted,signal()),/durable store unavailable/);
  assert.equal(s.disks.size,0);assert.deepEqual(s.calls.map(x=>x[0]),['create','remove-owned']);
});

test('corrupt durable publication is refused without allocation or guest execution',async()=>{
  const s=setup(),c=s.controller();await c.begin(permitted,signal());await c.idle(permitted);
  [...s.rows.values()][0].publication.workspaceCandidate.metadata.runtimeId='foreign';
  await assert.rejects(s.controller().begin(permitted,signal()),/retained_manifest_invalid/);
  assert.equal(s.creates,1);assert.equal(s.disks.size,1);
});

test('configuration mismatch still permits authorized cold final cleanup of its exact old resource',async()=>{
  const s=setup(),c=s.controller();await c.begin(permitted,signal());await c.idle(permitted);
  await s.controller({fingerprint:'sha256:'+'c'.repeat(64)}).archive(permitted,signal());
  assert.equal(s.disks.size,0);
  await assert.rejects(s.controller().begin(permitted,signal()),/retained_session_archived/);
});

test('cleanup failure is explicit and enqueues the owned unpublished allocation without fence tokens',async()=>{
  const s=setup(),orphans=[];
  const publish=s.fences.publish.bind(s.fences);
  s.fences.publish=async input=>{if(input.workspaceCandidate.metadata.state==='retained')throw Error('initial publication unavailable');return publish(input);};
  s.provider.reap=async()=>{throw Error('provider unavailable');};
  await assert.rejects(s.controller({orphans:{async enqueue(input){orphans.push(input);}}}).begin(permitted,signal()),/retained_initial_cleanup_failed/);
  assert.equal(orphans.length,1);assert.equal(orphans[0].sandbox.runtimeId,'box_1');
  assert.equal(JSON.stringify(orphans).includes('FAKE_FENCE_'),false);
  assert.equal([...s.rows.values()][0].active,false);
});

test('successful stop followed by publication failure remains stopped and retries without stale-handle use or double stop',async()=>{
  const s=setup(),c=s.controller();await c.begin(permitted,signal());
  const publish=s.fences.publish.bind(s.fences);let fail=true;
  s.fences.publish=async input=>{if(fail){fail=false;throw Error('publication transient');}return publish(input);};
  await assert.rejects(c.idle(permitted),/publication transient/);
  await assert.rejects(c.use(r=>r.exec('stale')),/retained_not_active/);
  assert.equal(s.calls.filter(x=>x[0]==='stop').length,1);
  await c.idle(permitted);await c.begin(permitted,signal());
  await c.use(r=>r.exec('fresh'));assert.equal(s.nativeHandles.length,2);
  await c.idle(permitted);assert.equal(s.calls.filter(x=>x[0]==='stop').length,2);
});

for(const committed of [false,true])test(`stop followed by release failure is retryable (commit=${committed})`,async()=>{
  const s=setup(),c=s.controller();await c.begin(permitted,signal());
  const release=s.fences.release.bind(s.fences);let fail=true;
  s.fences.release=async input=>{if(fail){fail=false;if(committed)await release(input);throw Error('release transient');}await release(input);};
  await assert.rejects(c.idle(permitted),/release transient/);
  await assert.rejects(c.use(r=>r.exec('stale')),/retained_not_active/);
  await c.idle(permitted);assert.equal(s.calls.filter(x=>x[0]==='stop').length,1);
  await c.begin(permitted,signal());await c.use(r=>r.exec('fresh'));await c.idle(permitted);
  assert.equal(s.nativeHandles.length,2);
});

test('initial publication committed before acknowledgement failure retains canonical disk and resumes fresh',async()=>{
  const s=setup(),publish=s.fences.publish.bind(s.fences);let fail=true;
  s.fences.publish=async input=>{const result=await publish(input);if(fail && input.workspaceCandidate.metadata.state==='retained'){fail=false;throw Error('lost publication acknowledgement');}return result;};
  await assert.rejects(s.controller().begin(permitted,signal()),/lost publication acknowledgement/);
  assert.equal(s.disks.size,1);assert.equal([...s.disks.values()][0].running,false);
  assert.equal(s.calls.some(x=>x[0]==='remove-owned'),false);
  const resumed=s.controller();await resumed.begin(permitted,signal());await resumed.idle(permitted);
  assert.equal(s.creates,1);
});

test('resource fence loss after allocation enqueues exact unpublished handle without stale cleanup',async()=>{
  const s=setup(),create=s.provider.create.bind(s.provider),orphans=[];
  s.provider.create=async(...args)=>{const runtime=await create(...args);[...s.rows.values()][0].active=false;return runtime;};
  await assert.rejects(s.controller({orphans:{async enqueue(input){orphans.push(input);}}}).begin(permitted,signal()),/retained_initial_cleanup_failed/);
  assert.equal(s.disks.size,1);assert.equal(orphans.length,1);assert.equal(orphans[0].sandbox.runtimeId,'box_1');
  assert.equal(s.calls.some(x=>x[0]==='stop'||x[0]==='remove-owned'),false);
});
