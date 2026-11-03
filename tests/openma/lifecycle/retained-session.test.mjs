const openmaSource = process.env.TANK_AUDIT_OPENMA_ROOT ? pathToFileURL(process.env.TANK_AUDIT_OPENMA_ROOT.replace(/\/?$/, '/')) : new URL('../../../vendor/openma/', import.meta.url);
import { pathToFileURL } from 'node:url';
import { test } from 'node:test';
import assert from 'node:assert/strict';
const { RetainedSessionLifecycle } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/retained-session.ts', openmaSource));

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

const { stableDigest } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/runtime-identity.ts', openmaSource));
const schema2Identity = {version:2,image:'sha256:'+'d'.repeat(64),compatibility:{scope:'sha256:'+'e'.repeat(64)}};
async function schema2Fixture() {
  const s=setup(),c=s.controller();await c.begin(permitted,signal());await c.idle(permitted);
  const row=[...s.rows.values()][0],scope=JSON.parse(row.publication.workspaceCandidate.metadata.scope);
  const manifest={handle:{provider:'litebox',runtimeId:'box_1',ownershipName:'recorded-owner'},state:'retained',archived:false,identity:schema2Identity,retired:[]};
  const save=()=>{const encoded=JSON.stringify(manifest);row.publication.workspaceCandidate={id:'retained-box_1',contentHash:stableDigest({scope,manifest:encoded}),metadata:{schema:2,scope:JSON.stringify(scope),manifest:encoded,provider:'litebox',runtimeId:'box_1',state:manifest.state}};};
  save();return {s,manifest,save};
}
test('schema2 resumes exact disk and preserves identity and owner across idle and cold archive',async()=>{
  const {s}=await schema2Fixture(),resume=s.provider.resume.bind(s.provider);
  s.provider.resume=async(handle,...args)=>{assert.equal(handle.ownershipName,'recorded-owner');return resume(handle,...args);};
  const c=s.controller({identity:schema2Identity});await c.begin(permitted,signal());await c.idle(permitted);
  const candidate=[...s.rows.values()][0].publication.workspaceCandidate;
  assert.equal(candidate.metadata.schema,2);assert.equal(JSON.parse(candidate.metadata.manifest).handle.ownershipName,'recorded-owner');
  await s.controller().archive(permitted,signal());assert.equal(s.disks.size,0);assert.equal(s.creates,1);
  await assert.rejects(s.controller().begin(permitted,signal()),/retained_session_archived/);
});
test('image update replaces only the native runtime and discards files packages and browser profile',async()=>{
  const {s}=await schema2Fixture();
  const old=s.disks.get('box_1');old.files['/workspace/value']='private';old.files['/usr/local/package']='installed';old.profile.cookie='secret';
  const identity={...schema2Identity,image:'sha256:'+'f'.repeat(64)};
  const updated=s.controller({identity});await updated.begin(permitted,signal());
  await updated.use(async runtime=>{const {disk}=await runtime.exec('read');assert.notEqual(disk.id,'box_1');assert.deepEqual(disk.files,{});assert.deepEqual(disk.profile,{});assert.equal(JSON.parse(disk.scope).sessionId,'session_A');});
  await updated.idle(permitted);
  assert.equal(s.creates,2);assert.equal(s.disks.has('box_1'),false);
  assert.equal(s.calls.some(call=>call[0]==='fresh-get'),false);
  const manifest=JSON.parse([...s.rows.values()][0].publication.workspaceCandidate.metadata.manifest);
  assert.deepEqual(manifest.identity,identity);
  const next=s.controller({identity});await next.begin(permitted,signal());await next.idle(permitted);assert.equal(s.creates,2);
});

test('legacy update retires the exact old disk without adopting its opaque configuration',async()=>{
  const s=setup(),old=s.controller();await old.begin(permitted,signal());
  await old.use(async runtime=>{const {disk}=await runtime.exec('write');disk.files.private='discard';});
  await old.idle(permitted);
  const updated=s.controller({fingerprint:'sha256:'+'b'.repeat(64),identity:schema2Identity});
  await updated.begin(permitted,signal());
  await updated.use(async runtime=>{const {disk}=await runtime.exec('read');assert.deepEqual(disk.files,{});assert.equal(disk.id,'box_2');});
  await updated.idle(permitted);
  assert.equal(s.disks.has('box_1'),false);assert.equal(s.calls.some(call=>call[0]==='fresh-get'),false);
  assert.equal([...s.rows.values()][0].publication.workspaceCandidate.metadata.schema,2);
});

for(const fault of ['integrity','scope','provider','fingerprint','execution','resource'])test(`legacy replacement refuses ${fault} before deletion`,async()=>{
  const s=setup(),old=s.controller();await old.begin(permitted,signal());await old.idle(permitted);
  const candidate=[...s.rows.values()][0].publication.workspaceCandidate;
  if(fault==='integrity')candidate.contentHash='sha256:'+'0'.repeat(64);
  if(fault==='scope')candidate.metadata.scope='{}';
  if(fault==='provider')candidate.metadata.provider='foreign';
  if(fault==='fingerprint')candidate.metadata.fingerprint='broken';
  if(fault==='resource')s.fences.isCurrent=async()=>false;
  await assert.rejects(s.controller({fingerprint:'sha256:'+'b'.repeat(64),identity:schema2Identity}).begin(async()=>{if(fault==='execution')throw Error('unauthorized');},signal()));
  assert.equal(s.disks.has('box_1'),true);assert.equal(s.creates,1);assert.equal(s.calls.some(call=>call[0]==='remove-owned'),false);
});

test('new identified runtimes publish schema2 without adopting legacy provenance',async()=>{
  const s=setup(),c=s.controller({identity:schema2Identity});await c.begin(permitted,signal());await c.idle(permitted);
  const candidate=[...s.rows.values()][0].publication.workspaceCandidate;
  assert.equal(candidate.metadata.schema,2);assert.deepEqual(JSON.parse(candidate.metadata.manifest).identity,schema2Identity);
});

for(const boundary of ['cancelled','execution_lost','resource_lost'])test(`image replacement refuses ${boundary} before deleting old runtime`,async()=>{
  const {s}=await schema2Fixture(),before=JSON.stringify([...s.rows.values()][0].publication);
  const identity={...schema2Identity,image:'sha256:'+'f'.repeat(64)},abort=new AbortController();
  if(boundary==='cancelled')abort.abort(Error('cancelled'));
  let checks=0;
  const authorized=async()=>{checks++;if(boundary==='execution_lost'&&checks>1)throw Error('execution_lost');};
  if(boundary==='resource_lost')s.fences.isCurrent=async()=>false;
  await assert.rejects(s.controller({identity}).begin(authorized,abort.signal));
  assert.equal(s.disks.has('box_1'),true);assert.equal(s.creates,1);
  assert.equal(JSON.stringify([...s.rows.values()][0].publication),before);
  assert.equal(s.calls.some(call=>call[0]==='remove-owned'),false);
});

test('replacement retries deletion acknowledgement failure without resuming or copying old runtime',async()=>{
  const {s}=await schema2Fixture(),reap=s.provider.reap.bind(s.provider);
  let fail=true;s.provider.reap=async(...args)=>{await reap(...args);if(fail){fail=false;throw Error('cleanup acknowledgement lost');}};
  const identity={...schema2Identity,image:'sha256:'+'f'.repeat(64)};
  await assert.rejects(s.controller({identity}).begin(permitted,signal()),/cleanup acknowledgement lost/);
  assert.equal(s.disks.size,0);assert.equal(s.creates,1);
  const retry=s.controller({identity});await retry.begin(permitted,signal());await retry.idle(permitted);
  assert.equal(s.disks.size,1);assert.equal(s.creates,2);assert.equal(s.calls.some(call=>call[0]==='fresh-get'),false);
});

for(const kind of ['compatibility','missing','integrity','upgrading'])test(`schema2 refuses ${kind} without migration or allocation`,async()=>{
  const {s,manifest,save}=await schema2Fixture();
  let identity=structuredClone(schema2Identity);
  if(kind==='compatibility')identity.compatibility.scope='sha256:'+'f'.repeat(64);
  if(kind==='missing')identity=undefined;
  if(kind==='upgrading'){manifest.state='upgrading';manifest.upgrade={source:manifest.handle,target:{provider:'litebox',runtimeId:'target'},owner:'old',identity:schema2Identity};save();}
  if(kind==='integrity')[...s.rows.values()][0].publication.workspaceCandidate.metadata.manifest+=' ';
  await assert.rejects(s.controller({identity}).begin(permitted,signal()),/retained_(configuration_changed|identity_required|manifest_invalid|upgrade_incomplete)/);
  assert.equal(s.creates,1);assert.equal(s.calls.some(call=>call[0]==='fresh-get'),false);assert.equal(s.disks.size,1);
});
test('schema2 archive cleans retired and interrupted target handles without resuming',async()=>{
  const {s,manifest,save}=await schema2Fixture();
  manifest.state='upgrading';manifest.retired=[{provider:'litebox',runtimeId:'retired'}];
  manifest.upgrade={source:manifest.handle,target:{provider:'litebox',runtimeId:'target'},owner:'old',identity:schema2Identity};save();
  await s.controller().archive(permitted,signal());
  assert.deepEqual(s.calls.filter(call=>call[0]==='remove-owned').map(call=>call[1]),['retired','target','box_1']);
  assert.equal(s.calls.some(call=>call[0]==='fresh-get'),false);
});
