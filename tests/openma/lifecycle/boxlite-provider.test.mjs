const openmaSource = process.env.TANK_AUDIT_OPENMA_ROOT ? pathToFileURL(process.env.TANK_AUDIT_OPENMA_ROOT.replace(/\/?$/, '/')) : new URL('../../../vendor/openma/', import.meta.url);
import { pathToFileURL } from 'node:url';
import { BoxLiteFilesystemDurability } from '../../../vendor/openma/packages/managed-runtime-boxlite/src/guest-filesystem.ts';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { Readable, Writable } from 'node:stream';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,readFile,writeFile,rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { imageRecoveryRetryBoundary } from '../../../vendor/openma/packages/session-runtime-contract/src/continuation.ts';
const { pinnedBoxLiteProvider, RetainedBoxOperations } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/boxlite-provider.ts', openmaSource));
const scope={workspaceId:'w',environmentId:'production',sessionId:'session',workId:'tank-retained-session-v1'};
const fence={...scope,ownerId:'owner',generation:1,token:'FAKE_FENCE',expiresAt:'2099-01-01T00:00:00Z'};
const signal=()=>new AbortController().signal;
test('retained writable-memory resume proves every native host binding before opening the old runtime', async () => {
  const name = 'oma-' + createHash('sha256').update(`litebox:${scope.sessionId}`).digest('hex').slice(0, 32);
  const allocation = { image: 'pinned', volumes: [
    { hostPath: '/owned/.openma-managed-memory-workspaces/w/session/generation/store/data', guestPath: '/mnt/memory/notes', readOnly: false },
    { hostPath: '/owned/outputs', guestPath: '/mnt/session/outputs', readOnly: false },
  ] };
  const record = { id: 'owned', name, options: { volumes: allocation.volumes.map(volume => ({ host_path: volume.hostPath, guest_path: volume.guestPath, read_only: volume.readOnly })) } };
  let resumes = 0;
  const module = { BoxLiteFilesystemDurability, createBoxLiteProvider() { return { async resume() { resumes++; return { runtimeHandle: () => ({ provider: 'litebox', runtimeId: 'owned' }) }; } }; },
    createBoxLiteManagedRuntime() { return { sandbox: {} }; } };
  const provider = pinnedBoxLiteProvider({ module, allocation, leaseTtlMs: 30000,
    client: { async getInfo() { return { id: 'owned', name, state: { running: false } }; } }, readLegacyConfiguration: async () => record, isCurrent: async () => true });
  await provider.resume({ provider: 'litebox', runtimeId: 'owned' }, scope, fence, signal());
  assert.equal(resumes, 1);
  for (const index of [0, 1]) {
    record.options.volumes[index].host_path += '-foreign';
    await assert.rejects(provider.resume({ provider: 'litebox', runtimeId: 'owned' }, scope, fence, signal()), /retained_memory_binding_unproven/);
    record.options.volumes[index].host_path = allocation.volumes[index].hostPath;
  }
  record.options.volumes[0].read_only = true;
  await assert.rejects(provider.resume({ provider: 'litebox', runtimeId: 'owned' }, scope, fence, signal()), /retained_memory_binding_unproven/);
  assert.equal(resumes, 1);
});
test('public provider bridge resumes a fresh handle and reaps without resume or allocating secret env',async()=>{
  const calls=[];
  const module={
    BoxLiteFilesystemDurability,
    createBoxLiteProvider(options){calls.push(['options',options]);return {
      async create(...args){calls.push(['create',...args]);return {runtimeHandle:()=>({provider:'litebox',runtimeId:'box'})};},
      async resume(...args){calls.push(['resume',...args]);return {runtimeHandle:()=>({provider:'litebox',runtimeId:'box'})};},
    };},
    createBoxLiteManagedRuntime(){return {sandbox:{async reap(input){calls.push(['reap',input]);}}};},
  };
  const allocation={image:'pinned:sha256',rootfsPath:'/readonly-oci',cpus:1,memoryMib:512,
    security:{jailerEnabled:false,seccompEnabled:false},
    network:{mode:'enabled',allowNet:['postgres','172.16.0.5']},volumes:[{hostPath:'/owned/session',guestPath:'/mnt/session/outputs'}]};
  const p=pinnedBoxLiteProvider({module,client:{async getInfo(id){return id==='box'?{state:{running:false}}:null;}},allocation,leaseTtlMs:30000});
  const first=await p.create(scope,fence,signal());await p.resume(first.runtimeHandle(),scope,fence,signal());
  await p.reap(first.runtimeHandle(),scope,fence,signal());
  assert.deepEqual(calls.map(x=>x[0]),['options','create','resume','reap']);
  assert.deepEqual(calls[0][1].allocationOptions(),allocation);
  assert.deepEqual(calls[1][2],{});
  assert.equal('env' in calls[0][1].allocationOptions(),false);
  assert.equal(calls[3][1].lease.runtimeId,'box');
});
test('SimpleBox-compatible command/file bridge preserves current per-exec env and binary files',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'tank-retained-offline-'));
  const commands=[],children=[];
  const runtime={
    async spawnDuplexProcess(spec){
      commands.push(spec);
      if(spec.command==='python3'){
        const python=process.env.TANK_FILE_IO_TEST_PYTHON;
        assert.ok(python,'explicit Python3.13 test interpreter required');
        const child=spawn(python,spec.args,{stdio:['pipe','pipe','pipe'],env:{PATH:'/usr/bin:/bin'}});
        children.push(child);
        const exited=new Promise((resolve,reject)=>{child.once('error',reject);child.once('exit',(code,signal)=>resolve({code,signal}));});
        return {stdin:Writable.toWeb(child.stdin),stdout:Readable.toWeb(child.stdout),stderr:Readable.toWeb(child.stderr),exited,async kill(signal){child.kill(signal);}};
      }
      return {
      stdin:new WritableStream(),stdout:new Response('command output').body,stderr:new Response('warning').body,
      exited:Promise.resolve({code:7,signal:null}),async kill(){throw Error('unexpected kill');},
    };},
    async readFileBytes(){assert.fail('native copyOut must not be used');},async writeFileBytes(){assert.fail('native copyIn must not be used');},
  };
  const box=new RetainedBoxOperations(async fn=>fn(runtime));
  try {
    assert.deepEqual(await box.exec('sh',['-c','echo test'],{OPENROUTER_API_KEY:'FAKE_KEY'},{cwd:'/workspace',timeoutSecs:1}),
      {stdout:'command output',stderr:'warning',exitCode:7});
    assert.equal(commands[0].env.OPENROUTER_API_KEY,'FAKE_KEY');
    const source=join(directory,'source'),dest=join(directory,'dest');await writeFile(source,new Uint8Array([0,255,1,0]));
    const guest=join(directory,'guest');await box.copyIn(source,guest);await box.copyOut(guest,dest);
    assert.deepEqual(await readFile(dest),Buffer.from([0,255,1,0]));
    await assert.rejects(box.exec('id',[],{}, {user:'nobody'}),/retained_exec_user_not_supported/);
  } finally {for(const child of children){if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');}await Promise.all(children.map(child=>child.exitCode!==null||child.signalCode!==null?undefined:new Promise(resolve=>child.once('exit',resolve))));await rm(directory,{recursive:true,force:true});}
});
test('command timeout kills the native process rather than leaving background work',async()=>{
  let killed=false;
  const box=new RetainedBoxOperations(async fn=>fn({async spawnDuplexProcess(){return {
    stdin:new WritableStream(),stdout:new ReadableStream(),stderr:new ReadableStream(),exited:new Promise(()=>{}),
    async kill(signal){assert.equal(signal,'SIGKILL');killed=true;},
  };}}));
  await assert.rejects(box.exec('blocked',[],{}, {timeoutSecs:0.005}),/retained_exec_timeout/);assert.equal(killed,true);
});

test('replacement normalizes a real guest timeout only inside preflight and retains no-effects retry proof', async (t) => {
  let killed = 0, stopped = 0, timedOutOperations;
  const runtime = {
    runtimeHandle: () => ({ provider: 'litebox', runtimeId: 'source' }),
    async suspend() { stopped++; },
    async spawnDuplexProcess(spec) {
      if (spec.command === 'rm') return {
        stdin: new WritableStream(), stdout: new Response('').body, stderr: new Response('').body,
        exited: Promise.resolve({ code: 0, signal: null }), async kill() {},
      };
      return { stdin: new WritableStream(), stdout: new ReadableStream(), stderr: new ReadableStream(),
        exited: new Promise(() => {}), async kill(signal) { assert.equal(signal, 'SIGKILL'); killed++; } };
    },
  };
  const originalExec = RetainedBoxOperations.prototype.exec;
  t.mock.method(RetainedBoxOperations.prototype, 'exec', function(command, args, env, options) {
    if (command !== 'rm') timedOutOperations = this;
    return originalExec.call(this, command, args, env, { ...options, timeoutSecs: 0.005 });
  });
  const module = {
    BoxLiteFilesystemDurability,
    createBoxLiteProvider() { return { async resume() { return runtime; }, async create() { assert.fail('timeout must precede replacement allocation'); } }; },
    createBoxLiteManagedRuntime() { return { sandbox: { async reap() { assert.fail('source must remain published'); } } }; },
  };
  const provider = pinnedBoxLiteProvider({ module, allocation: { image: 'new' }, leaseTtlMs: 30000, isCurrent: async () => true,
    client: { async getInfo(id) { return id === 'source' ? { id, state: { running: false } } : null; } } });
  const source = { provider: 'litebox', runtimeId: 'source' };
  const target = await provider.replacementIntent(source, {}, scope, fence, signal());
  let failure;
  await assert.rejects(provider.replace({ source, target }, scope, fence, signal(), async () => {}), error => {
    failure = error.message;
    return failure === 'retained_upgrade_transfer_failed:guest_timeout';
  });
  assert.equal(killed, 1);
  assert.equal(stopped, 1);
  await assert.rejects(timedOutOperations.exec('overlap', []), /retained_runtime_requires_stop/);
  const events = [
    { id: 'old-error', type: 'session.error', error: { message: 'retained_configuration_changed' } },
    { id: 'old-idle', type: 'session.status_idle', stopReason: { type: 'retries_exhausted' } },
    { id: 'timeout-error', type: 'session.error', error: { message: failure, retryStatus: 'exhausted' } },
    { id: 'timeout-idle', type: 'session.status_idle', stopReason: { type: 'retries_exhausted' } },
  ];
  const snapshot = { revision: 4, executions: [{ id: 'original', laneId: 'sthr_primary', state: 'running', revision: 2,
    events: [{ id: 'original', type: 'user.message', content: [{ type: 'text', text: 'pending' }] }],
    attemptCount: 1, interruptRequestedAt: null, imageRecoveryAfterEventId: 'old-idle', imageRecoveryInterruptRequestedAt: null }],
    orderedEvents: events.map((event, index) => ({ event, executionId: 'original', position: { revision: index + 1, index: 0 } })) };
  assert.equal(imageRecoveryRetryBoundary(snapshot, 'original'), 'timeout-idle');
  snapshot.orderedEvents.push({ event: { id: 'effect', type: 'span.model_request_start' }, executionId: 'original', position: { revision: 5, index: 0 } });
  assert.equal(imageRecoveryRetryBoundary(snapshot, 'original'), null);
});

test('crash recovery quiesces orphan processes then resumes using a second fresh native handle',async()=>{
  const calls=[];
  const module={
    BoxLiteFilesystemDurability,
    createBoxLiteProvider(){return {async create(){throw Error('never allocate');},async resume(){calls.push('fresh-resume');return {async suspend(){calls.push('owned-stop');}};}};},
    createBoxLiteManagedRuntime(){return {sandbox:{async reap(){throw Error('never remove retained disk');}}};},
  };
  const p=pinnedBoxLiteProvider({module,client:{async getInfo(){return {state:{running:true}};}},allocation:{image:'pinned'},leaseTtlMs:30000,isCurrent:async()=>true});
  await p.resume({provider:'litebox',runtimeId:'owned'},scope,fence,signal());
  assert.deepEqual(calls,['fresh-resume','owned-stop','fresh-resume']);
});

test('unpublished existing native disk and missing published disk fail closed without provider allocation',async()=>{
  let effects=0;
  const module={BoxLiteFilesystemDurability,createBoxLiteProvider(){return {async create(){effects++;},async resume(){effects++;}};},createBoxLiteManagedRuntime(){return {sandbox:{async reap(){effects++;}}};}};
  const existing=pinnedBoxLiteProvider({module,client:{async getInfo(){return {state:{running:false}};}},allocation:{image:'pinned'},leaseTtlMs:30000});
  await assert.rejects(existing.create(scope,fence,signal()),/retained_unpublished_runtime_exists/);
  const missing=pinnedBoxLiteProvider({module,client:{async getInfo(){return null;}},allocation:{image:'pinned'},leaseTtlMs:30000});
  await assert.rejects(missing.resume({provider:'litebox',runtimeId:'lost'},scope,fence,signal()),/retained_runtime_missing/);
  assert.equal(effects,0);
});

test('unconfirmed timeout poisons all further guest I/O until fresh stopped/resumed runtime',async()=>{
  const directory=await mkdtemp(join(tmpdir(),'tank-retained-poisoned-'));
  let spawned=0,killCalled=false,releaseKill;
  try {
  const killPending=new Promise(resolve=>{releaseKill=resolve;});
  const blocked={async spawnDuplexProcess(){spawned++;return {stdin:new WritableStream(),stdout:new ReadableStream(),stderr:new ReadableStream(),exited:new Promise(()=>{}),async kill(){killCalled=true;await killPending;}};},async readFileBytes(){assert.fail('poisoned read');}};
  let runtime=blocked;
  const box=new RetainedBoxOperations(async fn=>fn(runtime));
  await assert.rejects(box.exec('blocked',[],{}, {timeoutSecs:0.005}),/retained_exec_timeout/);
  assert.equal(killCalled,true);
  await assert.rejects(box.exec('overlap',[]),/retained_runtime_requires_stop/);
  await assert.rejects(box.copyOut('/workspace/raw',join(directory,'unused')),/retained_runtime_requires_stop/);
  assert.equal(spawned,1);
  releaseKill();runtime={async spawnDuplexProcess(){spawned++;return {stdin:new WritableStream(),stdout:new Response('fresh').body,stderr:new Response('').body,exited:Promise.resolve({code:0,signal:null}),async kill(){}};}};
  assert.equal((await box.exec('fresh',[])).stdout,'fresh');assert.equal(spawned,2);
  } finally {releaseKill?.();await rm(directory,{recursive:true,force:true});}
});

test('one original deadline bounds deferred spawn and kills its late exact process without stdin or overlap',{timeout:1500},async()=>{
  let resolveSpawn,releaseKill,rejectExit,spawned=0,killed=0,closed=0,cancelled=0;
  const pendingSpawn=new Promise(resolve=>{resolveSpawn=resolve;});
  const killPending=new Promise(resolve=>{releaseKill=resolve;});
  const exit=new Promise((_,reject)=>{rejectExit=reject;});
  const runtime={async spawnDuplexProcess(){spawned++;return pendingSpawn;}};
  const box=new RetainedBoxOperations(async fn=>fn(runtime));
  const started=performance.now();
  await assert.rejects(box.exec('late-spawn',[],{}, {timeoutSecs:0.01}),/retained_exec_timeout/);
  assert.ok(performance.now()-started<500,'spawn acknowledgement must not escape the original deadline');
  await assert.rejects(box.exec('overlap-before-spawn',[]),/retained_runtime_requires_stop/);
  resolveSpawn({stdin:new WritableStream({close(){closed++;}}),
    stdout:new ReadableStream({cancel(){cancelled++;}}),stderr:new ReadableStream({cancel(){cancelled++;}}),
    exited:exit,async kill(signal){assert.equal(signal,'SIGKILL');killed++;await killPending;}});
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(killed,1);assert.equal(closed,0);assert.equal(cancelled,2);
  await assert.rejects(box.exec('overlap-after-late-spawn',[]),/retained_runtime_requires_stop/);
  assert.equal(spawned,1);releaseKill();rejectExit(Error('late native exit rejection consumed'));
  await new Promise(resolve=>setImmediate(resolve));
});

test('one original deadline bounds deferred stdin close and refuses new I/O while kill and exit remain unconfirmed',{timeout:1500},async()=>{
  let releaseClose,releaseKill,resolveExit,spawned=0,killed=0,closeStarted=false;
  const closePending=new Promise(resolve=>{releaseClose=resolve;});
  const killPending=new Promise(resolve=>{releaseKill=resolve;});
  const exited=new Promise(resolve=>{resolveExit=resolve;});
  const runtime={async spawnDuplexProcess(){spawned++;return {
    stdin:new WritableStream({async close(){closeStarted=true;await closePending;}}),
    stdout:new Response('already produced output').body,stderr:new Response('').body,exited,
    async kill(){killed++;await killPending;},
  };}};
  const box=new RetainedBoxOperations(async fn=>fn(runtime));
  const started=performance.now();
  await assert.rejects(box.exec('stdin-stalled',[],{}, {timeoutSecs:0.01}),/retained_exec_timeout/);
  assert.ok(closeStarted);assert.ok(performance.now()-started<500,'stdin close must not extend the requested deadline');
  assert.equal(killed,1);await assert.rejects(box.exec('overlap',[]),/retained_runtime_requires_stop/);assert.equal(spawned,1);
  releaseClose();releaseKill();resolveExit({code:137,signal:'SIGKILL'});
  await new Promise(resolve=>setImmediate(resolve));
  await assert.rejects(box.exec('still-poisoned-after-ack',[]),/retained_runtime_requires_stop/);
});
