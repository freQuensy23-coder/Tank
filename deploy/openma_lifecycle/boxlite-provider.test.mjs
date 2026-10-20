import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,readFile,writeFile,rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pinnedBoxLiteProvider, RetainedBoxOperations } from './boxlite-provider.ts';
const scope={workspaceId:'w',environmentId:'production',sessionId:'session',workId:'tank-retained-session-v1'};
const fence={...scope,ownerId:'owner',generation:1,token:'FAKE_FENCE',expiresAt:'2099-01-01T00:00:00Z'};
const signal=()=>new AbortController().signal;
test('public provider bridge resumes a fresh handle and reaps without resume or allocating secret env',async()=>{
  const calls=[];
  const module={
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
  const commands=[],files=new Map();
  const runtime={
    async spawnDuplexProcess(spec){commands.push(spec);return {
      stdin:new WritableStream(),stdout:new Response('command output').body,stderr:new Response('warning').body,
      exited:Promise.resolve({code:7,signal:null}),async kill(){throw Error('unexpected kill');},
    };},
    async readFileBytes(path){return files.get(path);},async writeFileBytes(path,bytes){files.set(path,new Uint8Array(bytes));return path;},
  };
  const box=new RetainedBoxOperations(async fn=>fn(runtime));
  try {
    assert.deepEqual(await box.exec('sh',['-c','echo test'],{OPENROUTER_API_KEY:'FAKE_KEY'},{cwd:'/workspace',timeoutSecs:1}),
      {stdout:'command output',stderr:'warning',exitCode:7});
    assert.equal(commands[0].env.OPENROUTER_API_KEY,'FAKE_KEY');
    const source=join(directory,'source'),dest=join(directory,'dest');await writeFile(source,new Uint8Array([0,255,1,0]));
    await box.copyIn(source,'/workspace/raw');await box.copyOut('/workspace/raw',dest);
    assert.deepEqual(await readFile(dest),Buffer.from([0,255,1,0]));
    await assert.rejects(box.exec('id',[],{}, {user:'nobody'}),/retained_exec_user_not_supported/);
  } finally {await rm(directory,{recursive:true,force:true});}
});
test('command timeout kills the native process rather than leaving background work',async()=>{
  let killed=false;
  const box=new RetainedBoxOperations(async fn=>fn({async spawnDuplexProcess(){return {
    stdin:new WritableStream(),stdout:new ReadableStream(),stderr:new ReadableStream(),exited:new Promise(()=>{}),
    async kill(signal){assert.equal(signal,'SIGKILL');killed=true;},
  };}}));
  await assert.rejects(box.exec('blocked',[],{}, {timeoutSecs:0.005}),/retained_exec_timeout/);assert.equal(killed,true);
});

test('crash recovery quiesces orphan processes then resumes using a second fresh native handle',async()=>{
  const calls=[];
  const module={
    createBoxLiteProvider(){return {async create(){throw Error('never allocate');},async resume(){calls.push('fresh-resume');return {async suspend(){calls.push('owned-stop');}};}};},
    createBoxLiteManagedRuntime(){return {sandbox:{async reap(){throw Error('never remove retained disk');}}};},
  };
  const p=pinnedBoxLiteProvider({module,client:{async getInfo(){return {state:{running:true}};}},allocation:{image:'pinned'},leaseTtlMs:30000,isCurrent:async()=>true});
  await p.resume({provider:'litebox',runtimeId:'owned'},scope,fence,signal());
  assert.deepEqual(calls,['fresh-resume','owned-stop','fresh-resume']);
});

test('unpublished existing native disk and missing published disk fail closed without provider allocation',async()=>{
  let effects=0;
  const module={createBoxLiteProvider(){return {async create(){effects++;},async resume(){effects++;}};},createBoxLiteManagedRuntime(){return {sandbox:{async reap(){effects++;}}};}};
  const existing=pinnedBoxLiteProvider({module,client:{async getInfo(){return {state:{running:false}};}},allocation:{image:'pinned'},leaseTtlMs:30000});
  await assert.rejects(existing.create(scope,fence,signal()),/retained_unpublished_runtime_exists/);
  const missing=pinnedBoxLiteProvider({module,client:{async getInfo(){return null;}},allocation:{image:'pinned'},leaseTtlMs:30000});
  await assert.rejects(missing.resume({provider:'litebox',runtimeId:'lost'},scope,fence,signal()),/retained_runtime_missing/);
  assert.equal(effects,0);
});

test('unconfirmed timeout poisons all further guest I/O until fresh stopped/resumed runtime',async()=>{
  let spawned=0,killCalled=false,releaseKill;
  const killPending=new Promise(resolve=>{releaseKill=resolve;});
  const blocked={async spawnDuplexProcess(){spawned++;return {stdin:new WritableStream(),stdout:new ReadableStream(),stderr:new ReadableStream(),exited:new Promise(()=>{}),async kill(){killCalled=true;await killPending;}};},async readFileBytes(){assert.fail('poisoned read');}};
  let runtime=blocked;
  const box=new RetainedBoxOperations(async fn=>fn(runtime));
  await assert.rejects(box.exec('blocked',[],{}, {timeoutSecs:0.005}),/retained_exec_timeout/);
  assert.equal(killCalled,true);
  await assert.rejects(box.exec('overlap',[]),/retained_runtime_requires_stop/);
  await assert.rejects(box.copyOut('/workspace/raw','/unused'),/retained_runtime_requires_stop/);
  assert.equal(spawned,1);
  // Only the lifecycle's successful native stop/fresh resume supplies a new runtime.
  releaseKill();runtime={async spawnDuplexProcess(){spawned++;return {stdin:new WritableStream(),stdout:new Response('fresh').body,stderr:new Response('').body,exited:Promise.resolve({code:0,signal:null}),async kill(){}};}};
  assert.equal((await box.exec('fresh',[])).stdout,'fresh');assert.equal(spawned,2);
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
