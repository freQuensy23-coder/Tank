const openmaSource = process.env.TANK_AUDIT_OPENMA_ROOT ? pathToFileURL(process.env.TANK_AUDIT_OPENMA_ROOT.replace(/\/?$/, '/')) : new URL('../../../vendor/openma/', import.meta.url);
import { pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { Readable, Writable } from 'node:stream';
import { after, test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp,readFile,writeFile,rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
const { pinnedBoxLiteProvider, RetainedBoxOperations } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/boxlite-provider.ts', openmaSource));
const nativeHome=await mkdtemp(join(tmpdir(),'tank-retained-native-'));
after(()=>rm(nativeHome,{recursive:true,force:true}));
const provider=options=>pinnedBoxLiteProvider({...options,nativeHome});
const scope={workspaceId:'w',environmentId:'production',sessionId:'session',workId:'tank-retained-session-v1'};
const fence={...scope,ownerId:'owner',generation:1,token:'FAKE_FENCE',expiresAt:'2099-01-01T00:00:00Z'};
const signal=()=>new AbortController().signal;
const name='oma-'+createHash('sha256').update('litebox:'+scope.sessionId).digest('hex').slice(0,32);
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
  const p=provider({module,client:{async getInfo(id){return id==='box'?{id,name,state:{running:false,status:'Stopped'}}:null;}},allocation,leaseTtlMs:30000});
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
test('command timeout signals the exact native process',async()=>{
  let killed=false,exit;
  const exited=new Promise(resolve=>{exit=resolve;});
  const box=new RetainedBoxOperations(async fn=>fn({async spawnDuplexProcess(){return {
    stdin:new WritableStream(),stdout:new Response('').body,stderr:new Response('').body,exited,
    async kill(signal){assert.equal(signal,'SIGKILL');killed=true;exit({code:137,signal});},
  };}}));
  await assert.rejects(box.exec('blocked',[],{}, {timeoutSecs:0.005}),/retained_exec_timeout/);
  assert.equal(killed,true);
});

test('crash recovery stops the exact box before a single fresh resume',async()=>{
  const calls=[];
  let running=true;
  const info=()=>({id:'owned',name,state:{running,status:running?'Running':'Stopped'}});
  const client={async getInfo(){return info();},async get(){return {id:'owned',name,info,
    async exec(command){assert.equal(command,'/bin/sync');return {async wait(){return {exitCode:0};}};},
    async stop(){calls.push('owned-stop');running=false;}};}};
  const module={
    createBoxLiteProvider(){return {async create(){throw Error('never allocate');},async resume(){calls.push('fresh-resume');return {runtimeHandle:()=>({provider:'litebox',runtimeId:'owned'})};}};},
    createBoxLiteManagedRuntime(){return {sandbox:{async reap(){throw Error('never remove retained disk');}}};},
  };
  const p=provider({module,client,allocation:{image:'pinned'},leaseTtlMs:30000,isCurrent:async()=>true});
  await p.resume({provider:'litebox',runtimeId:'owned'},scope,fence,signal());
  assert.deepEqual(calls,['owned-stop','fresh-resume']);
});

test('unpublished existing native disk and missing published disk fail closed without provider allocation',async()=>{
  let effects=0;
  const module={createBoxLiteProvider(){return {async create(){effects++;},async resume(){effects++;}};},createBoxLiteManagedRuntime(){return {sandbox:{async reap(){effects++;}}};}};
  const existing=provider({module,client:{async getInfo(){return {state:{running:false}};}},allocation:{image:'pinned'},leaseTtlMs:30000});
  await assert.rejects(existing.create(scope,fence,signal()),/retained_unpublished_runtime_exists/);
  const missing=provider({module,client:{async getInfo(){return null;}},allocation:{image:'pinned'},leaseTtlMs:30000});
  await assert.rejects(missing.resume({provider:'litebox',runtimeId:'lost'},scope,fence,signal()),/retained_runtime_missing/);
  assert.equal(effects,0);
});

test('leftover names only the runtime under the allocation name of the scope and has no effects',async()=>{
  const { createHash }=await import('node:crypto');
  const name='oma-'+createHash('sha256').update('litebox:'+scope.sessionId).digest('hex').slice(0,32),asked=[];
  let effects=0,info=null,current=true;
  const module={createBoxLiteProvider(){return {async create(){effects++;},async resume(){effects++;}};},createBoxLiteManagedRuntime(){return {sandbox:{async reap(){effects++;}}};}};
  const p=provider({module,client:{async getInfo(id){asked.push(id);return info;},async remove(){effects++;}},allocation:{image:'current'},leaseTtlMs:30000,isCurrent:async()=>current});
  assert.equal(await p.leftover(scope,fence,signal()),null);
  info={id:'native',name,state:{running:false}};
  assert.deepEqual(await p.leftover(scope,fence,signal()),{provider:'litebox',runtimeId:'native'});
  info={id:'native',name:'foreign',state:{running:false}};
  await assert.rejects(p.leftover(scope,fence,signal()),/retained_cleanup_ownership_mismatch/);
  current=false;await assert.rejects(p.leftover(scope,fence,signal()),/retained_fence_lost/);
  const aborted=new AbortController();aborted.abort(Error('cancelled'));current=true;
  await assert.rejects(p.leftover(scope,fence,aborted.signal),/cancelled/);
  assert.deepEqual(asked,[name,name,name]);assert.equal(effects,0);
});
test('timeout poisons the runtime while deferred spawn and native kill drain',async()=>{
  let resolveSpawn,releaseKill,rejectExit,spawned=0,closed=0,killStarted;
  const pendingSpawn=new Promise(resolve=>{resolveSpawn=resolve;});
  const killPending=new Promise(resolve=>{releaseKill=resolve;});
  const killing=new Promise(resolve=>{killStarted=resolve;});
  const exited=new Promise((_,reject)=>{rejectExit=reject;});
  const runtime={async spawnDuplexProcess(){spawned++;return pendingSpawn;}};
  const box=new RetainedBoxOperations(async fn=>fn(runtime));
  let settled=false;
  const failed=assert.rejects(box.exec('late-spawn',[],{}, {timeoutSecs:0.01}),/retained_exec_timeout/).then(()=>{settled=true;});
  await new Promise(resolve=>setTimeout(resolve,30));
  assert.equal(settled,true);
  await assert.rejects(box.exec('overlap-before-spawn',[]),/retained_runtime_requires_stop/);
  resolveSpawn({stdin:new WritableStream({close(){closed++;}}),stdout:new Response('').body,stderr:new Response('').body,
    exited,async kill(signal){assert.equal(signal,'SIGKILL');killStarted();await killPending;}});
  await killing;
  await assert.rejects(box.exec('overlap-after-late-spawn',[]),/retained_runtime_requires_stop/);
  assert.equal(closed,0);assert.equal(spawned,1);assert.equal(settled,true);
  releaseKill();rejectExit(Error('late native exit rejection consumed'));
  await failed;
});

test('a stalled stdin close and kill do not extend the caller timeout',async()=>{
  let releaseClose,releaseKill,resolveExit,killStarted;
  const closePending=new Promise(resolve=>{releaseClose=resolve;});
  const killPending=new Promise(resolve=>{releaseKill=resolve;});
  const killing=new Promise(resolve=>{killStarted=resolve;});
  const exited=new Promise(resolve=>{resolveExit=resolve;});
  const runtime={async spawnDuplexProcess(){return {
    stdin:new WritableStream({close(){return closePending;}}),
    stdout:new Response('').body,stderr:new Response('').body,exited,
    async kill(){killStarted();await killPending;},
  };}};
  const box=new RetainedBoxOperations(async fn=>fn(runtime));
  let settled=false;
  const failed=assert.rejects(box.exec('stdin-stalled',[],{}, {timeoutSecs:0.01}),/retained_exec_timeout/).then(()=>{settled=true;});
  await killing;
  await assert.rejects(box.exec('overlap',[]),/retained_runtime_requires_stop/);
  releaseKill();resolveExit({code:137,signal:'SIGKILL'});
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(settled,true);
  releaseClose();
  await failed;
  await assert.rejects(box.exec('still-poisoned-after-ack',[]),/retained_runtime_requires_stop/);
});

test('schema2 owner metadata permits only exact scope-owned archive and rejects foreign owner',async()=>{
  const { createHash }=await import('node:crypto');
  const name='oma-'+createHash('sha256').update(JSON.stringify(scope)).digest('hex').slice(0,32)+'-upgrade-'+'a'.repeat(32);
  const removed=[];
  const module={createBoxLiteProvider(){return {};},createBoxLiteManagedRuntime(){return {sandbox:{async reap(){assert.fail('must use exact owner cleanup');}}};}};
  const p=provider({module,client:{async getInfo(){return {id:'native',name,state:{running:false}};},async remove(id){removed.push(id);}},allocation:{image:'current'},leaseTtlMs:30000,isCurrent:async()=>true});
  await assert.rejects(p.reap({provider:'litebox',runtimeId:'native',ownershipName:name.replace(/a$/,'b')},scope,fence,signal()),/retained_cleanup_ownership_mismatch/);
  assert.deepEqual(removed,[]);
  await p.reap({provider:'litebox',runtimeId:'native',ownershipName:name},scope,fence,signal());assert.deepEqual(removed,['native']);
});

test('a hung stopped-state observation times out and retries without another stop',async()=>{
  let running=true,stopCount=0,hangConfirmation=true;
  const info=()=>({id:'owned',name,state:{running,status:running?'Running':'Stopped'}});
  const client={
    async getInfo(){if(!running&&hangConfirmation)return new Promise(()=>{});return info();},
    async get(){return {id:'owned',name,info,
      async exec(){return {async wait(){return {exitCode:0};}};},
      async stop(){stopCount++;running=false;}};},
  };
  const module={createBoxLiteProvider(){return {};},createBoxLiteManagedRuntime(){return {sandbox:{}};}};
  const p=provider({module,client,allocation:{image:'pinned'},leaseTtlMs:30000,stopTimeoutMs:150});
  const stop=()=>p.quiesceExact({provider:'litebox',runtimeId:'owned'},scope,async()=>{});
  await assert.rejects(stop(),/retained_recovery_stop_timeout/);
  assert.equal(stopCount,1);
  hangConfirmation=false;
  await stop();
  assert.equal(stopCount,1);
});

test('a stop still in native code holds the lock after the caller deadline',async()=>{
  let running=true,releaseStop,settledStop,stopCount=0;
  const pendingStop=new Promise(resolve=>{releaseStop=resolve;});
  const nativeSettled=new Promise(resolve=>{settledStop=resolve;});
  const info=()=>({id:'owned',name,state:{running,status:running?'Running':'Stopped'}});
  const client={
    async getInfo(){return info();},
    async get(){return {id:'owned',name,info,
      async exec(){return {async wait(){return {exitCode:0};}};},
      async stop(){stopCount++;await pendingStop;running=false;settledStop();}};},
  };
  let resumes=0;
  const module={
    createBoxLiteProvider(){return {async resume(){resumes++;return {};}};},
    createBoxLiteManagedRuntime(){return {sandbox:{}};},
  };
  const p=provider({module,client,allocation:{image:'pinned'},leaseTtlMs:30000,stopTimeoutMs:150});
  const handle={provider:'litebox',runtimeId:'owned'};
  await assert.rejects(p.quiesceExact(handle,scope,async()=>{}),/retained_recovery_stop_timeout/);
  assert.equal(stopCount,1);
  await assert.rejects(p.resume(handle,scope,fence,signal()),/retained_native_busy/);
  assert.equal(resumes,0);
  releaseStop();await nativeSettled;
  await p.quiesceExact(handle,scope,async()=>{});
  await p.resume(handle,scope,fence,signal());
  assert.equal(stopCount,1);
  assert.equal(resumes,1);
});

test('same-home native mutations contend across processes while another scope can proceed',async()=>{
  const {once}=await import('node:events');
  const lockURL=new URL('packages/control-plane-node/src/lib/tank-retained/native-lock.ts',openmaSource);
  const {existsSync}=await import('node:fs');
  const readyFile=join(nativeHome,'child-holds-lock'),releaseFile=join(nativeHome,'release-child-lock');
  const child=spawn(process.execPath,['--input-type=module','-e',`
    import {withNativeLock} from ${JSON.stringify(lockURL.href)};
    import {existsSync,writeFileSync} from 'node:fs';
    import {setTimeout} from 'node:timers/promises';
    const [home,name,ready,release]=process.argv.slice(1);
    await withNativeLock(home,name,'shared',async()=>{
      writeFileSync(ready,'locked');
      for(let i=0;i<400;i++){
        if(existsSync(release))return;
        await setTimeout(10);
      }
      throw Error('lock test release missing');
    });
  `,nativeHome,name,readyFile,releaseFile],{stdio:'inherit'});
  const exit=once(child,'exit');
  try {
    for(let i=0;i<200&&!existsSync(readyFile);i++)await new Promise(resolve=>setTimeout(resolve,10));
    assert.equal(await readFile(readyFile,'utf8'),'locked');
    const asked=[];
    const module={createBoxLiteProvider(){return {};},createBoxLiteManagedRuntime(){return {sandbox:{}};}};
    const p=provider({module,client:{async getInfo(id){asked.push(id);return {id,name,state:{running:false,status:'Stopped'}};}},allocation:{image:'pinned'},leaseTtlMs:30000});
    const handle={provider:'litebox',runtimeId:'owned'};
    await assert.rejects(p.quiesceExact(handle,scope,async()=>{}),/retained_native_busy/);
    assert.deepEqual(asked,[]);
    await assert.rejects(p.quiesceExact(handle,{...scope,sessionId:'other'},async()=>{}),/retained_recovery_resident_changed/);
    await writeFile(releaseFile,'release');assert.deepEqual(await exit,[0,null]);
    await p.quiesceExact(handle,scope,async()=>{});
  } finally {
    await writeFile(releaseFile,'release');
    await exit;
  }
});
