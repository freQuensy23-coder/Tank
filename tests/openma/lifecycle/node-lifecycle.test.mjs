const openmaSource = process.env.TANK_AUDIT_OPENMA_ROOT ? pathToFileURL(process.env.TANK_AUDIT_OPENMA_ROOT.replace(/\/?$/, '/')) : new URL('../../../vendor/openma/', import.meta.url);
import { pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';
import { registerHooks } from 'node:module';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { after, test } from 'node:test';
const nativeHome=await mkdtemp(join(tmpdir(),'tank-retained-native-'));
after(()=>rm(nativeHome,{recursive:true,force:true}));
import assert from 'node:assert/strict';
registerHooks({resolve(specifier,context,nextResolve){
  if(specifier==='@open-managed-agents/observability')return {url:new URL('packages/observability/src/index.ts',openmaSource).href,shortCircuit:true};
  if(specifier==='@boxlite-ai/boxlite')return {url:'data:text/javascript,'+encodeURIComponent('export class SimpleBox {}'),shortCircuit:true};
  return nextResolve(specifier,context);
}});
const { NodeRetainedLifecycle } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/node-lifecycle.ts', openmaSource));
import { setup } from './fakes.mjs';
function fixture() {
  const s=setup(),allocations=[];
  const decorate=runtime=>Object.assign(runtime,{async spawnDuplexProcess(spec){
    const {disk,env}=await runtime.exec(spec.command,spec.env);let out='';
    if(spec.command==='write'){disk.files['/workspace/value']='persisted';disk.profile.cookie='saved';out=env.OPENROUTER_API_KEY;}
    if(spec.command==='read')out=JSON.stringify({workspace:disk.files['/workspace/value'],profile:disk.profile.cookie,key:env.OPENROUTER_API_KEY});
    return {stdin:new WritableStream(),stdout:new Response(out).body,stderr:new Response('').body,
      exited:Promise.resolve({code:0,signal:null}),async kill(){}};
  }});
  const module={
    createBoxLiteProvider(options){allocations.push(options);return {
      async create(_ctx,_env,acq){return decorate(await s.provider.create(acq.scope));},
      async resume(handle,_ctx,_env,acq){return decorate(await s.provider.resume(handle,acq.scope));},
    };},
    createBoxLiteManagedRuntime(){return {sandbox:{async reap({lease,scope}){await s.provider.reap(lease,scope);}}};},
  };
  const name='oma-'+createHash('sha256').update('litebox:session_A').digest('hex').slice(0,32);
  const client={
    async getInfo(id){
      const disk=s.disks.get(id)??(id===name?[...s.disks.values()][0]:undefined);
      return disk?{id:disk.id,name,state:{running:disk.running,status:disk.running?'Running':'Stopped'}}:null;
    },
    async get(id){
      const disk=s.disks.get(id);
      return disk?{id,name,info:()=>client.getInfo(id),
        async exec(command){assert.equal(command,'/bin/sync');return {async wait(){return {exitCode:0};}};},
        async stop(){s.calls.push(['stop',id]);disk.running=false;}}:null;
    },
  };
  const lifecycle=(changes={})=>new NodeRetainedLifecycle({fences:s.fences,providerModule:module,client,ownerId:'controller',leaseTtlMs:30000,nativeHome,
    fingerprint:async()=>s.options.fingerprint,assertExecutionCurrent:async fence=>{assert.equal(fence.current,true);},...changes});
  const allocation={image:'image-pinned',cpus:1,memoryMib:512,network:{mode:'enabled'},volumes:[]};
  const sandbox=()=>new class {
    factory;box;
    setRetainedBoxFactory(factory){assert.equal(this.box,undefined);this.factory=factory;}
    async exec(command,key){this.box??=await this.factory(allocation);return this.box.exec(command,[],{OPENROUTER_API_KEY:key});}
  }();
  const context=sb=>({workspaceId:'workspace_A',sessionId:'session_A',session:{id:'session_A',environmentId:'production_A'},
    environment:{id:'production_A'},sandbox:sb,executionFence:{current:true,workspaceId:'workspace_A',sessionId:'session_A',
      executionId:'exec_A',attemptId:'attempt_A',ownerId:'worker',generation:1}});
  return {...s,allocations,allocation,lifecycle,sandbox,context,client};
}

test('runner bind is lazy, preserves per-exec hooks and same disk across two turns and controller replacement',async()=>{
  const s=fixture(),manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await manager.bind(ctx);assert.equal(s.disks.size,0);
  assert.equal((await sandbox.exec('write','FAKE_FIRST')).stdout,'FAKE_FIRST');
  await manager.idle(ctx);assert.equal([...s.disks.values()][0].running,false);
  const next={...ctx,executionFence:{current:true}};
  await manager.bind(next);
  assert.deepEqual(JSON.parse((await sandbox.exec('read','FAKE_SECOND')).stdout),{workspace:'persisted',profile:'saved',key:'FAKE_SECOND'});
  await manager.idle(next);
  const replacement=s.lifecycle(),newSandbox=s.sandbox(),newContext=s.context(newSandbox);
  await replacement.bind(newContext);
  assert.equal(JSON.parse((await newSandbox.exec('read','FAKE_ROTATED')).stdout).profile,'saved');
  await replacement.idle(newContext);assert.equal(s.disks.size,1);
  assert.equal(JSON.stringify([...s.rows.values()]).includes('FAKE_ROTATED'),false);
  await replacement.archive({...newContext,reason:'archived'});assert.equal(s.disks.size,0);
});

test('no-guest-I/O turn does not allocate a VM and cold archive does not restore one',async()=>{
  const s=fixture(),manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await manager.bind(ctx);await manager.idle(ctx);assert.equal(s.disks.size,0);
  await s.lifecycle().archive({...ctx,reason:'archived'});assert.equal(s.disks.size,0);
});

test('new turn cannot bind before preceding turn finalization; shutdown preserves retained disk',async()=>{
  const s=fixture(),manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await manager.bind(ctx);await sandbox.exec('write','FAKE_SECRET');
  await assert.rejects(manager.bind(ctx),/retained_previous_turn_not_quiescent/);
  await manager.shutdown();assert.equal(s.disks.size,1);assert.equal([...s.disks.values()][0].running,false);
});

test('scope mismatch and absent execution fence fail before allocation',async()=>{
  const s=fixture(),manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await assert.rejects(manager.bind({...ctx,environment:{id:'replay'}}),/retained_scope_mismatch/);
  await assert.rejects(manager.bind({...ctx,executionFence:undefined}),/retained_execution_fence_required/);
  assert.equal(s.disks.size,0);
});

test('durable orphan reconciliation removes only unpublished owned allocation, preserving published retained disk',async()=>{
  const s=fixture(),scope={...s.options.scope,workId:'tank-retained-session-v1'};
  const orphanRuntime=await s.provider.create(scope),orphanHandle=orphanRuntime.runtimeHandle();
  const queueRows=[{id:'orphan_A',scope,sandbox:orphanHandle}],resolved=[];
  const orphans={async enqueue(){},async list(){return queueRows;},async resolve({id}){resolved.push(id);},async failed(){assert.fail('orphan cleanup should work');}};
  await s.lifecycle({orphans}).reconcileOrphans();assert.equal(s.disks.size,0);assert.deepEqual(resolved,['orphan_A']);
  const manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);await manager.bind(ctx);await sandbox.exec('write','FAKE_KEY');await manager.idle(ctx);
  const retainedId=[...s.disks.keys()][0];queueRows[0]={id:'old-record-now-published',scope,sandbox:{provider:'litebox',runtimeId:retainedId}};
  await s.lifecycle({orphans}).reconcileOrphans();assert.equal(s.disks.has(retainedId),true);
  assert.deepEqual(resolved,['orphan_A','old-record-now-published']);
});

for(const failure of ['publish','release'])test(`same-owner idle retries ${failure} after stop proof without stopping again`,async()=>{
  const s=fixture(),manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await manager.bind(ctx);await sandbox.exec('write','FAKE_FIRST');
  const original=s.fences[failure].bind(s.fences);let fail=true;
  s.fences[failure]=async input=>{if(fail){fail=false;throw Error('transient finalization failure');}return original(input);};
  await manager.idle(ctx);
  ctx.executionFence.current=false;
  await assert.rejects(manager.bind(ctx));
  const next={...ctx,executionFence:{current:true}};
  await manager.bind(next);
  assert.equal(s.calls.filter(c=>c[0]==='stop').length,1);
  assert.equal(JSON.parse((await sandbox.exec('read','FAKE_ROTATED')).stdout).workspace,'persisted');
  assert.equal(s.nativeHandles.length,2);await manager.idle(next);
});

function recovery(ctx) {
  return {fence:{...ctx.executionFence},context:ctx};
}

test('a warm bind losing its fence before acquisition leaves a stopped prior entry untouched',async()=>{
  const s=fixture(),manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await manager.bind(ctx);await sandbox.exec('write','FAKE_FIRST');
  const lost={...ctx,executionFence:{...ctx.executionFence,executionId:'exec_B',attemptId:'attempt_B',current:false}};
  await assert.rejects(manager.bind(lost));
  await assert.rejects(manager.quiesceLostTurn(recovery(lost)),/retained_lost_turn_changed/);
  await manager.idle(ctx);
  await assert.rejects(manager.bind(lost));
  const stopped=structuredClone([...s.rows.values()]);
  await manager.quiesceLostTurn(recovery(lost));
  assert.deepEqual([...s.rows.values()],stopped);
  assert.equal(s.calls.filter(call=>call[0]==='stop').length,1);
  const next={...lost,executionFence:{...lost.executionFence,attemptId:'attempt_C',generation:2,current:true}};
  await manager.bind(next);
  assert.equal(JSON.parse((await sandbox.exec('read','FAKE_SECOND')).stdout).workspace,'persisted');
  await manager.idle(next);
});

test('pre-claim quiescence uses the retained resource fence and stops only the published resident',async()=>{
  const s=fixture(),manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await manager.bind(ctx);await sandbox.exec('write','FAKE_FIRST');
  const row=[...s.rows.values()][0],publication=JSON.stringify(row.publication);
  ctx.executionFence.current=false;
  row.active=false;
  await manager.abandon(ctx);
  assert.equal([...s.disks.values()][0].running,true);
  assert.equal([...s.rows.values()][0].active,false);
  const input=recovery(ctx);
  await assert.rejects(manager.quiesceLostTurn({...input,fence:{...input.fence,attemptId:'unrelated'}}),/retained_lost_turn_changed/);
  await assert.rejects(s.lifecycle().bind(s.context(s.sandbox())),/retained_previous_turn_not_quiescent/);
  await manager.quiesceLostTurn(input);
  assert.equal([...s.disks.values()][0].running,false);
  assert.equal(JSON.stringify([...s.rows.values()][0].publication),publication);
  assert.equal(s.nativeHandles.length,1);
  const next={...ctx,executionFence:{...ctx.executionFence,attemptId:'attempt_B',generation:2,current:true}};
  await manager.bind(next);
  assert.equal(JSON.parse((await sandbox.exec('read','FAKE_SECOND')).stdout).workspace,'persisted');
  await manager.idle(next);
  assert.equal(s.nativeHandles.length,2);
});

test('lost recovery refuses changed publication and rechecks the resource fence after native awaits',async()=>{
  const s=fixture(),manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await manager.bind(ctx);await sandbox.exec('write','FAKE_FIRST');
  ctx.executionFence.current=false;
  await manager.abandon(ctx);
  const input=recovery(ctx),row=[...s.rows.values()][0],original=structuredClone(row.publication);
  row.publication.workspaceCandidate.id='replacement';
  await assert.rejects(manager.quiesceLostTurn(input),/retained_recovery_publication_changed/);
  const currentRow=()=>[...s.rows.values()][0];
  currentRow().publication=original;
  const get=s.client.get.bind(s.client);
  s.client.get=async id=>{
    const box=await get(id);
    currentRow().active=false;
    return box;
  };
  await assert.rejects(manager.quiesceLostTurn(input),/retained_fence_lost/);
  assert.equal(s.calls.filter(call=>call[0]==='stop').length,0);
  assert.equal(s.disks.get('box_1').running,true);
  s.client.get=get;
  await manager.quiesceLostTurn(input);
  assert.equal(s.disks.get('box_1').running,false);
});

test('lost recovery independently verifies the stopped resident and retries without stopping a replacement',async()=>{
  const s=fixture(),manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await manager.bind(ctx);await sandbox.exec('write','FAKE_FIRST');
  ctx.executionFence.current=false;
  await manager.abandon(ctx);
  const input=recovery(ctx),getInfo=s.client.getInfo.bind(s.client);
  s.client.getInfo=async id=>{
    const info=await getInfo(id);
    return info?.state.running===false?{...info,id:'different-incarnation'}:info;
  };
  await assert.rejects(manager.quiesceLostTurn(input),/retained_recovery_resident_changed/);
  assert.deepEqual(s.calls.filter(call=>call[0]==='stop'),[['stop','box_1']]);
  assert.equal(s.disks.size,1);
  await assert.rejects(manager.quiesceLostTurn(input),/retained_recovery_resident_changed/);
  assert.deepEqual(s.calls.filter(call=>call[0]==='stop'),[['stop','box_1']]);
  s.client.getInfo=getInfo;
  await manager.quiesceLostTurn(input);
  assert.deepEqual(s.calls.filter(call=>call[0]==='stop'),[['stop','box_1']]);
});

test('archive across controllers waits for lazy start and refuses a live owner',async()=>{
  const s=fixture(),manager=s.lifecycle(),other=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  let release,start,archived=false;
  const ready=new Promise(resolve=>{start=resolve;});
  const pending=new Promise(resolve=>{release=resolve;});
  const create=s.provider.create.bind(s.provider);
  s.provider.create=async scope=>{start();await pending;return create(scope);};
  await manager.bind(ctx);
  const startup=sandbox.factory(s.allocation);
  await ready;
  const final=assert.rejects(other.archive({...ctx,reason:'archived'}),/retained_previous_turn_not_quiescent/)
    .then(()=>{archived=true;});
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(archived,false);
  release();await startup;await final;
  await manager.idle(ctx);
  await other.archive({...ctx,reason:'archived'});
  assert.equal(s.disks.size,0);
});

test('lost execution stops heartbeat even when release acknowledgement fails',async()=>{
  const s=fixture(),manager=s.lifecycle({leaseTtlMs:300}),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await manager.bind(ctx);await sandbox.exec('write','FAKE_FIRST');
  let renewals=0;const renew=s.fences.renew.bind(s.fences),release=s.fences.release.bind(s.fences);
  s.fences.renew=async input=>{renewals++;return renew(input);};
  s.fences.release=async()=>{throw Error('release unavailable');};ctx.executionFence.current=false;
  await assert.rejects(manager.abandon(ctx),/release unavailable/);
  const afterAbandon=renewals;await new Promise(resolve=>setTimeout(resolve,130));
  assert.equal(renewals,afterAbandon);assert.equal([...s.disks.values()][0].running,true);
  s.fences.release=release;
  await manager.quiesceLostTurn(recovery(ctx));
});

test('guest timeout returns before a late spawn and pre-claim recovery waits to stop the exact box',async()=>{
  const s=fixture(),manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  let spawn,spawnStarted,kill,killStarted,exit;
  const spawned=new Promise(resolve=>{spawn=resolve;});
  const starting=new Promise(resolve=>{spawnStarted=resolve;});
  const killed=new Promise(resolve=>{kill=resolve;});
  const killing=new Promise(resolve=>{killStarted=resolve;});
  const exited=new Promise(resolve=>{exit=resolve;});
  await manager.bind(ctx);
  const box=await sandbox.factory(s.allocation);
  s.nativeHandles[0].spawnDuplexProcess=async()=>{spawnStarted();return spawned;};
  const failed=assert.rejects(box.exec('late',[],{}, {timeoutSecs:0.05}),/retained_exec_timeout/);
  await starting;
  await failed;
  ctx.executionFence.current=false;
  const input=recovery(ctx);
  await assert.rejects(manager.quiesceLostTurn(input),/retained_native_busy/);
  assert.equal(s.calls.some(call=>call[0]==='stop'),false);
  await assert.rejects(s.lifecycle().bind(s.context(s.sandbox())),/retained_previous_turn_not_quiescent/);
  let closed=false;
  spawn({stdin:new WritableStream({close(){closed=true;}}),stdout:new Response('').body,stderr:new Response('').body,
    exited,async kill(){killStarted();await killed;}});
  await killing;
  await assert.rejects(manager.quiesceLostTurn(input),/retained_native_busy/);
  kill();exit({code:137,signal:'SIGKILL'});
  for(let retry=0;;retry++){
    try{await manager.quiesceLostTurn(input);break;}
    catch(error){if(retry===20||error.message!=='retained_native_busy')throw error;}
  }
  assert.equal(closed,false);
  await manager.archive({...ctx,reason:'archived'});
  assert.deepEqual(s.calls.filter(call=>['stop','remove-owned'].includes(call[0])),[['stop','box_1'],['remove-owned','box_1']]);
});

test('warm current-prefix metadata advances preserve allocation/workspace and stale or swapped bindings fail',async()=>{
  const {validateReplayAttachment}=await import(new URL('packages/control-plane-node/src/lib/tank-retained/replay-attachment.ts', openmaSource));
  const s=fixture(),selected={hostPath:'/owned/run_A',guestPath:'/replay/evidence',readOnly:true};
  s.allocation.volumes.push(selected);
  let currentHash='prefix_1',currentCutoff='cutoff_1',validations=0;
  const fingerprint=async(ctx,allocation)=>{
    await validateReplayAttachment(ctx,allocation,async options=>{
      validations++;assert.equal(options.root,'/owned');
      if(options.manifestSha256!==currentHash||options.cutoff!==currentCutoff)throw Error('current prefix mismatch');
      return {...selected,hostPath:'/owned/'+options.runId};
    },'/owned');
    return s.options.fingerprint;  
  };
  const manager=s.lifecycle({fingerprint}),sb=s.sandbox(),ctx=s.context(sb);
  ctx.environment.name='tank-replay';ctx.session.metadata={tank_replay_run_id:'run_A',tank_replay_evidence_sha256:currentHash,tank_replay_cutoff:currentCutoff};
  await manager.bind(ctx);await sb.exec('write','FAKE_FIRST');await manager.idle(ctx);
  const id=[...s.disks.keys()][0],old=structuredClone(ctx.session.metadata);
  currentHash='prefix_2';currentCutoff='cutoff_2';
  const next={...ctx,session:{...ctx.session,metadata:{...old,tank_replay_evidence_sha256:currentHash,tank_replay_cutoff:currentCutoff}},executionFence:{current:true}};
  await manager.bind(next);assert.equal(validations,2);
  assert.equal(JSON.parse((await sb.exec('read','FAKE_ROTATED')).stdout).workspace,'persisted');await manager.idle(next);
  await assert.rejects(manager.bind({...next,session:{...next.session,metadata:old}}),/current prefix mismatch/);
  await assert.rejects(manager.bind({...next,session:{...next.session,metadata:{...next.session.metadata,tank_replay_run_id:'run_B'}}}),/retained_replay_mount_changed/);
  assert.equal(s.disks.size,1);assert.equal([...s.disks.keys()][0],id);
  const replacement=s.lifecycle({fingerprint}),newSb=s.sandbox(),fresh={...next,sandbox:newSb};
  await replacement.bind(fresh);assert.equal(JSON.parse((await newSb.exec('read','FAKE_AFTER_RESTART')).stdout).workspace,'persisted');await replacement.idle(fresh);
  assert.equal(s.allocations.filter(options=>options.providerId==='litebox').length,2);
  assert.equal([...s.disks.keys()][0],id);
});

test('warm sandbox with a changed fingerprint is refused for reuse and the cold path replaces its runtime once',async()=>{
  const s=fixture();let current=s.options.fingerprint;
  const manager=s.lifecycle({fingerprint:async()=>current}),sandbox=s.sandbox(),ctx=s.context(sandbox);
  assert.equal(await manager.bind(ctx),true);await sandbox.exec('write','FAKE_FIRST');await manager.idle(ctx);
  const old=[...s.disks.keys()][0],calls=s.calls.length;
  current='sha256:'+'b'.repeat(64);
  const next={...ctx,executionFence:{current:true}};
  assert.equal(await manager.bind(next),false);
  assert.deepEqual(s.calls.slice(calls),[]);assert.equal(s.disks.get(old).running,false);
  assert.equal(manager.takeRuntimeReplacedReminder(next),null);
  const fresh=s.sandbox(),cold={...next,sandbox:fresh};
  assert.equal(await manager.bind(cold),true);
  assert.deepEqual(JSON.parse((await fresh.exec('read','FAKE_SECOND')).stdout),{key:'FAKE_SECOND'});
  assert.equal(s.disks.has(old),false);assert.equal(s.disks.size,1);
  assert.equal(s.calls.some(call=>call[0]==='fresh-get'),false);
  assert.match(manager.takeRuntimeReplacedReminder(cold),/sandbox of this session was replaced.*Files under \/workspace.*installed packages.*browser profile.*\/mnt\/session\/outputs.*\/mnt\/memory are kept.*conversation and its history are intact/);
  assert.equal(manager.takeRuntimeReplacedReminder(cold),null);
  await manager.idle(cold);
  const later={...cold,executionFence:{current:true}};
  assert.equal(await manager.bind(later),true);await fresh.exec('read','FAKE_THIRD');
  assert.equal(manager.takeRuntimeReplacedReminder(later),null);await manager.idle(later);
});

test('cold controller after a configuration change starts a fresh runtime and reminds once',async()=>{
  const s=fixture(),first=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await first.bind(ctx);await sandbox.exec('write','FAKE_FIRST');await first.idle(ctx);
  const replacement=s.lifecycle({fingerprint:async()=>'sha256:'+'c'.repeat(64)}),newSandbox=s.sandbox(),newContext=s.context(newSandbox);
  await replacement.bind(newContext);
  assert.equal(replacement.takeRuntimeReplacedReminder(newContext),null);
  assert.deepEqual(JSON.parse((await newSandbox.exec('read','FAKE_ROTATED')).stdout),{key:'FAKE_ROTATED'});
  assert.equal(s.disks.size,1);assert.equal(s.calls.some(call=>call[0]==='fresh-get'),false);
  assert.match(replacement.takeRuntimeReplacedReminder(newContext),/sandbox of this session was replaced/);
  assert.equal(replacement.takeRuntimeReplacedReminder(newContext),null);
  await replacement.idle(newContext);
});

test('cold LiteBox sandbox starts its replaced runtime while it is prepared and only the first prompt carries the reminder',async()=>{
  const {LiteBoxSandbox}=await import(new URL('packages/sandbox-adapter-litebox/src/litebox.ts',openmaSource));
  const s=fixture(),scratch=await mkdtemp(join(tmpdir(),'tank-litebox-reminder-'));
  try{
    const litebox=()=>new LiteBoxSandbox({scratchRoot:scratch,logger:{warn(){},log(){}}});
    const first=s.lifecycle(),before=litebox(),ctx=s.context(before);
    await first.bind(ctx);await before.prepareRetainedRuntime();
    const old=[...s.disks.keys()][0];assert.equal(s.disks.get(old).running,true);
    assert.equal(first.takeRuntimeReplacedReminder(ctx),null);await first.idle(ctx);await before.destroy();
    const manager=s.lifecycle({fingerprint:async()=>'sha256:'+'c'.repeat(64)}),sandbox=litebox(),cold=s.context(sandbox);
    await manager.bind(cold);
    assert.equal(manager.takeRuntimeReplacedReminder(cold),null);assert.equal(s.disks.has(old),true);
    await sandbox.prepareRetainedRuntime();
    assert.equal(s.disks.has(old),false);assert.deepEqual([...s.disks.values()].map(disk=>disk.running),[true]);
    assert.match(manager.takeRuntimeReplacedReminder(cold),/sandbox of this session was replaced/);
    assert.equal(await sandbox.exec('true'),'');
    assert.equal(manager.takeRuntimeReplacedReminder(cold),null);await manager.idle(cold);
    const warm={...cold,executionFence:{current:true}};
    assert.equal(await manager.bind(warm),true);assert.equal(await sandbox.exec('true'),'');
    assert.equal(manager.takeRuntimeReplacedReminder(warm),null);
    assert.equal(s.calls.filter(call=>call[0]==='create').length,2);
    await manager.idle(warm);await sandbox.destroy();
  }finally{await rm(scratch,{recursive:true,force:true});}
});

const { discoverStartupResidents } = await import(new URL(
  'packages/control-plane-node/src/lib/tank-retained/startup-recovery.ts', openmaSource,
));

test('startup discovers and stops a detached guest under a replacement resource fence', async () => {
  const s = fixture();
  const prior = s.controller();
  await prior.begin(async () => {}, new AbortController().signal);
  const disk = [...s.disks.values()][0];
  const row = [...s.rows.values()][0];
  const scope = prior.scope;
  const sql = {
    prepare() {
      return { bind: () => ({ all: async () => ({ results: [{
        workspace_id: scope.workspaceId,
        environment_id: scope.environmentId,
        session_id: scope.sessionId,
        publication_json: JSON.stringify(row.publication),
        publication_generation: row.publication.generation,
        revision: row.publication.revision,
      }] }) }) };
    },
  };
  const replacement = s.lifecycle({
    ownerId: 'configured:new',
    client: { ...s.client, listInfo: async () => [await s.client.getInfo(disk.id)] },
  });
  const [resident] = await discoverStartupResidents(sql, replacement.options);
  assert.equal(resident.sessionId, scope.sessionId);
  await assert.rejects(resident.quiesce(), /retained_resource_busy/);
  assert.equal(disk.running, true);
  await s.fences.release({ fence: row.fence });
  const originalGet = replacement.options.client.get;
  replacement.options.client.get = async id => ({ ...await originalGet(id), stop: async () => {} });
  await assert.rejects(resident.quiesce(), /retained_recovery_stop_unconfirmed/);
  assert.equal(disk.running, true);
  replacement.options.client.get = originalGet;
  await resident.quiesce();
  assert.equal(disk.running, false);
  assert.deepEqual(s.calls.filter(([op]) => op === 'stop'), [['stop', disk.id]]);
  assert.deepEqual(await discoverStartupResidents(sql, replacement.options), []);
});

test('startup refuses to stop a same-name guest with a different published runtime ID', async () => {
  const s = fixture();
  const prior = s.controller();
  await prior.begin(async () => {}, new AbortController().signal);
  const { publication } = [...s.rows.values()][0];
  const scope = prior.scope;
  const original = await s.client.getInfo([...s.disks.keys()][0]);
  const replacement = s.lifecycle({
    client: { ...s.client, listInfo: async () => [{ ...original, id: 'another_box' }] },
  });
  const sql = {
    prepare() {
      return { bind: () => ({ all: async () => ({ results: [{
        workspace_id: scope.workspaceId,
        environment_id: scope.environmentId,
        session_id: scope.sessionId,
        publication_json: JSON.stringify(publication),
        publication_generation: publication.generation,
        revision: publication.revision,
      }] }) }) };
    },
  };
  await assert.rejects(discoverStartupResidents(sql, replacement.options), /retained_startup_resident_ownership_unconfirmed/);
  assert.equal(s.calls.some(([op]) => op === 'stop'), false);
});
