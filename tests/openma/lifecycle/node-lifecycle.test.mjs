const openmaSource = process.env.TANK_AUDIT_OPENMA_ROOT ? pathToFileURL(process.env.TANK_AUDIT_OPENMA_ROOT.replace(/\/?$/, '/')) : new URL('../../../vendor/openma/', import.meta.url);
import { pathToFileURL } from 'node:url';
import { test } from 'node:test';
import assert from 'node:assert/strict';
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
  const lifecycle=(changes={})=>new NodeRetainedLifecycle({fences:s.fences,providerModule:module,client:{async getInfo(id){const disk=s.disks.get(id);return disk?{state:{running:disk.running}}:null;}},ownerId:'controller',leaseTtlMs:30000,
    fingerprint:async()=>s.options.fingerprint,assertExecutionCurrent:async fence=>{assert.equal(fence.current,true);},...changes});
  const allocation={image:'image-pinned',cpus:1,memoryMib:512,network:{mode:'enabled'},volumes:[]};
  const sandbox=()=>new class {
    factory;box;
    setRetainedBoxFactory(factory){assert.equal(this.box,undefined);this.factory=factory;}
    async exec(command,key){this.box??=await this.factory(allocation);return this.box.exec(command,[],{OPENROUTER_API_KEY:key});}
  }();
  const context=sb=>({workspaceId:'workspace_A',sessionId:'session_A',session:{id:'session_A',environmentId:'production_A'},
    environment:{id:'production_A'},sandbox:sb,executionFence:{current:true}});
  return {...s,allocations,allocation,lifecycle,sandbox,context};
}

test('runner bind is lazy, preserves per-exec hooks and same disk across two turns and controller replacement',async()=>{
  const s=fixture(),manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await manager.bind(ctx);assert.equal(s.disks.size,0);
  assert.equal((await sandbox.exec('write','FAKE_FIRST')).stdout,'FAKE_FIRST');
  await manager.idle(ctx);assert.equal([...s.disks.values()][0].running,false);
  await manager.bind({...ctx,executionFence:{current:true}});
  assert.deepEqual(JSON.parse((await sandbox.exec('read','FAKE_SECOND')).stdout),{workspace:'persisted',profile:'saved',key:'FAKE_SECOND'});
  await manager.idle(ctx);
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

for(const failure of ['publish','release'])test(`new native turn finalizes stopped ${failure} failure before fresh resume`,async()=>{
  const s=fixture(),manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await manager.bind(ctx);await sandbox.exec('write','FAKE_FIRST');
  const original=s.fences[failure].bind(s.fences);let fail=true;
  s.fences[failure]=async input=>{if(fail){fail=false;throw Error('transient finalization failure');}return original(input);};
  await assert.rejects(manager.idle(ctx),/transient finalization failure/);
  ctx.executionFence.current=false;
  await assert.rejects(manager.bind(ctx));
  const next={...ctx,executionFence:{current:true}};
  await manager.bind(next);
  assert.equal(s.calls.filter(c=>c[0]==='stop').length,1);
  assert.equal(JSON.parse((await sandbox.exec('read','FAKE_ROTATED')).stdout).workspace,'persisted');
  assert.equal(s.nativeHandles.length,2);await manager.idle(next);
});

test('lost native execution finalization abandons only current resource and allows next native turn',async()=>{
  const s=fixture(),manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await manager.bind(ctx);await sandbox.exec('write','FAKE_FIRST');
  const publication=JSON.stringify([...s.rows.values()][0].publication);
  ctx.executionFence.current=false;
  await manager.abandon(ctx);
  assert.equal([...s.disks.values()][0].running,false);
  assert.equal([...s.rows.values()][0].active,false);
  assert.equal(JSON.stringify([...s.rows.values()][0].publication),publication);
  const next={...ctx,executionFence:{current:true}};
  await manager.bind(next);await sandbox.exec('read','FAKE_SECOND');await manager.idle(next);
  assert.equal(s.nativeHandles.length,2);
});

test('lost execution stops heartbeat even when release acknowledgement fails',async()=>{
  const s=fixture(),manager=s.lifecycle({leaseTtlMs:300}),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await manager.bind(ctx);await sandbox.exec('write','FAKE_FIRST');
  let renewals=0;const renew=s.fences.renew.bind(s.fences);s.fences.renew=async input=>{renewals++;return renew(input);};
  s.fences.release=async()=>{throw Error('release unavailable');};ctx.executionFence.current=false;
  await assert.rejects(manager.abandon(ctx),/release unavailable/);
  const afterAbandon=renewals;await new Promise(resolve=>setTimeout(resolve,130));
  assert.equal(renewals,afterAbandon);assert.equal([...s.disks.values()][0].running,false);
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
