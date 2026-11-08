const openmaSource = process.env.TANK_AUDIT_OPENMA_ROOT ? pathToFileURL(process.env.TANK_AUDIT_OPENMA_ROOT.replace(/\/?$/, '/')) : new URL('../../../vendor/openma/', import.meta.url);
import { pathToFileURL } from 'node:url';
import { test } from 'node:test';
import assert from 'node:assert/strict';
const { validateReplayAttachment } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/replay-attachment.ts', openmaSource));
const { DefaultNodeManagedSessionRunner } = await import(new URL('packages/control-plane-node/src/lib/node-managed-session-runner.ts', openmaSource));
const { NodeManagedWorkspaceCheckpoints } = await import(new URL('packages/control-plane-node/src/lib/node-managed-workspace-checkpoints.ts', openmaSource));
const { NodeRetainedLifecycle } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/node-lifecycle.ts', openmaSource));
import { setup } from './fakes.mjs';
import { createHash } from 'node:crypto';
const { stableDigest } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/runtime-identity.ts', openmaSource));
const { decodeSchema2Publication } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/retained-session.ts', openmaSource));
function historicalPublication(schema,scope,handle,identity,fingerprint,boundary=undefined) {
  const optional=boundary===undefined?{}:{checkpointBoundary:boundary};
  if(schema===1){
    const preimage={scope,provider:handle.provider,runtimeId:handle.runtimeId,fingerprint,state:'retained'};
    return {id:`retained-${handle.runtimeId}`,contentHash:'sha256:'+createHash('sha256').update(JSON.stringify(preimage)).digest('hex'),metadata:{schema:1,scope:JSON.stringify(scope),provider:handle.provider,runtimeId:handle.runtimeId,fingerprint,state:'retained',...optional}};
  }
  const manifest=JSON.stringify({handle,state:'retained',archived:false,identity,retired:[],...optional});
  return {id:`retained-${handle.runtimeId}`,contentHash:stableDigest({scope,manifest}),metadata:{schema:2,scope:JSON.stringify(scope),provider:handle.provider,runtimeId:handle.runtimeId,state:'retained',manifest}};
}
const selected={hostPath:'/owned/run_A',guestPath:'/replay/evidence',readOnly:true};
const allocation={image:'pinned',volumes:[selected]};
const context=metadata=>({environment:{name:'tank-replay'},session:{metadata}});
test('latest mutable prefix is revalidated without mutating static allocation identity',async()=>{
  const calls=[],before=JSON.stringify(allocation);
  const verify=async options=>{calls.push(options);return selected;};
  for(const suffix of ['old','new'])await validateReplayAttachment(context({tank_replay_run_id:'run_A',tank_replay_evidence_sha256:suffix,tank_replay_cutoff:suffix}),allocation,verify,'/owned');
  assert.deepEqual(calls.map(call=>call.manifestSha256),['old','new']);assert.equal(JSON.stringify(allocation),before);
});
test('fresh validation failure and changed, writable or duplicate mounts fail closed',async()=>{
  await assert.rejects(validateReplayAttachment(context({}),allocation,async()=>{throw Error('current prefix mismatch');},'/owned'),/current prefix mismatch/);
  for(const volumes of [[],[{...selected,hostPath:'/owned/run_B'}],[{...selected,readOnly:false}],[selected,selected]])
    await assert.rejects(validateReplayAttachment(context({}),{...allocation,volumes},async()=>selected,'/owned'),/retained_replay_mount_changed/);
});
test('ordinary analysis attachments do not invoke replay validation',async()=>{
  await validateReplayAttachment({environment:{name:'tank-analysis'},session:{}},allocation,async()=>assert.fail('no replay validation'),undefined);
});

function checkpointFixture(retained=true) {
  const s=setup(),writes=[],extracts=[],reads=[];
  let image='a',current=true,resource=true;
  const identity=()=>({version:2,image:'sha256:'+image.repeat(64),compatibility:{scope:'sha256:'+'d'.repeat(64)}});
  const decorate=runtime=>Object.assign(runtime,{async spawnDuplexProcess(spec){
    const {disk}=await runtime.exec(spec.command);
    if(spec.command.includes('tar -xf')){extracts.push(disk.id);disk.files.old='restored';}
    return {stdin:new WritableStream(),stdout:new Response('').body,stderr:new Response('').body,exited:Promise.resolve({code:0,signal:null}),async kill(){}};
  }});
  const client={async getInfo(id){const disk=s.disks.get(id)??[...s.disks.values()].find(disk=>disk.name===id);return disk?{id:disk.id,name:disk.name,state:{running:disk.running}}:null;},async get(id){const disk=s.disks.get(id);return disk?{id,name:disk.name,info:async()=>({id,name:disk.name,state:{running:disk.running}})}:null;},async remove(id){s.disks.delete(id);}};
  const module={createBoxLiteManagedRuntime(){return {sandbox:{async reap(){assert.fail('unexpected delegated cleanup');}}};},createBoxLiteProvider(){return {async create(_a,_b,acq){return decorate(await s.provider.create(acq.scope));},async resume(handle,_a,_b,acq){return decorate(await s.provider.resume(handle,acq.scope));}};}};
  const makeManager=()=>new NodeRetainedLifecycle({fences:{...s.fences,isCurrent:async fence=>resource&&await s.fences.isCurrent(fence)},providerModule:module,client,ownerId:'controller',leaseTtlMs:30000,identity:async()=>identity(),fingerprint:async()=>'sha256:'+image.repeat(64),assertExecutionCurrent:async()=>{await s.beforeExecution?.();if(!current)throw Error('execution_lost');}});
  let manager=makeManager();
  const sandbox=()=>new class {
    box;factory;
    setRetainedBoxFactory(factory){this.factory=factory;}
    async prepareRetainedRuntime(){this.box??=await this.factory({image,volumes:[]});}
    async exec(command){if(retained){await this.prepareRetainedRuntime();return (await this.box.exec(command,[])).stdout;}if(command.includes('tar -xf'))extracts.push('ordinary');return '';}
    async writeFileBytes(path,bytes){if(retained)await this.prepareRetainedRuntime();writes.push([path,bytes.length]);return path;}
    async destroy(){await this.box?.stop();}
  }();
  const archive=Buffer.alloc(2048),header=archive.subarray(0,512);
  header.write('old');header.write('0000644\0',100);header.write('0000000\0',108);header.write('0000000\0',116);header.write('00000000000\0',124);header.write('00000000000\0',136);header.fill(32,148,156);header[156]=48;header.write('ustar\0',257);header.write('00',263);header.write([...header].reduce((a,b)=>a+b,0).toString(8).padStart(6,'0')+'\0 ',148);
  const candidate={id:'wsc_fixture',contentHash:'sha256:'+createHash('sha256').update(archive).digest('hex'),metadata:{'openma.workspace.blob-key.v1':'managed-session-workspace-checkpoints/workspace_A/session_A/wsc_fixture.tar'}};
  const blobs={async head(){reads.push('head');await s.beforeHead?.();return s.missing?null:{size:archive.length};},async get(){reads.push('get');return {size:archive.length,body:new Response(s.corrupt?Buffer.from('bad'):archive).body};}};
  const checkpoints=new NodeManagedWorkspaceCheckpoints({sql:{},blobs,intervalMs:1000});
  checkpoints.active=async()=>({candidate,publishedAtMs:0});
  const input={workspaceId:'workspace_A',sessionId:'session_A',session:{id:'session_A',environmentId:'production_A',agent:{skills:[]},resources:[]},environment:{id:'production_A',config:{}},executionFence:{workspaceId:'workspace_A',sessionId:'session_A',executionId:'turn'}};
  const runner=()=>new DefaultNodeManagedSessionRunner({buildSandbox:async()=>sandbox(),workspaceCheckpoints:checkpoints,...(retained?{retainedLifecycle:{bind:ctx=>manager.bind({...ctx,sessionId:ctx.session.id}),idle:ctx=>manager.idle({...ctx,sessionId:ctx.session.id}),abandon:ctx=>manager.abandon({...ctx,sessionId:ctx.session.id}),archive:ctx=>manager.archive(ctx)}}:{})});
  return {s,input,candidate,writes,extracts,reads,runner,sandbox,checkpoints,get manager(){return manager;},change(){image='b';input.environment.config={changed:true};},cold(){manager=makeManager();},lose(){current=false;},loseResource(){resource=false;}};
}

for(const schema of [1,2])for(const cold of [false,true])for(const legacy of [false,true])test(`runner checkpoint attachment excludes predecessor guest archive after identity change (schema=${schema}, cold=${cold}, legacy=${legacy})`,async()=>{
  const f=checkpointFixture();
  try{
    await f.runner().start(f.input);
    const old=[...f.s.disks.values()][0];old.files.old='private';old.profile.cookie='private';
    await f.manager.idle(f.input);
    const row=[...f.s.rows.values()][0],scope={...f.s.options.scope,workId:'tank-retained-session-v1'};
    const original=JSON.parse(row.publication.workspaceCandidate.metadata.manifest);
    row.publication.workspaceCandidate=historicalPublication(schema,scope,original.handle,original.identity,f.s.options.fingerprint);
    const frozen=JSON.stringify(row.publication);
    if(!legacy)f.candidate.metadata['openma.workspace.retained-runtime.v1']=JSON.stringify({provider:'litebox',runtimeId:old.id});
    f.cold();const r=f.runner();await r.start(f.input);
    assert.equal([...f.s.disks.keys()][0],old.id);assert.equal(f.s.creates,1);
    assert.equal(old.files.old,'restored');assert.equal(f.writes.length,1);assert.deepEqual(f.extracts,[old.id]);assert.deepEqual(f.reads,['head','get']);
    assert.equal(JSON.stringify([...f.s.rows.values()][0].publication),frozen);
    await f.manager.idle(f.input);f.writes.length=0;f.extracts.length=0;f.reads.length=0;
    f.change();if(cold)f.cold();
    await (cold?f.runner():r).start(f.input);
    assert.equal(f.s.disks.has(old.id),false);assert.equal(f.s.disks.size,1);
    assert.deepEqual([...f.s.disks.values()][0].files,{});assert.deepEqual([...f.s.disks.values()][0].profile,{});
    assert.deepEqual(f.writes,[]);assert.deepEqual(f.extracts,[]);assert.deepEqual(f.reads,[]);
    await f.manager.idle(f.input);f.cold();delete f.candidate.metadata['openma.workspace.retained-runtime.v1'];await f.runner().start(f.input);
    assert.deepEqual(f.writes,[]);assert.deepEqual(f.extracts,[]);assert.deepEqual(f.reads,[]);
  }finally{await f.manager.abandon(f.input);await f.manager.shutdown();}
});

for(const retained of [false,true])for(const failure of ['none','missing','corrupt'])test(`runner checkpoint restore retains fail-closed archive semantics (retained=${retained}, failure=${failure})`,async()=>{
  const f=checkpointFixture(retained);
  try{
    if(retained){await f.runner().start(f.input);const id=[...f.s.disks.keys()][0];f.candidate.metadata['openma.workspace.retained-runtime.v1']=JSON.stringify({provider:'litebox',runtimeId:id});await f.manager.idle(f.input);f.cold();}
    f.s.missing=failure==='missing';f.s.corrupt=failure==='corrupt';
    if(failure==='none'){await f.runner().start(f.input);assert.equal(f.writes.length,1);assert.equal(f.extracts.length,1);}
    else{await assert.rejects(f.runner().start(f.input),new RegExp(failure));assert.deepEqual(f.writes,[]);assert.deepEqual(f.extracts,[]);}
  }finally{await f.manager.abandon(f.input);await f.manager.shutdown();}
});

for(const loss of ['execution','resource','cancel'])test(`runner refuses checkpoint guest writes after awaited blob lookup loses ${loss}`,async()=>{
  const f=checkpointFixture(),abort=new AbortController();
  try{
    await f.runner().start(f.input);f.candidate.metadata['openma.workspace.retained-runtime.v1']=JSON.stringify({provider:'litebox',runtimeId:[...f.s.disks.keys()][0]});await f.manager.idle(f.input);f.cold();
    f.s.beforeHead=async()=>{if(loss==='execution')f.lose();else if(loss==='resource')f.loseResource();else abort.abort(Error('cancelled'));};
    await assert.rejects(f.runner().start({...f.input,signal:abort.signal}),/execution_lost|retained_fence_lost|cancelled/);
    assert.deepEqual(f.writes,[]);assert.deepEqual(f.extracts,[]);
  }finally{await f.manager.abandon(f.input);await f.manager.shutdown();}
});

for(const operation of ['write','extract','archive'])for(const state of ['execution','resource','cancel','current','unlink','ordinary-abort'])test(`checkpoint ${operation} preserves primary failure with ${state} cleanup`,async()=>{
  const retained=state!=='ordinary-abort',f=checkpointFixture(retained),abort=new AbortController(),primary=Error(`primary_${operation}`),removals=[];
  try{
    const sandbox=f.sandbox();
    const ownership=retained?await (await f.manager.bind({...f.input,sandbox,signal:abort.signal})).prepareWorkspace():undefined;
    if(ownership)f.candidate.metadata['openma.workspace.retained-runtime.v1']=ownership.runtimeKey;
    const fail=()=>{if(state==='execution')f.lose();if(state==='resource')f.loseResource();if(state==='cancel'||!retained)abort.abort(Error('cleanup_cancelled'));throw primary;};
    const port=f.checkpoints.port({
      async writeFileBytes(path,bytes){if(operation==='write')fail();return sandbox.writeFileBytes(path,bytes);},
      async readFileBytes(){assert.fail('archive failure must precede binary read');},
      async exec(command){
        if(command.startsWith('rm -f')){removals.push(command);if(state==='unlink')throw Error('cleanup_unlink');return '';}
        if((operation==='extract'&&command.includes('tar -xf'))||(operation==='archive'&&command.startsWith('tar -C')))fail();
        return sandbox.exec(command);
      },
    },ownership);
    const scope={...f.s.options.scope,workId:'turn'},input={scope,strategy:'checkpoint_restore',signal:abort.signal,fence:{generation:1}};
    if(operation==='archive')await assert.rejects(port.checkpoint(input),error=>error===primary);
    else{
      const binding=await port.materialize({...input,activeCheckpoint:f.candidate});
      await assert.rejects(port.attach({...input,binding}),error=>error===primary);
    }
    assert.equal(removals.length,['execution','resource','cancel'].includes(state)?0:1);
    assert.deepEqual(f.extracts,[]);
  }finally{await f.manager.abandon(f.input);await f.manager.shutdown();}
});

for(const operation of ['write','extract','archive','size','read'])for(const phase of ['operation','cleanup-authorization'])test(`retained checkpoint ${operation} cleanup honors distinct operation cancellation during ${phase}`,async()=>{
  const f=checkpointFixture(),startup=new AbortController(),turn=new AbortController(),primary=Error(`primary_${operation}`),removals=[];
  const entered=Promise.withResolvers(),release=Promise.withResolvers();
  try{
    const sandbox=f.sandbox();
    const ownership=await (await f.manager.bind({...f.input,sandbox,signal:startup.signal})).prepareWorkspace();
    f.candidate.metadata['openma.workspace.retained-runtime.v1']=ownership.runtimeKey;
    const pause=async()=>{entered.resolve();await release.promise;};
    const fail=async()=>{
      if(phase==='operation')await pause();
      else f.s.beforeExecution=async()=>{delete f.s.beforeExecution;await pause();};
      throw primary;
    };
    const port=f.checkpoints.port({
      async writeFileBytes(path,bytes){if(operation==='write')return fail();return sandbox.writeFileBytes(path,bytes);},
      async readFileBytes(){assert.equal(operation,'read');return fail();},
      async exec(command){
        if(command.startsWith('rm -f')){removals.push(command);return '';}
        if((operation==='extract'&&command.includes('tar -xf'))||(operation==='archive'&&command.startsWith('tar -C')))return fail();
        if(command.includes('wc -c')){if(operation==='size')return fail();return '__OPENMA_WS_SIZE_BEGIN__2048__OPENMA_WS_SIZE_END__';}
        return sandbox.exec(command);
      },
    },ownership);
    const input={scope:{...f.s.options.scope,workId:'turn'},strategy:'checkpoint_restore',signal:turn.signal,fence:{generation:1}};
    assert.notEqual(startup.signal,turn.signal);
    const binding=['write','extract'].includes(operation)?await port.materialize({...input,activeCheckpoint:f.candidate}):undefined;
    const pending=binding?port.attach({...input,binding}):port.checkpoint(input);
    const rejected=assert.rejects(pending,error=>error===primary);
    await entered.promise;
    assert.equal(startup.signal.aborted,false);assert.equal(turn.signal.aborted,false);
    turn.abort(Error('turn_cancelled'));release.resolve();
    await rejected;
    assert.equal(startup.signal.aborted,false);assert.equal(turn.signal.aborted,true);
    await ownership.assertCurrent();
    assert.deepEqual(removals,[]);assert.deepEqual(f.extracts,[]);
  }finally{release.resolve();delete f.s.beforeExecution;await f.manager.abandon(f.input);await f.manager.shutdown();}
});

for(const schema of [1,2])for(const boundary of [undefined,false,0,'true',null])test(`retained checkpoint boundary type contract (schema=${schema}, boundary=${String(boundary)})`,async()=>{
  const f=checkpointFixture();
  try{
    await f.runner().start(f.input);await f.manager.idle(f.input);
    const row=[...f.s.rows.values()][0],scope={...f.s.options.scope,workId:'tank-retained-session-v1'};
    const original=JSON.parse(row.publication.workspaceCandidate.metadata.manifest);
    const candidate=historicalPublication(schema,scope,original.handle,original.identity,f.s.options.fingerprint,boundary);
    row.publication.workspaceCandidate=candidate;
    const frozen=JSON.stringify(row.publication),calls=f.s.calls.length;
    f.cold();
    const valid=boundary===undefined||boundary===false;
    if(schema===2){
      if(valid)assert.equal(decodeSchema2Publication(candidate,scope,'litebox').handle.runtimeId,original.handle.runtimeId);
      else assert.throws(()=>decodeSchema2Publication(candidate,scope,'litebox'),{message:'retained_manifest_invalid'});
    }
    if(valid){
      await f.runner().start(f.input);
      assert.equal(f.s.creates,1);assert.equal(f.writes.length,1);assert.deepEqual(f.extracts,[original.handle.runtimeId]);assert.deepEqual(f.reads,['head','get']);
    }else{
      await assert.rejects(f.runner().start(f.input),{message:'retained_manifest_invalid'});
      assert.equal(f.s.calls.length,calls);assert.equal(f.s.creates,1);
      assert.deepEqual(f.writes,[]);assert.deepEqual(f.extracts,[]);assert.deepEqual(f.reads,[]);
    }
    assert.equal(JSON.stringify([...f.s.rows.values()][0].publication),frozen);
  }finally{await f.manager.abandon(f.input);await f.manager.shutdown();}
});
