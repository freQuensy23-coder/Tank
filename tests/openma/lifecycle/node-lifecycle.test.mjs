const openmaSource = process.env.TANK_AUDIT_OPENMA_ROOT ? pathToFileURL(process.env.TANK_AUDIT_OPENMA_ROOT.replace(/\/?$/, '/')) : new URL('../../../vendor/openma/', import.meta.url);
import { pathToFileURL } from 'node:url';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
const { stableDigest } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/runtime-identity.ts', openmaSource));
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
    createBoxLiteManagedRuntime(options){return {sandbox:{async reap({lease,scope}){const disk=await options.client.get(lease.runtimeId);if(!disk)return;assert.equal(disk.name,'oma-'+createHash('sha256').update(`litebox:${scope.sessionId}`).digest('hex').slice(0,32));await options.client.remove(lease.runtimeId,true);}}};},
  };
  const client={
    async lookup(id){await s.provider.beforeNativeLookup(id);return s.disks.get(id)??[...s.disks.values()].find(disk=>disk.name===id);},
    async getInfo(id){const disk=await client.lookup(id);return disk?{id:disk.id,name:disk.name,state:{status:disk.running?'Running':'Stopped',running:disk.running}}:null;},
    async get(id){const disk=await client.lookup(id);return disk?{id:disk.id,name:disk.name,info:async()=>{await s.provider.beforeNativeInfo?.(id);return {id:disk.id,name:disk.name,state:{status:disk.running?'Running':'Stopped',running:disk.running}};}}:null;},
    async remove(id){const disk=s.disks.get(id);await s.provider.reap({provider:'litebox',runtimeId:id},disk?JSON.parse(disk.scope):s.options.scope);},
  };
  const lifecycle=(changes={})=>new NodeRetainedLifecycle({fences:s.fences,providerModule:module,client,ownerId:'controller',leaseTtlMs:30000,
    fingerprint:async()=>s.options.fingerprint,assertExecutionCurrent:async fence=>{assert.equal(fence.current,true);},...changes});
  const allocation={image:'image-pinned',cpus:1,memoryMib:512,network:{mode:'enabled'},volumes:[]};
  const sandbox=()=>new class {
    factory;box;
    setRetainedBoxFactory(factory){assert.equal(this.box,undefined);this.factory=factory;}
    async exec(command,key){this.box??=await this.factory(allocation);return this.box.exec(command,[],{OPENROUTER_API_KEY:key});}
  }();
  const context=sb=>({workspaceId:'workspace_A',sessionId:'session_A',session:{id:'session_A',environmentId:'production_A'},
    environment:{id:'production_A'},sandbox:sb,executionFence:{workspaceId:'workspace_A',sessionId:'session_A',current:true}});
  return {...s,allocations,allocation,lifecycle,sandbox,context};
}

test('runner bind is lazy, preserves per-exec hooks and same disk across two turns and controller replacement',async()=>{
  const s=fixture(),manager=s.lifecycle(),sandbox=s.sandbox(),ctx=s.context(sandbox);
  await manager.bind(ctx);assert.equal(s.disks.size,0);
  assert.equal((await sandbox.exec('write','FAKE_FIRST')).stdout,'FAKE_FIRST');
  await manager.idle(ctx);assert.equal([...s.disks.values()][0].running,false);
  await manager.bind({...ctx,executionFence:{workspaceId:'workspace_A',sessionId:'session_A',current:true}});
  assert.deepEqual(JSON.parse((await sandbox.exec('read','FAKE_SECOND')).stdout),{workspace:'persisted',profile:'saved',key:'FAKE_SECOND'});
  await manager.idle(ctx);
  const replacement=s.lifecycle(),newSandbox=s.sandbox(),newContext=s.context(newSandbox);
  await replacement.bind(newContext);
  assert.equal(JSON.parse((await newSandbox.exec('read','FAKE_ROTATED')).stdout).profile,'saved');
  await replacement.idle(newContext);assert.equal(s.disks.size,1);
  assert.equal(JSON.stringify([...s.rows.values()]).includes('FAKE_ROTATED'),false);
  await replacement.archive({...newContext,reason:'archived'});assert.equal(s.disks.size,0);
});

for(const legacy of [false,true])test(`Managed driver reopens persisted context through Pi streamSimple after clean replacement (legacy=${legacy})`,async()=>{
  const {planSessionContinuation}=await import(new URL('packages/session-runtime-contract/src/continuation.ts',openmaSource));
  const {encodeSessionEventDocument,decodeSessionEventDocument}=await import(new URL('packages/session-runtime-contract/src/history.ts',openmaSource));
  const s=fixture();let fingerprint=s.options.fingerprint;
  let identity=legacy?undefined:{version:2,image:'sha256:'+'c'.repeat(64),compatibility:{scope:'sha256:'+'d'.repeat(64)}};
  const manager=s.lifecycle({fingerprint:async()=>fingerprint,identity:async()=>identity}),sb=s.sandbox(),ctx=s.context(sb);
  await manager.bind(ctx);await sb.exec('write','FAKE_FIRST');await manager.idle(ctx);
  const content=value=>[{type:'text',text:value}];
  const events=[
    {id:'prompt',type:'user.message',content:content('Remember the full context')},
    {id:'running',type:'session.status_running'},
    {id:'model',type:'span.model_request_start'},
    {id:'bash-call',type:'agent.tool_use',name:'bash',input:{command:'pwd'}},
    {id:'bash-result',type:'agent.tool_result',toolUseId:'bash-call',content:content('/workspace'),isError:false},
    {id:'rejected',type:'agent.custom_tool_use',name:'record_event',input:{precision:'MINUTE'}},
    {id:'rejected-result',type:'agent.tool_result',toolUseId:'rejected',content:content('Use HOUR'),isError:true},
    {id:'pending',type:'agent.custom_tool_use',name:'record_event',input:{precision:'HOUR'}},
    {id:'model-end',type:'span.model_request_end',modelRequestStartId:'model',isError:false,modelUsage:{inputTokens:10,outputTokens:5,cacheReadInputTokens:0,cacheCreationInputTokens:0}},
    {id:'waiting',type:'session.status_idle',stopReason:{type:'requires_action',eventIds:['pending']}},
    {id:'callback',type:'user.custom_tool_result',customToolUseId:'pending',content:content('Recorded'),isError:false},
  ].map(event=>({processedAt:'2026-11-03T00:00:00Z',...event}));
  const documents=events.map((event,index)=>encodeSessionEventDocument(event,{revision:index+1,index:0},index>0&&index<10?'original':undefined));
  const snapshot={revision:events.length,orderedEvents:documents.map(document=>decodeSessionEventDocument(document)),executions:[
    {id:'original',laneId:'sthr_primary',state:'completed',revision:1,events:[events[0]],attemptCount:1,interruptRequestedAt:null},
    {id:'reply',laneId:'sthr_primary',state:'running',revision:1,events:[events[10]],attemptCount:1,interruptRequestedAt:null},
  ]};
  const before=planSessionContinuation(snapshot,'reply');assert.equal(before.mode,'model');
  fingerprint='sha256:'+'b'.repeat(64);identity={version:2,image:'sha256:'+'e'.repeat(64),compatibility:{scope:'sha256:'+'d'.repeat(64)}};
  const {mkdtemp,writeFile,readFile,rm}=await import('node:fs/promises');
  const {tmpdir}=await import('node:os');const {join}=await import('node:path');
  const {createModels,fauxProvider,fauxAssistantMessage}=await import(new URL('apps/agent/tests/helpers/pi-ai.mjs',openmaSource));
  const {PiHarness}=await import(new URL('apps/agent/src/harness/pi-loop.ts',openmaSource));
  const {DefaultNodeManagedSessionRunner}=await import(new URL('packages/control-plane-node/src/lib/node-managed-session-runner.ts',openmaSource));
  const {DefaultNodeManagedSessionRuntimeDriver,ApplicationBackedNodeManagedSessionRuntimeEngine}=await import(new URL('packages/control-plane-node/src/lib/node-managed-session-runtime.ts',openmaSource));
  const root=await mkdtemp(join(tmpdir(),'tank-model-context-')),journal=join(root,'history.json');
  try{
    await writeFile(journal,JSON.stringify({documents,executions:snapshot.executions}));
    const original=await readFile(journal,'utf8');
    const reopen=async()=>{const saved=JSON.parse(await readFile(journal,'utf8'));return {revision:saved.documents.length,orderedEvents:saved.documents.map(document=>decodeSessionEventDocument(document)),executions:saved.executions};};
    const faux=fauxProvider({tokensPerSecond:100000});faux.setResponses([fauxAssistantMessage('Context continued')]);
    const models=createModels();models.setProvider(faux.provider);
    const requests=[],streamSimple=models.streamSimple.bind(models);
    const boundary=Object.create(models);
    Object.defineProperty(boundary,'streamSimple',{value:(model,context,options)=>{requests.push({messages:structuredClone(context.messages)});return streamSimple(model,context,options);}});
    let sequence=0;
    const runner=new DefaultNodeManagedSessionRunner({
      harnessRetry:{attempts:1,delayMs:()=>0},confirmedTools:{execute:async()=>assert.fail('no tool reissue')},outcomes:{evaluate:async()=>assert.fail('no outcome')},
      buildSandbox:async()=>sb,
      retainedLifecycle:{bind:input=>manager.bind({...input,sessionId:input.session.id}),idle:input=>manager.idle({...input,sessionId:input.session.id}),abandon:input=>manager.abandon({...input,sessionId:input.session.id}),archive:input=>manager.archive(input)},
      buildModel:async()=>({}),buildTools:async()=>({}),buildHarness:()=>new PiHarness(),
      buildHarnessContext:async input=>{assert.deepEqual(JSON.parse((await sb.exec('read','FAKE_MODEL')).stdout),{key:'FAKE_MODEL'});return {agent:input.session.agent,userMessage:events[10],session_id:ctx.sessionId,tools:input.tools,model:input.model,pi:{models:boundary,model:faux.getModel(),thinkingLevel:'off',speed:'standard'},systemPrompt:'Continue the conversation',env:{},runtime:input.runtime};},
      clock:{now:()=>new Date('2026-11-03T00:01:00Z')},ids:{nextEventId:()=>`produced_${++sequence}`},
    });
    const engine=new ApplicationBackedNodeManagedSessionRuntimeEngine({runner,continuationSnapshot:reopen,historyFor:()=>({loadSessionRuntimeHistory:async()=>({type:'found',initialEvents:[],events:(await reopen()).orderedEvents.map(row=>row.event)})})});
    const emitted=[];
    const driver=new DefaultNodeManagedSessionRuntimeDriver({engine,realtime:{publish(){},closeSession(){}},projectionFor:()=>({recordSessionRuntimeEvents:async input=>{emitted.push(...input.events);return {type:'recorded'};}}),startRetry:{attempts:1,delayMs:()=>0}});
    await driver.accept({workspaceId:ctx.workspaceId,sessionId:ctx.sessionId,session:{...ctx.session,agent:{id:'agent-test',model:faux.getModel().id,skills:[]},resources:[]},environment:{...ctx.environment,config:{}},events:[events[10]],executionFence:{...ctx.executionFence,executionId:'reply'}});
    assert.equal(requests.length,1);
    assert.equal(s.disks.has('box_1'),false);assert.equal(s.disks.size,1);
    assert.equal(s.calls.some(call=>call[0]==='fresh-get'),false);
    const messages=requests[0].messages;
    assert.ok(JSON.stringify(messages).includes('Remember the full context'));
    const calls=messages.flatMap(message=>Array.isArray(message.content)?message.content.filter(block=>block.type==='toolCall'):[]);
    assert.deepEqual(calls.map(call=>call.id),['bash-call','rejected','pending']);
    assert.deepEqual(calls.map(call=>[call.name,call.arguments]),[['bash',{command:'pwd'}],['record_event',{precision:'MINUTE'}],['record_event',{precision:'HOUR'}]]);
    const results=messages.filter(message=>message.role==='toolResult');
    assert.deepEqual(results.map(result=>result.toolCallId),['bash-call','rejected','pending']);
    assert.ok(JSON.stringify(results[0]).includes('/workspace'));
    assert.equal(results[1].isError,true);assert.ok(JSON.stringify(results[1]).includes('Use HOUR'));
    assert.ok(JSON.stringify(results[2]).includes('Recorded'));
    assert.equal(results.filter(result=>result.toolCallId==='pending').length,1);
    assert.equal(emitted.some(event=>event.type==='agent.custom_tool_use'),false);
    assert.equal(emitted.some(event=>event.type==='session.status_idle'&&event.stopReason?.type==='requires_action'),false);
    assert.equal(await readFile(journal,'utf8'),original);
  }finally{await manager.shutdown();await rm(root,{recursive:true,force:true});}
});

for(const withResolver of [false,true])test(`Pi history preserves complete error content and legacy success (resolver=${withResolver})`,async()=>{
  const {createModels,fauxProvider,fauxAssistantMessage}=await import(new URL('apps/agent/tests/helpers/pi-ai.mjs',openmaSource));
  const {PiHarness}=await import(new URL('apps/agent/src/harness/pi-loop.ts',openmaSource));
  const events=[
    {type:'user.message',content:[{type:'text',text:'Remember these tool results'}]},
    {type:'agent.tool_use',id:'ordinary',name:'echo',input:{value:'ok'}},
    {type:'agent.custom_tool_use',id:'rejected-string',name:'record_event',input:{precision:'MINUTE'}},
    {type:'agent.custom_tool_use',id:'rejected-multiline',name:'record_event',input:{precision:'SECOND'}},
    {type:'agent.custom_tool_use',id:'rejected-single-block',name:'record_event',input:{precision:'DAY'}},
    {type:'agent.custom_tool_use',id:'rejected-blocks',name:'record_event',input:{precision:'WEEK'}},
    {type:'agent.tool_use',id:'legacy',name:'echo',input:{value:'legacy'}},
    {type:'agent.tool_result',tool_use_id:'ordinary',content:'ordinary result',is_error:false},
    {type:'agent.tool_result',tool_use_id:'rejected-string',content:'String rejection',is_error:true},
    {type:'agent.tool_result',tool_use_id:'rejected-multiline',content:'Use HOUR\nOriginal rejection',is_error:true},
    {type:'agent.tool_result',tool_use_id:'rejected-single-block',content:[{type:'text',text:'Single block rejection'}],is_error:true},
    {type:'agent.tool_result',tool_use_id:'rejected-blocks',content:[
      {type:'text',text:'Use HOUR'},
      {type:'image',source:{type:'base64',data:'aW1hZ2U=',media_type:'image/png'}},
      {type:'text',text:'Original details'},
    ],is_error:true},
    {type:'agent.tool_result',tool_use_id:'legacy',content:[{type:'text',text:'legacy result'}]},
    {type:'user.message',content:[{type:'text',text:'continue'}]},
  ];
  const faux=fauxProvider({tokensPerSecond:100000}),models=createModels();models.setProvider(faux.provider);
  let results;
  faux.setResponses([context=>{
    results=structuredClone(context.messages.filter(message=>message.role==='toolResult'));
    return fauxAssistantMessage('continued');
  }]);
  const noop=async()=>{};
  const ctx={
    agent:{id:'agent-test',model:faux.getModel().id},userMessage:events.at(-1),session_id:'session-test',tools:{},model:{},
    pi:{models,model:faux.getModel(),thinkingLevel:'off',speed:'standard'},systemPrompt:'You are concise.',env:{},
    runtime:{
      history:{getEvents:()=>events,getMessages:()=>[],append:event=>events.push(event)},sandbox:{},broadcast:event=>events.push(event),
      broadcastStreamStart:noop,broadcastChunk:noop,broadcastStreamEnd:noop,
      broadcastThinkingStart:noop,broadcastThinkingChunk:noop,broadcastThinkingEnd:noop,
      broadcastToolInputStart:noop,broadcastToolInputChunk:noop,broadcastToolInputEnd:noop,
      reportUsage:noop,pendingConfirmations:[],
    },
  };
  if(withResolver)ctx.fileFetcher=async()=>null;
  await new PiHarness().run(ctx);
  assert.deepEqual(results.map(({timestamp,...result})=>{assert.equal(typeof timestamp,'number');return result;}),[
    {role:'toolResult',toolCallId:'ordinary',toolName:'echo',content:[{type:'text',text:'ordinary result'}],isError:false},
    {role:'toolResult',toolCallId:'rejected-string',toolName:'record_event',content:[{type:'text',text:'String rejection'}],isError:true},
    {role:'toolResult',toolCallId:'rejected-multiline',toolName:'record_event',content:[{type:'text',text:'Use HOUR\nOriginal rejection'}],isError:true},
    {role:'toolResult',toolCallId:'rejected-single-block',toolName:'record_event',content:[{type:'text',text:'Single block rejection'}],isError:true},
    {role:'toolResult',toolCallId:'rejected-blocks',toolName:'record_event',content:[
      {type:'text',text:'Use HOUR'},
      {type:'image',data:'aW1hZ2U=',mimeType:'image/png'},
      {type:'text',text:'Original details'},
    ],isError:true},
    {role:'toolResult',toolCallId:'legacy',toolName:'echo',content:[{type:'text',text:'legacy result'}],isError:false},
  ]);
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
  for(const foreign of [{workspaceId:'foreign'},{sessionId:'foreign'}])
    await assert.rejects(manager.bind({...ctx,executionFence:{...ctx.executionFence,...foreign}}),/retained_execution_fence_scope_mismatch/);
  assert.equal(s.disks.size,0);
  await manager.bind(ctx);await sandbox.exec('write','FAKE_FIRST');await manager.idle(ctx);
  const before=JSON.stringify([...s.rows.values()][0].publication);
  await assert.rejects(manager.bind({...ctx,executionFence:{...ctx.executionFence,sessionId:'foreign'}}),/retained_execution_fence_scope_mismatch/);
  assert.equal(s.disks.has('box_1'),true);assert.equal(JSON.stringify([...s.rows.values()][0].publication),before);
  assert.equal(s.calls.some(call=>call[0]==='remove-owned'),false);
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
  const next={...ctx,executionFence:{workspaceId:'workspace_A',sessionId:'session_A',current:true}};
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
  const next={...ctx,executionFence:{workspaceId:'workspace_A',sessionId:'session_A',current:true}};
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
  const next={...ctx,session:{...ctx.session,metadata:{...old,tank_replay_evidence_sha256:currentHash,tank_replay_cutoff:currentCutoff}},executionFence:{workspaceId:'workspace_A',sessionId:'session_A',current:true}};
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


for(const boundary of ['get','info'])for(const refusal of ['execution','cancel'])test(`replacement preserves publication after ${refusal} during native ${boundary}`,async()=>{
  const s=fixture();let fingerprint=s.options.fingerprint;
  const identity=async()=>({version:2,image:fingerprint,compatibility:{configuration:stableDigest({scope:s.options.scope,allocation:s.allocation})}});
  const manager=s.lifecycle({fingerprint:async()=>fingerprint,identity,assertExecutionCurrent:async fence=>{if(!fence.current)throw Error('execution_authority_lost');}});
  const sb=s.sandbox(),ctx=s.context(sb);
  await manager.bind(ctx);await sb.exec('write','FAKE_FIRST');await manager.idle(ctx);
  fingerprint='sha256:'+'b'.repeat(64);
  const cancellation=new AbortController(),next={...ctx,signal:cancellation.signal,executionFence:{...ctx.executionFence,current:true}};
  ctx.executionFence.current=false;
  await manager.bind(next);
  const publication=structuredClone([...s.rows.values()][0].publication),disks=structuredClone([...s.disks]),before=s.calls.length;
  let lookups=0;
  const hook=boundary==='get'?'beforeNativeLookup':'beforeNativeInfo';
  s.provider[hook]=async id=>{
    assert.equal(id,disks[0][0]);lookups++;
    await Promise.resolve();
    const row=[...s.rows.values()][0];assert.equal(await s.fences.isCurrent(row.fence),true);
    if(refusal==='execution')next.executionFence.current=false;else cancellation.abort(Error('retirement_cancelled'));
    assert.equal(await s.fences.isCurrent(row.fence),true);
  };
  await assert.rejects(sb.exec('read','FAKE_REJECTED'),refusal==='execution'?/execution_authority_lost/:/retirement_cancelled/);
  assert.equal(lookups,1);assert.deepEqual([...s.disks],disks);assert.deepEqual(s.calls.slice(before),[]);
  assert.deepEqual([...s.rows.values()][0].publication,publication);assert.equal([...s.rows.values()][0].active,false);
  s.provider[hook]=async()=>{};
  fingerprint=s.options.fingerprint;
  const successor={...ctx,executionFence:{...ctx.executionFence,current:true}};
  await manager.bind(successor);
  assert.equal(JSON.parse((await sb.exec('read','FAKE_SUCCESSOR')).stdout).workspace,'persisted');
  await manager.idle(successor);assert.equal(s.disks.size,1);assert.equal(s.disks.has(disks[0][0]),true);
});

const orphanIdentity={version:2,image:'sha256:'+'c'.repeat(64),compatibility:{scope:'sha256:'+'d'.repeat(64)}};
function orphanQueue(rows=[]) {
  const resolved=[],failures=[];
  return {rows,resolved,failures,async enqueue(input){rows.push({id:`orphan_${rows.length}`,scope:input.scope,sandbox:input.sandbox});},
    async list(){return rows.filter(row=>!resolved.includes(row.id));},async resolve({id}){resolved.push(id);},async failed({id}){failures.push(id);}};
}
function schema2Candidate(scope,manifest) {
  const encoded=JSON.stringify(manifest);
  return {id:'publication',contentHash:stableDigest({scope,manifest:encoded}),metadata:{schema:2,scope:JSON.stringify(scope),manifest:encoded,runtimeId:'untrusted-outer-id',state:'allocating'}};
}
async function seedPublication(s,scope,candidate) {
  const acquired=await s.fences.acquire({scope,ownerId:'seed'});
  await s.fences.publish({fence:acquired.fence,workspaceCandidate:candidate});
  await s.fences.release({fence:acquired.fence,reason:'completed'});
}
for(const schema of [1,2])test(`failed retained publication and cleanup hand off to protected intent reconciliation (schema=${schema})`,async()=>{
  const s=fixture(),queue=orphanQueue(),manager=s.lifecycle({orphans:queue,...(schema===2?{identity:async()=>orphanIdentity}:{})});
  const sb=s.sandbox(),ctx=s.context(sb),publish=s.fences.publish.bind(s.fences),reap=s.provider.reap.bind(s.provider);
  s.fences.publish=async input=>{if(input.workspaceCandidate.metadata.state==='retained')throw Error('publication unavailable');return publish(input);};
  s.provider.reap=async()=>{throw Error('cleanup unavailable');};
  await manager.bind(ctx);await assert.rejects(sb.exec('write','FAKE_KEY'),/retained_initial_cleanup_failed/);
  assert.equal(queue.rows.length,1);assert.equal(s.disks.has(queue.rows[0].sandbox.runtimeId),true);
  const publication=structuredClone([...s.rows.values()][0].publication),before=s.calls.length;
  s.provider.reap=reap;await s.lifecycle({orphans:queue}).reconcileOrphans();
  assert.deepEqual(queue.resolved,['orphan_0']);assert.deepEqual(queue.failures,[]);
  assert.equal(s.disks.size,1);assert.deepEqual([...s.rows.values()][0].publication,publication);
  assert.deepEqual(s.calls.slice(before),[]);assert.equal([...s.rows.values()][0].active,false);
});
for(const protectedRole of ['current','retired','upgrade-source','upgrade-target'])test(`schema2 protects authoritative ${protectedRole} and reaps unrelated exact-owned queued disk`,async()=>{
  const s=fixture(),scope={...s.options.scope,workId:'tank-retained-session-v1'};
  const handles=[];for(let i=0;i<4;i++)handles.push((await s.provider.create(scope)).runtimeHandle());
  const [current,retired,target,stale]=handles;
  const upgrading=protectedRole.startsWith('upgrade');
  const manifest={handle:current,state:upgrading?'upgrading':'retained',archived:false,identity:orphanIdentity,retired:[retired],
    ...(upgrading?{upgrade:{source:current,target,owner:'upgrade-owner',identity:orphanIdentity}}:{})};
  const protectedHandle=protectedRole==='retired'?retired:protectedRole==='upgrade-target'?target:current;
  await seedPublication(s,scope,schema2Candidate(scope,manifest));
  const publication=structuredClone([...s.rows.values()][0].publication),before=s.calls.length;
  const queue=orphanQueue([{id:'protected',scope,sandbox:protectedHandle},{id:'stale',scope,sandbox:stale}]);
  await s.lifecycle({orphans:queue}).reconcileOrphans();
  assert.deepEqual(queue.resolved,['protected','stale']);assert.deepEqual(queue.failures,[]);
  assert.equal(s.disks.has(protectedHandle.runtimeId),true);assert.equal(s.disks.has(stale.runtimeId),false);
  assert.deepEqual(s.calls.slice(before),[['remove-owned',stale.runtimeId]]);
  assert.deepEqual([...s.rows.values()][0].publication,publication);assert.equal([...s.rows.values()][0].active,false);
});
test('schema2 ordinary queued disk survives fence replacement inside awaited native lookup',async()=>{
  const s=fixture(),scope={...s.options.scope,workId:'tank-retained-session-v1'};
  const published=(await s.provider.create(scope)).runtimeHandle(),orphan=(await s.provider.create(scope)).runtimeHandle();
  await seedPublication(s,scope,schema2Candidate(scope,{handle:published,state:'retained',archived:false,identity:orphanIdentity,retired:[]}));
  const publication=structuredClone([...s.rows.values()][0].publication),disks=structuredClone([...s.disks]),before=s.calls.length;
  const queue=orphanQueue([{id:'lookup-loss',scope,sandbox:orphan,attempts:0}]);
  queue.failed=async({id})=>{queue.failures.push(id);queue.rows[0].attempts++;};
  let lookups=0,successor;
  s.provider.beforeNativeLookup=async id=>{
    assert.equal(id,orphan.runtimeId);lookups++;
    const row=[...s.rows.values()][0];assert.equal(await s.fences.isCurrent(row.fence),true);
    row.active=false;
    successor=await s.fences.acquire({scope,ownerId:'successor'});assert.equal(successor.type,'acquired');
  };
  await s.lifecycle({orphans:queue}).reconcileOrphans();
  assert.equal(lookups,1);assert.deepEqual(queue.resolved,[]);assert.deepEqual(queue.failures,['lookup-loss']);
  assert.equal(queue.rows[0].attempts,1);assert.equal((await queue.list()).length,1);
  assert.deepEqual([...s.disks],disks);assert.deepEqual(s.calls.slice(before),[]);
  assert.deepEqual([...s.rows.values()][0].publication,publication);assert.equal(await s.fences.isCurrent(successor.fence),true);
});
for(const refusal of ['hash','json','scope','provider','identity','retired','upgrade-owner','upgrade-source','upgrade-target','upgrade-compatibility','unknown-schema','foreign-native','lost-fence','lost-after-intent','missing-intent','foreign-intent'])test(`schema2 orphan reconciliation fails closed for ${refusal}`,async()=>{
  const s=fixture(),scope={...s.options.scope,workId:'tank-retained-session-v1'};
  const handle=(await s.provider.create(scope)).runtimeHandle();
  const manifest={handle:{...handle,runtimeId:'published'},state:'retained',archived:false,identity:structuredClone(orphanIdentity),retired:[]};
  if(refusal.startsWith('upgrade-')){
    manifest.state='upgrading';manifest.upgrade={source:manifest.handle,target:{provider:'litebox',runtimeId:'target'},owner:'owner',identity:structuredClone(orphanIdentity)};
    if(refusal==='upgrade-owner')manifest.upgrade.owner='';
    if(refusal==='upgrade-source')manifest.upgrade.source=handle;
    if(refusal==='upgrade-target')manifest.upgrade.target=manifest.handle;
    if(refusal==='upgrade-compatibility')manifest.upgrade.identity.compatibility.scope='sha256:'+'e'.repeat(64);
  }
  if(refusal==='provider')manifest.handle.provider='foreign';
  if(refusal==='identity')manifest.identity.version=3;
  if(refusal==='retired')manifest.retired=[{provider:'foreign',runtimeId:'retired'}];
  if(['lost-after-intent','missing-intent','foreign-intent'].includes(refusal)){
    manifest.state='allocating';manifest.handle={provider:'litebox',runtimeId:[...s.disks.values()][0].name};
    if(refusal==='missing-intent')[...s.disks.values()][0].name='unrelated';
    if(refusal==='foreign-intent')manifest.handle.runtimeId='foreign-intent';
  }
  const candidate=schema2Candidate(scope,manifest);
  if(refusal==='hash')candidate.contentHash='sha256:'+'0'.repeat(64);
  if(refusal==='json'){candidate.metadata.manifest='{';candidate.contentHash=stableDigest({scope,manifest:'{'});}
  if(refusal==='scope')candidate.metadata.scope=JSON.stringify({...scope,sessionId:'foreign'});
  if(refusal==='unknown-schema')candidate.metadata.schema=3;
  await seedPublication(s,scope,candidate);
  if(refusal==='foreign-native')[...s.disks.values()][0].name='foreign-owner';
  const publication=structuredClone([...s.rows.values()][0].publication),before=s.calls.length;
  const current=s.fences.isCurrent.bind(s.fences);let checks=0;
  if(refusal==='lost-fence')s.fences.isCurrent=async()=>false;
  if(refusal==='lost-after-intent')s.fences.isCurrent=async fence=>++checks<3&&await current(fence);
  const queue=orphanQueue([{id:'refused',scope,sandbox:handle}]);
  await s.lifecycle({orphans:queue}).reconcileOrphans();s.fences.isCurrent=current;
  assert.deepEqual(queue.resolved,[]);assert.deepEqual(queue.failures,['refused']);
  assert.equal(s.disks.has(handle.runtimeId),true);assert.deepEqual(s.calls.slice(before),[]);
  assert.deepEqual([...s.rows.values()][0].publication,publication);
});
