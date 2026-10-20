/** Actual patched Node runner hooks. No model, VM, secrets or network. */
import assert from 'node:assert/strict';
const root=process.env.TANK_AUDIT_OPENMA_ROOT;
assert.ok(root);
const {DefaultNodeManagedSessionRunner}=await import(root+'/packages/control-plane-node/src/lib/node-managed-session-runner.ts');
const session={id:'runner_session',agent:{id:'runner_agent',description:null,mcpServers:[],model:{id:'offline-no-model'},multiagent:null,name:'offline',skills:[],system:null,tools:[],version:1},archivedAt:null,budget:null,createdAt:'2026-10-18T00:00:00.000Z',environmentId:'runner_env',metadata:{},outcomeEvaluations:[],resources:[],stats:{},status:'running',title:null,updatedAt:'2026-10-18T00:00:00.000Z',usage:{},vaultIds:[]};
const environment={id:'runner_env',archivedAt:null,config:{type:'self_hosted'},createdAt:'2026-10-18T00:00:00.000Z',description:null,metadata:{},name:'offline',updatedAt:'2026-10-18T00:00:00.000Z'};
for(const scenario of ['lease_lost','interrupt','after_failure']){
  const calls=[];let next=0,started;
  const ready=new Promise(resolve=>{started=resolve;});
  const runner=new DefaultNodeManagedSessionRunner({
    outcomes:{async evaluate(){assert.fail('no outcomes');}},confirmedTools:{async execute(){assert.fail('no confirmation');}},
    async buildSandbox(){return {};},async buildModel(){return {};},async buildTools(){return {};},
    async disposeTools(){calls.push('tools-disposed');},async synchronizeSandbox(){calls.push('synchronized');},
    async afterExecution(){calls.push('after-execution');if(scenario==='after_failure')throw Error('injected publication failure');},
    buildHarness(){return {async run(context){started();if(scenario==='after_failure')return;const signal=context.runtime.abortSignal;await new Promise((_,reject)=>signal.addEventListener('abort',()=>reject(Error('injected aborted model')),{once:true}));}};},
    async buildHarnessContext(input){return input;},clock:{now:()=>new Date('2026-10-18T00:00:00.000Z')},ids:{nextEventId:()=>`runner_event_${++next}`},
    retainedLifecycle:{async bind(){calls.push('bound');},async idle(){calls.push('retained-idle');},async abandon(){calls.push('retained-abandon');},async archive(){calls.push('archive');}},
  });
  const start={workspaceId:'runner_workspace',sessionId:session.id,session,environment,initialEvents:[],executionFence:{current:true}};
  await runner.start(start);
  const message={id:'runner_message',type:'user.message',content:[{type:'text',text:'offline'}],processedAt:'2026-10-18T00:00:00.000Z'};
  const turn=runner.accept({...start,events:[message],historyEvents:[message],async output(){calls.push('output-drained');}});
  const rejection=assert.rejects(turn,scenario==='after_failure'?/injected publication failure/:/injected aborted model/);
  await ready;
  if(scenario==='lease_lost')runner.cancel({...start,reason:'lease_lost'});
  if(scenario==='interrupt')await runner.accept({...start,events:[{id:'interrupt',type:'user.interrupt',processedAt:message.processedAt}],historyEvents:[message],async output(){}});
  await rejection;
  const hook=scenario==='lease_lost'?'retained-abandon':'retained-idle';
  assert.equal(calls.filter(c=>c===hook).length,1,scenario);
  assert.equal(calls.includes(scenario==='lease_lost'?'retained-idle':'retained-abandon'),false);
  assert.ok(calls.indexOf(hook)>calls.indexOf('tools-disposed'));
  assert.ok(calls.indexOf(hook)>calls.indexOf('after-execution'));
}
console.log(JSON.stringify({status:'PASS',actual_patched_node_runner:true,lease_lost_abandon_after_draining:true,publication_failure_finalization:true,concurrent_user_interrupt_preserved:true,native_vm:'NOT_RUN'}));
