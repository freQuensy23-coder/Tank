import {fileURLToPath} from 'node:url';
import assert from 'node:assert/strict';
assert.equal(Number(process.versions.node.split('.')[0]),24,'Node24 required');
const root=(process.env.TANK_AUDIT_OPENMA_ROOT??fileURLToPath(new URL('../../../vendor/openma/',import.meta.url))).replace(/\/?$/, '/')+'packages/control-plane-node/src/lib/';
const {ManagedNodeHarnessRuntime}=await import(root+'node-managed-harness-runtime.ts');
const {DefaultNodeManagedSessionRuntimeDriver}=await import(root+'node-managed-session-runtime.ts');
const {installDbUnhandledRejectionNet}=await import(root+'db-unhandled-rejection.ts');
const mode=process.argv[2];
const fatal=[];
const remove=installDbUnhandledRejectionNet({logger:{fatal(record,line){fatal.push(record);console.log(JSON.stringify({kind:'actual-process-net-fatal',record,line}));}}});
const pause=()=>new Promise(resolve=>setTimeout(resolve,30));
let sequence=0;
const basic=output=>({initialEvents:[],events:[],sandbox:{},output,clock:{now:()=>new Date('2026-10-19T00:00:00Z')},ids:{nextEventId:()=>`event_${++sequence}`}});
const outputs=new Map(),projected=[],delivered=[];let drainError;
const engine={
 async start(input,output){outputs.set(input.sessionId,output);},async stop(){},async archiveThread(){},
 async accept(input){
   const runtime=new ManagedNodeHarnessRuntime(basic(outputs.get(input.sessionId)));
   runtime.broadcastProducedEvent({type:'session.status_running'});
   runtime.broadcastProducedEvent({type:'session.status_idle',stopReason:{type:'end_turn'}});
   await pause();
   try{await runtime.drain();}catch(error){drainError=error;throw error;}
 }
};
const driver=new DefaultNodeManagedSessionRuntimeDriver({engine,realtime:{attach(){return()=>{};},publish(frame){delivered.push(frame);},closeSession(){}},projectionFor(){return {async recordSessionRuntimeEvents(input){projected.push(input);return input.sessionId==='stale'?{type:'execution_fence_lost'}:{type:'recorded'};}};}});
function input(id){return {workspaceId:'tenant',sessionId:id,session:{},environment:{},events:[],executionFence:{workspaceId:'tenant',sessionId:id,executionId:'input-'+id,attemptId:'attempt-'+id,ownerId:'owner',generation:1,expiresAt:'2026-10-19T01:00:00Z'}};}
let rejected;
try{await driver.accept(input('stale'));}catch(error){rejected=error;}
assert(rejected instanceof Error);
assert.equal(rejected.message,'Session tenant/stale execution fence was lost');
assert.strictEqual(rejected,drainError);
assert.equal(projected.filter(p=>p.sessionId==='stale').length,1,'queued frame after failure reached projection');
assert.equal(delivered.length,0,'stale frame reached realtime');
assert.equal(fatal.length,0,'delayed-drain rejection escaped owning runtime');
assert.equal(mode,'controls','negative control unexpectedly survived');
await driver.accept(input('healthy'));
assert.equal(projected.filter(p=>p.sessionId==='healthy').length,2);
const error=new Error('recording failed deliberately');let writes=0;
const recording=new ManagedNodeHarnessRuntime(basic(async()=>{writes++;throw error;}));
recording.broadcastProducedEvent({type:'session.status_running'});
recording.broadcastProducedEvent({type:'session.status_idle',stopReason:{type:'end_turn'}});
await pause();await assert.rejects(recording.drain(),e=>e===error);assert.equal(writes,1);
const order=[];
const healthy=new ManagedNodeHarnessRuntime(basic(async frame=>{await Promise.resolve();order.push(frame.type);}));
healthy.broadcastProducedEvent({type:'session.status_running'});
healthy.broadcastProducedEvent({type:'session.status_idle',stopReason:{type:'end_turn'}});
await healthy.drain();assert.deepEqual(order,['session.status_running','session.status_idle']);
const stream=new ManagedNodeHarnessRuntime(basic(async()=>{throw error;}));
const pending=stream.broadcastToolInputChunk('tool','x');
await pause();await assert.rejects(pending,e=>e===error);await assert.rejects(stream.drain(),e=>e===error);
assert.equal(fatal.length,0);
remove();
console.log(JSON.stringify({status:'PASS',checks:['actual_driver_fence_rejection_owned_until_delayed_drain','drain_preserves_original_error','queued_stale_frame_not_delivered','unrelated_session_continues','recording_error_preserved','success_order_and_drain','awaited_stream_error_preserved'],native:false,models:0}));
