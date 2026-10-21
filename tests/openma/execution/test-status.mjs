import assert from 'node:assert/strict';
import {pathToFileURL,fileURLToPath} from 'node:url';
const root=process.argv[2]??process.env.TANK_AUDIT_OPENMA_ROOT??fileURLToPath(new URL('../../../vendor/openma/',import.meta.url));
const {sessionExecutionStatusStore}=await import(pathToFileURL(root+'/packages/control-plane-node/src/lib/node-session-execution-status.ts'));
const rows=new Map(); const requests=[];
const store=sessionExecutionStatusStore({async find(input){requests.push(input);return rows.get(input.executionId)??null;}});
function put(id,state,settledAt){rows.set(id,{id,workspaceId:'tenant-a',sessionId:'session-a',state,settledAt,events:[{secret:'never-public'}],attempt:{ownerId:'never-public'},failure:'never-public'});}
for(const state of ['queued','running','completed','failed','cancelled']){
  put(state,state,['completed','failed','cancelled'].includes(state)?'2026-10-18T23:00:00Z':null);
}
const result=await store.list('tenant-a','session-a',['queued','running','completed','failed','cancelled','missing']);
assert.deepEqual(result.map(r=>r.state),['QUEUED','RUNNING','COMPLETED','FAILED','CANCELLED','UNKNOWN']);
assert.deepEqual(result.map(r=>r.settled_outcome),[null,null,'COMPLETED','FAILED','CANCELLED',null]);
assert(!JSON.stringify(result).includes('never-public'));
put('half-settled','completed',null);
assert.equal((await store.list('tenant-a','session-a',['half-settled']))[0].state,'UNKNOWN');
assert.equal((await store.list('tenant-b','session-a',['completed']))[0].state,'UNKNOWN');
assert.equal((await store.list('tenant-a','session-b',['completed']))[0].state,'UNKNOWN');
put('afterExecution-failed','failed','2026-10-18T23:00:00Z');
assert.equal((await store.list('tenant-a','session-a',['afterExecution-failed']))[0].settled_outcome,'FAILED');
put('settle-rolled-back','running',null);
assert.equal((await store.list('tenant-a','session-a',['settle-rolled-back']))[0].settled_outcome,null);
await assert.rejects(store.list('tenant-a','session-a',Array.from({length:65},(_,i)=>''+i)));
await assert.rejects(store.list('tenant-a','session-a',['same','same']));
assert(requests.every(r=>Object.keys(r).sort().join(',')==='executionId,workspaceId'));
console.log(JSON.stringify({status:'PASS',terminal_state_not_idle_inferred:true,tenant_and_session_scope:true,bounded_request_not_session_limit:true,no_payload_leak:true}));
