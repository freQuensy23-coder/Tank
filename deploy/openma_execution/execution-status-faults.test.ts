import { describe, expect, it } from "vitest";
import { createBetterSqlite3SqlClient } from "@open-managed-agents/sql-client";
import { ensureSessionExecutionCoordinatorSchema, SqlSessionExecutionCoordinator } from "@open-managed-agents/session-runtime-sql/coordination";
import type { Environment, Session } from "@open-managed-agents/managed-agents-application";
import { NodeSessionExecutionWorker } from "../../../packages/control-plane-node/src/lib/node-session-execution-worker";
import { sessionExecutionStatusStore } from "../../../packages/control-plane-node/src/lib/node-session-execution-status";

const session: Session = {
  id: "session-fault", agent: {id:"agent",description:null,mcpServers:[],model:{id:"model"},multiagent:null,name:"Agent",skills:[],system:null,tools:[],version:1},
  archivedAt:null,budget:null,createdAt:"2026-09-04T00:00:00Z",environmentId:"env",metadata:{},outcomeEvaluations:[],resources:[],stats:{},status:"idle",title:null,updatedAt:"2026-09-04T00:00:00Z",usage:{},vaultIds:[],
};
const environment: Environment = {id:"env",archivedAt:null,config:{type:"cloud",networking:{type:"unrestricted"},packages:{apt:[],cargo:[],gem:[],go:[],npm:[],pip:[]}},createdAt:session.createdAt,description:null,metadata:{},name:"Env",updatedAt:session.createdAt};
async function setup() {
  const sql=await createBetterSqlite3SqlClient(":memory:");
  await ensureSessionExecutionCoordinatorSchema(sql);
  const coordinator=new SqlSessionExecutionCoordinator(sql);
  let now=new Date("2026-09-04T00:00:02Z"); let attempt=0;
  const make=(runtime:{run:(input:unknown)=>Promise<void>;cancel:()=>Promise<void>}, control=coordinator)=>new NodeSessionExecutionWorker({coordinator:control,context:{find:async()=>({session,environment,revision:1})},runtime,ownerId:"fault-worker-"+(++attempt),clock:{now:()=>now},ids:{nextAttemptId:()=>"attempt-"+(++attempt)},leaseTtlMs:30000,heartbeatIntervalMs:10000,maxConcurrent:4,onError:()=>{}});
  const accepted=(id:string)=>({workspaceId:"tenant",sessionId:session.id,session,environment,events:[{id,type:"user.message" as const,content:[{type:"text" as const,text:"Fixture input"}],processedAt:"2026-09-04T00:00:01Z"}]});
  return {coordinator,make,accepted,status:sessionExecutionStatusStore(coordinator),advance:()=>{now=new Date(now.getTime()+31000);}};
}
describe("committed execution status fault boundary",()=>{
  it("does not call recorded end_turn successful when runtime finalization throws",async()=>{
    const fixture=await setup();const recordedTerminal=[];
    const worker=fixture.make({run:async()=>{recordedTerminal.push({type:"session.status_idle",stopReason:{type:"end_turn"}});throw new Error("Injected afterExecution finalization failure");},cancel:async()=>{}});
    await worker.sessionEventsAccepted(fixture.accepted("afterExecution-failure"));await worker.waitForIdle();worker.stop();
    expect(recordedTerminal).toHaveLength(1);
    expect(await fixture.status.list("tenant",session.id,["afterExecution-failure"])).toEqual([{execution_id:"afterExecution-failure",state:"FAILED",settled_outcome:"FAILED"}]);
    expect((await fixture.coordinator.find({workspaceId:"tenant",executionId:"afterExecution-failure"}))?.settledAt).not.toBeNull();
  });
  it("keeps failed-settlement RUNNING and completes only after fenced reclaim actually settles",async()=>{
    const fixture=await setup();let runCount=0;
    const runtime={run:async()=>{runCount++;},cancel:async()=>{}};
    let reject=true;
    const control=new Proxy(fixture.coordinator,{get(target,key){if(key==="settle")return async (...args:Parameters<typeof target.settle>)=>{if(reject){reject=false;throw new Error("Injected settle transaction failure");}return target.settle(...args);};const value=Reflect.get(target,key);return typeof value==="function"?value.bind(target):value;}});
    const first=fixture.make(runtime,control);
    await first.sessionEventsAccepted(fixture.accepted("settlement-recovery"));await first.waitForIdle();first.stop();
    expect(await fixture.status.list("tenant",session.id,["settlement-recovery"])).toEqual([{execution_id:"settlement-recovery",state:"RUNNING",settled_outcome:null}]);
    fixture.advance();const recovered=fixture.make(runtime);await recovered.poll();await recovered.waitForIdle();recovered.stop();
    expect(runCount).toBe(2);
    expect(await fixture.status.list("tenant",session.id,["settlement-recovery"])).toEqual([{execution_id:"settlement-recovery",state:"COMPLETED",settled_outcome:"COMPLETED"}]);
    expect((await fixture.coordinator.find({workspaceId:"tenant",executionId:"settlement-recovery"}))?.attemptCount).toBe(2);
  });
});
