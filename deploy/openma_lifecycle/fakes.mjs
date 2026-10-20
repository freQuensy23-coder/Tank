import assert from 'node:assert/strict';
import { RetainedSessionLifecycle } from './retained-session.ts';
const fingerprint = 'sha256:' + 'a'.repeat(64);
export function setup() {
  // Durable publication belongs to the injected canonical fence store, not a
  // controller object. A new controller below simulates a process restart.
  const rows = new Map();
  let number = 0;
  const key = scope => JSON.stringify(scope);
  const fences = {
    async acquire({scope, ownerId}) {
      const old = rows.get(key(scope));
      if (old?.active && old.fence.ownerId !== ownerId) return {type:'conflict',expiresAt:null};
      const row = {active:true,publication:old?.publication ?? null,
        fence:{...scope,ownerId,generation:++number,token:'FAKE_FENCE_'+number,expiresAt:'2099-01-01T00:00:00Z'}};
      rows.set(key(scope),row);
      return {type:'acquired',fence:row.fence,publication:structuredClone(row.publication)};
    },
    async isCurrent(fence) { const row=rows.get(key(fenceScope(fence)));return Boolean(row?.active && row.fence.token===fence.token); },
    async renew({fence}) { return await this.isCurrent(fence) ? {type:'renewed',fence} : {type:'lost'}; },
    async publish({fence,workspaceCandidate}) {
      if(!await this.isCurrent(fence))return {type:'lost'};
      rows.get(key(fenceScope(fence))).publication={workspaceCandidate:structuredClone(workspaceCandidate)};
      return {type:'published',revision:1};
    },
    async release({fence}) { if(await this.isCurrent(fence))rows.get(key(fenceScope(fence))).active=false; },
  };
  const disks=new Map(), calls=[], nativeHandles=[];
  let next=0, creates=0;
  function native(disk) {
    // A stopped handle is INVALID, as in BoxLite0.9 and0.10.6. Only a new
    // provider.resume handle can be used to execute again.
    let invalid=false;
    const value={
      runtimeHandle:()=>({provider:'litebox',runtimeId:disk.id}),
      async suspend(){assert.equal(invalid,false);calls.push(['stop',disk.id]);invalid=true;disk.running=false;},
      async exec(command,env={}){assert.equal(invalid,false);assert.equal(disk.running,true);calls.push(['exec',disk.id,command]);return {disk,env};},
    };
    nativeHandles.push(value);return value;
  }
  const provider={
    async allocationIntent(scope){if([...disks.values()].some(d=>d.scope===JSON.stringify(scope)))throw Error('retained_unpublished_runtime_exists');return {provider:'litebox',runtimeId:'intent:'+scope.sessionId};},
    async resolveIntent(handle,scope){assert.equal(handle.runtimeId,'intent:'+scope.sessionId);const disk=[...disks.values()].find(d=>d.scope===JSON.stringify(scope));return disk?{provider:'litebox',runtimeId:disk.id}:null;},
    async create(scope){creates++;const disk={id:'box_'+(++next),scope:JSON.stringify(scope),files:{},profile:{},running:true};disks.set(disk.id,disk);calls.push(['create',disk.id]);return native(disk);},
    async resume(handle,scope){calls.push(['fresh-get',handle.runtimeId]);const disk=disks.get(handle.runtimeId);if(!disk)throw Error('owned retained disk missing');assert.equal(disk.scope,JSON.stringify(scope));disk.running=true;return native(disk);},
    async reap(handle,scope){const disk=disks.get(handle.runtimeId);if(disk)assert.equal(disk.scope,JSON.stringify(scope));calls.push(['remove-owned',handle.runtimeId]);disks.delete(handle.runtimeId);},
  };
  const options={scope:{workspaceId:'workspace_A',environmentId:'production_A',sessionId:'session_A'},fingerprint,
    ownerId:'controller_A',providerId:'litebox',leaseTtlMs:30000,fences,provider};
  return {rows,fences,provider,disks,calls,nativeHandles,options,get creates(){return creates;},
    controller:changes=>new RetainedSessionLifecycle({...options,...changes})};
}
function fenceScope(fence){const {workspaceId,environmentId,sessionId,workId}=fence;return {workspaceId,environmentId,sessionId,workId};}

