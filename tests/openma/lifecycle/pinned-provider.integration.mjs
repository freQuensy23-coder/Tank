const openmaSource = process.env.TANK_AUDIT_OPENMA_ROOT ? pathToFileURL(process.env.TANK_AUDIT_OPENMA_ROOT.replace(/\/?$/, '/')) : new URL('../../../vendor/openma/', import.meta.url);
import { pathToFileURL } from 'node:url';
import assert from 'node:assert/strict';
import { mkdtemp,readFile,writeFile,rm,mkdir,rename,stat } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createHash } from 'node:crypto';
const { validateReplayAttachment } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/replay-attachment.ts', openmaSource));
import { spawn, spawnSync } from 'node:child_process';
const { NodeRetainedLifecycle } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/node-lifecycle.ts', openmaSource));
const { pinnedBoxLiteProvider } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/boxlite-provider.ts', openmaSource));
const { GUEST_FILE_WORKER } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/guest-file-worker.ts', openmaSource));
const { RetainedSessionLifecycle } = await import(new URL('packages/control-plane-node/src/lib/tank-retained/retained-session.ts', openmaSource));
const root=process.env.TANK_AUDIT_OPENMA_ROOT;
assert.ok(root,'TANK_AUDIT_OPENMA_ROOT required');
const providerModule=await import(root+'/packages/managed-runtime-boxlite/src/index.ts');
const {LiteBoxSandbox,verifyReplayEvidence}=await import(root+'/packages/sandbox-adapter-litebox/src/index.ts');
const {createBetterSqlite3SqlClient}=await import(root+'/packages/sql-client/src/index.ts');
const {ensureRuntimeResourceFenceSchema,SqlRuntimeResourceFencePort,SqlRuntimeOrphanPort}=await import(root+'/packages/runtime-resource-fence-sql/src/index.ts');
const directory=process.env.TANK_AUDIT_CRASH_DIR??await mkdtemp(join(tmpdir(),'oma-retained-offline-'));
const database=join(directory,'canonical-resource-pointer.sqlite');
const sql=await createBetterSqlite3SqlClient(database);await ensureRuntimeResourceFenceSchema(sql);
const disks=new Map(),calls=[],children=[];
let sequence=0,failWorkspace=0,failRemove=0,failAfterAllocation=0,loseFenceAfterAllocation=false,injectedFenceLost=false;
function native(disk) {
  let invalid=false;
  return {
    id:disk.id,name:disk.name,
    async info(){return {id:disk.id,name:disk.name,state:{running:disk.running,status:disk.running?'Running':'Stopped'}};},
    async start(){assert.equal(invalid,false);disk.running=true;calls.push(['start',disk.id]);},
    async stop(){assert.equal(invalid,false);invalid=true;disk.running=false;calls.push(['stop',disk.id]);},
    async exec(command,args=[],env=[]) {
      assert.equal(invalid,false);assert.equal(disk.running,true);
      if(command==='python3') {
        const python=process.env.TANK_FILE_IO_TEST_PYTHON;
        assert.ok(python,'explicit Python3.13 test interpreter required');
        assert.equal(args[0],'-c');assert.equal(args[1],GUEST_FILE_WORKER);
        assert.ok(['read','write'].includes(args[2]));
        assert.equal(args.length,args[2]==='write'?5:4);
        assert.ok(args[3].startsWith('/')&&!args[3].split('/').includes('..'));
        const guestArgs=[...args];guestArgs[3]=join(directory,'guest-'+disk.id,args[3].slice(1));
        const child=spawn(python,guestArgs,{stdio:['pipe','pipe','pipe'],env:{PATH:'/usr/bin:/bin',...Object.fromEntries(env)}});
        children.push(child);child.stdout.setEncoding('utf8');child.stderr.setEncoding('utf8');
        const exited=new Promise((resolve,reject)=>{child.once('error',reject);child.once('exit',code=>resolve({exitCode:code}));});
        const output=stream=>{const iterator=stream[Symbol.asyncIterator]();return {async next(){const row=await iterator.next();return row.done?null:row.value;}};};
        return {async stdin(){return {
          async write(data){await new Promise((resolve,reject)=>child.stdin.write(data,error=>error?reject(error):resolve()));},
          async close(){if(child.stdin.destroyed||child.stdin.writableFinished)return;await new Promise((resolve,reject)=>{child.stdin.once('error',reject);child.stdin.end(resolve);});},
        };},async stdout(){return output(child.stdout);},async stderr(){return output(child.stderr);},
          async wait(){return exited;},async kill(){child.kill('SIGKILL');await exited;},async id(){return 'exec_worker_'+child.pid;}};
      }
      let output='';const script=command==='sh'?args.at(-1):command;
      if(command==='mkdir' && args.join(' ')==='-p /workspace' && failWorkspace>0){failWorkspace--;throw Error('injected workspace initialization failure');}
      if(script==='AUDIT_WRITE') {
        disk.files.set('/root/marker',Buffer.from('retained-rootfs'));
        disk.files.set('/workspace/analysis',Buffer.from('retained-workspace'));
        disk.files.set('/workspace/.tank-browser/profile-cookie',Buffer.from('retained-profile'));
        output=new Map(env).get('AUDIT_ONLY_SECRET')??'';
      }
      if(script==='AUDIT_READ')output=JSON.stringify({root:disk.files.get('/root/marker')?.toString(),workspace:disk.files.get('/workspace/analysis')?.toString(),profile:disk.files.get('/workspace/.tank-browser/profile-cookie')?.toString(),key:new Map(env).get('AUDIT_ONLY_SECRET')});
      return {async stdin(){return {async write(){},async close(){}};},
        async stdout(){let sent=false;return {async next(){if(sent||!output)return null;sent=true;return output;}};},
        async stderr(){return {async next(){return null;}};},async wait(){return {exitCode:0};},async kill(){},async id(){return 'exec_FAKE';}};
    },
    async copyIn(){assert.fail('native copyIn must not be used');},
    async copyOut(){assert.fail('native copyOut must not be used');},
  };
}
const client={
  async getInfo(id){const disk=disks.get(id)??[...disks.values()].find(d=>d.name===id);return disk?native(disk).info():null;},
  async get(id){const disk=disks.get(id)??[...disks.values()].find(d=>d.name===id);calls.push(['fresh-get',id]);return disk?native(disk):null;},
  async getOrCreate(options,name){let disk=[...disks.values()].find(d=>d.name===name);const created=!disk;
    if(!disk){assert.equal(options.autoRemove,false);assert.equal(options.detach,true);assert.equal(options.env,undefined);
      disk={id:process.env.TANK_AUDIT_CRASH_DIR?'native_crash':'native_'+(++sequence),name,running:false,files:new Map(),allocation:options};disks.set(disk.id,disk);calls.push(['allocate',disk.id]);}
    if(process.env.TANK_AUDIT_CRASH_DIR){await writeFile(join(directory,'synthetic-crashed-native.json'),JSON.stringify({id:disk.id,name:disk.name,running:disk.running}));process.exit(23);}
    if(loseFenceAfterAllocation){loseFenceAfterAllocation=false;injectedFenceLost=true;}
    if(failAfterAllocation>0){failAfterAllocation--;throw Error('injected getOrCreate post-allocation failure');}
    return {created,box:native(disk)};},
  async remove(id){calls.push(['remove-owned',id]);if(failRemove>0){failRemove--;throw Error('injected remove failure');}disks.delete(id);},
};
if(process.env.TANK_AUDIT_CRASH_DIR){
  const fences=new SqlRuntimeResourceFencePort(sql);
  const provider=pinnedBoxLiteProvider({module:providerModule,client,allocation:{image:'synthetic-no-pull'},leaseTtlMs:30000,isCurrent:f=>fences.isCurrent(f)});
  await new RetainedSessionLifecycle({scope:{workspaceId:'crash_w',environmentId:'production_A',sessionId:'crash_s'},fingerprint:'sha256:'+'c'.repeat(64),providerId:'litebox',ownerId:'abrupt-child',leaseTtlMs:30000,fences,provider}).begin(async()=>{},new AbortController().signal);
  throw Error('crash child must exit during native allocation');
}
const manager=async()=>new NodeRetainedLifecycle({fences:new SqlRuntimeResourceFencePort(await createBetterSqlite3SqlClient(database)),providerModule,client,ownerId:'offline-controller',leaseTtlMs:30000,
  fingerprint:async()=> 'sha256:'+'a'.repeat(64),assertExecutionCurrent:async f=>{assert.equal(f.current,true);}});
const makeSandbox=()=>new LiteBoxSandbox({image:'synthetic-no-pull',cpus:1,memoryMib:512,scratchRoot:directory});
const context=sandbox=>({workspaceId:'workspace_A',sessionId:'session_A',session:{id:'session_A',environmentId:'production_A'},environment:{id:'production_A'},sandbox,executionFence:{current:true}});
try {
  const first=await manager(),sandbox=makeSandbox(),ctx=context(sandbox);
  await sandbox.setEnvVars({AUDIT_ONLY_SECRET:'FAKE_INITIAL'});await first.bind(ctx);
  assert.equal((await sandbox.exec('AUDIT_WRITE')).trim(),'FAKE_INITIAL');
  await sandbox.writeFileBytes('/workspace/binary',new Uint8Array([0,255,2,0]));
  await first.idle(ctx);assert.equal([...disks.values()][0].running,false);
  const second=await manager(),replacement=makeSandbox(),next=context(replacement);
  await replacement.setEnvVars({AUDIT_ONLY_SECRET:'FAKE_ROTATED'});await second.bind(next);
  assert.deepEqual(JSON.parse(await replacement.exec('AUDIT_READ')),
    {root:'retained-rootfs',workspace:'retained-workspace',profile:'retained-profile',key:'FAKE_ROTATED'});
  assert.deepEqual(await replacement.readFileBytes('/workspace/binary'),new Uint8Array([0,255,2,0]));
  await second.idle(next);await sandbox.destroy();await replacement.destroy();
  assert.equal(disks.size,1);assert.equal(JSON.stringify(await sql.prepare('SELECT publication_json FROM runtime_resource_fences').all()).includes('FAKE_ROTATED'),false);
  await (await manager()).archive({...next,reason:'archived'});assert.equal(disks.size,0);
  assert.equal(calls.filter(c=>c[0]==='allocate').length,1);
  const scope={workspaceId:'failure_workspace',environmentId:'production_A',sessionId:'failure_session',workId:'tank-retained-session-v1'};
  let clockOffset=0;
  const fences=new SqlRuntimeResourceFencePort(sql,{now:()=>new Date(Date.now()+clockOffset)}),orphanQueue=new SqlRuntimeOrphanPort(sql);
  const actualCurrent=fences.isCurrent.bind(fences);fences.isCurrent=async f=>!injectedFenceLost&&await actualCurrent(f);
  const provider=pinnedBoxLiteProvider({module:providerModule,client,allocation:{image:'synthetic-no-pull'},leaseTtlMs:30000,isCurrent:f=>fences.isCurrent(f),unpublishedCleanupFailed:async(sandbox,scope,fence)=>{await orphanQueue.enqueue({scope,generation:fence.generation,ownerId:fence.ownerId,sandbox,reason:'failed',error:Error('static cleanup failure')});}});
  const controller=(resourceScope=scope)=>new RetainedSessionLifecycle({scope:resourceScope,fingerprint:'sha256:'+'b'.repeat(64),providerId:'litebox',ownerId:'failure-controller',leaseTtlMs:30000,fences,provider,orphans:orphanQueue});
  failWorkspace=1;failRemove=1;
  await assert.rejects(controller().begin(async()=>{},new AbortController().signal),/injected workspace initialization failure/);
  assert.equal(disks.size,0,'wrapper compensates when pinned provider suppressed remove failure');
  failAfterAllocation=1;
  await assert.rejects(controller().begin(async()=>{},new AbortController().signal),/injected getOrCreate post-allocation failure/);
  assert.equal(disks.size,0,'lookup removes only newly allocated unpublished exact scoped runtime');
  const retained=controller();await retained.begin(async()=>{},new AbortController().signal);await retained.idle(async()=>{});
  const retainedId=[...disks.keys()][0];
  failWorkspace=1;
  await assert.rejects(controller().begin(async()=>{},new AbortController().signal),/injected workspace initialization failure/);
  assert.equal(disks.get(retainedId).running,false,'failed resume stops via fresh owned handle and retains disk');
  const retry=controller();await retry.begin(async()=>{},new AbortController().signal);
  const publish=fences.publish.bind(fences);let failPublish=true;
  fences.publish=async input=>{if(failPublish){failPublish=false;throw Error('injected publication failure');}return publish(input);};
  await assert.rejects(retry.idle(async()=>{}),/injected publication failure/);
  await assert.rejects(retry.use(async()=>{}),/retained_not_active/);
  const stops=calls.filter(c=>c[0]==='stop').length;await retry.idle(async()=>{});
  assert.equal(calls.filter(c=>c[0]==='stop').length,stops,'retry cannot stop invalidated native handle again');
  const recovered=controller();await recovered.begin(async()=>{},new AbortController().signal);await recovered.idle(async()=>{});
  await controller().archive(async()=>{},new AbortController().signal);assert.equal(disks.size,0);
  const acknowledgementScope={...scope,sessionId:'acknowledgement_failure_session'};
  fences.publish=publish;
  let initialAcknowledgementFailure=true;
  fences.publish=async input=>{const result=await publish(input);if(initialAcknowledgementFailure && input.workspaceCandidate.metadata.state==='retained'){initialAcknowledgementFailure=false;throw Error('injected lost initial publication acknowledgement');}return result;};
  await assert.rejects(controller(acknowledgementScope).begin(async()=>{},new AbortController().signal),/injected lost initial publication acknowledgement/);
  assert.equal(disks.size,1);assert.equal([...disks.values()][0].running,false);
  const acknowledged=controller(acknowledgementScope);await acknowledged.begin(async()=>{},new AbortController().signal);
  const release=fences.release.bind(fences);let failRelease=true;
  fences.release=async input=>{await release(input);if(failRelease){failRelease=false;throw Error('injected lost release acknowledgement');}};
  await assert.rejects(acknowledged.idle(async()=>{}),/injected lost release acknowledgement/);
  await assert.rejects(acknowledged.use(async()=>{}),/retained_not_active/);
  const releaseStops=calls.filter(c=>c[0]==='stop').length;await acknowledged.idle(async()=>{});
  assert.equal(calls.filter(c=>c[0]==='stop').length,releaseStops);
  await controller(acknowledgementScope).archive(async()=>{},new AbortController().signal);assert.equal(disks.size,0);
  for(const throwAfter of [false,true]){
    const lossScope={...scope,sessionId:'lost_fence_'+throwAfter};
    loseFenceAfterAllocation=true;if(throwAfter)failAfterAllocation=1;
    await assert.rejects(controller(lossScope).begin(async()=>{},new AbortController().signal),/retained_initial_cleanup_failed/);
    assert.equal(disks.size,1);assert.equal((await orphanQueue.list({limit:128})).length,1);
    injectedFenceLost=false;
    const reconciler=new NodeRetainedLifecycle({fences,orphans:orphanQueue,providerModule,client,ownerId:'orphan-reconciler',leaseTtlMs:30000,fingerprint:async()=> 'sha256:'+'b'.repeat(64),assertExecutionCurrent:async()=>{}});
    await reconciler.reconcileOrphans();assert.equal(disks.size,1,'durable allocating intent protects its exact native root');
    assert.equal((await orphanQueue.list({limit:128})).length,0);
    const recoveredLoss=controller(lossScope);await recoveredLoss.begin(async()=>{},new AbortController().signal);await recoveredLoss.idle(async()=>{});
    await controller(lossScope).archive(async()=>{},new AbortController().signal);assert.equal(disks.size,0);
  }
  const crashed=spawnSync(root+'/apps/main-node/node_modules/.bin/tsx',[import.meta.filename],{env:{...process.env,TANK_AUDIT_CRASH_DIR:directory},encoding:'utf8'});
  assert.equal(crashed.status,23,crashed.stderr||crashed.stdout);
  const crashDisk=JSON.parse(await readFile(join(directory,'synthetic-crashed-native.json'),'utf8'));crashDisk.files=new Map();disks.set(crashDisk.id,crashDisk);
  const {results:intentRows}=await sql.prepare('SELECT publication_json FROM runtime_resource_fences WHERE session_id = ?').bind('crash_s').all();
  assert.equal(JSON.parse(intentRows[0].publication_json).workspaceCandidate.metadata.state,'allocating');
  clockOffset=60000;
  const crashScope={workspaceId:'crash_w',environmentId:'production_A',sessionId:'crash_s',workId:'tank-retained-session-v1'};
  const crashController=new RetainedSessionLifecycle({scope:crashScope,fingerprint:'sha256:'+'c'.repeat(64),providerId:'litebox',ownerId:'replacement-after-exit',leaseTtlMs:30000,fences,provider});
  const allocatedBefore=calls.filter(c=>c[0]==='allocate').length;
  await crashController.begin(async()=>{},new AbortController().signal);await crashController.idle(async()=>{});
  assert.equal(calls.filter(c=>c[0]==='allocate').length,allocatedBefore,'abrupt restart resolves intent without creating a duplicate disk');
  await crashController.archive(async()=>{},new AbortController().signal);assert.equal(disks.size,0);
  const evidenceRoot=join(directory,'replay-prefix'),runId='run_A',runRoot=join(evidenceRoot,runId);
  await mkdir(runRoot,{recursive:true});const rootInode=(await stat(runRoot)).ino;
  const publishPrefix=async(run,cutoff)=>{
    const selected=join(evidenceRoot,run);await mkdir(selected,{recursive:true});
    const manifest={schema:'tank.replay-evidence.v1',run_id:run,input_sha256:'d'.repeat(64),cutoff,cutoff_prec:'EXACT',cutoff_prec_reason:null,
      guest_root:'/replay/evidence',files:[],identity_key:['instrument_id','trade_id'],format:'jsonl',
      sealed_at:'2026-10-18T00:00:00+00:00',sealed_at_prec:'COLLECTION_ONLY',sealed_at_prec_reason:null};
    const bytes=JSON.stringify(manifest)+'\n';await writeFile(join(selected,'manifest.staging'),bytes);await rename(join(selected,'manifest.staging'),join(selected,'manifest.json'));
    return {tank_replay_run_id:run,tank_replay_evidence_sha256:createHash('sha256').update(bytes).digest('hex'),tank_replay_cutoff:cutoff};
  };
  let validated=0;
  const prefixFingerprint=async(context,allocation)=>{
    validated++;await validateReplayAttachment(context,allocation,verifyReplayEvidence,evidenceRoot);
    return 'sha256:'+createHash('sha256').update(JSON.stringify({scope:[context.workspaceId,context.sessionId],allocation,policy:context.environment.config})).digest('hex');
  };
  const prefixManager=async()=>new NodeRetainedLifecycle({fences:new SqlRuntimeResourceFencePort(await createBetterSqlite3SqlClient(database)),providerModule,client,
    ownerId:'prefix-controller',leaseTtlMs:30000,fingerprint:prefixFingerprint,assertExecutionCurrent:async fence=>assert.equal(fence.current,true)});
  const prefixSandbox=()=>new LiteBoxSandbox({image:'synthetic-no-pull',cpus:1,memoryMib:2048,scratchRoot:directory,
    tankNetwork:{mode:'disabled'},replayEvidence:{hostPath:runRoot,guestPath:'/replay/evidence',readOnly:true}});
  const metadata1=await publishPrefix(runId,'2026-10-18T00:00:01+00:00');
  const prefixSb=prefixSandbox(),prefixCtx={workspaceId:'prefix_workspace',sessionId:'prefix_session',session:{id:'prefix_session',environmentId:'prefix_env',metadata:metadata1},
    environment:{id:'prefix_env',name:'tank-replay',config:{type:'cloud',networking:{type:'limited',allowedHosts:['postgres']}}},sandbox:prefixSb,executionFence:{current:true}};
  const prefix=await prefixManager();await prefix.bind(prefixCtx);await prefixSb.exec('AUDIT_WRITE');await prefix.idle(prefixCtx);
  const prefixId=[...disks.keys()][0],staticAllocation=disks.get(prefixId).allocation;
  assert.ok(staticAllocation.volumes.some(volume=>volume.hostPath===runRoot&&volume.guestPath==='/replay/evidence'&&volume.readOnly===true));
  assert.equal(staticAllocation.memoryMib,2048);
  const metadata2=await publishPrefix(runId,'2026-10-18T00:00:02+00:00'),nextCtx={...prefixCtx,session:{...prefixCtx.session,metadata:metadata2},executionFence:{current:true}};
  await prefix.bind(nextCtx);assert.equal(validated,2,'warm bind must revalidate latest metadata');
  assert.equal(JSON.parse(await prefixSb.exec('AUDIT_READ')).workspace,'retained-workspace');await prefix.idle(nextCtx);
  const lookups=calls.filter(call=>call[0]==='fresh-get').length;
  await assert.rejects(prefix.bind(prefixCtx),/session manifest hash mismatch/);
  const foreignMeta=await publishPrefix('run_B','2026-10-18T00:00:02+00:00');
  await assert.rejects(prefix.bind({...nextCtx,session:{...nextCtx.session,metadata:foreignMeta}}),/retained_replay_mount_changed/);
  await writeFile(join(runRoot,'unpublished.staging'),'interrupted prefix');
  await assert.rejects(prefix.bind(nextCtx),/unlisted files/);await rm(join(runRoot,'unpublished.staging'));
  assert.equal(calls.filter(call=>call[0]==='fresh-get').length,lookups,'invalid attachment must not resume native resource');
  assert.equal((await stat(runRoot)).ino,rootInode);assert.equal(disks.get(prefixId).running,false);
  const restartedPrefix=await prefixManager(),restartedSb=prefixSandbox(),restartedCtx={...nextCtx,sandbox:restartedSb};
  await restartedPrefix.bind(restartedCtx);assert.equal(JSON.parse(await restartedSb.exec('AUDIT_READ')).workspace,'retained-workspace');await restartedPrefix.idle(restartedCtx);
  assert.equal([...disks.keys()][0],prefixId);assert.equal(disks.size,1);
  await restartedPrefix.archive({...restartedCtx,reason:'archived'});assert.equal(disks.size,0);
  console.log(JSON.stringify({status:'PASS',actual_pinned_provider:true,actual_patched_adapter:true,actual_sql_fence:true,persistent_sqlite_pointer:true,native_client:'SYNTHETIC_NO_VM',retention_scenario_allocation_count:1,failure_compensation:true,partial_idle_retry:true,initial_publication_ambiguous_commit:true,release_ambiguous_commit:true,real_child_process_exit_before_publication:true,durable_allocation_intent_recovery:true,fence_loss_orphan_receipt:true,warm_replay_prefix_revalidation:true,mutable_prefix_excluded_static_identity:true,interrupted_prefix_fail_closed:true,replay_mount_root_inode_stable:true,controller_restart_same_prefix_scope:true,native_mount_visibility:'NOT_PROVED',retained_rootfs_workspace_profile_binary:true,per_exec_secret_rotation:true,cold_archive_reaped:true,calls}));
} finally {for(const child of children){if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');}await Promise.all(children.map(child=>child.exitCode!==null||child.signalCode!==null?undefined:new Promise(resolve=>child.once('exit',resolve))));await rm(directory,{recursive:true,force:true});}
