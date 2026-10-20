import { test } from 'node:test';
import assert from 'node:assert/strict';
import { validateReplayAttachment } from './replay-attachment.ts';
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
