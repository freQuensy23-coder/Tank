import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
const sourceRoot = process.argv[2];
if (!sourceRoot) throw new Error('Explicit patched source root required');
const history = await import(pathToFileURL(sourceRoot + '/packages/session-runtime-contract/src/history.ts'));
const kinds = ['session.status_running','session.status_idle','session.status_rescheduled','session.status_terminated'];
for (const type of kinds) {
  const event = {id:'native-'+type,type,processedAt:'2026-10-18T23:00:00Z',...(type.endsWith('idle') ? {stopReason:{type:'end_turn'}} : {})};
  const document = history.encodeSessionEventDocument(event,{revision:7,index:0},'accepted-input-id');
  assert.deepEqual(history.decodeSessionEventDocument(document).event,event); // internal contract unchanged
  const projected = history.decodePublicSessionEventDocument(document);
  assert.equal(projected.executionId,'accepted-input-id');
  assert.equal('source_position' in projected,false);
  assert.equal('revision' in projected,false);
  assert.equal('index' in projected,false);
  assert.deepEqual(history.decodePublicSessionEventDocument(JSON.stringify(event)),event); // legacy never fabricated
}
const opaque={id:'tool',type:'agent.custom_tool_use',name:'record_event',input:{execution_id:'OPAQUE',source_position:{revision:'OPAQUE'}},processedAt:'2026-10-18T23:00:00Z'};
const document=history.encodeSessionEventDocument(opaque,{revision:8,index:0},'execution');
assert.deepEqual(history.decodePublicSessionEventDocument(document),{...opaque,executionId:"execution"});
assert.deepEqual(history.decodePublicSessionEventDocument(document).input,opaque.input);
const bad=JSON.stringify({id:'bad',type:'session.status_idle',source_position:{revision:-1,index:0,execution_id:'untrusted'}});
assert.equal(history.decodePublicSessionEventDocument(bad).executionId,undefined);
console.log(JSON.stringify({status:'PASS',status_kinds:kinds.length,storage_metadata_private:true,legacy_not_fabricated:true,opaque_json_preserved:true}));
