import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ContinuationProtocolError, decodeContinuationExecutionEvents, decodeContinuationImageRecovery } from '../../../vendor/openma/packages/session-runtime-contract/src/continuation.ts';

test('persisted continuation decoder preserves every admitted event phase and optional absence', (): void => {
  const events = [
    { id: 'message', type: 'user.message', content: [{ type: 'text', text: '' }, { type: 'redacted' }, { type: 'image', source: { type: 'url', url: 'https://fixture.invalid/image' } }, { type: 'document', source: { type: 'text', data: 'source', mediaType: 'text/plain' }, title: null }] },
    { id: 'system', type: 'system.message', content: [{ type: 'text', text: 'system' }], processedAt: null },
    { id: 'interrupt', type: 'user.interrupt', sessionThreadId: null },
    { id: 'confirmation', type: 'user.tool_confirmation', result: 'deny', toolUseId: 'tool', denyMessage: '' },
    { id: 'custom', type: 'user.custom_tool_result', customToolUseId: 'call' },
    { id: 'local', type: 'user.tool_result', toolUseId: 'local-tool', isError: false, content: [{ type: 'search_result', source: '', title: '', citations: { enabled: false }, content: [{ type: 'text', text: '' }] }] },
    { id: 'outcome', type: 'user.define_outcome', description: '', rubric: { type: 'file', fileId: 'rubric' }, maxIterations: null, outcomeId: 'outcome-id', processedAt: '2026-10-26T00:00:00Z' },
  ];
  const document = JSON.stringify(events);
  const decoded = decodeContinuationExecutionEvents(document);
  assert.deepEqual(decoded, events);
  assert.equal(Object.hasOwn(decoded[4], 'content'), false);
  assert.equal(Object.hasOwn(decoded[4], 'isError'), false);
  assert.equal(JSON.stringify(events), document);
});

for (const document of [
  'null', '{}', '[]', '[null]', '[{"id":"a","type":"user.interrupt","id":"b"}]',
  '[{"id":"a","type":"user.interrupt","\\u0069d":"b"}]',
  '[{"id":"a","type":"user.tool_result","toolUseId":"call","isError":false,"isError":true}]',
  '[{"id":"a","type":"user.message","content":[{"type":"text","text":"first","text":"second"}]}]',
  '[{"id":"a","type":"user.custom_tool_result","customToolUseId":"call","isError":1}]',
  '[{"id":"a","type":"user.interrupt","secret":"FAKE_SECRET_VALUE"}]',
  '[{"id":"a","type":"user.interrupt"},{"id":"a","type":"user.interrupt"}]',
  '[{"id":"a","type":"user.custom_tool_result","customToolUseId":"call","content":null}]',
  '[{"id":"a","type":"user.message","content":[{"type":"text"}]}]',
  '[{"id":"a","type":"user.tool_result","toolUseId":"call","content":[{"type":"redacted"}]}]',
  '[{"id":"a","type":"user.define_outcome","description":"","rubric":{"type":"text","content":""},"maxIterations":1e99,"outcomeId":"o","processedAt":"now"}]',
]) test('invalid persisted continuation input is refused with a static diagnostic: ' + document.slice(0, 25), (): void => {
  assert.throws((): void => { decodeContinuationExecutionEvents(document); }, (error: unknown): boolean => {
    assert.ok(error instanceof ContinuationProtocolError);
    assert.equal(error.message, 'Invalid persisted continuation input');
    assert.equal(error.message.includes('FAKE_SECRET_VALUE'), false);
    return true;
  });
});

test('image recovery keeps its explicit structured receipt and original bare-boundary format', (): void => {
  assert.deepEqual(decodeContinuationImageRecovery('terminal-old'), { afterEventId: 'terminal-old', interruptRequestedAt: null });
  assert.deepEqual(decodeContinuationImageRecovery('{"afterEventId":"terminal","interruptRequestedAt":null,"retryAfterEventId":"retry"}'),
    { afterEventId: 'terminal', interruptRequestedAt: null, retryAfterEventId: 'retry' });
  for (const document of ['{"afterEventId":"terminal"}', '{"afterEventId":"terminal","interruptRequestedAt":null,"retryAfterEventId":null}',
    '{"afterEventId":"first","afterEventId":"second","interruptRequestedAt":null}', '{"afterEventId":"terminal","interruptRequestedAt":null,"extra":"FAKE_SECRET_VALUE"}']) {
    assert.throws((): void => { decodeContinuationImageRecovery(document); }, ContinuationProtocolError);
  }
});
