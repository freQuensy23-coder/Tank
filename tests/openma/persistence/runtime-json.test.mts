import assert from 'node:assert/strict';
import test from 'node:test';
import { decodeRuntimeJson, RuntimeJsonDocumentError } from '../../../vendor/openma/packages/session-runtime-contract/src/json.ts';

for (const document of [
  '{"private":"secret-marker","private":{}}',
  '{"nested":{"a":1,"\\u0061":2}}',
  '[{"x":1,"x":2}]',
  '{"finite":1e400}',
  '{"not_zero":1e-400}',
  '{"not_zero":-1e-400}',
  '{"value":NaN}',
  '{"unterminated":"secret-marker}',
]) {
  test(`runtime JSON refuses ambiguous or malformed case ${document.length}:${document.indexOf('nested')}`, () => {
    assert.throws(() => decodeRuntimeJson(document), error => {
      assert.ok(error instanceof RuntimeJsonDocumentError);
      assert.equal(error.message, 'Invalid session runtime JSON document');
      assert.ok(!error.message.includes('secret-marker'));
      return true;
    });
  });
}
test('runtime JSON retains independent object keys and actual JSON scalar types', () => {
  assert.deepEqual(decodeRuntimeJson('{"a":0,"b":false,"c":null,"nested":[{"a":"x"},{"a":1.25}]}'), {
    a: 0, b: false, c: null, nested: [{a: 'x'}, {a: 1.25}],
  });
});

test('runtime JSON preserves representable subnormals and semantic integral protocol numbers', () => {
  assert.equal(decodeRuntimeJson('1.0'), 1);
  assert.equal(decodeRuntimeJson('5e-324'), Number.MIN_VALUE);
  assert.equal(Object.is(decodeRuntimeJson('-0'), -0), true);
  assert.equal(decodeRuntimeJson('0e-400'), 0);
});
