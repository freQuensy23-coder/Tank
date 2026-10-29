import { test } from 'node:test';
import assert from 'node:assert/strict';
import { toSessionEventResponse } from '../../../vendor/openma/packages/managed-agents-api/src/mappers/session-events.ts';

const span = extra => ({
  id: 'sevt_end',
  type: 'span.model_request_end',
  isError: false,
  modelRequestStartId: 'sevt_start',
  modelUsage: { cacheCreationInputTokens: 0, cacheReadInputTokens: 20736, inputTokens: 16871, outputTokens: 147 },
  processedAt: '2026-10-29T14:00:33.014Z',
  ...extra,
});

test('model request end keeps the stored provider finish reason on the wire', () => {
  for (const reason of ['toolUse', 'stop', 'length', 'error']) {
    assert.equal(toSessionEventResponse(span({ finishReason: reason })).finish_reason, reason);
  }
});

test('model request end omits an absent or empty finish reason', () => {
  assert.equal('finish_reason' in toSessionEventResponse(span({})), false);
  assert.equal('finish_reason' in toSessionEventResponse(span({ finishReason: '' })), false);
  assert.deepEqual(toSessionEventResponse(span({})).model_usage, {
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 20736,
    input_tokens: 16871,
    output_tokens: 147,
  });
});
