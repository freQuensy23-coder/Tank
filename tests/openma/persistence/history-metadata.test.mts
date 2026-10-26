import assert from 'node:assert/strict';
import test from 'node:test';
import { decodePublicSessionEventDocument, decodeSessionEventDocument, encodeSessionEventDocument, type SessionEventExecutionMetadata } from '../../../vendor/openma/packages/session-runtime-contract/src/history.ts';
import { decodeContinuationHistorySourceDocument, encodeContinuationSessionEventDocument, encodeDefaultRuntimeProjectionDocument, encodeTankContinuationProjectionDocument } from '../../../vendor/openma/packages/session-runtime-contract/src/continuation.ts';
import type { SessionExecutionFence } from '../../../vendor/openma/packages/session-runtime-contract/src/coordination.ts';
import type { SessionEventView } from '../../../vendor/openma/packages/managed-agents-domain/src/sessions/index.ts';

const event: SessionEventView = { id: 'event', type: 'session.status_running', processedAt: '2026-10-26T00:00:00Z' };
const metadata: SessionEventExecutionMetadata = {
  attemptId: 'attempt', generation: 2,
  progress: { version: 1, previousReceiptId: null, consumedInputIds: ['first'], remainingInputIds: [],
    phaseInputIds: ['first'], phaseStartEventId: 'start', terminalEventId: 'terminal',
    laneId: 'lane', attemptId: 'attempt', generation: 2 },
};
const fence: SessionExecutionFence = {
  workspaceId: 'scope', sessionId: 'session', executionId: 'execution', attemptId: 'attempt',
  ownerId: 'worker', generation: 2, expiresAt: '2026-10-26T00:00:10Z',
};
test('generic projection retains the original public shape with genuine optional fence absence', () => {
  const position = {revision: 3, index: 0};
  assert.deepEqual(decodeSessionEventDocument(encodeDefaultRuntimeProjectionDocument(event, position, fence)), {
    event, position, executionId: fence.executionId,
  });
  assert.deepEqual(decodeSessionEventDocument(encodeDefaultRuntimeProjectionDocument(event, position, undefined)), {
    event, position,
  });
});
test('named Tank projection encodes the actual fence attempt and generation', () => {
  const position = {revision: 3, index: 0};
  assert.deepEqual(decodeSessionEventDocument(encodeTankContinuationProjectionDocument(event, position, fence)), {
    event, position, executionId: fence.executionId,
    executionMetadata: {attemptId: fence.attemptId, generation: fence.generation},
  });
});
test('Tank projection refuses an absent or malformed fence without guessing receipt facts', () => {
  assert.throws(() => encodeTankContinuationProjectionDocument(event, {revision: 3, index: 0}, undefined), /requires an execution fence/);
  assert.throws(() => encodeTankContinuationProjectionDocument(event, {revision: 3, index: 0}, {...fence, generation: 0}), /Invalid persisted continuation history source/);
});
test('history private metadata preserves genuine absence and actual declared fields', () => {
  assert.deepEqual(decodeSessionEventDocument(JSON.stringify(event)), { event });
  const document = encodeSessionEventDocument(event, { revision: 3, index: 0 }, 'execution', metadata);
  const decoded = decodeSessionEventDocument(document);
  assert.deepEqual(decoded, { event, position: { revision: 3, index: 0 }, executionId: 'execution', executionMetadata: metadata });
  assert.deepEqual(decodePublicSessionEventDocument(document), { ...event, executionId: 'execution' });
  assert.equal(Object.hasOwn(decoded.event, 'source_position'), false);
  assert.equal(Object.hasOwn(decodeContinuationHistorySourceDocument(JSON.stringify({...event, source_position: {revision: 1, index: 0}})), 'executionMetadata'), false);
});
for (const source of [null, {}, {revision: 0, index: 0}, {revision: true, index: 0}, {revision: 1, index: -1},
  {revision: Number.MAX_SAFE_INTEGER + 1, index: 0}, {revision: 1, index: 0, execution_id: null},
  {revision: 1, index: 0, execution_id: 'execution', execution_metadata: null},
  {revision: 1, index: 0, execution_id: 'execution', execution_metadata: {attemptId: 'secret-marker', generation: false}},
  {revision: 1, index: 0, execution_id: 'execution', execution_metadata: {...metadata, progress: {...metadata.progress, version: true}}},
  {revision: 1, index: 0, execution_id: 'execution', execution_metadata: {...metadata, progress: {...metadata.progress, previousReceiptId: undefined}}},
  {revision: 1, index: 0, execution_id: 'execution', execution_metadata: {...metadata, progress: {...metadata.progress, generation: 3}}},
  {revision: 1, index: 0, execution_id: 'execution', execution_metadata: {...metadata, progress: {...metadata.progress, attemptId: 'other'}}},
]) {
  test(`malformed reported history metadata refuses ${JSON.stringify(source)}`, () => {
    assert.throws(() => decodeContinuationHistorySourceDocument(JSON.stringify({...event, source_position: source})), error => {
      assert.ok(error instanceof Error);
      assert.equal(error.message, 'Invalid persisted continuation history source');
      return true;
    });
  });
}
test('history writer refuses malformed copied metadata before serializing an event', () => {
  const invalid = {...metadata, generation: Number.NaN};
  assert.throws(() => encodeContinuationSessionEventDocument(event, {revision: 1, index: 0}, 'execution', invalid), /Invalid persisted continuation history source/);
  assert.deepEqual(event, {id: 'event', type: 'session.status_running', processedAt: '2026-10-26T00:00:00Z'});
});
test('private metadata uses duplicate-refusing JSON before projection', () => {
  assert.throws(() => decodeContinuationHistorySourceDocument('{"id":"event","type":"session.status_running","source_position":{"revision":1,"index":0,"index":1}}'), /Invalid persisted continuation input/);
});

test('protected public legacy behavior stays unchanged while Tank refuses reported malformed position', () => {
  const document = JSON.stringify({...event, source_position: {revision: -1, index: 0}});
  assert.deepEqual(decodeSessionEventDocument(document), {event});
  assert.throws(() => decodeContinuationHistorySourceDocument(document), /Invalid persisted continuation history source/);
  assert.deepEqual(decodeContinuationHistorySourceDocument(JSON.stringify(event)), {});
  const documentWithMetadata = encodeContinuationSessionEventDocument(event, {revision: 3, index: 0}, 'execution', metadata);
  assert.equal(documentWithMetadata, encodeSessionEventDocument(event, {revision: 3, index: 0}, 'execution', metadata));
});

test('reported execution metadata cannot lack its actual execution identity', () => {
  assert.throws(() => decodeContinuationHistorySourceDocument(JSON.stringify({...event, source_position: {revision: 1, index: 0, execution_metadata: metadata}})), /Invalid persisted continuation history source/);
});
test('raw nonzero source position cannot underflow into a fabricated known zero', () => {
  assert.throws(() => decodeContinuationHistorySourceDocument('{"id":"event","type":"session.status_running","source_position":{"revision":1,"index":1e-400}}'), /Invalid persisted continuation input/);
});
