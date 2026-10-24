import { test } from 'node:test';
import assert from 'node:assert/strict';
import { imageMismatchRecoveryBoundary, imageRecoveryRetryBoundary, pendingImageRecoveries, planSessionContinuation } from '../../../vendor/openma/packages/session-runtime-contract/src/continuation.ts';

function fixture() {
  const message = { id: 'input-old', type: 'user.message', content: [{ type: 'text', text: 'pending original input' }], processedAt: '2026-10-24T00:00:00Z' };
  const entries = [
    { id: 'failed-error', type: 'session.error', error: { type: 'unknown_error', message: 'retained_configuration_changed', retryStatus: 'exhausted' } },
    { id: 'failed-idle', type: 'session.status_idle', stopReason: { type: 'retries_exhausted' } },
  ].map((event, index) => ({ event, executionId: message.id, position: { revision: index + 1, index: 0 } }));
  return { revision: 2, orderedEvents: entries, executions: [{ id: message.id, laneId: 'sthr_primary', state: 'failed', revision: 1,
    events: [message], attemptCount: 1, interruptRequestedAt: null }] };
}
const recover = snapshot => ({ ...snapshot, executions: snapshot.executions.map(row => ({ ...row, state: 'running', imageRecoveryAfterEventId: 'failed-idle', imageRecoveryInterruptRequestedAt: null })) });

test('only exact image-mismatch preflight without agent effects becomes an unfinished-input recovery', () => {
  const snapshot = fixture();
  assert.equal(imageMismatchRecoveryBoundary(snapshot, 'input-old'), 'failed-idle');
  assert.equal(planSessionContinuation(snapshot, 'input-old').mode, 'blocked');
  const plan = planSessionContinuation(recover(snapshot), 'input-old');
  assert.equal(plan.mode, 'model');
  assert.deepEqual(plan.phaseInputIds, ['input-old']);
  assert.deepEqual(plan.historyEvents.map(event => event.id), ['input-old']);
  assert.deepEqual(snapshot.orderedEvents.map(entry => entry.event.id), ['failed-error', 'failed-idle']);
});

test('an input already committed by another execution cannot be revived from its old failed row', () => {
  const snapshot = fixture();
  snapshot.executions.push({ ...snapshot.executions[0], id: 'completed-copy', state: 'completed' });
  snapshot.orderedEvents.push({ event: { id: 'committed-copy', type: 'session.status_idle', stopReason: { type: 'end_turn' } },
    executionId: 'completed-copy', position: { revision: 3, index: 0 } });
  assert.equal(imageMismatchRecoveryBoundary(snapshot, 'input-old'), null);
});

for (const state of ['completed', 'cancelled', 'queued', 'running']) test(`image recovery never reopens ${state} input`, () => {
  const snapshot = fixture(); snapshot.executions[0].state = state;
  assert.equal(imageMismatchRecoveryBoundary(snapshot, 'input-old'), null);
});

test('unrelated failures, any tool/model output and interrupted inputs cannot enter recovery', () => {
  const unrelated = fixture(); unrelated.orderedEvents[0].event.error.message = 'provider unavailable';
  assert.equal(imageMismatchRecoveryBoundary(unrelated, 'input-old'), null);
  const interrupted = fixture(); interrupted.executions[0].interruptRequestedAt = '2026-10-24T00:01:00Z';
  assert.equal(imageMismatchRecoveryBoundary(interrupted, 'input-old'), null);
  for (const type of ['agent.message', 'agent.tool_use', 'agent.tool_result', 'span.model_request_start']) {
    const snapshot = fixture(); snapshot.orderedEvents.push({ event: { id: 'effect', type }, executionId: 'input-old', position: { revision: 3, index: 0 } });
    assert.equal(imageMismatchRecoveryBoundary(snapshot, 'input-old'), null);
  }
  const retired = fixture(); retired.orderedEvents.push({ event: { id: 'interrupt', type: 'user.interrupt' }, position: { revision: 3, index: 0 } });
  assert.equal(imageMismatchRecoveryBoundary(retired, 'input-old'), null);
});

test('restart uses the durable recovery boundary but refuses forged effectful or missing boundaries', () => {
  const snapshot = recover(fixture());
  snapshot.orderedEvents[0].event = { id: 'effect', type: 'agent.tool_use' };
  assert.equal(planSessionContinuation(snapshot, 'input-old').mode, 'failure');
  const missing = recover(fixture()); missing.executions[0].imageRecoveryAfterEventId = 'missing';
  assert.equal(planSessionContinuation(missing, 'input-old').mode, 'failure');
});

test('a later input remains blocked behind the recovered original execution', () => {
  const snapshot = recover(fixture());
  snapshot.executions.push({ id: 'later', laneId: 'sthr_primary', state: 'queued', revision: 1, attemptCount: 0, interruptRequestedAt: null,
    events: [{ id: 'later', type: 'user.message', content: [{ type: 'text', text: 'later input' }] }] });
  assert.equal(planSessionContinuation(snapshot, 'later').mode, 'blocked');
  assert.equal(planSessionContinuation(snapshot, 'input-old').mode, 'model');
});

test('successful committed output after recovery finalizes instead of re-running its model or tools', () => {
  const snapshot = recover(fixture());
  snapshot.orderedEvents.push({ event: { id: 'done', type: 'session.status_idle', stopReason: { type: 'end_turn' } }, executionId: 'input-old', position: { revision: 3, index: 0 } });
  const plan = planSessionContinuation(snapshot, 'input-old');
  assert.equal(plan.mode, 'finalize');
  assert.deepEqual(plan.recoveredStopReason, { type: 'end_turn' });
});

test('interrupting a recovered execution preserves its historical proof and never poisons the next input', () => {
  const snapshot = recover(fixture());
  snapshot.executions[0].interruptRequestedAt = '2026-10-24T00:02:00Z';
  snapshot.orderedEvents.push({ event: { id: 'later-interrupt', type: 'user.interrupt' }, position: { revision: 3, index: 0 } });
  assert.deepEqual(planSessionContinuation(snapshot, 'input-old'), { mode: 'blocked', reason: 'interrupted_or_terminal_failure' });
  snapshot.executions[0].state = 'cancelled';
  snapshot.executions.push({ id: 'fresh', laneId: 'sthr_primary', state: 'running', revision: 1, attemptCount: 1, interruptRequestedAt: null,
    events: [{ id: 'fresh', type: 'user.message', content: [{ type: 'text', text: 'fresh valid input' }] }] });
  const plan = planSessionContinuation(snapshot, 'fresh');
  assert.equal(plan.mode, 'model');
  assert.deepEqual(plan.phaseInputIds, ['fresh']);
  assert.deepEqual(plan.historyEvents.map(event => event.id), ['later-interrupt', 'fresh']);
  assert.equal(snapshot.executions[0].imageRecoveryInterruptRequestedAt, null);
});

test('temporary no-effects upgrade failures have durable retry boundaries without suppressing later normal output', () => {
  const snapshot = recover(fixture());
  snapshot.orderedEvents.push(
    { event: { id: 'temporary', type: 'session.error', error: { message: 'retained_legacy_provenance_missing:opaque_fingerprint_mismatch;historical_image_and_configuration_required' } }, executionId: 'input-old', position: { revision: 3, index: 0 } },
    { event: { id: 'temporary-idle', type: 'session.status_idle', stopReason: { type: 'retries_exhausted' } }, executionId: 'input-old', position: { revision: 4, index: 0 } },
  );
  assert.equal(imageRecoveryRetryBoundary(snapshot, 'input-old'), 'temporary-idle');
  snapshot.executions[0].imageRecoveryRetryAfterEventId = 'temporary-idle';
  const restarted = structuredClone(snapshot);
  assert.equal(planSessionContinuation(restarted, 'input-old').mode, 'model');
  restarted.orderedEvents.push({ event: { id: 'completed-after-retry', type: 'session.status_idle', stopReason: { type: 'end_turn' } }, executionId: 'input-old', position: { revision: 5, index: 0 } });
  assert.equal(imageRecoveryRetryBoundary(restarted, 'input-old'), null);
  assert.equal(planSessionContinuation(restarted, 'input-old').mode, 'finalize');
});

for (const sessionThreadId of [undefined, 'sthr_primary', 'sthr_other']) test(`journal interrupt ${sessionThreadId ?? 'session-wide'} between recovery refund and reclaim cannot revive cancelled input`, () => {
  const snapshot = recover(fixture());
  snapshot.executions[0].state = 'failed';
  snapshot.executions[0].imageRecoveryRetryAfterEventId = 'retry-idle';
  snapshot.orderedEvents.push(
    { event: { id: 'retry-error', type: 'session.error', error: { message: 'retained_upgrade_transfer_failed:temporary' } }, executionId: 'input-old', position: { revision: 3, index: 0 } },
    { event: { id: 'retry-idle', type: 'session.status_idle', stopReason: { type: 'retries_exhausted' } }, executionId: 'input-old', position: { revision: 4, index: 0 } },
    { event: { id: 'between-attempts-interrupt', type: 'user.interrupt', ...(sessionThreadId === undefined ? {} : { sessionThreadId }) }, position: { revision: 5, index: 0 } },
  );
  const restarted = structuredClone(snapshot);
  const pending = pendingImageRecoveries(restarted, ['input-old']);
  assert.equal(pending.executions[0].interruptRequestedAt, null);
  assert.equal(pending.executions[0].state, sessionThreadId === 'sthr_other' ? 'queued' : 'failed');
  assert.equal(planSessionContinuation(pending, 'input-old').mode, sessionThreadId === 'sthr_other' ? 'model' : 'blocked');
  pending.executions.push({ id: 'fresh', laneId: 'sthr_primary', state: 'running', revision: 1, attemptCount: 1, interruptRequestedAt: null,
    events: [{ id: 'fresh', type: 'user.message', content: [{ type: 'text', text: 'fresh after cancellation' }] }] });
  const plan = planSessionContinuation(pending, 'fresh');
  assert.equal(plan.mode, sessionThreadId === 'sthr_other' ? 'blocked' : 'model');
  if (plan.mode === 'model') assert.deepEqual(plan.phaseInputIds, ['fresh']);
  assert.equal(snapshot.executions[0].state, 'failed');
});

test('missing retained memory binding remains a retryable no-effects preflight refusal without hiding the diagnostic', () => {
  const snapshot = recover(fixture());
  snapshot.orderedEvents.push(
    { event: { id: 'missing-memory', type: 'session.error', error: { message: 'retained_memory_binding_unproven', retryStatus: 'exhausted' } }, executionId: 'input-old', position: { revision: 3, index: 0 } },
    { event: { id: 'missing-memory-idle', type: 'session.status_idle', stopReason: { type: 'retries_exhausted' } }, executionId: 'input-old', position: { revision: 4, index: 0 } },
  );
  assert.equal(imageRecoveryRetryBoundary(snapshot, 'input-old'), 'missing-memory-idle');
  assert.equal(planSessionContinuation(snapshot, 'input-old').mode, 'failure');
  snapshot.executions[0].imageRecoveryRetryAfterEventId = 'missing-memory-idle';
  assert.equal(planSessionContinuation(snapshot, 'input-old').mode, 'model');
});

for (const message of ['unrelated failure', 'permission denied']) test(`recovery does not refund or hide ${message}`, () => {
  const snapshot = recover(fixture());
  snapshot.orderedEvents.push({ event: { id: 'unrelated', type: 'session.error', error: { message } }, executionId: 'input-old', position: { revision: 3, index: 0 } });
  assert.equal(imageRecoveryRetryBoundary(snapshot, 'input-old'), null);
  snapshot.executions[0].imageRecoveryRetryAfterEventId = 'unrelated';
  assert.equal(planSessionContinuation(snapshot, 'input-old').mode, 'failure');
});

test('a retry boundary containing model/tool effects or a pre-recovery interrupt cannot become permanent proof', () => {
  const snapshot = recover(fixture());
  snapshot.orderedEvents.push({ event: { id: 'effect', type: 'span.model_request_start' }, executionId: 'input-old', position: { revision: 3, index: 0 } });
  assert.equal(imageRecoveryRetryBoundary(snapshot, 'input-old'), null);
  snapshot.executions[0].imageRecoveryRetryAfterEventId = 'effect';
  assert.equal(planSessionContinuation(snapshot, 'input-old').mode, 'failure');
  const interrupted = recover(fixture()); interrupted.executions[0].imageRecoveryInterruptRequestedAt = '2026-10-24T00:00:00Z';
  assert.equal(planSessionContinuation(interrupted, 'input-old').mode, 'failure');
});

test('owner death before model execution retains a retryable boundary but never converts model effects to preflight', () => {
  const snapshot = recover(fixture());
  assert.equal(imageRecoveryRetryBoundary(snapshot, 'input-old', true), 'failed-idle');
  snapshot.orderedEvents.push({ event: { id: 'preflight-running', type: 'session.status_running' }, executionId: 'input-old', position: { revision: 3, index: 0 } });
  assert.equal(imageRecoveryRetryBoundary(snapshot, 'input-old', true), 'preflight-running');
  snapshot.executions[0].imageRecoveryRetryAfterEventId = 'preflight-running';
  assert.equal(planSessionContinuation(structuredClone(snapshot), 'input-old').mode, 'model');
  snapshot.orderedEvents.push({ event: { id: 'model-started', type: 'span.model_request_start' }, executionId: 'input-old', position: { revision: 4, index: 0 } });
  assert.equal(imageRecoveryRetryBoundary(snapshot, 'input-old', true), null);
});

for (const started of [false, true]) test(`refunded no-effects owner death remains pending after reopen (started=${started})`, () => {
  const snapshot = recover(fixture());
  snapshot.executions[0].state = 'failed';
  if (started) {
    snapshot.orderedEvents.push({ event: { id: 'owner-running', type: 'session.status_running' }, executionId: 'input-old', position: { revision: 3, index: 0 } });
    snapshot.executions[0].imageRecoveryRetryAfterEventId = 'owner-running';
  }
  const pending = pendingImageRecoveries(structuredClone(snapshot), ['input-old']);
  assert.equal(pending.executions[0].state, 'queued');
  assert.equal(pending.executions[0].imageRecoveryRetryAfterEventId, started ? 'owner-running' : undefined);
  assert.equal(planSessionContinuation(pending, 'input-old').mode, 'model');
  assert.deepEqual(planSessionContinuation(pending, 'input-old').phaseInputIds, ['input-old']);
});

function siblingFixture() {
  const snapshot = { revision: 1, executions: [
    { id: 'declare', state: 'completed', events: [{ id: 'question', type: 'user.message', content: [] }] },
    ...['X', 'Y'].map(call => ({ id: 'R' + call, state: 'failed', events: [{ id: 'result-' + call, type: 'user.custom_tool_result', customToolUseId: call,
      content: [{ type: 'text', text: 'completed external effect ' + call }] }] })),
  ].map(row => ({ ...row, laneId: 'sthr_primary', revision: 1, attemptCount: 1, interruptRequestedAt: null })), orderedEvents: [] };
  const append = (executionId, event) => snapshot.orderedEvents.push({ event, executionId, position: { revision: snapshot.orderedEvents.length + 1, index: 0 } });
  for (const id of ['X', 'Y']) append('declare', { id, type: 'agent.custom_tool_use', name: 'external-side-effect', input: {} });
  append('declare', { id: 'declaration', type: 'session.status_idle', stopReason: { type: 'requires_action', eventIds: ['X', 'Y'] } });
  for (const id of ['RX', 'RY']) {
    append(id, { id: id + '-error', type: 'session.error', error: { message: 'retained_configuration_changed', retryStatus: 'exhausted' } });
    append(id, { id: id + '-idle', type: 'session.status_idle', stopReason: { type: 'retries_exhausted' } });
  }
  return snapshot;
}

test('independently proven failed sibling callbacks remain pending and each enters committed intake before one continuation', () => {
  const original = siblingFixture();
  const one = pendingImageRecoveries(original, ['RX']);
  assert.equal(planSessionContinuation(one, 'RX').mode, 'noop');
  const snapshot = pendingImageRecoveries(original, ['RX', 'RY']);
  assert.deepEqual(snapshot.executions.map(row => row.state), ['completed', 'queued', 'queued']);
  assert.deepEqual(snapshot.executions.slice(1).map(row => row.imageRecoveryAfterEventId), ['RX-idle', 'RY-idle']);
  const first = planSessionContinuation(snapshot, 'RX');
  assert.equal(first.mode, 'intake');
  assert.deepEqual(first.phaseInputIds, ['result-X']);
  assert.deepEqual(first.remaining, ['Y']);
  snapshot.executions[1].state = 'completed';
  snapshot.orderedEvents.push({ executionId: 'RX', event: { id: 'RX-intake', type: 'session.status_idle', stopReason: { type: 'requires_action', eventIds: ['Y'] } },
    position: { revision: 8, index: 0 } });
  const restarted = structuredClone(snapshot);
  const second = planSessionContinuation(restarted, 'RY');
  assert.equal(second.mode, 'model');
  assert.deepEqual(second.phaseInputIds, ['result-Y']);
  assert.deepEqual(second.remaining, []);
  assert.deepEqual(second.historyEvents.filter(event => event.type === 'user.custom_tool_result').map(event => event.id), ['result-X', 'result-Y']);
  assert.deepEqual(second.historyEvents.filter(event => event.type === 'agent.custom_tool_use').map(event => event.id), ['X', 'Y']);
  restarted.executions[2].state = 'running';
  restarted.orderedEvents.push({ executionId: 'RY', event: { id: 'RY-done', type: 'session.status_idle', stopReason: { type: 'end_turn' } }, position: { revision: 9, index: 0 } });
  assert.equal(planSessionContinuation(restarted, 'RY').mode, 'finalize');
  assert.deepEqual(original.executions.map(row => row.state), ['completed', 'failed', 'failed']);
});

for (const reason of ['unrelated', 'effects', 'interrupt', 'retired']) test(`sibling dependency with ${reason} is never classified as recoverable pending work`, () => {
  const snapshot = siblingFixture();
  if (reason === 'unrelated') snapshot.orderedEvents.find(entry => entry.event.id === 'RY-error').event.error.message = 'permission denied';
  if (reason === 'effects') snapshot.orderedEvents.push({ executionId: 'RY', event: { id: 'effect', type: 'agent.tool_use' }, position: { revision: 8, index: 0 } });
  if (reason === 'interrupt') snapshot.executions[2].interruptRequestedAt = '2026-10-24T00:00:00Z';
  const eligible = reason === 'retired' ? ['RX'] : ['RX', 'RY'];
  const pending = pendingImageRecoveries(snapshot, eligible);
  assert.equal(pending.executions[2].state, 'failed');
  assert.equal(pending.executions[2].imageRecoveryAfterEventId, undefined);
});



