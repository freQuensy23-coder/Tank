import { test } from 'node:test';
import assert from 'node:assert/strict';
import { imageMismatchRecoveryBoundary, imageRecoveryRetryBoundary, planSessionContinuation } from '../../../vendor/openma/packages/session-runtime-contract/src/continuation.ts';

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

