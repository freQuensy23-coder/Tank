import { test } from 'node:test';
import assert from 'node:assert/strict';
import { imageMismatchRecoveryBoundary, planSessionContinuation } from '../../../vendor/openma/packages/session-runtime-contract/src/continuation.ts';

function fixture() {
  const message = { id: 'input-old', type: 'user.message', content: [{ type: 'text', text: 'pending original input' }], processedAt: '2026-10-24T00:00:00Z' };
  const entries = [
    { id: 'failed-error', type: 'session.error', error: { type: 'unknown_error', message: 'retained_configuration_changed', retryStatus: 'exhausted' } },
    { id: 'failed-idle', type: 'session.status_idle', stopReason: { type: 'retries_exhausted' } },
  ].map((event, index) => ({ event, executionId: message.id, position: { revision: index + 1, index: 0 } }));
  return { revision: 2, orderedEvents: entries, executions: [{ id: message.id, laneId: 'sthr_primary', state: 'failed', revision: 1,
    events: [message], attemptCount: 1, interruptRequestedAt: null }] };
}
const recover = snapshot => ({ ...snapshot, executions: snapshot.executions.map(row => ({ ...row, state: 'running', imageRecoveryAfterEventId: 'failed-idle' })) });

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
