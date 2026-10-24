import assert from 'node:assert/strict';

export async function settledNativeHistory({ completed, events, validate }) {
  if (!completed()) return null;
  const history = await events();
  if (!validate(history)) throw Object.assign(Error('Durably settled native inputs have incomplete guest receipts'), { fatal: true });
  return history;
}

export function completedInputExecutions(rows, ids, sessionId) {
  assert.ok(Array.isArray(rows));
  assert.ok(Array.isArray(ids) && ids.length > 0 && ids.every(id => typeof id === 'string' && id.length > 0));
  assert.equal(new Set(ids).size, ids.length, 'duplicate expected native input ID');
  const selected = rows.filter(row => ids.includes(row.id));
  assert.equal(new Set(selected.map(row => row.id)).size, selected.length, 'duplicate durable native input');
  for (const row of selected) {
    assert.equal(row.session_id, sessionId, 'native input belongs to another session');
    if (['failed', 'cancelled'].includes(row.state)) throw Object.assign(Error('Native input terminal failure: ' + JSON.stringify(row)), { fatal: true });
    assert.ok(['queued', 'running', 'completed'].includes(row.state), 'unknown durable native input state');
    if (row.state === 'completed') {
      assert.ok(Number.isSafeInteger(row.settled_at_ms) && row.settled_at_ms > 0, 'completed native input has no durable settlement');
      assert.equal(row.failure, null, 'completed native input retains failure');
    }
  }
  return selected.length === ids.length && selected.every(row => row.state === 'completed');
}
