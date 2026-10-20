import assert from 'node:assert/strict';
import test from 'node:test';
import { emitHarnessToolUseFromCall } from '/app/apps/agent/src/harness/default-loop.ts';
import { planSessionContinuation } from '/app/packages/session-runtime-contract/src/continuation.ts';
import { buildTools } from '/app/apps/agent/src/harness/tools.ts';

const emit = (name, tools, id = 'call') => {
  const events = [];
  emitHarnessToolUseFromCall({ broadcast: e => events.push(e) }, tools, id, name, { url: 'https://example.com/' });
  return events;
};

test('an undeclared tool remains a local error, never a client callback', () => {
  const events = emit('browser_navigate', {});
  assert.equal(events.length, 1);
  assert.equal(events[0].type, 'agent.tool_use');
  assert.equal(events[0].id, 'call');
  assert.equal(events[0].name, 'browser_navigate');
});

test('explicit custom registration retains callback ownership, including builtin names', () => {
  for (const name of ['fetch_page', 'browser']) {
    assert.equal(emit(name, { [name]: { metadata: { openmaCustom: true } } })[0].type, 'agent.custom_tool_use');
  }
});

async function browserTools() {
  return buildTools({ tools: [{
    type: 'agent_toolset_20260401',
    default_config: { enabled: false, permission_policy: { type: 'always_allow' } },
    configs: [{ name: 'browser', enabled: true, permission_policy: { type: 'always_allow' } }],
  }] }, {}, { browser: { launch: () => { throw new Error('No browser launch expected in this registration test'); } } });
}

test('every actual dynamically built browser tool stays in the native event family', async () => {
  const tools = await browserTools();
  const names = Object.keys(tools).filter(name => name.startsWith('browser_'));
  assert.equal(names.length, 7);
  for (const name of names) assert.equal(emit(name, tools)[0].type, 'agent.tool_use', name);
});

function snapshot(includeInvalidRealCall = false, actualBrowserTools = {}) {
  const start = { id: 'input', type: 'user.message', content: [{ type: 'text', text: 'Analyze source' }] };
  const result = { id: 'reply', type: 'user.custom_tool_result', customToolUseId: 'real', content: [{ type: 'text', text: 'Source content' }] };
  const unknown = emit('browser_navigate', actualBrowserTools, 'unknown')[0];
  const custom = emit('fetch_page', { fetch_page: { metadata: { openmaCustom: true } } }, 'real')[0];
  const events = [
    unknown,
    { id: 'local-error', type: 'agent.tool_result', toolUseId: 'unknown', isError: true, content: [{ type: 'text', text: 'Tool not found' }] },
    custom,
    ...(includeInvalidRealCall ? [{ ...custom, id: 'unlisted-real' }] : []),
    { id: 'idle', type: 'session.status_idle', stopReason: { type: 'requires_action', eventIds: ['real'] } },
  ];
  return {
    revision: events.length,
    orderedEvents: events.map((event, i) => ({ event, executionId: 'origin', position: { revision: i + 1, index: 0 } })),
    executions: [
      { id: 'origin', laneId: 'sthr_primary', state: 'completed', revision: 1, events: [start], attemptCount: 1, interruptRequestedAt: null },
      { id: 'callback', laneId: 'sthr_primary', state: 'queued', revision: 1, events: [result], attemptCount: 0, interruptRequestedAt: null },
    ],
  };
}

test('local unknown-tool recovery followed by a real custom call can continue', () => {
  const plan = planSessionContinuation(snapshot(), 'callback');
  assert.equal(plan.mode, 'model');
  assert.deepEqual(plan.phaseInputIds, ['reply']);
});

test('actual browser call followed by custom callbacks does not poison continuation', async () => {
  const plan = planSessionContinuation(snapshot(false, await browserTools()), 'callback');
  assert.equal(plan.mode, 'model');
  assert.deepEqual(plan.phaseInputIds, ['reply']);
});

test('missing real callback IDs still fail the strict group validator', () => {
  const plan = planSessionContinuation(snapshot(true), 'callback');
  assert.equal(plan.mode, 'failure');
  assert.match(plan.failure, /Invalid custom-tool group declaration/);
});
