import { test } from 'node:test';
import assert from 'node:assert/strict';
import { planSessionContinuation } from '../../../vendor/openma/packages/session-runtime-contract/src/continuation.ts';

const text = value => [{ type: 'text', text: value }];
const message = id => ({ id, type: 'user.message', content: text('Record the event') });
const custom = (id, precision = 'HOUR') => ({ id, type: 'agent.custom_tool_use', name: 'record_event', input: { precision } });
const localResult = (id, toolUseId) => ({ id, type: 'agent.tool_result', toolUseId, isError: true, content: text('Invalid precision MINUTE; expected HOUR') });
const callback = (id, customToolUseId) => ({ id, type: 'user.custom_tool_result', customToolUseId, content: text('recorded'), isError: false });
const requires = eventIds => ({ type: 'requires_action', eventIds });
const idle = (id, stopReason) => ({ id, type: 'session.status_idle', stopReason });

function span(id, calls, results = []) {
  return [
    { id, type: 'span.model_request_start' },
    ...calls,
    { id: `${id}-end`, type: 'span.model_request_end', modelRequestStartId: id, isError: false,
      modelUsage: { inputTokens: 10, outputTokens: 5, cacheReadInputTokens: 0, cacheCreationInputTokens: 0 } },
    ...results,
  ];
}

function append(snapshot, executionId, event) {
  const entry = { event: { processedAt: '2026-10-27T00:00:00Z', ...event }, position: { revision: ++snapshot.revision, index: 0 } };
  if (executionId !== undefined) {
    entry.executionId = executionId;
    entry.executionMetadata = { attemptId: `${executionId}-attempt-1`, generation: 1 };
  }
  snapshot.orderedEvents.push(entry);
  return entry.event;
}

function admit(snapshot, id, input, state = 'running') {
  const event = append(snapshot, undefined, input);
  snapshot.executions.push({ id, laneId: 'sthr_primary', state, revision: 1, events: [event], attemptCount: 1, interruptRequestedAt: null });
}

function journal(output, state = 'completed') {
  const snapshot = { revision: 0, orderedEvents: [], executions: [] };
  admit(snapshot, 'original', message('prompt'), state);
  append(snapshot, 'original', { id: 'running', type: 'session.status_running' });
  for (const event of output) append(snapshot, 'original', event);
  return snapshot;
}

function rejectedThenCorrected() {
  return [
    ...span('invalid-model', [custom('invalid', 'MINUTE')], [localResult('invalid-result', 'invalid')]),
    ...span('corrected-model', [custom('corrected')]),
  ];
}

function waiting() {
  return journal([...rejectedThenCorrected(), idle('declaration', requires(['corrected']))]);
}

function assertFailure(snapshot, executionId, reason) {
  const plan = planSessionContinuation(snapshot, executionId);
  assert.equal(plan.mode, 'failure');
  assert.match(plan.failure, reason);
  assert.deepEqual(plan.historyEvents, []);
}

test('a locally rejected custom call does not poison the corrected callback or its retained history', () => {
  const snapshot = waiting();
  admit(snapshot, 'reply', callback('reply', 'corrected'));
  const plan = planSessionContinuation(snapshot, 'reply');
  assert.equal(plan.mode, 'model');
  assert.deepEqual(plan.phaseInputIds, ['reply']);
  assert.deepEqual(plan.consumedInputIds, ['reply']);
  assert.deepEqual(plan.remaining, []);
  assert.deepEqual(plan.historyEvents.filter(event => event.type.startsWith('agent.')).map(event => event.id),
    ['invalid', 'invalid-result', 'corrected']);
  assert.deepEqual(plan.historyEvents.find(event => event.id === 'invalid-result'), {
    processedAt: '2026-10-27T00:00:00Z', ...localResult('invalid-result', 'invalid'),
  });
  assert.deepEqual(plan.historyEvents.filter(event => event.type === 'user.custom_tool_result').map(event => event.customToolUseId), ['corrected']);
});

test('end_turn with all custom calls locally resolved remains usable history for the next input', () => {
  const snapshot = journal([
    ...span('invalid-model', [custom('invalid', 'MINUTE')], [localResult('invalid-result', 'invalid')]),
    ...span('answer-model', [{ id: 'answer', type: 'agent.message', content: text('Could not record the event') }]),
    idle('done', { type: 'end_turn' }),
  ]);
  admit(snapshot, 'next', message('next'));
  const plan = planSessionContinuation(snapshot, 'next');
  assert.equal(plan.mode, 'model');
  assert.deepEqual(plan.remaining, []);
  assert.deepEqual(plan.historyEvents.filter(event => event.type.startsWith('agent.')).map(event => event.id), ['invalid', 'invalid-result', 'answer']);
  assert.deepEqual(plan.historyEvents.filter(event => event.type === 'user.message').map(event => event.id), ['prompt', 'next']);
});

test('native and MCP results do not consume real custom callbacks during durable partial intake', () => {
  const snapshot = journal([
    ...span('mixed-model', [
      custom('invalid', 'MINUTE'),
      { id: 'native', type: 'agent.tool_use', name: 'bash', input: { command: 'pwd' } },
      { id: 'mcp', type: 'agent.mcp_tool_use', name: 'mcp__clock__now', mcpServerName: 'clock', input: {} },
    ], [
      localResult('invalid-result', 'invalid'),
      { id: 'native-result', type: 'agent.tool_result', toolUseId: 'native', content: text('/workspace'), isError: false },
      { id: 'mcp-result', type: 'agent.mcp_tool_result', mcpToolUseId: 'mcp', content: text('now'), isError: false },
    ]),
    ...span('corrected-model', [custom('corrected'), custom('second')]),
    idle('declaration', requires(['corrected', 'second'])),
  ]);
  admit(snapshot, 'reply', callback('reply', 'corrected'));
  const first = planSessionContinuation(snapshot, 'reply');
  assert.equal(first.mode, 'intake');
  assert.deepEqual(first.remaining, ['second']);
  assert.deepEqual(first.phaseInputIds, ['reply']);

  const restarted = structuredClone(snapshot);
  restarted.executions.find(row => row.id === 'reply').state = 'completed';
  append(restarted, 'reply', { id: 'intake-running', type: 'session.status_running' });
  append(restarted, 'reply', idle('intake-idle', requires(['second'])));
  admit(restarted, 'second-reply', callback('second-reply', 'second'));
  const second = planSessionContinuation(restarted, 'second-reply');
  assert.equal(second.mode, 'model');
  assert.deepEqual(second.remaining, []);
  assert.deepEqual(second.phaseInputIds, ['second-reply']);
  assert.deepEqual(second.historyEvents.filter(event => event.type === 'user.custom_tool_result').map(event => event.id), ['reply', 'second-reply']);
  assert.deepEqual(second.historyEvents.filter(event => event.type === 'agent.custom_tool_use').map(event => event.id), ['invalid', 'corrected', 'second']);
  assert.deepEqual(second.historyEvents.filter(event => event.type === 'agent.tool_result' || event.type === 'agent.mcp_tool_result').map(event => event.id),
    ['invalid-result', 'native-result', 'mcp-result']);
});

test('retained completed callbacks deduplicate identical retries without accepting conflicting or undeclared replies', () => {
  const snapshot = waiting();
  admit(snapshot, 'reply', callback('reply', 'corrected'), 'completed');
  append(snapshot, 'reply', { id: 'reply-running', type: 'session.status_running' });
  append(snapshot, 'reply', { id: 'answer', type: 'agent.message', content: text('Recorded') });
  append(snapshot, 'reply', idle('reply-done', { type: 'end_turn' }));

  const retry = structuredClone(snapshot);
  admit(retry, 'retry', callback('retry', 'corrected'));
  const plan = planSessionContinuation(retry, 'retry');
  assert.equal(plan.mode, 'noop');
  assert.deepEqual(plan.historyEvents.filter(event => event.type === 'user.custom_tool_result').map(event => event.id), ['reply']);

  const conflicting = structuredClone(snapshot);
  admit(conflicting, 'conflicting', { ...callback('conflicting', 'corrected'), isError: true });
  assertFailure(conflicting, 'conflicting', /Conflicting previously consumed custom-tool result/);
  for (const customToolUseId of ['invalid', 'foreign']) {
    const unknown = structuredClone(snapshot);
    admit(unknown, 'unknown', callback('unknown', customToolUseId));
    assertFailure(unknown, 'unknown', /unknown or failed custom-tool group/);
  }
});

for (const [name, corrupt] of [
  ['missing pending ID', snapshot => { snapshot.orderedEvents.at(-1).event.stopReason.eventIds = []; }],
  ['extra pending ID', snapshot => { snapshot.orderedEvents.at(-1).event.stopReason.eventIds = ['corrected', 'foreign']; }],
  ['duplicate pending ID', snapshot => { snapshot.orderedEvents.at(-1).event.stopReason.eventIds = ['corrected', 'corrected']; }],
  ['already locally resolved ID', snapshot => { snapshot.orderedEvents.at(-1).event.stopReason.eventIds = ['invalid', 'corrected']; }],
  ['foreign result target', snapshot => { snapshot.orderedEvents.find(entry => entry.event.id === 'invalid-result').event.toolUseId = 'foreign'; }],
  ['foreign result execution', snapshot => { snapshot.orderedEvents.find(entry => entry.event.id === 'invalid-result').executionId = 'other-execution'; }],
  ['foreign result lane', snapshot => { snapshot.orderedEvents.find(entry => entry.event.id === 'invalid-result').event.sessionThreadId = 'sthr_other'; }],
  ['foreign result attempt', snapshot => { snapshot.orderedEvents.find(entry => entry.event.id === 'invalid-result').executionMetadata.attemptId = 'other-attempt'; }],
  ['foreign result generation', snapshot => { snapshot.orderedEvents.find(entry => entry.event.id === 'invalid-result').executionMetadata.generation = 2; }],
  ['result before the call', snapshot => {
    const result = snapshot.orderedEvents.find(entry => entry.event.id === 'invalid-result');
    const use = snapshot.orderedEvents.find(entry => entry.event.id === 'invalid');
    [result.event, use.event] = [use.event, result.event];
  }],
  ['result after the phase terminal', snapshot => {
    const result = snapshot.orderedEvents.splice(snapshot.orderedEvents.findIndex(entry => entry.event.id === 'invalid-result'), 1)[0];
    result.position.revision = ++snapshot.revision;
    snapshot.orderedEvents.push(result);
  }],
  ['MCP result for a custom call', snapshot => {
    const result = snapshot.orderedEvents.find(entry => entry.event.id === 'invalid-result').event;
    result.type = 'agent.mcp_tool_result';
    result.mcpToolUseId = result.toolUseId;
    delete result.toolUseId;
  }],
]) test(`local resolution never legitimizes a declaration with ${name}`, () => {
  const snapshot = waiting();
  corrupt(snapshot);
  admit(snapshot, 'next', message('next'));
  assertFailure(snapshot, 'next', /Invalid custom-tool group declaration/);
});

for (const [name, corrupt] of [
  ['duplicate event identity', snapshot => { snapshot.orderedEvents.find(entry => entry.event.id === 'invalid-result').event.id = 'invalid'; }],
  ['untrusted source position', snapshot => { snapshot.orderedEvents[1].position.index = -1; }],
]) test(`local resolution does not weaken journal validation for ${name}`, () => {
  const snapshot = waiting();
  corrupt(snapshot);
  admit(snapshot, 'reply', callback('reply', 'corrected'));
  assertFailure(snapshot, 'reply', /Untrusted continuation journal order/);
});

test('owner retry without an idle receipt reconstructs only unresolved custom calls from the completed latest span', () => {
  const snapshot = journal(span('model', [custom('invalid', 'MINUTE'), custom('corrected')], [localResult('invalid-result', 'invalid')]), 'running');
  snapshot.executions[0].attemptCount = 2;
  const plan = planSessionContinuation(structuredClone(snapshot), 'original');
  assert.equal(plan.mode, 'finalize');
  assert.deepEqual(plan.recoveredStopReason, requires(['corrected']));
  assert.deepEqual(plan.historyEvents.filter(event => event.type.startsWith('agent.')).map(event => event.id), ['invalid', 'corrected', 'invalid-result']);
});

test('owner retry after a fully resolved custom-tool round resumes the model instead of inventing end_turn', () => {
  const snapshot = journal(span('model', [custom('invalid', 'MINUTE')], [localResult('invalid-result', 'invalid')]), 'running');
  const plan = planSessionContinuation(snapshot, 'original');
  assert.equal(plan.mode, 'model');
  assert.equal(plan.recoveredStopReason, undefined);
  assert.deepEqual(plan.remaining, []);
  assert.deepEqual(plan.historyEvents.filter(event => event.type.startsWith('agent.')).map(event => event.id), ['invalid', 'invalid-result']);
});

test('earlier-span local results do not settle the latest-span custom call during owner retry', () => {
  const snapshot = journal(rejectedThenCorrected(), 'running');
  const plan = planSessionContinuation(snapshot, 'original');
  assert.equal(plan.mode, 'finalize');
  assert.deepEqual(plan.recoveredStopReason, requires(['corrected']));
  assert.deepEqual(plan.historyEvents.filter(event => event.type.startsWith('agent.')).map(event => event.id), ['invalid', 'invalid-result', 'corrected']);
});

for (const isError of [false, undefined, null]) test(`local results settle custom calls when isError is ${isError}`, () => {
  const snapshot = waiting();
  const result = snapshot.orderedEvents.find(entry => entry.event.id === 'invalid-result').event;
  if (isError === undefined) delete result.isError;
  else result.isError = isError;
  admit(snapshot, 'reply', callback('reply', 'corrected'));
  assert.equal(planSessionContinuation(snapshot, 'reply').mode, 'model');
});

test('legacy local results without private attempt metadata still settle within their owned phase', () => {
  const snapshot = waiting();
  for (const entry of snapshot.orderedEvents) delete entry.executionMetadata;
  admit(snapshot, 'reply', callback('reply', 'corrected'));
  assert.equal(planSessionContinuation(snapshot, 'reply').mode, 'model');
});

test('an all-locally-settled phase cannot declare a foreign callback', () => {
  const snapshot = journal([
    ...span('model', [custom('invalid', 'MINUTE')], [localResult('invalid-result', 'invalid')]),
    idle('declaration', requires(['foreign'])),
  ]);
  admit(snapshot, 'reply', callback('reply', 'foreign'));
  assertFailure(snapshot, 'reply', /Invalid custom-tool group declaration/);
});

for (const boundary of ['missing', 'failed']) test(`local results cannot recover a ${boundary} model boundary`, () => {
  const output = span('model', [custom('invalid', 'MINUTE')], [localResult('invalid-result', 'invalid')]);
  if (boundary === 'missing') output.splice(output.findIndex(event => event.type === 'span.model_request_end'), 1);
  else output.find(event => event.type === 'span.model_request_end').isError = true;
  assertFailure(journal(output, 'running'), 'original', /no complete model\/tool terminal boundary/);
});
