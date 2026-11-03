import { registerHooks } from 'node:module';
import { readFileSync } from 'node:fs';

const root = new URL('../../', import.meta.url);
registerHooks({ resolve(specifier, context, nextResolve) {
  if (specifier === '@open-managed-agents/session-runtime-contract/continuation')
    return nextResolve(new URL('vendor/openma/packages/session-runtime-contract/src/continuation.ts', root).href, context);
  return nextResolve(specifier, context);
} });
const { sessionExecutionStatusStore } = await import(new URL('vendor/openma/packages/control-plane-node/src/lib/node-session-execution-status.ts', root));
const wire = JSON.parse(readFileSync(0, 'utf8'));
const executionId = wire.execution_id;
const event = {
  id: wire.id, type: wire.type, name: wire.name, input: wire.input, processedAt: wire.processed_at,
};
const input = { id: executionId, type: 'user.message', content: [], processedAt: wire.processed_at };
const row = {
  id: executionId, laneId: 'sthr_primary', state: 'completed', revision: 1,
  events: [input], attemptCount: 1, interruptRequestedAt: null,
};
const output = [
  { id: 'running', type: 'session.status_running', processedAt: wire.processed_at },
  event,
  { id: `declaration-${executionId}`, type: 'session.status_idle', processedAt: wire.processed_at,
    stopReason: { type: 'requires_action', eventIds: [event.id] } },
];
const snapshot = {
  revision: 4, executions: [row],
  orderedEvents: [
    { event: input, position: { revision: 1, index: 0 } },
    ...output.map((event, index) => ({
      event, position: { revision: index + 2, index: 0 }, executionId,
      executionMetadata: { attemptId: 'original-attempt', generation: 1 },
    })),
  ],
};
const store = sessionExecutionStatusStore({ find: async () => ({
  ...row, workspaceId: 'workspace', sessionId: 's', settledAt: wire.processed_at,
}) }, async () => snapshot);
process.stdout.write(JSON.stringify((await store.list('workspace', 's', [executionId]))[0]));
