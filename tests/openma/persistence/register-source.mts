import { registerHooks, type ResolveHookSync } from 'node:module';

const root = new URL('../../../vendor/openma/', import.meta.url);
const aliases = new Map<string, URL>([
  ['@open-managed-agents/session-runtime-contract/history', new URL('packages/session-runtime-contract/src/history.ts', root)],
  ['@open-managed-agents/session-runtime-contract/continuation', new URL('packages/session-runtime-contract/src/continuation.ts', root)],
  ['@open-managed-agents/session-runtime-contract/coordination', new URL('packages/session-runtime-contract/src/coordination.ts', root)],
  ['@open-managed-agents/sql-client/db-errors', new URL('packages/sql-client/src/db-errors.ts', root)],
]);
const relative = new Map<string, URL>([
  [new URL('packages/session-runtime-contract/src/continuation.ts', root).href + '|./history', new URL('packages/session-runtime-contract/src/history.ts', root)],
  [new URL('packages/session-runtime-contract/src/continuation.ts', root).href + '|./json', new URL('packages/session-runtime-contract/src/json.ts', root)],
  [new URL('packages/session-runtime-sql/src/continuation.ts', root).href + '|./history', new URL('packages/session-runtime-sql/src/history.ts', root)],
  [new URL('packages/session-runtime-sql/src/coordination.ts', root).href + '|./continuation', new URL('packages/session-runtime-sql/src/continuation.ts', root)],
  [new URL('packages/sql-client/src/adapters/postgres.ts', root).href + '|../db-errors', new URL('packages/sql-client/src/db-errors.ts', root)],
  [new URL('packages/sql-client/src/adapters/postgres.ts', root).href + '|./postgres-errors', new URL('packages/sql-client/src/adapters/postgres-errors.ts', root)],
  [new URL('packages/sql-client/src/adapters/postgres-errors.ts', root).href + '|../db-errors', new URL('packages/sql-client/src/db-errors.ts', root)],
]);
const resolve: ResolveHookSync = (specifier, context, nextResolve): ReturnType<ResolveHookSync> => {
  const target = aliases.get(specifier);
  if (target !== undefined) return nextResolve(target.href, context);
  if (context.parentURL !== undefined) {
    const adjacent = relative.get(context.parentURL + '|' + specifier);
    if (adjacent !== undefined) return nextResolve(adjacent.href, context);
  }
  return nextResolve(specifier, context);
};
registerHooks({ resolve });
