import type { SqlClient, SqlStatement, SqlSelectResult, SqlRunResult } from '@open-managed-agents/sql-client';
import { ContinuationProtocolError, decodeContinuationExecutionEvents, decodeContinuationImageRecovery, decodeContinuationHistorySourceDocument,
  type ContinuationExecution, type ContinuationImageRecovery, type ContinuationSnapshot } from '@open-managed-agents/session-runtime-contract/continuation';
import { SqlSessionRuntimeHistorySource } from './history';

export class SessionContinuationMissingError extends Error {
  readonly name = 'SessionContinuationMissingError';
}
function isContinuationRow(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function continuationRow(value: unknown, fields: readonly string[]): Record<string, unknown> {
  if (!isContinuationRow(value) || Object.keys(value).some(key => !fields.includes(key))) throw new ContinuationProtocolError('Invalid persisted continuation row');
  return value;
}
export function requiredContinuationRows<T>(values: T[] | undefined): T[] {
  if (values === undefined) throw new ContinuationProtocolError('Missing continuation SELECT result');
  return values;
}
function rowString(value: unknown): string {
  if (typeof value !== 'string' || value.trim().length === 0) throw new ContinuationProtocolError('Invalid persisted continuation row');
  return value;
}
function rowInteger(value: unknown, minimum: number | null): number {
  const decoded = typeof value === 'number' ? value
    : typeof value === 'string' && /^-?(0|[1-9]\d*)$/.test(value) ? Number(value) : NaN;
  if (!Number.isSafeInteger(decoded) || (minimum !== null && decoded < minimum)) throw new ContinuationProtocolError('Invalid persisted continuation integer');
  return decoded;
}
function rowState(value: unknown): ContinuationExecution['state'] {
  switch (value) {
    case 'queued': case 'running': case 'completed': case 'failed': case 'cancelled': return value;
    default: throw new ContinuationProtocolError('Invalid persisted continuation execution state');
  }
}
function rowClock(value: unknown): string | null {
  if (value === null) return null;
  const milliseconds = rowInteger(value, null);
  const stamp = new Date(milliseconds);
  if (!Number.isFinite(stamp.getTime())) throw new ContinuationProtocolError('Invalid persisted continuation clock');
  return stamp.toISOString();
}
function executionRow(value: unknown): ContinuationExecution {
  const row = continuationRow(value, ['id', 'lane_id', 'state', 'revision', 'events_json', 'attempt_count', 'interrupt_requested_at_ms']);
  return { id: rowString(row.id), laneId: rowString(row.lane_id), state: rowState(row.state),
    revision: rowInteger(row.revision, 1), events: decodeContinuationExecutionEvents(rowString(row.events_json)),
    attemptCount: rowInteger(row.attempt_count, 0), interruptRequestedAt: rowClock(row.interrupt_requested_at_ms) };
}
export async function loadSessionContinuationSnapshot(sql: SqlClient, scope: {
  workspaceId: string;
  sessionId: string;
}): Promise<ContinuationSnapshot> {
  const result = await sql.prepare(`SELECT id, lane_id, state, revision, events_json, attempt_count,
    interrupt_requested_at_ms FROM managed_session_executions WHERE workspace_id = ? AND session_id = ?
    ORDER BY admitted_at_ms, id`).bind(scope.workspaceId, scope.sessionId).all<unknown>();
  const executions = requiredContinuationRows(result.results).map(executionRow);
  const identities = new Set(executions.map(row => row.id));
  if (identities.size !== executions.length) throw new ContinuationProtocolError('Duplicate continuation execution identity');
  const recoveries = await sql.prepare(`SELECT execution_id, after_event_id FROM managed_session_image_recoveries
    WHERE workspace_id = ? AND execution_id IN (SELECT id FROM managed_session_executions WHERE workspace_id = ? AND session_id = ?)`)
    .bind(scope.workspaceId, scope.workspaceId, scope.sessionId).all<unknown>();
  const recoveryBoundaries = new Map<string, ContinuationImageRecovery>();
  for (const value of requiredContinuationRows(recoveries.results)) {
    const row = continuationRow(value, ['execution_id', 'after_event_id']), id = rowString(row.execution_id);
    if (!identities.has(id) || recoveryBoundaries.has(id)) throw new ContinuationProtocolError('Invalid continuation recovery identity');
    recoveryBoundaries.set(id, decodeContinuationImageRecovery(rowString(row.after_event_id)));
  }
  const history = await new SqlSessionRuntimeHistorySource(continuationHistoryClient(sql)).load(scope);
  if (history === null) throw new SessionContinuationMissingError('Continuation Session no longer exists');
  if (history.revision === undefined) throw new ContinuationProtocolError('Missing continuation history revision');
  return {
    revision: rowInteger(history.revision, 0), orderedEvents: history.orderedEvents ?? [],
    ...(history.orderedEvents === undefined ? { invalidReason: 'Continuation requires complete trusted ordered native history' } : {}),
    executions: executions.map(row => {
      const recovery = recoveryBoundaries.get(row.id);
      if (recovery === undefined) return row;
      return { ...row, imageRecoveryAfterEventId: recovery.afterEventId,
        imageRecoveryInterruptRequestedAt: recovery.interruptRequestedAt,
        ...(recovery.retryAfterEventId === undefined ? {} : { imageRecoveryRetryAfterEventId: recovery.retryAfterEventId }) };
    })
  };
}

function continuationHistoryStatement(statement: SqlStatement): SqlStatement {
  return {
    bind(...parameters: unknown[]): SqlStatement { return continuationHistoryStatement(statement.bind(...parameters)); },
    async run<T = unknown>(): Promise<SqlRunResult<T>> { return invalidHistoryOperation(); },
    async first<T = unknown>(): Promise<T | null> { return invalidHistoryOperation(); },
    async all<T = unknown>(): Promise<SqlSelectResult<T>> {
      const result = await statement.all<T>();
      let revision: number | undefined;
      for (const value of requiredContinuationRows(result.results)) {
        const row = continuationRow(value, ['revision', 'history_kind', 'document']);
        const current = rowInteger(row.revision, 0);
        if (revision !== undefined && current !== revision) throw new ContinuationProtocolError('Conflicting continuation history revision');
        revision = current;
        if (row.history_kind === null) {
          if (row.document !== null) throw new ContinuationProtocolError('Invalid persisted continuation history row');
        } else if (row.history_kind === 'event' || row.history_kind === 'initial') {
          decodeContinuationHistorySourceDocument(rowString(row.document));
        } else throw new ContinuationProtocolError('Invalid persisted continuation history row');
      }
      return result;
    },
  };
}
function continuationHistoryClient(client: SqlClient): SqlClient {
  return {
    prepare(query: string): SqlStatement {
      if (!query.trimStart().startsWith('SELECT session.revision, history.history_kind, history.document')) return invalidHistoryOperation();
      return continuationHistoryStatement(client.prepare(query));
    },
    async batch<T = unknown>(_statements: SqlStatement[]): Promise<SqlRunResult<T>[]> { return invalidHistoryOperation(); },
    async exec(_query: string): Promise<void> { return invalidHistoryOperation(); },
  };
}

function invalidHistoryOperation(): never {
  throw new ContinuationProtocolError('Continuation history admission requires its read-only SELECT');
}
