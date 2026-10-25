import type { SqlClient } from '@open-managed-agents/sql-client';
import type { ContinuationSnapshot } from '@open-managed-agents/session-runtime-contract/continuation';
import { SqlSessionRuntimeHistorySource } from './history';
export class SessionContinuationMissingError extends Error {
  readonly name = 'SessionContinuationMissingError';
}
export async function loadSessionContinuationSnapshot(sql: SqlClient, scope: {
  workspaceId: string;
  sessionId: string;
}): Promise<ContinuationSnapshot> {
  const result = await sql.prepare(`SELECT id, lane_id, state, revision, events_json, attempt_count,
    interrupt_requested_at_ms FROM managed_session_executions WHERE workspace_id = ? AND session_id = ?
    ORDER BY admitted_at_ms, id`).bind(scope.workspaceId, scope.sessionId).all<{
    id: string;
    lane_id: string;
    state: ContinuationSnapshot['executions'][number]['state'];
    revision: number | string;
    events_json: string;
    attempt_count: number | string;
    interrupt_requested_at_ms: number | string | null;
  }>();
  const recoveries = await sql.prepare(`SELECT execution_id, after_event_id FROM managed_session_image_recoveries
    WHERE workspace_id = ? AND execution_id IN (SELECT id FROM managed_session_executions WHERE workspace_id = ? AND session_id = ?)`)
    .bind(scope.workspaceId, scope.workspaceId, scope.sessionId).all<{ execution_id: string; after_event_id: string }>();
  const recoveryBoundaries = new Map<string, { afterEventId: string; interruptRequestedAt: string | null; retryAfterEventId?: string }>((recoveries.results ?? []).map(row => [row.execution_id, row.after_event_id.startsWith('{')
    ? JSON.parse(row.after_event_id) as { afterEventId: string; interruptRequestedAt: null; retryAfterEventId?: string }
    : { afterEventId: row.after_event_id, interruptRequestedAt: null }]));
  const history = await new SqlSessionRuntimeHistorySource(sql).load(scope);
  if (!history || history.revision === undefined)
    throw new SessionContinuationMissingError('Continuation Session no longer exists');
  return {
    revision: Number(history.revision), orderedEvents: history.orderedEvents ?? [],
    ...(history.orderedEvents === undefined ? { invalidReason: 'Continuation requires complete trusted ordered native history' } : {}),
    executions: (result.results ?? []).map(row => ({
      id: row.id, laneId: row.lane_id,
      ...(recoveryBoundaries.has(row.id) ? { imageRecoveryAfterEventId: recoveryBoundaries.get(row.id)!.afterEventId,
        imageRecoveryInterruptRequestedAt: recoveryBoundaries.get(row.id)!.interruptRequestedAt,
        imageRecoveryRetryAfterEventId: recoveryBoundaries.get(row.id)!.retryAfterEventId } : {}),
      state: row.state, revision: Number(row.revision), events: JSON.parse(row.events_json),
      attemptCount: Number(row.attempt_count), interruptRequestedAt: row.interrupt_requested_at_ms === null
        ? null : new Date(Number(row.interrupt_requested_at_ms)).toISOString()
    }))
  };
}
