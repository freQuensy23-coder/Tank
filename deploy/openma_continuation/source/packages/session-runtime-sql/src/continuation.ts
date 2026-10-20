/** Load continuation facts from the existing session journal and execution queue. */
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
  // Execution revisions are bound again in the atomic claim. Journal revision is from one SELECT snapshot.
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
  const history = await new SqlSessionRuntimeHistorySource(sql).load(scope);
  if (!history || history.revision === undefined)
    throw new SessionContinuationMissingError('Continuation Session no longer exists');
  return {
    revision: Number(history.revision), orderedEvents: history.orderedEvents ?? [],
    ...(history.orderedEvents === undefined ? { invalidReason: 'Continuation requires complete trusted ordered native history' } : {}),
    executions: (result.results ?? []).map(row => ({
      id: row.id, laneId: row.lane_id,
      state: row.state, revision: Number(row.revision), events: JSON.parse(row.events_json),
      attemptCount: Number(row.attempt_count), interruptRequestedAt: row.interrupt_requested_at_ms === null
        ? null : new Date(Number(row.interrupt_requested_at_ms)).toISOString()
    }))
  };
}
