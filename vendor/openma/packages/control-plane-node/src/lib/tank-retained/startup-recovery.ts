import type { SqlClient } from '@open-managed-agents/sql-client';
import type { SessionClaimScope, StartupResident } from '../node-session-execution-worker.js';
import {
  pinnedBoxLiteProvider, retainedBoxName, retainedOwnershipName, type NativeBoxInfo,
} from './boxlite-provider.js';
import type { NodeRetainedOptions } from './node-lifecycle.js';
import { decodeRetainedPublication, type Candidate, type Scope } from './retained-session.js';

const workId = 'tank-retained-session-v1';

function matchResident(candidate: Candidate, scope: Scope, boxes: readonly Pick<NativeBoxInfo, 'id' | 'name'>[]) {
  const manifest = decodeRetainedPublication(candidate, scope, 'litebox');
  const intent = manifest.state === 'allocating' ? manifest.handle : undefined;
  if (!intent && manifest.state !== 'retained') return;
  const name = retainedOwnershipName(manifest.handle, scope);
  if (intent && (intent.runtimeId !== name || intent.ownershipName)) return;
  const box = boxes.find(box => box.name === name && (intent || box.id === manifest.handle.runtimeId));
  if (box) return { box, handle: { ...manifest.handle, runtimeId: box.id }, intent };
}

export async function listClaimScopes(sql: SqlClient, now: Date): Promise<SessionClaimScope[]> {
  const { results } = await sql.prepare(`
    SELECT workspace_id, session_id, MIN(admitted_at_ms) AS first_admission
    FROM managed_session_executions
    WHERE (state = 'queued' OR (state = 'running' AND lease_expires_at_ms <= ?))
      AND deadline_at_ms > ? AND attempt_count < max_attempts
    GROUP BY workspace_id, session_id
    ORDER BY first_admission, workspace_id, session_id
  `).bind(now.getTime(), now.getTime()).all<{ workspace_id: string; session_id: string }>();
  return (results ?? []).map(row => ({ workspaceId: row.workspace_id, sessionId: row.session_id }));
}

export async function discoverStartupResidents(
  sql: SqlClient,
  options: NodeRetainedOptions,
): Promise<StartupResident[]> {
  const client = options.client as { listInfo(): Promise<NativeBoxInfo[]> };
  const boxes = (await client.listInfo()).filter(box =>
    /^oma-[a-f0-9]{32}(?:-upgrade-[a-f0-9]{32})?$/.test(box.name ?? '')
    && (box.state.running || !/^(stopped|exited)$/i.test(box.state.status)))
    .map(({ id, name }) => ({ id, name }));
  if (!boxes.length) return [];

  const { results } = await sql.prepare(`
    SELECT workspace_id, environment_id, session_id, publication_json
    FROM runtime_resource_fences
    WHERE work_id = ? AND publication_json IS NOT NULL
  `).bind(workId).all<{
    workspace_id: string; environment_id: string; session_id: string;
    publication_json: string;
  }>();
  const matches = new Map<string, StartupResident[]>();
  const provider = pinnedBoxLiteProvider({
    module: options.providerModule, client, nativeHome: options.nativeHome,
    allocation: { image: 'startup-quiescence-only' }, leaseTtlMs: options.leaseTtlMs,
  });

  for (const row of results ?? []) {
    const scope: Scope = {
      workspaceId: row.workspace_id, environmentId: row.environment_id, sessionId: row.session_id, workId,
    };
    const { workspaceCandidate } = JSON.parse(row.publication_json) as { workspaceCandidate: Candidate };
    if (!boxes.some(box => box.name === retainedBoxName(scope)
      || box.id === workspaceCandidate?.metadata?.runtimeId)) continue;
    const discovered = matchResident(workspaceCandidate, scope, boxes);
    if (!discovered) continue;
    const { box } = discovered;
    const resident: StartupResident = {
      workspaceId: scope.workspaceId, sessionId: scope.sessionId,
      async quiesce() {
        const acquired = await options.fences.acquire({
          scope, ownerId: `${options.ownerId}:startup`, ttlMs: options.leaseTtlMs,
        });
        if (acquired.type !== 'acquired') throw Error('retained_resource_busy');
        const { fence, publication: current } = acquired;
        let verified = false;
        try {
          const matched = current && fence.generation > current.generation
            && matchResident(current.workspaceCandidate, scope, [box]);
          if (!matched) throw Error('retained_recovery_publication_changed');
          const assertCurrent = async () => {
            if (!await options.fences.isCurrent(fence)) throw Error('retained_fence_lost');
          };
          await provider.quiesceExact!(matched.handle, scope, assertCurrent, matched.intent);
          verified = true;
        } finally {
          await options.fences.release({ fence, reason: verified ? 'completed' : 'failed' });
        }
      },
    };
    const owners = matches.get(box.id) ?? [];
    owners.push(resident);
    matches.set(box.id, owners);
  }

  return boxes.map(box => {
    const owners = matches.get(box.id);
    if (owners?.length !== 1) throw Error('retained_startup_resident_ownership_unconfirmed');
    return owners[0]!;
  });
}
