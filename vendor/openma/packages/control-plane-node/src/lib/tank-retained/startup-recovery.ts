import type { SqlClient } from '@open-managed-agents/sql-client';
import type { SessionClaimScope, StartupResident } from '../node-session-execution-worker.js';
import {
  pinnedBoxLiteProvider, retainedBoxName, retainedOwnershipName, type NativeBoxInfo,
} from './boxlite-provider.js';
import type { NodeRetainedOptions } from './node-lifecycle.js';
import { decodeRetainedPublication, type Candidate, type Publication, type Scope } from './retained-session.js';
import { stableDigest } from './runtime-identity.js';

const workId = 'tank-retained-session-v1';

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
    && (box.state.running || !/^(stopped|exited)$/i.test(box.state.status)));
  if (!boxes.length) return [];

  const { results } = await sql.prepare(`
    SELECT workspace_id, environment_id, session_id, publication_json, publication_generation, revision
    FROM runtime_resource_fences
    WHERE work_id = ? AND publication_json IS NOT NULL
  `).bind(workId).all<{
    workspace_id: string; environment_id: string; session_id: string;
    publication_json: string; publication_generation: number | string; revision: number | string;
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
    const publication: Publication = {
      workspaceCandidate: (JSON.parse(row.publication_json) as { workspaceCandidate: Candidate }).workspaceCandidate,
      generation: Number(row.publication_generation), revision: Number(row.revision),
    };
    if (!boxes.some(box => box.name === retainedBoxName(scope)
      || box.id === publication.workspaceCandidate?.metadata?.runtimeId)) continue;
    const manifest = decodeRetainedPublication(publication.workspaceCandidate, scope, 'litebox');
    const intent = manifest.state === 'allocating' ? manifest.handle : undefined;
    if (!intent && manifest.state !== 'retained') continue;
    const name = retainedOwnershipName(manifest.handle, scope);
    if (intent && (intent.runtimeId !== name || intent.ownershipName)) continue;
    const box = boxes.find(box => box.name === name && (intent || box.id === manifest.handle.runtimeId));
    if (!box) continue;
    const handle = { ...manifest.handle, runtimeId: box.id };
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
          if (!current || fence.generation <= publication.generation
            || current.generation !== publication.generation || current.revision !== publication.revision
            || stableDigest(current.workspaceCandidate) !== stableDigest(publication.workspaceCandidate)) {
            throw Error('retained_recovery_publication_changed');
          }
          const assertCurrent = async () => {
            if (!await options.fences.isCurrent(fence)) throw Error('retained_fence_lost');
          };
          await provider.quiesceExact!(handle, scope, assertCurrent, intent);
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
