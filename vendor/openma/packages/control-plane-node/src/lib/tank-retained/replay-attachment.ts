import type { Allocation } from './boxlite-provider.js';
export interface ReplayAttachmentContext {
  session: { metadata?: Record<string, string> };
  environment: { name: string };
}
export type PrefixVerifier = (options: { root?: string; runId?: string; manifestSha256?: string; cutoff?: string }) => Promise<{
  hostPath: string; guestPath: '/replay/evidence'; readOnly: true;
}>;
export async function validateReplayAttachment(context: ReplayAttachmentContext, allocation: Allocation,
  verify: PrefixVerifier, evidenceRoot: string | undefined): Promise<void> {
  if (context.environment.name !== 'tank-replay') return;
  const metadata = context.session.metadata;
  const selected = await verify({ root: evidenceRoot, runId: metadata?.tank_replay_run_id,
    manifestSha256: metadata?.tank_replay_evidence_sha256, cutoff: metadata?.tank_replay_cutoff });
  const mounts = allocation.volumes?.filter(volume => volume.guestPath === '/replay/evidence') ?? [];
  if (mounts.length !== 1 || mounts[0]!.hostPath !== selected.hostPath || mounts[0]!.readOnly !== true) {
    throw Error('retained_replay_mount_changed');
  }
}
