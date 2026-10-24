import assert from "node:assert/strict";
import { pathToFileURL } from "node:url";
import type {
  Candidate, Fence, Fences, Handle, Options, Provider, Scope,
  RetainedSessionLifecycle as Lifecycle,
} from "../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/retained-session.ts";

const configured = process.env.TANK_AUDIT_OPENMA_ROOT;
const openmaSource = configured === undefined
  ? new URL("../../../vendor/openma/", import.meta.url)
  : pathToFileURL(configured.replace(/\/?$/, "/"));
const lifecycle: typeof import("../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/retained-session.ts") =
  await import(new URL("packages/control-plane-node/src/lib/tank-retained/retained-session.ts", openmaSource).href);
const fingerprint = "sha256:" + "a".repeat(64);

interface FenceRow {
  active: boolean;
  publication: { workspaceCandidate: Candidate } | null;
  fence: Fence;
}
interface Disk {
  id: string;
  scope: string;
  files: Record<string, string>;
  profile: Record<string, string>;
  running: boolean;
}
export interface NativeRuntime {
  runtimeHandle(): Handle;
  suspend(): Promise<void>;
  exec(command: string, env?: Record<string, string>): Promise<{ disk: Disk; env: Record<string, string> }>;
}
export type Call = ["stop" | "fresh-get" | "create" | "remove-owned", string] | ["exec", string, string];
interface FakeProvider extends Provider<NativeRuntime> {
  allocationIntent(scope: Scope): Promise<Handle>;
  resolveIntent(handle: Handle, scope: Scope): Promise<Handle | null>;
  create(scope: Scope): Promise<NativeRuntime>;
  resume(handle: Handle, scope: Scope): Promise<NativeRuntime>;
  reap(handle: Handle, scope: Scope): Promise<void>;
}
interface Setup {
  rows: Map<string, FenceRow>;
  fences: Fences;
  provider: FakeProvider;
  disks: Map<string, Disk>;
  calls: Call[];
  nativeHandles: NativeRuntime[];
  options: Options<NativeRuntime>;
  readonly creates: number;
  controller(changes?: Partial<Options<NativeRuntime>>): Lifecycle<NativeRuntime>;
}

function fenceScope(fence: Fence): Scope {
  const { workspaceId, environmentId, sessionId, workId } = fence;
  return { workspaceId, environmentId, sessionId, workId };
}

export function setup(): Setup {
  const rows = new Map<string, FenceRow>();
  let number = 0;
  const key = (scope: Scope): string => JSON.stringify(scope);
  const requireRow = (fence: Fence): FenceRow => {
    const row = rows.get(key(fenceScope(fence)));
    if (row === undefined) throw Error("owned fence row missing");
    return row;
  };
  const fences: Fences = {
    async acquire({ scope, ownerId }: Parameters<Fences["acquire"]>[0]): ReturnType<Fences["acquire"]> {
      const old = rows.get(key(scope));
      if (old?.active && old.fence.ownerId !== ownerId) return { type: "conflict", expiresAt: null };
      const publication = old === undefined ? null : old.publication;
      const row: FenceRow = {
        active: true, publication,
        fence: { ...scope, ownerId, generation: ++number, token: "FAKE_FENCE_" + number, expiresAt: "2099-01-01T00:00:00Z" },
      };
      rows.set(key(scope), row);
      return { type: "acquired", fence: row.fence, publication: structuredClone(row.publication) };
    },
    async isCurrent(fence: Fence): Promise<boolean> {
      const row = rows.get(key(fenceScope(fence)));
      return row !== undefined && row.active && row.fence.token === fence.token;
    },
    async renew({ fence }: Parameters<Fences["renew"]>[0]): ReturnType<Fences["renew"]> {
      return await this.isCurrent(fence) ? { type: "renewed", fence } : { type: "lost" };
    },
    async publish({ fence, workspaceCandidate }: Parameters<Fences["publish"]>[0]): ReturnType<Fences["publish"]> {
      if (!await this.isCurrent(fence)) return { type: "lost" };
      requireRow(fence).publication = { workspaceCandidate: structuredClone(workspaceCandidate) };
      return { type: "published", revision: 1 };
    },
    async release({ fence }: Parameters<Fences["release"]>[0]): Promise<void> {
      if (await this.isCurrent(fence)) requireRow(fence).active = false;
    },
  };
  const disks = new Map<string, Disk>();
  const calls: Call[] = [];
  const nativeHandles: NativeRuntime[] = [];
  let next = 0;
  let creates = 0;
  function native(disk: Disk): NativeRuntime {
    let invalid = false;
    const value: NativeRuntime = {
      runtimeHandle: (): Handle => ({ provider: "litebox", runtimeId: disk.id }),
      async suspend(): Promise<void> {
        assert.equal(invalid, false);
        calls.push(["stop", disk.id]);
        invalid = true;
        disk.running = false;
      },
      async exec(command: string, env: Record<string, string> = {}): ReturnType<NativeRuntime["exec"]> {
        assert.equal(invalid, false);
        assert.equal(disk.running, true);
        calls.push(["exec", disk.id, command]);
        return { disk, env };
      },
    };
    nativeHandles.push(value);
    return value;
  }
  const provider: FakeProvider = {
    async allocationIntent(scope: Scope): Promise<Handle> {
      if ([...disks.values()].some((disk: Disk): boolean => disk.scope === JSON.stringify(scope))) throw Error("retained_unpublished_runtime_exists");
      return { provider: "litebox", runtimeId: "intent:" + scope.sessionId };
    },
    async resolveIntent(handle: Handle, scope: Scope): Promise<Handle | null> {
      assert.equal(handle.runtimeId, "intent:" + scope.sessionId);
      const disk = [...disks.values()].find((candidate: Disk): boolean => candidate.scope === JSON.stringify(scope));
      return disk === undefined ? null : { provider: "litebox", runtimeId: disk.id };
    },
    async create(scope: Scope): Promise<NativeRuntime> {
      creates++;
      const disk: Disk = { id: "box_" + (++next), scope: JSON.stringify(scope), files: {}, profile: {}, running: true };
      disks.set(disk.id, disk);
      calls.push(["create", disk.id]);
      return native(disk);
    },
    async resume(handle: Handle, scope: Scope): Promise<NativeRuntime> {
      calls.push(["fresh-get", handle.runtimeId]);
      const disk = disks.get(handle.runtimeId);
      if (disk === undefined) throw Error("owned retained disk missing");
      assert.equal(disk.scope, JSON.stringify(scope));
      disk.running = true;
      return native(disk);
    },
    async reap(handle: Handle, scope: Scope): Promise<void> {
      const disk = disks.get(handle.runtimeId);
      if (disk !== undefined) assert.equal(disk.scope, JSON.stringify(scope));
      calls.push(["remove-owned", handle.runtimeId]);
      disks.delete(handle.runtimeId);
    },
  };
  const options: Options<NativeRuntime> = {
    scope: { workspaceId: "workspace_A", environmentId: "production_A", sessionId: "session_A" },
    fingerprint, ownerId: "controller_A", providerId: "litebox", leaseTtlMs: 30000, fences, provider,
  };
  return {
    rows, fences, provider, disks, calls, nativeHandles, options,
    get creates(): number { return creates; },
    controller: (changes: Partial<Options<NativeRuntime>> = {}): Lifecycle<NativeRuntime> =>
      new lifecycle.RetainedSessionLifecycle({ ...options, ...changes }),
  };
}
