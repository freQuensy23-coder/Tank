import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { pathToFileURL } from 'node:url';
import { setup } from './fakes.mjs';

const openmaSource = process.env.TANK_AUDIT_OPENMA_ROOT
  ? pathToFileURL(process.env.TANK_AUDIT_OPENMA_ROOT.replace(/\/?$/, '/'))
  : new URL('../../../vendor/openma/', import.meta.url);
const { discoverStartupResidents } = await import(new URL(
  'packages/control-plane-node/src/lib/tank-retained/startup-recovery.ts', openmaSource,
));
const { retainedBoxName } = await import(new URL(
  'packages/control-plane-node/src/lib/tank-retained/boxlite-provider.ts', openmaSource,
));

async function startupFixture(context, onCreate) {
  const store = setup();
  const nativeHome = await mkdtemp(join(tmpdir(), 'tank-startup-recovery-'));
  const prior = store.controller({ provider: {
    ...store.provider,
    async allocationIntent(scope) { return { provider: 'litebox', runtimeId: retainedBoxName(scope) }; },
    async create(scope) {
      const runtime = await store.provider.create(scope);
      await onCreate?.();
      return runtime;
    },
  } });
  context.after(async () => {
    await prior.abandon();
    await rm(nativeHome, { recursive: true, force: true });
  });
  const { scope } = prior;
  const name = retainedBoxName(scope);
  const nativeCalls = [];
  const nativeReads = [];
  const info = disk => ({ id: disk.id, name, state: {
    running: disk.running, status: disk.status ?? (disk.running ? 'Running' : 'Stopped'),
  } });
  const client = {
    async listInfo() { return [...store.disks.values()].map(info); },
    async getInfo(id) {
      nativeReads.push(id);
      const disk = store.disks.get(id) ?? (id === name ? [...store.disks.values()][0] : undefined);
      return disk ? info(disk) : null;
    },
    async get(id) {
      const disk = store.disks.get(id);
      return disk ? {
        id, name, async info() { return info(disk); },
        async exec(command) {
          nativeCalls.push([command, id]);
          return { async wait() { return { exitCode: 0 }; } };
        },
        async stop() { nativeCalls.push(['stop', id]); disk.running = false; },
      } : null;
    },
  };
  const row = () => store.rows.get(JSON.stringify(scope));
  const discover = async () => {
    const sql = {
      prepare() {
        return { bind: () => ({ all: async () => ({ results: [{
          workspace_id: scope.workspaceId,
          environment_id: scope.environmentId,
          session_id: scope.sessionId,
          publication_json: JSON.stringify(row().publication),
          publication_generation: row().publication.generation,
          revision: row().publication.revision,
        }] }) }) };
      },
    };
    const [resident] = await discoverStartupResidents(sql, {
      client, nativeHome, ownerId: 'replacement', fences: store.fences, leaseTtlMs: 30000,
      providerModule: { createBoxLiteProvider() {}, createBoxLiteManagedRuntime() {} },
    });
    assert.equal(resident.sessionId, scope.sessionId);
    return resident;
  };
  return { ...store, prior, row, discover, nativeCalls, nativeReads };
}

test('startup retries a retained publication advanced by the old owner after discovery', async context => {
  const { prior, row, discover, disks, nativeCalls, nativeReads } = await startupFixture(context);
  await prior.begin(async () => {}, new AbortController().signal);
  const [disk] = disks.values();
  const discoveredRevision = row().publication.revision;
  const resident = await discover();
  await assert.rejects(resident.quiesce(), /retained_resource_busy/);

  await prior.idle(async () => {});
  assert.ok(row().publication.revision > discoveredRevision);
  assert.equal(disk.running, false);
  disk.status = 'Stopping';
  await assert.rejects(resident.quiesce(), /retained_recovery_stop_unconfirmed/);
  disk.status = 'Stopped';
  await resident.quiesce();
  assert.ok(nativeReads.includes(disk.id));
  assert.deepEqual(nativeCalls, []);
});

test('startup uses the retained runtime ID when discovery saw its allocation intent', async context => {
  let resident;
  const fixture = await startupFixture(context, async () => { resident = await fixture.discover(); });
  await fixture.prior.begin(async () => {}, new AbortController().signal);
  const [disk] = fixture.disks.values();
  await fixture.fences.release({ fence: fixture.row().fence });

  await resident.quiesce();
  assert.equal(disk.running, false);
  assert.ok(fixture.nativeReads.length > 0);
  assert.ok(fixture.nativeReads.every(runtimeId => runtimeId === disk.id));
  assert.deepEqual(fixture.nativeCalls, [['/bin/sync', disk.id], ['stop', disk.id]]);
});

test('startup does not mutate a discovered guest when the current publication names another runtime', async context => {
  const { prior, row, discover, disks, fences, nativeCalls, nativeReads } = await startupFixture(context);
  await prior.begin(async () => {}, new AbortController().signal);
  const [disk] = disks.values();
  const resident = await discover();
  const { fence } = row();
  const { provider, fingerprint, state } = row().publication.workspaceCandidate.metadata;
  const runtimeId = 'replacement-box';
  const contentHash = 'sha256:' + createHash('sha256').update(JSON.stringify({
    scope: prior.scope, provider, runtimeId, fingerprint, state,
  })).digest('hex');
  await fences.publish({ fence, workspaceCandidate: {
    id: `retained-${runtimeId}`, contentHash, metadata: {
      schema: 1, scope: JSON.stringify(prior.scope), provider, runtimeId, fingerprint, state,
    },
  } });
  await fences.release({ fence });

  await assert.rejects(resident.quiesce(), /retained_recovery_publication_changed/);
  assert.deepEqual(nativeReads, []);
  assert.deepEqual(nativeCalls, []);
  assert.equal(disk.running, true);
});
