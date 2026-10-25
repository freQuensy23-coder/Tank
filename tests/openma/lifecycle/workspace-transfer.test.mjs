import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, mkdir, writeFile, readFile, rm, symlink, readlink, chmod, stat, copyFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { workspaceTransferProgram } from '../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/workspace-transfer.ts';
import { transferWorkspaceArchive, workspaceChunkProgram, workspaceChunkBytes } from '../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/workspace-chunks.ts';

async function fixture(fn) {
  const root = await mkdtemp(join(tmpdir(), 'tank-workspace-transfer-'));
  const source = join(root, 'source'), target = join(root, 'target'), archive = join(root, 'workspace.tar');
  await mkdir(source); await mkdir(target);
  const transfer = (mode, directory, file = archive) => spawnSync('python3', ['-c', workspaceTransferProgram, mode, file, '[]', directory], { encoding: 'utf8', timeout: 3000 });
  try { await fn({ root, source, target, archive, transfer }); } finally { await rm(root, { recursive: true, force: true }); }
}

test('actual archive transfer preserves binary workspace files, executable modes, profile bytes and safe output links', async () => {
  await fixture(async ({ source, target, transfer }) => {
    const profile = '.tank-browser/' + 'a'.repeat(32) + '/profile';
    await mkdir(join(source, profile), { recursive: true });
    const bytes = Buffer.from([0, 255, 10, 45, 42]);
    await writeFile(join(source, 'upload.bin'), bytes);
    await writeFile(join(source, 'run.sh'), 'printf image-independent-user-script');
    await chmod(join(source, 'run.sh'), 0o751);
    await writeFile(join(source, profile, 'Cookies'), 'exact persistent profile bytes');
    await symlink('/tmp/dead-chromium-socket', join(source, profile, 'SingletonSocket'));
    await symlink('/mnt/session/outputs', join(source, 'outputs'));
    const exported = transfer('export', source);
    assert.equal(exported.status, 0, exported.stderr);
    const imported = transfer('import', target);
    assert.equal(imported.status, 0, imported.stderr);
    assert.equal(imported.stdout, exported.stdout);
    assert.deepEqual(await readFile(join(target, 'upload.bin')), bytes);
    assert.equal((await stat(join(target, 'run.sh'))).mode & 0o777, 0o751);
    assert.equal(await readFile(join(target, profile, 'Cookies'), 'utf8'), 'exact persistent profile bytes');
    assert.equal(await readlink(join(target, 'outputs')), '/mnt/session/outputs');
    await assert.rejects(readlink(join(target, profile, 'SingletonSocket')), { code: 'ENOENT' });
  });
});

test('transfer refuses OS-root symlinks and nonempty replacement workspaces without modifying the source', async () => {
  await fixture(async ({ source, target, transfer }) => {
    await writeFile(join(source, 'value'), 'recoverable');
    await symlink('/etc/passwd', join(source, 'escape'));
    const bad = transfer('export', source);
    assert.notEqual(bad.status, 0);
    assert.match(bad.stderr, /retained_workspace_external_symlink/);
    await rm(join(source, 'escape'));
    assert.equal(transfer('export', source).status, 0);
    await writeFile(join(target, 'base-template'), 'new image owns this');
    const refused = transfer('import', target);
    assert.notEqual(refused.status, 0);
    assert.match(refused.stderr, /retained_upgrade_target_not_empty/);
    assert.equal(await readFile(join(source, 'value'), 'utf8'), 'recoverable');
    assert.equal(await readFile(join(target, 'base-template'), 'utf8'), 'new image owns this');
  });
});

test('archive path traversal is rejected before touching replacement or host files', async () => {
  await fixture(async ({ root, source, target, archive, transfer }) => {
    const packed = spawnSync('python3', ['-c', "import io,sys,tarfile\nwith tarfile.open(sys.argv[1], 'w') as t:\n i=tarfile.TarInfo('../escape');i.size=4;t.addfile(i,io.BytesIO(b'evil'))", archive], { encoding: 'utf8', timeout: 1000 });
    assert.equal(packed.status, 0, packed.stderr);
    const refused = transfer('import', target);
    assert.notEqual(refused.status, 0);
    assert.match(refused.stderr, /retained_workspace_archive_invalid/);
    await assert.rejects(readFile(join(root, 'escape')), { code: 'ENOENT' });
  });
});

async function chunkFixture(run) {
  await fixture(async ({ root, source, target, archive, transfer }) => {
    await writeFile(join(source, 'larger-than-a-transfer'), Buffer.alloc(12289, 197));
    const exported = transfer('export', source);
    assert.equal(exported.status, 0, exported.stderr);
    const chunkBytes = 4096, copied = [];
    const guestArchive = '/owned-workspace.tar';
    const operations = (directory, corrupt = false) => ({
      exec: async (command, args) => {
        const mapped = args.map(arg => arg === guestArchive ? join(directory, 'workspace.tar') : arg === guestArchive + '.part' ? join(directory, 'workspace.tar.part') : arg);
        const result = spawnSync(command, mapped, { encoding: 'utf8', timeout: 3000 });
        return { stdout: result.stdout, stderr: result.stderr, exitCode: result.status ?? 1 };
      },
      copyOut: async (guest, host) => {
        const path = join(directory, 'workspace.tar.part');
        const bytes = (await stat(path)).size;
        assert.ok(bytes <= chunkBytes);
        copied.push(bytes);
        await copyFile(path, host);
      },
      copyIn: async (host, guest) => {
        assert.ok((await stat(host)).size <= chunkBytes);
        await copyFile(host, join(directory, 'workspace.tar.part'));
        if (corrupt) await writeFile(join(directory, 'workspace.tar.part'), Buffer.alloc((await stat(host)).size, 1));
      },
    });
    await copyFile(archive, join(source, 'workspace.tar'));
    await run({ root, source, target, archive, exported, chunkBytes, copied, guestArchive, operations });
  });
}

test('bounded chunks transfer an aggregate above the per-file ceiling with exact archive and inventory integrity', async () => {
  await chunkFixture(async ({ root, source, target, archive, exported, chunkBytes, copied, guestArchive, operations }) => {
    await transferWorkspaceArchive({ source: operations(source), target: operations(target), archive: guestArchive,
      hostDirectory: root, chunkBytes, assertCurrent: async () => {} });
    assert.ok((await stat(archive)).size > chunkBytes);
    assert.ok(copied.length > 1);
    assert.equal(copied.reduce((sum, size) => sum + size, 0), (await stat(archive)).size);
    assert.deepEqual(await readFile(join(target, 'workspace.tar')), await readFile(archive));
    await rm(join(source, 'workspace.tar'));
    const restored = join(root, 'restored'); await mkdir(restored);
    const imported = spawnSync('python3', ['-c', workspaceTransferProgram, 'import', join(target, 'workspace.tar'), '[]', restored], { encoding: 'utf8', timeout: 3000 });
    assert.equal(imported.status, 0, imported.stderr);
    assert.equal(imported.stdout, exported.stdout);
    assert.deepEqual(await readFile(join(restored, 'larger-than-a-transfer')), Buffer.alloc(12289, 197));
    await assert.rejects(readFile(join(root, 'workspace.part')), { code: 'ENOENT' });
    assert.ok(workspaceChunkBytes < 512 * 1024 * 1024);
  });
});

test('corrupt chunks and lease loss fail before publishing data and preserve the source archive', async () => {
  await chunkFixture(async ({ root, source, target, archive, chunkBytes, guestArchive, operations }) => {
    const original = await readFile(archive);
    await assert.rejects(transferWorkspaceArchive({ source: operations(source), target: operations(target, true), archive: guestArchive,
      hostDirectory: root, chunkBytes, assertCurrent: async () => {} }), /chunk_append/);
    assert.deepEqual(await readFile(archive), original);
    await assert.rejects(readFile(join(target, 'workspace.tar')), { code: 'ENOENT' });
    await assert.rejects(transferWorkspaceArchive({ source: operations(source), target: operations(target), archive: guestArchive,
      hostDirectory: root, chunkBytes, assertCurrent: async () => { throw Error('retained_fence_lost'); } }), /retained_fence_lost/);
    await assert.rejects(transferWorkspaceArchive({ source: operations(source), target: operations(target), archive: guestArchive,
      hostDirectory: root, chunkBytes: workspaceChunkBytes + 1, assertCurrent: async () => {} }), /chunk_bounds/);
  });
});

test('chunk append refuses duplicate or out-of-order boundaries', async () => {
  await fixture(async ({ root, archive }) => {
    const part = join(root, 'part'); await writeFile(part, 'abcd'); await writeFile(archive, 'existing');
    const result = spawnSync('python3', ['-c', workspaceChunkProgram, 'append', archive, part, '2', '4',
      '88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589'], { encoding: 'utf8', timeout: 3000 });
    assert.notEqual(result.status, 0);
    assert.match(result.stderr, /retained_workspace_chunk_order/);
    assert.equal(await readFile(archive, 'utf8'), 'existing');
  });
});

