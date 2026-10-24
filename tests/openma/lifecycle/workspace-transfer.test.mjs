import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, mkdir, writeFile, readFile, rm, symlink, readlink, chmod, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { workspaceTransferProgram } from '../../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/workspace-transfer.ts';

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
