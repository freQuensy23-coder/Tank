import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { rm, stat } from 'node:fs/promises';
import { join } from 'node:path';

export const workspaceChunkBytes = 64 * 1024 * 1024;
export const workspaceChunkProgram = String.raw`
import hashlib, json, os, sys
mode, archive, part, offset_text, size_text, expected = sys.argv[1:]
offset, size = int(offset_text), int(size_text)
if offset < 0 or size < 0 or size > 67108864:
    raise RuntimeError('retained_workspace_chunk_bounds')
def checksum(path):
    h = hashlib.sha256()
    with open(path, 'rb') as stream:
        for data in iter(lambda: stream.read(1048576), b''):
            h.update(data)
    return h.hexdigest()
if mode == 'describe':
    print(json.dumps({'bytes': os.stat(archive).st_size, 'sha256': checksum(archive)}))
elif mode == 'read':
    fd = os.open(part, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    h = hashlib.sha256()
    with open(archive, 'rb') as source, os.fdopen(fd, 'wb') as target:
        source.seek(offset)
        remaining = size
        while remaining:
            data = source.read(min(1048576, remaining))
            if not data:
                raise RuntimeError('retained_workspace_chunk_truncated')
            target.write(data)
            h.update(data)
            remaining -= len(data)
    print(h.hexdigest())
elif mode == 'append':
    if os.stat(part).st_size != size or checksum(part) != expected:
        raise RuntimeError('retained_workspace_chunk_integrity')
    flags = os.O_WRONLY | os.O_NOFOLLOW | (os.O_CREAT | os.O_EXCL if offset == 0 else os.O_APPEND)
    fd = os.open(archive, flags, 0o600)
    with os.fdopen(fd, 'ab') as target:
        if os.fstat(target.fileno()).st_size != offset:
            raise RuntimeError('retained_workspace_chunk_order')
        with open(part, 'rb') as source:
            for data in iter(lambda: source.read(1048576), b''):
                target.write(data)
    os.unlink(part)
    print(expected)
elif mode == 'verify':
    if os.stat(archive).st_size != offset or checksum(archive) != expected:
        raise RuntimeError('retained_workspace_archive_integrity')
    print(expected)
else:
    raise RuntimeError('retained_workspace_chunk_mode')
`;
type TransferOperations = {
  exec(command: string, args: string[], env?: Record<string, string>, options?: { cwd?: string; timeoutSecs?: number }): Promise<{ stdout: string; stderr: string; exitCode: number }>;
  copyIn(host: string, guest: string): Promise<void>;
  copyOut(guest: string, host: string): Promise<void>;
};
export async function transferWorkspaceArchive(input: {
  source: TransferOperations; target: TransferOperations; archive: string; hostDirectory: string;
  assertCurrent(): Promise<void>; chunkBytes?: number;
}): Promise<void> {
  const size = input.chunkBytes ?? workspaceChunkBytes;
  if (!Number.isSafeInteger(size) || size < 1 || size > workspaceChunkBytes) throw Error('retained_workspace_chunk_bounds');
  const run = async (operations: TransferOperations, mode: string, part = '', offset = 0, length = 0, hash = '') => {
    await input.assertCurrent();
    const result = await operations.exec('python3', ['-c', workspaceChunkProgram, mode, input.archive, part, String(offset), String(length), hash], undefined, { cwd: '/', timeoutSecs: 120 });
    if (result.exitCode !== 0) throw Error('retained_upgrade_transfer_failed:chunk_' + mode);
    return result.stdout.trim();
  };
  const description = JSON.parse(await run(input.source, 'describe')) as { bytes: number; sha256: string };
  if (!Number.isSafeInteger(description.bytes) || description.bytes < 1 || !/^[a-f0-9]{64}$/.test(description.sha256)) throw Error('retained_upgrade_transfer_failed:archive_descriptor');
  const guestPart = input.archive + '.part', hostPart = join(input.hostDirectory, 'workspace.part');
  try {
    for (let offset = 0; offset < description.bytes; offset += size) {
      const length = Math.min(size, description.bytes - offset);
      const expected = await run(input.source, 'read', guestPart, offset, length);
      if (!/^[a-f0-9]{64}$/.test(expected)) throw Error('retained_upgrade_transfer_failed:chunk_descriptor');
      await input.assertCurrent(); await input.source.copyOut(guestPart, hostPart);
      if ((await stat(hostPart)).size !== length) throw Error('retained_upgrade_transfer_failed:chunk_size');
      const hash = createHash('sha256');
      for await (const bytes of createReadStream(hostPart)) hash.update(bytes);
      if (hash.digest('hex') !== expected) throw Error('retained_upgrade_transfer_failed:chunk_integrity');
      await input.assertCurrent(); await input.target.copyIn(hostPart, guestPart);
      if (await run(input.target, 'append', guestPart, offset, length, expected) !== expected) throw Error('retained_upgrade_transfer_failed:chunk_acknowledgement');
      const removed = await input.source.exec('rm', ['-f', guestPart], undefined, { cwd: '/' });
      if (removed.exitCode !== 0) throw Error('retained_upgrade_transfer_failed:chunk_cleanup');
      await rm(hostPart);
    }
    if (await run(input.target, 'verify', '', description.bytes, 0, description.sha256) !== description.sha256) throw Error('retained_upgrade_transfer_failed:archive_integrity');
  } finally { await rm(hostPart, { force: true }); }
}
