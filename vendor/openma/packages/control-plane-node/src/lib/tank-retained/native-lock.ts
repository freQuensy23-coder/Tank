import { spawn } from 'node:child_process';
import { mkdir, open } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join, resolve } from 'node:path';

export function boxLiteHome(): string {
  return resolve(process.env.BOXLITE_HOME || join(homedir(), '.boxlite'));
}

export async function withNativeLock<T>(
  home: string, name: string, mode: 'shared' | 'exclusive', operation: () => Promise<T>,
): Promise<T> {
  const directory = join(home, '.tank-retained-locks');
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const file = await open(join(directory, name), 'a', 0o600);
  try {
    await new Promise<void>((resolveLock, reject) => {
      const child = spawn('flock', ['-n', mode === 'shared' ? '-s' : '-x', '-E', '75', '3'],
        { stdio: ['ignore', 'ignore', 'ignore', file.fd] });
      child.once('error', () => reject(Error('retained_native_lock_unavailable')));
      child.once('exit', code => {
        if (code === 0) resolveLock();
        else reject(Error(code === 75 ? 'retained_native_busy' : 'retained_native_lock_unavailable'));
      });
    });
    return await operation();
  } finally {
    await file.close();
  }
}
