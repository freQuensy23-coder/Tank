import { isIP } from 'node:net';

function isolatedHost(value: string): boolean {
  if (['127.0.0.1', 'localhost', 'pg'].includes(value)) return true;
  if (isIP(value) !== 4) return false;
  const octets = value.split('.').map(Number);
  return octets[0] === 10 || (octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31) ||
    (octets[0] === 192 && octets[1] === 168);
}
export function requireDisposablePostgresDsn(value: string | undefined, expectedHost: string | undefined): string {
  if (value === undefined || value.length === 0 || expectedHost === undefined || !isolatedHost(expectedHost)) throw new Error('An explicit disposable persistence database and registered host are required');
  let database: URL;
  try { database = new URL(value); }
  catch { throw new Error('Invalid disposable persistence database'); }
  if (
    !['postgres:', 'postgresql:'].includes(database.protocol) ||
    database.hostname !== expectedHost ||
    database.username !== 'tank_ci' ||
    !/^\/tank_test_[a-z0-9_]+$/.test(database.pathname)
  ) throw new Error('Invalid disposable persistence database');
  return value;
}
