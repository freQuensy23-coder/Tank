export function requireDisposablePostgresDsn(value: string | undefined): string {
  if (value === undefined || value.length === 0) throw new Error('An explicit disposable persistence database is required');
  let database: URL;
  try { database = new URL(value); }
  catch { throw new Error('Invalid disposable persistence database'); }
  if (
    !['postgres:', 'postgresql:'].includes(database.protocol) ||
    !['127.0.0.1', 'localhost', 'pg'].includes(database.hostname) ||
    database.username !== 'tank_ci' ||
    !/^\/tank_test_[a-z0-9_]+$/.test(database.pathname)
  ) throw new Error('Invalid disposable persistence database');
  return value;
}
