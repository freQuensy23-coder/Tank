/** Native network qualification: KVM guest, DNS and replay isolation. Does NOTHING without the GO flag.
 * Runs in the built OpenMA image with /dev/kvm, a dedicated /proof volume and the readonly sandbox OCI.
 * Three sequential microVMs through the production LiteBoxSandbox: unrestricted (tank-analysis policy),
 * replay (tank-replay policy: only the replay database IPv4), unrestricted again (no policy leak).
 * No model, OpenMA session or Tank state is touched. Passwords are read from mounted files, never printed. */
import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

const env = process.env;
if (env.TANK_NATIVE_NETWORK_GO !== 'AUTHORIZED_NATIVE_PROOF') throw Error('Native network qualification requires explicit GO');
const directory = env.TANK_NATIVE_PROOF_DIR ?? '';
assert.ok(/^\/proof\/[A-Za-z0-9_-]+$/.test(directory), 'dedicated /proof/<run> directory required');
assert.ok(/^\/proof\/[A-Za-z0-9_-]+\/boxlite$/.test(env.BOXLITE_HOME ?? ''), 'BOXLITE_HOME must be a dedicated /proof/<run>/boxlite home');
const databaseIp = env.TANK_NATIVE_DB_IPV4 ?? '';
assert.ok(/^(10\.\d+|172\.(1[6-9]|2\d|3[01])|192\.168)\.\d+\.\d+$/.test(databaseIp), 'private database IPv4 required');
const rootfs = env.TANK_NATIVE_ROOTFS ?? '/opt/tank-sandbox-oci';
const secret = async name => { const value = (env[name] ?? '').trim(); assert.ok(value, name + ' required'); return value; };
const url = (role, password, database) => `postgresql://${role}:${encodeURIComponent(password)}@${databaseIp}:5432/${database}`;
const agentPassword = await secret('TANK_AGENT_DB_PASSWORD');
const replayPassword = await secret('TANK_REPLAY_AGENT_DB_PASSWORD');
const hash = value => createHash('sha256').update(value).digest('hex');

const PROBE = await readFile(new URL('./native_network_probe.py', import.meta.url), 'utf8');

const policies = {
  unrestricted: { mode: 'enabled' },
  replay: { mode: 'enabled', allowNet: [databaseIp] },
};
async function run(name, policy, dbUrl, crossUrl) {
  const { LiteBoxSandbox } = await import('/app/packages/sandbox-adapter-litebox/src/index.ts');
  const scope = join(directory, name);
  await mkdir(join(scope, 'outputs'), { recursive: true });
  await mkdir(join(scope, 'scratch'), { recursive: true });
  const sandbox = new LiteBoxSandbox({
    tankNetwork: policy, rootfsPath: rootfs, memoryMib: 2048, cpus: 2, name: 'tank-native-network-' + name + '-' + randomUUID().slice(0, 8),
    outputsRoot: join(scope, 'outputs'), scratchRoot: join(scope, 'scratch'),
  });
  const started = Date.now();
  try {
    await sandbox.setEnvVars({ TANK_DB_URL: dbUrl, TANK_CROSS_DB_URL: crossUrl, TANK_NATIVE_DB_IPV4: databaseIp });
    const output = await sandbox.exec("python3 - <<'PY'\n" + PROBE + "\nPY", 240000);
    const line = output.split('\n').find(l => l.startsWith('TANKPROBE'));
    assert.ok(line, 'probe produced no result: ' + output.slice(-400));
    const result = { name, policy, bootAndProbeMs: Date.now() - started, probe: JSON.parse(line.slice(9)) };
    await writeFile(join(directory, name + '.json'), JSON.stringify(result, null, 2) + '\n', { mode: 0o600 });
    return result;
  } finally {
    await sandbox.destroy();
  }
}

const tankUrl = url('tank_agent', agentPassword, 'tank');
const replayUrl = url('tank_replay_agent', replayPassword, 'tank_replay');
const crossFromTank = url('tank_agent', agentPassword, 'tank_replay');
const crossFromReplay = url('tank_replay_agent', replayPassword, 'tank');
const results = [];
results.push(await run('unrestricted-before', policies.unrestricted, tankUrl, crossFromTank));
results.push(await run('replay', policies.replay, replayUrl, crossFromReplay));
results.push(await run('unrestricted-after', policies.unrestricted, tankUrl, crossFromTank));

const ok = (result, key) => result.probe[key]?.ok === true;
const failed = [];
const expect = (condition, label) => { if (!condition) failed.push(label); };
for (const result of results) {
  expect(result.probe.machine?.value === 'x86_64', result.name + ': guest is x86_64');
  expect(ok(result, 'db_tcp') && ok(result, 'db_select'), result.name + ': database reachable and readable');
  expect(!ok(result, 'db_write_refused'), result.name + ': database write refused');
  expect(!ok(result, 'db_cross_database'), result.name + ': cross-database login refused');
}
for (const result of [results[0], results[2]]) {
  for (const key of ['dns_getaddrinfo_example.com', 'dns_getaddrinfo_openrouter.ai', 'dns_getaddrinfo_api.etherscan.io',
    'dns_getaddrinfo_postgres', 'dns_udp_configured', 'dns_udp_public_1.1.1.1', 'tls_direct_ip_1.1.1.1', 'tls_direct_ip_8.8.8.8', 'https_example.com', 'https_openrouter_models']) {
    expect(ok(result, key), result.name + ': ' + key + ' succeeds');
  }
}
for (const key of ['dns_getaddrinfo_example.com', 'dns_getaddrinfo_openrouter.ai', 'dns_getaddrinfo_api.etherscan.io',
  'dns_getaddrinfo_postgres', 'dns_udp_configured', 'dns_udp_public_1.1.1.1', 'tls_direct_ip_1.1.1.1', 'tls_direct_ip_8.8.8.8', 'https_example.com', 'https_openrouter_models']) {
  expect(!ok(results[1], key), 'replay: ' + key + ' refused');
}
const summary = {
  status: failed.length ? 'FAIL' : 'PASS',
  failed,
  checks: {
    kvm: results.every(r => r.probe.machine?.value === 'x86_64') ? 'PASS' : 'FAIL',
    dns: failed.some(f => f.includes('dns_')) ? 'FAIL' : 'PASS',
    replay_isolation: failed.some(f => f.startsWith('replay:')) ? 'FAIL' : 'PASS',
  },
  databaseIp,
  imageId: env.TANK_NATIVE_IMAGE_ID,
  ociIndexSha256: hash(await readFile(join(rootfs, 'index.json'))),
  shimSha256: hash(await readFile('/opt/tank-boxlite-runtime/boxlite-shim')),
  finishedAt: new Date().toISOString(),
};
await writeFile(join(directory, 'result.json'), JSON.stringify(summary, null, 2) + '\n', { mode: 0o600 });
console.log(JSON.stringify(summary));
process.exit(failed.length ? 1 : 0);
