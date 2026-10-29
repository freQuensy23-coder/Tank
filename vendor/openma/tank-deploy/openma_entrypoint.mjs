import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { lookup } from 'node:dns/promises';
import { isIP } from 'node:net';

export function verifyNativePlatform(shim, metadata, system = { platform: process.platform, arch: process.arch }) {
  const architecture = { x64: 'amd64', arm64: 'arm64' }[system.arch];
  if (system.platform !== 'linux' || !architecture) throw Error('OpenMA requires Linux amd64 or arm64');
  const machine = { amd64: 62, arm64: 183 }[architecture];
  if (shim.length < 64 || !shim.subarray(0, 6).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46, 2, 1])) || shim.readUInt16LE(18) !== machine) {
    throw Error('BoxLite shim architecture differs from the OpenMA Linux container');
  }
  const declared = metadata.architecture ?? { aarch64: 'arm64', x86_64: 'amd64' }[metadata.elf?.architecture];
  if (declared && declared !== architecture) throw Error('BoxLite provenance architecture differs from the OpenMA Linux container');
  return architecture;
}

export function verifyNativeQualification(shim, provenance, qualifications, system = { platform: process.platform, arch: process.arch }) {
  const metadata = JSON.parse(provenance.toString('utf8'));
  const architecture = verifyNativePlatform(shim, metadata, system);
  const receipt = qualifications?.[architecture];
  const shimHash = createHash('sha256').update(shim).digest('hex');
  const provenanceHash = createHash('sha256').update(provenance).digest('hex');
  const version = metadata.boxlite_version ?? metadata.source?.version;
  const qualified = architecture === 'amd64'
    ? receipt?.schema === 'tank.boxlite-native-qualification.v1'
      && receipt.status === 'PASS' && receipt.architecture === architecture
      && ['kvm', 'state_persistence', 'dns', 'replay_isolation'].every(check => receipt.checks?.[check] === 'PASS')
    : receipt?.schema === 'tank.boxlite-qualified-replay-dns-artifact.v1'
      && receipt.status === 'QUALIFIED_NATIVE_REPLAY_NETWORK' && receipt.qualification?.status === 'PASS'
      && receipt.source?.version === '0.10.6' && receipt.files?.['boxlite-shim'] === shimHash;
  if (version !== '0.10.6' || !qualified || receipt.shim_sha256 !== shimHash || receipt.provenance_sha256 !== provenanceHash) {
    throw Error(`BoxLite native component qualification missing or invalid for ${architecture}`);
  }
  return architecture;
}

const SECRETS = ['OMA_DB_PASSWORD', 'OMA_API_KEY', 'OMA_ROOT_SECRET', 'OMA_AUTH_SECRET', 'TANK_AGENT_DB_PASSWORD', 'TANK_REPLAY_AGENT_DB_PASSWORD', 'OPENROUTER_API_KEY', 'ETHERSCAN_API_KEY'];

const secret = name => {
  const variable = name.toUpperCase();
  const value = (process.env[variable] ?? '').trim();
  if (!value) throw Error(`Empty required secret: ${variable}`);
  return value;
};

export const SANDBOX_SEARCH_PATH = ['reg', 'ingest', 'evt', 'mkt', 'ana', 'out', 'ops', 'scout', 'qa', 'public'];
const sandboxConnectionOptions = `?options=${encodeURIComponent(`-c search_path=${SANDBOX_SEARCH_PATH.join(',')}`)}`;

export function sandboxDatabaseRoute(env, readSecret) {
  const database = env.TANK_SANDBOX_DATABASE_NAME ?? 'tank';
  const role = env.TANK_SANDBOX_DATABASE_ROLE ?? 'tank_agent';
  for (const [name, value] of Object.entries({ database, role })) {
    if (!/^[a-z_][a-z0-9_]{0,62}$/.test(value)) {
      throw Error(`Invalid sandbox database ${name}; use a lowercase SQL identifier`);
    }
  }
  return `postgresql://${role}:${encodeURIComponent(readSecret('tank_agent_db_password'))}@postgres:5432/${database}${sandboxConnectionOptions}`;
}

export async function replayDatabaseRoute(readSecret, resolve = lookup) {
  const { address } = await resolve('postgres', { family: 4 });
  const octets = address.split('.').map(Number);
  if (isIP(address) !== 4 || !(octets[0] === 10 || (octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31) || (octets[0] === 192 && octets[1] === 168))) {
    throw Error('Replay database must resolve to a private IPv4 service address');
  }
  return { address, url: `postgresql://tank_replay_agent:${encodeURIComponent(readSecret('tank_replay_agent_db_password'))}@${address}:5432/tank_replay${sandboxConnectionOptions}` };
}

export async function main() {
  const nativeRoot = process.env.BOXLITE_RUNTIME_DIR ?? '/opt/tank-boxlite-runtime';
  verifyNativeQualification(
    readFileSync(`${nativeRoot}/boxlite-shim`), readFileSync(`${nativeRoot}/provenance.json`),
    JSON.parse(readFileSync(new URL('../tank-native-qualification.json', import.meta.url), 'utf8')),
  );
  process.env.DATABASE_URL = `postgresql://oma:${encodeURIComponent(secret('oma_db_password'))}@postgres:5432/oma`;
  process.env.API_KEY = secret('oma_api_key');
  process.env.PLATFORM_ROOT_SECRET = secret('oma_root_secret');
  process.env.BETTER_AUTH_SECRET = secret('oma_auth_secret');
  const replay = await replayDatabaseRoute(secret);
  process.env.TANK_SANDBOX_REPLAY_DB_URL = replay.url;
  process.env.TANK_SANDBOX_DATABASE_IPV4 = replay.address;
  process.env.TANK_SANDBOX_DB_URL = sandboxDatabaseRoute(process.env, secret);
  for (const [name, key] of Object.entries({ openrouter_api_key: 'TANK_SANDBOX_OPENROUTER_API_KEY', etherscan_api_key: 'TANK_SANDBOX_ETHERSCAN_API_KEY' })) process.env[key] = secret(name);
  process.env.TANK_SANDBOX_JEV_PROVIDER = 'openrouter';
  for (const variable of SECRETS) delete process.env[variable];
  const child = spawn(process.execPath, [fileURLToPath(new URL('../apps/main-node/node_modules/tsx/dist/cli.mjs', import.meta.url)), 'src/index.ts'], { stdio: 'inherit', env: process.env });
  for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => child.kill(signal));
  child.on('exit', (code, signal) => process.exit(code ?? (signal ? 1 : 0)));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main().catch(error => { console.error(error.message); process.exit(1); });
