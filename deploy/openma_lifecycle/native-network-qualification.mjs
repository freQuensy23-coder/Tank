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
const secret = async name => (await readFile('/run/secrets/' + name, 'utf8')).trim();
const url = (role, password, database) => `postgresql://${role}:${encodeURIComponent(password)}@${databaseIp}:5432/${database}`;
const agentPassword = await secret('tank_agent_db_password');
const replayPassword = await secret('tank_replay_agent_db_password');
const hash = value => createHash('sha256').update(value).digest('hex');

const PROBE = String.raw`
import json, os, socket, ssl, struct, subprocess, time, urllib.request
r = {}
def attempt(name, fn):
    t = time.monotonic()
    try:
        r[name] = {"ok": True, "value": fn()}
    except Exception as e:
        r[name] = {"ok": False, "error": (type(e).__name__ + ": " + str(e))[:240]}
    r[name]["ms"] = round((time.monotonic() - t) * 1000)
def udp_dns(server, name="example.com"):
    q = struct.pack(">HHHHHH", 0x5441, 0x0100, 1, 0, 0, 0) + b"".join(bytes([len(p)]) + p.encode() for p in name.split(".")) + b"\0" + struct.pack(">HH", 1, 1)
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); s.settimeout(4)
    s.sendto(q, (server, 53)); d, _ = s.recvfrom(1500); s.close()
    rcode, answers = d[3] & 15, struct.unpack(">H", d[6:8])[0]
    if rcode != 0 or answers == 0: raise RuntimeError("rcode=%d answers=%d" % (rcode, answers))
    return {"rcode": rcode, "answers": answers}
def tls(host, port=443, sni=None):
    raw = socket.create_connection((host, port), timeout=6)
    with ssl.create_default_context().wrap_socket(raw, server_hostname=sni or host) as s:
        return {"peer": s.getpeername()[0], "version": s.version()}
def https(target):
    with urllib.request.urlopen(target, timeout=12) as response:
        return response.status
def psql(dburl, sql):
    p = subprocess.run(["psql", dburl, "-v", "ON_ERROR_STOP=1", "-Atc", sql], capture_output=True, text=True, timeout=25)
    if p.returncode: raise RuntimeError(p.stderr.strip().splitlines()[-1] if p.stderr.strip() else "exit %d" % p.returncode)
    return p.stdout.strip()
attempt("machine", lambda: os.uname().machine)
attempt("kernel", lambda: os.uname().release)
attempt("hypervisor_cpu_flag", lambda: "hypervisor" in open("/proc/cpuinfo").read())
nameservers = [l.split()[1] for l in open("/etc/resolv.conf") if l.startswith("nameserver")]
r["nameservers"] = nameservers
for host in ("example.com", "openrouter.ai", "api.etherscan.io"):
    attempt("dns_getaddrinfo_" + host, lambda h=host: sorted({a[4][0] for a in socket.getaddrinfo(h, 443, proto=socket.IPPROTO_TCP)}))
attempt("dns_udp_configured", lambda: udp_dns(nameservers[0]))
attempt("dns_udp_public_1.1.1.1", lambda: udp_dns("1.1.1.1"))
attempt("tls_direct_ip_1.1.1.1", lambda: tls("1.1.1.1", sni="one.one.one.one"))
attempt("tls_direct_ip_8.8.8.8", lambda: tls("8.8.8.8", sni="dns.google"))
attempt("https_example.com", lambda: https("https://example.com/"))
attempt("https_openrouter_models", lambda: https("https://openrouter.ai/api/v1/models"))
attempt("db_tcp", lambda: socket.create_connection((os.environ["TANK_NATIVE_DB_IPV4"], 5432), timeout=6).getpeername()[0])
attempt("db_select", lambda: psql(os.environ["TANK_DB_URL"], "select current_user || ' ' || current_database()"))
attempt("db_write_refused", lambda: psql(os.environ["TANK_DB_URL"], "create table tank_native_probe(x int)"))
attempt("db_cross_database", lambda: psql(os.environ["TANK_CROSS_DB_URL"], "select current_database()"))
print("TANKPROBE" + json.dumps(r))
`;

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
    'dns_udp_configured', 'dns_udp_public_1.1.1.1', 'tls_direct_ip_1.1.1.1', 'tls_direct_ip_8.8.8.8', 'https_example.com', 'https_openrouter_models']) {
    expect(ok(result, key), result.name + ': ' + key + ' succeeds');
  }
}
for (const key of ['dns_getaddrinfo_example.com', 'dns_getaddrinfo_openrouter.ai', 'dns_getaddrinfo_api.etherscan.io',
  'dns_udp_configured', 'dns_udp_public_1.1.1.1', 'tls_direct_ip_1.1.1.1', 'tls_direct_ip_8.8.8.8', 'https_example.com', 'https_openrouter_models']) {
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
