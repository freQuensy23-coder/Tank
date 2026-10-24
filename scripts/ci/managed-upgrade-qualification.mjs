import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { randomBytes, createHash } from 'node:crypto';
import { mkdir, writeFile, readFile, mkdtemp, rm, stat } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { completedInputExecutions } from './qualification-execution-state.mjs';
import { trustedLegacyBuild } from '../../vendor/openma/packages/control-plane-node/src/lib/tank-retained/legacy-build-provenance.ts';
import { qualificationExecutor } from './qualification-executor.mjs';
import { historicalController, verifyHistoricalLayout } from './qualification-legacy-fixture.mjs';
import { requiredNativeCases } from './check-sandbox-acceptance.mjs';

const executor = await qualificationExecutor();
const evidence = process.env.TANK_QUALIFICATION_EVIDENCE;
assert.ok(evidence);
await mkdir(evidence, { recursive: true });
const privateDirectory = await mkdtemp(join(process.env.RUNNER_TEMP, 'tank-qualification-private-'));
const sourceSha = process.env.GITHUB_SHA;
const image = process.env.TANK_QUALIFICATION_IMAGE;
assert.match(image ?? '', /^sha256:[a-f0-9]{64}$/);
const rootfs = version => resolve(process.env.RUNNER_TEMP, 'sandbox-' + version);
const kvmGroup = String((await stat('/dev/kvm')).gid);
const observations = [];
const additionalCases = [];
const secretValues = [];
const sanitized = value => secretValues.reduce((text, secret) => text.replaceAll(secret, '[TEST_SECRET]'), String(value));
function docker(args, options = {}) {
  const child = spawnSync('docker', args, { encoding: 'utf8', timeout: 120000, maxBuffer: 8 * 1024 ** 2, ...options });
  assert.equal(child.status, 0, sanitized((child.stderr ?? '') + (child.stdout ?? '') + (child.error?.message ?? '')));
  return child.stdout.trim();
}
async function waitFor(label, predicate, seconds = 180) {
  const deadline = performance.now() + seconds * 1000;
  let last;
  while (performance.now() < deadline) {
    try { const value = await predicate(); if (value) return value; } catch (error) { if (error.fatal) throw error; last = error; }
    await new Promise(done => setTimeout(done, 250));
  }
  throw Error(label + ' deadline exceeded' + (last ? ': ' + sanitized(last.message) : ''));
}
function guestCommand(phase, nonce, uploadedHash, { legacy = false, memory = true, legacyExecutables = null } = {}) {
  const setup = phase === 'A';
  const expectedEffects = phase === 'A' ? [] : phase === 'B-first' ? ['A'] : phase === 'B-second' ? ['A', 'B-first'] : phase === 'policy-recovered' ? ['A', 'B-first', 'B-second', 'metadata'] : ['A', 'B-first', 'B-second'];
  const code = `import hashlib,json,os,subprocess,threading,http.server,socketserver
from pathlib import Path
from playwright.sync_api import sync_playwright
nonce=${JSON.stringify(nonce)}
phase=${JSON.stringify(phase)}
root=Path('/workspace/qualification')
root.mkdir(exist_ok=True)
file=root/'user.bin'
output=Path('/mnt/session/outputs/qualification.bin')
expected=bytes(range(256))+nonce.encode()
if ${setup ? 'True' : 'False'}:
 assert not file.exists(),'initial workspace was not fresh'
 file.write_bytes(expected)
 file.chmod(0o751)
 output.write_bytes(expected[::-1])

 large=root/'large-user-state.bin'
 if not large.exists():large.write_bytes(nonce.encode()*5242880)
assert hashlib.sha256((root/'large-user-state.bin').read_bytes()).hexdigest()==hashlib.sha256(nonce.encode()*5242880).hexdigest(),'large user state changed'
assert file.read_bytes()==expected,'workspace bytes changed'
assert file.stat().st_mode&0o777==0o751,'executable mode changed'
assert output.read_bytes()==expected[::-1],'output bytes changed'
assert hashlib.sha256(Path('/workspace/upload.bin').read_bytes()).hexdigest()==${JSON.stringify(uploadedHash)},'upload bytes changed'
if ${legacy && setup ? 'True' : 'False'}:
 import importlib.util
 assert not Path('/usr/local/bin/tank-upgrade-marker').exists(),'historical image contains a new fixture marker'
 assert importlib.util.find_spec('tank_qualification_version') is None,'historical image contains a new fixture package'
 for path,digest in ${JSON.stringify(legacyExecutables ?? {})}.items():
  assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==digest,'historical executable differs from the verified source OCI: '+path
else:
 assert subprocess.check_output(['/usr/local/bin/tank-upgrade-marker'],text=True).strip()==${JSON.stringify(setup ? 'A' : 'B')},'wrong executable image'
 import tank_qualification_version
 assert tank_qualification_version.VERSION==${JSON.stringify(setup ? 'A' : 'B')},'wrong package image'
ledger=root/'effects.json'
if ${phase === 'policy-recovered' ? 'True' : 'False'}:assert not Path('/workspace/second-upload.bin').exists(),'refused resource reached the retained guest'
previous=json.loads(ledger.read_text()) if ledger.exists() else []
assert previous==${JSON.stringify(expectedEffects)},'duplicate, missing or unordered guest effect: '+str(previous)
class Handler(http.server.BaseHTTPRequestHandler):
 def do_GET(self):
  self.send_response(200);self.end_headers();self.wfile.write(b'<html>isolated qualification browser</html>')
 def log_message(self,*args):pass
class Server(socketserver.TCPServer):allow_reuse_address=True
server=Server(('127.0.0.1',18765),Handler)
threading.Thread(target=server.serve_forever,daemon=True).start()
with sync_playwright() as p:
 browser=p.chromium.launch_persistent_context(str(root/'browser-profile'),headless=True,args=['--no-sandbox'])
 page=browser.pages[0] if browser.pages else browser.new_page()
 page.goto('http://127.0.0.1:18765/')
 if ${setup ? 'True' : 'False'}:
  browser.add_cookies([{'name':'qualification','value':nonce,'url':'http://127.0.0.1:18765/','expires':4102444800}])
  page.evaluate('(v)=>localStorage.setItem("qualification",v)',nonce)
 assert next(c['value'] for c in browser.cookies() if c['name']=='qualification')==nonce,'browser cookie lost'
 assert page.evaluate('localStorage.getItem("qualification")')==nonce,'browser localStorage lost'
 browser.close()
server.shutdown()
memory=Path('/mnt/memory/qualification/note.txt')
if ${memory ? 'True' : 'False'}:
 assert memory.read_text()=='seed:'+nonce+''.join('\\n'+effect for effect in ${JSON.stringify(expectedEffects)}),'writable memory was lost or rebound to different data'
previous.append(phase)
ledger.write_text(json.dumps(previous))
if ${memory ? 'True' : 'False'}:memory.write_text(memory.read_text()+'\\n'+phase)
print('TANK_QUALIFICATION_RECEIPT:'+json.dumps({'phase':phase,'nonce':nonce,'image':${JSON.stringify(setup ? legacy ? 'legacy' : 'A' : 'B')},'workspaceSha256':hashlib.sha256(file.read_bytes()).hexdigest(),'outputSha256':hashlib.sha256(output.read_bytes()).hexdigest(),'uploadSha256':${JSON.stringify(uploadedHash)},'effects':previous,'browserCookie':True,'browserLocalStorage':True,'writableMemory':${memory ? 'True' : 'False'}}))
`;
  return 'python3 -c ' + "'" + code.replaceAll("'", "'\\''") + "'";
}
const imageFacts = JSON.parse(docker(['image', 'inspect', image]))[0];
assert.equal(imageFacts.Architecture, 'amd64');
assert.equal(imageFacts.Config.Labels['org.opencontainers.image.revision'], sourceSha, 'candidate image does not belong to this head');
await writeFile(join(evidence, 'image-inputs.json'), JSON.stringify({ sourceSha, runId: process.env.GITHUB_RUN_ID, image, architecture: imageFacts.Architecture, ociIndexSha256: Object.fromEntries(await Promise.all(['A', 'B'].map(async version => [version, createHash('sha256').update(await readFile(join(rootfs(version), 'index.json'))).digest('hex')]))), nativeArchiveSha256: createHash('sha256').update(await readFile('vendor/openma/tank-native/boxlite-runtime-v0.10.6-tank-replay-dns-amd64.tar.gz')).digest('hex') }, null, 2) + '\n');
const scenarios = ['happy-1', 'happy-2', 'transfer-crash', 'publication-crash', 'lease-concurrent', 'archive-race', 'legacy'];
for (const [index, scenario] of scenarios.entries()) {
  const legacy = scenario === 'legacy';
  const round = index + 1;
  const prefix = 'tank-qual-' + process.env.GITHUB_RUN_ID + '-' + round;
  const names = { network: prefix, postgres: prefix + '-pg', proxy: prefix + '-proxy', provider: prefix + '-provider', service: prefix + '-api', volume: prefix + '-data' };
  const credentials = Object.fromEntries(['POSTGRES_PASSWORD', 'OMA_DB_PASSWORD', 'OMA_API_KEY', 'OMA_ROOT_SECRET', 'OMA_AUTH_SECRET', 'TANK_AGENT_DB_PASSWORD', 'TANK_REPLAY_AGENT_DB_PASSWORD', 'OPENROUTER_API_KEY', 'ETHERSCAN_API_KEY', 'QUALIFICATION_PROVIDER_KEY'].map(name => [name, randomBytes(32).toString('hex')]));
  secretValues.push(...Object.values(credentials));
  const env = { ...process.env, ...credentials, ANTHROPIC_API_KEY: credentials.QUALIFICATION_PROVIDER_KEY };
  const caseName = { 'transfer-crash': 'interrupted-native-transfer', 'publication-crash': 'interrupted-native-publication', 'lease-concurrent': 'lease-loss-concurrent-native-resume', 'archive-race': 'native-archive-publication-race', legacy: 'legacy-native-publication' }[scenario] ?? 'managed-api-A-to-B-' + round;
  const receipt = { case: caseName, sourceSha, runId: process.env.GITHUB_RUN_ID, scenario, image, model: 'isolated deterministic Anthropic-wire provider; no paid model', status: 'FAIL', cleanup: [] };
  const nonce = randomBytes(16).toString('hex');
  const uploaded = Buffer.concat([Buffer.from('qualification-upload:' + nonce), Buffer.from([0, 255, 1, 128])]);
  const uploadedHash = createHash('sha256').update(uploaded).digest('hex');
  let base;
  let activeService = names.service;
  let incarnation = 0;
  const preserveService = async reason => {
    const owner = docker(['inspect', '--format', '{{index .Config.Labels "tank.qualification"}}', activeService]);
    assert.equal(owner, prefix);
    const logs = spawnSync('docker', ['logs', activeService], { encoding: 'utf8', timeout: 10000, maxBuffer: 16 * 1024 ** 2 });
    await writeFile(join(evidence, prefix + '-incarnation-' + incarnation + '-' + reason + '.txt'), sanitized((logs.stdout ?? '') + (logs.stderr ?? '')));
  };
  const sql = query => docker(['exec', names.postgres, 'psql', '-U', 'postgres', '-d', 'oma', '-At', '-c', query]);
  const completed = inputs => {
    const ids = inputs.flatMap(input => input.data.map(row => row.id));
    const rows = JSON.parse(sql("SELECT COALESCE(json_agg(json_build_object('id',id,'session_id',session_id,'state',state,'settled_at_ms',settled_at_ms,'failure',failure)), '[]'::json) FROM managed_session_executions"));
    receipt.executionSettlement = rows;
    return completedInputExecutions(rows, ids, receipt.sessionId);
  };
  const phase = async name => {
    receipt.phase = name;
    receipt.phaseStartedAt = new Date().toISOString();
    console.log(JSON.stringify({ sourceSha, runId: process.env.GITHUB_RUN_ID, scenario, phase: name, at: receipt.phaseStartedAt }));
    await writeFile(join(evidence, 'managed-upgrade-' + round + '.json'), JSON.stringify(receipt, null, 2) + '\n');
  };
  const control = (path, body) => JSON.parse(docker(['exec', names.proxy, 'node', '-e', "fetch('http://127.0.0.1:9001' + process.argv[1],{method:process.argv[2]==='undefined'?'GET':'POST',headers:{'x-api-key':process.env.QUALIFICATION_PROVIDER_KEY,'content-type':'application/json'},body:process.argv[2]==='undefined'?undefined:process.argv[2]}).then(async r=>{if(!r.ok)throw Error(await r.text());console.log(await r.text())})", path, body === undefined ? 'undefined' : JSON.stringify(body)]));
  const transferBarrier = () => {
    const code = "const fs=require('fs');const deadline=Date.now()+180000;const timer=setInterval(()=>{for(const name of fs.readdirSync('/tmp'))if(name.startsWith('tank-runtime-upgrade-')){const p='/tmp/'+name+'/workspace.part';try{const s=fs.statSync(p);if(s.size>0){clearInterval(timer);console.log(JSON.stringify({path:p,bytes:s.size,at:new Date().toISOString()}));return;}}catch{}}if(Date.now()>deadline){clearInterval(timer);process.exit(1)}},5);";
    const child = spawn('docker', ['exec', activeService, 'node', '-e', code], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    child.stdout.on('data', bytes => { stdout += bytes; }); child.stderr.on('data', bytes => { stderr += bytes; });
    return new Promise((resolve, reject) => {
      child.once('error', reject);
      child.once('exit', status => status === 0 ? resolve(JSON.parse(stdout.trim())) : reject(Error('Actual transfer barrier failed: ' + sanitized(stderr))));
    });
  };
  const api = async (path, body, method = body === undefined ? 'GET' : 'POST') => {
    const response = await fetch(base + path, { method, headers: { 'x-api-key': credentials.OMA_API_KEY, 'anthropic-beta': 'managed-agents-2026-04-01,files-api-2025-04-14,agent-memory-2026-07-22', ...(body === undefined || body instanceof FormData ? {} : { 'content-type': 'application/json' }) }, body: body === undefined ? undefined : body instanceof FormData ? body : JSON.stringify(body), signal: AbortSignal.timeout(path.endsWith('/archive') ? 180000 : 15000) });
    const text = await response.text();
    if (!response.ok) throw Object.assign(Error(`Managed API ${method} ${path}: ${response.status} ${sanitized(text)}`), { fatal: true });
    return JSON.parse(text);
  };
  const publication = () => {
    const rows = docker(['exec', names.postgres, 'psql', '-U', 'postgres', '-d', 'oma', '-At', '-c', 'SELECT publication_json FROM runtime_resource_fences WHERE publication_json IS NOT NULL']);
    const values = rows.split('\n').filter(Boolean).map(row => JSON.parse(row));
    assert.equal(values.length, 1);
    const value = values[0];
    if (value.workspaceCandidate.metadata.schema === 1) {
      const metadata = value.workspaceCandidate.metadata;
      return { schema: 1, handle: { provider: metadata.provider, runtimeId: metadata.runtimeId }, state: metadata.state, fingerprint: metadata.fingerprint, contentHash: value.workspaceCandidate.contentHash, scope: metadata.scope, publicationSha256: createHash('sha256').update(rows).digest('hex') };
    }
    const manifest = JSON.parse(value.workspaceCandidate.metadata.manifest);
    return { handle: manifest.handle, imageIdentity: manifest.identity.image, compatibility: manifest.identity.compatibility, state: manifest.state, upgradedFrom: manifest.upgradedFrom, retired: manifest.retired, upgrade: manifest.upgrade, legacy: manifest.legacy, publicationSha256: createHash('sha256').update(rows).digest('hex') };
  };
  const start = async (version, service = names.service) => {
    activeService = service;
    incarnation++;
    const selectedImage = version === 'legacy' ? historicalController.image : image;
    const launcher = version === 'legacy' ? ['--mount', `type=bind,src=${resolve('vendor/openma/tank-deploy/openma_entrypoint.mjs')},dst=/app/tank-deploy/openma_entrypoint.mjs,readonly`] : [];
    const concurrent = scenario === 'lease-concurrent' && version === 'B' ? ['--env', 'QUALIFICATION_PROVIDER_KEY', '--env', 'QUALIFICATION_OWNER_PREFIX=' + prefix, ...['qualification-concurrent-entrypoint.mjs', 'qualification-concurrent-app.mjs'].flatMap(name => ['--mount', `type=bind,src=${resolve('scripts/ci', name)},dst=/${name},readonly`]), '--entrypoint', 'node'] : [];
    docker(['run', '-d', '--name', activeService, '--label', 'tank.qualification=' + prefix, '--network', names.network, '--group-add', kvmGroup, '--device', '/dev/kvm:/dev/kvm', '--mount', `type=volume,src=${names.volume},dst=/app/data`, '--mount', `type=bind,src=${rootfs(version)},dst=/opt/tank-sandbox-oci,readonly`, ...launcher, ...Object.keys(credentials).filter(name => name !== 'POSTGRES_PASSWORD' && name !== 'QUALIFICATION_PROVIDER_KEY').flatMap(name => ['--env', name]), '--env', 'ANTHROPIC_API_KEY', '--env', 'ANTHROPIC_BASE_URL=http://provider:9000/v1', '--env', 'DREAM_CURATOR_MODE=dedup', ...concurrent, selectedImage, ...(concurrent.length ? ['/qualification-concurrent-entrypoint.mjs'] : [])], { env });
    const address = JSON.parse(docker(['inspect', '--format', '{{json .NetworkSettings.Networks}}', activeService]))[names.network]?.IPAddress;
    assert.match(address ?? '', /^(10\.|172\.(1[6-9]|2[0-9]|3[01])\.|192\.168\.)/, 'candidate must have an owned private bridge address');
    base = 'http://' + address + ':8787';
    await waitFor('candidate Managed API health', async () => {
      const state = JSON.parse(docker(['inspect', '--format', '{{json .State}}', activeService]));
      if (!state.Running) throw Object.assign(Error('Shipping OpenMA service exited before health: ' + JSON.stringify({ exitCode: state.ExitCode, error: state.Error, oomKilled: state.OOMKilled })), { fatal: true });
      return (await fetch(base + '/health', { signal: AbortSignal.timeout(2000) })).ok;
    });
  };
  const stop = async () => { docker(['stop', '--time', '45', activeService], { timeout: 60000 }); await preserveService('stopped'); docker(['rm', activeService]); };
  const nativeCatalog = () => JSON.parse(docker(['exec', activeService, 'node', '--input-type=module', '-e', "import {DatabaseSync} from 'node:sqlite';import {createHash} from 'node:crypto';const db=new DatabaseSync(process.env.BOXLITE_HOME+'/db/boxlite.db',{readOnly:true});console.log(JSON.stringify(db.prepare('SELECT id,name,json FROM box_config ORDER BY id').all().map(row=>({id:row.id,name:row.name,configurationSha256:createHash('sha256').update(row.json).digest('hex'),options:JSON.parse(row.json).options}))));db.close();"]));
  let latestHistory = [];
  const events = async id => {
    const collected = [];
    const pages = new Set();
    let page;
    do {
      const result = await api('/v1/sessions/' + id + '/events?order=asc&limit=100' + (page ? '&page=' + encodeURIComponent(page) : ''));
      assert.ok(Array.isArray(result.data)); collected.push(...result.data);
      page = result.next_page;
      assert.ok(!page || !pages.has(page), 'repeated event cursor'); if (page) pages.add(page);
    } while (page);
    assert.equal(new Set(collected.map(row => row.id)).size, collected.length, 'duplicate public event IDs');
    latestHistory = collected;
    await writeFile(join(evidence, 'progress-history-' + round + '.json'), sanitized(JSON.stringify({ sourceSha, runId: process.env.GITHUB_RUN_ID, scenario, events: collected }, null, 2)) + '\n');
    return collected;
  };
  const strings = value => typeof value === 'string' ? [value] : Array.isArray(value) ? value.flatMap(strings) : value && typeof value === 'object' ? Object.values(value).flatMap(strings) : [];
  const receipts = history => history.filter(row => row.type === 'agent.tool_result').flatMap(row => strings(row).flatMap(text => text.split('\n').filter(line => line.startsWith('TANK_QUALIFICATION_RECEIPT:')).map(line => JSON.parse(line.slice('TANK_QUALIFICATION_RECEIPT:'.length)))));
  try {
    await phase('owned-service-bootstrap');
    if (legacy) {
      docker(['pull', '--platform', 'linux/amd64', historicalController.image], { timeout: 300000 });
      const historical = JSON.parse(docker(['image', 'inspect', historicalController.image]))[0];
      assert.equal(historical.Config.Labels['org.opencontainers.image.revision'], historicalController.sourceSha);
      const program = "const fs=require('fs'),crypto=require('crypto');console.log(JSON.stringify(JSON.parse(process.argv[1]).map(row=>({path:row.path,sha256:crypto.createHash('sha256').update(fs.readFileSync('/app/'+row.path)).digest('hex')}))))";
      receipt.historicalSources = JSON.parse(docker(['run', '--rm', '--network', 'none', '--entrypoint', 'node', historicalController.image, '-e', program, JSON.stringify(trustedLegacyBuild.sources)]));
      assert.deepEqual(receipt.historicalSources, trustedLegacyBuild.sources.map(row => ({ path: row.path, sha256: row.importedSha256 })), 'historical controller sources are not the recognized build');
      receipt.historicalLauncherSha256 = createHash('sha256').update(await readFile('vendor/openma/tank-deploy/openma_entrypoint.mjs')).digest('hex');
      const extraction = prefix + '-historical-layout';
      docker(['create', '--name', extraction, '--label', 'tank.qualification=' + prefix, '--network', 'none', '--entrypoint', '/bin/false', historicalController.image]);
      try {
        await mkdir(rootfs('legacy'), { recursive: true });
        docker(['cp', extraction + ':/opt/tank-sandbox-oci/.', rootfs('legacy')], { timeout: 300000 });
      } finally { docker(['rm', extraction]); }
      receipt.historicalIndexSha256 = 'sha256:' + createHash('sha256').update(await readFile(join(rootfs('legacy'), 'index.json'))).digest('hex');
      receipt.historicalFixture = await verifyHistoricalLayout(rootfs('legacy'));
    }
    const init = join(privateDirectory, prefix + '.sql');
    await writeFile(init, `CREATE ROLE oma LOGIN PASSWORD '${credentials.OMA_DB_PASSWORD}';\nCREATE DATABASE oma OWNER oma;\nCREATE ROLE tank_agent LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '${credentials.TANK_AGENT_DB_PASSWORD}';\nALTER ROLE tank_agent SET default_transaction_read_only=on;\nCREATE DATABASE tank;\nCREATE ROLE tank_replay_agent LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '${credentials.TANK_REPLAY_AGENT_DB_PASSWORD}';\nALTER ROLE tank_replay_agent SET default_transaction_read_only=on;\nCREATE DATABASE tank_replay;\n`, { mode: 0o644 });
    docker(['network', 'create', '--internal', '--label', 'tank.qualification=' + prefix, names.network]);
    docker(['volume', 'create', '--label', 'tank.qualification=' + prefix, names.volume]);
    docker(['run', '--rm', '--network', 'none', '--user', '0', '--mount', `type=volume,src=${names.volume},dst=/app/data`, '--entrypoint', 'chown', image, '1000:1000', '/app/data']);
    docker(['run', '-d', '--name', names.postgres, '--label', 'tank.qualification=' + prefix, '--network', names.network, '--network-alias', 'database', '--env', 'POSTGRES_PASSWORD', '--mount', `type=bind,src=${init},dst=/docker-entrypoint-initdb.d/qualification.sql,readonly`, 'postgres:16-bookworm'], { env });
    await waitFor('isolated PostgreSQL bootstrap', () => { const child = spawnSync('docker', ['exec', names.postgres, 'pg_isready', '-h', '127.0.0.1', '-U', 'oma', '-d', 'oma'], { encoding: 'utf8', timeout: 5000 }); return child.status === 0; });
    docker(['run', '-d', '--name', names.proxy, '--label', 'tank.qualification=' + prefix, '--network', names.network, '--network-alias', 'postgres', '--env', 'QUALIFICATION_PROVIDER_KEY', '--mount', `type=bind,src=${resolve('scripts/ci/qualification-postgres-proxy.mjs')},dst=/proxy.mjs,readonly`, '--entrypoint', 'node', image, '/proxy.mjs'], { env });
    await waitFor('owned PostgreSQL transparent proxy', () => control('/state'));
    docker(['run', '-d', '--name', names.provider, '--label', 'tank.qualification=' + prefix, '--network', names.network, '--network-alias', 'provider', '--env', 'QUALIFICATION_PROVIDER_KEY', '--mount', `type=bind,src=${resolve('scripts/ci/qualification-provider.mjs')},dst=/qualification-provider.mjs,readonly`, '--entrypoint', 'node', image, '/qualification-provider.mjs'], { env });
    await start(legacy ? 'legacy' : 'A');
    const agent = await api('/v1/agents', { name: 'Isolated native upgrade qualification', model: 'claude-sonnet-4-6', system: 'Execute the supplied qualification command exactly once. Report tool output. No external requests.', tools: legacy ? [{ type: 'agent_toolset_20260401' }] : [{ type: 'agent_toolset_20260401', default_config: { enabled: false }, configs: [{ name: 'bash', type: 'bash', enabled: true, permission_policy: { type: 'always_allow' } }] }] });
    let memoryStore, memory;
    if (!legacy) {
      memoryStore = await api('/v1/memory_stores', { name: 'qualification', metadata: { qualification: prefix } });
      memory = await api('/v1/memory_stores/' + memoryStore.id + '/memories', { path: '/note.txt', content: 'seed:' + nonce });
      receipt.memoryStoreId = memoryStore.id;
    }
    const memoryContent = () => api('/v1/memory_stores/' + memoryStore.id + '/memories/' + memory.id + '?view=full');
    const environment = await api('/v1/environments', { name: 'Isolated qualification runtime', config: { type: 'cloud', networking: { type: 'unrestricted' }, packages: { apt: [], cargo: [], gem: [], go: [], npm: [], pip: [] } }, metadata: { qualification: prefix } });
    const persistedEnvironment = await api('/v1/environments/' + environment.id);
    assert.deepEqual(persistedEnvironment, environment);
    receipt.environmentId = environment.id;
    const session = await api('/v1/sessions', { agent: agent.id, environment_id: environment.id, resources: legacy ? [] : [{ type: 'memory_store', memory_store_id: memoryStore.id, access: 'read_write' }], metadata: { qualification: prefix } });
    receipt.sessionId = session.id;
    const form = new FormData(); form.set('file', new Blob([uploaded]), 'qualification-upload.bin');
    const file = await api('/v1/files', form);
    await api('/v1/sessions/' + session.id + '/resources', { type: 'file', file_id: file.id, mount_path: '/workspace/upload.bin' });
    const send = phase => api('/v1/sessions/' + session.id + '/events', { events: [{ type: 'user.message', content: [{ type: 'text', text: 'TANK_QUALIFICATION_COMMAND:' + Buffer.from(guestCommand(phase, nonce, uploadedHash, { legacy, memory: !legacy, legacyExecutables: receipt.historicalFixture?.executables })).toString('base64') }] }] });
    receipt.initialInput = await send('A');
    await phase('A-native-execution-and-durable-settlement');
    const initialStarted = performance.now();
    const initialHistory = await waitFor('actual A guest file/browser execution', async () => {
      const rows = await events(session.id);
      if (performance.now() - initialStarted > 30000 && rows.length === 1) {
        receipt.executionDiagnostics = JSON.parse(sql("SELECT COALESCE(json_agg(row_to_json(e)), '[]'::json) FROM managed_session_executions e"));
        if (!receipt.executionDiagnostics.some(row => row.state === 'running' || row.state === 'queued')) throw Object.assign(Error('Accepted A input has no runnable durable execution: ' + sanitized(JSON.stringify(receipt.executionDiagnostics))), { fatal: true });
      }
      if (rows.some(row => row.type === 'session.error')) throw Object.assign(Error(JSON.stringify(rows.filter(row => row.type === 'session.error'))), { fatal: true });
      const ended = rows.some(row => row.type === 'session.status_idle' && row.stop_reason?.type === 'end_turn');
      if (ended && receipts(rows).length === 0) throw Object.assign(Error('A model turn ended without a successful native guest receipt: ' + sanitized(JSON.stringify(rows))), { fatal: true });
      return receipts(rows).length >= 1 && ended && completed([receipt.initialInput]) ? rows : null;
    }, 600);
    receipt.before = publication();
    receipt.nativeBefore = nativeCatalog();
    const memoryBindings = runtimeId => JSON.parse(docker(['exec', activeService, 'node', '--input-type=module', '-e', "import {DatabaseSync} from 'node:sqlite';const db=new DatabaseSync(process.env.BOXLITE_HOME+'/db/boxlite.db',{readOnly:true});const row=db.prepare('SELECT id,name,json FROM box_config WHERE id=?').get(process.argv[1]);if(!row)throw Error('missing actual native catalog row');const data=JSON.parse(row.json);if(data.id!==row.id||data.name!==row.name)throw Error('actual native catalog identity mismatch');console.log(JSON.stringify(data.options.volumes.filter(v=>v.guest_path.startsWith('/mnt/memory/'))));db.close();", runtimeId]));
    if (legacy) {
      assert.equal(receipt.before.schema, 1, 'historical controller did not publish its original schema');
      assert.equal(receipt.before.state, 'retained');
      assert.match(receipt.before.fingerprint, /^sha256:[a-f0-9]{64}$/);
      receipt.historicalNative = receipt.nativeBefore;
      assert.equal(receipt.historicalNative.length, 1);
      assert.equal(receipts(initialHistory)[0].image, 'legacy');
    } else {
      receipt.memoryBefore = await waitFor('A writable-memory synchronization through Managed API', async () => { const value = await memoryContent(); return value.content === 'seed:' + nonce + '\nA' ? value : null; });
      receipt.memoryBindingsBefore = memoryBindings(receipt.before.handle.runtimeId);
      assert.equal(receipt.memoryBindingsBefore.length, 1);
      assert.equal(receipt.memoryBindingsBefore[0].guest_path, '/mnt/memory/qualification');
      assert.equal(receipt.memoryBindingsBefore[0].read_only, false);
      assert.ok(receipt.memoryBindingsBefore[0].host_path.includes(memoryStore.id));
    }
    receipt.historyBefore = initialHistory.map(row => ({ id: row.id, sha256: createHash('sha256').update(JSON.stringify(row)).digest('hex') }));
    await phase('B-service-restart');
    await stop();
    await start('B');
    const actualTransfer = ['transfer-crash', 'lease-concurrent'].includes(scenario) ? transferBarrier() : null;
    if (['publication-crash', 'archive-race'].includes(scenario)) control('/arm', { mode: scenario === 'publication-crash' ? 'publication-ack' : 'publication-before', sessionId: session.id });
    receipt.upgradeInput = await send('B-first');
    receipt.queuedInput = await send('B-second');
    await phase('B-upgrade-interruption-and-queued-inputs');
    receipt.queueObserved = JSON.parse(docker(['exec', names.postgres, 'psql', '-U', 'postgres', '-d', 'oma', '-At', '-c', "SELECT COALESCE(json_agg(json_build_object('id',id,'state',state)), '[]'::json) FROM managed_session_executions"]));
    assert.ok(receipt.queueObserved.some(row => row.state === 'queued'), 'No actual durably queued input was observed during replacement');
    if (actualTransfer) {
      receipt.transferBarrier = await actualTransfer;
      receipt.interruptedPublication = publication();
      assert.equal(receipt.interruptedPublication.state, 'upgrading', 'observed bytes were not an in-progress upgrade');
      assert.deepEqual(receipt.interruptedPublication.handle, receipt.before.handle, 'incomplete transfer became authoritative');
      if (scenario === 'transfer-crash') {
        docker(['kill', '--signal', 'KILL', activeService]); await preserveService('transfer-killed'); docker(['rm', activeService]);
        assert.deepEqual(publication().handle, receipt.before.handle, 'crash deleted the authoritative source');
        await start('B');
      } else {
        receipt.ownerBeforePartition = JSON.parse(sql("SELECT row_to_json(r) FROM (SELECT generation,owner_id,expires_at_ms FROM runtime_resource_fences) r"));
        receipt.partition = control('/partition', {});
        assert.equal(receipt.partition.partitioned, true);
        await waitFor('actual resource owner lease expiry without clock/SQL mutation', () => Number(sql('SELECT expires_at_ms FROM runtime_resource_fences')) <= Date.now(), 90);
        receipt.concurrentControllers = JSON.parse(docker(['exec', activeService, 'node', '-e', "fetch('http://127.0.0.1:8790/start-competitor',{method:'POST',headers:{'x-api-key':process.env.QUALIFICATION_PROVIDER_KEY}}).then(async r=>{if(!r.ok)throw Error(await r.text());console.log(await r.text())})"]));
        assert.equal(receipt.concurrentControllers.sharedNativeRuntime, true);
        assert.equal(receipt.concurrentControllers.nativeBindings, 2);
        assert.deepEqual(receipt.concurrentControllers.owners, [prefix + ':primary', prefix + ':competitor']);
        await waitFor('competing real controller takes a newer resource generation', () => Number(sql('SELECT generation FROM runtime_resource_fences')) > Number(receipt.ownerBeforePartition.generation), 180);
        receipt.reconnection = control('/release', {});
        const address = JSON.parse(docker(['inspect', '--format', '{{json .NetworkSettings.Networks}}', activeService]))[names.network].IPAddress;
        base = 'http://' + address + ':8788';
      }
    }
    if (['publication-crash', 'archive-race'].includes(scenario)) {
      receipt.publicationBarrier = await waitFor('actual final publication wire boundary', () => control('/state').barrier, 600);
      if (scenario === 'publication-crash') {
        receipt.committedBeforeLostAck = await waitFor('durable target publication before acknowledgement', () => { const value = publication(); return value.state === 'retained' && value.handle.runtimeId !== receipt.before.handle.runtimeId ? value : null; });
        docker(['kill', '--signal', 'KILL', activeService]); await preserveService('publication-killed'); docker(['rm', activeService]);
        control('/release', {});
        await start('B');
      } else {
        const archived = api('/v1/sessions/' + session.id + '/archive', {});
        void archived.catch(() => undefined);
        await waitFor('durable archival while replacement publication is blocked', () => sql('SELECT archived_at IS NOT NULL FROM managed_sessions').trim() === 't');
        receipt.atArchive = publication();
        control('/release', {});
        await archived;
        receipt.archived = await waitFor('owned native archival and replacement fencing', () => { const value = publication(); return value.state === 'archived' ? value : null; }, 180);
        receipt.nativeAfterArchive = nativeCatalog();
        assert.deepEqual(receipt.nativeAfterArchive, [], 'archival retained owned source/target native configurations');
        const afterArchive = await events(session.id);
        for (let index = 0; index < initialHistory.length; index++) assert.deepEqual(afterArchive[index], initialHistory[index]);
        assert.deepEqual(receipts(afterArchive).map(row => row.phase), ['A'], 'archived race reached model/tool effects');
        receipt.guestReceipts = receipts(afterArchive);
        receipt.status = 'PASS';
        continue;
      }
    }
    const history = await waitFor('same-session B upgrade and queued-input completion', async () => {
      const rows = await events(session.id);
      if (receipts(rows).length >= 3 && completed([receipt.upgradeInput, receipt.queuedInput])) return rows;
      if (Number(sql("SELECT count(*) FROM managed_session_executions WHERE state IN ('running','queued')")) === 0) throw Object.assign(Error('B inputs settled without complete native receipts: ' + sanitized(JSON.stringify(rows))), { fatal: true });
      return null;
    }, 600);
    receipt.after = publication();
    receipt.nativeAfter = nativeCatalog();
    if (receipt.committedBeforeLostAck) assert.deepEqual(receipt.after.handle, receipt.committedBeforeLostAck.handle, 'lost publication acknowledgement caused a second replacement');
    if (scenario === 'lease-concurrent') {
      receipt.ownerAfterPartition = JSON.parse(sql("SELECT row_to_json(r) FROM (SELECT generation,owner_id,expires_at_ms FROM runtime_resource_fences) r"));
      assert.ok(Number(receipt.ownerAfterPartition.generation) > Number(receipt.ownerBeforePartition.generation), 'expired owner was not fenced by a new generation');
      await waitFor('both queued inputs durably settled under competing resumes', () => JSON.parse(sql("SELECT COALESCE(json_agg(id), '[]'::json) FROM managed_session_executions WHERE state IN ('running','queued')")).length === 0);
    }
    if (legacy) {
      assert.equal(receipt.after.legacy.fingerprint, receipt.before.fingerprint, 'migration did not preserve the actual historical fingerprint');
      assert.equal(receipt.after.legacy.runtimeId, receipt.before.handle.runtimeId);
      assert.equal(receipt.after.legacy.image, trustedLegacyBuild.image);
      assert.equal(receipt.after.upgradedFrom, trustedLegacyBuild.image);
    } else {
      receipt.memoryAfter = await waitFor('B writable-memory synchronization through Managed API', async () => { const value = await memoryContent(); return value.content === 'seed:' + nonce + '\nA\nB-first\nB-second' ? value : null; });
      receipt.memoryBindingsAfter = memoryBindings(receipt.after.handle.runtimeId);
      assert.deepEqual(receipt.memoryBindingsAfter, receipt.memoryBindingsBefore, 'restart/upgrade rebound a writable memory store to an unproved incarnation');
      receipt.memoryVerified = true;
    }
    assert.notEqual(receipt.before.handle.runtimeId, receipt.after.handle.runtimeId, 'image B did not replace A');
    assert.notEqual(legacy ? trustedLegacyBuild.image : receipt.before.imageIdentity, receipt.after.imageIdentity, 'image identity did not change');
    if (!legacy) assert.deepEqual(receipt.before.compatibility, receipt.after.compatibility, 'protected compatibility changed');
    for (let index = 0; index < initialHistory.length; index++) assert.deepEqual(history[index], initialHistory[index], 'history changed or reordered across upgrade');
    receipt.historyAfter = history.map(row => ({ id: row.id, type: row.type, sha256: createHash('sha256').update(JSON.stringify(row)).digest('hex') }));
    assert.deepEqual(receipts(history).map(row => row.phase), ['A', 'B-first', 'B-second'], 'duplicate, missing or unordered native command results');
    for (const row of receipts(history)) {
      assert.equal(row.nonce, nonce);
      assert.equal(row.uploadSha256, uploadedHash);
      assert.equal(row.browserCookie, true);
      assert.equal(row.browserLocalStorage, true);
    }
    assert.deepEqual(receipts(history).at(-1).effects, ['A', 'B-first', 'B-second']);
    receipt.guestReceipts = receipts(history);
    receipt.providerObservations = JSON.parse(docker(['exec', names.provider, 'node', '-e', "fetch('http://127.0.0.1:9000/observations',{headers:{'x-api-key':process.env.QUALIFICATION_PROVIDER_KEY}}).then(r=>r.json()).then(v=>console.log(JSON.stringify(v)))"]));
    assert.equal(receipt.providerObservations.filter(row => row.kind === 'tool_use').length, 3, 'model reissued or skipped a native tool effect');
    assert.equal(new Set(receipt.providerObservations.filter(row => row.kind === 'tool_use').map(row => row.commandSha256)).size, 3);
    assert.ok(receipt.providerObservations.some(row => row.upgradedNoticePresent), 'Continuing agent did not actually receive the runtime upgrade notice');
    receipt.guestReceiptCount = receipts(history).length;
    await phase('B-native-receipts-and-durable-settlement-verified');
    receipt.status = 'PASS';
    if (round === 1) {
      const metadata = { case: 'metadata-only-native-rebuild', sourceSha, sessionId: session.id, status: 'FAIL' };
      try {
        await stop();
        await start('metadata');
        metadata.input = await send('metadata');
        await phase('metadata-native-continuation-and-durable-settlement');
        const unchangedHistory = await waitFor('metadata-only retained VM continuation', async () => {
          const rows = await events(session.id);
          const settled = completed([metadata.input]);
          if (settled && receipts(rows).length < 4) throw Object.assign(Error('Metadata input settled without its native receipt: ' + sanitized(JSON.stringify(rows.filter(row => row.type === 'agent.tool_result' || row.type === 'session.error')))), { fatal: true });
          return receipts(rows).length >= 4 && settled ? rows : null;
        }, 600);
        metadata.publication = publication();
        assert.deepEqual(metadata.publication.handle, receipt.after.handle, 'metadata-only OCI export replaced the VM');
        assert.equal(metadata.publication.imageIdentity, receipt.after.imageIdentity);
        for (let index = 0; index < history.length; index++) assert.deepEqual(unchangedHistory[index], history[index]);
        assert.deepEqual(receipts(unchangedHistory).map(row => row.phase), ['A', 'B-first', 'B-second', 'metadata']);
        metadata.status = 'PASS';
      } catch (error) { metadata.error = sanitized(error.stack ?? error); process.exitCode = 1; }
      additionalCases.push(metadata);
      if (metadata.status === 'PASS') {
        const policy = { case: 'security-mismatch-native-refusal', sourceSha, sessionId: session.id, status: 'FAIL' };
        try {
          const priorCalls = JSON.parse(docker(['exec', names.provider, 'node', '-e', "fetch('http://127.0.0.1:9000/observations',{headers:{'x-api-key':process.env.QUALIFICATION_PROVIDER_KEY}}).then(r=>r.json()).then(v=>console.log(JSON.stringify(v)))"]));
          const resource = await api('/v1/sessions/' + session.id + '/resources', { type: 'file', file_id: file.id, mount_path: '/workspace/second-upload.bin' });
          policy.refusedInput = await send('policy-probe');
          const refusal = await waitFor('protected resources mismatch refusal', async () => (await events(session.id)).find(row => row.type === 'session.error' && JSON.stringify(row).includes('retained_configuration_changed:resources')), 180);
          policy.errorEventId = refusal.id;
          policy.publication = publication();
          assert.deepEqual(policy.publication.handle, metadata.publication.handle, 'policy refusal changed the authoritative VM');
          const afterCalls = JSON.parse(docker(['exec', names.provider, 'node', '-e', "fetch('http://127.0.0.1:9000/observations',{headers:{'x-api-key':process.env.QUALIFICATION_PROVIDER_KEY}}).then(r=>r.json()).then(v=>console.log(JSON.stringify(v)))"]));
          assert.deepEqual(afterCalls, priorCalls, 'refused policy change reached the model');
          policy.execution = await waitFor('exact incompatible input terminal settlement', () => {
            const id = policy.refusedInput.data[0].id;
            const rows = JSON.parse(sql("SELECT COALESCE(json_agg(json_build_object('id',id,'state',state,'settled_at_ms',settled_at_ms)), '[]'::json) FROM managed_session_executions"));
            return rows.find(row => row.id === id && row.state === 'failed' && Number(row.settled_at_ms) > 0);
          });
          await api('/v1/sessions/' + session.id + '/resources/' + resource.id, undefined, 'DELETE');
          policy.recoveredInput = await send('policy-recovered');
          const recovered = await waitFor('real retained guest remains unchanged after protected context refusal', async () => {
            const rows = await events(session.id);
            return completed([policy.recoveredInput]) && receipts(rows).some(row => row.phase === 'policy-recovered') ? rows : null;
          }, 600);
          policy.recoveredPublication = publication();
          assert.deepEqual(policy.recoveredPublication.handle, metadata.publication.handle, 'policy refusal/recovery replaced the real retained VM');
          policy.guestReceipt = receipts(recovered).at(-1);
          assert.deepEqual(policy.guestReceipt.effects, ['A', 'B-first', 'B-second', 'metadata', 'policy-recovered']);
          policy.status = 'PASS';
        } catch (error) { policy.error = sanitized(error.stack ?? error); process.exitCode = 1; }
        additionalCases.push(policy);
      }
    }
  } catch (error) {
    receipt.error = sanitized(error.stack ?? error);
    process.exitCode = 1;
  } finally {
    await writeFile(join(evidence, 'public-history-' + round + '.json'), sanitized(JSON.stringify({ sourceSha, runId: process.env.GITHUB_RUN_ID, sessionId: receipt.sessionId ?? null, events: latestHistory }, null, 2)) + '\n');
    if (receipt.status !== 'PASS') {
      try { receipt.executionDiagnostics = JSON.parse(sql("SELECT COALESCE(json_agg(row_to_json(e)), '[]'::json) FROM managed_session_executions e")); } catch (error) { receipt.diagnosticError = sanitized(error.message); }
    }
    for (const name of [names.service, names.provider, names.proxy, names.postgres]) {
      const inspect = spawnSync('docker', ['inspect', '--format', '{{index .Config.Labels "tank.qualification"}}', name], { encoding: 'utf8', timeout: 10000 });
      if (inspect.status !== 0) continue;
      assert.equal(inspect.stdout.trim(), prefix, 'cleanup ownership mismatch');
      const logs = spawnSync('docker', ['logs', name], { encoding: 'utf8', timeout: 10000, maxBuffer: 16 * 1024 ** 2 });
      await writeFile(join(evidence, name + '.txt'), sanitized((logs.stdout ?? '') + (logs.stderr ?? '')));
      docker(['rm', '-f', name]); receipt.cleanup.push(name);
    }
    for (const [kind, name] of [['volume', names.volume], ['network', names.network]]) {
      const inspect = spawnSync('docker', [kind, 'inspect', '--format', '{{index .Labels "tank.qualification"}}', name], { encoding: 'utf8', timeout: 10000 });
      if (inspect.status !== 0) continue;
      assert.equal(inspect.stdout.trim(), prefix, 'cleanup resource ownership mismatch');
      docker([kind, 'rm', name]); receipt.cleanup.push(name);
    }
    observations.push(receipt);
    await writeFile(join(evidence, 'managed-upgrade-' + round + '.json'), JSON.stringify(receipt, null, 2) + '\n');
  }
}
const happyRounds = observations.filter(row => row.scenario.startsWith('happy-'));
additionalCases.push({ case: 'writable-memory-restart-A-to-B', sourceSha, status: happyRounds.length === 2 && happyRounds.every(row => row.status === 'PASS' && row.memoryVerified) ? 'PASS' : 'FAIL', rounds: happyRounds.map(row => ({ case: row.case, memoryStoreId: row.memoryStoreId, bindingsBefore: row.memoryBindingsBefore, bindingsAfter: row.memoryBindingsAfter, verified: row.memoryVerified === true })) });
await rm(privateDirectory, { recursive: true, force: true });
const cases = [];
const inventory = [...observations, ...additionalCases];
for (const name of requiredNativeCases) if (!inventory.some(row => row.case === name)) inventory.push({ case: name, sourceSha, status: 'NOT_RUN', execution: 'not-observed', error: 'Required predecessor did not complete; no native execution receipt exists' });
for (const row of inventory) {
  row.runId = process.env.GITHUB_RUN_ID;
  const receipt = row.case + '.json';
  const bytes = JSON.stringify(row, null, 2) + '\n';
  await writeFile(join(evidence, receipt), bytes, { flag: 'wx' });
  cases.push({ case: row.case, status: row.status, receipt, receiptSha256: createHash('sha256').update(bytes).digest('hex') });
}
assert.deepEqual(cases.map(row => row.case).sort(), [...requiredNativeCases].sort());
const unproved = cases.filter(row => row.status !== 'PASS').map(row => row.case);
await writeFile(join(evidence, 'native-summary.json'), JSON.stringify({ schema: 'tank.native-managed-upgrade.v1', sourceSha, runId: process.env.GITHUB_RUN_ID, executor, cases, fullyQualified: unproved.length === 0, unproved }, null, 2) + '\n', { flag: 'wx' });
assert.deepEqual(unproved, [], 'Every required native case must have an actual passing execution');
