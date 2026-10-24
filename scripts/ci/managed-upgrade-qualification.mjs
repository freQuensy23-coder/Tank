import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { randomBytes, createHash } from 'node:crypto';
import { mkdir, writeFile, readFile, mkdtemp, rm, stat } from 'node:fs/promises';
import { join, resolve } from 'node:path';

assert.equal(process.env.GITHUB_ACTIONS, 'true');
assert.equal(process.env.RUNNER_ENVIRONMENT, 'github-hosted');
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
function guestCommand(phase, nonce, uploadedHash) {
  const setup = phase === 'A';
  const expectedEffects = phase === 'A' ? [] : phase === 'B-first' ? ['A'] : phase === 'B-second' ? ['A', 'B-first'] : ['A', 'B-first', 'B-second'];
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
assert file.read_bytes()==expected,'workspace bytes changed'
assert file.stat().st_mode&0o777==0o751,'executable mode changed'
assert output.read_bytes()==expected[::-1],'output bytes changed'
assert hashlib.sha256(Path('/workspace/upload.bin').read_bytes()).hexdigest()==${JSON.stringify(uploadedHash)},'upload bytes changed'
assert subprocess.check_output(['/usr/local/bin/tank-upgrade-marker'],text=True).strip()==${JSON.stringify(setup ? 'A' : 'B')},'wrong executable image'
import tank_qualification_version
assert tank_qualification_version.VERSION==${JSON.stringify(setup ? 'A' : 'B')},'wrong package image'
ledger=root/'effects.json'
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
assert memory.read_text()=='seed:'+nonce+''.join('\\n'+effect for effect in ${JSON.stringify(expectedEffects)}),'writable memory was lost or rebound to different data'
previous.append(phase)
ledger.write_text(json.dumps(previous))
memory.write_text(memory.read_text()+'\\n'+phase)
print('TANK_QUALIFICATION_RECEIPT:'+json.dumps({'phase':phase,'nonce':nonce,'image':${JSON.stringify(setup ? 'A' : 'B')},'workspaceSha256':hashlib.sha256(file.read_bytes()).hexdigest(),'outputSha256':hashlib.sha256(output.read_bytes()).hexdigest(),'uploadSha256':${JSON.stringify(uploadedHash)},'effects':previous,'browserCookie':True,'browserLocalStorage':True,'writableMemory':True}))
`;
  return 'python3 -c ' + "'" + code.replaceAll("'", "'\\''") + "'";
}
const imageFacts = JSON.parse(docker(['image', 'inspect', image]))[0];
assert.equal(imageFacts.Architecture, 'amd64');
assert.equal(imageFacts.Config.Labels['org.opencontainers.image.revision'], sourceSha, 'candidate image does not belong to this head');
await writeFile(join(evidence, 'image-inputs.json'), JSON.stringify({ sourceSha, runId: process.env.GITHUB_RUN_ID, image, architecture: imageFacts.Architecture, ociIndexSha256: Object.fromEntries(await Promise.all(['A', 'B'].map(async version => [version, createHash('sha256').update(await readFile(join(rootfs(version), 'index.json'))).digest('hex')]))), nativeArchiveSha256: createHash('sha256').update(await readFile('vendor/openma/tank-native/boxlite-runtime-v0.10.6-tank-replay-dns-amd64.tar.gz')).digest('hex') }, null, 2) + '\n');
for (let round = 1; round <= 2; round++) {
  const prefix = 'tank-qual-' + process.env.GITHUB_RUN_ID + '-' + round;
  const names = { network: prefix, postgres: prefix + '-pg', provider: prefix + '-provider', service: prefix + '-api', volume: prefix + '-data' };
  const credentials = Object.fromEntries(['POSTGRES_PASSWORD', 'OMA_DB_PASSWORD', 'OMA_API_KEY', 'OMA_ROOT_SECRET', 'OMA_AUTH_SECRET', 'TANK_AGENT_DB_PASSWORD', 'TANK_REPLAY_AGENT_DB_PASSWORD', 'OPENROUTER_API_KEY', 'ETHERSCAN_API_KEY', 'QUALIFICATION_PROVIDER_KEY'].map(name => [name, randomBytes(32).toString('hex')]));
  secretValues.push(...Object.values(credentials));
  const env = { ...process.env, ...credentials, ANTHROPIC_API_KEY: credentials.QUALIFICATION_PROVIDER_KEY };
  const receipt = { case: 'managed-api-A-to-B-' + round, sourceSha, image, model: 'isolated deterministic Anthropic-wire provider; no paid model', status: 'FAIL', cleanup: [] };
  const nonce = randomBytes(16).toString('hex');
  const uploaded = Buffer.concat([Buffer.from('qualification-upload:' + nonce), Buffer.from([0, 255, 1, 128])]);
  const uploadedHash = createHash('sha256').update(uploaded).digest('hex');
  let base;
  const api = async (path, body, method = body === undefined ? 'GET' : 'POST') => {
    const response = await fetch(base + path, { method, headers: { 'x-api-key': credentials.OMA_API_KEY, 'anthropic-beta': 'managed-agents-2026-04-01,files-api-2025-04-14', ...(body === undefined || body instanceof FormData ? {} : { 'content-type': 'application/json' }) }, body: body === undefined ? undefined : body instanceof FormData ? body : JSON.stringify(body), signal: AbortSignal.timeout(15000) });
    const text = await response.text();
    assert.ok(response.ok, `Managed API ${method} ${path}: ${response.status} ${sanitized(text)}`);
    return JSON.parse(text);
  };
  const publication = () => {
    const rows = docker(['exec', names.postgres, 'psql', '-U', 'postgres', '-d', 'oma', '-At', '-c', 'SELECT publication_json FROM runtime_resource_fences WHERE publication_json IS NOT NULL']);
    const values = rows.split('\n').filter(Boolean).map(row => JSON.parse(row));
    assert.equal(values.length, 1);
    const value = values[0];
    const manifest = JSON.parse(value.workspaceCandidate.metadata.manifest);
    return { handle: manifest.handle, imageIdentity: manifest.identity.image, compatibility: manifest.identity.compatibility, state: manifest.state, upgradedFrom: manifest.upgradedFrom, retired: manifest.retired };
  };
  const start = async version => {
    docker(['run', '-d', '--name', names.service, '--label', 'tank.qualification=' + prefix, '--network', names.network, '--group-add', kvmGroup, '--device', '/dev/kvm:/dev/kvm', '--mount', `type=volume,src=${names.volume},dst=/app/data`, '--mount', `type=bind,src=${rootfs(version)},dst=/opt/tank-sandbox-oci,readonly`, ...Object.keys(credentials).filter(name => name !== 'POSTGRES_PASSWORD' && name !== 'QUALIFICATION_PROVIDER_KEY').flatMap(name => ['--env', name]), '--env', 'ANTHROPIC_API_KEY', '--env', 'ANTHROPIC_BASE_URL=http://provider:9000/v1', '--env', 'DREAM_CURATOR_MODE=dedup', image], { env });
    const address = JSON.parse(docker(['inspect', '--format', '{{json .NetworkSettings.Networks}}', names.service]))[names.network]?.IPAddress;
    assert.match(address ?? '', /^(10\.|172\.(1[6-9]|2[0-9]|3[01])\.|192\.168\.)/, 'candidate must have an owned private bridge address');
    base = 'http://' + address + ':8787';
    await waitFor('candidate Managed API health', async () => {
      const state = JSON.parse(docker(['inspect', '--format', '{{json .State}}', names.service]));
      if (!state.Running) throw Object.assign(Error('Shipping OpenMA service exited before health: ' + JSON.stringify({ exitCode: state.ExitCode, error: state.Error, oomKilled: state.OOMKilled })), { fatal: true });
      return (await fetch(base + '/health', { signal: AbortSignal.timeout(2000) })).ok;
    });
  };
  const stop = () => { docker(['stop', '--time', '45', names.service], { timeout: 60000 }); docker(['rm', names.service]); };
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
    return collected;
  };
  const strings = value => typeof value === 'string' ? [value] : Array.isArray(value) ? value.flatMap(strings) : value && typeof value === 'object' ? Object.values(value).flatMap(strings) : [];
  const receipts = history => history.filter(row => row.type === 'agent.tool_result').flatMap(row => strings(row).flatMap(text => text.split('\n').filter(line => line.startsWith('TANK_QUALIFICATION_RECEIPT:')).map(line => JSON.parse(line.slice('TANK_QUALIFICATION_RECEIPT:'.length)))));
  try {
    const init = join(privateDirectory, prefix + '.sql');
    await writeFile(init, `CREATE ROLE oma LOGIN PASSWORD '${credentials.OMA_DB_PASSWORD}';\nCREATE DATABASE oma OWNER oma;\nCREATE ROLE tank_agent LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '${credentials.TANK_AGENT_DB_PASSWORD}';\nALTER ROLE tank_agent SET default_transaction_read_only=on;\nCREATE DATABASE tank;\nCREATE ROLE tank_replay_agent LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '${credentials.TANK_REPLAY_AGENT_DB_PASSWORD}';\nALTER ROLE tank_replay_agent SET default_transaction_read_only=on;\nCREATE DATABASE tank_replay;\n`, { mode: 0o644 });
    docker(['network', 'create', '--internal', '--label', 'tank.qualification=' + prefix, names.network]);
    docker(['volume', 'create', '--label', 'tank.qualification=' + prefix, names.volume]);
    docker(['run', '--rm', '--network', 'none', '--user', '0', '--mount', `type=volume,src=${names.volume},dst=/app/data`, '--entrypoint', 'chown', image, '1000:1000', '/app/data']);
    docker(['run', '-d', '--name', names.postgres, '--label', 'tank.qualification=' + prefix, '--network', names.network, '--network-alias', 'postgres', '--env', 'POSTGRES_PASSWORD', '--mount', `type=bind,src=${init},dst=/docker-entrypoint-initdb.d/qualification.sql,readonly`, 'postgres:16-bookworm'], { env });
    await waitFor('isolated PostgreSQL bootstrap', () => { const child = spawnSync('docker', ['exec', names.postgres, 'pg_isready', '-h', '127.0.0.1', '-U', 'oma', '-d', 'oma'], { encoding: 'utf8', timeout: 5000 }); return child.status === 0; });
    docker(['run', '-d', '--name', names.provider, '--label', 'tank.qualification=' + prefix, '--network', names.network, '--network-alias', 'provider', '--env', 'QUALIFICATION_PROVIDER_KEY', '--mount', `type=bind,src=${resolve('scripts/ci/qualification-provider.mjs')},dst=/qualification-provider.mjs,readonly`, '--entrypoint', 'node', image, '/qualification-provider.mjs'], { env });
    await start('A');
    const agent = await api('/v1/agents', { name: 'Isolated native upgrade qualification', model: 'claude-sonnet-4-6', system: 'Execute the supplied qualification command exactly once. Report tool output. No external requests.', tools: [{ type: 'agent_toolset_20260401', default_config: { enabled: false }, configs: [{ name: 'bash', enabled: true }] }] });
    const memoryStore = await api('/v1/memory_stores', { name: 'qualification', metadata: { qualification: prefix } });
    const memory = await api('/v1/memory_stores/' + memoryStore.id + '/memories', { path: '/note.txt', content: 'seed:' + nonce });
    const memoryContent = () => api('/v1/memory_stores/' + memoryStore.id + '/memories/' + memory.id + '?view=full');
    receipt.memoryStoreId = memoryStore.id;
    const session = await api('/v1/sessions', { agent: agent.id, environment_id: 'env-local-runtime', resources: [{ type: 'memory_store', memory_store_id: memoryStore.id, name: 'qualification', access: 'read_write' }], metadata: { qualification: prefix } });
    receipt.sessionId = session.id;
    const form = new FormData(); form.set('file', new Blob([uploaded]), 'qualification-upload.bin');
    const file = await api('/v1/files', form);
    await api('/v1/sessions/' + session.id + '/resources', { type: 'file', file_id: file.id, mount_path: '/workspace/upload.bin' });
    const send = phase => api('/v1/sessions/' + session.id + '/events', { events: [{ type: 'user.message', content: [{ type: 'text', text: 'TANK_QUALIFICATION_COMMAND:' + Buffer.from(guestCommand(phase, nonce, uploadedHash)).toString('base64') }] }] });
    receipt.initialInput = await send('A');
    const initialHistory = await waitFor('actual A guest file/browser execution', async () => { const rows = await events(session.id); if (rows.some(row => row.type === 'session.error')) throw Object.assign(Error(JSON.stringify(rows.filter(row => row.type === 'session.error'))), { fatal: true }); return receipts(rows).length >= 1 && rows.some(row => row.type === 'session.status_idle' && row.stop_reason?.type === 'end_turn') ? rows : null; }, 600);
    receipt.before = publication();
    const memoryBindings = runtimeId => JSON.parse(docker(['exec', names.service, 'node', '--input-type=module', '-e', "import {DatabaseSync} from 'node:sqlite';const db=new DatabaseSync(process.env.BOXLITE_HOME+'/db/boxlite.db',{readOnly:true});const row=db.prepare('SELECT id,name,json FROM box_config WHERE id=?').get(process.argv[1]);if(!row)throw Error('missing actual native catalog row');const data=JSON.parse(row.json);if(data.id!==row.id||data.name!==row.name)throw Error('actual native catalog identity mismatch');console.log(JSON.stringify(data.options.volumes.filter(v=>v.guest_path.startsWith('/mnt/memory/'))));db.close();", runtimeId]));
    receipt.memoryBefore = await waitFor('A writable-memory synchronization through Managed API', async () => { const value = await memoryContent(); return value.content === 'seed:' + nonce + '\nA' ? value : null; });
    receipt.memoryBindingsBefore = memoryBindings(receipt.before.handle.runtimeId);
    assert.equal(receipt.memoryBindingsBefore.length, 1);
    assert.equal(receipt.memoryBindingsBefore[0].guest_path, '/mnt/memory/qualification');
    assert.equal(receipt.memoryBindingsBefore[0].read_only, false);
    assert.ok(receipt.memoryBindingsBefore[0].host_path.includes(memoryStore.id));
    receipt.historyBefore = initialHistory.map(row => ({ id: row.id, sha256: createHash('sha256').update(JSON.stringify(row)).digest('hex') }));
    await stop();
    await start('B');
    receipt.upgradeInput = await send('B-first');
    receipt.queuedInput = await send('B-second');
    receipt.queueObserved = JSON.parse(docker(['exec', names.postgres, 'psql', '-U', 'postgres', '-d', 'oma', '-At', '-c', "SELECT COALESCE(json_agg(json_build_object('id',id,'state',state)), '[]'::json) FROM managed_session_executions"]));
    assert.ok(receipt.queueObserved.some(row => row.state === 'queued'), 'No actual durably queued input was observed during replacement');
    const history = await waitFor('same-session B upgrade and queued-input completion', async () => { const rows = await events(session.id); return receipts(rows).length >= 3 && rows.filter(row => row.type === 'session.status_idle' && row.stop_reason?.type === 'end_turn').length >= 2 ? rows : null; }, 600);
    receipt.after = publication();
    receipt.memoryAfter = await waitFor('B writable-memory synchronization through Managed API', async () => { const value = await memoryContent(); return value.content === 'seed:' + nonce + '\nA\nB-first\nB-second' ? value : null; });
    receipt.memoryBindingsAfter = memoryBindings(receipt.after.handle.runtimeId);
    assert.deepEqual(receipt.memoryBindingsAfter, receipt.memoryBindingsBefore, 'restart/upgrade rebound a writable memory store to an unproved incarnation');
    receipt.memoryVerified = true;
    assert.notEqual(receipt.before.handle.runtimeId, receipt.after.handle.runtimeId, 'image B did not replace A');
    assert.notEqual(receipt.before.imageIdentity, receipt.after.imageIdentity, 'image identity did not change');
    assert.deepEqual(receipt.before.compatibility, receipt.after.compatibility, 'protected compatibility changed');
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
    assert.ok(receipt.providerObservations.some(row => row.upgradedNoticePresent), 'Continuing agent did not actually receive the runtime upgrade notice');
    receipt.guestReceiptCount = receipts(history).length;
    receipt.status = 'PASS';
    if (round === 1) {
      const metadata = { case: 'metadata-only-native-rebuild', sourceSha, sessionId: session.id, status: 'FAIL' };
      try {
        await stop();
        await start('metadata');
        await send('metadata');
        const unchangedHistory = await waitFor('metadata-only retained VM continuation', async () => { const rows = await events(session.id); return receipts(rows).length >= 4 && rows.filter(row => row.type === 'session.status_idle' && row.stop_reason?.type === 'end_turn').length >= 3 ? rows : null; }, 600);
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
          await api('/v1/sessions/' + session.id + '/resources', { type: 'file', file_id: file.id, mount_path: '/workspace/second-upload.bin' });
          await send('policy-probe');
          const refusal = await waitFor('protected resources mismatch refusal', async () => (await events(session.id)).find(row => row.type === 'session.error' && JSON.stringify(row).includes('retained_configuration_changed:resources')), 180);
          policy.errorEventId = refusal.id;
          policy.publication = publication();
          assert.deepEqual(policy.publication.handle, metadata.publication.handle, 'policy refusal changed the authoritative VM');
          const afterCalls = JSON.parse(docker(['exec', names.provider, 'node', '-e', "fetch('http://127.0.0.1:9000/observations',{headers:{'x-api-key':process.env.QUALIFICATION_PROVIDER_KEY}}).then(r=>r.json()).then(v=>console.log(JSON.stringify(v)))"]));
          assert.deepEqual(afterCalls, priorCalls, 'refused policy change reached the model');
          policy.status = 'PASS';
        } catch (error) { policy.error = sanitized(error.stack ?? error); process.exitCode = 1; }
        additionalCases.push(policy);
      }
    }
  } catch (error) {
    receipt.error = sanitized(error.stack ?? error);
    process.exitCode = 1;
  } finally {
    for (const name of [names.service, names.provider, names.postgres]) {
      const inspect = spawnSync('docker', ['inspect', '--format', '{{index .Config.Labels "tank.qualification"}}', name], { encoding: 'utf8', timeout: 10000 });
      if (inspect.status !== 0) continue;
      assert.equal(inspect.stdout.trim(), prefix, 'cleanup ownership mismatch');
      const logs = spawnSync('docker', ['logs', '--tail', '300', name], { encoding: 'utf8', timeout: 10000, maxBuffer: 1024 ** 2 });
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
additionalCases.push({ case: 'writable-memory-restart-A-to-B', sourceSha, status: observations.length === 2 && observations.every(row => row.status === 'PASS' && row.memoryVerified) ? 'PASS' : 'FAIL', rounds: observations.map(row => ({ case: row.case, memoryStoreId: row.memoryStoreId, bindingsBefore: row.memoryBindingsBefore, bindingsAfter: row.memoryBindingsAfter, verified: row.memoryVerified === true })) });
await rm(privateDirectory, { recursive: true, force: true });
for (const row of additionalCases) await writeFile(join(evidence, row.case + '.json'), JSON.stringify(row, null, 2) + '\n');
await writeFile(join(evidence, 'native-summary.json'), JSON.stringify({ schema: 'tank.native-managed-upgrade.v1', sourceSha, runId: process.env.GITHUB_RUN_ID, cases: [...observations, ...additionalCases].map(row => ({ case: row.case, status: row.status })), fullyQualified: false, unproved: ['legacy-native-publication', 'interrupted-transfer-publication', 'lease-loss-concurrent-resume-archive'] }, null, 2) + '\n');
assert.equal(observations.filter(row => row.status === 'PASS').length, 2, 'Two actually completed Managed API A-to-B native runs are required');
