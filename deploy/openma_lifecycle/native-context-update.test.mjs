import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile, readdir, link, mkdtemp, rm } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { tmpdir } from 'node:os';
import { spawn } from 'node:child_process';

const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const root = resolve(process.env.TANK_NATIVE_OPENMA_ROOT ?? 'vendor/openma');
const load = path => import(pathToFileURL(join(root, path)).href);
const require = createRequire(join(root, 'apps/main-node/package.json'));
const pause = () => new Promise(resolve => setTimeout(resolve, 50));
const text = value => [{ type: 'text', text: value }];

async function changedFixture(source, target) {
  await mkdir(join(target, 'blobs', 'sha256'), { recursive: true });
  for (const name of await readdir(join(source, 'blobs', 'sha256'))) {
    await link(join(source, 'blobs', 'sha256', name), join(target, 'blobs', 'sha256', name));
  }
  await writeFile(join(target, 'oci-layout'), await readFile(join(source, 'oci-layout')));
  const put = async (descriptor, value) => {
    const bytes = Buffer.from(JSON.stringify(value));
    const digest = 'sha256:' + hash(bytes);
    await writeFile(join(target, 'blobs', 'sha256', digest.slice(7)), bytes, { flag: 'wx' });
    return { ...descriptor, digest, size: bytes.length };
  };
  let changed = 0;
  const visit = async descriptor => {
    if (descriptor.platform && (descriptor.platform.os !== 'linux' || descriptor.platform.architecture !== 'amd64')) return descriptor;
    const document = JSON.parse(await readFile(join(source, 'blobs', 'sha256', descriptor.digest.slice(7)), 'utf8'));
    if (document.manifests) return put(descriptor, { ...document, manifests: await Promise.all(document.manifests.map(visit)) });
    if (!document.config) return descriptor;
    const config = JSON.parse(await readFile(join(source, 'blobs', 'sha256', document.config.digest.slice(7)), 'utf8'));
    if (config.os !== 'linux' || config.architecture !== 'amd64') return descriptor;
    changed++;
    const next = { ...config, config: { ...config.config, Env: [...(config.config?.Env ?? []), 'TANK_NATIVE_CONTEXT_FIXTURE=after'] } };
    return put(descriptor, { ...document, config: await put(document.config, next) });
  };
  const index = JSON.parse(await readFile(join(source, 'index.json'), 'utf8'));
  const updated = { ...index, manifests: await Promise.all(index.manifests.map(visit)) };
  assert.equal(changed, 1, 'fixture must change exactly one real Linux amd64 executable configuration');
  await writeFile(join(target, 'index.json'), JSON.stringify(updated));
}

function guestCommand(stage, nonce, sessionId) {
  const profile = '/workspace/.tank-browser/' + hash(sessionId).slice(0, 32) + '/profile';
  const script = `import asyncio, importlib.util, importlib.metadata, json, subprocess, sys, sysconfig, threading, zipfile
from pathlib import Path
from http.server import BaseHTTPRequestHandler, HTTPServer
from playwright.async_api import async_playwright
stage=${JSON.stringify(stage)}
nonce=${JSON.stringify(nonce)}
profile=Path(${JSON.stringify(profile)})
workspace=Path('/workspace/native-context-proof')
rootfile=Path('/root/native-context-proof')
package=Path(sysconfig.get_paths()['purelib'])/'tank_native_context_probe.py'
if stage=='before':
 assert not profile.exists()
 workspace.write_text(nonce)
 rootfile.write_text(nonce)
 wheel=Path('/workspace/tank_native_context_probe-1.0-py3-none-any.whl')
 with zipfile.ZipFile(wheel,'w') as archive:
  archive.writestr('tank_native_context_probe.py','MARKER='+repr(nonce))
  archive.writestr('tank_native_context_probe-1.0.dist-info/METADATA','Metadata-Version: 2.1\\nName: tank-native-context-probe\\nVersion: 1.0\\n')
  archive.writestr('tank_native_context_probe-1.0.dist-info/WHEEL','Wheel-Version: 1.0\\nGenerator: native-context-fixture\\nRoot-Is-Purelib: true\\nTag: py3-none-any\\n')
  archive.writestr('tank_native_context_probe-1.0.dist-info/RECORD','')
 subprocess.run([sys.executable,'-m','pip','install','--no-index','--no-deps','--disable-pip-version-check',str(wheel)],check=True,stdout=subprocess.DEVNULL)
 import tank_native_context_probe
 assert tank_native_context_probe.MARKER==nonce
 assert importlib.metadata.version('tank-native-context-probe')=='1.0'
else:
 assert not workspace.exists()
 assert not rootfile.exists()
 assert not package.exists()
 assert importlib.util.find_spec('tank_native_context_probe') is None
 assert not profile.exists()
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  self.send_response(200); self.end_headers(); self.wfile.write(b'native-context-browser')
 def log_message(self,*args): pass
server=HTTPServer(('127.0.0.1',8765),Handler)
threading.Thread(target=server.serve_forever,daemon=True).start()
async def main():
 async with async_playwright() as playwright:
  context=await playwright.chromium.launch_persistent_context(str(profile),headless=True)
  try:
   page=context.pages[0] if context.pages else await context.new_page()
   await page.goto('http://127.0.0.1:8765')
   assert 'native-context-browser' in await page.content()
   if stage=='before':
    await page.evaluate('(value)=>{document.cookie="native_context="+value+"; Max-Age=3600; Path=/";localStorage.setItem("native_context",value)}',nonce)
   saved=await page.evaluate('()=>({cookie:document.cookie,local:localStorage.getItem("native_context")})')
   if stage=='before':
    assert nonce in saved['cookie'] and saved['local']==nonce
   else:
    assert saved['cookie']=='' and saved['local'] is None
   png=await page.screenshot()
   assert png[:8]==bytes([137,80,78,71,13,10,26,10])
  finally: await context.close()
asyncio.run(main())
server.shutdown()
print('NATIVE_'+stage.upper()+'_'+nonce)
`;
  return "python3 -c '" + script.replaceAll("'", "'\\''") + "'";
}

async function nativeCatalog(directory, runtimeDirectory, ids) {
  const module = createRequire(join(root, 'packages/sandbox-adapter-litebox/package.json')).resolve('@boxlite-ai/boxlite');
  const code = `const {JsBoxlite}=await import(${JSON.stringify(pathToFileURL(module).href)});const client=JsBoxlite.withDefaultConfig();const values=await Promise.all(${JSON.stringify(ids)}.map(id=>client.getInfo(id)));process.stdout.write(JSON.stringify(values));`;
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ['--input-type=module', '-e', code], {
      env: { PATH: process.env.PATH, HOME: directory, BOXLITE_HOME: join(directory, 'boxlite'), BOXLITE_RUNTIME_DIR: runtimeDirectory, LD_LIBRARY_PATH: runtimeDirectory },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '', stderr = '';
    const timer = setTimeout(() => child.kill('SIGKILL'), 5000);
    child.stdout.on('data', value => { stdout += value; });
    child.stderr.on('data', value => { stderr += value; });
    child.once('error', reject);
    child.once('exit', code => {
      clearTimeout(timer);
      if (code !== 0) reject(new Error('Native catalog read failed: ' + stderr.slice(0, 1000)));
      else { try { resolve(JSON.parse(stdout)); } catch (error) { reject(error); } }
    });
  });
}

test('native Managed API context-only update preserves SQL history and accepts pending callback exactly once across clean controller restart', { timeout: 120000 }, async context => {
  const receiptPath = resolve(process.env.TANK_NATIVE_CONTEXT_RECEIPT ?? '.test-results/native-context.json');
  await mkdir(resolve(receiptPath, '..'), { recursive: true });
  const receipt = { schema: 'tank.native-context-update.v1', status: 'FAIL', boundary: 'fixture-preparation', model: 'isolated scripted Anthropic HTTP transport, not a real LLM', shippingEntrypointExercised: false, sourceSha: process.env.GITHUB_SHA ?? null, checks: [] };
  let fixture, db, fixtureDirectory, failure;
  const abort = () => { void fixture?.dispose().catch(() => {}); };
  context.signal.addEventListener('abort', abort, { once: true });
  try {
    assert.match(process.env.TANK_NATIVE_IMAGE_ID ?? '', /^sha256:[a-f0-9]{64}$/);
    assert.equal(process.env.TANK_NATIVE_IMAGE_REVISION, process.env.GITHUB_SHA);
    assert.ok(Number.isSafeInteger(process.getuid?.()) && process.getuid() !== 0, 'qualification must not run as root');
    receipt.imageId = process.env.TANK_NATIVE_IMAGE_ID;
    receipt.executionUid = process.getuid();
    receipt.executionEnvelope = 'nonroot automatic CI runner with unmodified native access; extracted current shipping assets, not shipping container entrypoint';
    const source = resolve(process.env.TANK_NATIVE_ROOTFS ?? '/not-configured');
    const runtimeDirectory = resolve(process.env.BOXLITE_RUNTIME_DIR ?? '/not-configured');
    fixtureDirectory = await mkdtemp(join(tmpdir(), 'tank-native-context-oci-'));
    const after = join(fixtureDirectory, 'after');
    await changedFixture(source, after);
    const { executableImageIdentity } = await load('packages/control-plane-node/src/lib/tank-retained/runtime-identity.ts');
    const beforeIdentity = await executableImageIdentity(source), afterIdentity = await executableImageIdentity(after);
    assert.notEqual(beforeIdentity, afterIdentity);
    receipt.fixtures = { beforeIdentity, afterIdentity, beforeIndex: hash(await readFile(join(source, 'index.json'))), afterIndex: hash(await readFile(join(after, 'index.json'))), provenance: 'current normal build OCI plus explicitly test-owned executable environment variant; not historical image provenance' };
    const { bootOpenAINode } = await load('apps/main-node/test/_helpers/openai-node-process.ts');
    const nonce = randomUUID();
    let sessionId, originalCalls, originalResults, expectedCallback, selected = source, requestNumber = 0;
    fixture = await bootOpenAINode(input => {
      const number = requestNumber++;
      const parts = input.messages.flatMap(message => Array.isArray(message.content) ? message.content : []);
      if (number === 0) return { name: 'bash', input: { command: guestCommand('before', nonce, sessionId) } };
      if (number === 1) {
        assert.ok(JSON.stringify(parts).includes('NATIVE_BEFORE_' + nonce), 'actual native guest/browser/package setup must succeed before pending callback');
        receipt.checks.push('native-before-guest-browser-package');
        return { name: 'record_event', input: { precision: 'MINUTE' } };
      }
      if (number === 2) {
        assert.ok(parts.some(part => part.type === 'tool_result' && part.is_error), 'invalid custom arguments must produce an error in actual model input');
        return { name: 'record_event', input: { precision: 'HOUR' } };
      }
      if (number === 3) {
        const calls = parts.filter(part => part.type === 'tool_use');
        assert.deepEqual(calls, originalCalls);
        assert.deepEqual(calls.map(part => [part.name, part.input]), [
          ['bash', { command: guestCommand('before', nonce, sessionId) }],
          ['record_event', { precision: 'MINUTE' }], ['record_event', { precision: 'HOUR' }],
        ]);
        const results = parts.filter(part => part.type === 'tool_result');
        assert.deepEqual(results.map(part => part.tool_use_id), originalCalls.map(part => part.id));
        assert.deepEqual(results, [...originalResults, expectedCallback]);
        assert.equal(results[1].is_error, true);
        assert.equal(results.filter(part => part.tool_use_id === expectedCallback.tool_use_id).length, 1);
        assert.ok(JSON.stringify(input.messages).includes('REMEMBER_' + nonce));
        receipt.checks.push('actual-model-http-history-tool-identities-and-single-callback');
        return { name: 'bash', input: { command: guestCommand('after', nonce, sessionId) } };
      }
      assert.equal(number, 4, 'duplicate callback may not invoke the model again');
      assert.ok(JSON.stringify(parts).includes('NATIVE_AFTER_' + nonce), 'fresh real guest must prove absence of old files, package and Chromium state');
      receipt.checks.push('native-after-fresh-files-package-browser-profile');
      return { text: 'CONTEXT_CONTINUED_' + nonce };
    }, { startupTimeoutMs: 15000, native: { rootfsPath: () => selected, runtimeDirectory, stopTimeoutMs: 5000 } });
    const Anthropic = require('@anthropic-ai/sdk').default;
    const client = () => new Anthropic({ apiKey: 'test', baseURL: fixture.baseURL, maxRetries: 0, timeout: 5000 });
    const agent = await client().beta.agents.create({ name: 'Native context update', model: 'claude-sonnet-4-20250514', tools: [
      { type: 'agent_toolset_20260401' }, { type: 'custom', name: 'record_event', description: 'Record precision', input_schema: { type: 'object', properties: { precision: { type: 'string', enum: ['HOUR'] } }, required: ['precision'], additionalProperties: false } },
    ] });
    const session = await client().beta.sessions.create({ agent: { type: 'agent', id: agent.id, version: agent.version }, environment_id: 'env-local-runtime' });
    sessionId = session.id;
    receipt.boundary = 'native-before';
    await client().beta.sessions.events.send(sessionId, { events: [{ type: 'user.message', content: text('REMEMBER_' + nonce) }] });
    const Database = require('better-sqlite3');
    db = new Database(join(fixture.directory, 'oma.db'), { readonly: true });
    const rows = () => db.prepare('SELECT * FROM managed_session_events WHERE session_id = ? ORDER BY processed_at, id').all(sessionId);
    const publication = () => JSON.parse(db.prepare('SELECT publication_json FROM runtime_resource_fences WHERE session_id = ?').get(sessionId).publication_json).workspaceCandidate.metadata;
    const settled = async count => {
      const deadline = performance.now() + 45000;
      while (performance.now() < deadline) {
        const executions = db.prepare('SELECT state FROM managed_session_executions WHERE session_id = ?').all(sessionId);
        const events = rows();
        assert.ok(!events.some(row => JSON.stringify(row).includes('session.error')), 'real native runtime failure is not skippable');
        if (executions.length === count && executions.every(row => row.state === 'completed')) return events;
        await pause();
      }
      throw new Error('Native Managed execution did not settle within its fixed deadline');
    };
    const original = await settled(1);
    assert.equal(requestNumber, 3);
    const history = (await client().beta.sessions.events.list(sessionId, { limit: 100, order: 'asc' })).data;
    const generatedCalls = history.filter(event => event.type === 'agent.tool_use' || event.type === 'agent.custom_tool_use');
    assert.equal(generatedCalls.length, 3);
    for (const event of generatedCalls) assert.match(event.id, /^[a-zA-Z0-9_-]+$/);
    originalCalls = structuredClone(generatedCalls.map(event => ({ type: 'tool_use', id: event.id, name: event.name, input: event.input })));
    assert.deepEqual(originalCalls.map(part => [part.name, part.input]), [
      ['bash', { command: guestCommand('before', nonce, sessionId) }],
      ['record_event', { precision: 'MINUTE' }], ['record_event', { precision: 'HOUR' }],
    ]);
    const originalParts = fixture.requests[2].messages.flatMap(message => Array.isArray(message.content) ? message.content : []);
    assert.deepEqual(originalParts.filter(part => part.type === 'tool_use'), originalCalls.slice(0, 2));
    originalResults = structuredClone(originalParts.filter(part => part.type === 'tool_result'));
    assert.equal(originalResults.length, 2);
    for (const [index, result] of originalResults.entries()) {
      const generatedResults = history.filter(event => event.type === 'agent.tool_result' && event.tool_use_id === originalCalls[index].id);
      assert.equal(generatedResults.length, 1);
      assert.equal(result.tool_use_id, originalCalls[index].id);
      assert.deepEqual(result.content, generatedResults[0].content);
      assert.equal(result.is_error, index === 1);
      assert.equal(generatedResults[0].is_error, index === 1);
    }
    assert.ok(JSON.stringify(originalResults[0].content).includes('NATIVE_BEFORE_' + nonce));
    const pending = generatedCalls[2];
    assert.equal(pending.type, 'agent.custom_tool_use');
    assert.equal(history.filter(event => event.type === 'agent.tool_result' && event.tool_use_id === pending.id).length, 0);
    expectedCallback = { type: 'tool_result', tool_use_id: pending.id, content: text('CALLBACK_' + nonce), is_error: false };
    const old = publication();
    assert.ok(old.runtimeId);
    receipt.boundary = 'clean-controller-restart';
    await fixture.stop();
    const [stopped] = await nativeCatalog(fixture.directory, runtimeDirectory, [old.runtimeId]);
    assert.equal(stopped.state.running, false);
    selected = after;
    await fixture.restart();
    assert.deepEqual(rows(), original);
    const callback = { events: [{ type: 'user.custom_tool_result', custom_tool_use_id: pending.id, content: text('CALLBACK_' + nonce), is_error: false }] };
    const options = { headers: { 'Idempotency-Key': 'native-context-callback-' + nonce } };
    receipt.boundary = 'native-update-and-continuation';
    const accepted = await client().beta.sessions.events.send(sessionId, callback, options);
    const duplicate = await client().beta.sessions.events.send(sessionId, callback, options);
    assert.deepEqual(duplicate, accepted);
    const final = await settled(2);
    for (const row of original) assert.deepEqual(final.find(item => item.id === row.id), row);
    const finalHistory = (await client().beta.sessions.events.list(sessionId, { limit: 100 })).data;
    for (const event of history) assert.deepEqual(finalHistory.find(item => item.id === event.id), event);
    assert.equal(finalHistory.filter(event => event.type === 'user.custom_tool_result' && event.custom_tool_use_id === pending.id).length, 1);
    assert.equal(finalHistory.filter(event => event.type === 'agent.custom_tool_use').length, 2);
    assert.ok(JSON.stringify(finalHistory).includes('CONTEXT_CONTINUED_' + nonce));
    assert.equal(requestNumber, 5);
    const current = publication();
    assert.notEqual(current.runtimeId, old.runtimeId);
    await fixture.stop();
    const [retired, fresh] = await nativeCatalog(fixture.directory, runtimeDirectory, [old.runtimeId, current.runtimeId]);
    assert.equal(retired, null, 'old owned native disk must actually be retired');
    assert.equal(fresh.state.running, false);
    receipt.checks.push('unchanged-real-sql-and-public-history', 'callback-http-idempotency', 'clean-controller-shutdown', 'old-native-resource-retired');
    receipt.status = 'PASS';
    receipt.boundary = 'complete-native-managed-api-path';
  } catch (error) {
    failure = error;
  } finally {
    context.signal.removeEventListener('abort', abort);
    try { db?.close(); } catch (error) { failure ??= error; }
    try { await fixture?.dispose(); }
    catch (error) { failure ??= error; receipt.cleanupFailed = true; }
    try { if (fixtureDirectory) await rm(fixtureDirectory, { recursive: true, force: true }); }
    catch (error) { failure ??= error; receipt.cleanupFailed = true; }
    if (failure || context.signal.aborted) receipt.status = 'FAIL';
    await writeFile(receiptPath, JSON.stringify(receipt, null, 2) + '\n');
  }
  if (failure) throw failure;
  context.signal.throwIfAborted();
});
