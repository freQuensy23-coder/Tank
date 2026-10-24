/** Prepared native qualification. Does NOTHING without the explicit GO flag.
 * Run only in the owner's newly built candidate container with dedicated /proof,
 * BOXLITE_HOME, one CPU/2048MiB guest and the coherent readonly OCI layout.
 * This exercises actual production bridge/provider/fences/browser; no model/API
 * credential, external network request or OpenMA session/history row is used. */
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

import { hash, quote, configuration, phaseWallSeconds, PREPARATION_TIMEOUT_EXIT,
  preparationWatchdog, mountedOutputNonce, browserEvidence } from './qualification-contracts.mts';
export { configuration, phaseWallSeconds, PREPARATION_TIMEOUT_EXIT,
  preparationWatchdog, mountedOutputNonce, browserEvidence } from './qualification-contracts.mts';
async function json(path, value) { await writeFile(path, JSON.stringify(value, null, 2) + '\n', { mode: 0o600 }); }
async function proc(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 0) return null;
  try {
    const fields = (await readFile(`/proc/${pid}/stat`, 'utf8')).split(')').slice(1).join(')').trim().split(/\s+/);
    const status = await readFile(`/proc/${pid}/status`, 'utf8');
    return { pid, state: fields[0], startTime: fields[19], rssKiB: Number(status.match(/^VmRSS:\s+(\d+)/m)?.[1] ?? 0) };
  } catch (error) { if (error.code === 'ENOENT' || error.code === 'ESRCH') return null; throw error; }
}
async function stopped(client, id, before) {
  const info = await client.getInfo(id);
  assert.ok(info, 'retained native resource disappeared');
  assert.equal(info.state.running, false);
  assert.equal(info.autoDelete, 0);
  const after = await proc(before.pid);
  assert.ok(!after || after.startTime !== before.startTime || after.state === 'Z', 'owned native VM process remains alive after idle stop');
  const released = !after || after.startTime !== before.startTime || after.rssKiB === 0;
  assert.ok(released, 'owned VM process memory remains after stop');
  const metrics = await client.metrics();assert.equal(metrics.numRunningBoxes, 0, 'dedicated runtime still reports running VMs');
  return { id, state: info.state, autoDelete: info.autoDelete, oldProcess: before, oldProcessAfter: after,
    oldProcessMemoryReleased: released,
    runtimeMetrics: metrics };
}
async function observeBrowser(sb, config, scope, name, workerSha) {
  const guest = '/mnt/session/outputs';
  const host = join(config.directory, 'outputs', scope.workspaceId, scope.sessionId);
  const stem = `browser-${name}`;
  const phasePath = join(host, stem + '-phase.json'), stopPath = join(host, stem + '-stop');
  const script = await readFile(new URL('./native-startup-observer.py', import.meta.url));
  await sb.writeFileBytes('/workspace/native-startup-observer.py', script);
  const state = '/workspace/.tank-browser/' + hash(scope.sessionId).slice(0, 32);
  const args = ['python3', '/workspace/native-startup-observer.py', '--worker', '/opt/tank/browser-worker.py', '--worker-sha', workerSha,
    '--state', state, '--output', guest + '/' + stem + '-observer', '--phase', guest + '/' + stem + '-phase.json',
    '--stop', guest + '/' + stem + '-stop', '--interval', '5', '--max-seconds', '175'];
  const launch = "import subprocess; f=open(" + JSON.stringify(guest + '/' + stem + '-observer.log') + ", 'ab'); subprocess.Popen(" +
    JSON.stringify(args) + ",stdin=subprocess.DEVNULL,stdout=f,stderr=f,start_new_session=True)";
  await json(phasePath, { phase: 'observer_start', hostEpochMs: Date.now() });
  await sb.exec('python3 -c ' + quote(launch), 5000);
  const deadline = performance.now() + 5000;
  for (;;) {
    try { const ready = JSON.parse(await readFile(join(host, stem + '-observer', 'ready.json'), 'utf8'));
      const initialState = ready.path_stats[state];
      await json(join(config.directory, `browser-${name}-freshness.json`), { statePath: state, profilePath: state + '/profile', initialState, initialProfile: ready.initial_profile,
        publicCacheReused: Boolean(config.previousProof), beforeFirstBrowserOpen: true, observerScriptSha256: hash(script) });
      if (name === 'first') {
        assert.equal(initialState?.exists, false, 'first browser state must be fresh even when OCI cache is reused');
        assert.equal(ready.initial_profile.exists, false, 'first browser profile must be fresh even when OCI cache is reused');
      }break; }
    catch (error) { if (error.code !== 'ENOENT' || performance.now() >= deadline) throw error;await new Promise(resolve => setTimeout(resolve, 50)); }
  }
  return { host, scriptSha256: hash(script), state,
    mark: phase => json(phasePath, { phase, hostEpochMs: Date.now() }),
    stop: () => writeFile(stopPath, '', { mode: 0o600 }) };
}
async function phase(name, config) {
  const root = '/app';
  const installedSdk = JSON.parse(await readFile(root + '/packages/sandbox-adapter-litebox/node_modules/@boxlite-ai/boxlite/package.json', 'utf8'));
  assert.equal(installedSdk.version, '0.10.6');
  const { JsBoxlite } = await import(root + '/packages/sandbox-adapter-litebox/node_modules/@boxlite-ai/boxlite/dist/index.js');
  const providerModule = await import(root + '/packages/managed-runtime-boxlite/src/index.ts');
  const { NodeRetainedLifecycle } = await import(root + '/packages/control-plane-node/src/lib/tank-retained/node-lifecycle.ts');
  const { pinnedBoxLiteProvider } = await import(root + '/packages/control-plane-node/src/lib/tank-retained/boxlite-provider.ts');
  const { LiteBoxSandbox } = await import(root + '/packages/sandbox-adapter-litebox/src/index.ts');
  const { createBetterSqlite3SqlClient } = await import(root + '/packages/sql-client/src/index.ts');
  const { ensureRuntimeResourceFenceSchema, SqlRuntimeResourceFencePort } = await import(root + '/packages/runtime-resource-fence-sql/src/index.ts');
  const { createSandboxBrowserHarness } = await import(root + '/apps/agent/src/harness/tools.ts');
  const meta = JSON.parse(await readFile(join(config.directory, 'run.json'), 'utf8'));
  const overlay = JSON.parse(await readFile('/app/tank-overlay.json', 'utf8'));
  assert.equal(overlay.boxlite_sdk_version, '0.10.6');
  assert.match(overlay.sandbox_browser.worker_sha256, /^[a-f0-9]{64}$/);
  assert.ok((await readFile(root + '/packages/control-plane-node/src/modules/node-managed.ts', 'utf8')).includes(`runtimeIdentity: "${config.identity}"`), 'candidate embedded runtime identity differs');
  const sql = await createBetterSqlite3SqlClient(join(config.directory, 'resource-fences.sqlite'));
  await ensureRuntimeResourceFenceSchema(sql);
  const fences = new SqlRuntimeResourceFencePort(sql);
  const client = JsBoxlite.withDefaultConfig();
  const scope = { workspaceId: 'native_qualification', environmentId: 'native_qualification', sessionId: meta.sessionId };
  const fullScope = { ...scope, workId: 'tank-retained-session-v1' };
  const digest = allocation => 'sha256:' + hash(JSON.stringify({ identity: config.identity, scope, allocation, rootfsIndex: meta.rootfsIndex }));
  const controller = mismatch => new NodeRetainedLifecycle({ fences, providerModule, client, ownerId: `proof-${name}-${process.pid}`, leaseTtlMs: 30000,
    fingerprint: async (_context, allocation) => mismatch ? 'sha256:' + 'f'.repeat(64) : digest(allocation),
    assertExecutionCurrent: async fence => assert.equal(fence.current, true) });
  const context = sandbox => ({ ...scope, session: { id: scope.sessionId, environmentId: scope.environmentId },
    environment: { id: scope.environmentId }, sandbox, executionFence: { current: true } });
  const sandbox = async () => {
    const sb = new LiteBoxSandbox({ image: 'native-qualification-no-registry-pull', rootfsPath: config.rootfs, cpus: 1, memoryMib: config.memoryMib,
      tankNetwork: { mode: 'disabled' }, outputsRoot: join(config.directory, 'outputs'), scratchRoot: join(config.directory, 'scratch') });
    await sb.mountSessionOutputs({ tenantId: scope.workspaceId, sessionId: scope.sessionId });
    await sb.setEnvVars({ TANK_QUALIFICATION_KEY: name === 'first' ? 'FAKE_FIRST' : 'FAKE_ROTATED' });
    return sb;
  };
  const expectedName = 'oma-' + hash('litebox:' + scope.sessionId).slice(0, 32);
  if (name === 'first' && config.previousProof) {
    assert.deepEqual(await client.listInfo(), [], 'explicit reused cache home must have no native disks before new scope');
    const previous = JSON.parse(await readFile(join(config.previousProof, 'run.json'), 'utf8'));
    assert.notEqual(previous.sessionId, meta.sessionId, 'cache reuse may not restore the failed scope');
  }
  if (name === 'cleanup') {
    // Exact final probe cleanup only. This is NOT an OpenMA Session archive API.
    const before = await client.getInfo(expectedName);
    const recoveryDeadline = performance.now() + 35000;let acquisitionRetries = 0;
    for (;;) {
      try { await controller().archive({ ...context({}), reason: 'archived' });break; }
      catch (error) {
        if (error.message !== 'retained_resource_busy' || performance.now() >= recoveryDeadline) throw error;
        acquisitionRetries++;await new Promise(resolve => setTimeout(resolve, 500));
      }
    }
    assert.equal(await client.getInfo(expectedName), null);
    await controller().archive({ ...context({}), reason: 'archived' });
    await json(join(config.directory, 'phase-cleanup.json'), { status: 'PASS', id: before?.id ?? null, exactScopedReap: true, acquisitionRetries,
      nativeCatalogAfter: await client.listInfo(), noGuestRestoreOrBootRequired: true, openmaSessionArchiveApiCalled: false });
    return;
  }
  const first = name === 'first' ? null : JSON.parse(await readFile(join(config.directory, 'phase-first.json'), 'utf8'));
  if (name === 'resume') {
    const bad = await sandbox(), badContext = context(bad), badController = controller(true);
    await badController.bind(badContext);
    await assert.rejects(bad.exec('true'), /retained_configuration_changed/);
    assert.equal((await client.getInfo(first.id)).state.running, false);
    // A different scoped owner cannot start/remove the retained native identity.
    const foreign = { ...fullScope, sessionId: meta.sessionId + '_foreign' };
    const acquired = await fences.acquire({ scope: foreign, ownerId: 'foreign-scope-proof', ttlMs: 30000 });
    assert.equal(acquired.type, 'acquired');
    try {
      const bridge = pinnedBoxLiteProvider({ module: providerModule, client, allocation: { image: 'no-create' }, leaseTtlMs: 30000, isCurrent: fence => fences.isCurrent(fence) });
      await assert.rejects(bridge.resume({ provider: 'litebox', runtimeId: first.id }, foreign, acquired.fence, new AbortController().signal));
    } finally { await fences.release({ fence: acquired.fence, reason: 'completed' }); }
    assert.equal((await client.getInfo(first.id)).state.running, false);
  }
  const manager = controller(), sb = await sandbox(), ctx = context(sb);
  await manager.bind(ctx);
  const checkpoint = stage => json(join(config.directory, `progress-${name}.json`), { stage, hostEpochMs: Date.now() });
  await checkpoint('before_native_preparation');const preparationStart = performance.now();
  const clearPreparationWatchdog = preparationWatchdog(() => {
    // Exit this exact child rather than letting an unresolved native operation
    // continue into browser work. Parent finally performs exact scoped cleanup.
    const receipt = JSON.stringify({ classification: 'PREPARATION_TIMEOUT', limitSeconds: 240, hostEpochMs: Date.now(), nativePreparationMs: performance.now() - preparationStart });
    try { writeFileSync(join(config.directory, `preparation-${name}-timeout.json`), receipt + '\n', { mode: 0o600 });
      writeFileSync(join(config.directory, `progress-${name}.json`), receipt + '\n', { mode: 0o600 }); }
    finally { process.exit(PREPARATION_TIMEOUT_EXIT); }
  });
  try {
  if (name === 'first') {
    await sb.writeFileBytes('/root/native-retained-proof', Buffer.from(meta.nonce));
    await sb.writeFileBytes('/workspace/native-retained-proof', Buffer.from(meta.nonce));
    await mountedOutputNonce(sb, meta.nonce, true);
  } else {
    assert.equal(Buffer.from(await sb.readFileBytes('/root/native-retained-proof')).toString(), meta.nonce);
    assert.equal(Buffer.from(await sb.readFileBytes('/workspace/native-retained-proof')).toString(), meta.nonce);
    await mountedOutputNonce(sb, meta.nonce, false);
  }
  } finally { clearPreparationWatchdog(); }
  const nativePreparationMs = performance.now() - preparationStart;await checkpoint('native_preparation_and_retained_files_complete');
  assert.equal((await sb.exec('printf "%s" "$TANK_QUALIFICATION_KEY"')).trim(), name === 'first' ? 'FAKE_FIRST' : 'FAKE_ROTATED');
  const info = await client.getInfo(expectedName);
  assert.ok(info);assert.equal(info.autoDelete, 0);assert.equal(info.cpus, 1);assert.equal(info.memoryMib, config.memoryMib);
  if (first) assert.equal(info.id, first.id);
  const activeProcess = await proc(info.state.pid);
  assert.ok(activeProcess && activeProcess.state !== 'Z' && activeProcess.rssKiB > 0, 'active VM process/RSS evidence missing');
  if (name === 'first') {
    const rival = controller(), rivalSandbox = await sandbox(), rivalContext = context(rivalSandbox);
    await rival.bind(rivalContext);await assert.rejects(rivalSandbox.exec('true'), /retained_resource_busy/);
    assert.equal((await client.getInfo(info.id)).state.running, true);
  }
  const worker = Buffer.from(await sb.readFileBytes('/opt/tank/browser-worker.py'));
  assert.equal(hash(worker), overlay.sandbox_browser.worker_sha256, 'guest worker is not the qualified image worker');
  // VM-local loopback site; no external service or outbound network is used.
  const server = "import subprocess; f=open('/tmp/native-lifecycle-http.log','ab'); subprocess.Popen(['python3','-m','http.server','8765','--bind','127.0.0.1','--directory','/workspace'],stdin=subprocess.DEVNULL,stdout=f,stderr=f,start_new_session=True)";
  await sb.exec('python3 -c ' + quote(server), 5000);
  await sb.exec('python3 -c ' + quote("import time,urllib.request\nfor i in range(30):\n try: urllib.request.urlopen('http://127.0.0.1:8765',timeout=.2).read(1); break\n except Exception:\n  if i==29: raise\n  time.sleep(.1)"), 5000);
  if (name === 'first') await sb.writeFileBytes('/workspace/index.html', Buffer.from('<!doctype html><title>Native retained proof</title><h1>native-retained-browser-proof</h1>'));
  const observer = await observeBrowser(sb, config, scope, name, hash(worker));
  const browser = await createSandboxBrowserHarness(sb, meta.sessionId).launch();
  let browserProof, browserFailed = false;
  try {
    await checkpoint('before_browser_first_open');await observer.mark('before_first_open');const openStart = performance.now();
    const page = await browser.page();const firstOpenMs = performance.now() - openStart;
    await checkpoint('after_browser_first_open');await observer.mark('after_first_open');await page.goto('http://127.0.0.1:8765/');
    browserProof = { firstOpenMs, ...await browserEvidence(page, config.directory, name), observerScriptSha256: observer.scriptSha256, state: observer.state };
    await checkpoint('after_dom_png');await observer.mark('after_dom_png');
    if (name === 'first') await page.evaluate("value => { document.cookie='lifecycle_probe='+value+'; Max-Age=3600; Path=/'; localStorage.setItem('lifecycle_probe',value); }", meta.nonce);
    const profile = await page.evaluate("() => ({cookie:document.cookie,local:localStorage.getItem('lifecycle_probe')})");
    assert.ok(profile.cookie.split(';').some(value => value.trim() === 'lifecycle_probe=' + meta.nonce));assert.equal(profile.local, meta.nonce);
  } catch (error) {
    browserFailed = true;await observer.mark('after_first_failure');
    await json(join(config.directory, `browser-${name}-failure.json`), { hostEpochMs: Date.now(), error: error.message, observerHostDirectory: observer.host, workerSha256: hash(worker) });
    throw error;
  } finally {
    try { await browser.close(); }
    finally { if (!browserFailed) await observer.mark('after_browser_close');await observer.stop(); }
  }
  await checkpoint('before_idle');await manager.idle(ctx);await checkpoint('after_idle');
  const stop = await stopped(client, info.id, activeProcess);
  assert.equal(await readFile(join(config.directory, 'outputs', scope.workspaceId, scope.sessionId, 'native-retained-proof'), 'utf8'), meta.nonce);
  assert.equal((await client.listInfo()).length, 1, 'qualification must retain exactly one native disk');
  await manager.shutdown();
  await json(join(config.directory, `phase-${name}.json`), { status: 'PASS', id: info.id, expectedName, controllerPid: process.pid,
    fingerprint: config.identity, workerSha256: hash(worker), nativePreparationMs, browserProof, rootfsWorkspaceAndOutputRetained: true, actualChromiumCookieAndLocalStorageRetained: true,
    nativeInfo: info, stop, fingerprintMismatchRejected: name === 'resume', foreignScopeRejected: name === 'resume', competingOwnerRejected: name === 'first',
    noIdleArchiveOrRecreate: true, perExecFakeKeyRotation: true, openmaApiHistoryCertified: false });
}
export async function main() {
  const config = configuration(process.env);
  const child = process.argv[2];
  if (child) { assert.ok(['first', 'resume', 'cleanup'].includes(child));await phase(child, config);return; }
  // Refuse existing proof directories: never attach this probe to old or live VMs.
  await mkdir(config.directory, { mode: 0o700 });
  const meta = { sessionId: 'native_proof_' + randomUUID().replaceAll('-', ''), nonce: randomUUID(),
    rootfsIndex: hash(await readFile(join(config.rootfs, 'index.json'))), declaredHostImageId: config.imageId, identity: config.identity, cpus: 1, memoryMib: config.memoryMib };
  if (config.previousProof) {
    const previous = JSON.parse(await readFile(join(config.previousProof, 'run.json'), 'utf8'));
    const cleanup = JSON.parse(await readFile(join(config.previousProof, 'phase-cleanup.json'), 'utf8'));
    assert.equal(cleanup.status, 'PASS');assert.deepEqual(cleanup.nativeCatalogAfter, [], 'owned cache receipt must prove exact cleanup');
    assert.match(previous.sessionId, /^native_proof_[a-f0-9]{32}$/, 'only a native diagnostic proof HOME can be reused');
    assert.equal(cleanup.noGuestRestoreOrBootRequired, true);assert.equal(cleanup.openmaSessionArchiveApiCalled, false);
    for (const key of ['identity', 'declaredHostImageId', 'rootfsIndex', 'cpus', 'memoryMib']) assert.equal(previous[key], meta[key], 'cache provenance mismatch: ' + key);
    assert.notEqual(previous.sessionId, meta.sessionId);
    meta.reusedPublicCacheFrom = config.previousProof;meta.freshScopeAndBrowserProfileRequired = true;
  }
  await json(join(config.directory, 'run.json'), meta);
  const results = [];let failure;
  try {
    for (const name of ['first', 'resume']) {
      const result = spawnSync(process.execPath, [...process.execArgv, fileURLToPath(import.meta.url), name], { env: process.env,
        timeout: phaseWallSeconds[name] * 1000, killSignal: 'SIGKILL', maxBuffer: 2 * 1024 * 1024, encoding: 'utf8' });
      await writeFile(join(config.directory, `${name}.log`), result.stdout + result.stderr, { mode: 0o600 });
      let lastProgress = null;try { lastProgress = JSON.parse(await readFile(join(config.directory, `progress-${name}.json`), 'utf8')); } catch {}
      results.push({ phase: name, status: result.status, signal: result.signal, spawnError: result.error?.code ?? null, lastProgress,
        failureClassification: result.status === PREPARATION_TIMEOUT_EXIT ? 'PREPARATION_TIMEOUT' : result.error?.code === 'ETIMEDOUT' ? 'OUTER_DIAGNOSTIC_TIMEOUT_NOT_BROWSER_CLASSIFICATION' : result.status === 0 ? null : 'CHILD_PHASE_FAILURE_SEE_ARTIFACTS' });
      if (result.status !== 0) throw Error(result.status === PREPARATION_TIMEOUT_EXIT ? 'PREPARATION_TIMEOUT: exact phase child exited before browser; inspect receipt and scoped cleanup' : result.error?.code === 'ETIMEDOUT' ? `Outer diagnostic ${name} orchestration timed out; inspect last stage (not classified as browser failure)` : `Native ${name} phase failed; inspect its bounded local log`);
    }
  } catch (error) { failure = error; }
  finally {
    const cleanup = spawnSync(process.execPath, [...process.execArgv, fileURLToPath(import.meta.url), 'cleanup'], { env: process.env,
      timeout: phaseWallSeconds.cleanup * 1000, killSignal: 'SIGKILL', maxBuffer: 2 * 1024 * 1024, encoding: 'utf8' });
    await writeFile(join(config.directory, 'cleanup.log'), cleanup.stdout + cleanup.stderr, { mode: 0o600 });
    results.push({ phase: 'cleanup', status: cleanup.status, signal: cleanup.signal, spawnError: cleanup.error?.code ?? null });
    if (cleanup.status !== 0) failure ??= Error('Exact scoped native cleanup failed; preserve proof volume for explicit owned-resource recovery');
  }
  const report = { status: failure ? 'FAIL' : 'PASS', scope: 'ACTUAL_NATIVE_BRIDGE_ONLY_NOT_OPENMA_API_HISTORY', ...meta, results,
    modelCalls: 0, externalRequests: 0, idleSessionArchiveOrRecreate: false, openmaSessionHistoryProofStillRequired: true,
    maximumPhaseWallSeconds: [phaseWallSeconds.first, phaseWallSeconds.resume, phaseWallSeconds.cleanup], failure: failure?.message ?? null };
  await json(join(config.directory, 'result.json'), report);console.log(JSON.stringify(report));
  if (failure) process.exitCode = 1;
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => { console.error(error.message);process.exitCode = 1; });
}
