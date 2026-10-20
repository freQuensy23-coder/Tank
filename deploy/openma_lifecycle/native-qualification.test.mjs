import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { configuration, phaseWallSeconds, PREPARATION_TIMEOUT_EXIT } from './native-qualification.mjs';
const permitted={TANK_NATIVE_LIFECYCLE_GO:'AUTHORIZED_NATIVE_PROOF',TANK_NATIVE_MEMORY_MIB:'2048',TANK_NATIVE_PROOF_DIR:'/proof/native-run',BOXLITE_HOME:'/proof/native-run/boxlite',TANK_NATIVE_EXPECT_IDENTITY:'a'.repeat(64),TANK_NATIVE_IMAGE_ID:'sha256:'+'b'.repeat(64)};
test('native qualifier defaults to no native import/start and requires explicit GO',()=>{
  assert.throws(()=>configuration({}),/explicit deployment-owner GO/);
  const result=spawnSync(process.execPath,[new URL('./native-qualification.mjs',import.meta.url).pathname],{env:{},encoding:'utf8',timeout:1000});
  assert.equal(result.status,1);assert.match(result.stderr,/explicit deployment-owner GO/);
  assert.doesNotMatch(result.stderr,/Cannot find|boxlite|native module/i);
});
test('native qualifier requires exact candidate, dedicated proof storage and explicit runtime home',()=>{
  assert.throws(()=>configuration({...permitted,TANK_NATIVE_MEMORY_MIB:'512'}),/explicit memory2048/);
  assert.equal(configuration(permitted).rootfs,'/opt/tank-sandbox-oci');
  assert.throws(()=>configuration({...permitted,BOXLITE_HOME:'/app/data/boxlite'}),/dedicated proof directory/);
  assert.throws(()=>configuration({...permitted,TANK_NATIVE_PROOF_DIR:'/app/data'}),/dedicated new/);
  assert.throws(()=>configuration({...permitted,TANK_NATIVE_EXPECT_IDENTITY:'unknown'}),/Exact candidate/);
  assert.throws(()=>configuration({...permitted,TANK_NATIVE_IMAGE_ID:'latest'}),/Host-inspected/);
});
test('diagnostic DOM/PNG capture saves exact artifacts and rejects absent evidence',async()=>{
  const {browserEvidence}=await import('./native-qualification.mjs');
  const {mkdtemp,readFile,rm}=await import('node:fs/promises');
  const {tmpdir}=await import('node:os');
  const {join}=await import('node:path');
  const dir=await mkdtemp(join(tmpdir(),'tank-probe-guard-'));
  const png=Buffer.alloc(25);Buffer.from([137,80,78,71,13,10,26,10]).copy(png);png.writeUInt32BE(12,16);png.writeUInt32BE(9,20);
  try {
    const result=await browserEvidence({content:async()=>'<h1>native-retained-browser-proof</h1>',screenshot:async opts=>{assert.deepEqual(opts,{type:'png'});return png;}},dir,'first');
    assert.equal(result.width,12);assert.equal(result.height,9);
    assert.deepEqual(await readFile(join(dir,'browser-first.png')),png);
    assert.match(await readFile(join(dir,'browser-first.html'),'utf8'),/native-retained-browser-proof/);
    await assert.rejects(browserEvidence({content:async()=>'<h1>wrong site</h1>'},dir,'resume'),/DOM marker missing/);
    await assert.rejects(browserEvidence({content:async()=> 'native-retained-browser-proof',screenshot:async()=>Buffer.from('not png')},dir,'resume'),/PNG bytes/);
  } finally {await rm(dir,{recursive:true});}
});

test('outer orchestration contains cold preparation and all unchanged browser RPC budgets',()=>{
  // First: open/goto/content/PNG/set-profile/read-profile/close. Resume omits set-profile.
  assert.deepEqual(phaseWallSeconds,{first:1080,resume:900,cleanup:60});
  assert.ok(phaseWallSeconds.first >= 240 + 7*90 + 90);
  assert.ok(phaseWallSeconds.resume >= 240 + 6*90 + 90);
});

test('separate preparation watchdog exits an unresolved phase before browser work',()=>{
  const moduleUrl=new URL('./native-qualification.mjs',import.meta.url).href;
  const code=`import {preparationWatchdog,PREPARATION_TIMEOUT_EXIT} from ${JSON.stringify(moduleUrl)};preparationWatchdog(()=>process.exit(PREPARATION_TIMEOUT_EXIT),10);await new Promise(()=>{});console.log('browser_must_not_run');`;
  const result=spawnSync(process.execPath,['--input-type=module','-e',code],{encoding:'utf8',timeout:1000});
  assert.equal(result.status,PREPARATION_TIMEOUT_EXIT);assert.equal(result.stdout,'');assert.equal(result.error,undefined);
});
test('mounted output nonce uses bounded guest exec and never mounted-path file transfer',async()=>{
  const {mountedOutputNonce}=await import('./native-qualification.mjs');
  const calls=[],nonce='quote\' dollar$ newline\n';
  const port={writeFileBytes:()=>{throw Error('unsupported mounted transfer');},readFileBytes:()=>{throw Error('unsupported mounted transfer');},exec:async(command,budget)=>{calls.push({command,budget});return Buffer.from(nonce).toString('base64');}};
  await mountedOutputNonce(port,nonce,true);await mountedOutputNonce(port,nonce,false);
  assert.equal(calls.length,2);assert.ok(calls.every(call=>call.budget===5000));
  assert.ok(calls.every(call=>call.command.startsWith('python3 -c ') && call.command.includes('/mnt/session/outputs/native-retained-proof')));
  assert.match(calls[0].command,/write_bytes/);assert.doesNotMatch(calls[1].command,/write_bytes/);
  assert.ok(calls.every(call=>!call.command.includes(nonce)));
  await assert.rejects(mountedOutputNonce({exec:async()=> '[error: failure]'},nonce,true),/guest exec failed/);
});
test('public cache reuse is explicit and keeps a distinct proof/session directory',()=>{
  const next={...permitted,TANK_NATIVE_PROOF_DIR:'/proof/native-browser-v7',TANK_NATIVE_REUSE_CACHE_PROOF_DIR:'/proof/native-browser-v6',BOXLITE_HOME:'/proof/native-browser-v6/boxlite'};
  assert.equal(configuration(next).runtimeHome,'/proof/native-browser-v6/boxlite');
  assert.equal(configuration(next).directory,'/proof/native-browser-v7');
  assert.throws(()=>configuration({...next,TANK_NATIVE_REUSE_CACHE_PROOF_DIR:'/app/data'}),/explicit owned/);
  assert.throws(()=>configuration({...next,TANK_NATIVE_REUSE_CACHE_PROOF_DIR:'/proof/native-browser-v7'}),/distinct/);
  assert.throws(()=>configuration({...next,TANK_NATIVE_REUSE_CACHE_PROOF_DIR:undefined}),/dedicated proof directory/);
});
