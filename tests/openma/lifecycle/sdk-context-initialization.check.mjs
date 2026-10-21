import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { pathToFileURL } from 'node:url';
const root=process.env.TANK_SDK_CHECK_ROOT ?? '/app';
const caller=join(root,'packages/control-plane-node/src/modules/node-managed.ts');
const manifest=JSON.parse(await readFile(join(root,'packages/control-plane-node/package.json'),'utf8'));
assert.ok(!process.env.NODE_PATH,'NODE_PATH dependency workarounds are forbidden');
const directory=await mkdtemp(join(tmpdir(),'tank-sdk-context-init-'));
let control;
try {
  process.env.BOXLITE_HOME=join(directory,'boxlite');
const resolution=spawnSync(process.execPath,['--input-type=module','-e',
  'const sdk=await import("@boxlite-ai/boxlite"); console.log("__TANK_SDK_CONTEXT__ "+JSON.stringify({url:import.meta.resolve("@boxlite-ai/boxlite"),hasDefaultConfig:typeof sdk.JsBoxlite.withDefaultConfig === "function"}));'],
  {cwd:dirname(caller),env:process.env,encoding:'utf8',timeout:10000});
if(resolution.status!==0) throw Error('Actual native ESM control-plane caller import failed: '+resolution.stderr);
const context=JSON.parse(resolution.stdout.split('\n').find(line=>line.startsWith('__TANK_SDK_CONTEXT__ ')).slice('__TANK_SDK_CONTEXT__ '.length));
assert.equal(context.hasDefaultConfig,true);const resolved=context.url;
assert.equal(manifest.dependencies['@boxlite-ai/boxlite'],'0.10.6');
  const {loadNodeConfig}=await import(pathToFileURL(join(root,'apps/main-node/src/config.ts')).href);
  const {nodeDefaults}=await import(pathToFileURL(join(root,'apps/main-node/src/components.ts')).href);
  const {createNodeControlPlane}=await import(pathToFileURL(join(root,'packages/control-plane-node/src/index.ts')).href);
  const config=loadNodeConfig({NODE_ENV:'production',AUTH_DISABLED:'1',SANDBOX_PROVIDER:'litebox',MEMORY_QUEUE:'disabled',
    DATABASE_PATH:join(directory,'oma.sqlite'),AUTH_DATABASE_PATH:join(directory,'auth.sqlite'),
    MEMORY_BLOB_DIR:join(directory,'memory'),FILES_BLOB_DIR:join(directory,'files'),
    SANDBOX_WORKDIR:join(directory,'sandboxes'),SESSION_OUTPUTS_DIR:join(directory,'outputs'),
    API_KEY:'FAKE_SDK_INITIALIZATION_ONLY',HOST:'127.0.0.1',PORT:'0',DREAM_CURATOR_MODE:'dedup'});
  control=await createNodeControlPlane(await nodeDefaults(config));
  const {JsBoxlite}=await import(resolved);
  const runtime=JsBoxlite.withDefaultConfig();
  assert.deepEqual(await runtime.listInfo(),[]);
  assert.equal((await runtime.metrics()).numRunningBoxes,0);
  await control.stop('sdk-initialization-check');control=null;
  console.log(JSON.stringify({status:'PASS',actualControlPlaneCallerResolved:true,actualCreateManagedNodeRuntimeInitialized:true,
    nativeCatalogEmpty:true,nativeVmCreated:false,httpServerStarted:false,sessionCreated:false,modelCalls:0,externalRequests:0,
    scope:'ACTUAL_SQLITE_ASSEMBLY_ONLY_NOT_POSTGRES_SERVER_OR_API_HISTORY'}));
} finally {
  if(control) await control.stop('sdk-initialization-check-failure');
  await rm(directory,{recursive:true,force:true});
}
