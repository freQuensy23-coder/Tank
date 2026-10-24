import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { JsBoxlite } from '/app/packages/control-plane-node/node_modules/@boxlite-ai/boxlite/dist/index.js';
import { loadNodeConfig } from '/app/apps/main-node/src/config.ts';
import { nodeDefaults } from '/app/apps/main-node/src/components.ts';
import { createNodeControlPlane, serveNodeControlPlane } from '/app/packages/control-plane-node/src/index.ts';

const native = JsBoxlite.withDefaultConfig();
let nativeBindings = 0;
JsBoxlite.withDefaultConfig = () => { nativeBindings++; return native; };
const controllers = [];
let starting = false;
async function start(competitor) {
  const env = { ...process.env, PORT: competitor ? '8788' : '8787', OMA_SESSION_EXECUTION_OWNER_ID: process.env.QUALIFICATION_OWNER_PREFIX + (competitor ? ':competitor' : ':primary') };
  if (competitor) {
    const database = new URL(env.DATABASE_URL);
    database.hostname = 'database';
    env.DATABASE_URL = database.href;
  }
  const config = loadNodeConfig(env);
  const controller = await createNodeControlPlane(await nodeDefaults(config));
  await controller.start();
  const server = await serveNodeControlPlane(controller, config.http);
  controllers.push({ controller, server, ownerId: config.execution.ownerId });
}
await start(false);
const control = createServer(async (request, response) => {
  if (request.headers['x-api-key'] !== process.env.QUALIFICATION_PROVIDER_KEY) { response.writeHead(403).end(); return; }
  if (request.url !== '/start-competitor' || request.method !== 'POST' || controllers.length !== 1 || starting) { response.writeHead(409).end(); return; }
  starting = true;
  try {
    await start(true);
    assert.equal(nativeBindings, 2, 'both real control planes must bind the one native runtime client');
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ owners: controllers.map(row => row.ownerId), nativeBindings, sharedNativeRuntime: true, ports: controllers.map(row => row.server.address.port), pid: process.pid }));
  } catch (error) { response.writeHead(500).end(JSON.stringify({ error: error.message })); }
});
control.listen(8790, '127.0.0.1');
let stopping = false;
for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, async () => {
  if (stopping) return;
  stopping = true;
  control.close();
  try {
    await Promise.all(controllers.map(async ({ controller, server }) => { await server.close(); await controller.stop(signal); }));
    process.exit(0);
  } catch (error) { console.error(error.message); process.exit(1); }
});
