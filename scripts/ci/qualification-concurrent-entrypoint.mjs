import assert from 'node:assert/strict';
import childProcess from 'node:child_process';
import { syncBuiltinESMExports } from 'node:module';

const spawn = childProcess.spawn;
childProcess.spawn = (command, args, options) => {
  assert.equal(command, process.execPath);
  assert.deepEqual(args, ['/app/apps/main-node/node_modules/tsx/dist/cli.mjs', 'src/index.ts']);
  return spawn(command, ['--import', '/app/apps/main-node/node_modules/tsx/dist/loader.mjs', '/qualification-concurrent-app.mjs'], options);
};
syncBuiltinESMExports();
await (await import('/app/tank-deploy/openma_entrypoint.mjs')).main();
