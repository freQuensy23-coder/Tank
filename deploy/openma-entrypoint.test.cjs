const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = process.argv[2] || '/app/tank-deploy/openma_entrypoint.mjs';
const source = fs.readFileSync(path, 'utf8').replace(/^import .*;\n/gm, '');
const reads = [];
const env = {};
let spawned = false;
vm.runInNewContext(source, {
  readFileSync(name) {
    reads.push(name);
    assert(!name.includes('typesafe'));
    return 'synthetic-only-' + name.split('/').at(-1);
  },
  spawn(command, args, options) {
    assert.equal(command, 'pnpm');
    assert.deepEqual(Array.from(args), ['start']);
    assert.equal(options.env.TANK_SANDBOX_JEV_PROVIDER, 'openrouter');
    assert.equal(options.env.TANK_SANDBOX_OPENROUTER_API_KEY, 'synthetic-only-openrouter_api_key');
    assert.equal(options.env.TANK_SANDBOX_TYPESAFE_API_KEY, undefined);
    assert.equal(options.env.OPENROUTER_API_KEY, undefined);
    assert.equal(options.env.CODEX_AUTH, undefined);
    assert(options.env.TANK_SANDBOX_REPLAY_DB_URL.startsWith('postgresql://tank_replay_agent:'));
    assert(options.env.TANK_SANDBOX_DB_URL.startsWith('postgresql://tank_agent:'));
    spawned = true;
    return {on() {}, kill() {}};
  },
  process: {env, on() {}, exit() {}},
});
assert(spawned);
assert(reads.includes('/run/secrets/openrouter_api_key'));
console.log('PASS: synthetic OpenRouter secret selection, isolated replay role, no TypeSafe/Codex auth reads');
