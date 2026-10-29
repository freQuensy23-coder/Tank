import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const lanes = {
  "local-unit": [
    "deploy/openma-entrypoint.test.mjs",
    "tests/openma/agents/tool-config.test.mjs",
    "tests/openma/lifecycle/retained-session.test.mjs",
    "tests/openma/lifecycle/boxlite-provider.test.mjs",
    "tests/openma/lifecycle/guest-filesystem.test.mjs",
    "tests/openma/lifecycle/durable-publication.test.mjs",
    "tests/openma/lifecycle/node-lifecycle.test.mjs",
    "tests/openma/lifecycle/replay-attachment.test.mjs",
    "tests/openma/lifecycle/image-upgrade.test.mjs",
    "tests/openma/lifecycle/runtime-identity.test.mjs",
    "tests/openma/lifecycle/image-recovery.test.mjs",
    "tests/openma/lifecycle/custom-tool-validation.test.mjs",
    "tests/openma/lifecycle/memory-bindings.test.mjs",
    "tests/openma/events/span-finish-reason.test.mjs",
  ],
  "local-contract": [
    "deploy/openma_lifecycle/native-qualification.test.mjs",
    "tests/openma/replay/replay-evidence.test.mjs",
    "tests/openma/lifecycle/workspace-transfer.test.mjs",
  ],
};

const lane = process.argv[2];
if (process.argv.length !== 3 || !Object.hasOwn(lanes, lane)) {
  console.error(
    "Usage: node scripts/test-node.mjs <local-unit|local-contract>\n" +
      "These dependency-free lanes exclude upstream Vitest/SQLite, file-worker, " +
      "Chromium and native qualification tests.",
  );
  process.exit(2);
}
if (Number(process.versions.node.split(".")[0]) < 24) {
  console.error("Node 24 or later is required for the local JavaScript tests.");
  process.exit(2);
}

const result = spawnSync(
  process.execPath,
  [
    "--import", resolve(root, "scripts/node-typescript-loader.mjs"),
    "--experimental-transform-types",
    "--experimental-test-module-mocks",
    "--test",
    "--test-reporter=tap",
    "--test-concurrency=1",
    "--test-timeout=" + (lane === "local-unit" ? "10000" : "30000"),
    ...lanes[lane],
  ],
  { cwd: root, stdio: "inherit", timeout: 60000 },
);
if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}
process.exit(result.status ?? 1);
