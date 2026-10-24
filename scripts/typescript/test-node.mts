import { spawnSync } from "node:child_process";
import { statSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { verifyTestReport } from "./test-report.mts";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const lanes = {
  "local-unit": [
    "scripts/typescript/test-report.test.mts",
    "scripts/typescript/inventory.test.mts",
    "deploy/openma-entrypoint.test.mjs",
    "tests/openma/lifecycle/retained-session.test.mjs",
    "tests/openma/lifecycle/boxlite-provider.test.mjs",
    "tests/openma/lifecycle/node-lifecycle.test.mjs",
    "tests/openma/lifecycle/replay-attachment.test.mjs",
  ],
  "local-contract": [
    "deploy/openma_lifecycle/native-qualification.test.mjs",
    "tests/openma/replay/replay-evidence.test.mjs",
  ],
} as const;

type Lane = keyof typeof lanes;

function isLane(value: string): value is Lane {
  return value === "local-unit" || value === "local-contract";
}

const lane = process.argv[2];
if (process.argv.length !== 3 || lane === undefined || !isLane(lane)) {
  console.error(
    "Usage: node scripts/test-node.mjs <local-unit|local-contract>\n" +
      "These dependency-free lanes exclude upstream Vitest/SQLite, file-worker, " +
      "Chromium and native qualification tests.",
  );
  process.exit(2);
}
if (process.versions.node !== "24.22.0") {
  console.error("Node 24.22.0 is required for the local JavaScript tests.");
  process.exit(2);
}

for (const file of lanes[lane]) {
  if (!statSync(resolve(root, file)).isFile()) throw Error("Missing declared Node test file: " + file);
}
if (lane === "local-unit") {
  const python = process.env.TANK_FILE_IO_TEST_PYTHON;
  if (python === undefined) throw Error("TANK_FILE_IO_TEST_PYTHON must name Python 3.13.13");
  const version = spawnSync(python, ["-c", "import sys; assert sys.version_info[:3] == (3, 13, 13)"], { encoding: "utf8", timeout: 5000 });
  if (version.error !== undefined || version.status !== 0) throw Error("File bridge tests require Python 3.13.13");
}

const result = spawnSync(
  process.execPath,
  [
    "--import",
    fileURLToPath(new URL("./register-openma-source.mts", import.meta.url)),
    "--experimental-test-module-mocks",
    "--test",
    "--test-reporter=tap",
    "--test-timeout=" + (lane === "local-unit" ? "10000" : "30000"),
    ...lanes[lane],
  ],
  { cwd: root, stdio: ["inherit", "pipe", "pipe"], encoding: "utf8", timeout: 60000 },
);
process.stdout.write(result.stdout);
process.stderr.write(result.stderr);
if (result.error !== undefined) {
  console.error(result.error.message);
  process.exit(1);
}
if (result.status === null) {
  console.error("JavaScript test runner terminated without an exit status.");
  process.exit(1);
}
if (result.status === 0) verifyTestReport(result.stdout);
process.exit(result.status);
