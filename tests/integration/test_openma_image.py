"""Pinned OpenMA dependency stage only; no native BoxLite qualification."""


def test_node_24_and_real_native_typescript_transform(docker, image):
    docker("run", "--rm", "--network", "none", "--entrypoint", "node", image, "--eval", ESBUILD_CHECK)


def test_pinned_pnpm_executes_typed_source_offline(docker, image):
    version = docker("run", "--rm", "--network", "none", "--entrypoint", "pnpm", image, "--version")
    assert version.strip() == "11.0.8"
    typed = (
        "interface Result { answer: number }; const r: Result = {answer: 42}; "
        'if (r.answer !== 42) throw new Error("TypeScript execution failed"); '
        'console.log("tank-tsx-ready:" + r.answer)'
    )
    output = docker(
        "run",
        "--rm",
        "--network",
        "none",
        "--entrypoint",
        "pnpm",
        image,
        "--dir",
        "apps/main-node",
        "exec",
        "tsx",
        "--eval",
        typed,
    )
    assert "tank-tsx-ready:42" in output


ESBUILD_CHECK = r"""
const assert = require("node:assert/strict");
assert.equal(Number(process.versions.node.split(".")[0]), 24);
const appRequire = require("node:module").createRequire("/app/apps/main-node/package.json");
const tsxRequire = require("node:module").createRequire(appRequire.resolve("tsx"));
const esbuild = tsxRequire("esbuild");
const source = "const answer: number = [20, 22].reduce((a, b) => a + b, 0);";
const result = esbuild.transformSync(source, { loader: "ts", target: "node24" });
assert.equal(require("node:vm").runInNewContext(result.code + "answer;"), 42);
console.log(JSON.stringify({ node: process.versions.node, esbuild: esbuild.version, transformed: true }));
"""
