#!/usr/bin/env python3
"""Exercise built images offline; no database, operator credentials or service writes."""

import argparse
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[2]
DATABASE = "postgresql://synthetic:synthetic@127.0.0.1:1/tank_test_docker?connect_timeout=2"


def run(command, *, expected=0, stdin=None, timeout=240):
    result = subprocess.run(command, input=stdin, text=True, capture_output=True, timeout=timeout)
    print(result.stdout, end="")
    print(result.stderr, end="")
    if result.returncode != expected:
        raise RuntimeError(f"Expected exit {expected}, got {result.returncode}: {command}")
    return result.stdout


def inspect(image):
    return json.loads(run(["docker", "image", "inspect", "--format", "{{json .Config}}", image]))


def tank(image):
    config = inspect(image)
    assert config["Entrypoint"] == [
        "/usr/bin/tini", "--", "/opt/tank/venv/bin/python", "/app/deploy/tank_entrypoint.py"
    ], config["Entrypoint"]
    assert config["Cmd"] == ["tank", "serve"], config["Cmd"]
    packages = tomllib.loads((ROOT / "uv.lock").read_text())["package"]
    versions = {p["name"]: p["version"] for p in packages}
    required = {p["name"] for p in next(p for p in packages if p["name"] == "tank-v0")["dependencies"]}
    migrations = {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in (ROOT / "src/tank/migrations").glob("*.sql")
    }
    assert migrations
    code = "EXPECTED = " + repr(versions) + "\nREQUIRED = " + repr(required) + "\nMIGRATIONS = " + repr(migrations) + "\n" + r"""
import hashlib
import importlib
import importlib.metadata as md
import json
import os
from pathlib import Path
import re
import subprocess
import sys

assert sys.version_info[:2] == (3, 12), sys.version
assert os.getuid() == 1000, os.getuid()
for name in ("/data", "/data/replay/evidence", "/browser", "/codex-auth", "/backup"):
    assert os.access(name, os.W_OK), name
# psycopg_binary explicitly requires the public psycopg package first.
import psycopg
actual = {}
imports = {"pyyaml": "yaml", "python-dateutil": "dateutil", "python-dotenv": "dotenv", "tank-v0": "tank"}
for dist in md.distributions():
    name = re.sub(r"[-_.]+", "-", dist.metadata["Name"]).lower()
    assert name in EXPECTED, ("unexpected runtime distribution", name)
    assert dist.version == EXPECTED[name], (name, dist.version, EXPECTED[name])
    importlib.import_module(imports.get(name, name.replace("-", "_")))
    actual[name] = dist.version
assert REQUIRED <= actual.keys(), ("missing runtime dependencies", REQUIRED - actual.keys())
assert not {"pytest", "pytest-asyncio", "respx", "ruff"} & actual.keys()
import tank
files = Path(tank.__file__).parent / "migrations"
assert {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in files.glob("*.sql")} == MIGRATIONS
pg = {}
for name in ("pg_dump", "pg_restore", "psql"):
    value = subprocess.check_output([name, "--version"], text=True).strip()
    assert "(PostgreSQL) 16." in value, value
    pg[name] = value
from playwright.sync_api import sync_playwright
browsers = []
with sync_playwright() as playwright:
    for executable in (None, playwright.chromium.executable_path):
        options = {} if executable is None else {"executable_path": executable}
        browser = playwright.chromium.launch(timeout=45000, **options)
        try:
            page = browser.new_page()
            page.set_content("<title>Tank offline image check</title><h1>ready</h1>")
            assert page.title() == "Tank offline image check"
            assert page.locator("h1").inner_text() == "ready"
            assert page.evaluate("1 + 2") == 3
            assert page.screenshot(type="png")[:8] == b"\x89PNG\r\n\x1a\n"
            browsers.append({"version": browser.version, "executable": executable})
        finally:
            browser.close()
print(json.dumps({"packages_imported": actual, "pg": pg, "browsers": browsers, "uid": os.getuid()}))
"""
    run(["docker", "run", "--rm", "-i", "--network", "none", "--entrypoint", "/opt/tank/venv/bin/python", image, "-"], stdin=code)
    help_text = run([
        "docker", "run", "--rm", "--network", "none",
        "-e", "TANK_DATABASE_URL=" + DATABASE, "-e", "TANK_OPENMA_DATABASE_URL=" + DATABASE,
        image, "tank", "--help",
    ])
    assert "serve" in help_text and "backup" in help_text, help_text


def static_elf(data):
    assert data[:6] == b"\x7fELF\x02\x01", "Expected a little-endian ELF64 executable"
    start = struct.unpack_from("<Q", data, 32)[0]
    size, count = struct.unpack_from("<HH", data, 54)
    assert size >= 4 and count > 0
    assert start + size * count <= len(data)
    for index in range(count):
        assert struct.unpack_from("<I", data, start + index * size)[0] != 3, "Executable needs a dynamic interpreter"


def capture(image, revision):
    assert revision, "Capture requires --revision matching the build's GIT_SHA"
    config = inspect(image)
    assert config["User"] == "1000:1000", config["User"]
    assert config["Entrypoint"] == ["/tank-capture"], config["Entrypoint"]
    container = run(["docker", "create", "--network", "none", image]).strip()
    try:
        with tempfile.TemporaryDirectory(prefix="tank-capture-image-") as temporary:
            binary = Path(temporary) / "tank-capture"
            run(["docker", "cp", container + ":/tank-capture", str(binary)])
            data = binary.read_bytes()
            static_elf(data)
            assert (ROOT / "capture/internal/schema/market_schema.json").read_bytes() in data, "Embedded market schema missing"
    finally:
        run(["docker", "rm", container])
    output = run([
        "docker", "run", "--rm", "--network", "none",
        "--mount", "type=bind,src=" + str(ROOT / "config") + ",dst=/config,readonly",
        "-e", "TANK_DATABASE_URL=" + DATABASE, image,
    ], expected=1)
    rows = [json.loads(line) for line in output.splitlines() if line.strip()]
    starts = [row for row in rows if row.get("msg") == "starting"]
    assert len(starts) == 1, rows
    assert starts[0]["code_version"] == "tank-capture/" + revision, starts
    assert starts[0]["schema"] == "mkt" and starts[0]["instruments"] > 0, starts
    errors = [row for row in rows if row.get("level") == "ERROR"]
    assert any("connect" in row.get("error", "").lower() for row in errors), rows
    run(["docker", "run", "--rm", "--network", "none", image, "healthcheck"], expected=1)


def sandbox(image):
    contract = ROOT / "deploy/openma_browser/image-contract.py"
    run([
        "docker", "run", "--rm", "--network", "none",
        "--mount", "type=bind,src=" + str(contract) + ",dst=/tmp/tank-image-contract.py,readonly",
        "--entrypoint", "python3", image, "/tmp/tank-image-contract.py",
    ])


def openma_deps(image):
    code = r"""
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
    run(["docker", "run", "--rm", "--network", "none", "--entrypoint", "node", image, "--eval", code])
    version = run(["docker", "run", "--rm", "--network", "none", "--entrypoint", "pnpm", image, "--version"])
    assert version.strip() == "11.0.8", version
    typed = 'interface Result { answer: number }; const r: Result = {answer: 42}; if (r.answer !== 42) throw new Error("TypeScript execution failed"); console.log("tank-tsx-ready:" + r.answer)'
    output = run([
        "docker", "run", "--rm", "--network", "none", "--entrypoint", "pnpm", image,
        "--dir", "apps/main-node", "exec", "tsx", "--eval", typed,
    ])
    assert "tank-tsx-ready:42" in output, output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("kind", choices=("tank", "capture", "sandbox", "openma-deps"))
    parser.add_argument("--image", required=True)
    parser.add_argument("--revision")
    args = parser.parse_args()
    if args.kind == "tank":
        tank(args.image)
    elif args.kind == "capture":
        capture(args.image, args.revision)
    elif args.kind == "sandbox":
        sandbox(args.image)
    else:
        openma_deps(args.image)
    print(json.dumps({"image": args.image, "kind": args.kind, "status": "passed", "network": "none"}))


if __name__ == "__main__":
    main()
