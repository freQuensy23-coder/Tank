"""Contracts of the actual Tank image, with no external network or credentials."""

import hashlib
import tomllib


def test_default_entrypoint_and_cli(docker, image, image_config):
    assert image_config["Entrypoint"] == [
        "/usr/bin/tini",
        "--",
        "/opt/tank/venv/bin/python",
        "/app/deploy/tank_entrypoint.py",
    ]
    assert image_config["Cmd"] == ["tank", "serve"]
    database = "postgresql://synthetic:synthetic@127.0.0.1:1/tank_test_docker?connect_timeout=2"
    output = docker(
        "run",
        "--rm",
        "--network",
        "none",
        "-e",
        "TANK_DATABASE_URL=" + database,
        "-e",
        "TANK_OPENMA_DATABASE_URL=" + database,
        image,
        "tank",
        "--help",
    )
    assert "serve" in output and "backup" in output


def test_frozen_packages_migrations_pg_clients_and_permissions(docker, image, repository_root):
    packages = tomllib.loads((repository_root / "uv.lock").read_text())["package"]
    versions = {package["name"]: package["version"] for package in packages}
    required = {
        package["name"]
        for package in next(package for package in packages if package["name"] == "tank-v0")["dependencies"]
    }
    migrations = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (repository_root / "src/tank/migrations").glob("*.sql")
    }
    assert migrations
    code = (
        "EXPECTED = "
        + repr(versions)
        + "\nREQUIRED = "
        + repr(required)
        + "\nMIGRATIONS = "
        + repr(migrations)
        + "\n"
        + RUNTIME_CHECK
    )
    docker(
        "run",
        "--rm",
        "-i",
        "--network",
        "none",
        "--entrypoint",
        "/opt/tank/venv/bin/python",
        image,
        "-",
        stdin=code,
    )


def test_both_chromiums_and_canonical_single_driver(docker, image):
    docker(
        "run",
        "--rm",
        "-i",
        "--network",
        "none",
        "--entrypoint",
        "/opt/tank/venv/bin/python",
        image,
        "-",
        stdin=BROWSER_CHECK,
    )


RUNTIME_CHECK = r"""
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
print(json.dumps({"packages_imported": actual, "pg": pg, "uid": os.getuid()}))
"""

BROWSER_CHECK = r"""
from pathlib import Path
import json
from playwright.sync_api import sync_playwright
import playwright
package = Path(playwright.__file__).parent
canonical_driver = package / "driver" / "package"
links = list(Path("/opt/playwright/.links").glob("*"))
assert links, "Browser installation registration missing"
for link in links:
    registered = Path(link.read_text().strip())
    assert registered.is_dir() and registered.resolve() == canonical_driver.resolve(), (link, registered)
nodes = [path for path in Path("/opt").rglob("node") if path.is_file() and path.parent.name == "driver"]
assert nodes == [package / "driver" / "node"], nodes
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
print(json.dumps({"browsers": browsers}))
"""
