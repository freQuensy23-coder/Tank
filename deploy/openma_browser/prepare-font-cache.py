#!/usr/bin/env python3
"""Build the installed Chromium's public font caches without retaining a profile."""

import argparse
import hashlib
import importlib.metadata
import json
import re
import tempfile
from pathlib import Path

from playwright.sync_api import sync_playwright


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare(manifest_path) -> None:
    assert importlib.metadata.version("playwright") == "1.64.0"
    cache_root = Path("/var/cache/fontconfig")
    original8 = {p.name: digest(p) for p in cache_root.glob("*-le64.cache-8") if p.is_file()}
    original_names = {p.name for p in cache_root.iterdir()}
    with (
        tempfile.TemporaryDirectory(prefix="tank-browser-font-cache-") as temporary,
        sync_playwright() as playwright,
    ):
        context = playwright.chromium.launch_persistent_context(
            str(Path(temporary) / "profile"),
            headless=True,
            accept_downloads=True,
            downloads_path=str(Path(temporary) / "downloads"),
            viewport={"width": 1280, "height": 900},
        )
        try:
            version = context.browser.version
            assert version == "156.0.8078.4", version
            page = context.pages[0]
            page.set_content("<html><body><h1>Tank public font cache build</h1></body></html>")
            assert page.locator("h1").inner_text() == "Tank public font cache build"
            assert page.screenshot(type="png")[:8].hex() == "89504e470d0a1a0a"
        finally:
            context.close()
    assert not Path(temporary).exists()
    assert all(digest(cache_root / name) == value for name, value in original8.items())
    cache12 = {
        p.name: digest(p)
        for p in sorted(cache_root.glob("*-le64.cache-12"))
        if p.is_file() and not p.is_symlink()
    }
    assert cache12 and all(re.fullmatch(r"[0-9a-f]{32}-le64.cache-12", n) for n in cache12)

    removed_aliases = []
    for path in cache_root.iterdir():
        if (
            path.name not in original_names
            and path.is_symlink()
            and re.fullmatch(r"[0-9a-f]{32}-le64.cache-(9|10|11)", path.name)
            and path.resolve().parent == cache_root
            and path.resolve().name in cache12
        ):
            removed_aliases.append(path.name)
            path.unlink()
    fonts = {
        str(path): digest(path)
        for path in sorted(Path("/usr/share/fonts").rglob("*"))
        if path.is_file() and path.suffix.lower() in {".ttf", ".ttc", ".otf", ".pfa", ".pfb"}
    }
    manifest = {
        "playwright": "1.64.0",
        "worker_sha256": digest(Path("/opt/tank/browser-worker.py")),
        "chromium": version,
        "engine": "Playwright default bundled Chromium headless shell",
        "cache_format": 12,
        "cache12": cache12,
        "font_files": fonts,
        "original_cache8_unchanged": True,
        "new_older_format_aliases_removed": sorted(removed_aliases),
        "temporary_profile_removed": True,
        "fixture": "Local static HTML, text and PNG; no external network",
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"font_cache_manifest": str(manifest_path), "cache12_files": len(cache12)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=Path("/opt/tank/browser-font-cache.json"))
    prepare(parser.parse_args().manifest)
