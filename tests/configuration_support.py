"""Capture the exact synthetic configuration bytes before constructing E2E services."""

import re
import shutil
import subprocess
from collections.abc import Mapping
from pathlib import Path

import yaml
from pydantic import JsonValue

from tank.settings import Settings, resolve_versions

ROOT = Path(__file__).resolve().parents[1]
NAMESPACE = re.compile(r"[a-z][a-z0-9_]*")


def captured_fixture_settings(
    settings: Settings, overrides: Mapping[str, Mapping[str, JsonValue]], directory: Path
) -> Settings:
    if any(NAMESPACE.fullmatch(name) is None for name in overrides):
        raise ValueError("Fixture configuration requires a relative declared namespace")
    if directory.exists():
        raise ValueError("Fixture configuration capture directory must be fresh")
    names = (
        subprocess.check_output(["git", "-C", str(ROOT), "ls-files", "-z", "--", "config"])
        .decode()
        .split("\0")
    )
    directory.mkdir(mode=0o700)
    for name in names:
        if not name:
            continue
        relative = Path(name).relative_to("config")
        if relative == Path("delivery.yaml"):
            continue
        source = ROOT / name
        if source.is_symlink() or not source.is_file():
            raise ValueError("Fixture configuration baseline must contain regular tracked files")
        destination = directory / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    for name, document in overrides.items():
        (directory / (name + ".yaml")).write_text(yaml.safe_dump(dict(document)))
    return resolve_versions(settings.model_copy(update={"config_dir": directory}))
