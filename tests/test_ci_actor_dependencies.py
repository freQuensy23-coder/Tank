import re
import shutil
from pathlib import Path

import pytest

from scripts.ci.check_actor_dependencies import (
    ActorDependencyError,
    check_actor_dependencies,
    frozen_actor_requirements,
    locate_uv,
)

ROOT = Path(__file__).resolve().parents[1]


def copied_project(tmp_path: Path) -> Path:
    project = tmp_path / "project"
    project.mkdir()
    for name in ("pyproject.toml", "uv.lock"):
        shutil.copyfile(ROOT / name, project / name)
    return project


def test_actor_export_is_hashed_without_project_or_development_closure(tmp_path: Path) -> None:
    project = copied_project(tmp_path)
    uv = locate_uv()
    exported = frozen_actor_requirements(project, uv)
    names = set(re.findall(rb"^([A-Za-z0-9_.-]+)==", exported, flags=re.MULTILINE))
    assert {b"apify", b"beautifulsoup4", b"httpx"} <= names
    assert not names.intersection({b"tank-v0", b"pytest", b"pytest-xdist", b"mypy", b"ruff", b"pandas"})
    assert b"--hash=sha256:" in exported
    target = project / "apify/x-reader/requirements.txt"
    target.parent.mkdir(parents=True)
    target.write_bytes(exported)
    check_actor_dependencies(project, uv)
    damaged = exported + b"unlocked-package==1.0\n"
    target.write_bytes(damaged)
    with pytest.raises(ActorDependencyError, match="differ"):
        check_actor_dependencies(project, uv)
    assert target.read_bytes() == damaged
    assert not (project / ".venv").exists()


def test_actor_export_refuses_declaration_lock_drift_without_relocking(tmp_path: Path) -> None:
    project = copied_project(tmp_path)
    before = (project / "uv.lock").read_bytes()
    specification = project / "pyproject.toml"
    specification.write_text(
        specification.read_text().replace(
            "x-reader = [", 'x-reader = ["not-in-the-frozen-actor-lock==0.0.0",', 1
        )
    )
    with pytest.raises(ActorDependencyError, match="Frozen actor dependency tool failed"):
        frozen_actor_requirements(project, locate_uv())
    assert (project / "uv.lock").read_bytes() == before
    assert not (project / ".venv").exists()


def test_actor_export_refuses_wrong_uv_before_project_access(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    executable = tmp_path / "uv"
    executable.write_text("#!/bin/sh\nprintf 'uv 0.9.5\\n'\n")
    executable.chmod(0o700)
    with pytest.raises(ActorDependencyError, match="requires uv 0.11.7"):
        frozen_actor_requirements(project, executable)
    assert list(project.iterdir()) == []
