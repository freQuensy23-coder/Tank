"""Frozen input, real archive and ownership controls for public-history setup."""

import base64
import hashlib
import io
import json
import os
import tarfile
from pathlib import Path
from typing import Literal

import pytest
import yaml
from pydantic import Field, ValidationError

from scripts.openma_history.manifest import ROOT, Package, Resolution, compatible
from scripts.openma_history.models import Archive, Ownership
from scripts.openma_history.setup import cached_archive, extract_archive, own_directory, read_manifest


def archive_bytes(*, name: str, version: str, extra_path: str | None) -> bytes:
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as bundle:
        contents = json.dumps({"name": name, "version": version, "license": "MIT"}).encode()
        entry = tarfile.TarInfo("package/package.json")
        entry.size = len(contents)
        bundle.addfile(entry, io.BytesIO(contents))
        if extra_path is not None:
            entry = tarfile.TarInfo(extra_path)
            entry.size = 1
            bundle.addfile(entry, io.BytesIO(b"x"))
    return stream.getvalue()


def declared_archive(raw: bytes) -> Archive:
    return Archive(
        key="fixture-package@1.0.0",
        name="fixture-package",
        version="1.0.0",
        integrity="sha512-" + base64.b64encode(hashlib.sha512(raw).digest()).decode(),
    )


def owner(*, purpose: str) -> Ownership:
    return Ownership.model_validate(
        {
            "schema_version": "tank.openma-history.owned-setup.v1",
            "manifest_sha256": "a" * 64,
            "platform": "linux-arm64-gnu",
            "uid": os.geteuid(),
            "purpose": purpose,
        }
    )


def test_both_frozen_platform_plans_preserve_actual_roots_and_native_variants() -> None:
    manifest, _ = read_manifest(ROOT)
    plans = {plan.platform: plan for plan in manifest.plans}
    assert len(plans["linux-arm64-gnu"].snapshots) == 489
    assert len(plans["linux-x64-gnu"].snapshots) == 490
    arm = {snapshot.archive for snapshot in plans["linux-arm64-gnu"].snapshots}
    x64 = {snapshot.archive for snapshot in plans["linux-x64-gnu"].snapshots}
    assert x64 - arm == {
        "@esbuild/linux-x64@0.28.1",
        "@rollup/rollup-linux-x64-gnu@4.62.4",
        "@napi-rs/lzma-linux-x64-gnu@1.5.1",
    }
    assert arm - x64 == {"@esbuild/linux-arm64@0.28.1", "@rollup/rollup-linux-arm64-gnu@4.62.4"}
    for plan in plans.values():
        snapshots = {snapshot.key: snapshot for snapshot in plan.snapshots}
        assert len({snapshot.context for snapshot in plan.snapshots}) == len(snapshots)
        assert all(edge.target in snapshots for snapshot in snapshots.values() for edge in snapshot.edges)
        mocker = next(snapshot for snapshot in plan.snapshots if snapshot.archive == "@vitest/mocker@4.1.4")
        vite = next(edge for edge in mocker.edges if edge.name == "vite")
        assert vite.target in snapshots
        assert snapshots[vite.target].archive == "vite@7.3.6"
        support = next(link for link in plan.roots if link.name == "undici-types")
        assert support.target == "undici-types@8.9.0"
        assert support.reason == "DECLARED_ROOT_TYPE_SUPPORT"


@pytest.mark.parametrize("platform,accepted", [("linux-arm64-gnu", False), ("linux-x64-gnu", True)])
def test_actual_platform_declarations_do_not_select_wrong_native_architecture(
    platform: str, accepted: bool
) -> None:
    package = Package(
        resolution=Resolution(integrity="captured-by-parent-test"), os=["linux"], cpu=["x64"], libc=["glibc"]
    )
    if platform == "linux-arm64-gnu":
        assert compatible(package, "linux-arm64-gnu") is accepted
    else:
        assert compatible(package, "linux-x64-gnu") is accepted


def test_real_archive_extracts_required_identity_and_preserves_bytes(tmp_path: Path) -> None:
    raw = archive_bytes(name="fixture-package", version="1.0.0", extra_path="package/source.txt")
    extract_archive(raw, declared_archive(raw), tmp_path / "package")
    assert (tmp_path / "package/source.txt").read_bytes() == b"x"


def test_archive_integrity_refusal_precedes_extraction(tmp_path: Path) -> None:
    raw = archive_bytes(name="fixture-package", version="1.0.0", extra_path=None)
    with pytest.raises(ValueError, match="integrity"):
        extract_archive(raw + b"tamper", declared_archive(raw), tmp_path / "package")
    assert not (tmp_path / "package").exists()


def test_archive_cannot_escape_destination_even_with_declared_integrity(tmp_path: Path) -> None:
    raw = archive_bytes(name="fixture-package", version="1.0.0", extra_path="package/../../escaped")
    with pytest.raises(ValueError, match="escapes"):
        extract_archive(raw, declared_archive(raw), tmp_path / "package")
    assert not (tmp_path / "escaped").exists()


def test_reported_archive_identity_cannot_be_borrowed(tmp_path: Path) -> None:
    raw = archive_bytes(name="different-package", version="1.0.0", extra_path=None)
    with pytest.raises(ValueError, match="identity"):
        extract_archive(raw, declared_archive(raw), tmp_path / "package")


def test_unknown_existing_root_is_preserved(tmp_path: Path) -> None:
    folder = tmp_path / "root"
    folder.mkdir()
    evidence = folder / "unrelated"
    evidence.write_bytes(b"preserved")
    with pytest.raises(ValueError, match="unknown"):
        own_directory(folder, owner(purpose="DEPENDENCY_ROOT"), cache=False)
    assert evidence.read_bytes() == b"preserved"


def test_owned_materialized_root_cannot_be_reused_as_fresh(tmp_path: Path) -> None:
    folder = tmp_path / "root"
    own_directory(folder, owner(purpose="DEPENDENCY_ROOT"), cache=False)
    with pytest.raises(ValueError, match="fresh"):
        own_directory(folder, owner(purpose="DEPENDENCY_ROOT"), cache=False)


def test_cache_owner_is_bound_to_manifest_and_platform(tmp_path: Path) -> None:
    folder = tmp_path / "cache"
    actual = owner(purpose="ARCHIVE_CACHE")
    own_directory(folder, actual, cache=True)
    changed = actual.model_copy(update={"manifest_sha256": "b" * 64})
    with pytest.raises(ValueError, match="differs"):
        own_directory(folder, changed, cache=True)


def test_cache_symlink_is_not_followed(tmp_path: Path) -> None:
    actual = tmp_path / "actual"
    actual.mkdir()
    link = tmp_path / "link"
    link.symlink_to(actual, target_is_directory=True)
    with pytest.raises(ValueError, match="symbolic"):
        own_directory(link, owner(purpose="ARCHIVE_CACHE"), cache=True)


def test_offline_cache_refuses_missing_or_tampered_bytes(tmp_path: Path) -> None:
    raw = archive_bytes(name="fixture-package", version="1.0.0", extra_path=None)
    archive = declared_archive(raw)
    with pytest.raises(ValueError, match="absent"):
        cached_archive(tmp_path, archive, offline=True)
    (tmp_path / archive.filename).write_bytes(b"not-the-declared-archive")
    with pytest.raises(ValueError, match="integrity"):
        cached_archive(tmp_path, archive, offline=True)


@pytest.mark.parametrize("name", ["../escape", "/absolute", "@scope/../escape"])
def test_package_names_cannot_select_paths(name: str) -> None:
    with pytest.raises(ValidationError):
        Archive(
            key="fixture",
            name=name,
            version="1.0.0",
            integrity="sha512-" + base64.b64encode(b"x" * 64).decode(),
        )


def test_active_history_job_preserves_existing_lane_and_actual_runner_contract() -> None:
    from pydantic import BaseModel, ConfigDict

    class Step(BaseModel):
        model_config = ConfigDict(extra="ignore", strict=True)
        run: str | None = None

    class Job(BaseModel):
        model_config = ConfigDict(extra="ignore", strict=True, populate_by_name=True)
        steps: list[Step]
        runs_on: Literal["ubuntu-24.04"] = Field(alias="runs-on")
        timeout_minutes: int = Field(alias="timeout-minutes", strict=True)

    class Workflow(BaseModel):
        model_config = ConfigDict(extra="ignore", strict=True)
        jobs: dict[str, Job]

    document = Workflow.model_validate(
        yaml.safe_load((ROOT / ".github/workflows/test-suite.yml").read_bytes())
    )
    assert document.jobs["node-public-history"].timeout_minutes == 4
    existing = "\n".join(step.run for step in document.jobs["node-persistence"].steps if step.run is not None)
    history = "\n".join(
        step.run for step in document.jobs["node-public-history"].steps if step.run is not None
    )
    assert "node scripts/openma-persistence/run.mts" in existing
    assert "node scripts/openma-persistence/run.mts" not in history
    assert "node scripts/openma-persistence/history-module.mts" in history
    assert "env -i HOME=" in history
    assert "--ignore-scripts" in history
    assert "scripts.openma_history.manifest --check" in history


def test_declared_owner_cannot_substitute_an_unobserved_uid(tmp_path: Path) -> None:
    folder = tmp_path / "cache"
    wrong = owner(purpose="ARCHIVE_CACHE").model_copy(update={"uid": os.geteuid() + 1})
    with pytest.raises(ValueError, match="effective UID"):
        own_directory(folder, wrong, cache=True)
    assert not folder.exists()


def test_writable_by_other_users_directory_is_not_owned_setup(tmp_path: Path) -> None:
    folder = tmp_path / "cache"
    folder.mkdir(mode=0o777)
    folder.chmod(0o777)
    with pytest.raises(ValueError, match="ownership"):
        own_directory(folder, owner(purpose="ARCHIVE_CACHE"), cache=True)
    assert not (folder / ".tank-owned-setup.json").exists()
