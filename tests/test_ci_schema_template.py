"""Owned schema allocation is an identity claim, never a prepared-database receipt."""

import os
from pathlib import Path

import pytest

from scripts.ci import prepare_postgres
from scripts.ci.managed_clickhouse import CIContext
from scripts.ci.schema_template import allocate_template, read_template, template_path
from test_ci_managed_clickhouse import context


def allocated_context(tmp_path: Path) -> CIContext:
    owner = context(tmp_path)
    (tmp_path / ".test-results").mkdir()
    return owner


def test_allocation_is_fresh_owned_and_explicitly_unprepared(tmp_path: Path) -> None:
    owner = allocated_context(tmp_path)
    first = allocate_template(owner)
    assert read_template(owner) == first
    assert first.phase == "ALLOCATED_NOT_PREPARED"
    assert first.owner == owner
    assert first.name.startswith("tank_test_ci_schema_")
    original = template_path(owner).read_bytes()
    with pytest.raises(FileExistsError):
        allocate_template(owner)
    assert template_path(owner).read_bytes() == original


def test_other_attempt_cannot_consume_the_allocation(tmp_path: Path) -> None:
    owner = allocated_context(tmp_path)
    allocate_template(owner)
    with pytest.raises(ValueError, match="another CI owner"):
        read_template(owner.model_copy(update={"attempt": owner.attempt + 1}))


def test_symlink_allocation_is_refused_without_replacing_target(tmp_path: Path) -> None:
    owner = allocated_context(tmp_path)
    actual = tmp_path / "synthetic-public-target"
    actual.write_text("synthetic public evidence")
    template_path(owner).symlink_to(actual)
    with pytest.raises(FileExistsError):
        allocate_template(owner)
    with pytest.raises(OSError):
        read_template(owner)
    assert actual.read_text() == "synthetic public evidence"


def test_allocation_file_requires_the_observed_private_mode(tmp_path: Path) -> None:
    owner = allocated_context(tmp_path)
    allocate_template(owner)
    template_path(owner).chmod(0o644)
    with pytest.raises(ValueError, match="owned private file"):
        read_template(owner)


def test_preparer_refuses_mismatched_collection_identity_before_database_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = allocated_context(tmp_path)
    allocate_template(owner)
    monkeypatch.chdir(tmp_path)
    for name, value in owner.environment().items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_WORKSPACE", str(tmp_path))
    monkeypatch.setenv("TANK_CI_SCHEMA_TEMPLATE", "tank_test_ci_schema_" + "f" * 32)
    monkeypatch.delenv("CI_POSTGRES_DSN", raising=False)
    with pytest.raises(ValueError, match="actual allocated collection identity"):
        prepare_postgres.main()
    assert "CI_POSTGRES_DSN" not in os.environ
