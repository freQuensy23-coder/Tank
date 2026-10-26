"""Backup reference resolution cannot escape its admitted recovery tree."""

import json
from pathlib import Path

import pytest
from pydantic import JsonValue

from tank.contracts.backup_manifest import BackupManifest
from tank.scheduler import resolve_backup_reference, verify_backup
from tank.time_precision import collection_clock
from tank.util import sha256


def manifest_fixture(backup: Path, original: Path) -> dict[str, JsonValue]:
    backup.mkdir()
    files: dict[str, JsonValue] = {}
    for name in ("tank.dump", "oma.dump"):
        body = ("Synthetic fixture " + name).encode()
        (backup / name).write_bytes(body)
        files[name] = sha256(body)
    manifest: dict[str, JsonValue] = {
        "version": 1,
        **collection_clock().fields("created_at"),
        "reference_base": str(original.parent),
        "roots": {"bundles": str(original)},
        "files": files,
        "database_dumps": ["tank.dump", "oma.dump"],
        "scope": "Synthetic isolated recovery fixture, no database connection",
        "excluded": [],
        "restore": "Resolve only the fixture evidence; these are not real database dumps",
    }
    return manifest


def save_manifest(backup: Path, manifest: dict[str, JsonValue]) -> None:
    (backup / "manifest.json").write_text(json.dumps(manifest))
    (backup / "COMPLETE").write_text("Synthetic fixture completion")


def test_reference_refuses_symlink_escape_after_an_admitted_backup_changes(tmp_path: Path) -> None:
    original, backup, outside = (tmp_path / name for name in ("original", "backup", "outside"))
    original.mkdir()
    outside.mkdir()
    manifest = manifest_fixture(backup, original)
    bundled = backup / "bundles"
    bundled.mkdir()
    body = b"Synthetic retained evidence"
    (bundled / "item.json").write_bytes(body)
    files = manifest["files"]
    assert isinstance(files, dict)
    files["bundles/item.json"] = sha256(body)
    save_manifest(backup, manifest)
    verify_backup(backup)
    assert (
        resolve_backup_reference(
            backup, original / "item.json", manifest=BackupManifest.model_validate(manifest)
        ).read_bytes()
        == body
    )
    (bundled / "item.json").unlink()
    (bundled / "item.json").symlink_to(outside / "item.json")
    (outside / "item.json").write_bytes(b"Synthetic outside fixture, not credentials")
    with pytest.raises(ValueError, match="outside|symlink"):
        resolve_backup_reference(
            backup, original / "item.json", manifest=BackupManifest.model_validate(manifest)
        )


def test_explicit_empty_manifest_is_invalid_instead_of_replaced_from_disk(tmp_path: Path) -> None:
    original, backup = tmp_path / "original", tmp_path / "backup"
    original.mkdir()
    manifest = manifest_fixture(backup, original)
    bundled = backup / "bundles"
    bundled.mkdir()
    (bundled / "item.json").write_bytes(b"Synthetic fixture")
    files = manifest["files"]
    assert isinstance(files, dict)
    files["bundles/item.json"] = sha256(b"Synthetic fixture")
    save_manifest(backup, manifest)
    with pytest.raises(ValueError):
        resolve_backup_reference(backup, original / "item.json", manifest=BackupManifest.model_construct())


@pytest.mark.parametrize("version", [True, 1.0, 0, 2])
def test_manifest_version_is_an_actual_supported_integer(tmp_path: Path, version: JsonValue) -> None:
    manifest = manifest_fixture(tmp_path / "backup", tmp_path / "original")
    manifest["version"] = version
    with pytest.raises(ValueError):
        BackupManifest.model_validate(manifest)


@pytest.mark.parametrize("relative", ["../outside", "/outside", "bundles/../outside", "unknown/item"])
def test_manifest_refuses_unbound_or_escaping_inventory(tmp_path: Path, relative: str) -> None:
    backup = tmp_path / "backup"
    manifest = manifest_fixture(backup, tmp_path / "original")
    files = manifest["files"]
    assert isinstance(files, dict)
    files[relative] = sha256(b"Synthetic fixture")
    save_manifest(backup, manifest)
    with pytest.raises(ValueError):
        verify_backup(backup)


def test_unique_manifest_admission_preserves_original_bytes_and_unknown_historical_precision(
    tmp_path: Path,
) -> None:
    backup = tmp_path / "backup"
    manifest = manifest_fixture(backup, tmp_path / "original")
    manifest.pop("scope")
    manifest.pop("created_at_prec")
    manifest.pop("created_at_prec_reason")
    save_manifest(backup, manifest)
    original = (backup / "manifest.json").read_bytes()
    admitted = verify_backup(backup)
    assert admitted.created_at_prec is None and admitted.scope is None
    assert admitted.model_dump(mode="json", exclude_unset=True) == manifest
    assert (backup / "manifest.json").read_bytes() == original
    duplicated = original[:-1] + b', "files": {}, "ignored-secret": "synthetic-private-value"}'
    (backup / "manifest.json").write_bytes(duplicated)
    with pytest.raises(ValueError) as error:
        verify_backup(backup)
    assert "synthetic-private-value" not in str(error.value)
    assert (backup / "manifest.json").read_bytes() == duplicated


def test_resolver_rechecks_actual_bytes_against_the_admitted_digest(tmp_path: Path) -> None:
    backup, original = tmp_path / "backup", tmp_path / "original"
    manifest = manifest_fixture(backup, original)
    (backup / "bundles").mkdir()
    item = backup / "bundles/item"
    item.write_bytes(b"Synthetic original evidence")
    files = manifest["files"]
    assert isinstance(files, dict)
    files["bundles/item"] = sha256(item.read_bytes())
    save_manifest(backup, manifest)
    admitted = verify_backup(backup)
    item.write_bytes(b"Synthetic changed bytes")
    with pytest.raises(ValueError, match="checksum"):
        resolve_backup_reference(backup, original / "item", manifest=admitted)
    assert item.read_bytes() == b"Synthetic changed bytes"
