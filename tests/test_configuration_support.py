"""Captured configuration must isolate services from later fixture mutation."""

from pathlib import Path

import pytest
from configuration_support import captured_fixture_settings
from pydantic import JsonValue

from tank.settings import Settings


def test_capture_binds_immutable_overlay_bytes(tmp_path: Path) -> None:
    document: dict[str, JsonValue] = {"value": "captured"}
    settings = Settings(_env_file=None, data_dir=tmp_path / "data")
    captured = captured_fixture_settings(settings, {"synthetic": document}, tmp_path / "config")
    identity = captured.require_versions()
    document["value"] = "mutated input"
    (tmp_path / "config" / "synthetic.yaml").write_text("value: mutated disk\n")
    assert captured.load_config("synthetic") == {"value": "captured"}
    assert captured.require_versions() == identity
    changed = captured_fixture_settings(settings, {"synthetic": document}, tmp_path / "changed-config")
    assert changed.require_versions().config_version != identity.config_version


def test_invalid_namespace_cannot_create_capture_artifacts(tmp_path: Path) -> None:
    directory = tmp_path / "config"
    with pytest.raises(ValueError, match="relative declared namespace"):
        captured_fixture_settings(Settings(_env_file=None), {"../escape": {}}, directory)
    assert not directory.exists()
    assert not (tmp_path / "escape.yaml").exists()
