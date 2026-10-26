from pathlib import Path

import pytest

from tank.contracts.configuration import DeliveryConfiguration
from tank.settings import Settings, resolve_versions


def test_explicit_duplicate_cannot_hide_invalid_required_delivery_value(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    original = (Path(__file__).parents[1] / "config" / "delivery.yaml").read_bytes()
    candidate = b"max_attempts: 0\n" + original
    (config / "delivery.yaml").write_bytes(candidate)
    settings = Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None)
    with pytest.raises(ValueError, match="YAML.*unique"):
        settings.load_config_as("delivery", DeliveryConfiguration)
    assert (config / "delivery.yaml").read_bytes() == candidate
    with pytest.raises(ValueError, match="YAML.*unique"):
        resolve_versions(settings)
    assert not (settings.data_dir / "config_versions").exists()
    (config / "delivery.yaml").write_bytes(original)
    captured = resolve_versions(settings)
    delivery = captured.load_config_as("delivery", DeliveryConfiguration)
    versions = settings.data_dir / "config_versions"
    before = {file.name: file.read_bytes() for file in versions.iterdir()}
    (config / "delivery.yaml").write_bytes(candidate)
    with pytest.raises(ValueError, match="YAML.*unique"):
        resolve_versions(settings)
    assert {file.name: file.read_bytes() for file in versions.iterdir()} == before
    assert captured.read_config_bytes("delivery.yaml") == original
    assert captured.load_config_as("delivery", DeliveryConfiguration) == delivery


@pytest.mark.parametrize(
    "candidate",
    [
        b"nested:\n  limit: 1\n  limit: 2\n",
        b'max_attempts: 0\n"max_\\u0061ttempts": 5\n',
    ],
)
def test_raw_yaml_projection_refuses_duplicate_decoded_keys(tmp_path: Path, candidate: bytes) -> None:
    config = tmp_path / "config"
    config.mkdir()
    (config / "rules.yaml").write_bytes(candidate)
    settings = Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None)
    with pytest.raises(ValueError, match="YAML.*unique"):
        settings.load_config("rules")


def test_yaml_alias_merge_sequence_and_explicit_override_remain_supported(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    payload = (
        b"names: &names [first, second]\naliases: *names\n"
        b"first: &first {limit: 1, first_only: true}\n"
        b"second: &second {limit: 2, second_only: true}\n"
        b"merged: &overridden {<<: [*first, *second], limit: 3}\n"
        b"reused: {<<: *overridden}\n"
        b"inherited: {<<: [*first, *second]}\n"
        b"literal_equal: {=: value}\n"
    )
    (config / "rules.yaml").write_bytes(payload)
    settings = resolve_versions(Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None))
    values = settings.load_config("rules")
    assert values["names"] == values["aliases"] == ["first", "second"]
    assert values["merged"] == {"limit": 3, "first_only": True, "second_only": True}
    assert values["reused"] == values["merged"]
    assert values["inherited"] == {"limit": 1, "first_only": True, "second_only": True}
    assert values["literal_equal"] == {"=": "value"}
    assert settings.read_config_bytes("rules.yaml") == payload


def test_actual_registry_aliases_remain_independent_after_projection() -> None:
    settings = Settings(_env_file=None)
    registry = settings.load_config("registry")
    entities = registry["entities"]
    assert isinstance(entities, list)
    sec = next(row for row in entities if isinstance(row, dict) and row["entity_id"] == "sec")
    assert isinstance(sec, dict)
    assert sec["names"] == sec["aliases"]
    before = settings.read_config_bytes("registry.yaml")
    names = sec["names"]
    assert isinstance(names, list)
    names.append("caller mutation")
    assert settings.read_config_bytes("registry.yaml") == before
    assert settings.load_config("registry") != registry
