"""Actual captured configuration decoding preserves registered fields without synthesized metadata."""

from typing import Literal

import pytest
from pydantic import ValidationError
from pydantic_settings import SettingsConfigDict

from tank.contracts import DomainModel
from tank.contracts.json import decode_json, encode_json
from tank.contracts.registry import (
    DependencyConfiguration,
    ExchangeAddressesConfiguration,
    RegistryConfiguration,
    SemanticsConfiguration,
)
from tank.settings import Settings


class RegistryFixtureSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@pytest.mark.parametrize(
    ("name", "schema"),
    [
        ("registry", RegistryConfiguration),
        ("dependency_map", DependencyConfiguration),
        ("semantics", SemanticsConfiguration),
        ("exchange_addresses", ExchangeAddressesConfiguration),
    ],
)
def test_actual_configuration_loader_roundtrips_all_declared_fields(
    name: str, schema: type[DomainModel]
) -> None:
    settings = RegistryFixtureSettings()
    original = settings.load_config(name)
    captured = settings.load_config_as(name, schema)
    assert decode_json(captured.model_dump_json(exclude_unset=True)) == original


@pytest.mark.parametrize("defect", ["missing_identity", "boolean_multiplier", "unregistered_field"])
def test_registry_loader_refuses_invalid_captured_instrument_before_bootstrap(
    defect: Literal["missing_identity", "boolean_multiplier", "unregistered_field"],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RegistryFixtureSettings()
    document = settings.load_config("registry")
    instruments = document["instruments"]
    assert isinstance(instruments, list)
    instrument = instruments[0]
    assert isinstance(instrument, dict)
    if defect == "missing_identity":
        del instrument["instrument_id"]
    elif defect == "boolean_multiplier":
        instrument["contract_multiplier"] = True
    else:
        instrument["guessed_contract_size"] = "1"

    def captured(self: RegistryFixtureSettings, name: str) -> bytes:
        assert name == "registry.yaml"
        return encode_json(document)

    monkeypatch.setattr(RegistryFixtureSettings, "read_config_bytes", captured)
    with pytest.raises(ValidationError):
        settings.load_config_as("registry", RegistryConfiguration)
