"""Required context namespaces are validated before they can authorize a tool reference."""

from dataclasses import dataclass

import pytest
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

from tank.agents.context_registry import ContextRegistry
from tank.settings import Settings


class ContextFixtureSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@dataclass(frozen=True)
class CapturedContextConfiguration:
    documents: dict[str, dict[str, JsonValue]]

    def load_config(self, name: str) -> dict[str, JsonValue]:
        return self.documents[name]

    def read_config_json(self, name: str) -> JsonValue:
        raise AssertionError("Registry projection must not read tool manifests")


def configuration() -> CapturedContextConfiguration:
    return CapturedContextConfiguration(
        {
            "registry": {
                "tokens": [{"token_id": "synthetic:T", "symbol": "T", "project": "Synthetic project"}],
                "entities": [
                    {"entity_id": "synthetic", "name": None, "names": ["Synthetic display"], "aliases": []}
                ],
                "instruments": [
                    {
                        "instrument_id": "synthetic:spot:T",
                        "venue": "synthetic",
                        "market_type": "SPOT",
                        "venue_symbol": "TUSD",
                        "unconsumed_discovery_metadata": {"actual": True},
                    }
                ],
            },
            "sources": {"sources": [{"source_id": "fixture", "name": "Synthetic source"}]},
        }
    )


def test_context_projects_actual_captured_registry_without_io_defaults() -> None:
    settings = ContextFixtureSettings()
    snapshot = ContextRegistry.from_configuration(settings)
    registry = settings.load_config("registry")
    assert isinstance(registry["tokens"], list)
    assert len(snapshot.tokens) == len(registry["tokens"])
    assert snapshot.tokens and snapshot.entities and snapshot.instruments and snapshot.sources


def test_label_projection_preserves_explicit_name_and_identity() -> None:
    snapshot = ContextRegistry.from_configuration(configuration())
    assert snapshot.tokens[0].display_name() == "Synthetic project"
    assert snapshot.entities[0].display_name() == "Synthetic display"
    assert snapshot.instruments[0].venue_symbol == "TUSD"
    assert snapshot.sources[0].name == "Synthetic source"


@pytest.mark.parametrize("namespace", ["tokens", "entities", "instruments"])
def test_missing_required_registry_namespace_fails(namespace: str) -> None:
    settings = configuration()
    del settings.documents["registry"][namespace]
    with pytest.raises(KeyError):
        ContextRegistry.from_configuration(settings)


@pytest.mark.parametrize("namespace", ["tokens", "entities", "instruments", "sources"])
def test_duplicate_identity_cannot_silently_replace_registered_label(namespace: str) -> None:
    settings = configuration()
    document = settings.documents["sources" if namespace == "sources" else "registry"]
    rows = document[namespace]
    assert isinstance(rows, list)
    rows.append(rows[0])
    with pytest.raises(ValueError, match="identities must be unique"):
        ContextRegistry.from_configuration(settings)


def test_incomplete_instrument_cannot_authorize_blank_label() -> None:
    settings = configuration()
    rows = settings.documents["registry"]["instruments"]
    assert isinstance(rows, list) and isinstance(rows[0], dict)
    del rows[0]["venue_symbol"]
    with pytest.raises(KeyError):
        ContextRegistry.from_configuration(settings)
