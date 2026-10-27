import base64
import json
from pathlib import Path

import pytest

from tank.contracts.json import DuplicateJsonKeyError
from tank.contracts.openma import ManagedCustomTool
from tank.openma_sync import managed_tools
from tank.settings import Settings, resolve_versions


@pytest.mark.parametrize(
    "candidate",
    [
        b'{"required":["token_id"],"required":[]}',
        b'{"properties":{"token_id":{"type":"number","type":"string"}}}',
        b'{"additionalProperties":false,"\\u0061dditionalProperties":true}',
    ],
)
def test_ambiguous_json_configuration_never_publishes_candidate(tmp_path: Path, candidate: bytes) -> None:
    config = tmp_path / "config"
    config.mkdir()
    path = config / "schema.json"
    original = b'{ "required": ["token_id"], "additionalProperties": false }\n'
    path.write_bytes(original)
    settings = Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None)
    captured = resolve_versions(settings)
    versions = settings.data_dir / "config_versions"
    before = {file.name: file.read_bytes() for file in versions.iterdir()}
    path.write_bytes(candidate)
    with pytest.raises(DuplicateJsonKeyError, match="keys must be unique"):
        resolve_versions(settings)
    assert {file.name: file.read_bytes() for file in versions.iterdir()} == before
    assert captured.read_config_bytes("schema.json") == original
    assert captured.read_config_json("schema.json") == {
        "required": ["token_id"],
        "additionalProperties": False,
    }
    assert path.read_bytes() == candidate


def test_first_ambiguous_json_capture_does_not_create_version_directory(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    (config / "schema.json").write_bytes(b'{"type":"number","type":"string"}')
    settings = Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None)
    with pytest.raises(DuplicateJsonKeyError):
        resolve_versions(settings)
    assert not (settings.data_dir / "config_versions").exists()


def test_actual_managed_tool_reader_refuses_escaped_duplicate_schema_fact(tmp_path: Path) -> None:
    config = tmp_path / "config"
    skill = config / "skills" / "tank-event-analyst"
    skill.mkdir(parents=True)
    path = skill / "tools.json"
    path.write_bytes(
        b'[{"name":"read_fixture","description":"Read an authored fixture.",'
        b'"input_schema":{"type":"object","additionalProperties":false,'
        b'"\\u0061dditionalProperties":true}}]'
    )
    settings = Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None)
    with pytest.raises(DuplicateJsonKeyError):
        managed_tools(settings, "tank-event-analyst")
    assert not settings.data_dir.exists()
    path.write_bytes(
        b'[{"name":"read_fixture","description":"Read an authored fixture.",'
        b'"input_schema":{"type":"object","additionalProperties":false}}]'
    )
    tools = managed_tools(settings, "tank-event-analyst")
    custom = [tool for tool in tools if isinstance(tool, ManagedCustomTool)]
    assert len(custom) == 1
    assert custom[0].name == "read_fixture"
    assert custom[0].input_schema["additionalProperties"] is False


def test_unique_empty_and_ordinary_json_keep_exact_capture_bytes(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.mkdir()
    payloads = {"empty.json": b"{}\n", "ordinary.json": b'{ "number": 0, "items": [] }\n'}
    for name, payload in payloads.items():
        (config / name).write_bytes(payload)
    captured = resolve_versions(Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None))
    assert captured.read_config_json("empty.json") == {}
    assert captured.read_config_json("ordinary.json") == {"number": 0, "items": []}
    snapshot = next((captured.data_dir / "config_versions").glob("*.json"))
    envelope = json.loads(snapshot.read_bytes())
    for name, payload in payloads.items():
        assert captured.read_config_bytes(name) == payload
        assert base64.b64decode(envelope["files_base64"][name]) == payload
