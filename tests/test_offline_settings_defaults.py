"""Path defaults preserve test-owned locations and real Settings source precedence."""

import os
from pathlib import Path

import pytest

from tank.settings import Settings


def test_path_defaults_preserve_environment_and_constructor_precedence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    variable = tmp_path / "environment-codex.json"
    before = os.environ.get("TANK_CODEX_AUTH_PATH")
    with monkeypatch.context() as patch:
        patch.setenv("TANK_CODEX_AUTH_PATH", str(variable))
        assert Settings(_env_file=None).codex_auth_path == variable
        explicit = Settings(_env_file=None, codex_auth_path=tmp_path / "explicit-codex.json")
        assert explicit.codex_auth_path == tmp_path / "explicit-codex.json"
    assert os.environ.get("TANK_CODEX_AUTH_PATH") == before
