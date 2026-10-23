"""Session defaults preserve test-owned paths and real Settings source precedence."""

import os

import conftest as policy
import pytest

from tank.settings import Settings


def test_dynamic_defaults_follow_home_and_restore_after_failure(tmp_path, monkeypatch):
    names = ("x_profile_dir",)
    original = {
        name: (Settings.model_fields[name].default, Settings.model_fields[name].default_factory)
        for name in names
    }
    before = Settings(_env_file=None)
    codex_factory = Settings.model_fields["codex_auth_path"].default_factory
    with (
        pytest.raises(RuntimeError, match="synthetic guard cleanup"),
        policy._synthetic_settings_defaults(),
        monkeypatch.context() as patch,
    ):
        for home in (tmp_path / "first-home", tmp_path / "second-home"):
            patch.setattr(policy, "_OFFLINE_HOME", home)
            settings = Settings(_env_file=None)
            assert settings.x_profile_dir == home / "FAKE-browser-profile"
        raise RuntimeError("synthetic guard cleanup")
    for name, metadata in original.items():
        field = Settings.model_fields[name]
        assert (field.default, field.default_factory) == metadata
        assert getattr(Settings(_env_file=None), name) == getattr(before, name)
    assert Settings.model_fields["codex_auth_path"].default_factory is codex_factory


def test_path_defaults_preserve_environment_and_constructor_precedence(tmp_path, monkeypatch):
    variables = {
        "TANK_X_PROFILE_DIR": tmp_path / "environment-profile",
        "TANK_CODEX_AUTH_PATH": tmp_path / "environment-codex.json",
    }
    before = {key: os.environ.get(key) for key in variables}
    with monkeypatch.context() as patch:
        for name, value in variables.items():
            patch.setenv(name, str(value))
        settings = Settings(_env_file=None)
        assert settings.x_profile_dir == variables["TANK_X_PROFILE_DIR"]
        assert settings.codex_auth_path == variables["TANK_CODEX_AUTH_PATH"]
        explicit = Settings(
            _env_file=None,
            x_profile_dir=tmp_path / "explicit-profile",
            codex_auth_path=tmp_path / "explicit-codex.json",
        )
        assert explicit.x_profile_dir == tmp_path / "explicit-profile"
        assert explicit.codex_auth_path == tmp_path / "explicit-codex.json"
    assert {key: os.environ.get(key) for key in variables} == before
