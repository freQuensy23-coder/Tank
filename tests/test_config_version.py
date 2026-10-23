import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tank.settings import Settings, resolve_versions


def test_image_config_version_is_the_build_commit(tmp_path):
    config = tmp_path / "config"
    config.mkdir()
    (config / "rules.yaml").write_text("value: 1\n")
    settings = Settings(config_dir=config, data_dir=tmp_path / "data", build_sha="a" * 40, _env_file=None)
    assert resolve_versions(settings).config_version == "a" * 40


def test_development_config_version_follows_the_content(tmp_path):
    config = tmp_path / "config"
    config.mkdir()
    (config / "rules.yaml").write_text("value: 1\n")
    first = resolve_versions(
        Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None)
    ).config_version
    (config / "rules.yaml").write_text("value: 2\n")
    second = resolve_versions(
        Settings(config_dir=config, data_dir=tmp_path / "data", _env_file=None)
    ).config_version
    assert first.startswith("dev.") and second.startswith("dev.") and first != second


@pytest.mark.integration
@pytest.mark.process
def test_packaged_json_resource_changes_code_version_in_cold_process(tmp_path):
    package_root = tmp_path / "package"
    copied_package = package_root / "tank"
    shutil.copytree(Path(__file__).resolve().parents[1] / "src/tank", copied_package)
    resource = copied_package / "resources/fixture-input-authority-v1.json"
    assert resource.is_file()
    config = tmp_path / "config"
    config.mkdir()
    (config / "rules.yaml").write_text("value: 1\n")
    program = (
        "import sys; from pathlib import Path; "
        "from tank.settings import Settings, resolve_versions; "
        "print(resolve_versions(Settings(config_dir=Path(sys.argv[1]), "
        "data_dir=Path(sys.argv[2]), _env_file=None)).code_version)"
    )
    environment = dict(os.environ, PYTHONPATH=str(package_root))

    def cold_version():
        return subprocess.check_output(
            [sys.executable, "-c", program, str(config), str(tmp_path / "runtime")],
            env=environment,
            cwd=tmp_path,
            text=True,
        ).strip()

    original = cold_version()
    assert cold_version() == original
    resource.write_bytes(resource.read_bytes() + b"\n")
    assert cold_version() != original
