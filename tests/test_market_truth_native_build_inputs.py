import sys
import tomllib
from importlib.machinery import ExtensionFileLoader
from pathlib import Path

from tank import clickhouse_market, util
from tank.analysis import truth


def test_native_cache_invalidation_includes_the_companion_declaration():
    project = Path(__file__).resolve().parents[1] / "pyproject.toml"
    config = tomllib.loads(project.read_text())
    paths = {item["file"] for item in config["tool"]["uv"]["cache-keys"] if "file" in item}
    assert "src/tank/analysis/truth.pxd" in paths


def test_linux_native_artifacts_use_the_declared_clang16_toolchain():
    for module in (truth, clickhouse_market, util):
        assert isinstance(module.__loader__, ExtensionFileLoader)
        if sys.platform == "linux":
            assert b"clang version 16." in Path(module.__file__).read_bytes()
