import tomllib
from pathlib import Path


def test_native_cache_invalidation_includes_the_companion_declaration():
    project = Path(__file__).resolve().parents[1] / "pyproject.toml"
    config = tomllib.loads(project.read_text())
    paths = {item["file"] for item in config["tool"]["uv"]["cache-keys"] if "file" in item}
    assert "src/tank/analysis/truth.pxd" in paths
