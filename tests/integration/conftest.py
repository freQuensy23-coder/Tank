"""Explicit Docker integration boundary; the normal unit suite ignores this directory."""

import json
import subprocess
from pathlib import Path

import pytest


def pytest_addoption(parser):
    group = parser.getgroup("Docker integration")
    group.addoption("--image", help="Already built local Docker image to exercise")
    group.addoption("--revision", help="Expected capture GIT_SHA")
    group.addoption("--context-kind", choices=("tank", "sandbox"))
    group.addoption("--openma-source", type=Path, help="Clean pinned official OpenMA checkout")
    group.addoption("--openma-destination", type=Path, help="New prepared dependency context")


@pytest.fixture(scope="session")
def repository_root():
    return Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def docker():
    def execute(*arguments, expected=0, stdin=None, timeout=240):
        result = subprocess.run(
            ["docker", *map(str, arguments)],
            input=stdin,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        assert result.returncode == expected, (
            f"docker {' '.join(map(str, arguments))}: expected {expected}, got {result.returncode}\n"
            f"{result.stdout}\n{result.stderr}"
        )
        return result.stdout

    return execute


@pytest.fixture(scope="session")
def image(request):
    value = request.config.getoption("--image")
    if not value:
        raise pytest.UsageError("Image integration tests require --image with an already built image")
    return value


@pytest.fixture(scope="session")
def image_config(docker, image):
    return json.loads(docker("image", "inspect", "--format", "{{json .Config}}", image))


@pytest.fixture(scope="session")
def revision(request):
    value = request.config.getoption("--revision")
    if not value:
        raise pytest.UsageError("Capture integration tests require the build's --revision")
    return value
