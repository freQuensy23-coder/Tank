"""Prepare the real pinned source once, then validate the context consumed by Buildx."""

import hashlib
import json

import pytest
from openma_context import prepare, preparer


@pytest.fixture(scope="module")
def prepared_context(request):
    source = request.config.getoption("--openma-source")
    destination = request.config.getoption("--openma-destination")
    if source is None or destination is None:
        raise pytest.UsageError("OpenMA context test requires --openma-source and a new --openma-destination")
    return destination, prepare(source, destination)


def test_context_uses_real_renderer_and_verified_sdk_lock(prepared_context, repository_root):
    destination, receipt = prepared_context
    sdk = json.loads((repository_root / "deploy/boxlite/v0.10.6/openma-sdk-patch.json").read_text())
    assert receipt["upstream_commit"] == preparer().PIN
    assert receipt["frozen_lock_sha256"] == sdk["lockfile_sha256"]
    assert hashlib.sha256((destination / "pnpm-lock.yaml").read_bytes()).hexdigest() == sdk["lockfile_sha256"]
    assert receipt["native_runtime_bundle_supplied"] is False
    assert receipt["native_image_qualification"] == "NOT_RUN"
    assert not (destination / "tank-deploy/boxlite-runtime").exists()
    assert (destination / "pnpm-workspace.yaml").read_text().endswith(sdk["workspace_append"])
    assert (
        hashlib.sha256((destination / "apps/main-node/Dockerfile.tank-ci").read_bytes()).hexdigest()
        == receipt["dockerfile_sha256"]
    )
