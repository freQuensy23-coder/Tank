"""Bind packaged runtime metadata to the actual selected publisher image."""

import os
import stat
from pathlib import Path

from tank.contracts.json import decode_unique_json
from tank.deployment.artifact_contracts import PublisherArtifactRuntime
from tank.deployment.runtime import PublisherRuntimeError, verify_runtime
from tank.deployment.runtime_contracts import RuntimeInvocation


def read_control_file(path: Path) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise PublisherRuntimeError("Publisher control artifact must be a regular file")
        return stream.read()


def verify_package_runtime(
    package: Path, invocation: RuntimeInvocation, runtime_root: Path
) -> PublisherArtifactRuntime:
    document = read_control_file(package / "publisher-runtime.json")
    decode_unique_json(document)
    declared = PublisherArtifactRuntime.model_validate_json(document)
    if (
        declared.runtime.orchestrator_sha != invocation.orchestrator_sha
        or declared.runtime.selected_release_sha != invocation.selected_release_sha
    ):
        raise PublisherRuntimeError("Publisher artifact invocation identity mismatch")
    image = read_control_file(package / "publisher-image")
    if image != (declared.image_reference + "\n").encode("utf-8"):
        raise PublisherRuntimeError("Publisher image bootstrap does not match runtime artifact")
    actual = verify_runtime(invocation, runtime_root)
    if actual.runtime != declared.runtime:
        raise PublisherRuntimeError("Publisher image metadata does not match runtime artifact")
    return declared
