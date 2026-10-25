"""Declared publisher image invocation and observed runtime artifact identities."""

from typing import Literal

from tank.contracts import DomainModel
from tank.deployment.identities import SHA256, CommitSHA


class RuntimeInvocation(DomainModel):
    orchestrator_sha: CommitSHA
    selected_release_sha: CommitSHA


class RuntimeEvidence(DomainModel):
    orchestrator_sha: CommitSHA
    selected_release_sha: CommitSHA
    python_version: Literal["3.13.13"]
    uv_version: Literal["0.11.7"]
    lock_sha256: SHA256
    project_sha256: SHA256
