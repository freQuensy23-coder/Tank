"""Owned shell-process resources with real publisher HTTP and durable state."""

import os
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from publisher_transition_support import HTTPPublisherRig, write_artifact

from tank.deployment.apify_api import ApifyEndpoint
from tank.deployment.artifact_contracts import SourceArtifact

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class ShellHost:
    root: Path
    environment: dict[str, str]
    orchestrator: str

    def deploy(self, selected: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(self.root / "deploy.sh"), selected, self.orchestrator],
            cwd=self.root,
            env=self.environment,
            text=True,
            capture_output=True,
            timeout=10,
        )


def shell_host(
    fixture: HTTPPublisherRig,
    endpoint: ApifyEndpoint,
    *,
    selected: str,
    artifact: SourceArtifact,
    failure: Literal["SELECTED", "ALL"],
) -> ShellHost:
    root = fixture.journal.state.parent
    shutil.copyfile(ROOT / "scripts/deploy/deploy.sh", root / "deploy.sh")
    destination = root / "releases" / selected
    destination.parent.mkdir(exist_ok=True)
    assert artifact.release.release_sha == selected
    write_artifact(destination, artifact)
    worker = root / "publisher_worker.sh"
    worker.write_text(
        "#!/bin/sh\nexec "
        + shlex.quote(sys.executable)
        + " "
        + shlex.quote(str(ROOT / "tests/publisher_shell_worker.py"))
        + ' "$@"\n'
    )
    binary = root / "bin"
    binary.mkdir()
    docker = binary / "docker"
    docker.write_text(
        '#!/bin/bash\nprintf \'DOCKER %s %s\\n\' "${TANK_VERSION-}" "$*" >> effects.log\n'
        + f'if [[ "$*" == *\'pull -q\'* && "${{TANK_VERSION-}}" == {selected} ]]; then exit 1; fi\n'
        + ("if [[ \"$*\" == *'pull -q'* ]]; then exit 1; fi\n" if failure == "ALL" else "")
        + 'if [[ "$1" == inspect ]]; then if [[ "$*" == *State.Pid* ]]; then echo 42; else echo healthy; fi; fi\n'
        + 'if [[ "$1" == ps && -f capture-created ]]; then echo capture-new; fi\n'
        + 'if [[ "$*" == *--scale* ]]; then touch capture-created; fi\nexit 0\n'
    )
    (binary / "nsenter").write_text("#!/bin/bash\nexit 0\n")
    for path in binary.iterdir():
        path.chmod(0o755)
    environment = dict(os.environ)
    environment["PATH"] = str(binary) + os.pathsep + environment["PATH"]
    environment["TANK_PUBLISHER_STAND_ENDPOINT"] = endpoint.base_url
    return ShellHost(root, environment, "d" * 40)
