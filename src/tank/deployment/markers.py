"""Read the actual Tank release marker without inferring an absent identity."""

import os
import stat
from pathlib import Path

from tank.deployment.contracts import ReleaseMarker
from tank.deployment.identities import CommitSHA


class ReleaseMarkerError(RuntimeError):
    pass


class FilesystemTankReleaseState:
    def __init__(self, path: Path) -> None:
        self.path = path

    def deployed(self) -> CommitSHA | None:
        try:
            descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            return None
        with os.fdopen(descriptor, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ReleaseMarkerError("Tank release marker must be a regular file")
            text = stream.read().decode("utf-8").strip()
        return ReleaseMarker(release_sha=text).release_sha
