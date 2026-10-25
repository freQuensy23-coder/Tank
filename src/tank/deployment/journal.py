"""Private, durable publisher journal with named release and transaction records."""

import json
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from tank.contracts import DomainModel
from tank.deployment.contracts import (
    ActiveRelease,
    BuildDocument,
    BuildProvenance,
    Transaction,
    TransactionDocument,
)
from tank.deployment.runtime import SHA256


class JournalError(RuntimeError):
    pass


class BuildCacheKey(DomainModel):
    source_digest: SHA256


type JournalValue = ActiveRelease | BuildProvenance | Transaction


def private_identity(info: os.stat_result, *, directory: bool) -> None:
    if info.st_uid != os.geteuid():
        raise JournalError("Deployment path has wrong owner")
    valid_type = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if not valid_type:
        raise JournalError("Deployment path has wrong type")
    if stat.S_IMODE(info.st_mode) != (0o700 if directory else 0o600):
        raise JournalError("Deployment path has unsafe permissions")


class PublisherJournal:
    def __init__(self, state: Path) -> None:
        self.state = state
        state.mkdir(mode=0o700, exist_ok=True)
        with self._directory():
            pass

    @contextmanager
    def _directory(self) -> Iterator[int]:
        descriptor = os.open(self.state, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            private_identity(os.fstat(descriptor), directory=True)
            yield descriptor
        finally:
            os.close(descriptor)

    def _read(self, name: str) -> bytes | None:
        with self._directory() as directory:
            try:
                descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            except FileNotFoundError:
                return None
            with os.fdopen(descriptor, "rb") as stream:
                private_identity(os.fstat(stream.fileno()), directory=False)
                return stream.read()

    def _write(self, name: str, value: JournalValue) -> None:
        document = json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
        with self._directory() as directory:
            temporary = "." + name + "." + uuid4().hex + ".tmp"
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=directory,
            )
            identity = os.fstat(descriptor)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(document)
                    stream.flush()
                    os.fsync(stream.fileno())
                actual = os.stat(temporary, dir_fd=directory, follow_symlinks=False)
                private_identity(actual, directory=False)
                if (actual.st_dev, actual.st_ino, actual.st_nlink) != (
                    identity.st_dev,
                    identity.st_ino,
                    1,
                ):
                    raise JournalError("Deployment staging file changed")
                os.replace(temporary, name, src_dir_fd=directory, dst_dir_fd=directory)
                os.fsync(directory)
            finally:
                remaining: os.stat_result | None
                try:
                    remaining = os.stat(temporary, dir_fd=directory, follow_symlinks=False)
                except FileNotFoundError:
                    remaining = None
                if remaining is not None and (remaining.st_dev, remaining.st_ino, remaining.st_uid) == (
                    identity.st_dev,
                    identity.st_ino,
                    identity.st_uid,
                ):
                    os.unlink(temporary, dir_fd=directory)
                    os.fsync(directory)

    def _remove(self, name: str) -> None:
        with self._directory() as directory:
            try:
                info = os.stat(name, dir_fd=directory, follow_symlinks=False)
            except FileNotFoundError:
                return
            private_identity(info, directory=False)
            os.unlink(name, dir_fd=directory)
            os.fsync(directory)

    def read_active(self) -> ActiveRelease | None:
        document = self._read("active.json")
        return None if document is None else ActiveRelease.model_validate_json(document)

    def write_active(self, release: ActiveRelease) -> None:
        self._write("active.json", release)

    def remove_active(self) -> None:
        self._remove("active.json")

    def read_transaction(self) -> Transaction | None:
        document = self._read("pending.json")
        return None if document is None else TransactionDocument.model_validate_json(document).root

    def write_transaction(self, transaction: Transaction) -> None:
        self._write("pending.json", transaction)

    def remove_transaction(self) -> None:
        self._remove("pending.json")

    def read_cached_build(self, key: BuildCacheKey) -> BuildProvenance | None:
        document = self._read(key.source_digest + ".json")
        if document is None:
            return None
        build = BuildDocument.model_validate_json(document).root
        if build.source_digest != key.source_digest:
            raise JournalError("Cached build digest differs from its journal key")
        return build

    def write_cached_build(self, build: BuildProvenance) -> None:
        self._write(build.source_digest + ".json", build)
