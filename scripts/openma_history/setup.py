"""Materialize the exact public-history closure without lifecycle scripts or resolution."""

import argparse
import base64
import hashlib
import io
import os
import platform
import stat
import subprocess
import sys
import tarfile
import time
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath

from pydantic import Field

from scripts.openma_history.manifest import MANIFEST_PATH, ROOT, Projection, encoded, generate
from scripts.openma_history.models import Archive, Manifest, Ownership, Plan, Platform
from tank.contracts import DomainModel
from tank.contracts.json import decode_unique_json

MARKER = ".tank-owned-setup.json"


class PackageIdentity(Projection):
    name: str = Field(min_length=1)
    version: str = Field(min_length=1)


class SetupReport(DomainModel):
    platform: Platform
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    contexts: int = Field(gt=0)
    verified_archives: int = Field(gt=0)
    setup_seconds: float = Field(ge=0)


def read_manifest(root: Path) -> tuple[Manifest, str]:
    document = (root / MANIFEST_PATH.relative_to(ROOT)).read_bytes()
    decode_unique_json(document)
    manifest = Manifest.model_validate_json(document)
    if document != encoded(generate(root)):
        raise ValueError("Frozen public-history manifest differs from authoritative lock")
    return manifest, hashlib.sha256(document).hexdigest()


def native_platform() -> Platform:
    if sys.platform != "linux" or platform.libc_ver()[0] != "glibc":
        raise ValueError("Public history qualification requires declared Linux glibc platform")
    machine = platform.machine()
    if machine in {"aarch64", "arm64"}:
        return "linux-arm64-gnu"
    if machine in {"x86_64", "amd64"}:
        return "linux-x64-gnu"
    raise ValueError("Unsupported public-history architecture")


def select_plan(manifest: Manifest, selected: Platform) -> Plan:
    matching = [plan for plan in manifest.plans if plan.platform == selected]
    if len(matching) != 1:
        raise ValueError("Platform plan must be present exactly once")
    return matching[0]


def own_directory(path: Path, ownership: Ownership, *, cache: bool) -> None:
    if path.is_symlink():
        raise ValueError("Setup directory must not be symbolic")
    if not path.exists():
        path.mkdir(parents=True, mode=0o700)
    info = path.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
        raise ValueError("Setup directory ownership mismatch")
    marker = path / MARKER
    if marker.exists() or marker.is_symlink():
        if marker.is_symlink() or not marker.is_file():
            raise ValueError("Setup ownership marker must be regular")
        document = marker.read_bytes()
        decode_unique_json(document)
        if Ownership.model_validate_json(document) != ownership:
            raise ValueError("Setup ownership marker differs from requested inputs")
        if not cache:
            raise ValueError("Dependency root must be fresh")
    else:
        if any(path.iterdir()):
            raise ValueError("Refusing unknown existing setup contents")
        marker.write_text(ownership.model_dump_json() + "\n")


def verify_archive(raw: bytes, archive: Archive) -> None:
    actual = "sha512-" + base64.b64encode(hashlib.sha512(raw).digest()).decode()
    if actual != archive.integrity:
        raise ValueError("Archive bytes differ from frozen integrity")


def cached_archive(cache: Path, archive: Archive, *, offline: bool) -> Path:
    target = cache / archive.filename
    if target.is_symlink():
        raise ValueError("Archive cache entry must not be symbolic")
    if target.exists():
        if not target.is_file() or target.stat().st_uid != os.geteuid():
            raise ValueError("Archive cache entry ownership mismatch")
        verify_archive(target.read_bytes(), archive)
        return target
    if offline:
        raise ValueError("Frozen archive is absent from offline cache")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(archive.url, timeout=60) as response:
        if response.url != archive.url:
            raise ValueError("Frozen archive redirected to an undeclared URL")
        raw = response.read()
    verify_archive(raw, archive)
    temporary = cache / (".archive-" + uuid.uuid4().hex)
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def extract_archive(raw: bytes, archive: Archive, destination: Path) -> None:
    verify_archive(raw, archive)
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as package:
        members = package.getmembers()
        prefixes = {
            PurePosixPath(member.name).parts[0] for member in members if PurePosixPath(member.name).parts
        }
        if len(prefixes) != 1:
            raise ValueError("Archive requires one package root")
        prefix = next(iter(prefixes))
        for member in members:
            parts = PurePosixPath(member.name).parts
            if member.name.startswith("/") or ".." in parts:
                raise ValueError("Archive member escapes package root")
            if len(parts) == 1:
                if not member.isdir():
                    raise ValueError("Archive root must be a directory")
                continue
            if not (member.isfile() or member.isdir() or member.issym() or member.islnk()):
                raise ValueError("Unsupported archive member kind")
            member.name = PurePosixPath(*parts[1:]).as_posix()
            if member.islnk() and PurePosixPath(member.linkname).parts[0] == prefix:
                member.linkname = PurePosixPath(*PurePosixPath(member.linkname).parts[1:]).as_posix()
            package.extract(member, destination, filter="data")
    metadata = destination / "package.json"
    if metadata.is_symlink() or not metadata.is_file():
        raise ValueError("Extracted package metadata must be regular")
    document = metadata.read_bytes()
    decode_unique_json(document)
    actual = PackageIdentity.model_validate_json(document)
    if actual.name != archive.name or actual.version != archive.version:
        raise ValueError("Extracted package identity differs from frozen archive")


def materialize(root: Path, cache: Path, *, offline: bool) -> SetupReport:
    started = time.monotonic()
    manifest, digest = read_manifest(root)
    selected = native_platform()
    plan = select_plan(manifest, selected)
    target = root / "vendor/openma/node_modules"
    tracked = subprocess.check_output(["git", "ls-files", "--", "vendor/openma/node_modules"], cwd=root)
    ignored = subprocess.run(
        ["git", "check-ignore", "-q", "vendor/openma/node_modules/"], cwd=root, check=False
    )
    if tracked or ignored.returncode:
        raise ValueError("Dependency root must be an established ignored untracked directory")
    cache_owner = Ownership(
        schema_version="tank.openma-history.owned-setup.v1",
        manifest_sha256=digest,
        platform=selected,
        uid=os.geteuid(),
        purpose="ARCHIVE_CACHE",
    )
    own_directory(cache, cache_owner, cache=True)
    archives = {archive.key: archive for archive in manifest.archives}
    used = sorted({snapshot.archive for snapshot in plan.snapshots})
    permitted = {MARKER, *(archives[key].filename for key in used)}
    if any(path.name not in permitted for path in cache.iterdir()):
        raise ValueError("Unknown files in owned archive cache")
    with ThreadPoolExecutor() as pool:
        list(pool.map(lambda key: cached_archive(cache, archives[key], offline=offline), used))
    output_owner = cache_owner.model_copy(update={"purpose": "DEPENDENCY_ROOT"})
    own_directory(target, output_owner, cache=False)
    contexts = {snapshot.key: snapshot for snapshot in plan.snapshots}
    locations = {
        key: target / ".tank-lock" / snapshot.context / "node_modules" / archives[snapshot.archive].name
        for key, snapshot in contexts.items()
    }
    for key, snapshot in contexts.items():
        archive = archives[snapshot.archive]
        destination = locations[key]
        destination.mkdir(parents=True)
        extract_archive((cache / archive.filename).read_bytes(), archive, destination)
    for key, snapshot in contexts.items():
        parent = locations[key].parent
        if archives[snapshot.archive].name.startswith("@"):
            parent = parent.parent
        for edge in snapshot.edges:
            link = parent / edge.name
            link.parent.mkdir(parents=True, exist_ok=True)
            if link.exists() or link.is_symlink():
                raise ValueError("Dependency edge conflicts with package contents")
            link.symlink_to(os.path.relpath(locations[edge.target], link.parent), target_is_directory=True)
    for item in plan.roots:
        link = target / item.name
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(os.path.relpath(locations[item.target], link.parent), target_is_directory=True)
    driver = root / "scripts/openma-persistence/node_modules/postgres"
    if driver.is_symlink() or not driver.is_dir():
        raise ValueError("Canonical five-package PostgreSQL driver must be installed first")
    identity = PackageIdentity.model_validate_json((driver / "package.json").read_bytes())
    if identity.name != "postgres" or identity.version != "3.4.9":
        raise ValueError("Canonical PostgreSQL driver identity changed")
    (target / "postgres").symlink_to(os.path.relpath(driver, target), target_is_directory=True)
    return SetupReport(
        platform=selected,
        manifest_sha256=digest,
        contexts=len(contexts),
        verified_archives=len(used),
        setup_seconds=time.monotonic() - started,
    )


class Arguments(argparse.Namespace):
    cache: Path
    offline: bool
    report: Path


def main() -> None:
    if sys.version_info[:3] != (3, 13, 13):
        raise ValueError("Public history setup requires Python3.13.13")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, required=True)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--report", type=Path, required=True)
    arguments = parser.parse_args(namespace=Arguments())
    report = materialize(ROOT, arguments.cache, offline=arguments.offline)
    arguments.report.write_text(report.model_dump_json(indent=2) + "\n")
    print(report.model_dump_json())


if __name__ == "__main__":
    main()
