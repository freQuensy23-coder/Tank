"""Derive supported public-package contexts from the actual frozen pnpm inputs."""

import argparse
import hashlib
import json
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict

from scripts.openma_history.models import (
    Archive,
    Edge,
    Manifest,
    OmittedEdge,
    Plan,
    Platform,
    RootBinding,
    RootLink,
    Snapshot,
)

ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = Path(__file__).with_name("manifest.json")
PLATFORMS: tuple[Platform, ...] = ("linux-arm64-gnu", "linux-x64-gnu")


class Projection(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")


class Resolution(Projection):
    integrity: str


class Package(Projection):
    resolution: Resolution
    os: list[str] | None = None
    cpu: list[str] | None = None
    libc: list[str] | None = None


class LockedSnapshot(Projection):
    dependencies: dict[str, str] | None = None
    optionalDependencies: dict[str, str] | None = None


class Declaration(Projection):
    specifier: str
    version: str


class Importer(Projection):
    dependencies: dict[str, Declaration] | None = None
    devDependencies: dict[str, Declaration] | None = None


class Lock(Projection):
    packages: dict[str, Package]
    snapshots: dict[str, LockedSnapshot]
    importers: dict[str, Importer]


type Policy = Literal["native-only", "supported"]
type Section = Literal["dependencies", "devDependencies"]
ROOTS: tuple[tuple[str, str, Section, str, str, Policy], ...] = (
    ("effect", "packages/sql-client", "dependencies", "3.22.1", "3.22.1", "supported"),
    ("vitest", "packages/managed-agents-adapters-sql", "devDependencies", "^4.1.3", "4.1.4", "native-only"),
    (
        "better-sqlite3",
        "packages/managed-agents-adapters-sql",
        "devDependencies",
        "^13.0.3",
        "13.0.3",
        "native-only",
    ),
    (
        "@earendil-works/pi-ai",
        "packages/managed-agents-application",
        "dependencies",
        "1.1.0",
        "1.1.0",
        "supported",
    ),
    ("@openma/common", "packages/managed-agents-application", "dependencies", "^0.9.1", "0.9.1", "supported"),
    ("@types/node", ".", "devDependencies", "^26.6.3", "26.6.3", "supported"),
)


def physical(key: str) -> str:
    return key.split("(", 1)[0]


def target_key(lock: Lock, name: str, version: str) -> str:
    named = name + "@" + version
    if named in lock.snapshots:
        return named
    if version in lock.snapshots:
        return version
    raise ValueError("Frozen dependency target is absent")


def permits(values: list[str] | None, actual: str) -> bool:
    if values is None:
        return True
    if "!" + actual in values:
        return False
    positive = [value for value in values if not value.startswith("!")]
    return not positive or actual in positive


def compatible(package: Package, platform: Platform) -> bool:
    arch = "arm64" if platform == "linux-arm64-gnu" else "x64"
    return permits(package.os, "linux") and permits(package.cpu, arch) and permits(package.libc, "glibc")


def bindings(lock: Lock) -> tuple[RootBinding, ...]:
    result: list[RootBinding] = []
    for name, importer, section, specifier, version, policy in ROOTS:
        source = lock.importers[importer]
        declarations = source.dependencies if section == "dependencies" else source.devDependencies
        if declarations is None or name not in declarations:
            raise ValueError("Required upstream importer declaration is absent")
        declaration = declarations[name]
        if declaration.specifier != specifier or physical(declaration.version) != version:
            raise ValueError("Upstream importer no longer matches the qualified root")
        result.append(
            RootBinding(
                name=name,
                importer=importer,
                section=section,
                specifier=specifier,
                version=declaration.version,
                snapshot=target_key(lock, name, declaration.version),
                optional_policy=policy,
            )
        )
    return tuple(result)


def platform_plan(lock: Lock, roots: tuple[RootBinding, ...], platform: Platform) -> Plan:
    pending = [(root.snapshot, root.optional_policy) for root in roots]
    visited: set[tuple[str, str]] = set()
    edges: dict[str, dict[str, Edge]] = {}
    omitted: dict[str, dict[str, OmittedEdge]] = {}
    while pending:
        key, policy = pending.pop()
        if (key, policy) in visited:
            continue
        visited.add((key, policy))
        if not compatible(lock.packages[physical(key)], platform):
            raise ValueError("Required package is incompatible with selected platform")
        snapshot = lock.snapshots[key]
        selected = edges.setdefault(key, {})
        absent = omitted.setdefault(key, {})
        groups = (
            ("dependencies", snapshot.dependencies),
            ("optionalDependencies", snapshot.optionalDependencies),
        )
        for kind, declarations in groups:
            if declarations is None:
                continue
            for name, version in declarations.items():
                target = target_key(lock, name, version)
                facts = lock.packages[physical(target)]
                reason: Literal["UNREQUESTED_OPTIONAL_TEST_FEATURE", "UNSUPPORTED_PLATFORM"] | None = None
                if not compatible(facts, platform):
                    if kind == "dependencies":
                        raise ValueError("Required edge selects unsupported platform")
                    reason = "UNSUPPORTED_PLATFORM"
                elif (
                    kind == "optionalDependencies"
                    and policy == "native-only"
                    and facts.os is None
                    and facts.cpu is None
                    and facts.libc is None
                ):
                    reason = "UNREQUESTED_OPTIONAL_TEST_FEATURE"
                if reason is not None:
                    absent[name] = OmittedEdge(name=name, target=target, reason=reason)
                    continue
                edge = Edge(
                    name=name,
                    target=target,
                    kind="dependencies" if kind == "dependencies" else "optionalDependencies",
                )
                if name in selected and selected[name].target != target:
                    raise ValueError("Conflicting target in one snapshot context")
                selected[name] = edge
                pending.append((target, policy))
    for key, unavailable in omitted.items():
        for name, item in unavailable.items():
            if item.reason == "UNREQUESTED_OPTIONAL_TEST_FEATURE" and item.target in edges:
                edges[key][name] = Edge(name=name, target=item.target, kind="optionalDependencies")
    records = tuple(
        Snapshot(
            key=key,
            archive=physical(key),
            context=hashlib.sha256(key.encode()).hexdigest(),
            edges=tuple(selected[name] for name in sorted(selected)),
            omitted=tuple(value for name, value in sorted(omitted[key].items()) if name not in selected),
        )
        for key, selected in sorted(edges.items())
    )
    links = [
        RootLink(name=root.name, target=root.snapshot, reason="DECLARED_IMPORTER")
        for root in roots
        if root.name != "@types/node"
    ]
    type_root = next(root for root in roots if root.name == "@types/node")
    support = edges[type_root.snapshot].get("undici-types")
    if support is None or physical(support.target) != "undici-types@8.9.0":
        raise ValueError("Declared root type-support edge changed")
    links.append(RootLink(name="undici-types", target=support.target, reason="DECLARED_ROOT_TYPE_SUPPORT"))
    return Plan(platform=platform, snapshots=records, roots=tuple(links))


def generate(root: Path) -> Manifest:
    raw = (root / "vendor/openma/pnpm-lock.yaml").read_bytes()
    lock = Lock.model_validate(yaml.safe_load(raw))
    declared = bindings(lock)
    plans = tuple(platform_plan(lock, declared, platform) for platform in PLATFORMS)
    used = sorted({snapshot.archive for plan in plans for snapshot in plan.snapshots})
    archives = tuple(
        Archive(
            key=key,
            name=key.rsplit("@", 1)[0],
            version=key.rsplit("@", 1)[1],
            integrity=lock.packages[key].resolution.integrity,
        )
        for key in used
    )
    return Manifest(
        schema_version="tank.openma-public-history.dependencies.v1",
        upstream_lock_sha256=hashlib.sha256(raw).hexdigest(),
        bindings=declared,
        archives=archives,
        plans=plans,
    )


def encoded(manifest: Manifest) -> bytes:
    return (json.dumps(manifest.model_dump(mode="json"), indent=2, sort_keys=True) + "\n").encode()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()
    document = encoded(generate(ROOT))
    if arguments.check:
        if MANIFEST_PATH.read_bytes() != document:
            raise ValueError("Frozen public-history manifest is stale")
    else:
        MANIFEST_PATH.write_bytes(document)
    print(json.dumps({"manifest_sha256": hashlib.sha256(document).hexdigest()}))


if __name__ == "__main__":
    main()
