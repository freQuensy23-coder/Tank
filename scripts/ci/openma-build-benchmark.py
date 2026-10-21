#!/usr/bin/env python3
"""Benchmark genuine full OpenMA images on an owned hosted CI builder."""

import argparse
import hashlib
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
UPSTREAM_PIN = "85d5361f535c7221de975838dc48444e468cc02e"
OVERLAY_SHA256 = "358d8300c1c0a4f23739e57d0995e843e801f3b96ee255079a10010b53f61342"
OVERLAY_FILES = {
    "boxlite-shim",
    "provenance.json",
    "SHA256SUMS",
    *(
        "build-evidence/" + n
        for n in (
            "build-receipt.json",
            "glibc-version.txt",
            "go-version.txt",
            "patch-receipt.json",
            "rustc-version.txt",
            "source.patch",
            "submodules.txt",
            "test-receipt.json",
        )
    ),
}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    sys.modules[name] = result
    spec.loader.exec_module(result)
    return result


def command(args, *, cwd=None):
    return subprocess.check_output([str(v) for v in args], cwd=cwd, text=True).strip()


def new_directory(path):
    path = Path(path)
    if path.exists() or path.is_symlink():
        raise ValueError("Benchmark destination must be new: " + str(path))
    path.mkdir(parents=True)
    return path


def verify_runtime(runtime, deploy):
    verifier = module(deploy / "prepare_boxlite_runtime.py", "benchmark_native_verifier")
    verified = verifier.verify_runtime(runtime)
    composer = module(deploy / "prepare_openma_deployment.py", "benchmark_native_admission")
    qualification = composer.require_native_qualification(verified)
    registration = json.loads((deploy / "boxlite/v0.10.6/native-qualification.json").read_text())["amd64"]
    if (
        verified["architecture"] != "amd64"
        or sha(runtime / "provenance.json") != registration["provenance_sha256"]
    ):
        raise ValueError("Bundle differs from the exact registered amd64 native provenance")
    return {
        "artifact_sha256": verified["artifact_sha256"],
        "native_bundle_qualification": qualification["status"],
    }


def reconstruct_runtime(official, overlay, destination, deploy):
    platform = json.loads((deploy / "boxlite/v0.10.6/platforms.json").read_text())["platforms"]["amd64"]
    if sha(official) != platform["official_runtime"]["archive_sha256"]:
        raise ValueError("Official runtime archive hash mismatch")
    if sha(overlay) != OVERLAY_SHA256:
        raise ValueError("Qualified shim archive hash mismatch")
    destination = new_directory(destination)
    # Extract only exact regular members. Preserve original checked native bytes.
    with tarfile.open(official, "r:gz") as archive:
        expected = platform["original_files"]
        found = set()
        for member in archive:
            name = Path(member.name).name
            if name not in expected:
                continue
            if not member.isfile() or name in found:
                raise ValueError("Invalid official runtime archive member")
            data = archive.extractfile(member).read()
            if hashlib.sha256(data).hexdigest() != expected[name]:
                raise ValueError("Official runtime member hash mismatch: " + name)
            (destination / name).write_bytes(data)
            os.chmod(destination / name, 0o755)
            found.add(name)
        if found != set(expected):
            raise ValueError("Official runtime archive is incomplete")
    with tarfile.open(overlay, "r:gz") as archive:
        found = set()
        for member in archive:
            if member.name not in OVERLAY_FILES or member.name in found or not member.isfile():
                raise ValueError("Unexpected qualified overlay member")
            target = destination / member.name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.extractfile(member).read())
            os.chmod(target, 0o755 if member.name == "boxlite-shim" else 0o644)
            found.add(member.name)
        if found != OVERLAY_FILES:
            raise ValueError("Qualified native overlay is incomplete")
    return destination, verify_runtime(destination, deploy)


def fetch_upstream(destination):
    destination = new_directory(destination)
    command(["git", "init", "-q", destination])
    command(
        [
            "git",
            "-C",
            destination,
            "remote",
            "add",
            "origin",
            "https://github.com/openma-ai/open-managed-agents.git",
        ]
    )
    command(["git", "-C", destination, "fetch", "--depth", "1", "origin", UPSTREAM_PIN])
    command(["git", "-C", destination, "checkout", "--detach", "FETCH_HEAD"])
    if command(["git", "-C", destination, "rev-parse", "HEAD"]) != UPSTREAM_PIN:
        raise ValueError("Official OpenMA source pin mismatch")
    return destination


def prepare_variant(destination, reference, upstream, runtime, node_base, variant):
    if not re.fullmatch(r"node:24-bookworm-slim@sha256:[0-9a-f]{64}", node_base):
        raise ValueError("One shared digest-pinned Node24 Bookworm base is required")
    reference = command(["git", "-C", ROOT, "rev-parse", reference + "^{commit}"])
    export = new_directory(destination / "tank-source")
    raw = subprocess.check_output(["git", "-C", str(ROOT), "archive", reference, "deploy"])
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        archive.extractall(export, filter="data")
    context = destination / "context"
    receipt = subprocess.run(
        [
            sys.executable,
            str(export / "deploy/prepare_openma_source.py"),
            str(upstream),
            str(context),
            "--boxlite-runtime",
            str(runtime),
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    (destination / "canonical-prepare.stdout").write_text(receipt.stdout)
    (destination / "canonical-prepare.stderr").write_text(receipt.stderr)
    composer = module(export / "deploy/prepare_openma_deployment.py", "benchmark_composer_" + variant)
    verified = composer.deployment_verify(context)
    generated = context / "apps/main-node/Dockerfile.tank"
    text = generated.read_text()
    if text.count("FROM node:24-bookworm-slim") != 2:
        raise ValueError("Generated base-image contract changed")
    # Canonical generated source/manifests stay intact; only the external
    # benchmark Dockerfile pins tag resolution to the common input digest.
    dockerfile = destination / "Dockerfile.pinned"
    dockerfile.write_text(text.replace("FROM node:24-bookworm-slim", "FROM " + node_base))
    return {
        "variant": variant,
        "reference": reference,
        "context": str(context),
        "dockerfile": str(dockerfile),
        "canonical_dockerfile_sha256": sha(generated),
        "benchmark_dockerfile_sha256": sha(dockerfile),
        "runtime_identity": verified["runtime_identity"],
        "node_base": node_base,
        "frozen_lock_sha256": sha(context / "pnpm-lock.yaml"),
        "workspace_sha256": sha(context / "pnpm-workspace.yaml"),
        "entrypoint_sha256": sha(context / "tank-deploy/openma_entrypoint.mjs"),
        "native_shim_sha256": sha(runtime / "boxlite-shim"),
        "native_provenance_sha256": sha(runtime / "provenance.json"),
        "native_image_qualification": "NOT_RUN",
    }


def smoke_full_image(image, expected_runtime):
    code = r"""
const assert = require("node:assert/strict");
const fs = require("node:fs");
const crypto = require("node:crypto");
const native = "/opt/tank-boxlite-runtime";
for (const line of fs.readFileSync(native+"/SHA256SUMS","utf8").trim().split("\n")) {
  const [expected, name] = line.trim().split(/\s+/,2);
  const actual=crypto.createHash("sha256").update(fs.readFileSync(native+"/"+name)).digest("hex");
  assert.equal(actual,expected,name);
}
assert.equal(Number(process.versions.node.split(".")[0]),24);
(async()=> {
  const {verifyNativePlatform}=await import("/app/tank-deploy/openma_entrypoint.mjs");
  assert.equal(verifyNativePlatform(fs.readFileSync(native+"/boxlite-shim"),JSON.parse(fs.readFileSync(native+"/provenance.json","utf8"))),"amd64");
  console.log("tank-openma-native-files-ready");
})().catch(e=>{console.error(e);process.exitCode=1});
"""
    output = command(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--workdir",
            "/app",
            "--entrypoint",
            "node",
            image,
            "--eval",
            code,
        ]
    )
    if "tank-openma-native-files-ready" not in output:
        raise RuntimeError("Native runtime byte/platform smoke failed")
    for name in ("boxlite-shim", "provenance.json"):
        actual = command(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "none",
                "--workdir",
                "/opt/tank-boxlite-runtime",
                "--entrypoint",
                "sha256sum",
                image,
                name,
            ]
        ).split()[0]
        if actual != sha(expected_runtime / name):
            raise RuntimeError("Full image native payload changed")
    version = command(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--workdir",
            "/app",
            "--entrypoint",
            "pnpm",
            image,
            "--version",
        ]
    )
    if version != "11.0.8":
        raise RuntimeError("Full image pnpm version changed")
    output = command(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--workdir",
            "/app",
            "--entrypoint",
            "pnpm",
            image,
            "--dir",
            "apps/main-node",
            "exec",
            "tsx",
            "--eval",
            "const answer: number = 42; if(answer !== 42)throw Error('TS'); console.log('tank-openma-tsx:42')",
        ]
    )
    if "tank-openma-tsx:42" not in output:
        raise RuntimeError("Full image offline TypeScript execution failed")
    return {
        "native_checksums": "PASS",
        "native_platform": "amd64",
        "node_major": 24,
        "pnpm": version,
        "tsx": 42,
        "network": "none",
        "kvm_or_system_acceptance": "NOT_RUN",
    }


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def benchmark_variant(engine, output, reference, upstream, runtime, node_base, variant, inputs, *, paired):
    output.mkdir(parents=True)
    prepared = prepare_variant(output, reference, upstream, runtime, node_base, variant)
    context, dockerfile = Path(prepared["context"]), Path(prepared["dockerfile"])
    result = {"kind": "openma", **prepared, "scenarios": []}
    write_json(output / "prepared.json", prepared)
    with engine.BuildSession(
        output / "probe-builder", buildkit_image=inputs["digests"]["moby/buildkit:buildx-stable-1"]
    ) as probe:
        result["context_probe"] = probe.probe(
            context,
            dockerfile,
            [
                "pnpm-lock.yaml",
                "tank-overlay.json",
                "tank-deploy/openma_entrypoint.mjs",
                "tank-deploy/boxlite-runtime/provenance.json",
                "tank-deploy/boxlite-runtime/boxlite-shim",
            ],
        )
    with engine.BuildSession(
        output / "image-builder", buildkit_image=inputs["digests"]["moby/buildkit:buildx-stable-1"]
    ) as session:
        scenarios = (
            ("cold", "no-change", "source-edit", "dependency-text-edit-same-versions")
            if paired
            else ("cold",)
        )
        for scenario in scenarios:
            marker = context / "packages/markdown/src/tank-build-benchmark.ts"
            if scenario == "source-edit":
                if marker.exists() or not marker.parent.is_dir():
                    raise ValueError("Unexpected source benchmark marker path")
                # An unused valid TS source addition invalidates COPY packages
                # identically to editing package TS, without rewriting any
                # reviewed/identity-bound source bytes or metadata.
                marker.write_text("export const tankBuildBenchmarkMarker: number = 42;\n")
                composer = module(
                    output / "tank-source/deploy/prepare_openma_deployment.py", "benchmark_verify_marker"
                )
                composer.deployment_verify(context)
                result["source_edit"] = {
                    "path": str(marker.relative_to(context)),
                    "sha256": sha(marker),
                    "reviewed_source_bytes_unchanged": True,
                }
            elif scenario == "dependency-text-edit-same-versions":
                marker.unlink()
                text = dockerfile.read_text()
                if text.count("pnpm install ") != 1:
                    raise ValueError("Install benchmark context drift")
                dockerfile.write_text(
                    text.replace("pnpm install ", ": tank-benchmark-same-versions && pnpm install ", 1)
                )
                result["dependency_edit"] = {
                    "kind": "harmless shell no-op in the real install RUN",
                    "frozen_dependency_versions_unchanged": True,
                    "canonical_recipe_untouched": True,
                }
            build = session.build(context, dockerfile, scenario, build_args={"SKIP_CONSOLE": "1"})
            build["offline_runtime_smoke"] = smoke_full_image(build["image"], runtime)
            result["scenarios"].append(build)
            write_json(output / "result.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("before", "after"))
    parser.add_argument("--before-ref", required=True)
    parser.add_argument("--after-ref", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--inputs-file", type=Path, required=True)
    parser.add_argument("--official-archive", type=Path, required=True)
    parser.add_argument("--shim-archive", type=Path, required=True)
    args = parser.parse_args()
    inputs = json.loads(args.inputs_file.read_text())
    if inputs["before"] != args.before_ref or inputs["after"] != args.after_ref:
        raise ValueError("Shared benchmark refs differ")
    output = new_directory(args.output.resolve())
    engine = module(ROOT / "scripts/ci/docker-benchmark.py", "shared_docker_benchmark")
    runtime, native = reconstruct_runtime(
        args.official_archive, args.shim_archive, output / "runtime", ROOT / "deploy"
    )
    upstream = fetch_upstream(output / "upstream")
    write_json(output / "inputs.json", inputs)
    results = []
    for variant in (args.variant,) if args.variant else ("before", "after"):
        reference = args.before_ref if variant == "before" else args.after_ref
        results.append(
            benchmark_variant(
                engine,
                output / variant,
                reference,
                upstream,
                runtime,
                inputs["digests"]["node:24-bookworm-slim"],
                variant,
                inputs,
                paired=args.variant is None,
            )
        )
    write_json(
        output / "result.json",
        {
            "kind": "openma",
            "native_bundle": native,
            "results": results,
            "scope": "Real full image build/cache measurements and offline runtime file checks; no new KVM/system qualification.",
        },
    )
    print(json.dumps({"status": "PASS", "results": str(output / "result.json")}, indent=2))


if __name__ == "__main__":
    main()
