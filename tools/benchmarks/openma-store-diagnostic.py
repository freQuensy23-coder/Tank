"""One hosted diagnostic of pnpm store placement; never alter product recipes."""

import argparse
import importlib.util
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PRODUCT_REF = "fca4e38bee455d22c30dabcd4a758e3fb3261c2e"
MOUNT = "--mount=type=cache,id=tank-openma-pnpm11,target=/pnpm/store,sharing=locked "
MOUNTED_IMAGE_BYTES = 2_168_589_565

FILESYSTEM_METRICS = r"""
import json, os, stat, subprocess
paths = ["/app", "/app/node_modules", "/app/packages", "/app/apps",
         "/opt/tank-boxlite-runtime", "/app/tank-deploy"]
result = {}
for path in paths:
    if not os.path.exists(path):
        result[path] = {"exists": False}
        continue
    inodes = {}
    regular_files = symlinks = total_bytes = hardlinked_paths = 0
    for root, directories, files in os.walk(path, followlinks=False):
        for name in files + [name for name in directories if os.path.islink(os.path.join(root, name))]:
            item = os.path.join(root, name)
            meta = os.lstat(item)
            if stat.S_ISLNK(meta.st_mode):
                symlinks += 1
            elif stat.S_ISREG(meta.st_mode):
                regular_files += 1
                total_bytes += meta.st_size
                hardlinked_paths += meta.st_nlink > 1
                inodes.setdefault((meta.st_dev, meta.st_ino), (meta.st_size, meta.st_nlink))
    result[path] = {
        "exists": True,
        "regular_files": regular_files,
        "symlinks": symlinks,
        "file_path_bytes": total_bytes,
        "unique_regular_inodes": len(inodes),
        "unique_inode_bytes": sum(value[0] for value in inodes.values()),
        "hardlinked_paths": hardlinked_paths,
        "hardlinked_unique_inodes": sum(value[1] > 1 for value in inodes.values()),
        "du_allocated_bytes": int(subprocess.check_output(["du", "-s", "-B1", path], text=True).split()[0]),
        "du_apparent_bytes": int(subprocess.check_output(["du", "-s", "-B1", "--apparent-size", path], text=True).split()[0]),
    }
print(json.dumps(result, sort_keys=True))
"""


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def local_store_recipe(text):
    needle = "RUN " + MOUNT
    if text.count(needle) != 1 or text.count("--store-dir=/pnpm/store") != 1:
        raise ValueError("Expected one mounted frozen pnpm install")
    result = text.replace(needle, "RUN ", 1)
    command = "corepack enable && corepack prepare pnpm@11.0.8 --activate && pnpm install"
    if result.replace("RUN " + command, needle + command, 1) != text:
        raise ValueError("The diagnostic may remove only the pnpm cache mount")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs-file", type=Path, required=True)
    parser.add_argument("--official-archive", type=Path, required=True)
    parser.add_argument("--shim-archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not os.environ.get("GITHUB_ACTIONS"):
        raise RuntimeError("Hosted runner required; production-host Docker builds are forbidden")
    inputs = json.loads(args.inputs_file.read_text())
    if inputs["after"] != PRODUCT_REF:
        raise ValueError("Diagnostic must use the final frozen product ref")
    full = load_module(ROOT / "scripts/ci/openma-build-benchmark.py", "openma_store_full")
    engine = load_module(ROOT / "scripts/ci/docker-benchmark.py", "openma_store_engine")
    output = full.new_directory(args.output.resolve())
    full.write_json(output / "inputs.json", inputs)
    runtime, native = full.reconstruct_runtime(
        args.official_archive, args.shim_archive, output / "runtime", ROOT / "deploy"
    )
    upstream = full.fetch_upstream(output / "upstream")
    prepared_dir = output / "after"
    prepared_dir.mkdir()
    prepared = full.prepare_variant(
        prepared_dir,
        PRODUCT_REF,
        upstream,
        runtime,
        inputs["digests"]["node:24-bookworm-slim"],
        "after",
    )
    canonical = Path(prepared["context"]) / "apps/main-node/Dockerfile.tank"
    dockerfile = Path(prepared["dockerfile"])
    mounted_recipe = dockerfile.read_text()
    dockerfile.write_text(local_store_recipe(mounted_recipe))
    full.write_json(
        output / "prepared.json",
        {
            **prepared,
            "diagnostic_only": True,
            "canonical_recipe_unchanged": full.sha(canonical) == prepared["canonical_dockerfile_sha256"],
            "change": "Remove only external pnpm cache mount; keep identical frozen install and /pnpm/store path",
            "mounted_benchmark_dockerfile_sha256": prepared["benchmark_dockerfile_sha256"],
            "local_store_benchmark_dockerfile_sha256": full.sha(dockerfile),
        },
    )
    with engine.BuildSession(
        output / "image-builder",
        buildkit_image=inputs["digests"]["moby/buildkit:buildx-stable-1"],
    ) as session:
        build = session.build(
            Path(prepared["context"]),
            dockerfile,
            "cold-local-pnpm-store",
            build_args={"SKIP_CONSOLE": "1"},
        )
        build["offline_runtime_smoke"] = full.smoke_full_image(build["image"], runtime)
        history = full.command(
            ["docker", "history", "--no-trunc", "--human=false", "--format", "{{json .}}", build["image"]]
        )
        (output / "history.jsonl").write_text(history + "\n")
        inspect = json.loads(full.command(["docker", "image", "inspect", build["image"]]))
        full.write_json(output / "image-inspect.json", inspect)
        metrics = json.loads(
            full.command(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--network",
                    "none",
                    "--entrypoint",
                    "python3",
                    build["image"],
                    "-c",
                    FILESYSTEM_METRICS,
                ]
            )
        )
        full.write_json(output / "filesystem-metrics.json", metrics)
        full.write_json(
            output / "result.json",
            {
                "status": "PASS",
                "product_ref": PRODUCT_REF,
                "native_bundle": native,
                "build": build,
                "filesystem_metrics": metrics,
                "mounted_after_reference_image_bytes": MOUNTED_IMAGE_BYTES,
                "image_bytes_delta_from_mounted_after": build["image_bytes"] - MOUNTED_IMAGE_BYTES,
                "scope": "One diagnostic cold full image with store inside build-stage filesystem; unchanged product and runtime/native bytes. No KVM/system acceptance.",
            },
        )
    print(json.dumps({"status": "PASS", "results": str(output / "result.json")}, indent=2))


if __name__ == "__main__":
    main()
