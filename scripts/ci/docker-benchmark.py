#!/usr/bin/env python3
"""Paired real Docker builds on a hosted runner; never run on the production host."""

import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
BEFORE = "c9d2ced6cefcfc3e6b26cfccc2b0aa0e634caa81"
AFTER = "fdddf452f21d6e4871b32aa7b3809426730929b1"
RECIPES = {"tank": "Dockerfile", "sandbox": "deploy/sandbox.Dockerfile", "capture": "capture/Dockerfile"}
BUILD_KIT = "moby/buildkit:buildx-stable-1"


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


def execute(command, *, output=None, stdin=None):
    started = time.monotonic()
    result = subprocess.run(command, input=stdin, text=True, capture_output=True)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.with_suffix(".stdout").write_text(result.stdout)
        output.with_suffix(".stderr").write_text(result.stderr)
        write_json(output.with_suffix(".command.json"), {"command": command, "seconds": time.monotonic() - started, "exit_code": result.returncode})
    if result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}): {command}\n{result.stdout[-4000:]}\n{result.stderr[-4000:]}")
    return result.stdout


def git_file(ref, name, *, optional=False):
    result = subprocess.run(["git", "show", ref + ":" + name], cwd=ROOT, text=True, capture_output=True)
    if result.returncode and optional:
        return None
    result.check_returncode()
    return result.stdout


def image_references(text):
    stages = set()
    references = set()
    for line in text.splitlines():
        match = re.match(r"FROM\s+(?:--platform=\S+\s+)?(\S+)(?:\s+AS\s+(\S+))?", line, re.I)
        if match:
            image, alias = match.groups()
            if image.lower() not in stages and image.lower() != "scratch":
                if "$" in image:
                    raise ValueError("Resolve dynamic base image before benchmarking: " + image)
                references.add(image)
            if alias:
                stages.add(alias.lower())
    return references


def pin_recipe(text, digests):
    for image in sorted(image_references(text), key=len, reverse=True):
        pinned = digests[image]
        text = re.sub(r"(?m)^(FROM\s+(?:--platform=\S+\s+)?)" + re.escape(image) + r"(?=\s|$)", lambda match: match[1] + pinned, text)
    return text


def pip_requirements(text):
    logical = text.replace("\\\n", " ")
    requirements = []
    for match in re.finditer(r"(?:python(?:3)?\s+-m\s+)?pip\s+install\s+([^\n]+)", logical):
        command = match[1].split("&&", 1)[0]
        requirements.extend(token for token in shlex.split(command) if not token.startswith("-"))
    return requirements


def constrained_recipe(text):
    lines = text.splitlines(keepends=True)
    result = []
    index = 0
    while index < len(lines):
        block = lines[index]
        index += 1
        if block.startswith("RUN "):
            while block.rstrip().endswith("\\") and index < len(lines):
                block += lines[index]
                index += 1
            if re.search(r"\bpip\s+install\b", block):
                block = block.replace("RUN ", "RUN --mount=type=bind,from=benchmark-inputs,source=constraints.txt,target=/tmp/tank-benchmark-constraints.txt ", 1)
                block = re.sub(r"(?:python(?:3)?\s+-m\s+)?pip\s+install\b", lambda match: "PIP_CONSTRAINT=/tmp/tank-benchmark-constraints.txt " + match[0], block)
        result.append(block)
    text = "".join(result)
    if not text.startswith("# syntax="):
        text = "# syntax=docker/dockerfile:1.7\n" + text
    return text


def prepare_inputs(path, before, after):
    started = time.monotonic()
    refs = {BUILD_KIT, "node:24-bookworm-slim"}
    for kind, recipe in RECIPES.items():
        for ref in (before, after):
            refs.update(image_references(git_file(ref, recipe)))
    digests = {}
    for image in sorted(refs):
        output = execute(["docker", "buildx", "imagetools", "inspect", image])
        match = re.search(r"(?m)^Digest:\s+(sha256:[a-f0-9]{64})\s*$", output)
        if not match:
            raise RuntimeError("Cannot read immutable base digest for " + image)
        digests[image] = image.split("@", 1)[0] + "@" + match[1]
    old = pip_requirements(git_file(before, RECIPES["sandbox"]))
    new = pip_requirements(git_file(after, RECIPES["sandbox"]))
    assert sorted(old) == sorted(new), (old, new)
    with tempfile.TemporaryDirectory(prefix="tank-benchmark-resolution-") as temporary:
        temporary = Path(temporary)
        # Resolve metadata only, with explicit Bookworm-compatible wheel platforms.
        # --dry-run/--target never installs or changes the host environment.
        command = [sys.executable, "-m", "pip", "install", "--dry-run", "--ignore-installed", "--no-cache-dir", "--only-binary=:all:", "--implementation", "cp", "--python-version", "3.12", "--abi", "cp312", "--target", str(temporary / "target"), "--report", str(temporary / "resolution.json")]
        for platform in ["manylinux_2_" + str(minor) + "_x86_64" for minor in range(36, 4, -1)] + ["manylinux2014_x86_64", "manylinux2010_x86_64", "manylinux1_x86_64", "linux_x86_64"]:
            command += ["--platform", platform]
        execute(command + old, output=path.parent / "resolution")
        resolution = json.loads((temporary / "resolution.json").read_text())
    constraints = "\n".join(sorted(item["metadata"]["name"] + "==" + item["metadata"]["version"] for item in resolution["install"])) + "\n"
    value = {"before": before, "after": after, "digests": digests, "sandbox_requirements": old, "sandbox_constraints": constraints, "sandbox_resolution": resolution, "preparation_seconds": time.monotonic() - started, "platform": "linux/amd64", "resolver": "Host pip --dry-run metadata resolution; CPython3.12, binary wheels, manylinux<=2.36; no Docker run/build or package installation"}
    write_json(path, value)
    return value


def vertices(path):
    rows = {}
    for line in path.read_text().splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        lower = {key.lower(): value for key, value in event.items()}
        candidates = (lower.get("vertexes") or lower.get("vertices") or []) if ("vertexes" in lower or "vertices" in lower) else [event]
        for vertex in candidates:
            vertex = {key.lower(): value for key, value in vertex.items()}
            identity = vertex.get("id", vertex.get("digest"))
            if not identity or (not vertex.get("name") and identity not in rows):
                continue
            rows.setdefault(identity, {}).update(vertex)
    out = []
    for identity, row in rows.items():
        started, completed = row.get("started"), row.get("completed")
        seconds = None
        if started and completed:
            seconds = (datetime.fromisoformat(completed.replace("Z", "+00:00")) - datetime.fromisoformat(started.replace("Z", "+00:00"))).total_seconds()
        out.append({"id": identity, "name": row["name"], "started": started, "completed": completed, "seconds": seconds, "cached": row.get("cached", False), "error": row.get("error")})
    return out


class BuildSession:
    """One owned, initially empty builder; reusable for real prepared OpenMA trees."""

    def __init__(self, output_dir, *, buildkit_image):
        self.output = Path(output_dir)
        self.output.mkdir(parents=True, exist_ok=True)
        self.name = "tank-benchmark-" + uuid.uuid4().hex[:12]
        self.buildkit_image = buildkit_image
        self.images = []
        self.results = []

    def __enter__(self):
        execute(["docker", "buildx", "create", "--name", self.name, "--driver", "docker-container", "--driver-opt", "image=" + self.buildkit_image, "--bootstrap"], output=self.output / "builder-create")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # These names were created by this object; no global prune or shared-image removal.
        try:
            if self.images:
                execute(["docker", "image", "rm", "--force", *self.images], output=self.output / "remove-own-images")
        finally:
            execute(["docker", "buildx", "rm", "--force", self.name], output=self.output / "remove-own-builder")

    def build(self, context, dockerfile, scenario, *, build_args=None, build_contexts=None, target=None):
        output = self.output / scenario
        output.mkdir(parents=True, exist_ok=True)
        tag = "tank-benchmark:" + self.name + "-" + scenario
        command = ["docker", "buildx", "build", "--builder", self.name, "--platform", "linux/amd64", "--progress=rawjson", "--provenance=false", "--load", "--tag", tag, "--metadata-file", str(output / "metadata.json"), "--file", str(dockerfile)]
        if target:
            command += ["--target", target]
        for key, value in sorted((build_args or {}).items()):
            command += ["--build-arg", key + "=" + str(value)]
        for key, value in sorted((build_contexts or {}).items()):
            command += ["--build-context", key + "=" + str(value)]
        command.append(str(context))
        self.images.append(tag)
        started = time.monotonic()
        execute(command, output=output / "build")
        elapsed = time.monotonic() - started
        steps = vertices(output / "build.stderr")
        if not steps:
            raise RuntimeError("No BuildKit vertex timings captured; raw log retained")
        inspect = json.loads(execute(["docker", "image", "inspect", tag]))[0]
        result = {"scenario": scenario, "image": tag, "image_id": inspect["Id"], "image_bytes": inspect["Size"], "wall_seconds": elapsed, "vertices": steps, "export_vertices": [row for row in steps if any(word in row["name"].lower() for word in ("export", "tarball", "importing to docker"))]}
        write_json(output / "result.json", result)
        self.results.append(result)
        return result

    def smoke(self, kind, image, revision):
        started = time.monotonic()
        execute([sys.executable, str(ROOT / "scripts/ci/docker-smoke.py"), kind, "--image", image, "--revision", revision], output=self.output / ("smoke-" + str(len(self.results))))
        return time.monotonic() - started

    def probe(self, context, dockerfile, required_paths):
        # Scratch COPY . is a neutral policy measurement, not production transfer savings.
        temporary = self.output / "context-probe"
        temporary.mkdir()
        text = Path(dockerfile).read_bytes()
        ignore_path = Path(str(dockerfile) + ".dockerignore")
        specific = ignore_path.read_bytes() if ignore_path.exists() else None
        export = temporary / "export"
        marker = "tank_benchmark_" + uuid.uuid4().hex
        sentinels = [".env." + marker, "deploy/" + marker + "/auth.json", "deploy/" + marker + "/session-storage-state.json", "deploy/" + marker + "/.env.synthetic", "deploy/secrets/" + marker, "deploy/generated/" + marker, "deploy/.git/" + marker]
        created_dirs = set()
        for name in sentinels:
            path = Path(context) / name
            parent = path.parent
            while parent != Path(context) and not parent.exists():
                created_dirs.add(parent)
                parent = parent.parent
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as stream:
                stream.write(b"SYNTHETIC BENCHMARK INPUT, NO CREDENTIALS\n")
        try:
            Path(dockerfile).write_text("FROM scratch\nCOPY . /\n")
            command = ["docker", "buildx", "build", "--builder", self.name, "--progress=rawjson", "--file", str(dockerfile), "--output", "type=local,dest=" + str(export), str(context)]
            started = time.monotonic()
            execute(command, output=temporary / "probe")
            elapsed = time.monotonic() - started
            for relative in required_paths:
                original, copied = Path(context) / relative, export / relative
                assert copied.is_file(), "Required input missing: " + str(relative)
                assert original.read_bytes() == copied.read_bytes(), relative
            files = [path for path in export.rglob("*") if path.is_file()]
            result = {"wall_seconds": elapsed, "files": len(files), "logical_bytes": sum(path.stat().st_size for path in files), "required_input_hashes_match": True, "synthetic_inputs_excluded": [name for name in sentinels if not (export / name).exists()], "synthetic_inputs_selected": [name for name in sentinels if (export / name).exists()], "vertices": vertices(temporary / "probe.stderr"), "note": "Neutral scratch COPY .; extra old-policy inputs are recorded, not treated as production Dockerfile transfer."}
            write_json(temporary / "result.json", result)
            return result
        finally:
            Path(dockerfile).write_bytes(text)
            for name in sentinels:
                (Path(context) / name).unlink()
            for directory in sorted(created_dirs, key=lambda path: len(path.parts), reverse=True):
                directory.rmdir()
            if specific is not None:
                ignore_path.write_bytes(specific)
            shutil.rmtree(export, ignore_errors=False) if export.exists() else None


def archive_tree(ref, destination):
    destination.mkdir(parents=True)
    process = subprocess.Popen(["git", "archive", ref], cwd=ROOT, stdout=subprocess.PIPE)
    with tarfile.open(fileobj=process.stdout, mode="r|") as archive:
        archive.extractall(destination, filter="data")
    assert process.wait() == 0


def overlay_recipe(tree, kind, ref, inputs):
    name = RECIPES[kind]
    recipe = tree / name
    text = pin_recipe(git_file(ref, name), inputs["digests"])
    if kind == "sandbox":
        text = constrained_recipe(text)
    recipe.write_text(text)
    ignore_names = [".dockerignore", "deploy/sandbox.Dockerfile.dockerignore", "deploy/prepare_openma_source.py"] if kind != "capture" else ["capture/.dockerignore"]
    for ignore in ignore_names:
        value = git_file(ref, ignore, optional=True)
        path = tree / ignore
        if value is None:
            path.unlink(missing_ok=True)
        else:
            path.write_text(value)
    return recipe


def runtime_inventory(image, output):
    code = r"""import importlib.metadata as m,json,subprocess,sys
print(json.dumps({"python":sys.version,"packages":{d.metadata["Name"].lower().replace("_","-"):d.version for d in m.distributions()},"apt":subprocess.check_output(["dpkg-query","-W","-f=${Package}=${Version}\\n"],text=True).splitlines()},sort_keys=True))"""
    value = execute(["docker", "run", "--rm", "--network", "none", "--entrypoint", "python", image, "-c", code], output=output)
    return json.loads(value)


def dependency_text_edit(kind, tree, recipe):
    if kind == "tank":
        metadata = tree / "pyproject.toml"
        metadata.write_text(metadata.read_text() + "\n# benchmark metadata-only edit, dependency versions unchanged\n")
    elif kind == "sandbox":
        text = recipe.read_text()
        match = re.search(r"(pip\s+install(?:\s+--no-cache-dir)?)\s+pandas\b", text)
        assert match, "Scientific dependency RUN not found"
        # A real shell no-op changes RUN's cache key; versions stay fixed.
        run_start = text.rfind("RUN ", 0, match.start())
        block_end = text.find("\nRUN ", match.end())
        block_end = len(text) if block_end < 0 else block_end
        block = text[run_start:block_end]
        command = re.search(r"PIP_CONSTRAINT=\S+\s+(?:python(?:3)?\s+-m\s+)?pip\s+install", block)
        assert command
        block = block[:command.start()] + ": tank-benchmark-same-versions && " + block[command.start():]
        recipe.write_text(text[:run_start] + block + text[block_end:])
    else:
        metadata = tree / "capture/go.mod"
        metadata.write_text(metadata.read_text() + "\n// benchmark metadata-only edit, dependency versions unchanged\n")


def benchmark_arm(kind, variant, ref, after, inputs, output, paired):
    started = time.monotonic()
    tree = output / "tree"
    archive_tree(after, tree)  # Same product input snapshot for both recipe arms.
    recipe = overlay_recipe(tree, kind, ref, inputs)
    context = tree / "capture" if kind == "capture" else tree
    required = ["go.mod", "internal/schema/market_schema.json"] if kind == "capture" else (["deploy/openma_browser/browser-worker.py", "deploy/openma_browser/prepare-font-cache.py"] if kind == "sandbox" else ["pyproject.toml", "uv.lock", "src/tank/cli.py", "deploy/tank_entrypoint.py"])
    records = {"kind": kind, "variant": variant, "recipe_ref": ref, "product_ref": after, "recipe_sha256": hashlib.sha256(recipe.read_bytes()).hexdigest()}
    write_json(output / "instrumented-recipe.json", {"text": recipe.read_text(), "sha256": records["recipe_sha256"], "instrumentation": "Immutable FROM digests; sandbox pip constraints bind mount; production source recipe files untouched."})
    with BuildSession(output / "probe-builder", buildkit_image=inputs["digests"][BUILD_KIT]) as probe:
        records["context_probe"] = probe.probe(context, recipe, required)
    shared = output / "frozen-inputs"
    shared.mkdir()
    (shared / "constraints.txt").write_text(inputs["sandbox_constraints"])
    with BuildSession(output / "image-builder", buildkit_image=inputs["digests"][BUILD_KIT]) as session:
        cold_start = time.monotonic()
        kwargs = {"build_args": {"GIT_SHA": after}, "build_contexts": {"benchmark-inputs": str(shared)} if kind == "sandbox" else {}}
        cold = session.build(context, recipe, "cold", **kwargs)
        cold["smoke_seconds"] = session.smoke(kind, cold["image"], after)
        if kind != "capture":
            records["runtime_inventory"] = runtime_inventory(cold["image"], output / "runtime-inventory")
        records["cold_build_and_smoke_seconds"] = time.monotonic() - cold_start
        records["ci_style_core_seconds"] = records["context_probe"]["wall_seconds"] + records["cold_build_and_smoke_seconds"]
        records["ci_style_cold_seconds"] = time.monotonic() - started
        records["timing_definitions"] = {"ci_style_core_seconds": "Scratch-export wall plus actual cold build/load, smoke and inventory; excludes builder setup", "ci_style_cold_seconds": "Elapsed arm setup/archive, neutral context proof, empty cold builder setup, real build/load/smoke/inventory; excludes later warm scenarios and final cleanup", "arm_total_seconds": "Entire selected arm including owned image/builder and source cleanup"}
        records["scenarios"] = [cold]
        if paired:
            unchanged = session.build(context, recipe, "no-change", **kwargs)
            unchanged["smoke_seconds"] = session.smoke(kind, unchanged["image"], after)
            records["scenarios"].append(unchanged)
            source = tree / ("deploy/openma_browser/browser-worker.py" if kind == "sandbox" else ("capture/cmd/tank-capture/main.go" if kind == "capture" else "src/tank/__init__.py"))
            original = source.read_bytes()
            source.write_bytes(original + (b"\n// benchmark source-only comment\n" if kind == "capture" else b"\n# benchmark source-only comment\n"))
            changed = session.build(context, recipe, "worker-edit" if kind == "sandbox" else "source-edit", **kwargs)
            changed["smoke_seconds"] = session.smoke(kind, changed["image"], after)
            records["scenarios"].append(changed)
            source.write_bytes(original)
            dependency_text_edit(kind, tree, recipe)
            metadata = session.build(context, recipe, "dependency-text-edit-same-versions", **kwargs)
            metadata["smoke_seconds"] = session.smoke(kind, metadata["image"], after)
            records["scenarios"].append(metadata)
        records["total_seconds_including_setup_and_cleanup_pending"] = time.monotonic() - started
        write_json(output / "result.json", records)
    shutil.rmtree(tree)
    records["arm_total_seconds"] = time.monotonic() - started
    write_json(output / "result.json", records)
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=tuple(RECIPES), default="tank")
    parser.add_argument("--variant", choices=("before", "after"))
    parser.add_argument("--before-ref", default=BEFORE)
    parser.add_argument("--after-ref", default=AFTER)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--inputs-file", type=Path)
    parser.add_argument("--prepare-inputs", type=Path)
    args = parser.parse_args()
    if args.prepare_inputs:
        prepare_inputs(args.prepare_inputs.resolve(), args.before_ref, args.after_ref)
        return
    if not os.environ.get("GITHUB_ACTIONS"):
        raise RuntimeError("Hosted runner required; production-host Docker benchmark builds are forbidden")
    if args.output is None:
        parser.error("--output is required for a benchmark")
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    inputs = json.loads(args.inputs_file.read_text()) if args.inputs_file else prepare_inputs(args.output / "inputs.json", args.before_ref, args.after_ref)
    assert inputs["before"] == args.before_ref and inputs["after"] == args.after_ref
    write_json(args.output / "inputs.json", inputs)
    environment = {"cpu_count": os.cpu_count(), "platform": execute(["uname", "-a"]).strip(), "docker": execute(["docker", "version"]), "buildx": execute(["docker", "buildx", "version"]), "inputs_sha256": hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest(), "resource_quotas": "No CPU, memory or affinity limits applied by this harness"}
    write_json(args.output / "environment.json", environment)
    results = []
    for variant in ([args.variant] if args.variant else ["before", "after"]):
        arm = args.output / variant
        arm.mkdir()
        ref = args.before_ref if variant == "before" else args.after_ref
        results.append(benchmark_arm(args.kind, variant, ref, args.after_ref, inputs, arm, paired=args.variant is None))
    if len(results) == 2 and args.kind != "capture":
        assert results[0]["runtime_inventory"] == results[1]["runtime_inventory"], "Old/new runtime dependency inventories differ; comparison rejected"
    write_json(args.output / "result.json", {"kind": args.kind, "arms": results, "same_runtime_inventory": True if len(results) == 2 and args.kind != "capture" else None, "note": "Raw BuildKit vertex durations overlap; do not add them as elapsed wall time. Context scratch export is reported separately from production build transfer."})
    print(json.dumps({"output": str(args.output), "kind": args.kind, "arms": [row["variant"] for row in results]}))


if __name__ == "__main__":
    main()
