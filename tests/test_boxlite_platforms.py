"""Build/format contracts only; these tests never claim native KVM execution."""

import hashlib
import importlib.util
import io
import json
import struct
import subprocess
import tarfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
RECIPE = ROOT / "deploy/boxlite/v0.10.6"


def helper(path):
    spec = importlib.util.spec_from_file_location("tank_platform_contract", path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def synthetic_shim(architecture, *, needed=None, runpath=b"$ORIGIN"):
    platform = json.loads((RECIPE / "platforms.json").read_text())["platforms"][architecture]
    needed = needed if needed is not None else [b"libgcc_s.so.1", b"libm.so.6", b"libc.so.6"] + (
        [b"ld-linux-x86-64.so.2"] if architecture == "amd64" else []
    )
    dynamic_size = 16 * (len(needed) + 2 + (runpath is not None))
    image = bytearray(1024)
    image[:6] = b"\x7fELF\x02\x01"
    struct.pack_into("<H", image, 18, platform["elf_machine"])
    struct.pack_into("<Q", image, 32, 64)
    struct.pack_into("<HH", image, 54, 56, 3)
    interpreter = platform["interpreter"].encode() + b"\0"
    image[256 : 256 + len(interpreter)] = interpreter
    for index, values in enumerate(
        [
            (1, 4, 0, 0, 0, len(image), len(image), 4096),
            (3, 4, 256, 256, 0, len(interpreter), len(interpreter), 1),
            (2, 6, 320, 320, 0, dynamic_size, dynamic_size, 8),
        ]
    ):
        struct.pack_into("<IIQQQQQQ", image, 64 + index * 56, *values)
    strings = bytearray(b"\0")
    offsets = []
    for value in [*needed, *([] if runpath is None else [runpath]), b"GLIBC_2.36"]:
        offsets.append(len(strings))
        strings.extend(value + b"\0")
    image[512 : 512 + len(strings)] = strings
    tags = [(5, 512), *[(1, n) for n in offsets[: len(needed)]]]
    if runpath is not None:
        tags.append((29, offsets[len(needed)]))
    tags.append((0, 0))
    for index, values in enumerate(tags):
        struct.pack_into("<qQ", image, 320 + index * 16, *values)
    return bytes(image)


@pytest.mark.parametrize("architecture", ["amd64", "arm64"])
def test_actual_elf_machine_loader_dependency_contract(architecture):
    module = helper(RECIPE / "finalize-runtime.py")
    image = synthetic_shim(architecture)
    actual = module.elf_contract(image, architecture)
    assert actual["architecture"] == {"amd64": "x86_64", "arm64": "aarch64"}[architecture]
    assert actual["highest_required_glibc"] == "2.36"
    assert actual["needed"] == ["libgcc_s.so.1", "libm.so.6", "libc.so.6"] + (
        ["ld-linux-x86-64.so.2"] if architecture == "amd64" else []
    )
    opposite = "arm64" if architecture == "amd64" else "amd64"
    with pytest.raises(ValueError, match="machine"):
        module.elf_contract(image, opposite)
    with pytest.raises(ValueError, match="ELF"):
        module.elf_contract(image[:63], architecture)
    malformed = bytearray(image)
    struct.pack_into("<Q", malformed, 32, len(image))
    with pytest.raises(ValueError, match="headers"):
        module.elf_contract(malformed, architecture)


@pytest.mark.parametrize("architecture", ["amd64", "arm64"])
@pytest.mark.parametrize("violation", ["missing", "extra", "foreign-loader", "duplicate", "runpath", "no-runpath"])
def test_elf_contract_refuses_dependency_and_runpath_changes(architecture, violation):
    module = helper(RECIPE / "finalize-runtime.py")
    needed = [b"libgcc_s.so.1", b"libm.so.6", b"libc.so.6"] + (
        [b"ld-linux-x86-64.so.2"] if architecture == "amd64" else []
    )
    runpath = b"$ORIGIN"
    if violation == "missing":
        needed.remove(b"libc.so.6")
    elif violation == "extra":
        needed.append(b"libssl.so.3")
    elif violation == "foreign-loader":
        needed.append(b"ld-linux-aarch64.so.1" if architecture == "amd64" else b"ld-linux-x86-64.so.2")
    elif violation == "duplicate":
        needed.append(b"libc.so.6")
    elif violation == "runpath":
        runpath = b"/tmp/untrusted"
    elif violation == "no-runpath":
        runpath = None
    with pytest.raises(ValueError, match="dependencies or RUNPATH"):
        module.elf_contract(synthetic_shim(architecture, needed=needed, runpath=runpath), architecture)


@pytest.mark.parametrize("architecture", ["amd64", "arm64"])
def test_finalizer_does_not_inherit_other_platform_or_hardware_results(tmp_path, monkeypatch, architecture):
    module = helper(RECIPE / "finalize-runtime.py")
    recipe = tmp_path / "recipe"
    recipe.mkdir()
    export = tmp_path / "export"
    runtime, evidence = export / "runtime", export / "build-evidence"
    runtime.mkdir(parents=True)
    evidence.mkdir()
    expected = json.loads((RECIPE / "qualified-artifact.json").read_text())
    platforms = json.loads((RECIPE / "platforms.json").read_text())
    shim = synthetic_shim(architecture)
    for name in expected["original_files"]:
        (runtime / name).write_bytes(shim if name == "boxlite-shim" else (architecture + name).encode())
    files = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in runtime.iterdir()}
    platforms["platforms"][architecture]["original_files"] = files
    (recipe / "platforms.json").write_text(json.dumps(platforms))
    (recipe / "qualified-artifact.json").write_text(json.dumps(expected))
    (recipe / "submodules.txt").write_bytes((RECIPE / "submodules.txt").read_bytes())
    for name in [
        "build-packages.txt",
        "dynamic.txt",
        "file.txt",
        "ldd.txt",
        "program-headers.txt",
        "versions.txt",
    ]:
        (evidence / name).write_text("synthetic contract evidence\n")
    (evidence / "boxlite-shim").write_bytes(shim)
    (evidence / "SHA256SUMS").write_text(files["boxlite-shim"] + "  boxlite-shim\n")
    (evidence / "submodules.txt").write_bytes((RECIPE / "submodules.txt").read_bytes())
    (evidence / "rustc-version.txt").write_text("rustc 1.90.0 synthetic\n")
    (evidence / "go-version.txt").write_text("go version go1.25.14 linux/" + architecture + "\n")
    (evidence / "glibc-version.txt").write_text("glibc 2.36\n")
    monkeypatch.setattr(module, "BASE", recipe)
    result = module.finalize(export, tmp_path / "candidate", architecture)
    assert result["architecture"] == architecture
    assert result["status"] == "UNQUALIFIED_REBUILD"
    assert result["qualification"]["inherited_hardware_results"] is False
    assert result["qualification"]["production_adoption"] is False
    assert result["original_files"] == files
    assert result["build"]["command"].endswith(platforms["platforms"][architecture]["rust_target"])
    assert "elapsed_compile_seconds" not in result["build"]
    opposite = "arm64" if architecture == "amd64" else "amd64"
    (evidence / "go-version.txt").write_text("go version go1.25.14 linux/" + opposite + "\n")
    with pytest.raises(ValueError, match="Go toolchain"):
        module.finalize(export, tmp_path / "wrong-toolchain", architecture)
    assert not (tmp_path / "wrong-toolchain").exists()


def test_unproven_amd64_export_cannot_borrow_arm64_qualification(tmp_path):
    module = helper(ROOT / "deploy/prepare_boxlite_runtime.py")
    (tmp_path / "provenance.json").write_text(
        json.dumps({"source": {"version": "0.10.6"}, "architecture": "amd64"})
    )
    with pytest.raises(ValueError, match="verified pinned local build contract"):
        module.verify_runtime(tmp_path)


@pytest.mark.parametrize("architecture", ["amd64", "arm64"])
def test_oci_guest_matches_native_platform_and_refuses_mismatch(tmp_path, architecture):
    module = helper(ROOT / "deploy/prepare_sandbox.py")
    config = json.dumps({"os": "linux", "architecture": architecture}).encode()
    digest = hashlib.sha256(config).hexdigest()
    descriptor = {
        "mediaType": "application/vnd.oci.image.config.v1+json",
        "size": len(config),
        "digest": "sha256:" + digest,
    }
    archive = tmp_path / "oci.tar"
    with tarfile.open(archive, "w") as bundle:
        for name, value in {
            "oci-layout": b'{"imageLayoutVersion":"1.0.0"}',
            "index.json": json.dumps({"schemaVersion": 2, "manifests": [descriptor]}).encode(),
            "blobs/sha256/" + digest: config,
        }.items():
            info = tarfile.TarInfo(name)
            info.size = len(value)
            bundle.addfile(info, io.BytesIO(value))
    result = module.prepare(archive, tmp_path / "valid", boxlite_version="0.10.6", architecture=architecture)
    assert result["architecture"] == architecture
    assert result["vm_execution"] == "UNVERIFIED_KVM_REQUIRED"
    opposite = "arm64" if architecture == "amd64" else "amd64"
    with pytest.raises(ValueError, match="does not match"):
        module.prepare(archive, tmp_path / "invalid", architecture=opposite)
    assert not (tmp_path / "invalid").exists()


def test_both_recipe_architectures_keep_pinned_downloads_and_shell_syntax():
    platforms = json.loads((RECIPE / "platforms.json").read_text())["platforms"]
    dockerfile = (RECIPE / "Dockerfile").read_text()
    for architecture, value in platforms.items():
        assert architecture + ")" in dockerfile
        assert value["go_archive_sha256"] in dockerfile
        assert value["official_runtime"]["archive_sha256"] in dockerfile
    for path in [RECIPE / "build.sh", RECIPE / "replay-dns/build.sh"]:
        subprocess.run(["bash", "-n", str(path)], check=True)
        assert '--target "$target"' in path.read_text()


def test_build_monitor_refuses_mutable_or_wrong_platform_before_any_build(tmp_path, monkeypatch):
    module = helper(RECIPE / "replay-dns/monitored_build.py")
    root, assets = tmp_path / "build", tmp_path / "assets"
    root.mkdir()
    assets.mkdir()
    calls = []

    def inspect_only(args):
        calls.append(args)
        assert args[:2] == ["image", "inspect"]
        return json.dumps([{"Os": "linux", "Architecture": "arm64"}])

    monkeypatch.setattr(module, "command", inspect_only)
    with pytest.raises(ValueError, match="immutable"):
        module.run(
            root, assets, "build", "default", 1800, 1, builder_image="rust:latest", architecture="amd64"
        )
    assert not calls
    with pytest.raises(ValueError, match="does not match"):
        module.run(
            root,
            assets,
            "build",
            "default",
            1800,
            1,
            builder_image="sha256:" + "a" * 64,
            architecture="amd64",
        )
    assert len(calls) == 1
    assert not list(root.iterdir())


def test_openma_entrypoint_refuses_cross_architecture_before_secrets_or_spawn():
    import shutil

    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is required for the actual deployment entrypoint contract")
    program = """
import {verifyNativePlatform} from './deploy/openma_entrypoint.mjs';
const shim=machine=>{const data=Buffer.alloc(64);Buffer.from([0x7f,0x45,0x4c,0x46,2,1]).copy(data);data.writeUInt16LE(machine,18);return data;};
const amd64=verifyNativePlatform(shim(62),{architecture:'amd64'},{platform:'linux',arch:'x64'});
const arm64=verifyNativePlatform(shim(183),{elf:{architecture:'aarch64'}},{platform:'linux',arch:'arm64'});
let rejected=0;
for (const [image,meta,host] of [
  [shim(183),{architecture:'arm64'},{platform:'linux',arch:'x64'}],
  [shim(62),{architecture:'arm64'},{platform:'linux',arch:'x64'}],
  [shim(183),{architecture:'arm64'},{platform:'darwin',arch:'arm64'}],
]) {try {verifyNativePlatform(image,meta,host);}catch {rejected++;}}
console.log(JSON.stringify({amd64,arm64,rejected}));
"""
    result = subprocess.run(
        [node, "--input-type=module", "-e", program], cwd=ROOT, check=True, capture_output=True, text=True
    )
    assert json.loads(result.stdout) == {"amd64": "amd64", "arm64": "arm64", "rejected": 3}


def test_source_patch_format_is_independent_of_git_default_abbreviation(tmp_path):
    module = helper(RECIPE / "replay-dns/package_candidate.py")
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    tracked = tmp_path / "tracked.txt"
    tracked.write_text("original\n")
    subprocess.run(["git", "-C", str(tmp_path), "add", "tracked.txt"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "-c", "user.name=Tank QA", "-c", "user.email=qa@example.invalid", "commit", "-qm", "fixture"], check=True)
    tracked.write_text("reviewed change\n")
    outputs = []
    defaults = []
    for abbreviation in (4, 12):
        subprocess.run(["git", "-C", str(tmp_path), "config", "core.abbrev", str(abbreviation)], check=True)
        defaults.append(subprocess.check_output(["git", "-C", str(tmp_path), "diff", "--binary"]))
        outputs.append(module.tracked_source_diff(tmp_path))
    assert defaults[0] != defaults[1]
    assert outputs[0] == outputs[1]
    assert b"reviewed change" in outputs[0]
    assert "git diff --binary --abbrev=8 > /output/source.patch" in (RECIPE / "replay-dns/prepare.sh").read_text()
