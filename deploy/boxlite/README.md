# BoxLite 0.9.0 shim linked to Bookworm system glibc

Official source: https://github.com/boxlite-ai/boxlite.git

Exact v0.9.0 commit: `690794512fde9491836c1aa3d087d64c80bf4849`.
Pinned libkrun submodule: `e12b9b3780ffa8df9f3e1797b217d13453479167`.
No tracked upstream source changes are used.

The official shim build script explicitly selects static glibc. This build
selects system dynamic glibc, retaining the upstream stack size and duplicate
symbol handling, and uses the same intended shim features `gvproxy,krun`.
Explicit `-p boxlite` selects the shim's package instead of enabling unrelated
workspace packages' default features. Existing SDK/guest/bubblewrap components
are unchanged. No isolation feature or runtime security policy is disabled.

`Dockerfile` is the complete multi-stage build/export recipe. Its Debian
repositories are pinned to official snapshot timestamp `20261018T000000Z`,
with exact direct package versions. Both Debian and Debian Security snapshot
Release files and all package versions were checked.

The builder targets Linux aarch64 on Debian Bookworm. Rust 1.90.0 is explicitly
selected with `RUSTUP_TOOLCHAIN` because the upstream rust-toolchain file says
`stable`. The Rust base-image manifest digest is pinned in Dockerfile.
Go 1.24.6 is verified against the SHA-256 published by https://go.dev/dl/.
Cargo.lock and Go's go.sum pin dependency inputs. Upstream libkrun-sys verifies
its official v5.3.0 firmware download checksum. `output/build-packages.txt`
records exact Debian build-package versions; `output/ldd.txt` records runtime
libraries. This recipe reproduces the source/toolchain/linkage build. Upstream
generated inputs can be nondeterministic: a cached rebuild produced a different
binary SHA with unchanged source and dependencies. Each artifact therefore
needs its own SHA and KVM proof; bit-identical rebuilds are not claimed.

From a new work directory containing Dockerfile and build.sh:

```sh
docker build --platform linux/arm64 --target runtime-export \
  -f Dockerfile --output type=local,dest=./repro-export .
```

The build clones the exact source and recursively initializes its pinned
submodules inside Docker. It downloads the official v0.9.0 runtime archive and
checks SHA `cce6512be469c826c0cb62c604af240849980fa130ac8708e8d1e5ca289d0f31`.
The exported `runtime` contains that archive's files with only the shim
replaced; `build-evidence` records the new artifact's hashes and contracts.

`build.sh` checks source/submodule identity and tracked-source cleanliness
before and after compilation. It produces the shim, SHA-256, toolchain and
Debian dependency records, ELF headers, dynamic dependencies and symbol
version requirements. It performs no production deployment and reads no
application secrets. The build's own container and image are named explicitly;
it does not change the default Docker context or prune other images.

The production runtime must provide the same aarch64 Bookworm system loader
and required system libraries reported in `ldd.txt`. The existing BoxLite jail
already exposes `/usr`, `/lib` and `/lib64` read-only. Original unpatched
firmware is retained. The configuration owner completed a real Colima KVM boot
using the delivered c8259b artifact: guest `python3` exited 0 and reported
Python 3.12.15 on aarch64 with the full Python/Chromium OCI image.
The proven delivery directory is `runtime-bundle`; its SHA256SUMS and
provenance.json bind the boot-tested shim to the source/build inputs.

The exported `/runtime` directory must be finalized with
`deploy/prepare_boxlite_runtime.py` inside the Tank Python image before passing
`--boxlite-runtime` to `prepare_openma_source.py`. That helper verifies unchanged
original firmware/guest/helpers and the ARM64 system-loader ELF contract, writes
artifact hashes, and marks hardware verification as required for the newly built
artifact. It does not inherit a previous binary's KVM acceptance result.

No credentials, BoxLite per-box configuration, generated certificates, browser
profiles or model auth files belong in this directory. The verifier rejects
unexpected files before anything is copied into an image context.
