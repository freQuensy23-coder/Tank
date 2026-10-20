# Current deployment artifact

The registered historical deployment artifact below is **ARM64 only**. Linux
`arm64` and `amd64` local source builds are supported through the pinned public
build contract, without a developer-private binary registration. Their receipts
keep hardware qualification `NOT_RUN`; normal preparation does not claim x86 KVM
acceptance. Follow [INSTALL_LINUX.md](INSTALL_LINUX.md) for the clean-host build,
ordinary OpenMA preparation and public native qualification commands. The working
primary runtime is not changed by preparing a new one.

The existing primary uses the strict replay DNS artifact registered in
`replay-dns-qualified-artifact.json`: shim `b645c32295a3450c58536a7da9238ee44560a21800e16859ea906aa3801382fa`,
raw provenance `ed4d17b305f0695e7bcd1e82c8def8b541af07f7c069aa44c910389ebfbf3245`.
Its recipe, build controls and limited native network qualification are described
in [replay-dns/README.md](replay-dns/README.md). The default verifier rejects d44.
The d44 registry, artifact and historical qualification below remain immutable;
they anchor the original runtime/build inputs for both packaging recipes.

# BoxLite 0.10.6 shim linked to Bookworm system glibc

This versioned recipe builds official BoxLite commit
`7873ae4995e171237a3342adbb84000c8ab9965b` without tracked source changes.
It retains the complete official 0.10.6 runtime and replaces only `boxlite-shim`.
The upstream shim flags are retained except `+crt-static` becomes `-crt-static`.
The shim crate selects upstream `gvproxy,krun` features with core default
features disabled. No guest, firmware, SDK, security policy or deadline is
changed by this build.

The official static shim fails to load its original firmware on Bookworm
glibc 2.36 with the captured error `undefined symbol: _dl_var_init`.
The dated loader evidence is in
`docs/verification/boxlite-official-0.10.6-loader-2026-10-18`.

`Dockerfile` pins Rust 1.90.0 by image digest, Debian snapshot
`20261018T000000Z` and direct package versions, Go 1.25.14 by archive SHA,
the exact source commit and official runtime archive SHA. `GOTOOLCHAIN=local`
prevents an implicit Go toolchain download. Cargo uses the source lockfile;
Go uses its committed module checksum file. `source-pin.json` records those
fingerprints and `submodules.txt` records all four recursive source pins.

`platforms.json` pins the official per-architecture runtime archive and all eight
contained file hashes, Go archive hash, Rust target, ELF machine and system
loader. The amd64 archive was independently downloaded and hashed from the
official release; it is not an ARM file inventory with a different label. The
pinned Rust image digest is a manifest list containing both Linux architectures.
`TARGETARCH` selects Go and runtime downloads; the compiler script checks the
native builder architecture and chooses `aarch64-unknown-linux-gnu` or
`x86_64-unknown-linux-gnu`.

From this directory, build on the matching Linux architecture without host Rust
or Go. Select exactly one platform per native bundle:

```sh
docker --context desktop-linux build --platform linux/arm64 \
  --target runtime-export --file Dockerfile \
  --output type=local,dest=/tmp/tank-boxlite-0106-export .
```

For x86, use `--platform linux/amd64` and a separate export destination. An
emulated compiler build still cannot qualify KVM on the other architecture.

The exported `runtime` contains the eight official runtime files with only
the shim replaced. `build-evidence` records the actual binary SHA, compiler,
Debian dependencies, ELF interpreter, shared libraries and symbol versions.
Package that raw export with the versioned standard-library helper:

```sh
python3 finalize-runtime.py --export /tmp/tank-boxlite-0106-export \
  --destination /tmp/tank-boxlite-0106-candidate --architecture arm64
```

The destination must not exist. The helper checks the exact eight-file set,
original official file hashes, recorded toolchain/submodule evidence, shim
checksum and its actual ELF interpreter/dependencies/RUNPATH/glibc contract.
It writes fresh `SHA256SUMS` and `provenance.json` with
`status: UNQUALIFIED_REBUILD`, the actual shim and evidence hashes, and no
inherited hardware result or compile elapsed time. Build evidence is recorded
input evidence, not an independent supply-chain attestation.

For amd64, pass `--architecture amd64`. The actual ELF machine, dynamic loader,
Go platform and seven unchanged official files must all match that platform.
The same per-platform checks apply to the replay-DNS packager; the replay builder
can receive an explicit immutable `--builder-image sha256:...` produced from the
pinned `builder` stage and `--architecture amd64`. Its monitored 8-GiB free-space
floor remains mandatory. The historical default builder image is ARM64 and is
rejected for an amd64 request. Every new output keeps hardware qualification `NOT_RUN` until its own native isolation, mount/browser, retained-session and Managed API checks run. The source preparation path remains available before those checks.

This unpatched base export is insufficient for replay isolation and is refused by the normal verifier. The strict-DNS packager instead binds the exact public source/patch/toolchain, official files and successful offline-test/build receipts to its output. That locally built candidate can enter normal preparation while hardware qualification stays `NOT_RUN`; no developer SHA registration is required. Packaging changes no runtime install and grants no production acceptance.
For the retained qualified d44 artifact, install its existing complete
`runtime-bundle` (including its original `provenance.json` and `SHA256SUMS`),
not an unqualified re-export. The qualified artifact and every rebuilt output
remain distinguishable even if a rebuild happens to produce the same bytes.

The historical d44 artifact SHA is
`d44e3d54bf227b288bd059ece8c751339c6db0fb62a3d7c2077cd93241421255`.
It is an aarch64 dynamic executable using `/lib/ld-linux-aarch64.so.1`,
`libgcc_s.so.1`, `libm.so.6` and `libc.so.6`, with `$ORIGIN` RUNPATH and
maximum required glibc symbol version 2.34. No libc or loader is shipped.
Upstream static relocation flags remain; this is not described as a PIE.

The tested build used this exact `build.sh` inside the pinned builder with a
clean source checkout mounted at `/build/source` and `/output` mounted for
artifacts. Compilation completed in 175 seconds. The standalone multi-stage
clone/export recipe has not yet been independently executed for this version.
Source, dependencies and toolchain inputs are pinned; bit-identical rebuilds
are not claimed. Record and qualify every actual output SHA.

The configuration owner passed paired credential-free real KVM controls:
CPU1/512 MiB, original framed Python/Chromium OCI, unmounted cold preparation
and startup 61.66 seconds, mounted warm startup 3.26 seconds, guest worker hash
verified, and mount reads/writes in both directions. Both owned VMs stopped.
The SDK guest-ready deadline remained 30 seconds; cold image preparation is
separate. Docker default seccomp/AppArmor and production UID/groups remained.
Explicit native jailer/seccomp flags were false to match the existing 0.9
Linux baseline; this is a diagnostic comparison, not a production policy
selection. `qualified-artifact.json` records the exact tuple. Browser and
production adoption remain pending.

Do not place credentials, per-box configuration, generated certificates,
browser profiles or authentication files in a runtime bundle or this recipe.
The previous 0.9 recipe and its proof remain separate and unchanged.
