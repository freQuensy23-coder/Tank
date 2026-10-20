#!/bin/bash
set -euo pipefail
export RUSTUP_TOOLCHAIN=1.90.0
export CARGO_BUILD_JOBS=3
cd /build/source
expected=690794512fde9491836c1aa3d087d64c80bf4849
test "$(git rev-parse HEAD)" = "$expected"
test "$(git -C src/deps/libkrun-sys/vendor/libkrun rev-parse HEAD)" = e12b9b3780ffa8df9f3e1797b217d13453479167
git diff --exit-code
git submodule status --recursive > /output/submodules.txt
rustc --version --verbose > /output/rustc-version.txt
go version > /output/go-version.txt
dpkg-query -W -f='${Package}=${Version}\n' > /output/build-packages.txt
getconf GNU_LIBC_VERSION > /output/glibc-version.txt
# Use upstream's exact shim features. Change only the host CRT linkage policy:
# dynamic system glibc enables normal dlopen of the pinned libkrunfw.so.5.
export RUSTFLAGS='-C target-feature=-crt-static -C link-arg=-Wl,-z,stack-size=2097152 -C link-arg=-Wl,--allow-multiple-definition'
export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS="$RUSTFLAGS"
cargo build --locked --release -p boxlite --no-default-features --features gvproxy,krun --bin boxlite-shim --target aarch64-unknown-linux-gnu
install -m 755 target/aarch64-unknown-linux-gnu/release/boxlite-shim /output/boxlite-shim
(cd /output && sha256sum boxlite-shim > SHA256SUMS)
file /output/boxlite-shim > /output/file.txt
readelf --program-headers /output/boxlite-shim > /output/program-headers.txt
readelf --dynamic /output/boxlite-shim > /output/dynamic.txt
readelf --version-info /output/boxlite-shim > /output/versions.txt
ldd /output/boxlite-shim > /output/ldd.txt
git diff --exit-code
