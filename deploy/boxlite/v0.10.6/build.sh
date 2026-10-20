#!/bin/bash
set -euo pipefail
case "$(uname -m)" in
  aarch64) architecture=arm64; target=aarch64-unknown-linux-gnu ;;
  x86_64) architecture=amd64; target=x86_64-unknown-linux-gnu ;;
  *) echo "Unsupported native Linux builder architecture" >&2; exit 1 ;;
esac
if [[ -n "${TARGETARCH:-}" && "$TARGETARCH" != "$architecture" ]]; then
  echo "Builder and requested architecture differ; use a native matching Linux builder" >&2
  exit 1
fi
export RUSTUP_TOOLCHAIN=1.90.0 CARGO_BUILD_JOBS=3 GOTOOLCHAIN=local
cd /build/source
git config --global --add safe.directory /build/source
while IFS= read -r module; do git config --global --add safe.directory "/build/source/$module"; done < <(git config --file .gitmodules --get-regexp path | awk '{print $2}')
test "$(git rev-parse HEAD)" = 7873ae4995e171237a3342adbb84000c8ab9965b
test "$(git -C src/deps/libkrun-sys/vendor/libkrun rev-parse HEAD)" = e12b9b3780ffa8df9f3e1797b217d13453479167
git diff --exit-code
git submodule status --recursive > /output/submodules.txt
rustc --version --verbose > /output/rustc-version.txt
go version > /output/go-version.txt
dpkg-query -W -f='${Package}=${Version}\n' > /output/build-packages.txt
getconf GNU_LIBC_VERSION > /output/glibc-version.txt
# Match scripts/build/build-shim.sh exactly except +crt-static -> -crt-static.
# The shim crate itself pins gvproxy + krun and disables core default features.
export RUSTFLAGS='-C target-feature=-crt-static -C relocation-model=static -C link-arg=-Wl,-z,stack-size=2097152 -C link-arg=-Wl,--allow-multiple-definition'
target_key=${target^^}
target_key=${target_key//-/_}
export "CARGO_TARGET_${target_key}_RUSTFLAGS=$RUSTFLAGS"
cargo build --locked --release -p boxlite-shim --target "$target"
install -m 755 target/"$target"/release/boxlite-shim /output/boxlite-shim
(cd /output && sha256sum boxlite-shim > SHA256SUMS)
file /output/boxlite-shim > /output/file.txt
readelf --program-headers /output/boxlite-shim > /output/program-headers.txt
readelf --dynamic /output/boxlite-shim > /output/dynamic.txt
readelf --version-info /output/boxlite-shim > /output/versions.txt
ldd /output/boxlite-shim > /output/ldd.txt
git diff --exit-code
