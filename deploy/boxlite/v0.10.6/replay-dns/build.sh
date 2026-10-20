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
git config --global --add safe.directory /build/source
git config --global --add safe.directory /build/source/src/deps/libkrun-sys/vendor/libkrun
export RUSTUP_TOOLCHAIN=1.90.0 CARGO_BUILD_JOBS=3 GOTOOLCHAIN=local
cd /build/source
test "$(git rev-parse HEAD)" = 7873ae4995e171237a3342adbb84000c8ab9965b
test "$(git -C src/deps/libkrun-sys/vendor/libkrun rev-parse HEAD)" = e12b9b3780ffa8df9f3e1797b217d13453479167
python3 - <<'PY'
import hashlib,json,pathlib,subprocess
root=pathlib.Path('/build/source')
receipt=json.loads((root/'tank-replay-dns-patch.json').read_text())
for name,expected in receipt['patched_sha256'].items():
    assert hashlib.sha256((root/name).read_bytes()).hexdigest()==expected,name
changed=subprocess.check_output(['git','diff','--name-only'],text=True).splitlines()
assert sorted(changed)==sorted(['src/deps/libgvproxy-sys/gvproxy-bridge/main.go','src/deps/libgvproxy-sys/gvproxy-bridge/go.mod'])
PY
git submodule status --recursive > /output/submodules.txt
rustc --version --verbose > /output/rustc-version.txt
go version > /output/go-version.txt
dpkg-query -W -f='${Package}=${Version}\n' > /output/build-packages.txt
getconf GNU_LIBC_VERSION > /output/glibc-version.txt
export RUSTFLAGS='-C target-feature=-crt-static -C relocation-model=static -C link-arg=-Wl,-z,stack-size=2097152 -C link-arg=-Wl,--allow-multiple-definition'
target_key=${target^^}
target_key=${target_key//-/_}
export "CARGO_TARGET_${target_key}_RUSTFLAGS=$RUSTFLAGS"
# Upstream libkrun invokes Cargo recursively. A global CARGO_TARGET_DIR would
# make that child wait for its parent lock, so scope the cache flag to the
# outer invocation rather than exporting it into nested build scripts.
unset CARGO_TARGET_DIR
cargo build --locked --release -p boxlite-shim --target "$target" --target-dir /cache/target
install -m 755 "/cache/target/$target/release/boxlite-shim" /output/boxlite-shim
(cd /output && sha256sum boxlite-shim > SHA256SUMS)
file /output/boxlite-shim > /output/file.txt
readelf --program-headers /output/boxlite-shim > /output/program-headers.txt
readelf --dynamic /output/boxlite-shim > /output/dynamic.txt
readelf --version-info /output/boxlite-shim > /output/versions.txt
ldd /output/boxlite-shim > /output/ldd.txt
