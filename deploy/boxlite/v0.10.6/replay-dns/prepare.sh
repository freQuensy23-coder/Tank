#!/bin/bash
set -euo pipefail
git config --global --add safe.directory /build/source
git config --global --add safe.directory /build/source/src/deps/libkrun-sys/vendor/libkrun
export GOTOOLCHAIN=local
test "$(git rev-parse HEAD)" = 7873ae4995e171237a3342adbb84000c8ab9965b
test "$(git -C src/deps/libkrun-sys/vendor/libkrun rev-parse HEAD)" = e12b9b3780ffa8df9f3e1797b217d13453479167
git diff --exit-code
bridge=/build/source/src/deps/libgvproxy-sys/gvproxy-bridge
cd "$bridge"
go version > /output/go-version.txt
go mod download -json github.com/containers/gvisor-tap-vsock@v0.8.7 > /output/module-download.json
python3 - <<'PY'
import json, pathlib, shutil
data=json.loads(pathlib.Path('/output/module-download.json').read_text())
assert data['Path']=='github.com/containers/gvisor-tap-vsock'
assert data['Version']=='v0.8.7'
assert data['Sum']=='h1:mFMMU5CIXO9sbtsgECc90loUHx15km3AN6Zuhg3X4qM='
source=pathlib.Path(data['Dir'])
destination=pathlib.Path('tank-vendor/gvisor-tap-vsock')
assert not destination.exists() and not destination.is_symlink()
shutil.copytree(source,destination)
for path in destination.rglob('*'):
    if path.is_file(): path.chmod(path.stat().st_mode | 0o200)
PY
python3 /recipe/apply_patch.py /build/source > /output/patch-receipt.json
go mod download
# Warm both module graphs. Tests are executed separately without networking.
(cd tank-vendor/gvisor-tap-vsock && go mod download)
git diff --stat > /output/source-diff-stat.txt
git diff --binary --abbrev=8 > /output/source.patch
