#!/bin/bash
set -euo pipefail
export GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOFLAGS=-mod=mod
cd /build/source/src/deps/libgvproxy-sys/gvproxy-bridge
go test -count=1 -v -run '^TestTankReplay' ./... > /output/bridge-tests.log 2>&1
cat /output/bridge-tests.log
cd tank-vendor/gvisor-tap-vsock
go test -count=1 -v -run '^TestTankReplay' ./pkg/services/dns > /output/dns-tests.log 2>&1
cat /output/dns-tests.log
