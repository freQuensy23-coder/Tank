# Build on a clean Linux amd64 or arm64 KVM host

Use the deployment host's architecture throughout. This procedure does not edit
source hashes or register a developer-approved binary. The operator builds the
pinned strict-DNS source locally; preparation verifies its official runtime
files, source/patch/toolchain contracts, and exact successful build/offline-test
receipts. Hardware qualification stays `NOT_RUN` until actual native tests run.
Build provenance is an operator-controlled local record, not a third-party signed
attestation. Do not treat an untrusted downloaded candidate as a local build.

The host needs Docker Engine/buildx, Compose, `/dev/kvm`, and the capacity in HLD§8.
Keep at least 8 GiB free during compilation. The commands below use the local
Docker Unix socket and run Python/Git in the pinned builder, so no host Python,
Rust, Go or Git installation is required. Run from the checked-out Tank repository.
Use a fresh build directory; do not put credentials or a live native HOME there.

```sh
task_arch=amd64                       # arm64 on an ARM Linux host
task_deploy="$PWD/deploy"
task_recipe="$task_deploy/boxlite/v0.10.6"
task_build="/srv/tank-builds/boxlite-0106-$task_arch"
mkdir -p "$task_build/output"

docker buildx build --platform "linux/$task_arch" --target builder --load \
  -f "$task_recipe/Dockerfile" -t "tank-boxlite-builder:0106-$task_arch" "$task_recipe"
task_builder=$(docker image inspect --format '{{.Id}}' "tank-boxlite-builder:0106-$task_arch")

# Export the eight official files without compiling an unused unpatched shim.
docker buildx build --platform "linux/$task_arch" --target official-runtime-export \
  -f "$task_recipe/Dockerfile" --output "type=local,dest=$task_build/official" "$task_recipe"

docker run --rm --mount "type=bind,src=$task_build,dst=/build" \
  --entrypoint bash "$task_builder" -c '
  git init /build/source
  git -C /build/source remote add origin https://github.com/boxlite-ai/boxlite.git
  git -C /build/source fetch --depth 1 origin 7873ae4995e171237a3342adbb84000c8ab9965b
  git -C /build/source checkout --detach FETCH_HEAD
  git -C /build/source submodule update --init --recursive'

# The build supervisor alone gets the local Docker CLI/socket. No KVM, service
# volumes, auth files or operator Docker configuration is mounted in it.
# Bind paths are identical inside/outside because child containers use them.
task_docker=$(command -v docker)
monitor_phase() {
  docker run --rm --network none \
    --mount "type=bind,src=$task_docker,dst=/usr/local/bin/docker,readonly" \
    --mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock \
    --mount "type=bind,src=$task_deploy,dst=$task_deploy,readonly" \
    --mount "type=bind,src=$task_build,dst=$task_build" \
    --entrypoint python3 "$task_builder" \
    "$task_recipe/replay-dns/monitored_build.py" "$task_build" "$@" \
    --architecture "$task_arch" --builder-image "$task_builder"
}
monitor_phase prepare --network default --seconds 600
monitor_phase test --network none --seconds 600
monitor_phase build --network default --seconds 1800

docker run --rm --network none \
  --mount "type=bind,src=$task_deploy,dst=$task_deploy,readonly" \
  --mount "type=bind,src=$task_build,dst=$task_build" \
  --entrypoint python3 "$task_builder" \
  "$task_recipe/replay-dns/package_candidate.py" --build "$task_build" \
  --previous-runtime "$task_build/official/runtime" \
  --destination "$task_build/runtime-bundle"

docker run --rm --network none \
  --mount "type=bind,src=$task_deploy,dst=$task_deploy,readonly" \
  --mount "type=bind,src=$task_build,dst=$task_build,readonly" \
  --entrypoint python3 "$task_builder" \
  "$task_deploy/prepare_boxlite_runtime.py" "$task_build/runtime-bundle" --verify
```

A failed phase preserves its container/log/receipt. Use `--attempt 2` when explicitly
retrying the same phase and pass the successful receipt filenames to the packager
(`--test-receipt test-2-receipt.json`, `--build-receipt build-2-receipt.json`). Do not
replace a failed receipt with a fabricated PASS. Native compilation occurs once,
for the strict-DNS shim. The seven other files must remain the official platform
bytes; source contracts reject an unpatched or differently patched shim build.

The normal OpenMA build uses the committed `vendor/openma` source and native
archive in `vendor/openma/tank-native`. From the repository root, run
`docker compose --env-file "$TANK_COMPOSE_ENV" build openma`; no prepared checkout
or manual runtime export is required.

When intentionally adopting a newly built native artifact, update the versioned
archive, adjacent checksums, source/provenance mapping and matching runtime
identity together. Keep its qualification `NOT_RUN` until the new immutable image
and architecture-matched sandbox OCI pass their own native qualification.

Build the OpenMA image in isolation. Then run the public
[native qualification command](../../openma_lifecycle/NATIVE_QUALIFICATION.md#exact-invocation-template)
with that immutable image, its generated identity and the matching OCI. It uses
an owned proof volume/HOME, no credentials or live service database, and real KVM,
file mounts, browser, stop/resume and cleanup. Preserve the result and exact image,
OCI and artifact identities. That component command does not certify the public
Managed API/history or replay PostgreSQL/network E2E; run those deployed-system
acceptance checks too. No ARM run is counted as x86 evidence. Do not point a newly
prepared identity at the existing primary HOME or retained workspaces.
