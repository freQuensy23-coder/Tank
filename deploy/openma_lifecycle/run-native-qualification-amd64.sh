#!/bin/bash
# tank-platform: native BoxLite qualification on amd64 (KVM, state persistence, DNS, replay isolation).
# Runs the prepared lifecycle proof (native-qualification.mjs) and then the network proof
# (native-network-qualification.mjs) in the built OpenMA image, on one dedicated proof volume.
set -u
RUN=${RUN:-amd64-r86}
IMAGE=${IMAGE:-tank-openma:server-r86-amd64}
OCI=${OCI:-/srv/tank-assets/sandbox-oci-amd64}
ASSETS=${ASSETS:-/srv/tank-work/wt/tank-platform/deploy/openma_lifecycle}
EVIDENCE=${EVIDENCE:-/srv/tank-work/wt/tank-platform/docs/verification/native-qualification-amd64-2026-10-20}
VOLUME=tank-native-qualification-$RUN
LIFECYCLE=tank-native-lifecycle-$RUN
NETWORK=tank-native-network-$RUN
DB_IP=$(docker inspect -f '{{(index .NetworkSettings.Networks "tank_tank-net").IPAddress}}' tank-postgres-1)
QUAL_IP=${QUAL_IP:-172.30.60.10}
mkdir -p "$EVIDENCE"
exec > >(tee -a "$EVIDENCE/run.log") 2>&1
echo "start $(date -u +%FT%TZ) run=$RUN"

image_id=$(docker image inspect --format '{{.Id}}' "$IMAGE")
identity=$(docker run --rm --network none --entrypoint node "$image_id" \
  -e 'process.stdout.write(JSON.parse(require("node:fs").readFileSync("/app/tank-overlay.json","utf8")).retained_lifecycle.runtime_identity)')
kvm_gid=$(stat -c %g /dev/kvm)
{
  echo "{\"image\": \"$IMAGE\", \"image_id\": \"$image_id\", \"runtime_identity\": \"$identity\","
  echo " \"kvm_device\": \"$(stat -c '%A %U:%G %t:%T' /dev/kvm)\", \"kvm_gid\": $kvm_gid,"
  echo " \"host_kernel\": \"$(uname -r)\", \"host_machine\": \"$(uname -m)\","
  echo " \"host_cpu_virt\": \"$(grep -o -m1 -w 'vmx\|svm' /proc/cpuinfo)\", \"kvm_module\": \"$(lsmod | awk '$1 ~ /^kvm_(intel|amd)$/ {print $1}')\","
  echo " \"oci_index_sha256\": \"$(sha256sum "$OCI/index.json" | cut -d' ' -f1)\","
  echo " \"shim_sha256\": \"$(docker run --rm --network none --entrypoint sha256sum "$image_id" /opt/tank-boxlite-runtime/boxlite-shim | cut -d' ' -f1)\","
  echo " \"database_ip\": \"$DB_IP\", \"qualification_ip\": \"$QUAL_IP\"}"
} > "$EVIDENCE/inputs.json"
cat "$EVIDENCE/inputs.json"

if docker volume inspect "$VOLUME" >/dev/null 2>&1; then echo "volume $VOLUME exists; choose a new RUN"; exit 2; fi
docker volume create "$VOLUME" >/dev/null
docker run --rm --network none --user 0 --mount "type=volume,src=$VOLUME,dst=/proof" \
  --entrypoint sh "$image_id" -c 'chown 1000:1000 /proof'

echo "lifecycle start $(date -u +%T)"
docker run --name "$LIFECYCLE" --network none \
  --user 1000:1000 --group-add "$kvm_gid" --device /dev/kvm:/dev/kvm \
  --cpus 2 --memory 4g \
  --mount "type=volume,src=$VOLUME,dst=/proof" \
  --mount "type=bind,src=$OCI,dst=/opt/tank-sandbox-oci,readonly" \
  --mount "type=bind,src=$ASSETS,dst=/audit,readonly" \
  -e TANK_NATIVE_MEMORY_MIB=2048 \
  -e TANK_NATIVE_LIFECYCLE_GO=AUTHORIZED_NATIVE_PROOF \
  -e TANK_NATIVE_PROOF_DIR=/proof/native-lifecycle \
  -e BOXLITE_HOME=/proof/native-lifecycle/boxlite \
  -e TANK_NATIVE_ROOTFS=/opt/tank-sandbox-oci \
  -e "TANK_NATIVE_EXPECT_IDENTITY=$identity" \
  -e "TANK_NATIVE_IMAGE_ID=$image_id" \
  --entrypoint /app/apps/main-node/node_modules/.bin/tsx \
  "$image_id" /audit/native-qualification.mjs
lifecycle_rc=$?
echo "lifecycle rc=$lifecycle_rc $(date -u +%T)"
mkdir -p "$EVIDENCE/lifecycle"
for f in result.json run.json phase-first.json phase-resume.json phase-cleanup.json first.log resume.log cleanup.log \
  browser-first.html browser-first.png browser-resume.html browser-resume.png browser-first-freshness.json browser-resume-freshness.json; do
  docker cp "$LIFECYCLE:/proof/native-lifecycle/$f" "$EVIDENCE/lifecycle/$f" 2>/dev/null
done
docker cp "$LIFECYCLE:/proof/native-lifecycle/" "$EVIDENCE/lifecycle-tree-tmp" >/dev/null 2>&1 && {
  find "$EVIDENCE/lifecycle-tree-tmp" \( -name 'progress-*.json' -o -name 'preparation-*-timeout.json' -o -name 'browser-*-failure.json' \
    -o -name 'browser-*-phase.json' -o -name 'browser-*-observer.log' \) -exec cp {} "$EVIDENCE/lifecycle/" \;
  rm -rf "$EVIDENCE/lifecycle-tree-tmp"; }
docker rm "$LIFECYCLE" >/dev/null
df -B1 / | tail -1 > "$EVIDENCE/disk-after-lifecycle.txt"
docker run --rm --network none --mount "type=volume,src=$VOLUME,dst=/proof,readonly" --entrypoint du "$image_id" -sb /proof >> "$EVIDENCE/disk-after-lifecycle.txt"

echo "network start $(date -u +%T)"
docker run --name "$NETWORK" --network tank_tank-net --ip "$QUAL_IP" \
  --user 1000:1000 --group-add "$kvm_gid" --device /dev/kvm:/dev/kvm \
  --cpus 2 --memory 4g \
  --mount "type=volume,src=$VOLUME,dst=/proof" \
  --mount "type=bind,src=$OCI,dst=/opt/tank-sandbox-oci,readonly" \
  --mount "type=bind,src=$ASSETS,dst=/audit,readonly" \
  --mount "type=bind,src=/etc/tank/secrets/tank_agent_db_password,dst=/run/secrets/tank_agent_db_password,readonly" \
  --mount "type=bind,src=/etc/tank/secrets/tank_replay_agent_db_password,dst=/run/secrets/tank_replay_agent_db_password,readonly" \
  -e TANK_NATIVE_NETWORK_GO=AUTHORIZED_NATIVE_PROOF \
  -e TANK_NATIVE_PROOF_DIR=/proof/native-network \
  -e BOXLITE_HOME=/proof/native-lifecycle/boxlite \
  -e TANK_NATIVE_ROOTFS=/opt/tank-sandbox-oci \
  -e "TANK_NATIVE_DB_IPV4=$DB_IP" \
  -e "TANK_NATIVE_IMAGE_ID=$image_id" \
  --entrypoint /app/apps/main-node/node_modules/.bin/tsx \
  "$image_id" /audit/native-network-qualification.mjs
network_rc=$?
echo "network rc=$network_rc $(date -u +%T)"
mkdir -p "$EVIDENCE/network"
for f in result.json unrestricted-before.json replay.json unrestricted-after.json; do
  docker cp "$NETWORK:/proof/native-network/$f" "$EVIDENCE/network/$f" 2>/dev/null
done
docker rm "$NETWORK" >/dev/null
echo "{\"lifecycle_rc\": $lifecycle_rc, \"network_rc\": $network_rc, \"finished\": \"$(date -u +%FT%TZ)\"}" > "$EVIDENCE/exit-codes.json"
echo "done lifecycle=$lifecycle_rc network=$network_rc"
