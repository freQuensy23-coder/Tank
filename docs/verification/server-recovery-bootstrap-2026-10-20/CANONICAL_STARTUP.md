# Canonical server startup — prepared, not executed

Only `openclaw-dp`; current checkout `/srv/tank`. Runtime, OpenMA and models remain OFF until parent GO. PostgreSQL recovery/migrations are owned by storage; never run a second migration concurrently.

## Exact Compose invocation

```sh
cd /srv/tank
dc() {
  docker compose --env-file /etc/tank/compose.env -p tank \
    -f /srv/tank/compose.yaml \
    -f /etc/tank/compose.server.yaml \
    -f /srv/tank/docs/verification/server-recovery-bootstrap-2026-10-20/pg.resources.yaml "$@"
}
dc config --quiet
```

Always retain the final PostgreSQL resource override (2CPU/2GiB); its removal could recreate PG with different limits. Server overlay pins PG `postgres@sha256:0ea6700a3b4f0ae6ce746519073558aed4d88a79d8d07622a9a644946c7319c4` and final Tank `sha256:1c828c59acad147aa14f4f62c9c3932f4d25442721077f33c5a6089b0b7a9c9a`, pull never. OpenMA image pin is still absent: do NOT start the old base image. Wait for the actual verified amd64 OpenMA/runtime/OCI and parent pin approval.

## Dependency gates

1. Storage verifies restore and migrations030–035, unchanged retained evidence/history, read-only role grants for each DB, and separate fresh `tank_replay`. No dump into replay; no reset/deletion or parallel DDL.
2. `/etc/tank/secrets` permissions are verified against actual PG999 and Tank1000; auth volume `tank_codex-auth:/codex-auth` contains only `auth.json`, UID1000/0600, copied from the preexisting server auth without refresh. Original `/root/.codex/auth.json` is untouched. Receipt `data/qa/server-transfer-20261020/codex-auth-volume.json`. No whole `.codex` mount.
3. Source/market warming can use the separately root-authorized MarketService component with versioned public config; it must not dispatch agent jobs, OpenMA, gateway, delivery or models. Canonical config has22 registry tokens and current semantics; its old resource IDs are historical, not ready server bindings.
4. Actual amd64 runtime/OCI, guest resource tuple, KVM and controller identity must pass their own verification. Parent inserts the exact OpenMA image into `/etc/tank/compose.server.yaml`, retaining all mounts/resource overrides. Only then may root authorize `dc up -d --no-deps --no-build --pull never openma`.
5. Read actual restored OpenMA model/skills/agents/environments and verify root-secret decryption; do not inject absent host IDs into SQL. Use supported registration (commands below), exact readback and new config pins. Registration creates resources, not sessions.
6. Observe actual SDK guest PostgreSQL source address before adding only `tank_agent`→`tank` and `tank_replay_agent`→`tank_replay` /32 HBA rows; reload PG, no restart. Bootstrap currently denies native roles. Host-admin /32 is separate and only permitted after inspecting actual gateway172.30.60.1. Prove SELECT, privilege-based zero-row UPDATE refusal, cross-DB refusal and replay network isolation; no broad subnet or internet exception.
7. Before any `tank serve`, storage+workflow inventory every restored nonterminal job, local message/outbox, managed execution/session/work/resource/fence and required file path. Classify preserved vs missing original workspaces and snapshot-time gaps. Do NOT blindly resume a missing old native disk, reset attempts/statuses, fabricate resource IDs, drop history or replay ambiguous DELIVER jobs. Parent explicitly adjudicates safe existing-work recovery through supported APIs/idempotent jobs, with receipts. Preserve unresolved jobs/receipts while held; a source/market warmer is not a full Runtime substitute.
8. Parent separately authorizes final Runtime/models/Telegram E2E after config/resource/backlog/native gates. No command in this plan was an execution GO.

## Supported registration and exact pins — future GO only

```sh
dc run --rm --no-deps tank /opt/tank/venv/bin/python \
  /app/deploy/prepare_openma.py --config /config \
  --output /data/deployment/server-context10 --apply
```

This normal helper uses canonical `/v1/skills` multipart uploads, `/v1/agents`, `/v1/environments` and model-card API with secrets read only from mounts. Read back agent custom tools/schema and model/system/native config, exact analyst20+data2+scout3 files from `/v1/skills/<id>/versions/<version>/content` with both beta flags, and environment policy/packages. Match local context10 sources and registered content hashes; no automatic model dispatch.

Only after exact readback replace server `config/openma.yaml` fields `agent_id`, `scout_agent_id`, `environment_id`, `replay_environment_id`; set `prompt_version` to the registered context10, `openma_overlay` to the actual generated amd64 composition identity and preserve approved model/provider settings. Skill IDs/versions live in the agent bindings and receipt. Model-card IDs, versions and returned resource IDs must come from this server, not historical local readbacks. Keep upstream `release` unchanged unless a separately reviewed source pin changes. Regenerate `config/.tank-config-version.json` through `deploy/prepare_config.py`; final code/config versions must be read inside the actual image. The helper proposed YAML is a review artifact, not permission to overwrite unrelated settings.

```sh
# After reviewing actual returned IDs and copied proposal on server:
/srv/tank/.venv/bin/python /srv/tank/deploy/prepare_config.py /srv/tank/config
```

No registration, native observation, service start, model call or backlog mutation has been executed by this preparation.
