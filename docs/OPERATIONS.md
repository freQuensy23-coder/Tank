# Tank operations — Linux Compose / ChatGPT subscription

Production runs five Compose services on openclaw-dp: `tank`, `tank-capture` (exchange capture, [capture/README.md](../capture/README.md)), `openma`, PostgreSQL16 `postgres` and `clickhouse`. Tank includes its Codex OAuth gateway in the same process on internal port8790; no service publishes a host port. Model requests use the operator's ChatGPT subscription, not an OpenAI API key. Token and spending caps and budget notices are outside V0; usage remains recorded. No actual sign-in or private browser state is needed for offline builds and synthetic tests.

## Deployment

There is one way to deploy: the **Deploy** workflow (`.github/workflows/deploy.yml`), started by hand on `main` with an optional commit SHA of `main` (default: its head). It fails for any other branch or for a commit that is not on `main`.

1. CI (`ci.yml`) has already built and pushed every image of that commit to GHCR as `ghcr.io/frequensy23-coder/tank/<service>:<sha>`: `tank`, `tank-capture`, `tank-openma` (with the sandbox OCI layout baked in), `tank-postgres` and `tank-clickhouse`. The images contain all code, config, migrations, DDL, OpenMA agent definitions, skills and the PostgreSQL HBA rules. Nothing is built, generated or copied on the server.
2. The workflow waits for that commit's CI run to succeed, copies `compose.yaml` and `scripts/deploy/deploy.sh` to `/srv/tank-deploy/` on the host and runs `deploy.sh <sha>` there.
3. `deploy.sh` pulls the images and recreates `postgres`, `clickhouse` and `openma` only when the image label `org.tank.content` (a hash of the files that make that image) differs from the running container's. It starts a new `tank-capture` next to the old one, waits for `/readyz` and stops the old one 20 seconds into a minute, so no minute of prints is lost. Then it recreates `tank`.
4. On start, `tank serve` applies PostgreSQL migrations, the agent read-only grants and the ClickHouse DDL, then syncs OpenMA in the background: skills and agents from `config/openma.yaml` and `config/skills/` get a new version only when their files changed.
5. `deploy.sh` checks the live system for up to 8 minutes: one healthy capture, Tank running, fresh bars, prints, OI and funding, no incomplete BTC minute, exchange sources polled. If the checks do not pass, it deploys the previous commit (recorded in `/srv/tank-deploy/deployed`) the same way.

A config change is a commit and a deploy. There is no SIGHUP reload, mounted config or host-side preparation step.

## Configuration and secrets

All nonsecret settings are in the repository: `config/*.yaml`, the image `ENV` lines and `compose.yaml`. The host keeps exactly one file, `/srv/tank-deploy/.env` (mode0600, owner root), with the secrets and the few host-specific values. `.env.example` lists every variable; Compose refuses to start if a required one is missing. Never put secret values in source control, build arguments, image layers, command arguments or logs.

- Secrets: `POSTGRES_PASSWORD`, `TANK_DB_PASSWORD`, `OMA_DB_PASSWORD`, `TANK_AGENT_DB_PASSWORD`, `TANK_REPLAY_AGENT_DB_PASSWORD`, `OMA_API_KEY`, `OMA_ROOT_SECRET`, `OMA_AUTH_SECRET`, `OPENROUTER_API_KEY`, `OPENCODE_API_KEY`, `TELEGRAM_BOT_TOKEN`, `ETHERSCAN_API_KEY`, `CODEX_GATEWAY_KEY`, `X_SESSION`. Root, auth and internal keys are distinct random values.
- Host values: `TANK_X_ENABLED`, `KVM_GID` (`stat -c %g /dev/kvm`), `TANK_SANDBOX_CPUS`, `TANK_SANDBOX_MEMORY_MIB`, `TANK_BACKUP_PATH`.

Compose passes each service only the variables it needs: OpenMA receives neither Telegram/X secrets nor ChatGPT OAuth state, and its Model Card stores only the internal gateway key. Data lives in named volumes: `tank-data` at `/data`, `tank-browser` at `/browser`, `codex-auth` at `/codex-auth`, `pg-data`, `clickhouse-data`, `openma-data` and `sealed-replay-evidence`; `TANK_BACKUP_PATH` is mounted at `/backup`.

`X_SESSION` is the JSON text of a Playwright storage-state export made on the operator's computer. Tank imports validated X/twitter cookies/origins once into `/browser`, records a nonsecret seed marker, and never silently overwrites a working profile. To reseed after explicit operator review, stop Tank and remove the marker/reset the dedicated profile before supplying the new state. Missing X state does not prevent public S19 rendering, but X is unavailable. The private List ID is `list_id` of S29 in `config/sources.yaml`.

The operator obtains `auth.json` with `codex login` on their own computer and copies it once into the `codex-auth` volume as `/codex-auth/auth.json`, owned UID1000 and mode0600. The gateway refreshes its OAuth tokens in that volume. This is the only ChatGPT sign-in artifact; no API-key fallback is configured. Offline tests use entirely synthetic tokens and never inspect personal auth files.

The config version recorded with every output is the image's git commit (`GIT_SHA` build argument, a full 40-hex SHA set by CI). A local build without it uses `dev.<hash of the config files>`.

## Databases and OpenMA

The `tank-postgres` image initializes an empty volume with the `tank`, `oma`, `tank_agent` and `tank_replay_agent` roles and the `tank`, `oma` and `tank_replay` databases, using the passwords from `.env`. Its HBA file is part of the image: `tank` reaches tank/tank_replay and `oma` reaches oma from the Compose subnet `172.30.60.0/24`; `tank_agent` reaches only tank and `tank_replay_agent` only tank_replay, both only from OpenMA's fixed address `172.30.60.10`, where sandbox traffic leaves the host network. All other connections are rejected. Both agent roles default to read-only transactions; Tank's migrations grant them SELECT on its schemas, including future tables. Local administrator access uses peer authentication: `docker exec -it -u postgres tank-postgres-1 psql -d tank`.

The `tank-clickhouse` image creates the `tank` user and the read-only sandbox user `tank_agent` from `.env`; Tank creates the `mkt` tables on start. Sandbox sessions query ClickHouse read-only; INSERT is refused.

OpenMA uses `/v1/oma/model_cards` with custom provider `tank-codex`, `pi_config.api=openai-responses`, base URL `http://tank:8790/v1` and the internal key `CODEX_GATEWAY_KEY`. The custom provider avoids the upstream legacy creation probe that uses Chat Completions instead of Responses. The analysis model's per-token cost is zero; Jev paid usage remains separately audited. Agent and environment IDs are fixed in `config/openma.yaml`; previous resource IDs stay valid for historical sessions. Agent skill ZIPs include the full data guide, schemas and source evidence. Built-in web fetch/search and scheduling are disabled so evidence and time pass through Tank's tools. Web search uses ddgs9.16.0 with DuckDuckGo only, no API key or paid fallback; it is rate-limited to at least2seconds, with fixed retries5/15/45seconds. The actual pinned event SSE route ignores Last-Event-ID; Tank reconciles stored event history and deduplicates before following the stream.

The `tank_replay` database must be populated with reviewed fixture data/migrations before replay; an empty database is not a replay packet. Its environment is restricted to the database host only, not public internet, and receives no external API secrets. Replay credentials cannot connect to tank, and production agent credentials cannot connect to tank_replay. A KVM test must prove database SELECT works and arbitrary internet access fails; merely saving an environment JSON is insufficient proof of enforcement.

The replay evidence integration uses one dedicated `sealed-replay-evidence` volume: Tank writes prepared run artifacts under `/data/replay/evidence`; the trusted OpenMA controller reads that volume at `/app/replay-evidence`, and each replay guest receives only its validated run subdirectory at `/replay/evidence`, read-only. The normal replay CLI publishes only the first-message prefix before opening its fresh session, then advances the stable guest directory with ReplayClock before later messages/tools. Immutable full inputs and prefix generations stay outside the controller volume. Each advance awaits native manifest-hash/cutoff metadata update before dispatch; a transient mismatch fails closed. The same session/workspace is retained, and mutable prefix hashes are not static allocation fingerprint fields. Native metadata `tank_replay_run_id`, `tank_replay_evidence_sha256` and `tank_replay_cutoff` must match the exact manifest bytes and cutoff. The adapter verifies contained regular files and refuses symlinks, extra payloads or mismatched hashes. Original live files, evaluator outcomes, other run children and host-only audit receipts are not guest mounts.

## Start, checks and recovery

Before the first Tank service startup, run the offline `tank calibrate` workflow below and review/apply both measured Jev cutoffs. Then deploy. `tank-capture` holds every exchange stream and REST request; `serve` reads the market tables it writes. Record actual coverage and interruptions for acceptance.

Before starting `tank serve`, complete calibration, validate the configured source and instrument identities, and discover live instrument sizes. Exchange fields are interpreted by `semantics.yaml` immediately; there is no runtime meaning self-test or semantic-confidence gate. Missing data, incomplete coverage and stale observations still limit Data Confidence. Exact DefiLlama project IDs and metric availability are recorded in `config/fundamentals_evidence.json`; parent rollups are consumed once, and World Chain ETH gas is not WLD capture.

After a deploy, inspect `docker ps`, `docker logs --tail 100 tank-tank-1` and `docker exec tank-tank-1 tank doctor`. Restart recovers durable jobs, hot tokens and sessions without duplicate card publication. Check no usage/spending cap or budget-pause path remains. Actual subscription rate limits are propagated with provider reset information and no inferred retries before reset.

Backups use the external `/backup` volume and `tank backup --force` (the wrapper `deploy/backup.sh` invokes the same command). Recovery uses the Tank and OpenMA `pg_dump` files plus the recorded checksummed bundles and their manifest. Restore with `pg_restore` into fresh databases, verify the bundle hashes and durable records, and keep delivery disabled until duplicate-delivery state is verified. Canonical skill version archives are stored in OpenMA PostgreSQL and are included in its dump; do not confuse them with the legacy filesystem skill API. The [r57 restore exercise](verification/backup-world-r57-2026-10-20/restore-result.json) compared all 29 archives through the restored API and verified Model Card decryption with the preserved root secret. Archive-cache retention runs daily independently of backup success; PostgreSQL and completed-job retention run only after a verified backup. ClickHouse drops whole daily partitions: prints after 30 days, book summaries after 72 hours, the other series after 90 days; days overlapping a MAJOR+ event window (except book summaries) and bundle-referenced evidence remain permanent. Archive caches expire after 180 days and need no backup copy. A manual backup prunes only with `--prune`. After each verified nightly backup the oldest complete `tank-YYYY-MM-DD` snapshots beyond `backup_snapshots_kept` are removed (`tank.backup.prune_backup_snapshots`; lead ruling: 3 while the backup shares the production disk, 7 on its own disk). Protect the OpenMA root secrets separately: the database alone cannot recover encrypted Model Cards.

No build or synthetic test proves operational acceptance. Separate evidence is required for source failure/latency targets over72hours, all venue metric semantics and archive continuity for5tokens×7days, live Linux KVM replay isolation, ChatGPT-authenticated model/SSE and refresh behavior, Telegram threading/retry, and14-day shadow review. Current F-16 expects STRESS under the unchanged normal-range context rule; the earlier HEADWIND conflict is obsolete in the latest Technical Design.

## Snapshot time and authority

One market cutoff governs exchange/settlement times and completed-bar bucket ends. `received_ts` is collection latency and may be later than cutoff for historical backfills. `assembled_at` records availability: exclude rows received after snapshot assembly and semantic market points after cutoff. Official account authority is scoped to claim subject; `@sama` has documented World and OpenAI roles, not global trust.

## Source reconciliation

S08 reads DTCC's official `/press-releases` HTML, discovers matching article links and fetches each article; it does not extract a Coveo query token from JavaScript. A loaded page with zero article-pattern matches marks the feed/source DEGRADED rather than healthy. Robinhood S13 follows its canonical `robinhood.com/newsroom` host and `__NEXT_DATA__` canonicalUrl cards, accepting `/us/en/newsroom/` paths. S05 retains its official feed after a transient404 rechecked as valid RSS. OKX uses port443 and seqId/prevSeqId integrity; checksum was deprecated23June2026 and fixed0. These corrections retain original source identity and have recorded evidence.

## V0-B calibration workflow

User decision 2026-10-20: there is no offline calibration before launch. `filter_cutoff` and `routing_cutoff` stay 0.5 (the uncalibrated midpoint) until Tank has collected real news to label (`/srv/tank/todo.md`).

To calibrate later, run `docker exec tank-tank-1 tank calibrate packet.json --output-dir runs/<name>`.
- It calls only Jev's System One endpoint through the service's Pipeline, with the same state, questions and candidate builder, at each item's historical clock. It starts no service.
- The packet format, the selection rules and the V0-B checks are in [OFFLINE_CALIBRATION.md](OFFLINE_CALIBRATION.md). The rules: the filter cutoff is the highest one that keeps ≥95% of relevant items; the routing cutoff is the highest one with precision and recall ≥0.90.
- A run writes a report and `thresholds.proposed.yaml`. Cutoffs reach `config/thresholds.yaml` only as a recorded D6 checkpoint.

## Vendored OpenMA compatibility

Actual PostgreSQL16 integration exposed upstream defects despite a healthy HTTP endpoint. The vendored SQL adapter casts ten nullable SQL parameters to VARCHAR191 so PostgreSQL can infer parameter types, and normalizes bigint epoch strings at the SQL timestamp adapter boundary (thirteen substitutions across twelve files). Canonical agent tool codecs are extended consistently through schema/domain/mapper/resolver for browser and disabled scheduling names already supported by the pi harness. Register through canonical `/v1/agents`; the older `/v1/oma/agents` store is distinct and must not supply canonical session IDs. Skill creation uses canonical multipart `files[]` and the required `managed-agents-2026-04-01,skills-2025-10-02` beta flags. Model Cards use `/v1/oma/model_cards` with POST for create/update; list-before-update avoids the pinned duplicate-create500 defect.

The vendored browser adapter binds both ordinary and confirmed tool execution to the exact session SandboxPort. Python Playwright/Chromium and the Unix-socket worker run inside that VM; profile data stays in its checkpointed workspace. End-of-turn cleanup flushes the profile before checkpoint creation. See [OPENMA_BROWSER.md](OPENMA_BROWSER.md) for reproducible local Chromium/transport tests and the separate KVM acceptance requirement. The sandbox image includes Playwright 1.64.0 and its matching Chromium.

Compose builds OpenMA with `SKIP_CONSOLE=1`; Tank uses its internal API and publishes no console port. The pinned upstream console's default build currently fails resolving `../data/mcp-registry`; the API image builds using the explicitly configured skip flag. This does not certify any console workflow.

Delivery chat IDs come only from the versioned `config/delivery.yaml`. The User-Agent `TankV0/1.0 (<e-mail>)` (HLD §3) is built only from `contact_email` in `config/sources.yaml` (TDD §13). If `TANK_USER_AGENT`, `TANK_TELEGRAM_ALERTS_CHAT` or `TANK_TELEGRAM_OPS_CHAT` is set in the environment, Tank ignores it and logs a warning naming it. `tank doctor` checks the User-Agent format and the S29 `list_id` in `sources.yaml`. The scout receives its reviewer skill and three custom tools only, with all native defaults disabled; the analyst receives the analyst and data skills.

Compose fixes the subnet `172.30.60.0/24` and OpenMA's address `172.30.60.10`; the HBA rules in the `tank-postgres` image depend on both. Changing either is a code change to `compose.yaml` and `deploy/postgres/pg_hba.conf` in the same commit, followed by a repeat of the role-scoped checks from an actual sandbox: SELECT succeeds, INSERT and cross-database connections are refused. Evidence of earlier probes: [postgres-network-isolation.json](verification/postgres-network-isolation.json).

## Jev through OpenCode

User decision 2026-10-20: Jev runs through OpenCode's System One endpoint (`https://opencode.ai/inference/systemone/v1/systemone`).
- With `OPENCODE_API_KEY` set in `.env`, Tank uses model `jev-1.13`. Without it, Tank uses `jev-1.13-free`.
- OpenRouter and its key are no longer used by the Tank service.
- `tank doctor` reports whether the OpenCode key is present.
- Never put the key in source control, build arguments or images.

The analyst and scout models keep using the Codex subscription gateway.


## Source progress in Tank Ops

Tank Ops receives every newly ingested unique material before relevance filtering. Repeated captures of the same source identity/content retain ingestion deduplication. There is no bulk replay of historical materials. Agent-fetched pages and search evidence are also reported as additional sources, explicitly without a Jev requirement.

`ITEM_PROGRESS` notices include the source, title, a bounded text excerpt, source URL, item/event identifiers, the stage timestamp, Jev decision/score and the outcome or reason. Long source text and unusually long URLs are truncated to keep messages inside Telegram's UTF-16 length limit. These notices do not change the publication gates in Tank Alerts.

The recorded stages cover receipt, Jev pass/rejection or policy bypass, waiting for agent admission, accepted agent input, registered events and added observations, agent rejection, significance assessment, publication refusal, queued cards, confirmed Telegram delivery, terminal delivery failure, completed agent turns, and failed/expired processing jobs. Agent acceptance and extraction do not imply that a card passed publication or reached Alerts. A completed native turn without an observation is shown explicitly. Below-MAJOR assessments remain visible even when the agent never attempts publication.

Migration `003_ops_progress.sql` extends the existing notice-kind constraint without modifying historical notices. Notices use the existing durable, append-only Ops outbox and delivery ledger. Each distinct stage/outcome is idempotent; a replay of the same queue task or agent completion does not create the same notice again. Processing failure reconciliation and agent recovery restore missed terminal/acceptance notifications after a restart. Ingestion, filter/routing decisions and dispatched event-tool changes write their notice in the same transaction as their state change.

Progress deliveries use priority 3, below Alerts and analysis jobs, with a seven-day delivery window and the existing five-attempt transport policy. A shared repository lock spaces successful progress messages at least 3.1 seconds apart; this intentional deferral does not consume a network attempt. Heavy input can therefore create an Ops backlog. Heartbeat and delivery-failure notices retain their existing behavior. A failed Ops message never recursively creates another Ops failure notice.

`TANK_OPS_PROGRESS_ENABLED` defaults to `true`; set it to `false` to disable creation of new progress notices. Already queued notices still drain. Isolated replay runs suppress progress notices. No extra model calls are made for Ops reporting, and raw payloads, model transcripts and exception contents are not sent.
