# Tank operations — Linux Compose / ChatGPT subscription

The deployment runs exactly three Compose services: `tank`, `openma`, and PostgreSQL16 `postgres`. Tank includes its Codex OAuth gateway in the same process on internal port8790; OpenMA port8787 and PostgreSQL port5432 are never published. Model requests use the operator's ChatGPT subscription, not an OpenAI API key. Token and spending caps and budget notices are outside V0; usage remains recorded. No actual sign-in or private browser state is needed for offline builds and synthetic tests.

## Host and image preparation

Production needs Linux x86-64 or arm64, Docker Engine, Composev2, working `/dev/kvm`,32GB RAM and1TB SSD. Keep host NTP synchronized. Docker Desktop can build and run the control plane here but cannot certify Linux KVM sandbox behavior.

From the repository, build `docker build -t tank-v0:openrouter-live .`. The image uses Python3.12, frozen `uv.lock`, Chromium with system dependencies, and PostgreSQL16 dump/restore clients. Secrets, `.env` files, browser profiles, auth files, generated deployment bundles and local data are excluded from its build context. Tank runs as UID1000. Secret files require the per-consumer ownership/modes below; making every file mode0600 owned only by UID1000 would prevent PostgreSQL initialization from reading its shared password files. Named data/browser/auth volumes initialize under UID1000; prepare the external backup directory with the same ownership.

Clone the official `openma-ai/open-managed-agents` source at commit `85d5361f535c7221de975838dc48444e468cc02e`. Run prepare_openma_source.py inside the Tank image using the Docker-only commands below. The helper refuses a different commit, changed source hashes, or an existing destination, then creates the documented `tank2-openrouter` overlay. Set `OPENMA_SOURCE_PATH` to this prepared directory and build using Compose. The source checkout and overlay have no credentials.

The pinned upstream Dockerfile selected Node22 although its pnpm11 requires Node24; the prepared Dockerfile uses Node24. The overlay also passes each selected environment's network policy into BoxLite and injects only curated sandbox read-only database/API credentials. The original adapter ignored networking and always enabled internet, so an unpatched upstream image is not an acceptable replay runtime. The upstream environment-create API does not accept an image or arbitrary env property. Build `deploy/sandbox.Dockerfile` and export its verified OCI layout as described below. The guarded adapter passes SANDBOX_ROOTFS_PATH to the installed BoxLite0.9.0 SimpleBox rootfsPath option, which overrides image. Docker daemon image tags are not assumed visible to BoxLite. No registry publication or registry credentials are required for this local path.

## Nonsecret configuration and secret files

Copy `deploy/compose.env.example` to a host file outside the repository, fill the nonsecret contact email, list/chat IDs, prepared source path, config path, sandbox image digest, external backup directory and KVM group ID (`stat -c %g /dev/kvm`). Use `docker compose --env-file /path/to/compose.env ...` for subsequent commands. Config is mounted read-only at `/config`; data at `/data`, X profile at `/browser`, mutable gateway sign-in at `/codex-auth`, and external backups at `/backup`. OpenMA persists sandbox/session artifacts in its own `openma-data` volume; PostgreSQL uses `pg-data`.

Create secret files in the directory named `TANK_SECRETS_DIR`, outside this repository. Required service secrets are `oma_api_key`, `oma_root_secret`, `oma_auth_secret`, `openrouter_api_key`, `telegram_bot_token`, `etherscan_api_key`, `tank_agent_db_password`, `tank_replay_agent_db_password`, `x_session`, `codex_gateway_key`; separate `postgres_password`, `tank_db_password` and `oma_db_password` provision least-privilege database roles. Root/auth/internal keys are distinct randomly generated values. Never put them into the Compose env file, source control, build arguments or image layers. Compose grants each service only its needed secret files: OpenMA receives neither Telegram/X secrets nor ChatGPT OAuth state. Its Model Card stores only the internal gateway key.

The `x_session` file is a Playwright storage-state export made on the operator's computer. Tank imports validated X/twitter cookies/origins once into `/browser`, records a nonsecret seed marker, and never silently overwrites a working profile. To reseed after explicit operator review, stop Tank and remove the marker/reset the dedicated profile before supplying the new state. Missing X state does not prevent public S19 rendering, but X is unavailable. Headed Xvfb is an operator choice when headless is blocked; there is no automatic fallback.

The operator obtains `auth.json` with `codex login` on their own computer. Copy it once into the `codex-auth` volume at `/codex-auth/auth.json`, owned UID1000 and mode0600. For example, after providing secret files, run `docker compose ... run --rm --no-deps --entrypoint sh -v /absolute/outside-repo/auth.json:/bootstrap/auth.json:ro tank -c 'umask 077; cp /bootstrap/auth.json /codex-auth/auth.json; chmod 600 /codex-auth/auth.json'`. The source file must be readable by the container UID; the one-shot container receives only this explicit temporary mount. Never place it in OpenMA or source code. The gateway refreshes its OAuth tokens in that volume. This is the only ChatGPT sign-in artifact; no API-key fallback is configured. Offline tests use entirely synthetic tokens and never inspect personal auth files.

Before service startup, run prepare_config.py inside the Tank image with the config and its Git root mounted, using the Docker-only helper pattern below. Its hidden `.tank-config-version.json` records the actual git commit, dirty status and hash of exact versioned files. Runtime validates this hash before using the commit identity, so a read-only `/config` mount without `.git` remains auditable. Regenerate the manifest after every reviewed config update, including applied deployment IDs. Never invent a configuration commit through an environment override.

## Database and OpenMA initialization

Before startup, select an unused private IPv4 Compose CIDR and obtain the VM source CIDR from a controlled BoxLite network probe on the KVM host. Run prepare_network.py inside the Tank image with explicit installation values using the Docker-only command below. The helper rejects public/default/noncanonical subnets; Compose fixes its network to the selected CIDR and mounts this HBA file read-only. Rules admit only tank to tank/tank_replay and oma to oma from the Compose subnet, tank_agent only to tank from the observed VM subnet, and tank_replay_agent only to tank_replay from that VM subnet. All other IPv4/IPv6 connections are rejected. Local administrator access uses peer authentication: `docker compose --env-file /path/to/compose.env exec -u postgres postgres psql -U postgres -d tank`. Do not replace a missing observed VM CIDR with a guessed route or broad allow rule. BoxLite's guest path to Compose `postgres` DNS and its allowed-host enforcement must be verified on the Linux KVM host; do not broaden access to make a check pass.

Run `docker compose ... config --quiet` and inspect service/volume/secret assignments, then `docker compose ... up -d postgres openma`. Initial PostgreSQL setup creates separate tank/oma owners and `tank_agent`; only tank/oma owners can write their own database. No host ports are published. After Tank migrations, apply `deploy/read_only_role.sql` as the database administrator to set the sandbox role default_transaction_read_only and grant schema usage/SELECT plus default privileges for future tables. The script explicitly targets future tables created by the Compose owner role `tank`. Verify SELECT succeeds and INSERT fails from the actual sandbox. Calibrate connection limit and statement timeout at V0-G; unresolved values remain null.

Prepare registration payloads with `docker compose ... run --rm --no-deps tank python /app/deploy/prepare_openma.py --config /config --output /data/deployment`. This is local file work. Inspect `/data/deployment/model-card.json`, toolsets and environments. Add `--apply` only to register them in the running local OpenMA API. It creates no model sessions. The helper uses `/v1/oma/model_cards`, custom provider `tank-codex`, `pi_config.api=openai-responses`, base URL `http://tank:8790/v1`, and internal key read from `codex_gateway_key`. Custom provider avoids the upstream legacy creation probe that uses Chat Completions instead of Responses. The analysis model's per-token cost is zero; Jev paid usage remains separately audited. Web search uses ddgs9.16.0 with DuckDuckGo only, no API key or paid fallback. Search is rate-limited to at least2seconds, with fixed retries5/15/45seconds.

The helper writes `/data/deployment/openma.proposed.yaml` rather than attempting to edit the read-only config mount. Review its deployed agent/environment IDs, copy them into host `config/openma.yaml`, regenerate the config manifest, and retain the previous resource IDs for historical sessions. Agent SKILL ZIPs include the full data guide, schemas and source evidence. Built-in web fetch/search and scheduling are disabled so evidence and time pass through Tank's tools. The actual pinned event SSE route ignores Last-Event-ID; Tank reconciles stored event history and deduplicates before following the stream.

Initial PostgreSQL setup also creates a separate `tank_replay` database owned by tank. Populate it with reviewed fixture data/migrations and apply the read-only grants there before replay; an empty created database is not a replay packet. Its selected environment is restricted to the database host only, not public internet, and receives no external API secrets. The overlay supplies a distinct replay database URL with its own tank_replay_agent read-only role and password. PUBLIC CONNECT is revoked; replay credentials cannot connect to tank, and production agent credentials cannot connect to tank_replay. Apply deploy/read_only_role.sql separately in each database after migrations; it chooses the correct role from current_database(). This extra role enforces cutoff isolation within the existing PostgreSQL service, and does not add a service. A KVM test must prove database SELECT works and arbitrary internet access fails; merely saving an environment JSON is insufficient proof of enforcement.

## Start, checks and recovery

Complete the calibration workflow below, validate source/coverage identity counts22/34/86/181, discover live instrument sizes and run metric meaning self-tests. All initial semantic entries are DOCUMENTED_UNTESTED; only successful current-run adapter tests may promote the checked metric to VERIFIED with evidence. Failed checks become UNKNOWN. Missing values remain null with reasons. Exact DefiLlama project IDs and metric availability are recorded in `config/fundamentals_evidence.json`; parent rollups are consumed once, and World Chain ETH gas is not WLD capture.

Then start Tank with `docker compose ... up -d tank`, inspect `docker compose ... ps`, `logs --tail 100 tank openma postgres`, and `docker compose ... exec tank tank doctor`. Send SIGHUP after a reviewed config change using `docker compose ... kill -s HUP tank`; the runtime installs a verified complete config snapshot. Restart recovers durable jobs, hot tokens and sessions without duplicate card publication. Check no usage/spending cap or budget-pause path remains. Actual subscription rate limits are propagated with provider reset information and no inferred retries before reset.

Backups use the external `/backup` volume and `tank backup --force` (the wrapper `deploy/backup.sh` invokes the same command). The backup command verifies its recovery manifest; restore validation uses `pg_restore` into fresh databases plus the recorded manifest checks. Retention/pruning requires explicit `--prune`; no automatic destructive cleanup. Preserve both database dumps, complete data/config/logs and checksummed manifest. Back up the OpenMA root secrets separately under appropriate protection; the database alone cannot recover encrypted Model Cards. Restore into a fresh project/database first, verify hashes/immutable bundles, row counts, jobs and offline replay, and keep delivery disabled until duplicate-delivery state is verified.

No build or synthetic test proves operational acceptance. Separate evidence is required for source failure/latency targets over72hours, all venue metric semantics and archive continuity for5tokens×7days, live Linux KVM replay isolation, ChatGPT-authenticated model/SSE and refresh behavior, Telegram threading/retry, and14-day shadow review. Current F-16 expects STRESS under the unchanged normal-range context rule; the earlier HEADWIND conflict is obsolete in the latest Technical Design.

## Snapshot time and authority

One market cutoff governs exchange/settlement times and completed-bar bucket ends. `received_ts` is collection latency and may be later than cutoff for historical backfills. `assembled_at` records availability: exclude rows received after snapshot assembly and semantic market points after cutoff. Official account authority is scoped to claim subject; `@sama` has documented World and OpenAI roles, not global trust.

## Source reconciliation

S08 uses the official DTCC Coveo search backend with newest-first pagination and full articles, because server HTML contains only partial release anchors. Its read-only public query token is extracted at runtime from DTCC's script and never stored in config. Robinhood S13 follows its canonical `robinhood.com/newsroom` host and `__NEXT_DATA__` canonicalUrl cards, accepting `/us/en/newsroom/` paths. S05 retains its official feed after a transient404 rechecked as valid RSS. OKX uses port443 and seqId/prevSeqId integrity; checksum was deprecated23June2026 and fixed0. These corrections retain original source identity and have recorded evidence.

## V0-B calibration workflow


`docker compose --env-file /path/to/compose.env run --rm --no-deps tank tank calibrate /data/labeled-scores.json --output-dir /data/calibration/v0-b-001` performs offline calibration and writes `calibration-report.json` plus `thresholds.proposed.yaml`. The deploy/calibrate.py wrapper can also run inside that Python image. Neither path contacts Jev, edits thresholds.yaml, nor relaxes production sample minima. Keep the original scored file with its report and hash for review.

1. Label at least 200 distinct source items from S01–S34, including at least 30 repeats/developments of a known event and 10 repeats across sources. The PRD does not explicitly require all 34 sources in this sample; the report lists actual source coverage so the reviewer can assess representativeness. Identify the correct existing event or explicitly label a new event. Record relevance independently of whether an item is important. Cross-source repeats must be confirmed against their originating source.
2. Assign `split` as `calibration` or `heldout` before examining scores; keep each `label_group` in one split only. Record the split method, operator labeler and timestamp. The software imposes no invented split ratio. Both subsets must contain relevant items and true repeats so evaluation is defined. Do not move examples or tune against the heldout results after seeing them.
3. Acquire scores using the installed `Pipeline.score(item, "relevant", Pipeline.state(item))` for filtering and `Pipeline.score(item, "same_event", state)` for routing. The latter `state` is the runtime item state plus `event` containing statement, entities, tokens, stage and known_to_us_at, exactly as `Pipeline.route_item` builds it. Use the actual frozen online candidate list: event eligibility, identities, preceding three-day window, and original iteration order. Preserve all candidates, including wrong candidates and empty candidate lists. A labeled repeat whose correct event was absent remains a routing miss. Acquisition requires an explicit operator script/API action and consumes metered usage on the explicitly configured Jev provider; the offline calibrator never acquires scores itself. Save corresponding `ops.llm_usage` records and input contexts as evidence. Use pinned OpenRouter request model `jev-1.13`, actual resolved model `typesafe/jev-1.13-20260917`, prompt version `jev-v1`, and the exact code revision; record `synthetic: false` only for actual acquired production scores. Model and prompt names alone are not evidence that acquisition occurred.
4. Build one JSON object with the fields shown below. Every item must contain a probability filter score and an ordered candidate array. The minimal example illustrates format only and will fail the required sample gates. Scores below are placeholders for format, not calibrated defaults.

```json
{
  "dataset_id": "v0-b-001",
  "labeler": "operator name",
  "labeled_at": "2026-10-17T00:00:00Z",
  "split_method": "describe preassigned split and label grouping",
  "candidate_set_description": "describe frozen runtime candidate eligibility, order and saved contexts",
  "score_provenance": {
    "jev_provider": "openrouter",
    "model": "typesafe/jev-1.13-20260917",
    "requested_model_id": "jev-1.13",
    "resolved_model_id": "typesafe/jev-1.13-20260917",
    "model_pin_evidence": "saved provider catalog/request/response evidence matching the pinned 1.13 binding",
    "prompt_version": "jev-v1",
    "code_version": "exact installed revision",
    "acquisition": "paths to actual Pipeline score requests and ops.llm_usage evidence",
    "synthetic": false
  },
  "items": [{
    "item_id": "actual item ID",
    "source_id": "S33",
    "split": "calibration",
    "label_group": "real-world fact grouping ID",
    "relevant": true,
    "cross_source_repeat": true,
    "expected_event_id": "actual earlier event ID",
    "filter_score": 0.5,
    "candidates": [{"event_id": "actual earlier event ID", "score": 0.5}]
  }]
}
```

5. Review the report. The filter cutoff is the highest observed calibration threshold retaining at least 95% of relevant labels. Routing selects the single highest-scored candidate above cutoff, retaining the original candidate on ties exactly like Pipeline. A wrong chosen event contributes a false positive and, for a true repeat, a false negative. The selected routing cutoff is the highest at which precision and recall both reach 0.90 on calibration. Heldout labels only evaluate the already chosen thresholds. If heldout filter recall is below 0.95 or routing precision/recall below 0.90, the proposal is rejected. If no routing threshold qualifies, the report retains null rather than inventing one. Failed or synthetic runs produce no threshold patch. The report measures filter and routing separately; complete V0-B also checks end-to-end first-known times, state, freshness and latency.
6. Only a reviewed, passing report with actual acquired scores may be copied into a new version of thresholds.yaml. Copy the proposed `thresholds` values, retain the dataset hash/report alongside the config revision, review the diff and run acceptance checks before reload. `REVIEW_REQUIRED` means a proposal exists, not that production acceptance has been certified. The software cannot establish label truth or acquisition honesty from a JSON assertion; the operator must inspect the saved evidence. A failed heldout run needs a new reviewed experiment and fresh heldout evidence, not repeated tuning on the same heldout set.

The checked-in registry resolves DefiLlama fundamentals against the official fees catalog and exact summary IDs, with request URLs, HTTP status, methodology and response hashes in `config/fundamentals_evidence.json`. Nineteen token projects have verified fee summary slugs; ZRX, QNT and WLD have explicit provider NOT_SUPPORTED evidence. Metric support is separate: BTC and ENA holdersRevenue were unavailable in the verification response. Parent rollups are requested once; never add child series to the parent again. World Chain fees are supplementary ETH gas context and do not prove WLD capture. Reverify catalog identity when the provider changes; emission/unlock slugs remain a different namespace. Chain IDs come from the official `/v2/chains` catalog, including Plasma9745; non-EVM native chains retain null EVM IDs.


## Pinned OpenMA compatibility overlay

Actual PostgreSQL16 integration exposed upstream defects despite a healthy HTTP endpoint. The guarded overlay casts ten nullable SQL parameters to VARCHAR191 so PostgreSQL can infer parameter types, and normalizes bigint epoch strings at the SQL timestamp adapter boundary (thirteen substitutions across twelve files). Source hashes and exact match counts are checked before modification. Canonical agent tool codecs are extended consistently through schema/domain/mapper/resolver for browser and disabled scheduling names already supported by the pi harness. Register through canonical `/v1/agents`; the older `/v1/oma/agents` store is distinct and must not supply canonical session IDs. Skill creation uses canonical multipart `files[]` and the required `managed-agents-2026-04-01,skills-2025-10-02` beta flags. Model Cards use `/v1/oma/model_cards` with POST for create/update; list-before-update avoids the pinned duplicate-create500 defect.

The browser overlay binds both ordinary and confirmed tool execution to the exact session SandboxPort. Python Playwright/Chromium and the Unix-socket worker run inside that VM; profile data stays in its checkpointed workspace. End-of-turn cleanup flushes the profile before checkpoint creation. See [OPENMA_BROWSER.md](OPENMA_BROWSER.md) for reproducible local Chromium/transport tests and the separate KVM acceptance requirement. The sandbox image includes Playwright 1.64.0 and its matching Chromium.

Compose builds OpenMA with `SKIP_CONSOLE=1`; Tank uses its internal API and publishes no console port. The pinned upstream console's default build currently fails resolving `../data/mcp-registry`; the API image builds using the explicitly configured skip flag. This does not certify any console workflow.

Delivery chat IDs are read from versioned `config/delivery.yaml`. The optional empty `TANK_TELEGRAM_ALERTS_CHAT` and `TANK_TELEGRAM_OPS_CHAT` install variables override them only when explicitly populated. The scout receives its reviewer skill and three custom tools only, with all native defaults disabled; the analyst receives the analyst and data skills.

Actual PostgreSQL16 probes of generated HBA rules used two explicit synthetic Docker subnets and verified both read-only SELECTs, default read-only settings, write refusal, cross-database refusal, refusal from the service subnet, peer administrator access and valid parsed rules. Evidence: [postgres-network-isolation.json](verification/postgres-network-isolation.json). These probes do not establish the actual target BoxLite source CIDR or KVM network enforcement. Existing deployments must provision the new replay role/password and revoke any prior tank_agent CONNECT grant on tank_replay before enabling replay.

An observed VM egress address may be an OpenMA/NAT /32 inside the Compose subnet. Overlap is allowed because HBA rules are separately scoped by database and role; this does not claim layer-3 isolation. Record the observed source with a controlled authenticated connection and administrator query of pg_stat_activity.client_addr (or a packet capture on the database interface), then provision exactly that observed address/subnet. Confirm SELECT from the actual sandbox and rejected cross-database connections before enabling production.

For an existing database volume, after mounting the new replay password secret, run `docker compose --env-file /path/to/compose.env exec -T -u postgres postgres sh < deploy/provision_replay_role.sh`, then apply `deploy/read_only_role.sql` to tank_replay after its migrations. This explicitly revokes the obsolete production agent grant on tank_replay. Fresh volumes receive the same isolated roles during initialization.

For source discovery, run a no-credential connection probe from a disposable BoxLite session to postgres:5432 using a deliberately invalid nonsecret role. Read the rejected source host from PostgreSQL authentication diagnostics/logs; an administrator can also inspect `SELECT client_addr, usename, datname FROM pg_stat_activity WHERE client_addr IS NOT NULL` after the narrow rule is installed. Record the observed address and repeat the allowed/read-only/cross-database probes from that exact VM path. A source observed from an ordinary Compose client certifies only the synthetic NAT test, not the hardware VM path.

## Preparation with Docker only on the host

The host needs Docker/Compose/buildx and Linux KVM; Python, uv, Node and pnpm run inside the built images. First build Tank with `docker build -t tank-v0:openrouter-live .`. Clone the pinned upstream using an ephemeral Git container (mount a new source parent and use your host UID/GID), then check out the exact commit with that same container. Run helpers with the Tank image, explicitly overriding its entrypoint and mounting only the needed source/config/output directories. For example:

```sh
docker run --rm --user "$(id -u):$(id -g)" --entrypoint /opt/tank/venv/bin/python \
  -v "$PWD/deploy:/app/deploy:ro" -v /srv/tank-config:/config \
  -v /srv/tank-network:/network tank-v0:openrouter-live \
  /app/deploy/prepare_network.py --compose-cidr "$TANK_COMPOSE_CIDR" \
  --sandbox-cidr "$TANK_SANDBOX_CIDR" --output /network/pg_hba.conf
docker run --rm --user "$(id -u):$(id -g)" --entrypoint /opt/tank/venv/bin/python \
  -v "$PWD/deploy:/app/deploy:ro" -v /srv/tank-sources:/sources \
  tank-v0:openrouter-live /app/deploy/prepare_openma_source.py /sources/pinned /sources/prepared
```

The source clone can use `docker run --rm --user "$(id -u):$(id -g)" -v /srv/tank-sources:/sources alpine/git clone https://github.com/openma-ai/open-managed-agents.git /sources/pinned`, followed by the same container with `-C /sources/pinned checkout 85d5361f535c7221de975838dc48444e468cc02e`. The source helper verifies that commit plus exact upstream hashes. Use the same Python-container pattern for prepare_config.py (mount the config and its Git root), prepare_sandbox.py and offline prepare_openma.py. Applying OpenMA resources is an explicit separate --apply command on the internal Compose network with only fake credentials for acceptance probes or reviewed production secrets after authorization.

Export the actual sandbox artifact rather than relying on a Docker-local tag:

```sh
docker buildx build -f deploy/sandbox.Dockerfile \
  --output type=oci,dest=/srv/tank-artifacts/tank-analysis.oci.tar .
docker run --rm --user "$(id -u):$(id -g)" --entrypoint /opt/tank/venv/bin/python \
  -v "$PWD/deploy:/app/deploy:ro" -v /srv/tank-artifacts:/artifacts \
  tank-v0:openrouter-live /app/deploy/prepare_sandbox.py \
  /artifacts/tank-analysis.oci.tar /artifacts/tank-analysis-oci
```

Set TANK_SANDBOX_OCI_PATH to the resulting host layout directory. Compose mounts it read-only at /opt/tank-sandbox-oci and passes that path to BoxLite. The helper verifies OCI layout/index, every referenced manifest/config/layer size and SHA256 before accepting it, and records tank-oci-verification.json. Build for the actual KVM host architecture; the local test artifact is linux/arm64 and is not an x86_64 release. The pinned SDK contract explicitly describes rootfsPath as a local OCI layout directory overriding image, and the guarded factory test proves it reaches native SimpleBox options. Actual VM import/boot still requires KVM and remains a release gate.

Additional actual PostgreSQL probes override default_transaction_read_only to off at connection startup and still receive permission denied on INSERT for both roles. The SELECT-only ACL is enforced independently of that changeable session default. Evidence: [postgres-network-acl-isolation.json](verification/postgres-network-acl-isolation.json) and [postgres-network-nat-acl-isolation.json](verification/postgres-network-nat-acl-isolation.json).

## Linux secret-file ownership

Keep the host secret directory mode0700 owned by the operator; Docker mounts individual files, not that parent directory. Verify image identities with `docker run --rm --network none --entrypoint id postgres:16 postgres` and the corresponding application-image `id` commands. The tested PostgreSQL image uses UID999, while Tank/OpenMA use UID/GID1000. For this verified image combination, set the database password files as follows: the four shared role passwords (tank_db_password, oma_db_password, tank_agent_db_password, tank_replay_agent_db_password) use owner999, group1000, mode0440; postgres_password uses owner999 mode0400. Other service secret files use owner1000, group1000, mode0400. Keep secret values out of command arguments and logs; apply chown/chmod to file names only. If rebuilt images have different identities, derive equivalent owner/group access from their actual IDs before startup.

This grants PostgreSQL owner-read and applications group-read on shared password files while preserving each service's selective Compose secret mounts. Application-only Telegram, X and internal gateway secrets remain absent from OpenMA or PostgreSQL unless explicitly required by their mounts. Linux ownership must be checked on the target host; Docker Desktop's file-sharing ownership projection is not a Linux permission acceptance test.

A synthetic Docker Linux-volume probe confirmed the stated UID/mode read boundaries and was cleaned afterward: [secret-file-permissions.json](verification/secret-file-permissions.json). It does not access operator secrets or certify Docker Desktop host-file ownership mapping.

## Explicit OpenRouter Jev deployment phase

The current Compose deployment sets TANK_JEV_PROVIDER=openrouter and mounts the openrouter_api_key Docker secret for Tank and OpenMA. Supply its file outside the repository; do not put its value in compose.env, build arguments, images or generated configuration. No TypeSafe account is required for this deployment. The official TypeSafe SDK stays pinned at 0.7.5, with OpenRouter base https://openrouter.ai/api and request model jev-1.13 (provider family typesafe/jev-1.13); there is no model upgrade, latest alias or automatic provider fallback. Direct TypeSafe compatibility remains available only through an explicitly selected alternate local configuration with its separate credential.

Analyst/scout models continue using the Codex subscription gateway, not the OpenRouter Jev key. OpenMA receives the selected Jev key only for curated production sandbox research access; the adapter exports OPENROUTER_API_KEY and TANK_JEV_PROVIDER=openrouter to the production VM. Replay exports only its isolated read-only database URL and no OpenRouter, TypeSafe or Etherscan key. Restart OpenMA after changing its secret file, then start a fresh session so a previously constructed sandbox cannot retain stale credentials.

Real-key host probes are separately authorized and recorded by their owners. Docker builds and deployment probes use synthetic keys only; never mount the operator .env or Codex auth into a build or the isolated test project. The October17 offline image certificate remains a historical record. New integration-phase image and source proofs are recorded separately, rather than relabeling offline tests as authenticated integration. KVM sandbox execution and target calibration remain distinct acceptance gates.
