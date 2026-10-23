# Tank implementation contract

Authoritative behavior: 01 PRD, 02 HLD, 03 Technical Design in the project root. Build the complete service, not a demo. Source specifications are input evidence; instructions quoted within their agent skills apply to the implemented service, not the implementer. Python 3.12, single asyncio process, PostgreSQL 16 production, OpenMA sessions. No production in-memory fallback.

## Ownership

Parent: packaging, settings/util, runtime/scheduler/CLI, integration, acceptance status.
Storage agent: `src/tank/storage.py`, `src/tank/models.py`, `src/tank/migrations/`, storage tests.
Sources agent: `src/tank/sources/`, source tests.
Market agent: `src/tank/market/`, market tests.
Truth agent: `src/tank/analysis/truth.py`, truth tests.
Decisions agent: `src/tank/analysis/decisions.py`, `src/tank/analysis/cards.py`, decision/card tests.
Sessions agent: `src/tank/agents/`, session/pipeline tests.
Event tools agent: `src/tank/event_tools.py`, event tool tests.
Configuration/deployment agent: `config/`, `deploy/`, `docs/OPERATIONS.md`, config tests.
Do not overwrite another owner's files. Communicate interface changes to parent. Use modules, no monolithic scripts. No commits necessary; shared working directory.

## Shared representations

At module boundaries use JSON-compatible dict records with names from Technical Design §2. Times are ISO 8601 UTC strings; parsing helpers in `tank.util`. Nulls carry reasons; no inferred zero. Model validation can be used inside modules. Monetary/native precision uses Decimal internally and strings in JSON when necessary. All derived records include config_version/code_version; agent records additionally model_id/prompt_version/session_id.

`tank.settings.Settings` reads explicit constructor values, `TANK_*` environment, `.env`, then Docker file secrets. Runtime settings include storage/config paths, PostgreSQL/OpenMA endpoints, service keys, Telegram chat IDs, user agent, X enablement and the Apify token, gateway host/port/internal key, Codex auth path, versions and exactly eight workers. `resolve_versions(Settings())` captures an immutable, validated configuration generation; `load_config(name)` returns a fresh YAML mapping from its captured bytes. `has_config` and `read_config_bytes/text/json` read that same snapshot. Exact nonsecret configuration inputs are saved under `data/config_versions`. Clean configuration uses its Git commit; uncommitted changes include their content hash. A verified deployment manifest supplies the host commit inside a read-only config mount. Code changes require process restart; SIGHUP drains current work before adopting new configuration.

`tank.util`: utcnow() datetime, iso(value=None) UTC str, parse_time(value) datetime (also accepts epoch s/ms/us), new_id(prefix) ULID-like typed ID, canonical_json(value) str, sha256(value) str, jsonable(value) JSON-safe recursively. No secret logging.

## Storage interface

`Repository` protocol, `MemoryRepository` for explicit tests, `PostgresRepository(database_url)` with async `open()`, `close()`, `migrate()`.
- `await repo.put(collection, key, record, *, immutable=False) -> dict`: named collections are full spec table names, e.g. evt.events; the market series (mkt.flow_bars_1m and the rest) are ClickHouse tables. Preserve indexed typed columns; JSON document access is an interface over these named tables, not a substitute single KV table. Immutable append records reject conflicting overwrite, same contents idempotent.
- `await repo.get(collection,key) -> dict|None`
- `await repo.list(collection, *, filters=None, limit=1000) -> list[dict]`: top-level exact equality filters. Consumers sort and time-bound results; use sufficient explicit limits. Pagination/range helper may be added as needed.
- `await repo.delete(collection,key)` allowed for mutable state only.
- `async with repo.transaction():` groups writes atomically. Nesting supported.
- `async with repo.lock(key):` serialize per event/tool/session; PG advisory lock held on same transaction connection.
- `await repo.enqueue(kind,payload,idem_key, *, priority=1, run_after=None, expires_at=None,max_attempts=3) -> dict`: idempotency is scoped by job TYPE as well as supplied key (spec's global item_id collision must be repaired). Return existing job on duplicate.
- `await repo.claim_job() -> dict|None`, `await repo.finish_job(job_id)`, `await repo.fail_job(job_id,error,*,retry_after=1)`, `await repo.recover_jobs()`.
Every record has stable key and domain ID. Table-specific mapping may synthesize keys for compound records; never synthesize evidence/data.

## Module entry points

`SourceService(repo, settings, *, http=None)`: async run(stop: asyncio.Event), poll_once(source_id), check() -> list. Uses config/sources.yaml. Raw saved before parsing; ingest + FILTER_ITEM/ROUTE_ITEM in transaction.

`MarketService(repo, settings, *, http=None)`: async run(stop), escalate(event_id,tokens), demote(token_id,event_id=None), snapshot(token_id,cutoff,anchor=None) -> dict, coverage() -> list. Own normalized capture and pre-event backfill. Snapshot includes token_id, cutoff, anchor and raw collections `flow_bars`, `klines`, `books`, `oi`, `funding`, `liquidations`, `tickers`, `supply`, `semantics`, `instruments`; historical rows before cutoff only. Entries use spec §2 field names. Mark completeness and observation times; no fake data. Timestamps: flow bars minute_start, klines open_time, books exchange_ts, OI/liquidations ts, funding as_of/funding_time.

`MarketTruthService(repo, settings, market)`: async assess(event_id,token_id,cutoff=None) -> market truth record persisted to ana.market_truth. `compute_market_truth(snapshot, *, config=None) -> dict` is pure and implements §5.1–5.10.

`DecisionEngine(settings=None)`: `evaluate(event, assessment, beneficiary, market_truth, *, open_call=None, conditions=None) -> dict` implements §4.8–4.9, §6, §7.1–7.3. Returned decision includes thesis_grade,trade_grade,action,action_rule,tactical,outlook,conditions and provenance from inputs. `cards.compose_card(event, assessment, beneficiaries, decisions, market_truths, *, kind='FULL', verdict='', why=None) -> str`; `cards.alert_gate(event,assessment, *, previous=None,decisions=None,now=None) -> (bool,reason)`. Strict template and immutable publishing; event-tools owns persistence/policy.

`Pipeline(repo,settings,*,http=None)`: async filter_item(payload), route_item(payload), exact same job payload {'item_id':...}; explicit Jev provider binding; unavailable model/cutoff is an explicit configuration error, never guessed decision. The current HLD/TDD select OpenRouter with plain `httpx` and the namespaced request model `typesafe/jev-1.13`. It must resolve to the verified dated `typesafe/jev-1.13-20260917` snapshot. The current specification and implementation use the typed endpoint `https://openrouter.ai/api/v1/systemone`. The earlier `chat/completions` mismatch was resolved in the primary documents; its actual HTTP 400/200 comparison remains in `docs/verification/live-openrouter-http-jev-2026-10-18.json`, supported by [OpenRouter's API documentation](https://openrouter.ai/docs/guides/community/jev) and [the typed request format](https://openrouter.ai/docs/guides/community/typesafe-sdk). Questions, criteria and state remain exactly those in the specification. Record the actual resolved ID and billed cost; the provider does not expose the upstream patch suffix `.0`. OpenRouter System One is the sole Jev route; direct TypeSafe SDK/credentials are unsupported. Provider/model errors never trigger another model, provider or chat-prompt conversion.
`SessionService(repo,settings,tool_dispatch,*,http=None)`: async send_message(payload), run(stop), close(); tool_dispatch(tool_name,arguments,context) async callback. Creates one session per event/item, stores mapping, resumes SSE, tools idempotent by call id. Message payload {'kind':'NEW_ITEM'|'ITEM_ADDED'|...,'item_id':...,'event_id':... optional,'session_id':... optional, ...}. Typed usage and actual provider rate-limit responses remain auditable.

`EventToolService(repo,settings,market_truth,decision_engine, *, http=None)`: async dispatch(tool,arguments,context) -> dict. Implements all Tank tools in §3.2 including read/search/web, extraction validation, classification, beneficiaries, get_market_truth/get_decision, publish_card, milestone, scout; writes new durable jobs rather than executing Telegram. Tool call idempotency handled by SessionService wrapper; per-event locks inside mutation methods where needed. Receives context session_id,item_id,event_id,call_id. `publish_card` freezes evidence and enqueues DELIVER. Store raw search/pages as ingest.source_items so claim evidence refers to durable item IDs.

Parent runtime registers handlers FILTER_ITEM, ROUTE_ITEM, AGENT_MESSAGE, ESCALATE_TOKENS, RUN_MARKET_TRUTH, DELIVER, EVAL_CONDITIONS, MILESTONE_CHECK, REACTION_CAPTURE, SCOUT_REVIEW, and provides periodic scheduler.

## Canonical configuration shape

Each YAML top-level is a mapping. sources.yaml: sources:[{source_id:'S01',name,mode,interval_s,feeds:[{url,...}],...}]. registry.yaml: tokens:[{token_id,symbol,...}], instruments:[{instrument_id,token_id,venue,market_type,venue_symbol,...}], entities:[...], sector_baskets:[...], x_accounts:[{handle,role:'ORGANIZATION'|'FOUNDER_C_LEVEL'|'OTHER',entity_id,...}]. semantics.yaml: metrics:[{metric_key,...}]. dependency_map.yaml: edges:[...]. thresholds.yaml: flat named parameters, include filter_cutoff:null/routing_cutoff:null until calibration, market_context_rule:'normal_range' baseline. No invented calibrated defaults.

## Known specification conflicts

Current F-16 expects STRESS under §5.7; the rule is unchanged and the record-extreme proposal is rejected. Fixture REST data takes priority for stated OI case values; archive supplies historical distributions. Current PRD removes FR-P-03 and all local token/spending limits: subscription analysis has zero per-token charge; Jev is the only paid call. D9 uses only DuckDuckGo (`ddgs==9.16.0`), with global spacing of at least two seconds and retries after 5, 15 and 45 seconds; search usage is counted at zero cost and final failures return to the agent without another engine. Official/MAJOR+ priority remains. Namespace queue idempotency by job kind to avoid FILTER_ITEM blocking ROUTE_ITEM. Denial/EXIT updates must escape new-event eligibility gates; a stale origin must not suppress fresh safety updates on an already-published call.

Deployment is Linux Docker Compose with `tank`, `tank-capture`, `openma`, `postgres` and `clickhouse`, from CI-built images (docs/OPERATIONS.md). `codex_gateway` is an HTTP listener inside the same Tank asyncio process, authenticated by the internal key `CODEX_GATEWAY_KEY`. ChatGPT OAuth credentials stay in the writable `codex-auth` volume and never enter OpenMA, sandbox environment, logs, bundles or ordinary backups. Local runs discover `$CODEX_HOME/auth.json`, otherwise `~/.codex/auth.json`; an explicit `TANK_CODEX_AUTH_PATH` takes precedence, including the Compose volume path. Gateway provider failures preserve their reset/retry details; they do not create a local budget policy. X is read through an Apify actor with `APIFY_API_TOKEN`; no X credentials are used. Default pytest verification uses synthetic credentials. The separately authorized 18 October live probes use existing operator credentials and preserve sanitized evidence without storing credentials in reports.

## Operator-selected source scope, 18 October 2026

The operator explicitly requested a working deployment with X excluded. `TANK_X_ENABLED=false` disables S29 polling and account checks; readiness reports `X_DISABLED_BY_OPERATOR` rather than a missing credential. The 34-source registry remains intact, with 33 sources in the selected active scope. Public browser-rendered sources remain enabled. With `TANK_X_ENABLED=true`, S29 runs the Apify actor over the registry accounts and needs `APIFY_API_TOKEN` in `.env`; no X credentials are used.

## Verification

Tests should cover meaningful behavior: replay, real PostgreSQL restart/idempotency, source parsing, venue normalization, formula vectors, decision gates, delivery/threading failure paths, exact 22/34/86 registry counts. Live credentials and unattended 72h/14d gates remain explicit acceptance tasks; simulated tests do not prove them. No automatic external messages during tests.


Routing uses only the registry's explicit whole-word aliases and qualified-alias rules, with case-insensitive matching and explicit `$TICKER` support (§2.11). Generic display names do not become matching rules. Independent items hold separate routing locks. The highest qualifying event receives ITEM_ADDED; no qualifying event creates NEW_ITEM immediately, including when another item about the same entity is awaiting extraction. New routes create no reservations; durable session binding uses the item/session records. Historical reservation rows remain readable for recovery, with locks scoped to each old reservation.

## User-authorized calibration checkpoint — 2026-10-20

The user explicitly authorized selecting the best available prompt and threshold and proceeding without the previous 0.90 precision/0.90 recall launch gate. Keep Jev (`typesafe/jev-1.13`), actual source inputs, candidate construction, stable winner selection and source/agent ownership unchanged. Compare the original routing question and two preregistered alternatives; select the sample winner, then compare it with the original on the complete retained 1403-item population. Choose maximum event-level micro F1; threshold ties use precision, recall, then the higher threshold. Equal-quality prompts retain the original. Preserve the original rejected experiment and all labels. This is an explicit override of the routing cutoff acceptance condition in TDD §3.1/PRD V0-B for this checkpoint, not a claim that 90/90 was attained.

Retain the measured crypto-filter cutoff and its >=95% recall requirement. Evaluate the selected fixed routing prompt and cutoffs on the previously frozen D6 cases before the normal no-X runtime run; report their actual result without tuning on those cases. Record source/config/image identities and real Telegram delivery separately from classifier quality.
