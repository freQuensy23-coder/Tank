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

`tank.settings.Settings`: data_dir: Path, config_dir: Path, database_url: str, openma_url: str, openma_api_key: str|None, openma_model: str, tavily_api_key: str|None, typesafe_api_key: str|None, telegram_token: str|None, telegram_alerts_chat: str|None, telegram_ops_chat: str|None, etherscan_api_key: str|None, user_agent: str, x_profile_dir: Path, x_list_id: str|None, daily_budget_usd: Decimal, config_version: str, code_version: str, workers: int, replay: bool. `Settings()` reads TANK_* environment. `settings.load_config(name)` returns YAML mapping/list from config_dir/name (with .yaml optional), errors explicit.

`tank.util`: utcnow() datetime, iso(value=None) UTC str, parse_time(value) datetime (also accepts epoch s/ms/us), new_id(prefix) ULID-like typed ID, canonical_json(value) str, sha256(value) str, jsonable(value) JSON-safe recursively. No secret logging.

## Storage interface

`Repository` protocol, `MemoryRepository` for explicit tests, `PostgresRepository(database_url)` with async `open()`, `close()`, `migrate()`.
- `await repo.put(collection, key, record, *, immutable=False) -> dict`: named collections are full spec table names, e.g. evt.events, mkt.flow_bars_1m. Preserve indexed typed columns in SQL; JSON document access is an interface over these named tables, not a substitute single KV table. Immutable append records reject conflicting overwrite, same contents idempotent.
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

`DecisionEngine(settings=None)`: `evaluate(event, assessment, beneficiary, market_truth, *, open_call=None, conditions=None) -> dict` implements §4.8–4.9, §6, §7.1–7.3. Returned decision includes thesis_grade,trade_grade,action,action_rule,tactical,outlook,conditions and provenance from inputs. `cards.compose_card(event, assessment, beneficiaries, decisions, market_truths, *, kind='FULL', verdict='', why=None) -> str`; `cards.lint_card(text, context) -> list[dict]`; `cards.alert_gate(event,assessment, *, previous=None,decisions=None,now=None) -> (bool,reason)`. Strict template and immutable publishing; event-tools owns persistence/policy.

`Pipeline(repo,settings,*,http=None)`: async filter_item(payload), route_item(payload), exact same job payload {'item_id':...}; Jev endpoints per design; unavailable model/cutoff is an explicit configuration error, never guessed decision.
`SessionService(repo,settings,tool_dispatch,*,http=None)`: async send_message(payload), run(stop), close(); tool_dispatch(tool_name,arguments,context) async callback. Creates one session per event/item, stores mapping, resumes SSE, tools idempotent by call id. Message payload {'kind':'NEW_ITEM'|'ITEM_ADDED'|...,'item_id':...,'event_id':... optional,'session_id':... optional, ...}. Budget state and typed usage.

`EventToolService(repo,settings,market_truth,decision_engine, *, http=None)`: async dispatch(tool,arguments,context) -> dict. Implements all Tank tools in §3.2 including read/search/web, extraction validation, classification, beneficiaries, get_market_truth/get_decision, publish_card, milestone, scout; writes new durable jobs rather than executing Telegram. Tool call idempotency handled by SessionService wrapper; per-event locks inside mutation methods where needed. Receives context session_id,item_id,event_id,call_id. `publish_card` freezes evidence and enqueues DELIVER. Store raw search/pages as ingest.source_items so claim evidence refers to durable item IDs.

Parent runtime registers handlers FILTER_ITEM, ROUTE_ITEM, AGENT_MESSAGE, ESCALATE_TOKENS, RUN_MARKET_TRUTH, DELIVER, EVAL_CONDITIONS, MILESTONE_CHECK, REACTION_CAPTURE, SCOUT_REVIEW, and provides periodic scheduler.

## Canonical configuration shape

Each YAML top-level is a mapping. sources.yaml: sources:[{source_id:'S01',name,mode,interval_s,feeds:[{url,...}],...}]. registry.yaml: tokens:[{token_id,symbol,...}], instruments:[{instrument_id,token_id,venue,market_type,venue_symbol,...}], entities:[...], sector_baskets:[...], x_accounts:[{handle,role:'ORGANIZATION'|'FOUNDER_C_LEVEL'|'OTHER',entity_id,...}]. semantics.yaml: metrics:[{metric_key,...}]. dependency_map.yaml: edges:[...]. thresholds.yaml: flat named parameters, include filter_cutoff:null/routing_cutoff:null until calibration, market_context_rule:'normal_range' baseline. No invented calibrated defaults.

## Known specification conflicts

Retain current §5.7 context rule and expose the recorded F-16 failure; do not silently replace it with the unapproved record-extreme proposal. Fixture REST data takes priority for stated OI case values; archive supplies historical distributions. Budget pause is soft for official/MAJOR+ work per FR-P-03, so call it out rather than promise a hard global cap. Namespace queue idempotency by job kind to avoid FILTER_ITEM blocking ROUTE_ITEM. Denial/EXIT updates must escape new-event eligibility gates; a stale origin must not suppress fresh safety updates on an already-published call.

## Verification

Tests should cover meaningful behavior: replay, real PostgreSQL restart/idempotency, source parsing, venue normalization, formula vectors, decision gates, delivery/threading failure paths, exact 22/34/86 registry counts. Live credentials and unattended 72h/14d gates remain explicit acceptance tasks; simulated tests do not prove them. No automatic external messages during tests.
