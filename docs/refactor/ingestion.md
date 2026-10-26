# Ingestion and delivery refactor evidence

## First increment: capture HTTP and read coverage

Capture HTTP now has strict request, completion-state and response contracts. It rejects absent methods, unsupported destinations, absent required completion fields, missing or ambiguous body representations, malformed base64, and byte-count/digest mismatches. Queue mappings exist only at the PostgreSQL/capture wire boundary. HTTP headers remain associative maps. Failed, expired and pending requests legitimately have nullable results/reasons. HTTP headers/body and source identity are optional request inputs. `FetchResult.body` and `body_b64` are optional individually but exactly one is mandatory. Empty UTF-8 bodies remain valid. `FetchResult.content_type` may be an empty declared string, in which case the HTTP adapter omits that header.

Malformed successful capture records are retained instead of deleted before validation, preserving evidence. Verified successful records and terminal failure records retain the existing queue release behavior. A missing queue record is represented by `None`, never an empty fabricated record. The elapsed request budget and nonterminal state produce the existing TransportError outcome.

Read coverage computes with `ReadCounter`, `FeedReads`, `ReadSnapshot`, `SourceReadRate`, `ReadWindow`, and `ReadReport`. Feed/source keys are genuinely associative. `snapshot` accepts config JSON and projects only feed inventory because read reporting does not interpret adapter-specific feed payloads. `rates` is the existing external JSON interface; calculations occur in typed `report`. Missing checkpoints and absent counters before the first poll/detail read legitimately mean zero. Present malformed or null counters fail validation. `FeedReads.status/error` can be absent before the first poll. A source without reads has unknown failure rate and fails acceptance. Counter reset during a baseline window now raises an explicit error instead of producing negative read counts and a false pass. Degradation errors remain attached to the source report.

Raw item storage now fully declares path/time/byte inputs and durability result types; immutable byte comparison, fsync hierarchy, atomic hard-link publication and retry semantics are retained.

Verification: Python 3.13.13 shared isolated environment with frozen dependencies. Ruff check/format and mypy pass on the changed modules. 80 tests pass across existing source tests and new ingestion contract tests with `-n auto`; no skips. New tests exercise response corruption, binary bytes, evidence retention/release, request policy, coverage aggregation and counter reset.

## Inventory and remaining work

Owned source modules: `sources/__init__.py`, `organization.py`, `capture_fetch.py`, `read_report.py`, `polling.py`, `raw_store.py`, `probe.py`, `rate_limits.py`, `parsers.py`, `xsensor.py`, `policy.py`.

Owned application/presentation modules: `delivery.py` (654 lines), `scheduler.py` (1945), `telegram_inbox.py` (1285), `telegram_rich.py` (144), `ops_cards.py` (548), `ops_entry_errors.py` (183), `ops_progress.py` (179), `cli.py` (590). Config: YAML except openma.yaml; config/skills excluded.

This increment is not global completion. Publisher wire parsers, polling checkpoints, source config, X sensor contracts, Telegram delivery/inbox, scheduler, operational presentation and CLI still require their complete typed domain migration and exact-symbol optional/default inventory. Existing dedup, attachment, forward-pairing, follow-up and security behavior remains covered by existing tests but has not yet all been migrated.

## Request-budget increment

`BudgetBucket` and `BudgetAdmission` replace tuple-shaped budget records; dictionaries now exclusively map genuine bucket identities to histories, locks, and waiting counts. Every function in `sources/rate_limits.py` has declared inputs/results, including the psycopg connection row shape. The ledger rejects missing/non-numeric admission delay instead of indexing an assumed row. Window/capacity/weight validation rejects nonpositive or nonfinite configured values. A `None` capacity deliberately disables only that unconfigured budget; retries remain exactly two attempts; absent host is permitted for a non-host budget. Connection and ledger `None` represent unopened/released resources, respectively. Unknown priorities retain the existing sign-based urgency behavior. Concurrent admission preserves urgent yielding and permits fitting baseline requests while a larger request sleeps. Existing source and non-integration external-capture tests plus new contracts: 86 passed, zero skips, in 5.21 seconds. PostgreSQL ledger integration has not yet run in this increment.

## Telegram rich presentation

`ScoreDimension`, `ScoreDimensions`, and `ScoreAssessment` require all seven declared dimensions and a value for each; an explicitly unknown value is `None`, never a missing dimension fabricated by presentation. Values outside 0–10 and nonfinite values fail validation. Rationale/reason remain legitimately optional explanations; the existing readable unknown-score wording is preserved. Analysis owns decisions and will consume this canonical score contract. `BoldText`, `ParagraphBlock`, `TableCell`, `ScoreTable`, and `RichMessage` describe approved output internally. Only `telegram_payload`, `score_table` and `card_rich_message` serialize Telegram wire dictionaries. The wire reader has separate concrete recursive block/cell/item models. Telegram may add layout metadata, so incoming wire projections ignore fields unrelated to text matching; missing table cells, list items, details blocks and paragraph/heading text fail explicit validation. Other media blocks can legitimately carry only captions or expressions. Recursive RichText remains JsonValue exactly at the wire boundary; its text/alternative_text/expression precedence preserves Telegram matching semantics. Message text and caption are genuinely optional and validated when present. All functions are fully typed, no casts/Any. Approved block text remains literal including angle brackets, Markdown and multiline rationale. 116 Telegram rich/delivery/follow-up tests pass with zero skips in 1.63 seconds; Ruff and mypy pass for the presentation module.

## Source dependency direction

Polling no longer imports its application service package to read a mutable clock. `SourceService.now/stamp` provide the clock dependency directly; polling delegates to its injected service, preserving module-level clock overrides used by deterministic tests. Parser imports now name `tank.sources.parsers` explicitly in service, polling, and probe modules, removing package-reexport edges that obscure the dependency direction. Source smoke/probe and poller depend on the parser adapter, while service orchestrates poller/probe. 75 source tests pass with zero skips in 1.66 seconds after the cycle fix.

## Active source configuration migration

`SourceService` now validates the complete `SourceConfiguration` before allocating its HTTP client. Source IDs map to `SourceDefinition` objects throughout service, poller, organization resolution and probes. Polling uses required feed kind/interval and source class directly; retired mode/format/class fallback chains are removed. `HttpFetchPlan` is the typed HTTP application request, constructed from `SourceFeed.request_plan` with the source limit model; article and Binance-detail requests construct explicit plans. Request methods, URLs, bodies and limits are accessed through attributes, including pagination and pending-proposal reads. Parser configuration uses concrete `FeedParsingSpec`; the retained public parser mapping inputs are explicit external/config wire projections, not runtime domain records. Source maps and host-capacity maps are genuinely associative. Operational entry-error reporting now consumes typed source/feed objects.

All source metadata required by the checked-in registry is mandatory. Nullable `SourceLimits.rate_per_min` means no independently documented rate; it cannot be absent. Optional source author organization is deliberate because source attribution may instead come from a feed, Federal Register agency, GitHub repository or monitored account. Optional endpoint verification/detail URL/paid-gap references exist only for sources that declare those capabilities. Feed pattern and instrument venue/market become mandatory under their adapter kinds; GraphQL requires POST plus a JSON payload. Feed method defaults to GET following HTTP acquisition semantics. Headers, embedded JSON fields, body/form and official index URL are optional adapter capabilities, not missing required state. Request weight defaults to one admission unit; no configured requests-per-minute capacity means that specific override is absent. Runtime budget limits remain explicit source models. Body/form/header maps are genuine external request payloads; page-query parameters and embedded publisher JSON likewise remain at wire boundaries.

Intervals accept positive finite fractional seconds as well as whole seconds; tests use this to exercise concurrent detail/cadence behavior without long sleeps. Empty source/feed lists are explicitly allowed for a disabled or offline registry; absent lists are invalid. Duplicate source IDs fail validation. Complete test source fixtures derive required registry metadata from checked-in source definitions and explicitly preserve optional attribution absence. Fixtures now call domain APIs using models and supply complete adapter inputs. HTTP callback annotations incorrectly labeled as pytest fixture requests were corrected to httpx.Request in owned tests.

Verification of this increment: 390 owned source/delivery/ops/scheduler/Telegram/CLI/e2e/contract unit tests passed with zero skips in 4.11 seconds using `-n auto`. Ruff passed all touched source/test files, and mypy passed the six source modules plus the typed fixture support module under the current checker configuration. This is a complete source-configuration consumer migration, not a claim that legacy item/checkpoint and scheduler/delivery records have all been migrated; that remains the outstanding inventory below.

## Malformed-entry reporting domain

`DigestEntryError`, `ProposalEntryError`, `AccountEntryError`, and `DocumentEntryError` replace structured error dictionaries inside reporting. Their concrete `EntryIdentity`, digest/proposal/account/exception identity records and `EntryErrorMarker`/`EntryErrorNotice` models preserve the difference between an absent account identity and an explicitly null run-level account. Required reasons, nonnegative entry indices and supplied SHA-256 evidence are validated. Error detail, short entry metadata and response hash are genuinely optional because malformed document/run failures may precede discovery of any entry. Nullable account identity means the run as a whole, never an invented account. Known reason labels remain an associative localization lookup; pending/fresh maps associate stable error digests to concrete errors.

`ops_entry_errors` is now fully typed and computes with these domain records. The reporter parses the existing external parser error wire input once. Serialization happens only in the repository/hash boundary, retaining exactly the old flat identity payload and omitting fields absent from the original error. This preserves existing dedup marker hashes, explicit null account semantics, raw evidence, transaction rollback, concurrent report idempotency, per-poll burst grouping and Telegram UTF-16 clipping. Invalid report input raises before any marker is written; the poller retains its existing retry backlog. Existing tests cover restart, concurrency, rollback, bursts, broken-sibling retention, report retry and durable storage. New tests additionally validate required evidence and identity differences. 93 source/report/contract unit cases pass with zero skips in 2.45 seconds, and mypy/Ruff pass the seven changed typed contract/report modules with current foundation dependencies. Hash evidence fixture strings were corrected to actual hexadecimal shapes rather than weakening validation.

## Runtime delivery/answer payload ports

Delivery now consumes the shared closed `AlertDeliveryJob|NoticeDeliveryJob` model union directly throughout subject selection, channel selection, locks and recipient queries. Required channels are no longer guessed inside the application workflow. Its public dictionary input is an explicit worker/wire decoding boundary through `parse_delivery_job`; runtime can pass models without reserialization. Telegram answer and answer-failure ports likewise consume `TelegramAnswerJob.question_id` directly. Successful delivery/answer and their terminal callbacks have explicit `None` returns. `DeliveryError.terminal` is a declared boolean, not a dynamically optional attribute. Deferred delivery requires an actual scheduled timestamp before ordering it. Terminal delivery before an alert row exists keeps `source=None`; it never fabricates an alert record for optional source timestamps or event identity. The existing ledger/idempotency/forward-thread/answer-chunk logic is retained. Test fixtures explicitly declare their delivery destinations and now exercise the same model input as the runtime. 194 delivery/operator/Telegram unit cases passed with zero skips in 2.66 seconds. Mypy and Ruff passed the two application modules for this incremental payload-port migration. Full delivery ledger, question, incoming Telegram update and presentation snapshot record migrations remain outstanding.

## Scheduler configuration and backup verification identities

Scheduler now requires a complete typed `Thresholds` configuration, either supplied explicitly for an isolated composition or resolved through `Settings.thresholds()`. Every prior name/default helper call became a direct required threshold attribute; `fixture_default`, settings duck-typing and empty-config substitutions were removed. Backup settings use the declared `Settings` fields. Owned tests use real Settings objects and explicitly inject the complete checked-in threshold contract, with a precise typed override in the call-expiry behavior case. Supplying an explicit Thresholds dependency is legitimate composition, not a missing-field fallback.

Backup verification caches now contain only strict `BackupIdentity`/`BackupFileIdentity` records (path, calendar day, device/inode/size/mtime identities of manifest and completion marker). Unused manifest dictionaries were removed from the cache; immutable on-disk manifests and verification behavior are preserved. File absence is an explicit nullable identity, so creating/replacing a marker invalidates the cache. Scheduler background tasks have declared task, callback and operation types. 189 scheduler/delivery/Telegram unit cases pass with zero skips in 2.04 seconds; module mypy and Ruff pass. Existing main lookup-by-key changes from PR37 remain incorporated. Full watch/reaction/lifecycle/backup-manifest domain migrations remain ongoing.

## Canonical discovery, capture and source lifecycle contracts

`contracts.source_items` now defines closed article, Federal Register, exchange-announcement, instrument-change, governance and social discovery variants. `DiscoveryEnvelope` separately carries publisher material, local observation context and raw transport bytes. The publisher identifier adapter accepts only a nonblank string or integer (never bool) and normalizes the allowed integer wire form to the canonical strict string field. A wire row cannot replace the configured source ID. Publication declaration is required, including explicit null when unknown; provenance aliases are validated through the timestamp codec without inferring exactness. `PendingSourceItem` binds required item/feed/first-seen/config/code identity. Its nullable raw reference means a detail page is still pending. Conversion to `CapturedItem` requires both durable reference and SHA-256; transport bytes never enter checkpoint/source JSON. Queue decisions now use these concrete models and serialize only into the durable checkpoint document.

RSS and JSON adapters validate inside their existing per-entry failure boundaries. A malformed typed metadata field therefore retains valid siblings, the raw entry identity, digest and explicit degradation rather than failing the whole batch. Required title/text/URL/publication keys cannot be replaced by missing-value defaults; providers can explicitly declare an empty optional textual value or unknown publication. New tests exercise a malformed agency metadata sibling, source-ID spoofing, missing publication, actual compressed raw evidence, pending-capture rejection and precise clocks.

The shared persisted `FeedSourceItem` is six concrete provider subclasses with required capture, publisher/display attribution, deferral, relevance, extraction, observation and version fields. Processing is the closed `UnprocessedSource|ClassifiedSource|RoutedSource` union. Classification provenance and status/score must agree; routed message/item/event identities and probabilities must agree. Dangling route/filter fields without a status are rejected. Explicit `classified`, `routed`, `with_extraction`, and `observe` transformations revalidate the aggregate. `WebEvidenceItem` separately requires the actual agent-requested web origin, raw media/byte/readable-text hashes, first-seen clock and complete generation provenance; it never fabricates feed-source attribution. `SourceItemDocument` is a concrete closed-union repository wire codec with `SOURCE_ITEMS` collection metadata. It exposes the actual typed variant through `.root`; it has no arbitrary mapping/get facade. Its serializer preserves the existing flat document shape. Real SourceService and EventToolService producer tests round-trip unchanged through the codec and typed repository, including routed transitions and raw references. Full source/pipeline consumers are being converted to these contracts; this commit does not claim the remaining checkpoint/application dictionary inventory is complete.

Exact optional data: `DiscoveryBase.published_prec`, `published_at_precision`, their reason/bucket/basis siblings, and `source_updated_at`/precision/reason siblings express only the provenance an adapter actually declares; `publication` rejects conflicts. `DiscoveryObservation.observed_at` and its canonical/legacy precision siblings can be absent before the acquisition workflow binds its local clock. `DiscoveryEnvelope.payload` can be absent for an index discovery awaiting a detail page; `PendingSourceItem.raw` is absent only until capture. `ArticleDiscovery.categories/subtype` are optional feed metadata; CMS identity/slug/recovery/URL aliases exist only for the verified publisher recovery flow. `SourceText.quoted` is optional quote metadata, never a guessed quote. `IssuingAgency` requires a declared name/raw name or identifier; parent ID/URLs/slug are optional provider metadata and never substitute an inferred organization name. `AnnouncementLabel.key/title/name` are alternative explicit provider labels. Federal document type/PDF/agencies and exchange announcement type/tags/business-publication siblings are genuinely provider-specific optional fields. Governance proposal/state/end/scores/total and social author/monitored author are required. CMS recovery clock aliases can be absent until the checkpoint annotates them.

Lifecycle optional data: nullable author roles and author organization mean unresolved attribution, not a default role. `observed_feeds=None` represents an older single-sighting document with only its required captured feed; adding a sighting reconstructs exactly that proven first feed, and multiple sightings without known feeds fail. Discovery-version fields can be absent from earlier captured documents; current queue producers always supply them. Extraction decision/reason are present only when that outcome supplies a detail. Source routing event/session fields remain nullable before a target/session is established, while an ADDED route requires its event/probability. An ALWAYS source has no classifier score; classified routes require it. `SourceIdentityHints` optionally preserves canonical explicit token/entity routing hints (with concrete entity IDs), while supplied null/wrong-shaped arrays fail; absence supplies no hints. These are genuine associative/event identity aids, not inferred labels. `AlwaysRelevance.reason` is present only for explicit agent-requested WEB evidence. Initial policy for an uncaptured/out-of-scope source remains an explicit pending reason. Calibration/replay must consume the typed relevance result and serialize only at their storage boundary.

Verification before this increment: the complete owned integration set passed 32/32 with zero skips against isolated PostgreSQL, ClickHouse and actual process contracts in 11.71 seconds; retained and supplementary archives were verified and staged unchanged (196 and 173 files). After the discovery/queue changes, 134 focused source/contract units passed with zero skips in 2.69 seconds; the expanded contract tests and canonical model module pass mypy/Ruff. A fresh full integration run remains required for this new increment.

## Complete typed X actor scan

`sources.xsensor` now uses strict concrete wire models and strict domain `XPostObservation`, `AccountFailure`, and `Scan` models throughout. The wire separates successful account reports (required nonnegative post count and explicit history-gap flag) from failed reports (required error evidence, no fabricated successful count). Unknown actor telemetry is ignored only by the concrete wire projection and retained in the exact original response bytes. Every function and owned X test helper is fully typed; no Any/object record substitutes or dataclasses remain. X observations stay typed through acquisition queue entry rather than converting back to dictionaries.

Post IDs, handles, URL/author/status binding, timezone-bearing publication instants, duplicate account reports and numeric cursor identities are validated. Cursor maps are genuine account associations. Missing requested reports and missing cursors with known posts remain explicit failures. Failed accounts cannot advance their cursor, regressing cursors retain their prior value and emit a degradation, and actor output cannot expand the requested account scope. Valid partial post evidence is still retained; other successful accounts can progress. Removed-account history remains retained without allowing returned unrequested data to advance it. Baseline no-backfill behavior, raw bytes, exact publication provenance and post-ID dedup remain unchanged.

Optional actor fields are exact: `ApifyRun.error` and successful-account error text are absent when there is no detailed failure; a failed run still requires global or account error evidence. Failed-account `history_gap=False` means no additional declared gap flag, while its required failure already degrades the read; it never defaults a successful read count. `XSensor.token=None` explicitly disables collection and raises before HTTP. No-cursor/unknown-report dictionary lookups represent missing wire members and produce named failures rather than empty successful records. Global degraded status without a detailed cause remains a real run-level RUN_DEGRADED marker. HTTP/scan error text is redacted, wire validation hides input values, and credential-bearing responses are rejected before they can become raw source artifacts.

Tests now inspect actual MemoryRepository source items/jobs and canonical social models rather than recorded enqueue echoes. Added tests exercise missing successful-read metadata, URL/time evidence, failed/regressing cursor ownership, request scope and credential echo rejection. X module and all its test helpers pass mypy/Ruff; 130 source/X/contract unit cases passed in 2.44 seconds, and 21 focused X/entry-error cases passed in 2.39 seconds, all zero skips. The owned 32-case integration rerun also passed in 11.80 seconds; evidence remains in the isolated worktree for reconciliation against the forthcoming clean v2 checkpoint.

Registry role audit found the real configured FOUNDER_C_LEVEL role (including the declared World/OpenAI authority record) in addition to organization/other roles. `SourceAuthorRole` is now the shared closed source-author role type, and canonical source records accept that actual role without changing filter policy. A test uses the real configured account metadata, a baseline and new actor post, actual source capture and the strict durable decoder to prove the founder role, unresolved-filter decision and declared organization survive unchanged. X source tests: 11 passed in 1.66 seconds; canonical source models/config and the entire X test module pass mypy/Ruff.

## Explicit extraction outcome replacement and context identities

Source route event IDs and routing-message item/event/session IDs now reject empty or whitespace-bearing identities while retaining genuine nullable context before an event/session exists. Extraction updates now accept a closed typed outcome instead of a status plus ambiguous optional keyword defaults: PendingExtraction, RunningExtraction and CompletedExtraction intrinsically clear old rejection/failure metadata; SkippedExtraction requires explicit nullable decision and reason; FailedExtraction requires an explicit nullable reason. None is a deliberate unknown/absent target value, never both “omit” and “clear.” The validated replacement preserves capture bytes/reference/hash, first-seen clock and routing ownership. A real source producer plus typed repository roundtrip regression stores a NO_EVENT outcome, then verifies retry and success clear both old fields without altering evidence. Application success/retry consumers are being coordinated with analysis/session owners; the actual EventToolService success paths and fresh-replay import reset were identified as stale-metadata boundaries requiring their paired fixes.

## Injected source progress reporting port

SourceProgressSink separates Pipeline decisions from Ops persistence and delivery. RepositorySourceProgress implements the port against a precise repository/settings protocol; ProgressPreferences requires both feature flags, and DisabledSourceProgress is an explicit composition choice. Missing settings no longer silently disable reporting. The item boundary validates a concrete source subject and matching durable identity before writing; known missing source rows produce no notice because no valid subject exists. ProgressDetails and PublicationReceipt validate approved reporting output, including actual gate_reason and stage-dependent nullable publication IDs. Optional reason/event/key fields retain their existing presentation/idempotency meanings; custom keys must be nonempty. JSON maps remain only at repository serialization and the narrow external stored-subject projection. Reporting still owns the existing lock, transaction, immutable notice and one delivery enqueue; WEB evidence intentionally produces no delivery. Tests exercise actual MemoryRepository dedup, disabled reporting and mismatched identities, rather than enqueue echoes. Focused source/Ops/X tests: 151 passed in 2.54 seconds; the new shared contract and item adapter pass scoped mypy, Ruff and formatting. Remaining event/session/tool/job reporting functions still require their full typed migration; this port checkpoint does not claim the entire Ops module is complete.


Tool/event/session reporting now uses explicit read-only projections. EventObservationSubject requires each durable observation item identity. PublicationProgressResult requires the producer's published flag/reason and accepted card identity/version; missing reason fails before persistence instead of displaying UNKNOWN. ToolProgressContext event/call identity is optional because non-event calls and absent upstream call IDs are genuine; explicit empty identities fail. SessionProgressView requires its session identity and source-message collection, derived from the required durable message_log at the wire boundary. SourceMessageProgress requires actual message identity, payload, turn status and retry count; the active source writer declares retry_count=0 at creation. sent_at is genuinely absent while ACCEPTING before remote acceptance, and session event_id is absent before initial binding. Non-source message kinds are explicitly ignored by this source-only reporter without reinterpreting their payload. ExtractionProgressView reads required extraction_status with genuinely nullable rejection reason, and an absent source row remains a completed source turn without an available extraction projection. Actual session/followup behavior tests pass117 cases in2.78seconds; a missing retry-owner regression verifies no notice/job writes. Session schemas remain narrow presentation projections, not parallel workflow aggregates.


JobFailureProgressView and FailureProgressTarget now complete the typed Ops progress adapter: supported source jobs require actual job identity/status and an item/event subject. USER_QUESTION and SCOUT_REVIEW have separate user/proposal ownership and intentionally produce no source notice; unsupported queue types also produce none. Malformed required source targets fail instead of silently disappearing. Runtime reason EXPIRED or durable status EXPIRED maps to the existing expiration presentation stage, without changing queue retry/expiry rules. Raw job/payload mappings remain only the dispatcher wire boundary; the reporter operates on its explicit immutable projection. Actual queue/notice tests verify expiry, idempotent single delivery and missing-target rejection. The complete ops_progress.py and contracts.progress modules now pass strict mypy, with no untyped functions or Any/object facades.


## Source authority registry and publisher boundary

SourceAccountRegistryView validates the complete configured X account list before allocating an owned HTTP client. Each SourceAccount requires handle, actual closed SourceAuthorRole, explicitly nullable entity_id/token_id and typed optional SourceOfficialAuthority declarations. The account association is genuinely keyed by normalized handle; duplicate/empty normalized handles fail. Optional official_authorities means no additional declared organizational authority, never permission inferred from a name. Existing founder authority for World/OpenAI remains preserved. Source ingestion, X polling and account health now consume account attributes instead of dictionary role/entity defaults. This is a source-specific configuration projection; other registry resolution fields remain owned by their canonical identity workflow.

The organization resolver now validates PublisherContext at its captured-record wire boundary and operates entirely on concrete typed evidence. URL/feed are required provenance. Optional explicitly declared organization, monitored/original author and issuing agencies correspond to actual provider metadata; no repost alias or empty account fallback remains. IssuingAgency allows an identifier without a display name, which cannot itself produce an organization label. Exact configured-feed binding and verified Federal Register/GitHub endpoint restrictions retain their precedence; missing matching authority remains explicit None. Synthetic captured fixtures now declare their URL/feed instead of relying on production defaults. Remaining SourceService construction/checkpoint/ingest dictionaries are still under migration; these authority changes do not claim that aggregate is complete.


SourceService now requires validated SourceApiLimitsConfiguration and shared Thresholds before allocating an owned HTTP client. Missing files/limits/escalation policy no longer become empty successful limits or an invented15second cadence. Each concrete provider budget variant carries its required provider tag, documentary evidence and applicable required capacities. Optional per-second/day/weight/cost/spacing/provenance fields match genuinely different provider policies; endpoint-specific budgets require path/capacity/period/scope. request_weights and response_size_extra_weights are genuine operation-name associations. Inference limitations and retry delay lists preserve configured provider evidence, while impossible nonpositive weights/delays and duplicate provider rules fail. No undocumented capacity is inferred from missing fields; the existing exchange capture boundary continues owning exchange admission. Non-exchange source admission now reads typed budget attributes. Tests use full checked-in operational configuration rather than fakeempty loader results. A missing required limits regression prevents source startup before any item/job persistence.


## Active typed ingestion application workflow

SourceIngestion now owns live source capture deduplication, cross-feed observation, attribution/policy, validated persistence, progress and job selection. It takes required IngestionStore, ResolvedVersionIdentity, RawCaptureStore and SourceProgressSink ports; HTTP polling and presentation formatting are outside the application workflow. SourceService.ingest/captured remain explicit flat checkpoint/storage adapters, immediately decode CaptureEvidence/DiscoveredItem and serialize the validated result. All capture identity/feed/raw digest/first-seen/discovery versions and publisher URL/publication declaration are required; no local clock or source ID replaces an absent captured fact. Pending/unknown publication stays its declared unknown precision instead of being fabricated as an occurrence. Optional DiscoveryBase.author preserves a genuinely declared contributor; SocialDiscovery still requires its own post author. The required lifecycle author uses the configured publisher display label only when no contributor is declared; authority remains separately resolved and never inferred from that label.

SOURCE_ITEMS typed Repository reads every existing dedup candidate through the concrete source/WEB union. Immutable observe transitions count each proven feed once, retain first capture identity/bytes/hash/clock and preserve processing ownership. WEB observations now have paired nullable count/feeds: both absent means only the original captured web origin is proven; a new observed feed records that proven origin plus the actual new sighting. Partial pairs/inconsistent counts fail, and duplicate sightings produce no new count. RawItemStorage.discard restricts cleanup to the exact source/item/date raw path derived from the incoming capture; a foreign path fails before deletion. EmptyContentIdentity.media preserves the existing empty-media fingerprint used by current X producers, whose full actor bytes remain raw evidence; no active acquisition producer supplies a decoded media list, and unknown capture fields fail validation.

The only record dictionaries in this workflow are explicit repository query/queue wire values and immutable Pydantic serialization at the inherited SourceLifecycle+DiscoveredItem assembly boundary (captured_source_item), followed immediately by exact concrete-variant validation; no arbitrary map is used for decisions. source/source_name wire duplication remains for existing provenance/calibration consumers. Old captured record lookup dictionaries and generic observation mutations are removed. New real application regressions cover WEB/feed cross-content dedup, immutable original raw bytes, single counting on repeated sightings, foreign raw deletion rejection and missing-clock validation before persistence. All420 owned units pass in5.19seconds;139 source/X/contract cases pass4.67seconds. SourceIngestion, canonical source-item contracts and RawItemStorage pass strict mypy/Ruff. SourceService's remaining HTTP/checkpoint/health orchestration and other owned modules still need completion; full integration after the coordinated Pipeline merge remains required.


The Pipeline cohort caller migration now uses required SourceProgressSink injection and typed ItemJob in owned Ops tests. Real Jev MockTransport responses exercise actual immutable score-ledger creation and replay-safe dedup, threshold rejection/routing and source notices; the partial _score_record mock and HTTP object() placeholder are removed. Pure publisher identity tests use RoutingSelector/RoutingItem projections directly rather than constructing a network/application Pipeline for a pure identity decision. Source tests preserve all actual authority and attribution assertions.103 Ops/source/publisher cases pass3.93seconds before the final request-ledger count assertions.


Clean combined checkpoint a5e1819 (canonical SourceIngestion0e7381b, Pipelinea8666d5, E2E64ed6c3) passed420 owned units4.23seconds and31/32actual integrations12.26seconds withzero skips; trackedHEAD was unchanged before/after. The sole failure identified the manually constructed PostgreSQL source fixture lacking required feed and discovery versions; its explicit captured provenance is now declared. The real source producer and full four E2E process cases passed. A fresh pinned full rerun will verify this fixture correction.


## Typed CLI readiness output

readiness_checks now requires concrete Settings and returns strict ReadinessCheck models; doctor carries a ReadinessReport until the terminal JSON serialization boundary. Missing configured attributes no longer become getattr defaults. Required source/delivery/threshold schemas validate before reporting success; invalid calibration explicitly reports false with the exception class. ReadinessOpenmaIds is a narrow required-key projection for the four binding IDs, whose nullable values reflect a deliberately unconfigured deployment. Missing keys fail validation rather than becoming empty values. Canonical Settings.registry/semantics/dependencies accessors validate their complete schemas, including required registry tokens, metric definitions and dependency edges. configured accepts only declared text/path/None and detects the existing placeholder strings; no secret values or validation inputs reach output. codex_auth_file_present checks file metadata only and never reads credentials. New tests exercise an absent calibrated cutoff and missing OpenMA identity through actual Settings loaders and assert redaction. Remaining operator commands/output/run_db closures still require full CLI typing/domain migration; this readiness increment does not claim the entire module complete.


## Explicit source publication units and processing clocks

published/timestamp_fields now have precise scalar inputs and require an explicit epoch_unit for numeric publisher fields. Binance releaseDate/publishDate/detail publication, OKX pTime/businessPTime and Bybit publishTime/dateTimestamp use declared milliseconds; Snapshot created/start/end use declared seconds. BlackRock's designated numeric publicationDate uses milliseconds with an explicit calendar-day bucket. Generic article/RSS fields cannot acquire epoch semantics from numeric magnitude. Calendar-only strings use the explicitly UTC calendar transport decoder and retain BUCKET(1d); zone-less clock strings remain unknown with UNVERIFIED_SEMANTICS. DTCC masthead parsing explicitly marks its intermediate calendar as UTC before returning a calendar date, never an exact occurrence. preferred_publication chooses a genuine alternate provider field only when the primary is None, preserving a valid zero epoch. Missing external publication remains a genuine optional source fact emitted as an explicit unknown declaration.

Operator error/progress notices, probe start and read-snapshot processing times use clock_iso. HTTP received_at uses the injected SourceService.now clock explicitly, preserving actual request/admission/response timing and deterministic clock tests. Retry-After accepts only finite nonnegative seconds or a valid offset-bearing HTTP date; malformed values retain the existing bounded-attempt exponential delay and never call parse_time(None) to invent a date. Acquisition still owns the same retry count and no new provider capacity/precision is guessed. All176 source/Ops focused units pass5.13seconds before the new explicit-unit/zero-epoch regressions. Other parser/health/checkpoint functions remain to be fully typed/modeled; this timestamp slice does not close those inventories.


SourceDefinition.interval_s and SourceFeed.interval_s retain the declared int|float numeric form, with positive/finite strict validation. The union preserves114configured integer intervals during unchanged canonical JSON serialization; it is not a default or coercion fallback, and booleans fail validation. An actual complete sources.yaml roundtrip test proves byte-equivalent canonical JSON values rather than normalizing declared integers to floats. Other runtime-only numeric budget fields retain their documented typed semantics.


SourceItemDocument now explicitly inherits the established secret-safe validation presentation policy through hide_input_in_errors=True (RootModel does not inherit DomainModel configuration). Telegram incoming rich projections use the same setting. Required schema validation is unchanged. A real typed repository JSON decode regression logs a complete malformed source record through the standard exception traceback path and proves its private text/input_value are absent; incoming malformed rich-message content likewise never appears in exception text. Active owned source/reporting paths use exception text/class names and do not serialize ValidationError.errors/json input payloads. Structured diagnostic consumers must continue using include_input=False if introduced; the hide-input setting is not a promise that direct error.json defaults suppress input.


ProgressSubject now distinguishes KnownProgressOrigin from UncapturedProgressOrigin explicitly. A genuinely retained native-session source prefix may contain item identity/extraction outcome without the collector source_id; missing/null collector provenance does not justify inventing an authority or admitting it as a complete live SourceItem. Such a terminal outcome is retained as one local notice with ProgressDetails.source_provenance=NOT_CAPTURED and no delivery job. Source payload identity/lookup matching and native message retry ownership remain required. Complete live source admission stays unchanged. A real retained-prefix session completion regression proves rejection outcome/dedup survive, the original partial row remains byte-equivalent as JSON data, and no Telegram delivery is enqueued.


SourceObservedClock validates the required aware string clock of PendingSourceItem, CaptureEvidence, SourceLifecycle and WebEvidenceItem through canonical parse_iso_time. It rejects a zone-less clock or numeric epoch string before source persistence instead of guessing timezone/unit. SourceService.stamp accepts only a declared aware datetime/string or its explicit local now branch; raw_item_path/write_raw_item likewise accept only aware datetime/string and use the same shared validator. Existing clock strings remain unchanged as evidence; the validation does not invent or rewrite their timestamp/precision. Real ingestion regressions prove both invalid clock forms leave no source/job rows.


CLI readiness now activates canonical Settings.registry/semantics/dependencies accessors and typed source/delivery/threshold loading. The earlier temporary syntax-only limitation for registry/semantics/dependency configuration is resolved. Syntactically valid files with missing tokens, metrics or dependency edges now fail their real schema check, while operator output still contains only boolean/exception-class diagnostics. Three actual incomplete-config file regressions verify rejection and input redaction; no permissive fallback schema or duplicated registry model is introduced.


EndpointVerification recognizes the existing optional checked_at clock declaration facets (_prec, _precision, their reason siblings, _reason, _bucket_size_s and _bucket_anchor). Only explicitly supplied facets trigger shared timestamp consistency validation; their absence leaves the original audit-label representation and unknown precision untouched, including current calendar-only verification labels. No confidence is inferred or rewritten. Exact declared aware-clock metadata roundtrips unchanged; contradictory precision aliases fail. Ordinary unknown configuration fields remain forbidden. These facets preserve the previously supported explicit configuration metadata rather than introducing a generic model/mapping escape.


The same declared JSON-number preservation applies to SourceLimits.budget_fraction, SourceFeed/HttpFetchPlan request_weight/requests_per_minute and SourceProviderBudget.tank_max_requests_per_s. Their strict int|float forms represent genuine valid JSON numbers without coercing integer capacities/counts to float; existing bounds/nonfinite/bool checks remain. Actual api_limits.yaml canonical roundtrip and an explicit mixed integer-budget/fractional-cadence source configuration roundtrip provide behavior evidence. Runtime rolling-budget arithmetic still uses its precise numeric model.


Known provider budgets now validate their actual required operational facets instead of accepting a nullable missing capacity: sec.gov/Coinbase/Kraken/Etherscan require RPS; Binance spot/usdm/Hyperliquid require per-minute weight; OKX requires endpoint budgets; Bybit requires period/capacity/budget/scope; DuckDuckGo requires declared client/backend/spacing/retry/access policy. A declared Tank capacity cannot exceed the existing api_limits.yaml half-documented-capacity policy, including endpoint budgets. Closed provider variants omit unrelated facets entirely; only the genuinely uncaptured Coinbase/Kraken documented RPS remains optional. Real SourceService startup regressions reject a missing SEC RPS or an excessive configured capacity before any source/job persistence, closing the nullable-capacity admission gap without guessing a fallback quota.

## Retained source-item exceptions by exact symbol/site

Sites below refer to `src/tank/contracts/source_items.py` in this checkpoint. Required nullable values and fixed variant discriminators are explicitly distinguished from omitted optional metadata. All capture identity, raw reference/hash, current version, source/relevance/extraction state and aware collection clock requirements remain strict.

| Symbols and line sites | Exact omission/default semantics | Behavior evidence |
|---|---|---|
| `PublisherCMSRecovery.observed_at_prec:43`, `PublisherCMSRecovery.observed_at_prec_reason:44` | Recovery evidence can lack the later canonical precision sibling; it never gains an EXACT occurrence. The original404 response identity/hash and recovery observation remain required. | test_source_cms_recovery.py: retained same-entry404 recovery and mismatched identity rejection |
| `IssuingAgency.name:48`, `IssuingAgency.raw_name:49`, `IssuingAgency.id:50` | These are actual alternative provider labels/identifiers. declared_publisher requires at least a nonblank label or an ID; an ID-only agency cannot fabricate a human organization name. | test_ingestion_contracts.py: malformed agency sibling isolation; test_source_organization.py: actual Treasury/SEC attribution |
| `IssuingAgency.parent_id:51`, `IssuingAgency.url:52`, `IssuingAgency.json_url:53`, `IssuingAgency.slug:54` | The API may omit optional hierarchy/link metadata or declare no parent. These values do not replace the required usable agency identity. | test_source_organization.py: retained Federal Register entries |
| `AnnouncementLabel.key:67`, `AnnouncementLabel.title:68`, `AnnouncementLabel.name:69` | Provider label formats differ; named_label requires a real key/name. Empty structured label records fail rather than defaulting a label. | test_sources.py: real OKX/Bybit announcement parsing |
| `SourceText.quoted:81` | A distinct quoted block exists only when the capture declares one; absent quote metadata is not synthesized from publisher text. | test_agents_routing.py: canonical item text projection |
| `DiscoveryBase.author:85` | Nonsocial materials may omit a contributor. Ingestion uses only the configured publisher display label in that case; SocialDiscovery separately requires the post author and authority stays independently resolved. | test_source_organization.py and test_sources.py: configured publisher/contributor precedence |
| `DiscoveryBase.published_at:89` | Required nullable declaration: the external source can provide no verified publication occurrence. The capture must still explicitly say null; missing the key fails. | test_ingestion_contracts.py::test_source_discovery_cannot_replace_configured_identity_or_invent_missing_publication |
| `DiscoveryBase.published_prec:90`, `DiscoveryBase.published_at_precision:91` | Only explicitly declared canonical/legacy precision is used. Alias conflicts fail; numeric units must be declared by the adapter. No absent field becomes EXACT. | test_sources.py: publication meta/ambiguous article precision and explicit epoch-unit regressions |
| `DiscoveryBase.published_prec_reason:92`, `DiscoveryBase.published_at_reason:93`, `DiscoveryBase.published_at_missing_reason:94` | Reason aliases describe missing/unverified publication; no reason is necessary for an already declared known precision. Their absence does not supply a publication clock. | test_sources.py: INCOMPLETE and UNVERIFIED_SEMANTICS timestamp cases |
| `DiscoveryBase.published_at_bucket_size_s:95`, `DiscoveryBase.published_at_bucket_anchor:96` | These apply only to declared bucket timestamps. Missing anchor stays unspecified; it never becomes START/END. Valid numeric/string bucket-size transport forms are checked by the shared precision codec. | test_sources.py::test_wire_timestamp_precision_survives_normalization |
| `DiscoveryBase.published_at_basis:97` | Only identified extraction paths emit this evidence label, such as JSON-LD, designated metadata or CMS detail publishDate. Generic RSS declarations do not invent a basis string. | test_sources.py: article JSON-LD and calendar metadata evidence |
| `DiscoveryBase.source_updated_at:98`, `DiscoveryBase.source_updated_at_prec:99`, `DiscoveryBase.source_updated_at_prec_reason:100`, `DiscoveryBase.source_updated_at_precision:101` | RSS/Atom may declare a separate publisher update. Absence means no update was supplied; it never substitutes for original publication. | test_sources.py: RSS/Atom publication and update parsing |
| `ArticleDiscovery.categories:161` | No category elements means the explicit empty category collection. A category element can omit its term, retained as null instead of inventing a label. | test_sources.py: RSS/Atom adapters |
| `ArticleDiscovery.subtype:162` | Only GitHub release/commit feeds declare this distinction; other article sources have no GitHub subtype. Source policy consumes it only for S25. | test_source_organization.py: actual retained GitHub commit feed |
| `ArticleDiscovery.publisher_cms_identity:163`, `ArticleDiscovery.publisher_cms_slug_url:164`, `ArticleDiscovery.publisher_cms_recovery:165`, `ArticleDiscovery.publisher_url_aliases:166` | Only a CMS-backed article/recovery supplies these facts. Empty alias lists mean no proven alternate URL; a changed slug requires the original CMS identity/content digest match. | test_source_cms_recovery.py: same-entry recovery, foreign-host and mismatched-content rejection |
| `ArticleDiscovery.author_organization:167` | A capture can explicitly declare publisher organization; otherwise configured feed/source or verified repository authority is considered. No display-name employer inference. | test_sources.py::test_declared_author_organization_precedence_and_identity_without_body_name |
| `FederalRegisterDiscovery.document_type:172`, `FederalRegisterDiscovery.pdf_url:173`, `FederalRegisterDiscovery.agencies:174` | Document type/PDF link/issuing agencies are independent optional API metadata. Missing agencies do not create issuer authority; malformed supplied agencies fail per-entry validation. | test_source_organization.py and malformed agency sibling regression |
| `FederalRegisterDiscovery.author_organization:175` | Only a declared capture organization overrides verified issuing-agency attribution; the absent field is not a generic Federal Register organization claim. | test_source_organization.py: actual issuing agency names |
| `ExchangeAnnouncementDiscovery.announcement_type:180`, `ExchangeAnnouncementDiscovery.tags:181` | Provider classification fields can be omitted; supplied scalar/structured labels validate in their concrete types. Missing tags do not become a made-up category. | test_sources.py: Binance/OKX/Bybit retained responses |
| `ExchangeAnnouncementDiscovery.business_published_at:182`, `ExchangeAnnouncementDiscovery.business_published_at_prec:183`, `ExchangeAnnouncementDiscovery.business_published_at_prec_reason:184`, `ExchangeAnnouncementDiscovery.business_published_at_precision:185` | OKX business timing is a separate optional declaration. It cannot replace pTime, and an absent/unverified value retains its missing-time reason. | test_sources.py: OKX separate business-time parsing |
| `GovernanceDiscovery.end:197`, `GovernanceDiscovery.end_prec:198`, `GovernanceDiscovery.end_prec_reason:199`, `GovernanceDiscovery.end_precision:200` | A proposal may have no verified end occurrence. end is required nullable; only its actual supplied precision is used, while vote/state/proposal identity stay required. | test_sources.py: Snapshot proposal state and closed-proposal revisit |
| `DiscoveryObservation.observed_at:222`, `DiscoveryObservation.observed_at_prec:223`, `DiscoveryObservation.observed_at_prec_reason:224`, `DiscoveryObservation.observed_at_precision:225` | Before acquisition binds the local first-seen clock there may be no collection observation. PendingSourceItem/CaptureEvidence/SourceLifecycle require an aware clock afterward. | test_ingestion_contracts.py: missing and unzoned capture clock rejection |
| `DiscoveryEnvelope.payload:231` | An index discovery awaiting a detail page has no self-contained entry bytes yet. XPostObservation narrows this to required bytes; no empty raw artifact is invented. | test_ingestion_contracts.py: pending capture rejection; test_xsensor.py: actual captured actor bytes |
| `PendingSourceItem.raw:339` | Null means detail capture is still pending. captured() refuses conversion until durable raw reference/hash exist. | test_ingestion_contracts.py: pending-to-captured evidence transition |
| `PendingRelevance.decision:372` | Literal null is the explicit not-yet-classified state, accompanied by a required pending reason. It is not a zero relevance score or guessed decision. | test_ops_progress.py: real Jev filter/bypass paths |
| `AlwaysRelevance.reason:378` | Agent-requested WEB evidence emits AGENT_REQUESTED_EVIDENCE; ordinary approved ingestion policy has no additional reason field. | test_ingestion_contracts.py: actual WEB producer and typed source producer |
| `SourceRoutingMessage.event_id:405`, `SourceRoutingMessage.session_id:406` | NEW_ITEM can await event/session creation; an existing binding can be carried independently. Present identities reject blanks/whitespace, and the routed aggregate must match its approved target. | test_ingestion_contracts.py: context identity rejection and wrong-item route transition |
| `SourceRoute.event_id:411`, `SourceRoute.p_same:412` | Both are explicit null only for NEW_SESSION. ADDED requires its existing event and probability; contradictory combinations fail. | test_ingestion_contracts.py: routed lifecycle roundtrip |
| `UnprocessedSource.state:424` | Fixed internal discriminator for a row with no stored processing result. It is never a default relevance decision or serialized fake status. | test_ingestion_contracts.py: unprocessed/classified/routed durable variants |
| `ClassifiedSource.state:431` | Fixed internal discriminator; required actual status and score still validate separately. | test_ops_progress.py: actual immutable Jev ledger and threshold behavior |
| `RoutedSource.state:440`, `RoutedSource.route_event_id:445`, `RoutedSource.filter_score:446` | The fixed discriminator labels an approved route. route_event_id is null only for a new session; filter_score is absent/null for ALWAYS sources, while classified relevance must retain the exact score. | test_ingestion_contracts.py: routing consistency; test_ops_progress.py: bypass/score/routing |
| `SkippedExtraction.decision:482`, `SkippedExtraction.reason:483` | Required nullable details distinguish a native turn without an observation from an explicit NO_EVENT rejection. Passing null intentionally clears that detail. | test_ingestion_contracts.py: NO_EVENT retry/completion clearing |
| `FailedExtraction.reason:488` | Required nullable failure detail permits an explicitly unknown reason, without carrying an old rejection. | test_agents_native_retry.py: terminal extraction updates |
| `SourceExtractionFields.extraction_decision:498`, `SourceExtractionFields.extraction_reason:499` | New/PENDING/approved source records have no extraction outcome detail. Explicit retry/running/completed transitions clear stale NO_EVENT/failure metadata. | test_ingestion_contracts.py: actual source/repository extraction replacement |
| `SourceEntityHint.name:518` | Identity is required; an optional human label is not an entity-ID fallback or resolution algorithm. | test_agents_routing.py: canonical supplied identity hints |
| `SourceIdentityHints.tokens_mentioned:522`, `SourceIdentityHints.entities:523` | These are independent optional hint namespaces. An absent namespace supplies no hint; exclude_unset serialization preserves absence rather than adding an empty namespace to the stored row. | test_ingestion_contracts.py: hinted source codec roundtrip |
| `SourceLifecycle.author_role:532`, `SourceLifecycle.original_author_role:533`, `SourceLifecycle.author_organization:534` | Each is required nullable because a contributor/account can be unresolved. None is not OTHER or ORGANIZATION and cannot authorize unconditional social routing. | test_xsensor.py: founder/organization/other real producer routing policy |
| `SourceLifecycle.observed_at_prec_reason:542`, `SourceLifecycle.observed_at_precision:543` | The local collection clock and canonical precision are required. The reason is literal null for this known local clock; the redundant older _precision alias may be absent. | test_ingestion_contracts.py: required aware collection clocks and source serializer roundtrip |
| `SourceLifecycle.observed_feeds:547` | The initial capture itself proves one feed, so a separate sighting list need not yet exist. observe() reconstructs only that proven origin and rejects counts greater than one with missing feed history. | test_source_requirements_update.py::test_duplicate_preserves_capture_and_counts_one_observation; cross-feed dedup cases |
| `SourceLifecycle.discovery_config_version:548`, `SourceLifecycle.discovery_code_version:549` | The pre-refactor direct-ingest writer did not require discovery headers (1216b4b SourceService.ingest); that information cannot be invented on retained complete source records. New CaptureEvidence and live ingestion require both. | test_sources.py: actual PostgreSQL direct-ingest fixture; required capture header migration |
| `SourceLifecycle.identity_hints:553` | Only explicitly supplied canonical routing hints create this object. Text/registry identity inference remains in RoutingSelector instead of being fabricated as stored hints. | test_ingestion_contracts.py: typed hint roundtrip; test_agents_routing.py |
| `WebEvidenceItem.published_at:781`, `WebEvidenceItem.published_prec_reason:783` | WEB capture does not claim an extracted publisher publication occurrence: required null with COLLECTION_ONLY declaration, optional missing-occurrence reason. Raw origin/clock/hash/generation provenance remain required. | test_ingestion_contracts.py::test_real_web_evidence_record_roundtrips_without_feed_source_assumptions |
| `WebEvidenceItem.observed_at_prec_reason:796`, `WebEvidenceItem.observed_at_precision:797` | The actual local collection clock has a required null reason; the older redundant precision alias can be absent because the canonical precision is required. | test_ingestion_contracts.py: actual save_web producer and aware-clock rejection |
| `WebEvidenceItem.observation_count:799`, `WebEvidenceItem.observed_feeds:800` | save_web initially emits only the proven captured web origin, without a separate sighting counter/list. Both remain absent/null together until an actual additional feed observation; partial pairs or contradictory counts fail. | test_ingestion_contracts.py::test_feed_duplicate_of_web_evidence_counts_feeds_without_replacing_origin |

Maps in this completed source workflow are limited to these explicit sites: `discovered_item`/`captured_item` and `parse_feed_source_item` decode external publisher/checkpoint/storage JSON; `CapturedItem.storage_record`, `PendingSourceItem.storage_record`, `SourceLifecycle.storage_record` and `SourceItemDocument` serialize the existing flat physical record; `captured_source_item` assembles two already validated immutable models and immediately validates the concrete inherited variant. `RoutedSource.routing_scores` is genuinely event-ID-to-probability association, with bounded values. `SourceIngestion.accounts` is normalized-handle association; captured/content lookup filters and enqueue payloads are the exact storage/queue boundary. No generic model/get facade is retained. `SourceIngestion.captured`/`same_content` return None only for no matching stored record, `observe_known` returns False only for no capture, and the optional HTTP client constructor choice means create/own one client versus use the explicitly supplied client; it is not a missing required service.


## Required provider budget variants (repair of7ee6528)

The independent omission review exposed that optional documented quantities in the first provider-wide DTO could disable half-capacity validation. SourceProviderBudget is now a closed discriminated union, not a bag of unrelated nullable quantities. Source admission branches on these concrete models, so required capacities cannot become optional at use sites. Unknown provider tags fail validation.

| Concrete variant | Required budget coordinates | Genuine omitted evidence |
|---|---|---|
| SourceSecBudget | Documented and assigned RPS | Additional reference/hash/review notes only; numeric documentation cannot disappear |
| SourceBinanceBudget | Documented and assigned per-minute weight, for spot/usdm | observed_header is omitted by the actual USD-M configuration; no invented response-header rule |
| SourceHyperliquidBudget | Documented/assigned weight, IP scope, named request weights and response-size rules | Extra inference notes/hash only when captured |
| SourceEtherscanBudget | Documented/assigned RPS AND documented/assigned daily caps, FREE tier/API_KEY scope | No optional computational capacity; absent extra documentation notes do not imply no limitations |
| SourceUncapturedRpsBudget | Assigned RPS and stated basis, for Coinbase/Kraken | Actual yaml does not record a numeric documented RPS. That absence stays None/omitted, not a guessed doubled ceiling; if supplied, the half-capacity check applies |
| SourceOkxBudget | Nonempty endpoint budgets, each with path/documented requests/period/assigned budget/scope | No generic top-level RPS is invented for endpoint-specific quotas |
| SourceBybitBudget | Documented requests, period, assigned request budget and IP scope | No absent quota coordinate is accepted |
| SourceDuckDuckGoBudget | Explicit client, duckduckgo backend, spacing/retry policy, zero cost and false boolean key requirement | No paid/backend/key fallback; integers do not coerce to the boolean access policy |

Eight actual SourceService startup omission/over-allocation regressions cover missing SEC/Binance/Hyperliquid documentation and each missing Etherscan daily coordinate, including removal of both daily fields. They fail before HTTP/source/job writes. Actual complete api_limits canonical JSON remains unchanged, while explicit Coinbase/Kraken uncaptured-documentation behavior is tested independently. Readiness now validates this full API budget schema as well. Half-capacity comparisons multiply the assigned value instead of dividing a potentially large documented integer.


## Required captured version facet at provenance producers

SourceService and Scheduler now require the canonical ResolvedVersionIdentity constructor facet, validated before service startup, instead of reading raw Settings version defaults. Production CLI source/backup commands pass settings.require_versions after run_db's existing resolve_versions capture. SourceIngestion uses the shared identity directly (the duplicate local IngestionVersions model is removed); filing-cache keys, polling discovery headers, probe results and reaction/scout writes use the same explicit facet. HTTP/scoped readiness and raw path/config intake remain separate and do not require a fictitious captured build. Owned synthetic fixtures declare their known test identity through source_support.fixture_versions rather than promoting raw Settings strings; the fixture is an explicit test declaration, not a runtime fallback. Scheduler's optional thresholds argument still means load/validate the required actual Settings.thresholds configuration when not injected; it never supplies fixture threshold values. The unused SourceService.locks map is removed; real acquisition/state/processing locks remain in SourcePoller. Actual source persistence proves current/discovery versions come from an explicit facet even when raw Settings.require_versions refuses an uncaptured identity. A scout write with a custom fixture root proves the explicit facet can be used without adding unrelated runtime configuration files.


## Explicit existing public-host client protection

The two existing Go public-host limits are now explicit configuration data, with exact provider/host pairs: binance.www→www.binance.com at60requests/60seconds, and binance.vision→data.binance.vision at10requests/1second. SourceClientProtectionBudget requires CLIENT_PROTECTION, NOT_CAPTURED provider documentation, an explicit null documented_requests, IP scope, assigned count/period and operator-policy basis. These are existing internal protections, not invented provider quotas or aliases for weighted API capacity. Unknown hosts, API provider tags, a supplied documented count or API_KEY scope fail this variant. In particular60/minute remains a rolling60second window and is not collapsed to1/second. Negative omission tests cover every required policy coordinate; the actual full API configuration still roundtrips canonical JSON unchanged. Go admission consumption remains quality-owned and separately qualified.


Calibration CLI now consumes the typed CalibrationReport and serializes the existing six-field terminal summary explicitly, retaining complete best_measured_routing metrics (the former decisions exclusion did not correspond to any actual metric field). No indexing facade is added. Owned Market/Event/Pipeline/Session constructor consumers now pass declared synthetic version facets; the production CLI MarketService passes settings.require_versions. EndpointVerification's clock guard now tests non-null declarations rather than model_fields_set: a model roundtrip that materializes null optional facets does not turn a calendar-only audit label into an assumed timestamp. An explicitly declared day bucket is validated through the named UTC-calendar transport adapter without altering the original serialized label; EXACT on a date-only label fails. The generic aware timestamp parser stays strict.


Scheduler.ensure_event_reactions is the structural implementation of the required ID-based reaction scheduling port. It validates a nonblank requested identity, loads the actual event, rejects missing/mismatched durable identity, then delegates the existing reaction workflow. Event tools no longer need a permanent generic event-dictionary scheduling port. Real repository tests prove invalid target identities cannot enqueue jobs; the reaction calculation/pack policy is unchanged and remains covered by its existing integration cases.


USD-M's two existing internal subwindows are explicit required client_protection_limits on SourceBinanceUsdmBudget, separate from its required documented aggregate weight budget: binance.usdm.funding shares250requests/300seconds across exactly fundingRate/fundingInfo; binance.usdm.statistics allows500requests/300seconds for exactly openInterestHist. Each declares CLIENT_PROTECTION/NOT_CAPTURED/null provider documentation/IP/operator basis. No doubled provider documentation or new quota values are inferred. Spot and USD-M now have distinct concrete provider variants so the required USD-M operation windows cannot disappear through an optional list. Missing groups, unknown paths, changed scope and invented numeric documentation fail. Go's verified zero aggregate weight for these operations remains request-planner behavior; the internal count windows still apply.


### Configured HTTP admission and atomic quota groups

The shared rolling-window engine now lives inward in `tank.request_budget`.
`sources.rate_limits` only reexports its exact singleton while peer imports migrate;
there is no second ledger or domain implementation. `HttpAdmission.admit` accepts
the actual built `httpx.Request` immediately before Python I/O. A caller that
queues Go transport never calls this port. Hostnames do not decide transport.

`ConfiguredHttpAdmission._etherscan` admits only HTTPS GET api.etherscan.io/v2/api
with one nonempty decoded UTF-8 apikey. The exact logical key is SHA-256 hashed,
without trimming/case changes, into the shared API_KEY scope across chain IDs.
Its required configured RPS and daily constraints reserve atomically.
Public explorer hosts return an explicit unconfigured outcome; unsupported API
routes, duplicate/missing/invalid keys, or missing provider policy return fixed
refusal codes without credential/query output. `_sec` shares the configured
sec.gov IP scope across subdomains. Unknown routes have an explicit unconfigured
outcome, never a fabricated quota.

`BudgetConstraint.host=None` means a primitive quota lacks a provider-block
association; it does not suppress its quota. `SharedRequestBudgets.acquire` requires a validated positive capacity.
Unregistered public routes are explicit policy outcomes and never call an
unlimited/missing-capacity primitive. Priority=0
is the ordinary fairness class; negative priorities receive existing urgent
precedence. Budget maps are associative indexes by validated complete quota
identity; deques are rolling weighted admission histories. `RequestLedger.connection`
is absent before opening/after closing, and a missing SQL row is an explicit
validation error. The serialized key preserves the existing cross-language
float window/capacity suffix and never partitions by owner/version.

`RequestLedger.wait_many` sorts the same serialized bucket identities as Go,
holds all reservations in one PostgreSQL transaction, and rolls back tentative
spends when any window returns a delay. Memory admission locks the complete
group in that order and appends every spend at one monotonic instant only when
all windows are ready. Waiting happens outside locks/transactions.

Evidence: 87 source/admission units pass in 2.01s; the actual PostgreSQL grouped
rollback test passes in 1.64s with zero skips. It proves the available long window
is not charged when a later short-window constraint blocks. Memory tests prove
the same boundary and retry behavior; actual built-client query tests prove
cross-chain scope and exact key hashing. Strict mypy passes the new engine,
adapter, contract and test module; Ruff checks/format pass. Consumer injection
into market/event workflows remains the corresponding owner's next activation;
this checkpoint alone does not claim all external attempts use the port.


The follow-up session fixture now declares the actual repository registry/source
configuration and reads the real analyst tool schemas. Its mock transport handles
the actual toolset-update operation separately from user-event submission; the
failure injection targets the intended event submission. Existing answer/session,
retry-barrier, attachment and prompt-provenance assertions remain intact.
Combined Ops/Telegram follow-up cohort: 94 units passed in 2.58s, zero skips.
This fixes required fixture inputs rather than restoring missing-config defaults.
Full typing of these large preexisting test modules remains open.


### Source request admission activation

`SourceHttpTransport.request` builds the actual client request, admits it, sends
with automatic redirects disabled, and repeats admission for each HTTPX-generated
redirect request. Its redirect count uses the client's explicit maximum.
A redirect into an exchange capture host is refused before Python I/O. Conditional
headers, body/form selection, client query parameters and HTTPX's redirect/auth
handling remain on the actual request. Source retry orchestration repeats this
transport, hence each actual attempt is admitted. The optional request-start
callback is invoked after the first admission, preserving capture's actual
request-start clock rather than pre-wait time.

`SourceService.acquire_budget` combines the strictest declared feed/source/host
minute constraint with configured provider windows into one atomic group.
Missing minute constraints genuinely mean no separately configured local minute
policy; they never remove a configured API window. A missing `request` argument
only means the explicit direct-budget helper must build the actual client request;
the active transport always supplies its already-built request. The Go queue
branch bypasses Python provider admission and retains only a separately declared
source minute policy. Generic unconfigured public HTTP remains allowed under
source policy; a known API refusal raises a fixed code before I/O.

The old provider-name/hostname comparison missed api.etherscan.io and could not
charge daily API_KEY quotas; it has been removed. Provider policies now resolve
inward from the actual destination and logical key. `HttpAdmissionPlan` is a
strict decision plus concrete immutable quota tuple, permitting the source's
additional constraints to reserve in the same transaction. It does not store the
native request or credential query.

Evidence: 89 source/admission units pass in 2.20s, zero skips. The real redirect
transport test admits both SEC subdomains into the same shared provider history;
the exchange redirect test proves the second destination never reaches Python
admission or I/O. Source quota proof now inspects actual rolling histories rather
than mocking an acquire call echo. Strict mypy passes the transport, policy and
admission tests; Ruff passes all touched source/tests. Existing source checkpoint
mapping/workflow and the remaining SourceService methods still require their full
domain migration and typing; this activation does not claim that work complete.


### Filing label projection

`SourceFilingRegistryView.from_document` now validates required token names/symbols
and entity kinds/names/aliases before the filing decision. Missing namespaces or
labels cannot quietly produce an incomplete relevance pattern. Its
`FilingRegistryDocument.tokens/entities` maps exist solely at the named external
configuration projection boundary; the actual pattern algorithm consumes strict
`FilingTokenLabel` and `FilingEntityLabel` objects. The entity's secondary
`name: str | None` is genuinely optional in the canonical registry. It is consulted
only when the required names list is explicitly empty. Regulators remain excluded
and ambiguous bare names retain the original context policy. The compiled pattern
uses a cached property bound to the service's captured immutable configuration;
the old dynamic getattr tuple/version cache has been removed.

Evidence: 136 source/contract units pass in 2.51s, zero skips, including missing
required namespaces/token labels/entity names and existing real SEC identity/date
boundary tests. The contracts module is strict-mypy clean; Ruff passes touched
source/tests. This narrow projection does not claim to replace the canonical
aggregate registry schema or resolution algorithms.


### Typed HTTP reader and conditional checkpoint boundary

`SourceHttpReader.read` returns the closed `FetchedContent | NotModified` outcome,
rather than a copied whole checkpoint dictionary. All polling/article/CMS callers
now consume its explicit payload and immutable receipt; serialization is confined
to their still-existing checkpoint persistence adapter. `FetchEvidence` contains
the actual response digest, status, URL and collection clock. `FetchReceipt` adds
optional HTTP cache validators. Only the evidence projection may be persisted
before parsing. ETag/Last-Modified commit remains governed by successful acquisition
and processing, preserving malformed-entry retry/dedup behavior. A 304 is an
explicit no-new-content outcome and creates no invented response receipt.

`ConditionalCheckpointWire` validates the narrow storage-wire fields before
projecting `HttpCheckpointProjection`; generic error-record JSON exists only in
that storage adapter and is reduced to a typed processing-error disposition.
The reader's application state contains no generic error records or workflow
checkpoint map. Absent ETag/Last-Modified are genuine optional response headers;
absent article/entry error collections are the actual fresh/transport-only
checkpoint phase. Their defaults never fabricate source identity, time or content.
The core projection's empty defaults represent a fresh conditional-cache state.

`SourceHttpReader.error` is absent until an actual failed attempt; reaching retry
exhaustion without an error is an explicit invariant failure. The four-attempt
retry cap and 2/4/8-second backoff remain unchanged. Retry-After can be absent,
an explicit finite nonnegative duration, or a valid publication-format HTTP date;
invalid values retain backoff rather than invent a date. Header lookups are
optional HTTP protocol fields. Request-start callbacks remain genuinely optional
for detail reads. Fixed HTTP/network failure messages keep query credentials out
of persisted/logged error text while retaining the actual HTTP response for the
existing verified 404-only CMS recovery path.

Evidence: 172 source/contract units passed in 4.28s before the final wire/core
projection split, including real CMS recovery, malformed siblings, conditional
GET and retry evidence tests. New reader/tests are strict-mypy clean; final scoped
verification follows this split. The exact pre-reader missing-signature/lookup
inventory is retained in ingestion-open-inventory.json (snapshot a97bde8); its
lookup counts are review candidates, not blanket defect classifications.
Full polling checkpoint/domain migration remains open.


Final reader activation proof: 172 source/contract units pass in 3.19s after the
wire/core split and explicit NotModified branches. Detail/history reads reject
unexpected 304 outcomes before accessing a required receipt. Initial acquisition
and optional head-refresh 304 outcomes retain their explicit no-content branch.
Required receipt digest/status/response URL/fetch clock accesses no longer use
empty receipt maps or invented current-time fallbacks. Strict reader/test mypy
and touched Ruff checks pass. Full pinned services verification follows the commit.


### Independent clean scheduling companion: operational test composition

`scheduler_support.operational_reaction_services` constructs the real MarketService,
MarketTruthService and Scheduler with the actual threshold configuration and an
explicit version facet. Its async exit stack closes scheduler tasks before market
flush and client cleanup. It supplies a real HTTP client whose transport refuses
undeclared external requests, rather than a no-op scheduler or fabricated capture
answer. The small dataclass contains resource handles, not serialized domain
records.

The helper's actual background-task lifetime proof passes with zero skips, and
both helper/test modules pass strict mypy and Ruff. Constructor consumer migration
will be paired with analysis' exact required ReactionScheduling activation.
This clean branch is based on parent 95ce994 and does not include the held
admission/7af27d3 cohort or the independent polling-state WIP.
The former `SharedRequestBudgets.configure(repo, settings)` capability guess
and unused Settings dependency are removed. Backend selection now belongs to
explicit composition, as described below. Owner labels remain observability only,
never quota partitions.


`ops_cards.plain` now takes an explicit scalar display value and handles only
None as absent. Zero and false retain their visible values rather than disappearing
through truthiness. Its 400-character default is the established presentation cap;
callers can choose their named field-specific cap. This closes the untyped helper
dependency in `ops_entry_errors`, which now passes strict mypy together with
`sources.errors`. Entry-error variant parsing chooses a declared digest/proposal
identity by key presence; empty declared identity values therefore reach that
variant's required validation rather than trying a different record shape.
Evidence: 164 Ops/source units pass in 7.26s, including new zero/false/markup cases;
Ruff passes touched source/tests. The rest of ops_cards remains open.


### Explicit admission backend and lifetime activation

`SharedRequestBudgets(backend)` requires the closed concrete choice
`MemoryBudgetBackend | RequestLedger`; unsupported backend objects reject before
binding. There is no repository-based fallback to memory and no capacity=None
admission path. Every actual acquisition revalidates its complete constraints,
including previously constructed model instances. Unknown public routes and
genuinely absent additional caps remain explicit `HttpUnconfigured`/constraint-plan
decisions at the caller boundary, not an unlimited engine operation.

`MemoryBudgetBackend` requires its operational clock and enforces weighted windows,
atomic groups and observed provider-host blocks. `RequestLedger` retains real
PostgreSQL transactions and shared host-block storage; its database and owner are
required. Both expose host-only block waiting without fabricating a provider quota.
An unregistered public URL still waits for an actually observed host block while
returning the explicit unconfigured-policy outcome. Host-block absence means
unblocked, which is distinct from declaring any provider capacity. The old
already-serialized single-window compatibility method has been removed; all
physical acquisition keys come from the same validated canonical identity.

`RequestBudgetScope._active=None` is solely the real before-bind/after-release
resource state. Acquire, host wait and block all raise when unbound. `bind` returns
the exact owned engine lease and refuses a second binding while that lease is
active. After the old lease is released, a replacement can bind; stale repeated
cleanup of the old lease cannot clear that replacement.
Runtime composition must supply an explicit backend and inject the scope into its
services; the core does not choose a backend by inspecting a structural repository.

`SourceService(..., budgets:RequestBudgets)` now requires its port. It validates the
actual source-configured API policy and constructs the configured resolver with
that required engine. Direct operator source checks bind a PostgreSQL ledger for
their exact command lifetime and release it even if service construction fails.
`HttpAdmission.block(actual_request, until, reason)` uses the same injected engine
as request admission; it never consults a global optional ledger flag. Live
provider admission remains separate from verified recorded-evidence transport.

Retained resource maps are genuinely associative: `windows` indexes weighted
history by complete quota identity; `locks` indexes concurrency resources;
`urgent.get(bucket, 0)` means no waiting urgent requests; `blocked_hosts.get(host)`
means no observed host block. Empty history on a new configured bucket is actual
absence of prior spend, not guessed capacity. `RequestLedger.connection=None`
is lazy/closed connection state, never a missing backend. Its host-delay SQL
zero for a missing block row represents the actual absence of a block. All clock
values used for memory admission must be finite; block instants require aware
timestamps. The production clock is real and the test clock is explicitly supplied.

`source_support.fixture_request_budgets` creates the real memory implementation
with an isolated virtual operational clock advancing only on waits.
`fixture_http_admission` uses the actual configured resolver and declared synthetic
version facet. It never fabricates an ADMITTED decision/provider or bypasses quota
arithmetic. The synthetic operational clock is not event/publication provenance.
Owned source fixtures explicitly choose this backend; cross-owner runtime/E2E/
market callers must merge their paired constructor companion before activation.

Evidence: 200 source/admission units pass in 3.34s; two actual PostgreSQL cases pass
in 1.61s, zero skips. They prove all-or-none rollback and cross-owner host-block
visibility with no quota-spend rows for unknown public routes. Memory cases prove
unbound refusal, effective host blocking, no partial spend, required capacity
revalidation and replacement lease ownership. Five engine/policy/helper/test
modules pass strict mypy. Full combined constructor cohort remains pending the
corresponding owners' canonical companions; prior ee97e89 remains the last pinned
487-unit/33-integration green snapshot.


The complete owned unit selection after this backend activation passes 496 cases
in 6.33s, zero skips (owned-explicit-backend-unit.xml). The two actual PostgreSQL
admission cases remain the focused physical proof; the full integration/E2E cohort
must wait for the coordinated required constructor/resource companions.


### Remaining source transports enter admission

`XSensor` now requires `HttpAdmission` and admits its actual built actor request,
including each redirect, before I/O. Its explicit 130-second actor timeout,
Authorization header, account/cursor body, raw response and cursor/degradation
rules remain intact. Actor requests require the declared HTTPS api.apify.com
destination; redirects cannot send a run to another host. Missing token still
fails before any request. The actor's uncaptured API quota remains an explicit
unconfigured-policy outcome; this does not invent a quota or start additional
paid runs. Existing configured S29 acquisition/cost/cadence ownership remains
the authorization boundary.

`SourceHttpTransport.send` is the actual-request entry point shared by reader,
actor and diagnostic adapters; `request` performs the standard feed request
build before that same loop. Source smoke probes now use this admitted redirect
transport with the configured headers and form/body. Public account diagnostics
use the actual built query request and wait for empirical host blocks. They
return strict response/failure models. HTTP status alone cannot establish an
account's existence or organizational authority: `exists=None` and
`identity_status=REQUIRES_OWNER_EVIDENCE` are explicit diagnostic semantics,
with the actual HTTP status retained. No source authority is changed by a probe.

Retained X adapter cases: token=None is legitimate when S29 is inactive and
causes an explicit error if a run is requested; optional successful-account/run
error text is provider telemetry. Failed-account omitted historyGap means no
reported gap and never permits cursor advancement, because that account remains
an explicit failed read. Run-level failed health requires nonblank run error or
a concrete failed-account reason; blank run telemetry cannot hide the real
account failure. Degraded health without a detailed reason produces the exact
RUN_DEGRADED status witness, never a fabricated cause. Cursor/report lookups are
genuinely associative, and missing requested reports/cursors become explicit
failures. Extra actor telemetry remains only in retained raw bytes. Required
successful-account post counts/history-gap facets and post identity/aware
publication fields still validate strictly.

Evidence before the final failed-run edge regression: 205 source/admission units
pass in 3.11s. Real transport tests prove unbound scope cannot start the paid
actor, cross-host actor redirect refuses before second I/O, and 200/404/503 public
diagnostics wait for observed blocks while retaining unknown identity. The
source configuration, common HTTP transport and X adapter are strict-mypy clean;
the X test module still has five calls into the currently untyped polling API,
which remains explicitly open. Probe result/workflow typing also remains open.


Final scoped transport proof: 206 source/admission units pass in 3.11s after
the blank failed-run evidence edge is closed; the three completed source modules
remain strict-mypy clean. This adds no cross-owner constructor requirement beyond
the already-coordinated SourceService budget port.


### Required source cadence controls

`SourceEscalationControlView` is the narrow persisted-control projection consumed
by `SourceService.is_escalated`. Source identity, lifecycle status, aware start
and expiry are required; missing status no longer becomes ACTIVE, and absent
clock fields no longer silently disable or fabricate a window. Confirmed and
cancelled statuses require their actual terminal clock. Terminal markers remain
genuinely absent during ACTIVE phase; a present terminal marker suppresses active
reads even if a retained row still carries the older ACTIVE label. The declared
window must be positive and remains half-open at expiry.

The reader validates all source-filtered rows through the declared projection and
rejects identity mismatch. It uses iter_all rather than truncating control
eligibility at an arbitrary 10,000 rows. Optional `now=None` selects the actual
service operational clock; supplied instants must be aware ISO/datetime values.
No current-time fallback replaces a missing persisted control clock.
The existing actual assessment/milestone producer-to-15-second-read tests remain
in the source cohort. Before final terminal/iterator checks, 208 source/admission
units passed in 3.19s; contracts strict mypy passes. Final scoped proof follows.


### Observed synchronization lifetime in serve

`run_with_synchronization` monitors the actual synchronizer future alongside the
owned supervisor. A permanent synchronization failure sets the real stop event,
drains the runtime, awaits background cleanup and propagates the failure.
A successful one-shot synchronization is normal and leaves runtime running until
its stop signal. Caller cancellation shields the runtime from abrupt cancellation,
signals graceful drain, cancels/awaits the owned synchronizer, and then propagates
cancellation. Simultaneous synchronization/drain failures are observed together;
neither disappears. CLI error groups expose only error classes, without arbitrary
provider/configuration payloads or a misleading successful stopped/CURRENT result.

The generic future result type is intentionally unused: this helper governs
resource lifetime and never invents or interprets a synchronization report.
`primary=None` means no caught task failure, and the shutdown-cancellation flag
distinguishes an owned cleanup cancellation from an unexpected service
cancellation. Exception identity de-duplication is a real collection of observed
failures, not a domain mapping facade. Transient retry policy stays inside the
existing synchronizer and is unchanged.

Evidence: 11 CLI/readiness units pass in 1.57s, including the actual uncaptured
Settings preflight guard, normal one-shot continuation, awaited cancellation and
dual-failure drain. Tests are strict-mypy clean and the helper emits no errors in
the full CLI strict check. The wider CLI still has its explicitly inventoried
untyped command/service boundaries. The permanent OpenMA effort/binding activation
must be paired by its owner before integrated verification.


### Held HTTP cohort: named web-page read provenance

`WebEvidenceItem.page_read_provenance` now imports the closed shared
`PageReadProvenance` union from contracts.page_evidence. It is genuinely optional
for historical captured rows that were written without this information. Omission
remains omission through storage serialization and SourceItemDocument; no current
clock, input digest or provider authority is backfilled. New page reader producers
must supply their actual live or verified-recorded provenance.

Verified recorded-body provenance must match the row's actual raw_sha256. The
field does not admit a speculative reconstruction variant or label reconstructed
bytes as original captured material. Live receipts preserve their declared
collection Timestamp. The test wire-writer port is the exact used interface to
the real historical save_web producer, followed by validation through the full
WebEvidenceItem schema; it does not fabricate a source row or provider result.
The producer itself remains analysis-owned and incompletely typed.

Evidence: six page/source scheduling cases pass in 1.72s with zero skips; source
item and page tests pass strict mypy and Ruff. Historical real producer output,
live roundtrip, verified-body roundtrip and digest-conflict rejection are covered.
This additive checkpoint belongs only to the held HTTP cohort descended from
4041eb68/477d58e. It does not activate HTTP consumers or alter the clean scheduling
cohort or independent source-state branch.



The independent required-scheduling caller companion now uses that real async
composition for all owned EventToolService constructors. It retains the original
extraction, publication, web evidence, watch-condition and source-cadence assertions.
The MAJOR source-cadence case now uses actual tool dispatch with a schema-complete,
explicitly unestimated impact horizon. It verifies ten durable benchmark POST
reaction jobs for the event and the existing real 15-second source reads.
Resources drain before repository shutdown.

Scoped proof: 216 owned constructor/helper units pass in 3.84s, and both actual
source-cadence integration cases pass in 2.45s. No external HTTP is allowed by the
real composition fixture. Full pinned owned verification is the next gate; these
results do not assert completed reaction measurement or full scheduler typing.


### Held HTTP cohort: required owned consumer composition

Owned MarketService constructors now receive the real configured admission port.
The operational scheduling fixture constructs it from the actual settings policy,
the same explicit version facet and an explicitly declared memory backend with
isolated operational time. The resource container exposes that same port to each
EventToolService's `LivePageAccess`; it does not fabricate admission or page data.
Fixture version override is a legitimate choice between the declared synthetic
default and an explicitly supplied captured facet.

The standalone CLI market command owns a PostgreSQL RequestLedger, configured
resolver, MarketService and shutdown lifetime. It never infers a memory backend
from a missing capability or silently skips a required close. Source constructors
retain their already-required budget port. The exact clean scheduling constructor
companion was merged normally; source-budget fields were preserved in the owned
merge conflicts, with both evidence ledgers retained.

Proof: all 512 owned unit cases pass in 7.12s, zero skips. Four operational helper/
page test modules pass strict mypy. Full physical/E2E qualification remains pending
the corresponding runtime, recorded-read and quality-owned constructor companions;
this checkpoint is not a claim that the held HTTP cohort is integrated.


### Held HTTP lifetime follow-up

`build_budget_backend(repo, *, owner)` centralizes explicit composition for the
two supported concrete repository backends. PostgresRepository selects a real
RequestLedger with its required database/owner; MemoryRepository explicitly selects
the actual process-local implementation. Every other repository implementation
raises TypeError, rather than being guessed to mean memory. The factory is a
composition boundary, not an admission fallback. CLI source and market commands
reuse it, so real isolated memory fixtures remain supported without accessing a
nonexistent database_url.

`RequestBudgetScope.bind` now refuses an already-active lease before replacing
anything. The regression proves the first backend remains active and receives
actual spend. After release, the replacement can bind, and stale old cleanup
does not disable it. This preserves orderly reload ownership without silently
abandoning a live backend.

Proof: 29 admission/CLI cases pass in 1.64s, zero skips; engine/admission tests pass
strict mypy and Ruff. Factory cases exercise explicit memory and unopened physical
resources; unsupported repository rejection is an explicit closed runtime guard.
Agents owns the CLI fake Source constructor update and production reuse of this
common factory. Combined runtime/physical verification follows that exact pair.


## Capacity reload admission review (paired activation held)

The parent review demonstrated that capacity was incorrectly part of quota
identity. The local correction separates `BudgetBucket(key, window_s)` from
`BudgetConstraint.capacity`. Memory history, locks, and urgency accounting now
share the same provider scope and declared period across capacity changes.
Capacity remains required, positive policy input for each actual admission.

The regression consumes two units at capacity two, verifies a capacity-one
request waits without spending its grouped sibling, then checks capacity three
admits only the third total unit and period expiry releases the original spend.
The actual SEC source workflow also retains both host/minute debits when its two
feed operations select different configured capacities. The selected source and
admission cohort passes 96 unit cases.

This is not yet an activated PostgreSQL or Go transition. Existing persisted
capacity-suffixed rows must retain their history, and old and new clients must
share one lock domain. Arbitrary period changes and a global minimum policy
across stale deployments are not established by this memory proof.

The new ordered migration `006_request_budget_policy_identity.sql` leaves every
applied migration unchanged. `request_budget_acquire_scope` receives raw scope
separately and locks/stores `request_budget_scope_key(scope, period)`, the SQL
JSON-array encoding. The legacy entrypoint validates its supplied numeric suffixes
and delegates to the same structured function. Under that canonical lock,
historical capacity variants are rekeyed without rewriting timestamps, weights,
or owners. Scope parsing splits only the final two numeric components, preserving
opaque scopes that themselves contain pipes or numeric suffixes.

Migration iteration uses an associative storage identity set; it recognizes only
the declared positive legacy numeric grammar and exact scope/period. This is an
explicit bridge for already-written quota evidence, not a generic record parser.
The transaction owns both rekeying and debit writes, so a blocked grouped attempt
rolls back all tentative changes. Period remains a separate constraint identity.

The scheduling test composition now receives an explicit shared request-budget
port. Owned source workflows pass the same actual engine to their source reader
and reaction services; isolated web/operations cases declare their own real
configured memory engine. No helper grants admission or allocates an unrelated
private quota ledger. The consumer selection passes 222 unit cases and four
actual integration cases, with zero skips; scheduling helpers and source-page
tests pass strict typing.
### Independent next branch: source workflow outcomes (not activated)

The independent source-state branch replaces actual acquire/drain count records
with `FeedPollOutcome`, and the aggregate with `PolledSource | UnconfiguredSource`.
Producers and aggregation use model attributes; no model implements dictionary
access. An explicitly empty feed configuration returns UNCONFIGURED rather than
claiming a healthy read. Required item counts/errors remain supplied by the actual
workflow, and a nonempty error list cannot validate as healthy.

Owned callers now consume attributes, retaining their original raw evidence,
restart, malformed sibling, concurrency and queue assertions. The existing
disabled/retired Binance-source regression now expects the honest unconfigured
outcome and still verifies no reconstruction or I/O. Source configuration and
the X tests pass strict mypy; the previous five untyped poll-call dependencies
in that test module are closed. 209 source/admission units pass in 3.24s.

This is deliberately not part of held head 477d58e. Two quality-owned E2E map
subscriptions still need a later paired attribute migration; full integration
is not claimed. Checkpoint ownership/state schemas and unknown prior Coinbase
flags remain the next work inside this independent branch.


### Independent next branch: explicit checkpoint operation ownership

Every checkpoint write now receives the actual `SourceDefinition` operation.
`SourcePoller.save` never derives source identity from key text or the number of
configured sources. Existing contradictory ownership rejects before persistence;
new checkpoint ownership comes from the required operation object. Opaque storage
addresses are addresses, not evidence of a publisher identity.

HTTP reads also receive that source context. The Go request's source_id and the
receipt callback use the actual definition, not key.split(). The application
must explicitly select whether a read persists feed HTTP evidence. The generic
reader accepts a required nullable evidence hook: None means a detail/history
read returns its typed receipt to its owning backlog instead of mutating the
feed checkpoint. Primary versus detail behavior no longer depends on matching a
generic key regex. The actor's request-start callback now marks the owning feed
after admission and before actual I/O, preserving cadence when a provider block
delays the paid run.

Evidence before final start-hook assertion: 211 source/admission units pass in
3.05s. The opaque/misleading-key test uses two configured sources and proves
ownership follows the explicit definition; contradictory rows refuse with no
write. Reader tests prove an opaque feed address persists evidence when explicitly
selected, while a feed-looking detail address does not. Reader/X adapter strict
checks pass. Full checkpoint record schemas and generic clock annotation remain
open in this independent branch; these changes do not imply that migration is done.


### Independent next branch: unknown prior Coinbase restriction flags

The available imported baseline (0d3e0cb2, retained in 1216b4b) already contains
the status_flags parser and the history backfill; 23aaac8b moved that diff work
into a thread. Its regression simulates an older snapshot by removing the
field. This establishes migration intent, not any actual prior flag values.
Copying the current flags into that history therefore lacks evidentiary support.

The backfill is removed. `CoinbaseRestrictionFlags` is a strict named five-flag
facet: None means an unreported prior/current value, never false. A flag transition
requires both observations. A newly observed flag alone updates the real snapshot
baseline without claiming a market-state change. Existing proven false-to-true/
true-to-false restriction changes still emit source items. If another real
instrument state changes while prior flags are absent, new change evidence writes
status_flags:null for that unknown side; it never inserts the current flags into
the past. Historical input maps and retained raw bytes remain unchanged.

Evidence: 214 source/admission units pass in 3.39s, including existing live flag
transition, restart/baseline and dedup regressions plus explicit unknown-history
evidence and input-immutability tests. The named facet is strict-mypy clean and
Ruff passes touched files. Full instrument snapshot/parser record typing remains
open; this is a bounded correction of a concrete evidence defect, not a claim
that the entire polling module is complete.

The paired transition has now been qualified on immutable combined checkpoint
9f35afaa: 515 owned unit cases passed in 6.06 seconds at its unchanged application
parent, 38 actual integration cases passed in 7.99 seconds, and the final combined
four-E2E plus ten PostgreSQL admission/transition selection passed all 14 cases in
11.49 seconds with zero skips. Quality's exact paired Go checkpoint passed all
354 race-enabled cases against actual PostgreSQL/ClickHouse, including legacy
capacity variants and twelve mixed old/new concurrent callers. Global Ruff and
formatting passed 336 files; six focused budget/helper/source-page modules passed
strict typing. Default mypy retained one analysis-owned Truth cache annotation
error, reported to that owner.

The new SQL tests seed two genuinely historical capacity-suffixed records with
unequal weights, compare timestamps/weights/owners before and after a blocked
group, and then verify a larger-cap request sees exactly the combined prior spend.
Adversarial scopes contain pipes and trailing numeric segments. Old serialized
and new structured callers contend on one canonical lock; capacity remains each
caller's explicit policy input. Distinct periods retain distinct constraints.
There is no claim of an authoritative minimum capacity across stale deployments
or continuity when a configured period is changed.

## Instrument snapshot ownership migration

contracts.source_checkpoints now owns the required instrument observation
schema, associative symbol-indexed snapshot, observed Coinbase restriction flags,
and the inward ReadCounter. read_report.ReadCounter is a temporary import
reexport; the source-only flag implementation is retired. Snapshot validation
rejects missing observation fields and mismatched symbol keys, hides raw inputs
in validation errors, and verifies venue/market ownership against the actual feed
before decisions.

InstrumentObservation.base, quote, and contract_type are required nullable
facts: OKX swap observations omit the base/quote projection, Hyperliquid
universe rows omit an explicit quote, and spot listings omit contract type.
InstrumentObservation.status is required nonempty text; an omitted provider
status fails validation instead of becoming an unknown healthy snapshot. They are not filled from symbol spelling or
market assumptions. CoinbaseInstrument.status_flags=None means no historical
flag collection; each nullable flag means that specific flag was absent. Only
two known observations of the same flag establish a flag change. Unknown prior
flags remain explicitly unknown in event evidence.

The pure sources.instruments.instrument_changes consumes validated snapshots
and actual InstrumentPage evidence and returns typed instrument discoveries.
It refuses an owner mismatch or a change without response evidence. Composite
action tuples group equivalent transitions; symbol maps retain associative
identity, and symbol-to-page-index maps preserve the actual supporting response.
JSON dictionaries exist only when hashing the established serialized identity or
rendering before/after evidence. The grouping digest and external identity retain
the existing canonical algorithm, including a dedicated parity assertion.

The poller validates both newly parsed and persisted instrument snapshots before
comparison and serializes models only at checkpoint storage. It no longer mutates
or copies current restriction flags into prior history. Tests declare synthetic
unknown currencies/contract metadata explicitly; this fixture constructor is not
a production source fallback. The selected source/contract cohort passes 152
unit cases; the new contract, pure comparison, fixture helper, and behavioral
tests pass strict typing. Full feed lifecycle checkpoints, pending batches,
parser wire variants, and recursive clock annotation remain open and are not
claimed complete by this instrument slice.

The actual source workflow regression additionally admits a valid mocked provider
response, stores its baseline, then observes a response missing required status.
The result is DEGRADED with no new source item; the prior instrument checkpoint
is exactly equal as a decoded stored document, and the validation report omits an
unrelated private raw field. Twelve focused instrument cases now cover this
workflow alongside identity/parity, required fields, and actual page ownership.


At canonical source-state checkpoint 29195f2d the full owned selection passed
533 unit cases in 6.86 seconds and 38 actual integration cases in 7.60 seconds,
with zero skips. Quality's two typed-outcome E2E caller changes preserved the
four actual workflows and passed all four cases in 10.85 seconds; the exact
consumer was merged normally into 10c7b8aa. These checks qualify the explicit
owner/outcome and instrument tranche, not the remaining full checkpoint/parser
migration. The ready quota/HTTP checkpoint remains separately preserved.

## X range boundary preservation

XSensor.scan now retains a previously checkpointed account cursor when that
account reports history_gap=True, even if the response offers a higher cursor.
Returned posts and their exact run bytes are retained; the next actual request
still asks from the prior boundary. Only a complete successful account report
can advance that existing range. Cursor-regression and missing-account/error
validation continue to run.

The no-prior-cursor branch retains the explicit baseline policy: it establishes
the first observed head, emits no historical posts, and still reports the actor's
gap. It does not claim a completed historical range before monitoring began.
The new regression performs two real MockTransport requests: the partial run
offers cursor 110 over checkpoint 100, the follow-up still requests 100 and
returns the previously missing 105, and only then commits 110. Exact response
bytes remain attached to the respective post observations.

All 20 X adapter cases pass with zero skips, including both new boundary/baseline
cases; source and tests pass strict typing and Ruff. This is independent of the
unactivated pending checkpoint/quarantine work.
## Reserved decision-watch caller companion

The Telegram followup watch lifecycle test now calls the typed
DecisionWatchCommand API with a validated WatchCondition and the actual arming
producer's GenerationProvenance projection. It no longer constructs EventToolService
through object.__new__ or supplies a fake settings bag. The fixture owns real
market/truth/scheduler services, a configured budget engine and live page-access
port, and drains them on exit. No financial assessment or MTA record is invented.

prepared_condition_ids=None is explicit because this standalone agent-owned
monitor has no rendered decision-card subset. The test verifies that a command
with no new decision proposals does not cancel the existing AGENT rule, that
invalid metrics still refuse, and that explicit disarm persists cancellation.
All 81 Telegram followup unit cases pass with zero skips. This companion is based
on the held card/decision activation 3a3d077a and must be integrated with that
cohort rather than independently activating its breaking interface.

## Atomic instrument snapshot and pending capture

SourcePoller._fetch_rows now prepares an InstrumentSnapshotUpdate without writing
the comparison baseline. Raw item bytes are made durable while preparing pending
rows. SourcePoller.acquire then writes the pending checkpoint and the prepared
snapshot in the same repository transaction. A raw capture failure leaves the old
baseline intact; a failure after either actual database put rolls both puts back.
The independently recorded HTTP receipt remains truthful I/O evidence and does
not acknowledge instrument coverage.

InstrumentSnapshotUpdate requires the actual S14 owner, checkpoint key, validated
snapshot/digest and captured version facet. Snapshot writes record those real
config/code versions. No-change snapshots still avoid redundant writes, and the
initial snapshot remains a baseline rather than a generated business event.

The real PostgreSQL oracle exposed missing publication precision in S14
discoveries. The producer now explicitly declares published_at=None and
COLLECTION_ONLY/INCOMPLETE, retaining its separate actual observed_at collection
clock. Snapshot comparison establishes when a change was observed, not when it
happened. No observation is substituted as a publication or transition time.

The bounded proof includes three actual MemoryRepository cases (raw failure and
failure after either checkpoint put), plus three PostgreSQL cases. Both SQL put
faults preserve the prior baseline across service restart and then emit the
Trading-to-Settling change. A separate later-ingest fault proves that an already
durable pending capture keeps its original item ID, observation clock and raw
bytes across restart. The source/instrument unit selection passes all 94 cases;
all three PostgreSQL cases pass, with zero skips. Three focused source/test
targets pass strict typing. Envelope success-code validation and the unfinished
quarantine workflow remain separate workstreams.


### Instrument protocol envelopes after snapshot durability

The new `sources.instrument_adapters.parse_instrument_page` validates named strict Pydantic success envelopes before producing `InstrumentSnapshot`; polling consumes the typed cursor directly. `parsers.instruments` is an explicit serialization adapter retained for probe/test callers that consume the existing wire snapshot. External provider objects are projected at this boundary because their additional descriptive fields are not workflow facts; canonical instrument observations are fully validated before leaving it. Snapshot maps are associative by provider symbol; Kraken's result keys remain the native identifiers. Duplicate symbols within a response refuse rather than silently replace an observation.

Required facts: Bybit `retCode` must be an integer zero (booleans/strings/null/missing refuse), `result.list` must exist; OKX `code` must be string zero and `data` must exist; Kraken requires both `error` and `result`, with a nonempty error list refusing. Binance requires `symbols` and refuses an error-code envelope. Coinbase requires its actual array response, Hyperliquid its `universe` array. Empty declared arrays/maps remain valid successful catalogs. Explicit declared null/wrong-type cursor, text and restriction flags refuse. Provider payloads are excluded from validation exception text.

Retained optional sites: `BybitSpotResult.nextPageCursor=""` represents documented absence in spot responses; explicit empty terminal values are mapped to `InstrumentPageResult.next_cursor=None`; it never supplies a success code. [Bybit instruments](https://bybit-exchange.github.io/docs/v5/market/instrument). `optional_text` returns None for uncaptured/nonapplicable base, quote and contract metadata (notably OKX derivative base/quote and Hyperliquid's absent quote); present text must actually be text. Empty `contractType` retains the existing alternative `ctType` projection; neither creates a contract type. [OKX instruments](https://www.okx.com/docs-v5/trick_en/).

`parse_instrument_page` retains the Hyperliquid sparse `isDelisted` omission rule: absent means the existing catalog label TRADING (not marked delisted), not a captured false flag or proof of execution availability. Official metadata examples omit it for BTC/ETH and supply true for a delisted asset; the official SDK's AssetInfo does not require it. Explicit null/string/numeric flags refuse. Raw response bytes preserve whether the flag was absent. This current protocol rule does not backfill historical snapshot flags. [Hyperliquid perpetual metadata](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals), [official SDK metadata types](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/hyperliquid/utils/types.py).

Coinbase flag omission remains an unknown observation, serialized only for actually declared booleans; no old snapshot receives today's flags. Existing trading_disabled=true overrides the catalog status. [Coinbase product schema](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-all-known-trading-pairs). Kraken's error list is an explicit success fact, not truthiness of a missing value. [Kraken asset pairs](https://docs.kraken.com/api-reference/market-data/get-tradable-asset-pairs).

Behavior evidence: the new strict test module exercises all six successful empty envelopes, malformed/missing/null/wrong-type envelopes and flags, sparse Hyperliquid metadata, cursor semantics and safe errors. Actual SourceService/MemoryRepository tests for Bybit and OKX seed a live snapshot, replay missing-code empty responses through restart, and prove DEGRADED/no removals/no baseline advance. A subsequent explicit successful empty response correctly emits REMOVED. The existing raw-failure/transaction-rollback/PostgreSQL restart oracle remains unchanged. The broader pending/quarantine migration remains separate and unfinished.

Pinned envelope qualification: exact 2bcc6374e93cec496701b31a633699aefb83c1ca remained unchanged and tracked-clean before/after the complete owned cohort: 570 unit cases passed in 8.07s and all 37 collected actual integration cases passed in 14.15s, zero skips. Evidence: instrument-envelope-owned-unit.xml, instrument-envelope-owned-integration.xml, instrument-envelope-owned-cohort.log in the isolated evidence directory. Independent strict adapter/test proof and affected-source default mypy passed; Ruff check/format passed for src/tests/scripts (372 files). Broad polling/parsers strict closure and the pending/quarantine workflow remain open.


### Acquisition identity under a declared frozen clock

New source acquisition batches now receive a genuinely new batch identity through new_id("batch"), independent of the clock, page digest and feed key. Publisher external IDs, dedup fingerprints, item IDs, raw bytes and observed clocks retain their existing semantics. Historical batches are not relabelled. The former clock-derived batch key could collide for different reads and merge the drain's per-batch completion bookkeeping.

The actual frozen-clock source oracle performs two different instrument reads at one collection timestamp, retains both rows in order, deliberately fails the first row while the later row succeeds, and proves the first batch remains a coverage barrier. A new service instance drains the original remaining item identity; both original item IDs exist exactly once, pending coverage then empties, and an additional drain does nothing. The same oracle runs against MemoryRepository and isolated PostgreSQL. This closes new-acquisition identity separately from the still-open historical quarantine workflow.


Bybit pagination review correction: omission is permitted only by BybitSpotResult. BybitPerpetualResult requires a declared string nextPageCursor even on an empty success page; an explicit empty string is the terminal fact. The earlier generic cursor default was too broad and is replaced before activation. The actual source restart refusal oracle now covers missing perpetual cursor as well as missing success codes; absent cursor cannot overwrite the previous snapshot, emit removals, or claim successful coverage.

Frozen-clock identity qualification: exact c399945e2001af769e6ecc58db98ac015a41d92a passed 571 owned units in 7.86s and all 38 actual integrations in 14.29s, zero skips, tracked-clean and unchanged throughout. This includes the new same-clock PostgreSQL acquisition/restart oracle. One obsolete direct test caller was migrated to the new batch allocator after its first pinned run exposed the removed token argument.

Final cursor companion qualification: exact 08c311a9066ad43e9cfc0f34ad2432d6868aac72 passed all 573 owned unit cases in 10.54s and all 38 actual integration cases in 14.23s, zero skips; the tracked snapshot remained clean and unchanged throughout. Evidence is bybit-cursor-final-unit.xml, bybit-cursor-final-integration.xml and bybit-cursor-final-cohort.log. Strict adapter/envelope/durability targets and src/tests/scripts Ruff/format passed before that pinned run.


Final spot protocol refinement: BybitSpotResult.no_spot_pagination rejects a declared nonempty cursor. Omitted and explicit-empty spot cursor values remain valid; perpetual cursors remain required and may contain an actual next-page token. The actual source refusal/restart test asserts no outgoing cursor request, no false removals and no snapshot advance for a contradictory spot declaration, followed by a valid explicit-empty observation. The generic successful-pagination unit fixture now declares PERP_LINEAR, the real supported protocol.

Spot contradiction companion qualification: exact c50959f717fc20389dfc44ed3cdc83e1e2bf838b passed all 575 owned units in 8.23s and all 38 actual integrations in 15.68s, zero skips; tracked snapshot remained clean and unchanged. Evidence is bybit-spot-cursor-final-unit.xml, bybit-spot-cursor-final-integration.xml and bybit-spot-cursor-final-cohort.log. Strict adapter/envelope targets and src/tests/scripts Ruff/format passed.


### Upstream X diagnostic integration after main 122d8c72

The functional main merge at 152f371e preserves the new actor diagnostic/retry behavior together with the existing typed X admission, requested-account ownership and gap boundary rules. The conflict resolution used actual Git index ours/theirs, not the ahead scope/quarantine worktree. The separately approved sources/errors.py companion adds named validated request/attempt/response/evidence-identity/document-reference fields. Captured response digest and byte count must match the actual stored body representation. A missing HTTP response remains a distinct empty identity unit, not a fabricated status/body.

This follow-up migrates the upstream diagnostic tests to explicit SourceDefinition/SourceAccount settings and required versions/budgets/admission. The unrelated legacy constructor-bypass Runtime tests are not reused: document retry tests construct the real production Runtime with all actual required ports, replace only its factory-owned DeliveryService with another real DeliveryService borrowing the guarded HTTP fixture, run the actual durable worker, and close the runtime and borrowed-client context. Global logging is isolated as a test side effect; no application service is replaced with a no-op.

The exact legacy failed-account-row assertion is corrected to match upstream main's explicit rejection: the successful account row remains accepted, the failed account's row is withheld, its prior cursor and failure evidence remain, and successful-but-gapped partial-read tests are unchanged. Diagnostic evidence dedup projection belongs to AccountEntryError.identity(); the Ops renderer/reporter delegates to that domain decision.

Required nullable facts remain explicit: SourceAccount.token_id=None declares that the synthetic OTHER account has no associated token. Diagnostics=None means that the failed operation supplied no captured diagnostic envelope; response=None/status=None mean that no HTTP response/status exists for that attempt. Optional wire/capture byte metrics and read/decode errors are uncaptured facts, not invented zero counts. document_manifest=None means that a notice has no generated diagnostic documents and is omitted from its old wire shape. The declared body/hash/count and attempt-count consistency validators do not fill missing required evidence.

The 93 selected X sensor/diagnostic/delivery/entry-error behavior cases pass with actual required composition and HTTP fixtures. The new x_diagnostics helper and actor runtime/test families remain open for full strict typing and the reproducible dependency/image cohort; this functional caller repair does not claim their five completion gates are closed. Ready f9 scope/quarantine and live-row WIP remain on their separate canonical branch/immutable snapshot and are not imported into this main-based follow-up.


### X decoded-wire credential guard

XSensor now validates the complete decoded JSON tree before the actor projection can ignore telemetry. contains_credential visits every key, scalar string and nested array/object; sensitive_text recognizes the actual known credential, URL-escaped text and valid base64 carriers. The old literal raw-byte check remains as an additional guard. A credential cannot hide in JSON Unicode escapes or an unconsumed nested/key field and then enter the raw evidence bytes.

For a tainted reply, redacted_projection is only an in-memory validation projection. It is never serialized as source raw evidence. diagnostic_projection separately normalizes the original diagnostic envelopes through the existing sanitizer and named validated diagnostic models, preserving correct redaction/count/digest facts; any residual credential material refuses. scan receives retain_posts=False for tainted wire and constructs no post rows and acknowledges no new cursors. Tainted replies with no valid requested diagnostic evidence raise a fixed safe error. Approved diagnostic-only failures still reach the Ops path, with an explicit credential-material failure marker. Untainted replies retain the exact original response bytes, including safe ignored telemetry.

The new narrow privacy test module covers direct, JSON-escaped, key, nested, encoded-carrier and unsafe discriminator placements; exception text contains neither the synthetic credential nor its escaped form. A safe nested Unicode/control reply retains byte-for-byte original raw evidence and its valid cursor. A tainted diagnostic reply produces only a sanitized failure/body and no rows/cursor advance. Actual SourceService/MemoryRepository also proves an escaped nested credential creates no stored source item/raw file and leaves the durable cursor at its prior value. The selected privacy/X diagnostic/sensor cohort passes 55 cases; the new privacy test module is independently strict-clean. Full actor-reader/package qualification still awaits the coordinated locked environment.


### Actor lock/export and isolated wire fixture

The actor requirements are exported from canonical root group x-reader with pinned uv0.11.7 using offline/frozen/only-group/no-project/no-header/no-annotate. The exact export SHA256 is 4c60a4a1fa0486de1b7b727673afed3db00dc6087fb204a442d5c7311e3be421, matching quality's prerequisite a4665c90. The actor Dockerfile now uses Python3.13.13 and requires hashes for every package install. No actor main/cloud job or deploy is invoked by this change.

The actual wire integration fixture keeps the application's declared HTTPS Apify/Telegram destinations and required source versions/budgets/account model, then uses an explicitly refusing loopback transport for only those known operations. It does not weaken production HTTP admission to permit a test localhost URL. The reader still makes actual HTTP requests to its owned local profile server, and the delivery path still emits actual multipart requests/receipts. Reader group installation and full actor/image proof remain pending the single coordinated shared-env sync; no independent pip install or duplicate environment was used.

Actor gate pinned qualification: exact 2232dc071e9c6df5671bc59dbac34c9639f78043 stayed tracked-clean and unchanged while 135 selected units passed in 7.33s (p90 0.101s, maximum 2.000s) and all six owned loopback/TCP/TLS integration cases passed in 8.88s, zero skips. Evidence is x-actor-final-unit.xml, x-actor-final-integration.xml and x-actor-final.log. The exact hashed closure built successfully as tank-x-reader-codex:2232dc07; offline containers verified Python3.13.13 and exact apify4.1.0/beautifulsoup4 4.15.0/httpx0.28.1, and pip check reported no broken requirements. Actor main/cloud/deploy was never run. The first image attempt failed before building because the legacy server CLI lacks --progress; x-actor-image.log retains that failure, and x-actor-image-v2.log/.exit records the successful unchanged-source retry. Full actor/X helper strict typing and scalar/default audit remain open; this closes the functional/runtime/dependency/image gate only.


### Instrument acquisition scope owns baseline and conditional coverage

InstrumentAcquisitionScope records the effective declared venue, market type, HTTP method, complete configured URL, JSON/form body digests and declared-header digest. It deliberately excludes config/code versions, cadence and quota policy. A new global configuration identity therefore does not erase the baseline for an unchanged request. Headers and bodies are fingerprinted without storing credential values in the scope. body_sha256/form_sha256 are required nullable facts: None means that transport body is absent, not an inferred default.

ScopedInstrumentBaseline requires its actual scope, owner, digest and version provenance. Polling compares only a baseline under the exact recorded scope key. The existing last-baseline pointer is retained for reporting, while scope-specific records preserve each scope's current baseline. Switching all-symbols to a symbol-filtered URL, body or header profile initializes a distinct baseline instead of declaring missing symbols removed. Returning to a prior scope resumes its own known baseline; it does not borrow the most recently configured feed's catalog.

When a scope changes, the previous pointer record is archived intact through InstrumentBaselineArchive and the typed repository. Historical inline/unattributed records are also preserved before their operational field is retired. original_record is the exact decoded storage document, a forensic JSON boundary rather than a domain state bag; provider raw files remain separate and untouched. original_scope=None plus NOT_RECORDED explicitly records that historical scope was never captured. An explicitly present malformed/null scope refuses validation; it is not silently classified as historical absence. The archive's source_id is the actual new artifact owner; original owner declarations remain unchanged inside original_record. captured_at and capture_versions describe this preservation operation, not the old baseline's creation.

InstrumentSnapshotUpdate contains the actual scope and a required list of preservation operations. Archive, per-scope baseline, last-baseline pointer and durable pending queue are committed in the existing transaction after raw durability. Archives retain their first successful preservation receipt when the same original record already exists. Changed business snapshots within one scope keep the existing item raw/before/after evidence behavior; full baseline archives are added for scope transitions rather than on every poll.

ConditionalCheckpointWire/HttpCheckpointProjection.cache_scope is genuinely optional for historical or noninstrument HTTP caches, with explicit null meaning unknown/unbound. Declared values must be a valid digest. Every S14 catalog acquisition supplies its required scope digest to SourceService.fetch; a mismatched/unbound committed cache suppresses conditional headers. A 304 without a matching committed scope refuses. cache_scope travels with the actual batch coverage and becomes committed with those response headers after drain, so it cannot attach old headers to a new acquisition merely because a fetch began. Historical instrument coverage without a recorded scope clears the binding explicitly. The fetch keyword's None default preserves the existing noninstrument transport contract; the instrument application never uses it as a missing required scope.

Pagination replaces only the actual cursor query entries and preserves every other declared query pair, including blank values and repeated forwarded fields. It no longer converts the query into a lossy dictionary. The provider cursor remains a continuation within the configured root acquisition, not a new global catalog scope.

Behavior evidence: actual SourceService/MemoryRepository and isolated PostgreSQL tests keep a constant two-symbol provider catalog while a restarted configuration selects only one symbol: no removal or source item is fabricated, and the old baseline remains under its original scope. Returning to all-symbols retains comparison continuity. A later genuine status change under a new global config version still emits the real STATE_UPDATE. Separate actual tests preserve a queued old-scope item's ID, original feed, collection clock, raw bytes and discovery versions across a filtered restart; archive an unattributed historical baseline without assigning it today's feed; refuse a new-scope 304; distinguish POST body/header filters; and preserve query context through pagination.

This scope-specific slice is independent of the qualified quarantine activation. Their combined consumer/coverage schema merge is still required before claiming a unified checkpoint gate; broader polling/parser/article full strict typing remains open.

Scope-only pinned qualification: exact a3bae4cc1264ba32c5e3ba3711a0047998f437a6 stayed tracked-clean and unchanged while all 582 owned unit cases passed in 10.32s (p90 0.306s, maximum 1.229s) and all 39 actual integrations passed in 20.24s, zero skips. Evidence: instrument-scope-final-unit.xml, instrument-scope-final-integration.xml and instrument-scope-final-cohort.log. Strict source-checkpoint/fetch/new test targets, affected service/poller default mypy, source comment policy and src/tests/scripts Ruff/format pass. The normal merge with the separate quarantine activation and its typed coverage/transport consumer adaptation follows; this scope-only proof does not substitute for that combined gate.


### Durable pending quarantine and ordered coverage

The active polling workflow now decodes stored pending arrays through operation-owned Pydantic batch/row/coverage variants. Malformed entries are preserved as PendingBatchQuarantine.raw_batch, with their original digest, row position, declared owner/item/batch/external facts and explicit DECLARED/ABSENT/INVALID states. The newly generated evidence ID names the forensic record; it never fills an unknown historical item or batch identity. Whole batches missing acquisition metadata become QuarantinedSourceBatch and cannot be treated as an empty executable batch.

SourcePoller.queue_rows constructs SourcePendingBatch with the actual SourceDefinition owner, required collection clock and immutable version facet. SourcePoller.process_pending_row returns an explicit completion or failure. CompletedPendingRow binds the original preassigned item/capture metadata to the real ingestion result; completion matches the full original row fingerprint instead of assuming historical batch IDs are unique. SourceIngestion remains the typed core behind SourceService.ingest's explicit wire adapter. Existing article/CMS fallback and dedup behavior remain in their actual producers.

PendingCheckpointState.coverage_commit permits only the contiguous completed prefix. A row quarantine remains a barrier even after every executable row (including later acquisitions) has finished. SourcePoller.acquire and drain retain DEGRADED and do not advance last_success_at across that barrier. Pending coverage commits are explicit HttpCoverage, GovernanceCoverage, InstrumentCoverage, CapturedInstrumentCoverage, SocialCoverage or NoCommittedCoverage values, not recursive dictionaries of inferred progress. Raw archival happens before pending/snapshot commit; instrument snapshot and queue writes retain their qualified shared transaction.

QuarantineArchiveRepository stores immutable forensic evidence and recovery receipts in the existing checkpoint collection under quarantine:<evidence_id>. Its typed repository uses the actual feed lock and transaction; the main pending update and archive writes roll back together. Stale retries cannot erase an existing recovery or replace old raw evidence. The first detected clock/versions remain immutable; the archive is written again only when an actual new recovery changes it. SourcePoller.save annotates only its enumerated operational clocks and never recursively rewrites publisher/quarantine data.

Recovery requires a real captured item and real persisted matching item, source/external identity, raw reference/digest, collection clock and discovery versions. A dedup result with a different item ID cannot masquerade as a newly materialized capture. PendingCheckpointState.link_completions also requires the matching completed row to belong to a strictly later acquisition in the durable backlog; a same/earlier acquisition cannot resolve the defect, including at an equal declared clock. PendingRecovery records that actual acquisition batch ID. The original raw batch, original declared identifiers and defect reason remain after recovery. Whole-batch unknown metadata is conservatively unrecoverable through row linkage.

Retained exception inventory for this slice:

- PendingCaptureMetadata precision aliases are optional only because retained rows use either observed_at_prec or observed_at_precision; its validator requires an actual collector declaration and a real aware observed_at. Discovery config/code identity is required. Raw fields are a validated pair; PendingSourceItem.raw=None exists only before article/detail capture. Publisher publication time remains genuinely nullable; collection time is separate and never supplies publication time.
- PendingBatchMetadata acquired_at_prec/prec_reason are optional spelling facets around the required acquired_at_precision=COLLECTION_ONLY. PendingBatchWire.source_id=None represents historical absence only; explicit conflicting/null/invalid declarations are quarantined, with the original declaration retained. New batches always write the actual owner.
- PendingQuarantineIssue.recovery=None means unresolved. Declared IDs are nullable when absent or unusably typed, with separate declaration status and preserved raw value. row_position=None denotes a whole-batch defect, with NOT_APPLICABLE row declarations; no row is invented. PendingRecovery identity/raw/version/clock fields and acquisition_batch_id are required.
- SourcePendingBatch/PendingBatchWire.quarantines default empty only for a batch with no recorded defects. decode_pending_checkpoint uses an absent pending_batches as the unseen/no-backlog state; present malformed values are quarantined. NoCommittedCoverage is the explicit empty unit for an intermediate captured page with no authorized progress update.
- HttpCoverage header values are required nullable facts: a response can lack ETag/Last-Modified. discovery_checkpoint=None means that protocol supplies no discovery cursor. GovernanceCoverage may omit header/cursor fields when valid proposal refresh facts coexist with publisher entry errors and HTTP-head advancement is withheld. CapturedInstrumentCoverage optional headers/schema preserve historical inline snapshot declarations without inventing them. SocialCoverage.account_cursors is associative by the actual account ID.
- SourceCadence's four optional clocks represent stages that have not happened or genuinely absent retained markers. poll_remaining uses only a recorded attempt or recorded successful read; detail_remaining uses the recorded absolute retry deadline when the old failure clock is absent. Neither fabricates now as a missing historical event.
- SourceDegradation's optional failure/history/scan facts describe observed independent failure channels; empty error collections mean no recorded failures. Historical UnverifiableArticleMarker and UnspecifiedArticleMarker preserve their actual narrow retained shapes. ArticleFailure requires real HTTP status/body digest/URL/collection clock; FailedSourceRow.article_failure=None is a non-HTTP processing failure with no fabricated HTTP evidence.
- SourceDuplicateCache's optional bounded list is a performance cache; observe_known/persisted ingestion remain authoritative. ReadCounter is zero only when the entire counter was absent (no recorded reads); a declared counter must validate both nonnegative fields. count_read's n=1 is one actual attempt, while detail batches supply their computed count.
- The snapshot/pending JSON documents, raw forensic JsonValue and external flat capture adapter are explicit storage/wire boundaries. Domain batches, completions, recovery, cadence and degradation use named models. Associative maps remain for symbol observations, account cursors, quarantine evidence identity, URL-indexed failures and actual source/account/lock/task lookup. with_archives.get retains the current validated evidence only when no durable archive exists yet; merge_quarantine retains an already durable recovery over a stale unresolved retry.
- Conditional cache headers are suppressed while a quarantine is unresolved, independently of durable committed headers. This permits a real fresh read instead of a 304 starvation loop, while retaining the prior committed ETag/Last-Modified and withholding later coverage.

Behavior evidence includes actual SourceService/MemoryRepository and isolated PostgreSQL. A malformed row keeps its raw file bytes and prior committed head; restart preserves the identical archive without HTTP. A later unrelated publisher item can be fully ingested while every executable row becomes empty, but the quarantine, DEGRADED state, prior head and coverage barrier remain. Only a genuinely fresh matching capture links recovery and commits the ordered prefix. The archive still contains the original defect and raw batch; repeat drain performs no I/O or duplicate ingestion. The same core test is run on PostgreSQL, plus an actual rollback after both archive and main writes. Historical whole batches without acquisition metadata remain preserved and blocked; they are not silently deleted and relabelled as rediscovered.

This is a bounded pending-lifecycle activation. Full polling/parser/article/orchestration strict typing remains open; only the new shared contracts, operation codec/repository and narrow new test module are independently strict targets. The separately reported instrument acquisition-scope binding defect remains queued; this slice does not claim that comparison ownership problem is closed.

Pending activation pinned qualification: exact dbd15135a720211ca4c8a35b7c1a38b2ad82dd69 stayed tracked-clean and unchanged while all 587 owned unit cases passed in 10.36s (p90 0.326s, maximum 1.289s) and all 40 actual integration cases passed in 16.39s, zero skips. Evidence: pending-quarantine-final-unit.xml, pending-quarantine-final-integration.xml and pending-quarantine-final-cohort.log. Strict source-checkpoint contracts/operation codec/new test module pass; affected SourceService/Poller default mypy passes. Whole default mypy on this ancestry has only the pre-existing analysis/truth.py:5890 condition-cache annotation error (128 checked files), reported to the parent rather than edited across ownership. Source comment policy and src/tests/scripts Ruff/format (374 files) pass.

Combined checkpoint consumer: the normal merge retains both full quarantine and scope implementations. InstrumentCoverage.cache_scope is nullable only for a genuinely unrecorded historical instrument batch; typed drain explicitly clears the binding for that case instead of carrying a newer scope backward. New instrument batches always supply the actual digest. SourcePollingPort.fetch declares the cache_scope policy keyword, and SourceService preserves both scope mismatch refusal and unresolved-quarantine suppression in its typed HTTP projection. All other domain variants retain their prior required fields and the two complete evidence ledgers.

Combined pinned checkpoint qualification: exact 21b74d75582be5409accc081fd86f68ae0878dd9 stayed tracked-clean and unchanged while all 594 owned units passed in 9.91s (p90 0.300s, maximum 1.384s) and all 41 actual integration cases passed in 19.27s, zero skips. Evidence: combined-checkpoint-final-unit.xml, combined-checkpoint-final-integration.xml and combined-checkpoint-final-cohort.log. Five independent strict targets (shared checkpoint contracts, operation codec, HTTP fetch, scope tests and quarantine tests), affected SourceService/Poller default mypy, source comment policy and src/tests/scripts Ruff/format (375 files) pass. This qualifies the two bounded checkpoint workflows together; it does not claim full strict closure of the remaining owned source, scheduler, delivery or Telegram families.


### Combined current source, privacy and actor checkpoint (2026-10-25)

Pinned application head `15e453e316a9a77aeedb12f065c19aee2986e7d1` joins the current parent, immutable scope/quarantine cohort, privacy checks, packaged actor and native callback fixture correction through normal merges. The tracked tree remained clean and unchanged during verification. All 662 owned unit cases passed in 14.32 seconds, with p90 0.322 seconds and maximum 2.360 seconds; all 51 collected actual integration cases passed in 25.15 seconds, with zero skips. Selection includes integration cases in both `test_x_reader.py` and `test_x_reader_integration.py`, preserving the 86/200-account aggregate byte, memory and truncation assertions.

The initial combined run retained its 47-pass/four-failure evidence. Those four failures were missing explicit fixture arguments after the diagnostic helper migration. The correction supplies the actual temporary Settings, guarded borrowed HTTP transport and required notice fixture path; it adds no production signature fallback and changes no stress assertion. Evidence remains under `.refactor-evidence/current-all-owned-checkpoint*`; the passing pinned rerun uses the `-v2` prefix.

Ruff check, Ruff formatting of all 403 active files and the source-comment checker passed. Independent strict checking passed for `contracts/source_checkpoints.py`, `sources/checkpoints.py`, `sources/fetch.py`, `test_source_instrument_scope.py` and `test_source_pending_checkpoints.py`. This is bounded qualification of those contracts and behavior, not closure of the actor semantic/type inventory or broader scheduler, Telegram, CLI and source parser migration. Actor request/response/diagnostic domain records and the separately preserved instrument-row variants remain next work.


### Strict source fixed integer declarations

`PublicationReceipt.contract_version` now consumes the canonical `OneInt`; `UnconfiguredSource.items` consumes `ZeroInt`. The exact input declarations `ProgressNotice.format_version`, `PublisherCMSRecovery.original_http_status`, `InstrumentCoverage.instrument_snapshot_schema_version` and `CapturedInstrumentCoverage.instrument_snapshot_schema_version` use the shared `require_integer` before their literal constraints. Integral floats and booleans cannot impersonate protocol/schema integers. The stored values and accepted real integer versions remain unchanged.

`CapturedInstrumentCoverage.instrument_snapshot_schema_version=None` and omission remain genuine historical absence, preserving omission with `exclude_unset`; no old snapshot receives an invented version. `ProgressNotice.format_version=2` is a local writer format default, retained in this narrow scalar correction; this does not close the separate full default audit. `QuarantinedSourceBatch.may_commit_coverage` returns the program constant `False`, not a raw input field, and needs no scalar decoder. No other fixed boolean input declarations exist in these four source/progress contracts.

Seven complete boundary records exercise actual model validation and JSON serialization, refusal of float/bool aliases through both Python and JSON intake, and historical unknown-version omission. The combined source/checkpoint/progress selection passed 48 units in 7.08 seconds, zero skips. Independent strict checking passed the four changed contracts and the new scalar test module; Ruff check and formatting passed. This follows the separately pinned 662-unit/51-actual checkpoint without changing provider evidence, quota policy, actor wire output or production signatures.


### Explicit missing native resource follow-up reader

The reader companion normally joins parent `acd149957a14be3bcc79a2dcefe40f2aa456ecee`, the qualified native lookup cohort and inward lifecycle relocation `23aad359eeaa7dda30834e8162065225da586361`; it does not import the separate actor candidate. `telegram_inbox.remote_session_missing` validates the exact `NativeSessionMissing` DTO from `contracts.native_readback` before allowing a stored missing observation to affect selection. It verifies the local session identity, rejects false/numeric flags, absent required identity/discriminator/flag, contradictory status/archive declarations and unexpected payload fields. It strips only the four separately recorded `checked_at` collection timestamp facets at the storage adapter boundary; these clocks are not lifecycle facts. The original stored row remains unchanged.

`closed_reason` now returns NO_SESSION/GONE for an actual retained MISSING observation, so `item_session` prefers another genuinely live candidate. Its existing closed-session fallback remains deliberate: an item with only a closed/missing session still carries its original session identity to the answer path, which explains the reason instead of inventing a new continuation. A recorded termination receipt retains TERMINATED precedence; a missing resource never authors termination/archive fields. The actual SessionService question path plus actual Telegram delivery proves repeated questions against the retained observation perform zero native HTTP, send one GONE answer, keep NO_SESSION status and preserve reply pairing/durable queue idempotency. The Ops selector test proves live preference and the truthful closed fallback.

Exact optional lookup rationale: absent/null `remote_state` means no retained native observation, not an affirmative native existence claim; the normal live continuation path still queries the provider. Older non-MISSING state records remain on the existing status/archive reader path. The discriminator lookup is optional only to distinguish those older observations; any declared `missing` field invokes strict missing-model validation. The rest of the broad Telegram lifecycle mapping/default/type migration remains open and is not described as closed by this helper.

The initial combined fixture selection retained 14 failures in `.refactor-evidence/native-missing-initial-fixtures.log`. Synthetic GET replies were completed with the canonical native session fixture; tool-update mocks now retain the posted tools and return their resolved actual readback instead of a fabricated successful hash. The obsolete 404→TERMINATED assertion now expects NO_SESSION and explicitly forbids status/archive fabrication. A 400 update rejection is no longer silently accepted: all 400/429/503 cases assert their actual HTTP error, one attempted update and no tools stamp; service-level swallowing was removed by the native owner and caller retry policy remains responsible for transient retries. Native owner confirmed this contract.

Pinned combined application head `e81f451383f635413217872196a2dcbfdc9835b1` passed all 679 owned units in 15.25 seconds (p90 0.312; maximum 2.093) and all 51 actual integrations in 21.21 seconds, zero skips, with clean tracked HEAD unchanged. Evidence prefix `.refactor-evidence/native-missing-owned`. The focused followup/retention/readback selection passed 138 cases before the final termination-precedence assertion; the full pinned run includes that assertion. The new missing-boundary test module is independently strict-clean; default mypy on the affected Telegram module passes. Ruff check/format of all 413 active files and source-comment policy pass. Quality's qualified E2E/CI companion `730b40404845f206d4561bb79f3d6efd43fb834e` is joined normally. Full Telegram/session source typing remains explicitly unfinished.


### Native archive-clock companion reader qualification

Normal merge `65461e22cdfa240d6f9f458373ffdd946b897b5d` joins the qualified reader cohort with native owner's `274555eff98c47268c383135e6903dc0bb9740bb`. The inward state model now validates a present archive clock through the explicit aware ISO parser, preserving the original wire string; None remains the provider's explicit not-archived fact. Empty, malformed, date-only or naive declarations refuse before local mutation or native input publication. No missing observation receives a timestamp/status default.

All 150 affected native reader/tool, Telegram/Ops followup and retention unit cases passed in 3.89 seconds. The separately guarded exact-head actual selection collected all four E2E cases and passed them in 13.33 seconds, zero skips; its 13 Ops helper units also passed in 2.70 seconds. Tracked HEAD and source stayed unchanged during that run. Evidence `.refactor-evidence/native-archive-reader-unit.xml` and `.refactor-evidence/native-archive-followup-actual*`. Independent strict checking passes the inward native contract and missing-boundary test module. Earlier broad 679-unit/51-actual proof stays attributed to its original e81 head; it is not relabelled as a run of the new clock validator. The instrument-row work was preserved separately in immutable stash `8fda3969e0bfe69040a84aaf9f9834650f8efed4`, without dropping original `95a67052dd79807f3f91db45acc01876ece870e4` or its third-parent test.


### Duplicate raw JSON keys cannot hide X credential material

The opt-in shared decoder `b96199a8` is joined normally on the clean main/native parent base. `XSensor.posts` now calls `decode_unique_json` on the original `response.content` before credential inspection, typed projection, diagnostic fallback, raw retention or cursor acknowledgement. Duplicate keys at any object depth, including escaped spellings of the same key, refuse. The Source adapter exposes one static error, `Apify X response JSON is invalid or ambiguous`, for malformed/ambiguous raw JSON instead of echoing key names or payload. This does not rewrite raw bytes, parse JSON-looking opaque text as extra objects, change legacy decoder behavior globally or alter valid output/cursor rules.

The reproduced prior defect used only the synthetic test credential: an escaped token in the first telemetry value/key could be overwritten by a duplicate field before the full-tree scan. Actual XSensor emitted a retained row and cursor 105; actual SourceService likewise retained the raw payload and advanced from 100. Evidence `.refactor-evidence/x-duplicate-escaped-privacy-review.json` and the initial red test log `.refactor-evidence/x-duplicate-wire-initial-red.log` are preserved. All eight new negative controls failed before the fix while the valid unique control passed, demonstrating actual privacy/acknowledgement effects rather than a mocked validator echo.

New controls cover top-level duplicate values, nested duplicate values, escaped equivalent duplicate key names and discarded credential-bearing escaped keys. Both XSensor and actual SourceService now refuse all four variants, retain no item/raw file, keep the original cursor and emit the safe error. A unique JSON reply with formatting, Unicode and distinct case-sensitive metadata keys retains exactly the original bytes and observed cursor. Original escaped/key/nested/Base64 privacy controls remain intact. The new test module passes independent strict checking; affected source default mypy passes; the combined primitive/X/privacy/diagnostic selection passes all 88 units in 6.86 seconds, zero skips. Actual actor wire/stress qualification is recorded separately after the immutable candidate run.

Open architecture item retained from native review: SessionService still imports `telegram_inbox.closed_reason` eligibility policy and wording. The full Telegram/Session migration must move typed eligibility/reason facts inward and make Telegram formatting consume them; this narrow privacy correction does not expand or relabel the already qualified native cohort. Row/catalog unique-fact adoption remains a separate owned tranche.


### Duplicate-wire X compatibility proof

Exact immutable application head `49eb73e7e20b684db21cea012769b4b4f3a58620` also passed all 33 reader-file units in 2.64 seconds and all ten actual actor integration cases in 17.49 seconds, zero skips, with tracked tree and HEAD unchanged. Selection includes both reader files, retaining the 86/200-account memory/byte/truncation assertions and actual loopback/TLS/body-deadline behavior. Evidence prefix `.refactor-evidence/x-duplicate-actor-actual`. This compatibility run uses the existing actor source on the clean main/native base; the separately qualified actor model/image/package candidate is neither imported nor relabelled here.
### Standalone actor request and result domain migration (candidate)

The standalone image owns `actor_contracts.py`; it imports Pydantic from its already frozen Apify dependency closure and never imports `tank`. Docker explicitly copies that file. `ActorRequest` validates monitored handles/cursor ownership before collection. `PublicPost`, separate `AccountSuccess`/`AccountFailure`, `DiagnosticRequest`/`DiagnosticResponse`/`DiagnosticAttempt`/`ProfileDiagnostics`, `CollectionReport` and `ActorSnapshot` replace computation over structured dictionary bags. `collect` consumes the validated request and returns typed posts/report; `fetch_profile` authors named attempts and typed failure evidence. The discarded `known_ids`/`cutoff` arguments and `_accountState` ad-hoc request bag were unused historical interfaces and are removed. SDK input and final output serialization remain explicit external boundaries. `ProfileFailure.diagnostics` serializes typed evidence solely for its existing external exception/report consumer; collector decisions use `.evidence`, never a mapping facade.

Exact retained cases: `ActorRequest.mode="fetch"` is the sole operation, concurrency 6 is the declared bounded request policy (valid range 1–8), absent cursors mean this acquisition has no prior account cursor, and nullable requestId means the caller supplied no correlation ID. Cursor and post maps are associative owner/post-ID indexes. Diagnostic response headers are the allowlisted external header map; Content-Encoding omission means identity HTTP encoding, Content-Length omission/invalid syntax means no asserted total and keeps omittedWireBytes unknown. readError/decodeError are nullable when no such failure occurred. serializedBodyLimitBytes is nullable only for standalone evidence calls without an aggregate run allocation; actual collection supplies its account share. Attempt response/status omission means the attempt did not retain a response (e.g. connection failure); no HTTP status is fabricated. AccountFailure.diagnostics is null for failures before a captured retry report, such as stale samples. The trace event map is an external HTTPX callback containing runtime objects; its stream is checked against the precise async-close protocol before owned cleanup registration, with no Any/cast.

`run_deadline` retains the existing 100-second local safety cap, 1/5-second retry delays and two-second shutdown margin. Optional ACTOR_TIMEOUT_AT absence uses that local cap; a present invalid or timezone-free declaration now refuses explicitly instead of silently inventing a local occurrence or swallowing malformed input. The 2 MB body and 12 MB aggregate diagnostic caps remain unchanged. Optional DOM href/content/media nodes reflect actual HTML observations, not required publisher facts. Exact raw decoded post text and retained diagnostic prefixes are preserved. Diagnostic models verify retained body bytes/digest, captured wire bound, attempt counts and response/status pairing; source schema version 1 uses an independent strict integer guard within the standalone closure.

Independent strict checking passes all four actor source modules and the new contract test module; no Any remains in active actor code. Existing reader tests explicitly project diagnostic models only at their assertion/wire boundary and use post attributes. 108 selected actor/privacy/diagnostic units pass in 6.82 seconds. The initial new test collection typo used pytest's reserved request parameter; it was corrected before this candidate checkpoint, without changing behavior assertions. Full actual collector stress/loopback and package closure qualification remains required before activation. Existing reader test helpers still need their separate full precise-typing audit; this is not a global ownership completion claim.


### Standalone actor immutable qualification

Exact actor application head `d40b985a63a0677ab31a8ffbbd0541d96c1aa2f1` passed all 684 owned units in 14.71 seconds (p90 0.316 seconds; maximum 2.036 seconds) and all 51 actual integrations in 18.70 seconds, zero skips. Both actor reader files are included, retaining all ten actor integration cases and the unchanged 86/200-account byte/memory/truncation assertions. The tracked tree and HEAD were unchanged before/after the run. Evidence is `.refactor-evidence/actor-models-owned-d40*`; the earlier initial combined fixture failure remains separately preserved. Ruff check and format check pass for all 412 current active files including actor sources; source-comment policy passes. The four actor modules and new contract test pass independent strict checking. Quality owns the paired tracked-module package/digest regression; this source head must not be called package-qualified until that companion is ready. The earlier Docker/image proof applies to the earlier source checkpoint, not this new module, until a new image import proof is captured.


### Catalog row contract migration candidate (actual cohort still held)

This source candidate normally joins clean main/native/actor/privacy parent 62a14395 and the shared opt-in unique JSON decoder; it does not restore loose dependency copies. Original row snapshots 95a67052, 8fda3969 and 18935896 remain preserved. Live source polling already consumes `parse_instrument_page`; it now validates named venue/surface row projections and returns canonical InstrumentRecord models directly, instead of normalizing required facts through generic optional text and structured dictionary records. Raw payload bytes enter `decode_unique_json` before projection, so duplicate success codes, categories, lists, row status and escaped equivalent row keys cannot collapse into invented facts.

Required identity/base/quote/status fields are explicit per implemented wire surface: Binance spot/perpetual, Bybit spot/linear, Kraken spot, OKX spot/linear, Coinbase Exchange spot and Hyperliquid perpetual metadata. Linear Binance/Bybit contractType and OKX ctType are required. Protocol identities retain exact case/content and reject blank/whitespace/control identifiers; they are not coerced or parsed out of a guessed symbol. Kraken takes its symbol from the actual associative result key. Status is the nonempty observed provider label; this adapter does not translate unknown labels into an approved trading state. The implemented adapter refuses Coinbase/Kraken perpetual or Hyperliquid spot requests instead of inventing unsupported surface semantics.

Bybit reported category is now required and matched to the selected spot/linear variant, independently of the cursor. [The official instrument protocol](https://bybit-exchange.github.io/docs/v5/market/instrument) reports category and distinguishes spot from linear pagination. Spot cursor omission/explicit-empty is its documented no-pagination state; nonempty spot cursor refuses. Linear cursor is required even when empty, and empty maps explicitly to no next page. An actual same-scope SourceService test proves a wrong-category empty reply cannot remove existing instruments, while a later correctly reported empty catalog creates exactly one removal. No current feed is used to guess a missing reported category.

Exact retained optionality: OKX spot ctType may be absent or the actual empty not-applicable string; linear baseCcy/quoteCcy may be absent/empty because those currency fields are not applicable to that response surface. Present null/wrong types refuse; symbols never supply invented currency facts. Hyperliquid quote is genuinely unreported when absent, and no USDC quote is invented. Its sparse isDelisted omission keeps the previously documented catalog-not-marked-delisted rule, with the same limited catalog-label interpretation and primary-source links already recorded above; explicit null/wrong types refuse. Contract type is None on surfaces that do not report one. `normalise.native_symbol=None` is legitimate only for the non-Kraken array protocols; Kraken requires the actual nonempty key.

[Coinbase's product contract](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-all-known-trading-pairs) declares post_only, limit_only, cancel_only and auction_mode; those live facts are required booleans. trading_disabled is genuinely optional and remains unknown/unset when absent, never assumed false. Only the four documented mutually exclusive order restrictions are checked together; auction mode remains independent. Present null/non-boolean refusal is tested with otherwise complete valid records. The existing restriction snapshot model preserves historical missing flags as unknown; a temporary all-unknown comparison value is never persisted into history.

`instruments.differs` always compares actual identities/status and compares optional base/quote/contract facts only when both observations contain nonempty known values. New metadata availability is enrichment, not proof that an instrument changed; known-to-known metadata changes still produce transitions. Tests retain the genuine null prior facts in the emitted before-evidence. Associative maps are only provider-key/symbol indexes; `snapshot.root.get(symbol)` distinguishes actual added/removed membership. Projection dictionaries and synthetic row payload dictionaries are explicit external wire boundaries; internal normalized records are strict named models.

All 371 focused source cases pass in 10.05 seconds, with zero skips; the six adapter/comparison/row/envelope/scope/durability strict targets pass. This includes required-field omission/null/type/blank controls, actual SourceService no-baseline-advance checks across all nine surfaces, optional-field controls, known/unknown metadata behavior, category mismatch, duplicate raw JSON and retained raw bytes. One new ETag assertion initially expected the previous committed tag; inspection confirmed the existing acquisition-error policy deliberately clears conditional headers. The corrected behavior test asserts the failed tag is not committed, the next request is fresh, the existing item/raw bytes survive unchanged, and a later valid empty catalog produces exactly one removal. This candidate still requires the full pinned owned unit/actual PostgreSQL/E2E cohort and any quality-owned synthetic fixture companion before activation; no global completion is claimed.

### Explicit actor direct dependency and new-module image proof

Actor branch normally joins quality's direct declaration companion `128cd221031d3df921a05ff3541e8a527c1fcd6a`; Pydantic 2.14.1 is now a direct member of the standalone x-reader group. The exact lock digest is `54a3cc4118b6601fcfd7d6e3452b61eab70c220a783b9448d26342901b6b3a86`; the frozen actor requirements remain byte-identical at `4c60a4a1fa0486de1b7b727673afed3db00dc6087fb204a442d5c7311e3be421`. Quality separately proves unchanged dependency identities and environment fingerprint, without an environment sync.

New immutable application/dependency head `21aa740f9623b4c483a9a3d3cf1215d072405d06` built image `tank-x-reader-codex:21aa740f`, exact image ID `sha256:128e864fb89c4c2f0b30cac9df2078cf6c563b103d858170026507a984a9636a`. Offline containers with `--network none` imported all four modules, including the actual `/app/actor_contracts.py`, and verified Python 3.13.13 plus all four direct versions. Actual request normalization, boolean-concurrency refusal and diagnostic retained-body/digest roundtrip model probes passed; `pip check` reported no broken requirements. Source HEAD and tracked tree were unchanged throughout. Evidence `.refactor-evidence/actor-domain-image.log`. Actor.main, live jobs, cloud/X requests and deployment were never invoked. This is a new image proof rather than reuse of the old image identity; the paired tracked-source package/digest regression remains quality-owned.

The stricter deadline interpretation follows [Apify's official environment-variable contract](https://docs.apify.com/actors/development/programming-interface/environment-variables): ACTOR_TIMEOUT_AT is the timeout date, and date variables use timezone-bearing UTC ISO 8601. Its genuine absence still uses the explicitly documented local safety cap; present malformed or naive input does not acquire invented timezone authority.


### Four typed policy/card caller repairs on current parent

The bounded caller companion starts from parent f7b6f3a7 and touches only the two generated-condition Scheduler cases and the two parameterized Ops delivery-receipt cases (plus their shared helper/imports). The actual DecisionEngine is constructed without the retired Settings argument; EventToolService still performs real decision materialization/persistence and generates the actual watch rules. The helper now exposes validated WatchCondition records, and the tests inspect real typed counters and emitted ConditionFiredMessage jobs. Both closed nonoverlapping hour boundaries, duplicate-hour suppression, one fired message and next-minute fatal behavior remain asserted.

The receipt cases remove the retired alert_gate/compose_card monkeypatches entirely. They run the real typed publication policy and real pending PARTIAL card renderer with an explicitly authored synthetic STRUCTURAL_SHOCK criterion and seven declared unassessed dimensions. No beneficiary, decision or market-truth record is fabricated. They validate the actual PublicationReceipt, require the real INITIAL gate and pending-analysis text, and retain queued-before-send, no premature SENT, success idempotency, five failed delivery attempts and exactly one terminal outcome assertions. Synthetic Telegram success/error envelopes now declare their actual test receipt date/error fields.

All four assigned cases pass in 3.46 seconds; both complete owned unit files pass all 79 cases in 4.61 seconds, zero skips. Broader Scheduler/Telegram/Ops test typing remains open: running default mypy on these historical files exposes existing module-wide issues, so this companion does not add them to a completed strict family or claim those diagnostics are closed. No production fallback, facade, policy bypass or source change was introduced. The held instrument-row candidate remains separately preserved as 9baf9050 pending its actual cohort and observed-kind/acquisition-scope review.


Pinned caller head `b1222a0f39c1ddc8945eedb1e2d733c8ebbef546` passed both full files again: 79 units in 3.76 seconds and their complete collected actual PostgreSQL case in 2.95 seconds, zero skips. Tracked HEAD/source stayed unchanged. Evidence prefix `.refactor-evidence/typed-card-callers-b122`; no unrelated actual case or broader typing gate is relabelled by this bounded repair.


### Acquisition selector versus reported derivative contract facts (held follow-on)

The domain attribute is now `InstrumentObservation.acquisition_market_type`. Explicit validation/serialization aliases retain the existing checkpoint wire key `market_type`; there is no compatibility getter and no interpretation of that selector as a physical contract kind. Canonical storage/identity serialization still uses the legacy alias, while user-facing evidence uses the explicit acquisition name. A legacy snapshot roundtrip preserves its exact normalized wire dictionary/digest, and the generated external ID matches the prior independent formula. The requested scope remains the comparison owner; no configuration/code revision resets a known baseline.

Current Binance and Bybit derivative rows now require their reported contract code plus actual marginAsset/settleCoin. Named strict observations record the actual perpetual/dated family; Bybit additionally records linear/inverse mode from its explicit contract enum. [Binance's current enum contract](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/common-definition) includes perpetual, perpetual-delivering, month and quarter variants; its [exchange-info schema](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data) reports marginAsset. Perpetual-delivering remains a reported perpetual-family delivery variant, not an assertion that trading is active or that no delivery can occur. No expiry timestamp or settlement currency is guessed from the selector.

[Bybit's explicit contract types](https://bybit-exchange.github.io/docs/v5/enum#contracttype) distinguish both perpetual/futures and linear/inverse. The actual reported settleCoin is retained; it is not invented from base/quote equality. A linear-category baseCoin acquisition can contain all four contract variants, so the owner check deliberately compares the acquisition selector, not the observed row kind/mode. Unknown or contradictory current contract facts refuse the entire read before baseline mutation; rows are never silently filtered into false removals.

`VenueInstrument.reported_contract=None` preserves genuinely unrecorded historical observations and is also absent where this specific Binance/Bybit reported-derivative view does not apply. It never fills a perpetual kind from the acquisition selector. The current prepared snapshot writer requires a reported observation for every new Binance/Bybit derivative capture. Old unknown-to-known enrichment alone is not a state transition; known settlement changes remain material. Other provider-specific raw contract fields remain their exact observed codes, without being relabelled as this shared derivative classification.

An actual mixed Bybit collector/restart test retains all four symbols, rejects an unknown future code without changing the baseline or creating items, then captures the real dated-future state changes. It has a separate actual PostgreSQL case for the pinned integration cohort. Legacy alias/digest/external-ID controls, required reported assets, contradictory-family refusal and current-writer missing-observation refusal are included. All seven changed contract/adapter/comparison/row/envelope/scope/durability targets are independently strict-clean. The focused source selection passed all 399 cases before the final plain-title adjustment; the changed exact grouping assertion also passes independently.

Grouped user-facing titles now say e.g. `Bybit instrument catalog`; acquisition details remain metadata and no title implies all linear-query results are perpetual. The former technical acquisition-label title in the earlier candidate ledger is superseded by this wording. The grouped-title fixture retains all grouping/symbol/order assertions; only its expected wording and capitalization order changed. Full pinned owned unit/actual proof and the quality-owned E2E fixture review remain required before releasing this row cohort.


### Complete catalog cohort qualification

Exact clean combined application head `a330a083b76c435cfa1fa7f774072ca3ffec0e1e` normally joins current parent d98c and passed all 981 owned units in 13.70 seconds (p90 0.265 seconds; maximum 2.647 seconds) and all 52 collected actual integrations in 18.67 seconds, zero skips. Tracked HEAD/source were unchanged before and after verification. Evidence prefix `.refactor-evidence/catalog-contracts-owned`. This includes the new actual PostgreSQL mixed-contract/restart case, all four actual E2Es, earlier durable raw/pending/scope/quarantine cases and both actor reader files' ten integrations. No frozen payload or unrelated production fixture was modified. Quality's existing current E2E fixture companion already supplies sufficient spot facts; no additional E2E payload change was required.

Independent strict checking passes the seven contract/adapter/comparison/row/envelope/scope/durability targets. Ruff check, format check across all 446 active Python files including actor sources and source-comment policy pass. The actual class/settlement observations, unchanged legacy alias/digest/external-ID control and user-facing plain catalog titles are qualified together; earlier per-step proof remains attributed to its original heads. This releases the bounded live catalog admission/comparison cohort for parent review, not the unfinished global Scheduler/Telegram/CLI/source parser migration or older actor helper typing.

### Native transport peer fixture ownership

Normally joined parent 7c4d33215dad7f281d0678437cea2c6e484a40ae and owner 61954aa5d0e9430d7d4eafcfd8b887b8cd6621b5. The existing managed history fixture already authors terminal next_page=None. Two old synthetic journal records acknowledged in_0/in_q without an attempt number, so the stricter actual ownership lookup correctly refused them. The seed and direct history-boundary test now explicitly declare their authored initial attempt zero and exact input-to-attempt association. No runtime inference, historical repair, frozen payload, deadline, MISSING policy or answer oracle changed.

Initial five-file unit proof retained at .refactor-evidence/native-transport-peers-initial.log:159 passed/two refused incomplete synthetic journals. Corrected test_telegram_followups, test_ops_followups, test_ops_progress, test_x_diagnostics and test_cli_serve selection:161 passed, zero skips,6.56s. Ruff check/format pass. This narrow fixture checkpoint does not relabel the earlier row/actual-service proof or close broader Telegram typing; combined actual native qualification remains a separate gate.

### Required X diagnostic admission before redaction

sanitize now accepts a validated XDiagnostics model or the explicit external JSON mapping boundary and returns XDiagnostics. Source projection, failed-account collection and operator reporting consume that model directly. The sanitizer no longer invents GET/unknown/empty-body facts, truncates an unvalidated attempt list, coerces flags, or replaces malformed base64 with fabricated evidence. The unused mapping-based evidence_identity duplicate was retired; AccountEntryError.identity remains the canonical named evidence identity.

Required request method/URL/body, attempt count, attempt fields and response status/body/encoding/metrics/redaction flag validate before redaction. None for an absent attempt response is a genuine transport failure; an empty recorded body stays an empty response with its actual zero-byte digest. Existing optional wire size/read/decode metadata remain nullable because historical collectors did not capture them; sanitization preserves their exact values and omission semantics through the existing contract. Headers remain an associative case-normalized approved-name map. The known-token redactor accepts text only. Existing two-million-byte clipping, Latin-1 byte-preserving secret replacement and UTF-8 replacement decoding are retained; post-redaction metrics describe the actual sanitized attachment bytes, with clipping and prior truncation/redaction flags preserved.

Actual SourceService tests cover missing response status/body, contradictory request method, invalid base64, nonboolean flag, inconsistent count and digest. All refuse before source-item/raw retention and preserve the prior account cursor. An absent response and a real empty response have separate positive assertions. Two old test fixtures now declare captured body metrics/redacted state; the oversized one-attempt fixture declares its true count rather than relying on the former count rewrite. Existing escaped-credential positive diagnostic evidence remains sanitized.

72 X diagnostic/privacy/sensor units pass, zero skips,4.91s. New eight-case test module passes independent strict mypy; affected three application modules pass default mypy; Ruff check/format pass. Initial two incomplete-fixture failures remain in .refactor-evidence/x-diagnostic-admission-initial.log. Attachment documents() remains an explicitly open untyped mapping projection and is the next connected migration; this checkpoint does not claim the whole diagnostic or delivery family strict-clean.

### Typed diagnostic attachment projection and stable delivery evidence

documents now takes concrete SourceEntryError records (or validates records at the storage-wire adapter) and returns DiagnosticAttachment binary values. DiagnosticDocumentMetadata/Attempt/Response explicitly project the JSON document schema; the application no longer round-trips through JSON or mutates diagnostic records with pop. Operator reporting passes its validated errors directly and obtains typed DiagnosticDocumentReference values. Delivery consumes named filename/content/digest attributes while preserving its existing per-document identity, manifest comparison, reply pairing and retry/defer journal rules.

DiagnosticAttachment.content is actual bounded binary evidence, not arbitrary JSON; its digest/reference derive from those bytes. Response bodyDocument is null only for an actual empty body, with body='' retained; nonempty bodies move into their exact attachment and omit the metadata body field. A missing response remains absent/null according to its authored field presence. Optional historical capture/read/decode metadata keeps unset-vs-explicit-null through exclude_unset; no new clocks or wire sizes are inferred. The filename's run label applies only when the recorded account identity is genuinely absent/empty, as a presentation filename rather than invented source authority. Headers remain an actual associative map. Projection dictionaries exist only at explicit serialization boundaries and immediately validate the named output schema.

A literal historical metadata-byte oracle proves field order/omission, stable filename and digest/reference and repeated projection without evidence mutation. Existing UTF-8/base64/empty/oversized body upload, partial-document retry and large-batch deferral tests pass with the real delivery path under synthetic HTTP. The two formerly partial direct attachment fixtures now declare their actual error reason.

Final focused X/Delivery/Ops checkpoint:144 units passed, zero skips,7.48s. Strict mypy independently passes the complete x_diagnostics module plus new nine-case admission/projection test; default mypy passes delivery/ops_entry_errors, and Ruff check/format plus source-comment policy pass seven affected files. Initial missing-reason fixture failures and the transient test import typo remain in local evidence; no runtime relaxation was added. Broader Delivery/Telegram/Ops typing and unrelated utility/map fallbacks remain open; this is not an actual external-service or whole-family completion claim.

The attachment body's serialization predicate is an explicitly typed absent_body(str|None)->bool, avoiding an implicitly Any-typed Field callback. Exact-schema/byte/idempotence and full diagnostic test files rerun:35 passed, zero skips,5.46s; independent two-target strict and Ruff check/format remain green. The earlier144-case result stays attributed to its original checkpoint.

### Inward source-error contracts and attachment presentation

This separate layer cohort starts at immutable88c7c610; Foundry/main integration is not yet joined or claimed. Pure error/diagnostic/identity/notice models move unchanged to contracts.source_errors. Source scanner, checkpoints and reporting import inward. diagnostic_documents contains only admitted-evidence attachment/metadata presentation; provider-wire validation and token/cookie redaction remain in sources.x_diagnostics. SourceService.report_errors now invokes actual source admission before the typed reporting port. The reporter refuses raw mapping inputs instead of silently assuming responsibility for provider redaction. Its callers/tests supply named records admitted by the real source adapter. Delivery imports only the inward document presenter.

Fresh isolated Python imports of contracts.source_errors, diagnostic_documents, ops_entry_errors and delivery each prove that no tank.sources module/application initializes. This closes the actual presentation-to-source-orchestration import inversion, rather than hiding it behind a renamed mixed adapter. The temporary sources.errors module contains exact-object import reexports only; no validation or mapping facade. All active repository consumers are migrated. Remove that bridge after the canonical main/layer join is consumed by all coordinated owner branches; it is not an external wire or compatibility implementation. Source x_diagnostics documents exports are removed because every active consumer migrated.

Retained schema exceptions, unchanged by the move: EntryIdentity.id/title/link/excerpt are independently absent when a malformed entry exposes no usable publisher metadata; entry_sha256 remains the required immutable row identity. ErrorEvidence.entry_index is absent for document-level failure, entry is absent without a readable row, error is absent for a named non-exception rejection, and payload_sha256 is absent when only the independently retained row digest exists. AccountEntryError.account=None is a run-level failure, diagnostics=None means no captured HTTP evidence, and an absent response/status describes a transport attempt without an HTTP response. Optional response wire/capture/read/decode facets remain genuinely uncaptured historical metadata; none is synthesized from body length. EntryErrorNotice.document_manifest=None is a plain notice without captured response documents and is omitted from storage, rather than an empty invented manifest. Marker/notice kind, created_by and collection-only precision defaults are controlled output-authoring constants from the poller, not inferred provider facts. GET body=None is required and states actual absence. Header dictionaries are genuine name-to-value maps; model_dump/projection mappings occur only at storage/document JSON serialization boundaries.

Five new layer tests prove four fresh-process import boundaries plus actual token/header redaction before named presentation, exact attachment bytes, immutable original evidence and repeated admission/document idempotence. Existing report-backlog retry, source raw/cursor privacy, diagnostic delivery retry/deferral and frozen-byte tests remain. Initial seven incomplete synthetic-settings failures are retained in source-error-layer-initial.log; the shared source fixture now explicitly declares apify_api_token=None for its no-Apify configuration, with no runtime getattr fallback.

Standalone layer checkpoint:263 units passed, zero skips,8.15s. Strict mypy passes five targets (three complete contract/presenter/admission modules and two new test modules); six affected application modules pass default mypy. Ruff check/format and source-comment policy pass all16 changed/new active files. Evidence is source-error-layer-unit.xml/log. Whole-family typing, remaining parser maps and combined current-main/actual-service qualification remain open and are not relabeled by this result.

### Canonical Foundry/main join and connected source qualification

Normally merged current main09633e83793bc03ef7e5ea0921efb65b8b058cae into the layer/88 lineage without conflicts, producing0635f83027a600f5b83ab0161fa349be1b08b731. Foundry Decision-1 settings, classifier response/usage fixtures and neutral presentation labels remain intact; stable persisted JEV_* history codes remain unchanged. No copies/cherry-picks or older source replacements were used.

Exact clean0635 connected proof is terminal:996 owned unit cases passed16.26s (p90.322s,max3.324s) and the complete52 selected actual integration cases passed18.82s, both zero skips. The integration set includes all4 actual E2Es, source/report durable PostgreSQL paths, row/scope/quarantine/durability tests and all actor reader integration cases. The guarded runner staged existing retained/supplementary payloads, used actual disposable services under heavy.lock and verified identical HEAD/clean tracked source before and after. Initial earlier standalone diagnostics/fixture failures remain attributed to their original logs. Evidence: .refactor-evidence/source-error-layer-combined.log and -unit.xml/-integration.xml, terminalPID3905198; no restart required.

All458 active Python files pass Ruff formatting; Ruff checks and source-comment policy pass. Five completed layer targets independently pass strict mypy. Broader direct mypy src checks154 files and still reports the analysis-owned truth.py5890 _condition_cache missing annotation. The owner confirms the actual typed full/selective cache activation is not ready; no opaque map annotation or cache removal workaround was made. Thus this coherent layer cohort has qualified connected behavior and its complete strict target subset, while broader Truth/default-source typing remains honestly open. This report-only descendant does not relabel the immutable0635 test snapshot.

### Bound follow-up refusal adapter and actual question writers (held cohort)

Normally joined exactbcdd3b37, preservingce9f226d/f28 canonical pure policy and finalFoundry09633 ancestry; the independently qualified dfc1 source layer remains immutable. SessionService's required refusal port is not yet activated in parent/composition. Telegram format_followup_closure renders typed inward facts only, including generic wording for known RETENTION with genuinely unreported days. closed_reason/remote_session_missing now consume the inward view; recorded termination still precedes typed MISSING. Retention day formatting takes a validated integer rather than coercing input.

New inward question_answers contains the actual typed minimal repository port, narrow stored answer-state projection, output transition and named receipt. Its wire patch preserves every unrelated question field. Question ID must match the storage key on every answer; a FollowupRefusalTarget additionally requires the actual stored session ID. These checks occur after the authoritative read inside the existing question lock and transaction, before answer mutation or delivery enqueue. Unknown historical session binding is nullable in the projection and refuses a bound refusal; it is never invented from the target. Absence of answer_status is the actual QUEUED/PROMPTED producer's not-yet-answered state. A missing question remains absent with no delivery request. The first recorded answer wins; repeated delivery requests retain that answer and use the same durable idempotency key/max-five-attempt policy.

RepositoryFollowupRefusals(repo) is the concrete Telegram edge adapter. It accepts the required named question+session target and typed closure, validates it, formats existing wording and invokes the inward writer. The Telegram record_answer name temporarily reexports the exact inward function while agents migrates Session/Runtime imports; no parallel writer or compatibility implementation exists. The writer's old raw-row return is now a QuestionAnswerReceipt; all production callers ignored the old return. Collection time remains actual answered_at provenance; queue run_after/one-day expiry are explicit offsets from a real collection clock.

Production writer trace: only shared record_answer, TelegramInbox.prompt/question, and answer/answer_failed write out.user_questions; Session/Runtime only read or call the shared writer, and no production question deletion/rebind path was found. Prompt and question previously lacked the question advisory lock. Both now acquire that same lock+transaction, validate existing binding before file/HTTP or enqueue, and retain the existing question rather than overwrite an answer. Known answered prompt retries return without another prompt. Answer delivery/status writers already acquire the lock; they now also validate the stored key/binding before response status writes, with answer's initial identity check before HTTP. This does not claim that advisory locks block arbitrary raw database writes: every actual application participant is traced and coordinated.

Standalone focused proof:58 pure policy, MemoryRepository race/ownership, formatter and native-readback cases passed, zero skips,1.95s. The observer delegates every operation/lock/transaction to the actual backend; it only signals lock entry. Wrong key/session/unreported binding leaves rows/jobs unchanged; a queued reporter rechecks a rebound row after the actual lock, matching binding records the existing wording and one job, and repeat refusal preserves answer/clock/job identity. Actual Telegram prompt/question paths additionally prove wrong-session zero HTTP/file/job effects and a real creation race. question_answers plus core refusal tests are strict-clean; Telegram module default mypy and Ruff/checkformat/comment policy pass. Full Inbox API typing, the separate actual-binder test module's existing untyped Inbox calls, caller/composition migration and actual PostgreSQL qualification remain open. No full/cohort activation is claimed at this checkpoint.

### LOCAL held follow-up peer caller qualification

Created the assigned managed local worktree from exact664ab499 and normally joined canonical12cfa8fb/902f4792, preserving both the required-port policy and exhausted-retry correction. Local65740ba7012007cede7fe8d8420e86ca3560ea41 adds only28 actual RepositoryFollowupRefusals(repo) injections across the existing Telegram/Ops peer fixtures. No trap replaces a real refusal, no source fallback is introduced, and existing HTTP/status/deadline/pairing/answer assertions are unchanged. This remains a separate inactive cohort; latest-main19872 and the agents composition/quality E2E companions are not claimed joined here.

Exact local65740ba7 passes all97 two-file units2.04s, zero skips,p90.188/max.474. Two actual native-platform local PostgreSQL writer/binder cases pass2.09s, zero skips,max.886, through quality's owned-service/shared-lock wrapper. The first PG attempt also passed both tests but its wrapper correctly rejected the run because a generated JUnit file under the untracked worktree evidence directory changed its all-untracked identity hash. That file and diagnostic are preserved; rerunning with JUnit/stdout in the external local evidence directory passed the complete immutable wrapper without any source or wrapper change. No server development/tests were resumed.

Question writer/core refusal tests independently pass local strict mypy; Ruff/format pass six affected files. The broader two legacy follow-up files still have286 default-mypy errors (raw nested storage access, missing fixture annotations and method reassignment), and the separate actual binder test still has eight calls into the unfinished Inbox API. These are explicit remaining work, not hidden with casts or generic facades and not called a completed strict family. Local open-error log is /Users/a.mametyev/.codex/tank-refactor-20261026/evidence/ingestion-followup-callers-open-mypy-65740ba7.log; valid PG artifacts are ingestion-followup-pg-65740ba7.log/xml there. The unit artifacts remain .refactor-evidence/local-followup-callers-unit.log/xml in this owned local worktree. This report-only descendant keeps the tested application snapshot attributed to65740ba7.

### LOCAL main19872 X quoted/replied/linked context merge

Resolved only the assigned public_profile/xsensor/test_xsensor conflicts from the actual merge index, preserving both latest-main context behavior and the existing typed actor/source admission. Required companions are actor_contracts, test_x_reader, test_x_reader_contracts and the referenced-text privacy control in test_xsensor_wire_privacy. No server development or live X/Actor.main/cloud/deploy action occurred. The separate held follow-up worktree/caller changes are not copied into this merge.

Named actor ReferencedPost, ReferencedPostContent and EmbeddedTweet records replace structured enrichment dictionaries. ReferenceReadSuccess(content) and ReferenceReadFailure(error) are closed, extra-forbidden phases: no null success body, no null/empty failure reason, and no record with both facts. ReferenceResolution is their union. The actual resolver retains its upstream success, failure and fetch-limit branches without projecting a null error as unresolved. Every public actor function retains precise full typing; the four actor modules plus the existing contract test module pass independent strict mypy.

Required root post identity/author/text/URL/publication, run health, account outcomes and cursor facts remain required. New references require relation/status identity/URL; URL identity agrees with reported post ID and author when known. Actor reference author, publication time and text are required nullable facts: a missing author is explicitly unknown, never inferred from the i status route; an unavailable body is distinct from an actual empty body. Source wire projection additionally tolerates historical/unreported optional reference facets. Its published_at validator preserves the upstream behavior: malformed optional reference-time text becomes unverified/unknown and is not printed as a publication instant; the exact original actor packet remains raw evidence. Primary root publication requirements are unchanged.

ReferencedPost.error=None is the genuine not-attempted/unreported state before resolution or on baseline reads. Failed/limited fetches retain their actual named reason. Source presentation labels these states as NOT_RETURNED or the recorded failure, never an invented body; actual empty text gets the existing no-text wording. Positive GraphQL created_at_ms is observed milliseconds; otherwise the existing Snowflake rule applies only to dated status IDs, with small undated reference IDs left unknown. Optional/unrecognized GraphQL extensions are projected into named observed facts; they are not a permissive substitute for required root post data. Missing enrichment text falls back to that article's actual visible own text. Nested articles are not misclassified as monitored own posts; when enrichment is unavailable, visible nested quote identity/text remain actual DOM observations. Media/quote-only posts can have observed empty own text; an article with no text/media/quote refuses as before.

Only new monitored posts above a declared account cursor resolve missing reference bodies. Baseline reads perform no extra reference fetch. Existing 24 unique-reference request limit, shared concurrency/deadline, bounded retries and 4096-byte per-reference error evidence remain; successful page reads retain the existing two-million-byte wire/decompression bounds. The profile aggregate diagnostic allocation remains unchanged. This is a finite reference fetch bound, not a claim that a failed read returned text or a newly invented provider quota. Repeated references share an ID-keyed result; failure never advances an account's failed/gapped cursor rules. Generic typed fetch_public reuses the actual connection cleanup, public-host redirect refusal, diagnostics, body/digest/truncation and deadline rules for PROFILE and POST.

Map exceptions are precise: the decoded GraphQL tree is heterogeneous external wire data only, admitted by EmbeddedJsonDocument before immediate named projection; tweet/edit aliases, found references, pending references and resolved outcomes are genuine ID indexes. HTTP diagnostic headers remain a real name/value map. No structured domain outcome is kept as a generic map. ReferencedPosts serializes explicitly through actor models; nullable facts remain declared and the unset error field stays omitted. SocialDiscovery.quoted is set only for actual references. No-reference rows preserve absence rather than add a null quoted key, so their classifier/dedup input stays unchanged; the new collector's additional wire metadata can legitimately change a newly captured raw packet digest and is not relabeled as old bytes. Existing SocialDiscovery source/session/provenance ownership and exact raw response bytes remain intact.

Duplicate/privacy boundaries: the Tank actor-response decoder still rejects every duplicate decoded key and scans the full decoded tree, including unconsumed metadata, before projection/raw retention/cursor updates. Added escaped reference-text privacy controls exercise the new structured path without rewriting tainted raw evidence. A separate actual-parser synthetic reproduction confirmed last-key-wins embedded rest_id/details/full_text/quoted-ID enrichment; before evidence is retained locally. Standalone actor decode_embedded_json now checks every nested decoded object key (including escaped spellings) before authoritative typed JSON decoding. Its static error echoes no key/body. Ambiguous optional enrichment is discarded and actual DOM content survives; JSON-looking post text stays opaque. An offline no-I/O after probe confirms all four ambiguous inputs now keep Visible original/no fabricated references, plus a unique/opaque-text positive control. The contract tests retain these controls, a visible-quote fallback and invalid closed-resolution-state cases.

Qualification remains scoped: the earlier local merge smoke had38 actor units pass but source collection failed through the then-unresolved Scheduler merge import; its log is preserved at .refactor-evidence/local-x-context-merge-unit.log, not called combined green. Duplicate before/after offline evidence is embedded-json-duplicate-before.json/after.json in the same local evidence directory. Five-target strict, Ruff check/format and the owned-file comment policy pass. Root pytest/runtime/actual-service gates are held until all merge owners are ready; no new image/package/whole-family qualification is implied. The broader legacy reader/integration test typing backlog remains open even though the new source functions and contract tests are typed.

A second actual offline parser counterexample showed a matching status ID with known GraphQL author bob being emitted as monitored DOM author alice while borrowing bob's text. parse_profile now refuses PUBLIC_POST_AUTHOR_CONFLICT before using known contradictory enrichment. Unknown/missing core still leaves authority with the observed DOM; case-insensitive matches and edited aliases of the same author remain valid. The original DOM/GraphQL inputs are not rewritten. Typed contract controls cover mismatch refusal and unknown/case-matching positives; a real actor fetch_snapshot→SourceService synthetic-HTTP boundary case asserts failed account, unchanged cursor, zero source rows/raw files/jobs. These root tests await the coordinated merge gate. Offline before/after evidence is embedded-author-conflict-before.json/after.json, and no current-main source-effects claim is substituted for the pending full test. The closed ReferenceReadSuccess/ReferenceReadFailure variants additionally prevent either both-present or both-absent outcome facts; contract refusal cases are retained without changing the external referenced-post wire shape.

Reference availability is optional enrichment, not primary-account coverage: a complete primary profile read can advance its cursor while preserving an unavailable/limited reference reason, exactly as upstream; primary failed/gapped reads keep the existing no-advance rules. The 4096-byte reference error cap applies inside the shared fetcher. The reference output retains the actual failure code, not a claim that its HTTP error body was uploaded or stored as post text. Source raw evidence remains the unchanged complete actor API response, with its reported reference fields; this ledger does not relabel derived reference text as the original underlying HTML bytes.

### Clean main9e watch-hydration fixture companion

This bounded branch starts directly at parent9e7e940c7d04dfa49bee4c465d2bfd5903484be0. It carries no required-refusal-port/reporter ancestry or constructor change. The existing provenance-hydration test's old action-only ARM request is no longer a declared tool form after main's timed/metric watch schema. It now supplies an actual timed ARM with a concrete review question and a due time one hour after a fixed injected Session clock. No unrelated token, financial assessment or watch row is fabricated.

The actual host path still hydrates the native snapshot through exactly one GET and derives the64-character prompt provenance hash before dispatch. The callback additionally checks the bound event, due time and review question. This test qualifies hydration and argument binding, not watch persistence or native mutation effects. Touched helper/callback parameters/returns are typed; existing broader follow-up test typing debt is not claimed closed.

Exact482c43abab3df56556c221d5559c45a969557449 passed the complete affected Telegram test file on the local pinned Linux runtime:84 units, zero skips,9.99s suite wall time,p90.208/max.694 per case. The owned-service wrapper exited0 with its source identity check intact. Ruff check/format and the comment policy pass. External evidence: /Users/a.mametyev/.codex/tank-refactor-20261026/evidence/ingestion-watch-fixture-linux-482c43ab.log/xml. Parent's original main9e action-only schema failure remains in its original failure ledger. This report-only descendant does not relabel the tested application snapshot.

### Local bounded supply diagnostic retention (2026-10-26)

Transferred ownership for this repair is limited to `market/supply.py` diagnostic creation and `tests/test_supply_error_retention.py`, from clean parent `ee54ddba`. Financial/provider/cache logic and graph contracts/codecs are unchanged. `SupplyCapture.diagnostic_reason` uses the existing `redact_text` with the current `Settings.configured_secrets()` set at the point of failure: all configured credential values, credential URL/query patterns, Telegram token paths and Bearer patterns are covered. The original provider response is never replaced by this derived diagnostic.

Exact leaves: `poll` catch-to-record reason; `inventory` decimals coverage error and wallet coverage error; `chain_liquidity` per-chain failure; `fundamentals` per-slug/per-metric failure shared by its metric result and coverage error. `record` sanitizes only an actually present string `reason`. Its associative keyword lookup is necessary because successful observations do not author a failure reason; absent/None remain distinct, and an actual blank exception stays blank. The previous Etherscan-only replacement across every top-level string could alter chain/identity facts while leaving nested diagnostics unsafe; it is removed. Wallet, chain, chain ID, contract, operation, slug, metric and coverage counts are preserved. Successful zero quantities, partial observations and raw response bytes remain unchanged.

Evidence: the initial six-case run reproduced five unsafe diagnostic paths; the blank-reason control passed. The repaired affected selection has 19 units passing, zero skips, 2.84 s total, p90 0.157 s/max 0.171 s; the existing actual PostgreSQL supply case passes separately (1 case, 3.55 s). New tests execute real `SupplyCapture` workflows with HTTPX transport fault/provider controls and actual MemoryRepository storage; they do not mock the redaction or financial calculation. A provider error response containing synthetic credentials is retained byte-for-byte while its derived coverage reason is sanitized. Evidence is external under `evidence/supply-errors-{before,after,final,pg}.*` in the local tool root; the intermediate unsupported-field fixture diagnostic is retained, then corrected to use declared identity evidence.

Ruff/check-format, source-comment policy and default mypy for the two affected files pass. This is not a claim that the pre-existing untyped SupplyCapture financial workflow is strict-clean: its full domain/port migration remains owned by the market workstream. The new diagnostic helper and new test callbacks/helpers have explicit types; no generic recursive JSON redaction facade, input casts or schema weakening were added.

### Held followup writer: explicit absent text versus invalid supplied text (2026-10-26)

This separate companion normally joins canonical policy `79a9287a` and does not activate the Telegram wire branch. Actual caller audit: SessionService.deliver_answers passes ANSWERED with the actual settled turn text, which is None when end_turn produced no text (including file-only output); it passes FAILED/None for a failed native turn. Runtime terminal failure likewise passes FAILED/None. RepositoryFollowupRefusals supplies the concrete nonblank closure formatter output for NO_SESSION, TERMINATED and ARCHIVED. No active caller requires a supplied empty/whitespace string, and no failure reason is fabricated for an unreported FAILED reason.

`QuestionAnswerContent` now validates the actual requested status/text before any question write or idempotent delivery re-enqueue. ANSWERED/FAILED retain explicit None; closure statuses require their actual formatted reason; any supplied string must contain nonwhitespace, without trimming or rewriting its bytes. `QuestionAnswerUpdate.storage_patch` selects answer/reason by status only. Its other None field is an explicitly inapplicable presentation slot, not a truthiness substitute for reported text. An invalid repeated request cannot manufacture a delivery job even if a durable answer already exists. Existing both-ID/key ownership checks, question lock/transaction, prior-answer idempotence and delivery effects remain unchanged.

Evidence: strict source/test checks pass independently. The connected refusal/binding/followup selection passes 137 units, zero skips, 8.18 s (p90 .186 s/max .550 s), plus all 3 actual PostgreSQL cases in 3.47 s. New behavior tests cover actual writer/enqueue effects for None, all statuses with supplied blanks before first write and after an existing answer, closure None refusal, exact nonblank-byte preservation and wrong-type noncoercion. Existing real Session turn-history regression still verifies end_turn with text None after a failed prior attempt, and the real runtime terminal-failure path remains in the connected selection. External evidence: `evidence/answer-text-{unit,pg}.*` under the local tool root. Ruff/check-format and source-comment policy pass. Broader Session answer-settlement typing is agents-owned and remains a separate tranche; the three direct turn_answers tests were not edited here.

### Local Telegram update admission checkpoint (2026-10-26)

This bounded checkpoint starts from clean `ee54ddba`, without the required-refusal policy ancestry. `telegram_api.response_result` runs `decode_unique_json` on the original HTTP bytes before projection. Closed success/failure envelopes preserve actual HTTP status and optional reported retry delay; malformed/ambiguous responses raise static sanitized protocol errors. `TelegramInbox.call` now uses that boundary. An unsuccessful API reply's `error_code` and `description` are required facts; the shared synthetic too-large response now declares its actual 400 code rather than relying on an incomplete envelope. API descriptions are not interpolated into exception messages.

`update_receipts` validates the complete getUpdates batch before any stored message or cursor effect. Each supported message requires actual message ID, positive ordinary Unix date, nonzero chat ID/type, and, when present, an actual positive sender ID. Sender omission is a real Telegram protocol state, persisted as None rather than an invented empty identity; two unknown senders cannot acquire pairing authority. A present null/invalid sender refuses. Original decoded message fields remain untouched only as retained wire at the existing persistence/presentation boundary; the named required identity drives key/date/pair bucket. This is an explicit narrow intake contract, not a full Message/application-model claim.

Protocol-allowed unsupported update payload kinds are explicit acknowledged outcomes, so they do not block intake permanently. A malformed supported message never becomes that outcome. `message_id=0` is explicitly unusable and never stored as an addressable message, while its update can be acknowledged. Ordinary Message.date is positive; the independent InaccessibleMessage contract uses strictly integer zero and cannot enter ordinary update admission. Telegram's current primary API states these distinctions: https://core.telegram.org/bots/api#message and https://core.telegram.org/bots/api#inaccessiblemessage. Present saved cursors require their actual owner and nonnegative decimal cursor; only a genuinely absent checkpoint starts at zero.

Retained exceptions: `TelegramMessageIdentity.sender` is None only when the wire omits `from`; `TelegramRetryParameters.retry_after`/`migrate_to_chat_id` are optional protocol parameters, never invented values; `TelegramSuccess.result` is JSON solely at the external API envelope before method-specific validation; `TelegramMessageReceipt.source_message` is the unchanged external message at the existing storage boundary, not a domain mapping facade. Unknown unsupported payload kind names are retained as actual key names, with one object payload required; no business interpretation is invented. The initial poll timeout remains the declared long-poll configuration, and the initial zero offset applies only when no cursor exists.

Evidence: 114 connected units pass with zero skips in 8.57 s; the actual PostgreSQL followup case passes separately (1 case, 3.10 s). Duplicate literal/escaped keys, whole-batch no-effects, required root facts, strict integers, positive dates, unavailable sender authority, unusable scheduled messages, unsupported updates and retained malformed cursor controls are exercised through actual TelegramInbox/MemoryRepository or strict admission models. The initial 95/99 diagnostic retained four incomplete synthetic failure-envelope cases; the shared fake was corrected without changing their notice/pair/retry oracles. External evidence: local tool `evidence/telegram-wire-{initial,final,final2,pinned-unit,pg}.*`. Two new source modules pass independent strict mypy; these plus Inbox and the new tests pass default mypy. Ruff/check-format and source-comment checks pass. Existing broad Inbox/presentation typing, typed media/getFile/download, original MIME provenance, attachment historical identity and byte-stream policy remain explicitly open for the next slice.

### Telegram historical sender and protocol correction companion (2026-10-26)

Review of the actual queued-row reader confirmed that original intake wrote `user_id=""` when source `from` was absent. Such retained rows remain reachable after restart and must not be paired as one anonymous user. `telegram_api.pairing_sender_id` now projects the exact historical empty sentinel (or new None) to no-pair only when the original message actually omitted `from`. A reported sender must validate its positive ID and match the stored identity; contradictory/malformed known identities refuse rather than becoming optional. `TelegramInbox.companion` applies the same check to subject and candidates. Retained rows are never rewritten. Actual old-row tests prove no question/job/row effects, while the known matching sender positive control still returns the companion.

Primary-source review also corrected two overly narrow assumptions in the first checkpoint. The Making Requests section permits an optional success `description`; `TelegramSuccess` now retains this actual optional string, rejects a present nonstring/null, and still requires result: https://core.telegram.org/bots/api#making-requests. Update permits *at most* one optional payload, not exactly one: https://core.telegram.org/bots/api#update. An update containing only its required update_id now has explicit `TelegramEmptyUpdate(reason=NO_OPTIONAL_PAYLOAD)` and can advance the cursor without a message effect. Multiple payload kinds and malformed present supported messages still refuse. This supersedes the first checkpoint's one-payload assumption; no missing message is manufactured for the valid empty outcome.

Evidence: full connected selection passes 125 units, zero skips, 8.46 s, and the actual PostgreSQL followup case passes separately with zero skips. Original response-path controls cover optional success description and an update_id-only response through actual Inbox.poll. Historical sender gate before the protocol additions passed 120 cases; all original pairing/notice/retry oracles remain. External evidence: `evidence/telegram-historical-sender-unit.*` and `evidence/telegram-wire-protocol-companion-{unit,pg}.*`. New source2 strict, affected source3/new-test default mypy, Ruff/check-format and source-comment policy pass. No pending refusal-port ancestry was imported; file acquisition remains the next open slice.

### Independent outbound Telegram ambiguity repair (2026-10-26)

This repair starts from clean parent `976a131a`; the larger file-acquisition work is separately preserved in immutable stash `8f0c261cd8294a01698727df7731d90d0b700094` and is not part of this companion. Review found that TelegramProtocolError inherited RuntimeError, while NOT_SENT treated every RuntimeError as a definite rejection. A malformed or ambiguous response could therefore create FAILED and authorize a blind resend even when Telegram might already have accepted the request.

TelegramProtocolError now represents uncertainty as ValueError. NOT_SENT is narrowly the actual typed provider-declared rejection or HTTPX connection-establishment failure/timeout; a generic RuntimeError, malformed body, duplicate-key JSON or read timeout is not evidence of safe resend. A contradictory success envelope under a non-200 HTTP status is likewise uncertain. The remaining sendDocument response.json bypass now uses the same original-byte unique JSON admission as sendMessage.

`TelegramSentMessageReceipt` is a narrow consumed acknowledgement projection, not a full returned Message facade. Its strict positive message_id is required before any SENT receipt; absent, zero/unusable, negative, string, boolean or float identities cannot authorize delivery. An ancillary returned date is retained only if actually reported and valid; omission stays unreported in the delivery ledger, never collection time or epoch. This projection does not claim the omitted unrelated Message fields were present. Text/document send methods return this named model and all actual notice/prompt/answer/file receipt writers consume its attributes.

An unresolved GENERATED answer chunk is not retried and prevents the question from being marked DELIVERED. Other chunks with actual definite rejection remain independently retryable, preserving the original chunk retry oracle. Actual provider ok:false with typed error facts remains a known rejection, preserves its reported delay/status and permits retry. Synthetic tests intending that condition now declare their actual error_code/description; they no longer use malformed bodies to stand for definite provider rejection. Uncertain document attempts retain the existing UNKNOWN tool outcome without new sends.

Evidence: new actual Inbox/Memory writer/send/receipt tests reproduced 18 failures and 2 passes before repair. The first connected repair run retained one original chunk-order failure (210 pass/1 fail); preserving that independent retry oracle while withholding final DELIVERED produces 211 passing units, zero skips, 8.95 s (p90 .187 s/max .549 s). All 4 affected actual PostgreSQL cases pass, zero skips, 4.29 s, including a new real PG uncertain-send ledger/state/idempotence oracle. Controls cover literal/escaped duplicate returned IDs, missing/null/zero/boolean/float/string IDs, text/document paths, explicit provider rejection then one actual receipt, and post-send read timeout. Sanitized protocol errors do not echo source bodies or token-bearing URLs. External evidence: `evidence/telegram-ambiguity-{before,unit,final-unit,pg}.*`. Strict new source2 and default affected Inbox/new-test checks pass, plus Ruff/check-format/source-comment policy. Larger file/MIME/cache and full Inbox workflow typing remain explicitly open.

### Telegram attested file acquisition and MIME/byte facts (2026-10-26)

The file workflow normally joins canonical parent `39153956`, including the qualified outbound uncertainty fix and agents' typed answer API. Original WIP objects `8f0c261c` and `57e16d73` remain preserved; no retained rows or payloads were rewritten. Actual source references, photo dimensions, selected media, getFile metadata, downloaded bytes, cache metadata, stored payloads and attachment references now have named strict contracts. Application/cache decisions stay in Inbox; original-byte API admission and nonmutating download execution stay in telegram_api; pure facts stay in contracts.telegram_wire.

`media_selection` requires distinct actual file_id and file_unique_id and validates the selected source media before supported updates become durable. It never borrows a downloadable ID as stable identity. Photo selection uses required reported dimensions, with no missing-size zero sentinel. Source file_size omission remains None, while an actual reported zero remains zero; present null/boolean/float/string/negative sizes refuse. filename/mime_type are optional protocol metadata, not required identities. A local safe filename is generated only when an original name is absent/empty/unusable; generated names do not guess a file-format extension. The original source message retains the reported name and all other untouched wire fields. The existing document-first media priority is retained (including Telegram's animation/document compatibility representation).

`download_file` validates a unique original getFile JSON response and its required stable file identity before issuing GET. The returned downloadable file_id may differ legitimately; equality of the actual file_unique_id is required. Optional missing file_path is explicitly unavailable, not an invented path or oversized-file claim. Unsafe relative paths and redirects refuse. Actual source/getFile declared sizes are checked against the existing 20 MiB application limit; conflicting known declarations or received counts refuse. Unknown sizes remain unknown until bytes are actually received. Streaming counts actual received file bytes, aborts/closes once the limit is exceeded and never persists the over-limit payload. A generic 400 is not oversized: only the actual 400/400 provider response with exact observed 'Bad Request: file is too big' description maps to that notice. Other declared API errors retain their actual status; transport/validation errors use static safe messages without token URLs or source-body echoes.

MIME facts are explicit: reported_mime_type is the sender's actual optional metadata, response_content_type is the actual optional HTTPX response-header value, and mime_basis identifies SENDER_REPORTED, DOWNLOAD_HEADER or STORAGE_DEFAULT. Missing and reported-empty strings remain distinct facts. The effective serving MIME uses a usable reported value, then a usable actual header, otherwise neutral application/octet-stream; it is not relabelled as a captured source header or inferred JPEG. Cache validation checks that effective MIME/basis match those recorded facts. Attachment references carry the approved serving metadata, not a claim of independent content-format inspection. Primary protocol references: https://core.telegram.org/bots/api#photosize, https://core.telegram.org/bots/api#document, https://core.telegram.org/bots/api#file and https://core.telegram.org/bots/api#getfile.

New cache identity hashes question_id, actual file_unique_id and the explicit TELEGRAM_FILE_UNIQUE_ID basis. This is a distinct namespace from the old `file` hash input, whose value could have been a borrowed downloadable ID. Old metadata/bytes stay unattested and untouched; a genuinely new observation/download authors a new attested receipt. The actual question binding lock/transaction is preserved. New cache reuse verifies question/session/stable identity, required metadata, bounded canonical base64, byte count and SHA-256; an unavailable/corrupt/foreign cache refuses rather than silently rewriting evidence. Bytes and metadata are committed together under the file lock and repository transaction. Serialization dictionaries are confined to HTTP request bodies, explicit provider projections and the actual persistence/native attachment boundary; the unused attachments(row,...) argument is retired and application attachment results are typed models.

Evidence: final connected 235 units pass with zero skips in 49.93 s (p90 .390 s/max 3.353 s). All 5 selected actual PostgreSQL cases pass with zero skips in 47.71 s, including new actual cache/history/ownership coverage. The initial PG command mistakenly inherited the unit 10 s timeout and retained five timeout diagnostics; unchanged source passes with the documented integration --timeout=300. The earlier 235-unit proof and original 157-case file proof are retained. Controls exercise required IDs before message/cursor effects, strict declared sizes, unknown versus known zero, streamed over-limit abort/close, exact original binary bytes, absent/real headers, unrelated 400 refusal, getFile identity/path/duplicate JSON failure, conflicting declarations, original cache retention and verified reuse with foreign-session no-HTTP refusal. Actual typed answer and uncertainty regressions remain in the connected selection. External evidence: local tool `evidence/telegram-file-{initial-unit,unit,final-unit,pinned-unit,pg,pg-qualified}.*`. New source2 strict and affected source3/new-test default mypy pass, plus Ruff/check-format/source-comment policy. Full Inbox constructor/ledger/presentation typing remains open; this checkpoint does not claim the whole Telegram family is strict-clean.

### File reuse current-size and active credential follow-through (2026-10-26)

Review reproduced a real 579 reuse gap: an original unknown declared size with six verified cached bytes accepted a later declaration of twenty. The actual attachments/helper regression fails before repair. After cache base64/count/SHA verification, every current non-None declaration is now compared with the actual verified byte count, including a reported zero. Unknown None and a matching declaration reuse the existing receipt; contradictions refuse without HTTP or metadata/payload writes. Tests also change the later MIME/name claim while verifying that the original cached MIME/name/source-size facts remain untouched. The same helper exercises MemoryRepository and actual PostgreSQL.

`require_telegram_token` explicitly validates/narrows the active download dependency, covering Settings.telegram_token's genuine disabled/unconfigured None and blank values before any HTTP. Inbox.download passes the resulting real string to download_file, which also protects its direct transport boundary. No botNone/bot-empty URL is manufactured. A verified cache reuse performs no download and remains valid without a live token; this does not invent current Telegram authorization or rewrite the earlier receipt. This is explicit operation-fact validation, not a claim that Inbox's still-untyped constructor proves presence. Outbound credential preflight/GENERATED sequencing is deliberately not changed here and remains a separate required follow-through.

Evidence: 239 connected units pass, zero skips, 49.72 s (p90 .384 s/max 2.608 s); all 5 selected actual PostgreSQL cases pass, zero skips, 45.44 s with the documented 300 s integration limit. New controls cover conflicting current positive/zero declarations, matching/unknown reuse, unchanged old rows/bytes/MIME receipt, uncached None/empty/whitespace credentials before requests or writes, direct blank-token refusal, and cached credential-free reuse. Strict new source2, default Inbox/new-test checks and Ruff/check-format/source-comment policy pass. External evidence: `evidence/telegram-current-size-before.*` and `evidence/telegram-file-current-size-{unit,pg}.*`. The actual runner waited behind the parent's immutable full Linux gate; source remained unchanged throughout.

### Outbound credential preflight before delivery intent (2026-10-26)

Separate child of qualified `0a15af3a`: Inbox.active_token narrows the actual required credential, and url/call no longer manufacture botNone or empty-token URLs. Answer chunks, session files, notices and prompts validate the credential before creating a new GENERATED marker. Prompt preserves the existing both-ID question-lock guard and checks whether a new send is required before creating a new prompt question. Existing confirmed or uncertain receipt reconciliation performs no new HTTP and therefore does not require a current credential. The underlying required secret is never synthesized or trimmed into a different credential.

TelegramConfigurationError is a known-before-HTTP local precondition failure, distinct from TelegramProtocolError/ReadTimeout uncertainty. Its explicit NOT_SENT classification does not make arbitrary ValueError/RuntimeError or malformed provider responses safe to resend. Initial missing credentials create no delivery marker; a final URL validation still protects direct method calls. Provider rejection, connection-establishment failure, cached receipts and unknown post-send outcomes keep their existing qualified semantics. Test wire/file fixtures that actually request synthetic API replies now explicitly declare their synthetic active credential instead of relying on botNone.

Evidence: 261 connected units pass, zero skips, 50.85 s (p90 .396 s/max 5.432 s); all 6 actual PostgreSQL cases pass, zero skips, 47.00 s, including a new all-four-operation no-marker/no-prompt/no-HTTP guard. New controls cover None/empty/whitespace credentials for answer/document/notice/prompt, direct getMe/getUpdates calls, actual successful and read-timeout attempts followed by credential-free reconciliation. Existing rows, notice retry, paired input, binding and byte/MIME/uncertainty oracles remain in the connected selection. Default affected source/test mypy, Ruff/check-format and source-comment policy pass. External evidence: `evidence/telegram-outbound-preflight-{unit,pg}.*`. Full Inbox constructor/ledger typing and the independently reproduced membership request/response identity gap remain the next bounded work; no broader permission-rule or cache-TTL change is claimed here.

### Telegram membership proof and getMe identity boundary (2026-10-26)

Separate child of qualified `75b882b3`. The original read-only actual Inbox/HTTPX probe requested user42, received a member result identifying user99, and still granted/cached permission for42. The original synthetic-only counterexample is retained as local tool `evidence/telegram-membership-identity-review.json`; no live API or source edits were used for that probe.

The active authorization workflow now projects required chat/sender identities and binds the actual positive requested user ID to the actual returned user ID before permission evaluation or cache insertion. Known membership states and restricted membership are distinct strict models; restricted requires its reported Boolean is_member. Creator/administrator/member and restricted-current-member retain the existing allow policy; actual matching left/kicked results remain denial. Unknown/missing/malformed role facts cannot become a false permission decision. The cache contains named verified observations plus local monotonic expiry, keyed by the actual request selector/user context. Its selector is request provenance, not a returned/resolved chat identity: getChatMember does not report a chat ID and none is invented. Existing local 600 s TTL remains unchanged; expired entries require new proof, and changing the requested selector cannot reuse a different lookup's observation.

API lookup failures (including400/403) now propagate as unavailable rather than being cached as false membership. Actual durable input remains RECEIVED/retryable with the existing processing-error policy, without an UNAUTHORIZED label, question or delivery. A genuinely unreported private sender is explicitly unverifiable and is not replaced by the chat ID; configured trusted chat scopes still admit protocol-allowed absent senders without a membership lookup. The active membership query requires a real configured nonblank selector before HTTP. Missing supported identity facts fail explicit validation rather than acquiring default strings/zero IDs. getMe now requires the actual reported positive ID and strict is_bot assertion before caching; it never guesses identity from the secret. Unconsumed profile/admin/expiry fields are explicit wire projections, not full User/ChatMember facades or guessed permission facts. Primary contracts: https://core.telegram.org/bots/api#getchatmember, https://core.telegram.org/bots/api#chatmemberrestricted, https://core.telegram.org/bots/api#getme and https://core.telegram.org/bots/api#user.

Retained optional/cache cases: AuthorizationSubject.sender is required nullable only to express omitted protocol from; a present null/malformed sender is invalid. bot_id=None is the unqueried local state, not a provider ID. The membership map is a genuine associative index over request context; no observation means no cached fact, never denial. Cache expiry is a local monotonic policy deadline, not a claimed source timestamp or inferred membership-until date. Role-specific unconsumed API fields are not required merely to evaluate the existing policy. Shared synthetic replies now declare actual matching user identity and actual bot flag; independent foreign-user controls prevent a request-echo-only test claim.

Evidence: final 290 connected units pass, zero skips, 8.80 s (p90 .184 s/max .553 s), and all 8 actual PostgreSQL cases pass, zero skips, 5.12 s. Actual workflow tests distinguish unavailable/foreign proofs from matching known denial, preserve durable input and no cache/queue effects on uncertainty, preserve trusted sender-optional scope, verify role-specific strict flags, TTL/request scope and verified bot caching. Initial286/7 gates are retained; a test-edit script diagnostic did not modify source and was corrected before the final gate. Strict source2, default Inbox/new-test, Ruff/check-format/source-comment checks pass. Mypy explicitly still notes that the broader Inbox constructor body is untyped; the new critical wire facts are runtime-validated, and this is not a whole-module strict/type/layer completion claim. External evidence: `evidence/telegram-membership-{unit,pg,final-unit,final-pg}.*`. Full stored-message/question/ledger models, constructor ports and remaining presentation/CLI/scheduler audit stay open.

### Local Telegram stored-answer boundary (2026-10-26)

`contracts.telegram_state` now owns the consumed recipient, settled-answer and answer-chunk variants. `question_answers.AnswerStatus` is an exact temporary reexport of the inward alias. `TelegramInbox.__init__` has real storage/settings/client ports; this does not close the remaining untyped routing/presentation methods. Raw repository mappings remain at the storage projection/write boundary and retain unrelated fields verbatim. The query filters are associative storage selectors; configured chats and credentials remain genuinely nullable while intake is disabled or an already-sent receipt is reconciled without HTTP.

`settled_question` requires the stored outcome and its actual phase fields. ANSWERED text and FAILED reason may explicitly be None (real end-turn/file-only or unreported-failure semantics); supplied blank strings refuse. A closure requires its actual formatted reason. No missing status becomes an invented failure. `TelegramQuestionRecipient.reply_to_message_id` is optional because an initial thread reply may be absent; present references must be positive IDs. Chat integer/string representations are admitted explicitly and compared only after validation.

`answer_chunk` projects only consumed attempt facts and checks actual question/session/chat against the recipient before a SENT receipt can suppress a POST or authorize DELIVERED. Missing/foreign scope and nonpositive/bool message references refuse before effects. GENERATED/FAILED need no returned message ID; SENT does. Metadata such as timestamp/text/attempt remains preserved in storage rather than being invented by the read projection. Terminal failure records without a chunk are a separate writer phase and are not chunk receipts. Existing uncertain GENERATED-vs-FAILED retry accounting remains unchanged. One authored interrupted-send fixture now includes the session/chat that the real chunk writer always emits; its one-definite-retry oracle is unchanged.

The closed file outcome contracts are reserved for the coordinated required-both-ID Session file caller. That breaking API is not activated here. Existing file delivery scope validation and broader Inbox typing remain open, as does the historical terminal-failure attempt default audit.

Evidence: full Telegram units 301 passed/zero skips (`telegram-state-final-unit.xml`, 8.93s); full Telegram actual PostgreSQL family 9 passed/zero skips (`telegram-state-qualified3-pg.xml`, 5.40s). New tests exercise foreign-session/chat and invalid SENT identities with no HTTP/no stored mutation, plus verified cached reconciliation without a credential. Actual PostgreSQL preserves the foreign immutable row and proves the independent matching recipient settles. Earlier new-fixture failures (required collection clock, required first attempt, attempted rewrite of immutable history) are retained in the initial/qualified/qualified2 PG logs; no production relaxation was introduced. Strict contracts/writer/new-test checks and default Inbox checking pass; the entire Inbox is not yet strict-clean.

### Stored-answer admission follow-through: ambiguous attempt history

Review found that the actual `TelegramInbox.answer` comprehension filtered missing/null chunk fields before strict admission. Eight real-caller negative controls reproduced a new POST for GENERATED/SENT history with missing/null chunk or purpose (`telegram-state-history-before.xml`, 8 failures retained). This was a caller defect despite the individual chunk model being strict.

The query now excludes only the explicitly separate ATTACHMENT, NOTICE and PROMPT purposes. Every ANSWER row, and every historical row whose purpose is absent/null/unknown, reaches `answer_chunk` before any new marker, POST or DELIVERED write. Missing/null chunk therefore refuses rather than inventing an index or assuming an empty attempt history. Original stored rows are preserved; no quarantine identity or receipt fields are synthesized. A terminal failure ANSWER record without a chunk also cannot authorize a fresh chunk send: it refuses this caller admission, superseding the earlier blanket statement that such records are simply ignored. Known other purposes remain separate only because actual named writers emit them explicitly.

Evidence: all 311 Telegram units pass/zero skips in 6.31s (`telegram-state-history-unit.xml`), including the eight no-HTTP/no-new-ledger/no-question-mutation controls and three separate-purpose positive controls. All 13 actual PostgreSQL Telegram cases pass/zero skips in 5.76s (`telegram-state-history-pg.xml`), including each missing/null purpose/chunk with immutable original history and no effects. The file sender API remains held separately.

### Held condition-evaluation application result (separate from prompt/file APIs)

`Scheduler.eval_conditions` now accepts a required actual `Timestamp` and returns `contracts.scheduler_results.ConditionEvaluationResult`. Missing occurrence refuses before evaluation; the application no longer invents a current cutoff when a payload omits it. Queue adapters must construct the timestamp from admitted job facts. Replay can pass its existing trigger timestamp directly. The prior return dictionary's three fields remain the explicit serialization shape: evaluated IDs, fired IDs and deferred rows. Named `DeferredCondition` records admit only actual BATCH_BUDGET and READ_TIMEOUT causes. Lists are created empty only by a real evaluation with no corresponding outcomes, not as a missing-provider substitute; their original processing order is retained.

The application accumulates three named outcomes rather than a mixed record dictionary. `requirement_sets` remains a genuinely associative index from event/token to unique metric/window pairs; it is not an execution-result bag. Existing lower watch/Truth helper and repository shapes, the Scheduler constructor ports and the rest of the Scheduler methods are still open typing/model work. Default source checking is not a claim that the whole module is strict-clean. No opaque full Truth result was introduced. Reaction results remain unchanged in this checkpoint and are the next separate producer tranche.

All existing own evaluation callers now pass timestamps admitted from their original explicit cutoff fields; no precision/clock facts were padded. Existing fired/fatal/hourly persistence/read-budget effects remain asserted. The READ_TIMEOUT test now reads typed result attributes and the same actual deferred ID/reason. Separate tests prove a missing occurrence cannot turn into now or write jobs, and a real empty evaluation produces explicit empty outcomes. An initial new fixture omitted the reason required for a missing occurrence and failed before the application; that diagnostic is retained, and the corrected fixture truthfully declares NOT_CAPTURED.

Qualification: all 396 Scheduler+Telegram units pass/zero skips in 7.59s, p90 0.027s/max 0.406s including fixtures (`scheduler-evaluation-qualified-unit.xml`). All 14 actual Scheduler+Telegram integration cases pass/zero skips in 10.07s (`scheduler-evaluation-actual.xml`). Contracts/new tests independently pass strict mypy, Scheduler default checking passes, and Ruff/format/source policy pass. This breaking API remains held for the agents-owned actual worker timestamp/result serialization and analysis/quality caller companions; no raw dictionary compatibility wrapper is retained.

### Independent preliminary-card evaluation caller companion

Quality delegated only the seven tests/test_preliminary_cards.py eval_conditions cutoff calls. This clean branch normally joins root2f3d389a and analysisf54, with no held prompt, file or reaction-command ancestry. Each call passes propagate_timestamp over its original authored cutoff map; no timestamp value, precision declaration, deadline, policy or assertion changes. In particular, the formerly unreported precision remains unreported rather than being invented from ISO formatting. Timed review, disarm race, original-session notification, SENT-after-receipt and archive oracles are preserved.

All 35 file unit cases pass/zero skips (15.92s, p90 0.437s/max 0.640s; preliminary-eval-unit.xml), and its complete actual PostgreSQL selection passes 1/1/zero skips (4.73s; preliminary-eval-actual.xml). Ruff/format/source-comment checks pass. Whole-file mypy is not green: 22 current errors remain at unchanged raw fixture mutation statements (91/576/750) and an unguarded optional current_task().get_name() (869); preliminary-eval-mypy.log retains that evidence. No suppressions, casts or unrelated fixture rewrites were added in this delegated API repair.
