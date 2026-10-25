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
the exact owned engine lease. Releasing an older lease closes its backend while
preserving a newer binding, so reload cleanup cannot disable its replacement.
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
