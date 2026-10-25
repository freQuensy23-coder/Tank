# Analysis refactor audit

This is a live audit, not a claim that the complete workstream meets the five-point gate. Work runs only in the authorized isolated server worktree, with pinned Python 3.13.13 and frozen dependencies. Production state is unchanged.

## Implemented domain boundaries

| Modules | Defect and correction |
| --- | --- |
| market.funding / normalize.funding | A numeric zero realized rate was replaced by forecast truthiness. Strict FundingObservation preserves zero, validates finite rates, positive intervals, declared funding kind and timezone, and normalizes rates with explicit Decimal precision. |
| market.book_contracts / market.books | Unvalidated instrument and level dictionaries drove stateful book calculations. Strict BookInstrument/BookLevel/BookSummary and nested depth models now validate supported venue/market identities, verified OKX base multipliers, exact quantity/checksum spellings, and precise two-field serialized levels. Methods are fully typed. |
| market.rate_gate | Nonpositive capacities deadlocked work; state was implicit. PriorityGate now rejects invalid capacity and types admission, futures, priority, cancellation and release. |
| market.capture | Stored dictionaries mixed with anchor precedence and verification truthiness. EventAnchorCandidates/EventAnchor and pure choose_event_anchor preserve verified exact T0/public T1/known-time precedence and precision. Canonical event_anchor returns the domain model; consumers use .value and explicit .to_wire at serialization. Missing escalation events cannot fabricate a current-time anchor. |
| analysis.identity / event_tools | Identity resolution was embedded in tool orchestration and silently coerced malformed identities. Strict registry/entity/token/qualified-alias models now drive typed pure resolution with whole-word qualifiers; configuration projection is explicit. |
| analysis.positioning / analysis.truth | OI decomposition passed dictionary blobs through financial logic. OIDecomposition/MissingOIDecomposition and pure typed classification preserve exact quantity/mark/interaction terms, missing USD evidence and strict-majority semantics independently of rounded display shares. |
| analysis.watch_contracts / event_tools | Watch predicates/counters/snapshots were permissive dictionaries. Typed recursive predicates, persistence, exact-majority values, observations/history, WatchCondition/WatchCall/MilestoneSnapshot now validate full producer records before persistence. GenerationProvenance is a lower-layer shared contract. |
| analysis.watch_policy / analysis.decisions | Monitoring policy used nested dictionary fallbacks inside the decision engine. Pure build_watch_conditions consumes WatchReferencePrices and returns WatchProposal/typed expressions; the engine serializes its output explicitly. |
| event_tools.web_search | SearchHit models were consumed as dictionaries. Read named attributes; serialize explicit model dumps only at retained evidence boundaries. |
| event_tools.propose_expansion | New scout jobs used obsolete prp_id payload spelling. Producer now uses canonical proposal_id, coordinated with runtime durable-boundary decoding. |
| market.__init__ | Pure contracts eagerly imported service adapters, creating a package/service SCC. Removed implementation reexports; explicit imports are canonical. Clean-interpreter verification shows book contracts do not import market.service. |

## Retained fallback and optional-value categories: exact symbols

- IdentityRegistry.from_wire: the sole .get is an absent optional identity namespace, valid for a partial registry. Present malformed namespaces fail. EntityIdentity.name/names/aliases/qualified_aliases and TokenIdentity.symbol/entity_id express actual optional registry identities. Entity/token ids themselves are required. QualifiedAlias.requires_any cannot be empty.
- resolve_entity.name: None denotes no supplied name; entity_identity rejects missing names when creating identities. Explicit external chain token ids remain supported. Dictionary de-duplication in resolve_tokens/token_identities is an actual keyed identity operation.
- BookInstrument.quote_asset/quote_usd_rate: unavailable quote valuation leaves USD depth unavailable while native depth remains measured. USD-to-USD conversion is exactly one by denomination identity, never an assumed stablecoin peg. contract_multiplier is optional for already-native levels; OKX perpetual books require it.
- BookLevel.quantity==0 is a level deletion, not absence. price_spelling/quantity_spelling retain provider checksum text. OrderBook bids/asks/_prices/_strings are actual keyed incremental indexes. Idempotent price deletions retain pop(..., None).
- OrderBook sequence/checksum/previous/first are provider transport capabilities; ts absence explicitly yields COLLECTION_ONLY provenance. Unsupported checksum verification raises. Empty books, crossed books and sequence gaps remain explicit integrity failures.
- DepthBands may be unavailable when received levels do not cover the band. DepthQuantity.usd may be None for unavailable FX; measured native zero stays zero. BookSummary gap and timestamp-precision reasons express actual integrity/provenance state.
- EventAnchorCandidates event_time/public_at/known_to_us_at/state_verified are observational optionals. event_anchor has four boundary presence .get calls. Pure choose_event_anchor has no dictionary/default logic. capture_boundary_points.anchor=None is a supported cutoff-only request; a supplied anchor must contain a timestamp.
- OIDecomposition.pos_share is None only when exact absolute terms sum to zero. MissingOIDecomposition fixes null terms plus explicit reason/detail. oi_class returns None for unavailable normal range or decomposition; missing evidence never votes native or markup.
- WatchPersistence and WatchProposal encode fixed variant invariants. WatchReferencePrices optionals are unavailable observations; policy may explicitly use the event anchor when entry evidence is absent, never zero. Open-call invalidation takes precedence.
- WatchCondition lifecycle fields are absent before their corresponding observation/transition. counters is a real collection keyed by predicate path. WatchCounter.history can be empty before history exists. WatchObservation matched/dependency/count fields differ by persistence branch.
- WatchCall close fields are absent while open; price text is decoded exactly at its storage validator and must be finite/positive/nonboolean. MilestoneSnapshot outcome/status/expiry/missed fields represent different observations; a timer miss is not a source outcome.
- Shared GenerationProvenance nullable fields preserve real unavailable facts; no guessed model, prompt or session identity is supplied.

## Dictionary and serialization boundaries

Converted domain calculations depend on named models. Dictionaries remain for actual keyed indexes, identity de-duplication, predicate-path histories and explicit wire/configuration/repository projections. IdentityRegistry.from_wire and BookInstrument.from_wire select named semantics from larger registry records without changing retained evidence. EventAnchor.to_wire, BookSummary dumps, watch proposal dumps and the existing MTA output are explicit serialization boundaries, not mapping facades used by domain calculations.

## Tests and checks

New tests cover realized zero and exact rate normalization, invalid financial values/timezones/identities, checksum lexical fidelity, missing-versus-zero USD depth, independent frozen books, gate priority/cancellation/capacity, typed anchors and mirrored capture boundaries, qualified aliases, malformed registry identities, typed recursive watches and invalid predicates.

Recorded focused runs: 53 market/precision cases passed; 103 truth/formula cases passed; 175 decisions/tools/watch cases passed; 198 combined decisions/holders/tools cases passed after full-score/render-view integration; 139 typed-anchor market/truth cases passed. These use -n auto and zero skips. Ordinary mypy passes identity, positioning, watch_contracts, watch_policy, book_contracts, books, capture, funding and rate_gate after the shared typed JSON/util changes. Changed files pass Ruff checks/formatting.

HTTP callback annotations were corrected at 39 sites to httpx.Request, retaining the actual lending_repository pytest fixture. Capture tests now match the real complete Go response and verify retained digest/bytes. Priority tests verify urgent cleanup independently of internal key representation. Missing-score fixtures explicitly declare all seven NOT_ASSESSED dimensions, with no fabricated assessed values or rendering fallback.

## Remaining migration: not complete

| Existing module | Incomplete functions at audit | .get calls at audit |
| --- | ---: | ---: |
| analysis.truth | 129 | 975 |
| market.service | 77 | 235 |
| event_tools | 68 | 475 |
| replay | 66 | 213 |
| analysis.cards | 28 | 278 |
| fixture_reference | 22 | 200 |
| calibration | 15 | 23 |
| review | 14 | 108 |
| market.supply | 14 | 49 |
| market.normalize | 13 | 56 |
| market.flow | 9 | 30 |
| analysis.decisions | 7 before watch-policy extraction | 117 before extraction |
| replay_evidence | 5 | 9 |

These counts inventory open work, not a claim that every .get is wrong. Required next work is substantive domain conversion of market snapshots/results/truth, remaining decision/card use cases, event-tool requests/results/orchestration, replay packet/provenance/sealing/review/calibration, and all owned test annotations. Every remaining fallback must be justified by exact symbol at its true boundary. Immutable fixture evidence, units, missing-versus-zero distinctions and cutoff/receipt admissibility must remain intact. Final complete unit/integration checks are still required. Additive models do not close this inventory.

## Capture lifecycle migration, canonical v2

The active control path separates strict capture_contracts, required capture_ports, capture_lifecycle orchestration, capture_repository persistence decoding/serialization, and capture_composition. MarketService delegates escalation, protection, cap enforcement, tier changes, readiness, bounded waits, repair requests, and restart obligation scheduling. Python writes only control requests/HOT policy; Go remains the external capture producer. Canonical Go1901d63 supplies actual worker-start clocks and releases its in-flight obligation guard.

Corrected defects: missing thresholds file no longer becomes empty policy; configuration validation precedes owned HTTP allocation. Permanent Python backfill markers no longer prevent retry after durableFAILED. Readiness/waits bind to the exact current/requested event anchor instead of an unrelated latest started_at row. Missing events and scope mismatches fail. Pending started_at is unavailable/PENDING rather than request creation time. Snapshot control-history ordering uses recorded_at only when started_at is explicitly unavailable; market observation cutoff clocks are unchanged. Missing declared currentEMS targets fail rather than choosing another assessment. Missing armed-watch expiry fails rather than protecting forever. Repair COMPLETE/FAILED/EXPIRED/INCOMPLETE/replayNOT_RUN outcomes are explicit. A COMPLETE empty instrument set is valid. Writes reread the durable acknowledged row even when idempotency returns no changed rows.

Retained cases and exact reasons:
- CaptureMetadata/CapturePointMetadata nullable headers mirror physical nullable market columns and retained Go rows. Unavailable historical starts remain unavailable.
- RepairPending/Running/Failed/Expired preserve raw budget_s SQL numeric text or absence, including NaN evidence. Claim precedes worker validation; expiry can precede invalid-input classification. NewRepairRequest requires positive finite Decimal, nonempty token, ordered bounds and future deadline. RepairComplete validates usable fields. Legacy deadline-bounded missing budgets are read-only variants; new commands always provide budgets.
- CaptureReadiness.backfill_id is absent only for NOT_REQUESTED. CaptureWaitOutcome can lack an ID when no obligation existed within its bound.
- HotTokenIntent.expires_at=None means actual open-call/armed-watch protection. Unprotected72-hour TTL remains existing explicit policy. No assessment explicitly maps to LOW priority1; malformed metadata never does.
- TokenEscalation.priority=None selects validated event priority; explicit zero is retained. hot_token_cap=0 disables HOT admission; negative capacity fails.
- Maps are real keyed collections: instrument repair counts, condition expiries, token-grouped HOT rows and obligation-keyed wait observations. Repository projections/redaction/control documents are persistence boundaries, not domain blobs.
- An absent current_ems_id permits validated history selection; a dangling declared pointer fails.
- Empty tier history starts sequence1. Matching tier/cadence/config suppresses duplicate transitions.
- Physical control tables store timestamp triplets, not bucket-origin columns. Source event provenance remains authoritative; application anchors retain Timestamp.
- Current Settings.configured_secrets is materialized on each control write for recursive wire redaction, preserving rotation.

Evidence: six memory adapter/application cases cover pending clocks, strict new-budget rejection versus raw claimedNaN, missing watch expiry, exact-anchor waits, explicit repair timeout, tier/demotion history. Actual PostgreSQL command roundtrip passed1case/zero skips.44 focused service/capture/adapter units passed;42 supply/selective-loader units passed after fixtures supplied real baseline thresholds. Full combined memory/PostgreSQL/Go capture proof remains required after runtime/scheduler/e2e consumers migrate.

Remaining scope: MarketService snapshots/history and full persisted Market Truth still use dictionary domain processing. capture_changes/metadata readers and other owned modules still require full five-point audits. This application migration does not establish completion of those paths.

The activated external capture/PostgreSQL selection now passes all4 collected cases with zero skips in6.68s, including repair COMPLETE/FAILED/boundedINCOMPLETE exchanges, escalation control creation, actual physical clocks, shared ledger behavior, and the new typed command adapter. The pending-start assertion now explicitly requires unavailable start/PENDING and a distinct creation collection clock. Go peer payload decoder proof and combined runtime/scheduler/e2e integration remain outstanding.

The HOT synchronization and startup-tier paths now also consume validated HotToken records through the coordinator. An explicit never-refreshed state forces reconciliation on first synchronization, even when an injected monotonic clock starts at0. The generic MarketService._put path now refuses all three capture-control collections; caller inventory found no remaining production caller after delegation. This removes its alternate permissive control writer and its fabricated intent return on an empty idempotent acknowledgement. A behavior test verifies that bypass fails without creating a durable row.48 focused control/service cases pass in4.23s.

Combined capture integration now passes all17 collected cases/zero skips in9.16s: memory/PostgreSQL automatic refresh/cancel/concurrency, scheduler late capture, actual external repair/control storage, guarded runtime capture consumption and both native-condition replay states. Consumers merged exact agents67fe7b4 and ingestionf1239db, preserving canonical ancestry.

ReplaySourcePolicy is a narrow captured-metadata input model, not an active SourceItem substitute. source_id/subtype/author_role may be absent from historical captured prefixes; that absence is explicitly SOURCE_INGESTION_POLICY_NOT_CAPTURED through the shared initial_relevance API. The three retained boundary get calls encode this genuine uncaptured state. Invalid captured shapes fail validation rather than becoming unknown. Active SourceItem required fields remain strict. This corrects my earlier erroneous assumption that source_id was always recorded in historical replay input.71 replay unit cases pass in2.89s; original native watch predicates, causal evidence and3-second deadlines remain intact.

Retained actual Go producer outputs are now byte-identical fixtures under tests/fixtures/capture_lifecycle, with origin and SHA-256 documented beside them. Three Python cases decode the real nested BackfillResult/RepairResult, preserving complete flow versus missing klines, real pre24 trade identity,30 repaired minutes, and unexecuted expiry. These are memory-store producer rows, not SQL projections; missing legacy metadata was not filled. Together with actual PostgreSQL control-envelope behavior and17 combined application integration cases, this establishes the migrated lifecycle boundary's independent producer/consumer evidence. It does not migrate capture_changes/stream/snapshot domain dictionaries or full Truth payloads.

## Calibration classifier/query boundary

Calibration now composes JevClassifier over a narrow immutable file score ledger and RoutingSelector over a real isolated read model. The ledger no longer pretends to implement a full queue/database, and routing no longer requires constructing a full Pipeline. Existing JSON score records are validated as JevScoreRecord on read and write; key/usage identity mismatch fails. HTTP ownership stays in acquire. Cached malformed scores are rejected, and a second identical acquisition makes no additional classifier calls.

The calibration routing read model explicitly opens DETECTED events and OPEN sessions at their labeled historical prefix. This replaces the old unsupported event lifecycle OPEN sentinel. DETECTED describes the counterfactual labeled event's first recognition for candidate admission; it is not claimed as captured production lifecycle evidence and never enters the Jev request state. Optional quoted/organization/identity-hint/monitored-author metadata remains unknown when not recorded in the frozen source packet. Required item/source/author/title/text fields are validated by RoutingItem at this boundary.

Removed the dynamic settings proxy and its arbitrary getattr; registry/config are loaded explicitly and routing_window_days is required. The absence of an OpenCode key is explicitly provided for the free pinned endpoint in the test, not silently recovered from a retired provider key.

Four calibration unit cases and the historical-clock/cache integration case pass. The integration now also rejects a corrupted cached score outside0..1. Full packet/label/report domain-model migration and remaining calibration function annotations are still open; this checkpoint completes the shared classifier/query consumer interface only.

A shared storage-order change exposed an existing MarketService window-cache defect: new late points were inserted while rewrites still used list positions computed before insertion. Both independent flow/book fresh-read equivalence cases failed. The cache now merges changed points by actual storage key and sorts by semantic time/key, preserving book-level retention. No oracle was weakened. After merging exact Pipelinea8666d5, the complete replay/calibration/capture consumer selection passes120units in5.07s and35actualintegration cases in20.28s. Replay has no direct Pipeline construction remaining; calibration is the explicit classifier/routing consumer.

Canonical Go dcf823d now fails REPAIR_FAILED on unavailable initial/final reads instead of inventing0/0 or before/before counts. The Python companion keeps RepairFailed.result as typed RepairResult|None and preserves observed instrument entries in RepairUnavailable. No unobserved instrument is assigned counts. A coordinator/real-memory-boundary regression retains30/2 only for the observed instrument; all16 focused unit cases pass in4.42s and4actual PostgreSQL lifecycle cases pass in6.85s. Terminal budget/deadline rejection and bounded timeout retain result=None.

Integrated7 repair cohort: after merging the canonical narrow historical extraction repository e4c8c67 and explicit unknown progress subject420bf71, all47 collected actual integration cases across replay, fixture_reference, event_tools, card_template and truth_condition_postgres pass with zero skips in35.23s. Historical prefixes were not enriched with collector provenance. The card test explicitly disables unrelated ops progress on its narrow delivery settings; selective PostgreSQL loader uses real baseline thresholds and full declared cadence keys. The replay abort synthetic fixture cohort1d491ea passes11units, retaining its sparse historical preservation case unchanged.

### Accepted source completion and native concurrency

EventToolService._complete_source now calls the authoritative typed SourceExtractionRepository.complete command: required item identity is checked, exactly DONE/decisionNone/reasonNone is written, and unrelated captured fields are preserved. No prior extraction status, URL, author or collector identity is invented. A missing/deleted source returnsNone from the repository and raises in _complete_source so the enclosing accepted-fact transaction cannot silently succeed.

Lock-order audit:
- EventToolService.dispatch for record_event: optional actual session lock, then ingest-dedup, then transaction, then event lock. add_observation starts at ingest-dedup and does not acquire a session lock. record_event's later same-session lock is reentrant within its wrapper.
- Pipeline.filter_item/route_item: filter/routing admission lock, then ingest-dedup and transaction. Their protected paths record progress/enqueue jobs; they never acquire event or session locks.
- SourceIngestion.capture/ingest: ingest-dedup then transaction; no event/session lock.
- SessionService.process_event native failure: session lock then the extraction repository's ingest-dedup/transaction. CAS only changes the real eligible prior state, so a completed source is preserved.
Thus the shared source edge does not add an ingest-dedup -> session/event acquisition to Pipeline/source ingestion.

A real concurrent MemoryRepository regression pauses actual record_event after its event write, overlaps actual SessionService terminal failure, and verifies bounded completion, final sourceDONE, accepted event/observation retention and canonical-JSON equality of every unrelated source field. Missing URL/author remain missing. Owned tasks are released/cancelled and joined on failure. The single case passes;107existing event-tool units also passed before the new case was added. All47 actual replay/reference/event-tool/card/selective-PG integration cases pass after this change in35.95s. Two PostgreSQL fixtures now construct the service with its actual repository instead of mutating repo after its typed repository adapters were bound.

### Explicit processing clocks, partial utility closure

Audited processing observations now use clock_iso/utcnow explicitly: EventToolService.publish_card:cutoff; MarketTruthService.assess:current minute cutoff; MarketService._put:recorded_at, _late_normalized_changes:open availability end, _save_day_counts:saved_at, _warm_series_counts:assembled availability, condition_snapshot/snapshot:assembled_at, snapshot:shared assembly epoch and capture-status observed_at, coverage:artifact assembly; SupplyCapture.record:local observation/receipt and unlocks:discovery observed_at. unlock_schedule uses an explicitly measured evaluation clock only when now is absent; a supplied value must be ISO/aware and is not replaced because it is falsy.

These are processing/query observations, not substitute occurrence times for missing provider data.235 affected event/replay/truth/market/supply unit cases pass in11.54s. Provider numeric timestamp and current-estimate receipt decisions in normalize remain open and are not attested closed by these edits. The required Flow adapter cohort is now the next integration priority.
