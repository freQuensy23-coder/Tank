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

## Trade/flow adapter cohort

normalize.trade now requires FlowInstrument, explicit QuoteUsdValuation, an aware received_ts and declared capture source, and returns NormalizedTrade. It does not look up hidden global FX or choose a default transport. Eight concrete provider DTOs in trade_wire cover Binance, Coinbase, OKX, Bybit WS/REST, Kraken WS/REST and Hyperliquid. Calculations consume attributes from validated decoded records. The sole raw map/list path is the provider wire boundary: registered field projection validates known fields while the entire envelope remains canonical source_payload_json evidence. No model supplies mapping/index compatibility methods.

Trade timestamps use declared milliseconds for Binance/OKX/Bybit/Hyperliquid, seconds for Kraken REST, and explicit-zone ISO for Coinbase/Kraken WS. Numeric magnitude is never used to infer a trade clock unit. Unsupported venue/market pairs fail rather than becoming spot/perp. Coinbase ARCHIVE lacks sufficient original metric provenance and is refused rather than being labeled WS. OKX linear quantity requires the actual multiplier; there is no guessed1. A future FX sample is refused. Native quantities and unavailable USD remain separate.

Retained cases/sites:
- BinanceTrade.f/last_id and TradeSequence.seq/seqId/stream_sequence defaultNone only for unreported auxiliary original IDs/stream metadata. value() preserves field presence and explicit0; it does not replace nulls/zeros with another ID.
- Provider DTO trade-ID fields permit reported null at the wire boundary only so _decoded emits the existing missing-ID error; NormalizedTrade requires a nonempty canonical string ID. IDs/sequence bools are rejected.
- _project.errors=None means no registered public semantic-error translation, not missing domain data. Required omitted fields fail. It is an adapter codec, not a generic domain map.
- _trade_metric.prefixes/suffixes are actual keyed catalogs of supported venue/market metric identities; no default key exists.
- QuoteUsdValuation.rate/as_of/unavailable_reason/basis are foundation-owned explicit variants. USD_UNIT is mathematical USD identity; HISTORICAL_SAMPLE has a real clock; EXPLICIT_UNCLOCKED_INPUT represents only an explicitly declared caller factor (used by synthetic fixtures), not a fabricated historical sample. UNAVAILABLE carries no rate/clock and produces no USD value.
- Replay adversarial mutation serializes a validated FlowBar only when constructing its actual input packet; review serializes bars only at the batch storage boundary. Domain assertions use attributes/Decimal values.

Caller inventory found active consumers only in market_normalize, replay quiet-minute proof, review_typed_market_stored and foundation-owned storage_native_flow. scripts/capture/gen_goldens.py mentions the retired reference in documentation and runs only against its pinned old commit; it was not changed or revived. Current caller proofs:119 normalize/flow/replay unit cases pass in1.73s, including explicit small-ms epoch, zero IDs, boolean-ID rejection and unsupported identity;4actual memory/PostgreSQL/ClickHouse stored-review integrations pass in2.38s. Foundation's native-flow cohort follows this canonical adapter. Other normalize functions and quote lookup remain outside this completed trade boundary and need their own full migration.

### Declared provider clocks and receipt absence

normalize._provider_epoch now decodes only explicitly declared numeric/string epochs, rejecting booleans; _reported_milliseconds searches only registered timestamp aliases and preserves epoch0. kline uses seconds for Coinbase/Kraken and milliseconds for Binance/OKX/Bybit/Hyperliquid; its now=None measures an explicit processing observation. oi uses provider milliseconds, while Hyperliquid current context requires an actual captured receipt. funding distinguishes settled millisecond occurrence timestamps from current estimates: a missing exchange timestamp requires a captured receipt and COLLECTION_ONLY precision. liquidation uses declared milliseconds. No missing provider occurrence becomes the current clock.

normalize._receipt returnsNone only when neither an explicit receipt nor captured _capture_observed_at exists; _received_fields preserves this as RECEIPT_NOT_CAPTURED. Current-only observations reject that absence. Supplied receipt/mark/quote clocks must be ISO with an explicit zone or aware datetime. SupplyCapture.unlocks decodes Etherscan timeStamp and DefiLlama emissions timestamps as seconds, retaining original precision metadata. Provider wire keyed maps remain adapter boundaries pending their complete domain migration.68 focused normalization/Bybit/supply tests pass in5.70s; synthetic numeric receipts now declare milliseconds explicitly, retained payloads are unchanged.

### Book clock boundary

OrderBook.snapshot/update accept only explicit-zone ISO or aware datetime source clocks. An omitted source timestamp remains exchange_ts=None with unknown precision; BookSummary now accurately admits that absence. summary requires a captured receipt and validates its zone rather than measuring a hidden current clock. pending_snapshot's receipt measures the snapshot-processing observation explicitly with utcnow; it does not replace the exchange clock. Optional sequence/checksum remain actual venue-dependent delivery/checksum metadata. The source timestamp remains optional because some documented deliveries omit it; the receipt is required when emitting a summary.63 book/normalization/precision cases pass in1.45s including an omitted-clock snapshot/update regression and naive-source refusal. Numeric fixture source clocks now decode declared milliseconds before this boundary.

### Replay clock injection and utility attestation

Both replay_iso(value=None) monkeypatches are removed. Replay injects its explicit utcnow processing clock; iso remains serialization of a supplied timestamp. The first full unit run after removal passed2171 cases and found one synthetic Binance candle using ISO strings in provider millisecond slots; that fixture now uses real millisecond shape. The125 focused replay/reference/sector cases then pass in2.39s; all27 actual replay/reference integrations pass in30.49s with zero skips, including retained reference clocks, phase boundaries and immutable checkpoint behavior. No frozen payload was rewritten.

Caller audit: normalization functions are not active exchange producers (Go owns exchange acquisition); MarketService only uses normalize venue/decimal/timestamp utilities. The provider adapters now require declared epoch units and ISO/aware internal clocks. fixture_reference.validate_reference's three strptime sites preserve an explicitly checked cutoff_timezone=UTC: date-only literals identify the whole named UTC day, exact/minute authored literals are assigned that declared UTC zone. market.funding.FundingObservation's fromisoformat validator rejects missing zones rather than assigning one. Remaining replay.parse_time(raw[value/ts]) sites decode the explicit normalized anchor Timestamp fields, not numeric provider epochs. There are no intentional numeric or naive inputs remaining in owned active callers of generic iso/parse_time. Whole-module typing and the separate retained-market write-admission audit remain open.

### Required version identity consumer activation

Active calibration.acquire obtains settings.require_versions once, uses that resolved facet for generation metadata and JevClassifier construction. Replay's fresh session and scheduler receive ResolvedVersionIdentity constructed from the already verified captured ReplayInputs config/code identity, preserving captured lineage rather than labeling it with the checkout. These required constructors have no optional version fallback. Synthetic Session/Source/Scheduler fixtures explicitly inject source_support.fixture_versions, whose declaration is test provenance and makes no runtime-resolution claim.136 affected unit cases pass in8.72s and all56 actual calibration/event/replay/reference/capture/card/truth integrations pass in31.19s with zero skips. This activates shared consumers only; remaining owned MarketService/EventToolService/MarketTruthService raw Settings version reads are still an explicit next tranche.

### Capture clock type closure

market.capture.TimeInput is datetime|str, matching its actual semantic clock boundary. capture_boundary_points rejects numeric/bool JSON anchor values rather than accepting a declared-free epoch. MarketService.repair_windows/hot_protected/refresh_hot_expiry inherit that precise input type; None in the latter two means explicitly measure the processing observation with utcnow, not infer an occurrence.56 focused capture/service cases passed before reducing redundant adversarial variants; the final three malformed anchor cases are independently rerun. This closes the six default-mypy type sites found during foundation's strict utility activation without casts or guessed units.

## Complete calibration vertical slice

calibration.py now consumes/returns strict named domain objects throughout packet admission, historical ordering/window checks, filter and routing measurements, cutoff selection, scored rows and report/proposal generation. analysis/calibration_contracts.py owns the complete authored packet schema and calculation results, rather than a generic mapping facade. CalibrationGeneration inherits the required resolved version facet. The application orchestrates existing JevClassifier and RoutingSelector use cases; it does not create a fake live Pipeline, collector source or queue. Current Thresholds is validated through the real settings.thresholds method. File/cache and MemoryRepository query codecs serialize validated models at actual persistence boundaries. acquire returns CalibrationReport; CLI must project/serialize it explicitly.

Defects fixed: permissive packet dictionaries admitted contradictory identities, truthy nonboolean labels and missing original text; domain models now reject them. S25 subtype and S29 author role determine the ingestion filter path, so their absence is refused instead of silently choosing ALWAYS/filter. Missing publication clocks remain absent; present clocks require explicit zones and present evidence digests require real SHA256 spelling. Metrics preserve undefined precision/recall asNone and known zero as0. A wrong existing-event match contributes both false positive and false negative. Failed calibration emits a proposal without an invented routing cutoff.

Exact retained absence/default sites:
- CalibrationSource.quoted/author_organization/identity_hints/monitored_author: classifier input metadata genuinely absent in some authored sources; original item source/author/title/text/IDs remain required. subtype is required forS25 and author_role forS29; elsewhere these may be absent because those source policies do not inspect them.
- CalibrationSource.source_name/url/published_at/observed_at/raw_sha256/content_hash/external_id: optional captured provenance in this authored packet format, never reconstructed from current collector config. Present clocks/digests validate. The frozen packet itself remains immutable.
- CalibrationLabel.event_id=None: negative/unmatched label; clock_basis/label_rationale=None: authored explanation not supplied. CalibrationEntity.role=None represents unassigned role, preserving both string-ID and explicitly authored entity-record variants.
- CalibrationEvent.statement_basis/reviewed_group_event_id/opened_by/known_to_us_at and CalibrationPacket.provenance: optional authored lineage metadata. Historical eligibility is derived from label clocks in load_packet, not guessed from omitted event metadata.
- CalibrationRow.expected_event_id/cross_source_repeat=None: no earlier labeled event occurrence; HistoricalCalibrationEvent.known_to_us_at=None: event never observed in labeled history and therefore not an eligible historical candidate.
- routing_metrics.best/best_score and RoutingError.predicted/predicted_score=None: no candidate met cutoff; expected=None: new-event target. RoutingSummary.precision/recall=None: zero denominator, kept distinct from measured zero. routing_cutoff/result.selected.routing_cutoff/result.routing=None: no cutoff met both targets; report selected lookup preserves this failure.
- report.best_f1 denominator max1 is solely deterministic ranking when all TP/FP/FN are zero; it does not publish a fabricated precision/recall. Candidate-free curves still include the real cutoff0 comparison.
- _Ledger.get/put None: cache miss or a failed durable reread. immutable=False is the shared score-port default; this adapter explicitly rejects it rather than performing a mutable write. Existing immutable identity returns its durable record. .tmp is a file replacement boundary.
- acquire.settings/http=None: application composition selects real resolved Settings or owns an HTTP client; supplied dependencies are not replaced. _routing_candidate.lifecycle=DETECTED is the authored calibration candidate projection used by the original historical query, not a claim about live event lifecycle.
- ScoredCalibrationWire.clock_basis/label_rationale=None retain the same optional authored explanations at the scored artifact boundary.

Exact map/collection sites:
- load_packet.events/history/historical and acquire.sessions/candidate_sets are genuinely keyed event/item lookup collections of typed records/aware clocks/tuples.
- Jev questions/state internals are existing typed classifier contracts. No dictionary domain state remains in calibration calculations.
- CalibrationSettings.load_config(registry) is the actual configuration-wire adapter; RoutingRegistry.from_wire validates its explicit registry projection. Threshold decisions use typed Thresholds instead.
- _routing_candidate entity list is an explicit read-only query projection; _ROUTING_EVENTS/_ROUTING_SESSIONS Collection.encode occurs only at MemoryRepository boundaries.
- _scored_wire uses _LABELS and _CANDIDATE_SCORES codecs only while constructing the scored JSON artifact; _SCORED validates the complete result. encode_json and proposal.model_dump_json/decode_json feed actual JSON/YAML output serializers.

Evidence:14 unit cases pass in2.90s, including independent wrong-match/undefined-rate oracles, policy-dependent missing metadata rejection and malformed clock/digest rejection. The actual acquisition integration passes1.39s with real resolved settings, historical candidate exclusion, exact typed Jev request capture, durable cache reuse and report/proposal assertions. Strict mypy --strict --follow-imports=silent passes all three changed source/test modules; this qualification does not claim imported legacy modules are fully strict. The real frozen packet contains1401 items and1101 events; model_dump(exclude_unset=True) equals its complete original decoded document and original compressed bytes remain identical. S25 observed authored variants are81releases/40commits; there are noS29items in that packet, so separate synthetic policy tests cover that boundary. Previous immutable reference comparison proved1401 item-wire/identity and1101 event-wire equivalence with zero network requests. CLI companion is required before activation.

### Required provenance identity in owned producers

MarketService, MarketTruthService and EventToolService now require an explicit ResolvedVersionIdentity; MarketService passes the identical facet into build_capture_lifecycle. All actual self.settings config/code producer reads in these modules now use self.versions, including persistence stamps, snapshot/dependency identities, assessment configuration, tool provenance and publication metadata. No constructor falls back to raw unversioned Settings. Replay builds this facet from verified captured ReplayInputs and supplies it to its MarketTruth/EventTool/session/scheduler graph. Synthetic owned fixtures declare known fixture identity; they do not turn unresolved Settings into a resolution claim.

The existing actual record_event/publication provenance test now asserts the injected current producer identity while preserving distinct prior-session config/code/model lineage in its evidence bundle; it passes. Initial broad owned units passed655 and exposed only missed module-qualified constructor consumers plus the obsolete raw-Settings provenance expectation. The repaired300-case selection then passed299 with that remaining bundle assertion; its corrected actual publication case passes2.55s. Nonowned runtime/CLI/agent/ingestion/E2E companions are coordinated before combined activation. Pure compute_market_truth's historical config/snapshot/unversioned fallback remains separately open; this constructor change does not claim that large calculation schema migration is complete.

### Strict utility fixture and overlooked provider correction

The strict parser trial exposed two additional real provider paths missed by the earlier source inventory: SupplyCapture.chain_liquidity normalizes DefiLlama stablecoincharts date as declared epoch seconds before propagating its existing precision; supply_truth's dailyHoldersRevenue.totalDataChart likewise decodes seconds. Chart value0 remains observed, future points stay excluded, NaN values remain unavailable, and no24h period is inferred. This corrects the earlier overbroad active-caller attestation; it did not justify restoring a generic epoch guess.

106 zero-argument iso calls in owned synthetic fixtures are now explicit clock_iso observations. docs/refactor/analysis-test-clock-sites.json records every original path/function/line and its fixture clock meaning. These sites create processing/evaluation/lifecycle points in authored tests; none converts a missing captured provider occurrence into now. The optional recorded_at fixture override now distinguishes None from an explicit invalid empty value. Required version-facet migration also found one CANDIDATE.EventToolService fetch fixture missed by the simple-name caller inventory; it now injects the declared fixture identity. Initial broad unit proof passed717 with9 failures only from that omitted constructor; all10 fetch-page unit cases then pass1.52s. Full strict-utility verification follows the exact canonical utility merge; this evidence is not substituted for that run.

### Strict parser follow-through in publication

After merging foundation9baf3c2, the owned selection passed723 with3 failures. Two exposed a further genuine renderer boundary: cards._holders_revenue_text retained DefiLlama's original provider_timestamp seconds string, so its chart display now explicitly decodes seconds before rendering; scalar as_of clocks remain ISO/aware. Its optional clock helper distinguishes None from an invalid empty string. The third was a synthetic nested day-bucket fixture without a timezone: the valid precision-independence case now explicitly declares UTC bucket start, while a separate test rejects the ambiguous date-only value. No frozen date or timestamp was rewritten, and no implicit timezone policy was added. All104 focused card/holders/supply cases now pass in3.43s.

## MarketService operational cache boundary

market/cache_contracts.py now defines strict RequestCounts, SavedDayCounts, SavedKlineDayCounts and DependencyHeadMemo. Service counters update from the current stored immutable counter after awaits, retaining concurrent successes/failures. Day-count persistence validates the entire existing version1 file before mutating the live cache: counts are actual integer complete-minute counts0..1440 per aligned UTCday, keys are unique instrument/day identities, saved clocks are aware, and boolean/negative/overfull/conflicting data is refused. The mutable operational file is not retained evidence and remains untouched on invalid input. A cache miss causes later database reconstruction, not a fabricated market observation.

Service cache map sites are now precise:
- _reads: host -> RequestCounts; explicit initial0 means no requests observed by this process.
- _hot_since: token -> validated HotToken.since ISO from CaptureLifecycle.sync_hot_set; only its token keys select active acquisition scope.
- _kline_day_counts: (instrument,awareUTCday) -> backend-observed complete1m-bar count. _kline_day_counts_saved stores a validated snapshot of that map plus actual monotonic save clock; initial empty map/0clock represents no saved state.
- _pending_market: collection -> persistence key -> Record is the actual normalized JSON batch write buffer after jsonable/provenance stamping, never a calculator domain payload. Its unknown wire fields are preserved for storage.
- _condition_heads_memo: exact query-scope key -> DependencyHeadMemo. The three nullable Record heads are persistence-wire metadata projections for availability/latest/earliest fingerprinting; None means no matching stored row. No financial calculation consumes them as generic domain payloads.
- _last_request: host -> measured monotonic float; _request_locks/_request_pace_locks: host -> typed admission synchronization primitives. Empty maps represent no observed host state.

Retained absence/fallback sites: _load_day_counts.read returnsNone for absent/unreadable/invalid optional optimization files; _load_day_counts returns0 loaded entries, not known-zero market data. Invalid entire files do not partly seed valid-prefix counts. _save_day_counts.force=False preserves bounded flush cadence; explicit force writes the real current snapshot. DependencyHeadMemo.at is always a known assembly clock; caller absence of assembled_at prevents memoization and leaves the backend query authoritative. Existing aggregate query omissions map to mathematical zero only for queried complete-bar counts, not unavailable USD/price evidence. Full MarketService snapshot-domain records remain open separately.

Evidence:56 existing/new service/cache/rate cases pass in4.02s. New regressions verify concurrent two-success/one-failure accounting, actual file reopen without market writes, whole-cache rejection of boolean/negative/overfull counts, and conflicting duplicate counts refusing first-row wins. Default mypy passes service plus cache contracts; strict --follow-imports=silent passes cache contracts. This closes the nine constructor cache-type errors without Any/object payload substitution. Truth's statistical/graph caches and required scheduling port are a separate active tranche.

## Verified retained input scope

Foundation reverified25 application-input artifact hashes from the checked-in acceptance fixture manifest and staged provenance at /srv/tank-refactor-evidence/retained-input-staging.json. Its immutable inventory reports51records:25ingest.source_items and26replay.market_snapshots, with18captured tool results. There are zero directmkt.* records in this verified subset. F-02's hash mismatch is excluded, never treated as an empty fixture.

Replay audit: run_captured_tools excludes replay.market_snapshots before its market batch loop; replay_agent_session.import_prefix likewise excludes that collection before batch admission. ReplayInputs.records returns copies, and ReplayMarketReader.snapshot returns a deepcopy of the selected captured snapshot. Nested snapshot arrays remain read-only calculator evidence and never become ordinary market writes. The actual56integration strict-parser cohort passes with these paths. Thus these25inputs justify no historical writer validation bypass or invented receipt/count enrichment. Foundation's schema comparison and parent artifacts remain independent evidence; deliberately incomplete synthetic rows need honest fault-read projections where they test corrupt stored evidence.

## Beta availability and downstream review correctness

The1.0 fallback in truth.beta had no justified missing-data policy. Insufficient paired history and zero benchmark variance now return no numerical beta with their actual INSUFFICIENT_HISTORY/NOT_ESTIMABLE reason. relative_truth does not manufacture adjusted BTC/ETH returns, adjusted historical samples, adjusted pre-event runup or normal ranges when that regression is unavailable. Raw returns remain measurable. Estimated zero beta is retained as a real value. The configured Blume shrinkage for an actually estimable regression remains unchanged and has an independent2->5/3 slope oracle.

ReviewService.outcome previously used frozen beta_btc without inspecting beta_flags and always subtracted BTC even for benchmark R_RAW measurements. It now requires captured measurement semantics. R_BTC_ADJ additionally requires an explicit successful estimation marker and a finite captured beta; flagged or uncaptured provenance leaves direction unavailable while preserving completed raw return/MFE/MAE. R_RAW compares its actual raw return against the frozen raw normal range and requires no regression. New frozen normal_ranges carry relative_measure explicitly; historical absence is reported as FROZEN_RELATIVE_MEASUREMENT_UNAVAILABLE instead of guessed.

Exact retained None/map sites: beta's unavailable numerical value and relative adjusted-return/range/runup nulls represent unestimable statistics, never a unit coefficient. beta_flags preserves the actual reason. frozen_beta_unavailable_reason uses the persisted relative projection only at the frozen evidence boundary: absent btc flag metadata -> BETA_ESTIMATION_PROVENANCE_NOT_CAPTURED; flagged regression -> its exact known reason; missing/invalid/nonfinite value -> explicit unavailable reason. Numeric-string coefficients remain supported only as finite captured numeric wire values, preserving monetary Decimal calculations. A captured successful zero remains valid. R_RAW's btc_adjusted_return staysNone because the adjustment is inapplicable, not zero. Raw and adjusted direction fields are distinct.

The authored synthetic START positive fixture had constant BTC history and therefore exposed10 positive assertions that had relied on the invalid1.0 assumption. Its positive data generator now supplies nonconstant synthetic benchmark observations; beta is still computed from them. A separate full producer case deliberately flattens those same observations and verifies betaNone/NOT_ESTIMABLE, raw1.1% retained, unavailable adjusted range and UNKNOWN move. No frozen payload or independent positive expected classification was rewritten.

Evidence:212truth/review unit cases pass in11.31s. Direct history/variance/estimable-slope tests and actual stored-price review outcomes cover flagged/uncaptured beta, valid zero beta and explicit raw benchmark direction. Five actual memory/PostgreSQL/ClickHouse review integrations pass in3.72s with zero skips, retaining tiny monetary return precision and immutable frozen-reference behavior. Foundation's replacement UnavailableBeta model is canonical and follows in the typed baseline-cache activation; the old AssumedBeta model is not an approved policy.

### Strict context consumer fixtures

The exposure-anchor fixture now supplies an explicit declared version facet to its EventToolService. The abort-finalization synthetic replay config now declares its actual S01 source display label, required by the typed context registry; this is an authored label-only view for the test, not reconstructed collector provenance. Its failure-case assertions still exercise actual native stream retirement, acceptance loss, timeout/cancellation and durable finalization. All33 exposure/replay-abort cases pass in1.71s; no schema/default fallback was restored.

## Parent95 compatible integration repairs

Authored replay context configs now declare their actual empty entity/instrument namespaces and source label catalogs; the captured CMS feed includes its explicit display name. Reference reconstruction includes the source catalog from its declared baseline when using the full config, and explicit empty label namespaces in its synthetic minimal config. These are authored fixture facts, not production fallbacks or changes to frozen captured registries.

The retained Binance fixture adapters now declare milliseconds for REST openInterestHist timestamps and REST kline array open-time fields, and parse the archive metrics CSV create_time with its proven UTC convention. Original provider bytes are unchanged. Generic parse_time never guesses their unit or timezone.

The current-minute synthetic refresh helper preserves requested minute-bound cutoff/flow observations but moves declared hourly bar open/close fields by whole hours, retaining the actual UTC hourly sample grid. The PostgreSQL positive fixture now declares nonconstant minute BTC observations matching the authored hourly benchmark rather than overwriting it with flat100. Positive paths assert an estimated beta with no unavailable flag; the independent flat-history negative remains intact. No coefficient/default is inserted.

supply_truth now propagates actual fundamentals.ts timestamp provenance into as_of. COLLECTION_ONLY and explicit unknown precision/reasons remain distinct instead of a formatted ISO string becoming an intraday accuracy claim; receipt clocks and cadence are not substituted. A real calculator regression checks both states.

Broad baseline/freshness migration is isolated under immutable stash efc063547cbd58a42aaad677dfc0b6aad0ccf0d1 while this compatible repair is qualified.248 affected unit cases pass4.76s and35 targeted current-minute/holders cases pass2.15s. Actual integration qualification follows on the immutable committed repair.

### Required reaction scheduling capability (activation cohort)

`EventToolService.__init__.scheduler` is now required and implements the narrow
`ReactionScheduling.ensure_event_reactions(event_id)` command. Successful assessment
dispatch always issues that command; the former `scheduler is not None` branch
silently discarded required scheduling work. Runtime composition and operational
replay must supply the actual Scheduler with captured policy and own its cleanup.
This activation is held until those constructor companions are integrated.

`AssessmentFreeDiagnosticScheduling` is an explicit restricted capability for a
verified recorded diagnostic whose complete call catalog contains no assessment.
`diagnostic_call_catalog` validates required call identities/tools and uniqueness,
and binds the entire captured catalog (including arguments) to a digest alongside
its verified input digest. Artifact verification remains the recorded reader's
responsibility; a caller-supplied digest alone does not establish artifact trust.
Dispatch refuses `set_assessment` before schema lookup, storage access or mutation
when this capability is present. It cannot silently absorb a scheduling command.
No live request, quota admission or clock sleep belongs to this capability.

Three behavior cases prove that declared assessments prohibit this mode, changes
to captured arguments change the catalog binding, and forbidden assessment cannot
reach its implementation or change persisted event/assessment state. The named
contract passes strict mypy; Ruff/format pass. Actual scheduling effects and replay
reader qualification belong to the companion cohort, not this initial checkpoint.

### Scheduling consumer qualification

`run_operational_replay` constructs the real Scheduler with the packet's validated
complete `Thresholds`, shares it with EventToolService and closes it in the owning
finally scope. `replay_agent_session` requires that same concrete Scheduler and
checks repository/settings/market/truth identity instead of creating a second one.
The recorded diagnostic runner binds the complete call catalog to the verified
`ReplayInputs.input_sha256`; assessment-bearing catalogs require captured policy
and an owned real Scheduler. Assessment-free catalogs use the restricted capability.
Scheduler's processing clock is patched to the existing deterministic replay clock;
recorded market snapshots remain the read-only external evidence adapter.

Synthetic operational packets in test_replay, test_fixture_reference,
test_replay_abort_finalization and test_replay_session_boundaries now explicitly
capture the complete declared fixture policy instead of `{}` or a lone expiry key.
Frozen packets are unchanged. Other isolated EventTool fixtures declare the
observable `RecordingReactionScheduling` adapter at construction; it records every
intent and never pretends to create jobs. This includes exposure, fetch deadline,
ownership, certainty, claim context, formula, observation, review, token watch and
market-refresh fixtures whose independent purpose does not assert scheduling.
Actual replay/session tests compose the real scheduler from their actual truth
reader. Operational composition remains a consumer-owner responsibility.

`test_assessment_defers_reaction_ownership_to_scheduler_after_commit` preserves its
independent transaction/event-lock read oracle and now consumes only the event ID.
`test_assessment_dispatch_creates_real_reaction_jobs` invokes actual assessment
through dispatch and the real owned services: it observes ten durable POST jobs
for the actual BTC/ETH benchmark scope, all five expected checkpoints, and drained
scheduler tasks/closed HTTP resources. The analysis set is not populated until the
later beneficiary command in that authored sequence, so no token is fabricated.

Qualification: 339 owned units passed in 12.40s before the additional real-job case;
the final five capability/post-commit/real-job cases passed in 2.45s. All 56 collected
actual affected integrations passed in 30.29s with zero skips
(`/tmp/analysis-scheduling-integration.xml`). Ruff and formatting pass. Default
mypy reports only the four preexisting Truth cache declarations and the pending
runtime-composition constructor companion; no generic annotation substitute added.

### Statistical baseline caches and declared reference freshness

The statistical baseline slice now consumes the inward models in
`analysis.statistics_contracts`: `hourly_values` uses `HourlyReturns`, `beta` returns
`EstimatedBeta | UnavailableBeta`, OI/funding history uses the concrete range models,
`relative_truth.hist` stores `RelativeHistory`, BTC context uses
`BitcoinContextHistory`, native-volume prefix accumulation stores `NativeVolumeIndex`
and `condition_depth` stores `DepthBaseline`. `MarketTruthService` now declares
`_condition_baselines: BaselineCaches`, `_assessment_baselines: dict[str, BaselineCaches]`
and `_assessment_locks: dict[str, asyncio.Lock]`; those maps are actual token-keyed
collections, not domain payloads. `BaselineCaches.with_capacity(128)` preserves the
single global FIFO budget across families and original non-refreshing update order.
The old untyped local StatisticalPopulation and mutable exposed arrays are removed.

Explicit absence sites: `_BASELINE_CACHE` defaults to None only outside an active
calculation context; full assessments install a fresh or explicitly caller-owned
cache and restore the inherited context on success/failure. Cache `get` misses mean
recompute, never a fabricated measurement. `FloatRange/DecimalRange.bounds=None`
retains the actual empty/degenerate-history reason. `UnavailableBeta.value=None`
means insufficient pairs or non-estimable variance/arithmetic; measured zero beta
remains numerical evidence. `UnavailableHistoryDiagnostics.resolution_s=None`
represents absent actual cadence, with zero observed samples and no invented bounds.
`NativeVolumeIndex.last/last_open_us=None` means no observed endpoint/prefix cursor;
segment continuity breaks on unavailable/incomplete/nonfinite/negative quantity.
An observed zero volume remains valid. `condition_depth` caches actual Decimal USD
means and never inserts a zero for unavailable book depth.

`analysis.price_reference.ReferencePriceSeries` is a statistical reference-price
collection keyed by aware observed times, with a separate actual per-observation
cadence collection. `at` returns named observed/unavailable variants. Its None cases
are precisely no prior observation, cadence not captured, or measured age exceeding
that cadence; no prior-gap inference and no 60-second floor. `return_between`
requires ordered bounds and two fresh endpoints, preserves measured zero return,
and reports non-estimable arithmetic explicitly. Its published freshness projection
omits the internal numerical price so it cannot imply an unverified USD unit.
`relative_truth` consumes required variant attributes for BTC/ETH/peer returns and
propagates unavailable adjusted returns while retaining the actual raw return.

Cadence resolution sites in `relative_truth.cadence_for`: an explicit series cadence,
actual scoped collection metric override, matching declared metric semantics,
collection cadence, or actual bar interval may establish freshness. Missing declared
cadence stays None. Malformed declarations refuse. Mixed compact statistical series
can carry the actual `intervals_s` vector; vector/time counts must match. The authored
compact fixture preserves each source row's interval instead of calling all mixed
hour/minute observations one-minute bars. `MarketService` sector statistical price
projection now explicitly records interval_s=60 because its existing query selects
only interval_s=60 rows. This describes the observed query, not a default cadence.
`compute_condition_values` selected-node projection copies only present fields;
it no longer invents [] for absent capture_metadata/anchor/config-like facts.

Behavior evidence: 237 Truth/review/holders units pass in11.89s; seven additional
freshness/domain/downstream cases pass in2.39s; all16 collected actual Truth,
condition PostgreSQL, review, capture-refresh and benchmark-coalescing integrations
pass in19.22s with zero skips (`/tmp/analysis-baseline-freshness-integration.xml`).
New price-reference source/tests pass independent strict mypy; Ruff/format pass.
Context tests retain the independent concurrent opposite-direction calculation,
outer-context restoration and failure-isolation oracles using real typed cache values.
The sparse benchmark regression retains actual estimable history/raw1.1% move, then
refuses adjustment from the actual stale minute endpoint despite a large source gap.

Remaining work is explicit: `_condition_cache` still stores entire condition graph
nodes (spot/relative/derivatives/supply and composite outputs), and requires the full
named graph/output schema migration. It is not typed as JSON/Any/object to conceal
that work. Default mypy currently reports that one declaration plus the separate
pending runtime scheduling constructor companion. Whole Truth snapshots/results,
calculation input maps, configurable-policy fallbacks and sibling reference-clock
precision defaults remain on the broad owned migration inventory. This tranche
completes the statistical baseline/declared-freshness layer, not the full Truth module.

### Additive admitted-HTTP/page boundary (held activation)

`http_transport.send_admitted` takes actual built requests, a required admission
port and a required per-request guard. It admits every actual attempt/redirect,
checks the admitted host matches the transport destination, runs a declared
first-request accounting hook only after admission, and explicitly disables
HTTPX's implicit redirects. Intermediate responses close. UNCONFIGURED retains
its explicit generic-site meaning; it does not fabricate a provider quota.
`retry_after_deadline` accepts only actual nonnegative finite Retry-After seconds
or a valid aware HTTP date; missing/invalid instructions remain unavailable and
no sixty-second value/clamp is synthesized.

`contracts.page_evidence.PageRead` validates binary bytes against their actual
computed digest and the two-megabyte bound. An original recorded-body claim must
match its recorded digest. Live receipt and verified original-body provenance are distinct named records.
Unknown source Content-Type remains None. No reconstruction variant is introduced
without an actual consumer. Infrastructure
handles are precise protocol/dataclass composition objects in analysis.page_access,
not Pydantic arbitrary types. `LivePageAccess` requires admission; `RecordedPageAccess`
requires its recorded reader. The live reader owns only a client it constructs and
closes all response streams, preserving a borrowed client's lifetime.

Six independent boundary cases pass in1.35s; all five new source/test modules pass
independent strict mypy and Ruff/format. Tests prove actual client query credentials
reach configured admission, configured waits occur, unconfigured redirects invent
no waits, refused destinations never reach transport, accounting hook ordering,
binary digest fidelity, unknown media and client lifetime. These are additive
contracts; Market/Event activation and actual verified offline reader proof remain
held for the companion cohort. The verified replay inventory already substitutes
complete captured external tool results; raw response reconstruction is not assumed.


Actual caller inventory correction: run_captured_tools already intercepts external
web_search/fetch_page through exact captured call identity, arguments and availability;
native replay uses CapturedToolResult. Its old captured_http callback reconstructing
JSON response bytes is unused by those reads and is removed during activation.
RecordedResultsOnlyPageReader binds the verified packet/result catalog and refuses
raw-page requests because this input format retains complete tool results rather
than original response bytes. This refusal occurs before admission/client creation.
No speculative reconstruction model/reader is retained.
### Exact Decimal interpolation repair

Independent review identified that the statistical typing change constructed the
Decimal percentile weight directly from a binary float. For financial populations,
`percentile` now uses `Decimal(str(position - index))`, preserving the established
decimal-text convention, and calls `exact_context(low, high, fraction)` before
subtraction/multiplication/addition. It does not mix float observations into Decimal
populations. This companion is based directly on b123f2f and has no held HTTP ancestry.

Four independently specified exact-equality cases cover USD1.05, a negative funding
rate, very large native OI and adjacent subunit values under an outer precision of6.
There are no approximate assertions. Together with beta, condition calculations,
START fixtures and formula vectors,59 unit cases pass in7.68s; the new regression
module passes strict mypy and Ruff/format. HTTP work remains preserved separately
at immutable stash55c26d818e567184ecf101c9b5a1ca2e8ebd6733.

### Required Python HTTP/page activation API (held for caller cohort)

MarketService now requires admission: HttpAdmission. Every actual built request,
redirect and retry passes that port through send_admitted, and provider feedback
calls the same injected port.block with the actual request and actual Retry-After
deadline. The old shared-global backend inspection and undocumented generic0.08s/
Etherscan0.35s local pacing are removed. The existing priority/in-flight gate remains
an operational concurrency bound. Go-owned exchange URLs still refuse Python I/O,
including redirects; Go transport is never inferred merely from a host label.
A missing/invalid Retry-After refuses the retry instead of inventing60s; an actual
120s instruction is retained without a60s clamp. Generic418 does not invent an
exchange ban classification; its actual instruction can block the host and its
status remains an error.

EventToolService now requires pages: LivePageAccess | RecordedPageAccess. A live
composition requires actual admission; a recorded composition requires its explicit
reader. Recorded fetch refusal happens before any Go enqueue or Python client
creation. FetchPageRequest validates URL and the actual published optional20s
budget default with positive/max20 constraints; explicit invalid zero is not replaced
by20. The retired untyped borrowed-client facade is removed. Actual outgoing client
query credentials participate in admission but never replace the safe persisted
incoming evidence URL. Live errors expose host/status/type, not credential URLs.
The live page receipt requires a real COLLECTION_ONLY instant. Unknown Content-Type
remains None at the read boundary; application/octet-stream is only the explicit
storage format for absent header metadata. Go capture remains its existing separate
queue/result provenance path and does not claim a Python live-page receipt.

New Python page rows retain typed page_read_provenance; source model historical
omission remains omitted. Recorded original-body provenance must match both actual
read bytes and stored raw_sha256. Search results and the separate Go path legitimately
lack this Python-page provenance. No missing source header or occurrence clock is
reconstructed. Complete external captured tool results remain the existing offline
replay input: recorded_page_access binds verified input/result-catalog identity and
refuses unavailable original page bytes. The unused diagnostic JSON-response callback
and its HTTP client are removed; native replay verifies the supplied results-only
reader matches its actual input before proceeding.

Twelve admitted-transport/page/actual-caller cases pass in1.62s, including actual
Market120s feedback wait, missing-instruction refusal, actual EventTool Etherscan
client credential admission and safe stored URL, recorded raw-byte refusal with
live admission/block/wait/client traps, actual binary digest/lifetime and credential-
safe HTTP error behavior. Five new helper/domain/test modules remain independently
strict-clean. Existing constructor consumers and actual retained-reader proof are
still being migrated/qualified; parent must hold this activation until companions.
The clean b123/1459 scheduling/statistical boundary remains independently mergeable.
### Clean scheduling policy fixtures and stale-sector negative oracle

The generic authored test_replay packet now explicitly captures the complete known
fixture Thresholds. The one reference operational-composition unit authors the same
policy before hashing its packet. The real scheduler still refuses a packet whose
required reaction policy is absent; no packaged-policy fallback is restored and no
frozen input bytes change. The earlier339unit result preceded the final strict
captured-policy precondition; this broader fixture check closes that verification gap.

The comparator fixture's invalid_peer_close branch intentionally corrupts the two
latest PEER1 close values and marks its15-minute bar incomplete. An independent
raw-row calculation now proves its latest valid completed close is15minutes stale,
against the actual declared60-second cadence. The preferred historical series can
no longer contribute a fabricated fresh sector return. The test preserves the full
dense-vs-compact equality oracle and actual available raw return, asserts unavailable
sector adjustment/reason for that negative branch, and retains the positive sector
adjustment assertions on all valid-peer cases. This is not a restored gap-derived
freshness allowance or an alternative venue fabricated for the fixture.

All132 unit cases in test_replay/test_fixture_reference/test_market_snapshot_comparators
pass in3.24s with zero skips. Ruff/format pass. This companion descends clean26862f39;
held HTTP caller work remains isolated at immutable e68ab1f1dbe749b934182165618791eb811d85b2.

The separate replay_ingest_policy operational fixture now explicitly captures its
complete known Thresholds as well. Both actual memory/PostgreSQL cases pass in1.94s
with zero skips, retaining all original classifier-output reset, policy rederivation
and imported-source assertions. This is an authored fixture change; no production
fallback or frozen data is altered. This follow-up remains a clean fd077 descendant
without held HTTP ancestry.

Page provenance now requires the actual content_type_header value or explicit None
on both live and original recorded-body variants. PageRead validates this matches
its read media_type. Thus an absent source header remains visible even when the
existing raw-byte storage format uses application/octet-stream. Live receipts refuse
missing/unknown instants. Live HTTP status/transport exceptions retain their HTTPX
categories/types with bounded credential-free messages, preserving SessionService's
SOURCE_HTTP/SOURCE_TRANSPORT protocol instead of relabeling them tool execution errors.
Twelve page/admission boundary cases pass in1.73s; three tightened source/test modules
pass strict mypy. Backend quota-history identity changes remain a separate held team
prerequisite; these results do not claim the current backend is final.

### Owned required-admission consumer checkpoint

Owned MarketService fixtures now inject the real configured admission resolver and
explicit memory budget backend; owned EventTool fixtures declare live page access
or, for operational replay, recorded_page_access of the actual validated packet.
SourceService fixture construction supplies its explicit budget port. None of these
helpers returns a fabricated ADMITTED result. The replay cases retain their actual
shared Scheduler and verified input-bound recorded capability.

The former generic0.08s timing assertions are replaced with actual configured
Etherscan admission oracles: overdue starts do not create a future burst, cancelling
a wait reserves no future request, and freeing a saturated in-flight slot does not
restart the provider window. The generic-site case independently verifies overlap
with zero invented quota wait. Negative-priority budget admission now uses an explicit
real memory backend. The former FakeLedger/Instant/optional-global-ledger fixture is
removed: actual injected memory host blocks observe starts at0,120,720 seconds for
real418/429 feedback, preserving120s/600s without clamps. PostgreSQL cross-process
and host-isolation checks use explicit RequestLedger-backed engines and shutdown;
the binding regression now proves unbound/closed refusal rather than implicit memory.

The owned broad selection passes769 units in34.93s, p90 .85s and max3.35s, zero skips
(/tmp/analysis-http-owned-unit-2.xml). Twenty targeted rate/deadline/source-field/real-
scheduling cases pass in2.74s, and twelve explicit external ownership/rate cases pass
in4.36s. The final HTTPX-category/header refinements have their additional focused
proof. These are held-cohort results; the shared budget identity correction and
combined actual integration/retained-artifact qualification remain required.

Actual retained-input validation was attempted with all live admission/scope/block/
provider-sleep/client-creation traps. It correctly stopped before exposing captures
when fixture_reference required a referenced immutable raw source body absent from
the25 staged input-only artifacts. No hash/derivation check was bypassed and no
production data was read. The scratch verifier is retained at
 data/qa/verify-retained-http-effects.py; no success artifact is claimed. Verified raw
bodies and an explicit historical-path reader are needed for that full loader proof.

The requested exact raw-body inventory is analysis-retained-raw-inputs.json:38 named
source-body path/hash pairs at80 originating sites across all25 hash-reverified
acceptance inputs. It records only source raw/detail/discovery/original-projection
body references and their recorded decompressed-payload SHA256, with checked-in
fixture ID, originating packet path/compressed SHA and exact field site. No raw body,
private map, sealed outcome or credential was read. F02 remains excluded for its
manifest mismatch. The generation scratch script is data/qa/inventory-retained-http-raw.py.
An explicit resolver must accept only these verified staged originals; no global
/data alias or production path access is proposed.

### Exact retained-body and captured-code resource boundaries

fixture_reference_sources owns a precise RetainedSourceReader port. The stateless
filesystem reader preserves ordinary saved-capture behavior. VerifiedRetainedSources
accepts the owned required-reference inventory plus the parent's staging manifest;
it validates the inventory binding and exact reference set, then checks root
containment, transport bytes/hash and decompressed payload bytes/hash on every read.
Unlisted paths refuse before filesystem access; no historical /data alias is created.
Zstd/gzip magic denotes actual retained wire formats; bare bytes remain bare bytes.
Zstd streaming also preserves frames whose header omits decoded-size metadata.
Source_candidate is audit metadata only and is never opened by this reader.

load_inputs passes its explicit raw_sources and projection_code through normal
fixture_reference validation. The original body, time, derivation and host-mask checks
remain. CurrentProjectionCode retains the normal exact-current-file check; the
explicit ArchivedProjectionCodes path only accepts an allowlisted recorded SHA with
actual fixed-path Git evidence. It rechecks archive length/SHA256/Git object identity,
commit→src/tank/fixture_reference.py→blob binding and actual Git blob bytes. It never
executes/imports archived code. Unknown digests refuse. Infrastructure resource handles
remain outside Pydantic; manifests and all structured origin records use strict models.

analysis-captured-projection-code.json inventories5 recorded module hashes at14sites.
One has exact archived bytes at Git e5a1cef / blob28e8928, independently verified by the
parent; four remain unresolved. The immutable .source archive under docs/verification
is evidence, not a restored implementation. Neither its Git origin nor a recorded
module hash claims that code was deployed. Parent independently showed that main's
fixture_reference bytes match none of the five hashes, so these are pre-existing
availability limits rather than a refactor hash regression.

Actual retained proof (analysis-recorded-http-evidence.json) rechecks25 frozen input
hashes, all38 staged raw bodies and the known code artifact. Eight packets pass all
normal loader checks and five actual captured results are selected through the
production reader; all admission/acquisition/block/provider-sleep traps remain zero,
and no live client is created. Eleven packets authentically refuse unavailable
captured code, and six require the deliberately unsupplied host-only identity map.
No validation check is skipped, no private map/sealed outcome is opened, no frozen
bytes change and F02 remains excluded. This is an eligible-reader/no-live-effect
proof, not a claim of full agent acceptance for the refused fixtures.

Eight independent resource cases pass in1.48s, proving unknown-path/hash refusal,
post-construction transport tampering, payload tampering despite matching transport
metadata, root escape, owner-inventory binding, actual Git provenance and non-execution.
Together with replay/reference behavior,134 units pass in1.90s. Four resource source/
test targets pass strict mypy; default source mypy again reports only the independently
reserved whole condition cache. Required preserved cutoff literals now explicitly
reject absent/nontext values; day/exact/minute parser locals remain distinct types,
and publication interval checks require both genuine bounds. Declared UTC and every
frozen literal remain unchanged. The remaining legacy fixture_reference domain-map
migration is still open; these resource ports do not claim to complete that module.
### Clean authored PARTIAL narrative fixture

The native replay publication fixture now supplies two actual factual WHY bullets
for every kind, matching the strengthened public skill/schema. It authors the captured
World-approval text and the fact that that publication does not quantify price or
liquidity effects. It does not synthesize prose from a renderer fallback or claim an
unmeasured economic effect. The first-reference QUEUED/unsent intent and ordinary
missing-SENT refusal oracles remain unchanged. Both actual integration profiles pass
in2.89s with zero skips. This companion is clean3e26558b + exact485 schema ancestry;
held HTTP and decision/card renderer activation are excluded.

### Final paired HTTP qualification repairs

The first full owned selection at e773ee78 executed779 units and83 actual integrations.
Its remaining failures were nine authored publication fixtures with empty/undersized
WHY under the new public schema and two assertions for retired capacity-bearing SQL
bucket text. Seven named positive/publication-purpose fixtures now author their actual
World-approval/product-announcement facts plus the publication's absence of quantified
price/liquidity effects. Vague test headlines are replaced with the actual product
announcement where that was the seed. The overlength-WHY test retains a valid second
bullet so it reaches the intended221-character refusal. Lifecycle, model provenance,
metadata fidelity, durability, generation-clock and bounded-read oracles are unchanged.
SQL ledger assertions now decode the actual two-field stable key/window identity;
capacity is deliberately not part of that identity. Cross-process waits, host isolation,
actual persisted owner rows and unbound/closed refusal remain independently asserted.
All115 targeted units pass in12.07s and all3 actual ledger/ownership cases pass in7.12s,
zero skips. A fresh complete owned selection is run on the resulting immutable head;
previous logs/XML remain preserved separately. No production-policy waiver is involved.

### Qualified complete owned HTTP cohort

The complete owned cohort at immutable c17512bfd10f1bf3d96954288ac332eee85e9697
passes all779 units in30.72s (p90 .845s, max3.04s) and every one of83 collected
actual integrations in83.50s, zero skips. This includes PostgreSQL cross-process
budget sharing/stable identity/host isolation, memory behavior, real20-second fetch
deadlines, replay/reference, publication, reaction and source evidence consumers.
All27 required caller files are present; this is not the earlier API-only a783 head.
The actual shared-budget operational helper is explicitly owned/drained. Paired
canonical Go/backend/SQL/E2E and clean native narrative companions are included.
CI-target Ruff/format (src tests scripts deploy,392 files) and all9 new strict
HTTP/page/retained-resource source/test targets pass. Default mypy has only the
separately assigned whole condition graph cache error; it remains visible.

analysis-http-qualification.json records exact targets, counts and evidence paths.
The known historical artifact limits remain explicit:8normal loader admissions,
5actual captured result selections,11unavailable captured-code refusals and6host-only
identity-map refusals, all zero live effects. These are not acceptance waivers.
No held decision755/renderer activation, production deployment or global completion
is claimed. Whole Truth/Market/event-tools typing and the other owned module/fallback
inventory remain active work after this coherent application boundary.

### Typed decision-watch lifecycle consumer boundary

The additive `decision_watches.DecisionWatchCommand` carries actual decision/event/token identities, generation provenance and allocated watch proposals. It deliberately requires no unrelated financial assessment fields. `reconcile_decision_watches` is a pure calculation with explicit processing clock and lifetime/grace policy; application persistence remains outside it. Matching predicates reuse actual stored creation/expiry clocks; missing required clocks are rejected instead of replaced with now. Rearming requires the actual predecessor firing timestamp. Prepared firing IDs distinguish the explicit all-firings command (`None`) from an explicitly empty set. Agent-authored watches survive engine reconciliation. Dictionaries are keyed final-write and identity indexes, not domain payloads; nullable predecessor/milestone fields mean those actual relationships are absent. Five behavior tests prove reuse, prepared consumption, rearming/cancellation, absent firing-clock refusal and actual call/milestone expiry bounds. Both source and test pass independent strict mypy and Ruff. Application activation remains open in this held card/decision cohort.

### Held decision/card consumer activation checkpoint

The application now invokes the pure decision engine with its validated `DecisionInputs`, materializes actual IDs, MTA cutoff, captured threshold version, known version identity and generation, and persists validated watch lifecycle writes. A tracking-expired copy uses `end_decision_tracking`: financial cutoff/calculation/EMS/MTA/threshold/generation remain frozen while a separate current local status clock/writer is recorded. A missing historical calculation chain is refused, never relabeled current.

Publication now projects actual current contradictions into `publication_gate_inputs_from_records`, decodes the prior card through the narrow baseline adapter, and uses the actual SENT acknowledgement for cooldown. The required `PublicationNarrative` validates supplied WHAT/verdict and 2–3 WHY bullets for every kind; no canonical-statement or empty-prose repair remains. The narrative is frozen in card context. Captured policy supplies character/cooldown limits without local numerical defaults.

This is an intentionally held activation checkpoint, not a ready parent cohort. Remaining work includes canonical durable outlook/watch references, affected synthetic producer fixtures, actual publication/replay integration and the broader typed tool request/context/persistence graph. Seven independently strict pure watch tests and four actual claim-verification adapter cases pass; the actual numerical Truth→decision projection→card codec→renderer integration case also passes with units/windows and canonical audit companions preserved. The broad initial diagnostic identified old test calls and incomplete synthetic call/assessment facts; no validator was weakened.

The publication receipt now validates through the existing strict `contracts.progress.PublicationReceipt` at the result serialization boundary. Every outcome supplies its actual required reason; the former UNKNOWN/GENERATED guess is removed. Accepted alert/version fields are required on published outcomes and explicitly absent on suppression/refusal. Current decision MTA references use their canonical required field. Seventeen current watch/claim/cutoff behavior cases pass. The precision cases now exercise the actual EventTool application producer, rather than asking the pure calculation engine to allocate clocks.

### Held consumer behavior evidence and fixture distinctions

The two actual native condition replay stages (APPROVED/PROPOSED) pass with the original timeout, real durable reconciliation, callback identity substitution and actual watch predicate execution. A callback failure diagnostic now reports its refused tool receipt before a secondary waiter timeout. The synthetic frozen assessment explicitly declares its empty contradiction list, and the prior card context retains the authored empty event contradiction list; these are authored test inputs, not changes to frozen acceptance packets or production defaults.

All 22 exposure-anchor cases pass after the pure engine consumer adopts complete authored decision facts; the positive START control remains alongside the proposed/noncredible WAIT refusal. The weaker `test_full_card_blocks_only_analysis_tokens_with_decisions` renderer-mock duplicate is retired in favor of the three actual `test_current_full_publication_uses_literal_render_and_excludes_undecided_token` cases, which independently assert literal prose, stored selected-token blocks, actual delivery and zero market HTTP. Bundle/durability tests retain renderer isolation for their physical write/queue purpose, use the typed card input and exercise the actual publication gate instead of overriding it.

The removed raw-argument spy's separate WHO concern is retained by `test_outside_participant_keeps_current_claim_eligibility_without_a_financial_block`. The actual wire adapter and renderer name an outside participant supported by a current accepted integration claim, keep exactly one financial block, and exclude that participant when the same supporting claim is rejected. Both positive/negative cases pass within the six-case claim/WHO family. This covers participant eligibility without retaining the old internal dictionary argument shape.
