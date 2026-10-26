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

The two actual native condition replay stages (APPROVED/PROPOSED) pass with the original timeout, real durable reconciliation, callback identity substitution and actual watch predicate execution. A callback failure diagnostic now reports its refused tool receipt before a secondary waiter timeout. The producer audit found the gate codec wrongly demanded a duplicate assessment contradiction list: set_assessment does not emit that field. The codec now requires only the authoritative queried event contradiction snapshot; the temporary synthetic assessment field is removed. The prior card context retains its authored empty event contradiction snapshot. Frozen acceptance packets and production rows are unchanged.

All 22 exposure-anchor cases pass after the pure engine consumer adopts complete authored decision facts; the positive START control remains alongside the proposed/noncredible WAIT refusal. The weaker `test_full_card_blocks_only_analysis_tokens_with_decisions` renderer-mock duplicate is retired in favor of the three actual `test_current_full_publication_uses_literal_render_and_excludes_undecided_token` cases, which independently assert literal prose, stored selected-token blocks, actual delivery and zero market HTTP. Bundle/durability tests retain renderer isolation for their physical write/queue purpose, use the typed card input and exercise the actual publication gate instead of overriding it.

The removed raw-argument spy's separate WHO concern is retained by `test_outside_participant_keeps_current_claim_eligibility_without_a_financial_block`. The actual wire adapter and renderer name an outside participant supported by a current accepted integration claim, keep exactly one financial block, and exclude that participant when the same supporting claim is rejected. Both positive/negative cases pass within the six-case claim/WHO family. This covers participant eligibility without retaining the old internal dictionary argument shape.

`test_expired_tracking_status_preserves_the_frozen_financial_calculation` now exercises the actual EventTool application with a newer available MTA and a historical closed-call identity. It proves the emitted status copy preserves the original calculation, semantic cutoff, calculated_at, generation, EMS/MTA/threshold identity and outlook reference, records the current writer separately, drops current watches, and leaves the stored frozen decision unchanged. The actual case passes; current calculation is not rerun.

The actual structural-assessment→agent-PARTIAL producer now passes with `PendingCardAnalysis`, without a beneficiary map or financial subjects. Five isolated structural storage/durability fixtures explicitly declare their authored criterion instead of relying on renderer omission. The broad current EventTools unit diagnostic is 102 passing / 7 failing: three call/lifecycle cases await the agreed narrow read context; the others still contain shorthand full-analysis/map/current-record fixtures. This is a diagnostic inventory, not a qualified release, and no source validation is relaxed.

### Transferred decision persistence additions

The approved additions in `decision_contracts`/`decision_codec` are now implemented without changing the pure engine or its 61 transferred behavior cases. `DecisionCallContext`/`decision_call_from_record` require only actual call/event/token identity, OPEN status, the actual opening Timestamp and exact positive invalidation price. The storage projection reads those named fields; it does not fabricate delivery, entry or alert-origin facts. An absent historical precision remains unknown with the shared Timestamp reason; a missing opening instant or invalidation price refuses. The full WatchCall producer/scheduler contract remains separate.

`OutlookConditionRefs` derives ordered upgrade/downgrade IDs from validated reconciled watches; INVALIDATION watches remain in the full decision condition list and are not mislabeled as either outlook category. `ReconciledOutlook` names the actual row identity, semantic cutoff, versions, generation, pure calculation and references. `encode_outlook` validates the flat persistence wire and refuses ended tracking, preserving its historical outlook row. `encode_decision` includes the same reference projection; `decode_decision` rejects disagreement with actual reconciled conditions. `_pure_outlook` explicitly projects calculation fields, avoiding an extra-field dictionary facade.

Retained wire/absence sites are exact: `decision_call_from_record` consumes a storage Mapping; `_OutlookRefsWire` converts JSON arrays to the domain's fixed category tuples; `_OutlookRowWire.decode_sparse_scenario_reasons` reads the three historically sparse nullable scenario-reason fields. A missing scenario with no captured reason is classified EXPLANATION_NOT_CAPTURED; it is never assigned an invented cause. `encode_outlook` preserves that sparse wire convention. The nullable cutoff bucket anchor represents a timestamp without such an anchor. All other structured values use named strict models.

Thirty-one existing/new decision projection and watch cases pass, including all 16 original contract cases. Independent strict mypy passes both transferred source files, the pure watch source and both narrow owned tests; Ruff passes. Eight new persistence cases prove minimal historical call reads, required-fact refusals, exact decimal prices, reconciled reference equality, mismatch refusal, ended-row protection and sparse available-scenario reason serialization. Full held application qualification remains open.

### Selected financial participant scope invariant

The separately approved `CardBeneficiary.market_identity` aggregate check refuses a selected financial subject with an actual outside_scope classification. Existing `other_subjects`/WHO support remains intact. The actual adapter positive test names an accepted outside participant without adding a market block; the negative adapter test removes that selected token's registered-label membership and verifies financial selection refuses. Rejected supporting claims still remove the outside WHO participant. Eighty-five current claim/render/template cases pass with zero skips. This is a boundary validation finding; no live mis-selection is asserted.

The application now consumes `decision_call_from_record` and persists `encode_outlook` directly; no full WatchCall padding or `dict(result["outlook"])` domain round trip remains. The pure reconciler uses the actual narrow context's opening Timestamp. All 109 EventTools unit cases pass. The completeness/diverse-three/pinning case now uses seven actual registered token IDs, complete validated calculation/outlook fixtures, a real material stage advance through the publication gate, and the actual renderer; it retains the original missing-analysis refusal, seven-decision evidence bundle and three-token pinning oracles. The bounded-history case uses the actual computed unavailable-data Truth graph rather than forcing a positive move; both memory and actual PostgreSQL checks pass.

An actual six-case integration selection (PostgreSQL bounded history, both native condition replay stages and three literal full publication/storage/delivery variants) passes with zero skips. The older-trigger fixture now obtains its empty analysis map through the real set_beneficiaries producer with explicit actor-supplied layer reasons. A PARTIAL capture-metadata fixture keeps an explicitly unassessed empty layer collection, with no financial beneficiaries; its computed MTA evidence remains intact. The held broad cohort still requires final constructor companion and full owned behavior/static checks.

### Nullable exposure ordering repair

Publication's three score-ordering sites now use exposure_priority_from_record. The required persisted field is validated as an exact nullable integer in0..10. Known larger scores rank first; a genuinely known zero still outranks unavailable exposure. The unknown rank carries None and never invents0. Existing primary-role selection and the least-repriced-secondary preference stay ahead of exposure ordering. Missing fields, booleans, numeric strings, fractional/out-of-range scores refuse at the storage projection boundary. The keyed record is a wire boundary; the pure priority calculation receives only validated int|None. Nine independent scalar/order cases and strict source/test checks pass. The actual diverse-three publication case exercises least-edge exposure None/0/positive while preserving its seven-decision completeness and three-token pinning controls.

### Registry membership closure, independent of WHO evidence

CardRegistryMembership now carries an immutable actual token-ID set and the canonical SHA of its configured/captured registry snapshot. card_registry_from_record reads required token identity fields from that real configuration wire, rejects missing/duplicate/blank identities, and never derives membership from display labels. The publication caller uses the same registry snapshot for membership and labels. encode_card_registry freezes a stable sorted membership projection and its snapshot fingerprint in the card context/evidence bundle; the hash names canonical configuration content, not original file bytes or collector identity.

CardSubject.registry_membership is explicit REGISTERED/UNREGISTERED_TOKEN/NO_MAPPED_TOKEN. outside_evidence_accepted is a separate current-claim eligibility fact; outside_scope is only a derived presentation property. CardInputs binds each subject's classification to the supplied actual roster, and financial selection requires affirmative membership. An unsupported unregistered row remains unregistered even without outside evidence. Accepted outside participants remain eligible for WHO without market blocks; unsupported outside rows do not acquire that eligibility. Registered-but-noncredible secondary links do not gain credibility merely from an accepted evidence reference.

Nine narrow membership cases pass: accepted/no-evidence unregistered financial refusals despite existing labels; genuine membership despite a missing label; falsely declared registered status rejected against the actual roster; missing/duplicate/blank registry evidence refusals; and independent canonical-hash/sorted-set output. Five independent strict source/helper/test targets pass. The actual claim/template/renderer/decision family passes156 cases; mixed application/card checks previously passed267. Shared authored wire fixtures moved unchanged into card_wire_support instead of importing an entire integration-test module; only relevant API consumers changed. Current-model registry membership comes from actual configured snapshots; the synthetic T calculation fixture explicitly declares its own authored registry, not an inferred label-based namespace. Full cohort qualification remains held.

### Historical scenario explanation closure

DecisionOutlook.scenario_explanations distinguishes available text, an actual unavailable cause, and EXPLANATION_NOT_CAPTURED. The read codec preserves literal missing reasons; encode_decision_draft, active encode_decision and encode_outlook require a real absence explanation for current unavailable scenarios. Ended status copies retain the frozen unknown and original captured causes without rewriting the original decision or outlook. cards._token_block displays “explanation not captured” only for this explicit presentation absence. No NOT_ESTIMABLE cause is fabricated.

Six narrow contract/presentation cases and the literal historical application test pass. The latter now runs against both Memory and PostgreSQL, derives its financial record and full Market Truth through actual producers, and asserts the original stored decision/outlook remain unchanged. Actual PostgreSQL proof: one collected integration passed in 4.12 seconds; 256 mixed application/decision/template/scenario units passed before the fixture expansion. Full held cohort qualification and atomic current-state ordering remain open.

### Atomic decision current-state activation

`analysis.decision_state` owns strict DecisionScope, DecisionStateHead and the named LegacyDecisionSelection read basis. Head revisions describe committed indexed writes; revision 1 does not assert a first-ever historical calculation. The key is canonical JSON of the actual event/token pair. The head stores a complete DecisionStatusWriter and a collection write Timestamp independently of the immutable financial cutoff/calculated_at. Its digest binds the actual persisted document read after writing, rather than PostgreSQL jsonb textual formatting. The adapter compares complete wire content for immutable-ID reuse, preserving numeric JSON semantics while distinguishing booleans and numbers; numeric strings remain strings.

`EventToolService.get_decision` now takes the state scope lock and transaction before reading the head or financial inputs. Watch reconciliation is computed by `_plan_conditions` without effects. `DecisionStateRepository.advance` rechecks the exact expected head before invoking `_persist_condition_writes`, writes the validated immutable outlook/decision, rereads and validates that document, then advances the head in the same transaction. The previous state ID and financial tracking lineage are separate. A stale expected head, older input cutoff/calculation, unrelated ended-copy source or conflicting immutable identity refuses. An ended copy preserves all frozen metadata and never overwrites its historical outlook. A genuinely newer active calculation can follow the ended state even with an equal collection clock. No financial time is bumped to force selection.

`latest_record(ana.decisions)` uses the verified current head. Only a headless historical scope uses `legacy_current`: it pages the seven named identity/write-clock/lineage fields in batches of 1000, then reads the selected document. A sole historical display projection can be selected without inventing unused calculation facts. Multiple rows require actual reported write clocks; equal clocks require a unique complete explicit status lineage. Missing clocks or sibling ambiguity refuse. Selection returns identity plus SOLE_LEGACY_RECORD, REPORTED_WRITE_CLOCK or EXPLICIT_STATUS_LINEAGE, never an invented historical revision. Expiry separately decodes the full frozen financial source before adopting a first indexed status write. Old records remain untouched.

Retained absence sites are exact: `current` returns None only for no head; `legacy_current` returns None only for no matching historical metadata; expected/prior IDs are None only before the first indexed write; `_LegacyProjection` nullable clocks/ended lineage preserve genuinely uncaptured old fields. `advance.legacy_source` is present only for explicit historical-source adoption. Dictionaries in this module are serialized storage documents or keyed legacy metadata/clock indexes. Resource handles remain a precise required store protocol and an explicit asynchronous reconciliation effect.

Eleven independently strict repository cases pass in 2.37 seconds, including concurrent first writers, reversed commits, actual equal-clock lineage, stale cutoff refusal, full-document conflict and rollback. The actual PostgreSQL application case passes in 5.23 seconds: it preserves a literal missing scenario reason and original captured cause, rejects conflicting reuse, selects ended then newer active states at the same write clock, and injects a final head failure to prove watch/outlook/decision rollback with the prior head intact. Three actual publication cases and the offline captured-market-watch case passed; both native condition replay phases pass after their authored financial writer was moved onto the real state adapter. Broad held-cohort qualification remains pending.


### Card repricing computation phase

The actual set_beneficiaries producer writes a beneficiary map before repricing is computed; get_market_truth later writes least_repriced_edge, including explicit None when no eligible candidate exists. card_inputs_from_records must therefore distinguish absence of that computation from a computed negative. CardBeneficiary.least_repriced is required bool|None: None is uncomputed, False is a computed nonselection, and True is the selected actual edge. _token_block renders the badge only for True; unknown does not claim a financial ranking. A supplied ranking must be an actual nonblank edge identity or explicit None. The exact retained mapping-key membership branch represents this real producer phase, not an optional default for required captured data.

The actual PostgreSQL record_event→assessment→beneficiaries→Truth→decision→publication path now proves the map has no ranking field and the rendered card has no invented badge (one integration passed6.13s). Eight narrow phase/type cases exercise uncomputed, explicit no candidate, actual selection/nonselection and invalid ranking values; independently strict source/test checks pass. The previously authored historical event fixtures that omit next_milestone are being repaired with explicit no-milestone facts; record_event already requires and persists this value, so its codec requirement remains correct. Broader held consumer qualification remains open.

### Actual card/decision consumer completion

The broad consumer run exposed two additional producer/reader defects: DeliveryService._sync_calls writes ALERTED, and Scheduler._advance_lifecycle handles ALERTED and emits COOLDOWN, but the closed publication lifecycle type and decision input validator omitted both. Those exact active phases are now admitted; unknown phases still refuse. No terminal or material-change policy was widened. Three independently strict projection/gate cases and the actual captured-delivery/expiry replay prove the correction.

Authored consumer records now state no milestone explicitly, matching record_event's required persisted field. A genuinely unavailable exposure stays None. Beneficiary fixture evidence arrays are explicitly empty where no claim references are authored, and primary-only fixture maps state that secondary dependencies were not assessed; they do not manufacture supporting claims or rank results. The actual set_beneficiaries output remains the maintained PostgreSQL rank-phase oracle.

The HOLD/REDUCE, expiry and completed-review cases now calculate their full Truth from the existing nonflat positive corpus, with a declared UTC-hour clock. Token identity is rebound consistently in instruments and each observation family while BTC remains unchanged. Positive helpers assert SPOT_LED_ACCUMULATION and a real reference venue, preventing an identity mismatch from making an UNKNOWN case accidentally pass. The default corpus is unchanged; explicit event_window_h allows the safety scenario to retain one actual T0 across its four-hour initial and five-hour later measurements. No event timestamp or return is relabeled to force ordering.

Expiry now first calculates while the actual captured call lifetime has thirty seconds remaining, advances the authored clock by one minute, then expires it. The prohibition on arming a new watch beyond its deadline remains intact. The material-update negative uses the real unavailable-data Truth producer instead of overriding a few fields of a positive numerical record. The fatal/EXIT repetition case now creates an actual call through START and delivery, exits it on the authored FATAL, proves repeated safety publication is silent, resolves that contradiction, then calculates a genuine later START inside the cooldown window. Existing alert-version, queue, delivery-count, immutability, retry and restart oracles remain.

Restart and captured-delivery fixtures persist the actual validated outlook referenced by their financial row. The replay packet remains authored temporary input; no retained packet/body/code evidence changed. Its current registry declares no instrument for T, so its current edge explicitly remains nontradable while captured historical delivery/call binding and expiry are exercised. Original three-second native deadlines are unchanged; the test reports underlying refused receipts/schema errors before a secondary timeout can hide them.

Focused repaired groups pass: nine fresh replay cases; six positive delivery/review/restart cases (17.44s); real fatal/EXIT/START publication; and both captured delivery/expiry paths (2.79s). Whole-cohort immutable qualification follows this checkpoint. Whole EventTools request/context typing and full Market Truth/Snapshot/condition graph migration remain open; these active test fixtures do not claim those upstream functions are already strict.


### First active head legacy precondition correction

Parent review found that the first active calculation passed no legacy witness, and the original repository implementation only loaded legacy state when a caller supplied one. That permitted an older first active cutoff to bypass predecessor eligibility. DecisionStateRepository.advance now always resolves the actual legacy selection whenever there is no current head. The optional legacy_source argument is only an additional expected-document witness; omission cannot disable the mandatory read, ambiguity handling or financial eligibility checks. A supplied witness carries the hash of the actual stored document, so unchanged identity cannot conceal changed historical content.

EventToolService.get_decision observes legacy state under the same scoped lock/transaction before reading the calculation inputs and passes the witness for both active and expiry writes. The repository independently reloads it before effects. Unknown/ambiguous equal-clock history refuses; a known newer financial cutoff cannot be replaced by an older incoming cutoff. No historical revision or write clock is invented.

Fifteen strict repository cases plus two actual producer memory cases pass (17/17,3.20s). Two actual PostgreSQL producer cases pass7.40s. The older-input producer case deliberately uses an earlier as-of processing clock, so the real latest-before market query selects the older incoming MTA while a valid newer legacy financial row remains stored. The equal-clock case supplies two independent valid legacy decisions with no status lineage. Both prove no reconciliation invocation and no attempted watch/outlook/decision/head writes, and preserve complete pre-call table snapshots. Repository cases separately prove an omitted witness cannot bypass either predecessor and an unchanged ID cannot bypass a changed full-document hash. The original immutable d7 698-unit/55-integration evidence remains preserved; broader correction qualification follows.


### Long-history steady-state proof after the first-head guard

The first-head correction correctly refused the old long-history fixture because that fixture deliberately stores only native financial columns and does not capture historical write order. Those 1001 historical MTA/decision records retain their missing clocks and provenance. The test now creates the current head through the actual producer before appending the older immutable unindexed financial projections, then exercises the distinct steady-state bounded-selector path. It does not fabricate a revision, digest or historical timestamp.

Both namespaces are still counted at 1003 records (1001 historical rows plus actual current/future MTA or the two actual indexed decisions). The second producer calculation retains the same financial cutoff and advances the real head from revision1 to2. Exact current MTA/decision bundle references, no heavy whole-history scan, and future-cutoff exclusion remain asserted. The separate headless older/ambiguous-history negative cases continue to refuse. Memory passes4.41s and actual PostgreSQL passes9.30s; diagnostic broad36 evidence (703/56 passing with only this fixture failure) remains untouched.


### Canonical native fixture and captured-settings companion

After normal merge of parent1d944947 (f0810e1f; only foundation ledger conflict, both sections preserved), the prompt producer calls NativeSessionResolution.from_wire with explicit ANALYST/live composition. POST responses use created_native_session with actual posted resource bindings; GET returns the same direct complete resource. The intentional unknown-provenance control uses an actual floating skill version, retaining a valid observed model/header while refusing prompt binding and financial writes. A known POST snapshot can already establish provenance, so the obsolete exact GET-count spy was replaced by the meaningful single-creation and actual event/assessment provenance assertions.

Replay abort fixtures now use the actual captured deployment configuration (including required prompt label/effort), direct complete session headers, explicit terminal pagination and canonical parsed sent-event acknowledgments. The acknowledgment body is wrapped in a real HTTP response. Server status changes to running with the actual accepted input. All seven original abort/durability/ownership controls remain, including follower retirement before archival, exactly one owned archive, failed journal finalization, source preservation, lost acceptance and native failure/termination. Two-second replay, 0.3-second timeout and one-second cancellation readiness deadlines remain unchanged.

The three-control immediate-market-access test now resolves/captures its actual settings and binds require_versions before seeding. It uses the existing immutable parsed-config cache; no dynamic settings cache, configuration values or deadline changed. All three actual assessment/trade evidence controls remain.

Fourteen focused units pass4.60s with zero skips (p90 .730s,max1.650s); both actual PostgreSQL prompt cases pass5.13s. Broader merged-native consumer qualification is still required. Original d7 and36 immutable diagnostics remain preserved in .verification.


### Complete authored native replay/reference wire cohort

The merged native contract also applies to authored replay provider/config fixtures beyond the ten unit cases. Their requested prompt labels and reasoning effort are now explicitly synthetic fixture declarations. No real retained config, body, code pin or unavailable captured identity was filled from current configuration. Session-list pagination is explicit; request-derived creation records and identical direct GET resources retain actual metadata bindings. Sent-event replies echo the actual parsed event through the canonical acknowledgment body. The foreign live-session discovery control still advertises a complete foreign resource and must not be reused for replay.

Phased reference fixtures update their same native resource's metadata only from the actual posted phase binding, preserving cutoff/evidence-digest assertions. The previously omitted pure DecisionEngine constructor caller is now parameterless. Missing lookup, future-canary exclusion, immutable checkpoint and failure-after-first-checkpoint controls remain. All21 replay/policy integrations pass23.12s; the durable HTTP error/replay four-case group passes; all8 actual phased/reference cases pass38.70s. Whole merged cohort proof remains the next gate.

### Strict fixed scalar adoption

BestMeasuredRouting.automatic_apply and CapturedProjectionCodeInventory.archived_code_executed are required FalseFlag assertions, never numeric zero. CaptureWaitOutcome.incomplete_windows_excluded uses TrueFlag so numeric1 cannot impersonate its policy assertion. Its retained defaultTrue is a declared output-policy constant: wait results describe the application's exclusion of incomplete venue windows, not a default for uncaptured Go data. CaptureLifecycleCoordinator.wait_capture constructs this Python output on every terminal/bounded-wait path; the marker does not claim complete capture.

SavedKlineDayCounts.version uses canonical OneInt for the integer cache format identifier. This site already had an explicit integer prevalidator; consolidation preserves that behavior and removes the duplicate check. Integral floats and booleans remain invalid identifiers, while no financial quantity/rate numeric policy is changed. All retained/raw evidence bytes are unchanged.

Twenty new wire-boundary cases reject numeric/bool confusion and preserve valid booleans/default policy/version roundtrips. The four source modules and new test pass independent strict mypy; 46 related calibration/code-evidence/cache/capture units pass2.41s. This companion is based on canonical24 and has no held decision/card ancestry.

### Discrete spot sign scalar

CardSpotVenue.sign now uses an integer-only -1/0/1 classification alias with require_integer. This is a discrete classification, not a numeric multiplier or financial coefficient: the actual truth.spot_truth venue producer chooses integer literals1,-1,0 (truth.py708), as does flow_core at458. Known zero remains a real neutral class with an unavailable volume share when that ratio is undefined; no share is filled. Valid financial Decimal volume_share values and serialization remain unchanged. Booleans and integral floats cannot impersonate this integer class.

Twelve new sign wire/roundtrip cases and the existing presentation/registry/ranking cohort pass89/0skips in3.15s; independent strict source/test typing passes. The clean scalar4 companion729d953f is normally joined atc6faa7f2; its only append conflict preserved both analysis ledger sections.



### Parent f7b6 caller closure and revenue absence

This bounded companion starts directly at f7b6f3a7; it does not include the held condition graph. The condition producer checkpoint c81c93d5 and input WIP stash 6507c941 remain preserved.

- `card_contracts.CardMarket.holders_revenue` distinguishes an explicitly unprojected view (`None`), observed provider figures (`CardRevenue`), and a captured unavailable descriptor (`UnavailableCardRevenue`). `card_codec._market` accepts only explicit null for the unprojected phase; an empty protocol list is not a phase heuristic. The authored shared literal fixture now declares null; its exact PRD card oracle is unchanged.
- `card_codec.holders_revenue_from_record` no longer drops every all-unavailable result. It preserves unavailable protocol identities and their captured diagnostics, without inventing a value, provider period, or observation clock. Known numeric zero remains an observed provider value. The input record is unchanged.
- `card_codec._revenue_absence` and `cards.holders_revenue_text` expose five bounded presentation labels. Unrecognized or absent root explanations receive the safe display classification “incomplete provider data”; this is not a manufactured provider failure cause. Root diagnostics of the wrong scalar type refuse. Raw URL/error strings are never interpolated into the unavailable label. Existing protocol-level fixed-label handling likewise keeps raw diagnostics out of prose.
- `test_holders_revenue.render` uses the real record-to-card adapter with separately authored canonical presentation fixtures. Tests that isolate a holders-revenue projection supply that feature to an explicitly authored unrelated market fixture; actual complete computed Market Truth records still pass through unchanged. The existing exact decimal/zero, parent-child non-summing, stale/future/receipt, provider-period/chart, unknown token-capture and three-selected-token/3500-character assertions remain. The length-refusal control now lowers the actual rendered-card limit by one character rather than relying on an obsolete event-statement prose fallback.
- `test_event_tools.resolved_native_domain_fixture` now uses the canonical complete native-session fixture. Both failures were actual tool receipts reporting missing `mcp_servers`; prompt validation was not weakened. Explicit result assertions retain the actual diagnostic on future failure.
- `test_open_call_breadth.decision_case/generated_condition` use typed input projections and real `DecisionWatch` expressions, then construct complete authored durable watch records at the repository boundary. The actual consecutive-window evaluator and fired-watch decision effects remain exercised. Pure open-call facts retain only the consumed invalidation price, without invented delivery/entry fields.
- `test_open_call_breadth.test_unsupported_revenue_claim_is_refused_before_loser_calculation` retains the original unsupported `REVENUE_CLAIM` input as a strict codec refusal. The declared tool/model capture enum has no such value; the old unrecognized-value fallthrough must not grant grade B. Independent supported DIRECT→B and INDIRECT→C controls remain.
- `test_formula_vectors.test_t8_data_confidence_medium_funding_freshness_blocks_start` supplies the canonical authored decision inputs and applies the actual computed funding-freshness confidence result. The positive START and stale-funding refusal controls are both retained. Token-fatal tests use the parameterless pure engine without policy defaults.

Initial diagnostics are preserved separately: one nonexistent test filename collected no tests; the first broad valid selection passed 320 cases and exposed the literal unprojected-phase mismatch. Neither diagnostic is claimed as a successful final run. Full condition graph/cache activation and the broader remaining dictionary/function migration remain open.


### Condition producer input checkpoint — not activated

The new `condition_inputs.py` and `condition_input_codec.py` are the consumed read boundary for the pending typed spot calculation. They do not yet replace `truth.spot_truth`, its full/selective consumers or the whole condition cache. No global or layer-completion claim follows from these additive contracts.

- `SpotCalculationInputs.actual_scope`, `decode_spot_inputs` and `_identity` bind actual token/instrument/venue/market context. A qualified instrument read can supply fields genuinely missing on a row; a row's contradictory declaration refuses. Unknown instrument scope is never reconstructed from identifier spelling. Explicit known perpetual instruments are excluded from the spot calculation.
- `SpotFlowObservation` requires actual aligned full-minute or positive boundary intervals. Missing quantities, counts and completeness remain required nullable read facts; they never become zero/true. Known side counts and squared quantities cannot contradict known native flow. Financial admission and capture geometry are distinct; this input does not manufacture missing quantitative totals.
- `SpotFlowObservation.visible_at` returns unknown when the actual receipt or assembly instant is missing, and false for a known late receipt/observation. A known receipt can establish collection visibility even when the source observation instant was not captured; it does not create that source instant or its precision. Full calculation policy and receipt qualification remain for the pure consumer implementation.
- `SpotTickerObservation` retains an undated native volume without inventing a source clock; it cannot be selected as a dated estimate merely by existing. `decode_spot_inputs.native_ticker_volume_field` is required and explicitly declares canonical native volume or the known legacy native alias; no automatic `volume` fallback occurs.
- `FlowSourceDescriptor`, `source_descriptor_digest` and `SpotSourceEvidence` distinguish canonical original storage-read projections, effective projections after explicit scope inheritance, actual scope-source projections, and actual parent evidence lineage. These digests are not provider-response byte hashes. Original bytes/unknown fields remain separate and unchanged; no reconstructed HTTP body is asserted.
- Embedded pieces bind their actual parent minute, scope and source-descriptor identity. The evidence port refuses duplicate locator identities, mismatched digests and missing/wrong parents. Both standalone and embedded candidate records remain available for later as-of selection; equal numeric fields do not silently overwrite different source witnesses.
- The source sidecar is a precise composition object owning immutable evidence bytes, outside the mathematical domain graph. It is not an arbitrary JSON graph field, model-indexing facade, transport handle, repository escape or replacement for typed consumed facts.

Fourteen boundary behaviors pass with zero skips in1.80s; three source/test targets pass independent strict mypy, Ruff and formatting. Pure typed spot, output projection/evidence binding, real financial oracles and every changed consumer remain open. Foundation owns the corresponding output scope/hash helper and graph models; the producer activation will merge that exact canonical dependency.

The pending pure producer now has exact native-flow summation and significance predicates in `spot.py`, with typed observed/missing outcomes and an explicit policy factory from actual `Thresholds`. Twenty-four input/math behaviors pass with zero skips in2.40s; five source/test targets pass strict mypy. Independent exact equality/just-below significance and wide financial quantities remain correct under an outer Decimal precision6 without mutating that context. Missing observations/metrics do not become observed zero; mixed token units refuse. This is still not activated: base/windowed/enriched constructors, output evidence binding and actual orchestration/consumer migration remain open. The old application sigma/venue-count defaults are not claimed closed until those callers are migrated.
### Parent 1bb refresh consumer closure

This bounded repair starts directly at parent1bb2fa97 and excludes held condition-graph ancestry. Producer head35e82891 and pure-spot WIP stash3bb7aa20 are preserved separately.

- The first lost-ACK diagnostic recorded a queued AGENT_MESSAGE with last_error=AttributeError and a traceback at Runtime.worker223, before native I/O. Its focused object.__new__(Runtime) fixture omitted the already-real SessionService required by admission. The fixture now binds that exact real service before starting the worker. No Runtime fallback, optional service or fabricated admission result was added. The constructor bypass remains explicit test-composition debt for the broader typed-port audit; this bounded repair does not claim full Runtime composition.
- The actual CONDITION_FIRED native input response uses acknowledge_native_input to echo the real posted event shape. No id-only acknowledgment is treated as a complete provider event. The existing lost custom-tool ACK, same-byte retry, once-only ledger ordinal, stable financial/watch/job identities and immutable original MTA assertions remain.
- Concurrent-refresh observation now wraps the actual decision_inputs_from_records boundary with its precise Mapping/WatchCondition/DecisionInputs signature. The pure engine no longer consumes raw conditions kwargs or condition identities, so the retired spy observed an empty list even though a decision was persisted. The test still observes the exact first and deferred second IDs at the consumed-record boundary and now binds each durable consumed_by to its own actual decision ID, preserving the first consumption after the second decision.
- Both event-only risk fixtures are complete authored WatchCondition records using the real EventToolService provenance result, explicit EVENT predicates with single-observation persistence, authored identity/text and actual collection-clock companions. No MTA, price, delivery or decision facts were manufactured. EXIT/REDUCE still reuse the actual prior MTA, refuse any market-refresh call and queue no jobs.
- Existing20-second refresh deadlines, cancellation/lost-ACK/idempotence effects, no-wait risk behavior and numerical START/benchmark/freshness controls are unchanged. The original failing diagnostic remains .verification/refresh-callers-diagnostic.log; the first complete eight-case actual run passed in10.34s with zero skips. Final pinned proof follows on the committed candidate.

Parent review correction: the event-risk expires_at fixture retains the exact same future deadline but now uses chosen_boundary(..., precision="EXACT"). It is an authored schedule boundary, not an actually observed collection clock. created_at/fired_at remain actual collection observations. The original8-case actual proof is retained; the two affected risk cases are requalified on the child candidate.

The same review correction is applied to test_open_call_breadth.generated_condition: its explicit start+10h expiry remains byte-identical as an instant but is marked as a chosen EXACT deadline, not a future observed collection clock. Native creation metadata and every numeric/condition-firing oracle are unchanged.

### Local spot parent lineage and generated-empty visibility

The exact transferred producer WIP is preserved at be15732e, then normally joined with foundation's local canonical dece3a46 at4b15c5cd. This tranche does not include the inactive publication-history selector or modify the active parent merge.

MarketService's full and condition snapshots now retain cutoff-partial stored bars in the explicit `flow_parent_bars` read projection. The pre-existing completed `flow_bars` and flattened `flow_segments` views remain unchanged. Dependency fingerprints include the actual partial-parent records. The typed decoder derives pieces from their actual stored parents, binds their token/instrument/venue/market/interval and preserves original/effective/scope/parent descriptor digests. The descriptor format is now explicitly SPOT_INPUT_READ_V1; these canonical input records are never called original provider bytes or a physical flow_segments table.

SpotFlowParent is a minimal consumed parent projection, not a full current-writer admission model. Its last-trade receipt is not a document publication clock. A future actual parent receipt/observation restricts child eligibility; it cannot authorize a child whose own receipt and supported generation remain unknown. Active source receipts stay unchanged. A complete actual EmptyBoundarySegment writer projection may instead prove GENERATED_EMPTY_PIECE visibility through its required COLLECTION_ONLY processing observation; SOURCE_RECEIPT remains a separate basis. `_generated_empty_piece` returns explicit absence of that witness on schema refusal, leaving financial facts known where actually captured but visibility unavailable. Zeros, a legacy clock alone, missing writer fields or an inherited receipt cannot manufacture this witness.

Requested geometry is selected before native financial fields are parsed: unrequested parent totals and future pieces cannot affect the result. Actual declared benchmark instruments are projected separately from the subject; contradictory benchmark source identity refuses rather than becoming target data. Actual MarketService snapshot→decoder→pure spot coverage is exercised with a real FlowAccumulator empty-piece output, explicit configured thresholds, no external HTTP and unchanged stored parent. No fake receipt, provider occurrence precision, side count or financial amount is added.

The combined local input/math/output-contract/output-codec/MarketService unit cohort passes201cases in5.50s, zero skips, p90 .017s, maximum2.719s. The strengthened legacy zero-plus-processing-clock refusal case passes separately in1.44s. Five precise input/math source/test targets pass independent strict mypy; touched targets pass Ruff/format. Evidence is external under local evidence/analysis/spot-parent-* to preserve worktree identity. The first service diagnostic exposed the genuine benchmark-scope mismatch and is retained. Truth's full/selective producer/caller and operational cache activation remain open; this tranche does not annotate or delete `_condition_cache`.

### Typed five-window spot calculation checkpoint

The pure spot producer now composes all five current/prior windows into ConditionSpotWindowGraph. SpotWindowCalculationInputs shares one validated immutable source read and carries small actual current/prior ConditionIntervals, rather than duplicating/revalidating the complete observation graph ten times. Required captured SpotCalculationPolicy remains the only sigma/venue-count source. Primary EVENT selection requires its actual declared anchor in the written15m–24h range; without one, the declared W4H analysis interval remains distinct from a captured event anchor. No event clock is fabricated.

Each complete venue preserves the actual endpoint metadata from every required selected instrument. Full-minute end metadata is the explicit one-minute offset of its captured minute-start metadata; boundary ends retain their own captured Timestamp. No formatted datetime, spacing or window name manufactures BUCKET/EXACT precision. Different endpoint metadata is represented as explicitly unknown COVERAGE_ENDPOINT_METADATA_DIFFERS; no complete venues carry no observed endpoint. Receipts and GENERATED_EMPTY_PIECE clocks remain independent availability facts.

Common cohorts use actual numeric native-majority and configured venue requirements. Prior unknown coverage yields INCOMPLETE velocity; a proven zero baseline yields ZERO_BASELINE, not a fabricated velocity0. Measured current native flow survives unavailable history. Current zero volume remains known neutral with no arbitrary top/cohort majority. Explicit selected event-anchor uncertainty reaches the scoped serializer unchanged. An actual bound-parent piece is selected over an equal flattened input projection while preserving both distinct source hashes and original records; ambiguous equal authority still refuses, and conflicting eligible financial projections are not silently overwritten.

The combined local scoped-window/input/basic-math/output-contract/output-codec/MarketService cohort passes207cases in5.89s, zero skips, p90 .025s, maximum2.721s. The separately added equal-parent-versus-flat identity regression passes1case in1.51s. Six precise source/test targets pass strict typing. The initial positive48h oracle passed but exposed repeated deep validation cost; shared-source composition reduced that individual case to .606s without quotas or changed data. The first expanded no-anchor test diagnostic caught an authored helper accidentally passing its fixed positive anchor; the helper now passes the actual supplied anchor and the negative oracle remains intact. Evidence is external local evidence/analysis/spot-windows-* and spot-parent-authority-*. Truth's real callers and full/selective operational cache activation remain open; no opaque cache annotation or active parent source change is included.
### Local managed replay paging fixture closure

The server-development freeze preserved the exact three-file fixture patch, imported locally with SHA256 `e0466d994e40fbe014cd50e7041bd44132a1428279ac74ed174dc4a4023938c4`. Local commit `3c0812a3` changes only fourteen lines in each direction across test_replay, test_fixture_reference and test_replay_abort_finalization. No retained input, original response bytes, evidence hash, financial assertion or timeout was changed.

The authored transport is the managed paging family: its actual session-events response requires `next_page`, with explicit null for a complete terminal page. Synthetic GET responses now declare that captured-in-the-fixture protocol fact. Its stream producer emits flat event JSON without numeric SSE cursors. Private local sequence counters remain where they allocate fixture event or tool identities, but are no longer mislabeled as reported transport cursors. A Node sequence is not inferred from a list index or a synthetic SSE header. Flat event bodies, ordering, tool identities and accepted-input receipts remain unchanged.

The isolated normal merge `3a8829d2` joins the actual exhausted-retry product correction `902f4792`. Neither historical partial user-message body was padded: a known exhausted attempt does not consume that body, whereas a ready attempt still validates the actual NativeUserMessage. The combined local affected gate covers the three fixture modules, retry-phase/state contracts and the accepted-completion/native-failure race: 172 passed in 2.69 seconds, zero skips, p90 0.021 seconds, maximum 0.315 seconds, using Python 3.13.13 and pytest-xdist `-n auto`. Ruff and format checks pass for all three edited fixture files. Exact local evidence is `.verification/replay-pages-local-unit.log` and `.xml`; the 25 actual replay/reference integration cases remain pending local service availability and are not claimed by this unit proof.

### Read-only equal-clock publication-history finding

An actual local MemoryRepository probe stores immutable authored revisions 9 and 10 at the identical generated_at value `2026-10-26T10:00:00Z`, using the unchanged real key pattern `alert-update:9` / `alert-update:10` and the equivalent preliminary pair. `list_latest(..., generated_at, limit=1)` selects revision 9; the publication consumer would allocate 10 again, although that immutable identity exists. The per-ordinary-kind top-one query also discards revision 10 before the consumer's numeric maximum. Both original documents remain hash-identical. This is a publication-history selection defect, not a generic repository sorting defect: the repository correctly implements its documented timestamp-descending, stable-key-descending order. A lexical storage key does not promise numeric revision ordering.

Evidence is `.verification/alert-equal-clock-selector.json` and the standalone actual-repository reproduction `.verification/reproduce_alert_equal_clock.py`. No active source or parent-merge file changed. The initial probe-script diagnostic used an asynchronous generator inside `all()` and was corrected to an explicit awaited read loop before the successful evidence run; it was not a product failure. The proposed later boundary is a narrow typed revision reference over the actual complete event history, numeric revision selection before exact immutable-row loading, and explicit refusal of conflicting scope/chain identities. Generic repository ordering and captured clocks must remain unchanged. The preliminary specification's P-02–P-05 gaps remain open independently.

### Actual Truth spot-window activation checkpoint (held)

The producer branch normally joins foundation12ff6e4b atc90db1c1. Truth's actual five-window path now decodes qualified source/read projections, calculates the typed SpotWindowCalculationInputs graph and serializes through the scoped codec. It requires the two captured spot policy facts through a strict numeric configuration projection; Decimal conversion preserves the configured lexical numeric value and discrete venue counts cannot coerce float/bool. Missing policy is refused rather than using sigma/venue defaults. The actual snapshot supplies cutoff/anchor/assembly metadata and the distinct parent-bar read candidates. Other old spot_truth callers and downstream derivative/liquidity/enrichment dictionary consumers remain open; this checkpoint does not close the result cache or satisfy the global typing requirement.

The synthetic START fixture explicitly authors its spot policy, receipt/assembly collection clocks and source-row venue/market scope. Removing the registry therefore tests actual surviving source identity instead of ID-string inference. Its loser-service test now composes real Settings rather than an object without captured policy. The first wire consumer's CVD text formatter explicitly admits Decimal text before numeric formatting. Original exact-value, threshold-isolation, confidence, missing-registry, loser orientation, sparse benchmark and zero-variance controls remain. No retained payload is altered.

Focused actual Truth/window tests pass16 in24.69s, zero skips; this expensive selected subset has p90 3.092s/max4.621s and is not claimed to meet the full unit p90 gate. The precise input/pure producer/window selection passes56 in6.65s, zero skips, including required/coercion/nonfinite policy refusals. Six typed source/test targets pass strict mypy; Truth's remaining graph is not claimed converted. Evidence is external analysis/typed-spot-truth-scopes and typed-spot-policy-contracts logs/XML. Earlier policy/formatter/import diagnostics are retained. Broader actual caller migration, typed enrichment/family adapters and the operational result cache remain required before integration readiness.

### Spot native measurements, selective payload and explicit policy consumers (held)

The full Truth computation keeps its typed spot graph through measurement collection. SpotNativeMeasurement supplies declared Decimal quantities, units, windows, actual coverage endpoint Timestamp and venue directly from validated graph attributes; the existing metadata walker no longer needs numeric wire strings to recover those facts. Native large-plus-tiny financial equality and original measurement/precision/coverage assertions remain. Missing instrument-registry input does not erase scope carried by actual source rows; no identifier-string inference is added. Required source receipt/assembly and minute-bucket evidence are explicitly authored only in synthetic positive fixtures.

Selective spot evaluation now caches SpotConditionEvaluation containing BasicConditionSpot and precise immutable SpotSourceEvidence, then invokes the wire serializer after lookup. Its actual pure-producer reuse test proves a typed payload, identical reused financial output and invalidation after a corrected source row. The shared FIFO budget remains128 across existing families; no generic cache annotation is added. State dependencies now include the distinct actual parent-bar projection. Typed native graph payloads for the remaining families, named key activation and assembly-context cache qualification remain open; the existing old key lacks that assembly coordinate, so this is not an integration-ready cache claim.

Direct authored full/selective callers now explicitly pass fixture_truth_config, which captures the actual Thresholds file and returns an isolated copy. Actual service fixtures use real Settings in place of objects without a captured policy source. The broad affected unit selection passes377 in60.15s, zero skips, p90.855/max6.510; selected32 selective/context cases pass29.46s, zero skips, subsetp90 1.733/max3.002 (not a whole-unit p90 claim). Precise input/pure windows pass56 in6.65s, p90.011/max.890. Seven precise source/test targets pass strict typing; touched source Ruff/format and diff checks pass. Evidence is external analysis/typed-spot-source-clocks, typed-spot-selective-native and typed-spot-policy-contracts. Earlier diagnosed failures remain retained. Actual PG/retained caller qualification and complete typed condition-family/cache activation are still required.

Actual caller follow-up at immutable d2685374: all42 PostgreSQL/retained cases across Truth, condition storage, MarketService, EventTools, fixture reference and market-refresh pass in59.57s, zero skips. Wrapper exits0/source guard unchanged; evidence is external analysis/typed-spot-actual-callers.{log,xml}. No captured input is edited, source clock repaired or old receipt absence filled. Named assembly-aware cache keys and complete typed graph-family/cache activation remain open before final migration readiness.

### Actual assembly-aware condition cache identity

Foundation's required assembly-key contract9160175a normally joins the producer branch. Actual top and state-node cache paths now use ConditionTopCacheKey and ConditionStateNodeKey, preserving subject/window/duration/bounds/dependency/config/direction identity and including the original assembled_at Timestamp. Selected subgraphs retain all actual assembly companions; no cutoff/now substitutes for an uncaptured assembly. The externally returned request fingerprint includes the same assembly evidence. The shared FIFO insertion/eviction order and total128 budget are unchanged, and remaining non-spot payload families are still an open typed migration.

An independent actual computation regression first reproduced the old stale cache: one unchanged opaque source head/raw flow bundle/cutoff/policy evaluated before receipt remained at zero complete venues even after a later assembly reached that receipt. This was a consumer identity defect, not a source-head mutation or generic repository issue. The corrected test runs both selective spot and composite state paths; later assembly recomputes to two complete venues, cache keys retain both actual assembly contexts and the request fingerprint changes. Existing unchanged-input reuse and corrected-source invalidation assertions remain. All34 selective/context cases pass in30.70s with zero skips; red/green evidence is external analysis/typed-spot-assembly-before-corrected and typed-spot-assembly-both logs/XML. An earlier queued local test command with a syntax issue was stopped before creating test children, repaired and rerun; it is not counted as proof. Actual consumer requalification follows this commit; the whole family/cache migration remains open.
### Inactive numeric publication history contract

The separate local history branch adds only PublicationRevisionRef, PublicationHistory and their exact record projection. A revision requires the actual event/alert identity, a strictly positive integer revision and a declared card kind; no timestamp, financial measurement or terminal-correction fact is required by this selector. Empty history remains explicit absence. Mixed event/alert chains, duplicate revisions, missing identity facts, unknown kinds, blank identities and coerced boolean/float/string revisions refuse. Selection uses the maximum actual numeric revision for current/all and ordinary history and the minimum FULL revision for its original; sparse retained revisions are not filled or rewritten.

Both insertion orders of actual immutable MemoryRepository revisions9/10 at an equal clock reproduce the generic lexical top-one result9 while the pure selector chooses10 from the complete actual metadata. Original document hashes remain unchanged. Eighteen local cases pass in1.55s, zero skips; independent strict source/test typing, Ruff and format pass. This contract is inactive: no publisher, delivery or archive import has changed. Actual producer/PG version11, no-overwrite, ordinary/preliminary/thread qualification waits the coherent current-main join and local services. P-02–P-05 remain explicitly open.

### Publication history consumer checkpoint after local main39

The inactive typed selector28d305c0 normally joins current main39d74add at1983e87a, separately from producer50cc70b5. A narrow read-only PublicationHistoryReader/Repository projects all actual event revision metadata, validates one chain and loads the exact selected immutable row with a second scoped identity check. EventTools now uses numeric newest/all and newest/ordinary revisions for publication and the minimum actual FULL revision for managed-token pinning; the unused generic first_alert API is retired. Generic repository timestamp/key ordering is unchanged.

The affected local unit gate passes23cases in4.55s, zero skips, including actual Memory publisher creation through revisions9/10/11 at one unchanged generated clock, preserved original row hashes, full promotion or denial correction, actual SENT receipts and the existing Telegram thread. Real set_assessment produces the initial MATERIAL and later MAJOR records, and actual MarketTruth/Decision writes support FULL; no financial records or missing assessment versions are padded. The ordinary/preliminary separation and known P-02–P-05 gaps remain unchanged. Three precise history/reader/pure-test targets pass strict typing; the larger existing EventTools/producer test family is not claimed converted by that narrow gate.

Actual PostgreSQL qualification remains OPEN at this checkpoint. The first diagnostic exposed the old minimal source helper's missing physical content_hash; the next exposed the synthetic SourceItem fixture's uncaptured publication precision versus the physical required field. The owned new helper now authors a complete synthetic source with an actual raw fixture file/hash and its explicitly declared publication clock/precision, before any publication. No retained input or runtime admission default is changed. The original diagnostics are preserved in external evidence/analysis/publication-history-producer-actual*. Root's new main57 merge temporarily takes priority before rerunning those actual cases; this checkpoint is not an integration-ready claim.

Actual PostgreSQL version11 follow-up: complete authored source records now include actual local raw files/digests and their explicitly declared synthetic publication clock/precision. Tool invocations declare the actual synthetic model/prompt fixture context, rather than leaving required current-writer attribution absent. Initial MATERIAL and promoted MAJOR assessments are produced by the real tool; no retained row or runtime default is changed. The source-only diagnostic then passed both FULL11 and preliminary-DENIAL11 actual PG cases in7.79s with zero skips. Final affected Memory/history/EventTools/Delivery/card unit selection passes167cases in50.59s, zero skips. These results are pinned to the39-based candidate; current57/no-outlook-schema requalification follows the normal main join and is not inferred from the earlier run.

### Current no-outlook history and physical store qualification

Normal main57 join fd07c832 retains the numeric history consumer separately from the condition producer. The actual PostgreSQL FULL11 and preliminary-DENIAL11 cases both pass in7.78s, zero skips, against the current schema without ana.outlooks. Canonical embedded decision outlooks, original revisions9/10, equal generated clocks, exact receipt/thread and version11 effects remain asserted. Evidence is external `analysis/publication-history-fd07-actual.{log,xml}`; the wrapper exits0 with unchanged source.

The Linux physical Go store race package at the same immutable head passes40 tests/subtests, zero failures/skips, package elapsed8.193s. Evidence is external `analysis/go-store-fd07-race-mkt-ready.{jsonl,stderr}`. This includes actual PostgreSQL/ClickHouse latest-value cases and the strict narrowing HTTP controls. Earlier physical diagnostics were harness preparation defects: first missing schemas, then a preparation client targeting default and rewriting mkt DDL. The tooling owner corrected preparation to canonical mkt and verified that database before this successful run; no product fallback or query broadening was introduced. This selected package proof does not claim the complete Go suite or global condition cache migration. P-02–P-05 remain open.

### Required follow-up refusal caller companion

The isolated local caller branch normally joins canonical agents4419c817. CapturedExternalSessionService receives RepositoryFollowupRefusals of its actual replay repository, and all fourteen owned SessionService test callers receive the same real repository-backed adapter. No optional constructor default, silent sink, fabricated closure or unknown historical fact is introduced. Existing replay capture interception, cancellation/finalization, lost acknowledgment, refresh ordering and actual publication effects remain exercised.

The affected three-file unit selection passes126 cases in56.36s with zero skips, p90.969s and max6.142s. The actual four-file replay/reference/market-refresh/EventTools selection passes54 cases in51.73s with zero skips. Both pinned native wrapper runs exit0 and preserve source identity. External evidence is `analysis/followup-callers-{unit,actual}.{log,xml}`. Ruff, format and diff checks pass for all four modified source/test files. This is a constructor consumer companion; it does not claim the still-unconverted replay/EventTools graphs are strictly typed or close the condition-result cache migration.

### Isolated pure pairwise spot cohort checkpoint

A separate managed branch from6a40af71 normally joins foundation8d2feea8 while the actual visibility caller worktree stays immutable. combine_native_flow and compose_native_flow share exact native arithmetic with the existing basic/windowed producer; no second significance algorithm or policy default is introduced. compare_spot_windows uses actual complete-venue intersections, strict native majority and the required captured policy. enrich_spot_cohorts composes the named COHORT_ENRICHED graph; primary size-profile computation remains explicitly NOT_REQUESTED at this phase, not a fabricated unavailable result or completed full enrichment.

Thirty pure producer/window/phase cases pass in9.60s, zero skips; two source/test targets pass strict typing and Ruff/format. New independent controls preserve240/480 native volumes and120/240 CVD for the actual common cohort, distinguish directed strict-half/count failures, retain known zero separately from absent observations, and refuse mixed token scopes or duplicate venue evidence. Exact1E150+1E-40 and1E300+1E-80 aggregates survive outer Decimal precision6 without changing it. Input graph bytes remain unchanged. Evidence is external analysis/spot-pairwise-pure-scope.{log,xml}. Actual Truth enrichment/bucket/capture consumers are not activated by this isolated checkpoint; the140 legacy spot_truth calls remain open until their proper typed consumer/capture projections replace them.
### Beta availability policy aligned with normative design (local main49 companion)

Technical Design §5.7 still prescribed beta 1 with a flag for insufficient history, while the implemented no-fabrication policy already returned a required unavailable regression. The normative rule now explicitly requires 168 paired hourly returns before OLS/Blume estimation; insufficient pairs produce null/INSUFFICIENT_HISTORY, non-estimable regression produces null/NOT_ESTIMABLE, and a genuinely estimated zero remains available. This is an intentional policy correction to the old numeric fallback, not a restoration of it. Raw measured returns and explicitly selected raw benchmarks remain independent. Adjusted returns/history/normal ranges/percentiles/runup and frozen adjusted review direction cannot borrow a missing regression. Historical flagged numeric records remain unchanged and cannot authorize an adjusted conclusion.

No production calculation changed in this companion. Tests cover 1/5/167 pairs versus the existing 168-pair estimable slope oracle, additional unpaired observations, actual compute_market_truth with six days of history retaining the 0.011 raw move while adjusted conclusions remain unavailable, and frozen review of both current null and historical flagged-1 beta. Existing known-zero and raw-benchmark review controls remain intact. All 27 affected unit cases passed in 9.66 s, zero skips; the full collected actual review subset passed 1/1 in 3.79 s with owned PostgreSQL/ClickHouse services, zero skips. Evidence: beta-policy-5b-unit and beta-policy-5b-actual log/XML in the local analysis evidence directory. Ruff check/format and diff validation passed. Frozen fixture and verification payloads were not edited.
### Visibility identity refinement and measured allocation work (held)

The exact-assembly prototype e886a48c is superseded, not accepted as a final reuse design. Actual live PostgreSQL requalification was41pass/1fail because fresh processing instants defeated the intentional repeated-live reuse oracle. The temporary same-clock fixture probe passed42 but narrowed that contract and is diagnostic only; the fixture was fully restored. Foundation32e4ba9f now separates content/visibility cache identity from fresh assembled_at proof/root context. The finite current receipt/observation/reference/independent-confirmation gates have an explicit source-clock projector, with no recursive clock-name guessing. Cached source proof extensions retain their original source clocks. Compact financial series identity includes actual values/times/completeness/scope/cadence and excludes previous-head/append/change-index optimization hints; actual read content remains separate from a provider's opaque head.

The first coherent caller probe completed43 cases but hit the unchanged10s deadline in eleven fixture/case results. Its full-MTA baseline runs before the visibility projector, so a signature-cost attribution was not established. The independent shared-lock snapshot-build/fullTruth cProfile at6a40af71 measured11.28s with profiler overhead, including5.17s in typed input decoding and11,523 `_wire` operations for2,880 bars. This is diagnostic, not a passed unit deadline. Memory-pressure readout showed46% free; no infrastructure limit or deadline was changed.

The read adapter now bounds geometry before canonicalizing selected financial records, reuses identical canonical original/effective bytes, and creates parent clock context only for actual embedded boundary pieces. Requested scalar/clock admission, source hashes and parent binding remain. Immutable pre-optimization representative hash vectors were recorded from the still-preserved decoder (SHA256 ce45a7bafe61ace35fff54023252e80bc86a1c9ef0a47cef0714e2a98f213582) in the separate managed worktree; fixed minute/qualified-minute/embedded-parent+child hashes are asserted directly. This is current repository code qualification, not execution of captured archived code evidence. Repeated identical clock facts are grouped with every exact source path preserved, avoiding thousands of duplicate clock models without dropping unknown/fault states. Behavioral/timing and original live-reuse qualification follow; this checkpoint is still held.

### Restored actual live reuse and final caller delta

At immutable4aef9f96 the original unmodified live PostgreSQL reuse/SQL/no-write selection passes42/42 in55.19s, zero skips/source guard exit0. The repeated-live selective case took17.725s versus18.524s in the earlier d268 selection; these are end-to-end observations, not a controlled hash microbenchmark. The combined producer/input/visibility/condition selection passes114 in28.49s, zero skips, max2.547s/subsetp90 1.341s. The same-source-head later-eligible receipt, corrected financial content under an unchanged opaque head and unchanged-input reuse controls all remain.

The full direct-caller unit selection completed543pass/1fail in56.64s. Its sole failure was an obsolete query spy: native metadata projection0f12a635 no longer delegates to iter_all, so the spy omitted Ops' item_id-only event-observation query. Production EventTools/ops_progress/storage match published091; the test now counts both APIs and requires one full read plus exactly one item_id-only projection, preserving the original combined two-read budget. Its focused case passes in2.78s; financial publication/dedup assertions are unchanged. Evidence is external analysis/typed-spot-full-callers-final and publish-read-footprint-projection logs/XML.

The second independent shared-lock fullTruth profile at4aef measured4.47s versus11.28s before, with profiler overhead; source decoder cumulative time was1.49s versus5.17s. Structural `_wire` calls fell11,523→2,883 for the same2,880-bar fixture and the fixed original/effective/scope/parent hash vectors pass. Wall-time comparison is diagnostic and can vary with load; no timeout, worker count, resource cap or dataset size changed. The140 remaining legacy spot_truth calls are not concealed by this result. Whole-family typed cache/pure-fact versus fresh-context completion remains open, as do historical FX/unit policy and remaining function/consumer migration.

### First actual typed spot/cache input cohort qualified on main49

Canonical production head 61316bef normally joins main49 5b77f741 and foundation scoped spot/OI contracts 5353d80f; no loose source copies. The newly introduced untyped condition_input_fingerprints function is retired in favor of independently strict condition_fingerprints.py. Its hash-input boundary explicitly represents the actual compact int/float arrays and bytearray buffers alongside existing wire values, projects only consumed compact-series facts, and preserves the prior exact buffer spelling and original source-clock identities. Three literal digest coordinates established from the prior trusted owned implementation remain identical. Optimization hints do not become financial facts; corrected compact values change content even under the same opaque provider head. Fresh processing assembly remains outside pure keys, while explicit eligible-source visibility and content identities preserve unchanged live reuse and detect changed receipt eligibility.

Qualification at immutable 61316bef: 82 narrow input/codec/visibility/spot cases pass 4.78 s; all 42 collected affected actual cases pass 46.48 s with owned services, including the unchanged repeated-live PostgreSQL reuse, bounded SQL and no-assessment-write oracle. The 23-file actual caller unit selection passes 547/547 in 39.62 s, zero skips. Eleven precise source/test targets pass independent strict mypy; Ruff/format checks pass. Evidence: fingerprints-535-unit, fingerprints-535-actual and fingerprints-535-callers-unit log/XML outside the worktree. These are bounded cohort results, not the whole-suite latency or global migration gate.

Only the spot top-family cache payload is currently closed: SpotConditionEvaluation carries BasicConditionSpot and its bound immutable source-evidence lookup. Other top/subfamily OI, funding, reference-close, confidence, depth, full state, relative, derivatives and supply payloads still use their original legacy shapes. Their actual producer/proof/serializer construction is required before the shared 128-entry cache can receive a complete precise union. The outstanding configured-mypy cache annotation is not closed by this cohort, and no dict/JsonValue/object/Any cache annotation or deleted cache is used to conceal it. Scoped spot, actual OI synchronization contributions/tolerance, passive full/selective graphs and codecs are available contracts, not an assertion that their source producers are already activated. Pairwise/sign-history retirement remains a separate follow-up.

### Required Scheduler evaluation clock callers

This independent companion normally joins ingestion8f94be84 and root253f50da. Replay passes its existing trigger Timestamp directly to Scheduler.eval_conditions; market-refresh fixtures pass their existing chosen boundary directly. Open-call and fatal-watch fixtures propagate their existing authored cutoff value with explicit unknown precision retained. No new current-time fallback, precision, financial values or condition rows are synthesized. The new named evaluation receipt does not change these callers' financial/watch/consumption/replay effects.

All 84 affected unit cases pass in 3.99 s, zero skips. All 27 collected affected replay/market-refresh actual cases pass in 18.99 s with owned services, zero skips and source guard success. Original restart, offline, lost-ack, refresh ordering, durable consumption/defer, EXIT/REDUCE and deadline controls remain. Ruff/format and diff checks pass. Evidence: condition-eval-timestamp-unit and condition-eval-timestamp-actual log/XML outside the worktree. Scheduler's remaining raw Truth/result helpers and the complete analysis cache migration stay open; this closes only the required clock call sites, not those domain boundaries.
### Typed native sign histories replace repeated legacy spot reconstruction

This separate cohort normally joins root253f50da and the previously qualified pure pairwise companion. Actual full and composite-selective Truth now compute top-venue quarter-hour signs from their already decoded native flow inputs, preserving the exact requested grid and requiring every selected venue's actual instrument coverage and receipt eligibility. Neutral observed zero remains distinct from an unavailable sign; missing windows cannot establish flips. The unchanged full synthetic START oracle now also proves at most seven remaining legacy spot_truth calls, replacing the previously profiled 140-call path. The remaining calls are explicitly open reference/capture/confidence consumers, not claimed retired by this slice.

Cached source facts are represented by SpotCalculationSource, which has no processing assembled_at field. Each request binds its actual fresh Timestamp through at(...); original receipt/observation clocks remain unchanged. The enrichment seam requires a scoped ConditionTopVenueSignHistory and refuses borrowed subject, interval or selected-venue evidence before mutation. Existing isolated pairwise/price tests explicitly author no selected history; the raw audit derives its actual history through the production codec and pure calculator. Its genuine positive alternating-flow case now declares its synthetic capture batch receipt and assembly separately from financial cutoff, while new missing-receipt and before-receipt controls remain unavailable. No retained capture was padded, and no missing-clock availability fallback was restored.

Diagnostics retained: the first broad run had twelve direct caller TypeErrors from the new required port; the next exposed the unclocked positive audit assumption. The first secondary-worktree actual run had 37 passes and five missing ignored fixture-data failures; all 195 frozen fixture files were then individually SHA-verified against the already qualified local fixture copy, with an external manifest and originals untouched. These were setup failures, not changed financial results. Final immutable source0ed5d89d passes 606/606 affected caller units in39.76s and 42/42 collected actual integrations in41.68s, zero skips/source-guard exit0. Eleven precise helper/model/test targets pass strict mypy; Ruff/format checks pass. Evidence: sign-history-final-callers-unit and sign-history-final-actual log/XML outside the worktree. Full graph/cache union and global migration remain open; pairwise calculation helpers are qualified but their complete size-profile/enrichment producer activation is still a separate step.

### Strict immutable spot snapshots and named state-node cache record

Review of896 correctly found that a plain dataclass SpotCalculationSource projection did not make its public constructor strict. The source now shares a strict Pydantic facts base with request inputs: only the request subtype adds required assembled_at. Public source construction rejects wrong scalar/container types, foreign scope and an extra processing clock. Public native-measurement and visibility-evaluation records are strict too. FlowSourceProjection, individual evidence entries and SpotSourceEvidence are strict immutable bytes/tuple snapshot records, retaining original/effective/scope/lineage checks. No arbitrary type allowance, unchecked construction or deepcopy exception is used. Returned decoded wire views cannot mutate captured evidence, and unknown identities or changed effective bytes refuse.

The actual state spot subnode now stores SpotWindowEvaluation with closed graph/source/evidence/clocks rather than a six-item tuple containing rendered dictionaries. Serialization occurs after lookup; fresh processing assembly remains outside stored facts. The ordinary top-spot payload is a strict record as well. Existing literal source/hash and source-clock oracles remain unchanged. Other family cache payloads still require their own actual typed producer activation; the whole shared cache annotation is deliberately unresolved.

The first strict snapshot run at9aa103c7 had601 passes and8 original10-second timeouts. Its red artifacts are retained. Profiles showed repeated full-source validation inside each quarter-hour history request. The pure SpotCalculator now performs one public admission to an owned, transitively immutable validated request snapshot, then private calculations over it. Pydantic revalidation creates independent nested records; caller-held raw input mutation and invalid copied models are tested. Strict constructor and from_inputs admission remain enforced, with no validation policy weakened. Source data, exact quantities, original timeout, automatic workers and hash/proof checks are unchanged.

Same-case serialized profiles9aa→08bc9709 (profiler overhead, not unit budgets): compact START4.388→3.957s; stale-funding vector7.345→6.562s; unknown OI-edge case6.671→6.142s, with identical results/assertions. The stale-funding profile reduced nested validate_python calls342029→272901. Final immutable08bc9709 passes610/610 affected caller units40.64s, zero skips, max4.856s; selected-subset p901.0222s is reported without claiming the whole-suite budget gate. All42 collected actual cases pass43.16s, zero skips/source guard, preserving original live repeated-read reuse, SQL/no-write, late-eligibility, corrected-content and delivery/replay oracles. Eleven precise source/test targets pass strict mypy; Ruff/format/diff checks pass. Evidence: strict-spot-owned-callers-unit, strict-spot-owned-actual and strict-spot-profile-{before,owned} log/XML/stats outside the worktree. Root's full matrix and complete domain/cache migration remain required.

### Enrichment body locals exposed by the required sign-history annotation

On parent855a0c7a the now-checked legacy enrichment body exposed16 additional mypy errors beyond the unfinished shared cache. This companion closes those actual local shapes without removing the required history type: latest source books are addressed by a distinct venue-to-row-index map, split walks preserve Decimal allocations and nullable measured impacts, quote pairs are narrowed explicitly before arithmetic, and gap/funding history/confirmation collections carry their actual scalar types and distinct names. A zero-allocation missing walk is not silently relabeled a measured zero. Missing conversion margin leaves the comparison unevaluated rather than synthesizing one. Existing raw-book/currency qualification and other enrichment input shapes remain legacy/open; these local annotations do not claim that the whole function or liquidity family is converted.

The affected162 unit cases pass27.01s, zero skips, retaining exact cohort-majority, price/quote/REST, slippage, data-confidence, source-precision and raw financial vectors. All7 collected affected Truth/PostgreSQL actual cases pass17.05s with owned services, zero skips/source guard. Ruff/format checks pass. Configured mypy over178 source modules now again reports only the explicit _condition_cache annotation dependency, not the16 body errors. Evidence: enrichment-855-default-mypy, enrichment-855-locals-unit and enrichment-855-locals-actual log/XML outside the worktree.

Independent diagnosis of the parent515-case replay failure retained the original5-second dispatch deadline and complete integration workload. Business source855 was unchanged; a target-only owned plugin logged phase/type/duration and durable queue status without payloads. Isolated native and Linux runs passed. The complete Linux515-case reproduction passed114.52s, zero skips; refreshed computation2.507s, assessment2.627s and waiting tool2.732s ended with the actual RUN_MARKET_TRUTH job DONE and a persisted result. No failed/deferred/background exception was observed in that reproduction. This does not retrospectively explain the original timeout or prove it cannot recur; it is not attributed to contention without evidence. The diagnostic plugin source/hash is preserved externally and its owned temporary project file was removed after terminal completion. Source/proof: replay-refresh-855-full-actual and replay-refresh-probe-source artifacts. The original parent red evidence remains unchanged.

## Rejected-book admission ordering — local 2026-10-26

The split-book enrichment selector now checks actual integrity eligibility before resolving its venue. The actual OrderBook crossed-book summary omits venue but retains instrument_id, so that supported writer shape already resolved its venue and does not demonstrate a production failure from the prior ordering. The correction preserves the earlier unconsumed rejected-row path: malformed rejected placeholders do not require an unused venue, while an eligible unresolved SPOT row still refuses. No source fields, clocks, financial quantities or historical records were padded.

Four real-enrichment controls passed on Python 3.13.13 with normal 14 automatic workers in 1.63 seconds, zero skips. Ruff/format and the new typed test module's mypy check passed. Evidence: analysis/book-admission-order-3.xml. Earlier diagnostic runs exposed the new authored harness's omitted cutoff; their failures remain preserved separately and did not identify a product defect.

## OI read/selection and actual direct-window spot — local 2026-10-26

The exact preserved OI prototype now admits canonical `mkt.oi_obs` read projections with immutable original/scope bytes and independently bound descriptor hashes. Each unregistered source row must carry its own actual scope; it cannot borrow a preceding row's declaration. Registered scope qualifies absent metadata without rewriting the source document, while conflicting token, venue, market or quote declarations refuse. Native zero, malformed reported quantities, missing USD, missing mark clocks and unresolved bucket edges remain separate facts.

`OpenInterestCalculator` owns validated immutable reads once and selects actual per-instrument source endpoints under the required declared tolerance. It preserves logical bounds separately from source clocks, excludes future/unreceived/unresolved observations, and refuses ambiguous equal-nearest witnesses rather than choosing interchangeable evidence. Exact native endpoint comparisons survive an unrepresentable FLOAT64 display. Public copied invalid results revalidate before comparison. These input/selection APIs are not yet active OI cache production: actual synchronization semantics, baseline populations, qualified historical FX, final graph construction and derivative cache activation remain open.

The actual direct spot cache now carries `DirectWindowConditionSpot` rather than an unscoped Basic result. It uses the same one-window admitted native inputs and actual complete-source coverage endpoint; cohort comparisons and size are explicitly NOT_REQUESTED. Serialization uses fresh request clock metadata and verified retained evidence. No other windows are loaded and no source timestamp precision is inferred from interval arithmetic. The existing calculator reuse spy now observes the actual typed direct entry point with precise arguments; its one-computation, unchanged reuse and corrected-content recomputation assertions remain intact.

Qualification at source head `4923cbb4`: Linux pinned pytest console, normal fourteen automatic workers, **131 units passed in 36.20 s**, zero skips; selected subset p90 1.696 s/max 3.582 s is not a whole-suite performance-budget claim. The actual disposable PostgreSQL live-reuse/SQL-footprint/no-write case passed in 20.49 s, zero skips, with its original live processing clocks and assertions. Independent strict seven-target mypy and Ruff/format nine targets pass. Configured truth mypy still reports the single real `_condition_cache` annotation error: remaining cache families are not hidden behind an opaque type. Evidence is `analysis/oi-direct-window-linux-3.xml` and `analysis/oi-direct-window-linux-actual.xml`.

Earlier evidence remains distinct: the native precollection attempt failed in owned PostgreSQL setup without collecting tests; Linux runs one/two each had 130 passes plus a new authored comparison failure, first from reading per_venue out of an OI-only public projection, then from comparing its explicit decimal wire string directly with Decimal. The final control checks the actual synchronized projection, exact wire spelling and native numeric values. No financial source behavior, retained fixture, dataset, deadline or worker policy changed to satisfy those controls.

## Native OI history population — local 2026-10-26, held producer work

Actual legacy `compute_condition_values` was exercised with 242 authored, scoped observations: ten days of hourly finite native quantities alternating 1 and 1E400, current native endpoints 10 and 12, absent USD, actual declared receipt/assembly clocks and synchronization semantics. It refused with `ValidationError`/`finite_number` while admitting the FLOAT64 historical range. The isolated Linux diagnostic and source guard exited zero after recording that refusal; the script is external `analysis/oi-baseline-representability-repro.py`. This is an adversarial supported scalar case, not a claim about a production provider observation.

The held native-history calculator keeps observed endpoint pairing separate from FLOAT64 admission. It retains every actual valid pair in its count, reports unestimable pairs explicitly, and makes the whole baseline NOT_ESTIMABLE without dropping or clamping them. The shared `exact_oi_endpoint_fraction` normalizes common Decimal scale and refuses unrepresentable relative gaps before huge integer expansion. Thus a shared exponent of one billion can still yield ordinary 1/-0.5 changes, while a genuinely extreme relative gap leaves the current native endpoints intact and the baseline unavailable. Representable populations preserve the existing FLOAT64 percentile arithmetic; exact fractions guard against manufacturing zero from an unrepresentable nonzero result.

True observed zero changes remain a known zero baseline. A sparse observation cannot pair with itself to manufacture a zero window change; missing distinct endpoint pairs remain insufficient history. Summary records require actual source/pair clocks, ordered historical scope, declared lookback/minimum/quantile policy and consistent pair counts; copied partial clocks or an unsupported longer minimum refuse. These are pure history facts with no cached request assembly clock and no claim of complete temporal capture.

Qualification: `analysis/oi-native-history-linux-2.xml`, **56 cases passed in 4.76 s**, zero skips, pinned Linux pytest console/normal fourteen workers/source guard exit zero. Strict source/test checks and Ruff/format pass. Foundation 7003/50ec/6aef prerequisites were joined normally. This does not activate the OI cache: actual synchronized selection, stored/qualified USD history, decomposition, fresh proof wiring and the remaining family migrations are still required. The previously qualified actual direct-spot checkpoint remains `01b3ac08`.

### Recorded OI USD component, not a retained FX claim

`recorded_oi_usd` treats a finite nonnegative normalized `oi_usd` field as RECORDED_USD, including known zero. The retained field may have been provider-reported or previously derived; this category asserts neither universal provider origin nor retained historical FX qualification. Original canonical read evidence and actual source clocks remain bound. Missing values become RECORDED_USD_NOT_CAPTURED, malformed/nonfinite/negative components become REPORTED_USD_INVALID, and native amounts remain independent. `mark_price` never fills this component: newly deriving USD is a separate operation requiring actual qualified mark/quote/rate clocks and receipts. `OIObservation.usd_derivation_label` was removed because its unused extension could otherwise overvalidate a known recorded amount; the exact extension, including nontext forms, remains in original hash-bound evidence instead of becoming an invented collector identity.

`recorded_oi_synchronization` attaches these stored components to the exact native-selected per-instrument sources and original contribution clocks. It never drops an instrument because USD is unavailable and never sums only the available USD subset. Known zero remains zero; any unavailable contribution makes the endpoint USD total unavailable while exact native totals remain available.

Immutable sourcee20ab27f passes101/101 selected Linux console units in17.88seconds, zero skips/sourceguard. This includes the previously held native-only witness/reprojection controls, recorded zero/positive/missing/negative/nonfinite values, arbitrary unused origin metadata, mixed USD availability and exact selected source-clock controls. Strict three-target mypy and Ruff/format pass. Evidence is analysis/oi-recorded-components-unit.xml. This is still an unactivated input/math component: per-instrument recorded-USD history/statistics, like-for-like synchronized baseline populations, actual OI cache producer/fresh-proof integration and remaining families are required. No opaque cache annotation or fabricated FX provenance is introduced.
### Exact recorded-USD OI decomposition authority

A read-only actual legacy _oi_stats reproduction with 240 hourly history rows, native3→4 and recordedUSD1→1 yields REAL_NATIVE despite an exact position share of one half. Rounded implied price1/3 changes the strict native-majority predicate. The pure recorded_oi_decomposition authority uses endpoint-bound sparse polynomial numerators over one positive monomial denominator, rather than pretending inferred USD/native is a captured mark. It imports only standard-library arithmetic, Pydantic and the inward DomainModel. Root native/USD observations, timestamps, FX and collector provenance are not invented by this mathematical basis.

DecimalPowerTerm keeps a canonical integer-valued Decimal coefficient and an arbitrary strict Python exponent. SparseDecimalPolynomial and RecordedDecompositionTerms retain exact position/markup/interaction numerators and their common denominator. Exact sign comparison either proves leading-term dominance or combines only near-magnitude coefficients; it never expands an arbitrary exponent gap. Native1→1E400, native1→1E1000000000 and common exponents above JavaScript's safe integer range retain exact conclusions without a FLOAT64 limit. The output codec must serialize coefficient/exponent text exactly; that consumer remains the foundation owner's separate work.

Both result variants declare RECORDED_USD_IMPLIED_PRICE and retain all four endpoints. Zero native endpoints explicitly make this implied-price basis undefined, without requiring an unused mark. Defined zero absolute terms have neither a share nor a majority; known zero is not substituted. Finite ordinary share display comes from exact rational terms. Positive FLOAT64 underflow is unavailable, and a4096-digit materialization strategy applies only to display; exact symbolic terms and native majority remain available. Proportional huge polynomials can still expose their exact finite share. Copies with altered endpoints/predicates, noncanonical terms and boolean exponents refuse strict admission.

Immutable source f58d139f passes20/20 Linux console units in6.26seconds, zero skips/sourceguard, including an independently specified400-endpoint Fraction population, the exact half-boundary reproduction, zero-native/zero-total cases, common/relative enormous exponents, display underflow/materialization and copied-model refusal. Strict two-target mypy, Ruff and formatting pass. Evidence is analysis/recorded-oi-exact-authority-unit.xml and the separate read-only legacy repro analysis/oi-recorded-decomposition-boundary.json. This qualifies the additive pure authority only; output/model/serializer/classification consumption and actual OI producer/cache migration remain open.

## Local recorded OI statistics qualification

Exact source 60ae397aa727e756b12107ddbb8b7b2db7bfc980 (statistics source 0c9cfcc9) passed the bounded Linux console gate: 97 selected units, zero skips, 5.29 seconds, wrapper/source guard exit 0. The approved versioned read-only dependency runner used the existing owned services and shared lock with normal `-n auto`; original data and deadlines were retained. Evidence is `analysis/oi-statistics-60ae-unit.{log,xml}` outside the checkout. This covers strict input admission, actual selected endpoints, native and recorded USD historical populations, synchronization source selection, exact recorded decomposition and the new statistics boundary controls. It does not claim actual Truth/cache activation or global typing completion.

The next synchronized-baseline adapter must preserve the written existing `_composite_history` policy: a historical pair sums only instruments actually observed at both bounds, with distinct native and recorded-USD pair cohorts. It must not sum individual percentile ranges or silently require the entire present cohort at every old point. Current source/receipt/rule eligibility and explicit pair witnesses remain required before that policy becomes typed production evidence.
### Held replay refresh admission performance investigation

The unchanged ff3 captured-tools refresh diagnostic passed alone once, then the authorized phase probe reproduced its original five-second timeout. The actual RUN_MARKET_TRUTH job had started assessment and financial calculation, remained QUEUED under the existing worker protocol, and had no error/result at cancellation. Spot used 3.989 process CPU seconds over 3.994 wall seconds; whole financial math completed after cancellation at 5.435 CPU/5.449 wall seconds. This establishes a calculation-cost trigger in that reproduction, without attributing every earlier full-run timeout to one cause. The cProfile diagnostic counted 14,400 descriptor digests for 2,880 rows, four evidence validations and repeated strict source admission. Concurrent cancellation/reporting frames appear in its statistics, so its aggregate wall attribution is not treated as an isolated benchmark.

`source_descriptor_digest` retains strict public admission. Its private admitted helper is used only after strict descriptor construction or nested evidence validation; it encodes JSON-mode model output with the original canonical sorting/separators/Unicode/finite-number options, avoiding text decoding and a generic recursive JSON conversion. Hashes remain canonical storage-read descriptor hashes, never provider-byte hashes. `SpotCalculationSource.from_inputs` constructs and validates every consumed calculation fact directly once; request assembly is intentionally absent from this pure source record and is not redundantly admitted there. New controls reject copied invalid quantities, scope and descriptor integers through the real public/nested boundaries. Existing literal original/effective/scope/descriptor hash and lineage controls pass unchanged. No constructor bypass, weakened validation policy, clock/deadline/dataset change or resource restriction is introduced.

Source dd640cd9 passed 76 selected codec/spot/window units and strict three-target mypy. Its phase-instrumented actual refresh passed with spot CPU 2.831 seconds, whole math CPU 4.139 seconds and get_market_truth wall 4.745 seconds; the actual job reached DONE with a result. The inherited diagnostic source_head label remained ff3 accidentally; the separate probe-source manifest explicitly binds the actual dd640 candidate and plugin hash. This bounded improvement does not close the full-workload failure.

Final source f73e2db7 additionally removes the redundant generic JSON walk. Its 76 selected units pass in 42.15 seconds with zero skips (subset p90 1.510/max 3.461 seconds, not a whole-suite budget claim); strict three-target mypy and Ruff/format pass. The wider 614-unit run at dd640 had 581 passes, 23 failures and ten setup errors in 170.12 seconds: every red is an unchanged ten-second pytest timeout, with no assertion/schema failure. The final uninstrumented 61-case actual consumer selection has 60 passes and the original captured refresh five-second timeout in 135.58 seconds. Actual live reuse/SQL/no-write, other replay/reference, publication and refresh controls pass, but this cohort remains HELD. No further unchanged repeat is presented as closure. External evidence is analysis/replay-refresh-ff3-*, replay-spot-ff3-cprofile-*, replay-refresh-spot-readmission-after*, spot-readmission-perf-unit-*, spot-readmission-all-callers-unit.xml and spot-readmission-actual-callers.xml. Whole cache/OI activation remains open; the preserved OI prototype is separate.

### Inactive indexed spot evidence ownership prototype

`SpotEvidenceLookup` is an adapter lookup capability, not a domain-data replacement. Its public constructor accepts actual raw read collections and required policy/clock context, invokes the strict existing decoder, then owns immutable source facts and exact original/effective/scope evidence snapshots. Private dictionaries index real evidence digests and typed source references; they contain no arbitrary domain JSON. The strict pure source excludes assembly. `inputs_at` requires fresh request assembly and keeps original receipt clocks and evidence unchanged. Aggregate resolution validates the dual reference and expected projection, verifies stored bytes, uniquely decodes actual evidence, and reprojects consumed facts through the existing decoder/math before the real serializer rejoins extensions.

Source29acea41 is intentionally inactive. Forty-five focused Linux console units pass in13.86seconds with zero skips/sourceguard, including four new actual serializer, cross-scope/reference refusal, copied-policy refusal, nested caller/resolved-view mutation isolation and changed receipt eligibility controls; strict two-target mypy and Ruff/format pass. This prototype still calls the existing decoder, so it does not claim fewer whole-source admissions or a performance-budget fix. Replacing intermediate whole-source wrappers, integrating the actual producer/cache owner and qualifying before/after CPU plus complete affected callers remain required. The separate pure ISO parser cache is being qualified by the shared utility owner against fixed44db baseline before deciding whether the larger migration is needed for the current deadline failure.

### Active owned spot read, cached facts and serializer capability

Source `ce49787a84b42de89fcba6d897796d40892ad1f4` replaces the production wrapper chain with actual `SpotEvidenceLookup`, `SpotWindowEvidenceLookup` and `SpotDirectEvidenceLookup` raw-read capabilities. Raw row admission now constructs the owned strict source and immutable original/effective/scope evidence once. The public decoders remain strict adapters for their real pure callers, but production Truth no longer passes the source through DecodedSpotInputs, DecodedSpotWindows or the retired evaluation DTOs. The untyped snapshot factory helper is retired in favor of the fully annotated raw constructor; captured policy composition remains explicit and refuses missing policy.

`SpotWindowFacts` and `SpotDirectFacts` are named strict cache facts containing the calculated graph/result, canonical scoped source identity and visibility fingerprint. Processing assembly is not a cached financial fact. Adapter-private dictionaries index actual typed evidence references/projections. Real serializers verify and reproject the selected immutable bytes through these indexes; original hashes, scope and parent lineage are preserved. Explicit visibility relations come from admitted flow/ticker models, without recursively guessing clocks. A changed relation requires recomputation, while unchanged eligibility at a later processing instant remains reusable. Window graph/fact views revalidate into independent small copies, so caller mutations cannot alter cached graph dictionaries. State cache ownership retains the real capability instead of deep-copying the whole source; there is no deepcopy exception or arbitrary Pydantic resource field.

The source-independent strict `SpotWindowPlan` keeps logical bounds and written window policy separate from the large source. Private pure calculations consume the owned source and required current assembly; public calculator inputs still fully revalidate copied models. Sign history no longer creates and re-admits a full source for every composition. The two touched internal venue/qualification data carriers are now strict Pydantic records, including a real-integer discrete sign guard. No constructor bypass, guard token, model_construct, Any/object cache facade or changed validation policy is used. Existing financial, hash, null/zero, precision, receipt visibility, source correction and live-reuse/SQL/no-write assertions remain intact. The cache calculator spy now wraps the actual private direct calculation used by the raw owner, retaining all original call-count and result assertions.

The fixed-data replay CPU/count probe passed the original five-second inner deadline and all original assertions. Its job reached DONE with a stored result and no error; get_market_truth wall time was 3.692 seconds. Whole financial math used 3.238 process CPU seconds, owned spot admission/calculation 2.168 CPU seconds; this probe includes cProfile overhead. Actual 2,880-row validator counts fell from 25,920 to 5,760 for flow invariants, source-scope validation from eight to one, evidence-snapshot validation from four to one, and descriptor digests from 14,400 to 5,760. Earlier CPU samples and all red runs remain separate; no controlled end-to-end percentage improvement is claimed. The executed probe's two strict import-export diagnostics are retained: it accessed the actual class through Truth's imported namespace. Its exact executed bytes and hash remain unchanged in the external probe manifest; this is not an active-source type suppression or a relabeled run.

Qualification on immutable `ce49787a`: 84 focused Linux-console units pass in 36.21 seconds, zero skips/sourceguard, subset p90 1.172/max 3.007 seconds. All 61 collected actual consumer cases pass in 118.19 seconds, zero skips/sourceguard, including complete EventTools, reference, market refresh, Truth, PostgreSQL condition SQL/live reuse and replay files. The formerly failing captured refresh passes uninstrumented with its original inner five-second deadline (testcase including setup 6.622 seconds). Strict five precise source/test targets and Ruff/format eight targets pass. Configured Truth mypy still reports the real remaining `_condition_cache` annotation error; other actual producer/cache families are not concealed by an opaque union.

The full affected unit selection is not green: 632/636 pass in 134.18 seconds, zero skips and no setup errors; all four reds are unchanged ten-second timeouts. They are the legacy Bybit unknown-edge control at condition fingerprint serialization, slow-denial lifecycle at Session context registry YAML loading, formula T8 freshness control, and DOCUMENTED_UNTESTED semantic-status control in legacy liquidity calculation. No assertion/schema failure occurred. This selected cohort's p90 is 2.7445 seconds/max 10.067 seconds, so neither the per-case nor p90 budget is declared closed. Parent review/full combined qualification and remaining analysis/cache migration are still required. External evidence: analysis/spot-owned-admission-{focused-unit,all-callers-unit,actual-callers}.xml, replay-refresh-owned-admission-{cpu.jsonl,cpu.xml,source.json}, and spot-owned-admission-after-2.prof. No further unchanged repeat is offered as closure.

## Actual OI-only family and synchronized historical pairs

Source 38ed95bb86141da663571775fe7c89089337c9c6 normally includes qualified parent f3455ba4. The new owned calculator path selects actual synchronized current source witnesses and builds historical same-duration pairs from the current eligible instrument universe. Each historical pair retains its real contributing instrument/source references and original clocks; native and recorded USD measures can use different like-for-like subsets. A younger current instrument does not discard the older cohort's valid history. Unrepresentable observed pairs remain retained and make that measure's whole FLOAT64 baseline NOT_ESTIMABLE. Missing or invalid USD never borrows marks or erases native totals.

`calculate_oi_family` now produces the actual strict Basic derivative family through the existing serializer. It computes per-instrument and synchronized native/recorded-USD facts and explicitly marks unrelated funding, basis, liquidation and incremental-volume calculations NOT_REQUESTED. A shared strict endpoint-facts authority preserves exact recorded decomposition and classification for both individual and composite endpoints. Original scope, received/source clocks, configured tolerance/history/quantiles and semantic rule eligibility remain separate actual inputs.

The bounded changed-source Linux console gate passed all 102 selected units in 5.97 seconds, zero skips, wrapper/source guard exit 0 (`analysis/oi-composite-38ed-unit.{log,xml}`). Strict five-target mypy and Ruff/format pass. New actual admission/calculator/family controls exercise historical cohort changes, missing USD, unrepresentable whole populations, late receipts, copied-range refusal and final wire composition. This qualifies the source-to-typed-family tranche, not yet the operational Truth/cache activation or global cache typing closure. Existing actual/live reuse, financial callers and Scheduler migration remain required.

## Actual selective OI cache activation qualification

Production source `9c1a427e8454e5fc066673a120713a56d08ad5f8` activates the owned typed OI-only family in `compute_condition_values`; it no longer obtains that requested family through the legacy whole-derivatives dictionary. `OIConditionLookup` owns the immutable admitted storage-read/evidence snapshots; `OIConditionFacts` keeps only the strict result, canonical storage-read manifest identity and receipt-visibility signature. Actual selected closing contributions drive freshness, and failed freshness gates the exposed wire without changing the stored source facts. Fresh assembly stays in each request/proof. No historical FX or provider provenance is inferred for recorded USD amounts.

The operational `BaselineCaches` assembly moved from `statistics_contracts.py` to `baseline_cache.py` because the new actual per-instrument/composite population types otherwise create an inward graph cycle. All actual imports were migrated without a compatibility re-export. Statistical value contracts and existing typed cache/retention mechanics remain inward. The added OI population caches share the same existing global 128-entry FIFO owner with the other baseline families. Historical keys bind subject/cohort, source evidence, visibility, captured policy and logical bounds independently of current-only observations. Cached immutable populations are read directly rather than wrapped in a new full-history model that recomputes percentile validation on reuse.

The authored `synthetic_start_snapshot` explicitly represents a batch collected at its chosen end. Its OI rows now author that collection receipt directly at creation, with COLLECTION_ONLY precision; all source clocks, numerical inputs and original behavioral assertions remain. This is a newly authored fixture fact, not a production fallback, retained-capture backfill or inferred receipt from financial values. Funding fixtures remain unchanged. Separate missing/future-receipt controls retain genuine unavailability.

All 177 selected units passed in 29.39 seconds, zero skips, wrapper/source guard exit 0 (`analysis/oi-active-9c1a-unit.{log,xml}`). Selected max3.119 seconds/p901.032 seconds is not a whole-suite performance-budget claim. Strict ten-target source/test checks plus the separately checked new cache-control file and Ruff/format passed. Actual historical work is observed through both the native baseline calculator and composite range authority: current-only corrections reuse them, while changed historical evidence invalidates them. Independent native/recorded-USD quantities, exact MIXED classification, same-opaque-head late eligibility, unchanged reuse and caller-mutation controls remain.

Actual PostgreSQL proof at `28253b60` (production identical to `9c1a427e`) passed the complete original selective SQL/no-write case in 23.19 seconds, zero skips, wrapper/source guard exit 0 (`analysis/oi-active-28253-actual.{log,xml}`). The original full/relative/spot/depth/confidence, live repeated-read and no-MTA assertions remain. Added direct OI phases use real live assembly clocks, actual family serialization, current-source correction with observed historical reuse, exact recorded-USD MIXED output, and a future receipt that cannot expose a new amount. Controlled authored market writes happen only in the source-correction stages; evaluation writes remain forbidden and no `ana.market_truth` row is created. No clock freeze, deadline, dataset, worker or query-budget weakening was used.

This closes the bounded actual selective OI family activation candidate. The legacy full-assessment `_oi_stats` producer still needs the exact recorded-decomposition correction, and funding/close/confidence/depth/relative/state/supply producer and consumer migrations remain open. `_condition_cache` is deliberately not annotated as an opaque JSON/object union to conceal those remaining families.

## Requested-read and exact selected-evidence OI boundaries

The actual `MarketService.condition_snapshot` producer always declares the subject's `instruments` collection and, for an OI request, performs the per-instrument historical read into `oi`. The requested-read contract therefore distinguishes an omitted collection (the scope/read was not captured) from an explicit empty captured collection (no configured instruments or no observed samples). The latter stays unavailable without invented zero endpoints or a phantom venue. Generic `bounded_market_snapshot` previously manufactured absent `oi` as `[]`; the OI branch now retains original collection presence before that generic operation, refuses missing required reads before cache lookup, and indexes the admitted fields. The cached-empty-to-omitted regression also refuses before a cache hit can conceal the missing read. No broad removal of defaults in unrelated namespaces was made.

Per-symbol changes: `calculate_oi_family` now returns strict `OIConditionCalculation`, containing the unchanged Basic derivative result plus `OISelectedClosingEvidence` references for the actual selected closing cohort. Its real codec and typed pure-family test caller were migrated together. `OIConditionFacts` retains that same required evidence facet. `closing_observations` resolves each exact `OIInputEvidenceRef` through the admitted calculator's owned digest index; it no longer scans by instrument ID, Timestamp and native amount. The graph/cached-facts validators bind the distinct reference cohort to the actual synchronized instrument cohort. Full raw canonical source evidence is retained, and no source clock, receipt, USD amount or metric identity is substituted.

Actual read admission permits multiple source rows with the same lower-dimensional clock/native values; market storage identity itself also includes metric key. Selection can legitimately exclude an unreported-receipt row while retaining another actual source at the same instrument/clock/native quantity. The old freshness scan then included the excluded row and failed the real `compute_condition_values` caller. Before-fix receipt `oi-boundaries-37347-unknown-before` records exactly that one failure in 4.67 seconds: valid selected USD1 versus another metric's USD999 with unreported receipt. Earlier `oi-boundaries-0d192-before` records two required-collection omission failures and three passing controls in 5.14 seconds. Known-future receipt and unresolved bucket duplicates were passing controls at the actual producer boundary: generic bounding already filtered the former, while Timestamp metadata distinguished the latter. They are not mislabeled as reproduced product failures.

Final source `8e736d5f` passes all 60 changed-caller units in 24.51 seconds, zero skips, max2.391 seconds/p901.0773 seconds including fixtures (`oi-boundaries-8e736-unit.log`/XML). This selected p90 is not a whole-suite performance-budget claim. The set includes both omission controls, explicit empty scope, cached-empty refusal, unknown/future/unresolved duplicates and existing conditional/temporal/baseline financial controls. Strict four-target source/test checks and Ruff/format five targets pass. The complete actual PostgreSQL live SQL/no-write/current-correction/visibility case passes again on that exact changed source in 22.81 seconds, zero skips (`oi-boundaries-8e736-actual.log`/XML). Both wrappers/source guards exit0. Prior 177-unit and PostgreSQL evidence remains bound to its original source. Other cache families and the full-assessment OI/FX producer seam remain open.

### Closed Runtime market-refresh caller companion

This isolated companion normally joins parent1519805b, worker09962d81 and its qualified caller9a9adfc7; it contains no held Spot/OI ancestry. RuntimeMarketPort models only Runtime's consumed market operations with named CaptureReadiness/CaptureWaitOutcome/RepairOutcome/TokenEscalation values, an actual event anchor and service lifecycle. Product changes are limited to this inward protocol plus Runtime's import and constructor/property annotations. Worker, dispatcher, registry and fencing behavior are unchanged. The concrete factory test now explicitly narrows its actual MarketService before inspecting service-specific admission, rather than adding unused concrete properties to the Runtime port or casting.

The six parametrized market-refresh worker cases now build actual Runtime resources and invoke the real closed worker. Their required real Session, Truth and Scheduler remain the original tested services; ReleasedMarket remains the explicit controlled-acquisition adapter. Full registry/configuration is retained, current replay/ops/worker flags are preserved, and declared synthetic service credentials/private scratch paths make the unrelated real resources constructible without provider I/O. No empty registry, dummy financial record, fake inheritance, handler dictionary, active-jobs setter or constructor bypass is used for these workers. The helper owns worker shutdown and closes both current and replaced originally composed resources. The older separate direct-assess ordering unit still has its pre-existing fragment; this companion does not claim all Runtime fixture debt closed.

An AST comparison confirms all41 original assertions across the three parametrized tests are unchanged. The original20-second/no-wait/preflight5-second/lost-ACK/cancellation/deferred-condition/job-identity/decision-consumption and event-only EXIT/REDUCE controls remain. At immutable sourcee3f3f8ef, all8 collected actual file cases pass20.27seconds, zero skips/sourceguard, and all4 connected unit/composition cases pass7.77seconds, zero skips/sourceguard (selected-subset p90 1.6404seconds/max1.953seconds, not a whole-suite performance-budget claim). Current child534befad only narrows the Protocol anchor annotation and adds the real factory isinstance assertion; strict protocol/helper/factory three-target mypy, targeted Runtime/composition checking, Ruff/format five targets pass. No global source-mypy closure is asserted.

The final four-case qualifier attempt on534befad failed before collection when OCI could not create an owned mountpoint: the shared VM had exhausted its inodes despite free bytes. Quality independently confirmed5.24M/5.24M inodes used. No test ran, source was unchanged, and no cleanup/restart/retry was performed by this stream. That setup failure is separate from the earlier green unit/actual evidence and is not relabeled as a source failure or a final green run. Evidence: analysis/runtime-market-refresh-callers-{unit,actual}.xml and the retained final-wrapper setup receipt. Parent/peer integration and final combined qualification remain required.
