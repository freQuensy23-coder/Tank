# Agent orchestration refactor evidence

## Ownership inventory

This workstream owns agents/context.py, agents/jev.py, agents/pipeline.py,
agents/security.py, agents/sessions.py, agents/wire.py, agents/workflow.py,
agents/__init__.py, codex_gateway.py, runtime.py, bootstrap.py and openma_sync.py;
config/openma.yaml and config/skills/** align their public contracts. The initial
Python inventory is 5,260 lines. Other workstreams own repository/settings
contracts, event tools and service implementations.

## Completed contracts

JevQuestion, JevAnswer, JevUsage and JevResponse are strict frozen Pydantic
domain objects. Pipeline consumes attributes rather than indexing provider
dictionaries. Jev validates the exact requested answer-name set, calibrated
finite probability range and nonnegative strict accounting fields before a
response reaches durable usage storage. Missing provider accounting stays
explicitly unknown, rather than fabricated zero cost/tokens.

JevQuestion.criteria is absent for instruction-only protocol questions.
JevUsage input_tokens/output_tokens/cost and JevResponse id/provider may be
absent because the provider does not guarantee reporting them. These optional
fields express protocol absence. Question/answer dictionaries are genuinely
dynamic maps keyed by caller-selected classification names; criteria is a map
of probability labels to descriptions. Jev state remains a wire JSON value:
the external classification endpoint accepts task-specific state, whose
domain projections are produced by Pipeline. Serialization occurs only when
constructing an HTTP request, request fingerprint or repository document.

## Credential and wire boundaries

The pure redactor takes only a frozenset of configured credentials, rather than
inspecting arbitrary Settings internals. Settings owns credential discovery.
Its recursive dictionary/list handling is necessary because untrusted tool JSON
can put credentials at arbitrary nested positions; no domain record is inferred
from that tree. Configured secrets, percent-encoded variants, URL query
credentials and URL/DSN userinfo remain scrubbed before durable tool capture.

CodexGateway is entirely annotated and passes strict mypy. Credentials,
RefreshedTokens, RefreshRequest, GatewayErrorDetail and GatewayErrorResponse
are strict domain models. Credential fields are excluded from repr. Every JSON
decode goes through a validated JSON-value boundary. OAuth update selection
distinguishes omitted refresh/id tokens (preserve existing token) from present
invalid values (refuse response). OAuth refresh/recovery, atomic credential
persistence, shared refresh locking and cancellation shielding remain explicit.

Credentials.document intentionally preserves the operator-owned auth JSON as
an opaque adapter document; unknown operator/token fields are retained during
atomic writes. JWT claims are similarly an extensible external wire map.
Responses body, content items and terminal provider responses are external
protocol payloads relayed by this bridge, not Tank domain entities. Their
dynamic JSON maps stay inside this adapter and are structurally validated
before use. Headers are inherently string-keyed HTTP maps. None denotes an
absent refresh token, unknown expiry, unopened HTTP resources and no downstream
response before prepare. Missing last_refresh triggers refresh conservatively,
because unknown credential age cannot establish a still-valid generation.

## Validation

Pinned Python 3.13.13 environment and frozen dependencies used.
Jev focused suite: 26 passed, zero skips, pytest-xdist -n auto, 1.57 seconds.
Ruff checks/format pass for changed source/tests. Strict mypy passed jev.py
before shared base adoption; final cross-module verification is pending.
Gateway and security strict mypy pass. Gateway: 2 unit tests and all 49 local
HTTP integration tests pass, zero skips. Security: 18 tests pass. Combined
Jev/security/session targeted unit suite: 86 passed, zero skips, 2.40 seconds.

## Remaining scope

Session persistence, stream events, tool ownership/idempotency, native session
recovery, context projection, gateway OAuth, OpenMA sync, registry bootstrap
and runtime orchestration still require their complete typed-domain migration.
This evidence does not claim those unconverted modules are complete.


## Native provenance and runtime commands

agents.provenance is a dedicated native-JSON adapter. SessionService consumes
AgentProvenance objects, serializing them only for persisted session metadata.
ResolvedSkillVersion requires type/skill_id/version together; incomplete or
floating native snapshots cannot produce a pinned prompt fingerprint.
Native fields are required once their enclosing snapshot is present. Missing
native model/version uses an explicitly labelled deployment declaration,
never an unlabeled provider/model substitution. Historical session hydration
can have no agent ID; explicit null preserves that absence without assigning
the current deployment's agent to an older session.

The direct package cycle was removed by importing SOURCE_COMPLETION_VERSION
from agents.workflow rather than importing the agents reexport package.
Session retention consumes the strict versioned Thresholds contract.

runtime_contracts defines closed durable command families for item filtering/
routing, market escalation/assessment, scheduler checks/captures, delivery,
Telegram answers, scout reviews and all six agent message kinds. Each accepts
dependency metadata under its existing _depends_on wire name. Serialization
with exclude_unset preserves absent versus explicitly empty dependencies.
ScoutReviewJob accepts the existing durable prp_id spelling explicitly at the
adapter boundary and exposes proposal_id internally; this preserves already
queued jobs while producers move to proposal_id. Ambiguous double spelling
is refused. Delivery requires an explicit channel and exactly one subject.
Market cutoff precision must be declared or have an explicit missing reason;
it cannot be invented from the timestamp string.

Market escalation and assessment now consume their models via worker adapter
wrappers. Remaining command consumers are coordinated with the scheduler/
delivery/Telegram and event-tool workstreams. The complete agent notification
union references shared WatchCondition/WatchCall/MilestoneSnapshot contracts.
An expired-call notification requires the closed EXPIRED snapshot; this exposed
a scheduler producer bug that emitted its old OPEN record after persisting
a different CLOSED copy.

Validation: the owned cross-module unit suite passes all 315 collected cases,
zero skips, 8.07 seconds. New runtime payload contracts add 11 passing cases.
Strict mypy passes runtime_contracts.py and agents/provenance.py.


## Tool completion ownership and native HTTP

CapturedToolResult is a strict completed-outcome model with a validated JSON
result, replacing the previous object-typed dataclass. Its error flag is
derived from DONE/ERROR/REFUSED. Older durable captures may omit that duplicate
flag; when present it must be an exact consistent boolean. This preserves
old capture recovery without treating a failed/refused capture as a newly
successful fetch. Signature equality includes the outcome and detached result.

ToolFailure and FailedToolResponse close the audited failure shape. NativeText
and NativeToolResult close the native response body before it is persisted for
idempotent resend. Successful response bodies omit is_error; failures send true.
Native HTTP now has explicit method/path/query/upload/idempotency/timeout
parameters, validates decoded JSON and rejects non-object endpoint responses.
Query mappings and repeated query-pair lists are both necessary HTTP wire forms;
multipart mappings contain exact filename/bytes/media-type tuples.

Focused session completion/retry/workflow validation: 62 tests pass, including
10 new captured-outcome and native result contract tests, zero skips, 3.02s.
tool_contracts.py passes strict mypy; sessions.py passes current configured mypy.
SessionService as a whole still needs its remaining domain-record and port
migration; current mypy passing is not a claim that all methods are annotated.


Gateway tests now also pass strict mypy in their entirety. The fixture factory
uses a precise protocol for synchronous/asynchronous HTTP mock handlers and a
typed async context manager, and real Settings rather than an arbitrary
namespace. All helper functions, test parameters, stream iterators and cleanup
methods have exact annotations. Synthetic JSON auth/protocol fixtures remain
wire maps with runtime narrowing where nested structures are inspected.
All 49 gateway integration cases still pass, zero skips, 1.87 seconds.


Integrated regression fixes: Runtime consumes the canonical EventAnchor model's
guaranteed value for capture readiness and repair intervals. It no longer
indexes a JSON field and passes a union to parse_time. The denial-turn fixture
declares all seven score dimensions as explicitly unestimated; no score is
fabricated. Watch-disarm fixtures instantiate complete WatchCondition records
with provenance, lifecycle timestamps and fully specified expressions.
Milestone notification outcomes retain verified descriptive source outcomes,
including event-state labels and official denials, rather than a narrower
unsupported enum. These 24 focused regression/runtime tests pass, zero skips.


CI found a real native REST deadline regression introduced during request
signature typing: a default argument captured REST_TIMEOUT_S at definition
time, ignoring subsequent configured/test deadline changes. The native request
adapter now reads REST_TIMEOUT_S at each request; its stream timeout remains
independent. Both local real-HTTP deadline integration cases pass, zero skips,
1.18 seconds. The PostgreSQL scout integration expectation now matches its
canonical proposal_id producer contract.


## Required runtime composition

Runtime.__init__ now requires every service and the registry-bootstrap/settings
validation callbacks explicitly. It constructs no services and has no nullable
dependency injections or hidden default service factories. Production
build_runtime in runtime_composition creates and wires one generation.
RuntimeSupervisor and production registry/config validation moved into that
composition module, so core runtime no longer imports bootstrap/composition.
The supervisor's factory/validator/candidate loader are required typed ports;
build_supervisor supplies the production implementations. Candidate generations
must implement preflight preparation, rather than using hasattr to skip it.
Configuration identity comparisons use required database/native/data-directory
attributes. Candidate/generation nullable state only means not constructed yet.

Runner/cleanup and gateway lifecycle methods have narrow protocols. Domain
service dependencies still use concrete types pending their exact application
ports; this is recorded open work, not a declaration of completed inversion.
The stateless EventToolService owns no HTTP client and is not a runtime lifecycle
dependency: its injected client is borrowed and its temporary clients use an
exit stack. The composition root binds its scheduler and the session's Telegram
sender before constructing the runtime. Shutdown closes owning services only.

All 21 runtime/reload/drain/precision unit cases pass in 1.57 seconds with explicit
required lifecycle fakes. Configured mypy passes both runtime modules; full
strict runtime method migration remains open. CLI/E2E owners are coordinating
the new build_supervisor import and explicit test construction.


## Market assessment application port

Runtime now depends on the precise MarketTruthPort.assess_for_job protocol and
receives a strict MarketTruthAssessmentRef. The worker adapter serializes only
that reference into the job result; numerical/provenance evidence remains in
the assessment repository and is hydrated by evidence consumers. This removes
the full untyped MTA document from the runtime application port without claiming
that the calculation engine's complete stored document is already migrated.
Window repair is a required capability and always runs before assessment;
a missing fake/implementation method can no longer silently skip repair.

Every owned durable worker fixture registers assess_wire, never the typed
assess service directly. Queued cutoffs carry explicit precision. The bounded
external-capture integration cases hydrate persisted assessments through the
typed job reference and still verify incomplete bounded waits, actual terminal
capture history, immutable previous results, exact repair windows and budgets.
Both PARTIAL/FAILED cases pass in 2.02 seconds with real asyncio capture/repair workers,
zero skips. All 21 runtime unit cases pass in 1.65 seconds. Configured mypy passes both
runtime modules. Typed direct service callers remain model-based.


Controlled shutdown validates the precise JobRetryState projection before
refunding a claimed attempt. It no longer performs arithmetic on a generic
JSON field. A RUNNING job with no consumed attempt is explicitly inconsistent,
rather than silently clamped to zero. The drain and last-attempt preservation
cases pass, zero skips; configured runtime mypy passes against the queue-state
contract.


JevItem/JevEvent/JevState now carry classification inputs through Pipeline
and the provider boundary as strict models. State dictionaries are created
only by explicit to_wire serialization for HTTP and canonical fingerprints.
Optional context fields preserve the previous omission rules; exact wire
projection tests verify the online/calibration question and state shapes stay
unchanged.86 focused pipeline/provider/agent tests pass, zero skips, 2.82 seconds.
SourceItem repository-domain migration remains coordinated with ingestion;
raw source/config access inside Pipeline is not declared complete by this
classification-input migration.


The guarded actual integration run for all gateway-audit, runtime,
session-admission and scout-PostgreSQL cases passes: 31 collected, 31 passed,
zero skips, 35.27 seconds. This includes both database backends for delivery
order/lock contention and admission cap ordering, both bounded-capture terminal
states, the locked native Node gateway library, and gateway replacement on the
same loopback port. Fixtures declare delivery channels and complete configured
thresholds; supervisor fixtures use real Settings and the composition API.
No timeout or outcome assertion was weakened.

agents-inventory.json enumerates every owned Python function, line, missing
annotation and dynamic-map usage. Remaining entries are implementation work,
not blanket approved exceptions; it makes unfinished scope reviewable while
the domain migrations continue.


The final tool wire adapter has one meaningful parameter: its projected result.
Unused tool/session/call parameters were removed from both native emission
paths. It accepts the shared, explicit JsonInput serialization types and returns
validated JSON values. Its recursive guard now processes strict Pydantic results
and tuple/set containers before serialization, so typed results cannot bypass
internal-field removal. Decimal values are sanitized as numbers and serialized
afterward; their exact strings are not mistaken for opaque record IDs. Two
regressions prove nested domain results hide IDs while retaining a 34-digit
public amount exactly. The wire module and gateway pass strict mypy, including
explicit return annotations on all gateway constructors.101 focused context,
migration, session-security and wire cases pass, zero skips,2.00 seconds.
Generic JSON maps/containers here are necessary external serialization inputs;
they are not substitutes for application domain records.


## Validation diagnostic privacy

SessionService.tool_failure handles Pydantic ValidationError separately from
ordinary domain ValueError. It returns a bounded schema diagnostic and never
forwards the raw exception string, input values, error context or custom
validator messages. The actual dispatch regression raises a provider schema
error whose custom message contains an unconfigured credential, then verifies
the durable capture, native response and logs all omit it while preserving the
REFUSED/error outcome. This works independently of shared hide-input policy.
Jev provider validation raises its static refusal with suppressed exception
context so a later traceback logger cannot reveal the provider input document.

Gateway _claims/_load and refresh validation already convert malformed JSON,
claims and token fields to static GatewayError codes with suppressed context.
Gateway responses and cleanup logs contain bounded static diagnostics; provider
bodies and auth documents are never forwarded as validation diagnostics.
The new tool-path privacy case and focused security/provider/gateway unit
cases pass:33 tests,zero skips,1.52 seconds. Existing actual gateway integration
coverage includes credential-bearing provider exceptions and malformed refresh
responses.


The two integrated-check-5 fixture corrections are isolated: reload configuration
now supplies the complete declared source-config root while preserving snapshot
and malformed-edit assertions; the PostgreSQL score callback reads JevState
attributes, so it reaches the overlap barrier. The reload unit case passes in
1.50 seconds. All seven actual routing PostgreSQL integration cases pass in
3.15 seconds via the guarded runner, zero skips; timeout budgets are unchanged.


## Inward tool wire contracts

contracts.tool_schema owns scoped-tool names and the sole shared strict
JSON-schema-subset validator; context projection no longer imports the
EventToolService application module. The application owner is removing its
previous definition and importing this shared implementation in the companion
commit. This change must be integrated with that removal so no duplicate
validator remains. Tool ID scope/ownership checks stay in ModelContext.

The validator has precise JSON types and no dynamic Python-type registry.
It keeps required/unknown-field/union diagnostics, rejects nonfinite numeric
wire values, distinguishes booleans from numbers in enums, and enforces
previously ignored declared maxItems and additional-property schemas.
Malformed present keywords cannot masquerade as absent constraints.
Schema dictionaries are necessary wire-schema trees, not domain payload
facades. Absent additionalProperties means true by the JSON-schema contract;
absent properties means no named property constraints. Description/format
remain annotations in this checked-in subset; domain timestamp validation
continues at its actual service boundary.

ToolArgumentReference is a strict object carrying each extracted path/kind/ID
into scope checking. Static ID_FIELDS/PREFIXES tables define the shared
identifier taxonomy; an absent containing field is valid recursive traversal
state, not a missing required domain value.76 schema/context/migration cases
pass in1.97 seconds,zero skips; shared validator and its complete focused test
module pass strict mypy.


The v2 source tranche uses an explicit test-only synthetic publisher factory
in tests/agent_source_fixtures.py. It declares complete capture, actor,
publication-unknown, observation-clock and processing-policy fixture data,
then validates the real canonical SourceItem model and emits its actual flat
storage shape. It never patches runtime readers or relaxes model requirements;
unknown/retired fields still fail the canonical schema. This lets boundary
tests replace incomplete source bags with named complete fixture records.
The helper passes strict mypy with the actual src import path. A real offline
Pipeline/HTTP/repository smoke test classified its S30 item and re-read the
strict cached score successfully.


### Typed classification use case (v2)

The standalone agents.classification.JevClassifier owns nongenerative provider classification and exact immutable score reuse. Its required ClassificationScores port has only typed get/put operations; file-backed calibration no longer needs to impersonate the runtime queue repository. ClassificationSettings declares the four required configuration values; ClassificationSource declares the item identity consumed by the classifier. Caller-owned HTTP transport lifetime stays outside this use case.

ClassificationIdentity and JevScoreRecord replace tuple/dictionary application records, including exact usage, missing-field reasons, model bindings, cost, timestamps and fingerprints. Question-name dictionaries are bounded provider wire namespaces, and registry indexes are explicitly keyed domain collections. MissingClassificationFields optional reason absence represents metrics actually reported by the provider; its serializer retains the existing omission wire convention. This checkpoint is inactive until Pipeline/calibration consumers are migrated; full source/session typing remains open.

Validation: 27 offline Jev/classification contract cases passed in 1.54 seconds, including immutable cache reuse and rejection of a contradictory persisted source identity before another provider request. Strict mypy passed for classifier, score contracts, provider models and the new behavior test with real source imports. Pipeline activation remains separate.


### Read-only routing separation (v2)

agents.routing exposes a strict RoutingItem projection and pure item_state/item_view functions. RoutingSelector requires only a read-only get/iter_range port and an already validated RoutingRegistry. Calibration can now select and project events without queue mutation, provider transport or progress notification dependencies. RoutingItem.from_source projects canonical publisher source records; packet adapters must declare their own actual source metadata before constructing the projection.

Missing identity hints and monitored author represent unavailable producer metadata. Text formatting omits empty presentation fields by design. Unknown registry display labels use their explicit identifier as a readable label while preserving identity; required source text, event lifecycle, source relevance and repository identities are never invented. Read-only candidate projection requires all actual event/session fields it consumes, rejects event/key mismatch, preserves inclusive batch timestamps, and excludes closed lifecycles.

Validation: 15 focused routing/provider/classification cases passed in 1.66 seconds and strict mypy passed for the routing module/test. The seven real PostgreSQL routing concurrency cases passed in 2.98 seconds with complete synthetic records under the pending Pipeline activation. This commit only publishes the standalone selector; activation and caller migration remain separate.


### Canonical SourceItem and Pipeline activation (v2)

Pipeline now decodes stored sources through Repository[SOURCE_ITEMS] once, consumes FeedSourceItem/SourceProcessing/SourceRelevance attributes, and persists validated lifecycle transitions through SourceItemDocument. FILTER_ITEM and ROUTE_ITEM core handlers require ItemJob; explicit wire adapters validate job payloads and serialize source records at dispatcher boundaries. Production composition injects the required SourceProgressSink. Borrowed HTTP clients remain open; a constructed client is owned and closed by Pipeline.

Classification uses the standalone JevClassifier and strict immutable JevScoreRecord ledger. Routing uses the pure RoutingItem projection and read-only RoutingSelector, preserving calibrated Jev wire state and stable candidate order. Concurrent scoring cancellation never commits a partial winner. Source updates are reloaded under the deduplication transaction; unconditional relevance promotion wins over an in-flight score. A routed item remains routed on filter retry. Identity/key mismatches and incomplete capture metadata fail before provider calls or queue publication.

The retired source aliases body/source_name-as-publisher, tokens and reposted_by had no active collector producers; canonical source/text, identity_hints and monitored_author now carry those actual contracts. The dead source event_class priority override is removed: actual source deferrability determines priority. Unknown registry display names may use the explicit identity as their readable label; unavailable identity hints remain optional metadata. Missing source/event/session facts have no service defaults. Canonical source observation timestamps are collection clocks; optional publication provenance stays separate and queue expiry derives the observed collection clock.

Validation: 77 offline focused units passed in 5.41 seconds. All ten relevant integration cases passed with actual services in 3.54 seconds; the seven PostgreSQL concurrency cases were rerun after precise test typing and passed in 3.49 seconds. Strict mypy passed for Pipeline, classifier, routing, score contracts, refusal tests, provider tests and PostgreSQL tests; default mypy passed for runtime/composition/Pipeline. Ruff checks/formatting passed. Full typing of the larger existing agents.py/pipeline_audit test helpers remains part of the ongoing owned test migration, together with SessionService/context/OpenMA/bootstrap; this milestone does not close the global goal.


### OAuth refresh-clock zone validation

Credential refresh age now requires an explicitly zoned last_refresh timestamp. A missing, invalid or zone-less clock requests the normal OAuth refresh instead of interpreting local server time. The gateway continues to preserve the operator-owned auth document and emits a UTC Z refresh timestamp after a successful exchange. This is an absence/invalid-metadata recovery branch, not an inferred expiry.

The actual offline HTTP bridge regression uses a valid credential without an expiry claim and a future naive refresh clock, proves refresh happens before forwarding, and checks the replacement timestamp declares UTC. The one focused actual integration case passed; provider destinations and credentials remain synthetic.


### Native source outcome transitions

Native completion and terminal failure now load SourceItemDocument through the typed source repository and apply explicit SkippedExtraction/FailedExtraction transitions. A source lookup verifies the stored item identity. Completed native turns may mark pending/running extraction skipped with the actual completion reason; failures only change pending extraction. Missing source records remain a real retention/deletion outcome rather than a fabricated record.

Existing native retry, execution barrier and session audit fixtures now contain complete captured sources. Seventy-nine focused native/session unit cases passed in 5.88 seconds. A three-case real completion-barrier regression additionally verifies stale NO_EVENT metadata is cleared for a newly skipped outcome while an existing failed-source reason remains unchanged. The larger SessionService record/port migration remains open; default mypy still reports its unchecked legacy constructor, and this milestone is not presented as strict full-session coverage.


### Complete captured registry contracts (inactive bootstrap dependency)

contracts.registry now declares complete RegistryConfiguration, DependencyConfiguration, SemanticsConfiguration and ExchangeAddressesConfiguration models without application, storage or I/O imports. SourceAccount and its authority/role contracts are reused. Token identities, contract descriptors, lending catalogs, registered instrument metadata and nested fundamentals evidence remain explicit records. Metric families discriminate aggressor, depth, funding, liquidation, open-interest and price records; each requires the exact fields used by that family. No semantic-confidence policy or probe was added.

Instrument multipliers/tick/lot sizes are Decimal or explicit unknown, never guessed one. The YAML adapter validator accepts declared decimal text or existing Decimal values and rejects booleans/numeric coercion; configured wire roundtrips retain decimal strings. Entity names/authority/review/source hints, qualified aliases, chain evidence, supplementary fee metadata, metric-specific documentation/lag descriptors and optional explicit natural-key IDs represent actual absent config metadata; their None defaults preserve omission on serialization. The unavailable-wallet dictionary is a venue-keyed reason index, not an anonymous structured record.

All four actual captured configuration documents roundtrip unchanged through Settings.load_config_as and strict models. Three malformed instrument variants exercise that actual loader and refuse missing identity, boolean multiplier and undeclared guessed size. Seven cases passed in 1.65 seconds; strict mypy and Ruff passed. Bootstrap activation and prepared managed registry records are still pending.


### Historical native source projection and terminal guard

The full live SourceItem requirement in the first native outcome migration was too strong for retained historical source prefixes. contracts.source_repository now exposes required SourceExtractionState identity/status and SourceExtractionRepository.get/transition. Session native completion/failure consumes this inward port. It validates every named lifecycle update, reloads under ingest-dedup plus transaction, and preserves unrelated captured fields. Pipeline and active acquisition retain complete SourceItem validation.

Retained exceptions are site-specific: source_repository._state uses document.get only for extraction_decision and extraction_reason, whose absence is an actual unrecorded outcome annotation; identity and status use required indexing. SourceExtractionRepository.get returns None only for an absent persisted row. transition returns None only when no source survives retention/deletion; its status guard returns the actual validated state when a concurrent writer already made it terminal. The raw document and patch dictionaries in transition are restricted to the persistence adapter and retain unowned historical data, including raw evidence references; they never escape as domain records. SourceLifecycleStore's context methods are required ports.

The standalone production adapter regressions prove missing/mismatched identity and missing/unknown status reject without mutation, DONE survives late failure, old NO_EVENT metadata clears for a new terminal outcome, deleted rows are not reconstructed, and unrelated historical data remains canonical-serialization equivalent (including boolean/integer types). A real native completion-barrier test also exercises a historical source with no live source_id/author and preserves its capture prefix. The first 22 focused cases passed in 2.46 seconds; extended required-lifecycle cases are included in the final checkpoint proof.


The projection port is now narrowed to exactly get/put/lock/transaction; it does not require scanning, deleting or unrelated repository capabilities. SourceExtractionRepository.complete is a separate authoritative accepted-evidence command: it validates required identity, then writes explicit DONE/cleared rejection metadata without reading or manufacturing a historical prior status. Native transition/get continue to require actual prior lifecycle state. SourceExtractionIdentity is its strict input projection. complete's absent-row None has the same retention/deletion meaning as transition; _write is the sole validated lifecycle serialization/merge boundary. The minimal four-method store regression proves this dependency boundary and canonical historical prefix preservation, plus missing/mismatched identity rejection.


### Typed bootstrap activation

bootstrap.validate_registry now consumes the complete captured config models through RegistrySettings and prepares strict per-table PreparedRow objects. Required versions, IDs, unit descriptors and clock declarations validate before any database write; preflight runs serialization before activation. RegistryArtifact hashes each captured configuration file once and reuses that immutable provenance for its rows. Bootstrap no longer imports concrete Settings or storage implementations: RegistrySettings is the captured-config port and RegistryStore exposes only its actual persistence operations. sync_registry returns strict RegistrySyncResult; reporting uses its explicit to_wire adapter.

The persistence adapter holds the registry lock plus a transaction, validates managed storage identities, removes stale configured rows, and preserves independently discovered rows and unowned fields. A discovered defect in the former raw merge retained a removed qualifier on an alias; the adapter now replaces schema/provenance-owned fields while preserving only unowned extensions. The two-sync qualifier removal regression proves the declared configuration becomes authoritative and discovery metadata keeps identical canonical bytes/types. Corrupt managed identity rolls back earlier writes; no key is inferred.

Site-specific retained exceptions: _entity_record supplies only an absent display label from declared names or the explicit entity ID, never an alias or authority assertion; explicit name=None remains unchanged. validate_registry derives absent basket_id from the required sector and absent edge_id from the required entity/token natural key; optional token/chain is encoded as JSON null in _key, never guessed. _routing_aliases treats qualified_aliases=None as undeclared positive matching rules and leaves unqualified requires_any omitted. RegistryWalletList.chain=None is an unassigned EVM scope, not a guessed network. _sync_rows existing=None means a genuinely absent persisted row; its raw dictionaries, _document and _config_timestamp_metadata are confined to document serialization/merge boundaries. RegistrySyncResult.to_wire is the physical-table reporting projection, not a Mapping-compatible domain facade.

Registry clock-facet defaults on FundamentalsEvidence, RegistryInstrument and RegistryEntity denote absent declarations only; all supplied precision/reason/size/anchor aliases pass the shared consistency validator. Unknown retained precision stays unknown with its existing reason rules. Each optional field and mapping site is indexed in agents-exceptions.json; the complete module/function inventory is refreshed separately.

Independent proof:40 focused bootstrap/config/runtime unit cases passed in6.39seconds before final artifact-port cleanup; the exact candidate proof is recorded in agents-bootstrap-activation-unit-v2.log. Strict mypy passes for bootstrap, registry contracts and both bootstrap test modules; default composition mypy and Ruff pass. Actual E2E has3passes and one quality-owned incomplete synthetic registry fixture pending its isolated companion; this candidate is not claimed to pass the whole suite. The complete shared dependency is ingestionbaf3230, including c95fca5 integer/float preservation and declared EndpointVerification clock facets. Flow commitsd89fcf3/4d196a2/34ad290 are not ancestors of this candidate.


Independent review caught an ownership gap across valid metric variants: clearing only the new record type's fields retained old depth-only fields when the same metric key became funding. bootstrap._owned_fields now enumerates the complete closed schema family for every physical table; the metrics family unions all six supported variants. There is no new immutable metric-family policy. A two-sync test using actual valid depth/funding configurations proves removed family fields clear, new declared fields persist and unrelated discovery metadata remains canonical/type-equivalent.


### Required producer-version facet

Bootstrap, Pipeline/JevClassifier and SessionService now require a validated ResolvedVersionIdentity dependency. Bootstrap's config-reader port no longer exposes raw config/code version properties; classifier settings contain only actual provider/replay controls. Runtime composition obtains the facet with Settings.require_versions after capture, and passes it to source, scheduler and owned producer constructors. Native resolve/tool_context/usage use the required host producer identity while historical session_config_version/session_code_version remain explicitly nullable; old session identity is not relabelled.

Fixture calls declare synthetic identities explicitly. Production-effect regressions reject invalid/copied marker identities before client allocation or config reads/writes and prove a classifier with no raw version attributes writes the supplied identity. Session in-memory execution/history polling now uses strict ExecutionPoll/HistoryPoll records rather than positional tuples; absent cache entries mean a first poll, not unknown execution facts. The 50ms initial/one-second capped polling policy is unchanged. HTTP request query sequences normalize through the SDK QueryParams boundary, retaining repeated ID order. Borrowed/owned HTTP clients now branch on explicit None rather than truthiness.

The SessionFileSender port is the native bridge's file-response envelope; it is not a general domain-record substitute. Its temporary dict result reflects the current Telegram adapter API and is serialized at the native tool boundary. Full SessionService/Context/Telegram receipt migrations remain open, including constructor and journal domain types; this stage does not claim full session strict typing.

Proof:268 focused owned unit cases passed in13.96seconds, four required-facet production-effect cases passed in1.49seconds, strict classifier/Pipeline/bootstrap/poll-state/facet-test targets pass, and default owned source/composition mypy passes. The guarded integration result is recorded in agents-required-versions-integration-v2.log. Calibration and quality-owned E2E callers require their coordinated explicit-facet companion; no compatibility default is provided.


### Read-only OpenMA first-party provenance/caller classification

agents-openma-provenance.json records exact observed hashes, independently verified Tank-added origin and production/type-only/test/qualification callers for the six tank-retained modules, node-managed-skill-aliases.py and tank-replay-evidence.ts. Origin is checked against quality's checksum-recorded exact upstream archive at85d5361f535c7221de975838dc48444e468cc02e. All eight are Tank additions; upstream licenses/notices and historical TANK_SOURCE receipts remain untouched.

guest-file-worker.ts contains an active embedded Python program, passed by guest-file-io through the BoxLite guest execution adapter; its inner comments/types need first-party inventory coverage. The stable-skill alias Python worker is loaded by node-managed-stable-skills.ts, with direct Python claim tests. Retained lifecycle and replay modules are reached by the native managed route and sandbox adapter, with explicit offline/pinned-provider qualification callers. This classification assigns no vendor edit ownership and does not mark the implementation families complete. Quality owns inventory/manifest plumbing and will use these caller/hash records; no duplicate upstream archive was fetched.

### Required public schema boundary

ModelContext.public_schema now requires the captured read_config_json capability and a valid closed tool-definition list. Missing files, malformed roots/definitions, duplicate tool names and unknown requested tools fail before dispatch. parse_tool_definitions owns the JSON-schema adapter: ToolDefinition.input_schema remains a dictionary specifically because JSON Schema is an externally declared schema language, not application state. No permissive domain mapping was introduced. Existing SessionService typing and analyst_tools admission are still under migration.

Transport fixtures now send genuine required public inputs and explicitly declare current model_context_version plus the actually installed tool-manifest fingerprint. This preserves the separate legacy-migration tests instead of having unrelated current-session tests accidentally exercise an uninstalled manifest.

Validation: 227 agent unit cases passed in 8.87 seconds; 35 actual guarded integration cases passed in 16.64 seconds, zero skips. Four new tests exercise schema read/validation failures through native tool result publication and prove a private malformed input is absent from the native response and logs. The shared parser passes strict mypy; full SessionService/context strict closure remains open.

### Producer version composition companion

The canonical producer activation a89401b is merged. build_runtime passes the same already-resolved version identity to MarketService, MarketTruthService and EventToolService; ten owned test modules declare fixture identities explicitly at constructor boundaries. Historical stored provenance expectations remain unchanged. Validation: 339 owned units passed in 14.15 seconds and 40 actual guarded integration cases passed in 35.34 seconds, zero skips. Default composition mypy surfaced newly checked sibling constructor caches and scheduler's inferred None; those precise owner annotations are requested from analysis before cohort activation.

### Strict context registry read projection

ContextRegistry now validates the required token/entity/instrument/source namespaces before constructing model context; ModelContext no longer suppresses configuration lookup failures or invents an empty registry/source configuration. Closed TokenLabel, EntityLabel, InstrumentLabel and SourceLabel records replace display-record maps, require the fields actually consumed and reject duplicate namespace identities before indexing. Registry namespaces are read projections, not substitutes for the complete configuration loader.

Retained sites: TokenLabel.name/project are required nullable projection fields because captured identity views may only declare symbol; display_name uses the documented project/name/symbol label precedence. EntityLabel.name is required nullable; explicit aliases then names then entity key are display alternatives, not invented registry membership. ModelContext token/entity/instrument unknown-key rendering preserves explicit unresolved references; source_label returns None for an unknown source so diagnostic projection does not invent a registered source. Key-indexed dictionaries are identity indexes over validated models; label_rows consumes configuration JSON only at the adapter. Registry_pattern None means there are no registered names to substitute.

Validation: 339 owned units passed in 13.36 seconds, ten new required-namespace/identity/name projection cases passed in 1.57 seconds, and 35 actual guarded integrations passed in 27.75 seconds with zero skips. New projection source/tests pass independent strict mypy. The remaining context session/domain journal migration is still open.

### Calendar review metadata under strict utility parsing

calendar_review_metadata validates YYYY-MM-DD review labels as actual calendar dates, preserves the original text and reconciles declared precision aliases with the shared precision-only API. It never invents an occurrence, midnight or timezone. Registry review-clock validators and bootstrap's metadata serializer use this explicit branch; actual date-times still require aware occurrence parsing. Optional recognized precision/reason/bucket facets are adapter metadata and remain absent unless declared; unknown precision is None with the explicit INCOMPLETE reason. CalendarReviewMetadata has required nullable fields and validates canonical precision and bucket-anchor consistency. Dictionaries in this helper are serialized configuration inputs/metadata outputs only.

Proof: 39 actual-configuration/bootstrap/context cases passed in 3.66 seconds, including full canonical serialized roundtrips, calendar date with/without declared daily bucket, invalid calendar date, naive date-time refusal and conflicting alias refusal. Registry/bootstrap/tests independently pass strict mypy and Ruff. A wider strict-utility scan passed 379 cases; 12 remaining failures call analysis-owned event test helpers still using the retired zero-argument iso, owner notified.

### Typed context input-owner snapshot

ContextBinding/ContextScope/ContextInputOwner/ContextTrigger now validate the scope and input-owner facts consumed by implicit tool binding. The core uses typed source-selection methods rather than an anonymous active-row/empty-trigger tuple. A callback without incoming source cannot borrow the original source for an observation, while an actual incoming source owns it even after an earlier opening item. Duplicate active journal owners and malformed identities/payloads reject. Immutable projection survives later mutation of the wire document.

Required nullable scope fields represent genuinely unbound events, source-less proposals/callbacks and pre-admission active-input absence. ContextBinding.from_session alone reads persisted wire fields. The empty message_log default is intentional: SessionService.session persists a newly created session before message admission writes its first journal, and retained legacy sessions may only declare the explicit source origin. last_payload is considered only when no active pointer is declared. A non-null pointer must name exactly one actual journal row; the later lifecycle invariant below removes the prior unmatched-pointer recovery path. A missing active-row payload means no source and cannot use the session origin for observation. Trigger has_payload captures nonempty payload presence so a callback's absent item differs from the pre-input source-only state.

Proof: 361 owned unit cases passed in 12.87 seconds and 35 actual guarded native/context/session integration cases passed in 16.42 seconds, zero skips. Twelve focused projection cases prove callback isolation, incoming ownership, immutable snapshot, first-input/legacy absence and malformed/duplicate refusal. New projection source/tests pass independent strict mypy and Ruff; remaining full session/reference serialization migration remains open.

### Complete OpenMA configuration admission

OpenMAConfiguration now models every declared baseline field, requires max_running_sessions and a configured price record for the selected model, rejects boolean/nonfinite/negative prices and unknown fields, and preserves declared int/float JSON scalars. OpenMA sync validates this closed record before any request and uses typed required identities/effort instead of required-value defaults. Required nonempty effort remains the explicitly configured provider label rather than an invented local taxonomy.

model_prices is the genuine model-ID namespace containing strict OpenMAModelPrices records, not a generic settings blob. No configuration fields are nullable or defaulted. The actual configuration loader roundtrips canonical serialized content and checks scalar types. Four malformed complete-config cases pass through actual sync admission and prove zero OpenMA requests. Sixteen configuration/sync cases pass in 2.59 seconds; new contract/tests independently pass strict mypy and Ruff. Remaining sync protocol request/observation/report records and SessionService launch/limit consumption remain active work.

### Typed OpenMA synchronization application and adapter

openma_sync is now fully annotated and independently strict checked, including every test helper in test_openma_sync. OpenMASynchronizer consumes an immutable validated OpenMASyncPlan and required OpenMAOperations protocol. HTTPX, response JSON envelopes, multipart tuples, header secrets and provider projections stay in OpenMAApi and its wire helpers. Plan preparation requires captured versions/paths, complete required configuration, both credentials, every skill's SKILL.md and every tool manifest before remote effects. No private snapshot inspection, empty credential substitute, required effort default or optional stop event remains.

Named request, observation, report, skill-file, registration and binding models replace anonymous record tuples and nested domain maps. Keyed dictionaries now represent explicit namespaces (managed skill/agent/environment identities, model-price names, indexed tool identities) containing closed typed records. SDK JSON Schema and string-valued provider metadata are actual wire namespaces. Unrelated provider metadata is retained through a dedicated typed field and explicit HTTP serialization; it cannot replace managed skill_mount/tank_skills. SecretStr protects credential representation; retained provider metadata and upload bytes are excluded from repr.

The adapter accepts wrapped/direct responses, provider model ID/reference shapes and normalized tool permission extensions. Genuine uninitialized comparison fields remain nullable and cause reconciliation, without copying desired values into observations. Addressing identity/version remains required and is checked against the requested resource. Missing environment configuration reports DIFFERS and never invents an empty networking/package policy. Skill digest absence means an observed registration has no Tank content attestation yet, and forces an upload. A fresh per-attempt transport factory gives each retry client ownership of its transport; None delegates normal transport selection to HTTPX.

Tests: 45 focused OpenMA/configuration cases pass in 4.27 seconds; source/contracts/all helper tests pass independent strict mypy and Ruff. The broader owned cohort passed 442 units in 20.78 seconds and 46 actual native/session/runtime integrations in 35.77 seconds before the final observation-only compatibility additions, which the focused tests cover. Proof includes two-pass idempotency, exact captured uploads, later disk edit isolation, required-limit/credential/manifest refusal before writes, unknown provider metadata preservation, partial provider observation reconciliation, identity binding, actual sanitized retry logging, transport close ownership and configured price/endpoint scalar preservation.

Open issue: the prior reconciler did not compare configured reasoning_effort, although it writes that field. This is tracked for a separate explicit observed/managed-provenance repair; this checkpoint does not claim that defect or the remaining session/runtime/context-reference work complete.

### Isolated published-head CI reload repair

8ea20bb is based only on published 91b717f. The control-plane reload test now supplies a complete minimal synthetic configuration with one entity for its real malformed-qualifier oracle, plus an owned empty catalog and matching SHA. Real Settings capture, strict runtime validation, generation activation/swap and rejection assertions remain; two-second waits are unchanged. The original incidental production catalog caused a 3.31-second local case. The repaired case took 0.180 seconds under the full 2,190-case unit run; every case passed with zero skips, p90 0.369 seconds and maximum 5.160 seconds. No source cache/scheduler/budget changes are included. The independent configuration-cache optimization remains a separate foundation change.

### Independent required scheduling composition

This companion starts from verified parent 57c26c39 and merges canonical d19a9bf8 plus 970cfe51; it contains no held sync or request-budget activation. Production creates the real Scheduler with the same Market/Truth, captured versions and required thresholds before constructing EventToolService, passes the required ReactionScheduling port, and removes the late optional assignment. Runtime shutdown still owns that same scheduler. Runtime.close now explicitly returns None.

Owned native/controller component fixtures inject RecordingReactionScheduling, which records the scheduling command and never fabricates a successful market capture. Actual reaction behavior is qualified by the shared operational service fixture and analysis's real-dispatch tests. A new production composition test constructs real services and verifies scheduler identity, dependencies, version/threshold facets and closure of owned clients. The new test is independently strict-clean.

Validation: 330 owned unit cases pass in 14.82 seconds; 32 actual Memory/PostgreSQL/native integration cases pass in 19.17 seconds, zero skips. Ruff/format and source-comment checks pass. Default composition mypy no longer reports the scheduler assignment; only the four separately owned Truth cache declarations remain.


### Journal-backed input ownership across readers, writers and completion

A non-null active_message_id now requires exactly one matching journal row, including when the journal is absent or empty. ContextBinding refuses stale last_payload substitution for that state. The 22 session persistence sites share put_owned_session, a narrow typed storage-boundary guard that validates the projection and persistence identity. Message submission validates before migration/posting and again at persistence after its lock-time reload; execution-derived pointer assignment is validated against the journal. Genuine no-active-input/pre-admission and explicit origin-only legacy records remain valid.

Terminal failure captures the validated binding before clearing the active pointer and selects its observation source. An active source-less callback cannot mark either the opening source or stale last source FAILED. Real acknowledged accepted-input lineage may rebind a stale pointer to its actual recorded owner; generic last_payload cannot. Admission fixtures now declare their existing in-flight journal owner instead of an unrecorded marker.

Nine focused lifecycle/persistence cases pass, including actual send revalidation under a changed lock-time read, terminal callback isolation, refusal before native posting or source mutation, guarded persistence, and actual execution-owner rebinding. Broad owned units: 338 passed in 12.96 seconds. Actual native/Memory/PostgreSQL integration cohort: 43 passed in 18.09 seconds, zero skips. The new projection/persistence modules independently pass strict mypy; full legacy SessionService/native-event function typing remains open and is not approved by this guard.

### Structural publish-card narrative alignment

The owned publish_card.schema.json and tools.json definition now require a nonblank verdict (existing 160-character bound retained) and 2–3 nonblank WHY strings (existing 220-character bound retained). This follows the already-versioned SKILL.md line 69 and applies to PARTIAL/FULL/UPDATE; no sentence counting, semantic confidence, prose heuristic or narrative repair was added. Positive scope/stale-publication transport fixtures now supply their required two declared facts rather than relying on an empty/single bullet list.

Five actual native refusal cases prove empty/whitespace verdicts, empty/single WHY and whitespace bullets never dispatch publication and are acknowledged as REFUSED. An independently strict parity test verifies both schema surfaces. Validation: 317 owned agent/context unit cases pass in 14.00 seconds, 23 actual native/Memory/PostgreSQL cases pass in 16.39 seconds, zero skips; standalone parity passes. Foundation/analysis own the paired typed narrative/service activation.
