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

The initial typing checkpoint tracked reasoning-effort drift separately; the follow-up below closes that comparison/confirmation defect. Remaining session/runtime work is still open.

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


### Required runtime request-budget and live HTTP resource composition

Runtime now requires a RequestBudgetScope and a concrete BudgetBackend. Production composition explicitly chooses PostgreSQL RequestLedger for PostgresRepository or MemoryBudgetBackend with SystemBudgetClock for the declared MemoryRepository; unsupported repository implementations reject. One per-runtime scope is passed to SourceService and ConfiguredHttpAdmission. MarketService and LivePageAccess share that same configured admission, using captured required API policy and version identity. No repository capability guessing, replay-flag bypass, unconfigured memory fallback or singleton configuration remains in Runtime.

Runtime.run binds its owned backend after gateway startup and before intake/support loops. Runtime.close releases the exact stored lease after service closure and clears its ownership first. The nullable Runtime.budget_binding represents the real prepared/unstarted or already-closed lifecycle; no optional dependency is created. The production composition test proves unstarted admission refuses, the exact shared identities and explicit memory/PostgreSQL backend selection without connecting. The existing gateway/drain lifecycle test now verifies each service sees the actual binding before startup and that admission refuses again after release.

Initial verification: 376 of 377 owned units passed in 19.03 seconds; the remaining CLI source-check fixture exposed its separately owned unconditional PostgreSQL backend selection and is coordinated with ingestion. All 52 actual native/Memory/PostgreSQL owned integration cases passed in 36.26 seconds, zero skips. The expanded composition test passes the independent strict CI configuration. Default source mypy has only the separately owned Truth condition-cache declaration on this held ancestry. The complete constructor/lifetime batch remains held for the CLI and lease companion and final combined verification.

Strict-proof correction: the earlier isolated ownership-test invocation inherited project overrides. Independent quality verification confirms context_binding.py and session_ownership.py, but test_session_input_ownership.py still calls the open untyped SessionService/ModelContext APIs and is excluded from strict enforcement until those actual methods migrate. Its behavioral lifecycle proof remains valid; no wrapper, ignore or cast was added to mask that work.

### Confirmed OpenMA model binding and explicit skill ownership

The pinned managed-agent domain admits low/medium/high/xhigh/max effort; the active managed API exposes normalized model effort and retrieves an exact immutable version through GET /v1/agents/{id}?version=N. Configuration and desired requests now use that exact taxonomy. Reconciliation compares observed effort; after publication, the adapter reads the returned version explicitly and constructs AgentUpdateReceipt only from the observed identity/version/model. Missing, contradictory, unsupported or unreadable confirmation raises OpenMAContractViolation outside the retry category, so one unconfirmed write never becomes an automatic publish loop. Configured intent is never used as observed confirmation. Ingestion's CLI monitor companion is required for activation so a permanent failure drains/stops the owned runtime.

The former positional skill-role inference is removed, not justified by nullable digest. Existing resource mutation requires explicit validated Tank role metadata. Metadata-free nonempty references are ambiguous and refuse before any publication; empty sets can create fresh resources. Duplicate resource identities/role bindings refuse. Unrelated declared references survive owned updates, including the actual anthropic/custom discriminated variants; kind participates in identity and ordering. Unknown/malformed type tags are rejected rather than converted into custom references. A missing digest is only an explicit declared ownership binding with unknown content digest; it never establishes ownership itself.

The actual HTTP/update regressions cover changed effort -> confirmed new version, unchanged effort -> no new write, exact read-back despite a concurrently changed latest version, three unconfirmed read-back modes -> one publication then permanent refusal, reordered/missing/extra/duplicate metadata-free references -> zero writes, declared-role routing despite reference reordering, unrelated reference/resource preservation, built-in anthropic references with distinct/same-text IDs, and unknown/malformed tags. 63 focused configuration/sync cases pass in 5.21 seconds with zero skips; source/contracts/all sync test helpers independently pass strict mypy, Ruff and source-comment policy. No pinned/vendor source was changed for this evidence.

### Full managed-state read-back confirmation

AgentUpdateReceipt now retains the complete exact-version ManagedAgentObservation and validates identity/version/model consistency. The synchronizer verifies the same declared tools, skill references, system, name and managed metadata used by _agent_current before reporting UPDATED. A duplicate tool identity is a permanent contract violation rather than a retryable read-back error. Correct model/ID/version alone cannot certify a dropped owned field. Five real adapter/update regressions omit or contradict tools, skills, system, tank_skills or skill_mount and prove one publication then permanent refusal, without retry churn. The full 68-case sync/configuration suite passes in 5.42 seconds; independent strict source/contracts/helpers and Ruff/format checks pass. Activation remains paired with the owned CLI monitor.

### Harness and retained namespace confirmation

The final desired-definition field sweep now observes _oma.harness and compares the declared pi harness. Exact-version confirmation also requires every retained provider metadata key/value sent in the update to survive, while allowing new provider entries. Four adapter/update regressions remove/change the harness or a retained metadata entry and prove a single publish followed by permanent refusal. A separate additive-extension case succeeds. All 73 sync/configuration cases pass in 5.53 seconds; independent strict source/contracts/helpers and Ruff/format pass. This completes normalized field-by-field ManagedAgentDefinition confirmation rather than raw response-object equality.


### Combined publication-confirmation and serve-drain proof

The held runtime resource branch now merges the exact OpenMA confirmation cohort with ingestion's monitored serve lifetime. A real keep_in_sync adapter response with a missing published harness passes through run_with_synchronization: exactly one agent publication occurs, the permanent OpenMAContractViolation sets stop, the owned runtime finishes its drain, and only then does the failure propagate. This combines the actual confirmation and lifetime paths rather than testing the exception class alone. All 85 sync/configuration/CLI cases pass in 6.17 seconds; the three sync source/contracts/test modules also pass the independent strict CI configuration without inherited per-module overrides.


### Final owned resource-cohort qualification

The exact ingestion f2106cb3 follow-up is merged. Runtime production composition reuses the single tank.request_budget.build_budget_backend factory with explicit owner tank-core, rather than duplicating backend selection. RequestBudgetScope now refuses replacement while bound; its owner regression proves the first lease remains active and stale cleanup cannot release a later replacement. The CLI source-check fixture declares the required budgets port and its existing network opt-in assertions remain intact.

On the paired factory/lease/sync/runtime head, all 462 owned runtime/session/context/OpenMA/configuration/CLI unit cases pass in 16.10 seconds and all 52 actual native/Memory/PostgreSQL owned integration cases pass in 36.59 seconds, zero skips. The expanded production composition test passes independent strict CI configuration; sync source/contracts/test helpers independently pass strict. Ruff formatting/checks and source-comment policy pass. External analysis constructor fixtures and quality's actual E2E companion remain separately coordinated before parent activation; full SessionService/runtime job-domain migration remains open.
### Complete model-context reference and native-boundary typing

Every function in agents/context.py now has precise parameter/return types, independently checked without inherited project overrides. The context core holds an immutable ContextBinding and typed registry/reference namespaces, rather than retaining a mutable session document. Its repository port requires only get and iter_all. DocumentReference, EventReference, ClaimReference, ConditionReference and ProposalReference are closed readable projections; EventOwnership, EventMergeState and ProposalAvailability validate the exact authorization facts consumed. Missing event ownership/lifecycle refuses instead of inheriting the current event. Legacy description-field guidance is a named LegacyReference rather than an anonymous tuple.

Optional descriptive facts represent actual unknown/undeclared partial or historical record information, not defaults for active admission records. An absent claim evidence array remains unknown resolved-source information; explicit false verification survives. Excerpts require actual string text and cannot invent title/time. Existing complete watch expressions use the canonical WatchExpression contract. One synthetic watch audit fixture now explicitly declares its EVENT window and one-window persistence; captured evidence is unchanged. ContextMessageProjection names the complete native context frame and serializes only applicable event/proposal members while preserving required null item and scoped missing proposal.

Generic maps remain only in the explicitly listed native JSON/privacy/JSON Schema/document-store adapter sites and true typed namespaces. Known identity scalars/lists reject invalid types and empty identities, while a declared empty list remains empty. Descriptive reference memoization stores closed models or a real missing row. This does not claim that the remaining SessionService or runtime job-domain dictionaries are migrated.

Five new behavior cases exercise actual repository-backed reference caching, preserved unknown facts and false verification, missing event-owner refusal, empty-list/scalar validation, and real text-only references. Independent strict checks pass context.py, context_records.py and test_context_record_contracts.py. All 348 owned agent/context/session units pass in 16.38 seconds. Extended actual native/Memory/PostgreSQL/replay/reference qualification has 60 passing cases and four separately owned fixture failures: two replay policy fixtures omit required Thresholds, and two synthetic PARTIAL publication calls still supply empty WHY. The four fixture-only gaps are closed by analysis's clean b4abe687/c25cd2a4 companions, qualified in 42481a26; no schema or frozen evidence was changed and no context validation case failed. The owned native/session subset is 43 passing cases. Ruff/format and comment policy pass.

The refreshed complete owned Python inventory has 28 modules and 58 functions still missing some annotations, concentrated in the open SessionService/runtime work. Context is no longer annotation-incomplete; the exact retained-site ledger lists every necessary projection/wire/namespace/absence group by symbol.


### One authoritative source rule for callbacks

The last callback-origin fallback is removed. record_event and implicit get_item now use the same validated input source selection as add_observation and terminal completion. A real active source-less callback cannot borrow the opening item or stale last_payload. The duplicate incoming_source method, which encoded the wrong fallback, is deleted. Explicit pre-input/origin-only records remain supported by ContextBinding's separate genuine-absence branch; a model can still read any existing source by explicitly naming its item ID.

Two actual native tool lifecycle regressions exercise record_event and get_item under a source-less callback: both are recorded REFUSED and acknowledged without dispatch; an explicit get_item-by-ID then succeeds through the same service, and the source document remains unchanged. This follows the published get_item schema, whose omitted ID means incoming source. All 350 owned context/agent/session unit cases pass in 17.19 seconds and all 43 actual native/Memory/PostgreSQL cases pass in 18.71 seconds, zero skips. Five context source/test targets pass independent strict checks; the existing broader SessionService test module still awaits its actual source-method migration. No deadline or schema was loosened.

## Parameterless decision composition companion

The runtime factory and the slow-denial lifecycle fixture construct the pure
DecisionEngine without Settings. The fixture declares only the narrow open-call
facts consumed by DecisionCallContext, together with its synthetic issuer mapping
and explicitly unavailable market observations. Its exposure remains null;
publication uses the canonical nullable ordering repair rather than inventing
a numerical estimate. Existing publication, denial, EXIT, delivery and archival
assertions are unchanged. This companion adds no production fallback or dictionary
exception and does not claim completion of SessionService or native transport.

The combined owner prefix passes 388 unit/ranking cases and 78 actual integration
cases with zero skips. Ruff and formatting pass for both changed Python files;
configured mypy passes runtime_composition.py. Whole legacy test_agents.py and
SessionService are not signed off as independent strict targets here. The broader
card/decision cohort remains subject to its owning workstream's final qualification.

### Typed session admission and authoritative scoped references

Pure admission models/constants/AdmissionDeferred now live in contracts/session_admission.py. Runtime imports that inward contract directly. SessionAdmission consumes only closed policy/target/order/slot records and its narrow repository protocol; SessionAdmissionRepository alone decodes selected persisted metadata and applies sparse updates. Real session and reserved-job slots are distinct named models, replacing synthetic job-prefixed session IDs and anonymous slot/order tuples. Reserved-slot counting does not validate unused command payloads; waiting/input candidates must declare a real routing identity. Session metadata admits only the actual local status/runtime tags; an unsupported non-null state refuses before another job changes.

The hardcoded two-session capacity fallback is deleted. Runtime.validate and admission composition require a declared positive integer before gateway/process effects, proven through real Runtime.prepare. Native-only identity configuration reads do not consume admission policy, so recorded/retained configurations are neither padded nor assigned a fabricated modern limit. Existing running-session bypass, priority/first-enqueue order, waiting wake-up, expiry and attempt ownership remain. Explicit session IDs are authoritative and cannot fall through to a different event's busy session. Order comparisons use aware timestamps, fixing the actual offset-representation fairness bug.

SCOUT_REVIEW crosses the worker wire adapter as a strict ScoutReviewJob and the native use case returns a validated ScoutSessionReference. Stored proposal identity must match before native work; returned scope must match the requested proposal. The queue serializes only the session/proposal reference. Both owned consumers use attributes; no Mapping-compatible model was added. The native message serialization excludes unset replay/dependency fields, preserving the existing payload/idempotency hash. Legacy full send_message/session-state internals remain open; the reference does not claim their migration.

Canonical foundation cbad735 timestamp_owned_fields is ancestry. Wake, admission/delivery/dependency deferral, controlled interruption and expiry writes replace every owned clock alias/reason/bucket facet while retaining unrelated captured data. First waiting time inherits the real creation timestamp and its precision/reason, rather than copying only its scalar or guessing now. Actual wake regressions cover exact/alias and bucketed input, standard-reader compatibility, and canonical preservation of unrelated 1/1.0/true values. A real worker admission deferral proves no native dispatch, zero consumed attempts and inherited creation precision.

Two dynamic settings fixtures now implement a precise AgentFixtureSettings port with named required fields, complete Thresholds, explicit synthetic capacity, strict configuration-name lookup and explicit credential/decoded-URL-password coverage. No reflection or arbitrary override namespace remains in those factories.

Qualified proof: 398 owned units pass in 20.39 seconds; all 80 actual native/Memory/PostgreSQL/routing/REST/replay/reference cases pass in 37.85 seconds, zero skips. Unit fixture-inclusive JUnit p90 is 0.551 seconds and maximum 4.323 seconds. Seven independent strict targets pass: contracts/session_admission.py, agents/admission.py, agents/admission_repository.py, agents/session_contracts.py, runtime_contracts.py, tests/test_session_admission_contracts.py and tests/agent_configuration_fixtures.py. No legacy SessionService suppression was added. Current inventory has 31 owned modules and 49 annotation-incomplete functions; full session/journal/native-event and runtime job-domain migration remains open.


### Admission combined with the qualified runtime resource lifetime

The ready admission branch normally merges exact parent 679c694c at fa605387. The sole source conflict was the Runtime import block; budget backend/scope/binding types and the new scout/clock codec imports are both retained. Required per-runtime backend selection, bind-before-intake and exact lease release coexist with capacity preparation, typed admission and owned clock updates. All seven independent strict targets remain green. The affected combined runtime/admission/scout cohort passes 78 units in 6.62 seconds and 12 actual Memory/PostgreSQL/native cases in 36.07 seconds, zero skips; no source behavior was dropped to resolve the merge.

## Native executable-input acknowledgment increment

agents.native_inputs models the Tank-owned user-message and custom-tool-result
request shapes, a one-event batch, the pinned sent-event response variants, and
named accepted/unreported/empty observations. SessionService.post_input is the
shared adapter for initial durable messages, tool-result continuations and host
retries. A reported event must have its actual tag and identity; tool results must
name the posted call. Malformed, explicit-null or multiple acknowledgments fail
before sent_at, returned_at, retry ownership or native-input lineage acceptance.

The pinned provider schema permits absent data, an empty response list, an empty
user content list and nullable/omitted processing metadata. These actual protocol
cases remain explicit; no domain estimate or accepted identity is manufactured.
Synthetic HTTP endpoints now echo validated posted inputs instead of returning
id-only objects that the actual sent-event schema does not produce. Immutable
retained payloads were not edited. Real initial/retry write-boundary regressions
also prove malformed provider values do not appear in rendered validation errors.

Independent strict checking passes native_inputs.py, test_native_input_contracts.py
and native_input_fixtures.py. All 409 owned unit cases pass. The actual owner
integration scan passed 77 cases and identified one remaining Scout synthetic
id-only endpoint; after that fixture was corrected, its actual PostgreSQL case
passed independently. Default mypy accepts sessions.py; whole SessionService and
the legacy consumer test modules are not independent strict-complete. Discovery,
creation, native history and complete durable session/journal typing remain open.

## Native discovery, creation and input acknowledgment qualification

SessionService.resolve now consumes NativeSessionResolution. The required
NativeSessionEndpoint port, NativeSessionDiscovery coordinator and closed
deployment/create/page/observation models separate HTTP wire handling from service
facts. Native identity config validates the consumed deployment fields; admission
capacity remains a Runtime/admission responsibility. Cached historical sessions
retain genuinely missing replay metadata without reconstruction. Live lookup skips
known replay sessions, and explicit recorded bindings must match their run,
evidence digest, source session and cutoff before reuse. Opening-source lookup
cannot borrow an undeclared replay lineage for live input; an explicit cutoff
stays bound even without an evidence digest. These are caller identity labels,
not a grant of recording or network authority.

Discovery validates consumed page and native prompt facts, scans actual cursors,
and refuses ambiguous identities/cursors. A recovered or newly created resource
must confirm its requested agent/environment and every managed identity/replay
metadata value before local session storage or initial input. Provider metadata
extensions remain honest observations. Actual prompt JSON stays inside the adapter
for canonical fingerprinting, preserving extension values and numeric types;
services receive AgentProvenance rather than a remote dictionary. A typed identity
marker preserves the prior live/replay canonical wire hashes exactly.

Configured effort is recorded as configured_effort. effort comes only from the
optional native model.effort observation, with explicit absence reason/basis.
Malformed reported effort or extension shapes refuse; configured intent cannot
become observed confirmation. Real write-boundary tests prove wrong agent,
environment, identity and replay readbacks leave local sessions/tool calls absent
and the source record unchanged. Omitted input acknowledgments remain explicit,
while malformed reports cannot commit acceptance or retry lineage.

Final owner proof: 472 unit cases passed in 26.84 seconds, p90 0.494 seconds,
maximum 4.898 seconds; all 78 actual integrations passed in 38.91 seconds, with
zero skips. Six new native model/test/helper targets pass independent strict mypy
with both src and tests on MYPYPATH and the official Pydantic plugin. Ruff,
formatting and the source-comment policy pass. Configured whole-source mypy has
one existing analysis-owned truth cache annotation error; native/session consumers
default-check clean. The exact logs are agents-native-ready2-{unit,integration}
and metrics JSON in the owned worktree.

This is a complete bounded native client increment, not a whole SessionService
completion. Private session/journal state still uses repository JSON, and native
GET/readback, history/execution lifecycle, usage, tool orchestration and stream
state still need their full typed domain migration. The refreshed inventory
records every remaining unannotated owned function; maps/defaults there remain
implementation work rather than approved exceptions. The 42 assigned OpenMA
vendor programs remain a separate implementation backlog.

## Fresh creation-role selector companion

The qualified native checkpoint remains 017e8402. Its separate companion adds
NativeSessionResolution.fresh_context_matches_role: fresh scouts require their
proposal and cannot select analyst event/item context; fresh analysts require
their event/item context and cannot select a scout proposal. These are the actual
ScoutMessage/SourceMessage producer shapes. Mixed fresh selectors otherwise
collide with the resolver's identity priority or invert the proposal-based context.

An explicit session_id is a continuation selector. It does not require invented
fresh-role facts, and the existing stored analyst/scout context remains unchanged
for either creation-role hint. An explicit missing ID still refuses before native
creation. Controls cover all valid fresh shapes, all absent/mixed role contexts,
both actual stored roles, opaque numeric/boolean field preservation, and missing
continuation IDs.

All 496 owner unit cases pass in 26.60 seconds (p90 0.526,
maximum 4.870); all 78 actual integrations pass in 38.68
seconds, zero skips. Native source/helper/contracts retain their independent
strict gate, and Ruff/format/source-comment checks pass. This adds no default,
wire dictionary, endpoint signature or broader session-lifecycle policy.

## Native readback and tool confirmation candidate

The native input checkpoint and role companion remain immutable. This separate
candidate moves direct session GET decoding into NativeSessionReader: exact
resource identity, required lifecycle/archive observations, and a distinct
NativeSessionMissing outcome for HTTP 404. Missing resources cannot become a
fabricated terminated session. The question continuation path records the missing
observation and answers NO_SESSION without posting input.

ManagedToolManifest now owns the pure tool-definition boundary shared by sync
and session migration. The session service no longer imports the OpenMA sync
application orchestrator. Resolved native tools preserve the actual custom,
builtin and MCP variants and require their documented nested policy/configuration
facts. NativePromptObservation hashes the actual resolved session snapshot;
configured labels and effort remain separate from observed confirmation.

SessionPromptState and PromptRecoveryRecord project only provenance recovery
facts. Recovery reads the exact native session, refuses absent/incomplete resolved
evidence, and checks that the stored descriptor still matches before committing.
It updates the caller's view only after the durable write. Legacy unqualified
prompt_version labels remain labels and never become resolved hashes.

SessionToolBoundaryState and LegacyToolContext project the actual migration
owner, role, context versions, journal identities and tool hash. sync_tools reads
the native session before an update and confirms the requested tools afterwards.
A contradictory readback cannot record success. If native acceptance preceded a
lost HTTP response, retry confirms the already-applied toolset without publishing
again. A changed local owner projection refuses the final write. Read-only replay,
busy boundaries and unchanged confirmed hashes are explicit named outcomes.
Private journal storage and the remaining SessionService lifecycle remain open.

The focused 137-case readback/scalar/manifest/sync cohort passed in 47.78 seconds.
The subsequent ownership-race and migration selection passed all 14 cases in
8.39 seconds. New projections and the HTTP confirmation tests pass independent
strict mypy with both source and test roots; sessions.py passes configured mypy.
Final owner proof: all 613 unit cases pass in 44.29 seconds; all 78 actual
integrations pass in 38.92 seconds, with zero skips. Unit p90 is
0.62 seconds and maximum 4.87 seconds. The exact
logs/XML/metrics use agents-native-readback-final-unit and
agents-native-readback-owned-integration in the isolated worktree. Cross-owner
caller qualification remains pending. This is not a whole-session completion.

Strict fixed scalar fields now use canonical ZeroInt, TrueFlag and FalseFlag:
JevScoreRecord.web_searches, FailedToolResponse.ok,
NativeToolResult.is_error, RevisedReactionJob.automatic_revision, and
NativeSessionMissing.missing. Forty real DTO/classifier cases prove boolean,
integer and integral-float aliases cannot silently satisfy the wrong wire type.

The lifecycle-only NativeSessionReference, NativeSessionState,
NativeSessionMissing and NativeSessionLookup contracts now live in
contracts/native_readback.py. The HTTP adapter reexports the same objects for
existing callers. Presentation and followup selectors can validate missing
observations without importing native transport code. The move changes no
field, default or lifecycle behavior; 19 reader/tool-confirmation cases pass
in 2.15 seconds, and the inward contract/adapter/test independent strict gate
passes.

## Durable outbound ownership work in progress

The separate outbound branch begins with JournalInputLineage,
ToolInputLineage and NativeInputOwner. Current input identity and its actual
recorded retry count can prove ownership; explicit native_input_attempts entries
prove older attempts. A superseded ID without an ordinal is unknown historical
lineage, not an array-position retry count. A tool callback without a recorded
retry count cannot acquire attempt zero. Conflicting journal owners refuse.

A known native execution can still rebind an obsolete active pointer to its
actual acknowledged journal owner. The owner resolver therefore validates the
journal identities and accepted lineage before the process_event write guard
validates the newly established pointer. It does not use an inconsistent active
pointer to infer a source.

Forty ownership/native-retry/execution-barrier cases pass in 4.97 seconds,
including the actual acknowledged-execution rebind. The new pure model and
service-boundary tests pass independent strict mypy. Pending/accepted/retry
intent phase models and durable write/recovery migration remain work in
progress; this checkpoint is not a release of the outbound slice.

PendingOutboundInput and AcceptedOutboundInput now drive initial posting and
outbound recovery. An accepted unreported or empty protocol receipt is retained
as that exact observation; it is never fabricated into an event identity and
never reposted merely because its ID is absent. Historical accepted rows whose
receipt was not retained use LegacyUnreportedAcceptance with an explicit reason.
A later retry ID cannot be substituted for an unknown original receipt.

Posting compares the durable initial definition before committing acceptance.
Concurrent confirmed receipts retain stronger identity evidence; an actual
reported receipt can refine an unreported receipt only for the same unchanged
initial input. Recovery by a new controller reuses the original durable body and
idempotency key after a lost response. Invalid scalar aliases, null retained
receipt, changed body and conflicting identities refuse before posting or
before the acceptance write as appropriate. Source observation time remains
explicitly unknown when unreported, distinct from the actual local acceptance
clock.

Seventy focused phase/lineage/native-retry/audit/barrier cases pass in 5.65
seconds. Independent strict models/test checks pass. The generic private journal
storage adapter remains a boundary under migration; these models do not pretend
to be a full SessionService aggregate. The retry writer still requires its
separate pending/reported/unknown-budget conversion before outbound release.

Retry phase models now retain the declared budget, actual input definition,
verified owner and real failure identity. Missing counts, missing current owner
or missing original body refuse before posting without fabricating terminal
native events. Declared legacy zero remains valid; a fresh journal input uses
its own count even when unrelated origin history lacks that fact.

The diagnostic cutoff requirement was removed after the complete consumer
inventory proved the fields were write-only. No fixture was padded to satisfy
it. The 642-case diagnostic (629 passes and 13 cutoff refusals) remains preserved
in agents-outbound-diagnostic-unit.log/XML. Existing captured cutoff/index
metadata remains unchanged. Operational late callback fencing continues to use
message IDs and actual retry attempt identities. Optional replay ordinal
reporting and the old tool ordering counter are separate open tool-ledger work;
no discovery API or new sequence epoch was introduced for retry.

Seventy phase/lineage/retry cases initially passed; the subsequent affected
workflow selection passes all 127 cases after retiring the unused dependency.
The actual missing-budget/owner tests prove zero HTTP, unchanged retained rows
and no fabricated terminal failure. A new declared input is not blocked by
unknown origin history. The behavior tests that call process_event remain
outside independent strict enforcement until that legacy boundary is migrated;
no suppression or protocol facade hides this open work.

## Native archive-clock companion

NativeSessionState.actual_archive_clock uses the shared aware timestamp parser
for each non-null archived_at and returns the original wire string. None retains
the provider's explicit unarchived fact. Empty, malformed, naive and calendar-only
strings cannot be persisted or treated as either live or archived observations.
Z and explicit-offset clocks retain their exact text. The actual continuation
reader regression proves invalid input refuses before local state mutation or
native input posting. All 27 reader/tool-boundary cases pass in 4.38 seconds;
the inward model, HTTP adapter and test pass independent strict mypy.

The unknown-budget behavior proof now covers actual process_event dispatch for
a journal without a count, an origin-only retained row without a count, and stale
journal history without a current owner. All three cases pass in 2.38 seconds:
zero HTTP calls, unchanged retained row, and no fabricated terminal failure.
The behavior test is not independently strict-complete while process_event
remains an untyped legacy boundary; no test suppression or protocol facade was
introduced to hide that open work.

The complete consumer audit corrects the earlier ordinal-fence assumption:
retry_call_ordinal_cutoffs and _host_retry_call_ordinal_cutoff are write-only.
native_turn_call_index is reported as optional replay checkpoint metadata.
Operational late callback supersession compares actual message and retry
attempt identities. The WIP cutoff requirement therefore remains under review
and must not be used to justify padding retained fixture metadata.


Outbound receiver and recovery guards now verify the stored session identity
before native effects, and refuse unmatched active pointers before reposting an
otherwise valid pending intent. Native input resource IDs are required and
encoded as one path segment. Acceptance cannot assign a prior native input ID
to a different host retry attempt. Reported receipts preserve or refine existing
evidence without discarding known IDs when a concurrent response omits them.
Private initial/retry bodies are hidden from model representations; the actual
wire is serialized through the typed JSON codec only at HTTP/storage boundaries.

Owner proof on source/test checkpoint c4e119f9: all 649 unit cases pass in
42.27 seconds, p90 0.534 seconds and maximum 4.459 seconds; all 78 actual
integrations pass in 38.35 seconds, zero skips. Logs/XML/metrics are retained
as agents-outbound-qualified-* in the isolated worktree. Four new/changed
source targets and two actual HTTP/lineage test targets pass independent strict
mypy with the official plugin and source/test roots. The retry behavior tests
that call the legacy process_event boundary remain explicitly open for that
gate. Configured mypy of all 143 source files has only the existing
analysis/truth.py:5890 condition-cache annotation error on this ancestry; no
owned source error remains. Ruff/format/source-comment checks pass.

The inventory records 38 owned source files with 34 SessionService/free-helper
functions and four Runtime functions still annotation-incomplete. The full
journal/event stream, tool orchestration, usage, followup presentation layering,
runtime worker domain and 42 assigned vendor programs remain open. This bounded
migration does not approve their generic maps or remaining fallbacks.

## Native chronology transport companion in progress

The separate transport branch preserves the qualified outbound checkpoint.
NativeRetryRefusal and NativeRefusalKey identify an actually observed native
event/execution within its session. A host_retry_refusals namespace records the
named host refusal, actual controller observation clock and required controller
version facet. Historical producer metadata and retry budgets are not relabeled.
The receipt and any genuinely reported cursor commit in one session-document
write before memory dedup advances; committed-write acknowledgment loss is
distinguished from a write that never committed.

Page and stream callers now handle that permanent host outcome without inventing
native termination or starving a later independently owned frame. Direct
process_event still surfaces its named refusal. Existing durable refusal lookup
skips only the exact recorded identity across restart; a different execution
with the same event ID remains independently processable.

Two source-backed HTTP paging contracts are explicit: managed data/next_page
and native Node data/has_more with after_seq derived only from reported event
sequences. Mixed, missing, repeated and non-progressing contracts refuse rather
than guessing. Both source-backed SSE forms are flat events: cursor-free managed
events and Node events with a reported seq, optionally bound to the same SSE id.
The undocumented seq/event wrapper fallback is removed. Actual comment/ping
heartbeats remain non-events; no event ID or cursor comes from list position,
clock or inherited empty content.

Actual native HTTP/SSE bytes opt into canonical decode_unique_json before
ownership projection. Model serialization roundtrips and opaque tool/file text
remain unparsed. First proof: 22 transport cases pass in 1.98 seconds, two actual
PostgreSQL persistence/restart cases pass in 2.62 seconds, and the model/adapter
plus behavior test pass independent strict mypy. The earlier 93 existing caller
cases pass after authoring the already-declared Node seq in its SSE fixture and
an explicit terminal managed cursor for the complete synthetic history page.
Full owner and peer qualification remains pending.

The first full 671-case owner diagnostic had 634 passes and 37 failures, all
exactly the missing paging-contract error in five authored HTTP fixture families.
Each handler returns its complete available history snapshot; those responses
now declare terminal next_page=None. Frozen payloads, budgets, deadlines and
behavior assertions were not changed. The 75 affected transport/workflow cases
pass in 19.11 seconds after that correction. Full combined qualification is
still pending.

Final transport owner proof on source/test checkpoint 5b09c8ec: all 671 unit
cases pass in 37.44 seconds, p90 0.484 seconds and maximum 4.382
seconds; all 80 actual integrations pass in 38.20 seconds, zero skips. Logs,
XML and metrics use agents-native-transport-final-* in the isolated worktree.
Eight source/test targets pass the independent strict gate with both source
and test roots and the official Pydantic plugin. Ruff/format/source-comment
checks pass. Configured mypy checks 145 source files with only the existing
analysis/truth.py:5890 condition-cache annotation error on this ancestry; no
owned source error remains. Peer E2E/followup fixture qualification is pending.

The transport adapter's dictionaries are actual native HTTP/SSE wire objects,
not a permissive application record model. Full native process_event/tool/
history execution domain migration remains open. The refreshed inventory lists
30 SessionService/free-helper functions and four Runtime functions still
annotation-incomplete, plus the separate assigned vendor backlog. The Telegram
eligibility/presentation policy dependency is still explicitly open.

## Foundry Decision-1 upstream integration, precommit checkpoint

The upstream replacement of Jev/OpenCode is implemented through the existing
extracted layers: agents.decision admits the provider protocol,
classification.DecisionClassifier owns immutable score identities/accounting,
routing projects typed source/event facts, and Pipeline orchestrates typed
repositories and progress. The Jev runtime adapter is deleted; no compatibility
alias, provider switch or raw-record classifier facade remains. The configured
deployment, returned microsoft-decision-1 identity and declared
microsoft-decision-1:1 configuration are separate facts. The response does not
attest a version, and model_pin_evidence says so.

Settings owns the declared endpoint/deployment defaults from upstream main.
The adapter requires an explicit validated binding and never supplies a missing
provider, credential or deployment. Binding admission requires an HTTPS Azure
resource origin without credentials, query, fragment or nonstandard port.
Live classification requires a nonblank key before HTTP, sends only api-key,
and refuses redirects. Exact-cache and offline replay may operate without a
credential; a missing replay score cannot invoke Foundry.

DecisionItem optional author/title/text/quoted fields and DecisionState.event
represent actual absent or duplicate-cleaned projection facts. DecisionEvent
optional stage/entities/tokens retain existing event projection semantics.
DecisionQuestion.criteria is genuinely optional in the verified provider
contract. DecisionUsage token counts/cost and DecisionResponse id/provider
remain genuinely optional provider reports. Omitted monetary cost is unknown
with an explicit reason; reported zero remains known. Question/answer
dictionaries are actual label-keyed protocol namespaces, serialized at the
request, response and fingerprint boundaries; persisted scores use the closed
DecisionScoreRecord. Raw responses pass unique-key admission before strict
Pydantic validation, preventing duplicate ownership/answer facts from collapsing.

Cache identity includes resource, deployment, provider, semantic declaration,
prompt version and exact state/question fingerprints. Cache consumption
crosschecks the corresponding closed record. Historical Jev rows remain
untouched and cannot answer Decision-1 requests.

Precommit verification: independent strict mypy passes twelve exact owned
source/test targets with both real src/tests roots and the official Pydantic
plugin. Ruff/format and staged whitespace checks pass. Nine directly invoked
offline async smoke cases pass, including provider HTTP/cache, reported cost,
raw duplicate refusal, old-provider cache separation with known zero cost,
optional metadata, redirects and missing credentials. The smoke script's
printed count eleven was a counting typo; nine cases actually ran. Formal
pytest remains pending resolution of the other owners' active merge conflicts.
The complete SessionService/Runtime and assigned vendor backlog remains open.

Formal merged-tree owner selection is now green: all 155 classifier, pipeline,
provenance/scalar and agent cases pass in 6.56 seconds, zero skips, using the
shared pinned runner and -n auto. The first selection had one outdated
authored routing-fixture assertion for the former OpenCode path; it now checks
/providers/microsoft/v1/systemone. All original routing/model/effect assertions
remain. Logs are agents-foundry-merge-unit.log (diagnostic) and
agents-foundry-merge-unit-final.log (terminal green) under shared evidence.
No live Foundry request occurred. Whole merged-tree and actual PostgreSQL/E2E
qualification remain parent/peer gates.

## Exhausted retry phase correction on clean main 916300d1

The complete parent matrix exposed two genuine overvalidation failures:
known retry_count=1 terminal paths loaded a historical body containing only its
actual retained type, but KnownRetryState demanded content even though that
phase never posts the body. The original event-tools completion/native-failure
race and replay terminal-failure fixtures remain unchanged.

The computed retry projection now has distinct ReadyRetryState and
ExhaustedRetryState models. Ready owns integer zero, its strict original
NativeUserMessage when retained, and its genuinely nullable prior reported
input identity. Missing ready body still produces the existing named refusal
before HTTP; malformed declared ready bodies remain invalid. Exhausted owns
only the actual journal/origin identity and positive strict integer count.
It has no body/prior-input fields to fabricate or validate. Unknown count and
owner remain the existing explicit unknown state. The session consumer matches
the ready phase before posting; all positive exhausted counts perform the
existing terminal bookkeeping. Captured bodies/prior IDs are preserved without
reinterpretation, and original retry idempotency keys/cutoff policy are intact.

Verification: 91 retry/lineage/outbound/transport unit cases and both original
unmodified failing cases pass in 3.35 seconds, zero skips. This includes actual
terminal writes with absent/partial/opaque retained bodies, unchanged completed
siblings, no native HTTP, strict ready-body/count refusals and existing lost
response recovery. Independent strict mypy passes native_retry.py and the new
pure tests/test_native_retry_phases.py with both source/test roots. Ruff,
formatting, comment policy and whitespace checks pass. Configured mypy checks
154 source files and reports only the existing analysis/truth.py:5890
_condition_cache annotation error on this exact ancestry; no owned source
error was reported. Evidence is agents-exhausted-retry-unit.log under shared
verification evidence.

The actual SessionService lifecycle tests run normally; the whole
process_event method and tests calling its still-untyped API are not claimed
as independently strict-complete. Whole Session/Runtime typing remains open.
The separate followup-policy branch is preserved at dd9d1f9a and is not an
ancestor of this bounded repair. The distinct UnprovedInputOwnership versus
durable retry-refusal chronology audit is queued; this correction does not
claim to resolve that unproven case.

## Local Context3 and required followup reporting cohort

Server development is frozen. This cohort uses the managed local agents worktree,
normal canonical main 4d955ee0 and peer reporter lineage 664ab499/65740ba7/6397590d.
The required-port composition is 87bac743; source checkpoint 43a2c7af contains
that composition and the current-main peer fixture. No source was copied from a
peer worktree and no inactive policy entered the parent main merge.

SessionService now consumes pure inward followup eligibility and a required
FollowupRefusalSink. Its target requires both actual question and session IDs;
repository refusal formatting/persistence stays outside the core. The writer
checks both identities inside the existing question lock/transaction and keeps
first-answer/idempotent delivery behavior. Current/retained MISSING remains
absence, not termination; known termination wins. Historical unknown lifecycle
facts remain unknown and can only permit a native preflight, not input by
themselves. Known retention with missing duration has an explicit unreported
period variant. Concurrent termination after a 404 lookup, failed refusal
persistence, foreign stored session identity and cross-session question binding
have actual behavior oracles. The core imports answer persistence from inward
question_answers; message/file formatting still imports Telegram helpers and
is separate remaining layer work.

Context3 preserves the incoming preliminary-card and timed-review instructions.
Closed timed/metric fired-message variants preserve actual saved event/question/
schedule and origin-session routing, without a fake token or metric predicate.
Both require FIRED phase. The optional lifecycle-clock correction is consumed
from its owner; no dummy firing/last-evaluation/cancellation times were authored.
Condition-reference projection supports the actual timed expression and existing
AGENT role.

Archive selection uses strict card/delivery projections only in the phase that
consumes them. CLOSED/MERGED do not demand unused denial metadata. INVALIDATED
requires an actual aware finalization clock, required kind/positive revision,
latest ordinary UPDATE or preliminary-only terminal correction and matching
SENT delivery. No missing kind/revision/correction becomes ordinary/zero/false.
The ordinary chain takes precedence over later preliminary cards.

The actual tool validator implements the needed oneOf/const/not/allOf/if/then/
else subset. It preserves numeric/boolean distinctions, object constraints in
typeless branch schemas, schema-vs-instance failures and static no-input-echo
errors. The existing single-WHY ordinary-card oracle still refuses before
actual dispatch; preliminary one-WHY has a separate positive control. The
watch manifest matches its three exclusive action schemas; validation does not
insert defaults. The stated expiry72 default comes from the written policy.

Local terminal proofs, with every log/XML outside the tested worktree:

- 9e5542ba focused 370 cases: 19.14 s, zero skips, p90 0.193 s, max 4.608 s.
- 43a2c7af owned 705 units: 41.47 s, zero skips, p90 0.557 s, max 5.114 s.
- 43a2c7af selected actual native PostgreSQL cases: 13 passed in 6.82 s.
- 43a2c7af full collected owned Linux integration selection: 130 passed in
  40.26 s, zero skips, including actual pinned Node codec/pi gateway and service
  lifecycle cases. The exact node-gateway lock matched the immutable tool image.
- Independent strict 14 source/test targets and scoped Ruff pass. Configured
  whole-source mypy on the tested prefix reports only the owner watch nullable
  loop variable and existing analysis truth-cache annotation; no owned error.

The local wrapper verified unchanged source identities and exact owned services;
these are local results, not relabeled server evidence. Evidence prefixes are
followup-9e5542ba-native-unit, followup-43a2c7af-owned-unit,
followup-43a2c7af-native-integration and
followup-43a2c7af-owned-linux-integration under the external agents evidence.
The cohort remains held until replay and E2E required-constructor companions
qualify together. Full SessionService/Runtime typing, legacy archival aggregate
maps and assigned vendor programs remain open. The queued distinct
UnprovedInputOwnership chronology concern still requires reproduction before
any fix or completion claim.

The final unknown-facts preflight fixture explicitly retains empty native and
termination components; these carry no observed terminal fact. All 35 policy
contract/actual-preflight cases pass again in 1.89 seconds, zero skips, with
source identity verified (followup-final-unknown-facts external evidence).
The refreshed inventory covers 41 owned active Python files and still lists
30 SessionService/free-helper and four Runtime functions annotation-incomplete.

Final bounded cohort readiness supersedes the earlier peer hold: canonical
35cc6309 normally contains current parent ee54ddba, ingestion reporter/callers,
quality c70f323e E2E/preliminary callers and analysis 8e8a5b65 replay/callers.
Only the timed Telegram fixture constructor overlap needed reconciliation;
all incoming typed guards/dynamic event identity and the real refusal writer
were preserved. The complete joint selection covers 71 affected source/caller
test files. All 1,165 units pass in 65.54 seconds, zero skips, p90 0.631 seconds,
maximum 6.137 seconds. All 192 collected Linux integrations pass in 64.30
seconds, zero skips. Both local wrappers exit zero and verify unchanged source
identities and exact owned service/runtime identities. Evidence prefix is
followup-35cc6309-joint-{unit,integration} under external agents evidence.
Independent strict 14 targets, scoped Ruff and comment checks pass. Configured
whole-source mypy checks 157 files with only the existing owner
analysis/truth.py:5890 condition-cache annotation error; the nullable watch
loop error is resolved by its canonical owner companion. This qualifies the
required refusal/eligibility cohort and its current callers for parent review;
it does not close the remaining whole-session/runtime/vendor work or claim
that the queued ownership-chronology concern has been reproduced or fixed.

### Durable ownership refusal preserves native chronology

The immutable aedee0d2 diagnostic reproduces one historical acknowledged input
with no retained attempt starving a later independently owned fresh frame in
managed paging, Node paging and SSE, including a restarted controller. It is
negative evidence, not an integrated success claim. The bounded correction
adds four explicit UnprovedOwnershipReason tags at the actual journal/tool
lineage readers and handles only that named exception beside the existing
UnknownNativeRetryState. Generic ValueError, contradictory lineage and schema
failures still propagate. No attempt, owner, body, terminal event or producer
provenance is invented. Existing captured unknown fields remain unchanged.

The existing closed host refusal receipt admits those four reasons. Its
host_retry_refusals wire map and NativeRetryRefusal names are preserved for
existing durable storage; values remain named strict records indexed by the
actual frame/session/execution digest. Persistence precedes both in-memory
seen state and cursor advancement. Exact duplicate receipts retain their
original controller/version and observation clock. A lost commit response is
reconciled from that exact receipt; a failure before persistence leaves the
entire row and cursor unchanged. A subsequent independently owned turn keeps
its declared zero and original turn-retry:session:fresh idempotency key.

The event-kind audit found input lineage is consumed by custom-tool,
running/rescheduled and failure/error turn effects. Actual
session.status_terminated and span.model_request_end billing are outside that
check. An actual service regression proves both still persist their native
facts with unproved historical turn attempts and no input HTTP. Another
regression leaves contradictory stored attempts unhandled, with no receipt,
seen state, cursor or HTTP effects. SessionService.close and stop_followers
now declare their actual None return while tests retain real owned cleanup.
The inventory remains incomplete: 28 SessionService/free-helper and four
Runtime functions still require full annotation/model migration; all vendor
implementation work and broader session orchestration remain open.

Pre-final proof: 52 focused units pass in 2.47 seconds, zero skips; all twelve
collected actual PostgreSQL cases pass in 5.90 seconds, zero skips. The latter
crosses managed/Node/SSE with failure-before-write/lost-commit-ack and both
unknown retry and unproved input-owner states. Independent strict four
source/test targets and scoped Ruff pass. A final unit run includes the added
contradictory-lineage control and the final cleanup annotations; its terminal
result is recorded below rather than relabelling these earlier proofs.

Final source-prefix qualification: all 59 collected affected units pass in
2.56 seconds, zero skips, including contradictory-lineage refusal and actual
cleanup. External evidence prefixes are owner-chronology-qualified-unit and
owner-chronology-pg. The final source adds only that negative control beyond
the twelve-case PG-qualified prefix; no receipt persistence code changed.
## Required termination identity correction beyond qualified fa1ca989

The producer/caller audit does not justify the earlier empty-receipt allowance.
The sole active termination writer is SessionService.process_event for an actual
session.status_terminated event; it always writes event_id, event_type and the
collection clock. No active writer emits termination_receipt={}. Old reader
truthiness is not producer evidence. A present receipt now requires its actual
nonblank event_id; empty/unidentified/malformed identity is rejected before
native GET/POST, refusal reporting or delivery enqueue. True absence/None remains
unknown and still uses actual native preflight. The earlier synthetic empty
receipt positive fixture was corrected rather than used as compatibility proof;
its historical qualified checkpoint is preserved unchanged.

ANSWERED with text=None has an actual producer path: turn_answers only saves
stripped nonblank agent text, and an observed end_turn can complete without a
final textual message. deliver_answers records that completion with None.
File delivery is separately owned/idempotent and need not create fake answer
text. Telegram's existing EMPTY_TEXT honestly reports completion without text.
The existing failed-attempt text test proves that its stale text is not reused
when the accepted retry ends without text; the actual file-reply test covers
separate delivery. No active producer requires blank whitespace. Existing old
empty-string normalization is not a claim that a new required text field may
be silently defaulted, and the ingestion-owned writer was not broadened.

Focused policy/retention/refusal/binding proof: 68 units pass in 2.60 seconds,
zero skips; independent strict three targets and Ruff pass. Evidence is
termination-receipt-correction-unit.{log,xml} under external agents evidence.
This is a separate bounded correction from the still-unfixed chronology
reproduction; it does not alter the qualified fa1ca989 checkpoint.

### Typed observed answers and locked question settlement

This bounded slice replaces the anonymous turn_answers text/ended payloads
with NativeTurnAnswer records keyed by actual journal message identity.
NativeOwnershipSnapshot carries actual recorded lineage, while absent journal
metadata remains None rather than an invented empty journal. Full native
callback ownership resolution still validates the full journal. A declared
answer-query selection preserves other actual message IDs as explicitly
unselected; it never manufactures their retry counts or accepted inputs.

The first broad test pass exposed six unchanged exhausted-retry regressions:
the initial new settlement projection was incorrectly validating prior input
metadata for FAILED questions. That product overvalidation was removed.
QuestionSettlementSession now reads only basic identity/phase facts first;
FAILED settlement never reads the historical input, retry count or body.
Lineage is projected only for the selected history-consuming questions. The
six original opaque-body/input regressions pass unchanged. A new real-service
case also proves an opaque failed question and old tool lineage cannot block
a fresh independently owned answer, while preserving all unrelated bytes.

The managed history adapter requires actual data and next_page, including a
real string cursor "0". It rejects missing or incompatible page declarations,
cycles and conflicting event identities. No Node descending cursor is
invented. The pinned managed route accepts order=desc and returns exactly
those two paging fields. Only event identity, running execution, agent text
or redacted blocks, and actual idle stop reason are projected. Missing native
execution correlation remains uncorrelated; it cannot inherit another
input's text. A new running observation invalidates earlier end_turn
completion, and a higher declared attempt cannot inherit failed text.
COMPLETED with text=None preserves an observed end_turn without observed
text; UNSETTLED means completion was not confirmed, not invented native
status. File delivery remains its existing separate idempotent effect.

The pure ManagedEventPage/NodeEventPage/NativeWireEvent definitions moved
unchanged into contracts/native_events.py. native_transport explicitly
reexports them for existing callers, and answer projections import the
inward DTOs instead of the persistence-containing adapter. The response maps
remain only native wire admission, current storage patches, and precise
indexes keyed by actual event/execution/message identities. Exact None,
default and mapping sites and reasons are in agents-exceptions.json.

Before answering, deliver_answers re-reads the actual session, journal
question and retry lineage under the session lock. It passes both actual
question/session IDs to the existing locked question writer. A missing
question produces no answer_recorded marker, a changed lineage cannot
publish old text, and a changed question binding refuses. A historical
payload need not invent a missing session_id because the actual session key
and the stored question's binding are both checked; a present conflicting
payload value rejects. Existing first-answer and idempotent delivery
behavior is retained. Three ingestion-owned direct turn_answers tests were
narrowly migrated to typed snapshot input and attribute assertions, with the
same text, no-text, retry and page-count oracles. No peer intake/media edits
were replaced.

Qualification on the frozen owned source prefix based on 1f748f91: all 837
collected broad Session/native/runtime/Telegram units pass in 24.55 seconds,
zero skips, p90 0.313 seconds and maximum 5.462 seconds. All 126 collected
affected Linux integrations pass in 72.57 seconds, zero skips, including
actual replay/reference/market/E2E callers and the two real PostgreSQL
matching/foreign-question settlement cases. Both wrappers exit zero and
verify unchanged source/service identity. Evidence prefixes are
answer-settlement-final-broad-unit and answer-settlement-affected-integration
under external agents evidence. Independent strict seven targets, configured
default four source targets, scoped Ruff/format, source-comment and diff
checks pass. Tests ran against the frozen owned work-in-progress source
before the report/commit; no product code changed after these gates.

This closes turn_answers/deliver_answers typing and the associated answer
contracts, not whole SessionService. The refreshed inventory retains 26
SessionService/free-helper and four Runtime functions incomplete. Whole
execution-barrier/tool/usage/event orchestration, remaining Telegram/file
presentation coupling and the assigned vendor implementation backlog remain
open. Earlier 19e0dc59 was an inactive contract checkpoint, not qualified
caller activation; the final source and real caller proofs supersede it.


Held terminal-failure adapter qualification (6c2d1e5c descendant)

The terminal-failure command validates original queue payloads through the
wire-only delivery/answer decoders before constructing internal typed
notifications. Internal container revalidation uses Pydantic by_name=True
so the already typed JobPayload.depends_on field is revalidated under its
Python field name. This does not enable field-name admission at the public
wire decoder: a raw depends_on key still rejects, while _depends_on remains
the actual queue alias. Constructed version=True still rejects. The exact
regression is test_terminal_failure_keeps_wire_alias_admission_and_revalidates_typed_payload.

The test-only runtime_failure_fixture.http_routes callback is genuinely
optional HTTP expectation setup: absence permits no HTTP route. It never
replaces a required Runtime service. The native-missing reconciliation case
registers and counts its actual GET through this strict router.

On the unchanged owned source after the canonical guard join, the focused
alias/original delivery cases pass 2/2 in 2.81 seconds; the two new retirement
files pass all 19 units in 3.78 seconds and all four actual PostgreSQL
POST-inflight/committed-ACK-loss/refusal-write/restart cases in 6.10 seconds,
zero skips. The local wrappers verify source identity and exit zero.
Independent strict four contracts/adapter/helper targets and scoped Ruff
pass. Evidence prefixes retirement-6c2-alias-focused,
retirement-6c2-final-phase-unit and retirement-6c2-phase-pg are external.
The earlier broad diagnostic remains 944 passed/two failed; one delivery
failure is fixed by the tested alias correction, while the remaining
Telegram terminal-answer legacy Runtime fixture still needed migration at
that snapshot. Commit 4a362a56 later migrated that fixture with its original
assertions intact and a passing focused control.
This is a held checkpoint, not full Runtime/caller qualification or whole
SessionService completion.

Inactive worker command admission checkpoint

runtime_dispatch_contracts defines the eleven actual durable command variants,
ClaimedWork (RUNNING with an observed positive claimed attempt and matching
kind), and DependencyState (only actual job identity/status). This is an
application contract, not the persistence JobEnvelope renamed as a domain
aggregate. RepositoryWorkerQueue uses the canonical JobRepository claim and
invalid-revision quarantine. Original payload facts remain at that queue
boundary; a concrete command refusal records only static validation codes
and the payload field name. The initial normalized-envelope digest was later found insufficient for
omitted nested clock facets; the adapter now consumes the unprojected raw
revision from canonical ClaimedJob (see the correction/proof below). Quarantine still compares
the actual complete stored revision under the canonical job lock before any
terminal change. The dependency adapter refuses mismatched storage identity;
a truly missing dependency remains None for explicit worker policy.

Seven command/real Memory queue cases pass in 1.73 seconds, and the real
PostgreSQL command-quarantine case passes in 2.47 seconds, zero skips, under
the unchanged local wrappers. Independent strict contracts/adapter/test three
targets and scoped Ruff pass. Evidence worker-0441-queue-unit-3 and
worker-0441-queue-pg is external. Earlier two setup diagnostics remain recorded:
the malformed synthetic dependency omitted required storage fields, then
reused an actual idempotency key; authored complete setup now preserves the
intended wrong-identity refusal oracle. These contracts/adapter are inactive.
The worker, Session send producer/result phases and scheduler side-effect
result ports still need their coordinated migration and caller proof; this
checkpoint does not close the remaining Runtime method or whole Session.

Inactive session-message result phases

message_dispatch distinguishes an actually recorded pending initial intent,
an observed native acceptance (including explicit UNREPORTED/EMPTY receipts),
a historical accepted-message index with no retained receipt, and a declined
question target. INDEXED_UNREPORTED does not claim a newly dispatched native
input. The observer reads only the requested journal owner and actual stored
index, rejects duplicate/contradictory owner evidence, and does not require an
unused body/retry budget to describe already observed acceptance. A pending
intent still requires its real declared zero attempt and native input body.
Actual no-session refusal remains a separate typed target phase.

Twenty result/outbound cases pass in 2.14 seconds, zero skips, including the
real mocked-HTTP initial accepted/unreported/empty post and idempotency cases
with their existing original oracles. Independent result source/test strict
two targets and Ruff pass. This is an inactive result observer, not an untyped
send_message wrapper presented as a complete port. The actual send producer,
attachment preparation and coordinated scheduler results remain under
migration. Evidence worker-573-message-phase-unit-2 is external; the prior
single test expectation diagnostic used ValidationError where an actually
missing required retry_count correctly raises KeyError, with no source
fallback introduced.

Correction to inactive worker claim-revision rationale: foundation identified
that nested Timestamp serializers can add omitted nullable facets even under
exclude_unset. Therefore normalized envelope serialization is not a reliable
original raw revision in every valid ledger case. At checkpoint 57377441 the adapter therefore required a raw claim-revision
API and an omitted-nested-facet regression before activation; that boundary
is corrected in the qualified raw-revision checkpoint below. The limited passing cases
above did not contain such a ledger and do not establish full parity.

Send admission prerequisite in the inactive worker stream

send_message now admits the actual closed AgentMessage before using its
routing facts. NativeSessionResolution.mapping_identity selects the declared
existing-session/event/proposal/item namespace in the existing order and
refuses missing routing facts instead of str(None). ContextBinding provides
the actual session ID; LegacyToolContext.needs_migration distinguishes an
unreported context version from a declared version, without inventing zero.
An actual missing session after tool synchronization raises instead of being
passed onward as a usable session. USER_QUESTION scope uses its typed target.
All 197 affected Agent/workflow/model-context/Telegram unit callers pass in
17.54 seconds, zero skips (worker-e587-send-admission-unit). Default Session
source mypy, independent native session-contract strict and Ruff pass.
Independent whole Session strict remains incomplete: actual untyped
reopening/follower/attachment/presentation dependencies and constructor
repository inference are open; this does not claim the send result port is
complete or activate the new worker dispatcher.

Typed question reopening dependency

SessionService.reopen_for_question now consumes UserQuestionMessage and a
real owner key, using the strict session_reopening policy/adapter. The
actual session ID must match the question target; duplicate journal owners,
reported-null retired indexes and a failed-session reopening without an
observed controller clock refuse before mutation. Only actual unfinished
owners are retired, completed/current owners survive, and only their
USER_QUESTION rows receive the existing local FAILED turn status. This is
local controller lifecycle, not a fabricated native termination. The raw
session/history dictionaries remain at the explicit sparse persistence
mutation boundary and preserve unrelated bytes. ReopenState.retired=None
means no retired index was recorded; creating the first index adds only
actual observed owners. session.get("reopened", []) initializes the existing
lazy append-only controller history on the first real reopening; reported
null/non-array history rejects and old entries are not reconstructed.
Missing historical turn_status/payload-kind remain explicit and do not gain
invented native facts or unused retry/body requirements.

All 105 connected reopening/Telegram/outbound units pass in 8.82 seconds,
zero skips, with independent source/test strict two targets and Ruff clean
(worker-0dd-reopening-unit). The one direct authored Telegram helper test now
declares its actual synthetic session ID and typed question request while
retaining its old/current/retired/status assertions. This closes that helper
dependency, not the still-untyped follower/attachment/presentation portions
of SessionService or the inactive worker activation.

Inactive dependency policy

RepositoryWorkerQueue.dependencies returns named READY/WAITING states from
actual dependency identity/status projections; a missing or FAILED/EXPIRED
prerequisite raises a typed DependencyFailure carrying that exact observed
reason/identity. No dependency reads change the worker attempt/status or
pretend missing state is complete. Dynamic job_ids are actual queue IDs;
WaitingDependencies requires at least one ID. All nine command/Memory queue
cases pass in 1.89 seconds, zero skips, independent strict three targets
(worker-e587-dependencies-unit). The raw revision correction remains pending
foundation's additive API; this is still inactive worker work.

Typed follower dependency on coherent parent 59b963cf

start_stream/follow now declare their actual string/task lifecycle. The new
FollowedSession projection distinguishes an unreported runtime state from
known status; NativeSessionReader provides actual identity-bound lifecycle
facts. Missing or archived resources are recorded as typed UNAVAILABLE
observations with actual checked_at, without RUNNING/ENDED or termination
receipt invention. NativeFollowerUnavailable retains the existing bounded
follower backoff while the current stream task is cancelled/awaited. A later
changed local state wins an in-flight readback. Only an actual matching
record_event false result sets no_event; absent/null result remains unknown
and does not become false or true. The protocol translation is an exhaustive
closed map with no default.

Seventy-six connected follower/session-audit/retention/REST/transport units
pass in 3.02 seconds and the actual PostgreSQL missing-observation case passes
in 3.05 seconds, zero skips (worker-491-follower-final-unit and
worker-491-follower-pg-2). The first PG setup omitted required captured-session
metadata and failed before the workflow; its corrected freshly authored
scout declares actual synthetic identity/opening clock, explicit unavailable
native-version/source reasons, and initial zero cursor (not a rewritten
historical row). The initial connected test's unsupported CONDITION_FIRED
marker was replaced by a real third source message: that test concerns new
input not resetting original retry, and all original post-count/body/retry/
event-state assertions remain. No unused financial condition was fabricated.
Independent follower projection strict and scoped Ruff pass; default Session
source passes. Whole Session strict still exposes the remaining real
execution/attachment/presentation/constructor dependencies and is not claimed.
AST inventory now has twenty Session functions lacking complete signatures;
Runtime's worker signature is present but its implementation remains open,
so a zero annotation-gap count is not a Runtime migration claim. Exact
retained sites/reasons and behavior controls are appended to the exception
ledger. Retirement 4a362a56 remains immutable and separate.


Original claim revision correction

The worker stream normally joins foundation 641ac01c. RepositoryWorkerQueue
now calls claim_with_revision and carries the actual raw claimed-document
revision into InvalidJobClaim; it never recomputes a digest from a normalized
Pydantic envelope. The actual command refusal with a valid nested historical
receipt omitting precision_reason/bucket_anchor quarantines exactly once on
Memory and PostgreSQL, preserves the ledger's original serialized omissions,
and retains the canonical changed-revision refusal. All fourteen connected
unit cases pass in 1.86 seconds and all six actual PostgreSQL cases pass in
4.26 seconds, zero skips, wrapper/source guards exit zero. Independent strict
contracts/adapter/test three targets and Ruff pass. Evidence
worker-641-raw-revision-final-unit and worker-641-raw-revision-pg is external.
This resolves the inactive adapter digest defect; worker dispatch/result
activation and the remaining Session producer dependencies remain open.

Phase-specific native deployment configuration

SessionService.config now returns NativeSessionDeployment for the explicitly
requested ANALYST/SCOUT and live/replay phase, rather than a raw configuration
map with agent/environment truthiness fallbacks. resolve consumes that typed
deployment directly; Runtime.validate supplies its actual replay mode. The
existing required prompt/effort/selected agent/environment contract validates
before creation effects. Unused role identities and admission capacity are
not required merely to decode a selected native deployment; running-session
capacity remains its separately validated composition/admission policy.
All 96 native/config/runtime/composition/version unit callers pass in 5.92
seconds, zero skips (worker-6f646-native-config-unit), scoped Ruff clean.
This closes that configuration accessor, not the still-open whole Session
constructor or worker body. No permissive settings default was introduced.

Inactive typed queue lifecycle and actual admission-clock parity

RunningJobClaim carries only actual job/kind/RUNNING/positive attempt and the
original payload digest. RequeueRequest uses that narrow phase scope rather
than revalidating unused full commands (the initial two failing cases exposed
that overvalidation). RepositoryWorkerQueue rechecks current scope under the
canonical job lock, patches only the owned schedule/completion fields and
validates before writing original raw JSON. It does not reserialize unrelated
historical receipts. Waits refund exactly the observed claimed attempt;
changed scopes and repeated transitions produce no effects. Expiry requires
an actual supplied observation boundary and actual current due expiry; an
unbounded job does not invent one. Counter zero under an unproved recovery
flag becomes a once-only invalid claim, not attempt one or a running effect.
This preserves the existing single-controller scope, not a new distributed
execution lease or cross-controller POST safety claim.

Root authorized the narrow JobMetadata admission-clock companion after the
real writer audit: slot_waiting_since_prec/prec_reason/bucket_anchor are now
closed optional fields. Any reported non-null metadata validates coherently
with Timestamp; scalar-only/all-null-facet retained rows keep strict aware
parsing and no invented precision/reason. Foundation reviewed the exact
frozen diff at HEAD597dea63+WIP and found no source issue. Actual defer then
next typed claim passes on Memory/PostgreSQL; malformed copied models reject
before a repository write. The generic extra-field policy remains closed.

All 31 connected queue/envelope/revision units pass in 1.76 seconds and all
18 actual PostgreSQL cases pass in 3.81 seconds, zero skips, unchanged local
wrappers/source guards exit zero. Independent jobs/dispatch/adapter/test four
targets and Ruff pass. Evidence worker-597-lifecycle-final-unit and
worker-597-lifecycle-final-pg is external. Earlier intermediate 30/17 proof
is retained; the final extra case specifically proves zero-attempt quarantine.
These are inactive queue transitions; concrete worker dispatch/results and
remaining send/follower/attachment dependencies still require activation and
whole caller proof. Retirement 4a remains separate and immutable.

Held question-scope preflight candidate

Immutable negative checkpoint fbc2e5d1 reproduces a real boundary breach: an
actual UserQuestionMessage aimed at live session s reads a stored question
bound to foreign and posts its synthetic private text after native GET.
The matching-session control is idempotent. The negative gate is one fail/
one pass in 1.60 seconds (worker-8840-question-binding-before), not a product
qualification. The candidate now checks both actual question/session IDs under
the question lock before native reads for a fresh input, and rechecks before
fresh text/attachment effects. Existing retained acceptance/indexed inputs and
known-zero pending bodies do not reconstruct an unused deleted question or
retry budget; pending ownership still requires its actual original payload/
idempotency key. The lock order remains controller send lock, session lock,
then question lock for the actual body phase; the early read does not hold a
question lock while waiting for a session lock.

The branch normally joins prompt370. Session imports the typed pure prompt
formatter inward and pre-admits required prompt facts before attachment
work; prepare_attachments returns QuestionPrompt. This return is only an
intermediate caller alignment: its input/body and native file lifecycle are
explicitly still open and are not signed as a complete typed port. The
unsupported generic data-wrapped upload response fallback was removed after
reading the pinned direct file route. Twenty-five connected question/output/
initial-post cases pass in 1.87 seconds, zero skips, including exact original
pending idempotency key, accepted opaque historical body/no budget, and a
binding-change race refusing a native POST. Default Session mypy passes;
whole independent strict remains incomplete. Evidence
worker-fbc-question-phase-unit is external. This candidate remains held with
the worker/native producer cohort; it is not a parent-ready activation.

Standalone public queue repair on clean main49 parent

This companion starts from canonical 5b77f741 (normal 8f/main49 ancestry),
without the held worker/native/retirement streams. Immutable negative
checkpoint 047e21df reproduces three actual workflow failures and one passing
provided-delay control in 5.36 seconds: an omitted Telegram retry_after leaves
a real answer job RUNNING for both retryable and terminal attempts, and an
actual admission cap defer writes clock facets rejected by typed queue reads.
Evidence public-5b-before-unit is external. No caller/assertion was weakened.

runtime_retry now makes the existing application backoff explicit through
closed models. TelegramResponseError keeps genuine provider retry_after=None;
only the scheduling decision supplies application backoff from actual
JobRetryState.attempts. Reported Telegram delays and actual DeliveryError
directives remain distinct (including known delivery zero). Exponential
backoff saturates at 60 without exponentiating unbounded counts, preserving
the existing numeric policy. No default attempt 1 or arbitrary getattr error
field remains at this worker call site. This does not claim the rest of the
still-untyped worker body is migrated.

The approved/reviewed slot-wait schema logic from held checkpoint 8840 is reimplemented
narrowly here, not merged with its inactive ancestry. JobMetadata recognizes
only the three actual optional clock facets already written by production.
Any reported non-null facet uses Timestamp coherence validation; scalar-only/
all-null metadata remains unreported and strictly aware, without invented
precision/reason. The actual Runtime→SessionAdmission cap defer/wake path now
survives JobRepository get, idempotent enqueue and next claim on both Memory
and PostgreSQL. Malformed copied models refuse before writes; canonical JSON
roundtrip checks preserve original scalar/nullable-facet representation.

All 68 connected unit cases pass in 5.56 seconds, zero skips, p90 .404 seconds
and maximum 1.005 seconds. All twelve selected actual PostgreSQL Runtime/
precision/public-repair cases pass in 35.63 seconds, zero skips, complete
collected set and source/service guards. Three answer controls use actual
build_runtime, Inbox, HTTP response admission, worker and queue: omitted delay
requeues at the declared 2 seconds, reported 7 remains 7, and the final attempt
reaches FAILED/DELIVERY_FAILED without replacing the authoritative answer.
Every service is real; the fixture preserves only pytest logging and rejects
all undeclared external HTTP. Actual integration evidence is
public-047-runtime-pg; units are public-047-connected-unit. Independent strict
five policy/jobs/helper/pure-test targets and Ruff/check-format pass. Selected
default mypy exposes existing outside-owner truth.py:5890 _condition_cache and
deployment_postgres_helpers.py:65 nullable-row errors; no new target errors or
suppressions were added. Whole Runtime/Session migration and assigned vendor
backlog remain open; the clean public repair does not activate those cohorts.

These owner gates ran on the frozen owned work-in-progress source above
047e21df before the report/commit; product source did not change after the
passing checks. The immutable wrapper fingerprints remain the exact proof
identity; parent final-head integration qualification is still required.

Qualified joint question-retirement and Runtime failure cohort

Canonical product source 75e3bd1c normally joins the preserved 4a retirement,
analysis fd35085e real worker fragments, quality 280fbc07 actual E2E adapter,
public 494 retry/slot fixes and current parent 253f50da. The Runtime conflict
kept both the required typed failure repository and explicit retry policy;
append-only reports retain both complete histories. No inactive worker/native
producer branch is ancestry. The only subsequent addition is the supported
unknown-versus-occurred marker regression and final inventory/evidence text;
product source did not change after the broad passing gates.

SessionService.abandon_question/reconcile_answers and Runtime terminal failure/
restart reconciliation now consume phase-specific strict records. Actual
initial ACCEPTED/UNREPORTED/EMPTY receipts or sent_at are preserved without
requiring an unused old body/budget. Unknown initial acceptance/journal state
refuses explicitly before answer/native effects; no missing native ID becomes
an unsent assertion. The existing controller send lock then repository session
lock serializes retirement with actual initial/recovery POSTs and acceptance
commit. Both-ID authoritative answer state is rechecked under the question
lock, preserving the first answer and idempotent delivery. No distributed
POST guarantee is claimed beyond these actual participating locks.

Runtime requires a concrete RepositoryRuntimeFailures port in production
composition. Its narrow store port owns actual queue locks, checks current
job/type/status/target and phase-consumed facts, and performs exact sparse
writes. Host refusals contain actual captured controller identity, actual
recording clock and request-digest scope, remain distinct from native receipts,
and are persisted before any handled marker. The closed JobEnvelope map and
pre-serialization entity validation preserve absent-versus-explicit-null facts.
Transient write/broken-ACK outcomes are sanitized named availability failures;
permanent driver rejection and malformed global data do not become handled.
Actual unrelated worker work still completes and restart preserves original
receipt identity. Changed native 404 facts can close the question through the
real refusal writer without rewriting past refusal or fabricating termination.

All 966 broad affected units pass in 24.30 seconds, zero skips, p90 .321 seconds
and maximum 5.632 seconds. The entire 140-case affected Linux integration
collection passes in 65.66 seconds, zero skips, covering real PostgreSQL,
replay/reference/market-refresh/four E2Es and public queue controls. Evidence
retirement-75e3-joint-{unit,integration} is external; wrappers verify unchanged
source/service identity. Independent strict contracts/runtime contracts/
adapter/helper four targets pass. The two peer caller omissions from the
previous 129/7 diagnostic are closed without weaker assertions/deadlines.

Final audit correction: the suspected unknown-marker skip was disproved.
Consumers already inspect marker.value, so an explicit unavailable timestamp
remains eligible and never masquerades as handled. No product behavior was
changed. The failed policy diagnostic is preserved externally as
retirement-75e3-marker-policy-diagnostic.py and marker-before reports. The
accurate current regression adds four passing units in 1.32 seconds and one
actual PostgreSQL case in 2.13 seconds, zero skips, and independent test strict
passes. It preserves explicit unknown reason versus all-null absence and
proves the actual pending adapter does not invent a handled marker.

The refreshed inventory includes the new inward retirement contracts, sparse
failure adapter and public retry policy. Twenty-four SessionService/free-helper
functions and the Runtime worker still lack complete signatures in this
cohort; annotation status is not whole-body strict proof. Required send/file/
usage/tool/event/execution-barrier migrations, remaining presentation coupling,
and the assigned vendor programs remain open on their separate workstreams.
Exact retained map/default/None classes and behavior controls are recorded by
symbol/site in agents-exceptions.json. This bounded cohort does not waive any
of the remaining global work.

### Inactive native file preparation phases

The preserved worker/native branch now defines inward native_files records for
fresh NOT_REQUESTED cache state, durable UPLOAD_UNCONFIRMED intent, an actual
FILE_ID_OBSERVED receipt, durable MOUNT_UNCONFIRMED intent and MOUNT_CONFIRMED
resource receipt. Source scope carries the actual question/session/cache key,
cached digest, filename, MIME and byte count. Controller occurrence and resolved
versions are separate from native IDs; fresh cache state does not invent a
controller version or native ID. Missing legacy preparation remains unreported.
No parser or writer initializes missing state on reads.

Ten pure phase controls pass in 1.33 seconds with zero skips, including known
zero-byte cache, unknown occurrence refusal, nested constructed owner mutation
refusal and canonical workspace target refusal. Independent source/test strict
checks pass. This is an inactive contract checkpoint, not native upload or
mount recovery proof. SessionService.prepare_attachments still has its old
untyped upload/mount implementation. Required route beta selection, durable
phase persistence, scoped resource reconciliation, historical pointer handling,
pre-upload ownership admission and paired fresh-cache writer remain open.
Pinned file/resource routes provide no upload/mount Idempotency-Key guarantee;
an unconfirmed upload without an observed ID cannot authorize a blind repost.

### Native preparation adapter candidate — paired writer still held

The actual Session producer now accepts the authoritative question/session target
and re-reads it under the question lock. It admits the prompt, all file references
and cache ownership before effects. The native adapter records a committed upload
intent, retains the actually observed file ID, records mount intent, and confirms
only an actual scoped file resource. Upload uses files-api-2025-04-14; resource
routes use managed-agents-2026-04-01. Neither route is credited with honoring an
Idempotency-Key. Unknown upload acceptance refuses automatic repost. Lost mount
acknowledgment is recovered by actual complete resource pages; foreign occupied
paths and disappearance of confirmed resources refuse. Legacy pointers require
matching scoped resource evidence and retain their original cache fields.

PostgreSQL advisory locks hold transactions. Therefore the adapter owns and drains
an awaited preparation task, using the repository's actual task-owned transaction
contract to commit each short file CAS independently of the parent session/question
transaction. HTTP runs outside file transactions. The child never requests session
or question locks; no file-first path acquires a parent lock. Foundation reviewed
that boundary. Actual PostgreSQL tests observe committed intent from an independent
task during HTTP while the parent owns its session transaction. Concurrent callers
cannot authorize a second upload; cancellation drains the owned preparation task.

Observed effect receipts are retained in the cache row's typed digest-keyed
native_effect_receipts map before the current-phase CAS. This preserves an actual
upload identity even when cache ownership changes; it does not rewrite the new
owner or continue mounting. A durable observed receipt can recover a failed hot
phase update, while absent acceptance remains unknown. The earlier draft's proposed
unregistered collection failed its diagnostic and was removed before this candidate.
No schema or storage implementation was changed.

The final transport source passes independent strict checks with contracts and
three test modules. Twenty-six units pass in 1.80 seconds; six actual PostgreSQL
cases pass in 3.10 seconds, zero skips, including the changed-owner receipt control.
This remains an inactive paired candidate: ingestion's fresh-cache initialization
and the existing Session caller/attachment fixture cohort must normally join and
qualify before activation. Whole worker and remaining Session typing stay open.

### Required condition evaluation caller

Clean parent 956666e1 normally joins producer 7c0e8b27, without held worker or
native-file ancestry. Runtime.eval_conditions_wire admits the queued payload
through EvalConditionsJob and passes its actual cutoff facets as Timestamp.
The checked runtime_evaluation protocol consumes ConditionEvaluationResult;
that result is revalidated before the durable ops.jobs.result JSON boundary.
Neither missing cutoff nor unknown occurrence becomes a processing clock.

Negative workflow evidence at e869 records four failures from the old direct
dictionary dispatch. Final source passed 80 connected units in 16.26 seconds
and all five collected actual PostgreSQL cases in 5.31 seconds, zero skips,
with unchanged source fingerprints. Actual Scheduler.tick, queue claim, Runtime
worker and evaluator produce DONE with the real serialized receipt; missing,
naive and unknown cutoffs fail before evaluation. A constructed invalid typed
receipt also refuses before serialization. Independent strict checks pass for
runtime_evaluation, scheduler_results, runtime_public_fixtures and the result
boundary test. Whole Runtime.worker body typing remains open.

The unchanged native adapter candidate has additional source-backed boundary
controls: verified retained mount pointers preserve unrelated session history,
duplicate raw upload identity facts refuse with static private-value-free errors,
nonprogressing resource cursors cannot authorize mounting, and a confirmed
resource ID cannot be replaced merely because path/file ID match. The full
31-case native unit selection passes in 1.63 seconds with zero skips; independent
five-target strict checks pass. These do not replace the pending real fresh-cache
writer and complete Session caller proof.

Known-ID recovery now consumes the exact NativeFileSourceScope projection rather
than re-admitting unused original Telegram sender MIME/unique-ID provenance. Live
cache admission remains strict at its producer. A real lost-mount-response control
removes the already-consumed original bytes and sender metadata, recovers the
observed resource without another upload/mount, and proves those absent fields
are not reconstructed. Thirty-two units pass in 1.55 seconds; the same six actual
PostgreSQL cases pass in 3.33 seconds, zero skips. Independent adapter/test strict
checks pass. The actual pre-writer attachment fixture currently refuses missing
native preparation, as expected; its negative one-case evidence is preserved and
must turn green only through the paired fresh writer, not fixture initialization.

The refreshed held-branch signature inventory includes native preparation,
closed worker commands/queue adapter and the qualified eval boundary. Eighteen
SessionService methods still have incomplete signatures in this branch. Runtime
worker now has an annotation, but its raw mapping/handler implementation has not
been migrated or independently strict-qualified; this is still an open worker
body, not a completed function. The inventory deliberately does not equate an
annotation with precise body typing or waive remaining aggregate/transport/vendor
work. Final current-main caller qualification remains required for this branch.

### Native metadata confirmation before mount

An observed upload ID is now distinct from FILE_METADATA_VERIFIED. Before a new
mount, files/{id} readback must report the same ID and the actual requested name,
byte count and MIME (case normalization follows File.type), plus a matching
session if native scope is reported. An unreported native scope remains None;
it is not relabelled as a reported owner. Cached SHA remains a host-side byte
receipt, never a claimed remote checksum. Missing or contradictory metadata
preserves FILE_ID_OBSERVED, refuses mounting, and retries only readback. Existing
confirmed mounts and verified historical pointers do not re-fetch unused bytes.
Ambiguous native URL dot segments refuse before upload, and generated flat mount
filenames escape path syntax without replacing the original declared filename.

Thirty-nine native units pass in 2.24 seconds, and all six actual PostgreSQL
cases pass in 3.91 seconds with zero skips/source guards. Independent strict five
targets pass. Contradictory name/size/MIME/session scope and bool-as-size controls
prove the ID survives while mount count stays zero; corrected readback finishes
with one original upload. The fresh-cache writer/connected caller cohort remains
held until its normal join and whole selection proof.

### Paired writer and current-parent caller qualification

The candidate normally joins ingestion's fresh writer f2552fa0 and current parent
855a0c7a. The first full affected selection passed 1,201 cases and found seven
old authored callback fixtures in test_model_context_migration that omitted their
actual variant's required facts. Those controls now send the canonical source,
milestone, fired timed-review, expired-call or proposal message. Their existing
migration, untouched prior history, ID refusal, exact POST counts and failed-update
no-delivery assertions are unchanged. The timed review adds no financial fields;
the expired-call control explicitly declares its synthetic call snapshot instead
of pretending an incomplete callback is a valid historical row.

All 1,208 affected units now pass in 18.37 seconds, zero skips, p90 .148 seconds
and maximum 2.992 seconds including fixtures. The callback factory independently
passes strict typing; the older migration test module's whole typing remains open.
The actual fresh Inbox-to-Session attachment ready/again control is green using
real writer state and scoped metadata/resource readbacks. Full collected actual
integration proof is still pending; no parent activation is claimed here.

### Replay annotations and raw phase revision correction — qualification pending

The wider direct-consumer integration selection passed 36 cases and exposed 22
real replay/reference admission failures: production replay emits replay_at and
its canonical clock facets, and authored references declare the exact origin plus
historical=False. Analysis confirmed that catalog. ReplayBinding now models the
actual clock fields; scalar-only aware clocks preserve unknown precision, while
reported facets validate coherently. SourceMessage alone supports the closed
nonhistorical reference pair, requires its actual run/time binding, and refuses
captured-source relabelling. Actual replay continuation session selectors are
represented on milestone/expired-call messages. Native live composition refuses
these replay annotations. No retained payload or replay producer was edited.
Independent strict checks and 109 focused unit cases passed in 4.44 seconds.

A separate negative race control confirmed that Python dictionary equality masked
a stored phase's integer-to-float change and allowed the old CAS to proceed. The
phase guard now compares canonical raw JSON, preserving numeric kinds and field
presence rather than overwriting changed/malformed state. An actual storage race
control is added for Memory/PostgreSQL. The combined follow-up gate passed static
strict eight-target and Ruff checks but stopped in wrapper database preparation:
the owned PostgreSQL service reported recovery mode, before any tests collected.
Positive CAS and wider replay integration qualification therefore remain pending;
no parent activation or complete native/worker migration is claimed.

### Qualified paired native file/source caller checkpoint

The final native cohort joins current parent 855a0c7a and fresh-cache writer f2552fa0
normally. Its actual Source/Runtime/Session/Telegram/native/replay/reference/E2E
and Scheduler selection passes 1,605 units in 51.47 seconds, zero skips, p90 .284
seconds and maximum 3.372 seconds including fixtures. The complete 167-case actual
Linux integration selection passes in 65.28 seconds with zero skips and unchanged
production source. Ten independently strict source/test targets pass; scoped Ruff,
formatting, source comments and diff checks pass. The final complete-event version
of the new dedicated expiry fixture also passes its Memory control separately.

The enlarged consumer proof exposed one additional actual producer defect:
Scheduler.expire_calls persisted CLOSED/EXPIRED but queued its earlier OPEN row.
Ingestion delegated exactly that close/queue block; it now emits the actual returned
persisted row. The dedicated real-runtime expiry/queue/consumer test compares the
canonical stored snapshot and validates actual CallExpiredMessage admission on
Memory/PostgreSQL; the original real replay expiry/divergence oracles remain intact.
No schema guard, actual timing or frozen fixture was relaxed. The first new PG
control failed because its authored event was incomplete; the existing complete
synthetic event factory corrected only that fixture before the whole 167 passed.

The earlier setup refusal during autonomous PG recovery, preceding negative CAS
race, 22 replay annotation refusals and 7 old authored migration-shape failures
remain separately retained evidence. No service, deadline, worker limit or guard
was changed for this cohort. A raw canonical phase revision now prevents equal
Python numeric values of different JSON kinds from masking concurrent mutation.

This qualifies native input file preparation and its actual callers; it does not
complete whole SessionService or Runtime. Eighteen Session signature
gaps, untyped aggregate/body/tool/usage/event/barrier flows, required output-file
sender migration, whole worker dispatch/body typing and assigned vendor programs
remain active. The worker annotation is not an independently strict body claim.
New independent reaction/milestone adapters and narrowed queue dependencies will
join only after this preserved checkpoint, through their canonical peer commits.


### Clean current-parent output-file caller companion

This separate cohort starts at parent f24bd4a9 and normally joins ingestion's
9b4443dd bound sender, without inactive native-input/worker/reaction APIs. The
Session native-tool boundary admits filename/caption/call identity and resolves
its actual owned journal message into the required question/session target before
native GET. It never recovers a target from last_payload or a list position.
Question identity is checked before reading the scoped output. The sender then
rechecks the authoritative question and every matching delivery row under its
question lock before a Telegram POST.

The delegated sender lookup and send share one locked context producer. Known
SENT/UNKNOWN outcomes return before native GET and need no active credential or
unused bytes. Fresh lookup validates availability without returning credentials;
actual send rechecks state. The use case keeps downloaded content opaque bytes,
preserves the existing read attempts/delay/REST deadline and document limit, and
validates the returned closed outcome before native-tool JSON serialization.
Malformed cached model instances refuse. Binding changes during the native read
cannot produce a Telegram POST or a GENERATED record for a foreign question.

All 357 affected Telegram/ownership/migration/E2E units pass in 14.28 seconds,
zero skips, p90 .144 seconds and maximum .993 seconds. The complete 45-case actual
Linux integration selection passes in 20.96 seconds with zero skips/source guards,
including actual PG binding/race/receipt cases and four E2Es. Independent strict
helper/contracts/new-test three targets passes, as do scoped Ruff/format/comments/diff checks.
The selected default Inbox/composition check reports only the imported baseline
truth.py _condition_cache annotation at 6031; no suppression or unrelated edit was
added. The first new PG controls omitted required at clocks; only their authored
fixture receipts were completed before the whole actual selection passed.

This does not close all SessionService typing. Constructor/aggregate/tool/event/
usage/barrier flows, its existing unconfigured sender state, presentation-limit
coupling and whole worker migration remain open. The separately qualified native
input checkpoint db160129 stays immutable and is not an ancestor of this clean
output companion. Exact retained maps/None sites are recorded in the machine
exception ledger rather than a blanket legacy exemption.
Worker queue completion/failure checkpoint (inactive dispatch branch)

RepositoryWorkerQueue now guards completion and failure writes with actual job
identity, kind, RUNNING status, claimed attempt and payload digest. Results remain
JSON only at the actual ops.jobs.result persistence boundary; closed application
result consumption is still pending in the whole worker migration. Sparse updates
preserve retained receipt bytes and replace only owned lifecycle clock families.
A missing completion clock is the pending phase, not an occurred timestamp.
ApplicationBackoff must match the claimed attempt; a declared delivery delay of
zero remains zero. Unknown/mutated ownership cannot acknowledge or fail another
claim, and a second completion cannot overwrite the first result.

Independent strict source/test three-target checking and Ruff pass. The first
27-case unit run exposed an invalid attempt to use an occurrence-only helper for
the pending completion clock (26 passed/one failed); after phase-correct sparse
replacement all 27 units passed in 4.92 seconds and all 14 actual PostgreSQL cases
passed in 8.53 seconds, zero skips with wrapper source guards. Evidence is external
worker-completion-fixed-unit and worker-completion-actual. This qualifies only the
queue lifecycle adapter; Runtime.worker activation, send_message orchestration,
remaining Session body typing and assigned vendor work remain open.

Typed actual message producer checkpoint (worker activation still held)

SessionService.dispatch_message consumes MessageDispatchRequest with a closed
AgentMessage, verified original payload digest and original queue wire evidence.
Business routing reads the typed variant through NativeSessionResolution.from_message;
ModelContext.message consumes the admitted request. Only context rendering and
journal persistence use its original wire, preserving omitted versus explicitly
null metadata rather than reconstructing evidence from normalized DTOs. Scout
consumes the closed outcome and reads its actual authoritative session reference.
The existing send_message caller admission/session-read boundary remains until
the two reserved replay callers migrate. Native accepted, pending intent and
historical indexed/unreported acceptance stay separate outcomes; refusal keeps
its actual both-ID question target. No new native acceptance is inferred from an
existing session row.

ClaimedWork now retains its actual JobEnvelope and verifies both the original
payload digest and re-admitted concrete command before dispatch. Constructed
command substitution and wrong digests refuse. Independent strict seven targets
pass (message dispatch, context, native session projections, worker contracts,
worker repository and their two focused tests); scoped Ruff/format pass. Whole
SessionService independent strict remains explicitly open, with its diagnostic
retained externally; this checkpoint does not claim the aggregate is complete.

Final source passed 128 connected units in 14.96 seconds (p90 .265 seconds,
maximum .592 seconds) and all 29 collected actual PostgreSQL cases in 8.60 seconds,
zero skips with unchanged wrapper fingerprints. Actual acknowledgment controls
cover ACCEPTED/EMPTY/UNREPORTED, replay clock precision omitted versus explicit
null, request JSON roundtrip, unchanged original native idempotency key and one
POST on retry, plus original journal payload presence. Earlier alias-revalidation
failures and new synthetic PG fixture admission failures remain diagnostic
evidence; only authored fixture provenance/cursor/publication declarations were
completed, with no historical evidence padding or production/schema relaxation.
External proof prefix is worker-message-final3. Whole worker command dispatch,
remaining Session body/tool/usage/event responsibilities and vendor work stay open.

Typed claimed-job admission checkpoint

runtime_admission.worker_admission projects only admitted AgentMessage/Scout
variants into the existing AdmissionCandidate, using actual envelope job ID,
priority and slot_waiting_since or original creation clock. None denotes a
non-session command, with no slot write. SessionService.admit_candidate requires
and revalidates the named candidate; the existing queue-wire caller delegates to
that same use case. Native resolution and admission share agent_message_target,
so routing does not reverse-serialize a model into an anonymous business input.
Independent strict four targets/Ruff pass, plus 16 connected units in 6.92 seconds
and all 13 actual PostgreSQL cases in 8.51 seconds, zero skips/source guards.
This is a whole-worker dependency checkpoint; dispatcher activation remains open.

### Clean current-parent output-file caller companion

This separate cohort starts at parent f24bd4a9 and normally joins ingestion's
9b4443dd bound sender, without inactive native-input/worker/reaction APIs. The
Session native-tool boundary admits filename/caption/call identity and resolves
its actual owned journal message into the required question/session target before
native GET. It never recovers a target from last_payload or a list position.
Question identity is checked before reading the scoped output. The sender then
rechecks the authoritative question and every matching delivery row under its
question lock before a Telegram POST.

The delegated sender lookup and send share one locked context producer. Known
SENT/UNKNOWN outcomes return before native GET and need no active credential or
unused bytes. Fresh lookup validates availability without returning credentials;
actual send rechecks state. The use case keeps downloaded content opaque bytes,
preserves the existing read attempts/delay/REST deadline and document limit, and
validates the returned closed outcome before native-tool JSON serialization.
Malformed cached model instances refuse. Binding changes during the native read
cannot produce a Telegram POST or a GENERATED record for a foreign question.

All 357 affected Telegram/ownership/migration/E2E units pass in 14.28 seconds,
zero skips, p90 .144 seconds and maximum .993 seconds. The complete 45-case actual
Linux integration selection passes in 20.96 seconds with zero skips/source guards,
including actual PG binding/race/receipt cases and four E2Es. Independent strict
helper/contracts/new-test three targets passes, as do scoped Ruff/format/comments/diff checks.
The selected default Inbox/composition check reports only the imported baseline
truth.py _condition_cache annotation at 6031; no suppression or unrelated edit was
added. The first new PG controls omitted required at clocks; only their authored
fixture receipts were completed before the whole actual selection passed.

This does not close all SessionService typing. Constructor/aggregate/tool/event/
usage/barrier flows, its existing unconfigured sender state, presentation-limit
coupling and whole worker migration remain open. The separately qualified native
input checkpoint db160129 stays immutable and is not an ancestor of this clean
output companion. Exact retained maps/None sites are recorded in the machine
exception ledger rather than a blanket legacy exemption.

Inactive atomic claim contract prerequisite

JobMetadata.claim_token preserves genuine historical/not-yet-claimed absence as
omission, including bare JSON dump/read roundtrip; explicit reported null refuses.
Current JobClaimScope and InvalidJobClaim require an observed nonblank token.
Scope.matches checks the original raw payload digest and actual RUNNING identity,
attempt and nullable observed recovery count before envelope admission. Recovery
count None is unreported context, never a generated zero or the authority token.
The backend must mint a fresh actual token on every claim, including same-attempt
recovery and refunded admission/dependency deferral.

QueueStore defines transition_claimed_job with a synchronous fresh-JobEnvelope to
closed JobClaimMutation callback under actual row atomicity, and separate token/
original-revision quarantine for malformed rows. Mutation.apply_to replaces only
reported lifecycle scalars and complete named timestamp families on original raw
wire; omitted fields remain untouched, explicit result None means actual void,
and terminal mutations require an occurred completion clock. Optional patch fields
represent an unrequested write, not guessed provider/domain facts. A None backend
outcome means no matching owned RUNNING claim and no effect; physical implementation
and consumer qualification are pending and must not be inferred from this API.

This pure checkpoint is based on canonical 025240b8, with no held worker/native
extras. Independent strict source/test three targets, Ruff/format and 21 pinned
native pure cases pass (10.30 seconds total, zero skips/source guards). Proof prefix
job-claim-mutations-roundtrip; backend Memory/PostgreSQL fencing and three stale
controller races remain held for the paired cohort. These claims concern atomic
queue acknowledgment/failure/requeue, not a distributed external-effect lease;
network calls must not run inside this callback/row lock.

The mutation's last_error field retains the existing storage str-or-None contract:
RepositoryQueries.fail_job writes str(error), and str(Exception()) is genuinely
empty. It must remain distinct from unreported None rather than become UNKNOWN.
The current worker separately records the actual exception class name through
WorkerFailureRequest.error_class. A direct empty-versus-None control extends the
pure family to 22 cases, all passing in 6.34 seconds, zero skips/source guards;
strict source/test and Ruff/format pass. No terminal-reconciliation policy is
changed by this lower-level field correction.

Controlled generation shutdown uses the same claim fence. The sparse mutation
contract adds only existing interrupted_at and CONTROLLED_GENERATION_STOP facts,
reported together for QUEUED state with an actual interruption occurrence. These
fields remain untouched when unrequested; no old clock/reason is reconstructed.
The pure family now passes all 23 pinned native cases in 5.94 seconds, zero skips
with source guards; strict source/test and Ruff/format pass. Actual controlled-stop
claim snapshot/cancellation/refund and stale-token consumer proof remain pending.

Atomic worker consumer checkpoint (whole dispatcher still held)

RunningJobClaim now carries the required actual token and observed nullable
recovery count through canonical JobClaimScope. RepositoryWorkerQueue delegates
completion/failure/requeue/expiry to transition_claimed_job with synchronous
named mutations. The original result is validated/cloned before the first await;
expiry's fresh not-due condition uses a named no-write exception, not a generic
malformed-state catch. A current-scope read can refuse already-stale dispatch, but
that observation is not an external network lease. Terminal effect commands use
closed actual commands and the matching terminal token/subject; no normalized
DTO is passed back as an anonymous service input.

The earlier actual PostgreSQL evidence worker-claim-fencing-before contains both
stale completion after recover/reclaim and read/recover/reclaim/write failures.
The paired consumer now refuses old tokens after recovery and refunded-attempt
ABA. Its concurrent control pauses the real put while the actual row lock is held,
observes the recovery UPDATE waiting for a PostgreSQL lock, then verifies valid
completion linearizes first and recovery changes zero rows. This replaces the old
get hook because the real atomic storage read is now SELECT FOR UPDATE; no stale
new claim is overwritten and every owned test task is drained on failure/cancel.

On the exact paired source, all 35 focused units pass in 17.62 seconds (p90 .044,
maximum .241 seconds), and all 21 collected actual PostgreSQL cases pass in 22.88
seconds, zero skips with wrapper fingerprints. Independent strict five targets
and Ruff/format pass. Proof prefixes worker-atomic-consumer-unit/actual; the
foundation backend's 49-case physical proof remains attributed to its582 prefix.
The complete Runtime worker, controlled-stop consumer, real producer/caller
migration and remaining Session/vendor responsibilities remain open. This
checkpoint is not whole-application activation or a cross-controller network lease.

Dependency reads retain their separate historical phase: the unchanged two-field
DependencyState now lives inward in contracts.jobs and is exactly re-exported by
runtime_dispatch_contracts. A known DONE dependency needs its actual job ID and
status, not an unrelated current claim token/full envelope. The one canonical
model prevents a reaction/dispatcher import cycle; actual readers must still
match the returned identity against the requested storage key. All 24 pure cases
pass in 6.35 seconds, zero skips/source guards; independent strict3/Ruff pass.

Closed whole-worker API checkpoint (caller cohort still held)

Runtime.worker now claims through RepositoryWorkerQueue, quarantines invalid
commands with the observed token/raw revision, and dispatches the closed command
variants through actual Pipeline/Session/Market/Delivery/Scheduler/Inbox use cases.
There is no handler dictionary or fabricated active_jobs fallback. The only
JsonValue result is the actual durable ops.jobs.result serialization boundary;
source, message, truth, condition and scout results are revalidated and bound to
the requested identity before serialization. Existing void ports return explicit
None without inventing a delivered/provider-success receipt. Reaction execution
receives ReactionJobInvocation.from_envelope with the authoritative original
claim, not an identity guessed from its payload.

All completion/failure/dependency/admission/delivery deferral and due-expiry writes
use the token-fenced atomic port. The current-scope read before dispatch is an
observation, not a network lease; native/Telegram effects retain their own actual
ownership and idempotency responsibilities. Active work is indexed by actual
(job ID, claim token), so an old worker finishing cannot erase a newer same-job
scope. active_jobs is a read-only view of those real owned records. Controlled
shutdown snapshots scopes before cancelling/draining owned tasks, then refunds
only still-owned attempts with the actual interruption clock. Unavailable commit
acknowledgment is not reported as success and retry cannot refund twice.

Malformed EVAL clocks are now admitted by the existing Timestamp contract at the
queue DTO boundary, so they quarantine once before the evaluator rather than
consume retries for a permanent malformed command. The original no-clock-guess,
no-evaluator-call/no-condition-write assertions remain. The unused duplicate
reaction DTOs were retired; strict flag tests now use the actual ChangedReactionJob.

Independent strict six targets including the complete Runtime source and new
controlled-stop caller tests pass; scoped Ruff/format pass. Configured whole-source
mypy has only the separately owned truth _condition_cache annotation error (199
sources); no Runtime errors remain. The selected 54 functional units pass in 23.70
seconds and every case is below 10 seconds, but their subset p90 is 1.692 seconds
(max 2.572). The failed subset-checker receipt is preserved, not a global unit-gate
claim. Per the lead's docs/TESTING interpretation, the full default unit selection
must still establish global p90. No fixture shrinking, caps or deadlines changed.
All 28 selected actual PostgreSQL cases pass in 34.16 seconds, zero skips/source
fingerprints. Proof prefixes whole-worker-controlled-smoke-unit/actual. The first
smoke's missing-clock error-name assertion was updated from ValidationError to
permanent INVALID_QUEUE_ENVELOPE and now additionally requires one attempt.

This is an inactive concrete API for peer constructor/dispatcher caller migration.
Existing raw handler/object.__new__ fixtures, all real producer/recovery callers,
full financial/E2E/Go cohort and global timing qualification remain required.
The existing strict-test dependency on untyped Scheduler.tick remains visible;
no suppression was added. Remaining Session body/tool/usage/event work and the
assigned vendor programs are still open.


### Whole-worker real lifecycle caller continuation (held)

The runtime timeout, malformed-kind continuation, failed dependency, generation drain
and immediate-stop controls now compose actual Runtime ports and resources instead
of assigning a handlers map to an unconstructed object. Domain evaluation delegates
to the actual Scheduler; only owned intake-loop and gateway-start boundaries are
recorded. The immediate-stop fixture declares the supported integer shutdown grace
0 before immutable Settings capture (the retired fractional fake was not a valid
Settings contract); the positive drain remains 1 second. Original attempt refund,
clock, frozen-session/tool-result and explicit barrier assertions remain.

The expiry-after-real-claim and question-refusal-persistence continuation cases now
use actual Runtime dispatch. Later independently owned work is a real condition
evaluation, not a mock FILTER result. The admission deferral uses the configured
cap of two and two actual occupied sessions, preserves creation precision and
refunds the attempt, and proves zero dispatch. Its prior alias-only job extras had
no actual queue producer; the fixture now uses the admitted canonical bucket clock
with an actual anchor and asserts the replacement removes that stale anchor.
The separate timestamp-alias storage adapter regressions remain unchanged.

Five lifecycle caller cases pass (21.53 seconds); all 13 precision/refusal unit cases
pass (19.40 seconds), zero skips with source guards. These are selected functional
proofs, not a full default-unit timing claim. Complete worker caller and actual
financial/E2E/Go qualification remain held.
Actual PostgreSQL refusal-write-failure/restart selection also passes both collected
cases in 23.62 seconds, zero skips/source guards. The changed refusal module and two
real composition helpers pass independent strict checks; scoped Ruff/format pass.

The remaining gateway-before-intake fixture now uses actual composed resources and
records only the gateway start barrier/real close order; budget binding and release
assertions remain. The pending-dependency control now queues an admitted evaluation
command and proves its actual evaluator is never called while the predecessor is
pending, without error or attempt cost. Gateway control passed; both failed/pending
dependency cases pass in 19.20 seconds, zero skips/source guards. The first pending
control exposed a fixture-only datetime/string assertion mismatch, preserved in
worker-gateway-dependency-callers; parsing both declared clocks corrected it without
changing the original one-second deadline or application policy.

Actual running-session admission caller migration now composes Runtime and all
required ports rather than assigning a Session-only handler fragment. It retains
the original real SessionService/native transport and delegates the reconciliation
observation to the actual failure repository. Both Memory/PostgreSQL fairness,
ordered wake, running-session bypass, unchanged source/event clocks, all jobs DONE
and original one-second barriers pass. The slow admission case binds the actual
Session service and blocks only its admission boundary, preserving 50 ms timeout,
zero dispatch, original expiry and attempt refund. All three actual admission
cases pass in 22.17 seconds, zero skips/source guards; Ruff/format pass. The older
module's untyped fixture helpers remain explicit test typing debt, not strict proof.

Both actual external-capture bounded-incomplete→PARTIAL/FAILED history controls now
run the closed worker on actual composed Runtime ports, sharing the test's actual
Market/Truth/Scheduler instances and retained capture transport. All original
capture request/history, confidence, timing, request independence and stored result
oracles remain. Both cases pass in 20.47 seconds, zero skips/source guards. Initial
setup incorrectly attempted to close the pure MarketTruthService; removing that
nonexistent cleanup call fixed the fixture only (first diagnostic preserved).

All six Memory/PostgreSQL delivery ordering/contention controls now run the closed
worker with actual composed required ports and the original actual DeliveryService.
Recording wrappers delegate the real delivery and stop only after its observed
outcome. The simulated application clock is passed through the real collection-clock
boundary; no provider retry delay is invented. Original seven scheduling deferrals,
attempt refunds, HTTP failure/retry count, expiry preservation, cancellation/lock
lifetime, ledger phases and one-send/idempotency assertions all pass in 59.03 seconds,
zero skips/source guards. All handler-map/object-fragment worker construction is now
removed from test_runtime and test_session_admission; peer reaction/X/market-refresh
caller migrations and global qualification remain open. The remaining isolated
Runtime.__new__ adapter test does not run a worker and is not a whole-module typing
completion claim.

The combined six-module owned worker/dispatch/fencing/lifecycle unit selection now
passes all 64 collected cases in 31.42 seconds, zero skips/source guards. This is
selected functional proof, not a global p90 claim. An earlier command named a
nonexistent test_runtime_worker_queue.py and collected zero; its exit-5 setup
receipt remains separate, and the corrected selection uses the actual
 test_runtime_dispatch_contracts.py module.

Consolidated actual caller/fencing/lifecycle proof now passes all 33 collected cases
in 68.87 seconds, zero skips/source guards (worker-owned-callers-actual). Independent
strict eight targets pass, including complete Runtime and its wire/dispatch/queue
contracts plus the actual helper/refusal/controlled-claim targets. Scoped six-file
Ruff and formatting pass. Tested functional prefix b6a681c0 is unchanged except
this report. Complete peer reaction/X/market-refresh/E2E producer qualification and
full default-unit timing remain outstanding; this checkpoint is not global release.

### Completion-lineage correction (separate held tranche)

The before checkpoint b9416450 reproduces two actual issues: a missing retry count
is reported as zero in source-completion diagnostics, and a failed tool input with
no retained attempt is retired as if it were known attempt zero under a current
attempt-one owner. The corrected-/v1 supersession before control fails only the
unknown case while known zero passes; original negative evidence is preserved.
Fresh dispatch writes explicit zero and retry writes increment an admitted count;
existing outbound and input-ownership models already preserve missing counts as
unknown. No counter/body is reconstructed from list position or another message.

The inward session_completion contracts carry the consumed message/attempt facts,
source-completion records and pure older-attempt policy. Actual settled-turn
callers use typed entries and serialize only at the persisted diagnostic/cache
boundary. Missing counter is None; known zero stays zero. Supersession requires
the actual same owner, both observed ordered counters and a distinct actual
replacement input. Unknown facts cannot authorize retirement. Native queue facts
are still retained, and independently proven terminal completion remains free of
unused retry/body requirements: count None/0/1, exhausted partial body, cached
repeat, one history GET/no POST/no retry rewrite all pass on Memory and PostgreSQL.
The remaining raw execution receipt/barrier/settled orchestration is still open,
not hidden behind a generic DTO or claimed whole-module completion.

All 79 connected units pass in 18.47 seconds and all 32 actual completion/workflow
cases pass in 20.36 seconds, zero skips/source guards. Independent strict contract
and pure-test targets pass; scoped Ruff/format pass. Configured 200-source mypy has
only the separately owned analysis _condition_cache error. The new actual test
module still calls the existing untyped barrier/completed-turn methods; no strict
whole-caller claim or suppression is made. Inventory now records 16 remaining
Session signatures, zero Runtime signatures; body/caller/vendor scope stays open.

Initial actual failures exposed incomplete newly authored PostgreSQL fixture rows,
not product permission to weaken schema: five lacked required session metadata;
then two lacked the declared source-origin FK, and the default synthetic source
helper had nullable publication precision rejected by the current SQL constraint.
The new source origin now explicitly authors its publication/time precision and
full request metadata. Unreported agent version/last-item time use explicit
absence reasons, opened_at is the actual fixture construction clock, and the new
local cursor is declared zero. Existing/frozen payloads and original attempt/body
controls are unchanged; no missing retry count was padded. All setup receipts stay
separate from the final green complete case set. Scalar version admission also
uses the canonical strict integer guard, rejecting float/string/bool lookalikes.

### Inactive native barrier result closure checkpoint

On separate cd4fca7f review ancestry, execution_barrier now requires a named
CompletionSessionSnapshot and returns NativeExecutionBarrier; settled_turn and
completed_turn return the same session-bound closed result. There is no raw-map
compatibility union or indexing facade. Actual orchestration consumes named
journal/tool/receipt/queue-state facts, correlates every requested response before
any persistence, and rechecks the locked current session before settlement.
The pinned managed API session-executions.ts declares exactly execution_id/state/
settled_outcome, unique ordered ids[] of length 1–64 and IDs at most 191 characters.
Those constraints and state/outcome agreement are admitted explicitly; UNKNOWN is
preserved, never changed to RUNNING/ENDED. Only queried state observations are
merged onto the fresh locked state, preserving unrelated current entries.

storage_record is the actual persisted/captured report boundary and excludes the
internal session_id while keeping all original external keys and actual optional
receipt-field presence. incomplete() revalidates a conservative false result without
fabricating a native pending/failed state. Missing retry counters and partial unused
bodies remain valid. Known superseded-input receipts retain their original two
writer variants and reported optional fields; no failure or replacement identity
is guessed for old declared markers. Empty associative indexes mean no recorded
authorizing fact, not invented history. Actual return/receipt clock strings must
be aware; absent unused receipt clocks remain unreported. Provider states and
actual call/session identity must agree with their index/query scope.

Owned caller assertions now use typed properties and explicit persisted-proof
serialization. The retention fixture no longer mocks both completion methods to
return true with zero accepted inputs: it authors the completed native input,
terminal receipt/state and independent cached proof that its idle-session premise
requires, preserving unknown retry count and all original archive/no-I/O/time
assertions. Actual execution-page tests are authored for 64+1 batching, wrong
order/missing rows/contradictory outcome/extra fields and second-batch failure with
no partial write. Replay's sole completed_turn consumer is reserved to analysis.

This is API synchronization only, NOT a qualified activation. Source Session mypy
and independent pure-contract targets passed before the first functional gate;
that Linux gate exited during dependency copy before pytest because the owned VM
exhausted inodes. Its complete setup log is preserved, quality owns diagnosis,
and all new qualifiers are held as instructed. No retry, cleanup, environment or
service change was made here. Worker14a and completion523 remain immutable and
separate, and financial/E2E/global worker qualification remains outstanding.
Caller inventory additionally reserves ingestion's two Telegram barrier sites and
analysis's replay consumer plus its old empty-success completion mock. The actual
native-retry completion assertions now use the typed result properties, retaining
all old/new exhausted input, restart and no-repeat effects. Barrier validation
also requires pending/failed lists to match the actual admitted queue states.
These changes remain unqualified under the shared inode hold; no tests were
silently skipped or reclassified and no compatibility map was restored.
Static review during the explicit qualifier hold added consistency refusal for a
reported prior-tool retirement whose queue state is not FAILED, whose execution
identity differs from its index, or whose replacement equals the retired input.
Genuinely omitted old receipt metadata remains unreported; no unused body/counter
was added. Independent strict four targets now pass (contract plus new pure,
actual-boundary and lineage caller modules); default checking of sessions.py and
scoped Ruff/format pass. These are static results only; functional qualification
still waits for the parent's actual retry and quality's seed checkpoint.
The completion snapshot also enforces the already established active-owner invariant:
a non-null active pointer must name one actual distinct journal row. Settlement
admits the current record before recovery can post anything, retaining valid
origin-only/pre-input absence. New pure and actual-boundary controls cover wrong,
empty and absent journals and zero native/recovery effects; these remain pending
functional qualification. This is enforcement of the existing ownership contract,
not a claim of a newly reproduced public defect. Static four-target/source checks
continue to pass; heavy qualifiers remain explicitly held by the parent.

### Scoped native observation compare-and-set

Checkpoint 4a242c32 reproduced both stale-write cases on Memory and PostgreSQL:
late RUNNING overwrote newer COMPLETED, and late FAILED overwrote newer QUEUED
from supported image recovery. Each before run had two failures and a passing
concurrent accepted-input completion control. The pinned coordinator explicitly
permits retained-configuration FAILED to queued recovery for the same execution;
a terminal-state priority rule is not a valid substitute for coordination.

The reader now captures its admitted state/host receipt before HTTP, admits every
three-field page/order first, and compares both under the existing session lock.
Only a matching actual observation is committed. Every applied nonempty status
read records a fresh HostExecutionObservation identity, actual scoped queried IDs,
controller collection clock and captured controller versions. This is host
coordination provenance, not a provider revision or invented historical generation;
old absence remains unreported, and explicit null/mismatched scope refuses. There
is no pre-network write or network wait inside the session transaction. Current
accepted tool inputs are re-read before the locked decision. A rejected stale
write derives the result from fresh admitted state, without regressing records or
claiming that its own response was committed. Cached reads without HTTP mint no
new observation. The new private namespace is excluded from agent projection.

All 27 bounded unit/pure controls pass in 6.77 s and all four actual PostgreSQL
races pass in 7.98 s, zero skips/source guards. The same original races are green;
added ABA proof returns cached values to the original RUNNING state but still
rejects the late response using genuinely new observed receipt identities. The
fresh accepted-input control still prevents old-input-set completion. Static
contract/test targets and Session source checking pass; scoped Ruff/format pass.
Actual 64+1, correlation/second-page no-write, full caller/replay/Telegram/E2E and
whole-cohort proof remain required before activation. No parent files changed.
The actual status-boundary pair on unchanged observation source19 now passes all
seven Memory units in 6.86 s and all seven PostgreSQL cases in 7.59 s, zero skips
and source guards. It proves exactly 64+1 queried IDs, correlated ordered states,
no partial write for wrong/missing/order/extra/outcome or second-page failures,
and actual ownership refusal before recovery/native effects. This adds real
boundary proof to the earlier pure contracts and four PG races; replay, Telegram
and full completion/E2E cohort closure remain pending.

### Scoped completion caller integration

The reserved replay consumer now uses the barrier's typed complete fact and explicit storage_record at singular/plural report and checkpoint boundaries, enforcing actual session identity. Its synthetic completed-turn boundary uses actual accepted-input and terminal state/cache facts rather than an empty successful mock. The delegated two Telegram callers admit CompletionSessionSnapshot and consume accepted_input_ids directly, retaining all answer, retired-message, POST count and idempotence assertions. The abort fixture now reports the required nullable settled_outcome field for RUNNING, matching the pinned three-field status endpoint; no production relaxation or retained payload change was made.

Frozen combined de47267b passed all 361 selected units in 19.19 seconds, with zero skips/source guards. Its connected actual set passed 81 of 82 cases in 28.97 seconds: the sole failure was the previously qualified autonomous E2E CapturedMarket fixture's missing instruments forwarding, because this separate completion branch had not yet joined the current parent/worker peer. Status correlation/64+1, stale/ABA/input-change PostgreSQL races, completion/retry/retention/binding, replay/reference/abort and Telegram cases passed. That diagnostic remains in external completion-callers-de472 receipts; no product attribution or whole-cohort green claim is made. Current-parent normal integration and affected caller proof remain required. Independent strict checking of the four precise completion contract/test targets passes.
### Concrete queue adapter moved outward

From caller-qualified worker 0d5bc86e, the concrete JobRepository now lives in
src/tank/job_repository.py. contracts/job_repository.py retains only the precise
ten-method QueueStore Protocol; required envelope/claim/mutation records remain
in contracts/jobs.py. The concrete class AST is identical before and after the
move/formatting (SHA256 4704fe313cd6cfa9e04beff8e93202d8a453d85ba8499b2ed7d82897189db6a8).
External job-repository-relocation-ast.json records both hashes and all 24 actual
caller paths. Pipeline and worker imports split port versus adapter; 22 test
imports use the new outward path. No compatibility re-export, forwarding class,
annotation suppression, storage algorithm or callback policy changed.

All 58 selected existing queue/claim/quarantine/raw-revision/clock/controlled/atomic
units pass in 7.93 s; all 37 actual PostgreSQL cases pass in 10.97 s, zero skips/source guards.
These retain real minimal-store transaction/rollback, malformed original revision,
raw payload/omitted clock metadata, fresh-token refund/recovery ABA and concurrent
row-lock controls. Independent strict 8 targets including both modules and actual
consumers pass; scoped Ruff/format and import search pass. Configured mypy across 202 sources has only the separately owned truth _condition_cache error. The proposed
layout-only assertion was removed before qualification per lead review; no new
mechanical-layout test remains. Quality will add the independently signed new
adapter to explicit strict CI/inventory while keeping the inward port target.

This closes this concrete inward-I/O violation, not the broader generic Repository,
source-adapter or whole-application architecture/typing scope. Historical recovery,
pending-demand and the separate completion-observation behavior correction remain
held and are not implied by this worker candidate. No parent worktree was edited.

### Milestone quarantine caller alignment

On clean parent b026dd0e, the six missing-required-fact milestone queue cases now assert the closed worker's declared `INVALID_QUEUE_ENVELOPE` quarantine result rather than the retired dispatch-time `ValidationError` label. The controls retain FAILED, the exact original payload, absent result, zero HTTP, and no AGENT_MESSAGE jobs; they additionally verify one actual claim attempt and zero event reads before quarantine. No production policy or milestone behavior changed.

The approved read-only dependency runner passed all six Memory cases in 8.71 seconds and all six PostgreSQL cases in 10.41 seconds, with zero skips and unchanged source guards. External receipts are `evidence/agents/milestone-quarantine-{unit,pg}.{log,xml}` under the local tool root. Ruff and format checks pass. Independent strict checking of the existing test file still reports its prior untyped Scheduler.tick call at line 149; this bounded caller repair neither suppresses nor claims to close that dependency.

### Source lifecycle I/O adapter moved outward

On coherent parent b026dd0e, SourceExtractionRepository and its two raw-document
admission helpers _identity/_state now live in tank/source_repository.py. Their
ASTs are identical to the base after formatting, recorded independently in
source-repository-relocation-ast.json. Pure SourceExtractionIdentity/State and the
four-method SourceLifecycleStore remain in contracts/source_repository.py. All
three actual callers (EventTools, SessionService and source lifecycle tests) use
the outward adapter; analysis delegated only its import hunk. There is no
compatibility re-export, behavior change, new layout-only test or storage rewrite.

All 37 existing lifecycle/Session/EventTools completion units pass in 16.45 s;
all 23 actual PostgreSQL caller cases pass in 17.81 s, zero skips/source guards.
Original historical-field/byte preservation, terminal/missing no-write guards,
identity refusal, capture-lock reload/concurrent metadata and actual observation
completion assertions remain. Independent strict three adapter/port/test targets
and scoped Ruff/format pass. Active exception/inventory locations are updated;
quality owns explicit strict CI/global inventory addition while retaining the
pure contract target. Generic Repository relocation is independently foundation
owned. The observation-CAS behavior branch is separate and remains held for its
full caller/boundary cohort; no parent files or immutable evidence changed here.
### Current-parent completion cohort qualification

Normal join of public b026dd0e into the completion/replay/Telegram cohort is 32433c62. Product and callers auto-merged; the three owned report conflicts preserve both cohorts, and the signature inventory is regenerated against the actual merged sources. The previously qualified E2E instruments forwarding is inherited through that normal join, with no copied fixture or assertion changes.

On this exact coherent source, all 361 selected units pass in 19.16 seconds and all 82 connected actual integrations pass in 30.51 seconds, zero skips and source guards. This includes native 64+1/correlation/no-partial-write admission, Memory/PostgreSQL stale observation/ABA/accepted-input races, original unknown-vs-zero and exhausted unused-body controls, workflow/retention/ownership, replay/reference/abort, the delegated Telegram consumers, all four actual E2Es and preliminary archive integration. Earlier de472's sole stale E2E fixture diagnostic remains preserved separately. Independent strict four precise completion targets and scoped Ruff/format13 pass. Configured mypy checks232 sources and retains only analysis.truth._condition_cache's existing missing annotation; untyped bodies elsewhere remain explicit work, not a green whole-application claim.

This qualifies the bounded completion/caller behavior cohort for lead review, not all remaining Session body/tool/usage/event typing, historical recovery/pending-demand, vendor implementation, or the whole application. No parent files were edited.

### Admitted native usage and immutable ledger writer

SessionService.usage now consumes NativeUsageSession and NativeUsageSpan, and actual process_event plus both direct usage tests and the concurrent-change hook use those projections. Pure native_usage contracts distinguish an absent usage report, observed empty report, partial measurements and reported zero. Counters are strict nonnegative integers; reported null, boolean, string and fractional counts refuse. Reported facts are checked against retained raw evidence again before writing. Unknown cache/search/effort/finish facts remain unknown, with the existing missing-field descriptions. Native input/cache counters remain independent: the managed span mapper copies all four, while Node span/cache interfaces permit omitted counters; no provider mode, total, or cache subtraction is inferred.

The concrete writer lives outward in agents/native_usage.py behind a narrow actual put port. Existing subscription charge fields are retained as the declared gateway application policy from config/openma.yaml, separate from observed provider estimates. This does not interpret an estimated/list cost as billing authority or compute token prices. Exact provider measurement evidence now stays in native_usage_evidence, while usage serializes only admitted counters. This deliberate split prevents the storage codec's raw-usage alias fallback from converting an unreported cache/search fact into a physical SQL measurement; actual PostgreSQL column controls cover that case. Unsupported extension keys remain retained as evidence and cannot overwrite the actual usage/session identity or billing fields. Historical stored rows are unchanged.

Frozen source44ce0e5c passed all82 connected units in8.02s and all15 actual PostgreSQL cases in6.81s, zero skips/source guards. These include existing real SSE/direct/concurrent usage callers and real partial/cache0/cache-greater-than-input, extension/estimate, invalid-counter/no-write, unreported/empty, mutation and immutable-ledger controls. Independent strict contracts/writer/newtest3 passes; scoped Ruff/format passes. This removes one more unchecked Session method signature, leaving12 actual Session methods (including constructor, event timestamp, tool/context/invoke, process_event, archive/follow/run and replay-abort) explicitly open. This bounded usage tranche does not claim those bodies or all Session dependencies are strict-complete.

### Retention phase migration checkpoint

Inactive source0f18ea06 migrates can_archive, finish_if_idle, last_message_at, followable and the follower sweep to consumed named phase facts, keeping repository/native I/O outward. Archive denials do not require unused identities, card versions or historical return bodies; terminal eligible phases require their actual scope. Actual call/watch/tool/job/publication/delivery readers check the requested ownership and phase. The actual last-activity Timestamp is retained through a fixed offset rather than relabelled as a collection clock; owned clock-family replacement also removes stale aliases and bucket edges. No activity becomes current time, no absent runtime state becomes RUNNING, and unknown retry/body facts remain unused for independently proven completion.

Before source changes, the status-only event archive control failed with an actual POST while its real lifecycle positive passed (ce0e9cb2,1red/1positive4.63s); unknown and BUCKET(1s)/END activity clock controls both failed because the old writer declared COLLECTION_ONLY (2a551537,2reds4.66s). On0f18 all260 selected units pass23.21s and all59 connected actual integrations pass29.30s, zero skips/source guards. This includes real PostgreSQL retention replacement with unknown terminal attempts/partial body, the original denial/EXIT/preliminary/card ownership controls, Telegram and replay/reference/E2E callers. Independent strict3 new contract/tests and default Sessions checks pass; Ruff/format8 pass. Further single missing-event ownership refusal coverage is being added before declaring this tranche ready.

### Retention phase caller closure

Final behavior sourcebc8d5a8b passes all263 selected units47.23s and all59 connected actual integrations33.39s, zero skips/source guards. The exact single missing-owned-event Memory red was preserved before the fix (4c73c8f2,1fail4.48s with an actual archive POST). It models incomplete captured state; PostgreSQL normally protects committed event references with its FK, so this is not presented as a PostgreSQL missing-parent production defect. The new explicit UNREPORTED/UNBOUND/BOUND binding model prevents both missing resources and absent binding keys from becoming an eventless negative. Fresh resolve's actual explicit null is the known-unbound contract, and the new authored PostgreSQL positive fixture declares that fact directly. No retained row is backfilled or normalized from absence to null.

The final tiny denial-model refinement9f13c650 distinguishes unreported lifecycle/runtime from known nonterminal/not-idle denial; all20 pure phase controls pass3.96s. Session actions/booleans and all actual I/O remain byte-identical to the fully qualifiedbc8d behavior. Independent strict3 new contract/tests, default Sessions and Ruff/format pass. The initial malformed historical deadline hypothesis3eca was withdrawn after finding no writer/captured contract supporting that text; its newly authored test was removed at4c73, history preserved, and actual reported retention clocks still validate. No malformed-clock fallback or blanket exception handling was introduced.

Whole-session observation writer audit is recorded externally in agents/session-observation-whole-writer-audit.json: all24 put_owned_session sites were reviewed against actual shared session locks/rereads or changed-field merging. Initial resolve is under actual dispatch mapping lock before follower start, EventTools association uses the shared session lock, and nested finish/settled barriers use verify_remote=False and cannot write observations. put_owned_session itself remains identity/journal validation plus full put; PRIVATE_FIELDS is only presentation protection. The unlocked replay clock update was not assumed quiescent: the sole replay owner reproduced a real interleave and supplied separate qualified789fd2c3 for normal integration. No all-writer/distributed guarantee is inferred before that peer joins.

This tranche types five more actual Session methods and leaves seven signatures/bodies explicitly open: constructor, event_timestamp, finalize_replay_abort, tool_context, invoke_tool, tool and process_event. The approved usage no-report/selected-model follow-up and coherent replay/progress peer joins remain required before whole activation. Existing ready usage33fa, completione94 and source-adapter876 checkpoints remain immutable; there is no whole-application completion claim.
### Usage phase metadata dependency follow-up

Separate source1f2a8502 follows immutable33fa. The actual process_event consumer recognizes UNREPORTED/EMPTY spans before constructing session usage metadata, so those no-ledger phases preserve genuinely absent model/prompt fields and still commit/deduplicate the real observed frame. Reported spans independently select their actual model before needing a recorded session model; absent historical model/prompt observations remain None, never current configuration defaults. A real write with neither observed model source refuses before storage. SelectedNativeUsageModel records the factual NATIVE_SPAN_MODEL or RECORDED_SESSION_MODEL basis and both source labels; recorded session basis/reason stay separate, without asserting provider resolution or changing the declared subscription policy. Raw measurement evidence/counter separation and billing fields are unchanged.

All five new Memory regressions failed on prior33fa in5.82s (unused missing/nullable model admission), preserved at13e93b8d. On changed source1f2a, all87 connected units pass9.61s (observed p90 .083/max2.474s), and all20 actual PostgreSQL cases pass9.43s, zero skips/source guards. Real Runtime no-report frames with missing metadata progress once and do not invent retry/model/prompt fields or usage rows; observed-model rows retain unknown historical metadata and actual attribution; both-models-unreported refuses before effects. Independent strict4, default Sessions and Ruff/format5 pass. This is distinct from prior33fa's82/15 proof and does not claim a new chronology policy for unattributed reported frames or complete process_event typing.

The separate ready retentioncba branch remains immutable with its five-method closure; this isolated usage branch intentionally retains the older archive source until normal coherent integration. Replay writer789fd2c3 and ops-progress2dac1bf8 remain separate qualified peers for the lead-selected join. Whole Session constructor/tool/context/event and native vendor work remains active.

### Coherent Session caller integration on lead-selected91e

Canonical source60fbe71e normally joins exactly91e49dc4, retentioncba8f6cb, usage-phase27351ab8, replay-writer789fd2c3 and source-progress2dac1bf8. Product files auto-merged. Only append reports conflicted; both complete evidence streams were preserved, active exception paths follow the actual outward facades, semantic duplicate entries were reconciled, and the owned signature inventory is regenerated from merged source. No dependency file was copied, no parent checkout changed, and prior qualified prefixes remain immutable.

The changed-source connected unit selection passes555/555 in26.34s; the separate actual integration selection passes108/108 in33.89s, zero skips/source guards. It covers Session completion/status correlation and real stale/ABA/accepted-input PostgreSQL races, retry/exhausted-body/unknown provenance, activity/retention/archive and clock replacement, actual no-report/selected-model usage, real replay metadata writer interleave, source-progress unknown/zero cases, outward extraction callers, retained replay/reference/abort, Telegram, all four E2Es and preliminary archival. Original effects, deadlines, captured data and financial assertions remain. Independent strict10 selected source/test targets pass, and scoped21-file Ruff/format pass. Configured mypy across238 source files retains only the separately owned analysis.truth._condition_cache annotation error; untyped bodies elsewhere are not claimed checked. These selected receipts are not the full default suite or whole-source timing/architecture proof.

The known unlocked replay whole-session writer is now closed by its real same-session lock+reread helper in this coherent source, rather than by presentation filtering or an assumption of quiescence. The source-progress peer selects actual non-attempt phases before needing an attempt key; it does not invent a retry count, broaden unsupported attempt identities or alter recorded source outcomes. Current host-observation guarantees remain bounded to the audited cooperative writers and proved receipt/state compare-and-set, not arbitrary stale full puts or a distributed external-effect lease.

Seven actual Session methods remain explicitly open: constructor, event_timestamp, finalize_replay_abort, tool_context, invoke_tool, tool and process_event. Broader unknown-attempt reporting identities, historical recovery/pending-demand, vendor bodies and the whole application remain active. A separate usage immutable-write/lost-ack observation is only a queued read-only candidate until reproduced; no hypothetical fix or global completion is implied by this integration.

### Immutable usage commit/frame acknowledgment repair

Separate before844cc317 reproduces both actual partial-persistence paths on real Memory and PostgreSQL: the immutable usage put commits then its acknowledgment is lost, or that usage commit succeeds and the later session-frame write definitely fails. The first usage row is visible while the frame remains unseen. Replaying the actual observe_native_event call then constructs a fresh created_at and fails permanently with ImmutableRecordError. Both Memory controls fail5.70s and both PostgreSQL controls fail6.79s; no clock freeze, fake put result or bypass of the real consumer was used. This behavior predates the usage extraction.

Source0e8fa4b3 now reads the actual immutable receipt before creating a replacement. It binds the real session/native-event key, AGENT/subscription identity, coherent recorded model, admitted counters/search/finish facts and canonical typed/raw observation. New rows retain native_usage_observation as an admitted fact projection, not a purported original-wire revision. Exact matches preserve the first collection time, controller/session/prompt provenance and amount. Only actual concurrent ImmutableRecordError triggers a fresh receipt read and the same strict validation; missing or contradictory data still refuses and never becomes a retry acknowledgment. No generic catch, provider/model guess, row overwrite, artificial clock reuse or new billing policy was added.

Historical decoding is tied to actual producer contracts: public2f3d389a stored the original usage object and processed-at metadata; typed33fa352b split raw native_usage_evidence from admitted counters; attributed27351ab8 added observed model-source labels. Their first rows remain byte-for-byte unchanged in the actual format controls, with no reconstructed historical model-source attestation or generation/clock backfill.

All97 connected units pass9.49s and all30 actual PostgreSQL cases pass11.04s, zero skips/source guards. Original red paths are green; actual two-controller read-empty/immutable-conflict reconciliation, no-row definite failure, counter/model/raw-boolean contradiction and all three prior producer formats pass. Independent strict5, default Sessions and scoped Ruff/format pass. This is a bounded real consumer repair atop coherent1e4, not whole process_event or application completion. The separate seven-body inventory records747 observed source lines,51 dictionary expressions,115 get calls and18 or expressions with their actual remaining roles; those syntax counts are not defect counts or a completed fallback/Pydantic classification.

### Typed replay-abort API checkpoint — caller activation held

Sourcee23031ed migrates the actual finalize_replay_abort use case to ReplayAbortTarget plus required-nullable ReplayAbortFailure and closed ReplayAbortOutcome. Actual session/run ownership is checked under the existing event/session locks and a fresh read. Foreign/unreported scope refuses without inspecting unused failure or journal data. A matching run requires actual failure, unique minimal journal identities/phases and a valid active pointer before any put. Unknown source body/count/acceptance metadata is not invented or required; completed/native/source facts remain untouched. Host-only receipt uses the injected controller/replay Timestamp, while actual empty exception text stays empty. Immutable tuple IDs and strict bool outcomes serialize to the same external JSON list/boolean shape at the eventual report boundary.

All66 selected pure/Memory lifecycle units pass7.00s and all7 actual PostgreSQL lifecycle cases pass8.24s, zero skips/source guards. They exercise matching ownership, None/foreign refusal with deliberately unused invalid journal shape, missing failure, inconsistent owner/duplicate/null journal no-write controls, DONE/FAILED/native evidence preservation and unchanged unknown counter/partial body. Independent strict3, default Sessions and scoped Ruff/format4 pass. This checkpoint is not activated: the sole replay cleanup and three existing direct abort-test calls are reserved to analysis for a normal caller companion and connected replay proof. Prior coherent1e4 and usage-recoverye865 remain immutable. Six Session method bodies remain open after eventual caller closure; tool/context dispatcher coordination and quantified fallback/mapping classification are still required.


### Consumed delivery phases and complete Ops-card bodies

Current application source normally joins the lead-selected9ac5b5ce atf88d3ace; f587a438 migrates the new document-resume fixture to the actual SourceReportRequest/SourceReportRecorded API. The final1916838b difference is only the real PostgresTemplate import/type in that fixture, not product behavior. Source-report RECORDED/DEDUPLICATED observations are never transport SENT receipts. The prior85a-based qualified prefixes remain preserved; no older source was copied over the newer parent.

All20 Delivery functions and all12 OpsCards functions now have complete checked bodies and precise signatures. The inward delivery module contains54 strict named records and two narrow protocols; DocumentDeliveryWork is an application-local record carrying actual prepared bytes. These are consumed snapshots, phase facts, typed render results and actual persistence/HTTP admission models, not a Session aggregate or a dictionary payload inside a typed envelope. The actual OpsCards producer builds typed relevance, notice, event, assessment, map, token, publication and delivery views; policy/rendering and Delivery consume attributes. All60 original OpsCards assertions are equivalent after the exact result-dictionary-to-attribute accessor migration (external AST receipt). Telegram's sole raw notice-sort caller uses explicit recorded_time_of admission. No raw/domain compatibility union or re-export was added.

START prices are required only when a new OPEN call will be written; an existing START, EXIT, inactive decision and other decision phases do not acquire an unused price requirement. Prices retain their actual JSON representation after finite positive validation. Delivered event/card and recipient/item scopes are admitted before their effects, while full original event/card rows are retained only for the guarded storage merge. Actual delivered timestamps remain required for call activation; source precision and bucket facets continue through the existing timestamp codec. The borrowed-versus-owned HTTP constructor behavior remains a real client ownership choice, not an optional missing service.

Declared ordinal phases are explicit. EventTools.publish_card writes GENERATED/attempt0 before enqueue (event_tools.py3355–3358 at the inspected prefix); retired add9f4dd/0d3e0cb2 suppression records carry FAILED/suppressed/attempt0. terminal_failure can also record an actual queue attempt0 before dispatch. These known zeros remain valid and are not network-attempt1, absent counters, list positions or reconstructed history. Pending document retries require their actual sendDocument method/positive ordinal; a missing counter cannot become0. Root's canonical Telegram retry parameter model is reused; absent provider delay selects the existing declared exponential application backoff, without inventing a wire retry_after.

Six actual report-to-delivery document-resume regressions failed on9d825bd2 in4.63s: pending uploads used missing token/chat, a changed recipient, or an unreported historical parent chat, while completed documents unnecessarily required a parent. The eb196bd4 repair determines actual pending work first. Already-SENT or exhausted document phases require no unused credentials or parent receipt. A new sendDocument captures nonblank configured credentials and requires an observed same-notice/channel parent message and the same recorded destination before any GENERATED receipt, persisted attempt or HTTP. A missing old parent chat stays unreported; it is never filled from current configuration. The exact parent destination is the recorded request destination string, not a fabricated provider-resolved chat identity. Existing implicit parent method absence is source-backed: the main notice writer stores no document_id/method, whereas the document writer stores sendDocument plus its actual content-derived document_id. The pending phase validates its stricter method/counter facts; the already-complete phase does not require unused historical parent facts. Original receipts and bytes remain unchanged. No new provider idempotency or exactly-once/remote rollback guarantee is claimed; ambiguous local attempt behavior remains the existing delivery policy.

The inherited operator fallbacks were checked rather than excused by annotations. Six pure rendering controls on de105044 produced3expected failures and3known-zero/known-empty positives in4.21s: unknown attempt count displayed0, and unknown map version/analysis_set became rank0/an empty completed universe.73ca93f6 uses explicit unknown presentation and SelectedOpsMap/UnknownOpsMap; actual0,5 and an explicitly empty analysis set retain their original meaning. It changes no persisted counter, retry, queue, pricing or financial policy. Other nullable presentation facts retain actual absence: an omitted provider date/reply, source title/URL/reason, map cutoff or historical optional provenance is not filled from current configuration. Root review disproved the initial display-only EPOCH assessment: time_of also feeds _deliver_progress first-send due. Those operational fallbacks are removed. A nonempty, actual item-bound history is contractual. Positive spacing requires actual notice creation clocks, an actual recorded card update, a matching own progress SENT receipt, or the explicitly captured scoped parent receipt of a pending adoption. No unreported historical clock becomes current time. Explicit zero spacing is a named immediate phase and consumes no historical clock; covered, unchanged and blocked phases do not acquire unused clock requirements. Fresh adoption records actual parent scope, receipt digest and clock as host coordination evidence; it is not a reconstructed historical provider fact. Display ordering and operational permission must be kept separate. The900-second missing-frozen-interval fallback is the existing declared update policy (config/thresholds.yaml46), not an attested historical threshold; it is not backfilled into the immutable card.

Mapping-role inventory is explicit:51 dictionary nodes in Delivery are storage/query/HTTP/clock-codec serialization and original-row guarded merges;21 in OpsCards are admission projections, actual stage indexes and presentation lookup tables;12 in inward delivery contracts are pure consumed-field admission/identity projections. There are47 get calls across those three files, retained at those boundary/index/presentation roles. RichMessage is admitted into its actual named presentation contract before Telegram serialization. Raw diagnostic/source text is preserved; structured JSON duplicate keys are refused before their projection. No known tool, usage, Session, financial or queue entity was replaced by an arbitrary JsonValue/object/Any container. This inventory is a role audit, not a blanket mapping exception.

On the coherent parent join, all118 connected units pass9.65s onf587 (observed p90.012/max1.833), and all15 actual cases pass14.00s on191, zero skips and unchanged source guards. The actual selection includes PostgreSQL delivery supersession/restart, Ops-card burst/edit recovery, Telegram restart, all four E2Es/preliminary-card effects, and six new real PostgreSQL report/document resume cases with pool close/open or an independently restored authored unreported-parent snapshot. The original immutable rows are retained; no retained fixture payload is edited. Independent strict6 targets at that prefix (whole delivery/OpsCards/contracts and the three new test modules), Ruff/format8 and diff checks pass. Configured mypy across250 source files retains only analysis.truth._condition_cache, owned separately; untyped bodies elsewhere are not claimed checked. Prior f4b alias/ordinal failures, df1 actual4failures, the bb4 setup-only0-test refusal, and all qualified prefixes are retained with their exact heads. The root-authorized cleanup removed only the verified dead own wrapper/CID; no services, VM, daemon, worker count or deadline changed.

The final source inventory reflects the normal parent join: six Session methods remain open (constructor, event_timestamp, tool_context, invoke_tool, tool, process_event) and28 Inbox signatures/bodies remain. The newly reserved inward tool-dispatch/call-owned context seam is next; active-pointer attribution is still only an audit candidate until an actual path proves it. Existing financial, recovery and vendor responsibilities remain under their owners and the lead inventory. This bounded delivery closure is not whole-application completion or whole-default-suite proof.

### Delivery operational clock and frozen-policy review closure

The a826 clock diagnostic retained four actual failures and four positive controls. The corrected source passes all21 focused clock/map/policy controls in4.29s, all109 seven-file connected units in5.26s, and the same15 actual connected PostgreSQL/E2E callers in15.15s, zero skips and source guards. These are selected receipts, not a default-suite timing claim. The earlier118/120-unit prefixes remain attributed to their own selections. No deadline, clock freezing, worker cap, runtime or service change was used.

FrozenDeliveryInterval now distinguishes a wholly unreported frozen policy from a reported object: the latter requires its actual nonnegative update_rate_limit_s, including valid0. The former uses the normative15-minute update policy in03 Technical Design §7.4/table and config/thresholds.yaml46, without backfilling a historical card. Decision codec normal writers omit actionable/tracking_ended; explicit inactive/end writers report false/true. None therefore remains unreported; action-specific consumed facts authorize their own phase, and no actionable=True is invented.

The actual map writer increments map_version and stores event_id:version; SQL001_schema has unique(event_id,map_version). The admitted selector additionally refuses contradictory equal-maximum sets in either array order, retains unknown sets explicitly, and accepts equivalent same-version sets. This is boundary admission, not a claim that a current writer produced duplicate contradictory rows. Display-only oldest-first unknown clock ordering remains separate: Inbox uses it only after actual no-session selection for explanatory text; operational delivery scheduling no longer calls that sentinel helper.

Final changed source independent strict7 targets pass, including all four new boundary test modules; scoped Ruff and format checks pass. The current AST inventory records20 Delivery functions,12 OpsCards functions and the exact boundary mapping sites; syntactic totals are not completion proof for unrelated modules.

### Persisted publication call owner instead of later active input

Separate2a47070a reproduced a real publication attribution defect: an actual pending native call owned the original NEW_ITEM journal input, a later CONDITION_MET input was active, and real publish_card produced a card but omitted the original trigger from its publication receipt. The negative receipt assertion failed in5.35s. The inward ToolCallOwner now admits actual session/call/tool/message ownership, preserves genuinely unreported retry provenance separately from0, and resolves exactly one actual journal input. Reported contradictory attempt facts refuse; this trigger selection does not authorize retries or failed-attempt supersession. EventTools.dispatch retains deliberate current event binding while publication trigger selection uses that persisted immutable owner, never the later active pointer or last_payload.

Missing, ambiguous, foreign-session, foreign-call and changed-attempt owners refuse before publication/card/DELIVER effects. Actual source rows, fresh session storage metadata and the requested call body are authored explicitly for PostgreSQL; an independent real foreign session satisfies the FK so the ownership rejection is tested inside dispatch. Initial PostgreSQL fixture-admission failures (content hash, publication precision, session metadata, then foreign FK) remain evidence, not source failures or schema waivers. All current six Memory controls pass6.31s and the complete current six PostgreSQL controls pass8.11s, zero skips/source guards. Connected146 unit callers pass45.13s before the test-only PostgreSQL fixture completion. The initial connected142/4 diagnostic proved an unused-fact regression: non-publication calls do not consume journal trigger facts, so they retain their original behavior rather than receive dummy input history.

Independent strict admission covers the new inward owner contract; Ruff/format/diff pass. Full EventTools context/signature/result typing and Session.tool_context remain active, not hidden by this bounded ownership fix. The new test composes real EventTools/publication/storage and preserves actual outputs; its imported historical helper and the existing EventTools methods remain explicit untyped caller debt. No full Session DTO, fallback owner, fake retry0, opaque payload wrapper or financial policy change was introduced.

The owner successor additionally closes the source-trigger fallback: actual NEW_ITEM/ITEM_ADDED inputs require their own item_id, while non-source inputs legitimately omit it. A new real publication negative fails on6c0951b2 in5.85s because missing input item falls through to the latest event observation. The phase validator now refuses before effects. Complete current eight Memory cases pass8.53s and complete eight PostgreSQL cases pass9.28s, zero skips/source guards; all original publication/identity/no-effect assertions remain. The test now declares actual EventToolService, PostgresTemplate, AsyncExitStack, Literal phases and None return. Independent owner-contract strict passes; checking the newly annotated test honestly exposes five unsuppressed preexisting untyped dispatch/assessment-helper calls, not Any adapters.

No existing shared inward journal-kind alias was found. AgentJournalInputKind is the closed current runtime message namespace (SourceMessage, UserQuestionMessage, ScoutMessage, metric/timed CONDITION_FIRED, CallExpiredMessage and MilestoneMessage), plus the explicitly retained CONDITION_MET journal phase. It does not reinterpret a missing/unknown kind as a source input. Full typed context/provenance/host/native-call migration remains open; current event binding remains intentionally read from the actual session after record_event.

Report-only follow-up: the stale operator-clock exception prose now agrees with qualified79e3 scheduling semantics; its earlier held wording was not a source-policy exception. ToolCallOwner.retry_count and actual DeliveryService constructor ledger sites were refreshed from qualified class/field ASTs rather than the first same-named function. Original diagnostic/proof prefixes remain retained.

### Actual delivered-decision ownership before effects

Separateadcb846c reproduces four real public Delivery.deliver effects on clean1aac: a selected stored key contains another event's decision or a different dec_id, yet START opens or EXIT closes the alert event's call after HTTP. Four reds and two matching positives complete in4.68s. Source13818927 introduces exact DeliveredDecisionReference admission for active START/EXIT against the actual selected identity and alert event. It checks before GENERATED/HTTP, then re-reads and checks again during transactional activation. Inactive decisions still return without unused identity/prices, other non-call actions retain their existing phase, and existing START/EXIT controls retain no-unused-price semantics. Synthetic positive records now declare the already-known selected decision/event identities; no retained row is backfilled.

The new actual caller controls include independently committed decision ownership changes while the send is in flight: transactional activation observes the changed row and cannot use the pre-send checked snapshot to alter calls. These controls correctly observe one actual mock-provider request; no zero-external-effect claim is made for that phase. The earlier definite foreign scope controls require zero requests/GENERATED rows/call changes, preserve original decisions and call evidence, and matching controls retain ordinary open/EXIT effects. No provider idempotency, transport-ambiguity or receipt-transaction policy is changed.

Current connected117 units pass5.86s. The old-parent actual selection passes22/23 in15.21s; its sole later source E2E failure is the already-qualified unsupported authored venue fixture. The normal a0 parent join retains foundation f72 rather than copying a fixture. Complete coherent23 actual callers then pass15.12s, zero skips/source guards, including all eight new PostgreSQL scope/re-read cases, original delivery/ops/document/Telegram restart, four E2Es and preliminary effects. Independent strict whole delivery/contracts plus activation test passes; the new fully annotated scope test exposes exactly three unsuppressed imported historical complete-event/truth/decision helper calls, which remain actual caller typing debt. Ruff/format/diff pass.

Observed price-unit authority and precise instrument presentation fields remain separate open responsibilities. This identity guard neither fills historical price_basis nor claims unknown units authorize new START. Canonical price-basis producer/caller coordination is pending; full typed tool context and remaining six Session bodies remain active.

### Original tool-owner caller fixtures: native intake versus host publication

On exact7a parent, f62d2868 retains the original denial failure and exposes its actual stored outcome: ValidationError/VALIDATION, native_turn_message_id=None, and the old controller fallback retry0. The seed had no input journal/active input/last payload, then directly authored native tool events; this was not evidence of a legitimate observed native no-source phase. The correction405109fb runs original and denial ITEM_ADDED inputs through real Session intake and observed POST acknowledgments, marks the original end_turn boundary before the next input, and stops/drains only owned followers when explicitly taking over native event delivery. Service cleanup is owned by AsyncExitStack. No journal owner, counter, input acknowledgment or successful barrier is padded into a retained row. The old unknown-owner fixture diagnostic stays recorded, not reinterpreted as actual attempt0.

The three card-template variants exercise direct publication/storage/delivery, not native intake. Their caller is now explicitly system/no-native and the unused two-field synthetic session is removed; no native ledger is fabricated. Real analyst scope/owner positive and missing/ambiguous/foreign/source-item negatives remain in the separately qualified actual owner family. External AST evidence tool-owner-caller-assertions.json proves all12 original denial assertions and all29 card-template assertions remain identical and ordered; only meaningful diagnostics/actual intake-count checks are added. Financial/render/excluded-token, delivery, lifecycle, EXIT, watch and archive effects remain.

Complete affected test_agents/card_template plus real owner controls pass116 units9.22s and all11 collected actual integrations9.39s, zero skips/source guards. The single repaired denial path also passes7.65s before the cleanup-only context refinement; no deadline, worker cap, config oracle or product ownership admission is weakened. Ruff/format/diff pass. This is fixture composition repair, not a legacy owner fallback or full typed-context completion.

### Reported instrument text and configured destination admission

Separate833803dd exposes27 boundary failures in4.97s:21 actual source_summary refusals are missing for numeric/boolean values in the seven known text fields; actual instrument evidence loses its acquisition_market_type label; unrelated source JSON is projected as instrument metadata; and whitespace token/chat reaches ALERT or PROGRESS HTTP. Sourceb10eba29 narrows reported fields to text or genuine absence, handles the actual current evidence field alongside the retained market_type field, and refuses contradictory simultaneous market declarations. Venue/status/assets/contracts remain reported display text, not newly inferred routing/provider identities. InstrumentObservation in contracts/source_checkpoints declares these semantic fields as text; no actual numeric/bool representation exists in the consumed writer.

The actual InstrumentDiscovery source is sealed to S14 (source_items.py191); source.instruments.evidence serializes acquisition_market_type by field name. Structural instrument projection is now limited to that known source. Other JSON-looking source text stays opaque display text and unchanged retained evidence, rather than being treated as an instrument because it happens to have before/after keys. Actual SPOT/PERP_LINEAR and preserved PERP display labels remain distinct reported strings; no market-data unit conversion or financial policy is added.

Both transport routes now capture actual token/destination strings and reject all-whitespace configuration before any GENERATED/attempt/index write or HTTP. Nonblank strings are preserved exactly; this does not assert provider authorization or replace a recorded destination with a current-config fallback. Existing complete diagnostic/ordinary-SENT phases retain their earlier no-unused-credential behavior. The new28-case boundary file plus original callers pass145/145 units6.05s and complete23 actual integrations16.30s, zero skips/source guards. Independent strict whole delivery/OpsCards/contracts plus new boundary file passes; Ruff/format/diff pass. Earlier27-red and all scope/clock/retry proof heads remain separate. Shared observed price-basis authority and full typed dispatch context remain active work, not implied by this display/credential checkpoint.

### Acquisition category is not a physical contract classification

The held ce899 display label was not valid evidence of perpetual kind: InstrumentObservation.acquisition_market_type records collection scope. New actual mixed Bybit parser→InstrumentSnapshot→instrument_changes S14 evidence controls retain all four LinearPerpetual/LinearFutures/InversePerpetual/InverseFutures members, their actual reported_contract kind/mode/settlement and whole response bytes, yet the operator title promotes their shared PERP_LINEAR tag to a perpetual contract. The corrected red2 selection fails all four in4.77s; the first wrong-argument test diagnostic is retained separately and not counted as source defect evidence.

The correction changes only the broad acquisition labels to human-readable «рынок деривативов». Observed physical contract facts remain separate in unchanged raw/provider records; absence does not become perpetual/futures or linear/inverse. No implementation tag is exposed as a product label, no instrument is filtered or removed, and the typed alias/conflict/field admission remains intact. Existing Markdown symbol cleanup is preserved; the new assertion isolates category wording rather than changing that unrelated display policy.

Complete connected150 units pass6.29s, including all four actual evidence-to-presentation controls and the original mixed-source restart/no-filtering test. Complete24 actual integrations pass16.54s, including the unchanged PostgreSQL mixed Bybit scope/physical-kind preservation oracle plus delivery/owner/docs/Ops/Telegram/E2E/preliminary callers. Zero skips/source guards; new test/source strict2 and Ruff/format/diff pass. Earlier145/23 ce899 proof remains only its prefix and does not justify the rejected physical label. Shared price-authority caller activation and full typed context remain open.
