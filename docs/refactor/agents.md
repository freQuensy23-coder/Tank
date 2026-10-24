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
