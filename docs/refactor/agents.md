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
