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
