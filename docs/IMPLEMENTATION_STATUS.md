# Current live integration status — 18 October 2026

**Current implementation and the scoped authorized live integrations pass.** The user selected Jev through OpenRouter and Codex's default auth file. This overrides earlier direct-TypeSafe deployment assumptions; the historical 665-test offline record and primary specs below remain unchanged.

The current [live audit](verification/live-integration-audit-2026-10-18.json) records **700 passed, six intentional Memory-only skips, zero failures/errors across 706 cases (111.56s)**. Real PostgreSQL roles, Chromium, 20 upstream OpenMA probes and four end-to-end tests ran. [Current JUnit](verification/live-phase-tests-2026-10-18.xml) and [suite report](verification/live-phase-suite-2026-10-18.json) retain the exact source hashes; the auditor independently rechecked all 146 files. Current bytes match 145 files; one gateway test file was subsequently line-wrapped with identical normalized AST and 70 focused tests passing. Production and deployment bytes remain unchanged. No known implementation or offline validation gap remains.

Actual authorized proofs cover three Telegram deliveries with reply threading and zero new requests when replaying already-SENT records after restart; 17 of 24 configured EVM contract decimal reads (seven free-plan restrictions, plus one non-EVM not applicable); three actual Codex model calls including a tool roundtrip and nonstream response; and two OpenRouter Jev requests. The Jev model is `typesafe/jev-1.13-20260917`; its upstream patch suffix is not exposed. These samples establish connectivity and the stated behaviors, not calibrated quality or unattended reliability.

The audit caught and closed item-model provenance and calibration-provider compatibility defects. The rebuilt Linux arm64 Tank image `d591b9e137e3ddc806a934e204aa6d68895f397ca0a22c79ff5ea44ee06eaf95` independently matches all 68 current package files; [image/deployment proof](verification/live-image-source-alignment-2026-10-18.json) also verifies 26 deployment files and exact dependency inputs. Codex analyst/scout routing remains through the subscription gateway; replay receives no external API keys.

X account state/list configuration and a Linux KVM host remain unavailable. Real OAuth refresh was not needed or forced; labeled calibration, complete historical packets and 72-hour/14-day acceptance remain unproven. Live probes did not exercise a production microVM or the complete source→model→card deployment. The auditor made no external calls or credential-file reads.

Final build/static checks passed: wheel/sdist, selected Ruff rules (`E4,E7,E9,F,I`), all 96 Python files formatted and diff checks. Temporary probe resources were cleaned; images and evidence remain.

---

# Historical offline implementation and acceptance ledger — 17 October 2026

This ledger follows the **current specifications** (committed Linux/subscription changes plus the current DuckDuckGo update), not the superseded daily-budget/macOS design. It covers all **47 current FR IDs**. The removed FR-P-03 is historical only; FR-P-01 now requires delivery-failure notices only. Subscription analysis has no local token/spending cap, spending notices or midnight pause.

Code and limited tests do not establish production acceptance. No real ChatGPT OAuth state, private X session or Telegram credentials were used in offline verification. The new Linux Compose/KVM deployment and authenticated integrations still need their explicit acceptance checks.

## Authority and change audit

| Document | Current SHA-256 |
|---|---|
| [01_Tank_V0_PRD.md](../01_Tank_V0_PRD.md) | `a93c66f3ee06fedda4a52f65bcd847ab14f65f1cd8b1316f8b1b8b65355c01ca` |
| [02_Tank_V0_HLD.md](../02_Tank_V0_HLD.md) | `60a7a468fbd9edd4fa41ac92a2b922db54c341c8df88f700dcdd6360fe9fd96e` |
| [03_Tank_V0_Technical_Design.md](../03_Tank_V0_Technical_Design.md) | `c29e8c416ec5bd5160bd2b3ebfb00fa475fc6405fb796acfc080e50a247613e2` |

[requirements-delta.json](verification/requirements-delta.json) records the initial **41 changed hunks / 55 changed clauses**, plus the newer **19 DuckDuckGo hunks / 21 clauses**, each with old→new text, implementation owner, code and test evidence. The initial Linux/subscription change is committed in `468d3f9`; the DuckDuckGo update is currently in the working tree; its complete comparison base is `5b9cfa1`, so an empty working-tree spec diff is not evidence of no requirements change. Earlier audit conclusions retaining removed budget behavior are explicitly superseded in [core audit history](verification/spec-audit-core.json).

The Linux deployment implementation uses three Linux Compose services, a same-process subscription Responses gateway, Docker file secrets, private persistent browser state, frozen configuration generations and durable PostgreSQL work. [Operations](OPERATIONS.md), [gateway protocol](CODEX_GATEWAY.md), [core audit](verification/spec-audit-core.json) and [market audit](verification/spec-audit-market.json) contain the detailed boundaries.

## Verification snapshot

**Offline implementation and validation are complete for the current reviewed scope.** All 47 current FR IDs are mapped and all 144 external audit findings have dispositions. No known implementation defect or offline validation gap remains. The [offline conformance record](verification/offline-conformance.json) states the exact evidence and limits; live acceptance remains separate.

- Final whole suite: **665 passed, 6 intentional Memory-only skips; 671 cases, zero failures/errors, 95.45s**. Actual PostgreSQL, Chromium, 20 upstream OpenMA probes, strict end-to-end paths, 40 new raw market cases, three exact truth/reference cases and two review precision cases ran. [JUnit](verification/tests.xml) and [run report](verification/final-suite.json) record the result. Current source hashes were independently rechecked against the run.
- Final Tank image: `03b3631a45f800d879935a622a13481ae4e30a9c1df62a63ed975cec34b5e91c`, Linux arm64. An independent network-disabled container check matched all 68 installed package files to the current workspace; [alignment proof](verification/tank-image-source-alignment.json) also records 25 deployment files and the exact lockfile.
- Root final wheel/sdist build, diff checks, formatting of all 94 Python files and selected Ruff rules (`E4,E7,E9,F,I`) passed. Unrestricted Ruff cleanliness is not claimed.
- Independent raw-path checks cover all 23 support terms with positive and one-at-a-time input contrasts. They caught and closed real basis, reference-price, resilience and review-return precision defects; no threshold or business rule changed.
- Additive budget-retirement migration 015 passed three independent real PostgreSQL upgrade tests, preserving exact expiry and immutable historical notices. Current DuckDuckGo search has the specified version/backend, interval/retries, no fallback and counted zero-cost usage.

## Current requirements

Every row below links **implementation and scoped code/test evidence**. Remaining live and data-dependent acceptance checks are listed separately below. Public-source completeness, semantic judgment, latency and unattended reliability require that evidence. The linked audits retain individual limitations and finding history.

| Current ID | Implementation | Scoped test evidence |
|---|---|---|
| FR-D-01 | [All 34 source schedules,raw-first ingest, first-observed time](../src/tank/sources/__init__.py) | [test_sources.py](../tests/test_sources.py) |
| FR-D-02 | [Durable 15s official/milestone escalation windows](../src/tank/event_tools.py) | [test_sources.py](../tests/test_sources.py) |
| FR-D-03 | [Six-venue instrument/status baseline diffs](../src/tank/sources/parsers.py) | [test_sources.py](../tests/test_sources.py) |
| FR-D-04 | [X post/quote/repost reader; one-time private storage-state import](../src/tank/sources/xsensor.py) | [test_sources.py](../tests/test_sources.py) |
| FR-D-05 | [Public/free market/discovery endpoints; DuckDuckGo is free; Jev paid usage separately identified](../config/sources.yaml) | [test_config.py](../tests/test_config.py) |
| FR-D-06 | [Pinned Jev filter, typed score record, durable routing](../src/tank/agents/pipeline.py) | [test_agents.py](../tests/test_agents.py) |
| FR-E-01 | [Verbatim quote validation and durable raw evidence](../src/tank/event_tools.py) | [test_event_tools.py](../tests/test_event_tools.py) |
| FR-E-02 | [Routing reservations, one-session binding,merge and restart](../src/tank/agents/pipeline.py) | [test_agents.py](../tests/test_agents.py) |
| FR-E-03 | [Public-time provenance, freshness buckets, future-time rejection](../src/tank/event_tools.py) | [test_event_tools.py](../tests/test_event_tools.py) |
| FR-E-04 | [Two stage ladders, evidence/availability and future-plan caps](../src/tank/event_tools.py) | [test_event_tools.py](../tests/test_event_tools.py) |
| FR-E-05 | [Scoped official authority, certainty promotion, denial safety](../src/tank/event_tools.py) | [test_event_tools.py](../tests/test_event_tools.py) |
| FR-E-06 | [Stored-history/search/page tools and analyst playbook](../config/skills/tank-event-analyst/SKILL.md) | [test_event_tools.py](../tests/test_event_tools.py) |
| FR-M-01 | [Seven evidence-backed dimensions; missing proof yields null](../src/tank/event_tools.py) | [test_event_tools.py](../tests/test_event_tools.py) |
| FR-M-02 | [Criterion/class/universe/certainty and proposal gates](../src/tank/event_tools.py) | [test_event_tools.py](../tests/test_event_tools.py) |
| FR-M-03 | [Economic capture chains,evidence caps and public holder-revenue path](../src/tank/event_tools.py) | [test_event_tools.py](../tests/test_event_tools.py) |
| FR-M-04 | [Impact start/ramp/dependencies/materiality/confidence and card output](../src/tank/analysis/cards.py) | [test_decisions.py](../tests/test_decisions.py) |
| FR-B-01 | [Eight-layer walk,quoted causal evidence and dependency candidates](../src/tank/event_tools.py) | [test_event_tools.py](../tests/test_event_tools.py) |
| FR-B-02 | [Credible secondary or explicit per-layer absence reasons](../src/tank/analysis/cards.py) | [test_decisions.py](../tests/test_decisions.py) |
| FR-B-03 | [Full analysis-set gate,4secondaries/2losers,least-repriced and15HOT cap](../src/tank/event_tools.py) | [test_event_tools.py](../tests/test_event_tools.py) |
| FR-T-01 | [Instrument-scoped semantic proof and START confidence gate](../src/tank/market/service.py) | [test_market_service.py](../tests/test_market_service.py) |
| FR-T-02 | [One fixed cutoff,complete bars and all five windows](../src/tank/analysis/truth.py) | [test_truth.py](../tests/test_truth.py) |
| FR-T-03 | [Per-venue/cohort flow,breadth and native-volume weights](../src/tank/analysis/truth.py) | [test_truth.py](../tests/test_truth.py) |
| FR-T-04 | [Fixed-cohort period comparison and exclusion reasons](../src/tank/analysis/truth.py) | [test_truth.py](../tests/test_truth.py) |
| FR-T-05 | [NativeOI,USD price-effect decomposition,synchronized venue cohort](../src/tank/analysis/truth.py) | [test_truth.py](../tests/test_truth.py) |
| FR-T-06 | [Per-venue settled/estimate funding and interval normalization](../src/tank/analysis/truth.py) | [test_truth.py](../tests/test_truth.py) |
| FR-T-07 | [Whole-window liquidation coverage and valid-zero gating](../src/tank/market/service.py) | [test_truth.py](../tests/test_truth.py) |
| FR-T-08 | [Verified books,spread/depth/impact/refill/response/compression](../src/tank/analysis/truth.py) | [test_truth.py](../tests/test_truth.py) |
| FR-T-09 | [Raw/BTC/ETH/sector returns,normal ranges and frozen evaluation references](../src/tank/analysis/truth.py) | [test_truth.py](../tests/test_truth.py) |
| FR-T-10 | [Written repricing bands with documented missing-data handling](../src/tank/analysis/truth.py) | [test_truth.py](../tests/test_truth.py) |
| FR-T-11 | [Move classification,divergence alternatives and decisive facts](../src/tank/analysis/truth.py) | [test_truth.py](../tests/test_truth.py) |
| FR-T-12 | [Confidence/freshness/critical-semantics and weakest input](../src/tank/analysis/truth.py) | [test_truth.py](../tests/test_truth.py) |
| FR-T-13 | [Inventory,unlocks,lending,chain supply and explicit missingcoverage](../src/tank/market/supply.py) | [test_market_service.py](../tests/test_market_service.py) |
| FR-O-01 | [Direction,potential,horizon,scenarios,confidence and conditions](../src/tank/analysis/decisions.py) | [test_decisions.py](../tests/test_decisions.py) |
| FR-O-02 | [Sentence scenarios and no-probability/range linting](../src/tank/analysis/cards.py) | [test_decisions.py](../tests/test_decisions.py) |
| FR-O-03 | [Weak-capture positive short horizon labeledTACTICAL](../src/tank/analysis/decisions.py) | [test_decisions.py](../tests/test_decisions.py) |
| FR-A-01 | [Ordered long-only actions,delivered-call lifecycle and expiry](../src/tank/analysis/decisions.py) | [test_event_tools.py](../tests/test_event_tools.py) |
| FR-A-02 | [Perp/absorption/vacuum/fragmentation/heavy-price blockers](../src/tank/analysis/decisions.py) | [test_decisions.py](../tests/test_decisions.py) |
| FR-A-03 | [Eligibility and analyst-owned PARTIAL deadline](../src/tank/runtime.py) | [test_runtime.py](../tests/test_runtime.py) |
| FR-A-04 | [SENT-time deferred updates,safety bypass,expiry and thread continuity](../src/tank/delivery.py) | [test_event_tools.py](../tests/test_event_tools.py) |
| FR-A-05 | [Strict≤3500chars/diverse3tokens,original shown-token pinning](../src/tank/analysis/cards.py) | [test_event_tools.py](../tests/test_event_tools.py) |
| FR-A-06 | [Hard-rule linter,one prose retry and optional-prose removal](../src/tank/analysis/cards.py) | [test_decisions.py](../tests/test_decisions.py) |
| FR-A-07 | [Immutable versions/bundles/ledger,frozenconfig and actual producing provenance](../src/tank/event_tools.py) | [test_event_tools.py](../tests/test_event_tools.py) |
| FR-R-01 | [All eight checkpoints,cumulative event windows,immutable revisions](../src/tank/scheduler.py) | [test_scheduler.py](../tests/test_scheduler.py) |
| FR-X-01 | [Four automatic scout hooks,evidence/dedup and lowest-priority review](../src/tank/scheduler.py) | [test_scheduler.py](../tests/test_scheduler.py) |
| FR-X-02 | [Scout mutation isolation,operator-only decision,no canonical promotion](../src/tank/event_tools.py) | [test_agents.py](../tests/test_agents.py) |
| FR-P-01 | [Delivery-failure notice after exhausted attempts;no spendingnotice](../src/tank/delivery.py) | [test_delivery.py](../tests/test_delivery.py) |
| FR-P-02 | [08UTC heartbeat with cards,HOTtokens,backlog,lag and actual usagecost](../src/tank/scheduler.py) | [test_scheduler.py](../tests/test_scheduler.py) |

## Cross-cutting implementation evidence

| Area | Code and verification | Limit |
|---|---|---|
| Linux/Compose/secrets | [compose](../compose.yaml), [image](../Dockerfile), [deployment tests](../tests/test_deploy.py), [settings tests](../tests/test_settings.py) | Builds/control-plane checks are distinct from Linux KVM microVM proof. OAuth volume is Tank-only; PostgreSQL/OpenMA publish no host ports. |
| Subscription gateway | [gateway](../src/tank/codex_gateway.py), [gateway tests](../tests/test_codex_gateway.py), [independent boundary tests](../tests/test_gateway_audit.py) | Fixed ChatGPT endpoints, internal authentication, atomic refresh, streaming, reset propagation and secret safety use fake tokens; actual sign-in is the next integration phase. |
| Restart/reload/provenance | [runtime](../src/tank/runtime.py), [settings](../src/tank/settings.py), [runtime tests](../tests/test_runtime.py) | Invalid candidates leave the old generation running; valid ones drain old work. Exact config bytes and producing session versions remain recoverable. |
| Storage/retired budget | [storage](../src/tank/storage.py), [migrations](../src/tank/migrations), [storage tests](../tests/test_storage.py) | Real PG16 append-only/idempotency/restart/upgrade tests; archived budget state is history, not an active policy. |
| Review/outcomes/S1–S9 | [review](../src/tank/review.py), [review tests](../tests/test_review.py), [review CLI](REVIEW.md) | Frozen horizon/beta, strict boundaries, immutable outcome revisions and explicit reviewer checks. Literal S9 requires a delivery record, not necessarily SENT; successful delivery is separately measured. |
| Offline replay | [replay](../src/tank/replay.py), [assertion manifest](../tests/fixtures/manifest.json), [replay tests](../tests/test_replay.py) | Cutoff/pinning/empty-database checks and exact assertion manifests do not replace all27 complete captured historical agent/market packets. |

## Retained actual external and recovery evidence

- [Source smoke](verification/source-smoke.json): **75 public endpoint/parser checks passed; 1 unavailable**, 17 October2026, **16:50–16:59UTC**. No analysis jobs were enqueued; authenticated X was excluded. This is not72-hour coverage or every-item/max-delay evidence.
- [Market smoke](verification/market-smoke.json): **9 public BTC REST adapter paths**, with current-run semantic sample evidence. [Multiplex WebSocket smoke](verification/market-ws-multiplex-smoke.json): **8 endpoint checks**, recorded17October2026 around17:04UTC. These are narrow instrument/time samples, not all181series or full-window historical completeness.
- [Actual recovery record](RECOVERY_VERIFICATION.json) and [explanation](RECOVERY_VERIFICATION.md): PostgreSQL16.10 `pg_dump`/`pg_restore` of isolated Tank and OpenMA databases; original evidence tree removed, snapshot relocated, both restore exits0, immutable bundle/source/event/job/transcript evidence recovered. Synthetic records were used; this does not prove production disaster recovery or recover real encryption/OAuth secrets.

## Acceptance still required

| Gate | Remaining actual evidence |
|---|---|
| V0-A | 72hours unattended coverage of34sources/market paths; failure rate<1% per path; authenticated86-account X coverage; actual semantics/archive checks. |
| V0-B | Actual200 labeled items and score evidence; routing precision/recall≥0.90, crypto recall≥0.95, freshness agreement≥0.90; measured cutoffs remain a live-start prerequisite. |
| V0-C | All27 complete captured historical replay packets with cutoff/pinning and actual agent inputs/results; manifest conformance alone is insufficient. |
| V0-D | Full CoW/World/Ondo/Quant beneficiary fixture outputs and quoted causal evidence. |
| V0-E | Real fixture-source comparisons in addition to formula vectors; saved REST OI values are the case-window authority, archives provide normal distributions. |
| V0-F | Live depth≥3venues×10tokens, first3sell-burst/refill checks, lead-lag and supply observations. |
| V0-G | Real subscription OAuth and refresh; actual OpenMA/KVM sandbox Python3.12, read-only DB and replay network isolation; operator X and Telegram integration; source→session→card delivery with measured90s/3min/8min/60s/10s bounds. |
| V0-H | 14days operation, daily reviews, weekly miss audits and measuredS1–S9. |

No offline checks remain open in the reviewed scope. Real credentials, authenticated service calls and full KVM operation are explicitly separate from build/test evidence.

## Current specification decisions

- **F-16 now expects STRESS** in the current TDD and `fixture_check/RESULTS.md` under unchanged§5.7 normal-range logic. The superseded HEADWIND assertion remains only as audit history; the record-extreme proposal is not adopted.
- F-11–F-14 case OI values come from the saved REST snapshots; archive snapshots are not interchangeable with them. F-14 price percentages use the specified hourly close anchors.
- Queue idempotency is scoped by job type+key so FILTER_ITEM and ROUTE_ITEM cannot block each other.
- Denial/EXIT and stop-following updates retain safety precedence; ordinary updates are measured from actual SENT time and keep the original displayed tokens.
- **DuckDuckGo D9:** no search key or charge; only Jev remains paid beyond the ChatGPT subscription. Search blocking is surfaced after the specified retries, with no engine fallback.
- **No local daily spend/token cap or midnight pause exists in current V0 requirements.** Usage remains auditable; actual subscription rate-limit/reset responses are propagated rather than replaced with an invented local budget.

[External audit disposition](verification/external-audit-disposition.json) retains all 144 clauses, their original text, scoped evidence and limitations. All identified implementation defects and offline validation gaps are now closed. The FAILED lifecycle enum/prose inconsistency is recorded as a specification ambiguity; the implementation follows the explicit failure rule. The final suite and source-aligned image checks have passed; the offline completion conclusion is scoped to the recorded evidence.

Independent final precision/path checks passed **45 tests in 11.21s**: 40 raw market cases cover all 23 support terms with positive and one-at-a-time raw-input contrasts, three exact reference/resilience cases, and two review-return cases. These caught and closed actual Decimal precision defects. Other focused independent checks cover credential refusal before ledger persistence, exact PostgreSQL job idempotency, decision safety and fresh replay/masking. They do not establish actual model output quality or complete historical acceptance.

Root built wheel and source distribution; diff checks and selected Ruff rules (`E4,E7,E9,F,I`) passed after mechanical cleanup. Unrestricted lint cleanliness is not claimed. Root compared three configured ignored-file secrets in memory against 248 versionable files with zero matches; the audit did not access their values. Final build/static checks match the final freeze.
