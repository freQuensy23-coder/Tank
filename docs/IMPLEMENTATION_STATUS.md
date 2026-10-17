# Implementation and acceptance ledger

Objective: implement the complete Tank V0 service end to end according to the three specifications. Parent coordinates architecture, module contracts, integration and acceptance; GPT-6.1 Sol agents implement owned modules in parallel.

This ledger distinguishes code/test evidence from live acceptance. A passing mock or replay never establishes source availability, model quality, production delivery or unattended operation.

## Required behavior

| Requirement | Owner | Evidence/status |
|---|---|---|
| FR-D-01 | sources/configuration | Implementation in progress; verification pending |
| FR-D-02 | sources/configuration | Implementation in progress; verification pending |
| FR-D-03 | sources/configuration | Implementation in progress; verification pending |
| FR-D-04 | sources/configuration | Implementation in progress; verification pending |
| FR-D-05 | sources/configuration | Implementation in progress; verification pending |
| FR-D-06 | sources/configuration | Implementation in progress; verification pending |
| FR-E-01 | event_tools/sessions | Implementation in progress; verification pending |
| FR-E-02 | event_tools/sessions | Implementation in progress; verification pending |
| FR-E-03 | event_tools/sessions | Implementation in progress; verification pending |
| FR-E-04 | event_tools/sessions | Implementation in progress; verification pending |
| FR-E-05 | event_tools/sessions | Implementation in progress; verification pending |
| FR-E-06 | event_tools/sessions | Implementation in progress; verification pending |
| FR-M-01 | event_tools | Implementation in progress; verification pending |
| FR-M-02 | event_tools | Implementation in progress; verification pending |
| FR-M-03 | event_tools | Implementation in progress; verification pending |
| FR-M-04 | event_tools | Implementation in progress; verification pending |
| FR-B-01 | event_tools | Implementation in progress; verification pending |
| FR-B-02 | event_tools | Implementation in progress; verification pending |
| FR-B-03 | event_tools | Implementation in progress; verification pending |
| FR-T-01 | market/truth | Implementation in progress; verification pending |
| FR-T-02 | market/truth | Implementation in progress; verification pending |
| FR-T-03 | market/truth | Implementation in progress; verification pending |
| FR-T-04 | market/truth | Implementation in progress; verification pending |
| FR-T-05 | market/truth | Implementation in progress; verification pending |
| FR-T-06 | market/truth | Implementation in progress; verification pending |
| FR-T-07 | market/truth | Implementation in progress; verification pending |
| FR-T-08 | market/truth | Implementation in progress; verification pending |
| FR-T-09 | market/truth | Implementation in progress; verification pending |
| FR-T-10 | market/truth | Implementation in progress; verification pending |
| FR-T-11 | market/truth | Implementation in progress; verification pending |
| FR-T-12 | market/truth | Implementation in progress; verification pending |
| FR-T-13 | market/truth | Implementation in progress; verification pending |
| FR-O-01 | decisions | Implementation in progress; verification pending |
| FR-O-02 | decisions | Implementation in progress; verification pending |
| FR-O-03 | decisions | Implementation in progress; verification pending |
| FR-A-01 | decisions/event_tools/runtime | Implementation in progress; verification pending |
| FR-A-02 | decisions/event_tools/runtime | Implementation in progress; verification pending |
| FR-A-03 | decisions/event_tools/runtime | Implementation in progress; verification pending |
| FR-A-04 | decisions/event_tools/runtime | Implementation in progress; verification pending |
| FR-A-05 | decisions/event_tools/runtime | Implementation in progress; verification pending |
| FR-A-06 | decisions/event_tools/runtime | Implementation in progress; verification pending |
| FR-A-07 | decisions/event_tools/runtime | Implementation in progress; verification pending |
| FR-R-01 | runtime/replay_qa | Implementation in progress; verification pending |
| FR-X-01 | event_tools/sessions/runtime | Implementation in progress; verification pending |
| FR-X-02 | event_tools/sessions/runtime | Implementation in progress; verification pending |
| FR-P-01 | runtime/sessions | Implementation in progress; verification pending |
| FR-P-02 | runtime/sessions | Implementation in progress; verification pending |
| FR-P-03 | runtime/sessions | Implementation in progress; verification pending |

## Acceptance gates

| Gate | Required evidence | Current status |
|---|---|---|
| V0-A | 34 sources + market paths unattended 72h; <1% failed reads each; 86 accounts; semantics self-tests; 5-token ×7-day archive checks | Pending live installation |
| V0-B | 200 labeled items; routing precision/recall ≥.90; crypto recall ≥.95; freshness ≥.90; state/first-known checks | Labeled set and calibrated cutoffs not present |
| V0-C | All 27 grading fixtures | Fixture manifests/testing in progress; full historical packets not present |
| V0-D | CoW/World/Ondo/Quant beneficiary outputs | Pending complete fixture replay |
| V0-E | Formula vectors and saved QNT/ONDO/WLD regressions | In progress; authoritative REST snapshots present |
| V0-F | Live depth ≥3 venues ×10 tokens; first three sell bursts; leadlag; supply context | Pending live capture |
| V0-G | Live source → Telegram, no manual steps, latency bounds and evidence | Local end-to-end harness in progress; operator credentials required for live gate |
| V0-H | 14 days operation, daily review, weekly miss audits, S1–S9 | Pending after earlier gates |

## Known specification issues

- F-16: current §5.7 produces STRESS; fixture expects HEADWIND. Baseline retained. Proposed rule in fixture_check/RESULTS.md remains a proposed change, requiring independent validation per D6.
- F-11–F-14: fixture OI values come from saved REST snapshots; archive series supplies normal ranges. They are not interchangeable.
- Queue idempotency in §3.8 reuses item_id for different job types while §14 made it globally unique. Implementation scopes uniqueness to type + key.
- $30 budget: official and MAJOR+ work continues after the threshold per FR-P-03. This is not a strict upper bound.
- Denial/EXIT on an existing card must publish even when the event is invalidated; new-card gates must not swallow safety updates.

## Technical coverage

| Design section | Verification status |
|---|---|
| 2.1 Registries | Pending integrated audit |
| 2.2 Ingestion | Pending integrated audit |
| 2.3 Events | Pending integrated audit |
| 2.4 Beneficiaries | Pending integrated audit |
| 2.5 Market data | Pending integrated audit |
| 2.6 Analysis outputs | Pending integrated audit |
| 2.7 Decision, alert and audit | Pending integrated audit |
| 2.8 Control, sidecar, QA | Pending integrated audit |
| 2.9 Storage mapping (PostgreSQL) | Pending integrated audit |
| 2.10 Metric Semantics registry — initial content (`semantics.yaml`) | Pending integrated audit |
| 2.11 Registry — initial content (`registry.yaml`) | Pending integrated audit |
| 2.12 Dependency map — initial content (`dependency_map.yaml`) | Pending integrated audit |
| 3.1 Ingestion, filter and routing | Pending integrated audit |
| 3.2 Agent sessions (OpenMA) | Pending integrated audit |
| 3.3 Event identity | Pending integrated audit |
| 3.4 Freshness (model estimate) | Pending integrated audit |
| 3.5 Verification (inside the session) | Pending integrated audit |
| 3.6 State machines | Pending integrated audit |
| 3.7 Event lifecycle | Pending integrated audit |
| 3.8 The service and its jobs | Pending integrated audit |
| 3.9 Latency budget (targets, verified at V0-G) | Pending integrated audit |
| 4.1 Score semantics | Pending integrated audit |
| 4.2 EMS dimension rubrics | Pending integrated audit |
| 4.3 Classification and EMS score | Pending integrated audit |
| 4.4 Event certainty and timing confidence | Pending integrated audit |
| 4.5 Effective Economic Capture and token capture | Pending integrated audit |
| 4.6 Impact Horizon rules | Pending integrated audit |
| 4.7 Beneficiary mapping | Pending integrated audit |
| 4.8 Thesis Grade (event-side strength, per event × token) | Pending integrated audit |
| 4.9 Trade Grade (entry-side quality, per token at cutoff) | Pending integrated audit |
| 5.1 Capture tiers | Pending integrated audit |
| 5.2 Trade normalization and 1m flow bars | Pending integrated audit |
| 5.3 Cross-Exchange Spot Truth | Pending integrated audit |
| 5.4 Derivatives Truth | Pending integrated audit |
| 5.5 Liquidity Truth | Pending integrated audit |
| 5.6 Supply context | Pending integrated audit |
| 5.7 Relative price, repricing and market context | Pending integrated audit |
| 5.8 Move classification | Pending integrated audit |
| 5.9 Price-Flow Divergence discriminator | Pending integrated audit |
| 5.10 Data Confidence | Pending integrated audit |
| 6.1 Direction bias | Pending integrated audit |
| 6.2 Move potential | Pending integrated audit |
| 6.3 Best horizon | Pending integrated audit |
| 6.4 Scenarios | Pending integrated audit |
| 6.5 Confidence | Pending integrated audit |
| 6.6 Upgrade / downgrade conditions | Pending integrated audit |
| 7.1 Action matrix (no open call) | Pending integrated audit |
| 7.2 Open-call lifecycle | Pending integrated audit |
| 7.3 Contradictions | Pending integrated audit |
| 7.4 Alert gate and update policy | Pending integrated audit |
| 7.5 Composition and prohibition linter | Pending integrated audit |
| 7.6 Delivery and ledger | Pending integrated audit |
| 11.1 Replay harness | Pending integrated audit |
| 11.2 Fixture coverage (27 fixtures, F-01–F-27) | Pending integrated audit |
| 11.3 Fixtures F-01–F-27 | Pending integrated audit |
| 11.4 Formula test vectors | Pending integrated audit |
| 11.5 Shadow-mode review (V0-H) | Pending integrated audit |
