# Global refactor plan

Initial base: `1216b4b48cbedd2d58324583c08f0524d272993a`. Main through `49c418014f8ba52bf1fce32e9cf8840733d1ae01` is integrated through the normal merge `5b77f741`. Its X actor, diagnostic and publisher additions are included in the active first-party audit.

The user requested a complete refactor across five requirements, with five GPT-6.1 Sol implementation agents and an orchestrating parent. All product edits and verification now happen in isolated local worktrees, following the user's explicit clarification. Earlier server evidence is retained as historical proof. Production is unchanged.

## Ownership

| Agent | Owned layers and modules |
| --- | --- |
| foundation | Domain model base, shared contracts, storage/repository ports, settings, utilities, time precision, logging, search, backup, market schemas/storage, ClickHouse, existing CLI, retained normalization and coordinated price-level/close contracts |
| analysis | Market/event domain, analysis/Truth and financial cache families, EventTools except the delegated dispatch-context seam, replay, review, calibration and fixture reference |
| agents | Model/OpenMA adapters, sessions/context/tools and the EventTools dispatch-context seam, runtime orchestration, gateway, bootstrap, OpenMA sync, agent skills, Delivery, Ops cards and remaining Telegram Inbox |
| ingestion | Source adapters/checks/parsers, polling, Scheduler, MarketService/SupplyCapture, source progress/reporting and the explicit reaction-recovery CLI; coordinates actual price-level comparison with foundation/analysis |
| quality | Existing test suite (specific files delegated explicitly), scripts, deployment tooling, Go capture, CI and exhaustive policy inventory |

Each implementation agent owns a separate Git worktree and a report under `docs/refactor/`. Changes to another owner's interface are coordinated before integration. The parent reviews, integrates commits, validates, and manages the MR; it does not implement product fixes.

## Required final architecture

Validated external wire models enter adapters; typed application services orchestrate domain operations through explicit ports; domain models/calculations do not depend on transport or repositories; persistence codecs handle serialized records. Presentation consumes typed results. Persistent dictionaries must not become an informal second domain model.

Strict Pydantic domain contracts reject unknown fields, coercion and invalid values. Required fields are required, and valid absence is explicit. Unknown JSON belongs only at an actual raw wire or persistence boundary. Genuine keyed collections may remain mappings with concrete key/value types and a recorded reason. Broad Any/object annotations, casts, permissive models and mapping facades do not count as conversion.

## Completion evidence

1. Exhaustive active-code inventory of fallback/default, required-field access and unnecessary optional cases, with removals or explicit per-symbol exception reasons.
2. All active first-party Python functions and variables have precise annotations; no Any or unnecessary optional unions. Go and active JS/TS boundaries use concrete structures appropriate to their languages. Immutable evidence and third-party vendor source remain preserved.
3. Structured domain/application payloads use strict Pydantic models. Every retained mapping category has a concrete need, owners and serialization policy.
4. Tests exercise independently specified behavior, effects, invariants and refusals. Remove or replace mock echoes and assertions on locally constructed constants; preserve security, parity, temporal, financial-unit and evidence guarantees.
5. Architecture inventory identifies responsibilities and dependency violations, with fixes and any necessary retained boundary decisions explained in the MR.
6. Required Python static/unit/integration, Node and Go lanes pass on the integrated commit, including zero skipped tests and prescribed unit timing budgets. Scans alone cannot prove behavior.
7. A non-draft GitHub MR targets main, CI passes on its current head, and Codex review passes on that same head. Review findings are addressed and verified before completion.

## Status

In progress. Initial models and adapter changes are incremental work, not completion. Baseline/final audits and test evidence are retained separately. No production deployment is part of this task.

## Coordinated ownership additions

Foundation owns market/flow.py, market/trade_contracts.py and the retained normalize.py contracts; ingestion owns MarketService callers and analysis owns their financial consumers. The active flow cohort must be integrated together. Quality owns the explicitly identified first-party vendor/openma/tank-deploy/openma_entrypoint.mjs boundary; additional Tank-authored vendor paths require provenance classification and exact ownership before edits. Actual upstream code, comments and licenses remain preserved.

Canonical v2 branches share the verified checkpoint ancestry and integrate through merges. Dependencies use exact canonical commits. Git stash is repository-wide across worktrees: use immutable recorded hashes, never moving stash indexes. The parent owns this plan, integration evidence and MR coordination; product changes remain with the five implementation agents.
