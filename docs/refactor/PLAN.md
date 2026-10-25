# Global refactor plan

Initial base: `1216b4b48cbedd2d58324583c08f0524d272993a`. Main through `122d8c7248c824cb8d07614221f27cce262bfe3b` is integrated. Its X actor, diagnostic and publisher additions are included in the active first-party audit.

The user requested a complete refactor across five requirements, with five GPT-6.1 Sol implementation agents and an orchestrating parent. All product edits and verification happen in isolated server worktrees. Production is unchanged.

## Ownership

| Agent | Owned layers and modules |
| --- | --- |
| foundation | Domain model base, shared contracts, storage and repository ports, settings, utilities, time precision, logging, search, backup, market schemas/storage, ClickHouse |
| analysis | Market/event domain, analysis, event tools, market application services, replay, review, calibration and fixture reference |
| agents | Model/OpenMA adapters, agent sessions/context/tools, runtime orchestration, gateway, bootstrap, OpenMA sync and agent skills |
| ingestion | Source adapters, polling, scheduler, delivery, Telegram inbox/rendering, Ops presentation/progress and CLI |
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

Foundation owns market/flow.py and the new market/trade_contracts.py; analysis retains normalize.py and its adapter/callers. The active flow cohort must be integrated together. Quality owns the explicitly identified first-party vendor/openma/tank-deploy/openma_entrypoint.mjs boundary; additional Tank-authored vendor paths require provenance classification and exact ownership before edits. Actual upstream code, comments and licenses remain preserved.

Canonical v2 branches share the verified checkpoint ancestry and integrate through merges. Dependencies use exact canonical commits. Git stash is repository-wide across worktrees: use immutable recorded hashes, never moving stash indexes. The parent owns this plan, integration evidence and MR coordination; product changes remain with the five implementation agents.
