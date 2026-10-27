# Global refactor integration checkpoint — 2026-10-27

This report records reviewed integration through `c782649d`. The exhaustive refactor remains in progress. Main `49c41801` was fetched again and is already an ancestor. Changes were integrated through normal canonical merges; source conflicts did not require manual resolution. Concurrent report additions were both retained.

## Integrated responsibilities and behavior

| Area | Actual correction | Evidence and remaining boundary |
| --- | --- | --- |
| Session completion and replay | Strict owned input/page records, host observation compare-and-update, and locked replay metadata rereads prevent stale execution observations and lost session updates. | The coherent owner cohort passed 555 units and 108 actual cases on `60fbe71e`. Seven remaining Session method bodies and broader lifecycle/tool consumers remain open. |
| Native usage | Reported counters and raw evidence are separate from application billing identity. A retry validates and reuses the actual immutable first receipt, including after lost acknowledgement or later session-write failure. | `0e8fa4b3` passed 97 units and 30 actual cases. Original two Memory and two PG failures remain retained. Historical persisted formats are traced to actual producer commits, not a blanket compatibility exemption. |
| Retention and progress | Explicit event binding/lifecycle authorizes archive; activity retains its real timestamp precision. Attempt counts are required only by attempt-specific notice phases. | Actual Memory/PG source completion, archive, precision and restart controls are in the agents and ingestion ledgers. Unknown attempt-specific reporting remains an explicit open policy question. |
| Late reaction | Discovery commits exact pending demands with its cursor and drains them separately, including empty scans. Only a validated owned marker retires a demand. | Coherent source `f558c610` passed 60 units and seven actual cases. Explicit old-coalesced-work recovery has a qualified API, not an automatic or deployed administrative invocation. Its CLI composition remains open. |
| OI analysis | Assessment consumes recorded normalized USD; it cannot reconstruct missing USD from an unqualified quote mark. Exact endpoint arithmetic controls class, with explicit absent display terms and preserved zero quantities. | `34881c4b` passed 154 units and 33 actual cases. Technical Design §5.4 and the data guide agree. Funding, other actual families, full function typing and the precise condition-cache union remain open. |
| Source admission | Actual fetched/not-modified variants and receipt clocks reach instrument discovery. Both Bybit announcement parsers validate success and raw key uniqueness before effects. | Polling: 411 units/nine actual. Announcement source `6f8caa49`: 90 units/11 actual. Reporting spool, publication-clock semantics and remaining parser bodies remain open. |
| Market storage | Within-batch proof retention, generated identity collision refusal, key-count preflight and nested Memory return isolation preserve existing admitted facts before effects. | Separate source-specific proofs are in the foundation ledger. One unchanged CH flow case failed in the isolation family, then passed individually; its cause remains unresolved. This is not proof that the Memory copy repair fixed CH. |
| Go capture | Named validated timing parameters retain declared omission policy. Six coverage reads consume a typed scope and strict physical three-column records before counting. | `0ce73269` passed all 607 Go test/subtest events, 283 top-level cases, and the actual 18-fixture parity comparison; vet/lint/format pass. Generic reads, nullable benchmark phases and other raw producer boundaries remain open. |
| Layers and tests | Concrete generic/source/job repositories are outward; pure contracts retain narrow ports. Native file/socket test boundaries are precise and preserve actual denial controls. | Declared strict CI targets pass on the combined `4c7bf9cd` source. Green configured checks do not prove untyped bodies or every application mapping complete. |

## Complete-set qualification limits

The initial combined `bd232ec2` unit collection stopped at a stale recovery-test import of the moved JobRepository. The sole import was corrected by `2a0333c6`; its narrow collection then passed. No tests executed in the failed collection attempt.

The complete `4c7bf9cd` unit selection contains 5,424 cases. It ran all of them: 5,423 passed, one failed, zero skipped; lanes took 76.125 and 68.082 seconds. Maximum case time was 5.597 seconds. All seven earlier b026 failures passed. The remaining publisher-worker case did not begin its first observed effect within its unchanged two-second startup deadline, before any SIGINT was sent. Its startup failure remains under diagnosis; no deadline, worker limit or assertion was weakened.

Every additional declared CI mypy command passed independently on `4c7bf9cd`. Default mypy reports one condition-cache annotation error across 246 source files. Exact configured Ruff, format, comment-policy and whitespace checks pass on `c782649d` (730 formatted files). An earlier overbroad Ruff invocation included protected historical evidence and documentation examples; those files were not changed to satisfy an unintended check scope.

The previous published b026 CI run was cancelled under the unchanged 240-second workflow budget. Integration lane 0 reached collection but did not execute tests. No full integration union or green current CI is claimed. The new supported managed-Python/npm archive cache inputs change setup only, and do not guarantee cold workflow timing.

Codex reviewed exact published `b026dd0e` without major findings (receipt 6430343604); four prior review threads are resolved. That review does not cover the newer local source. Final exact-head CI and review remain required.

## Remaining inventory

A fresh syntax inventory on `85a5d3e3` found 520 functions with missing annotations in 19 first-party source modules. The largest groups are Truth, replay, EventTools, market service, Scheduler, CLI, Telegram Inbox and source parsers. This is a candidate inventory, not 520 proven defects, and not evidence that annotated bodies are precise. Implicit receivers and genuine external/dynamic boundaries need their actual semantic disposition.

The five original owners continue their assigned implementation work. Foundation owns existing CLI families after the coordinated funding contract dependency; ingestion reserves the explicit recovery admin command and source-report spool. Analysis owns actual financial families/cache and coordinates EventTools consumers with the agents owner. Quality owns remaining Go, executable/test boundaries and the publisher startup diagnosis. Complete per-symbol fallback/mapping reasons, actual callers, meaningful effects, query/load evidence and final green checks remain completion requirements.
