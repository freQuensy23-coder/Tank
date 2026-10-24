# Ingestion and delivery refactor evidence

## First increment: capture HTTP and read coverage

Capture HTTP now has strict request, completion-state and response contracts. It rejects absent methods, unsupported destinations, absent required completion fields, missing or ambiguous body representations, malformed base64, and byte-count/digest mismatches. Queue mappings exist only at the PostgreSQL/capture wire boundary. HTTP headers remain associative maps. Failed, expired and pending requests legitimately have nullable results/reasons. HTTP headers/body and source identity are optional request inputs. `FetchResult.body` and `body_b64` are optional individually but exactly one is mandatory. Empty UTF-8 bodies remain valid. `FetchResult.content_type` may be an empty declared string, in which case the HTTP adapter omits that header.

Malformed successful capture records are retained instead of deleted before validation, preserving evidence. Verified successful records and terminal failure records retain the existing queue release behavior. A missing queue record is represented by `None`, never an empty fabricated record. The elapsed request budget and nonterminal state produce the existing TransportError outcome.

Read coverage computes with `ReadCounter`, `FeedReads`, `ReadSnapshot`, `SourceReadRate`, `ReadWindow`, and `ReadReport`. Feed/source keys are genuinely associative. `snapshot` accepts config JSON and projects only feed inventory because read reporting does not interpret adapter-specific feed payloads. `rates` is the existing external JSON interface; calculations occur in typed `report`. Missing checkpoints and absent counters before the first poll/detail read legitimately mean zero. Present malformed or null counters fail validation. `FeedReads.status/error` can be absent before the first poll. A source without reads has unknown failure rate and fails acceptance. Counter reset during a baseline window now raises an explicit error instead of producing negative read counts and a false pass. Degradation errors remain attached to the source report.

Raw item storage now fully declares path/time/byte inputs and durability result types; immutable byte comparison, fsync hierarchy, atomic hard-link publication and retry semantics are retained.

Verification: Python 3.13.13 shared isolated environment with frozen dependencies. Ruff check/format and mypy pass on the changed modules. 80 tests pass across existing source tests and new ingestion contract tests with `-n auto`; no skips. New tests exercise response corruption, binary bytes, evidence retention/release, request policy, coverage aggregation and counter reset.

## Inventory and remaining work

Owned source modules: `sources/__init__.py`, `organization.py`, `capture_fetch.py`, `read_report.py`, `polling.py`, `raw_store.py`, `probe.py`, `rate_limits.py`, `parsers.py`, `xsensor.py`, `policy.py`.

Owned application/presentation modules: `delivery.py` (654 lines), `scheduler.py` (1945), `telegram_inbox.py` (1285), `telegram_rich.py` (144), `ops_cards.py` (548), `ops_entry_errors.py` (183), `ops_progress.py` (179), `cli.py` (590). Config: YAML except openma.yaml; config/skills excluded.

This increment is not global completion. Publisher wire parsers, polling checkpoints, source config, X sensor contracts, Telegram delivery/inbox, scheduler, operational presentation and CLI still require their complete typed domain migration and exact-symbol optional/default inventory. Existing dedup, attachment, forward-pairing, follow-up and security behavior remains covered by existing tests but has not yet all been migrated.

## Request-budget increment

`BudgetBucket` and `BudgetAdmission` replace tuple-shaped budget records; dictionaries now exclusively map genuine bucket identities to histories, locks, and waiting counts. Every function in `sources/rate_limits.py` has declared inputs/results, including the psycopg connection row shape. The ledger rejects missing/non-numeric admission delay instead of indexing an assumed row. Window/capacity/weight validation rejects nonpositive or nonfinite configured values. A `None` capacity deliberately disables only that unconfigured budget; retries remain exactly two attempts; absent host is permitted for a non-host budget. Connection and ledger `None` represent unopened/released resources, respectively. Unknown priorities retain the existing sign-based urgency behavior. Concurrent admission preserves urgent yielding and permits fitting baseline requests while a larger request sleeps. Existing source and non-integration external-capture tests plus new contracts: 86 passed, zero skips, in 5.21 seconds. PostgreSQL ledger integration has not yet run in this increment.
