# Market Truth timing scope

Recovered original request: «Жду мердж квеста с фиксом этого высера. Какого хрена в журналах и логах нету информации про тайминги? Необходимо логировать все этапы.» The earlier request also requires evidence, not unsupported explanations of the historical delay.

## Small implementation plan

- Reuse application logging with one small nested-stage helper. Emit JSON start/terminal records with trace/span/parent IDs, monotonic inclusive wall seconds, synchronous calling-thread CPU seconds, and success/error/cancellation. Async wall intervals do not claim CPU ownership. Nested/parallel intervals must not be added together.
- Follow the existing tool, durable job, readiness/repair, snapshot/read/conversion, compute domains/windows, persistence, projection/audit and HTTP acceptance boundaries. Link caller traces to reused job IDs without changing durable payloads. Attach each ClickHouse HTTP interval's query ID to the actual server request; never log SQL, parameters, payloads or exception messages.
- Hoist invariant history bounds, reuse identical normalized row timestamps, and use a single-finite-Decimal precision path. Preserve admission, arithmetic order, every active-context precision effect, output values, cutoffs, locks and persistence. All general/subclass cases retain their original paths.
- Keep focused pytest regressions and the unchanged permanent CI budgets. A small source-isolated SQL/HTTP qualification stages audited immutable Git data through native gzip insertion; it has no per-row validation/export framework. One module-scoped ABBA setup feeds six complete cold/warm result/projection/snapshot comparisons, with independent databases and exact parity. Native day-count caches are explicitly warm; dense/analysis caches start cold.

## Automatic qualification evidence

Automatic [CI 40806826931](https://github.com/freQuensy23-coder/Tank/actions/runs/40806826931) tested PR head `8f243a124335bd8d94d93b0f6436fd99e56689f6` through merge revision `233fedf5c11924506bfcaf0652327799bae7aca3` against original production package `ff6512a4ac2107319effc8e7745ee898a47c6212`. All 2,173 unit and 495 integration cases passed with zero failures, errors or skips, including logging/query-ID/context/cancellation regressions and all six complete result/projection/snapshot parity cases. The request-bearing integration shard completed its 181 cases in 118.14 seconds within the unchanged job deadline. That workflow was red on formatting only; merge readiness must use the final head's required checks.

One same-worker, source/process/database-isolated ABBA sequence measured the complete `get_market_truth` call through durable jobs, real ClickHouse/PostgreSQL, report persistence, audit, projection and loopback HTTP acceptance:

| State | Original wall seconds | Candidate wall seconds | Observed reduction | Original / candidate process CPU seconds |
|---|---:|---:|---:|---:|
| Cold dense/analysis caches | 32.357827 | 29.933573 | 7.49% | 29.191991 / 26.677937 |
| Warm dense/analysis caches | 15.001082 | 13.540183 | 9.74% | 13.106449 / 11.797094 |

Cold/warm output, projection and snapshot digests match exactly; both workers' native-data receipts match. Each worker imported 43 product modules from its own archived source tree. Candidate receipts contain 54 cold and 52 warm terminal stage names, including 20/19 synchronous thread-CPU stage names. These inclusive nested stage totals are not an additive request breakdown. The fixture verifies successful worker shutdown and absence of both owned databases before the six parity consumers pass.

The tests-first automatic run [40805551685](https://github.com/freQuensy23-coder/Tank/actions/runs/40805551685) produced the intended eight work-count failures (six Decimal reduction cases and two timestamp-reuse cases), with preceding numeric/context/value assertions intact; all 495 integration cases passed. Its request-bearing job exceeded its deadline, so that workflow was not green. Product acceleration and parallel isolated fixture seeding avoid repeating this cost without changing test budgets, sharding or coverage.

## Acceptance limits

These are observations from one ABBA sequence, not a statistical or production-latency guarantee. Native whole-day count caches are warm in both workers. The immutable public-current-FINAL reconstruction is not a byte-identical incident replay: historical mutable stream state and private capture documents are unavailable, and the later Coinbase dataset differs by 453 underlying rows. Original live repair/model traffic and remote OpenMA latency are not reproduced. Full data limitations are attached to the JUnit receipt.

Dataset revision: `ebf1398a869359da7d628b871f618be682c861b0`; manifest SHA-256: `a4f924bafc62ca2d422da7245e2a4cade6b011fd87e864ea998edd07660c9285`. Only audited fixtures are read from that archived revision, never its rejected product implementation.

Historical snapshot 41.98 s and post-snapshot 38.60 s remain composite intervals; these measurements cannot retrospectively divide them into CPU, waiting and I/O. No fixed speedup or subsecond whole-tool SLA is claimed.

Product/test/build/check/formatter/profile execution occurs only in permanent automatic CI, never locally. Completion additionally requires final-head green CI, independent source review and explicit current-head Codex approval. Formatting/documentation-only follow-up changes must preserve the qualified Python AST; this document alone is not a completion receipt.
