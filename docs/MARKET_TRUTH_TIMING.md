# Market Truth timing scope

Recovered original request: «Жду мердж квеста с фиксом этого высера. Какого хрена в журналах и логах нету информации про тайминги? Необходимо логировать все этапы.» The earlier request also requires evidence, not unsupported explanations of the historical delay.

## Small implementation plan

- Reuse application logging with one small nested-stage helper. Emit JSON start/terminal records with trace/span/parent IDs, monotonic inclusive wall seconds, synchronous calling-thread CPU seconds, and success/error/cancellation. Async wall intervals do not claim CPU ownership. Nested/parallel intervals must not be added together.
- Follow the existing tool, durable job, readiness/repair, snapshot/read/conversion, compute domains/windows, persistence, projection/audit and HTTP acceptance boundaries. Link caller traces to reused job IDs without changing durable payloads. Attach each ClickHouse HTTP interval's query ID to the actual server request; never log SQL, parameters, payloads or exception messages.
- Hoist invariant history bounds, reuse identical normalized row timestamps, and use a single-finite-Decimal precision path. Preserve admission, arithmetic order, every active-context precision effect, output values, cutoffs, locks and persistence. All general/subclass cases retain their original paths.
- Keep focused pytest regressions and the unchanged permanent CI budgets. A small source-isolated SQL/HTTP qualification stages audited immutable Git data through native gzip insertion; it has no per-row validation/export framework. One module-scoped ABBA setup feeds six complete cold/warm result/projection/snapshot comparisons, with independent databases and exact parity. Native day-count caches are explicitly warm; dense/analysis caches start cold.

## Acceptance and limits

| Requirement | Check | Status |
|---|---|---|
| All major stages and correlated durable jobs | Structured-log regressions and existing full-path integration | Pending automatic CI |
| Query ownership and safe errors | MockTransport query-ID/read-result/error regression | Pending automatic CI |
| Context/thread isolation and cancellation | Concurrent roots, to_thread, original exception identity | Pending automatic CI |
| Same numeric/report/cutoff behavior | Existing truth/service regressions plus independent OI boundary oracle | Pending automatic CI |
| Less repeated preparation | One lookback preparation with unchanged OI values/ranges | Pending automatic CI |
| Actual full-request latency improvement | Same-worker original/candidate realistic full-path measurements | Not established |

Historical snapshot 41.98 s and post-snapshot 38.60 s are composite intervals. Existing evidence cannot divide them retrospectively into CPU, waiting and I/O. Removing repeated invariant work is a source-supported mechanism, not proof of whole-request acceleration. No fixed speedup or subsecond whole-tool SLA is claimed.

The tests-first and implementation revisions are prepared separately for the parent's automatic RED/GREEN qualification. No local product/test/check/profile execution is authorized. Completion additionally requires final-head green CI, independent source review and explicit current-head Codex approval; this document is not a completion receipt.
