# Market Truth timing scope

Recovered original request: «Жду мердж квеста с фиксом этого высера. Какого хрена в журналах и логах нету информации про тайминги? Необходимо логировать все этапы.» The earlier request also requires evidence, not unsupported explanations of the historical delay.

## Small implementation plan

- Reuse application logging with one small nested-stage helper. Emit JSON start/terminal records with trace/span/parent IDs, monotonic inclusive wall seconds, synchronous calling-thread CPU seconds, and success/error/cancellation. Async wall intervals do not claim CPU ownership. Nested/parallel intervals must not be added together.
- Follow the existing tool, durable job, readiness/repair, snapshot/read/conversion, compute domains/windows, persistence, projection/audit and HTTP acceptance boundaries. Link caller traces to reused job IDs without changing durable payloads. Attach each ClickHouse HTTP interval's query ID to the actual server request; never log SQL, parameters, payloads or exception messages.
- Hoist only invariant lookback configuration and timedelta construction out of relative, OI and native-volume history loops. Keep iteration, admitted samples, Decimal arithmetic, cache ownership, cutoffs, locks and persistence unchanged.
- Add small ordinary pytest regressions; retain existing permanent CI and budgets. No retained-data importer, exporter, baseline package copy or benchmark framework.

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
