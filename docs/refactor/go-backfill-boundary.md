# Bounded Go backfill lifecycle plan

Baseline2f3d389a has the same capture tree as parenta1e4d8b and passed the published Go/race/parity lanes. This branch is separate from the held publisher and persistence-CI branches. No capture source change is included in this planning checkpoint.

## Actual boundary

`capture.Store.BackfillRuns` is implemented by PostgreSQL `store.Store`, the capture test `memStore` and the rest-parity `noStore`. Its only live application caller is `Service.obligations`, which passes PENDING/RUNNING rows through the in-flight identity set to `runObligation` and `putRun`. HOT startup uses the separate already-typed `BackfillAnchors` projection; that lookup and its partial-order/refusal policy will stay unchanged.

The PostgreSQL table has26 physical fields: storage_key plus25 declared columns. Text fields are backfill_id, event_id, token_id, instrument_id, status, reason, config_version, code_version and semantic_fingerprint. Timestamp triplets are anchor/anchor_prec/anchor_prec_reason, started_at/started_at_prec/started_at_prec_reason, finished_at/finished_at_prec/finished_at_prec_reason, received_ts/received_ts_prec/received_ts_prec_reason and recorded_at/recorded_at_prec/recorded_at_prec_reason. Result is jsonb. The schema requires backfill_id and the physical storage key; the other columns remain physically nullable. Required projection keys must be present even when SQL NULL. A missing key must not become a Go zero/nil value.

`runObligation` consumes actual backfill identity, token identity and recorded anchor, writes actual worker start, captures benchmarks/instrument windows, then emits COMPLETE/PARTIAL or an explicit failure. Context cancellation currently leaves the durable run retryable; in-flight ownership is released on exit. The same-ID retry/no-concurrent-duplicate oracle must remain. An unobserved retained worker-start is not an earlier start, and an anchor with unavailable precision must never be relabelled as verified T0/T1 or replaced with now.

Current live result values already have named calculations: InstrumentCaptureCoverage contains CaptureWindows W1H/W4H/W24H, typed flow/klines minute coverage and an optional real pre24 reference; KlineCoverage contains benchmark interval/count/completeness/anchor-close facts. The remaining storageValue maps and []any aggregation are the serialization boundary to remove from application flow. The wider market Range/Projection row decoders used to calculate coverage are not part of this slice.

## Planned contracts and persistence

Add a backfill-specific raw physical projection/decoder and named lifecycle commands/results. Keep physical SQL decoding separate from validating consumed commands, so malformed consumed facts can reach explicit terminal refusal rather than repeatedly poison a query. Preserve raw nullable provenance, prior result evidence and exact numeric JSON at the persistence edge. Historical prior result is not an admissible new live result. New outcomes must carry named instrument/benchmark structures with original wire keys, absent-versus-zero behavior and actual clocks.

Replace full-row copying/reconstruction with backfill-specific typed persistence effects. Lifecycle-only failures must leave unconsumed historical fields/result bytes untouched; new successful results replace result only from actual named observations. Metadata/fingerprints must use the existing authoritative writer identities/canonical codec, never JSON float conversion or guessed source precision. The existing broader Writer/Points APIs are not a blanket any exemption or an invitation to migrate unrelated Store methods.

The current putRun/flush path can retain queued writes after a failed flush while continuing work. The typed persistence boundary must expose its actual acknowledgment/error before claiming a lifecycle write succeeded. Exact choice of backfill-specific claim/completion methods and optimistic identity/revision binding will be reviewed before activation; no unrelated global writer/leadership semantics are changed by this plan.

## Qualification

Retain all immutable parity inputs and original public Go error strings. Add missing-key versus explicit-null physical decoder controls, original numeric/text JSON fidelity, malformed input terminal refusal with zero capture effects, preserved unknown start/anchor provenance, actual start/finish clocks, typed COMPLETE/PARTIAL/FAILED results and prior-result preservation. Exercise the real PostgreSQL Store as well as memory producer behavior, same-ID retry/concurrency, lifecycle persistence failure and race/parity lanes. Retained old rows remain evidence and are not rewritten to make a new decoder pass. Sparse authored memory seeds are not automatically labelled complete SQL envelopes; fixtures must explicitly declare the facts their oracle consumes.
