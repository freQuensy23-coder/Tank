# Scoped coverage-minute reads

Qualified source: `0ce73269728e696f0081d1992f9927e153f1b397`, on the separate descendant of qualified timing `e093de39`. The final documentation commit changes no Go application or test bytes.

## Actual boundary and callers

`Service.captureWindowCoverage` now makes its six flow/kline window reads through `Store.CoverageMinutes(context.Context, coverage.Query) ([]coverage.Minute, error)`. The three actual facts are instrument identity, bucket timestamp and required boolean completion. A closed Flow/Klines series chooses the actual physical table/time field. Query construction requires its actual instrument and bounded aligned window; zero/uninitialized scope does not become an open read. Minute values privately retain series, instrument, timestamp and boolean, and Count rechecks their query binding before calculating the existing `backfillstate.MinuteCoverage`.

The existing fractional-anchor rounding and unresolved-boundary seconds remain in captureWindowCoverage. Exact duplicate observations count once; conflicting flags at the same bucket refuse. Explicit false and a missing row remain distinct inputs, then contribute to the deliberately named existing `missing_or_incomplete_minutes` aggregate. A successful empty query produces an observed zero-complete count, while a failed read, missing required field or NULL flag never becomes an empty successful observation.

Only these six required-boolean reads moved. Benchmark/backfill kline completion remains its genuinely nullable/unreported phase. Pre24 reference-point JSON and REST revision/numeric rows remain their separately inventoried raw boundaries. No unused OHLC, trade details, FX, clock-precision label or source payload is demanded to count a minute. Existing normalizers and their provenance/FX behavior are unchanged.

## Real persistence admission

The new Store reader selects exactly the three declared columns with FINAL, the actual instrument parameter and the unchanged inclusive-start/exclusive-end window. It does not use the generic Projection path, whose materialization indexes absent raw columns into nil, or alter any generic Range/Projection/JSON reader. The actual JSONEachRow response is checked for one object per line, unique known keys, valid UTF-8 and valid Unicode scalar strings before admission. Valid JSON surrogate pairs and literal escaped backslashes remain supported; invalid byte or unpaired-surrogate identities cannot be repaired into the requested identity. Unknown columns, duplicate escaped keys, trailing documents and later malformed rows refuse the whole observation instead of yielding partial counts.

Missing projection keys, explicit NULL and wrong scalar types have separate `CoverageProjectionProblem` values. The required boolean and bucket errors retain their exact prior public text. Typed errors unwrap to known inward sentinels so moving admission earlier does not add an I/O-context prefix to an existing domain refusal. Actual I/O failures retain the original contextual unavailable path. An invalid observation makes an obligation fail with the existing `COVERAGE_READ_FAILED` phase and no invented result.

This is a stricter grammar for the new fixed projection, not a universal raw-wire equivalence claim or an OS/network security boundary. Actual stored NULLs and malformed unselected JSON are preserved. The actual ClickHouse controls prove that NULL kline completion remains NULL after refusal and a malformed unselected first_trade string remains byte-exact while the three requested coverage facts are read.

## Per-symbol mapping and absence reasons

- `coverage.Query` and `coverage.Minute` contain no generic mappings or opaque payloads. Query getters retain actual UTC instants; no processing-now, source identity or clock-precision metadata is invented. A declared empty interval is distinct from an uninitialized query.
- `coverage.Count` uses `map[int64]bool` only as the actual bucket index. Its two-value membership check distinguishes an observed false flag from an absent bucket and detects contradictory duplicate evidence.
- `decodeCoverageObject`, `decodeCoverageBody` and `AdmitCoverageProjection` hold actual driver/JSON projection data only until all required scalar facts are admitted. Raw `any` values remain confined to that boundary; no raw map or malformed value enters a Minute. Presence checks do not conflate missing with NULL. The intermediate full set prevents a later bad row from leaking earlier partial results.
- `coverageColumns` is an exhaustive mapping from the closed series to actual physical schema names; an unsupported series returns an error, never a default table. Existing parameter serialization is reused for actual instrument/time values, not inferred from row order.
- The actual memory test backend projects the same three facts and uses the real admission codec. Existing read-failure injections still exercise the first, second and seventh reads; the seventh pre24 reference read remains unchanged. The recorded parity harness explicitly refuses the new history port if unexpectedly called instead of fabricating empty history.
- Existing generic Range/Projection methods, pre24 partial evidence, nullable benchmark completion and REST revision/version/alias behavior remain open audit work. This checkpoint does not justify their fallbacks or complete the whole Store/Go responsibility audit.

## Immutable qualification

Full `go test -race -count=1 -json ./...`: 607/607 passing test/subtest events, 283 top-level cases, zero failed/skipped cases, maximum 9.70 seconds. Seventeen packages execute cases; thirteen additional packages compile without test files. The checked-in result checker verifies the complete started/passed multiset. Actual scoped HTTP and ClickHouse controls, existing service/backfill/refusal/race callers and all original count/conflict oracles are included.

The actual Python parity consumer passes in 17.33 seconds wall against all 18 immutable recorded WebSocket/REST fixtures. The eleven schema/parity refusal controls are carried from the qualified timing checkpoint because their exact source/checker bytes are unchanged; they are not relabelled as newly rerun here. Go vet passes; pinned golangci-lint 2.5 reports zero issues; pinned gofmt, comment policy and diff checks pass.

`coverage-port-final-proof.json` binds source/image/tool hashes, four successful executor records, exact source hashes and setup/executor timings. All previous production `errors.New`/`fmt.Errorf` literal values remain present. Generic Store readers/codecs, normalizers, embedded schema, sealer/REST writer, config/timing parameters, module locks, lint exemptions and retained fixtures/goldens are byte-identical to the timing base. The inactive 21-event proof and the activation diagnostic are retained separately: that diagnostic's only failure was an authored assertion expecting REPAIR_FAILED instead of the actual correct COVERAGE_READ_FAILED phase; no production reason or result contract was weakened.

No worker, timeout, service, runtime image, cache or CI 240-second budget changed. No live provider or deployment operation ran.
