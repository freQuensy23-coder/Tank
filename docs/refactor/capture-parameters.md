# Capture timing parameters

This is a bounded Options/configuration boundary, not completion of the Go Range, normalizer, generic Store or whole Options audits. OI/FX normalization and all retained rows/payloads are unchanged. Qualification is pending on this working checkpoint.

## Actual consumed configuration

Previously `capture.Options.Threshold(name, fallback)` read an arbitrary YAML-shaped map on every use, defaulting missing, null and malformed supplied facts. `config.Config.Threshold` was a separate permissive implementation with no production callers. The capture configuration loader now projects the ten actual consumed facts into `captureparams.Parameters`; Options carries that named value. Service construction validates the value before using its dependencies, returns an error for an uninitialized value and never fabricates configuration from a zero struct. The five actual construction sites are the capture command, REST parity harness and three stream/REST test setup sites.

Omission means the existing declared Go override policy, not a missing required observation. The following per-symbol constants are retained exactly. Present null, boolean, malformed, nonfinite, nonpositive or unrepresentable timing facts refuse admission instead of selecting a fallback. Decimal seconds and the existing valid numeric-string forms remain supported; lookback days must be positive, integral and representable as the actual history duration. No historical market value is decoded or sanitized by this boundary.

| Configuration key | Omitted value | Actual consumers and reason |
| --- | --- | --- |
| `head_check_delay_s` | 45 seconds | Actual flow sealer delay. Its existing independent 30–60 second clamp remains unchanged. |
| `book_save_interval_s` | 8 seconds | Actual order-book summary pipeline cadence, formerly `pipeline.BookSaveS`. |
| `hot_poll_interval_s` | 30 seconds | Actual hot derivative grid and its half-interval staggering. |
| `hot_ticker_interval_s` | 20 seconds | Actual hot ticker grid; current repository YAML explicitly overrides this to 300 seconds. |
| `quiet_socket_s` | 30 seconds | Actual WebSocket quiet-message/reconnect delay; current YAML explicitly declares 30. |
| `kline_audit_interval_s` | 3,600 seconds | Actual completed audit rescheduling and instrument audit staggering. |
| `base_oi_funding_interval_s` | 300 seconds | Actual non-hot derivative poll rescheduling; current YAML explicitly declares 300. |
| `base_ticker_interval_s` | 300 seconds | Actual non-hot ticker poll rescheduling. |
| `base_kline_interval_s` | 60 seconds | Actual instrument-loop wait; current YAML explicitly declares 60. The existing minimum one-second wait remains unchanged. |
| `historical_lookback_days` | 90 days | Actual OI-history request interval; no replacement with current retention metadata. |

The current YAML's `book_interval_s: 10`, `hot_oi_funding_interval_s: 60` and `base_retention_days: 90` are different keys. This change does not silently alias them to the consumed Go override names. Whether those policies should converge is a separate behavior decision; the actual repository-configuration regression preserves the existing 8/30/90 capture values.

## Retained mapping and absence reasons

- `captureparams.Decode(raw map[string]any)` receives the actual shared thresholds YAML mapping. Other application keys are genuinely outside this capture projection, so unknown names are not treated as capture overrides. A finite descriptor list binds each of the nine known interval names to its specific private scalar field; it is admission machinery, not an opaque live domain payload. No raw map is retained in Parameters or exported Config.
- The local raw thresholds map is empty only when the optional thresholds document is absent. A present non-mapping document or YAML read/decode error retains the existing explicit refusal. Present invalid consumed values are not omission.
- `Options.Instruments` remains the real instrument-ID index and Order remains its declared sequence. `Options.Tokens` still carries the actual raw token registry into the existing benchmark-phase validator. This timing slice does not require unused symbols during all configuration loads, invent a missing symbol or claim that raw token boundary is closed.
- Existing optional leadership/write gates, raw-output/fetch ports and process logger defaults are unchanged and are not claimed as fully audited by this checkpoint.

## Evidence and limits

The before-fix actual `config.Load` diagnostic has five malformed-present reds (null, boolean, text, NaN and infinity) and one passing omitted-document control. The first changed-source focused attempt caught and corrected two mechanical compile errors; its log remains retained. A subsequent attempt executed zero tests: its owned pre-executor chown container had stale Docker Running state with an absent init PID. Only the exact verified owned wrapper and setup container were drained; its volume and all evidence were retained. This is separate setup evidence, not a source/test failure or a passing qualification.

Final changed-source behavior, race, parity and static evidence will be recorded only after actual terminal runs. No timeout, worker, service, cache or image change is part of this source slice.
