# Public ENA full-request reconstruction

This fixture replaces the earlier three-price-projection coverage assumption. It contains real later reads supporting all **16 dense projections / 784,665 projected rows**, nine ENA native-volume histories, real recent/wide observations, public supply inputs, and the original 81-spot reference-selection universe.

It is **not an exact historical snapshot**, **not all-caches-cold**, and **not an unredacted full storage dump**. Coinbase FINAL now has 453 fewer underlying rows than the incident. The benchmark qualification is a matched, seeded-request comparison of complete original/candidate packages using the same disclosed public reconstruction.

## Import contract

- `klines.jsonl.gz`: **663,226 unique actual storage keys**, merged only when every shared captured field agreed. Includes 605,895 underlying dense positions; 64,209 actual count-eligible partial-edge rows; 25,163 recent ENA wide klines; and 177 latest/sector/hourly selections before overlap. Dense/count-only projections deliberately omit uncaptured wide columns; absence does not mean a captured NULL. Never synthesize OHLC, volume, keys or metadata.
- `flow.jsonl.gz`: 4,354 actual recent rows, all nine ENA instruments.
- `oi.jsonl.gz`: 54,800 actual old five-minute selections plus 2,599 recent rows. All 57,399 prior numeric/admissibility records match the immutable earlier fixture by key and fields; this capture adds actual public metadata and fingerprints. Preserve 25,721 actual NULL `oi_usd` values.
- `funding.jsonl.gz`: 3,260 unique rows in the union of recent `as_of`, old SETTLED `as_of`, and `funding_time` fallback reads. Seed once by true key; let the original redundant SQL run normally.
- `tickers.jsonl.gz`: 156 actual rows from the four recorded endpoint ranges.
- `supply.jsonl.gz`: 764 actual typed PostgreSQL rows, including the real unavailable observations and retained public nested numeric data. Decimal JSON numbers retain their exact lexical numeric value.
- `registry-resolved.jsonl.gz`: 85 bounded public instrument projections. These are later resolved values, including genuine NULL tick/lot/resolution fields, not invented metadata. `registry-tokens.jsonl.gz` contains 18 bounded public token projections. Original-package Git config remains authoritative for registry/sector/semantics/threshold definitions; do not replace the package config with a new fixture config.
- `capture-tier-public.jsonl.gz`: 80 actual public cadence/tier projections, with private event/transition identifiers intentionally not selected. `capture-gaps-public.jsonl.gz`: 1,241 bounded public gap projections, with free-text reasons omitted where unsafe. These are public fact envelopes, not complete original operational records.
- Books and liquidations: actual bounded current COUNT receipts confirm zero. No invented market observations are supplied.

The gzip payloads are JSONEachRow storage/projection values. ClickHouse native JSONB columns remain JSON-encoded strings as returned by its SQL interface; decode them using the normal typed importer contract, rather than changing JSON text into a quoted product value. Retain each actual `storage_key`. Source semantic fingerprints are retained as provenance of the original public row, not recomputed after privacy omissions.

## Required cache state

Copy `kline_day_counts.json` to each isolated worker's normal `data_dir/market/kline_day_counts.json` and use unmodified `MarketService._load_day_counts`. The file uses supported `version: 1`, actual reconstruction `saved_at`, and `[instrument_id, UTC-midnight, count]` triples.

The cache has 7,209 entries: 81 instruments × 89 whole UTC days, July 31 through October 27. Its 2,101 positive aggregates cover 2,790,657 actual eligible rows. The 5,108 zeros mean absence from the exact complete server aggregate, not synthetic history. Together with the 64,209 real edge rows, they reproduce all 81 server counts, totaling 2,854,866 eligible positions. The product's normal recorded-since invalidation query must still run against the seeded database; no injected counters, monkeypatched count readers or hidden SQL bypasses.

This reconstruction warms **all** whole days; the historical process was substantially warm but still missed October 27. Dense-series and analysis caches must start cold. Do not label this strict all-application-caches-cold. `saved_at` records this reconstruction, not an original retained cache receipt; it is later than the frozen historical request clocks. Every captured kline was recorded by A, more than the normal 30-minute invalidation margin before this real save time. If replay freezes the clock at A, disclose that the native cache was constructed later from A-filtered public rows, not loaded from an incident cache artifact.

## Clocks and provenance

C = `2026-10-28T12:06:00Z`; anchor = `2026-10-28T12:01:26Z`; H = `2026-07-30T12:06:00Z`; L = H − 2 hours; R = `2026-10-26T12:05:00Z`; D = `2026-10-28T12:05:00.000001Z`; E = `2026-10-28T12:06:00.000001Z`; A = `2026-10-28T12:07:59.253250Z`.

`source-queries.json` preserves all successful source SQL, count/cap receipts, local capture times, wrappers and raw-result hashes. `failed-queries.json` records the single failed bounded wide-90-day selection and its successful one-day bounded recovery; no settings were changed. `original-count-queries.json` preserves the three incident count queries and query IDs, not their missing aggregate result bodies. `verification.json`, `privacy.json` and `manifest.json` give actual counts, reconciliation, omissions, archive/uncompressed hashes and remaining prerequisites.

All production operations used existing read-only SQL wrappers. No application imports/execution, manual tests, profiling, compilation, formatting, server/settings changes, deployments or production writes occurred during capture.

## Limits that must remain visible in benchmark results

1. Current FINAL and a common A cannot restore deleted/replaced incident versions. New current totals differ from incident counts; no missing rows were padded. The three old retained price files and all earlier OI fields do reconcile exactly to this later capture.
2. `source_archive` was intentionally not selected for ClickHouse observations. Minimal old dense/peer-edge klines contain only the actual projected columns, not fabricated full wide shapes. Arbitrary source errors, URL fields and prose inference limits were removed recursively; `privacy.json` lists every omitted occurrence. This qualifies public numeric/typed reconstruction, not byte-identical original hydration cost.
3. Original mutable capture-stream/socket/poll state is unavailable and was not retrospectively invented. Tier/gap facts do not reconstruct it. The matching-anchor status receipt was read later and shows PARTIAL with completion after A; it is not an as-of-A status snapshot and must not be relabeled COMPLETE. Any COMPLETE benchmark capture boundary must be explicitly fixture-owned/reconstructed, never claimed as historical capture state.
4. No raw 81-instrument whole-history export is included; the native cache is essential. Nor is the wider original dependency-head universe outside these 85 captured instruments reproduced as full raw history. Do not claim original historical head hashes or all-market coverage.
5. Parent-owned automatic CI must import these real typed values into disposable PostgreSQL/ClickHouse, exercise unchanged readers/full request entry points, verify native cache loading/invalidation and exact collection/projection counts, compare complete outputs, and measure balanced same-worker repetitions. Fixture validation alone establishes no latency result or output equivalence.
6. Seeded capture avoids the incident's real bounded wait/repair and production contention. Report seeded-request latency, not recovery of the historical 142-second request.
