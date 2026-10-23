# tank-capture

`tank-capture` is the only process that talks to crypto exchanges for market data. It
holds the public WebSocket streams, repairs gaps over REST, polls klines, 24h tickers,
open interest and funding, builds the 1m flow bars and 10 s book summaries with their
completeness proofs, and writes them and every print into ClickHouse (database `mkt`).
Tank core reads market data from ClickHouse only.

The interface is tables. There are no files between the two processes.

| Table | Store | Writer | Reader | Purpose |
|---|---|---|---|---|
| `mkt.flow_bars_1m`, `mkt.book_summaries`, `mkt.klines_1m`, `mkt.tickers_24h`, `mkt.oi_obs`, `mkt.funding_obs`, `mkt.liq_obs` | ClickHouse | capture | core | market series (ReplacingMergeTree on their identity, read with `FINAL`) |
| `mkt.trades` | ClickHouse | capture | core, agent | every print |
| `mkt.capture_gaps` | PostgreSQL | capture | core | explicit gap evidence |
| `mkt.capture_streams` | PostgreSQL | capture | core | stream state per instrument and stream (Data Confidence) |
| `mkt.hot_tokens` | PostgreSQL | core | capture | escalations: the hot set capture subscribes |
| `mkt.backfill_runs` | PostgreSQL | core creates `PENDING`, capture runs it | core | pre-event capture obligation and readiness |
| `mkt.capture_requests` | PostgreSQL | core creates `PENDING`, capture runs it | core | bounded window repair before a Market Truth run |
| `ops.request_budget_ledger`, `ops.request_budget_blocks` | PostgreSQL | both | both | one request budget per provider bucket and IP |

`tank db migrate` creates the PostgreSQL tables (`src/tank/migrations/001_schema.sql`)
and the ClickHouse tables (`tank.clickhouse_market.ddl`). The service refuses to start
before the PostgreSQL schema is in place or without `TANK_CLICKHOUSE_URL`.

## Running

`tank-capture` is a service of `compose.yaml` and is deployed with the others ([OPERATIONS.md](../docs/OPERATIONS.md#deployment)). Its image carries the config and settings; the host `.env` supplies only `TANK_DB_PASSWORD`. Environment:

| Variable | Default | |
|---|---|---|
| `TANK_DATABASE_URL` | built from `TANK_DB_PASSWORD` | PostgreSQL URL (role `tank`): requests, gaps, streams, hot set, ledger, writer lock |
| `TANK_CLICKHOUSE_URL` | `http://tank@clickhouse:8123` | ClickHouse HTTP URL; the password is `TANK_DB_PASSWORD` |
| `TANK_CONFIG_DIR` | `/config` | the versioned configuration baked into the image (`registry.yaml`, `semantics.yaml`, `thresholds.yaml`, `api_limits.yaml`, `sources.yaml`) |
| `TANK_CAPTURE_HEALTH_ADDR` | `:8791` | `GET /healthz` (200 when the writer lock is held and the last flush is recent), `GET /readyz`, `GET /status` |

Core starts no stream, poller, backfill or exchange REST request; it writes escalations and obligations and reads everything else.

## Image

CI builds the image from the repository root with `GIT_SHA` set to the commit, which becomes the `code_version` of every row it writes ([DOCKER_BUILDS.md](../docs/DOCKER_BUILDS.md)).

The image is a static binary on distroless, running as UID/GID 1000 like Tank. It publishes no port; the Compose healthcheck runs `/tank-capture healthcheck`,
which calls `/healthz` inside the container. The container is limited to 3 CPUs and 1536 MiB
(256 MiB reserved) with `GOMEMLIMIT=1400MiB`, so the Go collector tightens before the limit.

## Single writer

tank-capture is the only writer of the market tables. It holds the session advisory
lock `tank:capture:writer` (the key `tank.storage` derives: the first 8 bytes of its
SHA-256, big-endian, signed) on a dedicated connection. When that connection fails, the
lock is re-acquired on a new one with backoff while the streams keep running: flushes
keep their rows in memory until it is held again, so nothing is written without the lock
and nothing is dropped. Its own earlier session, still holding the lock on a half-open
connection, is terminated. Only another process holding the lock stops the service.

## Restart

Capture resumes from the database: the newest flow bar with a print per instrument
(`last_trade`, within 2 days) is the trade cursor; the newest ticker and funding points
seed the repeat suppression; the stablecoin USD samples stored with klines over the last
4 days seed the quote valuation. A restart is a reconnect: the gap from the cursor to the
first print of the new socket is repaired over REST, and incomplete minutes of the hot
windows are repaired from the bars stored. Nothing is re-parsed and no history is
rebuilt.

## Completeness

A flow bar is written provisionally while its minute is open (`complete=false`,
`gap_reason=STREAM_COVERAGE_UNPROVEN`, `bar_version=0`). The minute is *sealed* only
when

1. the prints of the minute form one proven chain: on Binance, Coinbase and Kraken every
   trade ID is the previous one plus one (`trade_aggregation` as the registry and
   `normalize` define it); on OKX (increasing IDs), Bybit (sequence) and Hyperliquid
   the chain is one uninterrupted connection; and
2. a print with `ts >= minute end` has been received on that chain, or, for a quiet
   minute, a head check at least `head_check_delay_s` (45 s, bounded to 30–60 s) after
   its end found no print after the last one received. Hyperliquid lists no REST trades,
   so its quiet minutes are sealed by the same connection's exchange-time watermark.

There is no timer seal. A trade-ID hole is repaired over REST by ID; until it is filled
the minutes it touches, both ends included, stay unsealed. A minute REST cannot fill is
sealed incomplete with its reason. A sealed minute is written once with `bar_version=1`.
A print that lands in a sealed minute later (a late WebSocket print, a REST repair that
found a print the stream missed) rewrites it as a revision: `bar_version + 1`,
`revised=true`, and `capture_proof.revision` names the reason. A complete bar is never
replaced by an incomplete one, and a minute before a chain's proven start is never
written as an empty bar.

## Request budget

Every REST request takes its weight from the shared ledger (`ops.request_budget_acquire`)
before it is sent, in the buckets `tank.sources.rate_limits` and
`tank.market.rate_gate` define (at most half of each documented capacity; Kraken 1
request/s in total). Core uses the same ledger, so both processes share one budget per IP. A 429 closes the
host for every process for `Retry-After`; a 418 closes it and is never retried; a Binance
`X-MBX-USED-WEIGHT-1M` at the venue limit closes the host for a minute.

## Layout

| Package | |
|---|---|
| `cmd/tank-capture` | the service |
| `cmd/capture-parity` | the parity harness binary (`scripts/capture/parity.py --go`) |
| `internal/config` | configuration, settings and versions |
| `internal/store` | typed market writes and reads in ClickHouse with `tank.market_storage` semantics; PostgreSQL requests, gaps, streams, lock, notifications |
| `internal/rawstore` | every print into `mkt.trades` |
| `internal/budget` | the shared request ledger, per-host admission and venue request weights |
| `internal/rest` | the venue REST adapters (`tank.market.adapters`) |
| `internal/capture` | hot set, stream groups, the flow sealer, repairs, obligations, pollers, stream state |
| `internal/stream`, `internal/pipeline`, `internal/points`, `internal/flow`, `internal/books`, `internal/normalize` | ports of `tank.market.{streams,service,flow,books,normalize}` |
| `internal/pydec`, `internal/pytime`, `internal/pyjson`, `internal/pyval`, `internal/schema` | exact Python decimal, time, JSON and typed-schema semantics |

## Tests

```
cd capture && go vet ./... && go test ./...
uv run python scripts/capture/parity.py --check --go capture/bin/capture-parity
```

The parity harness feeds the same recorded frames (`testdata/frames`) through the
Python reference (`tank.market`) and the Go port and requires identical trades, rows,
bars, fingerprints and watermarks. ClickHouse tests run when `CLICKHOUSE_TEST_URL` is set
(a disposable server with the `mkt` tables), PostgreSQL tests when `TANK_TEST_DATABASE_URL`
is set.

## Not in tank-capture

- Discovered instrument metadata (OKX `ctVal`, Hyperliquid `szDecimals`) is held in memory;
  `reg.instruments` keeps the registry's values.
