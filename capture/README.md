# tank-capture

`tank-capture` is the only process that talks to crypto exchanges for market data. It
holds the public WebSocket streams, repairs gaps over REST, polls klines, 24h tickers,
open interest and funding, builds the 1m flow bars and 10 s book summaries with their
completeness proofs, and writes them into the typed market tables (`mkt.*`). Tank core
reads market data from PostgreSQL only.

The interface is tables. There are no files between the two processes.

| Table | Writer | Reader | Purpose |
|---|---|---|---|
| `mkt.flow_bars_1m`, `mkt.book_summaries`, `mkt.klines_1m`, `mkt.tickers_24h`, `mkt.oi_obs`, `mkt.funding_obs`, `mkt.liq_obs` | capture | core | market data, same schemas and semantics as the in-process capture |
| `mkt.capture_gaps` | capture | core | explicit gap evidence |
| `mkt.capture_streams` | capture | core | stream state per instrument and stream (Data Confidence) |
| `mkt.hot_tokens` | core | capture | escalations: the hot set capture subscribes |
| `mkt.backfill_runs` | core creates `PENDING`, capture runs it | core | pre-event capture obligation and readiness |
| `mkt.capture_requests` | core creates `PENDING`, capture runs it | core | bounded window repair before a Market Truth run |
| `ops.request_budget_ledger`, `ops.request_budget_blocks` | both | both | one request budget per provider bucket and IP |

`src/tank/migrations/045_capture_service.sql` creates the new tables, the
`bar_version`/`revised` flow bar columns, the `tank_capture` notifications and the
budget functions. Apply it with `tank db migrate` before starting the service; the
service refuses to start without it.

## Running

```
docker compose --profile capture up -d tank-capture
```

The service is in the `capture` Compose profile, so a plain `docker compose up` does not
start it. Environment:

| Variable | Default | |
|---|---|---|
| `TANK_DATABASE_URL` | built from `/run/secrets/tank_db_password` | PostgreSQL URL (role `tank`) |
| `TANK_CONFIG_DIR` | `/config` | the versioned configuration (`registry.yaml`, `semantics.yaml`, `thresholds.yaml`, `api_limits.yaml`, `sources.yaml`) |
| `TANK_CAPTURE_MODE` | `shadow` | `live` writes `mkt.*`; `shadow` writes the same rows into `mkt_shadow.*` |
| `TANK_CAPTURE_HEALTH_ADDR` | `:8791` | `GET /healthz` (200 when the writer lock is held and the last flush is recent), `GET /status` |

Core keeps its in-process capture while `TANK_EXTERNAL_CAPTURE` is unset or false (the
default). With `TANK_EXTERNAL_CAPTURE=true` core starts no stream, poller, backfill or
exchange REST request; it writes escalations and obligations and reads everything else.

## Deployment

Build the image with the commit it is built from, which becomes the `code_version` of
every row it writes, using the same `--env-file` and overlays as the other services:

```
TANK_CAPTURE_GIT_SHA=$(git rev-parse HEAD) docker compose --profile capture build tank-capture
```

The image is a static binary on distroless, running as UID/GID 1000 like Tank: the shared
`tank_db_password` secret is group 1000 readable (docs/OPERATIONS.md, Linux secret-file
ownership). It publishes no port; the Compose healthcheck runs `/tank-capture healthcheck`,
which calls `/healthz` inside the container. The container is limited to 1 CPU and 768 MiB
(256 MiB reserved) with `GOMEMLIMIT=640MiB`, so the Go collector tightens before the limit.

## Single writer

The live writer holds the session advisory lock `tank:capture:writer` (the key
`tank.storage` derives: the first 8 bytes of its SHA-256, big-endian, signed) for its
whole life on a dedicated connection. Core's in-process capture takes the same lock, so
the two never write market rows at once. A lost lock connection stops the process; it
never writes without the lock. Shadow mode uses `tank:capture:shadow`.

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
request/s in total). Core uses the same ledger when `TANK_EXTERNAL_CAPTURE` or
`TANK_REQUEST_LEDGER` is set, so both processes share one budget per IP. A 429 closes the
host for every process for `Retry-After`; a 418 closes it and is never retried; a Binance
`X-MBX-USED-WEIGHT-1M` at the venue limit closes the host for a minute.

## Layout

| Package | |
|---|---|
| `cmd/tank-capture` | the service |
| `cmd/capture-parity` | the parity harness binary (`scripts/capture/parity.py --go`) |
| `internal/config` | configuration, settings and versions |
| `internal/store` | PostgreSQL: typed writes with `tank.market_storage` semantics, reads, lock, notifications, shadow schema |
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
bars, fingerprints and watermarks. PostgreSQL tests run when `TANK_TEST_DATABASE_URL`
is set.

## Shadow comparison

```
uv run python scripts/capture/shadow_compare.py --since 2026-10-21T00:00:00Z --until 2026-10-22T00:00:00Z
```

compares `mkt_shadow.flow_bars_1m` with `mkt.flow_bars_1m` per minute and instrument
(buy/sell quantity, trade counts, Σq², size buckets, completeness) and prints the result
per venue. It exits 1 when a minute complete in both schemas differs: the rollout gate.

## Not in tank-capture

- The Binance Vision daily archives (`data.binance.vision`): the 90-day kline and metrics
  bootstrap and the aggTrades fill of windows older than REST retention. With
  `TANK_EXTERNAL_CAPTURE` core does not fetch them either.
- The raw trade and book Parquet journals (`raw/trades`, `raw/books`): the interface is
  tables.
- Discovered instrument metadata (OKX `ctVal`, Hyperliquid `szDecimals`) is held in memory;
  `reg.instruments` keeps the registry's values.
