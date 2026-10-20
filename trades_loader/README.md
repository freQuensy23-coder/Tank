# trades_loader

A separate Go service that does one thing: download public spot trades over REST and write them to files. Tank reads those files; nothing goes back to the loader.

## What it loads

- Venues: Binance (`/api/v3/aggTrades`, `fromId`), Coinbase Exchange (`/products/{id}/trades`, `after`), OKX (`/api/v5/market/trades` plus `history-trades` to page back), Kraken (`/0/public/Trades`, `since`).
- Instruments: the active `SPOT` instruments of these venues in `config/registry.yaml`. Use `-markets SPOT,PERP_LINEAR` to add perps.
- Bybit and Hyperliquid are not loaded, because their REST trade history is too short to prove continuity. Their bars still come from the streams.
- Window: from 26 h back (`-backfill`) to now, kept for 48 h (`-retain`).
- Rate: at most half of each venue's public limit, shared by its instruments. Responses 429/418 back off.

## Files (loader → Tank, one direction)

```
<out>/<instrument>/<YYYY-MM-DDTHH>.ndjson   one trade per line: {"ts_ms","id","raw"}
<out>/<instrument>/WATERMARK.json           atomic rename after every batch
```

- `raw` is the venue's own REST object, unchanged. Tank normalizes it with `tank.market.normalize.trade`, the same code the streams use.
- A reader uses an hour file only up to `segments[hour]` bytes, and only minutes before `sealed_through`.
- A minute is sealed once the loader has read every trade up to a request made after it (5 s settle margin), with continuous IDs.
- An ID break that REST cannot close is listed in `gaps`. Tank keeps those minutes incomplete.
- Bytes written after the last watermark are discarded on restart.

## In Tank

`tank.market.loader_feed.LoaderFeed` reads sealed minutes every 10 s, off the event loop. It builds bars with `FlowAccumulator` and writes them to `mkt.flow_bars_1m` with `complete = true`, `capture_method = TRADES_LOADER` and `backfill_verified = true`. Stream and backfill writers never replace those minutes. Without the loader's directory (`TANK_TRADES_LOADER_DIR`, default `/data/trades_loader`), Tank works as before.

## Run

`compose.yaml` has the `trades-loader` service and the `trades-loader` volume. The loader mounts the volume read-write; `tank` mounts it read-only at `/data/trades_loader`.
