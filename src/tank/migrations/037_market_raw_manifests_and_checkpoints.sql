-- W11: market tables outside HLD §7.1. Raw Parquet is found by its HLD §7.2 path
-- (raw/trades/..., raw/books/...), so the per-flush manifest table goes. The reconnect
-- checkpoint is the last print of the newest mkt.flow_bars_1m row (03 §14 last_trade_id).
DROP TABLE IF EXISTS mkt.raw_archives;
DROP TABLE IF EXISTS mkt.capture_state;
DROP FUNCTION IF EXISTS ops.protect_sealed_raw_manifest();
