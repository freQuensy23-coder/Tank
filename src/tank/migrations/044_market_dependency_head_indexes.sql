-- Dependency heads (market_dependency_head) read the newest-recorded, newest and oldest row
-- of one instrument in a window. oi_obs and funding_obs had no instrument-leading index for
-- those orders, so every daily partition without that instrument's rows was scanned whole:
-- prod 2026-10-21, okx WLD-SWAP oi availability 19 s, latest 3 s; funding SETTLED 0.4-0.6 s,
-- 6 per perp per condition evaluation (EVAL_CONDITIONS timeouts). klines_1m already has the
-- recorded_at form; these mirror it.
CREATE INDEX IF NOT EXISTS oi_obs_instrument_recorded_key_idx
    ON mkt.oi_obs (instrument_id, recorded_at DESC, storage_key DESC) WHERE recorded_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS oi_obs_instrument_ts_key_idx
    ON mkt.oi_obs (instrument_id, ts DESC, storage_key DESC);
CREATE INDEX IF NOT EXISTS funding_obs_instrument_kind_recorded_key_idx
    ON mkt.funding_obs (instrument_id, kind, recorded_at DESC, storage_key DESC) WHERE recorded_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS funding_obs_instrument_kind_funding_time_key_idx
    ON mkt.funding_obs (instrument_id, kind, funding_time DESC, storage_key DESC);
