-- Dependency heads seek the latest correction within one instrument's history.
-- The global recorded_at index makes a bounded historical seek scan unrelated
-- instruments before finding a match. This also covers future daily partitions.
CREATE INDEX IF NOT EXISTS klines_1m_instrument_recorded_key_idx
    ON mkt.klines_1m (instrument_id, recorded_at DESC, storage_key DESC)
    WHERE recorded_at IS NOT NULL;
