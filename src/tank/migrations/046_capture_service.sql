-- tank-capture (capture/README.md) is the only exchange capture: it writes the typed market
-- tables (mkt.*) and Tank reads them; these are the tables between the two. Backward-safe:
-- the new flow-bar columns are nullable and every object is created IF NOT EXISTS, so a
-- core build without this migration still reads and writes as before.

-- A print landing in a flow bar that was already written rewrites the bar as a revision:
-- bar_version + 1 and revised set, with the reason in capture_proof. Bars written before
-- tank-capture leave both null (version 1).
ALTER TABLE mkt.flow_bars_1m
    ADD COLUMN IF NOT EXISTS bar_version bigint,
    ADD COLUMN IF NOT EXISTS revised boolean;

-- Stream state per instrument and stream (TRADES, DEPTH, KLINES_POLL): Data Confidence
-- reads connection, receipt, exchange-time watermark and last print here instead of the
-- in-process socket state. One row per (instrument, stream), rewritten every few seconds.
CREATE TABLE IF NOT EXISTS mkt.capture_streams (
    storage_key text NOT NULL,
    "code_version" text,
    "config_version" text,
    "connected_since" timestamptz,
    "connected_since_prec" text,
    "connected_since_prec_reason" text,
    "detail" jsonb,
    "endpoint" text,
    "instrument_id" text NOT NULL,
    "last_message_at" timestamptz,
    "last_message_at_prec" text,
    "last_message_at_prec_reason" text,
    "last_trade_id" text,
    "last_trade_ts" timestamptz,
    "last_trade_ts_prec" text,
    "last_trade_ts_prec_reason" text,
    "market_type" text,
    "recent_polls" jsonb,
    "recorded_at" timestamptz,
    "recorded_at_prec" text,
    "recorded_at_prec_reason" text,
    "sealed_through" timestamptz,
    "sealed_through_prec" text,
    "sealed_through_prec_reason" text,
    "semantic_fingerprint" text,
    "state" text,
    "stream" text NOT NULL,
    "token_id" text,
    "updated_at" timestamptz,
    "updated_at_prec" text,
    "updated_at_prec_reason" text,
    "venue" text,
    "watermark_ts" timestamptz,
    "watermark_ts_prec" text,
    "watermark_ts_prec_reason" text,
    "writer" text,
    UNIQUE (instrument_id, stream),
    UNIQUE (storage_key)
) WITH (fillfactor = 50);
CREATE INDEX IF NOT EXISTS capture_streams_token_idx ON mkt.capture_streams (token_id, stream);

-- Requests Tank makes of the capture service (the bounded REST window repair before a
-- Market Truth run, 03 §5.1 fill order). Tank inserts PENDING and waits a bounded time;
-- the capture service claims, runs and finishes them.
CREATE TABLE IF NOT EXISTS mkt.capture_requests (
    storage_key text NOT NULL,
    "budget_s" numeric,
    "code_version" text,
    "config_version" text,
    "deadline" timestamptz,
    "deadline_prec" text,
    "deadline_prec_reason" text,
    "end" timestamptz,
    "end_prec" text,
    "end_prec_reason" text,
    "event_id" text,
    "finished_at" timestamptz,
    "finished_at_prec" text,
    "finished_at_prec_reason" text,
    "kind" text,
    "reason" text,
    "recorded_at" timestamptz,
    "recorded_at_prec" text,
    "recorded_at_prec_reason" text,
    "request_id" text NOT NULL,
    "requested_at" timestamptz,
    "requested_at_prec" text,
    "requested_at_prec_reason" text,
    "result" jsonb,
    "semantic_fingerprint" text,
    "start" timestamptz,
    "start_prec" text,
    "start_prec_reason" text,
    "started_at" timestamptz,
    "started_at_prec" text,
    "started_at_prec_reason" text,
    "status" text,
    "token_id" text,
    "writer" text,
    UNIQUE (request_id),
    UNIQUE (storage_key)
);
CREATE INDEX IF NOT EXISTS capture_requests_status_idx ON mkt.capture_requests (status, requested_at);

-- Escalations, capture obligations and requests wake the capture service at once
-- (LISTEN tank_capture); it also polls, so a lost notification only delays.
CREATE OR REPLACE FUNCTION ops.notify_capture() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('tank_capture', TG_TABLE_NAME);
    RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS notify_capture ON mkt.hot_tokens;
CREATE TRIGGER notify_capture AFTER INSERT OR UPDATE OR DELETE ON mkt.hot_tokens
    FOR EACH STATEMENT EXECUTE FUNCTION ops.notify_capture();
DROP TRIGGER IF EXISTS notify_capture ON mkt.backfill_runs;
CREATE TRIGGER notify_capture AFTER INSERT OR UPDATE ON mkt.backfill_runs
    FOR EACH STATEMENT EXECUTE FUNCTION ops.notify_capture();
DROP TRIGGER IF EXISTS notify_capture ON mkt.capture_requests;
CREATE TRIGGER notify_capture AFTER INSERT ON mkt.capture_requests
    FOR EACH STATEMENT EXECUTE FUNCTION ops.notify_capture();

-- One request budget per provider bucket for every process on this IP (api_limits.yaml:
-- at most half the documented capacity; Kraken 1 request/s in total). A bucket is
-- "<key>|<window_s>|<capacity>" as tank.sources.rate_limits names it. Admission and
-- accounting are one statement under a per-bucket transaction lock; the ledger only
-- holds the current window, so it is unlogged.
CREATE UNLOGGED TABLE IF NOT EXISTS ops.request_budget_ledger (
    bucket text NOT NULL,
    at timestamptz NOT NULL,
    weight numeric NOT NULL,
    owner text NOT NULL
);
CREATE INDEX IF NOT EXISTS request_budget_ledger_bucket_at_idx ON ops.request_budget_ledger (bucket, at);

-- A provider host that answered 429/418, or reported its whole-IP weight at the limit,
-- is closed for every process until `until`.
CREATE TABLE IF NOT EXISTS ops.request_budget_blocks (
    host text PRIMARY KEY,
    until timestamptz NOT NULL,
    reason text,
    owner text,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Seconds to wait before asking again; 0 means admitted and recorded.
CREATE OR REPLACE FUNCTION ops.request_budget_acquire(
    p_bucket text, p_capacity numeric, p_window_s double precision, p_weight numeric, p_owner text,
    p_host text DEFAULT NULL
) RETURNS double precision LANGUAGE plpgsql AS $$
DECLARE
    now_ts timestamptz := clock_timestamp();
    blocked timestamptz;
    used numeric;
    oldest timestamptz;
BEGIN
    IF p_capacity <= 0 OR p_weight <= 0 OR p_weight > p_capacity THEN
        RAISE EXCEPTION 'invalid request budget % % %', p_bucket, p_capacity, p_weight;
    END IF;
    IF p_host IS NOT NULL THEN
        SELECT until INTO blocked FROM ops.request_budget_blocks WHERE host = p_host;
        IF blocked IS NOT NULL AND blocked > now_ts THEN
            RETURN greatest(0.001, extract(epoch FROM blocked - now_ts));
        END IF;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('tank:request_budget:' || p_bucket, 0));
    now_ts := clock_timestamp();
    DELETE FROM ops.request_budget_ledger
        WHERE bucket = p_bucket AND at <= now_ts - make_interval(secs => p_window_s);
    SELECT coalesce(sum(weight), 0), min(at) INTO used, oldest
        FROM ops.request_budget_ledger WHERE bucket = p_bucket;
    IF used + p_weight <= p_capacity THEN
        INSERT INTO ops.request_budget_ledger (bucket, at, weight, owner) VALUES (p_bucket, now_ts, p_weight, p_owner);
        RETURN 0;
    END IF;
    RETURN greatest(0.001, extract(epoch FROM oldest + make_interval(secs => p_window_s) - now_ts));
END $$;

CREATE OR REPLACE FUNCTION ops.request_budget_block(p_host text, p_until timestamptz, p_reason text, p_owner text)
RETURNS void LANGUAGE sql AS $$
    INSERT INTO ops.request_budget_blocks (host, until, reason, owner, updated_at)
    VALUES (p_host, p_until, p_reason, p_owner, now())
    ON CONFLICT (host) DO UPDATE SET
        until = greatest(ops.request_budget_blocks.until, EXCLUDED.until),
        reason = EXCLUDED.reason, owner = EXCLUDED.owner, updated_at = now();
$$;
