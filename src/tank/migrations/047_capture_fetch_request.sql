-- HTTP_FETCH capture requests: core asks tank-capture to fetch an exchange URL; core
-- never connects to exchange hosts itself.
ALTER TABLE mkt.capture_requests ADD COLUMN IF NOT EXISTS "request" jsonb;
