CREATE FUNCTION ops.request_budget_scope_key(p_scope text, p_window_s double precision)
RETURNS text LANGUAGE sql IMMUTABLE STRICT
AS $$ SELECT jsonb_build_array(p_scope, p_window_s)::text $$;

CREATE FUNCTION ops.request_budget_legacy_scope(
    p_bucket text, p_capacity numeric, p_window_s double precision
) RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    parts text[];
BEGIN
    parts := regexp_match(p_bucket, '^(.*)[|]([0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?)[|]([0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?)$');
    IF parts IS NULL OR parts[1] = '' THEN
        RAISE EXCEPTION 'invalid legacy request budget identity';
    END IF;
    IF parts[2]::double precision <> p_window_s OR parts[5]::numeric <> p_capacity THEN
        RAISE EXCEPTION 'legacy request budget identity does not match supplied policy';
    END IF;
    RETURN parts[1];
END $$;

CREATE FUNCTION ops.request_budget_acquire_scope(
    p_scope text, p_capacity numeric, p_window_s double precision,
    p_weight numeric, p_owner text, p_host text DEFAULT NULL
) RETURNS double precision LANGUAGE plpgsql AS $$
DECLARE
    now_ts timestamptz;
    blocked timestamptz;
    used numeric;
    oldest timestamptz;
    canonical text;
    legacy text;
    parts text[];
BEGIN
    IF p_scope IS NULL OR p_scope = '' OR p_owner IS NULL OR p_owner = ''
       OR p_capacity IS NULL OR NOT (p_capacity > 0 AND p_capacity < 'Infinity'::numeric)
       OR p_weight IS NULL OR NOT (p_weight > 0 AND p_weight <= p_capacity)
       OR p_window_s IS NULL OR NOT (p_window_s > 0 AND p_window_s < 'Infinity'::float8) THEN
        RAISE EXCEPTION 'invalid configured request budget';
    END IF;
    canonical := ops.request_budget_scope_key(p_scope, p_window_s);
    PERFORM pg_advisory_xact_lock(hashtextextended('tank:request_budget:' || canonical, 0));
    now_ts := clock_timestamp();
    FOR legacy IN SELECT DISTINCT bucket FROM ops.request_budget_ledger
        WHERE left(bucket, length(p_scope) + 1) = p_scope || '|'
    LOOP
        parts := regexp_match(legacy, '^(.*)[|]([0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?)[|]([0-9]+([.][0-9]+)?([eE][+-]?[0-9]+)?)$');
        IF parts IS NOT NULL AND parts[1] = p_scope
           AND parts[2]::double precision = p_window_s AND parts[5]::numeric > 0 THEN
            UPDATE ops.request_budget_ledger SET bucket = canonical WHERE bucket = legacy;
        END IF;
    END LOOP;
    IF p_host IS NOT NULL THEN
        SELECT until INTO blocked FROM ops.request_budget_blocks WHERE host = p_host;
        IF blocked IS NOT NULL AND blocked > now_ts THEN
            RETURN greatest(0.001, extract(epoch FROM blocked - now_ts));
        END IF;
    END IF;
    DELETE FROM ops.request_budget_ledger
        WHERE bucket = canonical AND at <= now_ts - make_interval(secs => p_window_s);
    SELECT coalesce(sum(weight), 0), min(at) INTO used, oldest
        FROM ops.request_budget_ledger WHERE bucket = canonical;
    IF used + p_weight <= p_capacity THEN
        INSERT INTO ops.request_budget_ledger(bucket,at,weight,owner)
            VALUES(canonical,now_ts,p_weight,p_owner);
        RETURN 0;
    END IF;
    RETURN greatest(0.001, extract(epoch FROM oldest + make_interval(secs => p_window_s) - now_ts));
END $$;

CREATE OR REPLACE FUNCTION ops.request_budget_acquire(
    p_bucket text, p_capacity numeric, p_window_s double precision,
    p_weight numeric, p_owner text, p_host text DEFAULT NULL
) RETURNS double precision LANGUAGE plpgsql AS $$
DECLARE
    scope text;
BEGIN
    scope := ops.request_budget_legacy_scope(p_bucket,p_capacity,p_window_s);
    IF scope IS NULL THEN
        RAISE EXCEPTION 'invalid legacy request budget identity';
    END IF;
    RETURN ops.request_budget_acquire_scope(scope,p_capacity,p_window_s,p_weight,p_owner,p_host);
END $$;
