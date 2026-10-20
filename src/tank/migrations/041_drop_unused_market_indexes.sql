-- Secondary indexes of the partitioned market tables that no query uses (production
-- pg_stat_user_indexes on 2026-10-20 after a day of tank serve: 0 scans). They held
-- about 3.5 GB on klines_1m, oi_obs and tickers_24h alone and slowed every batch write.
-- Kept: each table's unique identity key, (time, storage_key) for range paging and
-- retention, recorded_at for availability reads, and klines' instrument/recorded_at.
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT i.indexrelid::regclass AS idx, pg_get_indexdef(i.indexrelid) AS def
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'mkt'
          AND c.relkind = 'p'
          AND NOT i.indisunique
          AND c.relname IN ('klines_1m', 'oi_obs', 'tickers_24h', 'flow_bars_1m', 'book_summaries', 'liq_obs')
    LOOP
        IF r.def ~ '\(token_id, '
           OR r.def ~ '\((received_ts|quote_usd_rate_as_of|index_as_of|mark_as_of|confirmation_observed_at), storage_key\)'
           OR r.def ~ '\(storage_key\)$'
        THEN
            EXECUTE format('DROP INDEX %s', r.idx);
        END IF;
    END LOOP;
END $$;
