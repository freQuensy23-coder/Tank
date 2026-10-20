DO $$ BEGIN
IF to_regclass('mkt.trades') IS NOT NULL AND current_setting('tank.market_raw_export_verified',true) IS DISTINCT FROM 'yes' THEN
RAISE EXCEPTION '027 must use guarded Repository.migrate verified raw export';
END IF; END $$;
-- Typed market boundary; data relocation is performed by the guarded migration runner.
CREATE SCHEMA IF NOT EXISTS archive;
ALTER TABLE IF EXISTS mkt.flow_bars_1m SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.flow_bars_1m'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.flow_bars_1m (storage_key text NOT NULL,"backfill_verified" boolean,"buy_qty" numeric,"buy_usd" numeric,"c" numeric,"capture_method" text,"code_version" text,"complete" boolean,"config_version" text,"first_trade_id" text,"gap_reason" text,"h" numeric,"instrument_id" text NOT NULL,"l" numeric,"last_trade_id" text,"market_type" text,"minute_start" timestamptz NOT NULL,"minute_start_prec" text,"minute_start_prec_reason" text,"n_buy" bigint,"n_sell" bigint,"notional_reason" text,"o" numeric,"quote_asset" text,"quote_volume" numeric,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"sell_qty" numeric,"sell_usd" numeric,"source_archive" text,"sum_sq_qty" numeric,"token_id" text,"trade_aggregation" text,"venue" text,"volume_usd" numeric,"vwap" numeric,"zero_trade_minute" boolean,"size_buckets" jsonb,"null_reasons" jsonb,"capture_proof" jsonb,"first_trade" jsonb,"last_trade" jsonb,"boundary_segments" jsonb,"reference_points" jsonb,"semantic_fingerprint" text,UNIQUE (instrument_id,minute_start)) PARTITION BY RANGE (minute_start);
CREATE TABLE mkt.flow_bars_1m_historical PARTITION OF mkt.flow_bars_1m DEFAULT;
CREATE INDEX ON mkt.flow_bars_1m (minute_start,storage_key);
CREATE INDEX ON mkt.flow_bars_1m (token_id,minute_start,storage_key);
CREATE INDEX ON mkt.flow_bars_1m (received_ts,storage_key);
CREATE INDEX ON mkt.flow_bars_1m (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.flow_bars_1m (recorded_at,storage_key);
CREATE INDEX ON mkt.flow_bars_1m (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.flow_bars_1m(storage_key);
ALTER TABLE IF EXISTS mkt.klines_1m SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.klines_1m'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.klines_1m (storage_key text NOT NULL,"c" numeric,"code_version" text,"complete" boolean,"config_version" text,"h" numeric,"instrument_id" text NOT NULL,"interval_s" bigint,"l" numeric,"market_type" text,"n_trades" bigint,"o" numeric,"open_time" timestamptz NOT NULL,"open_time_prec" text,"open_time_prec_reason" text,"quote_asset" text,"quote_usd_rate" numeric,"quote_usd_rate_as_of" timestamptz,"quote_usd_rate_as_of_prec" text,"quote_usd_rate_as_of_prec_reason" text,"quote_volume" numeric,"quote_volume_method" text,"quote_volume_usd" numeric,"quote_volume_usd_reason" text,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"source_archive" text,"taker_buy_native" numeric,"taker_buy_quote" numeric,"token_id" text,"venue" text,"volume_native" numeric,"semantic_fingerprint" text,UNIQUE (instrument_id,open_time)) PARTITION BY RANGE (open_time);
CREATE TABLE mkt.klines_1m_historical PARTITION OF mkt.klines_1m DEFAULT;
CREATE INDEX ON mkt.klines_1m (open_time,storage_key);
CREATE INDEX ON mkt.klines_1m (token_id,open_time,storage_key);
CREATE INDEX ON mkt.klines_1m (quote_usd_rate_as_of,storage_key);
CREATE INDEX ON mkt.klines_1m (token_id,quote_usd_rate_as_of,storage_key);
CREATE INDEX ON mkt.klines_1m (received_ts,storage_key);
CREATE INDEX ON mkt.klines_1m (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.klines_1m (recorded_at,storage_key);
CREATE INDEX ON mkt.klines_1m (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.klines_1m(storage_key);
ALTER TABLE IF EXISTS mkt.oi_obs SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.oi_obs'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.oi_obs (storage_key text NOT NULL,"code_version" text,"config_version" text,"gap_reason" text,"index_as_of" timestamptz,"index_as_of_prec" text,"index_as_of_prec_reason" text,"index_price" numeric,"instrument_id" text NOT NULL,"mark_as_of" timestamptz,"mark_as_of_prec" text,"mark_as_of_prec_reason" text,"mark_price" numeric,"metric_key" text NOT NULL,"oi_native" numeric,"oi_usd" numeric,"oi_usd_reason" text,"raw_unit" text,"raw_value" numeric,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"semantic_confidence" text,"sidedness" text,"source_archive" text,"synchronized_eligible" boolean,"timestamp_precision" text,"token_id" text,"ts" timestamptz NOT NULL,"ts_prec" text,"ts_prec_reason" text,"ts_bucket_size_s" numeric,"semantic_fingerprint" text,UNIQUE (instrument_id,metric_key,ts)) PARTITION BY RANGE (ts);
CREATE TABLE mkt.oi_obs_historical PARTITION OF mkt.oi_obs DEFAULT;
CREATE INDEX ON mkt.oi_obs (index_as_of,storage_key);
CREATE INDEX ON mkt.oi_obs (token_id,index_as_of,storage_key);
CREATE INDEX ON mkt.oi_obs (mark_as_of,storage_key);
CREATE INDEX ON mkt.oi_obs (token_id,mark_as_of,storage_key);
CREATE INDEX ON mkt.oi_obs (received_ts,storage_key);
CREATE INDEX ON mkt.oi_obs (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.oi_obs (recorded_at,storage_key);
CREATE INDEX ON mkt.oi_obs (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.oi_obs (ts,storage_key);
CREATE INDEX ON mkt.oi_obs (token_id,ts,storage_key);
CREATE INDEX ON mkt.oi_obs(storage_key);
ALTER TABLE IF EXISTS mkt.funding_obs SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.funding_obs'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.funding_obs (storage_key text NOT NULL,"as_of" timestamptz NOT NULL,"as_of_prec" text,"as_of_prec_reason" text,"capture_source" text,"code_version" text,"config_version" text,"funding_time" timestamptz,"funding_time_prec" text,"funding_time_prec_reason" text,"instrument_id" text NOT NULL,"interval_h" numeric,"kind" text NOT NULL,"metric_key" text,"primary_source" text,"rate" numeric,"rate_8h_eq" numeric,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"semantic_confidence" text,"source_archive" text,"source_path" text,"token_id" text,"source_observations" jsonb,"source_disagreement" jsonb,"null_reasons" jsonb,"semantic_fingerprint" text,UNIQUE (instrument_id,kind,as_of),UNIQUE(storage_key));
CREATE INDEX ON mkt.funding_obs (as_of,storage_key);
CREATE INDEX ON mkt.funding_obs (token_id,as_of,storage_key);
CREATE INDEX ON mkt.funding_obs (funding_time,storage_key);
CREATE INDEX ON mkt.funding_obs (token_id,funding_time,storage_key);
CREATE INDEX ON mkt.funding_obs (received_ts,storage_key);
CREATE INDEX ON mkt.funding_obs (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.funding_obs (recorded_at,storage_key);
CREATE INDEX ON mkt.funding_obs (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.funding_obs(storage_key);
ALTER TABLE IF EXISTS mkt.liq_obs SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.liq_obs'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.liq_obs (storage_key text NOT NULL,"id" bigint,"code_version" text,"config_version" text,"coverage" text,"instrument_id" text,"metric_key" text,"notional_usd" numeric,"price" numeric,"price_kind" text,"qty_native" numeric,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"side" text,"token_id" text,"ts" timestamptz NOT NULL,"ts_prec" text,"ts_prec_reason" text,"usd_reason" text,"null_reasons" jsonb,"semantic_fingerprint" text,UNIQUE (storage_key,ts)) PARTITION BY RANGE (ts);
CREATE TABLE mkt.liq_obs_historical PARTITION OF mkt.liq_obs DEFAULT;
CREATE INDEX ON mkt.liq_obs (received_ts,storage_key);
CREATE INDEX ON mkt.liq_obs (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.liq_obs (recorded_at,storage_key);
CREATE INDEX ON mkt.liq_obs (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.liq_obs (ts,storage_key);
CREATE INDEX ON mkt.liq_obs (token_id,ts,storage_key);
CREATE INDEX ON mkt.liq_obs(storage_key);
ALTER TABLE IF EXISTS mkt.book_summaries SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.book_summaries'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.book_summaries (storage_key text NOT NULL,"best_ask" numeric,"best_bid" numeric,"code_version" text,"config_version" text,"confirmation_observed_at" timestamptz,"confirmation_observed_at_prec" text,"confirmation_observed_at_prec_reason" text,"exchange_ts" timestamptz NOT NULL,"exchange_ts_prec" text,"exchange_ts_prec_reason" text,"gap_reason" text,"instrument_id" text NOT NULL,"integrity_method" text,"integrity_ok" boolean,"levels_received" bigint,"metric_key" text,"mid" numeric,"quote_asset" text,"quote_usd_rate" numeric,"raw_levels_range_pct" numeric,"raw_levels_scope" text,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"second_path_confirmed" boolean,"second_path_reason" text,"spread_bps" numeric,"token_id" text,"depth" jsonb,"bids" jsonb,"asks" jsonb,"range_covered_pct" jsonb,"independent_rest" jsonb,"semantic_fingerprint" text,UNIQUE (instrument_id,exchange_ts)) PARTITION BY RANGE (exchange_ts);
CREATE TABLE mkt.book_summaries_historical PARTITION OF mkt.book_summaries DEFAULT;
CREATE INDEX ON mkt.book_summaries (confirmation_observed_at,storage_key);
CREATE INDEX ON mkt.book_summaries (token_id,confirmation_observed_at,storage_key);
CREATE INDEX ON mkt.book_summaries (exchange_ts,storage_key);
CREATE INDEX ON mkt.book_summaries (token_id,exchange_ts,storage_key);
CREATE INDEX ON mkt.book_summaries (received_ts,storage_key);
CREATE INDEX ON mkt.book_summaries (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.book_summaries (recorded_at,storage_key);
CREATE INDEX ON mkt.book_summaries (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.book_summaries(storage_key);
ALTER TABLE IF EXISTS mkt.tickers_24h SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.tickers_24h'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.tickers_24h (storage_key text NOT NULL,"capture_source" text,"code_version" text,"config_version" text,"index_as_of" timestamptz,"index_as_of_prec" text,"index_as_of_prec_reason" text,"index_price" numeric,"index_reason" text,"instrument_id" text NOT NULL,"kind" text,"last" numeric,"mark_as_of" timestamptz,"mark_as_of_prec" text,"mark_as_of_prec_reason" text,"mark_price" numeric,"quote_asset" text,"quote_volume" numeric,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"source_provenance_complete" boolean,"token_id" text,"ts" timestamptz NOT NULL,"ts_prec" text,"ts_prec_reason" text,"usd_reason" text,"volume_native" numeric,"volume_usd" numeric,"field_sources" jsonb,"unclocked_components" jsonb,"capture_sources" text[],"semantic_fingerprint" text,UNIQUE (instrument_id,ts)) PARTITION BY RANGE (ts);
CREATE TABLE mkt.tickers_24h_historical PARTITION OF mkt.tickers_24h DEFAULT;
CREATE INDEX ON mkt.tickers_24h (index_as_of,storage_key);
CREATE INDEX ON mkt.tickers_24h (token_id,index_as_of,storage_key);
CREATE INDEX ON mkt.tickers_24h (mark_as_of,storage_key);
CREATE INDEX ON mkt.tickers_24h (token_id,mark_as_of,storage_key);
CREATE INDEX ON mkt.tickers_24h (received_ts,storage_key);
CREATE INDEX ON mkt.tickers_24h (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.tickers_24h (recorded_at,storage_key);
CREATE INDEX ON mkt.tickers_24h (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.tickers_24h (ts,storage_key);
CREATE INDEX ON mkt.tickers_24h (token_id,ts,storage_key);
CREATE INDEX ON mkt.tickers_24h(storage_key);
ALTER TABLE IF EXISTS mkt.supply_obs SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.supply_obs'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.supply_obs (storage_key text NOT NULL,"available" boolean,"chain" text,"classification_required" boolean,"code_version" text,"config_version" text,"fee_scope" text,"instrument_id" text,"kind" text,"reason" text,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"token_id" text,"transfer_queries_succeeded" bigint,"ts" timestamptz,"ts_prec" text,"ts_prec_reason" text,"unlocks" jsonb,"flows" jsonb,"coverage_errors" jsonb,"balances" jsonb,"pools" jsonb,"series" jsonb,"metrics" jsonb,"protocols" jsonb,"identity_evidence" jsonb,"inference_limits" text[],"unsupported_venues" text[],"semantic_fingerprint" text,UNIQUE (storage_key));
CREATE INDEX ON mkt.supply_obs (received_ts,storage_key);
CREATE INDEX ON mkt.supply_obs (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.supply_obs (recorded_at,storage_key);
CREATE INDEX ON mkt.supply_obs (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.supply_obs (ts,storage_key);
CREATE INDEX ON mkt.supply_obs (token_id,ts,storage_key);
CREATE INDEX ON mkt.supply_obs(storage_key);
ALTER TABLE IF EXISTS mkt.supply SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.supply'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.supply (storage_key text NOT NULL,"available" boolean,"chain" text,"classification_required" boolean,"code_version" text,"config_version" text,"fee_scope" text,"instrument_id" text,"kind" text,"reason" text,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"token_id" text,"transfer_queries_succeeded" bigint,"ts" timestamptz,"ts_prec" text,"ts_prec_reason" text,"unlocks" jsonb,"flows" jsonb,"coverage_errors" jsonb,"balances" jsonb,"pools" jsonb,"series" jsonb,"metrics" jsonb,"protocols" jsonb,"identity_evidence" jsonb,"inference_limits" text[],"unsupported_venues" text[],"semantic_fingerprint" text,UNIQUE (storage_key));
CREATE INDEX ON mkt.supply (received_ts,storage_key);
CREATE INDEX ON mkt.supply (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.supply (recorded_at,storage_key);
CREATE INDEX ON mkt.supply (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.supply (ts,storage_key);
CREATE INDEX ON mkt.supply (token_id,ts,storage_key);
CREATE INDEX ON mkt.supply(storage_key);
ALTER TABLE IF EXISTS mkt.unlocks SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.unlocks'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.unlocks (storage_key text NOT NULL,"code_version" text,"config_version" text,"date" timestamptz,"date_prec" text,"date_prec_reason" text,"instrument_id" text,"kind" text,"label" text,"observed_at" timestamptz,"observed_at_prec" text,"observed_at_prec_reason" text,"qty" numeric,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"sale_inferred" boolean,"token_id" text,"semantic_fingerprint" text,UNIQUE (storage_key));
CREATE INDEX ON mkt.unlocks (date,storage_key);
CREATE INDEX ON mkt.unlocks (token_id,date,storage_key);
CREATE INDEX ON mkt.unlocks (observed_at,storage_key);
CREATE INDEX ON mkt.unlocks (token_id,observed_at,storage_key);
CREATE INDEX ON mkt.unlocks (received_ts,storage_key);
CREATE INDEX ON mkt.unlocks (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.unlocks (recorded_at,storage_key);
CREATE INDEX ON mkt.unlocks (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.unlocks(storage_key);
ALTER TABLE IF EXISTS mkt.hot_tokens SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.hot_tokens'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.hot_tokens (storage_key text NOT NULL,"code_version" text,"config_version" text,"event_id" text NOT NULL,"expires_at" timestamptz,"expires_at_prec" text,"expires_at_prec_reason" text,"instrument_id" text,"priority" bigint,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"since" timestamptz,"since_prec" text,"since_prec_reason" text,"token_id" text NOT NULL,"semantic_fingerprint" text,UNIQUE (token_id,event_id),UNIQUE(storage_key));
CREATE INDEX ON mkt.hot_tokens (expires_at,storage_key);
CREATE INDEX ON mkt.hot_tokens (token_id,expires_at,storage_key);
CREATE INDEX ON mkt.hot_tokens (received_ts,storage_key);
CREATE INDEX ON mkt.hot_tokens (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.hot_tokens (recorded_at,storage_key);
CREATE INDEX ON mkt.hot_tokens (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.hot_tokens (since,storage_key);
CREATE INDEX ON mkt.hot_tokens (token_id,since,storage_key);
CREATE INDEX ON mkt.hot_tokens(storage_key);
ALTER TABLE IF EXISTS mkt.capture_state SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.capture_state'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.capture_state (storage_key text NOT NULL,"code_version" text,"config_version" text,"instrument_id" text NOT NULL,"last_trade_id" text,"last_ts" timestamptz,"last_ts_prec" text,"last_ts_prec_reason" text,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"token_id" text,"updated_at" timestamptz,"updated_at_prec" text,"updated_at_prec_reason" text,"last_trade" jsonb,"missing_id_ranges" jsonb,"semantic_fingerprint" text,UNIQUE (instrument_id),UNIQUE(storage_key));
CREATE INDEX ON mkt.capture_state (last_ts,storage_key);
CREATE INDEX ON mkt.capture_state (token_id,last_ts,storage_key);
CREATE INDEX ON mkt.capture_state (received_ts,storage_key);
CREATE INDEX ON mkt.capture_state (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.capture_state (recorded_at,storage_key);
CREATE INDEX ON mkt.capture_state (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.capture_state (updated_at,storage_key);
CREATE INDEX ON mkt.capture_state (token_id,updated_at,storage_key);
CREATE INDEX ON mkt.capture_state(storage_key);
ALTER TABLE IF EXISTS mkt.capture_gaps SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.capture_gaps'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.capture_gaps (storage_key text NOT NULL,"code_version" text,"config_version" text,"end" timestamptz,"end_prec" text,"end_prec_reason" text,"gap_id" text NOT NULL,"instrument_id" text,"kind" text,"observed_at" timestamptz,"observed_at_prec" text,"observed_at_prec_reason" text,"reason" text,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"start" timestamptz,"start_prec" text,"start_prec_reason" text,"status" text,"token_id" text,"semantic_fingerprint" text,UNIQUE (gap_id),UNIQUE(storage_key));
CREATE INDEX ON mkt.capture_gaps ("end",storage_key);
CREATE INDEX ON mkt.capture_gaps (token_id,"end",storage_key);
CREATE INDEX ON mkt.capture_gaps (observed_at,storage_key);
CREATE INDEX ON mkt.capture_gaps (token_id,observed_at,storage_key);
CREATE INDEX ON mkt.capture_gaps (received_ts,storage_key);
CREATE INDEX ON mkt.capture_gaps (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.capture_gaps (recorded_at,storage_key);
CREATE INDEX ON mkt.capture_gaps (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.capture_gaps ("start",storage_key);
CREATE INDEX ON mkt.capture_gaps (token_id,"start",storage_key);
CREATE INDEX ON mkt.capture_gaps(storage_key);
ALTER TABLE IF EXISTS mkt.raw_archives SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.raw_archives'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.raw_archives (storage_key text NOT NULL,"archive_id" text NOT NULL,"byte_length" bigint,"checksum_scope" text,"code_version" text,"config_version" text,"hour_start" timestamptz,"hour_start_prec" text,"hour_start_prec_reason" text,"import_complete" boolean,"instrument_id" text,"journal_path" text,"kind" text,"parquet_path" text,"path" text,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"row_count" bigint,"sha256" text,"source_url" text,"status" text,"token_id" text,"integrity" jsonb,"semantic_fingerprint" text,UNIQUE (archive_id),UNIQUE(storage_key));
CREATE INDEX ON mkt.raw_archives (hour_start,storage_key);
CREATE INDEX ON mkt.raw_archives (token_id,hour_start,storage_key);
CREATE INDEX ON mkt.raw_archives (received_ts,storage_key);
CREATE INDEX ON mkt.raw_archives (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.raw_archives (recorded_at,storage_key);
CREATE INDEX ON mkt.raw_archives (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.raw_archives(storage_key);
ALTER TABLE IF EXISTS mkt.backfill_runs SET SCHEMA archive;
DO $$ DECLARE child regclass; BEGIN FOR child IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent='archive.backfill_runs'::regclass LOOP EXECUTE format('ALTER TABLE %s SET SCHEMA archive',child); END LOOP; END $$;
CREATE TABLE mkt.backfill_runs (storage_key text NOT NULL,"anchor" timestamptz,"anchor_prec" text,"anchor_prec_reason" text,"backfill_id" text NOT NULL,"code_version" text,"config_version" text,"event_id" text,"finished_at" timestamptz,"finished_at_prec" text,"finished_at_prec_reason" text,"instrument_id" text,"reason" text,"received_ts" timestamptz,"received_ts_prec" text,"received_ts_prec_reason" text,"recorded_at" timestamptz,"recorded_at_prec" text,"recorded_at_prec_reason" text,"started_at" timestamptz,"started_at_prec" text,"started_at_prec_reason" text,"status" text,"token_id" text,"result" jsonb,"semantic_fingerprint" text,UNIQUE (backfill_id),UNIQUE(storage_key));
CREATE INDEX ON mkt.backfill_runs (anchor,storage_key);
CREATE INDEX ON mkt.backfill_runs (token_id,anchor,storage_key);
CREATE INDEX ON mkt.backfill_runs (finished_at,storage_key);
CREATE INDEX ON mkt.backfill_runs (token_id,finished_at,storage_key);
CREATE INDEX ON mkt.backfill_runs (received_ts,storage_key);
CREATE INDEX ON mkt.backfill_runs (token_id,received_ts,storage_key);
CREATE INDEX ON mkt.backfill_runs (recorded_at,storage_key);
CREATE INDEX ON mkt.backfill_runs (token_id,recorded_at,storage_key);
CREATE INDEX ON mkt.backfill_runs (started_at,storage_key);
CREATE INDEX ON mkt.backfill_runs (token_id,started_at,storage_key);
CREATE INDEX ON mkt.backfill_runs(storage_key);
-- Raw rows may only be removed after Python export/readback verification.
DROP TABLE IF EXISTS mkt.trades;

-- Pure semantic comparison: transport receipt clocks never replace source clocks.
CREATE OR REPLACE FUNCTION ops.market_semantic(value jsonb) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE result jsonb; pair record;
BEGIN
 IF jsonb_typeof(value)='object' THEN
  result='{}';
  FOR pair IN SELECT key,val FROM jsonb_each(value) AS e(key,val) LOOP
   IF pair.val <> 'null'::jsonb AND pair.key NOT IN ('semantic_fingerprint','storage_key','code_version','config_version') AND pair.key !~ '^(received_ts|recorded_at|updated_at|observed_at)($|_)' THEN
    result=result || jsonb_build_object(pair.key,ops.market_semantic(pair.val));
   END IF;
  END LOOP;
  RETURN result;
 ELSIF jsonb_typeof(value)='array' THEN
  RETURN coalesce((SELECT jsonb_agg(ops.market_semantic(v) ORDER BY ord) FROM jsonb_array_elements(value) WITH ORDINALITY AS a(v,ord)),'[]');
 END IF;
 RETURN value;
END $$;

CREATE OR REPLACE FUNCTION ops.merge_market_point(kind text, oldrow jsonb, incoming jsonb) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE merged jsonb; sources jsonb; pair record; chosen jsonb; path text; field text; oldmeta jsonb; newmeta jsonb;
BEGIN
 merged=oldrow||jsonb_strip_nulls(incoming);
 IF kind='funding' THEN
  sources=coalesce(nullif(oldrow->'source_observations','null'::jsonb),'{}');
  FOR pair IN SELECT key,val FROM jsonb_each(coalesce(nullif(incoming->'source_observations','null'::jsonb),'{}')) AS e(key,val) LOOP
   IF ops.market_semantic(sources->pair.key) IS DISTINCT FROM ops.market_semantic(pair.val) THEN
    sources=jsonb_set(sources,ARRAY[pair.key],pair.val,true);
   END IF;
  END LOOP;
  merged=jsonb_set(merged,'{source_observations}',sources,true);
  merged=jsonb_set(merged,'{source_disagreement}',jsonb_build_object('present',
    (SELECT count(DISTINCT v->>'rate_8h_eq')>1 FROM jsonb_each(sources) AS e(k,v)),
    'values',coalesce((SELECT jsonb_object_agg(k,jsonb_build_object('rate',v->'rate','interval_h',v->'interval_h','rate_8h_eq',v->'rate_8h_eq')) FROM jsonb_each(sources) AS e(k,v)),'{}')),true);
  path=CASE WHEN incoming->>'kind'='SETTLED' AND sources ? 'PUBLIC_REST' THEN 'PUBLIC_REST' WHEN incoming->>'kind'<>'SETTLED' AND sources ? 'WEBSOCKET' THEN 'WEBSOCKET' ELSE incoming->>'capture_source' END;
  chosen=sources->path;
  IF chosen IS NOT NULL THEN
   FOREACH field IN ARRAY ARRAY['rate','interval_h','rate_8h_eq','as_of','as_of_prec','as_of_prec_reason','funding_time','funding_time_prec','funding_time_prec_reason','received_ts','received_ts_prec','received_ts_prec_reason','recorded_at','recorded_at_prec','recorded_at_prec_reason','source_path'] LOOP
    IF chosen ? field THEN merged=jsonb_set(merged,ARRAY[field],chosen->field,true); END IF;
   END LOOP;
   merged=jsonb_set(merged,'{capture_source}',to_jsonb(path),true);
   merged=jsonb_set(merged,'{primary_source}',to_jsonb(path),true);
  END IF;
 ELSIF kind='ticker' THEN
  sources=coalesce(nullif(oldrow->'field_sources','null'::jsonb),'{}');
  FOR pair IN SELECT key,val FROM jsonb_each(coalesce(nullif(incoming->'field_sources','null'::jsonb),'{}')) AS e(key,val) LOOP
   field=pair.key;newmeta=pair.val;oldmeta=sources->field;
   IF oldmeta IS NULL OR ((newmeta->>'as_of') IS NOT NULL AND ((oldmeta->>'as_of') IS NULL OR (newmeta->>'as_of')::timestamptz >= (oldmeta->>'as_of')::timestamptz)) THEN
    sources=jsonb_set(sources,ARRAY[field],newmeta,true);
   ELSE
    IF oldrow ? field THEN merged=jsonb_set(merged,ARRAY[field],oldrow->field,true); END IF;
   END IF;
  END LOOP;
  -- Fields absent from a partial packet retain their captured values and clocks.
  FOR pair IN SELECT key,val FROM jsonb_each(coalesce(nullif(oldrow->'field_sources','null'::jsonb),'{}')) AS e(key,val) LOOP
   IF NOT coalesce(nullif(incoming->'field_sources','null'::jsonb),'{}') ? pair.key AND oldrow ? pair.key THEN
    merged=jsonb_set(merged,ARRAY[pair.key],oldrow->pair.key,true);
   END IF;
  END LOOP;
  merged=jsonb_set(merged,'{field_sources}',sources,true);
  FOREACH field IN ARRAY ARRAY['mark','index'] LOOP
   chosen=sources->(field||'_price');
   IF chosen IS NOT NULL THEN
    merged=jsonb_set(merged,ARRAY[field||'_as_of'],coalesce(chosen->'as_of','null'),true);
    merged=jsonb_set(merged,ARRAY[field||'_as_of_prec'],coalesce(chosen->'as_of_prec','null'),true);
    merged=jsonb_set(merged,ARRAY[field||'_as_of_prec_reason'],coalesce(chosen->'as_of_prec_reason','null'),true);
   END IF;
  END LOOP;
  merged=jsonb_set(merged,'{capture_sources}',coalesce((SELECT jsonb_agg(DISTINCT v->'capture_source') FROM jsonb_each(sources) AS e(k,v) WHERE v->>'capture_source' IS NOT NULL),'[]'),true);
  merged=jsonb_set(merged,'{unclocked_components}',coalesce(nullif(oldrow->'unclocked_components','null'),'{}')||coalesce(nullif(incoming->'unclocked_components','null'),'{}'),true);
 END IF;
 merged=jsonb_set(merged,'{semantic_fingerprint}',to_jsonb(encode(sha256(convert_to(ops.market_semantic(merged)::text,'UTF8')),'hex')),true);
 RETURN merged;
END $$;

CREATE OR REPLACE FUNCTION ops.protect_market_archive() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'immutable legacy market evidence: %.%',TG_TABLE_SCHEMA,TG_TABLE_NAME; END $$;
CREATE OR REPLACE FUNCTION ops.protect_sealed_raw_manifest() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.status='SEALED' THEN RAISE EXCEPTION 'immutable sealed raw archive: %',OLD.archive_id; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER sealed_raw_manifest BEFORE UPDATE OR DELETE ON mkt.raw_archives FOR EACH ROW EXECUTE FUNCTION ops.protect_sealed_raw_manifest();

CREATE SEQUENCE mkt.liq_obs_id_seq OWNED BY mkt.liq_obs.id;
ALTER TABLE mkt.liq_obs ALTER COLUMN id SET DEFAULT nextval('mkt.liq_obs_id_seq');

CREATE TABLE ops.market_migration_exports (
 storage_key text PRIMARY KEY, document jsonb NOT NULL, immutable boolean NOT NULL DEFAULT true,
 export_id text NOT NULL UNIQUE, manifest_path text NOT NULL, manifest_sha256 text NOT NULL,
 row_count bigint NOT NULL CHECK(row_count>=0), created_at timestamptz NOT NULL,
 created_at_prec text NOT NULL CHECK(created_at_prec='COLLECTION_ONLY'), created_at_prec_reason text
);
CREATE TRIGGER immutable_market_export BEFORE UPDATE OR DELETE ON ops.market_migration_exports FOR EACH ROW EXECUTE FUNCTION ops.protect_market_archive();

-- TRUNCATE bypasses row triggers; audit ledgers reject the whole-table operation.
CREATE TRIGGER immutable_market_export_truncate BEFORE TRUNCATE ON ops.market_migration_exports FOR EACH STATEMENT EXECUTE FUNCTION ops.protect_market_archive();
CREATE TRIGGER sealed_raw_manifest_truncate BEFORE TRUNCATE ON mkt.raw_archives FOR EACH STATEMENT EXECUTE FUNCTION ops.protect_market_archive();
