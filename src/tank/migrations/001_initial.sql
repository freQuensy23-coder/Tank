CREATE SCHEMA IF NOT EXISTS reg;
CREATE SCHEMA IF NOT EXISTS ingest;
CREATE SCHEMA IF NOT EXISTS evt;
CREATE SCHEMA IF NOT EXISTS mkt;
CREATE SCHEMA IF NOT EXISTS ana;
CREATE SCHEMA IF NOT EXISTS out;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS scout;
CREATE SCHEMA IF NOT EXISTS qa;
CREATE OR REPLACE FUNCTION ops.forbid_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'append-only table: %', TG_TABLE_NAME; END $$;
CREATE TABLE IF NOT EXISTS ingest.source_items (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"item_id" text,
"source_id" text,
"feed" text,
"external_id" text,
"url" text,
"title" text,
"body" text,
"published_at" timestamptz,
"published_prec" text,
"observed_at" timestamptz,
"raw_path" text,
"raw_sha256" char(64),
"content_hash" char(64),
"source_class" text,
"relevance_decision" text,
"relevance_p" real,
"route_decision" text,
"route_event_id" text,
"route_p" real,
"extraction_status" text,
UNIQUE ("source_id", "feed", "external_id"));
CREATE INDEX IF NOT EXISTS ingest_source_items_item_id_idx ON ingest.source_items ("item_id");
CREATE INDEX IF NOT EXISTS ingest_source_items_source_id_idx ON ingest.source_items ("source_id");
CREATE INDEX IF NOT EXISTS ingest_source_items_external_id_idx ON ingest.source_items ("external_id");
CREATE INDEX IF NOT EXISTS ingest_source_items_published_at_idx ON ingest.source_items ("published_at");
CREATE INDEX IF NOT EXISTS ingest_source_items_observed_at_idx ON ingest.source_items ("observed_at");
CREATE INDEX IF NOT EXISTS ingest_source_items_content_hash_idx ON ingest.source_items ("content_hash");
CREATE INDEX IF NOT EXISTS ingest_source_items_route_event_id_idx ON ingest.source_items ("route_event_id");
CREATE INDEX IF NOT EXISTS ingest_source_items_doc_idx ON ingest.source_items USING gin(document);
CREATE TABLE IF NOT EXISTS evt.events (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"event_id" text,
"statement" text,
"theme_tags" text[],
"event_time" timestamptz,
"event_time_prec" text,
"public_at" timestamptz,
"public_at_prec" text,
"public_at_basis" text,
"known_to_us_at" timestamptz,
"verified_at" timestamptz,
"freshness" text,
"event_state" text,
"state_verified" boolean,
"negative_marker" text,
"certainty" text,
"timing_confidence" text,
"best_source_class" text,
"next_milestone" jsonb,
"lifecycle" text,
"merged_into" text,
"current_ems_id" text);
CREATE INDEX IF NOT EXISTS evt_events_event_id_idx ON evt.events ("event_id");
CREATE INDEX IF NOT EXISTS evt_events_event_time_idx ON evt.events ("event_time");
CREATE INDEX IF NOT EXISTS evt_events_public_at_idx ON evt.events ("public_at");
CREATE INDEX IF NOT EXISTS evt_events_known_to_us_at_idx ON evt.events ("known_to_us_at");
CREATE INDEX IF NOT EXISTS evt_events_verified_at_idx ON evt.events ("verified_at");
CREATE INDEX IF NOT EXISTS evt_events_lifecycle_idx ON evt.events ("lifecycle");
CREATE INDEX IF NOT EXISTS evt_events_current_ems_id_idx ON evt.events ("current_ems_id");
CREATE INDEX IF NOT EXISTS evt_events_doc_idx ON evt.events USING gin(document);
CREATE TABLE IF NOT EXISTS evt.event_entities (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"event_id" text,
"entity_id" text,
"role" text);
CREATE INDEX IF NOT EXISTS evt_event_entities_event_id_idx ON evt.event_entities ("event_id");
CREATE INDEX IF NOT EXISTS evt_event_entities_entity_id_idx ON evt.event_entities ("entity_id");
CREATE INDEX IF NOT EXISTS evt_event_entities_doc_idx ON evt.event_entities USING gin(document);
CREATE TABLE IF NOT EXISTS evt.event_observations (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"obs_id" text,
"event_id" text,
"item_id" text,
"link_type" text,
"public_at" timestamptz,
"freshness" text,
"match_method" text,
"observed_at" timestamptz);
CREATE INDEX IF NOT EXISTS evt_event_observations_obs_id_idx ON evt.event_observations ("obs_id");
CREATE INDEX IF NOT EXISTS evt_event_observations_event_id_idx ON evt.event_observations ("event_id");
CREATE INDEX IF NOT EXISTS evt_event_observations_item_id_idx ON evt.event_observations ("item_id");
CREATE INDEX IF NOT EXISTS evt_event_observations_public_at_idx ON evt.event_observations ("public_at");
CREATE INDEX IF NOT EXISTS evt_event_observations_observed_at_idx ON evt.event_observations ("observed_at");
CREATE INDEX IF NOT EXISTS evt_event_observations_doc_idx ON evt.event_observations USING gin(document);
CREATE TABLE IF NOT EXISTS evt.claims (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"claim_id" text,
"obs_id" text,
"claim_type" text,
"subject" text,
"value_num" numeric,
"state_claim" text,
"directionality" text,
"evidence_class" text,
"verified" boolean,
"quote" text,
"accepted" boolean,
"rejection_reason" text,
"predicate" text,
"object" text,
"value_unit" text);
CREATE INDEX IF NOT EXISTS evt_claims_claim_id_idx ON evt.claims ("claim_id");
CREATE INDEX IF NOT EXISTS evt_claims_obs_id_idx ON evt.claims ("obs_id");
CREATE INDEX IF NOT EXISTS evt_claims_doc_idx ON evt.claims USING gin(document);
CREATE TABLE IF NOT EXISTS evt.state_changes (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"id" bigint,
"event_id" text,
"edge_id" text,
"ladder" text,
"from_state" text,
"to_state" text,
"evidence" text[],
"changed_at" timestamptz);
CREATE INDEX IF NOT EXISTS evt_state_changes_event_id_idx ON evt.state_changes ("event_id");
CREATE INDEX IF NOT EXISTS evt_state_changes_edge_id_idx ON evt.state_changes ("edge_id");
CREATE INDEX IF NOT EXISTS evt_state_changes_changed_at_idx ON evt.state_changes ("changed_at");
CREATE INDEX IF NOT EXISTS evt_state_changes_doc_idx ON evt.state_changes USING gin(document);
CREATE TRIGGER append_only BEFORE UPDATE OR DELETE ON evt.state_changes FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();
CREATE TABLE IF NOT EXISTS evt.ems_assessments (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"ems_id" text,
"event_id" text,
"version" int,
"cutoff" timestamptz,
"dims" jsonb,
"gates" jsonb,
"class" text,
"criterion" text,
"class_reasons" text[],
"impact_horizon" jsonb,
"config_version" text,
"model_id" text,
"prompt_version" text,
UNIQUE ("event_id", "version"));
CREATE INDEX IF NOT EXISTS evt_ems_assessments_ems_id_idx ON evt.ems_assessments ("ems_id");
CREATE INDEX IF NOT EXISTS evt_ems_assessments_event_id_idx ON evt.ems_assessments ("event_id");
CREATE INDEX IF NOT EXISTS evt_ems_assessments_cutoff_idx ON evt.ems_assessments ("cutoff");
CREATE INDEX IF NOT EXISTS evt_ems_assessments_model_id_idx ON evt.ems_assessments ("model_id");
CREATE INDEX IF NOT EXISTS evt_ems_assessments_doc_idx ON evt.ems_assessments USING gin(document);
CREATE TABLE IF NOT EXISTS mkt.flow_bars_1m (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"instrument_id" text,
"minute_start" timestamptz,
"buy_qty" numeric,
"buy_usd" numeric,
"n_buy" int,
"sum_sq_qty" numeric,
"o" numeric,
"size_buckets" jsonb,
"first_trade_id" text,
"complete" boolean,
"gap_reason" text,
"sell_qty" numeric,
"sell_usd" numeric,
"n_sell" int,
"h" numeric,
"l" numeric,
"c" numeric,
"vwap" numeric,
"last_trade_id" text,
UNIQUE ("instrument_id", "minute_start"));
CREATE INDEX IF NOT EXISTS mkt_flow_bars_1m_instrument_id_idx ON mkt.flow_bars_1m ("instrument_id");
CREATE INDEX IF NOT EXISTS mkt_flow_bars_1m_minute_start_idx ON mkt.flow_bars_1m ("minute_start");
CREATE INDEX IF NOT EXISTS mkt_flow_bars_1m_first_trade_id_idx ON mkt.flow_bars_1m ("first_trade_id");
CREATE INDEX IF NOT EXISTS mkt_flow_bars_1m_last_trade_id_idx ON mkt.flow_bars_1m ("last_trade_id");
CREATE INDEX IF NOT EXISTS mkt_flow_bars_1m_doc_idx ON mkt.flow_bars_1m USING gin(document);
CREATE TABLE IF NOT EXISTS mkt.oi_obs (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"instrument_id" text,
"metric_key" text,
"ts" timestamptz,
"ts_prec" text,
"received_ts" timestamptz,
"raw_value" numeric,
"raw_unit" text,
"sidedness" text,
"oi_native" numeric,
"oi_usd" numeric,
UNIQUE ("instrument_id", "metric_key", "ts"));
CREATE INDEX IF NOT EXISTS mkt_oi_obs_instrument_id_idx ON mkt.oi_obs ("instrument_id");
CREATE INDEX IF NOT EXISTS mkt_oi_obs_ts_idx ON mkt.oi_obs ("ts");
CREATE INDEX IF NOT EXISTS mkt_oi_obs_received_ts_idx ON mkt.oi_obs ("received_ts");
CREATE INDEX IF NOT EXISTS mkt_oi_obs_doc_idx ON mkt.oi_obs USING gin(document);
CREATE TABLE IF NOT EXISTS mkt.funding_obs (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"instrument_id" text,
"kind" text,
"rate" numeric,
"interval_h" numeric,
"rate_8h_eq" numeric,
"funding_time" timestamptz,
"as_of" timestamptz,
"metric_key" text,
UNIQUE ("instrument_id", "metric_key", "kind", "as_of"));
CREATE INDEX IF NOT EXISTS mkt_funding_obs_instrument_id_idx ON mkt.funding_obs ("instrument_id");
CREATE INDEX IF NOT EXISTS mkt_funding_obs_funding_time_idx ON mkt.funding_obs ("funding_time");
CREATE INDEX IF NOT EXISTS mkt_funding_obs_as_of_idx ON mkt.funding_obs ("as_of");
CREATE INDEX IF NOT EXISTS mkt_funding_obs_doc_idx ON mkt.funding_obs USING gin(document);
CREATE TABLE IF NOT EXISTS mkt.liq_obs (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"id" bigint,
"instrument_id" text,
"ts" timestamptz,
"side" text,
"qty_native" numeric,
"notional_usd" numeric,
"price" numeric,
"coverage" text);
CREATE INDEX IF NOT EXISTS mkt_liq_obs_instrument_id_idx ON mkt.liq_obs ("instrument_id");
CREATE INDEX IF NOT EXISTS mkt_liq_obs_ts_idx ON mkt.liq_obs ("ts");
CREATE INDEX IF NOT EXISTS mkt_liq_obs_doc_idx ON mkt.liq_obs USING gin(document);
CREATE TABLE IF NOT EXISTS mkt.hot_tokens (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"token_id" text,
"event_id" text,
"priority" smallint,
"since" timestamptz,
"expires_at" timestamptz);
CREATE INDEX IF NOT EXISTS mkt_hot_tokens_token_id_idx ON mkt.hot_tokens ("token_id");
CREATE INDEX IF NOT EXISTS mkt_hot_tokens_event_id_idx ON mkt.hot_tokens ("event_id");
CREATE INDEX IF NOT EXISTS mkt_hot_tokens_priority_idx ON mkt.hot_tokens ("priority");
CREATE INDEX IF NOT EXISTS mkt_hot_tokens_since_idx ON mkt.hot_tokens ("since");
CREATE INDEX IF NOT EXISTS mkt_hot_tokens_expires_at_idx ON mkt.hot_tokens ("expires_at");
CREATE INDEX IF NOT EXISTS mkt_hot_tokens_doc_idx ON mkt.hot_tokens USING gin(document);
CREATE TABLE IF NOT EXISTS ana.market_truth (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"mta_id" text,
"token_id" text,
"event_id" text,
"cutoff" timestamptz,
"primary_window" text,
"move_class" text,
"support" int,
"alternatives" text[],
"divergence" jsonb,
"flags" text[],
"decisive_facts" text[],
"data_confidence" jsonb,
"class_confidence" text,
"detail" jsonb,
"config_version" text,
"code_version" text);
CREATE INDEX IF NOT EXISTS ana_market_truth_mta_id_idx ON ana.market_truth ("mta_id");
CREATE INDEX IF NOT EXISTS ana_market_truth_token_id_idx ON ana.market_truth ("token_id");
CREATE INDEX IF NOT EXISTS ana_market_truth_event_id_idx ON ana.market_truth ("event_id");
CREATE INDEX IF NOT EXISTS ana_market_truth_cutoff_idx ON ana.market_truth ("cutoff");
CREATE INDEX IF NOT EXISTS ana_market_truth_doc_idx ON ana.market_truth USING gin(document);
CREATE TABLE IF NOT EXISTS ana.decisions (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"dec_id" text,
"event_id" text,
"token_id" text,
"mta_id" text,
"cutoff" timestamptz,
"thesis_grade" char(1),
"trade_grade" char(1),
"action" text,
"action_rule" text,
"tactical" boolean,
"investigate" boolean,
"call_id" text,
"outlook" jsonb,
"conditions" text[],
"thesis_rule" text,
"trade_reasons" text);
CREATE INDEX IF NOT EXISTS ana_decisions_dec_id_idx ON ana.decisions ("dec_id");
CREATE INDEX IF NOT EXISTS ana_decisions_event_id_idx ON ana.decisions ("event_id");
CREATE INDEX IF NOT EXISTS ana_decisions_token_id_idx ON ana.decisions ("token_id");
CREATE INDEX IF NOT EXISTS ana_decisions_mta_id_idx ON ana.decisions ("mta_id");
CREATE INDEX IF NOT EXISTS ana_decisions_cutoff_idx ON ana.decisions ("cutoff");
CREATE INDEX IF NOT EXISTS ana_decisions_call_id_idx ON ana.decisions ("call_id");
CREATE INDEX IF NOT EXISTS ana_decisions_doc_idx ON ana.decisions USING gin(document);
CREATE TABLE IF NOT EXISTS ana.calls (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"call_id" text,
"event_id" text,
"token_id" text,
"opened_by_alert" text,
"opened_by_version" int,
"opened_at" timestamptz,
"entry_ref_price" numeric,
"invalidation_price" numeric,
"status" text,
"closed_by" text,
"closed_at" timestamptz);
CREATE INDEX IF NOT EXISTS ana_calls_call_id_idx ON ana.calls ("call_id");
CREATE INDEX IF NOT EXISTS ana_calls_event_id_idx ON ana.calls ("event_id");
CREATE INDEX IF NOT EXISTS ana_calls_token_id_idx ON ana.calls ("token_id");
CREATE INDEX IF NOT EXISTS ana_calls_opened_at_idx ON ana.calls ("opened_at");
CREATE INDEX IF NOT EXISTS ana_calls_status_idx ON ana.calls ("status");
CREATE INDEX IF NOT EXISTS ana_calls_closed_at_idx ON ana.calls ("closed_at");
CREATE INDEX IF NOT EXISTS ana_calls_doc_idx ON ana.calls USING gin(document);
CREATE TABLE IF NOT EXISTS out.alerts (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"alert_id" text,
"version" int,
"kind" text,
"event_id" text,
"generated_at" timestamptz,
"card_text" text,
"card_struct" jsonb,
"decisions" text[],
"evidence_bundle_id" char(64),
"linter" jsonb,
"supersedes_version" int,
"config_version" text,
"code_version" text,
"model_id" text,
"prompt_version" text,
UNIQUE ("alert_id", "version"));
CREATE INDEX IF NOT EXISTS out_alerts_alert_id_idx ON out.alerts ("alert_id");
CREATE INDEX IF NOT EXISTS out_alerts_event_id_idx ON out.alerts ("event_id");
CREATE INDEX IF NOT EXISTS out_alerts_generated_at_idx ON out.alerts ("generated_at");
CREATE INDEX IF NOT EXISTS out_alerts_evidence_bundle_id_idx ON out.alerts ("evidence_bundle_id");
CREATE INDEX IF NOT EXISTS out_alerts_model_id_idx ON out.alerts ("model_id");
CREATE INDEX IF NOT EXISTS out_alerts_doc_idx ON out.alerts USING gin(document);
CREATE TRIGGER append_only BEFORE UPDATE OR DELETE ON out.alerts FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();
CREATE TABLE IF NOT EXISTS out.ops_notices (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"notice_id" text,
"kind" text,
"subject_id" text,
"text" text,
"created_at" timestamptz,
"created_by" text);
CREATE INDEX IF NOT EXISTS out_ops_notices_notice_id_idx ON out.ops_notices ("notice_id");
CREATE INDEX IF NOT EXISTS out_ops_notices_subject_id_idx ON out.ops_notices ("subject_id");
CREATE INDEX IF NOT EXISTS out_ops_notices_created_at_idx ON out.ops_notices ("created_at");
CREATE INDEX IF NOT EXISTS out_ops_notices_doc_idx ON out.ops_notices USING gin(document);
CREATE TRIGGER append_only BEFORE UPDATE OR DELETE ON out.ops_notices FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();
CREATE TABLE IF NOT EXISTS out.deliveries (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"id" bigint,
"alert_id" text,
"version" int,
"notice_id" text,
"channel" text,
"status" text,
"message_ref" text,
"attempt" int,
"at" timestamptz,
"error" text);
CREATE INDEX IF NOT EXISTS out_deliveries_alert_id_idx ON out.deliveries ("alert_id");
CREATE INDEX IF NOT EXISTS out_deliveries_notice_id_idx ON out.deliveries ("notice_id");
CREATE INDEX IF NOT EXISTS out_deliveries_status_idx ON out.deliveries ("status");
CREATE INDEX IF NOT EXISTS out_deliveries_at_idx ON out.deliveries ("at");
CREATE INDEX IF NOT EXISTS out_deliveries_doc_idx ON out.deliveries USING gin(document);
CREATE TRIGGER append_only BEFORE UPDATE OR DELETE ON out.deliveries FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();
CREATE TABLE IF NOT EXISTS ops.jobs (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"job_id" text,
"type" text,
"priority" smallint,
"idem_key" text,
"payload" jsonb,
"status" text,
"attempts" int,
"max_attempts" int,
"run_after" timestamptz,
"expires_at" timestamptz,
"last_error" text,
CHECK (priority BETWEEN 0 AND 4),
CHECK (attempts >= 0 AND max_attempts > 0),
CHECK (status IN ('QUEUED','RUNNING','DONE','FAILED','EXPIRED')),
UNIQUE ("type", "idem_key"));
CREATE INDEX IF NOT EXISTS ops_jobs_job_id_idx ON ops.jobs ("job_id");
CREATE INDEX IF NOT EXISTS ops_jobs_priority_idx ON ops.jobs ("priority");
CREATE INDEX IF NOT EXISTS ops_jobs_status_idx ON ops.jobs ("status");
CREATE INDEX IF NOT EXISTS ops_jobs_run_after_idx ON ops.jobs ("run_after");
CREATE INDEX IF NOT EXISTS ops_jobs_expires_at_idx ON ops.jobs ("expires_at");
CREATE INDEX IF NOT EXISTS ops_jobs_doc_idx ON ops.jobs USING gin(document);
CREATE TABLE IF NOT EXISTS ops.llm_usage (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"id" bigint,
"at" timestamptz,
"call" text,
"subject_id" text,
"model" text,
"effort" text,
"input_tokens" int,
"cache_read_tokens" int,
"cache_write_tokens" int,
"output_tokens" int,
"cost_usd" numeric(10,6),
"web_searches" int,
"stop_reason" text);
CREATE INDEX IF NOT EXISTS ops_llm_usage_at_idx ON ops.llm_usage ("at");
CREATE INDEX IF NOT EXISTS ops_llm_usage_subject_id_idx ON ops.llm_usage ("subject_id");
CREATE INDEX IF NOT EXISTS ops_llm_usage_doc_idx ON ops.llm_usage USING gin(document);
CREATE TABLE IF NOT EXISTS ops.budget_state (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"day" date,
"spent_usd" numeric(10,4),
"paused_until" timestamptz);
CREATE INDEX IF NOT EXISTS ops_budget_state_day_idx ON ops.budget_state ("day");
CREATE INDEX IF NOT EXISTS ops_budget_state_paused_until_idx ON ops.budget_state ("paused_until");
CREATE INDEX IF NOT EXISTS ops_budget_state_doc_idx ON ops.budget_state USING gin(document);
CREATE TABLE IF NOT EXISTS ops.agent_sessions (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"session_id" text,
"event_id" text,
"opened_by_item" text,
"agent_version" text,
"status" text,
"opened_at" timestamptz,
"last_item_at" timestamptz,
"last_event_seq" bigint,
UNIQUE ("event_id"));
CREATE INDEX IF NOT EXISTS ops_agent_sessions_session_id_idx ON ops.agent_sessions ("session_id");
CREATE INDEX IF NOT EXISTS ops_agent_sessions_event_id_idx ON ops.agent_sessions ("event_id");
CREATE INDEX IF NOT EXISTS ops_agent_sessions_status_idx ON ops.agent_sessions ("status");
CREATE INDEX IF NOT EXISTS ops_agent_sessions_opened_at_idx ON ops.agent_sessions ("opened_at");
CREATE INDEX IF NOT EXISTS ops_agent_sessions_last_item_at_idx ON ops.agent_sessions ("last_item_at");
CREATE INDEX IF NOT EXISTS ops_agent_sessions_doc_idx ON ops.agent_sessions USING gin(document);
CREATE TABLE IF NOT EXISTS ops.tool_calls (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"call_id" text,
"session_id" text,
"tool" text,
"input" jsonb,
"result" jsonb,
"status" text,
"requested_at" timestamptz,
"answered_at" timestamptz);
CREATE INDEX IF NOT EXISTS ops_tool_calls_call_id_idx ON ops.tool_calls ("call_id");
CREATE INDEX IF NOT EXISTS ops_tool_calls_session_id_idx ON ops.tool_calls ("session_id");
CREATE INDEX IF NOT EXISTS ops_tool_calls_status_idx ON ops.tool_calls ("status");
CREATE INDEX IF NOT EXISTS ops_tool_calls_requested_at_idx ON ops.tool_calls ("requested_at");
CREATE INDEX IF NOT EXISTS ops_tool_calls_answered_at_idx ON ops.tool_calls ("answered_at");
CREATE INDEX IF NOT EXISTS ops_tool_calls_doc_idx ON ops.tool_calls USING gin(document);
CREATE TABLE IF NOT EXISTS reg.sources (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"source_id" text,
"name" text,
"mode" text);
CREATE INDEX IF NOT EXISTS reg_sources_source_id_idx ON reg.sources ("source_id");
CREATE INDEX IF NOT EXISTS reg_sources_doc_idx ON reg.sources USING gin(document);
CREATE TABLE IF NOT EXISTS reg.entities (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"entity_id" text,
"name" text);
CREATE INDEX IF NOT EXISTS reg_entities_entity_id_idx ON reg.entities ("entity_id");
CREATE INDEX IF NOT EXISTS reg_entities_doc_idx ON reg.entities USING gin(document);
CREATE TABLE IF NOT EXISTS reg.entity_aliases (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"entity_id" text,
"alias" text);
CREATE INDEX IF NOT EXISTS reg_entity_aliases_entity_id_idx ON reg.entity_aliases ("entity_id");
CREATE INDEX IF NOT EXISTS reg_entity_aliases_doc_idx ON reg.entity_aliases USING gin(document);
CREATE TABLE IF NOT EXISTS reg.tokens (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"token_id" text,
"symbol" text);
CREATE INDEX IF NOT EXISTS reg_tokens_token_id_idx ON reg.tokens ("token_id");
CREATE INDEX IF NOT EXISTS reg_tokens_doc_idx ON reg.tokens USING gin(document);
CREATE TABLE IF NOT EXISTS reg.instruments (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"instrument_id" text,
"token_id" text,
"venue" text,
"market_type" text,
"venue_symbol" text);
CREATE INDEX IF NOT EXISTS reg_instruments_instrument_id_idx ON reg.instruments ("instrument_id");
CREATE INDEX IF NOT EXISTS reg_instruments_token_id_idx ON reg.instruments ("token_id");
CREATE INDEX IF NOT EXISTS reg_instruments_doc_idx ON reg.instruments USING gin(document);
CREATE TABLE IF NOT EXISTS reg.sector_baskets (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"basket_id" text);
CREATE INDEX IF NOT EXISTS reg_sector_baskets_basket_id_idx ON reg.sector_baskets ("basket_id");
CREATE INDEX IF NOT EXISTS reg_sector_baskets_doc_idx ON reg.sector_baskets USING gin(document);
CREATE TABLE IF NOT EXISTS reg.dependency_map (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"edge_id" text,
"entity_id" text,
"token_id" text);
CREATE INDEX IF NOT EXISTS reg_dependency_map_edge_id_idx ON reg.dependency_map ("edge_id");
CREATE INDEX IF NOT EXISTS reg_dependency_map_entity_id_idx ON reg.dependency_map ("entity_id");
CREATE INDEX IF NOT EXISTS reg_dependency_map_token_id_idx ON reg.dependency_map ("token_id");
CREATE INDEX IF NOT EXISTS reg_dependency_map_doc_idx ON reg.dependency_map USING gin(document);
CREATE TABLE IF NOT EXISTS reg.metric_semantics (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"metric_key" text,
"venue" text,
"market_type" text,
"semantic_confidence" text);
CREATE INDEX IF NOT EXISTS reg_metric_semantics_doc_idx ON reg.metric_semantics USING gin(document);
CREATE TABLE IF NOT EXISTS reg.exchange_addresses (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"address" text,
"chain" text,
"entity_id" text);
CREATE INDEX IF NOT EXISTS reg_exchange_addresses_entity_id_idx ON reg.exchange_addresses ("entity_id");
CREATE INDEX IF NOT EXISTS reg_exchange_addresses_doc_idx ON reg.exchange_addresses USING gin(document);
CREATE TABLE IF NOT EXISTS evt.capture_chains (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"event_id" text,
"token_id" text,
"version" int,
"capture_class" text);
CREATE INDEX IF NOT EXISTS evt_capture_chains_event_id_idx ON evt.capture_chains ("event_id");
CREATE INDEX IF NOT EXISTS evt_capture_chains_token_id_idx ON evt.capture_chains ("token_id");
CREATE INDEX IF NOT EXISTS evt_capture_chains_doc_idx ON evt.capture_chains USING gin(document);
CREATE TABLE IF NOT EXISTS evt.beneficiary_maps (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"event_id" text,
"map_version" int,
"cutoff" timestamptz);
CREATE INDEX IF NOT EXISTS evt_beneficiary_maps_event_id_idx ON evt.beneficiary_maps ("event_id");
CREATE INDEX IF NOT EXISTS evt_beneficiary_maps_cutoff_idx ON evt.beneficiary_maps ("cutoff");
CREATE INDEX IF NOT EXISTS evt_beneficiary_maps_doc_idx ON evt.beneficiary_maps USING gin(document);
CREATE TABLE IF NOT EXISTS evt.beneficiary_edges (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"edge_id" text,
"event_id" text,
"map_version" int,
"entity_id" text,
"token_id" text,
"role" text,
"credible" boolean);
CREATE INDEX IF NOT EXISTS evt_beneficiary_edges_edge_id_idx ON evt.beneficiary_edges ("edge_id");
CREATE INDEX IF NOT EXISTS evt_beneficiary_edges_event_id_idx ON evt.beneficiary_edges ("event_id");
CREATE INDEX IF NOT EXISTS evt_beneficiary_edges_entity_id_idx ON evt.beneficiary_edges ("entity_id");
CREATE INDEX IF NOT EXISTS evt_beneficiary_edges_token_id_idx ON evt.beneficiary_edges ("token_id");
CREATE INDEX IF NOT EXISTS evt_beneficiary_edges_doc_idx ON evt.beneficiary_edges USING gin(document);
CREATE TABLE IF NOT EXISTS mkt.klines_1m (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"instrument_id" text,
"open_time" timestamptz,
"o" numeric,
"h" numeric,
"l" numeric,
"c" numeric,
"volume_native" numeric,
"quote_volume" numeric,
"complete" boolean,
UNIQUE ("instrument_id", "open_time"));
CREATE INDEX IF NOT EXISTS mkt_klines_1m_instrument_id_idx ON mkt.klines_1m ("instrument_id");
CREATE INDEX IF NOT EXISTS mkt_klines_1m_open_time_idx ON mkt.klines_1m ("open_time");
CREATE INDEX IF NOT EXISTS mkt_klines_1m_doc_idx ON mkt.klines_1m USING gin(document);
CREATE TABLE IF NOT EXISTS mkt.book_summaries (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"instrument_id" text,
"exchange_ts" timestamptz,
"received_ts" timestamptz,
"mid" numeric,
"best_bid" numeric,
"best_ask" numeric,
"spread_bps" numeric,
"integrity_ok" boolean,
UNIQUE ("instrument_id", "exchange_ts"));
CREATE INDEX IF NOT EXISTS mkt_book_summaries_instrument_id_idx ON mkt.book_summaries ("instrument_id");
CREATE INDEX IF NOT EXISTS mkt_book_summaries_exchange_ts_idx ON mkt.book_summaries ("exchange_ts");
CREATE INDEX IF NOT EXISTS mkt_book_summaries_received_ts_idx ON mkt.book_summaries ("received_ts");
CREATE INDEX IF NOT EXISTS mkt_book_summaries_doc_idx ON mkt.book_summaries USING gin(document);
CREATE TABLE IF NOT EXISTS mkt.tickers_24h (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"instrument_id" text,
"ts" timestamptz,
UNIQUE ("instrument_id", "ts"));
CREATE INDEX IF NOT EXISTS mkt_tickers_24h_instrument_id_idx ON mkt.tickers_24h ("instrument_id");
CREATE INDEX IF NOT EXISTS mkt_tickers_24h_ts_idx ON mkt.tickers_24h ("ts");
CREATE INDEX IF NOT EXISTS mkt_tickers_24h_doc_idx ON mkt.tickers_24h USING gin(document);
CREATE TABLE IF NOT EXISTS mkt.unlocks (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"token_id" text,
"date" timestamptz,
"qty" numeric);
CREATE INDEX IF NOT EXISTS mkt_unlocks_token_id_idx ON mkt.unlocks ("token_id");
CREATE INDEX IF NOT EXISTS mkt_unlocks_date_idx ON mkt.unlocks ("date");
CREATE INDEX IF NOT EXISTS mkt_unlocks_doc_idx ON mkt.unlocks USING gin(document);
CREATE TABLE IF NOT EXISTS mkt.supply_obs (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"token_id" text,
"ts" timestamptz,
"kind" text);
CREATE INDEX IF NOT EXISTS mkt_supply_obs_token_id_idx ON mkt.supply_obs ("token_id");
CREATE INDEX IF NOT EXISTS mkt_supply_obs_ts_idx ON mkt.supply_obs ("ts");
CREATE INDEX IF NOT EXISTS mkt_supply_obs_doc_idx ON mkt.supply_obs USING gin(document);
CREATE TABLE IF NOT EXISTS mkt.supply (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"token_id" text,
"ts" timestamptz,
"kind" text);
CREATE INDEX IF NOT EXISTS mkt_supply_token_id_idx ON mkt.supply ("token_id");
CREATE INDEX IF NOT EXISTS mkt_supply_ts_idx ON mkt.supply ("ts");
CREATE INDEX IF NOT EXISTS mkt_supply_doc_idx ON mkt.supply USING gin(document);
CREATE TABLE IF NOT EXISTS ana.outlooks (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"out_id" text,
"event_id" text,
"token_id" text,
"cutoff" timestamptz);
CREATE INDEX IF NOT EXISTS ana_outlooks_out_id_idx ON ana.outlooks ("out_id");
CREATE INDEX IF NOT EXISTS ana_outlooks_event_id_idx ON ana.outlooks ("event_id");
CREATE INDEX IF NOT EXISTS ana_outlooks_token_id_idx ON ana.outlooks ("token_id");
CREATE INDEX IF NOT EXISTS ana_outlooks_cutoff_idx ON ana.outlooks ("cutoff");
CREATE INDEX IF NOT EXISTS ana_outlooks_doc_idx ON ana.outlooks USING gin(document);
CREATE TABLE IF NOT EXISTS ana.contradictions (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"contradiction_id" text,
"event_id" text,
"token_id" text,
"detected_at" timestamptz,
"resolved_at" timestamptz);
CREATE INDEX IF NOT EXISTS ana_contradictions_contradiction_id_idx ON ana.contradictions ("contradiction_id");
CREATE INDEX IF NOT EXISTS ana_contradictions_event_id_idx ON ana.contradictions ("event_id");
CREATE INDEX IF NOT EXISTS ana_contradictions_token_id_idx ON ana.contradictions ("token_id");
CREATE INDEX IF NOT EXISTS ana_contradictions_detected_at_idx ON ana.contradictions ("detected_at");
CREATE INDEX IF NOT EXISTS ana_contradictions_resolved_at_idx ON ana.contradictions ("resolved_at");
CREATE INDEX IF NOT EXISTS ana_contradictions_doc_idx ON ana.contradictions USING gin(document);
CREATE TABLE IF NOT EXISTS ana.watch_conditions (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"cond_id" text,
"event_id" text,
"token_id" text,
"status" text,
"expires_at" timestamptz,
"fired_at" timestamptz);
CREATE INDEX IF NOT EXISTS ana_watch_conditions_cond_id_idx ON ana.watch_conditions ("cond_id");
CREATE INDEX IF NOT EXISTS ana_watch_conditions_event_id_idx ON ana.watch_conditions ("event_id");
CREATE INDEX IF NOT EXISTS ana_watch_conditions_token_id_idx ON ana.watch_conditions ("token_id");
CREATE INDEX IF NOT EXISTS ana_watch_conditions_status_idx ON ana.watch_conditions ("status");
CREATE INDEX IF NOT EXISTS ana_watch_conditions_expires_at_idx ON ana.watch_conditions ("expires_at");
CREATE INDEX IF NOT EXISTS ana_watch_conditions_fired_at_idx ON ana.watch_conditions ("fired_at");
CREATE INDEX IF NOT EXISTS ana_watch_conditions_doc_idx ON ana.watch_conditions USING gin(document);
CREATE TABLE IF NOT EXISTS out.evidence_bundles (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"bundle_id" text,
"cutoff" timestamptz);
CREATE INDEX IF NOT EXISTS out_evidence_bundles_bundle_id_idx ON out.evidence_bundles ("bundle_id");
CREATE INDEX IF NOT EXISTS out_evidence_bundles_cutoff_idx ON out.evidence_bundles ("cutoff");
CREATE INDEX IF NOT EXISTS out_evidence_bundles_doc_idx ON out.evidence_bundles USING gin(document);
CREATE TRIGGER append_only BEFORE UPDATE OR DELETE ON out.evidence_bundles FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();
CREATE TABLE IF NOT EXISTS out.reaction_checkpoints (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"rwp_id" text,
"event_id" text,
"token_id" text,
"captured_at" timestamptz,
"revision" int);
CREATE INDEX IF NOT EXISTS out_reaction_checkpoints_rwp_id_idx ON out.reaction_checkpoints ("rwp_id");
CREATE INDEX IF NOT EXISTS out_reaction_checkpoints_event_id_idx ON out.reaction_checkpoints ("event_id");
CREATE INDEX IF NOT EXISTS out_reaction_checkpoints_token_id_idx ON out.reaction_checkpoints ("token_id");
CREATE INDEX IF NOT EXISTS out_reaction_checkpoints_captured_at_idx ON out.reaction_checkpoints ("captured_at");
CREATE INDEX IF NOT EXISTS out_reaction_checkpoints_doc_idx ON out.reaction_checkpoints USING gin(document);
CREATE TRIGGER append_only BEFORE UPDATE OR DELETE ON out.reaction_checkpoints FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();
CREATE TABLE IF NOT EXISTS ops.source_checkpoints (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"source_id" text,
"cursor" text,
"updated_at" timestamptz);
CREATE INDEX IF NOT EXISTS ops_source_checkpoints_source_id_idx ON ops.source_checkpoints ("source_id");
CREATE INDEX IF NOT EXISTS ops_source_checkpoints_updated_at_idx ON ops.source_checkpoints ("updated_at");
CREATE INDEX IF NOT EXISTS ops_source_checkpoints_doc_idx ON ops.source_checkpoints USING gin(document);
CREATE TABLE IF NOT EXISTS ops.source_dedup (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"item_id" text);
CREATE INDEX IF NOT EXISTS ops_source_dedup_item_id_idx ON ops.source_dedup ("item_id");
CREATE INDEX IF NOT EXISTS ops_source_dedup_doc_idx ON ops.source_dedup USING gin(document);
CREATE TABLE IF NOT EXISTS ops.source_escalations (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"source_id" text,
"expires_at" timestamptz);
CREATE INDEX IF NOT EXISTS ops_source_escalations_source_id_idx ON ops.source_escalations ("source_id");
CREATE INDEX IF NOT EXISTS ops_source_escalations_expires_at_idx ON ops.source_escalations ("expires_at");
CREATE INDEX IF NOT EXISTS ops_source_escalations_doc_idx ON ops.source_escalations USING gin(document);
CREATE TABLE IF NOT EXISTS qa.reviews (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"review_id" text,
"subject" text,
"label" text,
"at" timestamptz);
CREATE INDEX IF NOT EXISTS qa_reviews_review_id_idx ON qa.reviews ("review_id");
CREATE INDEX IF NOT EXISTS qa_reviews_at_idx ON qa.reviews ("at");
CREATE INDEX IF NOT EXISTS qa_reviews_doc_idx ON qa.reviews USING gin(document);
CREATE TRIGGER append_only BEFORE UPDATE OR DELETE ON qa.reviews FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();
CREATE TABLE IF NOT EXISTS qa.fixtures (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"fixture_id" text,
"cutoff" timestamptz);
CREATE INDEX IF NOT EXISTS qa_fixtures_fixture_id_idx ON qa.fixtures ("fixture_id");
CREATE INDEX IF NOT EXISTS qa_fixtures_cutoff_idx ON qa.fixtures ("cutoff");
CREATE INDEX IF NOT EXISTS qa_fixtures_doc_idx ON qa.fixtures USING gin(document);
CREATE TABLE IF NOT EXISTS qa.replay_runs (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"run_id" text,
"fixture_id" text,
"at" timestamptz);
CREATE INDEX IF NOT EXISTS qa_replay_runs_run_id_idx ON qa.replay_runs ("run_id");
CREATE INDEX IF NOT EXISTS qa_replay_runs_fixture_id_idx ON qa.replay_runs ("fixture_id");
CREATE INDEX IF NOT EXISTS qa_replay_runs_at_idx ON qa.replay_runs ("at");
CREATE INDEX IF NOT EXISTS qa_replay_runs_doc_idx ON qa.replay_runs USING gin(document);
CREATE TABLE IF NOT EXISTS scout.expansion_proposals (storage_key text PRIMARY KEY,
document jsonb NOT NULL,
immutable boolean NOT NULL DEFAULT false,
"prp_id" text,
"hook" text,
"concept_name" text,
"human_action" text,
"decided_at" timestamptz);
CREATE INDEX IF NOT EXISTS scout_expansion_proposals_prp_id_idx ON scout.expansion_proposals ("prp_id");
CREATE INDEX IF NOT EXISTS scout_expansion_proposals_decided_at_idx ON scout.expansion_proposals ("decided_at");
CREATE INDEX IF NOT EXISTS scout_expansion_proposals_doc_idx ON scout.expansion_proposals USING gin(document);