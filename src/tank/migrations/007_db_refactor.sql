DROP TABLE IF EXISTS evt.event_entities;
DROP TABLE IF EXISTS qa.fixtures;
DROP TABLE IF EXISTS qa.replay_runs;
DROP TABLE IF EXISTS mkt.supply;
DROP TABLE IF EXISTS ana.outlooks;
DROP TABLE IF EXISTS reg.dependency_map;
DROP TABLE IF EXISTS reg.metric_semantics;

ALTER TABLE evt.events DROP COLUMN IF EXISTS next_milestone;

ALTER TABLE evt.ems_assessments
    DROP COLUMN IF EXISTS dims,
    DROP COLUMN IF EXISTS gates,
    DROP COLUMN IF EXISTS impact_horizon;
ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_dims CHECK (coalesce(jsonb_typeof(document -> 'dims'), 'null') <> 'null') NOT VALID;
ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_gates CHECK (
        coalesce(jsonb_typeof(document -> 'gates'), jsonb_typeof(document -> 'structural_gates'), 'null') <> 'null'
    ) NOT VALID;
ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_impact_horizon CHECK (
        coalesce(jsonb_typeof(document -> 'impact_horizon'), 'null') <> 'null'
    ) NOT VALID;

ALTER TABLE ana.market_truth
    DROP COLUMN IF EXISTS divergence,
    DROP COLUMN IF EXISTS data_confidence,
    DROP COLUMN IF EXISTS detail;
ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_divergence CHECK (coalesce(jsonb_typeof(document -> 'divergence'), 'null') <> 'null') NOT VALID;
ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_data_confidence CHECK (
        coalesce(jsonb_typeof(document -> 'data_confidence'), 'null') <> 'null'
    ) NOT VALID;

ALTER TABLE ana.decisions DROP COLUMN IF EXISTS outlook;
ALTER TABLE ana.decisions
    ADD CONSTRAINT required_outlook CHECK (coalesce(jsonb_typeof(document -> 'outlook'), 'null') <> 'null') NOT VALID;

ALTER TABLE out.alerts DROP COLUMN IF EXISTS card_struct;
ALTER TABLE out.alerts
    ADD CONSTRAINT required_card_struct CHECK (coalesce(jsonb_typeof(document -> 'card_struct'), 'null') <> 'null') NOT VALID;

ALTER TABLE ops.jobs DROP COLUMN IF EXISTS payload;
ALTER TABLE ops.jobs
    ADD CONSTRAINT required_payload CHECK (coalesce(jsonb_typeof(document -> 'payload'), 'null') <> 'null') NOT VALID;

ALTER TABLE ops.tool_calls DROP COLUMN IF EXISTS input, DROP COLUMN IF EXISTS result;
ALTER TABLE ops.tool_calls
    ADD CONSTRAINT required_input CHECK (coalesce(jsonb_typeof(document -> 'input'), 'null') <> 'null') NOT VALID;

ALTER TABLE ops.scheduler_checkpoints DROP COLUMN IF EXISTS state;

DROP INDEX IF EXISTS ana.ana_calls_call_id_idx;
DROP INDEX IF EXISTS ana.ana_calls_closed_at_idx;
DROP INDEX IF EXISTS ana.ana_calls_doc_idx;
DROP INDEX IF EXISTS ana.ana_calls_status_idx;
DROP INDEX IF EXISTS ana.ana_calls_token_id_idx;
DROP INDEX IF EXISTS ana.ana_contradictions_contradiction_id_idx;
DROP INDEX IF EXISTS ana.ana_contradictions_doc_idx;
DROP INDEX IF EXISTS ana.ana_contradictions_resolved_at_idx;
DROP INDEX IF EXISTS ana.ana_contradictions_token_id_idx;
DROP INDEX IF EXISTS ana.ana_decisions_call_id_idx;
DROP INDEX IF EXISTS ana.ana_decisions_cutoff_idx;
DROP INDEX IF EXISTS ana.ana_decisions_dec_id_idx;
DROP INDEX IF EXISTS ana.ana_decisions_doc_idx;
DROP INDEX IF EXISTS ana.ana_decisions_event_id_idx;
DROP INDEX IF EXISTS ana.ana_decisions_mta_id_idx;
DROP INDEX IF EXISTS ana.ana_decisions_token_id_idx;
DROP INDEX IF EXISTS ana.ana_market_truth_doc_idx;
DROP INDEX IF EXISTS ana.ana_market_truth_event_id_idx;
DROP INDEX IF EXISTS ana.ana_market_truth_mta_id_idx;
DROP INDEX IF EXISTS ana.ana_market_truth_token_id_idx;
DROP INDEX IF EXISTS ana.ana_watch_conditions_cond_id_idx;
DROP INDEX IF EXISTS ana.ana_watch_conditions_doc_idx;
DROP INDEX IF EXISTS ana.ana_watch_conditions_expires_at_idx;
DROP INDEX IF EXISTS ana.ana_watch_conditions_fired_at_idx;
DROP INDEX IF EXISTS ana.ana_watch_conditions_token_id_idx;

DROP INDEX IF EXISTS evt.evt_beneficiary_edges_doc_idx;
DROP INDEX IF EXISTS evt.evt_beneficiary_edges_edge_id_idx;
DROP INDEX IF EXISTS evt.evt_beneficiary_edges_entity_id_idx;
DROP INDEX IF EXISTS evt.evt_beneficiary_edges_token_id_idx;
DROP INDEX IF EXISTS evt.evt_beneficiary_maps_cutoff_idx;
DROP INDEX IF EXISTS evt.evt_beneficiary_maps_doc_idx;
DROP INDEX IF EXISTS evt.evt_beneficiary_maps_event_id_idx;
DROP INDEX IF EXISTS evt.evt_capture_chains_doc_idx;
DROP INDEX IF EXISTS evt.evt_capture_chains_event_id_idx;
DROP INDEX IF EXISTS evt.evt_claims_claim_id_idx;
DROP INDEX IF EXISTS evt.evt_claims_obs_id_idx;
DROP INDEX IF EXISTS evt.evt_claims_doc_idx;
DROP INDEX IF EXISTS evt.evt_ems_assessments_doc_idx;
DROP INDEX IF EXISTS evt.evt_ems_assessments_ems_id_idx;
DROP INDEX IF EXISTS evt.evt_ems_assessments_event_id_idx;
DROP INDEX IF EXISTS evt.evt_ems_assessments_model_id_idx;
DROP INDEX IF EXISTS evt.evt_event_observations_doc_idx;
DROP INDEX IF EXISTS evt.evt_event_observations_obs_id_idx;
DROP INDEX IF EXISTS evt.evt_event_observations_observed_at_idx;
DROP INDEX IF EXISTS evt.evt_event_observations_public_at_idx;
DROP INDEX IF EXISTS evt.evt_events_current_ems_id_idx;
DROP INDEX IF EXISTS evt.evt_events_doc_idx;
DROP INDEX IF EXISTS evt.evt_events_event_id_idx;
DROP INDEX IF EXISTS evt.evt_events_event_time_idx;
DROP INDEX IF EXISTS evt.evt_events_public_at_idx;
DROP INDEX IF EXISTS evt.evt_events_verified_at_idx;
DROP INDEX IF EXISTS evt.evt_state_changes_changed_at_idx;
DROP INDEX IF EXISTS evt.evt_state_changes_doc_idx;
DROP INDEX IF EXISTS evt.evt_state_changes_edge_id_idx;

DROP INDEX IF EXISTS ingest.ingest_source_items_doc_idx;
DROP INDEX IF EXISTS ingest.ingest_source_items_item_id_idx;
DROP INDEX IF EXISTS ingest.ingest_source_items_published_at_idx;
DROP INDEX IF EXISTS ingest.ingest_source_items_route_event_id_idx;
DROP INDEX IF EXISTS ingest.ingest_source_items_source_id_idx;

DROP INDEX IF EXISTS mkt.backfill_runs_anchor_storage_key_idx;
DROP INDEX IF EXISTS mkt.backfill_runs_received_ts_storage_key_idx;
DROP INDEX IF EXISTS mkt.backfill_runs_recorded_at_storage_key_idx;
DROP INDEX IF EXISTS mkt.backfill_runs_started_at_storage_key_idx;
DROP INDEX IF EXISTS mkt.backfill_runs_storage_key_idx;
DROP INDEX IF EXISTS mkt.backfill_runs_token_id_anchor_storage_key_idx;
DROP INDEX IF EXISTS mkt.backfill_runs_token_id_finished_at_storage_key_idx;
DROP INDEX IF EXISTS mkt.backfill_runs_token_id_received_ts_storage_key_idx;
DROP INDEX IF EXISTS mkt.backfill_runs_token_id_recorded_at_storage_key_idx;
DROP INDEX IF EXISTS mkt.capture_gaps_end_storage_key_idx;
DROP INDEX IF EXISTS mkt.capture_gaps_received_ts_storage_key_idx;
DROP INDEX IF EXISTS mkt.capture_gaps_recorded_at_storage_key_idx;
DROP INDEX IF EXISTS mkt.capture_gaps_start_storage_key_idx;
DROP INDEX IF EXISTS mkt.capture_gaps_storage_key_idx;
DROP INDEX IF EXISTS mkt.capture_gaps_token_id_end_storage_key_idx;
DROP INDEX IF EXISTS mkt.capture_gaps_token_id_observed_at_storage_key_idx;
DROP INDEX IF EXISTS mkt.capture_gaps_token_id_received_ts_storage_key_idx;
DROP INDEX IF EXISTS mkt.capture_gaps_token_id_recorded_at_storage_key_idx;
DROP INDEX IF EXISTS mkt.hot_tokens_expires_at_storage_key_idx;
DROP INDEX IF EXISTS mkt.hot_tokens_received_ts_storage_key_idx;
DROP INDEX IF EXISTS mkt.hot_tokens_recorded_at_storage_key_idx;
DROP INDEX IF EXISTS mkt.hot_tokens_since_storage_key_idx;
DROP INDEX IF EXISTS mkt.hot_tokens_storage_key_idx;
DROP INDEX IF EXISTS mkt.hot_tokens_token_id_expires_at_storage_key_idx;
DROP INDEX IF EXISTS mkt.hot_tokens_token_id_received_ts_storage_key_idx;
DROP INDEX IF EXISTS mkt.hot_tokens_token_id_recorded_at_storage_key_idx;
DROP INDEX IF EXISTS mkt.hot_tokens_token_id_since_storage_key_idx;
DROP INDEX IF EXISTS mkt.supply_obs_received_ts_storage_key_idx;
DROP INDEX IF EXISTS mkt.supply_obs_storage_key_idx;
DROP INDEX IF EXISTS mkt.supply_obs_token_id_received_ts_storage_key_idx;
DROP INDEX IF EXISTS mkt.unlocks_date_storage_key_idx;
DROP INDEX IF EXISTS mkt.unlocks_observed_at_storage_key_idx;
DROP INDEX IF EXISTS mkt.unlocks_received_ts_storage_key_idx;
DROP INDEX IF EXISTS mkt.unlocks_recorded_at_storage_key_idx;
DROP INDEX IF EXISTS mkt.unlocks_storage_key_idx;
DROP INDEX IF EXISTS mkt.unlocks_token_id_observed_at_storage_key_idx;
DROP INDEX IF EXISTS mkt.unlocks_token_id_received_ts_storage_key_idx;
DROP INDEX IF EXISTS mkt.unlocks_token_id_recorded_at_storage_key_idx;

DROP INDEX IF EXISTS ops.capture_tier_transitions_document;
DROP INDEX IF EXISTS ops.ops_agent_sessions_doc_idx;
DROP INDEX IF EXISTS ops.ops_agent_sessions_event_id_idx;
DROP INDEX IF EXISTS ops.ops_agent_sessions_last_item_at_idx;
DROP INDEX IF EXISTS ops.ops_agent_sessions_opened_at_idx;
DROP INDEX IF EXISTS ops.ops_agent_sessions_session_id_idx;
DROP INDEX IF EXISTS ops.sessions_item_ref;
DROP INDEX IF EXISTS ops.sessions_proposal_ref;
DROP INDEX IF EXISTS ops.sessions_reservation_ref;
DROP INDEX IF EXISTS ops.ops_jobs_doc_idx;
DROP INDEX IF EXISTS ops.ops_jobs_expires_at_idx;
DROP INDEX IF EXISTS ops.ops_jobs_job_id_idx;
DROP INDEX IF EXISTS ops.ops_jobs_priority_idx;
DROP INDEX IF EXISTS ops.ops_jobs_run_after_idx;
DROP INDEX IF EXISTS ops.ops_llm_usage_doc_idx;
DROP INDEX IF EXISTS ops.ops_llm_usage_subject_id_idx;
DROP INDEX IF EXISTS ops.scheduler_checkpoints_at;
DROP INDEX IF EXISTS ops.ops_source_checkpoints_doc_idx;
DROP INDEX IF EXISTS ops.ops_source_checkpoints_source_id_idx;
DROP INDEX IF EXISTS ops.ops_source_checkpoints_updated_at_idx;
DROP INDEX IF EXISTS ops.ops_source_escalations_doc_idx;
DROP INDEX IF EXISTS ops.ops_telegram_messages_doc_idx;
DROP INDEX IF EXISTS ops.ops_telegram_files_question_id_idx;
DROP INDEX IF EXISTS ops.ops_tool_calls_answered_at_idx;
DROP INDEX IF EXISTS ops.ops_tool_calls_doc_idx;
DROP INDEX IF EXISTS ops.ops_tool_calls_requested_at_idx;
DROP INDEX IF EXISTS ops.ops_tool_calls_session_id_idx;
DROP INDEX IF EXISTS ops.ops_tool_calls_status_idx;

DROP INDEX IF EXISTS out.out_alerts_alert_id_idx;
DROP INDEX IF EXISTS out.out_alerts_doc_idx;
DROP INDEX IF EXISTS out.out_alerts_evidence_bundle_id_idx;
DROP INDEX IF EXISTS out.out_alerts_model_id_idx;
DROP INDEX IF EXISTS out.out_deliveries_doc_idx;
DROP INDEX IF EXISTS out.out_deliveries_status_idx;
DROP INDEX IF EXISTS out.out_evidence_bundles_bundle_id_idx;
DROP INDEX IF EXISTS out.out_evidence_bundles_cutoff_idx;
DROP INDEX IF EXISTS out.out_evidence_bundles_doc_idx;
DROP INDEX IF EXISTS out.out_ops_notices_created_at_idx;
DROP INDEX IF EXISTS out.out_ops_notices_doc_idx;
DROP INDEX IF EXISTS out.out_ops_notices_notice_id_idx;
DROP INDEX IF EXISTS out.out_reaction_checkpoints_captured_at_idx;
DROP INDEX IF EXISTS out.out_reaction_checkpoints_doc_idx;
DROP INDEX IF EXISTS out.out_reaction_checkpoints_rwp_id_idx;
DROP INDEX IF EXISTS out.out_reaction_checkpoints_token_id_idx;
DROP INDEX IF EXISTS out.out_user_questions_doc_idx;

DROP INDEX IF EXISTS qa.qa_reviews_doc_idx;
DROP INDEX IF EXISTS qa.qa_reviews_review_id_idx;

DROP INDEX IF EXISTS reg.reg_entities_doc_idx;
DROP INDEX IF EXISTS reg.reg_entities_entity_id_idx;
DROP INDEX IF EXISTS reg.reg_entity_aliases_doc_idx;
DROP INDEX IF EXISTS reg.reg_entity_aliases_entity_id_idx;
DROP INDEX IF EXISTS reg.reg_exchange_addresses_doc_idx;
DROP INDEX IF EXISTS reg.reg_exchange_addresses_entity_id_idx;
DROP INDEX IF EXISTS reg.reg_instruments_doc_idx;
DROP INDEX IF EXISTS reg.reg_instruments_instrument_id_idx;
DROP INDEX IF EXISTS reg.reg_instruments_token_id_idx;
DROP INDEX IF EXISTS reg.reg_sector_baskets_basket_id_idx;
DROP INDEX IF EXISTS reg.reg_sector_baskets_doc_idx;
DROP INDEX IF EXISTS reg.reg_sources_doc_idx;
DROP INDEX IF EXISTS reg.reg_sources_source_id_idx;
DROP INDEX IF EXISTS reg.reg_tokens_doc_idx;
DROP INDEX IF EXISTS reg.reg_tokens_token_id_idx;

DROP INDEX IF EXISTS scout.scout_expansion_proposals_decided_at_idx;
DROP INDEX IF EXISTS scout.scout_expansion_proposals_doc_idx;
DROP INDEX IF EXISTS scout.scout_expansion_proposals_prp_id_idx;

CREATE INDEX evt_claims_doc_idx ON evt.claims USING gin (document jsonb_path_ops);
CREATE INDEX ops_agent_sessions_doc_idx ON ops.agent_sessions USING gin (document jsonb_path_ops);
CREATE INDEX ops_tool_calls_doc_idx ON ops.tool_calls USING gin (document jsonb_path_ops);
CREATE INDEX out_deliveries_doc_idx ON out.deliveries USING gin (document jsonb_path_ops);
CREATE INDEX out_user_questions_doc_idx ON out.user_questions USING gin (document jsonb_path_ops);
