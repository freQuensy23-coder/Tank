-- Deferrable links allow one atomic tool transaction to create claims before observations.
ALTER TABLE evt.events ADD CONSTRAINT event_merge_target FOREIGN KEY(merged_into) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE evt.event_observations ADD CONSTRAINT observation_event FOREIGN KEY(event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE evt.event_observations ADD CONSTRAINT observation_item FOREIGN KEY(item_id) REFERENCES ingest.source_items(item_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE evt.claims ADD CONSTRAINT claim_observation FOREIGN KEY(obs_id) REFERENCES evt.event_observations(obs_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE evt.state_changes ADD CONSTRAINT change_event FOREIGN KEY(event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE evt.ems_assessments ADD CONSTRAINT assessment_event FOREIGN KEY(event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE ana.market_truth ADD CONSTRAINT truth_event FOREIGN KEY(event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE ana.decisions ADD CONSTRAINT decision_event FOREIGN KEY(event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE ana.decisions ADD CONSTRAINT decision_truth FOREIGN KEY(mta_id) REFERENCES ana.market_truth(mta_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE ana.calls ADD CONSTRAINT call_event FOREIGN KEY(event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE out.alerts ADD CONSTRAINT alert_event FOREIGN KEY(event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE out.deliveries ADD CONSTRAINT delivery_alert FOREIGN KEY(alert_id,version) REFERENCES out.alerts(alert_id,version) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE out.deliveries ADD CONSTRAINT delivery_notice FOREIGN KEY(notice_id) REFERENCES out.ops_notices(notice_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE out.deliveries ADD CONSTRAINT delivery_subject CHECK ((alert_id IS NOT NULL AND version IS NOT NULL AND notice_id IS NULL) OR (alert_id IS NULL AND version IS NULL AND notice_id IS NOT NULL));
ALTER TABLE ops.agent_sessions ADD CONSTRAINT session_event FOREIGN KEY(event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE ops.agent_sessions ADD CONSTRAINT session_origin FOREIGN KEY(opened_by_item) REFERENCES ingest.source_items(item_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE ops.tool_calls ADD CONSTRAINT tool_session FOREIGN KEY(session_id) REFERENCES ops.agent_sessions(session_id) DEFERRABLE INITIALLY DEFERRED;
