ALTER TABLE ops.agent_sessions DROP CONSTRAINT agent_sessions_event_id_key;
CREATE INDEX ops_agent_sessions_event_id_idx ON ops.agent_sessions USING btree (event_id);
