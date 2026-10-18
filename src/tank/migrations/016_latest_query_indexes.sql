CREATE INDEX market_truth_event_token_latest ON ana.market_truth(event_id,token_id,cutoff DESC,storage_key DESC);
CREATE INDEX decisions_event_token_latest ON ana.decisions(event_id,token_id,cutoff DESC,storage_key DESC);
CREATE INDEX sessions_routing_latest ON ops.agent_sessions(last_item_at DESC,storage_key DESC);
CREATE INDEX routing_reservations_latest ON ops.routing_reservations(last_item_at DESC,storage_key DESC);
CREATE INDEX routing_reservations_created ON ops.routing_reservations(created_at DESC,storage_key DESC);
