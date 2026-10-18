CREATE TABLE mkt.backfill_runs(storage_key text PRIMARY KEY, document jsonb NOT NULL, immutable boolean NOT NULL DEFAULT false,
 backfill_id text NOT NULL UNIQUE,event_id text,token_id text,anchor timestamptz,started_at timestamptz,finished_at timestamptz,status text,reason text,result jsonb);
CREATE INDEX backfill_runs_event_token ON mkt.backfill_runs(event_id,token_id);
CREATE INDEX backfill_runs_status ON mkt.backfill_runs(status);
CREATE INDEX backfill_runs_doc ON mkt.backfill_runs USING gin(document);
CREATE TRIGGER immutable_record BEFORE UPDATE OR DELETE ON mkt.backfill_runs FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();
CREATE TABLE ops.routing_reservations(storage_key text PRIMARY KEY,document jsonb NOT NULL,immutable boolean NOT NULL DEFAULT false,
 reservation_id text NOT NULL UNIQUE,first_item_id text,event_id text,session_id text,status text CHECK(status IN('PENDING','BOUND','CLOSED')),created_at timestamptz,last_item_at timestamptz);
CREATE INDEX routing_reservations_status_time ON ops.routing_reservations(status,last_item_at);
CREATE INDEX routing_reservations_event ON ops.routing_reservations(event_id);
CREATE INDEX routing_reservations_session ON ops.routing_reservations(session_id);
CREATE INDEX routing_reservations_doc ON ops.routing_reservations USING gin(document);
CREATE TRIGGER immutable_record BEFORE UPDATE OR DELETE ON ops.routing_reservations FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();
