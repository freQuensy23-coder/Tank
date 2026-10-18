CREATE TABLE ops.scheduler_checkpoints(storage_key text PRIMARY KEY,document jsonb NOT NULL,immutable boolean NOT NULL DEFAULT false,
 checkpoint_id text NOT NULL UNIQUE,at timestamptz NOT NULL,state jsonb);
CREATE INDEX scheduler_checkpoints_at ON ops.scheduler_checkpoints(at);
CREATE TRIGGER immutable_record BEFORE UPDATE OR DELETE ON ops.scheduler_checkpoints FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();
