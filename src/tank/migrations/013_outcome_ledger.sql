-- Append-only computed review outcomes; revised inputs produce another ledger revision.
CREATE TABLE qa.outcomes (
 storage_key text PRIMARY KEY,document jsonb NOT NULL,immutable boolean NOT NULL DEFAULT true,
 outcome_id text NOT NULL UNIQUE,alert_id text NOT NULL,version int NOT NULL,
 dec_id text,computed_at timestamptz NOT NULL,revision int NOT NULL CHECK(revision>0),
 input_hash text NOT NULL CHECK(length(input_hash)=64),
 UNIQUE(alert_id,version,dec_id,revision),UNIQUE(alert_id,version,dec_id,input_hash)
);
CREATE INDEX outcomes_alert_decision ON qa.outcomes(alert_id,version,dec_id);
CREATE INDEX outcomes_computed_at ON qa.outcomes(computed_at);
CREATE INDEX outcomes_input_hash ON qa.outcomes(input_hash);
CREATE INDEX outcomes_document ON qa.outcomes USING gin(document);
CREATE TRIGGER append_only BEFORE UPDATE OR DELETE ON qa.outcomes FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();
