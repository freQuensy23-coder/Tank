-- Durable capture-policy provenance, committed atomically with HOT membership changes.
CREATE TABLE ops.capture_tier_transitions (
 storage_key text PRIMARY KEY,
 document jsonb NOT NULL,
 immutable boolean NOT NULL DEFAULT true,
 transition_id text NOT NULL UNIQUE,
 token_id text NOT NULL,
 tier text NOT NULL CHECK(tier IN ('HOT','BASE')),
 effective_at timestamptz NOT NULL,
 effective_at_prec text NOT NULL CHECK(effective_at_prec='COLLECTION_ONLY'),
 reason text NOT NULL CHECK(length(btrim(reason))>0),
 config_version text NOT NULL,
 code_version text NOT NULL
);
CREATE INDEX capture_tier_transitions_token_time
 ON ops.capture_tier_transitions(token_id,effective_at,storage_key);
CREATE INDEX capture_tier_transitions_time
 ON ops.capture_tier_transitions(effective_at,storage_key);
CREATE INDEX capture_tier_transitions_document
 ON ops.capture_tier_transitions USING gin(document);
CREATE TRIGGER append_only BEFORE UPDATE OR DELETE ON ops.capture_tier_transitions
 FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();
