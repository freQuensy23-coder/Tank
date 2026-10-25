CREATE UNIQUE INDEX ana_decisions_scoped_identity
    ON ana.decisions (event_id, token_id, dec_id);

CREATE FUNCTION ana.decision_state_head_key(p_event_id text, p_token_id text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$ SELECT '[' || to_json(p_event_id)::text || ',' || to_json(p_token_id)::text || ']' $$;

CREATE TABLE ana.decision_state_heads (
    storage_key text PRIMARY KEY,
    document jsonb NOT NULL,
    immutable boolean NOT NULL DEFAULT false,
    event_id text NOT NULL CHECK (length(btrim(event_id)) > 0),
    token_id text NOT NULL CHECK (length(btrim(token_id)) > 0),
    dec_id text NOT NULL CHECK (length(btrim(dec_id)) > 0),
    revision bigint NOT NULL CHECK (revision > 0),
    previous_head_dec_id text,
    recorded_at timestamptz NOT NULL,
    recorded_at_prec text NOT NULL CHECK (recorded_at_prec = 'COLLECTION_ONLY'),
    recorded_at_prec_reason text CHECK (recorded_at_prec_reason IS NULL),
    dec_sha256 text NOT NULL CHECK (dec_sha256 ~ '^[0-9a-f]{64}$'),
    writer jsonb NOT NULL CHECK (jsonb_typeof(writer) = 'object'),
    CONSTRAINT decision_state_heads_scope UNIQUE (event_id, token_id),
    CONSTRAINT decision_state_heads_mutable CHECK (NOT immutable),
    CONSTRAINT decision_state_heads_key CHECK (
        storage_key = ana.decision_state_head_key(event_id, token_id)
    ),
    CONSTRAINT decision_state_heads_previous CHECK (
        (revision = 1 AND previous_head_dec_id IS NULL)
        OR (revision > 1 AND previous_head_dec_id IS NOT NULL)
    ),
    CONSTRAINT decision_state_heads_document CHECK (
        jsonb_typeof(document) = 'object'
        AND document ?& ARRAY['writer', 'recorded_at']
        AND document @> jsonb_build_object(
            'event_id', event_id,
            'token_id', token_id,
            'dec_id', dec_id,
            'revision', revision,
            'previous_head_dec_id', previous_head_dec_id,
            'recorded_at_prec', recorded_at_prec,
            'recorded_at_prec_reason', recorded_at_prec_reason,
            'dec_sha256', dec_sha256
        )
        AND jsonb_typeof(document -> 'revision') = 'number'
        AND (document ->> 'revision') ~ '^[1-9][0-9]*$'
        AND document -> 'writer' = writer
        AND jsonb_typeof(document -> 'recorded_at') = 'string'
        AND (document ->> 'recorded_at') ~ '(Z|[+-][0-9]{2}:[0-9]{2})$'
        AND (document ->> 'recorded_at')::timestamptz = recorded_at
    ),
    CONSTRAINT decision_state_heads_current_scope_fk
        FOREIGN KEY (event_id, token_id, dec_id)
        REFERENCES ana.decisions (event_id, token_id, dec_id)
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT decision_state_heads_previous_scope_fk
        FOREIGN KEY (event_id, token_id, previous_head_dec_id)
        REFERENCES ana.decisions (event_id, token_id, dec_id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE FUNCTION ana.guard_decision_state_head() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'decision state heads cannot be deleted' USING ERRCODE = '23001';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF EXISTS (
            SELECT 1 FROM ana.decision_state_heads WHERE storage_key = NEW.storage_key
        ) THEN
            RETURN NEW;
        END IF;
        IF NEW.revision <> 1 OR NEW.previous_head_dec_id IS NOT NULL THEN
            RAISE EXCEPTION 'decision state head must start at revision 1' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.storage_key IS DISTINCT FROM OLD.storage_key
        OR NEW.event_id IS DISTINCT FROM OLD.event_id
        OR NEW.token_id IS DISTINCT FROM OLD.token_id THEN
        RAISE EXCEPTION 'decision state head scope is immutable' USING ERRCODE = '23514';
    END IF;
    IF NEW.document = OLD.document THEN
        RETURN NEW;
    END IF;
    IF NEW.revision <> OLD.revision + 1
        OR NEW.previous_head_dec_id IS DISTINCT FROM OLD.dec_id THEN
        RAISE EXCEPTION 'decision state head revision conflict' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER decision_state_head_guard
    BEFORE INSERT OR UPDATE OR DELETE ON ana.decision_state_heads
    FOR EACH ROW EXECUTE FUNCTION ana.guard_decision_state_head();
