SET LOCAL check_function_bodies = false;

CREATE SCHEMA IF NOT EXISTS ana;

CREATE SCHEMA IF NOT EXISTS evt;

CREATE SCHEMA IF NOT EXISTS ingest;

CREATE SCHEMA IF NOT EXISTS mkt;

CREATE SCHEMA IF NOT EXISTS ops;

CREATE SCHEMA "out";

CREATE SCHEMA IF NOT EXISTS qa;

CREATE SCHEMA IF NOT EXISTS reg;

CREATE SCHEMA IF NOT EXISTS scout;

CREATE FUNCTION ops.forbid_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$ BEGIN RAISE EXCEPTION 'append-only table: %', TG_TABLE_NAME; END $$;

CREATE FUNCTION ops.notify_capture() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM pg_notify('tank_capture', TG_TABLE_NAME);
    RETURN NULL;
END $$;

CREATE FUNCTION ops.protect_immutable() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
 IF OLD.immutable THEN
  RAISE EXCEPTION 'immutable record: %', OLD.storage_key;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION ops.request_budget_acquire(p_bucket text, p_capacity numeric, p_window_s double precision, p_weight numeric, p_owner text, p_host text DEFAULT NULL::text) RETURNS double precision
    LANGUAGE plpgsql
    AS $$
DECLARE
    now_ts timestamptz := clock_timestamp();
    blocked timestamptz;
    used numeric;
    oldest timestamptz;
BEGIN
    IF p_capacity <= 0 OR p_weight <= 0 OR p_weight > p_capacity THEN
        RAISE EXCEPTION 'invalid request budget % % %', p_bucket, p_capacity, p_weight;
    END IF;
    IF p_host IS NOT NULL THEN
        SELECT until INTO blocked FROM ops.request_budget_blocks WHERE host = p_host;
        IF blocked IS NOT NULL AND blocked > now_ts THEN
            RETURN greatest(0.001, extract(epoch FROM blocked - now_ts));
        END IF;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('tank:request_budget:' || p_bucket, 0));
    now_ts := clock_timestamp();
    DELETE FROM ops.request_budget_ledger
        WHERE bucket = p_bucket AND at <= now_ts - make_interval(secs => p_window_s);
    SELECT coalesce(sum(weight), 0), min(at) INTO used, oldest
        FROM ops.request_budget_ledger WHERE bucket = p_bucket;
    IF used + p_weight <= p_capacity THEN
        INSERT INTO ops.request_budget_ledger (bucket, at, weight, owner) VALUES (p_bucket, now_ts, p_weight, p_owner);
        RETURN 0;
    END IF;
    RETURN greatest(0.001, extract(epoch FROM oldest + make_interval(secs => p_window_s) - now_ts));
END $$;

CREATE FUNCTION ops.request_budget_block(p_host text, p_until timestamp with time zone, p_reason text, p_owner text) RETURNS void
    LANGUAGE sql
    AS $$
    INSERT INTO ops.request_budget_blocks (host, until, reason, owner, updated_at)
    VALUES (p_host, p_until, p_reason, p_owner, now())
    ON CONFLICT (host) DO UPDATE SET
        until = greatest(ops.request_budget_blocks.until, EXCLUDED.until),
        reason = EXCLUDED.reason, owner = EXCLUDED.owner, updated_at = now();
$$;

CREATE FUNCTION ops.stamp_job_lifecycle() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE generated_created boolean; generated_finished boolean;
BEGIN
 generated_created := NEW.created_at IS NULL AND NEW.document->>'created_at' IS NULL;
 NEW.created_at := coalesce(NEW.created_at,(NEW.document->>'created_at')::timestamptz,now());
 NEW.created_at_prec := CASE WHEN generated_created THEN 'COLLECTION_ONLY'
   ELSE coalesce(NEW.created_at_prec,NEW.document->>'created_at_prec',NEW.document->>'created_at_precision') END;
 NEW.created_at_prec_reason := CASE WHEN NEW.created_at_prec IS NULL
   THEN coalesce(NEW.created_at_prec_reason,NEW.document->>'created_at_prec_reason','INCOMPLETE') ELSE NULL END;
 IF NEW.status IN ('DONE','FAILED','EXPIRED') THEN
  generated_finished := NEW.finished_at IS NULL AND NEW.document->>'finished_at' IS NULL;
  NEW.finished_at := coalesce(NEW.finished_at,(NEW.document->>'finished_at')::timestamptz,now());
  NEW.finished_at_prec := CASE WHEN generated_finished THEN 'COLLECTION_ONLY'
    ELSE coalesce(NEW.finished_at_prec,NEW.document->>'finished_at_prec',NEW.document->>'finished_at_precision') END;
  NEW.finished_at_prec_reason := CASE WHEN NEW.finished_at_prec IS NULL
    THEN coalesce(NEW.finished_at_prec_reason,NEW.document->>'finished_at_prec_reason','INCOMPLETE') ELSE NULL END;
 ELSE
  NEW.finished_at := NULL; NEW.finished_at_prec := NULL; NEW.finished_at_prec_reason := 'PENDING';
 END IF;
 NEW.document := NEW.document || jsonb_build_object(
  'created_at',to_char(NEW.created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
  'created_at_prec',NEW.created_at_prec,'created_at_prec_reason',NEW.created_at_prec_reason,
  'finished_at',CASE WHEN NEW.finished_at IS NULL THEN NULL ELSE to_char(NEW.finished_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') END,
  'finished_at_prec',NEW.finished_at_prec,'finished_at_prec_reason',NEW.finished_at_prec_reason);
 RETURN NEW;
END $$;

CREATE TABLE ana.calls (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    call_id text NOT NULL,
    event_id text,
    token_id text,
    opened_by_alert text,
    opened_by_version integer,
    opened_at timestamp with time zone,
    entry_ref_price numeric,
    invalidation_price numeric,
    status text,
    closed_by text,
    closed_at timestamp with time zone,
    opened_at_prec text,
    opened_at_prec_reason text,
    closed_at_prec text,
    closed_at_prec_reason text,
    CONSTRAINT valid_closed_by CHECK ((closed_by = ANY (ARRAY['EXIT'::text, 'EXPIRED'::text]))),
    CONSTRAINT valid_status CHECK ((status = ANY (ARRAY['OPEN'::text, 'CLOSED'::text])))
);

CREATE TABLE ana.contradictions (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    contradiction_id text,
    event_id text,
    token_id text,
    detected_at timestamp with time zone,
    resolved_at timestamp with time zone,
    detected_at_prec text,
    detected_at_prec_reason text,
    resolved_at_prec text,
    resolved_at_prec_reason text
);

CREATE TABLE ana.decisions (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    dec_id text NOT NULL,
    event_id text,
    token_id text,
    mta_id text,
    cutoff timestamp with time zone,
    thesis_grade character(1),
    trade_grade character(1),
    action text,
    action_rule text,
    tactical boolean,
    investigate boolean,
    call_id text,
    outlook jsonb,
    conditions text[],
    thesis_rule text,
    trade_reasons text[],
    cutoff_prec text,
    cutoff_prec_reason text
);

CREATE TABLE ana.market_truth (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    mta_id text NOT NULL,
    token_id text,
    event_id text,
    cutoff timestamp with time zone,
    primary_window text,
    move_class text,
    support integer,
    alternatives text[],
    divergence jsonb,
    flags text[],
    decisive_facts text[],
    data_confidence jsonb,
    class_confidence text,
    detail jsonb,
    config_version text,
    code_version text,
    cutoff_prec text,
    cutoff_prec_reason text
);

CREATE TABLE ana.outlooks (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    out_id text,
    event_id text,
    token_id text,
    cutoff timestamp with time zone,
    cutoff_prec text,
    cutoff_prec_reason text
);

CREATE TABLE ana.watch_conditions (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    cond_id text NOT NULL,
    event_id text,
    token_id text,
    status text,
    expires_at timestamp with time zone,
    fired_at timestamp with time zone,
    expires_at_prec text,
    expires_at_prec_reason text,
    fired_at_prec text,
    fired_at_prec_reason text
);

CREATE TABLE evt.beneficiary_edges (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    edge_id text,
    event_id text,
    map_version integer,
    entity_id text,
    token_id text,
    role text,
    credible boolean
);

CREATE TABLE evt.beneficiary_maps (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    event_id text,
    map_version integer,
    cutoff timestamp with time zone,
    cutoff_prec text,
    cutoff_prec_reason text
);

CREATE TABLE evt.capture_chains (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    event_id text,
    token_id text,
    version integer,
    capture_class text
);

CREATE TABLE evt.claims (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    claim_id text NOT NULL,
    obs_id text,
    claim_type text,
    subject text,
    value_num numeric,
    state_claim text,
    directionality text,
    evidence_class text,
    verified boolean,
    quote text,
    accepted boolean,
    rejection_reason text,
    predicate text,
    object text,
    value_unit text
);

CREATE TABLE evt.ems_assessments (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    ems_id text NOT NULL,
    event_id text,
    version integer,
    cutoff timestamp with time zone,
    dims jsonb,
    gates jsonb,
    class text,
    criterion text,
    class_reasons text[],
    impact_horizon jsonb,
    config_version text,
    model_id text,
    prompt_version text,
    cutoff_prec text,
    cutoff_prec_reason text
);

CREATE TABLE evt.event_entities (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    event_id text,
    entity_id text,
    role text
);

CREATE TABLE evt.event_observations (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    obs_id text NOT NULL,
    event_id text,
    item_id text,
    link_type text,
    public_at timestamp with time zone,
    freshness text,
    match_method text,
    observed_at timestamp with time zone,
    public_at_prec text,
    public_at_prec_reason text,
    observed_at_prec text,
    observed_at_prec_reason text
);

CREATE TABLE evt.events (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    event_id text NOT NULL,
    statement text,
    theme_tags text[],
    event_time timestamp with time zone,
    event_time_prec text,
    public_at timestamp with time zone,
    public_at_prec text,
    public_at_basis text,
    known_to_us_at timestamp with time zone,
    verified_at timestamp with time zone,
    freshness text,
    event_state text,
    state_verified boolean,
    negative_marker text,
    certainty text,
    timing_confidence text,
    best_source_class text,
    next_milestone jsonb,
    lifecycle text,
    merged_into text,
    current_ems_id text,
    event_time_prec_reason text,
    public_at_prec_reason text,
    known_to_us_at_prec text,
    known_to_us_at_prec_reason text,
    verified_at_prec text,
    verified_at_prec_reason text,
    CONSTRAINT valid_freshness CHECK ((freshness = ANY (ARRAY['FRESH'::text, 'NORMAL'::text, 'MEDIUM'::text, 'POOR'::text, 'OLD'::text, 'STALE'::text])))
);

CREATE TABLE evt.state_changes (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    id bigint,
    event_id text,
    edge_id text,
    ladder text,
    from_state text,
    to_state text,
    evidence text[],
    changed_at timestamp with time zone,
    changed_at_prec text,
    changed_at_prec_reason text,
    CONSTRAINT valid_ladder CHECK ((ladder = ANY (ARRAY['EVENT'::text, 'INTEGRATION'::text])))
);

CREATE SEQUENCE evt.state_changes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE evt.state_changes_id_seq OWNED BY evt.state_changes.id;

CREATE TABLE ingest.source_items (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    item_id text NOT NULL,
    source_id text,
    feed text,
    external_id text,
    url text,
    title text,
    body text,
    published_at timestamp with time zone,
    published_prec text,
    observed_at timestamp with time zone,
    raw_path text,
    raw_sha256 character(64),
    content_hash character(64),
    source_class text,
    relevance_decision text,
    relevance_p real,
    route_decision text,
    route_event_id text,
    route_p real,
    extraction_status text,
    published_prec_reason text,
    observed_at_prec text,
    observed_at_prec_reason text,
    published_at_bucket_size_s numeric,
    CONSTRAINT valid_published_prec CHECK ((published_prec = ANY (ARRAY['EXACT'::text, 'BUCKET'::text, 'COLLECTION_ONLY'::text]))),
    CONSTRAINT valid_relevance_decision CHECK ((relevance_decision = ANY (ARRAY['ALWAYS'::text, 'SEND'::text, 'SKIP'::text]))),
    CONSTRAINT valid_route_decision CHECK ((route_decision = ANY (ARRAY['NEW_SESSION'::text, 'ADDED'::text])))
);

CREATE TABLE mkt.backfill_runs (
    storage_key text NOT NULL,
    anchor timestamp with time zone,
    anchor_prec text,
    anchor_prec_reason text,
    backfill_id text NOT NULL,
    code_version text,
    config_version text,
    event_id text,
    finished_at timestamp with time zone,
    finished_at_prec text,
    finished_at_prec_reason text,
    instrument_id text,
    reason text,
    received_ts timestamp with time zone,
    received_ts_prec text,
    received_ts_prec_reason text,
    recorded_at timestamp with time zone,
    recorded_at_prec text,
    recorded_at_prec_reason text,
    started_at timestamp with time zone,
    started_at_prec text,
    started_at_prec_reason text,
    status text,
    token_id text,
    result jsonb,
    semantic_fingerprint text
);

CREATE TABLE mkt.capture_gaps (
    storage_key text NOT NULL,
    code_version text,
    config_version text,
    "end" timestamp with time zone,
    end_prec text,
    end_prec_reason text,
    gap_id text NOT NULL,
    instrument_id text,
    kind text,
    observed_at timestamp with time zone,
    observed_at_prec text,
    observed_at_prec_reason text,
    reason text,
    received_ts timestamp with time zone,
    received_ts_prec text,
    received_ts_prec_reason text,
    recorded_at timestamp with time zone,
    recorded_at_prec text,
    recorded_at_prec_reason text,
    start timestamp with time zone,
    start_prec text,
    start_prec_reason text,
    status text,
    token_id text,
    semantic_fingerprint text
);

CREATE TABLE mkt.capture_requests (
    storage_key text NOT NULL,
    budget_s numeric,
    code_version text,
    config_version text,
    deadline timestamp with time zone,
    deadline_prec text,
    deadline_prec_reason text,
    "end" timestamp with time zone,
    end_prec text,
    end_prec_reason text,
    event_id text,
    finished_at timestamp with time zone,
    finished_at_prec text,
    finished_at_prec_reason text,
    kind text,
    reason text,
    recorded_at timestamp with time zone,
    recorded_at_prec text,
    recorded_at_prec_reason text,
    request_id text NOT NULL,
    requested_at timestamp with time zone,
    requested_at_prec text,
    requested_at_prec_reason text,
    result jsonb,
    semantic_fingerprint text,
    start timestamp with time zone,
    start_prec text,
    start_prec_reason text,
    started_at timestamp with time zone,
    started_at_prec text,
    started_at_prec_reason text,
    status text,
    token_id text,
    writer text,
    request jsonb
);

CREATE TABLE mkt.capture_streams (
    storage_key text NOT NULL,
    code_version text,
    config_version text,
    connected_since timestamp with time zone,
    connected_since_prec text,
    connected_since_prec_reason text,
    detail jsonb,
    endpoint text,
    instrument_id text NOT NULL,
    last_message_at timestamp with time zone,
    last_message_at_prec text,
    last_message_at_prec_reason text,
    last_trade_id text,
    last_trade_ts timestamp with time zone,
    last_trade_ts_prec text,
    last_trade_ts_prec_reason text,
    market_type text,
    recent_polls jsonb,
    recorded_at timestamp with time zone,
    recorded_at_prec text,
    recorded_at_prec_reason text,
    sealed_through timestamp with time zone,
    sealed_through_prec text,
    sealed_through_prec_reason text,
    semantic_fingerprint text,
    state text,
    stream text NOT NULL,
    token_id text,
    updated_at timestamp with time zone,
    updated_at_prec text,
    updated_at_prec_reason text,
    venue text,
    watermark_ts timestamp with time zone,
    watermark_ts_prec text,
    watermark_ts_prec_reason text,
    writer text
)
WITH (fillfactor='50');

CREATE TABLE mkt.hot_tokens (
    storage_key text NOT NULL,
    code_version text,
    config_version text,
    event_id text NOT NULL,
    expires_at timestamp with time zone,
    expires_at_prec text,
    expires_at_prec_reason text,
    instrument_id text,
    priority bigint,
    received_ts timestamp with time zone,
    received_ts_prec text,
    received_ts_prec_reason text,
    recorded_at timestamp with time zone,
    recorded_at_prec text,
    recorded_at_prec_reason text,
    since timestamp with time zone,
    since_prec text,
    since_prec_reason text,
    token_id text NOT NULL,
    semantic_fingerprint text
);

CREATE TABLE mkt.supply (
    storage_key text NOT NULL,
    available boolean,
    chain text,
    classification_required boolean,
    code_version text,
    config_version text,
    fee_scope text,
    instrument_id text,
    kind text,
    reason text,
    received_ts timestamp with time zone,
    received_ts_prec text,
    received_ts_prec_reason text,
    recorded_at timestamp with time zone,
    recorded_at_prec text,
    recorded_at_prec_reason text,
    token_id text,
    transfer_queries_succeeded bigint,
    ts timestamp with time zone,
    ts_prec text,
    ts_prec_reason text,
    unlocks jsonb,
    flows jsonb,
    coverage_errors jsonb,
    balances jsonb,
    pools jsonb,
    series jsonb,
    metrics jsonb,
    protocols jsonb,
    identity_evidence jsonb,
    inference_limits text[],
    unsupported_venues text[],
    semantic_fingerprint text
);

CREATE TABLE mkt.supply_obs (
    storage_key text NOT NULL,
    available boolean,
    chain text,
    classification_required boolean,
    code_version text,
    config_version text,
    fee_scope text,
    instrument_id text,
    kind text,
    reason text,
    received_ts timestamp with time zone,
    received_ts_prec text,
    received_ts_prec_reason text,
    recorded_at timestamp with time zone,
    recorded_at_prec text,
    recorded_at_prec_reason text,
    token_id text,
    transfer_queries_succeeded bigint,
    ts timestamp with time zone,
    ts_prec text,
    ts_prec_reason text,
    unlocks jsonb,
    flows jsonb,
    coverage_errors jsonb,
    balances jsonb,
    pools jsonb,
    series jsonb,
    metrics jsonb,
    protocols jsonb,
    identity_evidence jsonb,
    inference_limits text[],
    unsupported_venues text[],
    semantic_fingerprint text
);

CREATE TABLE mkt.unlocks (
    storage_key text NOT NULL,
    code_version text,
    config_version text,
    date timestamp with time zone,
    date_prec text,
    date_prec_reason text,
    instrument_id text,
    kind text,
    label text,
    observed_at timestamp with time zone,
    observed_at_prec text,
    observed_at_prec_reason text,
    qty numeric,
    received_ts timestamp with time zone,
    received_ts_prec text,
    received_ts_prec_reason text,
    recorded_at timestamp with time zone,
    recorded_at_prec text,
    recorded_at_prec_reason text,
    sale_inferred boolean,
    token_id text,
    semantic_fingerprint text,
    evidence_mode text,
    schedule_as_of timestamp with time zone,
    schedule_as_of_prec text,
    schedule_as_of_prec_reason text
);

CREATE TABLE ops.agent_sessions (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    session_id text NOT NULL,
    event_id text,
    opened_by_item text,
    agent_version text,
    status text,
    opened_at timestamp with time zone,
    last_item_at timestamp with time zone,
    last_event_seq bigint,
    opened_at_prec text,
    opened_at_prec_reason text,
    last_item_at_prec text,
    last_item_at_prec_reason text,
    CONSTRAINT valid_status CHECK ((status = ANY (ARRAY['OPEN'::text, 'ENDED'::text, 'FAILED'::text])))
);

CREATE TABLE ops.capture_tier_transitions (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT true NOT NULL,
    transition_id text NOT NULL,
    token_id text NOT NULL,
    tier text NOT NULL,
    effective_at timestamp with time zone NOT NULL,
    effective_at_prec text NOT NULL,
    reason text NOT NULL,
    config_version text NOT NULL,
    code_version text NOT NULL,
    effective_at_prec_reason text,
    CONSTRAINT capture_tier_transitions_effective_at_prec_check CHECK ((effective_at_prec = 'COLLECTION_ONLY'::text)),
    CONSTRAINT capture_tier_transitions_reason_check CHECK ((length(btrim(reason)) > 0)),
    CONSTRAINT capture_tier_transitions_tier_check CHECK ((tier = ANY (ARRAY['HOT'::text, 'BASE'::text])))
);

CREATE TABLE ops.jobs (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    job_id text NOT NULL,
    type text NOT NULL,
    priority smallint NOT NULL,
    idem_key text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL,
    attempts integer NOT NULL,
    max_attempts integer NOT NULL,
    run_after timestamp with time zone NOT NULL,
    expires_at timestamp with time zone,
    last_error text,
    created_at timestamp with time zone,
    finished_at timestamp with time zone,
    run_after_prec text,
    run_after_prec_reason text,
    expires_at_prec text,
    expires_at_prec_reason text,
    created_at_prec text,
    created_at_prec_reason text,
    finished_at_prec text,
    finished_at_prec_reason text,
    CONSTRAINT jobs_check CHECK (((attempts >= 0) AND (max_attempts > 0))),
    CONSTRAINT jobs_priority_check CHECK (((priority >= 0) AND (priority <= 4))),
    CONSTRAINT jobs_status_check CHECK ((status = ANY (ARRAY['QUEUED'::text, 'RUNNING'::text, 'DONE'::text, 'FAILED'::text, 'EXPIRED'::text])))
);

CREATE TABLE ops.llm_usage (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    id bigint,
    at timestamp with time zone,
    call text,
    subject_id text,
    model text,
    effort text,
    input_tokens integer,
    cache_read_tokens integer,
    cache_write_tokens integer,
    output_tokens integer,
    cost_usd numeric(10,6),
    web_searches integer,
    stop_reason text,
    at_prec text,
    at_prec_reason text
);

CREATE SEQUENCE ops.llm_usage_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE ops.llm_usage_id_seq OWNED BY ops.llm_usage.id;

CREATE TABLE ops.request_budget_blocks (
    host text NOT NULL,
    until timestamp with time zone NOT NULL,
    reason text,
    owner text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE UNLOGGED TABLE ops.request_budget_ledger (
    bucket text NOT NULL,
    at timestamp with time zone NOT NULL,
    weight numeric NOT NULL,
    owner text NOT NULL
);

CREATE TABLE ops.scheduler_checkpoints (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    checkpoint_id text NOT NULL,
    at timestamp with time zone NOT NULL,
    state jsonb,
    at_prec text,
    at_prec_reason text
);

CREATE TABLE ops.source_checkpoints (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    source_id text,
    cursor text,
    updated_at timestamp with time zone,
    updated_at_prec text,
    updated_at_prec_reason text
);

CREATE TABLE ops.source_escalations (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    source_id text,
    expires_at timestamp with time zone,
    expires_at_prec text,
    expires_at_prec_reason text
);

CREATE TABLE ops.tool_calls (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    call_id text NOT NULL,
    session_id text,
    tool text,
    input jsonb,
    result jsonb,
    status text,
    requested_at timestamp with time zone,
    answered_at timestamp with time zone,
    requested_at_prec text,
    requested_at_prec_reason text,
    answered_at_prec text,
    answered_at_prec_reason text,
    CONSTRAINT valid_status CHECK ((status = ANY (ARRAY['PENDING'::text, 'DONE'::text, 'REFUSED'::text, 'ERROR'::text])))
);

CREATE TABLE "out".alerts (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    alert_id text NOT NULL,
    version integer NOT NULL,
    kind text,
    event_id text,
    generated_at timestamp with time zone,
    card_text text,
    card_struct jsonb,
    decisions text[],
    evidence_bundle_id character(64),
    supersedes_version integer,
    config_version text,
    code_version text,
    model_id text,
    prompt_version text,
    generated_at_prec text,
    generated_at_prec_reason text,
    CONSTRAINT valid_kind CHECK ((kind = ANY (ARRAY['PARTIAL'::text, 'FULL'::text, 'UPDATE'::text])))
);

CREATE TABLE "out".deliveries (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    id bigint,
    alert_id text,
    version integer,
    notice_id text,
    channel text,
    status text,
    message_ref text,
    attempt integer,
    at timestamp with time zone,
    error text,
    at_prec text,
    at_prec_reason text,
    CONSTRAINT delivery_subject CHECK ((((alert_id IS NOT NULL) AND (version IS NOT NULL) AND (notice_id IS NULL)) OR ((alert_id IS NULL) AND (version IS NULL) AND (notice_id IS NOT NULL)))),
    CONSTRAINT valid_status CHECK ((status = ANY (ARRAY['GENERATED'::text, 'SENT'::text, 'FAILED'::text])))
);

CREATE SEQUENCE "out".deliveries_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE "out".deliveries_id_seq OWNED BY "out".deliveries.id;

CREATE TABLE "out".evidence_bundles (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    bundle_id text NOT NULL,
    cutoff timestamp with time zone,
    cutoff_prec text,
    cutoff_prec_reason text
);

CREATE TABLE "out".ops_notices (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    notice_id text NOT NULL,
    kind text,
    subject_id text,
    text text,
    created_at timestamp with time zone,
    created_by text,
    created_at_prec text,
    created_at_prec_reason text
);

CREATE TABLE "out".reaction_checkpoints (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    rwp_id text,
    event_id text,
    token_id text,
    captured_at timestamp with time zone,
    revision integer,
    captured_at_prec text,
    captured_at_prec_reason text
);

CREATE TABLE qa.fixtures (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    fixture_id text,
    cutoff timestamp with time zone,
    cutoff_prec text,
    cutoff_prec_reason text
);

CREATE TABLE qa.replay_runs (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    run_id text,
    fixture_id text,
    at timestamp with time zone,
    at_prec text,
    at_prec_reason text
);

CREATE TABLE qa.reviews (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    review_id text,
    subject text,
    label text,
    at timestamp with time zone,
    at_prec text,
    at_prec_reason text
);

CREATE TABLE reg.dependency_map (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    edge_id text,
    entity_id text,
    token_id text
);

CREATE TABLE reg.entities (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    entity_id text NOT NULL,
    name text
);

CREATE TABLE reg.entity_aliases (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    entity_id text,
    alias text
);

CREATE TABLE reg.exchange_addresses (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    address text,
    chain text,
    entity_id text
);

CREATE TABLE reg.instruments (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    instrument_id text NOT NULL,
    token_id text,
    venue text,
    market_type text,
    venue_symbol text
);

CREATE TABLE reg.metric_semantics (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    metric_key text,
    venue text,
    market_type text,
    semantic_confidence text
);

CREATE TABLE reg.sector_baskets (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    basket_id text
);

CREATE TABLE reg.sources (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    source_id text NOT NULL,
    name text,
    mode text
);

CREATE TABLE reg.tokens (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    token_id text NOT NULL,
    symbol text
);

CREATE TABLE scout.expansion_proposals (
    storage_key text NOT NULL,
    document jsonb NOT NULL,
    immutable boolean DEFAULT false NOT NULL,
    prp_id text,
    hook text,
    concept_name text,
    human_action text,
    decided_at timestamp with time zone,
    decided_at_prec text,
    decided_at_prec_reason text
);

ALTER TABLE ONLY evt.state_changes ALTER COLUMN id SET DEFAULT nextval('evt.state_changes_id_seq'::regclass);

ALTER TABLE ONLY ops.llm_usage ALTER COLUMN id SET DEFAULT nextval('ops.llm_usage_id_seq'::regclass);

ALTER TABLE ONLY "out".deliveries ALTER COLUMN id SET DEFAULT nextval('"out".deliveries_id_seq'::regclass);

ALTER TABLE ONLY ana.calls
    ADD CONSTRAINT calls_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY ana.contradictions
    ADD CONSTRAINT contradictions_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY ana.decisions
    ADD CONSTRAINT decisions_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY ana.market_truth
    ADD CONSTRAINT market_truth_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY ana.outlooks
    ADD CONSTRAINT outlooks_pkey PRIMARY KEY (storage_key);

ALTER TABLE ana.decisions
    ADD CONSTRAINT required_action CHECK ((action IS NOT NULL)) NOT VALID;

ALTER TABLE ana.decisions
    ADD CONSTRAINT required_action_rule CHECK ((action_rule IS NOT NULL)) NOT VALID;

ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_alternatives CHECK ((alternatives IS NOT NULL)) NOT VALID;

ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_class_confidence CHECK ((class_confidence IS NOT NULL)) NOT VALID;

ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_code_version CHECK ((code_version IS NOT NULL)) NOT VALID;

ALTER TABLE ana.decisions
    ADD CONSTRAINT required_conditions CHECK ((conditions IS NOT NULL)) NOT VALID;

ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_config_version CHECK ((config_version IS NOT NULL)) NOT VALID;

ALTER TABLE ana.decisions
    ADD CONSTRAINT required_cutoff CHECK ((cutoff IS NOT NULL)) NOT VALID;

ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_cutoff CHECK ((cutoff IS NOT NULL)) NOT VALID;

ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_data_confidence CHECK ((data_confidence IS NOT NULL)) NOT VALID;

ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_decisive_facts CHECK ((decisive_facts IS NOT NULL)) NOT VALID;

ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_detail CHECK ((detail IS NOT NULL)) NOT VALID;

ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_divergence CHECK ((divergence IS NOT NULL)) NOT VALID;

ALTER TABLE ana.calls
    ADD CONSTRAINT required_entry_ref_price CHECK ((entry_ref_price IS NOT NULL)) NOT VALID;

ALTER TABLE ana.calls
    ADD CONSTRAINT required_event_id CHECK ((event_id IS NOT NULL)) NOT VALID;

ALTER TABLE ana.decisions
    ADD CONSTRAINT required_event_id CHECK ((event_id IS NOT NULL)) NOT VALID;

ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_flags CHECK ((flags IS NOT NULL)) NOT VALID;

ALTER TABLE ana.calls
    ADD CONSTRAINT required_invalidation_price CHECK ((invalidation_price IS NOT NULL)) NOT VALID;

ALTER TABLE ana.decisions
    ADD CONSTRAINT required_investigate CHECK ((investigate IS NOT NULL)) NOT VALID;

ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_move_class CHECK ((move_class IS NOT NULL)) NOT VALID;

ALTER TABLE ana.decisions
    ADD CONSTRAINT required_mta_id CHECK ((mta_id IS NOT NULL)) NOT VALID;

ALTER TABLE ana.calls
    ADD CONSTRAINT required_opened_at CHECK ((opened_at IS NOT NULL)) NOT VALID;

ALTER TABLE ana.calls
    ADD CONSTRAINT required_opened_by_alert CHECK ((opened_by_alert IS NOT NULL)) NOT VALID;

ALTER TABLE ana.calls
    ADD CONSTRAINT required_opened_by_version CHECK ((opened_by_version IS NOT NULL)) NOT VALID;

ALTER TABLE ana.decisions
    ADD CONSTRAINT required_outlook CHECK ((outlook IS NOT NULL)) NOT VALID;

ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_primary_window CHECK ((primary_window IS NOT NULL)) NOT VALID;

ALTER TABLE ana.calls
    ADD CONSTRAINT required_status CHECK ((status IS NOT NULL)) NOT VALID;

ALTER TABLE ana.decisions
    ADD CONSTRAINT required_tactical CHECK ((tactical IS NOT NULL)) NOT VALID;

ALTER TABLE ana.decisions
    ADD CONSTRAINT required_thesis_grade CHECK ((thesis_grade IS NOT NULL)) NOT VALID;

ALTER TABLE ana.decisions
    ADD CONSTRAINT required_thesis_rule CHECK ((thesis_rule IS NOT NULL)) NOT VALID;

ALTER TABLE ana.calls
    ADD CONSTRAINT required_token_id CHECK ((token_id IS NOT NULL)) NOT VALID;

ALTER TABLE ana.decisions
    ADD CONSTRAINT required_token_id CHECK ((token_id IS NOT NULL)) NOT VALID;

ALTER TABLE ana.market_truth
    ADD CONSTRAINT required_token_id CHECK ((token_id IS NOT NULL)) NOT VALID;

ALTER TABLE ana.decisions
    ADD CONSTRAINT required_trade_grade CHECK ((trade_grade IS NOT NULL)) NOT VALID;

ALTER TABLE ana.decisions
    ADD CONSTRAINT required_trade_reasons CHECK ((trade_reasons IS NOT NULL)) NOT VALID;

ALTER TABLE ONLY ana.watch_conditions
    ADD CONSTRAINT watch_conditions_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY evt.beneficiary_edges
    ADD CONSTRAINT beneficiary_edges_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY evt.beneficiary_maps
    ADD CONSTRAINT beneficiary_maps_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY evt.capture_chains
    ADD CONSTRAINT capture_chains_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY evt.claims
    ADD CONSTRAINT claims_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY evt.ems_assessments
    ADD CONSTRAINT ems_assessments_event_id_version_key UNIQUE (event_id, version);

ALTER TABLE ONLY evt.ems_assessments
    ADD CONSTRAINT ems_assessments_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY evt.event_entities
    ADD CONSTRAINT event_entities_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY evt.event_observations
    ADD CONSTRAINT event_observations_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY evt.events
    ADD CONSTRAINT events_pkey PRIMARY KEY (storage_key);

ALTER TABLE evt.claims
    ADD CONSTRAINT required_accepted CHECK ((accepted IS NOT NULL)) NOT VALID;

ALTER TABLE evt.events
    ADD CONSTRAINT required_best_source_class CHECK ((best_source_class IS NOT NULL)) NOT VALID;

ALTER TABLE evt.events
    ADD CONSTRAINT required_certainty CHECK ((certainty IS NOT NULL)) NOT VALID;

ALTER TABLE evt.state_changes
    ADD CONSTRAINT required_changed_at CHECK ((changed_at IS NOT NULL)) NOT VALID;

ALTER TABLE evt.claims
    ADD CONSTRAINT required_claim_type CHECK ((claim_type IS NOT NULL)) NOT VALID;

ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_class CHECK ((class IS NOT NULL)) NOT VALID;

ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_class_reasons CHECK ((class_reasons IS NOT NULL)) NOT VALID;

ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_config_version CHECK ((config_version IS NOT NULL)) NOT VALID;

ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_criterion CHECK ((criterion IS NOT NULL)) NOT VALID;

ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_cutoff CHECK ((cutoff IS NOT NULL)) NOT VALID;

ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_dims CHECK ((dims IS NOT NULL)) NOT VALID;

ALTER TABLE evt.claims
    ADD CONSTRAINT required_directionality CHECK ((directionality IS NOT NULL)) NOT VALID;

ALTER TABLE evt.event_entities
    ADD CONSTRAINT required_entity_id CHECK ((entity_id IS NOT NULL)) NOT VALID;

ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_event_id CHECK ((event_id IS NOT NULL)) NOT VALID;

ALTER TABLE evt.event_entities
    ADD CONSTRAINT required_event_id CHECK ((event_id IS NOT NULL)) NOT VALID;

ALTER TABLE evt.event_observations
    ADD CONSTRAINT required_event_id CHECK ((event_id IS NOT NULL)) NOT VALID;

ALTER TABLE evt.state_changes
    ADD CONSTRAINT required_event_id CHECK ((event_id IS NOT NULL)) NOT VALID;

ALTER TABLE evt.events
    ADD CONSTRAINT required_event_state CHECK ((event_state IS NOT NULL)) NOT VALID;

ALTER TABLE evt.state_changes
    ADD CONSTRAINT required_evidence CHECK ((evidence IS NOT NULL)) NOT VALID;

ALTER TABLE evt.claims
    ADD CONSTRAINT required_evidence_class CHECK ((evidence_class IS NOT NULL)) NOT VALID;

ALTER TABLE evt.event_observations
    ADD CONSTRAINT required_freshness CHECK ((freshness IS NOT NULL)) NOT VALID;

ALTER TABLE evt.events
    ADD CONSTRAINT required_freshness CHECK ((freshness IS NOT NULL)) NOT VALID;

ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_gates CHECK ((gates IS NOT NULL)) NOT VALID;

ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_impact_horizon CHECK ((impact_horizon IS NOT NULL)) NOT VALID;

ALTER TABLE evt.event_observations
    ADD CONSTRAINT required_item_id CHECK ((item_id IS NOT NULL)) NOT VALID;

ALTER TABLE evt.events
    ADD CONSTRAINT required_known_to_us_at CHECK ((known_to_us_at IS NOT NULL)) NOT VALID;

ALTER TABLE evt.state_changes
    ADD CONSTRAINT required_ladder CHECK ((ladder IS NOT NULL)) NOT VALID;

ALTER TABLE evt.events
    ADD CONSTRAINT required_lifecycle CHECK ((lifecycle IS NOT NULL)) NOT VALID;

ALTER TABLE evt.event_observations
    ADD CONSTRAINT required_link_type CHECK ((link_type IS NOT NULL)) NOT VALID;

ALTER TABLE evt.event_observations
    ADD CONSTRAINT required_match_method CHECK ((match_method IS NOT NULL)) NOT VALID;

ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_model_id CHECK ((model_id IS NOT NULL)) NOT VALID;

ALTER TABLE evt.claims
    ADD CONSTRAINT required_obs_id CHECK ((obs_id IS NOT NULL)) NOT VALID;

ALTER TABLE evt.event_observations
    ADD CONSTRAINT required_observed_at CHECK ((observed_at IS NOT NULL)) NOT VALID;

ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_prompt_version CHECK ((prompt_version IS NOT NULL)) NOT VALID;

ALTER TABLE evt.event_observations
    ADD CONSTRAINT required_public_at CHECK ((public_at IS NOT NULL)) NOT VALID;

ALTER TABLE evt.events
    ADD CONSTRAINT required_public_at CHECK ((public_at IS NOT NULL)) NOT VALID;

ALTER TABLE evt.events
    ADD CONSTRAINT required_public_at_basis CHECK ((public_at_basis IS NOT NULL)) NOT VALID;

ALTER TABLE evt.events
    ADD CONSTRAINT required_public_at_prec CHECK ((public_at_prec IS NOT NULL)) NOT VALID;

ALTER TABLE evt.claims
    ADD CONSTRAINT required_quote CHECK ((quote IS NOT NULL)) NOT VALID;

ALTER TABLE evt.event_entities
    ADD CONSTRAINT required_role CHECK ((role IS NOT NULL)) NOT VALID;

ALTER TABLE evt.state_changes
    ADD CONSTRAINT required_serial_id CHECK ((id IS NOT NULL)) NOT VALID;

ALTER TABLE evt.events
    ADD CONSTRAINT required_state_verified CHECK ((state_verified IS NOT NULL)) NOT VALID;

ALTER TABLE evt.events
    ADD CONSTRAINT required_statement CHECK ((statement IS NOT NULL)) NOT VALID;

ALTER TABLE evt.events
    ADD CONSTRAINT required_theme_tags CHECK ((theme_tags IS NOT NULL)) NOT VALID;

ALTER TABLE evt.events
    ADD CONSTRAINT required_timing_confidence CHECK ((timing_confidence IS NOT NULL)) NOT VALID;

ALTER TABLE evt.state_changes
    ADD CONSTRAINT required_to_state CHECK ((to_state IS NOT NULL)) NOT VALID;

ALTER TABLE evt.claims
    ADD CONSTRAINT required_verified CHECK ((verified IS NOT NULL)) NOT VALID;

ALTER TABLE evt.ems_assessments
    ADD CONSTRAINT required_version CHECK ((version IS NOT NULL)) NOT VALID;

ALTER TABLE ONLY evt.state_changes
    ADD CONSTRAINT state_changes_pkey PRIMARY KEY (storage_key);

ALTER TABLE ingest.source_items
    ADD CONSTRAINT required_content_hash CHECK ((content_hash IS NOT NULL)) NOT VALID;

ALTER TABLE ingest.source_items
    ADD CONSTRAINT required_external_id CHECK ((external_id IS NOT NULL)) NOT VALID;

ALTER TABLE ingest.source_items
    ADD CONSTRAINT required_extraction_status CHECK ((extraction_status IS NOT NULL)) NOT VALID;

ALTER TABLE ingest.source_items
    ADD CONSTRAINT required_feed CHECK ((feed IS NOT NULL)) NOT VALID;

ALTER TABLE ingest.source_items
    ADD CONSTRAINT required_observed_at CHECK ((observed_at IS NOT NULL)) NOT VALID;

ALTER TABLE ingest.source_items
    ADD CONSTRAINT required_published_prec CHECK ((published_prec IS NOT NULL)) NOT VALID;

ALTER TABLE ingest.source_items
    ADD CONSTRAINT required_raw_path CHECK ((raw_path IS NOT NULL)) NOT VALID;

ALTER TABLE ingest.source_items
    ADD CONSTRAINT required_raw_sha256 CHECK ((raw_sha256 IS NOT NULL)) NOT VALID;

ALTER TABLE ingest.source_items
    ADD CONSTRAINT required_relevance_decision CHECK (((relevance_decision IS NOT NULL) OR (COALESCE(length((document #>> '{relevance,pending_reason}'::text[])), 0) > 0))) NOT VALID;

ALTER TABLE ingest.source_items
    ADD CONSTRAINT required_source_class CHECK ((source_class IS NOT NULL)) NOT VALID;

ALTER TABLE ingest.source_items
    ADD CONSTRAINT required_source_id CHECK ((source_id IS NOT NULL)) NOT VALID;

ALTER TABLE ONLY ingest.source_items
    ADD CONSTRAINT source_items_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY ingest.source_items
    ADD CONSTRAINT source_items_source_id_feed_external_id_key UNIQUE (source_id, feed, external_id);

ALTER TABLE ONLY mkt.backfill_runs
    ADD CONSTRAINT backfill_runs_backfill_id_key UNIQUE (backfill_id);

ALTER TABLE ONLY mkt.backfill_runs
    ADD CONSTRAINT backfill_runs_storage_key_key UNIQUE (storage_key);

ALTER TABLE ONLY mkt.capture_gaps
    ADD CONSTRAINT capture_gaps_gap_id_key UNIQUE (gap_id);

ALTER TABLE ONLY mkt.capture_gaps
    ADD CONSTRAINT capture_gaps_storage_key_key UNIQUE (storage_key);

ALTER TABLE ONLY mkt.capture_requests
    ADD CONSTRAINT capture_requests_request_id_key UNIQUE (request_id);

ALTER TABLE ONLY mkt.capture_requests
    ADD CONSTRAINT capture_requests_storage_key_key UNIQUE (storage_key);

ALTER TABLE ONLY mkt.capture_streams
    ADD CONSTRAINT capture_streams_instrument_id_stream_key UNIQUE (instrument_id, stream);

ALTER TABLE ONLY mkt.capture_streams
    ADD CONSTRAINT capture_streams_storage_key_key UNIQUE (storage_key);

ALTER TABLE ONLY mkt.hot_tokens
    ADD CONSTRAINT hot_tokens_storage_key_key UNIQUE (storage_key);

ALTER TABLE ONLY mkt.hot_tokens
    ADD CONSTRAINT hot_tokens_token_id_event_id_key UNIQUE (token_id, event_id);

ALTER TABLE mkt.hot_tokens
    ADD CONSTRAINT required_event_id CHECK ((event_id IS NOT NULL)) NOT VALID;

ALTER TABLE mkt.hot_tokens
    ADD CONSTRAINT required_priority CHECK ((priority IS NOT NULL)) NOT VALID;

ALTER TABLE mkt.hot_tokens
    ADD CONSTRAINT required_since CHECK ((since IS NOT NULL)) NOT VALID;

ALTER TABLE mkt.hot_tokens
    ADD CONSTRAINT required_token_id CHECK ((token_id IS NOT NULL)) NOT VALID;

ALTER TABLE ONLY mkt.supply_obs
    ADD CONSTRAINT supply_obs_storage_key_key UNIQUE (storage_key);

ALTER TABLE ONLY mkt.supply
    ADD CONSTRAINT supply_storage_key_key UNIQUE (storage_key);

ALTER TABLE ONLY mkt.unlocks
    ADD CONSTRAINT unlocks_storage_key_key UNIQUE (storage_key);

ALTER TABLE ONLY ops.agent_sessions
    ADD CONSTRAINT agent_sessions_event_id_key UNIQUE (event_id);

ALTER TABLE ONLY ops.agent_sessions
    ADD CONSTRAINT agent_sessions_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY ops.capture_tier_transitions
    ADD CONSTRAINT capture_tier_transitions_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY ops.capture_tier_transitions
    ADD CONSTRAINT capture_tier_transitions_transition_id_key UNIQUE (transition_id);

ALTER TABLE ONLY ops.jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY ops.jobs
    ADD CONSTRAINT jobs_type_idem_key_key UNIQUE (type, idem_key);

ALTER TABLE ONLY ops.llm_usage
    ADD CONSTRAINT llm_usage_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY ops.request_budget_blocks
    ADD CONSTRAINT request_budget_blocks_pkey PRIMARY KEY (host);

ALTER TABLE ops.agent_sessions
    ADD CONSTRAINT required_agent_version CHECK (((agent_version IS NOT NULL) OR (COALESCE(length((document ->> 'agent_version_reason'::text)), 0) > 0))) NOT VALID;

ALTER TABLE ops.llm_usage
    ADD CONSTRAINT required_at CHECK ((at IS NOT NULL)) NOT VALID;

ALTER TABLE ops.jobs
    ADD CONSTRAINT required_attempts CHECK ((attempts IS NOT NULL)) NOT VALID;

ALTER TABLE ops.llm_usage
    ADD CONSTRAINT required_cache_read_tokens CHECK (((cache_read_tokens IS NOT NULL) OR (COALESCE(length((document #>> '{missing_fields,cache_read_tokens}'::text[])), 0) > 0))) NOT VALID;

ALTER TABLE ops.llm_usage
    ADD CONSTRAINT required_cache_write_tokens CHECK (((cache_write_tokens IS NOT NULL) OR (COALESCE(length((document #>> '{missing_fields,cache_write_tokens}'::text[])), 0) > 0))) NOT VALID;

ALTER TABLE ops.llm_usage
    ADD CONSTRAINT required_call CHECK ((call IS NOT NULL)) NOT VALID;

ALTER TABLE ops.llm_usage
    ADD CONSTRAINT required_cost_usd CHECK (((cost_usd IS NOT NULL) OR (COALESCE(length((document #>> '{missing_fields,cost_usd}'::text[])), 0) > 0))) NOT VALID;

ALTER TABLE ops.llm_usage
    ADD CONSTRAINT required_effort CHECK (((effort IS NOT NULL) OR (COALESCE(length((document #>> '{missing_fields,effort}'::text[])), 0) > 0))) NOT VALID;

ALTER TABLE ops.jobs
    ADD CONSTRAINT required_expires_at CHECK (((expires_at IS NOT NULL) OR ((type = 'ESCALATE_TOKENS'::text) OR (type <> ALL (ARRAY['FILTER_ITEM'::text, 'ROUTE_ITEM'::text, 'AGENT_MESSAGE'::text, 'RUN_MARKET_TRUTH'::text, 'DELIVER'::text, 'EVAL_CONDITIONS'::text, 'MILESTONE_CHECK'::text, 'REACTION_CAPTURE'::text, 'SCOUT_REVIEW'::text]))))) NOT VALID;

ALTER TABLE ops.jobs
    ADD CONSTRAINT required_idem_key CHECK ((idem_key IS NOT NULL)) NOT VALID;

ALTER TABLE ops.tool_calls
    ADD CONSTRAINT required_input CHECK ((input IS NOT NULL)) NOT VALID;

ALTER TABLE ops.llm_usage
    ADD CONSTRAINT required_input_tokens CHECK (((input_tokens IS NOT NULL) OR (COALESCE(length((document #>> '{missing_fields,input_tokens}'::text[])), 0) > 0))) NOT VALID;

ALTER TABLE ops.agent_sessions
    ADD CONSTRAINT required_last_event_seq CHECK ((last_event_seq IS NOT NULL)) NOT VALID;

ALTER TABLE ops.agent_sessions
    ADD CONSTRAINT required_last_item_at CHECK (((last_item_at IS NOT NULL) OR (COALESCE(length((document ->> 'last_item_at_reason'::text)), 0) > 0))) NOT VALID;

ALTER TABLE ops.jobs
    ADD CONSTRAINT required_max_attempts CHECK ((max_attempts IS NOT NULL)) NOT VALID;

ALTER TABLE ops.llm_usage
    ADD CONSTRAINT required_model CHECK ((model IS NOT NULL)) NOT VALID;

ALTER TABLE ops.agent_sessions
    ADD CONSTRAINT required_opened_at CHECK ((opened_at IS NOT NULL)) NOT VALID;

ALTER TABLE ops.agent_sessions
    ADD CONSTRAINT required_opened_by_item CHECK (((opened_by_item IS NOT NULL) OR (COALESCE(length((document ->> 'proposal_id'::text)), 0) > 0))) NOT VALID;

ALTER TABLE ops.llm_usage
    ADD CONSTRAINT required_output_tokens CHECK (((output_tokens IS NOT NULL) OR (COALESCE(length((document #>> '{missing_fields,output_tokens}'::text[])), 0) > 0))) NOT VALID;

ALTER TABLE ops.jobs
    ADD CONSTRAINT required_payload CHECK ((payload IS NOT NULL)) NOT VALID;

ALTER TABLE ops.jobs
    ADD CONSTRAINT required_priority CHECK ((priority IS NOT NULL)) NOT VALID;

ALTER TABLE ops.tool_calls
    ADD CONSTRAINT required_requested_at CHECK ((requested_at IS NOT NULL)) NOT VALID;

ALTER TABLE ops.jobs
    ADD CONSTRAINT required_run_after CHECK ((run_after IS NOT NULL)) NOT VALID;

ALTER TABLE ops.llm_usage
    ADD CONSTRAINT required_serial_id CHECK ((id IS NOT NULL)) NOT VALID;

ALTER TABLE ops.tool_calls
    ADD CONSTRAINT required_session_id CHECK ((session_id IS NOT NULL)) NOT VALID;

ALTER TABLE ops.agent_sessions
    ADD CONSTRAINT required_status CHECK ((status IS NOT NULL)) NOT VALID;

ALTER TABLE ops.jobs
    ADD CONSTRAINT required_status CHECK ((status IS NOT NULL)) NOT VALID;

ALTER TABLE ops.tool_calls
    ADD CONSTRAINT required_status CHECK ((status IS NOT NULL)) NOT VALID;

ALTER TABLE ops.llm_usage
    ADD CONSTRAINT required_stop_reason CHECK (((stop_reason IS NOT NULL) OR (COALESCE(length((document #>> '{missing_fields,stop_reason}'::text[])), 0) > 0))) NOT VALID;

ALTER TABLE ops.llm_usage
    ADD CONSTRAINT required_subject_id CHECK ((subject_id IS NOT NULL)) NOT VALID;

ALTER TABLE ops.tool_calls
    ADD CONSTRAINT required_tool CHECK ((tool IS NOT NULL)) NOT VALID;

ALTER TABLE ops.jobs
    ADD CONSTRAINT required_type CHECK ((type IS NOT NULL)) NOT VALID;

ALTER TABLE ops.llm_usage
    ADD CONSTRAINT required_web_searches CHECK (((web_searches IS NOT NULL) OR (COALESCE(length((document #>> '{missing_fields,web_searches}'::text[])), 0) > 0))) NOT VALID;

ALTER TABLE ONLY ops.scheduler_checkpoints
    ADD CONSTRAINT scheduler_checkpoints_checkpoint_id_key UNIQUE (checkpoint_id);

ALTER TABLE ONLY ops.scheduler_checkpoints
    ADD CONSTRAINT scheduler_checkpoints_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY ops.source_checkpoints
    ADD CONSTRAINT source_checkpoints_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY ops.source_escalations
    ADD CONSTRAINT source_escalations_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY ops.tool_calls
    ADD CONSTRAINT tool_calls_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY "out".alerts
    ADD CONSTRAINT alerts_alert_id_version_key UNIQUE (alert_id, version);

ALTER TABLE ONLY "out".alerts
    ADD CONSTRAINT alerts_pkey PRIMARY KEY (storage_key);

ALTER TABLE "out".ops_notices
    ADD CONSTRAINT current_notice_kind CHECK ((kind = ANY (ARRAY['DELIVERY_FAILED'::text, 'HEARTBEAT'::text]))) NOT VALID;

ALTER TABLE ONLY "out".deliveries
    ADD CONSTRAINT deliveries_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY "out".evidence_bundles
    ADD CONSTRAINT evidence_bundles_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY "out".ops_notices
    ADD CONSTRAINT ops_notices_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY "out".reaction_checkpoints
    ADD CONSTRAINT reaction_checkpoints_pkey PRIMARY KEY (storage_key);

ALTER TABLE "out".alerts
    ADD CONSTRAINT required_alert_id CHECK ((alert_id IS NOT NULL)) NOT VALID;

ALTER TABLE "out".deliveries
    ADD CONSTRAINT required_at CHECK ((at IS NOT NULL)) NOT VALID;

ALTER TABLE "out".deliveries
    ADD CONSTRAINT required_attempt CHECK ((attempt IS NOT NULL)) NOT VALID;

ALTER TABLE "out".alerts
    ADD CONSTRAINT required_card_struct CHECK ((card_struct IS NOT NULL)) NOT VALID;

ALTER TABLE "out".alerts
    ADD CONSTRAINT required_card_text CHECK ((card_text IS NOT NULL)) NOT VALID;

ALTER TABLE "out".deliveries
    ADD CONSTRAINT required_channel CHECK ((channel IS NOT NULL)) NOT VALID;

ALTER TABLE "out".alerts
    ADD CONSTRAINT required_code_version CHECK ((code_version IS NOT NULL)) NOT VALID;

ALTER TABLE "out".alerts
    ADD CONSTRAINT required_config_version CHECK ((config_version IS NOT NULL)) NOT VALID;

ALTER TABLE "out".ops_notices
    ADD CONSTRAINT required_created_at CHECK ((created_at IS NOT NULL)) NOT VALID;

ALTER TABLE "out".ops_notices
    ADD CONSTRAINT required_created_by CHECK ((created_by IS NOT NULL)) NOT VALID;

ALTER TABLE "out".alerts
    ADD CONSTRAINT required_decisions CHECK ((decisions IS NOT NULL)) NOT VALID;

ALTER TABLE "out".alerts
    ADD CONSTRAINT required_event_id CHECK ((event_id IS NOT NULL)) NOT VALID;

ALTER TABLE "out".alerts
    ADD CONSTRAINT required_evidence_bundle_id CHECK ((evidence_bundle_id IS NOT NULL)) NOT VALID;

ALTER TABLE "out".alerts
    ADD CONSTRAINT required_generated_at CHECK ((generated_at IS NOT NULL)) NOT VALID;

ALTER TABLE "out".alerts
    ADD CONSTRAINT required_kind CHECK ((kind IS NOT NULL)) NOT VALID;

ALTER TABLE "out".ops_notices
    ADD CONSTRAINT required_kind CHECK ((kind IS NOT NULL)) NOT VALID;

ALTER TABLE "out".deliveries
    ADD CONSTRAINT required_serial_id CHECK ((id IS NOT NULL)) NOT VALID;

ALTER TABLE "out".deliveries
    ADD CONSTRAINT required_status CHECK ((status IS NOT NULL)) NOT VALID;

ALTER TABLE "out".ops_notices
    ADD CONSTRAINT required_text CHECK ((text IS NOT NULL)) NOT VALID;

ALTER TABLE "out".alerts
    ADD CONSTRAINT required_version CHECK ((version IS NOT NULL)) NOT VALID;

ALTER TABLE ONLY qa.fixtures
    ADD CONSTRAINT fixtures_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY qa.replay_runs
    ADD CONSTRAINT replay_runs_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY qa.reviews
    ADD CONSTRAINT reviews_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY reg.dependency_map
    ADD CONSTRAINT dependency_map_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY reg.entities
    ADD CONSTRAINT entities_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY reg.entity_aliases
    ADD CONSTRAINT entity_aliases_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY reg.exchange_addresses
    ADD CONSTRAINT exchange_addresses_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY reg.instruments
    ADD CONSTRAINT instruments_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY reg.metric_semantics
    ADD CONSTRAINT metric_semantics_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY reg.sector_baskets
    ADD CONSTRAINT sector_baskets_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY reg.sources
    ADD CONSTRAINT sources_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY reg.tokens
    ADD CONSTRAINT tokens_pkey PRIMARY KEY (storage_key);

ALTER TABLE ONLY scout.expansion_proposals
    ADD CONSTRAINT expansion_proposals_pkey PRIMARY KEY (storage_key);

CREATE INDEX ana_calls_call_id_idx ON ana.calls USING btree (call_id);

CREATE INDEX ana_calls_closed_at_idx ON ana.calls USING btree (closed_at);

CREATE INDEX ana_calls_doc_idx ON ana.calls USING gin (document);

CREATE UNIQUE INDEX ana_calls_domain_id ON ana.calls USING btree (call_id);

CREATE INDEX ana_calls_event_id_idx ON ana.calls USING btree (event_id);

CREATE INDEX ana_calls_opened_at_idx ON ana.calls USING btree (opened_at);

CREATE INDEX ana_calls_status_idx ON ana.calls USING btree (status);

CREATE INDEX ana_calls_token_id_idx ON ana.calls USING btree (token_id);

CREATE INDEX ana_contradictions_contradiction_id_idx ON ana.contradictions USING btree (contradiction_id);

CREATE INDEX ana_contradictions_detected_at_idx ON ana.contradictions USING btree (detected_at);

CREATE INDEX ana_contradictions_doc_idx ON ana.contradictions USING gin (document);

CREATE UNIQUE INDEX ana_contradictions_domain_id ON ana.contradictions USING btree (contradiction_id);

CREATE INDEX ana_contradictions_event_id_idx ON ana.contradictions USING btree (event_id);

CREATE INDEX ana_contradictions_resolved_at_idx ON ana.contradictions USING btree (resolved_at);

CREATE INDEX ana_contradictions_token_id_idx ON ana.contradictions USING btree (token_id);

CREATE INDEX ana_decisions_call_id_idx ON ana.decisions USING btree (call_id);

CREATE INDEX ana_decisions_cutoff_idx ON ana.decisions USING btree (cutoff);

CREATE INDEX ana_decisions_dec_id_idx ON ana.decisions USING btree (dec_id);

CREATE INDEX ana_decisions_doc_idx ON ana.decisions USING gin (document);

CREATE UNIQUE INDEX ana_decisions_domain_id ON ana.decisions USING btree (dec_id);

CREATE INDEX ana_decisions_event_id_idx ON ana.decisions USING btree (event_id);

CREATE INDEX ana_decisions_mta_id_idx ON ana.decisions USING btree (mta_id);

CREATE INDEX ana_decisions_token_id_idx ON ana.decisions USING btree (token_id);

CREATE INDEX ana_market_truth_cutoff_idx ON ana.market_truth USING btree (cutoff);

CREATE INDEX ana_market_truth_doc_idx ON ana.market_truth USING gin (document);

CREATE UNIQUE INDEX ana_market_truth_domain_id ON ana.market_truth USING btree (mta_id);

CREATE INDEX ana_market_truth_event_id_idx ON ana.market_truth USING btree (event_id);

CREATE INDEX ana_market_truth_mta_id_idx ON ana.market_truth USING btree (mta_id);

CREATE INDEX ana_market_truth_token_cutoff ON ana.market_truth USING btree (token_id, cutoff);

CREATE INDEX ana_market_truth_token_id_idx ON ana.market_truth USING btree (token_id);

CREATE INDEX ana_outlooks_cutoff_idx ON ana.outlooks USING btree (cutoff);

CREATE INDEX ana_outlooks_doc_idx ON ana.outlooks USING gin (document);

CREATE UNIQUE INDEX ana_outlooks_domain_id ON ana.outlooks USING btree (out_id);

CREATE INDEX ana_outlooks_event_id_idx ON ana.outlooks USING btree (event_id);

CREATE INDEX ana_outlooks_out_id_idx ON ana.outlooks USING btree (out_id);

CREATE INDEX ana_outlooks_token_id_idx ON ana.outlooks USING btree (token_id);

CREATE INDEX ana_watch_conditions_cond_id_idx ON ana.watch_conditions USING btree (cond_id);

CREATE INDEX ana_watch_conditions_doc_idx ON ana.watch_conditions USING gin (document);

CREATE UNIQUE INDEX ana_watch_conditions_domain_id ON ana.watch_conditions USING btree (cond_id);

CREATE INDEX ana_watch_conditions_event_id_idx ON ana.watch_conditions USING btree (event_id);

CREATE INDEX ana_watch_conditions_expires_at_idx ON ana.watch_conditions USING btree (expires_at);

CREATE INDEX ana_watch_conditions_fired_at_idx ON ana.watch_conditions USING btree (fired_at);

CREATE INDEX ana_watch_conditions_status_idx ON ana.watch_conditions USING btree (status);

CREATE INDEX ana_watch_conditions_token_id_idx ON ana.watch_conditions USING btree (token_id);

CREATE INDEX decisions_event_token_latest ON ana.decisions USING btree (event_id, token_id, cutoff DESC, storage_key DESC);

CREATE INDEX market_truth_event_token_latest ON ana.market_truth USING btree (event_id, token_id, cutoff DESC, storage_key DESC);

CREATE INDEX assessments_class_events ON evt.ems_assessments USING btree (class, event_id);

CREATE INDEX evt_beneficiary_edges_doc_idx ON evt.beneficiary_edges USING gin (document);

CREATE INDEX evt_beneficiary_edges_edge_id_idx ON evt.beneficiary_edges USING btree (edge_id);

CREATE INDEX evt_beneficiary_edges_entity_id_idx ON evt.beneficiary_edges USING btree (entity_id);

CREATE INDEX evt_beneficiary_edges_event_id_idx ON evt.beneficiary_edges USING btree (event_id);

CREATE INDEX evt_beneficiary_edges_token_id_idx ON evt.beneficiary_edges USING btree (token_id);

CREATE INDEX evt_beneficiary_maps_cutoff_idx ON evt.beneficiary_maps USING btree (cutoff);

CREATE INDEX evt_beneficiary_maps_doc_idx ON evt.beneficiary_maps USING gin (document);

CREATE INDEX evt_beneficiary_maps_event_id_idx ON evt.beneficiary_maps USING btree (event_id);

CREATE UNIQUE INDEX evt_beneficiary_maps_version ON evt.beneficiary_maps USING btree (event_id, map_version);

CREATE INDEX evt_capture_chains_doc_idx ON evt.capture_chains USING gin (document);

CREATE INDEX evt_capture_chains_event_id_idx ON evt.capture_chains USING btree (event_id);

CREATE INDEX evt_capture_chains_token_id_idx ON evt.capture_chains USING btree (token_id);

CREATE UNIQUE INDEX evt_capture_chains_version ON evt.capture_chains USING btree (event_id, token_id, version);

CREATE INDEX evt_claims_claim_id_idx ON evt.claims USING btree (claim_id);

CREATE INDEX evt_claims_doc_idx ON evt.claims USING gin (document);

CREATE UNIQUE INDEX evt_claims_domain_id ON evt.claims USING btree (claim_id);

CREATE INDEX evt_claims_obs_id_idx ON evt.claims USING btree (obs_id);

CREATE INDEX evt_ems_assessments_cutoff_idx ON evt.ems_assessments USING btree (cutoff);

CREATE INDEX evt_ems_assessments_doc_idx ON evt.ems_assessments USING gin (document);

CREATE UNIQUE INDEX evt_ems_assessments_domain_id ON evt.ems_assessments USING btree (ems_id);

CREATE INDEX evt_ems_assessments_ems_id_idx ON evt.ems_assessments USING btree (ems_id);

CREATE INDEX evt_ems_assessments_event_id_idx ON evt.ems_assessments USING btree (event_id);

CREATE INDEX evt_ems_assessments_model_id_idx ON evt.ems_assessments USING btree (model_id);

CREATE INDEX evt_event_entities_doc_idx ON evt.event_entities USING gin (document);

CREATE INDEX evt_event_entities_entity_id_idx ON evt.event_entities USING btree (entity_id);

CREATE INDEX evt_event_entities_event_id_idx ON evt.event_entities USING btree (event_id);

CREATE UNIQUE INDEX evt_event_entities_natural ON evt.event_entities USING btree (event_id, entity_id, role);

CREATE INDEX evt_event_observations_doc_idx ON evt.event_observations USING gin (document);

CREATE UNIQUE INDEX evt_event_observations_domain_id ON evt.event_observations USING btree (obs_id);

CREATE INDEX evt_event_observations_event_id_idx ON evt.event_observations USING btree (event_id);

CREATE INDEX evt_event_observations_item_id_idx ON evt.event_observations USING btree (item_id);

CREATE INDEX evt_event_observations_obs_id_idx ON evt.event_observations USING btree (obs_id);

CREATE INDEX evt_event_observations_observed_at_idx ON evt.event_observations USING btree (observed_at);

CREATE INDEX evt_event_observations_public_at_idx ON evt.event_observations USING btree (public_at);

CREATE INDEX evt_events_current_ems_id_idx ON evt.events USING btree (current_ems_id);

CREATE INDEX evt_events_doc_idx ON evt.events USING gin (document);

CREATE UNIQUE INDEX evt_events_domain_id ON evt.events USING btree (event_id);

CREATE INDEX evt_events_event_id_idx ON evt.events USING btree (event_id);

CREATE INDEX evt_events_event_time_idx ON evt.events USING btree (event_time);

CREATE INDEX evt_events_known_to_us_at_idx ON evt.events USING btree (known_to_us_at);

CREATE INDEX evt_events_lifecycle_idx ON evt.events USING btree (lifecycle);

CREATE INDEX evt_events_public_at_idx ON evt.events USING btree (public_at);

CREATE INDEX evt_events_verified_at_idx ON evt.events USING btree (verified_at);

CREATE INDEX evt_state_changes_changed_at_idx ON evt.state_changes USING btree (changed_at);

CREATE INDEX evt_state_changes_doc_idx ON evt.state_changes USING gin (document);

CREATE INDEX evt_state_changes_edge_id_idx ON evt.state_changes USING btree (edge_id);

CREATE INDEX evt_state_changes_event_id_idx ON evt.state_changes USING btree (event_id);

CREATE INDEX ingest_source_items_content_hash_idx ON ingest.source_items USING btree (content_hash);

CREATE INDEX ingest_source_items_doc_idx ON ingest.source_items USING gin (document);

CREATE UNIQUE INDEX ingest_source_items_domain_id ON ingest.source_items USING btree (item_id);

CREATE INDEX ingest_source_items_external_id_idx ON ingest.source_items USING btree (external_id);

CREATE INDEX ingest_source_items_item_id_idx ON ingest.source_items USING btree (item_id);

CREATE INDEX ingest_source_items_observed_at_idx ON ingest.source_items USING btree (observed_at);

CREATE INDEX ingest_source_items_published_at_idx ON ingest.source_items USING btree (published_at);

CREATE INDEX ingest_source_items_route_event_id_idx ON ingest.source_items USING btree (route_event_id);

CREATE INDEX ingest_source_items_source_id_idx ON ingest.source_items USING btree (source_id);

CREATE INDEX backfill_runs_anchor_storage_key_idx ON mkt.backfill_runs USING btree (anchor, storage_key);

CREATE INDEX backfill_runs_finished_at_storage_key_idx ON mkt.backfill_runs USING btree (finished_at, storage_key);

CREATE INDEX backfill_runs_received_ts_storage_key_idx ON mkt.backfill_runs USING btree (received_ts, storage_key);

CREATE INDEX backfill_runs_recorded_at_storage_key_idx ON mkt.backfill_runs USING btree (recorded_at, storage_key);

CREATE INDEX backfill_runs_started_at_storage_key_idx ON mkt.backfill_runs USING btree (started_at, storage_key);

CREATE INDEX backfill_runs_storage_key_idx ON mkt.backfill_runs USING btree (storage_key);

CREATE INDEX backfill_runs_token_id_anchor_storage_key_idx ON mkt.backfill_runs USING btree (token_id, anchor, storage_key);

CREATE INDEX backfill_runs_token_id_finished_at_storage_key_idx ON mkt.backfill_runs USING btree (token_id, finished_at, storage_key);

CREATE INDEX backfill_runs_token_id_received_ts_storage_key_idx ON mkt.backfill_runs USING btree (token_id, received_ts, storage_key);

CREATE INDEX backfill_runs_token_id_recorded_at_storage_key_idx ON mkt.backfill_runs USING btree (token_id, recorded_at, storage_key);

CREATE INDEX backfill_runs_token_id_started_at_storage_key_idx ON mkt.backfill_runs USING btree (token_id, started_at, storage_key);

CREATE INDEX capture_gaps_end_storage_key_idx ON mkt.capture_gaps USING btree ("end", storage_key);

CREATE INDEX capture_gaps_observed_at_storage_key_idx ON mkt.capture_gaps USING btree (observed_at, storage_key);

CREATE INDEX capture_gaps_received_ts_storage_key_idx ON mkt.capture_gaps USING btree (received_ts, storage_key);

CREATE INDEX capture_gaps_recorded_at_storage_key_idx ON mkt.capture_gaps USING btree (recorded_at, storage_key);

CREATE INDEX capture_gaps_start_storage_key_idx ON mkt.capture_gaps USING btree (start, storage_key);

CREATE INDEX capture_gaps_storage_key_idx ON mkt.capture_gaps USING btree (storage_key);

CREATE INDEX capture_gaps_token_id_end_storage_key_idx ON mkt.capture_gaps USING btree (token_id, "end", storage_key);

CREATE INDEX capture_gaps_token_id_observed_at_storage_key_idx ON mkt.capture_gaps USING btree (token_id, observed_at, storage_key);

CREATE INDEX capture_gaps_token_id_received_ts_storage_key_idx ON mkt.capture_gaps USING btree (token_id, received_ts, storage_key);

CREATE INDEX capture_gaps_token_id_recorded_at_storage_key_idx ON mkt.capture_gaps USING btree (token_id, recorded_at, storage_key);

CREATE INDEX capture_gaps_token_id_start_storage_key_idx ON mkt.capture_gaps USING btree (token_id, start, storage_key);

CREATE INDEX capture_requests_status_idx ON mkt.capture_requests USING btree (status, requested_at);

CREATE INDEX capture_streams_token_idx ON mkt.capture_streams USING btree (token_id, stream);

CREATE INDEX hot_tokens_expires_at_storage_key_idx ON mkt.hot_tokens USING btree (expires_at, storage_key);

CREATE INDEX hot_tokens_received_ts_storage_key_idx ON mkt.hot_tokens USING btree (received_ts, storage_key);

CREATE INDEX hot_tokens_recorded_at_storage_key_idx ON mkt.hot_tokens USING btree (recorded_at, storage_key);

CREATE INDEX hot_tokens_since_storage_key_idx ON mkt.hot_tokens USING btree (since, storage_key);

CREATE INDEX hot_tokens_storage_key_idx ON mkt.hot_tokens USING btree (storage_key);

CREATE INDEX hot_tokens_token_id_expires_at_storage_key_idx ON mkt.hot_tokens USING btree (token_id, expires_at, storage_key);

CREATE INDEX hot_tokens_token_id_received_ts_storage_key_idx ON mkt.hot_tokens USING btree (token_id, received_ts, storage_key);

CREATE INDEX hot_tokens_token_id_recorded_at_storage_key_idx ON mkt.hot_tokens USING btree (token_id, recorded_at, storage_key);

CREATE INDEX hot_tokens_token_id_since_storage_key_idx ON mkt.hot_tokens USING btree (token_id, since, storage_key);

CREATE INDEX supply_obs_received_ts_storage_key_idx ON mkt.supply_obs USING btree (received_ts, storage_key);

CREATE INDEX supply_obs_recorded_at_storage_key_idx ON mkt.supply_obs USING btree (recorded_at, storage_key);

CREATE INDEX supply_obs_storage_key_idx ON mkt.supply_obs USING btree (storage_key);

CREATE INDEX supply_obs_token_id_received_ts_storage_key_idx ON mkt.supply_obs USING btree (token_id, received_ts, storage_key);

CREATE INDEX supply_obs_token_id_recorded_at_storage_key_idx ON mkt.supply_obs USING btree (token_id, recorded_at, storage_key);

CREATE INDEX supply_obs_token_id_ts_storage_key_idx ON mkt.supply_obs USING btree (token_id, ts, storage_key);

CREATE INDEX supply_obs_ts_storage_key_idx ON mkt.supply_obs USING btree (ts, storage_key);

CREATE INDEX supply_received_ts_storage_key_idx ON mkt.supply USING btree (received_ts, storage_key);

CREATE INDEX supply_recorded_at_storage_key_idx ON mkt.supply USING btree (recorded_at, storage_key);

CREATE INDEX supply_storage_key_idx ON mkt.supply USING btree (storage_key);

CREATE INDEX supply_token_id_received_ts_storage_key_idx ON mkt.supply USING btree (token_id, received_ts, storage_key);

CREATE INDEX supply_token_id_recorded_at_storage_key_idx ON mkt.supply USING btree (token_id, recorded_at, storage_key);

CREATE INDEX supply_token_id_ts_storage_key_idx ON mkt.supply USING btree (token_id, ts, storage_key);

CREATE INDEX supply_ts_storage_key_idx ON mkt.supply USING btree (ts, storage_key);

CREATE INDEX unlocks_date_storage_key_idx ON mkt.unlocks USING btree (date, storage_key);

CREATE INDEX unlocks_observed_at_storage_key_idx ON mkt.unlocks USING btree (observed_at, storage_key);

CREATE INDEX unlocks_received_ts_storage_key_idx ON mkt.unlocks USING btree (received_ts, storage_key);

CREATE INDEX unlocks_recorded_at_storage_key_idx ON mkt.unlocks USING btree (recorded_at, storage_key);

CREATE INDEX unlocks_storage_key_idx ON mkt.unlocks USING btree (storage_key);

CREATE INDEX unlocks_token_id_date_storage_key_idx ON mkt.unlocks USING btree (token_id, date, storage_key);

CREATE INDEX unlocks_token_id_observed_at_storage_key_idx ON mkt.unlocks USING btree (token_id, observed_at, storage_key);

CREATE INDEX unlocks_token_id_received_ts_storage_key_idx ON mkt.unlocks USING btree (token_id, received_ts, storage_key);

CREATE INDEX unlocks_token_id_recorded_at_storage_key_idx ON mkt.unlocks USING btree (token_id, recorded_at, storage_key);

CREATE INDEX capture_tier_transitions_document ON ops.capture_tier_transitions USING gin (document);

CREATE INDEX capture_tier_transitions_time ON ops.capture_tier_transitions USING btree (effective_at, storage_key);

CREATE INDEX capture_tier_transitions_token_time ON ops.capture_tier_transitions USING btree (token_id, effective_at, storage_key);

CREATE INDEX jobs_terminal_age ON ops.jobs USING btree (finished_at) WHERE (status = ANY (ARRAY['DONE'::text, 'FAILED'::text, 'EXPIRED'::text]));

CREATE INDEX ops_agent_sessions_doc_idx ON ops.agent_sessions USING gin (document);

CREATE UNIQUE INDEX ops_agent_sessions_domain_id ON ops.agent_sessions USING btree (session_id);

CREATE INDEX ops_agent_sessions_event_id_idx ON ops.agent_sessions USING btree (event_id);

CREATE INDEX ops_agent_sessions_last_item_at_idx ON ops.agent_sessions USING btree (last_item_at);

CREATE INDEX ops_agent_sessions_opened_at_idx ON ops.agent_sessions USING btree (opened_at);

CREATE INDEX ops_agent_sessions_session_id_idx ON ops.agent_sessions USING btree (session_id);

CREATE INDEX ops_agent_sessions_status_idx ON ops.agent_sessions USING btree (status);

CREATE INDEX ops_jobs_claim ON ops.jobs USING btree (priority, run_after) WHERE (status = 'QUEUED'::text);

CREATE INDEX ops_jobs_doc_idx ON ops.jobs USING gin (document);

CREATE UNIQUE INDEX ops_jobs_domain_id ON ops.jobs USING btree (job_id);

CREATE INDEX ops_jobs_expires_at_idx ON ops.jobs USING btree (expires_at);

CREATE INDEX ops_jobs_job_id_idx ON ops.jobs USING btree (job_id);

CREATE INDEX ops_jobs_priority_idx ON ops.jobs USING btree (priority);

CREATE INDEX ops_jobs_run_after_idx ON ops.jobs USING btree (run_after);

CREATE INDEX ops_jobs_status_idx ON ops.jobs USING btree (status);

CREATE INDEX ops_llm_usage_at_idx ON ops.llm_usage USING btree (at);

CREATE INDEX ops_llm_usage_doc_idx ON ops.llm_usage USING gin (document);

CREATE INDEX ops_llm_usage_subject_id_idx ON ops.llm_usage USING btree (subject_id);

CREATE INDEX ops_source_checkpoints_doc_idx ON ops.source_checkpoints USING gin (document);

CREATE INDEX ops_source_checkpoints_source_id_idx ON ops.source_checkpoints USING btree (source_id);

CREATE INDEX ops_source_checkpoints_updated_at_idx ON ops.source_checkpoints USING btree (updated_at);

CREATE INDEX ops_source_escalations_doc_idx ON ops.source_escalations USING gin (document);

CREATE INDEX ops_source_escalations_expires_at_idx ON ops.source_escalations USING btree (expires_at);

CREATE INDEX ops_source_escalations_source_id_idx ON ops.source_escalations USING btree (source_id);

CREATE INDEX ops_tool_calls_answered_at_idx ON ops.tool_calls USING btree (answered_at);

CREATE INDEX ops_tool_calls_call_id_idx ON ops.tool_calls USING btree (call_id);

CREATE INDEX ops_tool_calls_doc_idx ON ops.tool_calls USING gin (document);

CREATE INDEX ops_tool_calls_requested_at_idx ON ops.tool_calls USING btree (requested_at);

CREATE UNIQUE INDEX ops_tool_calls_session_call ON ops.tool_calls USING btree (session_id, call_id);

CREATE INDEX ops_tool_calls_session_id_idx ON ops.tool_calls USING btree (session_id);

CREATE INDEX ops_tool_calls_status_idx ON ops.tool_calls USING btree (status);

CREATE INDEX request_budget_ledger_bucket_at_idx ON ops.request_budget_ledger USING btree (bucket, at);

CREATE INDEX scheduler_checkpoints_at ON ops.scheduler_checkpoints USING btree (at);

CREATE INDEX sessions_item_ref ON ops.agent_sessions USING btree (((document ->> 'item_id'::text)));

CREATE INDEX sessions_proposal_ref ON ops.agent_sessions USING btree (((document ->> 'proposal_id'::text)));

CREATE INDEX sessions_reservation_ref ON ops.agent_sessions USING btree (((document ->> 'reservation_id'::text)));

CREATE INDEX sessions_routing_latest ON ops.agent_sessions USING btree (last_item_at DESC, storage_key DESC);

CREATE INDEX out_alerts_alert_id_idx ON "out".alerts USING btree (alert_id);

CREATE INDEX out_alerts_doc_idx ON "out".alerts USING gin (document);

CREATE INDEX out_alerts_event_id_idx ON "out".alerts USING btree (event_id);

CREATE INDEX out_alerts_evidence_bundle_id_idx ON "out".alerts USING btree (evidence_bundle_id);

CREATE INDEX out_alerts_generated_at_idx ON "out".alerts USING btree (generated_at);

CREATE INDEX out_alerts_model_id_idx ON "out".alerts USING btree (model_id);

CREATE INDEX out_deliveries_alert_id_idx ON "out".deliveries USING btree (alert_id);

CREATE INDEX out_deliveries_at_idx ON "out".deliveries USING btree (at);

CREATE INDEX out_deliveries_doc_idx ON "out".deliveries USING gin (document);

CREATE INDEX out_deliveries_notice_id_idx ON "out".deliveries USING btree (notice_id);

CREATE INDEX out_deliveries_status_idx ON "out".deliveries USING btree (status);

CREATE INDEX out_evidence_bundles_bundle_id_idx ON "out".evidence_bundles USING btree (bundle_id);

CREATE INDEX out_evidence_bundles_cutoff_idx ON "out".evidence_bundles USING btree (cutoff);

CREATE INDEX out_evidence_bundles_doc_idx ON "out".evidence_bundles USING gin (document);

CREATE UNIQUE INDEX out_evidence_bundles_domain_id ON "out".evidence_bundles USING btree (bundle_id);

CREATE INDEX out_ops_notices_created_at_idx ON "out".ops_notices USING btree (created_at);

CREATE INDEX out_ops_notices_doc_idx ON "out".ops_notices USING gin (document);

CREATE UNIQUE INDEX out_ops_notices_domain_id ON "out".ops_notices USING btree (notice_id);

CREATE INDEX out_ops_notices_notice_id_idx ON "out".ops_notices USING btree (notice_id);

CREATE INDEX out_ops_notices_subject_id_idx ON "out".ops_notices USING btree (subject_id);

CREATE INDEX out_reaction_checkpoints_captured_at_idx ON "out".reaction_checkpoints USING btree (captured_at);

CREATE INDEX out_reaction_checkpoints_doc_idx ON "out".reaction_checkpoints USING gin (document);

CREATE INDEX out_reaction_checkpoints_event_id_idx ON "out".reaction_checkpoints USING btree (event_id);

CREATE INDEX out_reaction_checkpoints_rwp_id_idx ON "out".reaction_checkpoints USING btree (rwp_id);

CREATE INDEX out_reaction_checkpoints_token_id_idx ON "out".reaction_checkpoints USING btree (token_id);

CREATE INDEX qa_fixtures_cutoff_idx ON qa.fixtures USING btree (cutoff);

CREATE INDEX qa_fixtures_doc_idx ON qa.fixtures USING gin (document);

CREATE UNIQUE INDEX qa_fixtures_domain_id ON qa.fixtures USING btree (fixture_id);

CREATE INDEX qa_fixtures_fixture_id_idx ON qa.fixtures USING btree (fixture_id);

CREATE INDEX qa_replay_runs_at_idx ON qa.replay_runs USING btree (at);

CREATE INDEX qa_replay_runs_doc_idx ON qa.replay_runs USING gin (document);

CREATE INDEX qa_replay_runs_fixture_id_idx ON qa.replay_runs USING btree (fixture_id);

CREATE INDEX qa_replay_runs_run_id_idx ON qa.replay_runs USING btree (run_id);

CREATE INDEX qa_reviews_at_idx ON qa.reviews USING btree (at);

CREATE INDEX qa_reviews_doc_idx ON qa.reviews USING gin (document);

CREATE UNIQUE INDEX qa_reviews_domain_id ON qa.reviews USING btree (review_id);

CREATE INDEX qa_reviews_review_id_idx ON qa.reviews USING btree (review_id);

CREATE INDEX reg_dependency_map_doc_idx ON reg.dependency_map USING gin (document);

CREATE INDEX reg_dependency_map_edge_id_idx ON reg.dependency_map USING btree (edge_id);

CREATE INDEX reg_dependency_map_entity_id_idx ON reg.dependency_map USING btree (entity_id);

CREATE INDEX reg_dependency_map_token_id_idx ON reg.dependency_map USING btree (token_id);

CREATE INDEX reg_entities_doc_idx ON reg.entities USING gin (document);

CREATE UNIQUE INDEX reg_entities_domain_id ON reg.entities USING btree (entity_id);

CREATE INDEX reg_entities_entity_id_idx ON reg.entities USING btree (entity_id);

CREATE INDEX reg_entity_aliases_doc_idx ON reg.entity_aliases USING gin (document);

CREATE INDEX reg_entity_aliases_entity_id_idx ON reg.entity_aliases USING btree (entity_id);

CREATE INDEX reg_exchange_addresses_doc_idx ON reg.exchange_addresses USING gin (document);

CREATE INDEX reg_exchange_addresses_entity_id_idx ON reg.exchange_addresses USING btree (entity_id);

CREATE INDEX reg_instruments_doc_idx ON reg.instruments USING gin (document);

CREATE UNIQUE INDEX reg_instruments_domain_id ON reg.instruments USING btree (instrument_id);

CREATE INDEX reg_instruments_instrument_id_idx ON reg.instruments USING btree (instrument_id);

CREATE INDEX reg_instruments_token_id_idx ON reg.instruments USING btree (token_id);

CREATE INDEX reg_metric_semantics_doc_idx ON reg.metric_semantics USING gin (document);

CREATE UNIQUE INDEX reg_metric_semantics_domain_id ON reg.metric_semantics USING btree (metric_key);

CREATE INDEX reg_sector_baskets_basket_id_idx ON reg.sector_baskets USING btree (basket_id);

CREATE INDEX reg_sector_baskets_doc_idx ON reg.sector_baskets USING gin (document);

CREATE INDEX reg_sources_doc_idx ON reg.sources USING gin (document);

CREATE UNIQUE INDEX reg_sources_domain_id ON reg.sources USING btree (source_id);

CREATE INDEX reg_sources_source_id_idx ON reg.sources USING btree (source_id);

CREATE INDEX reg_tokens_doc_idx ON reg.tokens USING gin (document);

CREATE UNIQUE INDEX reg_tokens_domain_id ON reg.tokens USING btree (token_id);

CREATE INDEX reg_tokens_token_id_idx ON reg.tokens USING btree (token_id);

CREATE INDEX scout_expansion_proposals_decided_at_idx ON scout.expansion_proposals USING btree (decided_at);

CREATE INDEX scout_expansion_proposals_doc_idx ON scout.expansion_proposals USING gin (document);

CREATE UNIQUE INDEX scout_expansion_proposals_domain_id ON scout.expansion_proposals USING btree (prp_id);

CREATE INDEX scout_expansion_proposals_prp_id_idx ON scout.expansion_proposals USING btree (prp_id);

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON ana.calls FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON ana.contradictions FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON ana.decisions FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON ana.market_truth FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON ana.outlooks FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON ana.watch_conditions FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER append_only BEFORE DELETE OR UPDATE ON evt.state_changes FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON evt.beneficiary_edges FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON evt.beneficiary_maps FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON evt.capture_chains FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON evt.claims FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON evt.ems_assessments FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON evt.event_entities FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON evt.event_observations FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON evt.events FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON ingest.source_items FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER notify_capture AFTER INSERT OR UPDATE ON mkt.backfill_runs FOR EACH STATEMENT EXECUTE FUNCTION ops.notify_capture();

CREATE TRIGGER notify_capture AFTER INSERT ON mkt.capture_requests FOR EACH STATEMENT EXECUTE FUNCTION ops.notify_capture();

CREATE TRIGGER notify_capture AFTER INSERT OR DELETE OR UPDATE ON mkt.hot_tokens FOR EACH STATEMENT EXECUTE FUNCTION ops.notify_capture();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON ops.agent_sessions FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON ops.jobs FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON ops.llm_usage FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON ops.scheduler_checkpoints FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON ops.source_checkpoints FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON ops.source_escalations FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON ops.tool_calls FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER job_lifecycle BEFORE INSERT OR UPDATE ON ops.jobs FOR EACH ROW EXECUTE FUNCTION ops.stamp_job_lifecycle();

CREATE TRIGGER append_only BEFORE DELETE OR UPDATE ON "out".alerts FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();

CREATE TRIGGER append_only BEFORE DELETE OR UPDATE ON "out".deliveries FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();

CREATE TRIGGER append_only BEFORE DELETE OR UPDATE ON "out".evidence_bundles FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();

CREATE TRIGGER append_only BEFORE DELETE OR UPDATE ON "out".ops_notices FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();

CREATE TRIGGER append_only BEFORE DELETE OR UPDATE ON "out".reaction_checkpoints FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();

CREATE TRIGGER append_only BEFORE DELETE OR UPDATE ON qa.reviews FOR EACH ROW EXECUTE FUNCTION ops.forbid_change();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON qa.fixtures FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON qa.replay_runs FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON reg.dependency_map FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON reg.entities FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON reg.entity_aliases FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON reg.exchange_addresses FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON reg.instruments FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON reg.metric_semantics FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON reg.sector_baskets FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON reg.sources FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON reg.tokens FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

CREATE TRIGGER immutable_record BEFORE DELETE OR UPDATE ON scout.expansion_proposals FOR EACH ROW EXECUTE FUNCTION ops.protect_immutable();

ALTER TABLE ONLY ana.calls
    ADD CONSTRAINT call_event FOREIGN KEY (event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY ana.decisions
    ADD CONSTRAINT decision_event FOREIGN KEY (event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY ana.decisions
    ADD CONSTRAINT decision_truth FOREIGN KEY (mta_id) REFERENCES ana.market_truth(mta_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY ana.market_truth
    ADD CONSTRAINT truth_event FOREIGN KEY (event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY evt.ems_assessments
    ADD CONSTRAINT assessment_event FOREIGN KEY (event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY evt.state_changes
    ADD CONSTRAINT change_event FOREIGN KEY (event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY evt.claims
    ADD CONSTRAINT claim_observation FOREIGN KEY (obs_id) REFERENCES evt.event_observations(obs_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY evt.events
    ADD CONSTRAINT event_merge_target FOREIGN KEY (merged_into) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY evt.event_observations
    ADD CONSTRAINT observation_event FOREIGN KEY (event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY evt.event_observations
    ADD CONSTRAINT observation_item FOREIGN KEY (item_id) REFERENCES ingest.source_items(item_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY ops.agent_sessions
    ADD CONSTRAINT session_event FOREIGN KEY (event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY ops.agent_sessions
    ADD CONSTRAINT session_origin FOREIGN KEY (opened_by_item) REFERENCES ingest.source_items(item_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY ops.tool_calls
    ADD CONSTRAINT tool_session FOREIGN KEY (session_id) REFERENCES ops.agent_sessions(session_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY "out".alerts
    ADD CONSTRAINT alert_event FOREIGN KEY (event_id) REFERENCES evt.events(event_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY "out".deliveries
    ADD CONSTRAINT delivery_alert FOREIGN KEY (alert_id, version) REFERENCES "out".alerts(alert_id, version) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY "out".deliveries
    ADD CONSTRAINT delivery_notice FOREIGN KEY (notice_id) REFERENCES "out".ops_notices(notice_id) DEFERRABLE INITIALLY DEFERRED;
