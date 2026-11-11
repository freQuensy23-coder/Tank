CREATE TABLE reg.token_context (
    storage_key text PRIMARY KEY,
    document jsonb NOT NULL,
    immutable boolean NOT NULL DEFAULT false,
    token_id text NOT NULL UNIQUE,
    revision char(64) NOT NULL,
    updated_at timestamptz
);

CREATE TABLE reg.token_context_proposals (
    storage_key text PRIMARY KEY,
    document jsonb NOT NULL,
    immutable boolean NOT NULL DEFAULT false,
    proposal_id text NOT NULL UNIQUE,
    token_id text NOT NULL,
    status text NOT NULL,
    base_revision char(64) NOT NULL,
    created_at timestamptz
);

CREATE INDEX token_context_proposals_token_status_idx
    ON reg.token_context_proposals(token_id, status);
