CREATE TABLE out.ops_cards (
    storage_key text PRIMARY KEY,
    document jsonb NOT NULL,
    immutable boolean NOT NULL DEFAULT false,
    item_id text NOT NULL,
    chat_id text NOT NULL,
    message_ref text,
    UNIQUE (chat_id, item_id)
);
