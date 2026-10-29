ALTER TABLE out.reaction_checkpoints
    ADD COLUMN metadata jsonb GENERATED ALWAYS AS (document - 'market_truth') STORED;
