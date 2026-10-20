-- Late-receipt unlock rows of fixture replays (lead rulings F-02/F-04, 2026-10-20) keep
-- the schedule's own clock and evidence mode, which replay and get_token_facts use for
-- visibility. Live collector rows leave them null. *_prec_reason is the precision
-- companion every typed market timestamp carries.
ALTER TABLE mkt.unlocks
    ADD COLUMN IF NOT EXISTS evidence_mode text,
    ADD COLUMN IF NOT EXISTS schedule_as_of timestamptz,
    ADD COLUMN IF NOT EXISTS schedule_as_of_prec text,
    ADD COLUMN IF NOT EXISTS schedule_as_of_prec_reason text;
