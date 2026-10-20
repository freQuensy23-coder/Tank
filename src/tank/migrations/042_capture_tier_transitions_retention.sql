-- ops.capture_tier_transitions records when a token's capture tier (BASE/HOT) changed.
-- Market Truth reads it for the cadence in force at a cutoff; it describes the market
-- series, so it shares their retention (HLD §7.1: 90 days, MAJOR+ days kept), which
-- the append-only trigger and immutable rows would make the nightly prune fail on.
DROP TRIGGER IF EXISTS append_only ON ops.capture_tier_transitions;
UPDATE ops.capture_tier_transitions SET immutable = false WHERE immutable;
