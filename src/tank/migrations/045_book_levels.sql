-- Order-book levels for Market Truth slippage (walk_book at a cutoff), out of
-- mkt.book_summaries: the 03 §2 BookSummary carries no levels, and full bids/asks on every
-- 10 s summary were 10 KB of an 11 KB row (prod 2026-10-21: ~2.9 GB/day). One row per
-- summary that had levels, kept for the recent window a cutoff can still ask for
-- (BOOK_LEVELS_KEEP); older levels live only in the raw book archive.
CREATE TABLE IF NOT EXISTS mkt.book_levels (
    instrument_id text NOT NULL,
    exchange_ts timestamptz NOT NULL,
    bids jsonb NOT NULL,
    asks jsonb NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, exchange_ts)
);
