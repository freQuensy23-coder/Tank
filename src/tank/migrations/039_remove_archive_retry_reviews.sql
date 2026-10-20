-- qa.reviews holds review labels only (TDD §2.8 ReviewRecord). Rows with subject
-- ARCHIVE_RETRY were Binance archive retry state written by market/archive.py. Lead
-- ruling 2026-10-20 18:30: the filesystem and the imported rows are the only state, so
-- a rejected archive file is fetched again by the next daily bootstrap pass or backfill.
-- The verified backup of 2026-10-20 covers rollback. Production count on 2026-10-20 18:55 UTC: 3,625.
DO $$
DECLARE
    removed bigint;
BEGIN
    ALTER TABLE qa.reviews DISABLE TRIGGER append_only;
    DELETE FROM qa.reviews WHERE subject = 'ARCHIVE_RETRY';
    GET DIAGNOSTICS removed = ROW_COUNT;
    ALTER TABLE qa.reviews ENABLE TRIGGER append_only;
    RAISE NOTICE '039: removed % ARCHIVE_RETRY rows from qa.reviews', removed;
END $$;
