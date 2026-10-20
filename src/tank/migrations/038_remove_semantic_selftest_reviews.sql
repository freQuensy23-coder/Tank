-- qa.reviews holds review labels (TDD §2.8 ReviewRecord: subject is an alert_id or
-- event_id). Rows with subject METRIC_SEMANTICS are leftovers of the removed semantic
-- self-test: Tank trusts the semantics registry (03 §2.10). Lead ruling 2026-10-20:
-- delete them; the verified backup of 2026-10-20 covers rollback. Production count
-- before: 4705, all label VERIFIED.
DO $$
DECLARE
    unexpected bigint;
    removed bigint;
BEGIN
    SELECT count(*) INTO unexpected FROM qa.reviews
     WHERE subject = 'METRIC_SEMANTICS' AND label IS DISTINCT FROM 'VERIFIED';
    IF unexpected > 0 THEN
        RAISE EXCEPTION '038: % METRIC_SEMANTICS rows have an unexpected label', unexpected;
    END IF;
    ALTER TABLE qa.reviews DISABLE TRIGGER append_only;
    DELETE FROM qa.reviews WHERE subject = 'METRIC_SEMANTICS' AND label = 'VERIFIED';
    GET DIAGNOSTICS removed = ROW_COUNT;
    ALTER TABLE qa.reviews ENABLE TRIGGER append_only;
    RAISE NOTICE '038: removed % METRIC_SEMANTICS rows from qa.reviews', removed;
END $$;
