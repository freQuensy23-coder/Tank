\set ON_ERROR_STOP on
BEGIN;
DO $$ BEGIN
    IF current_database() NOT LIKE 'tank\_test\_%' THEN
        RAISE EXCEPTION 'Requires a disposable migrated tank_test_ database';
    END IF;
END $$;
SET LOCAL plpgsql.check_asserts = on;
INSERT INTO ops.jobs (storage_key,job_id,type,priority,idem_key,status,attempts,max_attempts,created_at,run_after,expires_at,document)
SELECT id,id,'AGENT_MESSAGE',1,id,'QUEUED',0,3,now()-interval '2 minutes',now()+delay,now()+expiry,
       jsonb_build_object('payload','{}'::jsonb) || evidence
FROM (VALUES
 ('capped', interval '5s', interval '1 hour', jsonb_build_object('slot_waiting_since',now()-interval '60s')),
 ('due', interval '-10s', interval '1 hour', '{}'::jsonb),
 ('future', interval '60s', interval '1 hour', '{}'::jsonb),
 ('expired', interval '5s', interval '-1s', jsonb_build_object('slot_waiting_since',now()-interval '90s'))
) AS j(id,delay,expiry,evidence);
DO $$ DECLARE q record; BEGIN
    SELECT * INTO STRICT q FROM tank_metrics.queue_now WHERE job_type='AGENT_MESSAGE';
    ASSERT (q.due,q.scheduled,q.awaiting_cleanup,q.running,q.oldest_due_seconds)=(2,1,1,0,60);
END $$;
ROLLBACK;
