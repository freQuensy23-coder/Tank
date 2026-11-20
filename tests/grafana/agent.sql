\set ON_ERROR_STOP on
BEGIN;
DO $$ BEGIN
    IF current_database() NOT LIKE 'tank\_test\_%' THEN
        RAISE EXCEPTION 'Requires a disposable migrated tank_test_ database';
    END IF;
END $$;
SET LOCAL plpgsql.check_asserts = on;

INSERT INTO ops.agent_sessions (storage_key, session_id, opened_at, status, last_event_seq, agent_version, document)
SELECT id, id, '2026-01-01 00:00Z', status, 0, 'fixture',
       '{"proposal_id":"fixture","last_item_at_reason":"no source item"}'::jsonb || evidence
FROM (VALUES
    ('ok','OPEN','{"message_log":[{"message_id":"m","remote_event_id":"i","turn_status":"DONE"},{"message_id":"old","turn_status":"FAILED"}],"retired_message_ids":["old"],"verified_native_completion":{"complete":true,"accepted_input_ids":["i"]}}'::jsonb),
    ('stale','OPEN','{"message_log":[{"message_id":"m","remote_event_id":"new","turn_status":"DONE"}],"verified_native_completion":{"complete":true,"accepted_input_ids":["old"]}}'),
    ('working','OPEN','{"message_log":[{"message_id":"m","remote_event_id":"i","turn_status":"RUNNING"}]}'),
    ('failed','FAILED','{}'), ('terminated','ENDED','{"message_log":[{"message_id":"m","turn_status":"FAILED"}]}'),
    ('incomplete','ENDED','{"agent_incomplete":true}'), ('archived','ENDED','{}'), ('empty','OPEN','{}')
) AS s(id,status,evidence);

INSERT INTO ops.tool_calls (storage_key, call_id, session_id, tool, status, requested_at, answered_at, document)
SELECT id, id, sid, 'get_decision', state, '2026-01-01 00:00Z', '2026-01-01 00:00Z'::timestamptz + delay,
       jsonb_build_object('input','{}'::jsonb,'result',answer)
FROM (VALUES
    ('t1','ok','DONE', interval '1s','{"action":"raw non-action"}'::jsonb),
    ('t2','ok','DONE','2s','{"action":"START","tracking_ended":true}'),
    ('t3','ok','ERROR','3s','{"action":"EXIT"}'), ('t4','ok','DONE','-1s','{"action":"EXIT"}'),
    ('t5','ok','DONE','40s','{"action":"WATCH_TO_START"}'), ('t6','ok','DONE','40s','{"action":"ADD"}'),
    ('t7','working','PENDING',NULL,NULL), ('t8','archived','PENDING',NULL,NULL)
) AS t(id,sid,state,delay,answer);

INSERT INTO ops.tool_calls (storage_key, call_id, session_id, tool, status, requested_at, answered_at, document)
SELECT id, id, sid, tool, state, '2026-01-01 00:00Z', '2026-01-01 00:00Z'::timestamptz + delay,
       jsonb_build_object('input','{}'::jsonb,'result',answer)
FROM (VALUES
    ('e1','ok','record_event','DONE',interval '1s','{"event_id":"evt"}'::jsonb),
    ('e2','ok','set_assessment','DONE','2s','{"class":"MAJOR"}'),
    ('e3','ok','set_assessment','DONE','3s','{"class":"LOW"}'),
    ('e4','working','record_event','ERROR','4s','{"is_event":false}'),
    ('e5','archived','record_event','DONE','5s','{"is_event":false}')
) AS t(id,sid,tool,state,delay,answer);

INSERT INTO ops.jobs (storage_key, job_id, type, priority, idem_key, status, attempts, max_attempts,
                      created_at, run_after, finished_at, expires_at, document)
SELECT id, id, kind, 1, key, state, 2, 3, '2026-01-01 00:00Z', '2026-01-01 00:00Z',
       '2026-01-01 00:00Z'::timestamptz + delay, '2026-01-01 01:00Z', doc
FROM (VALUES
    ('j1','EVAL_CONDITIONS','2026-01-01T00:00:00Z','DONE',interval '23s','{"payload":{"cutoff":"2026-01-01T00:00:00Z","cutoff_prec":"BUCKET(1m)"},"result":{"deferred":[{"reason":"BATCH_BUDGET"}]}}'::jsonb),
    ('j2','EVAL_CONDITIONS','2026-01-01T00:01:00Z','EXPIRED','-5s','{"payload":{"cutoff":"2026-01-01T00:01:00Z","cutoff_prec":"BUCKET(1m)"}}'),
    ('j3','EVAL_CONDITIONS','manual','DONE','1s','{"payload":{"cutoff":"other","cutoff_prec":"BUCKET(1m)"}}'),
    ('j4','AGENT_MESSAGE','e:CONDITION_FIRED:t','FAILED','34s','{"payload":{"kind":"CONDITION_FIRED","event_id":"e","condition":{"cond_id":"t","event_id":"e","status":"FIRED","agent_defined":true,"expr":{"review_at":"2026-01-01T00:00:00Z"}}}}'),
    ('j5','AGENT_MESSAGE','e:CONDITION_FIRED:m','QUEUED',NULL,'{"payload":{"kind":"CONDITION_FIRED","event_id":"e","condition":{"cond_id":"m","event_id":"e","status":"FIRED","agent_defined":true,"expr":{"all":[]}}}}'),
    ('j6','AGENT_MESSAGE','e:CONDITION_FIRED:a','DONE','23s','{"payload":{"kind":"CONDITION_FIRED","event_id":"e","condition":{"cond_id":"a","event_id":"e","status":"FIRED"}}}'),
    ('j7','AGENT_MESSAGE','e:CONDITION_FIRED:b','DONE','23s','{"payload":{"kind":"CONDITION_FIRED","event_id":"e","condition":{"cond_id":"c","event_id":"e","status":"FIRED","agent_defined":true}}}')
) AS j(id,kind,key,state,delay,doc);

SET LOCAL ROLE tank_grafana_reader;
DO $$ BEGIN
    ASSERT (SELECT jsonb_object_agg(outcome, n) = '{"Execution success":1,"Execution error":2,"Incomplete workflow":1,"Pending / running":1,"Unknown":3}'
            FROM (SELECT outcome, count(*) n FROM tank_metrics.agent_run_outcomes GROUP BY outcome) counts);
    ASSERT (SELECT count(*) = 8 AND count(action) = 1 AND min(action) = 'WATCH_TO_START'
            AND count(seconds_to_verdict) = 1 AND min(seconds_to_verdict) = 40
            AND count(*) FILTER (WHERE decision_state = 'Decision requested (pending)') = 1
            AND count(*) FILTER (WHERE decision_state = 'No recorded decision') = 6 FROM tank_metrics.agent_first_verdict);
    ASSERT (SELECT count(*) = 8 AND count(classification) = 2 AND count(seconds_to_classification) = 2
            AND count(*) FILTER (WHERE classification = 'MAJOR' AND seconds_to_classification = 2) = 1
            AND count(*) FILTER (WHERE classification = 'NO_EVENT' AND seconds_to_classification = 5) = 1
            FROM tank_metrics.agent_first_classification);
    ASSERT (SELECT count(*) = 4 AND count(seconds_in_queue_lifecycle) = 2 AND count(deferred_conditions) = 1
            FROM tank_metrics.agent_scheduled_jobs);
    ASSERT EXISTS (SELECT FROM tank_metrics.agent_scheduled_jobs WHERE job_kind = 'Tank minute condition check'
                   AND status = 'DONE' AND seconds_in_queue_lifecycle = 23 AND deferred_conditions = 1);
    ASSERT EXISTS (SELECT FROM tank_metrics.agent_scheduled_jobs WHERE job_kind = 'Agent timed review notification'
                   AND status = 'FAILED' AND seconds_in_queue_lifecycle = 34);
    ASSERT EXISTS (SELECT FROM tank_metrics.agent_scheduled_jobs WHERE job_kind = 'Agent metric watch notification' AND status = 'QUEUED');
    ASSERT NOT has_schema_privilege(current_user, 'ops', 'USAGE');
    ASSERT NOT has_table_privilege(current_user, 'tank_metrics.agent_run_outcomes', 'INSERT, UPDATE, DELETE');
END $$;
ROLLBACK;
