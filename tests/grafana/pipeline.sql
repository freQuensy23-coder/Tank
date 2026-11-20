\set ON_ERROR_STOP on
BEGIN;
DO $$ BEGIN
    IF current_database() NOT LIKE 'tank\_test\_%' THEN
        RAISE EXCEPTION 'Requires a disposable migrated tank_test_ database';
    END IF;
END $$;
SET LOCAL plpgsql.check_asserts = on;

INSERT INTO reg.sources (storage_key, source_id, mode, document)
VALUES ('S01', 'S01', 'REST', '{"feeds":[{"kind":"rss"},{"kind":"json"},{"kind":"x"}]}');
INSERT INTO ops.source_checkpoints (storage_key, source_id, document) VALUES
    ('S01:0', 'S01', '{"reads":{"ok":11,"failed":2},"detail_reads":{"ok":90,"failed":8}}'),
    ('S01:1', 'S01', '{"reads":{"ok":4,"failed":0}}'),
    ('unrelated:S01', 'S01', '{"reads":{"ok":25,"failed":17}}');

INSERT INTO ops.agent_sessions (storage_key, session_id, opened_at, status, last_event_seq, agent_version, document)
SELECT id, id, '2026-01-01 00:00Z', status, 0, 'fixture',
       '{"proposal_id":"test","last_item_at_reason":"no source item"}'::jsonb || failure
FROM (VALUES ('s1', 'ENDED', '{}'::jsonb),
             ('s2', 'FAILED', '{"terminal_failure":{"error":{"type":"model_rate_limited_error"}}}'),
             ('s3', 'FAILED', '{"terminal_failure":{"message":"not an error class"}}')) AS s(id,status,failure);

INSERT INTO ops.tool_calls (storage_key, call_id, session_id, tool, status, requested_at, answered_at, document)
SELECT id, id, 's1', tool, status, '2026-01-01 00:00Z', at::timestamptz,
       jsonb_build_object('input', '{}'::jsonb, 'result', result)
FROM (VALUES ('t0','get_decision','DONE','2025-12-31 23:59Z','{"action":"EXIT"}'::jsonb),
             ('t1','get_decision','ERROR','2026-01-01 00:01Z','{"action":"EXIT"}'),
             ('t2','get_decision','DONE','2026-01-01 00:02Z','{"action":"START"}'),
             ('t3','get_decision','DONE','2026-01-01 00:04Z','{"action":"HOLD"}'),
             ('m1','schedule_milestone','DONE','2026-01-01 00:05Z','{"job_id":"job1"}'),
             ('m2','schedule_milestone','DONE','2026-01-01 00:06Z','{"job_id":"job1"}'),
             ('m3','schedule_milestone','REFUSED','2026-01-01 00:07Z','{"job_id":"job2"}')) AS t(id,tool,status,at,result);

INSERT INTO ops.llm_usage (storage_key, document, call, at, subject_id, model, effort, input_tokens,
                           cache_read_tokens, cache_write_tokens, output_tokens, cost_usd, web_searches, stop_reason)
SELECT id, '{}', kind, '2026-01-01 00:01Z', 's1', 'fixture', 'low', 1, 0, 0, 1, 0, 0, 'stop'
FROM (VALUES ('span1','AGENT'),('span2','AGENT'),('search','WEB_SEARCH'),('class','FILTER'),('route','ROUTE')) AS u(id,kind);

INSERT INTO ana.watch_conditions (storage_key, cond_id, status, fired_at, document) VALUES
    ('w1','w1','ARMED',NULL,'{"agent_defined":true,"expr":{"metric":"event.state"}}'),
    ('w2','w2','FIRED','2026-01-01 00:10Z','{"agent_defined":true,"expr":{"review_at":"2026-01-01T00:10:00Z"}}'),
    ('w3','w3','CANCELLED',NULL,'{"session_id":"s1","cancelled_by":"AGENT"}'),
    ('w4','w4','FIRED','2026-01-01 00:11Z','{"agent_defined":true,"consumed_by":"missing","expr":{"metric":"event.state"}}');

INSERT INTO ops.jobs (storage_key, job_id, type, priority, idem_key, status, attempts, max_attempts,
                      created_at, run_after, expires_at, document)
SELECT id, id, 'MILESTONE_CHECK', 1, id, state, 1, 3, '2026-01-01 00:05Z', '2026-01-01 00:20Z',
       '2026-01-01 01:20Z', '{"payload":{}}'
FROM (VALUES ('job1','DONE'),('job2','FAILED'),('job3','EXPIRED')) AS j(id,state);

SET LOCAL ROLE tank_grafana_reader;
DO $$ BEGIN
    ASSERT (SELECT count(*) = 3 AND sum(successful) = 15 AND sum(failed) = 2 FROM tank_metrics.source_pulls);
    ASSERT EXISTS (SELECT FROM tank_metrics.source_pulls WHERE feed_kind = 'x' AND successful IS NULL AND failed IS NULL);
    ASSERT EXISTS (SELECT FROM tank_metrics.source_pulls WHERE feed_kind = 'json' AND failed = 0);
    ASSERT (SELECT count(*) = 3 FROM tank_metrics.agent_runs);
    ASSERT (SELECT array_agg(error_class ORDER BY error_class COLLATE "C") = ARRAY['Unclassified','model_rate_limited_error']
            FROM tank_metrics.agent_runs WHERE status = 'FAILED');
    ASSERT (SELECT count(*) = 2 FROM tank_metrics.agent_model_turns);
    ASSERT (SELECT count(*) = 3 AND count(action) = 1 AND min(action) = 'START' AND min(seconds_to_verdict) = 120
            FROM tank_metrics.agent_first_verdict);
    ASSERT (SELECT count(*) = 3 FROM tank_metrics.agent_watches);
    ASSERT EXISTS (SELECT FROM tank_metrics.agent_watches WHERE watch_kind = 'Timed review' AND status = 'FIRED'
                   AND fired_at IS NOT NULL AND consuming_action = 'No linked decision');
    ASSERT EXISTS (SELECT FROM tank_metrics.agent_watches WHERE consuming_action = 'Linked decision unavailable');
    ASSERT (SELECT array_agg(status) = ARRAY['DONE'] FROM tank_metrics.agent_milestones);
    ASSERT NOT has_schema_privilege(current_user, 'ops', 'USAGE');
    ASSERT NOT has_table_privilege(current_user, 'tank_metrics.agent_model_turns', 'INSERT,UPDATE,DELETE');
END $$;
ROLLBACK;
