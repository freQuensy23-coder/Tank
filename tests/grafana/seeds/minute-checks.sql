WITH cases(minute, state, attempts, kind, precision, key_prefix) AS (VALUES
    (0, 'DONE', 2, 'EVAL_CONDITIONS', 'BUCKET(1m)', ''),
    (1, 'DONE', 1, 'EVAL_CONDITIONS', 'BUCKET(1m)', ''),
    (2, 'FAILED', 2, 'EVAL_CONDITIONS', 'BUCKET(1m)', ''),
    (3, 'EXPIRED', 0, 'EVAL_CONDITIONS', 'BUCKET(1m)', ''),
    (4, 'QUEUED', 0, 'EVAL_CONDITIONS', 'BUCKET(1m)', ''),
    (5, 'DONE', 1, 'AGENT_MESSAGE', 'BUCKET(1m)', ''),
    (6, 'DONE', 1, 'EVAL_CONDITIONS', 'EXACT', ''),
    (7, 'DONE', 1, 'EVAL_CONDITIONS', 'BUCKET(1m)', 'manual:'),
    (10, 'RUNNING', 1, 'EVAL_CONDITIONS', 'BUCKET(1m)', ''),
    (11, 'EXPIRED', 1, 'EVAL_CONDITIONS', 'BUCKET(1m)', ''),
    (12, 'DONE', 1, 'EVAL_CONDITIONS', 'BUCKET(1m)', '')
), schedule AS (
    SELECT *, '2026-01-01 00:00Z'::timestamptz + minute * interval '1 minute' AS cutoff,
           'minute-fixture-' || minute AS id FROM cases
), jobs AS (
    SELECT *, to_char(cutoff AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS stamp FROM schedule
)
INSERT INTO ops.jobs (storage_key, job_id, type, priority, idem_key, status, attempts, max_attempts,
                      created_at, run_after, expires_at, finished_at, document)
SELECT id, id, kind, 2, key_prefix || stamp, state, attempts, 2,
       cutoff + interval '70 seconds', cutoff + interval '90 seconds', cutoff + interval '6 minutes',
       CASE WHEN state = 'EXPIRED' THEN cutoff + interval '7 minutes'
            WHEN state IN ('DONE', 'FAILED') THEN cutoff + interval '2 minutes' END,
       jsonb_build_object('payload', jsonb_build_object('cutoff', stamp, 'cutoff_prec', precision)) ||
       CASE WHEN minute = 0 THEN '{"result":{"deferred":[{"reason":"BATCH_BUDGET"}]}}'::jsonb ELSE '{}'::jsonb END
FROM jobs;
