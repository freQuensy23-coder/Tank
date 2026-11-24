WITH history AS (
    SELECT i, '2025-01-01 00:00Z'::timestamptz + i * interval '1 minute' AS cutoff
    FROM generate_series(1, 100000) AS series(i)
), jobs AS (
    SELECT 'minute-history-' || i AS id, cutoff,
           to_char(cutoff AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS stamp
    FROM history
)
INSERT INTO ops.jobs (storage_key, job_id, type, priority, idem_key, status, attempts, max_attempts,
                      created_at, run_after, expires_at, finished_at, document)
SELECT id, id, 'EVAL_CONDITIONS', 2, stamp, 'DONE', 1, 2, cutoff, cutoff, cutoff + interval '6 minutes', cutoff,
       jsonb_build_object('payload', jsonb_build_object('cutoff', stamp, 'cutoff_prec', 'BUCKET(1m)'))
FROM jobs;

ANALYZE ops.jobs;