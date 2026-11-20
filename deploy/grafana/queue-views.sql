CREATE OR REPLACE VIEW tank_metrics.queue_now AS
WITH active AS (
    SELECT type, status, run_after,
           (expires_at IS NULL OR expires_at > now()) AND attempts < max_attempts AS live,
           CASE WHEN document->>'slot_admitted' IS DISTINCT FROM 'true'
                THEN nullif(document->>'slot_waiting_since', '')::timestamptz END AS slot_wait
    FROM ops.jobs WHERE status IN ('QUEUED', 'RUNNING')
), classified AS (
    SELECT *, CASE WHEN status = 'RUNNING' THEN 'running'
                   WHEN NOT live THEN 'cleanup'
                   WHEN run_after <= now() OR slot_wait <= now() THEN 'waiting'
                   ELSE 'scheduled' END AS state
    FROM active
)
SELECT type AS job_type,
       count(*) FILTER (WHERE state = 'waiting') AS due,
       count(*) FILTER (WHERE state = 'running') AS running,
       count(*) FILTER (WHERE state = 'scheduled') AS scheduled,
       count(*) FILTER (WHERE state = 'cleanup') AS awaiting_cleanup,
       extract(epoch FROM now() - min(least(slot_wait, run_after)) FILTER (WHERE state = 'waiting')) AS oldest_due_seconds
FROM classified GROUP BY type;

CREATE OR REPLACE VIEW tank_metrics.queue_terminal AS
SELECT finished_at, type AS job_type,
       CASE WHEN status = 'FAILED' THEN 'Failed'
            WHEN attempts = 0 THEN 'Expired before first attempt'
            ELSE 'Expired after attempt' END AS outcome
FROM ops.jobs WHERE status IN ('FAILED', 'EXPIRED');
