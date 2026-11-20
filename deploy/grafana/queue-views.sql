CREATE OR REPLACE VIEW tank_metrics.queue_now AS
SELECT type AS job_type,
       count(*) FILTER (WHERE status = 'QUEUED' AND run_after <= now() AND (expires_at IS NULL OR expires_at > now()) AND attempts < max_attempts) AS due,
       count(*) FILTER (WHERE status = 'RUNNING') AS running,
       count(*) FILTER (WHERE status = 'QUEUED' AND run_after > now() AND (expires_at IS NULL OR expires_at > now()) AND attempts < max_attempts) AS scheduled,
       count(*) FILTER (WHERE status = 'QUEUED' AND (expires_at <= now() OR attempts >= max_attempts)) AS awaiting_cleanup,
       extract(epoch FROM now() - min(run_after) FILTER (WHERE status = 'QUEUED' AND run_after <= now() AND (expires_at IS NULL OR expires_at > now()) AND attempts < max_attempts)) AS oldest_due_seconds
FROM ops.jobs WHERE status IN ('QUEUED', 'RUNNING') GROUP BY type;

CREATE OR REPLACE VIEW tank_metrics.queue_terminal AS
SELECT finished_at, type AS job_type,
       CASE WHEN status = 'FAILED' THEN 'Failed'
            WHEN attempts = 0 THEN 'Expired before first attempt'
            ELSE 'Expired after attempt' END AS outcome
FROM ops.jobs WHERE status IN ('FAILED', 'EXPIRED');
