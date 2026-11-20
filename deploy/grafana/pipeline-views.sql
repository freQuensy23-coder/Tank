CREATE OR REPLACE VIEW tank_metrics.source_pulls AS
SELECT coalesce(s.mode, 'Unknown') AS source_mode,
       lower(coalesce(feed.value ->> 'kind', feed.value ->> 'format', s.mode, 'Unknown')) AS feed_kind,
       CASE WHEN c.document #>> '{reads,ok}' ~ '^[0-9]+$'
            THEN (c.document #>> '{reads,ok}')::numeric END AS successful,
       CASE WHEN c.document #>> '{reads,failed}' ~ '^[0-9]+$'
            THEN (c.document #>> '{reads,failed}')::numeric END AS failed
FROM reg.sources s
CROSS JOIN LATERAL jsonb_array_elements(s.document -> 'feeds') WITH ORDINALITY AS feed(value, n)
LEFT JOIN ops.source_checkpoints c ON c.storage_key = s.source_id || ':' || (feed.n - 1);

CREATE OR REPLACE VIEW tank_metrics.agent_runs AS
SELECT opened_at, status,
       CASE WHEN status <> 'FAILED' THEN NULL
            WHEN document #>> '{terminal_failure,error,type}' IN
                 ('unknown_error', 'model_overloaded_error', 'model_rate_limited_error',
                  'model_request_failed_error', 'mcp_connection_failed_error',
                  'mcp_authentication_failed_error', 'billing_error', 'credential_host_unreachable_error')
              THEN document #>> '{terminal_failure,error,type}'
            WHEN document #>> '{terminal_failure,finish_reason}' = 'content-filter' THEN 'content-filter'
            WHEN document #>> '{terminal_failure,event_type}' IN
                 ('session.error', 'agent.refusal', 'execution.status_failed')
              THEN document #>> '{terminal_failure,event_type}'
            WHEN document #>> '{terminal_failure,origin}' = 'HOST_REPLAY_ABORT' THEN 'HOST_REPLAY_ABORT'
            ELSE 'Unclassified' END AS error_class
FROM ops.agent_sessions;

CREATE OR REPLACE VIEW tank_metrics.agent_model_turns AS
SELECT at FROM ops.llm_usage WHERE call = 'AGENT';

CREATE OR REPLACE VIEW tank_metrics.agent_first_verdict AS
SELECT s.opened_at, v.action,
       extract(epoch FROM v.answered_at - s.opened_at) AS seconds_to_verdict
FROM ops.agent_sessions s
LEFT JOIN LATERAL (
    SELECT tc.answered_at, tc.document #>> '{result,action}' AS action
    FROM ops.tool_calls tc
    WHERE tc.session_id = s.session_id AND tc.tool = 'get_decision' AND tc.status = 'DONE'
      AND tc.answered_at >= s.opened_at AND nullif(tc.document #>> '{result,action}', '') IS NOT NULL
    ORDER BY tc.answered_at, tc.call_id LIMIT 1
) v ON true;

CREATE OR REPLACE VIEW tank_metrics.agent_watches AS
SELECT CASE WHEN w.document -> 'expr' ? 'review_at' THEN 'Timed review'
            WHEN w.document -> 'expr' ?| ARRAY['metric','all'] THEN 'Metric watch'
            ELSE 'Unclassified' END AS watch_kind,
       w.status, w.fired_at,
       CASE WHEN w.document ->> 'consumed_by' IS NULL THEN 'No linked decision'
            ELSE coalesce(d.action, 'Linked decision unavailable') END AS consuming_action
FROM ana.watch_conditions w
LEFT JOIN ana.decisions d ON d.dec_id = w.document ->> 'consumed_by'
WHERE w.document @> '{"agent_defined":true}';

CREATE OR REPLACE VIEW tank_metrics.agent_milestones AS
SELECT j.created_at, j.status
FROM ops.jobs j
WHERE j.type = 'MILESTONE_CHECK' AND EXISTS (
    SELECT 1 FROM ops.tool_calls tc
    WHERE tc.tool = 'schedule_milestone' AND tc.status = 'DONE'
      AND tc.document #>> '{result,job_id}' = j.job_id
);

GRANT SELECT ON tank_metrics.source_pulls, tank_metrics.agent_runs, tank_metrics.agent_model_turns,
                tank_metrics.agent_first_verdict, tank_metrics.agent_watches, tank_metrics.agent_milestones
TO tank_grafana_reader;
