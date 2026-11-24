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

CREATE OR REPLACE VIEW tank_metrics.source_poll_history AS
SELECT bucket_start, source_mode, feed_kind, successful, failed
FROM ops.source_poll_counts;

GRANT SELECT ON tank_metrics.source_poll_history TO tank_grafana_reader;

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

CREATE OR REPLACE VIEW tank_metrics.agent_run_outcomes AS
SELECT s.opened_at,
       CASE WHEN s.status = 'FAILED' OR turns.has_failed THEN 'Execution error'
            WHEN s.document @> '{"agent_incomplete":true}' THEN 'Incomplete workflow'
            WHEN s.status = 'OPEN' AND turns.has_pending THEN 'Pending / running'
            WHEN turns.all_verified AND s.document @> '{"verified_native_completion":{"complete":true}}'
              THEN 'Execution success'
            ELSE 'Unknown' END AS outcome
FROM ops.agent_sessions s
CROSS JOIN LATERAL (
    SELECT bool_or(m ->> 'turn_status' = 'FAILED') AS has_failed,
           bool_or(m ->> 'turn_status' IN ('ACCEPTING','PENDING','RUNNING')) AS has_pending,
           bool_and(coalesce(m ->> 'turn_status' = 'DONE' AND
               (s.document #> '{verified_native_completion,accepted_input_ids}') ? (m ->> 'remote_event_id'), false)) AS all_verified
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(s.document -> 'message_log') = 'array'
                                   THEN s.document -> 'message_log' END) AS t(m)
    WHERE NOT coalesce((s.document -> 'retired_message_ids') ? (m ->> 'message_id'), false)
) turns;

CREATE OR REPLACE VIEW tank_metrics.minute_condition_checks_by_cutoff AS
SELECT CASE WHEN pg_input_is_valid(idem_key, 'timestamptz') THEN idem_key::timestamptz END AS scheduled_at,
       status, idem_key AS cutoff_key
FROM ops.jobs
WHERE type = 'EVAL_CONDITIONS' AND idem_key = document #>> '{payload,cutoff}'
  AND document #>> '{payload,cutoff_prec}' = 'BUCKET(1m)'
  AND idem_key ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:00Z$';

CREATE OR REPLACE VIEW tank_metrics.minute_condition_checks AS
SELECT scheduled_at, status FROM tank_metrics.minute_condition_checks_by_cutoff;

GRANT SELECT ON tank_metrics.minute_condition_checks, tank_metrics.minute_condition_checks_by_cutoff TO tank_grafana_reader;

CREATE OR REPLACE VIEW tank_metrics.agent_scheduled_jobs AS
SELECT created_at, status,
       CASE WHEN type = 'EVAL_CONDITIONS' THEN 'Tank minute condition check'
            WHEN document #> '{payload,condition,expr}' ? 'review_at' THEN 'Agent timed review notification'
            WHEN document #> '{payload,condition,expr}' ?| ARRAY['metric','all'] THEN 'Agent metric watch notification'
            ELSE 'Agent watch notification (unknown type)' END AS job_kind,
       CASE WHEN status IN ('DONE','FAILED','EXPIRED') AND finished_at >= created_at
            THEN extract(epoch FROM finished_at - created_at) END AS seconds_in_queue_lifecycle,
       CASE WHEN type = 'EVAL_CONDITIONS' AND status = 'DONE' AND jsonb_typeof(document #> '{result,deferred}') = 'array'
            THEN jsonb_array_length(document #> '{result,deferred}') END AS deferred_conditions
FROM ops.jobs
WHERE (type = 'EVAL_CONDITIONS' AND idem_key = document #>> '{payload,cutoff}'
       AND document #>> '{payload,cutoff_prec}' = 'BUCKET(1m)')
   OR (type = 'AGENT_MESSAGE' AND document @> '{"payload":{"kind":"CONDITION_FIRED","condition":{"agent_defined":true,"status":"FIRED"}}}'
       AND idem_key = (document #>> '{payload,event_id}') || ':CONDITION_FIRED:' || (document #>> '{payload,condition,cond_id}')
       AND document #>> '{payload,event_id}' = document #>> '{payload,condition,event_id}');

GRANT SELECT ON tank_metrics.agent_run_outcomes, tank_metrics.agent_scheduled_jobs TO tank_grafana_reader;

CREATE OR REPLACE VIEW tank_metrics.agent_model_turns AS
SELECT at FROM ops.llm_usage WHERE call = 'AGENT';

CREATE OR REPLACE VIEW tank_metrics.agent_first_verdict AS
SELECT s.opened_at, v.action,
       extract(epoch FROM v.answered_at - s.opened_at) AS seconds_to_verdict,
       CASE WHEN v.action IS NOT NULL THEN v.action
            WHEN s.status = 'OPEN' AND EXISTS (
                SELECT FROM ops.tool_calls tc WHERE tc.session_id = s.session_id AND tc.tool = 'get_decision'
                  AND tc.status = 'PENDING' AND tc.requested_at >= s.opened_at AND tc.answered_at IS NULL
            ) THEN 'Decision requested (pending)'
            ELSE 'No recorded decision' END AS decision_state
FROM ops.agent_sessions s
LEFT JOIN LATERAL (
    SELECT tc.answered_at, tc.document #>> '{result,action}' AS action
    FROM ops.tool_calls tc
    WHERE tc.session_id = s.session_id AND tc.tool = 'get_decision' AND tc.status = 'DONE'
      AND tc.answered_at >= s.opened_at AND tc.answered_at >= tc.requested_at
      AND NOT (tc.document @> '{"result":{"tracking_ended":true}}')
      AND tc.document #>> '{result,action}' IN ('START','WAIT','DONT_CHASE','WATCH_TO_START','ADD','HOLD','REDUCE','EXIT')
    ORDER BY tc.answered_at, tc.call_id LIMIT 1
) v ON true;

CREATE OR REPLACE VIEW tank_metrics.agent_first_classification AS
SELECT s.opened_at, v.classification,
       extract(epoch FROM v.answered_at - s.opened_at) AS seconds_to_classification
FROM ops.agent_sessions s
LEFT JOIN LATERAL (
    SELECT tc.answered_at, CASE WHEN tc.tool = 'record_event' THEN 'NO_EVENT'
                                ELSE tc.document #>> '{result,class}' END AS classification
    FROM ops.tool_calls tc
    WHERE tc.session_id = s.session_id AND tc.status = 'DONE'
      AND tc.answered_at >= s.opened_at AND tc.answered_at >= tc.requested_at
      AND ((tc.tool = 'record_event' AND tc.document @> '{"result":{"is_event":false}}')
        OR (tc.tool = 'set_assessment' AND tc.document #>> '{result,class}' IN ('LOW','MATERIAL','MAJOR','STRUCTURAL_SHOCK')))
    ORDER BY tc.answered_at, tc.call_id LIMIT 1
) v ON true;

GRANT SELECT ON tank_metrics.agent_first_classification TO tank_grafana_reader;

CREATE OR REPLACE VIEW tank_metrics.session_first_importance AS
SELECT s.opened_at, extract(epoch FROM grade.answered_at - s.opened_at) AS seconds
FROM ops.agent_sessions s
CROSS JOIN LATERAL (
    SELECT tc.answered_at FROM ops.tool_calls tc
    WHERE tc.session_id = s.session_id AND tc.tool = 'set_assessment' AND tc.status = 'DONE'
      AND tc.requested_at >= s.opened_at AND tc.answered_at >= tc.requested_at
      AND NOT tc.document @> '{"is_error":true}'
      AND tc.document #>> '{result,class}' IN ('LOW','MATERIAL','MAJOR','STRUCTURAL_SHOCK')
    ORDER BY tc.answered_at LIMIT 1
) grade
WHERE s.document ->> 'replay_run_id' IS NULL;

CREATE OR REPLACE VIEW tank_metrics.session_first_sent_card AS
SELECT s.opened_at, extract(epoch FROM first_card.at - s.opened_at) AS seconds
FROM ops.agent_sessions s
CROSS JOIN LATERAL (
    SELECT d.at, a.document ->> 'session_id' AS producer, a.kind, a.generated_at,
           a.alert_id, a.version, d.message_ref, d.document ->> 'chat_id' AS chat_id,
           d.document ->> 'request_reply_to_message_ref' AS reply_to
    FROM out.alerts a JOIN out.deliveries d USING (alert_id, version)
    WHERE a.event_id = s.event_id AND d.channel = 'telegram:alerts' AND d.status = 'SENT'
      AND d.attempt > 0 AND d.message_ref ~ '^[1-9][0-9]*$' AND nullif(d.document ->> 'chat_id', '') IS NOT NULL
      AND coalesce(d.document ->> 'method', 'sendMessage') IN ('sendMessage','sendRichMessage')
    ORDER BY d.at FETCH FIRST 1 ROW WITH TIES
) first_card
WHERE s.document ->> 'replay_run_id' IS NULL
GROUP BY s.session_id, s.opened_at, first_card.at
HAVING count(DISTINCT (first_card.alert_id, first_card.version)) = 1
   AND bool_and(first_card.producer IS NOT DISTINCT FROM s.session_id
                AND first_card.kind IN ('PRELIMINARY','PARTIAL','FULL') AND first_card.reply_to IS NULL
                AND first_card.generated_at BETWEEN s.opened_at AND first_card.at
                AND NOT EXISTS (
                    SELECT FROM out.deliveries other
                    WHERE other.channel = 'telegram:alerts' AND other.status = 'SENT'
                      AND other.message_ref = first_card.message_ref AND other.document ->> 'chat_id' = first_card.chat_id
                      AND (other.alert_id, other.version) IS DISTINCT FROM (first_card.alert_id, first_card.version)
                ));

GRANT SELECT ON tank_metrics.session_first_importance, tank_metrics.session_first_sent_card TO tank_grafana_reader;

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
SELECT j.created_at, j.status, EXISTS (
    SELECT FROM ops.tool_calls tc
    WHERE tc.tool = 'schedule_milestone' AND tc.status = 'DONE' AND NOT tc.document @> '{"is_error":true}'
      AND tc.document #>> '{result,job_id}' = j.job_id
) AS tool_linked
FROM ops.jobs j WHERE j.type = 'MILESTONE_CHECK';

CREATE OR REPLACE VIEW tank_metrics.event_card_activity AS
SELECT a.generated_at, receipts.sent_at, a.kind,
       CASE WHEN receipts.sent_at IS NOT NULL THEN 'Confirmed'
            WHEN receipts.uncertain THEN 'Unconfirmed outcome'
            WHEN receipts.failed THEN 'Failed receipt'
            WHEN j.status IN ('QUEUED','RUNNING') THEN 'Pending job'
            ELSE 'No conclusive receipt' END AS delivery_state,
       s.session_id IS NOT NULL AS session_linked,
       EXISTS (
           SELECT FROM ops.tool_calls tc
           JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(s.document -> 'message_log') = 'array'
                                                  THEN s.document -> 'message_log' END) m ON true
           WHERE tc.session_id = s.session_id AND tc.tool = 'publish_card' AND tc.status = 'DONE'
             AND tc.document @> '{"result":{"published":true}}' AND NOT tc.document @> '{"is_error":true}'
             AND tc.document #>> '{result,reason}' IN ('GENERATED','QUEUED_RATE_LIMIT')
             AND tc.document #>> '{result,alert_id}' = a.alert_id AND tc.document #>> '{result,version}' = a.version::text
             AND m ->> 'message_id' = tc.document ->> 'native_turn_message_id' AND nullif(m ->> 'remote_event_id','') IS NOT NULL
       ) AS input_linked
FROM out.alerts a
LEFT JOIN ops.agent_sessions s ON s.session_id = a.document ->> 'session_id' AND s.event_id = a.event_id
  AND s.document ->> 'replay_run_id' IS NULL AND s.opened_at <= a.generated_at
LEFT JOIN ops.jobs j ON j.type = 'DELIVER' AND j.idem_key = a.alert_id || ':' || a.version || ':telegram:alerts'
CROSS JOIN LATERAL (
    SELECT min(d.at) FILTER (WHERE d.status = 'SENT' AND d.attempt > 0 AND d.message_ref ~ '^[1-9][0-9]*$'
                               AND nullif(d.document ->> 'chat_id','') IS NOT NULL
                               AND coalesce(d.document ->> 'method','sendMessage') IN ('sendMessage','sendRichMessage')) AS sent_at,
           bool_or(d.status = 'FAILED') AS failed,
           bool_or((d.status = 'GENERATED' AND d.attempt > 0 AND NOT EXISTS (
                      SELECT FROM out.deliveries settled WHERE settled.alert_id = d.alert_id AND settled.version = d.version
                        AND settled.channel = d.channel AND settled.attempt = d.attempt AND settled.status IN ('FAILED','SENT')
                   )) OR d.status = 'SENT' OR (d.status = 'FAILED' AND NOT d.document @> '{"terminal":true}'
                       AND (d.document ->> 'http_status' IS NULL OR d.document ->> 'http_status' = '200'))) AS uncertain
    FROM out.deliveries d WHERE d.alert_id = a.alert_id AND d.version = a.version AND d.channel = 'telegram:alerts'
) receipts;

CREATE OR REPLACE VIEW tank_metrics.card_publication_calls AS
SELECT tc.requested_at,
       CASE WHEN receipt.disposition IN ('SUPPRESSED','REFUSED','PUBLISHED') THEN receipt.disposition
            WHEN tc.status IN ('ERROR','REFUSED','PENDING') THEN tc.status
            WHEN tc.document @> '{"result":{"accepted":false}}' THEN 'REFUSED'
            ELSE 'Unknown' END AS disposition,
       CASE WHEN receipt.reason IN ('TERMINAL_EVENT','FIRST_CARD_ALREADY_GENERATED','BELOW_MAJOR','LOW_CERTAINTY',
                 'STALE','INVALIDATED','NO_MATERIAL_CHANGE','STALE_STATE_CHANGE','ORDINARY_CARD_EXISTS',
                 'PRELIMINARY_REQUIRES_MATERIAL','PRELIMINARY_REQUIRES_EVIDENCE','ANALYSIS_INCOMPLETE','EVENT_SCOPE',
                 'GENERATED','IDENTICAL_PENDING','QUEUED_RATE_LIMIT','VALIDATION','PROVENANCE') THEN receipt.reason
            ELSE 'Unknown / unclassified' END AS reason
FROM ops.tool_calls tc
CROSS JOIN LATERAL (SELECT tc.document #>> '{result,publication_receipt,disposition}' AS disposition,
    coalesce(tc.document #>> '{result,publication_receipt,reason}', tc.document #>> '{result,reason}',
             tc.document #>> '{result,error,category}') AS reason) receipt
WHERE tc.tool = 'publish_card';

GRANT SELECT ON tank_metrics.event_card_activity, tank_metrics.card_publication_calls TO tank_grafana_reader;

GRANT SELECT ON tank_metrics.source_pulls, tank_metrics.agent_runs, tank_metrics.agent_model_turns,
                tank_metrics.agent_first_verdict, tank_metrics.agent_watches, tank_metrics.agent_milestones
TO tank_grafana_reader;
