-- Existing queued messages must not continue obeying a retired daily-budget pause.
-- Only an explicit historical pause marker proves the schedule came from that policy.
UPDATE ops.jobs
SET run_after=least(run_after,now()),
 status=CASE WHEN expires_at<=now() THEN 'EXPIRED' ELSE 'QUEUED' END,
 document=jsonb_set(document,'{status}',to_jsonb((CASE WHEN expires_at<=now() THEN 'EXPIRED' ELSE 'QUEUED' END)::text))
   || jsonb_build_object('run_after',to_char(least(run_after,now()) AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
      'budget_retirement',jsonb_build_object('action','RELEASED_PAUSE','at',now(),
         'original_run_after',run_after,'original_expires_at',expires_at,'budget_paused_at',document->'budget_paused_at'))
WHERE type='AGENT_MESSAGE' AND status='QUEUED' AND run_after>now()
 AND document->>'budget_paused_at' IS NOT NULL;

-- Preserve historical notices and their immutable delivery ledger, but suppress pending sends.
UPDATE ops.jobs AS jobs
SET status='EXPIRED',last_error='Removed daily-budget policy: obsolete LLM_BUDGET delivery suppressed',
 document=jsonb_set(document,'{status}','"EXPIRED"'::jsonb)
  || jsonb_build_object('last_error','Removed daily-budget policy: obsolete LLM_BUDGET delivery suppressed',
      'budget_retirement',jsonb_build_object('action','SUPPRESSED_OBSOLETE_NOTICE','at',now(),
         'original_status',jobs.status,'original_run_after',jobs.run_after,'original_expires_at',jobs.expires_at))
WHERE jobs.type='DELIVER' AND jobs.status IN ('QUEUED','RUNNING')
 AND EXISTS (SELECT 1 FROM out.ops_notices AS notice
   WHERE notice.notice_id=jobs.payload->>'notice_id' AND notice.kind='LLM_BUDGET');
