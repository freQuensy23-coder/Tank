-- PARTIAL publication belongs to the agent; obsolete host publication must never resume.
UPDATE ops.jobs SET status='EXPIRED',last_error='Retired host-side PUBLISH_PARTIAL job',
 document=document||jsonb_build_object('status','EXPIRED','last_error','Retired host-side PUBLISH_PARTIAL job',
  'upgrade_retirement',jsonb_build_object('reason','AGENT_OWNS_PARTIAL_PUBLICATION','at',now(),'original_status',status))
 WHERE type='PUBLISH_PARTIAL' AND status IN ('QUEUED','RUNNING');
-- Known old default calculated expiry from creation, preceding the future checkpoint itself.
-- Repair only never-attempted rows with that impossible schedule, and revive only a live window.
UPDATE ops.jobs SET expires_at=run_after+interval '6 hours',
 status=CASE WHEN run_after+interval '6 hours'>now() THEN 'QUEUED' ELSE 'EXPIRED' END,
 document=document||jsonb_build_object('expires_at',to_char((run_after+interval '6 hours') AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
   'status',CASE WHEN run_after+interval '6 hours'>now() THEN 'QUEUED' ELSE 'EXPIRED' END,
   'finished_at',CASE WHEN run_after+interval '6 hours'>now() THEN NULL ELSE document->'finished_at' END,
   'upgrade_repair',jsonb_build_object('reason','CHECKPOINT_EXPIRY_PRECEDED_RUN_AFTER','at',now(),'original_expires_at',expires_at,'original_status',status))
 WHERE type='REACTION_CAPTURE' AND status IN ('QUEUED','EXPIRED') AND attempts=0 AND expires_at<run_after;
