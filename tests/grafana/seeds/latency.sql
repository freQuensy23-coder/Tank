BEGIN;

INSERT INTO evt.events (storage_key, event_id, statement, theme_tags, public_at, public_at_prec, public_at_basis,
                        known_to_us_at, freshness, event_state, state_verified, certainty, timing_confidence,
                        best_source_class, lifecycle, document)
SELECT id, id, 'fixture', '{}', '2026-01-01Z', 'EXACT', 'fixture', '2026-01-01Z', 'FRESH', 'ANNOUNCED',
       false, 'fixture', 'fixture', 'fixture', 'TRACKED', '{}'
FROM unnest(ARRAY['alpha','beta','gamma','unattributed','unsent','conflict','tie','update-only']) AS events(id);

INSERT INTO ops.agent_sessions (storage_key, session_id, event_id, opened_at, status, last_event_seq, agent_version, document)
SELECT sid, sid, eid, '2026-01-01Z'::timestamptz + delay * interval '1 second', 'ENDED', 0, 'fixture',
       '{"proposal_id":"fixture","last_item_at_reason":"fixture"}'
FROM (VALUES ('a','alpha',0), ('a-next','alpha',5), ('b','beta',30), ('c','gamma',600),
             ('unknown','unattributed',0), ('waiting','unsent',0), ('wrong','conflict',0),
             ('tie','tie',0), ('update','update-only',0)) AS sessions(sid,eid,delay);

INSERT INTO ops.tool_calls (storage_key, call_id, session_id, tool, status, requested_at, answered_at, document)
SELECT sid || ':' || seq, seq, sid, tool, state, opened_at, opened_at + delay * interval '1 second',
       jsonb_build_object('input','{}'::jsonb,'result',result)
FROM (VALUES
    ('a','1','record_event','DONE',1,'{"is_event":false}'::jsonb),
    ('a','2','set_assessment','REFUSED',2,'{"class":"MAJOR"}'),
    ('a','3','set_assessment','ERROR',3,'{"class":"LOW"}'),
    ('a','4','set_assessment','DONE',20,'{"class":"LOW"}'),
    ('a','5','set_assessment','DONE',40,'{"class":"MAJOR"}'),
    ('a-next','1','record_event','DONE',6,'{"is_event":false}'),
    ('b','1','set_assessment','DONE',60,'{"class":"STRUCTURAL_SHOCK"}'),
    ('c','1','set_assessment','DONE',25,'{"class":"MATERIAL"}'),
    ('waiting','1','set_assessment','PENDING',NULL,NULL),
    ('waiting','2','set_assessment','DONE',-1,'{"class":"MAJOR"}'),
    ('tie','1','set_assessment','DONE',605,'{"class":"MAJOR"}')
) AS calls(sid,seq,tool,state,delay,result) JOIN ops.agent_sessions ON session_id = sid;

INSERT INTO out.alerts (storage_key,alert_id,version,kind,event_id,generated_at,card_text,decisions,evidence_bundle_id,
                        code_version,config_version,document)
SELECT eid || ':' || version, eid, version, kind, eid, '2026-01-01Z'::timestamptz + delay * interval '1 second',
       'fixture','{}',repeat('a',64),'fixture','fixture',jsonb_build_object('card_struct','{}'::jsonb,'session_id',sid)
FROM (VALUES ('alpha',1,'PRELIMINARY','a',10), ('alpha',2,'FULL','a-next',101), ('alpha',3,'UPDATE','a-next',120),
             ('beta',1,'FULL','b',40), ('gamma',1,'PARTIAL','c',605), ('unattributed',1,'FULL',NULL,2),
             ('unattributed',2,'FULL','unknown',3), ('unsent',1,'FULL','waiting',2),
             ('conflict',1,'FULL','a',4), ('tie',1,'PARTIAL','tie',1), ('tie',2,'FULL','tie',2),
             ('update-only',1,'UPDATE','update',1)) AS cards(eid,version,kind,sid,delay);

INSERT INTO out.deliveries (storage_key,alert_id,version,status,attempt,at,channel,message_ref,document)
SELECT id, eid, version, state, attempt, '2026-01-01Z'::timestamptz + delay * interval '1 second', channel, ref,
       jsonb_build_object('chat_id','fixture') || extra
FROM (VALUES
    ('queue','alpha',1,'GENERATED',0,10,'telegram:alerts',NULL,'{}'::jsonb),
    ('attempt','alpha',1,'GENERATED',1,11,'telegram:alerts',NULL,'{}'),
    ('failed','alpha',1,'FAILED',1,12,'telegram:alerts',NULL,'{}'),
    ('sent','alpha',1,'SENT',2,100,'telegram:alerts','101','{}'),
    ('duplicate','alpha',1,'SENT',2,100,'telegram:alerts','101','{}'),
    ('later','alpha',2,'SENT',1,115,'telegram:alerts','102','{}'),
    ('update','alpha',3,'SENT',1,140,'telegram:alerts','103','{}'),
    ('sent-b','beta',1,'SENT',1,330,'telegram:alerts','201','{}'),
    ('sent-c','gamma',1,'SENT',1,645,'telegram:alerts','301','{}'),
    ('missing-session','unattributed',1,'SENT',1,20,'telegram:alerts','401','{}'),
    ('later-known','unattributed',2,'SENT',1,30,'telegram:alerts','402','{}'),
    ('queued-only','unsent',1,'GENERATED',0,2,'telegram:alerts',NULL,'{}'),
    ('failed-only','unsent',1,'FAILED',1,4,'telegram:alerts',NULL,'{}'),
    ('ops-only','unsent',1,'SENT',1,5,'telegram:ops','501','{}'),
    ('edit-only','unsent',1,'SENT',1,6,'telegram:alerts','502','{"method":"editMessageText"}'),
    ('no-receipt','unsent',1,'SENT',1,7,'telegram:alerts',NULL,'{}'),
    ('wrong-event','conflict',1,'SENT',1,8,'telegram:alerts','601','{}'),
    ('same-time-1','tie',1,'SENT',1,9,'telegram:alerts','701','{}'),
    ('same-time-2','tie',2,'SENT',1,9,'telegram:alerts','702','{}'),
    ('only-update','update-only',1,'SENT',1,10,'telegram:alerts','801','{}')
) AS attempts(id,eid,version,state,attempt,delay,channel,ref,extra);

INSERT INTO out.alerts (storage_key,alert_id,version,kind,event_id,generated_at,card_text,decisions,evidence_bundle_id,
                        code_version,config_version,document)
SELECT id,id,1,'FULL','unsent','2026-01-02Z','fixture','{}',repeat('a',64),'fixture','fixture','{"card_struct":{}}'
FROM (VALUES ('pending-card'),('rejected-card')) cards(id);
INSERT INTO ops.jobs (storage_key,job_id,type,priority,idem_key,status,attempts,max_attempts,created_at,run_after,expires_at,document)
VALUES ('pending-job','pending-job','DELIVER',0,'pending-card:1:telegram:alerts','QUEUED',0,5,'2026-01-02Z','2026-01-02Z','2026-01-03Z','{"payload":{"alert_id":"pending-card","version":1,"channel":"telegram:alerts"}}');
INSERT INTO out.deliveries (storage_key,alert_id,version,status,attempt,at,channel,document)
VALUES ('api-reject','rejected-card',1,'FAILED',1,'2026-01-02Z','telegram:alerts','{"error":"DeliveryError","http_status":429}');
UPDATE ops.agent_sessions SET document = document || '{"message_log":[{"message_id":"m","remote_event_id":"accepted"}]}' WHERE session_id='a';
INSERT INTO ops.tool_calls (storage_key,call_id,session_id,tool,status,requested_at,answered_at,document)
SELECT 'pub-' || id,id,'a','publish_card','DONE','2026-01-01Z','2026-01-01Z',
       jsonb_build_object('input','{}'::jsonb,'native_turn_message_id','m','result',result)
FROM (VALUES ('a-generated','{"published":true,"reason":"GENERATED","alert_id":"alpha","version":1,"publication_receipt":{"disposition":"PUBLISHED","reason":"GENERATED"}}'::jsonb),
             ('a-suppressed','{"published":false,"publication_receipt":{"disposition":"SUPPRESSED","reason":"BELOW_MAJOR"}}'),
             ('a-unknown','{}')) outcomes(id,result);

SET LOCAL ROLE tank_grafana_reader;
SET LOCAL plpgsql.check_asserts = on;
DO $$ BEGIN
    ASSERT (SELECT array_agg(seconds ORDER BY seconds) = ARRAY[20,25,60,605]::numeric[] FROM tank_metrics.session_first_importance);
    ASSERT (SELECT array_agg(seconds ORDER BY seconds) = ARRAY[45,100,300]::numeric[] FROM tank_metrics.session_first_sent_card);
END $$;
COMMIT;
