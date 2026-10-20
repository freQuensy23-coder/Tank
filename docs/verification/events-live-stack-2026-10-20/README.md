# Live stack check after roll r90 (tank-events, 20 Oct 2026)

Stack: roll r90 from main e1f369b, live since 19:35:56Z (includes acadaaa, 8a3951c, 9dfb4d6); skill baseline-2.6-spec-playbook registered. Read-only SQL as SELECT with default_transaction_read_only=on, inside tank-tank-1 (/srv/tank-work/logs/tank-events/rosql.sh).

Observed 19:39–19:45 UTC:
- No alert and no watch condition exists yet (out.alerts 0 rows, ana.watch_conditions 0 rows): no MAJOR+ event has reached a card since the roll, so the card → update → hold chain could not be watched live. Tank Ops got the heartbeat: out.deliveries GENERATED then SENT at 18:51:12.
- Queue under load: REACTION_CAPTURE held up to 8 workers. 54 were queued at 19:39, 40 of them late revisions created before the roll by the old code (no coalescing), for one event (EVT-01M5DBAE8N2CYZ402H11R5JHHC, 0x:ZRX, PRE_1H/4H/24H, 13–15 per checkpoint). After the roll the scheduler added only one per checkpoint (5 jobs).
- Effect: EVAL_CONDITIONS (priority 2, as is REACTION_CAPTURE per 03 §3.8; ordered by run_after) expired behind the older reaction jobs: 24 EXPIRED in 30 min. AGENT_MESSAGE (26) and ROUTE_ITEM (25) waited.
- Fix (main bc1a01f, needs a roll): a late revision with a newer one queued for the same checkpoint returns at once, so such a backlog drains in seconds.

Still to verify live, once a MAJOR+ event arrives on the rolled stack: the condition cycle (EVAL_CONDITIONS → CONDITION_FIRED → UPDATE), a held update, and START/EXIT replies in Tank Alerts. The same chain was verified on PG with the real scheduler and real time (../events-live-cycle-2026-10-20/).
