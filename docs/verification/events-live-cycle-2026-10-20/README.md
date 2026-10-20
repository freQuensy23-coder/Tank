# Live cycle: conditions, held updates, Telegram chains (tank-events, 20 Oct 2026)

What ran: the real Scheduler loop (30 s tick, EVAL_CONDITIONS, MILESTONE_CHECK), the real Runtime job worker (4 workers), the real EventToolService (tools, schemas, alert gate, cards) and the real DeliveryService, on a PostgreSQL 16 test database (tmpfs container tank-events-pgtest, DB tank_live_cycle, all migrations), with the real clock and the real 900 s update limit from config/thresholds.yaml.
Stand-ins: Telegram (an httpx MockTransport that records sendMessage; nothing reaches the network), the agent (a script that follows the 03 §3.2 playbook through the same tool calls) and market values (ScenarioTruth in live_cycle.py). Code: ws/tank-events at ee410ca.

Command: `TANK_TEST_DATABASE_URL=... python live_cycle.py OUT` (run 2, 17:44:48–18:07:45 UTC). Full record: evidence-run2.json.

| Time (UTC) | Step | Result |
|---|---|---|
| 17:45:05 | NEW_ITEM: record_event tokens ["WLD"] | stored as world:WLD (W5); MAJOR M3; FULL card WAIT (A1, data LOW) sent, msg 1002 |
| 17:46:04 | data turns HIGH; the scheduler fires the WAIT(data) upgrade | CONDITION_FIRED → START (A5) → UPDATE v2 immediate, reply to 1002; call OPEN (entry 10.4, invalidation 10) |
| 17:46:05 | ITEM_ADDED of 3-day-old news advancing the stage | publish refused STALE_STATE_CHANGE, nothing sent |
| 17:50:02 | MILESTONE due, resolved by the agent | UPDATE v3 accepted, held until 18:01:04 (START sent + 900 s) |
| 17:51:04 | ITEM_ADDED official denial | FATAL → EXIT (B1) → v4 immediate, reply to 1002; call CLOSED by EXIT |
| 17:52–17:53 | fatal and WAIT upgrades fire after EXIT | v5, v6 (WAIT, thesis D) held until 18:06:04 (EXIT sent + 900 s) |
| 18:01:04 | v3 slot reached | job DONE, no send: v3 superseded by v4 (W2) |
| 18:06:05 | v5/v6 slot | v5 dropped (superseded), v6 sent, reply to 1002 |

The Tank Alerts chat received exactly: 1002 FULL, 1003 START, 1004 EXIT, 1005 WAIT; every update replies to 1002. Tank Ops received the heartbeat (1001).
Watches: each fired watch stayed FIRED and consumed; the same expressions were re-armed (rearmed_from set) and did not re-fire on the lasting level (W1 edge trigger).
Run 1 (17:25–17:43, before the edge trigger) showed the same chain (logs in /srv/tank-work/logs/tank-events/live-pg-900-run1).
Not covered here: the deployed runtime and real Telegram sends (those wait for the lead's go). Expected noise in the log: "backup maintenance failed" (no TANK_BACKUP_DIR in the test settings).
