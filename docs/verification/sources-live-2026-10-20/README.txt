Live check of the deployed runtime, 2026-10-20 19:41Z (roll r90, main e1f369b, started 19:35:56Z with compose.x.yaml):
- 33/34 sources HEALTHY; S10 DEGRADED only on annType=latest-events (HTTP 400, code 51000, type not offered for this region; user decision pending)
- no "sources loop failed" since the roll
- S29: HEALTHY, scan COMPLETE, 13 posts captured by 19:41Z, all monitored accounts in the 86-handle list; reads before the roll failed (secret not mounted)
- User-Agent: TankV0/1.0 (inbox@ai.com) (see ../user-agent-2026-10-20)
Counters are cumulative since first start; the V0-A window is end minus a snapshot taken after the roll.
