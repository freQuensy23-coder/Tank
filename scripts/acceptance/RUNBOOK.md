# Acceptance run sheets: E2E (V0-G), speed, V0-A 72 h, V0-H 14 days

Owner: tank-acceptance. Every script here is read-only on the production database: queries run in a `READ ONLY` transaction through `psql` in `tank-postgres-1`, and nothing is sent, retried or edited. Telegram sends come only from the deployed runtime, after the lead's go.

Paths below use `A=/srv/tank/scripts/acceptance` (after merge to `main`) and `RUN=<evidence dir>`. Long jobs run detached, with logs in `/srv/tank-work/logs/tank-acceptance/`.

## Preconditions (all runs)

1. tank-platform has deployed the stack: `docker compose -p tank` with services `postgres`, `clickhouse`, `openma` and `tank` running the pinned image.
2. `tank doctor` passes inside the `tank` container, including the Telegram token, both chats, the OpenMA agent/environment ids and the OpenRouter key.
3. The lead has given the go for Telegram delivery and for this run.
4. For V0-A and V0-H, S29 needs X enabled (`TANK_X_ENABLED=true`, the 86-account list) because it is one of the 34 sources. With X disabled, V0-A cannot pass.
5. Record the code, config and image tuple at the start, in `$RUN/start.json`: `docker inspect` of the tank and openma images, plus `git -C /srv/tank rev-parse HEAD`.

## V0-G: end to end, source → Telegram

There is no manual step and no injected item. A real item arrives through the configured sources, and the runtime does FILTER/ROUTE → agent session → tools → Market Truth → decision → card → Telegram.

- Watch for the first event with a SENT card (cheap DB read every 5 min, runs detached):
  ```
  RUN=/srv/tank/docs/verification/e2e-source-telegram-$(date -u +%F); mkdir -p $RUN
  nohup sh -c 'until python3 '$A'/e2e_trace.py --since <go-time> --out '$RUN'; do [ $? -eq 1 ] && break; sleep 300; done' \
    > /srv/tank-work/logs/tank-acceptance/e2e-watch.log 2>&1 &
  ```
  Exit codes: 0 = chain complete and within limits; 1 = a card was sent but a stage is missing or a limit was missed, so the evidence is kept and the cause investigated; 2 = no sent card yet, keep waiting.
- Evidence in `$RUN/<event_id>/`:
  - `trace.json` / `trace.md`: stages, timeline, timings against limits, agent rule versions, evidence-bundle hash check.
  - One JSONL per table: items, jobs, tool_calls, market_truth, decisions, alerts, deliveries and the rest.
  - `receipt.json`: sha256 of every file.
- Pass (01 §10 V0-G):
  - every stage present: relevant item, filter and route jobs DONE, session, `record_event`, assessment, `get_market_truth` DONE, a market_truth row, a decision, a card, a SENT ledger row with `message_ref`, and a valid evidence bundle;
  - item → event ≤ 7 min (p95);
  - first card ≤ 3 min for STRUCTURAL SHOCK, FULL card ≤ 8 min for MAJOR+;
  - delivery ≤ 10 s;
  - no operator jobs in the chain.

  The hard rules are enforced by the agent skill (settled decision: no card linter). The trace records the model and prompt version in force for the review.

## Speed (01 §9, 03 §3.9)

```
python3 $A/latency.py --since <window start> --until <window end> --out $RUN/speed
```

| Metric | Stored timestamps | Target |
|---|---|---|
| item → event | `source_items.observed_at` → first DONE `record_event` answered_at (new session) or ROUTE_ITEM `finished_at` (ADDED) | p95 ≤ 7 min |
| STRUCTURAL SHOCK first card | `events.known_to_us_at` → first `out.alerts.generated_at` (sent also reported) | ≤ 180 s |
| MAJOR+ FULL card | `known_to_us_at` → first FULL `generated_at`, which includes the pre-event backfill | ≤ 480 s |
| update after a decisive condition | `watch_conditions.fired_at` → next UPDATE `generated_at`, when the session was idle at fire | ≤ 60 s |
| delivery | `generated_at` → first SENT `out.deliveries.at` | ≤ 10 s |

Items still without an event at the window end also count, with their age as a lower bound (`item_to_event_with_pending_lower_bound`), so a backlog cannot hide. Run it over the E2E window, over each 24 h of V0-A, and over the whole of V0-H.

## V0-A: 72 h unattended sources and market data

Start at the lead's go, with the stack untouched for the window:

```
RUN=/srv/tank/data/qa/v0a-$(date -u +%Y%m%dT%H%MZ); mkdir -p $RUN
python3 $A/v0a_health.py --snapshot $RUN/start.json
nohup sh -c "$TANK_COMPOSE logs -f --timestamps --since 0s tank | grep --line-buffered 'rest reads:'" \
  > $RUN/rest-reads.log 2> /srv/tank-work/logs/tank-acceptance/v0a-restlog.err &
```

- Sources: every feed keeps cumulative `reads` (one poll after retries) and `detail_reads` (article/detail fetches) counters in `ops.source_checkpoints`, written by tank-sources and surviving restarts. The window is end minus start, so nothing has to sample. `python -m tank.sources.read_report` reads the same counters and can cross-check.
- Each day and at 72 h: `python3 $A/v0a_health.py --start-snapshot $RUN/start.json --end-snapshot $RUN/end-dayN.json --rest-log $RUN/rest-reads.log --out $RUN/day-N`. Also run `latency.py` over the same window.
- Pass:
  - each of the 34 sources has reads, failed/(ok+failed) < 1% counting detail fetches, and its feeds were polled at ≥ 95% of their configured cadence (unattended, no stall);
  - BASE market path: < 1% failed REST reads per host, from the market service's per-minute `rest reads:` log lines (tank-market). Row coverage per instrument (1m klines, OI, funding as_of every 300 s) is a secondary check; Coinbase /candles omits trade-less minutes;
  - HOT path: streaming gap time < 1% of hot time.
  - The other V0-A clauses come from their owners: the X list holds exactly 86 accounts (tank-sources), the §2.10 meaning checks pass (tank-truth), and archive integrity holds for 5 tokens × 7 days (tank-market).
- Any restart, manual fix or config change inside the window restarts the 72 h.

## V0-H: 14-day shadow operation

Start at the lead's go, after V0-G has passed:

```
RUN=/srv/tank/data/qa/v0h-$(date -u +%F); mkdir -p $RUN; date -u +%FT%TZ > $RUN/started_at
systemd-run --unit=tank-acceptance-v0h-daily --on-calendar='*-*-* 00:30:00 UTC' \
  sh -c "python3 $A/shadow_daily.py prepare --run $RUN && python3 $A/shadow_daily.py metrics --run $RUN --start $(cat $RUN/started_at) --until \$(date -u +%FT00:00:00Z)"
systemd-run --unit=tank-acceptance-v0h-weekly --on-calendar='Mon *-*-* 01:00:00 UTC' \
  sh -c "python3 $A/shadow_weekly.py --run $RUN --week-start \$(date -u -d '7 days ago' +%F)"
```

- Daily (reviewer, cards of the previous UTC day):
  1. `days/<D>/review.md` lists each card with the L1–L12 / §7.6 checklist; `cards.jsonl` with `freeze-receipt.json` is the frozen copy.
  2. Label each card with `tank review label <alert_id> CATCH|LATE|FALSE_ALARM|NOISE --version V --reviewer R --notes …`. Label each MAJOR+ event without a card CORRECT_SILENCE or MISS.
  3. Fill `checks-template.json` with booleans, reviewer and reason, then run `shadow_daily.py merge --run $RUN --day <D+1>`.
  4. The next metrics run reads the cumulative `checks.json`. S1–S9 go to `metrics/S1-S9-until-<date>.{json,md}`.
- Weekly: fill `weeks/<start>/miss-audit-template.json`, with each major event from the covered buckets → CAUGHT or MISS + reason. `outcomes.jsonl` holds the realized moves; they are descriptive only, with no calibration.
- Configuration changes during shadow happen only as versioned config with a changelog entry in `$RUN/config-changelog.md`. Every alert keeps its `config_version`.
- Pass (01 §10 V0-H): 14 consecutive days with no gap in daily reviews; every card frozen and reviewed; weekly miss reviews done; S1–S9 measured and recorded with their status. S1–S9 targets: 01 §10 table.

## Status tracking

`/srv/tank-work/status/tank-acceptance.md` carries one row per run: started_at, evidence dir, latest daily result, blockers. The lead gets a message at each start, each failure that restarts a window, and each pass.
