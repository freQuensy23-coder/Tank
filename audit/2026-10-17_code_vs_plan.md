# Code vs plan — audit of 17 Oct 2026

Four read-only reviews compared `src/`, `config/` and `deploy/` with 01 PRD, 02 HLD and 03 TDD: (1) service, jobs, storage and deployment; (2) sources, X sensor, filter and routing; (3) OpenMA sessions, tools and skills; (4) market data and analysis. Snapshots were taken between 20:16 and 20:52; other agents kept editing, so line numbers refer to those snapshots. Findings come from reading the code; only the Coinbase confidence case (4.A1) and the card percent format (4.A3) were reproduced by running code.

## The plan changed while the review ran

The code must follow these changes (committed in `468d3f9` on `docs/plan-updates`):

1. **Docker Compose** on any Linux host with KVM: containers `tank`, `openma`, `postgres`; Docker secrets; volumes `tank-data`, `tank-browser`, `codex-auth`, `pg-data`, `backup`. `deploy/` still has launchd plists and Keychain items.
2. **`codex_gateway`** module in the Tank service: OpenMA's Model Card for `gpt-6.1-sol` is a custom provider (`pi_config.api: openai-responses`) with `base_url` `http://tank:8790/v1`; the gateway signs calls with the ChatGPT (Codex) sign-in from `codex-auth` and refreshes it.
3. **No token or spending limits in V0**: remove the budget pause, the LLM_BUDGET notice, `ops.budget_state` and FR-P-03. Tank Ops keeps DELIVERY_FAILED and HEARTBEAT.

Findings that these changes make moot are marked *(moot)*.

## Already matching the plan

- One asyncio process, 8 job workers, `FOR UPDATE SKIP LOCKED` by (priority, run_after) with per-job timeouts; no NOTIFY/LISTEN, no job heartbeat column; one polling loop per source.
- No health supervisor, anomaly detector, clock check or SOURCE DOWN notices.
- `mkt.hot_tokens` with the agreed columns, cap 15, hot only through `get_market_truth`, §9 demotion.
- 34 sources at the HLD intervals (except the forums, 2.A4), 22 tokens, 181 instruments, 86 X accounts (41 / 37 / 8) mapped to the right tokens; routing paths and the 2/1 priorities; Jev cutoffs null with no invented defaults; the ROUTE question text; one Jev call per candidate.
- Base tier (klines 60 s, OI/funding 300 s, tickers); Binance `@aggTrade`; hot OI/funding every 60 s; §0 conventions without "record"; Data Confidence without feed-DOWN or clock rules; the §5.7 context rule; A0–A12/B1–B4, `L_inv`, 7-day call expiry, 15-min update limit, 3,500 characters / 3 blocks; fixtures F-01–F-27; replay without network, bounded by the cutoff.

## Blockers

| # | Area | Problem | Where |
|---|---|---|---|
| B1 | Service | REACTION_CAPTURE jobs created by `set_assessment` expire 6 h after creation, so +24 h/+72 h never run; the scheduler cannot replace them (same key). Hot-set demotion and MONITORING→COOLDOWN→CLOSED stall | storage.py:61-72, event_tools.py:961-976 |
| B2 | Service | Two MILESTONE_CHECK jobs per milestone (different keys): the agent gets MILESTONE twice | scheduler.py:55, event_tools.py:1517-1527 |
| B3 | Service | Unplanned PUBLISH_PARTIAL job publishes the STRUCTURAL PARTIAL card from code and races the agent's own `publish_card` | runtime.py:76-97, event_tools.py:986 |
| B4 | Service | The nightly backup runs inside the scheduler tick and blocks it; with `TANK_LOGS_DIR` unset it raises after the dumps, so backup, retention and partition creation never finish | scheduler.py:84, 326-409, 373 |
| B5 | Service | Each tick queries every event one by one and scout hooks load up to 1M rows; `limit=100000` ordered by key drops the newest events | scheduler.py:52-79, 228-289, 474-492 |
| B6 | Service | Market Truth runs on the event loop; EVAL_CONDITIONS recomputes it for every armed token every minute and stores each result permanently | scheduler.py:198, truth.py:727 |
| B7 | Storage | Raw trades are immutable PostgreSQL rows that retention never deletes; raw capture writes one Parquet file per WebSocket frame | 003_capture.sql, storage.py:705, archive.py:21-40 |
| B8 | Pipeline | `tank serve` will not start while the Jev cutoffs are null, and routing needs the cutoff before it looks for candidates, so V0-A cannot run and no NEW_ITEM is ever created | runtime.py:58-60, pipeline.py:122,167 |
| B9 | Sources | A media-only post or an empty quoted post stops the X parser; the S29 checkpoint never advances and later posts are lost. The S29 "raw" file is parsed JSON, not the page | sources/__init__.py:167-168, 271, 436; browser.py:99,142 |
| B10 | Sources | The Uniswap, Aave, Ethena, CoW and Morpho forums are polled every 600 s instead of 300 s | sources.yaml:488-502 |
| B11 | Tools | `schedule_milestone` needs a `reg.sources` row, but `sources.yaml` is never loaded there; exchanges and regulators have no official sources in the registry. No milestone check or MAJOR+ escalation of S01–S14 ever happens | event_tools.py:1513, registry.yaml:4034-4105 |
| B12 | Sessions | A refused tool raises instead of answering the agent with the reason; the call is re-run forever and the event stalls | sessions.py:267,320,559 |
| B13 | Tools | A PRIMARY edge counts as credible only from DEPLOYED, so APPROVED and SIGNED events get Thesis D → WAIT (the plan allows A from APPROVED) | event_tools.py:1028-1029 |
| B14 | Truth | Coinbase's REST semantics row is never proven, so Data Confidence is LOW whenever Coinbase is a complete venue (21 of 22 tokens): START is never possible | truth.py:677, semantics.yaml:119, service.py:1439 |
| B15 | Truth | Extra Data Confidence conditions (derivatives readiness, reference price, capture prerequisite) keep it from HIGH and change the main-unknown order | truth.py:690,711,891 |
| B16 | Cards | Fractions are printed with a % sign (+7.05 % shows as "0.0705%"); excluded venues and their reasons are never printed (hard rule 9) | cards.py:191-196 |

## 1. Service, jobs, storage, deployment

**Mismatches**
1. High — B1.
2. High — B2.
3. High — B3.
4. High — B4 (nightly backup inside the tick).
5. High — B4 (`Path(None)` when `TANK_LOGS_DIR` is unset).
6. High — B5.
7. High — B6.
8. High — B7.
9. Medium — deploy/read_only_role.sql — `ALTER ROLE tank_agent SET default_transaction_read_only = on` is missing.
10. Medium — prepare_openma.py:17-18, generated/*.json — neither environment sets `TANK_DB_URL` or the Etherscan/Tavily/TypeSafe keys; `tank-replay` has no replay-database URL; no image is set; secrets are deferred to a vault although the plan has none.
11. Medium — storage.py:570-576 — on start, a RUNNING job on its last attempt is marked FAILED instead of QUEUED.
12. Medium — delivery.py:75-79 — DELIVERY_FAILED fires only when the fifth HTTP attempt fails; timeouts, restarts, validation errors and job expiry send no notice and leave no FAILED ledger row.
13. Medium — scheduler.py:52-73, storage.py:821-865 — MILESTONE_CHECK, REACTION_CAPTURE and PUBLISH_PARTIAL jobs are rebuilt from all past events; once the 30-day cleanup deletes the old keys they can be created again (stale MILESTONE, a PARTIAL card weeks late).
14. Medium — scheduler.py:480 — TRACKED→CLOSED needs `next_milestone` empty, but it is never cleared.
15. Medium — scheduler.py:183-185 — MILESTONE carries no outcome and no quote from the official source.
16. Medium — scheduler.py:339-343, 404-407, storage.py:652-684 — retention and new daily partitions run only after a verified backup; with a failing backup, capture falls into the DEFAULT partition.
17. Medium — scheduler.py:372-397 — each night copies all of `data/`, `config/` and `logs/` with no rotation instead of `pg_dump` + `bundles/`.
18. *(moot: limits removed)* — sessions.py:60-73 — unknown-cost usage rows pause deferrable work; the budget check loads up to 1M rows.
19. Medium — B8.
20. Medium — settings.py:47-74, cli.py:49 — `config_version` is a content hash taken once at start, not the config commit; SIGHUP is not handled.
21. Medium — migrations 001, 006 — tables are `storage_key` + JSON document with nullable copied columns; the §14 NOT NULL constraints, keys and bigserial ids are not enforced; the document, not the typed column, is the source of truth.
22. Low — scheduler.py:9,73, event_tools.py:951-976 — PRE_24H/4H/1H windows run as retro REACTION_CAPTURE jobs; the token set is all beneficiary edges, not the analysis set; `rwp_id` is `event:token`.
23. Low — service.py:679, scheduler.py:293-306 — `hot_tokens.expires_at` is always anchor + 72 h (should be null while a call or condition is open); the cap is applied per row; calls and conditions are checked per token, not per event.
24. Low — event_tools.py:1146-1160, sources/__init__.py:237, service.py:793-798 — ESCALATE_TOKENS priority 1 (plan 0) and key `event:call`; the RUN_MARKET_TRUTH key uses the tool-call id instead of the cutoff minute; max_attempts 3 instead of 2; `market` also queues RUN_MARKET_TRUTH and EVAL_CONDITIONS after a backfill.
25. Low — scheduler.py, settings.py — the 7-day call expiry, the 08:00 heartbeat, cap 15 and the retention periods are hard-coded or env settings, not read from `thresholds.yaml`.
26. *(partly moot: Docker secrets)* — `delivery.yaml` chat IDs are never read; the example env lacks `TANK_BACKUP_DIR` and `TANK_LOGS_DIR`; the User-Agent format differs from `TankV0/1.0 (<email>)`.
27. Low — prepare_openma.py:15 — no `harness: "pi"`; effort hard-coded instead of read from `openma.yaml`.
28. Low — delivery.py:96-97, util.py:40 — the first delivery skips ALERTED; id prefixes differ from the plan.
29. Low — storage.py:579-617, 685-709 — MAJOR+ data is kept row by row for the event tokens plus BTC/ETH, not whole partitions.

**Not implemented yet:** new revisions of reaction checkpoints for late data (§8.6); config reload on SIGHUP; network access for the sandboxes to PostgreSQL; the `tank_agent` statement timeout and connection limit (deferred to V0-G); Docker Compose deployment and `codex_gateway` (plan changes above).

## 2. Sources, X sensor, filter and routing

**Mismatches**
1. High — B8.
2. High — B9 (media-only or empty quoted posts).
3. High — B9 (S29 raw file).
4. High — B10.
5. High — B11 (`schedule_milestone` needs `reg.sources`).
6. High — B11 (registry without official sources for Binance, Coinbase, OKX, Bybit, Kraken, Robinhood; no entities for SEC, CFTC, Fed, OCC, Treasury/OFAC, FinCEN, FDIC).
7. Medium — browser.py:68-73,103, sources/__init__.py:184-195 — a repost is stored under the original author, so organization reposts go to FILTER_ITEM and reposts by the 8 other accounts get priority 1.
8. Medium — browser.py:107-118 — the checkpoint is one status ID; if that post is deleted, every poll scrolls for minutes holding the browser lock S19 also needs, then fails.
9. Medium — sources/__init__.py:406-419 — S09 re-fetches all 60 article details every 30 s (no seen check, no conditional GET): about 120 extra requests a minute.
10. Medium — pipeline.py:11 — the FILTER question drops "one of these projects:"; it still asks only "about crypto".
11. Medium — pipeline.py:47,149 — Jev text and entity matching leave out the item title.
12. Medium — pipeline.py:154-159, registry.yaml:3646-3648, 3839-3840 — bare slugs and tickers (world, near, curve, quant, cow, lit) match without the required qualifier; the Lighter and Plasma qualifiers never match; `token_id` is not added, so a shared token alone cannot make a candidate.
13. Medium — pipeline.py:72-74, 207-225 — an unplanned "routing reservation" adds non-event candidates and a third Jev question judged against the `same_event` cutoff.
14. Medium — sources/__init__.py:454-462 — S02 registry match is a raw substring ("near" in "linear"), ignoring entity names.
15. Medium — sources.yaml:145-179 — DTCC is read through a Coveo API with a token scraped from a content-hashed Next.js file that changes on any DTCC deploy.
16. Low — FILTER_ITEM and ROUTE_ITEM get 3 attempts instead of 2.
17. Low — no separate `xsensor` module (S19 and S29 are ordinary loops sharing one browser); Jev called with raw `httpx` instead of `typesafe-sdk`; raw files are gzip, not `.zst`.
18. Low — the 3-day window and the 15 s interval are hard-coded; `routing_window_days` and `escalated_interval_s` are unused.
19. Low — JSON and HTML index sources re-ingest every listed entry on each poll, so `observation_count` counts polls.
20. Low — date-only S03/S08 dates are marked EXACT; Jev `author` for news is null instead of the outlet.
21. Low — the §2.1 source fields (bucket, source_class, cost_class, auth, adapter_status) are missing.
22. Low — candidates are read with `LIMIT 100000` ordered by key; past 100k events the newest drop out.
23. Low — the Robinhood S13 host and path moved; recorded only in `sources.yaml`.

**Not implemented yet:** per-source rate budgets from `limits` and `api_limits.yaml` (shared sec.gov budget); loading `sources.yaml` and `registry.yaml` into `reg.*`; owner confirmation of the 86 handles; S24 `end` scheduling a milestone; a command to score items with Jev for the V0-B calibration.

## 3. OpenMA sessions, tools and skills

**Mismatches**
1. High — B12.
2. High — B13.
3. High — B11.
4. Medium — sessions.py:525-586, event_tools.py:524 — every open session is followed forever (SSE plus a full history list every 10 s), not only while the agent works; sessions whose item had no event never end.
5. Medium — sessions.py:237-239, 414-422 — the retry re-sends the latest posted message, not the failed turn's; each new message resets the retry count.
6. *(moot: limits removed)* — unknown-cost usage rows pause deferrable work.
7. Low *(budget gone; logging still useful)* — `web_search` does not log Tavily usage to `ops.llm_usage`.
8. Medium — sessions.py:302-315 — replay answers every tool, verdict tools included, from stored results matched on exact arguments instead of running the tool code; any difference hangs the session.
9. Medium — event_tools.py:415-421 — quotes are accepted only from the current item or this session's web pages; quotes from other Tank items found with `search_tank_items` are dropped.
10. Medium — event_tools.py:460-475 — LIVE+ claims without the lexicon become DEPLOYED instead of the stage the text supports; the "instrument actually trades" path is missing.
11. Medium — event_tools.py:876 — the C1 headline-only cap (≤ 3) never applies, because the schema always requires the objects it checks for.
12. Medium — event_tools.py:1043-1050, 1109-1117 — layer status is only FOUND or UNKNOWN; non-credible-only layers need no reason; `no_credible_secondary_reason` is never set; exposure scores are not checked.
13. Medium — event_tools.py:1351-1358, cards.py:186 — token blocks are the top 3 edges, not limited to analysis-set tokens with decisions; a block without a decision prints a default WAIT.
14. Medium — cards.py:49-50, event_tools.py:706 — `call_expired` is never set (the end-of-call update can be refused); `milestone_resolved` is never cleared.
15. Medium — scheduler.py:183-185 — MILESTONE has no outcome and no official quote.
16. Medium — prepare_openma.py:15 — no `harness: "pi"` and no `{name: browser, enabled: true}`, so browser is off; effort hard-coded.
17. Medium — prepare_openma.py:17-18, 30 — environments without `TANK_DB_URL`, the keys, the replay database URL or an image (see 1.10).
18. Medium — record_review.schema.json, event_tools.py:1531,1566 — `record_review` has no overlap/observability/reusability/decision-value scores, no recommendation enum, no duplicate-of; duplicates are found by exact name.
19. Medium — tank-event-analyst/SKILL.md:15 — item 7 cites the speed limits of 01 §9 without stating them (3 min PARTIAL, 8 min FULL, 60 s update).
20. Medium — tank-data/references/DATA_GUIDE.md — no Tavily or TypeSafe endpoints or key usage; no rule that the sandbox shares venue limits with the service.
21. Low — sessions.py:143-160,213 — a global lock makes every new session wait for a full paginated session list.
22. Low — `ops.tool_calls` has only PENDING/DONE; `llm_usage` records no effort, stop reason or web searches.
23. Low — agent, prompt and skill versions are fixed strings, not the deployed OpenMA versions.
24. Low — FAILED is written to `evt.events.status`, not to the lifecycle.
25. Low — the linter's "rewrite once" counts any earlier refusal in the session.
26. Low — a second `record_event` in one session creates another event.
27. Low — the new-card gate refuses INVALIDATED events but not CLOSED or MERGED ones.
28. Low — event_tools.py:809 — class MAJOR passes with criterion MATERIAL (prefix check on "M").
29. Low — B3.
30. Low — the scout agent also gets `tank-data` and the full sandbox toolset; both skills open with the TDD paragraph pasted verbatim.

**Not implemented yet:** OpenMA deployment (agent, scout agent and environment ids are null; no Model Card — now the `codex_gateway` custom provider); replay in a fresh `tank-replay` session; tool descriptions in `tools.json` are placeholders; effort calibration and `tank_agent` limits (V0-G).

## 4. Market data and analysis

**Mismatches**
1. High — B14.
2. High — B15.
3. High — B16 (percent format).
4. High — B16 (excluded venues not printed).
5. Medium — cards.py:112,220 — no "Data HIGH/MEDIUM/LOW" in the header; full ISO times instead of hh:mmZ; main unknown prints only the reason code.
6. Medium — service.py:1439-1440 — a metric without a current proof is set to UNKNOWN, not DOCUMENTED_UNTESTED, so T8 gives LOW instead of MEDIUM.
7. Medium — normalize.py:203-209, truth.py:237 — Coinbase and Kraken klines have no quote volume, so the spot-vs-perp share of new volume is always null and mixes USDT with USD.
8. Medium — event_tools.py:1186 — RUN_MARKET_TRUTH waits only for ESCALATE_TOKENS; the −24 h backfill runs in the background, so the FULL card's Market Truth is computed before the pre-event windows are filled.
9. Medium — truth.py:324 — mirroring the repricing band for LOSER tokens needs `expected_direction`, which nothing sets.
10. Medium — truth.py:36-37, cards.py:60,224, scheduler.py:332 — ⚙ values are hard-coded; `thresholds.yaml` is not read by truth, decisions, cards or demotion.
11. Medium — service.py:430-468, streams.py:525 — no "no message for 30 s" check: a quiet open socket writes zero-trade minutes as complete; on disconnect only bars still in memory are marked incomplete.
12. Medium — truth.py:500-524,727 — REAL_MARKET_FRAGMENTATION needs a book sample in every 10 s bucket plus REST confirmation instead of every venue complete for the window; the funding-dispersion route is missing.
13. Medium — B7.
14. Medium — service.py:1110 — `self_test` is never called; semantics are never set back to UNKNOWN when a venue changes.
15. Medium — service.py:387-420 — each 10 s book summary queues a REST confirmation on the venue lock; three hot Kraken markets exceed Kraken's 1 request/s and delay base klines.
16. Low — streams.py:381 — every reconnect re-fetches 24 h of trades by time instead of backfilling from the last trade ID.
17. Low — service.py:757,792 — the backfill anchor is always `public_at` (no T0 / `known_to_us_at` rule).
18. Low — flow.py:27 — trade-ID continuity is checked only for Binance, Coinbase and Kraken; OKX and Bybit checks are missing.
19. Low — decisions.py:344-402 — the perp-led, vacuum and heavy-repricing DON'T CHASE conditions reuse the generic downgrade; "1h close below anchor → WAIT" exists only as an invalidation.
20. Low — truth.py:743 — `class_confidence` equals the Data Confidence class; flags never include NOT_COHORT_CONSISTENT, PRE_EVENT_RUNUP or FRAGMENTATION_INVESTIGATE.
21. Low — cards.py:280 — linter rule L12 reads `divergence['trigger']`, truth writes `'triggered'`.

**Not implemented yet:** index price capture (basis always null); perp taker flow and per-venue perp volume; hourly Parquet files; archive re-download after a checksum mismatch; fragmentation via divergent top venues; labelled treasury/MM/unlock wallets (`supply_in_evidence` never fires); the remaining §5.8 support terms; replay in a fresh agent session and the name-masking map; `tank coverage` regenerating the §4.10 matrix; fixture packets (all 27 are `INCOMPLETE_PACKET`).

## Deviations already listed in `docs/IMPLEMENTATION_STATUS.md`

- F-16: §5.7 kept; STRESS against the fixture's HEADWIND.
- F-11–F-14: OI from saved REST snapshots; the archive only for normal ranges.
- Job uniqueness keyed on type + key.
- The $30 budget is soft *(moot: limits removed)*.
- Denial/EXIT updates on an existing card publish even when the event is invalidated.
