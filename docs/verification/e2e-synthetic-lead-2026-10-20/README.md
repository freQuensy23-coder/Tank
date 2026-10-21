# Synthetic-event E2E on prod (lead, 20–21 Oct 2026)

All items below are synthetic: the lead wrote them for the E2E test, and none is real news. Each one went through the production path from the source item onward:
- The product's own RSS parser (`tank.sources.parsers.feeds`) parsed it.
- The raw store saved its bytes.
- `SourceService.ingest` ingested it inside `tank-tank-1`.

The injection scripts are in `scripts/`. Trace bundles come from `scripts/acceptance/e2e_trace.py`, which is read-only.

| Item | Synthetic headline | Public / known (UTC) | Build | Outcome |
|---|---|---|---|---|
| ITM-01M5DQV6NY21K1QHKG1AGD0CSW | Japan's GPIF approves first bitcoin allocation | 22:35 / 22:45 | r102 | Class MATERIAL, no M-criterion. No Market Truth and no card, as the spec requires. |
| ITM-01M5DR32N4W3QPYBB9J0B33JXW | South Korea's FSC approves first spot bitcoin ETFs | 22:36 / 22:49 | r102 | MAJOR. RUN_MARKET_TRUTH OOM-killed tank 4 times (22:54–22:58). |
| ITM-01M5DTYHP4C1VW6WAK9C07XKG0 | Thailand's SEC opens spot bitcoin ETFs to retail | 23:25 / 23:39 | r103 | Card ALR-01M5DVXXBJGHH8EYRJE5W5PDDD, Telegram msg 15. Data MEDIUM, market context n/a. |
| ITM-01M5DXR6RWRETE0QWPTFWZYS19 | Indonesia's OJK approves first spot bitcoin ETFs | 00:13 / 00:28 | r105 | Card ALR-01M5DYGXK92QZ368H9MAS78DF2, Telegram msg 16. See below. |
| ITM-01M5E3Q6HRG13QFJSAS34VA9XK | Philippine SEC approves first spot bitcoin ETFs | 01:57 / 02:12 | r107 | Agent session FAILED after a 6.7 MB `get_token_facts` reply (fixed in 82984dc). No card. |
| ITM-01M5E5SHY9Q3VHF9H1JT280CB4 | Vietnam's securities regulator approves first spot bitcoin ETFs | 02:34 / 02:49 | r109 | Card ALR-01M5E6F7FEY9YNNPHB5APZXVDF, Telegram msg 17. Wrong "Priced: HEAVY" from the degenerate BTC-vs-BTC benchmark (fixed in 134c380). |
| ITM-01M5EA037BTSKCNTCXMABGN5MW | Malaysia's Securities Commission approves first spot bitcoin ETFs | 03:46 / 04:02 | r110 → r111 | r110 stalled its event loop for up to 616 s and was OOM-killed at 04:14:51. Card ALR-01M5EBRXY9VJQJ0MCDHM2DNHHM, Telegram msg 18, came from r111. Data MEDIUM: bybit lost minutes in the r110 stalls. |
| ITM-01M5EDBQP49XS17T5JW8FD0WFZ | Saudi Arabia's CMA approves first spot bitcoin ETFs | 04:45 / 05:01 | r113 | Card ALR-01M5EE4DVJNMMTFR6PF84R7Y8T, Telegram msg 19. Data HIGH. See "Run on r113" below. |

## Run on r105 (main f21c727)

The chain passed all 12 trace checks with no operator step. The bundle is in `indonesia-r105/`.
- Cutoff 00:31, primary window EVENT [00:13, 00:31).
- All 18 minutes are complete on binance, bybit, coinbase, kraken and okx.

Card numbers against an independent read of the stored rows:

| Card line | Card | Independent |
|---|---|---|
| Spot flow | CVD 94.26958614, BUY, CONCENTRATED, 5 venues, 100% coverage | Σ(buy−sell) over `mkt.flow_bars_1m` = 94.269587; spot_truth formula gives CONCENTRATED BUY |
| Net buying venues | binance 25.6%, kraken 18.5%, okx 9.2% | 25.62%, 18.51%, 9.21% of 391.1386 BTC |
| Synchronized OI (2 venues) | +0.01% | (94,749.039 + 29,464.7235) / (94,736.589 + 29,468.4294) − 1 = +0.0070% |
| Binance OI | +0.01% | 94,749.039 / 94,736.589 − 1 = +0.0131% |
| Market context | NEUTRAL, BTC 24h +1.96% | binance 1m close 00:30: 83,190.20 / 81,593.82 − 1 = +1.956% |
| Delivery | — | `out.deliveries` SENT, message_ref 16, 00:42:07.06; DELIVER job DONE |

Spec limits missed:
- item → event took 133 s (limit 90).
- known → FULL card took 804 s (limit 480).
- Delivery after generation took 9.0 s, within the 10 s limit.

Open items shown on the card:
- **Synchronized OI normal range: n/a.** 03 §0 needs 7 days of 5-min OI history, and OKX has about 3 days.
- **Data MEDIUM.** Tickers were 60–170 s late while startup repair saturated the loop, and Bybit OI arrives every 5 min.
- **No "Priced" line.** No BTC spot venue has a complete trailing 24 h of trades yet.

## Fixes found by these runs (all merged on main and live in r105)

- **78638c3 (lead).** The Market Truth snapshot kept full book levels on every row: 399 MB of text, about 3.5 GB in memory per BTC snapshot, which caused the OOM loop. It now keeps levels only on the newest rows. Read-only re-run: peak 1.7 GB.
- **88e1fc5, a6b569a (tank-market).** Window repair now runs on demand, at escalation and at startup. A stored complete bar can no longer be replaced by an incomplete one without evidence of loss. The all-venue gaps at 22:34 and 23:00 came from out-of-process repair runs.
- **d230a90 (tank-truth).** The OI normal range is taken over the requested window length. OI USD comes from the ticker mark. Freshness counts only the token's own instruments.
- **f21c727 (tank-market).** A 25 h kline gap audit runs per series, which enables the market context.
- **b34e3cf (tank-events).** An UNKNOWN move with usable data now names the observed flow instead of "market evidence incomplete".

## Run on r113 (main ec12e1b): Saudi Arabia, Data HIGH

The chain passed all 12 trace checks with no operator step. The bundle is in `saudi-r113/`.
- Public time 04:45:00 is the first minute with a complete bar on all 5 BTC spot venues after the r113 restart (04:43:29). The window was checked complete minute by minute before injection.
- Injected 05:01:22. The agent's cutoff was 05:04, so the primary window is EVENT [04:45, 05:04), 19 minutes, complete on binance, bybit, coinbase, kraken and okx.
- Data Confidence HIGH with no failed condition, the first HIGH of these runs.

Card numbers against an independent recomputation (scripts `independent_oi.py`, `independent_relative.py` and SQL over the stored rows; none of them imports Tank code):

| Card line | Card | Independent |
|---|---|---|
| Spot flow | CVD 24.59892324, BUY, CONCENTRATED, 5 venues, 100% coverage | Σ(buy−sell) over `mkt.flow_bars_1m` = −0.864090 + 7.576415 + 2.044334 − 0.636557 + 16.478821 = 24.598923. Flow test: 2·√Σq² = 2·√75.575 = 17.39 < 24.60, so BUY. okx gives 67% of the net flow, and only 28.8% of the remaining volume (bybit) also buys, so CONCENTRATED (03 §5.2) |
| Net buying venues | bybit 24.4%, okx 15.2% | bybit 45.861/188.002 = 24.39%, okx 15.16%. Only these two pass the per-venue flow test: bybit 7.58 > 3.21, okx 16.48 > 8.84 |
| Synchronized native OI | −0.02%, WITHIN, class MIXED, 2 venues | binance 95,167.024 → 95,131.444 and okx 29,689.1195 → 29,705.7119, so 124,837.1559 / 124,856.1435 − 1 = −0.0152%. Bybit's nearest observations are 05:00 (240 s before the cutoff) and 05:05 (after it), so it is out of the cohort. Normal range over each past 19-min window's observed eligible venues, base tier sampled at 300 s: [−0.3110%, +0.3405%], identical to the stored range |
| Binance OI | −0.04% | 95,131.444 / 95,167.024 − 1 = −0.0374% |
| Priced | EARLY · EVENT 0.14% raw (BTC is the benchmark) | binance 1m closes 82,891.70 (05:03 bar) / 82,777.66 (04:44 bar) − 1 = +0.13777%. Over 105,386 past 19-min windows: range [−0.4039%, +0.4235%], pctile 0.8254, upper quartile +0.0905%. Above the upper quartile and within the range is EARLY |
| Market context | NEUTRAL, BTC 24h +1.00% vs normal −2.80% to +6.53% | 82,891.70 / 82,073.15 (05:03 bar a day earlier) − 1 = +0.997%. Hourly 24h returns over 90 days: [−2.80%, +6.53%] |
| Delivery | — | `out.deliveries` SENT, message_ref 19, 05:15:10.10. DELIVER job JOB-01M5EE4H4F12XAQ9MEXZC2VMP7 DONE |

Spec limits missed (01 §9):
- item → event took 113 s (limit 90): filter 24 s, route 27 s, then the agent; its first `get_market_truth` was refused for cutoff_prec "MINUTE".
- known → FULL card took 804 s (limit 480). RUN_MARKET_TRUTH alone took 8 min 21 s, while the escalation backfill refilled the restart gaps in the −24 h window and the reaction-capture backlog ran.
- Delivery after generation took 23.1 s (limit 10). publish_card stamps generated_at 19 s before the DELIVER job becomes runnable.

Open on the card:
- "CONFIDENCE: LOW — main unknown: none recorded". Confidence is LOW because the move class is UNKNOWN (§6.5), but the main unknown names only a Data Confidence condition, and Data is HIGH.
- Scores M and S are null with the agent's reasons. These are agent judgments; "VERIFIED: n/a" is expected for a claim that cannot be confirmed.

## Fixes found by the later runs (r106–r113)

- **c77a80f (lead).** The synchronized OI history counts each past window over the venues observed at both of its bounds, per measure. The BTC 3-venue normal range had been n/a because OKX history starts on 18 Oct.
- **714e556 (lead).** The BTC Priced line names the benchmark instead of "n/a net BTC".
- **a80692f (lead).** tank serve sets a 0.5 ms GIL switch interval, so worker threads cannot starve the event loop's socket reads.
- **d2917f0 (lead).** Funding confirmations use a time index. The pairwise parse_time scan was quadratic on the event loop (21% of MainThread in the r111 profile).
- **134c380 (tank-truth).** BTC is judged by r_raw against its own history; there is no fake HEAVY band.
- **81183e1 (tank-truth).** One full Market Truth computation at a time.
- **96972bd (tank-truth).** Reaction captures read only their checkpoint windows: BTC window rows dropped from 72.7k to 26.0k.
- **31aec9f (tank-ops).** At most one REACTION_CAPTURE runs at a time; other job types never wait behind it.
- **ae62bb5 (tank-events).** The reaction plan reads revision metadata only. It no longer decodes a 2 MB Market Truth twice on the loop.
- **ec12e1b (tank-events).** `get_token_facts` returns the covered daily volume (the 03 §4.2 base of M).
- **c42b195, 791c586, c282b04, eb3b67e (tank-market).**
  - Seals run outside the flush, and the HOT ticker is polled first.
  - Books go on an 8 s grid.
  - A loop watchdog was added, and repair, late scans and snapshot reads now yield.
  - Book levels are read only for walked rows, and wide rows in 500-row pages.
