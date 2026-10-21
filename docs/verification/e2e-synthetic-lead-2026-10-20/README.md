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
| ITM-01M5EH88DZ6044Q9RJ5HA5M5PS | UAE's SCA approves first spot bitcoin ETFs for ADX | 05:53 / 06:09 | r114 | Card ALR-01M5EHQS83VWVJMNF96GTBZP7Q, Telegram msg 20. Data HIGH. See "Run on r114" below. |
| ITM-01M5EKPSE8YVTW0BC8C7PD51Q1 | Qatar's QFMA approves first spot bitcoin ETFs for QSE | 06:36 / 06:52 | r115 | Card ALR-01M5EM2M4BCS5X195FTFA9GWK2, Telegram msg 21. All three speed limits met, but false REAL_MARKET_FRAGMENTATION and Data MEDIUM. See "Run on r115" below. |
| ITM-01M5EP3XSA2FRWNC8906AN43ZB | Kazakhstan's AFSA approves first spot bitcoin ETFs for AIX | 07:17 / 07:34 | r116 | Card ALR-01M5EPHFTXFMTQPNXFWMBR1FZX, Telegram msg 22. Data HIGH, all speed limits met, no fragmentation, but the wrong move class PRICE_MARKUP_FAKE_OI. See "Run on r116" below. |
| ITM-01M5ES7BS9ZJ7PW3VM4JPEAJCT | Chile's CMF approves first spot bitcoin ETFs for Santiago Exchange | 08:11 / 08:28 | r117 | Card ALR-01M5ET2A02DMJ1AANRMKXK9JR0, Telegram msg 25. Numbers correct, but item → event 139 s, FULL card 883 s and Data MEDIUM (supply freshness). See "Run on r117" below. |
| ITM-01M5EWK3K1YEFRWYHFHSVZ8RJV | Kenya's CMA approves first spot bitcoin ETFs for NSE | 09:10 / 09:27 | r118 | Card ALR-01M5EWYX8MAE9Y0YD2SBZVFMFQ, Telegram msg 26. **Final run:** every number verified, Data HIGH, all three speed limits met. See "Run on r118" below. |

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

## Run on r114 (main 49e0434): UAE, item → event within its limit

All 12 trace checks passed with no operator step. The bundle is in `uae-r114/`.
- Public time 05:53:00 is the first complete minute after the r114 restart (05:51:31). Injected 06:09:23. Cutoff 06:11, so the window is EVENT [05:53, 06:11), 18 complete minutes on all 5 venues.
- The outage backlog of reaction captures was held back during the run, each kept ahead of its 6 h expiry (marker lead_deferred_e2e_9). New captures still ran.

Independent recomputation (same scripts):

| Card line | Card | Independent |
|---|---|---|
| Spot flow | CVD −34.86993699, SELL, CONCENTRATED, 5 venues | Σ(buy−sell) = 0.453700 − 0.023123 − 11.246930 − 1.891725 − 22.161858 = −34.869936. 2·√129.744 = 22.78 < 34.87, so SELL. okx gives 63.6% of the net flow, and only 10.6% of the remaining volume also sells |
| Net selling venues | okx 18.2%, coinbase 8.6% | 45.972/251.924 = 18.25%, 21.762/251.924 = 8.64%. kraken 1.892 < 2.005, so NEUTRAL |
| Synchronized native OI | +0.01%, WITHIN, MIXED, binance+okx | 124,829.332 / 124,812.5336 − 1 = +0.0135%. Normal range [−0.2948%, +0.3260%], identical to the stored range |
| Binance OI | −0.00% | 95,095.355 / 95,096.247 − 1 = −0.0009% |
| Priced | UNPRICED · EVENT 0.02% raw (BTC is the benchmark) | +0.019498%. Range [−0.3936%, +0.4115%], pctile 0.5658, under the upper quartile +0.0878%, so UNPRICED |
| Market context | NEUTRAL, BTC 24h +1.20% vs −2.80% to +6.53% | +1.2033%; hourly 24h range [−2.80%, +6.53%] |
| Confidence | MEDIUM — main unknown: Event certainty MEDIUM | min(Data HIGH, certainty MEDIUM, class MEDIUM: FRAGMENTATION unresolved) = MEDIUM; certainty is first at the minimum (§6.5) |
| Delivery | — | `out.deliveries` SENT, message_ref 20, 06:18:08.97 |

Timings: item → event 63.5 s (limit 90, met). known → FULL card 508.7 s (limit 480, missed by 29 s): RUN_MARKET_TRUTH took 5 min 23 s, and publish_card took 22 s. Delivery 17.3 s (limit 10, missed): the DELIVER claim waited 7.7 s behind a blocking advisory lock held by a transaction on a busy loop.

## Run on r115 (main ea26109): Qatar, speed limits met, two card defects found

All 12 trace checks passed. The bundle is in `qatar-r115/`. The window was EVENT [06:36, 06:53), complete on all 5 venues.

Timings, all within 01 §9:
- item → event 42.1 s (limit 90)
- known → FULL card 387.8 s (limit 480); RUN_MARKET_TRUTH took 4 min 32 s
- delivery 1.33 s (limit 10)

The spot line (CVD 13.39786147, NEUTRAL, DIVERGENT; binance net buying 38.4%) matches the stored bars: Σ(buy−sell) = 13.397862, and 2·√343.2 = 37.05 > 13.40.

Two defects on the card, so this run is not the final one:
- **False REAL_MARKET_FRAGMENTATION.** Every flagged pair crosses quote assets: binance, bybit and okx (USDT) against coinbase and kraken (USD). The fragmentation test compared raw book mids without converting USDT to USD (truth.py, liquidity fragmentation), so the USDT/USD basis of a few bp exceeded BTC's ~0.1 bp spreads. Run #6's "Unresolved: FRAGMENTATION" had the same cause.
- **Data MEDIUM.** Hyperliquid's REST 24h ticker group (PUBLIC_REST, kind empty) landed at 06:50:20, 06:51:00 and 06:53:20 against a 60 s cadence, so at the 06:53 cutoff it was 119.5 s old. Its WebSocket mark rows every 10 s were fresh.

## Run on r116 (main f08b328): Kazakhstan, Data HIGH, move class blocked by depth gaps

All 12 trace checks passed with no operator step. The bundle is in `kazakhstan-r116/`.
- r116 started at 07:16:25. The 07:16 minute was incomplete on every venue (the restart minute), and 07:17–07:32 were complete on all 5, so public time is 07:17:00. Injected 07:34:24. The agent's cutoff was 07:36, so the window is EVENT [07:17, 07:36): 19 complete minutes on all 5 venues.
- Data Confidence HIGH with no failed condition. No fragmentation pair in any window.

Timings, all within 01 §9:
- item → event 57.9 s (limit 90)
- known → FULL card 444.5 s (limit 480); RUN_MARKET_TRUTH took 4 min 38 s
- delivery 5.76 s (limit 10). Telegram msg 22 at 07:41:54.02; DELIVER job JOB-01M5EPHGAMP70MV9NAGNF50W00 DONE

Independent recomputation (same scripts, plus `independent_oi.py` on `oi_usd` for the USD leg):

| Card line | Card | Independent |
|---|---|---|
| Spot flow (decisive fact) | CVD 247.94666747, BUY, BROAD, 5 venues | Σ(buy−sell) = 76.677600 + 12.190516 + 24.842911 + 6.167505 + 128.068135 = 247.946667. 2·√Σq² = 42.28 < 247.95, so BUY. okx gives 51.7% of the net flow, but all of the remaining volume also buys, so it is not CONCENTRATED. BROAD: all volume agrees and the largest venue agrees |
| Net buying venues | okx 52.5%, binance 25.2%, bybit 15.2%, coinbase 5.7%, kraken 1.3% | 499.988/951.799 = 52.53%, then 25.23%, 15.16%, 5.74%, 1.34%. Every venue passes its own flow test (kraken 6.17 > 3.05) |
| Synchronized native OI | +0.15%, WITHIN, class PRICE_MARKUP, 2 venues | binance + okx 124,772.6911 → 124,953.6955 = +0.1451%. Normal range [−0.3017%, +0.3137%], identical to the stored range. USD 10,317,598,201.93 → 10,382,996,740.81 = +0.6339%, above the USD range, so PRICE_MARKUP (03 §5.4) |
| Binance OI | PRICE_MARKUP +0.08%, position share 13.8% | 95,113.782 → 95,190.356 = +0.0805%. Position share 0.0805 / 0.5842 = 13.8% of the USD change |
| Priced | SUBSTANTIAL · EVENT 0.51% raw (BTC is the benchmark) | binance closes 83,219.99 (07:35 bar) / 82,800.00 (07:16 bar) − 1 = +0.5072%. Range over 105,538 past 19-min windows: [−0.4038%, +0.4233%], pctile 0.9845. Beyond the window range and within the 24h range, so SUBSTANTIAL |
| Market context | NEUTRAL, BTC 24h +1.97% vs −2.80% to +6.53% | 83,219.99 / 81,616.01 − 1 = +1.9653%. Hourly 24h range [−2.80%, +6.53%] |
| Confidence | MEDIUM — main unknown: Event certainty MEDIUM | min(Data HIGH, certainty MEDIUM, class HIGH) = MEDIUM, limited by certainty |

Defects on the card, so this run is not the final one:
- **Wrong move class.** The card says PRICE_MARKUP_FAKE_OI, a fallback that needs "no other class eligible" (03 §5.8). SPOT_LED_ACCUMULATION met every condition except `not weak_response`, and weak_response was null for two reasons:
  - The net flow notional was null: the bybit and okx spot 07:32 bars had no USD value (HISTORICAL_QUOTE_USD_RATE_UNAVAILABLE). Over 3 days, 2463 BTC spot bars lost their USD value this way.
  - OKX `books` (400 levels) and Bybit `orderbook.200` cover only 0.07–0.31% of the BTC book. Their 1% band is null (03 §5.5), and every cross-venue depth aggregate required all venues.

  Coinbase, kraken and binance alone carry $74.0M of ask depth within 1%, against about $20.7M of net buying, so weak_response is False and the class is SPOT_LED_ACCUMULATION.
- **Raw decimals.** The price-move fact printed its normal range as raw decimals: "[-0.0040379591952467205, 0.00423267670536415]".
- **Contradicting outlook.** PRICE_MARKUP_FAKE_OI reused the PERP_LED_CHASE outlook: "Chase risk persists while spot does not confirm", printed with spot BUY BROAD.

## Run on r117 (main 76bb27c): Chile, correct numbers, too slow, supply stale

All 12 trace checks passed. The bundle is in `chile-r117/`.
- r117 started at 08:10:03. 08:10 was incomplete (the restart minute), and 08:11–08:26 were complete on all 5 venues, so public time is 08:11:00.
- Injected at 08:28:42, after the real BTC/THB and CAKE Market Truth jobs had finished. Cutoff 08:33, so the window is EVENT [08:11, 08:33).

Every number on the card matches the independent recomputation:

| Card line | Card | Independent |
|---|---|---|
| Spot flow | CVD 57.71188523, BUY, DIVERGENT, 5 venues | Σ(buy−sell) = 57.711885, and 2·√Σq² = 37.79, so BUY. binance is NEUTRAL (19.27 < 32.09) and bybit SELL. The buying venues carry 35.6% of the volume and the largest venue (binance) is NEUTRAL, so it is neither CONCENTRATED nor BROAD: DIVERGENT |
| Net buying / selling | okx 16.9%, coinbase 14.9%, kraken 3.8% / bybit 23.8% | 16.91%, 14.91%, 3.81% / 23.81% |
| Synchronized native OI | +0.17%, WITHIN, MIXED, binance+okx | 126,219.42 → 126,434.0947 = +0.1701%. Range [−0.2996%, +0.5101%] |
| Binance OI | MIXED +0.20% | +0.1960% |
| Priced | EARLY · EVENT 0.11% raw | +0.1076%. Range [−0.4327%, +0.4560%]; upper quartile +0.0979%; pctile 0.7672 |
| Market context | NEUTRAL, +2.09% | +2.0869%; hourly 24h range [−2.80%, +6.53%] |
| Move class | UNKNOWN, confidence LOW | Spot BUY but DIVERGENT: no class of 03 §5.8 is eligible. The outlook names the observed flow |

Defects, so this run is not the final one:
- **item → event 139 s (limit 90).**
  - Routing took 24 s.
  - The agent's tools took 10 s, 6 s, 11 s and 8 s; on r116 they took 1–3 s.
  - LLM turns took about 75 s, against about 22 s in #8.
- **known → FULL card 883 s (limit 480).** RUN_MARKET_TRUTH took 9 min 20 s; #8 took 4 min 38 s. The py-spy GIL profile shows where the loop went:
  - reaction captures (BTC snapshots) took 23.5% of the event loop and condition kline reads 12.3%;
  - in #8 the backlog hold had kept captures off the loop.
- **Data MEDIUM.** Supply FUNDAMENTALS for BTC was 6.15 h old against its 6 h cadence. A supply kind was refreshed only after its age reached the cadence, so every 6 h it is stale for a few minutes by construction.

## Run on r118 (main f494e9c): Kenya, the final run, clean card within every limit

All 12 trace checks passed with no operator step. The bundle is in `kenya-r118/`.
- r118 started at 09:09:34. 09:09 (the restart minute) was incomplete, and 09:10–09:25 were complete on all 5 venues, so public time is 09:10:00.
- No other Market Truth job was running when the item was injected at 09:27:32. The agent's cutoff was 09:29, so the window is EVENT [09:10, 09:29): 19 complete minutes on all 5 venues.
- Data Confidence HIGH with no failed condition.

Timings, all within 01 §9 (the item → event limit is 7 min since the owner's decision of 21 Oct):
- item → event 80.5 s (limit 420 s; also inside the old 90 s)
- known → FULL card 386.7 s (limit 480); RUN_MARKET_TRUTH took 3 min 48 s
- delivery 3.52 s (limit 10). Telegram msg 26 at 09:34:02.97; DELIVER job JOB-01M5EWYXB0A69CVW53Q7MW0KGT DONE

Independent recomputation (scripts in `scripts/`, none imports Tank code):

| Card line | Card | Independent |
|---|---|---|
| Spot flow | CVD 13.33782327, NEUTRAL, BROAD, 5 venues | Σ(buy−sell) = 5.018760 − 3.546533 + 6.632687 − 6.491071 + 11.723980 = 13.337823. 2·√Σq² = 27.18 > 13.34, so NEUTRAL. The NEUTRAL venues (binance, bybit, okx) carry 87.0% of the volume and the largest venue is NEUTRAL, so BROAD (03 §5.3 rule 3) |
| Net buying / selling | coinbase 7.7% / kraken 5.4% | coinbase 6.63 > 4.16, BUY, 7.69% of the volume. kraken −6.49 < −6.25, SELL, 5.35% |
| Synchronized native OI | +0.43%, WITHIN, MIXED, 2 venues | binance + okx 127,235.9618 → 127,783.8743 = +0.4306% (okx +1.4989%). Range [−0.2829%, +0.4895%], identical to the stored range. USD +0.44% is within its range, so the class is neither PRICE_MARKUP nor REAL_NATIVE: MIXED |
| Binance OI | MIXED +0.10%, position share 80.5% | 96,854.259 → 96,946.790 = +0.0955%. USD +0.0724%, so position share 0.0955 / (0.0955 + 0.0231) = 80.5% |
| Priced | UNPRICED · EVENT 0.09% raw (BTC is the benchmark) | +0.0855%. Range [−0.4037%, +0.4233%], upper quartile +0.0906%, so UNPRICED. pctile 0.7402 |
| Market context | NEUTRAL, BTC 24h +2.55% vs −2.80% to +6.53% | +2.5526%; hourly 24h range [−2.80%, +6.53%] |
| Move class | UNKNOWN | Spot NEUTRAL. SPOT_LED_ACCUMULATION and ORGANIC_MIXED need spot BUY, and no other class of 03 §5.8 holds |
| Outlook | BULLISH · MEDIUM · best 24h | BULLISH: PRIMARY beneficiary, repricing UNPRICED, context NEUTRAL (§6.1). MEDIUM potential: MAJOR without THIN, COMPRESSED or squeeze fuel (§6.2). 24h: the agent's impact start is UNKNOWN (§6.3) |
| Confidence | LOW — main unknown: move class unresolved | min(Data HIGH, certainty MEDIUM, class LOW) = LOW, limited by the class (§6.5) |

M n/a and S n/a carry the agent's reasons (03 §4.2 allows null). VERIFIED n/a is expected for a story with no primary source.

The first OI export for this check was cut mid-line by the SSH pipe, so the okx rows after 08:19 were missing and okx wrongly dropped out of the cohort. The export written on the server gave the result above, and the checks now run on server-side exports.

## Fixes found by runs #8 and #9 (all live in r118)

- **bfe76db (tank-truth).** Liquidity compares native net flow with native depth. Partially covered venues give a lower bound instead of null, so weak_response, excess_response and THIN are decided whenever the bound decides them. Decisive facts print ranges as percents. The #8 snapshot re-run moves from PRICE_MARKUP_FAKE_OI to SPOT_LED_ACCUMULATION.
- **25ebe43 (tank-events).** PRICE_MARKUP_FAKE_OI has its own scenarios, chosen by the spot direction.
- **025c6fc (tank-market).** Prints and klines are valued with the USDT/USDC sample as of their own time, restored at startup.
- **fad3ff0, f5dd97c (tank-market).**
  - HOT OI and funding run on a 30 s wall-clock grid, Hyperliquid contexts are cached for 10 s, and HOT tickers step every 20 s.
  - The kline cursor serves older assembly clocks.
  - Snapshot decode runs off the loop.
- **598dc57 (tank-truth).** Each supply kind is refreshed before its age can reach its cadence.
- **874fb5a (tank-ops).** A reaction capture is not claimed while event analysis is running or due, and a running capture yields to Market Truth. A capture that would otherwise expire within 20 min still runs.
- **Spec, owner decision of 21 Oct.** The item → event limit p95 went from 90 s to 7 min: spec repo c2e7f829, prod repo cec5acf (PRD, TDD §3.9, e2e_trace.py, latency.py, RUNBOOK).

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

## Fixes found by the runs on r113–r115 (all live in r116)

- **64af59a (tank-events).** The CONFIDENCE line names the input that limits it, in the §6.5 order data → certainty → class, so "main unknown: none recorded" is gone. A named cutoff unit (MINUTE, SECOND, HOUR) is accepted. generated_at is stamped when the card is ready.
- **084313e (tank-events).** publish_card keeps Market Truth once and reads each collection once.
- **935f644 (tank-ops).** A job claim never blocks on an advisory lock: capped types use a try-lock, and each claim logs its timing. Delivery took 1.33 s on r115, against 17.3 s on r114.
- **fb2f4d8 (tank-ops).** Due reaction captures are claimed earliest expiry first.
- **2971458 (tank-truth).** Market Truth compute bisects pre-resolved book and flow times instead of rescanning them. A cold BTC computation went from 70.5 s to 20.7 s.
- **b218759 (tank-truth).** The snapshot reads books and tickers only where they are used, and oi_usd is derived only at the window endpoints.
- **dc9d578, fd2ec02 (tank-truth).** A late-data reaction revision waits 30 min, so late completions fold into one revision.
- **1c3e794 (tank-truth).** Fragmentation compares mids in one quote asset. USDT books are converted to USD at their stored rate, and a gap must also clear a 2 bp conversion uncertainty. A read-only re-run of the r115 snapshot moves the class from REAL_MARKET_FRAGMENTATION to UNKNOWN.
- **3cc069b (tank-market).** Range reads and batch writes do their JSON and typed decode and encode off the event loop.
- **2bf404a (tank-market).** The kline cursor serves earlier checkpoints, REST backfill CPU runs off the loop, and live bars flush first.
- **6fde945 (tank-market).** Past-day kline counts survive a restart. Before, the first snapshots after a restart spent 57.5 s each recounting them.
- **c987074, fe3a0ee (tank-market).** A REST range never rewrites a complete bar with the same prints, and a reconnect backfill asks REST again when it trails the stream.
- **c091ae1 (tank-market).** HOT 24h REST tickers run on their own 30 s wall-clock grid, and Hyperliquid contexts are cached for 15 s. On r115 the Hyperliquid REST ticker was 119.5 s old at the cutoff.
- **0016b3d, 23aaac8 (tank-sources).** The S14 instrument snapshot lives in its own document, written only when it changes. Its diff and digest run in a thread.
