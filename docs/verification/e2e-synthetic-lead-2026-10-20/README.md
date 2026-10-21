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

## Final run (r105, main f21c727)

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
