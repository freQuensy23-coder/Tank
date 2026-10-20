# Production verification (tank serve, roll 19:49 UTC, 2026-10-20)

## Market Truth: MTA-01M5DJ4C5KD7JKQ88R6VW1WW62 (world:WLD, cutoff 20:56)
- Data Confidence LOW; the limiting condition is spot_coverage (first in §5.10 order), then derivatives_coverage and freshness.
  That's correct for this window: WLD went hot at 20:02, the pre-event fill was still running, and Binance WebSockets refused
  the host, so no spot venue was complete for 19:56–20:56 (books 40 s old against their 10 s cadence).
- Synchronized native OI equals the hand check exactly (Binance openInterestHist 5m sumOpenInterest + Bybit open-interest 5min / 2,
  because §5.4 uses singleOpenInterest, half of the both-sided openInterest):
  19:55 = 215,731,904 + 173,180,130.3/2 = 302,321,969.2 (Tank 302321969.20000000);
  20:55 = 212,212,776 + 172,182,254.2/2 = 298,303,903.1 (Tank 298303903.10000000).
- File: `prod_mta_wld_2056.json` (hand-check script output, incl. per-venue trade sums).

## Reaction pack: EVT-01M5DEDTQC4FA8CQV3QYW9HCRQ (WLD, T1 15:02)
- PRE checkpoints were captured at creation with retro = true; late data appended revisions (PRE_24H rev 2); POST jobs were created by the scheduler.
- PRE_1H r_raw 0.0221429 = 0.5724 / 0.5600 − 1. The anchor price 0.5600 equals the last Coinbase trade before 14:02 (trade 6643763).
  0.5724 was the stored Coinbase 15:01 candle close when the checkpoint was captured (20:50); Coinbase later revised that candle
  (its row was re-recorded at 20:56 with close 0.5723), which is late data for a revision.
- BTC/ETH benchmarks take the ruling path (NOT_APPLICABLE, BASE_KLINE_CLOSE_LARGEST_SPOT_VENUE).
- Bugs found and fixed in main: 3918568 (a benchmark r_raw of 0.0 across the BTC/ETH base-kline gap 10:00–15:28 is now n/a) and
  38e8acd (r_btc_adj/r_eth_adj equalled r_raw for the same reason). Both go out with the next roll.

## Blockers outside tank-truth (status at 21:10 UTC)
- market.snapshot: 144.9 s against the 90 s RUN_MARKET_TRUTH budget (compute 11.3 s); its market_series_counts query alone takes 18–21 s.
  tank-market's bounded reads are pending a roll.
- postgres was OOM-killed at 20:02:36 on a klines_1m range read (2 GiB container).
- Binance WebSockets refuse the host (per-IP), so spot completeness, and Data Confidence HIGH, can't be reached right now.
