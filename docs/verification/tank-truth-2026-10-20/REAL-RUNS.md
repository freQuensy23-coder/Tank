# tank-truth real runs on openclaw-dp (2026-10-20, test databases on 127.0.0.1:55438)

## supply-real-run/ (03 §5.6, HLD §4.8): product SupplyCapture against the real sources, all 22 tokens
- `run.jsonl`: one line per token × kind. The run was restarted by a host-level unit restart at 17:39 and died at 18:32 when the test DB tmpfs filled up,
  so the inventory for AVAX, ENA, NEAR, QNT, WLD, XPL and ZRO is missing (AVAX, NEAR and XPL have no EVM contract and record NOT_SUPPORTED anyway).
- `supply_obs_summary.json`: latest observation per token × kind.
- `handcheck_sources.json`: independent fetches, no Tank code. Unlocks for ENA, AVAX and ZRO match the emissions datasets tranche by tranche;
  fees24h for UNI, HYPE and AAVE equal /summary/fees; lending utilization, borrow APY and supply for AERO, MORPHO and ONDO equal /lendBorrow;
  stablecoins on Ethereum and Base equal stablecoincharts.
- `handcheck_etherscan_uni.json`: an independent Etherscan tokenbalance sum over the 340 addresses of exchange_addresses.yaml.
  OKX 20147503.974965187491926372 UNI equals Tank's value exactly. Binance is 0: the 17 pinned DefiLlama-Adapters withdrawal addresses hold no UNI.
- Fixed by this run: 6dfbdba (DefiLlama names BNB Chain "BSC"; one failing chain no longer drops the others).
- Facts for the user: the Etherscan v2 free key refuses BNB Chain (56) and Base (8453), so CAKE and AERO inventory and flows are n/a with that reason.
  Lending pools for UNI, CRV and CAKE in /lendBorrow are Compound, Venus, Moonwell and similar, not Aave, Morpho or Euler, so they are correctly excluded.

## reaction-test-run/ (01 FR-R-01, 03 §8): own MarketService on real exchanges + real Scheduler/Runtime.worker
- `checkpoints_cow_run.json`: RX-LIVE (anchor 17:20, COW escalated). PRE_24H/4H/1H were captured at creation, then revised as the
  pre-event fill arrived (late data appends revisions: rev 3–7); the final rows have r_raw, r_btc_adj, trade CVD, OI % and funding.
  POST_15M was captured by its scheduled REACTION_CAPTURE job. RX-RETRO (anchor −73 h): all 8 checkpoints were captured with retro = true,
  but the 4-day archive fill had not finished when the DB filled, so their trade values are still n/a. pctile is n/a: there's not enough stored history.
  BTC/ETH rows were captured before the benchmark ruling (n/a r_raw); the scheduler patch for the ruling is with tank-events.
- The job queue reached 2168 QUEUED revisions during the fill. The coalescing fix is in the same patch
  (/srv/tank-work/patches/tank-truth-scheduler-reaction.diff).
- `live_market_truth_cow_W1H.json` vs `live_handcheck_cow_W1H.json` (window 16:47–17:47): per-venue CVD for Binance and Coinbase equals the
  aggTrades/trades sums exactly; the anchor price equals the last Coinbase trade before 17:20; r_raw equals the Coinbase candle closes;
  Data Confidence is MEDIUM with derivatives_coverage first (§5.10 order). That window included driver restarts, so OI freshness failed.
