# Fixture check — history-based thresholds (TDD §0, §5.4, §5.7)

Checked on 17 Oct 2026 against public Binance and Bybit data, no API keys. The docs were not edited.

**Method.**
- **History.** The stored history is the 90 days before each fixture window.
- **Normal range.** The middle 95% (2.5th–97.5th percentile) of the token's own values for the same window length. Windows are stepped at the data resolution: 5 min for open interest, 1 h for prices.
- **BTC adjustment.** `r − β·r_BTC`, with β estimated by OLS on 1h log returns and Blume-adjusted (`(2/3)·β + 1/3`).
- **Open interest.** Binance native OI comes from the 5-minute `metrics` archive (data.binance.vision); Bybit OI is `singleOpenInterest` (5 min). Prices come from Binance spot and USDⓈ-M klines; funding from Binance `fundingRate`.

## Results

| Fixture | Assertion checked | Measured | Reference from the token's own history | Result |
|---|---|---|---|---|
| F-08 QNT, 24 Sep | Repricing HEAVY → DON'T CHASE | Event-window BTC-adjusted move +22.6% (anchor at 00:00) / +23.7% (anchor at the 02:00 low), up to the 20:15 call; β 0.81 | HEAVY threshold: upper edge of the 24h normal range, +4.59% | **PASS** |
| F-11 WLD, 16 Oct | 4h synchronized native OI (Binance + Bybit) → REAL_NATIVE | +8.18% from REST snapshots (Binance +10.37%, Bybit +3.38%); +7.26% from the archive; position share 0.52–0.54 | 4h normal range [−2.99%, +3.50%] → beyond; position term is most of the change | **PASS** |
| F-12 QNT, 26 Sep 19–23 | oi_class PRICE_MARKUP | Native +0.42% (REST 1h) / +1.80% (archive); USD +19.98% / +22.36% | Native range [−3.19%, +4.84%] → within; USD range [−3.52%, +5.83%] → beyond | **PASS** |
| F-13 ONDO, 24 Sep 12–16 | oi_class REAL_NATIVE; PERP_LED_CHASE eligible | Native +57.32% (REST 1h) / +38.39% (archive); position share 0.66–0.73; price +18.4% BTC-adjusted; perps took 86% of the new Binance volume | Native range up to +3.89% → beyond; 4h price range up to +3.02% → beyond | **PASS** |
| F-14 QNT, 27 Sep 19–23 | Move class ∈ {SHORT_SQUEEZE, LIQUIDITY_VACUUM, UNKNOWN}; direction TWO_SIDED or UNKNOWN | Native −30.36% / −31.84%; price up far beyond normal; last settled funding before 19:00 +0.0100% (16:00) | Native range [−3.19%, +5.91%] → beyond. SHORT_SQUEEZE needs funding ≤ 0 → not eligible; no order book in the archive → no vacuum → UNKNOWN; oi_class MIXED (position share 0.26) | **PASS** |
| F-16 BTC, 15 Sep | Context HEADWIND, not STRESS | BTC 24h −3.25%; realized volatility 0.45% | Lower quartile −0.85%; lower edge of the normal range −3.01% → the current §5.7 gives **STRESS** | **FAIL** (rule) |

## The failure is in the rule, not in the fixture

**F-16 — STRESS fires too easily.** Over a calm 90 days, BTC's lower normal edge is only −3.01%, so a −3.25% day counts as STRESS. Proposed §5.7 market context:

| Context | Rule |
|---|---|
| STRESS | BTC's 24h return sets a record low, or its 24h realized volatility a record high, over its stored history (§0) |
| HEADWIND | BTC's 24h return is below its normal range |
| NEUTRAL | Otherwise |

Evidence: −3.25% is below the normal range (−3.01%) but above the 90-day low (−5.29%). Volatility 0.45% is below the 90-day high (1.34%). Result: HEADWIND, as F-16 expects.

The STRESS rule needs a §0 convention: "Record — a new extreme of the token's stored history for the same measure and window length."

## Wording to change in TDD §11.3

1. **"Fixture data capture" paragraph** — add:
   > The open-interest figures of F-11–F-14 come from Binance `openInterestHist` (F-11: 5 m, with Bybit `open-interest` 5 m; F-12–F-14: 1 h). REST keeps only 30 days, so the values are stored with the packets (`fixture_check/data/rest_openInterestHist_fixtures.json`, pulled 17 Oct 2026). Normal ranges come from the 5-minute archive history. The window values are not taken from the archive, because its snapshots differ inside fast moves: for ONDO on 24 Sep at 12:00 the archive shows 159.0 M against 138.6 M from REST, which turns +57.32% into +38.39%.
2. **F-14 case text** — "price +54.09% (+50.78% in the 22:00 hour)" → "price +54.09% between the closes of the 19:00 and 23:00 hourly bars (+50.78% in the 22:00 hour)". The OI change uses the 19:00 and 23:00 snapshots, so the two figures cover different hours.
3. **F-16** — no wording change; the rule above makes the current assertion hold.

**PRD V0-E** ("+8.18% ± 0.5 pp") reproduces only from REST snapshots: +8.18% exactly. The archive gives +7.26%, which fails the ±0.5 pp tolerance. Item 1 covers this.

## Files

- `scripts/fetch.py` — downloads klines, archive OI, Bybit OI and funding into `data/`.
- `scripts/compute.py` → `results.json` — the table above.
- `scripts/alternatives.py` → `alternatives.json` — the F-16 rule alternative, plus the F-11 REST snapshot check.
- `scripts/diagnose.py` — REST against the archive, and the QNT price bars.
- `scripts/save_rest_oi.py` → `data/rest_openInterestHist_fixtures.json` — REST OI for the four windows, saved before the 30-day retention drops them (F-13 would go first, about 24 Oct 2026).

Each script takes the `data/` folder and, where it writes one, the output path as arguments. Run it from outside `data/` with `python3 -I`.
