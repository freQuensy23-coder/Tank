-- liquidations: the liquidations of every perp venue in the window, per side, and in the first half of the window
-- (03 section 5.4 liquidations).
-- One result row per window and instrument that has a liquidation in the window; an instrument without one has no
-- row. A window is [cutoff - length, cutoff). A row counts when it was recorded at or before {assembled} (a missing
-- time counts). Per side (LONG_LIQ, SHORT_LIQ) the number of rows, the sum of notional_usd and of qty_native over the
-- rows that have one and the number of rows that have none, so that the caller can tell a sum of every row from a sum
-- of some; the same for the rows of any side in the first half of the window (before the middle of the window) and in
-- all of it. `observations` counts the rows of any side.
-- Shape: one scan of mkt.liq_obs FINAL over the longest window, the windows cut from it.
-- param iids = ['binance:perp_linear:AVAXUSDT','okx:perp_linear:AVAX-USDT-SWAP','bybit:perp_linear:AVAXUSDT','hyperliquid:perp_linear:AVAX']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param names = ['W15M','W1H','W4H','W24H','EVENT']
-- param lens_us = ['900000000','3600000000','14400000000','86400000000','79500000000']
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {iids:Array(String)} AS iids,
    {names:Array(String)} AS names,
    arrayMap(x -> toInt64(x), {lens_us:Array(String)}) AS lens_us,
    toInt64(toUnixTimestamp64Micro(cutoff)) AS cutoff_us,
    arrayMap(l -> cutoff_us - l, lens_us) AS starts_us,
    arrayMap(l -> cutoff_us - l + intDiv(l, 2), lens_us) AS halves_us
SELECT
    names[w] AS window,
    instrument_id,
    count() AS observations,
    countIf(side = 'LONG_LIQ') AS long_count,
    sumKahanIf(usd, side = 'LONG_LIQ') AS long_usd,
    countIf(side = 'LONG_LIQ' AND usd IS NULL) AS long_usd_missing,
    sumKahanIf(native, side = 'LONG_LIQ') AS long_native,
    countIf(side = 'LONG_LIQ' AND native IS NULL) AS long_native_missing,
    countIf(side = 'SHORT_LIQ') AS short_count,
    sumKahanIf(usd, side = 'SHORT_LIQ') AS short_usd,
    countIf(side = 'SHORT_LIQ' AND usd IS NULL) AS short_usd_missing,
    sumKahanIf(native, side = 'SHORT_LIQ') AS short_native,
    countIf(side = 'SHORT_LIQ' AND native IS NULL) AS short_native_missing,
    sumKahan(usd) AS total_usd,
    countIf(usd IS NULL) AS total_usd_missing,
    countIf(t_us < halves_us[w]) AS first_half_count,
    sumKahanIf(usd, t_us < halves_us[w]) AS first_half_usd,
    countIf(t_us < halves_us[w] AND usd IS NULL) AS first_half_usd_missing
FROM
(
    SELECT
        instrument_id, side,
        toInt64(toUnixTimestamp64Micro(ts)) AS t_us,
        toFloat64OrNull(notional_usd) AS usd,
        toFloat64OrNull(qty_native) AS native
    FROM mkt.liq_obs FINAL
    WHERE instrument_id IN iids
      AND ts >= fromUnixTimestamp64Micro(cutoff_us - arrayMax(lens_us), 'UTC')
      AND ts < cutoff
      AND (recorded_at IS NULL OR recorded_at <= assembled)
)
ARRAY JOIN arrayEnumerate(names) AS w
WHERE t_us >= starts_us[w]
GROUP BY w, instrument_id
ORDER BY w, instrument_id
