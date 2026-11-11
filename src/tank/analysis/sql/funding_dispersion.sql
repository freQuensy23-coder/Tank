-- funding_dispersion: the 90-day normal range of the dispersion of the settled 8-hour-equivalent funding rate across
-- venues, per window (03 section 5.4 funding, funding dispersion).
-- Rule: the settled rows of the venues {iids} (the venues that have a funding rate now) are taken in the order
-- (as_of ASC, instrument_id DESC), so that settlements of the same instant have one fixed order; each row sets the
-- state of its venue to rate x 8 / interval; once at least two venues are given and every one has a state, each row
-- contributes one sample, the largest state minus the smallest. A row counts when it was recorded and received at or
-- before {assembled}, has a rate and an interval other than 0 and was settled before the window start and within
-- {lookback_s} of the cutoff; the states of a window are those of its own rows, and since a state only depends on the
-- earlier rows one pass in time order serves every window.
-- One result row per window that has a sample: the number of samples, the first and the last of their times and the 0.025 and 0.975
-- quantiles (inclusive). A history that spans less than the minimum days has no range: the caller decides from the
-- first and the last time.
-- Shape: one scan of mkt.funding_obs FINAL (the SETTLED rows of the venues), a running window over the rows in that
-- order, the windows cut from the samples.
-- param iids = ['binance:perp_linear:AVAXUSDT','okx:perp_linear:AVAX-USDT-SWAP','bybit:perp_linear:AVAXUSDT','hyperliquid:perp_linear:AVAX']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param names = ['W15M','W1H','W4H','W24H','EVENT']
-- param lens_us = ['900000000','3600000000','14400000000','86400000000','79500000000']
-- param lookback_s = 7776000
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {iids:Array(String)} AS iids,
    toUInt8(length(iids)) AS nv,
    {names:Array(String)} AS names,
    arrayMap(x -> toInt64(x), {lens_us:Array(String)}) AS lens_us,
    toInt64(toUnixTimestamp64Micro(cutoff)) AS cutoff_us,
    arrayMap(l -> cutoff_us - l, lens_us) AS starts_us,
    CAST(NULL AS Nullable(Float64)) AS nil
SELECT
    names[w] AS window,
    count() AS samples,
    if(count() = 0, NULL, fromUnixTimestamp64Micro(min(t_us), 'UTC')) AS history_first,
    if(count() = 0, NULL, fromUnixTimestamp64Micro(max(t_us), 'UTC')) AS history_last,
    if(count() = 0, NULL, quantilesExactInclusive(0.025, 0.975)(disp)[1]) AS normal_low,
    if(count() = 0, NULL, quantilesExactInclusive(0.025, 0.975)(disp)[2]) AS normal_high
FROM
(
    SELECT
        t_us,
        arrayMax(arrayMap(x -> assumeNotNull(x), st)) - arrayMin(arrayMap(x -> assumeNotNull(x), st)) AS disp
    FROM
    (
        SELECT
            t_us,
            anyLastForEach(cur) OVER (ORDER BY t_us ASC, instrument_id DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS st
        FROM
        (
            SELECT
                instrument_id,
                toInt64(toUnixTimestamp64Micro(as_of)) AS t_us,
                toUInt8(indexOf(iids, instrument_id)) AS vi,
                toFloat64OrNull(rate) * 8 / toFloat64OrNull(interval_h) AS eq,
                arrayMap(k -> if(k = vi, eq, nil), range(1, nv + 1)) AS cur
            FROM mkt.funding_obs FINAL
            WHERE instrument_id IN iids
              AND kind = 'SETTLED'
              AND as_of >= cutoff - toIntervalSecond({lookback_s:UInt32})
              AND as_of < cutoff
              AND (recorded_at IS NULL OR recorded_at <= assembled)
              AND (received_ts IS NULL OR received_ts <= assembled)
              AND toFloat64OrNull(rate) IS NOT NULL
              AND ifNull(toFloat64OrNull(interval_h), 0) != 0
        )
    )
    WHERE nv >= 2 AND arrayAll(x -> x IS NOT NULL, st)
)
ARRAY JOIN arrayEnumerate(names) AS w
WHERE t_us < starts_us[w]
GROUP BY w
ORDER BY w
