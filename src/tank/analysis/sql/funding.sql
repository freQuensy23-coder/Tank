-- funding: the funding of every perp venue and the 90-day normal range of its settled rate, per window
-- (03 section 5.4 funding).
-- One result row per window and instrument that has a funding row in the look-back. A window is
-- [cutoff - length, cutoff]. Rules:
-- * a row counts when it was recorded and received at or before {assembled} (a missing time counts) and was
--   observed (as_of) at or before {cutoff};
-- * the settled rate is the latest SETTLED row, the estimate the latest CURRENT_ESTIMATE row; each comes back with
--   its rate and interval as stored (decimal text), its time and its rate x 8 / interval in float (NULL when the
--   rate is missing or the interval is missing or 0);
-- * the rate at the window start is the latest SETTLED row at or before the window start, with its interval and time;
-- * the history of a window is the SETTLED rows before the window start within {lookback_s} of the cutoff that have a
--   rate and an interval above 0; its sample is the stored rate_8h_eq, else rate x 8 / interval; the result has the
--   number of samples, the first and the last of their times and the 0.025 and 0.975 quantiles (inclusive).
-- A history that spans less than the minimum days has no range: the caller decides from the first and the last time.
-- Shape: one scan of mkt.funding_obs FINAL; the SETTLED rows of an instrument (a few hundred) are collected into an
-- array and the windows are cut from that array, so a window costs nothing more than its own rows.
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
    {names:Array(String)} AS names,
    arrayMap(x -> toInt64(x), {lens_us:Array(String)}) AS lens_us,
    toInt64(toUnixTimestamp64Micro(cutoff)) AS cutoff_us,
    arrayMap(l -> cutoff_us - l, lens_us) AS starts_us,
    CAST(NULL AS Nullable(String)) AS nils
SELECT
    names[w] AS window,
    instrument_id,
    settled.1 AS settled_rate,
    settled.2 AS settled_interval_h,
    if(n_settled = 0, NULL, settled.3) AS settled_at,
    settled.4 AS settled_eq,
    estimate.1 AS estimate_rate,
    estimate.2 AS estimate_interval_h,
    if(n_estimate = 0, NULL, estimate.3) AS estimate_at,
    estimate.4 AS estimate_eq,
    if(empty(started), nils, started[-1].2) AS start_rate,
    if(empty(started), nils, started[-1].3) AS start_interval_h,
    if(empty(started), NULL, fromUnixTimestamp64Micro(started[-1].1, 'UTC')) AS start_at,
    length(hist) AS samples,
    if(empty(hist), NULL, fromUnixTimestamp64Micro(arrayMin(arrayMap(x -> x.1, hist)), 'UTC')) AS history_first,
    if(empty(hist), NULL, fromUnixTimestamp64Micro(arrayMax(arrayMap(x -> x.1, hist)), 'UTC')) AS history_last,
    if(empty(hist), NULL, qs[1]) AS normal_low,
    if(empty(hist), NULL, qs[2]) AS normal_high
FROM
(
    SELECT
        instrument_id, settled, estimate, n_settled, n_estimate, w,
        arraySort(x -> x.1, arrayFilter(x -> x.1 <= starts_us[w], settled_rows)) AS started,
        arrayFilter(x -> x.1 < starts_us[w] AND x.4 IS NOT NULL, settled_rows) AS hist,
        arrayReduce('quantilesExactInclusive(0.025, 0.975)', arrayMap(x -> assumeNotNull(x.4), hist)) AS qs
    FROM
    (
        SELECT
            instrument_id,
            argMaxIf((rate, interval_h, as_of, eq), as_of, ok AND kind = 'SETTLED') AS settled,
            argMaxIf((rate, interval_h, as_of, eq), as_of, ok AND kind = 'CURRENT_ESTIMATE') AS estimate,
            countIf(ok AND kind = 'SETTLED') AS n_settled,
            countIf(ok AND kind = 'CURRENT_ESTIMATE') AS n_estimate,
            groupArrayIf((toInt64(toUnixTimestamp64Micro(as_of)), rate, interval_h, sample_eq), ok AND kind = 'SETTLED') AS settled_rows
        FROM
        (
            SELECT
                instrument_id, kind, rate, interval_h, as_of,
                (recorded_at IS NULL OR recorded_at <= assembled) AND (received_ts IS NULL OR received_ts <= assembled) AS ok,
                toFloat64OrNull(rate) AS r,
                toFloat64OrNull(interval_h) AS ih,
                if(r IS NOT NULL AND ih != 0, r * 8 / ih, NULL) AS eq,
                if(r IS NOT NULL AND ih > 0, if(rate_8h_eq IS NOT NULL, toFloat64OrNull(rate_8h_eq), r * 8 / ih), NULL) AS sample_eq
            FROM mkt.funding_obs FINAL
            WHERE instrument_id IN iids
              AND kind IN ('SETTLED', 'CURRENT_ESTIMATE')
              AND as_of >= cutoff - toIntervalSecond({lookback_s:UInt32})
              AND as_of <= cutoff
        )
        GROUP BY instrument_id
    )
    ARRAY JOIN arrayEnumerate(names) AS w
)
ORDER BY w, instrument_id
