-- volume_normal: the native-token volume of the current and the previous window of every window length
-- against its 90-day normal range, for the spot market and for the perp market of a token (03 section 5.4,
-- spot_volume and perp_volume).
-- A market is the instruments of {instrument_ids} that are spot, or that are not. An instrument is a member of
-- its market when it has any one-minute kline recorded at or before {assembled} in the look-back. A window
-- total exists only when every member has, for every minute of the window, a complete kline with a
-- volume_native >= 0; it is the sum of their volumes. The current window is [cutoff - length, cutoff) and the
-- previous one the window of the same length before it; both exist only when {cutoff} and the length are
-- whole minutes. The history is every window of the same length that ends on a whole minute before the start
-- of the current window and starts inside the look-back; it must span at least {min_days} days from the start
-- of its first to the end of its last window, otherwise the range is empty (NULL).
-- One row per market and window; a market without members has no rows. normal_low / normal_high are the
-- 0.025 and 0.975 quantiles of the history totals.
-- Shape: one scan of mkt.klines_1m FINAL; one row per market and minute (bit mask of the members with a valid
-- bar, their summed volume); running sums of valid minutes and of volume per market; every minute joined on
-- equality to the first minute of the window that ends with it, for every window length; one GROUP BY.
-- The running volume is Decimal128: a difference of two Float64 running sums over 90 days loses up to 1e-7
-- of a small window total, a Decimal difference loses nothing.
-- param instrument_ids = ['binance:spot:AVAXUSDT','bybit:spot:AVAXUSDT','coinbase:spot:AVAX-USD','kraken:spot:AVAX/USD','okx:spot:AVAX-USDT','binance:perp_linear:AVAXUSDT','okx:perp_linear:AVAX-USDT-SWAP','bybit:perp_linear:AVAXUSDT','hyperliquid:perp_linear:AVAX']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param names = ['W15M','W1H','W4H','W24H','EVENT']
-- param lens_us = ['900000000','3600000000','14400000000','86400000000','79500000000']
-- param lookback_s = 7776000
-- param min_days = 7
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {instrument_ids:Array(String)} AS iids,
    {names:Array(String)} AS names,
    arrayMap(x -> toInt64(x), {lens_us:Array(String)}) AS lens,
    toInt64(toUnixTimestamp64Micro(cutoff)) AS cut_us,
    intDiv(cut_us, 1000000) AS cut_s,
    cum AS
    (
        SELECT
            spot, m, valid, ifNull(vol, 0) AS vol,
            sum(if(valid, ifNull(vol, 0), 0)) OVER w AS cv,
            sum(toUInt32(valid)) OVER w AS cn
        FROM
        (
            SELECT spot, m, vol, okmask = groupBitOr(seen) OVER (PARTITION BY spot) AS valid
            FROM
            (
                SELECT
                    spot, m,
                    groupBitOrIf(bit, vis) AS seen,
                    groupBitOrIf(bit, ok) AS okmask,
                    sumIf(v, ok) AS vol
                FROM
                (
                    SELECT
                        instrument_id LIKE '%:spot:%' AS spot,
                        toInt64(toUnixTimestamp(open_time)) AS m,
                        bitShiftLeft(toUInt64(1), indexOf(iids, instrument_id)) AS bit,
                        toDecimal128OrNull(volume_native, 18) AS v,
                        (recorded_at IS NULL OR recorded_at <= assembled) AND interval_s = 60 AS vis,
                        vis AND complete = true AND v >= 0 AS ok
                    FROM mkt.klines_1m FINAL
                    WHERE instrument_id IN iids
                      AND open_time >= cutoff - toIntervalSecond({lookback_s:UInt32})
                      AND open_time <= cutoff - INTERVAL 60 SECOND
                )
                GROUP BY spot, m
            )
        )
        WINDOW w AS (PARTITION BY spot ORDER BY m ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    )
SELECT
    if(spot, 'SPOT', 'PERP') AS market,
    names[wi] AS window,
    if(cut_us % 60000000 = 0 AND any(len_us) % 60000000 = 0 AND countIf(full AND t = cut_s) > 0,
        anyIf(total, full AND t = cut_s), NULL) AS value,
    if(cut_us % 60000000 = 0 AND any(len_us) % 60000000 = 0 AND countIf(full AND t = cut_s - intDiv(len_us, 1000000)) > 0,
        anyIf(total, full AND t = cut_s - intDiv(len_us, 1000000)), NULL) AS previous,
    countIf(hist) AS samples,
    if(samples > 0 AND maxIf(t, hist) - minIf(t - intDiv(len_us, 1000000), hist) >= {min_days:Float64} * 86400,
        quantileExactInclusiveIf(0.025)(total, hist), NULL) AS normal_low,
    if(samples > 0 AND maxIf(t, hist) - minIf(t - intDiv(len_us, 1000000), hist) >= {min_days:Float64} * 86400,
        quantileExactInclusiveIf(0.975)(total, hist), NULL) AS normal_high
FROM
(
    SELECT
        e.spot AS spot, e.wi AS wi, e.len_us AS len_us, e.t AS t,
        e.cn - a.cn + toUInt32(a.valid) = intDiv(e.len_us, 60000000) AS full,
        toFloat64(e.cv - a.cv + if(a.valid, a.vol, 0)) AS total,
        full AND t * 1000000 < cut_us - len_us AS hist
    FROM
    (
        SELECT spot, m + 60 AS t, cv, cn, wi, len_us, m + 60 - intDiv(len_us, 1000000) AS first_m
        FROM cum
        ARRAY JOIN arrayEnumerate(lens) AS wi, lens AS len_us
        WHERE len_us % 60000000 = 0
    ) AS e
    INNER JOIN cum AS a ON a.spot = e.spot AND a.m = e.first_m
)
GROUP BY spot, wi
ORDER BY spot DESC, wi
