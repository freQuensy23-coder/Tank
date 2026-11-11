-- relative_baselines: the price-relative numbers of one token (03 section 5 preamble and 5.7).
-- Beta against BTC and ETH from the hourly log returns of the 90-day look-back, the return of every
-- horizon (token, BTC, ETH, BTC- and ETH-adjusted), the 90-day history of the measure of every horizon
-- (sample count, quantiles 0.025 / 0.25 / 0.75 / 0.975, share of samples not above the current measure),
-- the BTC market context (24 h return and 24 h realised volatility with their 90-day quantiles) and the
-- same 24 h realised volatility of the reference instrument (volatility compression; its 90-day history
-- ends, like the horizon's own, where the horizon starts).
-- One scan of mkt.klines_1m FINAL for the reference, BTC and ETH instruments, one result row per horizon.
-- Shape: one row per minute of the look-back (the three closes side by side, missing minutes filled with
-- NULL, so that a missing minute is a missing price) collected into three arrays by minute number; the
-- hourly log returns and the 24 h statistics are computed over the arrays of the hourly closes (no window
-- function over the minutes), once, in a one-row subquery that the horizons (ARRAY JOIN of the parameters)
-- are cross joined to, so that the array work does not repeat per horizon.
-- A close is the close of a complete one-minute bar with a positive price that was recorded at or before
-- {assembled} (a NULL recorded_at counts as recorded). The token price at a bound is its last close at or
-- before it; a BTC or ETH price counts only when that close is not older than the spacing of its own
-- series (at least one minute); log(a / b) is written log1p(a / b - 1).
-- {event_raw} is the EVENT raw return from the trade prices at the anchor and at the cutoff (nan: none),
-- which the Python rule derives from reference_prices.sql; it replaces the kline-based raw return of the
-- EVENT horizon only. {is_btc} = 1: the measure is the raw return (a benchmark has no BTC adjustment).
-- {names} / {lens_us} / {ends}: horizon names, lengths in microseconds and end times; a horizon is the
-- interval [end - length, end].
-- param ref = binance:spot:UNIUSDT
-- param btc = binance:spot:BTCUSDT
-- param eth = binance:spot:ETHUSDT
-- param is_btc = 0
-- param cutoff = 2026-11-09 15:32:00.000000
-- param start = 2026-11-06 20:01:45.000000
-- param assembled = 2026-11-09 18:58:54.316570
-- param lookback_s = 7776000
-- param blume = 0.6666666666666666
-- param min_pairs = 168
-- param event_raw = nan
-- param names = ['W15M','W1H','W4H','W24H','EVENT','PRE24H','W12H','W3D','W7D']
-- param lens_us = ['900000000','3600000000','14400000000','86400000000','243015000000','86400000000','43200000000','259200000000','604800000000']
-- param ends = ['2026-11-09 15:32:00','2026-11-09 15:32:00','2026-11-09 15:32:00','2026-11-09 15:32:00','2026-11-09 15:32:00','2026-11-06 20:01:45','2026-11-09 15:32:00','2026-11-09 15:32:00','2026-11-09 15:32:00']
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {start:DateTime64(6, 'UTC')} AS start_,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    toInt64(ceil((toUnixTimestamp64Micro(cutoff) / 1000000 - {lookback_s:UInt32}) / 60) * 60 + 60) AS g0_s,
    toUInt32(ceil((toUnixTimestamp64Micro(start_) / 1000000 - g0_s) / 60)) AS hi,
    toUInt32(1 + intDiv((3600 - g0_s % 3600) % 3600, 60)) AS i0,
    toUInt32(intDiv(toInt64(toUnixTimestamp(cutoff)) - g0_s, 60) + 1) AS n
SELECT
    name,
    len_us / 1000000 AS length_s,
    n_pairs_btc AS beta_btc_pairs,
    beta_btc_raw AS beta_btc_ols,
    bb AS beta_btc,
    n_pairs_eth AS beta_eth_pairs,
    beta_eth_raw AS beta_eth_ols,
    be AS beta_eth,
    if(name = 'EVENT', if(isNaN({event_raw:Float64}), NULL, {event_raw:Float64}), p_end / p_begin - 1) AS raw_return,
    b_end / b_begin - 1 AS btc_return,
    e_end / e_begin - 1 AS eth_return,
    raw_return - bb * btc_return AS btc_adjusted_return,
    raw_return - be * eth_return AS eth_adjusted_return,
    if({is_btc:UInt8} = 1, raw_return, btc_adjusted_return) AS measure,
    length(xs) AS sample_count,
    history_q[1] AS history_q025,
    history_q[2] AS history_q25,
    history_q[3] AS history_q75,
    history_q[4] AS history_q975,
    if(measure IS NULL OR sample_count = 0, NULL, arrayCount(v -> v <= measure, xs) / sample_count) AS percentile,
    if(sample_count = 0, NULL, toDateTime64(g0_s + (k + first_x - 1) * 60, 6, 'UTC')) AS history_first_end,
    if(sample_count = 0, NULL, toDateTime64(g0_s + (k + last_x - 1) * 60, 6, 'UTC')) AS history_last_end,
    if(first_p = 0 OR first_p > hi, NULL, toDateTime64(g0_s + (first_p - 1) * 60, 6, 'UTC')) AS price_first_close,
    if(last_p = 0, NULL, toDateTime64(g0_s + (last_p - 1) * 60, 6, 'UTC')) AS price_last_close,
    history_gap_s AS history_resolution_s,
    if(first_b = 0, NULL, toDateTime64(g0_s + (first_b - 1) * 60, 6, 'UTC')) AS btc_first_close,
    if(last_b = 0, NULL, toDateTime64(g0_s + (last_b - 1) * 60, 6, 'UTC')) AS btc_last_close,
    ctx_rv24 AS btc_realized_vol_24h,
    ctx_r24_q[1] AS btc_return_24h_q025,
    ctx_r24_q[2] AS btc_return_24h_q25,
    ctx_r24_q[3] AS btc_return_24h_q975,
    ctx_rv24_q[1] AS btc_realized_vol_24h_q025,
    ctx_rv24_q[2] AS btc_realized_vol_24h_q975,
    ctx_n_r24 AS btc_return_24h_samples,
    ctx_n_rv24 AS btc_realized_vol_24h_samples,
    ref_rv24 AS ref_realized_vol_24h,
    ref_rv24_q[1] AS ref_realized_vol_24h_q025,
    ref_rv24_q[2] AS ref_realized_vol_24h_q975,
    ref_n_rv24 AS ref_realized_vol_24h_samples,
    if(ref_hour_first = 0, NULL, toDateTime64(g0_s + (ref_hour_first - 1) * 60, 6, 'UTC')) AS ref_hourly_first,
    if(ref_hour_last = 0, NULL, toDateTime64(g0_s + (ref_hour_last - 1) * 60, 6, 'UTC')) AS ref_hourly_last
FROM
(
    SELECT
        name, len_us, k,
        n_pairs_btc, beta_btc_raw, bb, n_pairs_eth, beta_eth_raw, be,
        ctx_rv24, ctx_r24_q, ctx_rv24_q, ctx_n_r24, ctx_n_rv24,
        ref_rv24, ref_hour_first, ref_hour_last,
        first_p, last_p, first_b, last_b, history_gap_s,
        toUInt32(greatest(toInt64(ceil((toUnixTimestamp64Micro(end_) - toInt64(len_us) - g0_s * 1000000) / 60000000)), 0)) AS hi_h,
        arrayMap(x -> assumeNotNull(x), arrayFilter((v, i) -> v IS NOT NULL AND i <= hi_h, rv_p, hid)) AS rvp_h,
        arrayReduce('quantilesExactInclusive(0.025, 0.975)', rvp_h) AS ref_rv24_q,
        length(rvp_h) AS ref_n_rv24,
        toInt64(intDiv(toUnixTimestamp64Micro(end_) - g0_s * 1000000, 60000000) + 1) AS ie_raw,
        toInt64(intDiv(toUnixTimestamp64Micro(end_) - toInt64(len_us) - g0_s * 1000000, 60000000) + 1) AS ib_raw,
        toUInt32(greatest(ie_raw, 0)) AS ie,
        toUInt32(greatest(ib_raw, 0)) AS ib,
        (toUnixTimestamp64Micro(end_) % 60000000) AS fe_us,
        ((toUnixTimestamp64Micro(end_) - toInt64(len_us)) % 60000000) AS fb_us,
        LP[ie] AS jpe,
        LP[ib] AS jpb,
        LB[ie] AS jbe,
        LB[ib] AS jbb,
        LE[ie] AS jee,
        LE[ib] AS jeb,
        if(jpe > 0, P[jpe], NULL) AS p_end,
        if(jpb > 0, P[jpb], NULL) AS p_begin,
        if(jbe > 0 AND (ie - jbe) * 60000000 + fe_us <= greatest(if(jbe > 1 AND LB[jbe - 1] > 0, (jbe - LB[jbe - 1]) * 60, 60), 60) * 1000000, B[jbe], NULL) AS b_end,
        if(jbb > 0 AND (ib - jbb) * 60000000 + fb_us <= greatest(if(jbb > 1 AND LB[jbb - 1] > 0, (jbb - LB[jbb - 1]) * 60, 60), 60) * 1000000, B[jbb], NULL) AS b_begin,
        if(jee > 0 AND (ie - jee) * 60000000 + fe_us <= greatest(if(jee > 1 AND LE[jee - 1] > 0, (jee - LE[jee - 1]) * 60, 60), 60) * 1000000, E[jee], NULL) AS e_end,
        if(jeb > 0 AND (ib - jeb) * 60000000 + fb_us <= greatest(if(jeb > 1 AND LE[jeb - 1] > 0, (jeb - LE[jeb - 1]) * 60, 60), 60) * 1000000, E[jeb], NULL) AS e_begin,
        if({is_btc:UInt8} = 1,
            arrayMap((a, c) -> a / c - 1, arraySlice(P, k + 1, greatest(toInt64(hi) - toInt64(k), 0)), arraySlice(P, 1, greatest(toInt64(hi) - toInt64(k), 0))),
            arrayMap((a, c, d, f) -> a / c - 1 - bb * (d / f - 1),
                arraySlice(P, k + 1, greatest(toInt64(hi) - toInt64(k), 0)), arraySlice(P, 1, greatest(toInt64(hi) - toInt64(k), 0)),
                arraySlice(B, k + 1, greatest(toInt64(hi) - toInt64(k), 0)), arraySlice(B, 1, greatest(toInt64(hi) - toInt64(k), 0)))) AS xn,
        arrayFirstIndex(v -> v IS NOT NULL, xn) AS first_x,
        arrayLastIndex(v -> v IS NOT NULL, xn) AS last_x,
        arrayMap(v -> assumeNotNull(v), arrayFilter(v -> v IS NOT NULL, xn)) AS xs,
        arrayReduce('quantilesExactInclusive(0.025, 0.25, 0.75, 0.975)', xs) AS history_q
    FROM
    (
        SELECT name, len_us, end_, k
        FROM (SELECT 1 AS one)
        ARRAY JOIN
            {names:Array(String)} AS name,
            arrayMap(x -> toUInt64(x), {lens_us:Array(String)}) AS len_us,
            arrayMap(x -> toDateTime64(x, 6, 'UTC'), {ends:Array(String)}) AS end_,
            arrayMap(x -> toUInt32(ceil(toUInt64(x) / 60000000)), {lens_us:Array(String)}) AS k
    ) AS horizons
    CROSS JOIN
    (
        SELECT
            P, B, E, n_pairs_btc, beta_btc_raw, n_pairs_eth, beta_eth_raw,
            arrayFill(x -> x > 0, arrayMap((v, i) -> if(v IS NULL, toUInt32(0), toUInt32(i)), P, arrayEnumerate(P))) AS LP,
            arrayFill(x -> x > 0, arrayMap((v, i) -> if(v IS NULL, toUInt32(0), toUInt32(i)), B, arrayEnumerate(B))) AS LB,
            arrayFill(x -> x > 0, arrayMap((v, i) -> if(v IS NULL, toUInt32(0), toUInt32(i)), E, arrayEnumerate(E))) AS LE,
            arrayFirstIndex(v -> v IS NOT NULL, P) AS first_p,
            arrayLastIndex(v -> v IS NOT NULL, arraySlice(P, 1, hi)) AS last_p,
            arrayFirstIndex(v -> v IS NOT NULL, B) AS first_b,
            arrayLastIndex(v -> v IS NOT NULL, arraySlice(B, 1, n)) AS last_b,
            if(n_pairs_btc >= {min_pairs:UInt32} AND isFinite(beta_btc_raw), {blume:Float64} * beta_btc_raw + (1 - {blume:Float64}), 1.) AS bb,
            if(n_pairs_eth >= {min_pairs:UInt32} AND isFinite(beta_eth_raw), {blume:Float64} * beta_eth_raw + (1 - {blume:Float64}), 1.) AS be,
            ctx_rv24, ctx_r24_q, ctx_rv24_q, ctx_n_r24, ctx_n_rv24,
            ref_rv24, rv_p, hid, ref_hour_first, ref_hour_last,
            arrayMap((v, i, q) -> v IS NOT NULL AND i <= hi AND q > 0, P, arrayEnumerate(P), arrayResize(arrayPushFront(LP, toUInt32(0)), length(LP))) AS gap_ok,
            floor(arrayReduce('quantileExactInclusive(0.5)', arrayMap((i, q) -> (i - q) * 60,
                arrayFilter((i, f) -> f, arrayEnumerate(P), gap_ok),
                arrayFilter((q, f) -> f, arrayResize(arrayPushFront(LP, toUInt32(0)), length(LP)), gap_ok)))) AS history_gap_s
        FROM
        (
            SELECT
                P, B, E, hist, hid, ph, lph, lbh, leh, bh,
                arrayMap((v, w, f) -> f AND v IS NOT NULL AND w IS NOT NULL, lph, lbh, hist) AS pair_b,
                arrayMap((v, w, f) -> f AND v IS NOT NULL AND w IS NOT NULL, lph, leh, hist) AS pair_e,
                length(arrayFilter(f -> f, pair_b)) AS n_pairs_btc,
                length(arrayFilter(f -> f, pair_e)) AS n_pairs_eth,
                arrayReduce('covarPopStable', arrayMap(x -> assumeNotNull(x), arrayFilter((v, f) -> f, lph, pair_b)), arrayMap(x -> assumeNotNull(x), arrayFilter((v, f) -> f, lbh, pair_b)))
                    / arrayReduce('varPopStable', arrayMap(x -> assumeNotNull(x), arrayFilter((v, f) -> f, lbh, pair_b))) AS beta_btc_raw,
                arrayReduce('covarPopStable', arrayMap(x -> assumeNotNull(x), arrayFilter((v, f) -> f, lph, pair_e)), arrayMap(x -> assumeNotNull(x), arrayFilter((v, f) -> f, leh, pair_e)))
                    / arrayReduce('varPopStable', arrayMap(x -> assumeNotNull(x), arrayFilter((v, f) -> f, leh, pair_e))) AS beta_eth_raw,
                arrayMap((x, y) -> if(x IS NULL OR y IS NULL, NULL, x / y - 1), bh, arrayResize(arrayConcat(arrayWithConstant(24, CAST(NULL AS Nullable(Float64))), bh), length(bh))) AS b24h,
                arrayMap(j -> arrayFilter(x -> x IS NOT NULL, arraySlice(lbh, greatest(j - 23, 1), if(j < 24, j, 24))), arrayEnumerate(lbh)) AS frames_b,
                arrayMap(j -> arrayFilter(x -> x IS NOT NULL, arraySlice(lph, greatest(j - 23, 1), if(j < 24, j, 24))), arrayEnumerate(lph)) AS frames_p,
                arrayMap(w -> if(length(w) = 24, arrayReduce('stddevSampStable', arrayMap(x -> assumeNotNull(x), w)), NULL), frames_b) AS rv_b,
                arrayMap(w -> if(length(w) = 24, arrayReduce('stddevSampStable', arrayMap(x -> assumeNotNull(x), w)), NULL), frames_p) AS rv_p,
                arrayMap(x -> assumeNotNull(x), arrayFilter((v, f) -> f AND v IS NOT NULL, b24h, hist)) AS r24_sample,
                arrayMap(x -> assumeNotNull(x), arrayFilter((v, f) -> f AND v IS NOT NULL, rv_b, hist)) AS rvb_sample,
                rv_b[length(rv_b)] AS ctx_rv24,
                arrayReduce('quantilesExactInclusive(0.025, 0.25, 0.975)', r24_sample) AS ctx_r24_q,
                arrayReduce('quantilesExactInclusive(0.025, 0.975)', rvb_sample) AS ctx_rv24_q,
                length(r24_sample) AS ctx_n_r24,
                length(rvb_sample) AS ctx_n_rv24,
                arrayLast(x -> x IS NOT NULL, rv_p) AS ref_rv24,
                arrayMin(arrayConcat([toUInt64(18446744073709551615)], arrayFilter((i, v) -> v IS NOT NULL, hid, ph))) AS ref_first_raw,
                if(ref_first_raw = 18446744073709551615, 0, ref_first_raw) AS ref_hour_first,
                arrayMax(arrayConcat([toUInt64(0)], arrayFilter((i, v) -> v IS NOT NULL, hid, ph))) AS ref_hour_last
            FROM
            (
                SELECT
                    P, B, E, hid, hist, ph, bh,
                    arrayMap((x, y) -> if(x IS NULL OR y IS NULL, NULL, log1p(x / y - 1)), ph, arrayResize(arrayPushFront(ph, CAST(NULL AS Nullable(Float64))), length(ph))) AS lph,
                    arrayMap((x, y) -> if(x IS NULL OR y IS NULL, NULL, log1p(x / y - 1)), bh, arrayResize(arrayPushFront(bh, CAST(NULL AS Nullable(Float64))), length(bh))) AS lbh,
                    arrayMap((x, y) -> if(x IS NULL OR y IS NULL, NULL, log1p(x / y - 1)), eh, arrayResize(arrayPushFront(eh, CAST(NULL AS Nullable(Float64))), length(eh))) AS leh
                FROM
                (
                    SELECT
                        P, B, E,
                        arrayFilter(i -> i % 60 = i0 % 60, arrayEnumerate(P)) AS hid,
                        arrayMap(i -> i <= hi, hid) AS hist,
                        arrayFilter((v, i) -> i % 60 = i0 % 60, P, arrayEnumerate(P)) AS ph,
                        arrayFilter((v, i) -> i % 60 = i0 % 60, B, arrayEnumerate(B)) AS bh,
                        arrayFilter((v, i) -> i % 60 = i0 % 60, E, arrayEnumerate(E)) AS eh
                    FROM
                    (
                        SELECT
                            arrayMap(t -> t.2, s) AS P, arrayMap(t -> t.3, s) AS B, arrayMap(t -> t.4, s) AS E
                        FROM
                        (
                            SELECT arraySort(t -> t.1, groupArray((idx, p, b, e))) AS s
                            FROM
                            (
                                SELECT
                                    toUInt32(intDiv(toInt64(toUnixTimestamp(open_time)) + 60 - g0_s, 60) + 1) AS idx,
                                    anyIf(toFloat64OrNull(c), instrument_id = {ref:String} AND (recorded_at IS NULL OR recorded_at <= assembled)) AS p,
                                    anyIf(toFloat64OrNull(c), instrument_id = {btc:String} AND (recorded_at IS NULL OR recorded_at <= assembled)) AS b,
                                    anyIf(toFloat64OrNull(c), instrument_id = {eth:String} AND (recorded_at IS NULL OR recorded_at <= assembled)) AS e
                                FROM mkt.klines_1m FINAL
                                WHERE instrument_id IN ({ref:String}, {btc:String}, {eth:String})
                                  AND open_time >= fromUnixTimestamp64Micro((g0_s - 60) * 1000000, 'UTC')
                                  AND open_time <= cutoff - INTERVAL 60 SECOND
                                  AND interval_s = 60 AND complete = true
                                  AND toFloat64OrNull(c) > 0
                                GROUP BY idx
                                ORDER BY idx WITH FILL FROM toUInt32(1) TO n + 1
                            )
                        )
                    )
                )
            )
        )
    ) AS arrays
)
ORDER BY length_s, name
