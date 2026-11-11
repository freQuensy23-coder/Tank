-- window_volumes: the observed USD volume of the current and the previous window of every window length, per
-- instrument (03 section 5.4, spot and perp volume and the incremental share).
-- A window is [start, end) with end = {cutoff} and start = end - length; the previous window is
-- [start - length, start). Per instrument and window the volume is
--   the sum of quote_volume_usd of the complete one-minute klines, when the window starts and ends on a minute
--     and every minute of it has a complete kline with a quote_volume_usd; otherwise
--   the sum of buy_usd + sell_usd of the flow bars, when every full minute of the window has a complete flow
--     bar with both notionals and the partial minutes at the two ends (a bound that is not on a minute) are
--     covered by a chain of complete boundary segments with both notionals; otherwise NULL.
-- One row per instrument of {instrument_ids} and window; an instrument without rows has NULL volumes.
-- A row counts when it was recorded or received at or before {assembled} (a missing time counts); a segment
-- also by its own observed_at and received_ts.
-- Scans: mkt.klines_1m FINAL and mkt.flow_bars_1m FINAL over twice the longest window without the JSON column,
-- and over the minutes at the window bounds with it. The windows are the ranges ra[p] .. rb[p] below,
-- p = 1..L the current windows and p = L+1..2L the previous ones, in microseconds.
-- param instrument_ids = ['binance:spot:AVAXUSDT','bybit:spot:AVAXUSDT','coinbase:spot:AVAX-USD','kraken:spot:AVAX/USD','okx:spot:AVAX-USDT','binance:perp_linear:AVAXUSDT','okx:perp_linear:AVAX-USDT-SWAP','bybit:perp_linear:AVAXUSDT','hyperliquid:perp_linear:AVAX']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param names = ['W15M','W1H','W4H','W24H','EVENT']
-- param lens_us = ['900000000','3600000000','14400000000','86400000000','79500000000']
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {instrument_ids:Array(String)} AS iids,
    {names:Array(String)} AS names,
    arrayMap(x -> toInt64(x), {lens_us:Array(String)}) AS lens,
    length(lens) AS n_windows,
    toInt64(toUnixTimestamp64Micro(cutoff)) AS cut_us,
    arrayConcat(arrayMap(l -> cut_us - l, lens), arrayMap(l -> cut_us - 2 * l, lens)) AS ra,
    arrayConcat(arrayMap(l -> cut_us, lens), arrayMap(l -> cut_us - l, lens)) AS rb,
    arrayMap(a -> if(a % 60000000 != 0, a - a % 60000000 + 60000000, a), ra) AS ceil_a,
    arrayMap(b -> b - b % 60000000, rb) AS floor_b,
    arrayMap(a -> a - a % 60000000, ra) AS floor_a,
    arrayMap((c, f) -> greatest(0, intDiv(f - c, 60000000)), ceil_a, floor_b) AS expected,
    arrayMap(a -> a % 60000000 != 0, ra) AS edge1,
    arrayMap((a, b) -> b % 60000000 != 0 AND NOT (a % 60000000 != 0 AND b - b % 60000000 = a - a % 60000000), ra, rb) AS edge2,
    arrayMap((c, b) -> least(c, b), ceil_a, rb) AS edge1_end,
    arrayMap((a, f) -> greatest(a, f), ra, floor_b) AS edge2_start,
    fromUnixTimestamp64Micro(cut_us - 2 * arrayMax(lens), 'UTC') AS scan_from,
    toDateTime64(0, 6, 'UTC') AS never
SELECT
    names[w] AS window,
    instrument_id,
    maxIf(usd, p <= n_windows) AS current_usd,
    maxIf(usd, p > n_windows) AS previous_usd
FROM
(
    SELECT
        base.instrument_id AS instrument_id,
        base.p AS p,
        ((p - 1) % n_windows) + 1 AS w,
        ra[p] % 60000000 = 0 AND rb[p] % 60000000 = 0 AND kl.n > 0
            AND kl.n = intDiv(rb[p] - ra[p], 60000000) AS from_klines,
        (expected[p] > 0 OR edge1[p] OR edge2[p])
            AND fm.n >= expected[p] AND fm.incomplete = 0 AND fm.missing + fe.missing = 0
            AND (NOT edge1[p] OR fe.end1 = edge1_end[p]) AND (NOT edge2[p] OR fe.end2 = rb[p]) AS from_flow,
        multiIf(from_klines, kl.total, from_flow, fm.total + fe.volume, NULL) AS usd
    FROM
    (
        SELECT arrayJoin(iids) AS instrument_id, arrayJoin(arrayEnumerate(ra)) AS p
    ) AS base
    LEFT JOIN
    (
        SELECT
            instrument_id, p,
            countIf(ok) AS n,
            sumKahanIf(v, ok) AS total
        FROM
        (
            SELECT
                instrument_id,
                toInt64(toUnixTimestamp64Micro(open_time)) AS m,
                toFloat64OrNull(quote_volume_usd) AS v,
                (recorded_at IS NULL OR recorded_at <= assembled) AND complete = true AND interval_s = 60
                    AND v IS NOT NULL AS ok
            FROM mkt.klines_1m FINAL
            WHERE instrument_id IN iids AND open_time >= scan_from AND open_time < cutoff
        )
        ARRAY JOIN arrayEnumerate(ra) AS p
        WHERE m >= ra[p] AND m < rb[p]
        GROUP BY instrument_id, p
    ) AS kl ON kl.instrument_id = base.instrument_id AND kl.p = base.p
    LEFT JOIN
    (
        SELECT
            instrument_id, p,
            countIf(ok) AS n,
            countIf(ok AND NOT ifNull(complete, false)) AS incomplete,
            countIf(ok AND (bu IS NULL OR su IS NULL)) AS missing,
            sumKahanIf(ifNull(bu, 0) + ifNull(su, 0), ok) AS total
        FROM
        (
            SELECT
                instrument_id, complete,
                toInt64(toUnixTimestamp64Micro(minute_start)) AS m,
                toFloat64OrNull(buy_usd) AS bu, toFloat64OrNull(sell_usd) AS su,
                (recorded_at IS NULL OR recorded_at <= assembled) AND (received_ts IS NULL OR received_ts <= assembled) AS ok
            FROM mkt.flow_bars_1m FINAL
            WHERE instrument_id IN iids AND minute_start >= scan_from AND minute_start < cutoff
        )
        ARRAY JOIN arrayEnumerate(ra) AS p
        WHERE m >= ceil_a[p] AND m < floor_b[p]
        GROUP BY instrument_id, p
    ) AS fm ON fm.instrument_id = base.instrument_id AND fm.p = base.p
    LEFT JOIN
    (
        SELECT
            instrument_id, p,
            chain1.1 AS end1,
            chain2.1 AS end2,
            chain1.2 + chain2.2 + chain1.3 + chain2.3 AS volume,
            chain1.4 + chain2.4 AS missing
        FROM
        (
            SELECT
                instrument_id, p, pieces,
                arrayFold((acc, x) -> if(x.1 = acc.1, (x.2, acc.2 + ifNull(x.4, 0), acc.3 + ifNull(x.5, 0), acc.4 + toUInt64(x.4 IS NULL OR x.5 IS NULL)), acc),
                    arraySort(x -> (x.1, -x.2),
                        arrayFilter(x -> x.3 AND edge1[p] AND ra[p] <= x.1 AND x.1 < x.2 AND x.2 <= edge1_end[p], pieces)),
                    (ra[p], 0., 0., toUInt64(0))) AS chain1,
                arrayFold((acc, x) -> if(x.1 = acc.1, (x.2, acc.2 + ifNull(x.4, 0), acc.3 + ifNull(x.5, 0), acc.4 + toUInt64(x.4 IS NULL OR x.5 IS NULL)), acc),
                    arraySort(x -> (x.1, -x.2),
                        arrayFilter(x -> x.3 AND edge2[p] AND edge2_start[p] <= x.1 AND x.1 < x.2 AND x.2 <= rb[p], pieces)),
                    (edge2_start[p], 0., 0., toUInt64(0))) AS chain2
            FROM
            (
                SELECT
                    instrument_id,
                    arrayFlatten(groupArray(arrayMap(e -> (
                        toInt64(toUnixTimestamp64Micro(ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(e, 'start'), 6, 'UTC'), never))),
                        toInt64(toUnixTimestamp64Micro(ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(e, 'end'), 6, 'UTC'), never))),
                        JSONExtractBool(e, 'complete')
                            AND ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(e, 'observed_at'), 6, 'UTC') <= assembled, true)
                            AND ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(e, 'received_ts'), 6, 'UTC') <= assembled, true),
                        toFloat64OrNull(JSONExtractString(e, 'buy_notional')),
                        toFloat64OrNull(JSONExtractString(e, 'sell_notional'))),
                        JSONExtractArrayRaw(ifNull(boundary_segments, '[]'))))) AS pieces
                FROM mkt.flow_bars_1m FINAL
                WHERE instrument_id IN iids
                  AND minute_start >= scan_from AND minute_start <= cutoff
                  AND has(arrayConcat(floor_a, floor_b), toInt64(toUnixTimestamp64Micro(minute_start)))
                  AND (recorded_at IS NULL OR recorded_at <= assembled)
                GROUP BY instrument_id
            )
            ARRAY JOIN arrayEnumerate(ra) AS p
        )
    ) AS fe ON fe.instrument_id = base.instrument_id AND fe.p = base.p
)
GROUP BY w, instrument_id
ORDER BY w, instrument_id
