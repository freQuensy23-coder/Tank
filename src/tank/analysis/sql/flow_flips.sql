-- flow_flips: the signed trade flow of every group of instruments in each consecutive 15-minute sub-window of a
-- window (03 section 5.3, the sign of a top venue in every sub-window, which the persistence of a flow reversal
-- is read from).
-- A group is the venue: {groups}[i] names the group of {instrument_ids}[i]. A window is [start, end) with end =
-- {cutoff} and start = end - length; its sub-windows are [start + 900 k s, start + 900 (k + 1) s) for k = 0 ..
-- length div 900 - 1, only for the windows with three or more of them. A sub-window is covered like a window of
-- flow_windows: its full one-minute bars and, where one of its bounds is not on a minute, the chain of complete
-- boundary segments over the partial minute (a bound inside a minute has segments only where one was requested).
-- One row per group and sub-window: expected and minutes (the full minutes it has and found), incomplete (found
-- but not complete), boundary_ok, the quantities buy, sell and sum_sq (sumKahan) and missing (bits 1 buy,
-- 2 sell, 4 sum_sq missing in a bar or a segment summed). A row counts when it was recorded and received at or
-- before {assembled}; a segment also by its own observed_at and received_ts.
-- Scans: mkt.flow_bars_1m FINAL over the longest window, and over the minutes at the sub-window bounds for the
-- boundary segments.
-- param instrument_ids = ['binance:spot:AVAXUSDT','bybit:spot:AVAXUSDT','coinbase:spot:AVAX-USD','kraken:spot:AVAX/USD','okx:spot:AVAX-USDT']
-- param groups = ['binance','bybit','coinbase','kraken','okx']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param names = ['W15M','W1H','W4H','W24H','EVENT']
-- param lens_us = ['900000000','3600000000','14400000000','86400000000','79500000000']
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {instrument_ids:Array(String)} AS iids,
    {groups:Array(String)} AS grps,
    {names:Array(String)} AS names,
    arrayMap(x -> toInt64(x), {lens_us:Array(String)}) AS lens,
    toInt64(900000000) AS step,
    toInt64(toUnixTimestamp64Micro(cutoff)) AS cut_us,
    arrayFilter(w -> intDiv(lens[w], step) >= 3, arrayEnumerate(lens)) AS ws,
    arrayFilter(w -> (cut_us - lens[w]) % 60000000 != 0, ws) AS ragged,
    arrayDistinct(arrayFlatten(arrayMap(w -> arrayMap(j -> (cut_us - lens[w] + j * step) - (cut_us - lens[w] + j * step) % 60000000,
        range(intDiv(lens[w], step) + 1)), ragged))) AS bound_minutes,
    fromUnixTimestamp64Micro(cut_us - arrayMax(arrayConcat([toInt64(0)], arrayMap(w -> lens[w], ws))) - 60000000, 'UTC') AS scan_from,
    toDateTime64(0, 6, 'UTC') AS never
SELECT
    names[base.w] AS window,
    base.k AS k,
    base.grp AS venue,
    base.want AS expected,
    a.minutes AS minutes,
    a.incomplete AS incomplete,
    toUInt8(ifNull(b.ok1 AND b.ok2, 1)) AS boundary_ok,
    a.buy + b.buy AS buy,
    a.sell + b.sell AS sell,
    a.sq + b.sq AS sum_sq,
    toUInt8(bitOr(a.missing, b.missing)) AS missing
FROM
(
    SELECT
        w, k, grp,
        (cut_us - lens[w] + k * step) AS s,
        s + step AS e,
        greatest(0, intDiv(e - e % 60000000 - if(s % 60000000 != 0, s - s % 60000000 + 60000000, s), 60000000)) AS want
    FROM (SELECT arrayJoin(ws) AS w, arrayJoin(arrayDistinct(grps)) AS grp)
    ARRAY JOIN range(intDiv(lens[w], step)) AS k
) AS base
LEFT JOIN
(
    SELECT
        w, k, grp,
        uniqExact(m) AS minutes,
        countIf(NOT done) AS incomplete,
        sumKahan(ifNull(f_buy, 0.)) AS buy,
        sumKahan(ifNull(f_sell, 0.)) AS sell,
        sumKahan(ifNull(f_sq, 0.)) AS sq,
        groupBitOr(toUInt8(f_buy IS NULL) + 2 * toUInt8(f_sell IS NULL) + 4 * toUInt8(f_sq IS NULL)) AS missing
    FROM
    (
        SELECT
            grp, w, m, done, f_buy, f_sell, f_sq,
            intDiv(m - (cut_us - lens[w]), step) AS k
        FROM
        (
            SELECT
                grps[indexOf(iids, instrument_id)] AS grp,
                toInt64(toUnixTimestamp64Micro(minute_start)) AS m,
                complete = true AS done,
                toFloat64OrNull(buy_qty) AS f_buy,
                toFloat64OrNull(sell_qty) AS f_sell,
                toFloat64OrNull(sum_sq_qty) AS f_sq,
                (recorded_at IS NULL OR recorded_at <= assembled) AND (received_ts IS NULL OR received_ts <= assembled) AS ok
            FROM mkt.flow_bars_1m FINAL
            WHERE instrument_id IN iids AND minute_start >= scan_from AND minute_start < cutoff
        )
        ARRAY JOIN ws AS w
        WHERE ok AND m >= cut_us - lens[w]
    )
    WHERE k < intDiv(lens[w], step) AND m + 60000000 <= cut_us - lens[w] + (k + 1) * step
    GROUP BY w, k, grp
) AS a ON a.w = base.w AND a.k = base.k AND a.grp = base.grp
LEFT JOIN
(
    SELECT
        w, k, grp,
        min(c1.1 = ceil_s OR s % 60000000 = 0) AS ok1,
        min(c2.1 = e OR e % 60000000 = 0) AS ok2,
        sumKahan(c1.2 + c2.2) AS buy,
        sumKahan(c1.3 + c2.3) AS sell,
        sumKahan(c1.4 + c2.4) AS sq,
        groupBitOr(bitOr(c1.5, c2.5)) AS missing
    FROM
    (
        SELECT
            grp, w, k, s, e,
            s - s % 60000000 + 60000000 AS ceil_s,
            arrayFold((acc, x) -> if(x.1 = acc.1,
                    (x.2, acc.2 + ifNull(x.4, 0.), acc.3 + ifNull(x.5, 0.), acc.4 + ifNull(x.6, 0.),
                     toUInt8(bitOr(acc.5, toUInt8(x.4 IS NULL) + 2 * toUInt8(x.5 IS NULL) + 4 * toUInt8(x.6 IS NULL)))),
                    acc),
                arraySort(x -> (x.1, -x.2),
                    arrayFilter(x -> x.3 AND s % 60000000 != 0 AND s <= x.1 AND x.1 < x.2 AND x.2 <= ceil_s, pc.pieces)),
                (s, 0., 0., 0., toUInt8(0))) AS c1,
            arrayFold((acc, x) -> if(x.1 = acc.1,
                    (x.2, acc.2 + ifNull(x.4, 0.), acc.3 + ifNull(x.5, 0.), acc.4 + ifNull(x.6, 0.),
                     toUInt8(bitOr(acc.5, toUInt8(x.4 IS NULL) + 2 * toUInt8(x.5 IS NULL) + 4 * toUInt8(x.6 IS NULL)))),
                    acc),
                arraySort(x -> (x.1, -x.2),
                    arrayFilter(x -> x.3 AND e % 60000000 != 0 AND e - e % 60000000 <= x.1 AND x.1 < x.2 AND x.2 <= e, pc.pieces)),
                (e - e % 60000000, 0., 0., 0., toUInt8(0))) AS c2
        FROM
        (
            SELECT
                grps[i] AS grp, iids[i] AS instrument_id, w, k,
                cut_us - lens[w] + k * step AS s,
                cut_us - lens[w] + (k + 1) * step AS e
            FROM (SELECT arrayJoin(ws) AS w, arrayJoin(arrayEnumerate(iids)) AS i)
            ARRAY JOIN range(intDiv(lens[w], step)) AS k
        ) AS base
        LEFT JOIN
        (
            SELECT
                instrument_id,
                arrayFlatten(groupArray(arrayMap(e -> (
                    toInt64(toUnixTimestamp64Micro(ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(e, 'start'), 6, 'UTC'), never))),
                    toInt64(toUnixTimestamp64Micro(ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(e, 'end'), 6, 'UTC'), never))),
                    JSONExtractBool(e, 'complete')
                        AND ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(e, 'observed_at'), 6, 'UTC') <= assembled, true)
                        AND ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(e, 'received_ts'), 6, 'UTC') <= assembled, true),
                    toFloat64OrNull(replaceAll(JSONExtractRaw(e, 'buy_qty'), '"', '')),
                    toFloat64OrNull(replaceAll(JSONExtractRaw(e, 'sell_qty'), '"', '')),
                    toFloat64OrNull(replaceAll(JSONExtractRaw(e, 'sum_sq_qty'), '"', ''))),
                    JSONExtractArrayRaw(ifNull(boundary_segments, '[]'))))) AS pieces
            FROM mkt.flow_bars_1m FINAL
            WHERE instrument_id IN iids
              AND minute_start >= scan_from AND minute_start <= cutoff
              AND has(bound_minutes, toInt64(toUnixTimestamp64Micro(minute_start)))
              AND (recorded_at IS NULL OR recorded_at <= assembled)
            GROUP BY instrument_id
        ) AS pc ON pc.instrument_id = base.instrument_id
    )
    GROUP BY w, k, grp
) AS b ON b.w = base.w AND b.k = base.k AND b.grp = base.grp
ORDER BY base.w, base.k, venue
