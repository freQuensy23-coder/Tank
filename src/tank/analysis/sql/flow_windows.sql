-- flow_windows: the signed trade flow of every group of instruments in the current and the previous window of
-- every window length (03 section 5.3: spot flow, cumulative volume delta, breadth, concentration, cohort comparison,
-- velocity and size profile; perp taker flow).
-- A group is the venue for the spot flow and the instrument for the perp taker flow: {groups}[i] names the group
-- of {instrument_ids}[i]. A window is [start, end) with end = {cutoff} and start = end - length; the previous window
-- is [start - length, start). A bound that is not on a minute leaves a partial minute at the start and/or at the end
-- of the window, which a chain of complete boundary segments of the minute's bar has to cover (the same rule as
-- window_volumes). Per group and window:
--   minutes      the distinct full one-minute bars inside the window and expected the minutes the window has;
--   incomplete   the number of those bars that are not complete;
--   boundary_ok  1 when every instrument of the group has a chain of complete segments over each partial minute;
--   buy, sell, sum_sq the quantities of those bars and of the segment chains (sumKahan), buy_usd and sell_usd the
--     notionals, large_buy and large_sell the notional of the size buckets named in {large};
--   missing      the bits of the values missing in the bars and the segments summed (1 buy, 2 sell, 4 sum_sq,
--     8 buy_usd, 16 sell_usd, 32 size buckets absent or without a notional);
--   reasons      the distinct gap reasons of those bars and the gap or coverage reasons of every boundary segment
--     of the bars scanned;
--   left_pieces, right_pieces  the chosen segments as stored JSON, at the start and at the end of the window.
-- One row per group and window, whatever the rows found: a group without a bar has minutes = 0.
-- A row counts when it was recorded and received at or before {assembled} (a missing time counts); a segment also
-- by its own observed_at and received_ts. Scans: mkt.flow_bars_1m FINAL over twice the longest window and a minute
-- more (the partial minute at its oldest bound) with the JSON column of the size buckets, over the same range for
-- the reasons of the boundary segments, and over the minutes at the window bounds with the segments themselves.
-- param instrument_ids = ['binance:spot:AVAXUSDT','bybit:spot:AVAXUSDT','coinbase:spot:AVAX-USD','kraken:spot:AVAX/USD','okx:spot:AVAX-USDT']
-- param groups = ['binance','bybit','coinbase','kraken','okx']
-- param large = ['B4','B5']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param names = ['W15M','W1H','W4H','W24H','EVENT']
-- param lens_us = ['900000000','3600000000','14400000000','86400000000','79500000000']
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {instrument_ids:Array(String)} AS iids,
    {groups:Array(String)} AS grps,
    {large:Array(String)} AS large,
    {names:Array(String)} AS names,
    arrayMap(x -> toInt64(x), {lens_us:Array(String)}) AS lens,
    length(lens) AS n_windows,
    toInt64(toUnixTimestamp64Micro(cutoff)) AS cut_us,
    arrayConcat(arrayMap(l -> cut_us - l, lens), arrayMap(l -> cut_us - 2 * l, lens)) AS ra,
    arrayConcat(arrayMap(l -> cut_us, lens), arrayMap(l -> cut_us - l, lens)) AS rb,
    arrayMap(a -> if(a % 60000000 != 0, a - a % 60000000 + 60000000, a), ra) AS ceil_a,
    arrayMap(b -> b - b % 60000000, rb) AS floor_b,
    arrayMap(a -> a - a % 60000000, ra) AS floor_a,
    arrayMap((c, f) -> greatest(0, intDiv(f - c, 60000000)), ceil_a, floor_b) AS want,
    arrayMap(a -> a % 60000000 != 0, ra) AS edge1,
    arrayMap((a, b) -> b % 60000000 != 0 AND NOT (a % 60000000 != 0 AND b - b % 60000000 = a - a % 60000000), ra, rb) AS edge2,
    arrayMap((c, b) -> least(c, b), ceil_a, rb) AS edge1_end,
    arrayMap((a, f) -> greatest(a, f), ra, floor_b) AS edge2_start,
    fromUnixTimestamp64Micro(cut_us - 2 * arrayMax(lens) - 60000000, 'UTC') AS scan_from,
    toDateTime64(0, 6, 'UTC') AS never
SELECT
    names[w] AS window,
    toUInt8(base.p > n_windows) AS previous,
    base.grp AS venue,
    want[base.p] AS expected,
    a.minutes AS minutes,
    a.incomplete AS incomplete,
    toUInt8(ifNull(b.ok1 AND b.ok2, 1)) AS boundary_ok,
    a.buy + b.buy AS buy,
    a.sell + b.sell AS sell,
    a.sq + b.sq AS sum_sq,
    a.buy_usd + b.buy_usd AS buy_usd,
    a.sell_usd + b.sell_usd AS sell_usd,
    a.large_buy + b.large_buy AS large_buy,
    a.large_sell + b.large_sell AS large_sell,
    toUInt8(bitOr(a.missing, b.missing)) AS missing,
    arraySort(arrayDistinct(arrayConcat(a.reasons, r.reasons))) AS reasons,
    b.left_pieces AS left_pieces,
    b.right_pieces AS right_pieces
FROM
(
    SELECT grp, p, ((p - 1) % n_windows) + 1 AS w
    FROM (SELECT arrayJoin(arrayDistinct(grps)) AS grp, arrayJoin(arrayEnumerate(ra)) AS p)
) AS base
LEFT JOIN
(
    SELECT
        grp, p,
        uniqExact(m) AS minutes,
        countIf(NOT done) AS incomplete,
        sumKahan(ifNull(f_buy, 0.)) AS buy,
        sumKahan(ifNull(f_sell, 0.)) AS sell,
        sumKahan(ifNull(f_sq, 0.)) AS sq,
        sumKahan(ifNull(f_bu, 0.)) AS buy_usd,
        sumKahan(ifNull(f_su, 0.)) AS sell_usd,
        sumKahan(f_lb) AS large_buy,
        sumKahan(f_ls) AS large_sell,
        groupBitOr(toUInt8(f_buy IS NULL) + 2 * toUInt8(f_sell IS NULL) + 4 * toUInt8(f_sq IS NULL)
            + 8 * toUInt8(f_bu IS NULL) + 16 * toUInt8(f_su IS NULL) + 32 * toUInt8(size_bad)) AS missing,
        groupUniqArrayIf(gap_reason, gap_reason IS NOT NULL AND gap_reason != '') AS reasons
    FROM
    (
        SELECT
            grps[indexOf(iids, instrument_id)] AS grp,
            toInt64(toUnixTimestamp64Micro(minute_start)) AS m,
            complete = true AS done,
            toFloat64OrNull(buy_qty) AS f_buy,
            toFloat64OrNull(sell_qty) AS f_sell,
            toFloat64OrNull(sum_sq_qty) AS f_sq,
            toFloat64OrNull(buy_usd) AS f_bu,
            toFloat64OrNull(sell_usd) AS f_su,
            arraySum(arrayMap(k -> ifNull(toFloat64OrNull(replaceAll(JSONExtractRaw(ifNull(size_buckets, ''), k, 'buy_notional'), '"', '')), 0.), large)) AS f_lb,
            arraySum(arrayMap(k -> ifNull(toFloat64OrNull(replaceAll(JSONExtractRaw(ifNull(size_buckets, ''), k, 'sell_notional'), '"', '')), 0.), large)) AS f_ls,
            JSONExtractRaw(ifNull(size_buckets, '')) IN ('', 'null', '{}')
                OR arrayExists(k -> toFloat64OrNull(replaceAll(JSONExtractRaw(ifNull(size_buckets, ''), k, 'buy_notional'), '"', '')) IS NULL
                    OR toFloat64OrNull(replaceAll(JSONExtractRaw(ifNull(size_buckets, ''), k, 'sell_notional'), '"', '')) IS NULL, large) AS size_bad,
            gap_reason,
            (recorded_at IS NULL OR recorded_at <= assembled) AND (received_ts IS NULL OR received_ts <= assembled) AS ok
        FROM mkt.flow_bars_1m FINAL
        WHERE instrument_id IN iids AND minute_start >= scan_from AND minute_start < cutoff
    )
    ARRAY JOIN arrayEnumerate(ra) AS p
    WHERE ok AND m >= ceil_a[p] AND m < floor_b[p]
    GROUP BY grp, p
) AS a ON a.grp = base.grp AND a.p = base.p
LEFT JOIN
(
    SELECT
        grp, p,
        min(c1.1 = edge1_end[p] OR NOT edge1[p]) AS ok1,
        min(c2.1 = rb[p] OR NOT edge2[p]) AS ok2,
        sumKahan(c1.2 + c2.2) AS buy,
        sumKahan(c1.3 + c2.3) AS sell,
        sumKahan(c1.4 + c2.4) AS sq,
        sumKahan(c1.5 + c2.5) AS buy_usd,
        sumKahan(c1.6 + c2.6) AS sell_usd,
        sumKahan(c1.7 + c2.7) AS large_buy,
        sumKahan(c1.8 + c2.8) AS large_sell,
        groupBitOr(bitOr(c1.9, c2.9)) AS missing,
        arrayFlatten(groupArray(c1.10)) AS left_pieces,
        arrayFlatten(groupArray(c2.10)) AS right_pieces
    FROM
    (
        SELECT
            base.grp AS grp, base.p AS p,
            arrayFold((acc, x) -> if(x.1 = acc.1,
                    (x.2, acc.2 + ifNull(x.4, 0.), acc.3 + ifNull(x.5, 0.), acc.4 + ifNull(x.6, 0.), acc.5 + ifNull(x.7, 0.), acc.6 + ifNull(x.8, 0.),
                     acc.7 + x.9, acc.8 + x.10,
                     toUInt8(bitOr(acc.9, toUInt8(x.4 IS NULL) + 2 * toUInt8(x.5 IS NULL) + 4 * toUInt8(x.6 IS NULL) + 8 * toUInt8(x.7 IS NULL)
                        + 16 * toUInt8(x.8 IS NULL) + 32 * x.11)),
                     arrayPushBack(acc.10, x.12)),
                    acc),
                arraySort(x -> (x.1, -x.2),
                    arrayFilter(x -> x.3 AND edge1[p] AND ra[p] <= x.1 AND x.1 < x.2 AND x.2 <= edge1_end[p], pc.pieces)),
                (ra[p], 0., 0., 0., 0., 0., 0., 0., toUInt8(0), CAST([], 'Array(String)'))) AS c1,
            arrayFold((acc, x) -> if(x.1 = acc.1,
                    (x.2, acc.2 + ifNull(x.4, 0.), acc.3 + ifNull(x.5, 0.), acc.4 + ifNull(x.6, 0.), acc.5 + ifNull(x.7, 0.), acc.6 + ifNull(x.8, 0.),
                     acc.7 + x.9, acc.8 + x.10,
                     toUInt8(bitOr(acc.9, toUInt8(x.4 IS NULL) + 2 * toUInt8(x.5 IS NULL) + 4 * toUInt8(x.6 IS NULL) + 8 * toUInt8(x.7 IS NULL)
                        + 16 * toUInt8(x.8 IS NULL) + 32 * x.11)),
                     arrayPushBack(acc.10, x.12)),
                    acc),
                arraySort(x -> (x.1, -x.2),
                    arrayFilter(x -> x.3 AND edge2[p] AND edge2_start[p] <= x.1 AND x.1 < x.2 AND x.2 <= rb[p], pc.pieces)),
                (edge2_start[p], 0., 0., 0., 0., 0., 0., 0., toUInt8(0), CAST([], 'Array(String)'))) AS c2
        FROM
        (
            SELECT grps[i] AS grp, iids[i] AS instrument_id, p
            FROM (SELECT arrayJoin(arrayEnumerate(iids)) AS i, arrayJoin(arrayEnumerate(ra)) AS p)
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
                    toFloat64OrNull(replaceAll(JSONExtractRaw(e, 'sum_sq_qty'), '"', '')),
                    toFloat64OrNull(replaceAll(JSONExtractRaw(e, 'buy_notional'), '"', '')),
                    toFloat64OrNull(replaceAll(JSONExtractRaw(e, 'sell_notional'), '"', '')),
                    arraySum(arrayMap(k -> ifNull(toFloat64OrNull(replaceAll(JSONExtractRaw(e, 'size_buckets', k, 'buy_notional'), '"', '')), 0.), large)),
                    arraySum(arrayMap(k -> ifNull(toFloat64OrNull(replaceAll(JSONExtractRaw(e, 'size_buckets', k, 'sell_notional'), '"', '')), 0.), large)),
                    toUInt8(JSONExtractRaw(e, 'size_buckets') IN ('', 'null', '{}')
                        OR arrayExists(k -> toFloat64OrNull(replaceAll(JSONExtractRaw(e, 'size_buckets', k, 'buy_notional'), '"', '')) IS NULL
                            OR toFloat64OrNull(replaceAll(JSONExtractRaw(e, 'size_buckets', k, 'sell_notional'), '"', '')) IS NULL, large)),
                    e),
                    JSONExtractArrayRaw(ifNull(boundary_segments, '[]'))))) AS pieces
            FROM mkt.flow_bars_1m FINAL
            WHERE instrument_id IN iids
              AND minute_start >= scan_from AND minute_start <= cutoff
              AND has(arrayConcat(floor_a, floor_b), toInt64(toUnixTimestamp64Micro(minute_start)))
              AND (recorded_at IS NULL OR recorded_at <= assembled)
            GROUP BY instrument_id
        ) AS pc ON pc.instrument_id = base.instrument_id
    )
    GROUP BY grp, p
) AS b ON b.grp = base.grp AND b.p = base.p
LEFT JOIN
(
    SELECT
        grp,
        arrayDistinct(arrayFlatten(groupArray(found))) AS reasons
    FROM
    (
        SELECT
            grps[indexOf(iids, instrument_id)] AS grp,
            arrayFilter(x -> x != '', arrayMap(e -> if(JSONExtractString(e, 'gap_reason') != '', JSONExtractString(e, 'gap_reason'), JSONExtractString(e, 'coverage_reason')),
                JSONExtractArrayRaw(ifNull(boundary_segments, '[]')))) AS found
        FROM mkt.flow_bars_1m FINAL
        WHERE instrument_id IN iids AND minute_start >= scan_from AND minute_start <= cutoff
          AND (recorded_at IS NULL OR recorded_at <= assembled)
          AND ifNull(boundary_segments, '[]') NOT IN ('', '[]')
    )
    GROUP BY grp
) AS r ON r.grp = base.grp
ORDER BY previous, base.p, venue
