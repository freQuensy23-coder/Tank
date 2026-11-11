-- book_fragmentation: for every pair of spot venues, whether their prices differ by more than their spreads at every
-- moment of the last {length_us} before {cutoff}, and the confirmation of the gap by an independent REST book
-- (03 section 5.5 fragmentation).
-- A venue is the group {groups}[i] of {instrument_ids}[i]; its rows are the spot books with integrity_ok at or before
-- {cutoff} that were recorded and received at or before {assembled} (a missing time counts), read from
-- {length_us} + {interval_us} before the cutoff.
-- A pair is looked at in the start of the period, the cutoff and every moment in between at which either venue has a
-- book. At such a moment each venue is represented by its last book at or before it that is not older than
-- {interval_us}. The moment is a `problem` 1 when a venue has none, or a book lacks the mid or the spread, or the
-- mid of the second venue is 0; for venues quoted in different assets the mids are converted to USD (USD as it is,
-- else by quote_usd_rate), and the moment is a `problem` 2 when that is not possible. The first moment with a problem
-- ends the pair: it has no gap. Otherwise the moment has a gap when |mid_a - mid_b| * 10000 > (spread_a + spread_b +
-- cost) * mid_b, with cost {conversion_bps} for venues quoted in different assets and 0 for the same one.
--   persistent  a gap at every moment before the cutoff;
--   closed      a gap at the last moment before the cutoff and none at the cutoff;
--   confirmed   at some moment both books carry an independent PUBLIC_REST book (second_path_confirmed, taken at or
--               before the cutoff, received at or before {assembled}, within {interval_us} of the book, with a mid above
--               0 and a spread) whose mids (converted likewise) differ by more than their spreads and the cost.
-- One result row per pair of venues (a before b by name) that has a book in the rows read.
-- param instrument_ids = ['binance:spot:AVAXUSDT','bybit:spot:AVAXUSDT','coinbase:spot:AVAX-USD','kraken:spot:AVAX/USD','okx:spot:AVAX-USDT']
-- param groups = ['binance','bybit','coinbase','kraken','okx']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param length_us = 900000000
-- param interval_us = 10000000
-- param conversion_bps = 2
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {instrument_ids:Array(String)} AS iids,
    {groups:Array(String)} AS grps,
    toInt64({length_us:UInt64}) AS length_us,
    toInt64({interval_us:UInt64}) AS interval_us,
    {conversion_bps:Float64} AS conversion,
    toInt64(toUnixTimestamp64Micro(cutoff)) AS cutoff_us,
    cutoff_us - length_us AS start_us
SELECT
    a_venue,
    b_venue,
    first_problem AS problem,
    toUInt8(first_problem = 0 AND length(before) > 0 AND arrayAll(r -> r.3 = 1, before)) AS persistent,
    toUInt8(first_problem = 0 AND length(before) > 0 AND before[-1].3 = 1 AND res[-1].3 = 0) AS closed,
    toUInt8(arrayExists(r -> r.4 = 1, res)) AS confirmed
FROM
(
    SELECT
        a_venue, b_venue, res,
        arrayFilter(r -> r.1 < cutoff_us, res) AS before,
        arrayFirstIndex(r -> r.2 > 0, res) AS bad,
        if(bad = 0, 0, res[bad].2) AS first_problem
    FROM
    (
        SELECT
            vr[p.1].1 AS a_venue,
            vr[p.2].1 AS b_venue,
            vr[p.1].2 AS a_rows,
            vr[p.2].2 AS b_rows,
            arraySort(arrayDistinct(arrayConcat(
                [start_us, cutoff_us],
                arrayFilter(t -> t >= start_us AND t < cutoff_us, arrayMap(x -> x.1, a_rows)),
                arrayFilter(t -> t >= start_us AND t < cutoff_us, arrayMap(x -> x.1, b_rows))))) AS probes,
            arrayMap(t -> arrayLastIndex(x -> x.1 <= t, a_rows), probes) AS ia,
            arrayMap(t -> arrayLastIndex(x -> x.1 <= t, b_rows), probes) AS ib,
            arrayMap(i -> a_rows[i], ia) AS xs,
            arrayMap(j -> b_rows[j], ib) AS ys,
            arrayMap((t, i, j) -> i > 0 AND j > 0 AND t - a_rows[i].1 <= interval_us AND t - b_rows[j].1 <= interval_us, probes, ia, ib) AS fresh,
            arrayMap((x, y) -> if(x.5 = y.5, x.2, x.3), xs, ys) AS mxs,
            arrayMap((x, y) -> if(x.5 = y.5, y.2, y.3), xs, ys) AS mys,
            arrayMap((x, y) -> if(x.5 = y.5, 0., conversion), xs, ys) AS costs,
            arrayMap((x, y) -> if(x.5 = y.5, x.7, x.8), xs, ys) AS rxs,
            arrayMap((x, y) -> if(x.5 = y.5, y.7, y.8), xs, ys) AS rys,
            arrayMap(
                (t, usable, x, y, mx, my, cost, rx, ry) -> (
                    t,
                    if(NOT usable OR x.2 IS NULL OR y.2 IS NULL OR x.4 IS NULL OR y.4 IS NULL OR y.2 = 0,
                        1,
                        if(mx IS NULL OR my IS NULL OR my = 0, 2, 0)),
                    toUInt8(ifNull(abs(mx - my) * 10000 > (x.4 + y.4 + cost) * my, 0)),
                    toUInt8(ifNull(x.6 = 1 AND y.6 = 1 AND abs(rx - ry) * 10000 > (x.9 + y.9 + cost) * ry, 0))
                ),
                probes, fresh, xs, ys, mxs, mys, costs, rxs, rys
            ) AS res
        FROM
        (
            SELECT arraySort(groupArray((venue, venue_rows))) AS vr
            FROM
            (
                SELECT venue, arraySort(x -> x.1, groupArray((ts_us, mid, mid_usd, spread, quote, rest_ok, rest_mid, rest_mid_usd, rest_spread))) AS venue_rows
                FROM
                (
                    SELECT
                        venue, ts_us, mid, spread, quote,
                        if(quote = 'USD', mid, if(rate IS NULL OR rate = 0, NULL, mid * rate)) AS mid_usd,
                        toUInt8(
                            second_path_confirmed = true
                            AND JSONExtractString(independent_rest, 'source') = 'PUBLIC_REST'
                            AND rest_at IS NOT NULL AND rest_at <= cutoff
                            AND (rest_received IS NULL OR rest_received <= assembled)
                            AND abs(toUnixTimestamp64Micro(assumeNotNull(rest_at)) - ts_us) <= interval_us
                            AND rest_mid > 0 AND rest_spread IS NOT NULL
                        ) AS rest_ok,
                        rest_mid,
                        if(quote = 'USD', rest_mid, if(rate IS NULL OR rate = 0, NULL, rest_mid * rate)) AS rest_mid_usd,
                        rest_spread
                    FROM
                    (
                        SELECT
                            grps[indexOf(iids, instrument_id)] AS venue,
                            toInt64(toUnixTimestamp64Micro(exchange_ts)) AS ts_us,
                            toFloat64OrNull(mid) AS mid,
                            toFloat64OrNull(spread_bps) AS spread,
                            upperUTF8(ifNull(quote_asset, '')) AS quote,
                            toFloat64OrNull(quote_usd_rate) AS rate,
                            second_path_confirmed,
                            independent_rest,
                            parseDateTime64BestEffortOrNull(JSONExtractString(independent_rest, 'ts'), 6, 'UTC') AS rest_at,
                            parseDateTime64BestEffortOrNull(JSONExtractString(independent_rest, 'received_ts'), 6, 'UTC') AS rest_received,
                            toFloat64OrNull(JSONExtractString(independent_rest, 'mid')) AS rest_mid,
                            toFloat64OrNull(JSONExtractString(independent_rest, 'spread_bps')) AS rest_spread,
                            (recorded_at IS NULL OR recorded_at <= assembled) AND (received_ts IS NULL OR received_ts <= assembled) AS ok
                        FROM mkt.book_summaries FINAL
                        WHERE instrument_id IN iids
                          AND exchange_ts >= cutoff - toIntervalMicrosecond(length_us + interval_us)
                          AND exchange_ts <= cutoff
                          AND integrity_ok = true
                    )
                    WHERE ok
                )
                GROUP BY venue
            )
        )
        ARRAY JOIN arrayFlatten(arrayMap(i -> arrayMap(j -> (i, j), range(i + 1, length(vr) + 1)), range(1, length(vr)))) AS p
    )
)
ORDER BY a_venue, b_venue
