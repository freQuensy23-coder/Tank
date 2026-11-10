-- reference_prices: per spot instrument, what the reference venue, the anchor price and the entry price
-- of a token are chosen from (03 section 5 preamble and 5.7).
-- One row per instrument of {instrument_ids}:
--   last close and last exact-hour close: the close of the last complete one-minute bar with a positive
--     price that closed at or before {cutoff}, within the look-back;
--   flow volume: the native volume (buy + sell quantity) of the 24 hours before {cutoff} when every full
--     minute of them has a complete flow bar and the partial minutes at the two ends (a bound that is not
--     on a minute) are covered by a chain of complete boundary segments; otherwise NULL with the reason
--     in flow_detail (FULL_MINUTE_COVERAGE_INCOMPLETE, BOUNDARY_AGGREGATE_UNAVAILABLE,
--     FLOW_AGGREGATE_FIELDS_MISSING);
--   anchor trade / entry trade: the stored last trade (JSON text of the trade point) before {start} and
--     before {cutoff} that a complete reference point of the flow bars at that bound names, or the last
--     trade of the complete bar that ends at the bound; the latest of them by its own time.
-- A bar, a reference point or a boundary segment counts when it was recorded or observed at or before
-- {assembled} (a missing time counts). Scans: mkt.klines_1m FINAL over the look-back, mkt.flow_bars_1m FINAL
-- over the 24 hours without the JSON columns, and over the minutes at the bounds with them. A source that
-- has no row for an instrument gives has_* = 0 and no value, whatever the join settings.
-- param instrument_ids = ['binance:spot:AVAXUSDT','bybit:spot:AVAXUSDT','coinbase:spot:AVAX-USD','kraken:spot:AVAX/USD','okx:spot:AVAX-USDT','binance:spot:BTCUSDT']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param start = 2026-11-09 14:35:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param lookback_s = 7776000
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {start:DateTime64(6, 'UTC')} AS start_,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {instrument_ids:Array(String)} AS iids,
    cutoff - INTERVAL 1 DAY AS w0,
    toDateTime64(toStartOfMinute(w0), 6, 'UTC') AS floor_start,
    toDateTime64(toStartOfMinute(cutoff), 6, 'UTC') AS floor_end,
    toDateTime64(toStartOfMinute(start_), 6, 'UTC') AS floor_anchor,
    if(w0 != floor_start, floor_start + INTERVAL 60 SECOND, w0) AS ceil_start,
    greatest(0, intDiv(toInt64(toUnixTimestamp(floor_end)) - toInt64(toUnixTimestamp(ceil_start)), 60)) AS expected,
    w0 != floor_start AS edge1,
    cutoff != floor_end AND NOT (edge1 AND floor_end = floor_start) AS edge2,
    least(ceil_start, cutoff) AS edge1_end,
    greatest(w0, floor_end) AS edge2_start,
    toDateTime64(0, 6, 'UTC') AS never
SELECT
    ids.iid AS instrument_id,
    if(ifNull(closes.has, 0) = 1, closes.last_close, NULL) AS last_close,
    if(ifNull(closes.has, 0) = 1, closes.last_close_time, NULL) AS last_close_time,
    if(ifNull(closes.has, 0) = 1, closes.last_close_prec, NULL) AS last_close_prec,
    if(ifNull(closes.has, 0) = 1, closes.last_close_prec_reason, NULL) AS last_close_prec_reason,
    if(ifNull(closes.has, 0) = 1, closes.hourly_close, NULL) AS hourly_close,
    if(ifNull(closes.has, 0) = 1, closes.hourly_close_time, NULL) AS hourly_close_time,
    if(ifNull(closes.has, 0) = 1, closes.hourly_close_prec, NULL) AS hourly_close_prec,
    if(ifNull(closes.has, 0) = 1, closes.hourly_close_prec_reason, NULL) AS hourly_close_prec_reason,
    if(ifNull(flow.has, 0) = 1, flow.volume, NULL) AS flow_volume_native,
    if(ifNull(flow.has, 0) = 1, flow.detail, 'FULL_MINUTE_COVERAGE_INCOMPLETE') AS flow_detail,
    if(ifNull(trades.has, 0) = 1, trades.anchor_trade, NULL) AS anchor_trade,
    if(ifNull(trades.has, 0) = 1, trades.entry_trade, NULL) AS entry_trade
FROM
(
    SELECT arrayJoin(iids) AS iid
) AS ids
LEFT JOIN
(
    SELECT
        instrument_id,
        1 AS has,
        nullIf(argMaxIf(c, open_time, ok), '') AS last_close,
        if(countIf(ok) > 0, argMaxIf(open_time + INTERVAL 60 SECOND, open_time, ok), NULL) AS last_close_time,
        argMaxIf(open_time_prec, open_time, ok) AS last_close_prec,
        argMaxIf(open_time_prec_reason, open_time, ok) AS last_close_prec_reason,
        nullIf(argMaxIf(c, open_time, ok AND (toUnixTimestamp(open_time) + 60) % 3600 = 0), '') AS hourly_close,
        if(countIf(ok AND (toUnixTimestamp(open_time) + 60) % 3600 = 0) > 0,
            argMaxIf(open_time + INTERVAL 60 SECOND, open_time, ok AND (toUnixTimestamp(open_time) + 60) % 3600 = 0), NULL) AS hourly_close_time,
        argMaxIf(open_time_prec, open_time, ok AND (toUnixTimestamp(open_time) + 60) % 3600 = 0) AS hourly_close_prec,
        argMaxIf(open_time_prec_reason, open_time, ok AND (toUnixTimestamp(open_time) + 60) % 3600 = 0) AS hourly_close_prec_reason
    FROM
    (
        SELECT
            instrument_id, open_time, c, open_time_prec, open_time_prec_reason,
            (recorded_at IS NULL OR recorded_at <= assembled) AND complete = true AND interval_s = 60
                AND toFloat64OrNull(c) > 0 AS ok
        FROM mkt.klines_1m FINAL
        WHERE instrument_id IN iids
          AND open_time >= toDateTime64(cutoff - toIntervalSecond({lookback_s:UInt32}), 6, 'UTC')
          AND open_time <= cutoff - INTERVAL 60 SECOND
    )
    GROUP BY instrument_id
) AS closes ON closes.instrument_id = ids.iid
LEFT JOIN
(
    SELECT
        minutes.instrument_id AS instrument_id,
        1 AS has,
        if(NOT ((NOT edge1 OR ifNull(edges.end1, never) = edge1_end) AND (NOT edge2 OR ifNull(edges.end2, never) = cutoff)),
            'BOUNDARY_AGGREGATE_UNAVAILABLE',
            if(minutes.n < expected OR minutes.incomplete > 0, 'FULL_MINUTE_COVERAGE_INCOMPLETE',
                if(minutes.missing + ifNull(edges.missing, 0) > 0, 'FLOW_AGGREGATE_FIELDS_MISSING', NULL))) AS detail,
        if(detail IS NULL, minutes.buy_total + minutes.sell_total + ifNull(edges.volume, 0), NULL) AS volume
    FROM
    (
        SELECT
            instrument_id,
            countIf(ok) AS n,
            countIf(ok AND NOT ifNull(complete, false)) AS incomplete,
            countIf(ok AND (bq IS NULL OR sq IS NULL OR qq IS NULL)) AS missing,
            sumKahanIf(ifNull(bq, 0), ok) AS buy_total,
            sumKahanIf(ifNull(sq, 0), ok) AS sell_total
        FROM
        (
            SELECT
                instrument_id, complete,
                toFloat64OrNull(buy_qty) AS bq, toFloat64OrNull(sell_qty) AS sq, toFloat64OrNull(sum_sq_qty) AS qq,
                (recorded_at IS NULL OR recorded_at <= assembled) AND (received_ts IS NULL OR received_ts <= assembled) AS ok
            FROM mkt.flow_bars_1m FINAL
            WHERE instrument_id IN iids AND minute_start >= ceil_start AND minute_start < floor_end
        )
        GROUP BY instrument_id
    ) AS minutes
    LEFT JOIN
    (
        SELECT
            instrument_id,
            chain1.1 AS end1,
            chain2.1 AS end2,
            chain1.2 + chain2.2 + chain1.3 + chain2.3 AS volume,
            chain1.4 + chain2.4 AS missing
        FROM
        (
            SELECT
                instrument_id,
                arrayFold((acc, x) -> if(x.1 = acc.1, (x.2, acc.2 + ifNull(x.4, 0), acc.3 + ifNull(x.5, 0), acc.4 + toUInt64(x.4 IS NULL OR x.5 IS NULL OR x.6 IS NULL)), acc),
                    arraySort(x -> (x.1, -toInt64(toUnixTimestamp64Micro(x.2))),
                        arrayFilter(x -> x.3 AND edge1 AND w0 <= x.1 AND x.1 < x.2 AND x.2 <= edge1_end, pieces)),
                    (w0, 0., 0., toUInt64(0))) AS chain1,
                arrayFold((acc, x) -> if(x.1 = acc.1, (x.2, acc.2 + ifNull(x.4, 0), acc.3 + ifNull(x.5, 0), acc.4 + toUInt64(x.4 IS NULL OR x.5 IS NULL OR x.6 IS NULL)), acc),
                    arraySort(x -> (x.1, -toInt64(toUnixTimestamp64Micro(x.2))),
                        arrayFilter(x -> x.3 AND edge2 AND edge2_start <= x.1 AND x.1 < x.2 AND x.2 <= cutoff, pieces)),
                    (edge2_start, 0., 0., toUInt64(0))) AS chain2
            FROM
            (
                SELECT
                    instrument_id,
                    arrayFlatten(groupArray(arrayMap(e -> (
                        ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(e, 'start'), 6, 'UTC'), never),
                        ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(e, 'end'), 6, 'UTC'), never),
                        JSONExtractBool(e, 'complete')
                            AND ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(e, 'observed_at'), 6, 'UTC') <= assembled, true),
                        toFloat64OrNull(JSONExtractString(e, 'buy_qty')),
                        toFloat64OrNull(JSONExtractString(e, 'sell_qty')),
                        toFloat64OrNull(JSONExtractString(e, 'sum_sq_qty'))),
                        JSONExtractArrayRaw(ifNull(boundary_segments, '[]'))))) AS pieces
                FROM mkt.flow_bars_1m FINAL
                WHERE instrument_id IN iids
                  AND minute_start IN (floor_start, floor_end)
                  AND (recorded_at IS NULL OR recorded_at <= assembled)
                GROUP BY instrument_id
            )
        )
    ) AS edges ON edges.instrument_id = minutes.instrument_id
) AS flow ON flow.instrument_id = ids.iid
LEFT JOIN
(
    SELECT
        instrument_id,
        1 AS has,
        argMaxIf(raw, ts, kind = 'anchor') AS anchor_trade,
        argMaxIf(raw, ts, kind = 'entry') AS entry_trade
    FROM
    (
        SELECT
            instrument_id, kind, b, raw,
            parseDateTime64BestEffortOrNull(JSONExtractString(raw, 'ts'), 6, 'UTC') AS ts
        FROM
        (
            SELECT
                instrument_id, bound.2 AS kind, bound.1 AS b,
                arrayConcat(
                    arrayMap(pt -> JSONExtractRaw(pt, 'last_trade'),
                        arrayFilter(pt -> parseDateTime64BestEffortOrNull(JSONExtractString(pt, 'boundary'), 6, 'UTC') = b
                                AND JSONExtractBool(pt, 'complete')
                                AND ifNull(parseDateTime64BestEffortOrNull(JSONExtractString(pt, 'observed_at'), 6, 'UTC') <= assembled, true),
                            JSONExtractArrayRaw(ifNull(reference_points, '[]')))),
                    if(ifNull(complete, false) AND minute_start + INTERVAL 60 SECOND = b, [ifNull(last_trade, '')], [])) AS candidates
            FROM mkt.flow_bars_1m FINAL
            ARRAY JOIN [(start_, 'anchor'), (cutoff, 'entry')] AS bound
            WHERE instrument_id IN iids
              AND minute_start IN (floor_anchor, floor_anchor - INTERVAL 60 SECOND, floor_end, floor_end - INTERVAL 60 SECOND)
              AND (minute_start IN (floor_anchor, floor_anchor - INTERVAL 60 SECOND) AND bound.2 = 'anchor'
                OR minute_start IN (floor_end, floor_end - INTERVAL 60 SECOND) AND bound.2 = 'entry')
              AND (recorded_at IS NULL OR recorded_at <= assembled)
        )
        ARRAY JOIN candidates AS raw
    )
    WHERE ts < b AND JSONExtractString(raw, 'price') != ''
    GROUP BY instrument_id
) AS trades ON trades.instrument_id = ids.iid
ORDER BY ids.iid
