-- book_liquidity: the depth, the spread, the depth drop, the depth of the side a flow hits, the sell bursts and
-- the ask-side flags of every spot venue, per window (03 section 5.5 liquidity truth, section 5.8 support).
-- A venue is the group {groups}[i] of {instrument_ids}[i]; its rows are the spot books with integrity_ok and its
-- one-minute flow bars that are complete. A window is [cutoff - length, cutoff]; the pre-event hour is
-- [anchor - 1 h, anchor) (no anchor: {anchor_us} is empty and there is no pre-event hour). One result row per
-- window and venue that has a book or a bar in the rows read, so the venue columns are the same in every window.
-- Rows read: books from {since} (minus {interval_us}, the age of the book a bar may look back to) to the cutoff,
-- bars from the earliest window start to the last whole minute before the cutoff. A row counts when it was
-- recorded and received at or before {assembled} (a missing time counts).
-- Depth of a side is the USD (or native) depth of the band {band_key} when range_covered_pct of that side reaches
-- {band_pct}, else NULL; it is never extrapolated.
--   books                         the books read;
--   latest_*                      the latest book at or before the cutoff: time, precision of the time, spread in bps,
--                                 depth of the bid and ask side in USD and native, range_covered_pct as stored;
--   pre_*                         the books of the pre-event hour: their number, the means of the bid and the ask depth
--                                 over the books that have it, of the sum of both over the books that have both, of
--                                 the native depths (and `pre_native_missing`, bit 1 bid and 2 ask: a pre-event book
--                                 has no native depth of that side), of the spread;
--   window_bid, window_ask        the means of the bid and the ask depth of the books in the window;
--   bars                          the complete bars of the window;
--   bursts ...                    a burst is a bar whose net selling (sell minus buy notional) is at least the bid
--                                 depth of the last book before it, not older than {interval_us}; it is replenished
--                                 when a later book within {refill_us} (and before the cutoff) has a bid depth back
--                                 at that level (NULL while the cutoff is nearer than {refill_us}), resilient when
--                                 no later bar within {resilience_us} has a lower low (NULL without one or while the
--                                 cutoff is nearer); the `burst_*` arrays list the bursts by
--                                 time with the time to the first such book;
--   ask_*                         the ask-side flags: `ask_building` the ask depth of the book at the window end
--                                 against the one at its start (the last book at or before it, not older than
--                                 {interval_us}), NULL without both; `ask_testable` the bars whose net buying is at
--                                 least the ask depth (band p1, any coverage) of the book before them with {refill_us}
--                                 left before the cutoff, `ask_recovered` those followed by a book within {refill_us}
--                                 with the ask depth back at that level.
-- Shape: books and bars on one timeline per venue; window functions give each bar the book before it, the books of
-- the next {refill_us} and the lowest low of the next {resilience_us}; the windows are cut from those rows.
-- param instrument_ids = ['binance:spot:AVAXUSDT','bybit:spot:AVAXUSDT','coinbase:spot:AVAX-USD','kraken:spot:AVAX/USD','okx:spot:AVAX-USDT']
-- param groups = ['binance','bybit','coinbase','kraken','okx']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param since = 2026-11-09 12:40:00.000000
-- param anchor_us = 1794234900000000
-- param names = ['W15M','W1H','W4H','W24H','EVENT']
-- param lens_us = ['900000000','3600000000','14400000000','86400000000','79500000000']
-- param band_key = p1
-- param band_pct = 1
-- param interval_us = 10000000
-- param refill_us = 60000000
-- param resilience_us = 900000000
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {since:DateTime64(6, 'UTC')} AS since,
    {instrument_ids:Array(String)} AS iids,
    {groups:Array(String)} AS grps,
    {names:Array(String)} AS names,
    arrayMap(x -> toInt64(x), {lens_us:Array(String)}) AS lens,
    toInt64OrNull({anchor_us:String}) AS anchor_us,
    toInt64({interval_us:UInt64}) AS interval_us,
    toInt64({refill_us:UInt64}) AS refill_us,
    toInt64({resilience_us:UInt64}) AS resilience_us,
    toInt64(toUnixTimestamp64Micro(cutoff)) AS cutoff_us,
    arrayMap(l -> cutoff_us - l, lens) AS starts,
    fromUnixTimestamp64Micro(arrayMin(starts), 'UTC') AS first_start,
    CAST(NULL AS Nullable(Float64)) AS nil,
    CAST(NULL AS Nullable(String)) AS nils
SELECT
    window, venue, books, latest_at, latest_prec, latest_prec_reason, latest_spread, latest_bid, latest_ask,
    latest_bid_native, latest_ask_native, latest_covered, pre_books, pre_bid, pre_ask, pre_bid_native, pre_ask_native,
    pre_native_missing, pre_depth, pre_spread, window_bid, window_ask, bars, bursts, replenished, resilient_bursts,
    burst_at, burst_replenished, burst_recovery_s, burst_resilient, ask_building,
    ask_testable_bars, ask_recovered_bars
FROM
(
    SELECT
        names[w] AS window,
        venue,
        countIf(kind = 1) AS books,
        if(latest.1 = 0, NULL, fromUnixTimestamp64Micro(latest.1, 'UTC')) AS latest_at,
        latest.8 AS latest_prec,
        latest.9 AS latest_prec_reason,
        latest.2 AS latest_spread,
        latest.3 AS latest_bid,
        latest.4 AS latest_ask,
        latest.5 AS latest_bid_native,
        latest.6 AS latest_ask_native,
        latest.7 AS latest_covered,
        countIf(kind = 1 AND pre) AS pre_books,
        avgIf(bid, kind = 1 AND pre AND bid IS NOT NULL) AS pre_bid,
        avgIf(ask, kind = 1 AND pre AND ask IS NOT NULL) AS pre_ask,
        avgIf(bid_native, kind = 1 AND pre AND bid_native IS NOT NULL) AS pre_bid_native,
        avgIf(ask_native, kind = 1 AND pre AND ask_native IS NOT NULL) AS pre_ask_native,
        groupBitOrIf(toUInt8(bid_native IS NULL) + 2 * toUInt8(ask_native IS NULL), kind = 1 AND pre) AS pre_native_missing,
        avgIf(bid + ask, kind = 1 AND pre AND bid IS NOT NULL AND ask IS NOT NULL) AS pre_depth,
        avgIf(spread, kind = 1 AND pre AND spread IS NOT NULL) AS pre_spread,
        avgIf(bid, kind = 1 AND ts_us >= starts[w] AND bid IS NOT NULL) AS window_bid,
        avgIf(ask, kind = 1 AND ts_us >= starts[w] AND ask IS NOT NULL) AS window_ask,
        countIf(kind = 2 AND ts_us >= starts[w]) AS bars,
        countIf(burst AND ts_us >= starts[w]) AS bursts,
        countIf(burst AND ts_us >= starts[w] AND repl_flag = 1) AS replenished,
        countIf(burst AND ts_us >= starts[w] AND repl_flag = 1 AND resil_flag = 1) AS resilient_bursts,
        arraySort(groupArrayIf((ts_us, repl_flag, if(recovered_us != 0, (recovered_us - ts_us) / 1000000, nil), resil_flag), burst AND ts_us >= starts[w])) AS found,
        arrayMap(x -> fromUnixTimestamp64Micro(x.1, 'UTC'), found) AS burst_at,
        arrayMap(x -> x.2, found) AS burst_replenished,
        arrayMap(x -> x.3, found) AS burst_recovery_s,
        arrayMap(x -> x.4, found) AS burst_resilient,
        argMaxIf((toUInt8(ask_any IS NOT NULL), ifNull(ask_any, 0.)), ts_us, kind = 1 AND ts_us <= starts[w] AND ts_us >= starts[w] - interval_us) AS before,
        argMaxIf((toUInt8(ask_any IS NOT NULL), ifNull(ask_any, 0.)), ts_us, kind = 1) AS after,
        if(before.1 = 1 AND after.1 = 1, toUInt8(after.2 > before.2), NULL) AS ask_building,
        countIf(ask_testable AND ts_us >= starts[w]) AS ask_testable_bars,
        countIf(ask_testable AND ts_us >= starts[w] AND ask_recovered) AS ask_recovered_bars,
        argMaxIf((ts_us, spread, bid, ask, bid_native, ask_native, covered, prec, prec_reason), ts_us, kind = 1) AS latest
    FROM
    (
        SELECT
            venue, ts_us, kind, spread, bid, ask, bid_native, ask_native, ask_any, covered, prec, prec_reason,
            kind = 1 AND anchor_us IS NOT NULL AND ts_us >= anchor_us - 3600000000 AND ts_us < anchor_us AS pre,
            kind = 2 AND base.1 != 0 AND base.1 >= ts_us - interval_us AS has_base,
            has_base AND net IS NOT NULL AND base.2 IS NOT NULL AND net >= base.2 AS burst,
            ifNull(arrayFirst(x -> x.2 >= base.2 AND x.1 <= cutoff_us, fut_books).1, 0) AS recovered_us,
            if(burst AND ts_us + refill_us <= cutoff_us, toUInt8(recovered_us != 0), NULL) AS repl_flag,
            if(burst AND fut_low IS NOT NULL AND low IS NOT NULL AND ts_us + resilience_us <= cutoff_us, toUInt8(fut_low >= low), NULL) AS resil_flag,
            has_base AND net_ask IS NOT NULL AND base.3 IS NOT NULL AND net_ask >= base.3 AND ts_us + refill_us <= cutoff_us AS ask_testable,
            ask_testable AND arrayExists(x -> x.3 >= base.3 AND x.1 <= ts_us + refill_us, fut_books) AS ask_recovered
        FROM
        (
            SELECT
                venue, ts_us, kind, spread, bid, ask, bid_native, ask_native, ask_any, covered, prec, prec_reason, low, net, net_ask,
                argMaxIf((ts_us, bid, ask_any), ts_us, kind = 1) OVER (PARTITION BY venue ORDER BY ts_us ASC, kind DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS base,
                groupArrayIf((ts_us, bid, ask_any), kind = 1) OVER (PARTITION BY venue ORDER BY ts_us ASC RANGE BETWEEN 1 FOLLOWING AND {refill_us:UInt64} FOLLOWING) AS fut_books,
                minIf(low, kind = 2) OVER (PARTITION BY venue ORDER BY ts_us ASC RANGE BETWEEN 1 FOLLOWING AND {resilience_us:UInt64} FOLLOWING) AS fut_low
            FROM
            (
                SELECT venue, ts_us, kind, spread, bid, ask, bid_native, ask_native, ask_any, covered, prec, prec_reason, low, net, net_ask, ok
                FROM
                (
                    SELECT
                        grps[indexOf(iids, instrument_id)] AS venue,
                        toInt64(toUnixTimestamp64Micro(exchange_ts)) AS ts_us,
                        toUInt8(1) AS kind,
                        toFloat64OrNull(spread_bps) AS spread,
                        toFloat64OrNull(replaceAll(if(startsWith(ifNull(range_covered_pct, ''), '{'), JSONExtractRaw(range_covered_pct, 'bid'), ifNull(range_covered_pct, '')), '"', '')) >= {band_pct:Float64} AS bid_covered,
                        toFloat64OrNull(replaceAll(if(startsWith(ifNull(range_covered_pct, ''), '{'), JSONExtractRaw(range_covered_pct, 'ask'), ifNull(range_covered_pct, '')), '"', '')) >= {band_pct:Float64} AS ask_covered,
                        if(bid_covered, toFloat64OrNull(replaceAll(JSONExtractRaw(depth, 'bid', {band_key:String}, 'usd'), '"', '')), nil) AS bid,
                        if(ask_covered, toFloat64OrNull(replaceAll(JSONExtractRaw(depth, 'ask', {band_key:String}, 'usd'), '"', '')), nil) AS ask,
                        if(bid_covered, toFloat64OrNull(replaceAll(JSONExtractRaw(depth, 'bid', {band_key:String}, 'native'), '"', '')), nil) AS bid_native,
                        if(ask_covered, toFloat64OrNull(replaceAll(JSONExtractRaw(depth, 'ask', {band_key:String}, 'native'), '"', '')), nil) AS ask_native,
                        toFloat64OrNull(replaceAll(JSONExtractRaw(depth, 'ask', 'p1', 'usd'), '"', '')) AS ask_any,
                        range_covered_pct AS covered,
                        exchange_ts_prec AS prec,
                        exchange_ts_prec_reason AS prec_reason,
                        nil AS low,
                        nil AS net,
                        nil AS net_ask,
                        (recorded_at IS NULL OR recorded_at <= assembled) AND (received_ts IS NULL OR received_ts <= assembled) AS ok
                    FROM mkt.book_summaries FINAL
                    WHERE instrument_id IN iids
                      AND exchange_ts >= since - toIntervalMicrosecond(interval_us)
                      AND exchange_ts <= cutoff
                      AND integrity_ok = true
                )
                UNION ALL
                SELECT
                    grps[indexOf(iids, instrument_id)] AS venue,
                    toInt64(toUnixTimestamp64Micro(minute_start)) AS ts_us,
                    toUInt8(2) AS kind,
                    nil AS spread, nil AS bid, nil AS ask, nil AS bid_native, nil AS ask_native, nil AS ask_any,
                    nils AS covered, nils AS prec, nils AS prec_reason,
                    toFloat64OrNull(l) AS low,
                    toFloat64OrNull(sell_usd) - toFloat64OrNull(buy_usd) AS net,
                    toFloat64OrNull(buy_usd) - toFloat64OrNull(sell_usd) AS net_ask,
                    (recorded_at IS NULL OR recorded_at <= assembled) AND (received_ts IS NULL OR received_ts <= assembled) AS ok
                FROM mkt.flow_bars_1m FINAL
                WHERE instrument_id IN iids
                  AND minute_start >= first_start
                  AND minute_start <= cutoff - INTERVAL 60 SECOND
                  AND complete = true
            )
            WHERE ok
        )
    )
    ARRAY JOIN arrayEnumerate(names) AS w
    GROUP BY w, venue
)
ORDER BY window, venue
