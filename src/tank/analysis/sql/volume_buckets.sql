-- volume_buckets: the 15-minute volume buckets of the 24 hours before {cutoff} that scale the impact tiers
-- (03 section 5.7, tier scaling), per bucket and venue.
-- A bucket is a quarter of an hour on the clock (minute_start divided by 900 s); only the flow bars of the
-- spot instruments {instrument_ids} that start inside [cutoff - 1 day, cutoff) count, so a bucket cut by
-- either end has fewer than 15 minutes. Per bucket and venue (the venue of the row, or the part of the
-- instrument id before the first colon): the number of distinct minutes, the bars that are not complete, the
-- bars without a buy or a sell notional, and the sum of buy_usd + sell_usd. Which venues must be covered, and
-- which buckets therefore count, is decided by the caller per window.
-- A bar counts when it was recorded or received at or before {assembled} (a missing time counts).
-- param instrument_ids = ['binance:spot:AVAXUSDT','bybit:spot:AVAXUSDT','coinbase:spot:AVAX-USD','kraken:spot:AVAX/USD','okx:spot:AVAX-USDT']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled
SELECT
    toDateTime64(bucket * 900, 6, 'UTC') AS bucket_start,
    venue,
    uniqExact(minute_start) AS minutes,
    countIf(NOT ifNull(complete, false)) AS incomplete,
    countIf(bu IS NULL OR su IS NULL) AS missing,
    sumKahan(ifNull(bu, 0) + ifNull(su, 0)) AS notional
FROM
(
    SELECT
        minute_start, complete,
        intDiv(toUnixTimestamp(minute_start), 900) AS bucket,
        ifNull(nullIf(venue, ''), splitByChar(':', instrument_id)[1]) AS venue,
        toFloat64OrNull(buy_usd) AS bu, toFloat64OrNull(sell_usd) AS su
    FROM mkt.flow_bars_1m FINAL
    WHERE instrument_id IN {instrument_ids:Array(String)}
      AND minute_start >= cutoff - INTERVAL 1 DAY AND minute_start < cutoff
      AND (recorded_at IS NULL OR recorded_at <= assembled)
      AND (received_ts IS NULL OR received_ts <= assembled)
)
GROUP BY bucket, venue
ORDER BY bucket, venue
