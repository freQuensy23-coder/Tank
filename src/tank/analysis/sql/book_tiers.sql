-- book_tiers: the slippage of the impact tiers on the latest book of every spot venue (03 section 5.5 slippage):
-- per venue, and for a depth-proportional split of the tier across the venues, per window.
-- A venue is the group {groups}[i] of {instrument_ids}[i]; its latest book is the latest spot book with integrity_ok
-- at or before {cutoff} and within the hour before it that was recorded and received at or before {assembled} (a missing time
-- counts), and its ask levels are the stored `asks` (pairs [price, quantity] or objects {price, qty}, best first).
-- {tiers} is the tier notional in USD of each window, three values (T1, T2, T3) for every window of {names}, an empty
-- text where a window has none; the caller scales them from the volume of the window (volume_buckets).
-- The walk takes a tier notional (in the quote currency: USD for a USD quote, else divided by quote_usd_rate; no rate
-- is no slippage) from the best level up: slippage_bps = (spent / filled quantity / mid - 1) * 10000, NULL when the
-- levels do not fill the notional (all but 1e-8 of it), when there are no levels or the mid is 0 or missing.
-- The split gives venue v the part ask_p1(v) / sum of ask_p1 of the tier (ask_p1 is the USD depth of the ask side
-- within 1% as stored, with any coverage); `split_tN` is the slippage of that part on that venue, NULL when it has no
-- part to walk. One result row per window and venue with a latest book.
-- No order-book levels are stored in production (tank-capture drops them), so there the slippage is always NULL.
-- param instrument_ids = ['binance:spot:AVAXUSDT','bybit:spot:AVAXUSDT','coinbase:spot:AVAX-USD','kraken:spot:AVAX/USD','okx:spot:AVAX-USDT']
-- param groups = ['binance','bybit','coinbase','kraken','okx']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param names = ['W15M','W1H','W4H','W24H','EVENT']
-- param tiers = ['19000','190000','950000','19000','190000','950000','19000','190000','950000','19000','190000','950000','19000','190000','950000']
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {instrument_ids:Array(String)} AS iids,
    {groups:Array(String)} AS grps,
    {names:Array(String)} AS names,
    {tiers:Array(String)} AS tiers
SELECT
    names[w] AS window,
    venue,
    fromUnixTimestamp64Micro(ts_us, 'UTC') AS observed_at,
    ask_any AS ask_p1,
    toUInt64(length(pr)) AS levels,
    slip[1] AS slippage_t1,
    slip[2] AS slippage_t2,
    slip[3] AS slippage_t3,
    slip[4] AS split_t1,
    slip[5] AS split_t2,
    slip[6] AS split_t3
FROM
(
    SELECT
        w, venue, ts_us, ask_any, pr, qt, mid,
        arrayCumSum(arrayMap((p, q) -> p * q, pr, qt)) AS cum,
        arrayMap(j -> toFloat64OrNull(tiers[(w - 1) * 3 + j]), [1, 2, 3]) AS tier_usd,
        if(quote = 'USD', 1., rate) AS fx,
        sum(ask_any) OVER (PARTITION BY w) AS total_ask,
        arrayConcat(
            arrayMap(n -> if(n IS NULL OR n <= 0 OR fx IS NULL OR fx <= 0, NULL, n / fx), tier_usd),
            arrayMap(n -> if(n IS NULL OR n <= 0 OR fx IS NULL OR fx <= 0 OR ask_any IS NULL OR total_ask <= 0, NULL, n * ask_any / total_ask / fx), tier_usd)
        ) AS notional,
        arrayMap(n -> if(n IS NULL, 0, arrayFirstIndex(c -> c >= n - 0.00000001, cum)), notional) AS idx,
        arrayMap(i -> if(i <= 1, 0., cum[i - 1]), idx) AS prior,
        arrayMap((n, i, p0) -> if(i = 0, 0., least(n - p0, pr[i] * qt[i])), notional, idx, prior) AS take,
        arrayMap((i, t) -> if(i = 0, 0., arraySum(arraySlice(qt, 1, i - 1)) + t / pr[i]), idx, take) AS filled,
        arrayMap((n, i, p0, t, f) -> if(n IS NULL OR i = 0 OR f <= 0 OR mid IS NULL OR mid = 0, NULL, ((p0 + t) / f / mid - 1) * 10000), notional, idx, prior, take, filled) AS slip
    FROM
    (
        SELECT
            venue,
            latest.1 AS ts_us,
            latest.2 AS mid,
            latest.3 AS quote,
            latest.4 AS rate,
            latest.5 AS ask_any,
            arrayMap(x -> ifNull(toFloat64OrNull(replaceAll(if(startsWith(x, '['), JSONExtractArrayRaw(x)[1], JSONExtractRaw(x, 'price')), '"', '')), 0.), levels) AS pr,
            arrayMap(x -> ifNull(toFloat64OrNull(replaceAll(if(startsWith(x, '['), JSONExtractArrayRaw(x)[2], JSONExtractRaw(x, 'qty')), '"', '')), 0.), levels) AS qt,
            JSONExtractArrayRaw(latest.6) AS levels,
            w
        FROM
        (
            SELECT
                grps[indexOf(iids, instrument_id)] AS venue,
                argMaxIf(
                    (
                        toInt64(toUnixTimestamp64Micro(exchange_ts)),
                        toFloat64OrNull(mid),
                        ifNull(quote_asset, ''),
                        toFloat64OrNull(quote_usd_rate),
                        toFloat64OrNull(replaceAll(JSONExtractRaw(depth, 'ask', 'p1', 'usd'), '"', '')),
                        ifNull(asks, '')
                    ),
                    exchange_ts,
                    (recorded_at IS NULL OR recorded_at <= assembled) AND (received_ts IS NULL OR received_ts <= assembled)
                ) AS latest
            FROM mkt.book_summaries FINAL
            WHERE instrument_id IN iids
              AND exchange_ts >= cutoff - INTERVAL 1 HOUR
              AND exchange_ts <= cutoff
              AND integrity_ok = true
            GROUP BY venue
            HAVING countIf((recorded_at IS NULL OR recorded_at <= assembled) AND (received_ts IS NULL OR received_ts <= assembled)) > 0
        )
        ARRAY JOIN arrayEnumerate(names) AS w
    )
)
ORDER BY w, venue
