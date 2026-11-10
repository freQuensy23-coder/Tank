-- sector_returns: the return of every peer instrument over every horizon (03 section 5.7, sector-relative
-- return). A horizon is [start, end]; a peer's price at a bound is the close of its last complete
-- one-minute bar (positive price, recorded at or before {assembled}) that closed at or before the bound,
-- and counts only when that close is not older than the spacing between it and the close before it (at
-- least one minute); the return is price(end) / price(start) - 1, NULL when either price does not count.
-- One row per peer instrument that has any close in the look-back and per horizon; an instrument without
-- a close has no rows. One scan of mkt.klines_1m FINAL over the look-back, the closes of an instrument
-- collected in time order into two arrays (the recorded-at rule is a column of the scan, not a filter, so
-- that the read stays on the sorting key).
-- param peer_ids = ['binance:spot:SOLUSDT','binance:spot:NEARUSDT','binance:spot:SUIUSDT','binance:spot:APTUSDT']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param lookback_s = 7776000
-- param names = ['W15M','W1H','W4H','W24H','EVENT']
-- param starts = ['2026-11-10 12:25:00','2026-11-10 11:40:00','2026-11-10 08:40:00','2026-11-09 12:40:00','2026-11-09 14:35:00']
-- param ends = ['2026-11-10 12:40:00','2026-11-10 12:40:00','2026-11-10 12:40:00','2026-11-10 12:40:00','2026-11-10 12:40:00']
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {peer_ids:Array(String)} AS iids,
    arrayMap(x -> toUnixTimestamp64Micro(toDateTime64(x, 6, 'UTC')), {starts:Array(String)}) AS a_us,
    arrayMap(x -> toUnixTimestamp64Micro(toDateTime64(x, 6, 'UTC')), {ends:Array(String)}) AS z_us
SELECT instrument_id, name, peer_return
FROM
(
    SELECT
        instrument_id,
        name,
        arrayMap(t -> t.2, arraySort(t -> t.1, grouped)) AS prices,
        arraySort(arrayMap(t -> t.1, grouped)) AS times,
        arrayLastIndex(t -> t <= a_bound, times) AS ia,
        arrayLastIndex(t -> t <= z_bound, times) AS iz,
        if(ia > 0 AND iz > 0
            AND a_bound - times[ia] <= greatest(if(ia > 1, times[ia] - times[ia - 1], 60000000), 60000000)
            AND z_bound - times[iz] <= greatest(if(iz > 1, times[iz] - times[iz - 1], 60000000), 60000000),
            prices[iz] / prices[ia] - 1, NULL) AS peer_return
    FROM
    (
        SELECT
            instrument_id,
            groupArrayIf((toUnixTimestamp64Micro(open_time) + 60000000, price), ok) AS grouped
        FROM
        (
            SELECT
                instrument_id, open_time, toFloat64OrNull(c) AS price,
                (recorded_at IS NULL OR recorded_at <= assembled) AND complete = true AND interval_s = 60 AND price > 0 AS ok
            FROM mkt.klines_1m FINAL
            WHERE instrument_id IN iids
              AND open_time >= toDateTime64(cutoff - toIntervalSecond({lookback_s:UInt32}), 6, 'UTC')
              AND open_time <= cutoff - INTERVAL 60 SECOND
        )
        GROUP BY instrument_id
        HAVING countIf(ok) > 0
    )
    ARRAY JOIN {names:Array(String)} AS name, a_us AS a_bound, z_us AS z_bound
)
ORDER BY instrument_id, name
