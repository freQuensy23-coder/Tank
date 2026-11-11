-- funding_confirmations: the funding estimate of each perp venue that a second, independent path confirmed
-- (03 section 5.5 fragmentation, funding path).
-- Observations are the entries of the source_observations of the CURRENT_ESTIMATE rows of mkt.funding_obs, or the row
-- itself when it has none. A REST observation (capture_source PUBLIC_REST, with a source_path) is confirmed when the
-- WEBSOCKET observation of the same instrument nearest to it in time, the earlier one on a tie, is within 60 s and has
-- another, non-empty source_path. A row counts when it was recorded at or before {assembled} and was observed between
-- {since} and {cutoff}; a REST observation also when it was received at or before {assembled} (a missing time counts).
-- One result row per instrument that has a confirmed observation: the latest of them, its time and its rate x 8 /
-- interval as stored (rate_8h_eq; the latest that has one), and how many were confirmed.
-- Shape: one scan of mkt.funding_obs FINAL; the nearest WEBSOCKET observation before and after each observation come
-- from two running windows in opposite order.
-- param iids = ['binance:perp_linear:AVAXUSDT','okx:perp_linear:AVAX-USDT-SWAP','bybit:perp_linear:AVAXUSDT','hyperliquid:perp_linear:AVAX']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param since = 2026-11-08 08:40:00.000000
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {iids:Array(String)} AS iids,
    toDateTime64(0, 6, 'UTC') AS never
SELECT
    instrument_id,
    argMaxIf(eq, t_us, eq IS NOT NULL) AS rate_8h_eq,
    fromUnixTimestamp64Micro(maxIf(t_us, eq IS NOT NULL), 'UTC') AS as_of,
    count() AS confirmed
FROM
(
    SELECT
        instrument_id, t_us, eq, path,
        if(prev.1 != 0 AND (next.1 = 0 OR t_us - prev.1 <= next.1 - t_us), prev, next) AS near
    FROM
    (
        SELECT
            instrument_id, t_us, eq, path, is_ws,
            anyLastIf((t_us, path), is_ws = 1)
                OVER (PARTITION BY instrument_id ORDER BY t_us ASC, is_ws DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prev,
            anyLastIf((t_us, path), is_ws = 1)
                OVER (PARTITION BY instrument_id ORDER BY t_us DESC, is_ws DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS next
        FROM
        (
            SELECT
                instrument_id,
                toInt64(toUnixTimestamp64Micro(observed)) AS t_us,
                toFloat64OrNull(rate_eq) AS eq,
                ifNull(path_text, '') AS path,
                toUInt8(source = 'WEBSOCKET') AS is_ws
            FROM
            (
                SELECT
                    instrument_id,
                    e,
                    if(e = '', capture_source, JSONExtractString(e, 'capture_source')) AS source,
                    if(e = '', kind, JSONExtractString(e, 'kind')) AS observed_kind,
                    ifNull(if(e = '', as_of, parseDateTime64BestEffortOrNull(JSONExtractString(e, 'as_of'), 6, 'UTC')), never) AS observed,
                    if(e = '', received_ts, parseDateTime64BestEffortOrNull(JSONExtractString(e, 'received_ts'), 6, 'UTC')) AS received,
                    if(e = '', source_path, JSONExtractString(e, 'source_path')) AS path_text,
                    if(e = '', rate_8h_eq, JSONExtractString(e, 'rate_8h_eq')) AS rate_eq
                FROM
                (
                    SELECT
                        instrument_id, capture_source, kind, as_of, received_ts, source_path, rate_8h_eq,
                        arrayJoin(if(empty(entries), [''], entries)) AS e
                    FROM
                    (
                        SELECT
                            instrument_id, capture_source, kind, as_of, received_ts, source_path, rate_8h_eq,
                            arrayMap(x -> x.2, JSONExtractKeysAndValuesRaw(ifNull(source_observations, ''))) AS entries
                        FROM mkt.funding_obs FINAL
                        WHERE instrument_id IN iids
                          AND kind = 'CURRENT_ESTIMATE'
                          AND as_of >= {since:DateTime64(6, 'UTC')}
                          AND as_of <= cutoff
                          AND (recorded_at IS NULL OR recorded_at <= assembled)
                    )
                )
            )
            WHERE observed_kind = 'CURRENT_ESTIMATE'
              AND observed > never AND observed <= cutoff
              AND (source = 'WEBSOCKET'
                   OR (source = 'PUBLIC_REST' AND path_text != '' AND (received IS NULL OR received <= assembled)))
        )
    )
    WHERE is_ws = 0
)
WHERE near.1 != 0 AND abs(near.1 - t_us) <= 60000000 AND near.2 != '' AND path != '' AND near.2 != path
GROUP BY instrument_id
ORDER BY instrument_id
