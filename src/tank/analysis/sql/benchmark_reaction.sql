-- benchmark_reaction: the reaction checkpoint of a benchmark outside the analysis set (03 section 8.1).
-- The raw return between the last complete spot kline close at or before the window start and at or
-- before the window end, on the spot instrument with the largest quote volume in the 24 hours before
-- the window end. A close older than its bar interval at a bound gives no price. One scan of
-- mkt.klines_1m FINAL over [least(start, end - 24 h) - 1 h, end]; one row out.
-- Stored rows are read as they are now: the scheduler never bounded this read by an as-of time.
-- param instrument_ids = ['binance:spot:BTCUSDT','bybit:spot:BTCUSDT','coinbase:spot:BTC-USD','okx:spot:BTC-USDT']
-- param window_start = 2026-11-09 15:55:15.182000
-- param window_end = 2026-11-10 15:55:15.182000
WITH
    {window_start:DateTime64(6, 'UTC')} AS w_start,
    {window_end:DateTime64(6, 'UTC')} AS w_end,
    {instrument_ids:Array(String)} AS iids,
    least(w_start, w_end - INTERVAL 24 HOUR) - INTERVAL 1 HOUR AS lower
SELECT
    nullIf(best.1, '') AS venue,
    best.2 AS quote_volume_24h,
    best.3 AS price_start,
    best.4 AS price_start_observed_at,
    best.5 AS price_end,
    best.6 AS price_end_observed_at,
    toFloat64OrNull(best.5) / toFloat64OrNull(best.3) - 1 AS r_raw,
    venues
FROM
(
    SELECT
        argMax((instrument_id, vol, p0, t0, p1, t1), (vol, instrument_id)) AS best,
        count() AS venues
    FROM
    (
        SELECT
            instrument_id,
            sumKahanIf(ifNull(toFloat64OrNull(quote_volume_usd), 0), close_time > w_end - INTERVAL 24 HOUR AND close_time <= w_end) AS vol,
            argMaxIf((c, close_time, step), close_time, close_time <= w_start) AS a0,
            argMaxIf((c, close_time, step), close_time, close_time <= w_end) AS a1,
            if(a0.2 + toIntervalSecond(a0.3) >= w_start AND a0.3 > 0, a0.1, NULL) AS p0,
            if(p0 IS NULL, NULL, a0.2) AS t0,
            if(a1.2 + toIntervalSecond(a1.3) >= w_end AND a1.3 > 0, a1.1, NULL) AS p1,
            if(p1 IS NULL, NULL, a1.2) AS t1
        FROM
        (
            SELECT
                instrument_id,
                c,
                ifNull(interval_s, 60) AS step,
                open_time + toIntervalSecond(step) AS close_time,
                quote_volume_usd
            FROM mkt.klines_1m FINAL
            WHERE instrument_id IN iids
              AND open_time >= lower
              AND open_time <= w_end
              AND complete = true
              AND toFloat64OrNull(c) != 0
        )
        GROUP BY instrument_id
        HAVING vol > 0
    )
)
