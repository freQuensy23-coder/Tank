-- lead_lag: which market first leaves its normal range (03 section 5.4 lead-lag), one row per instrument.
-- The one-minute klines of an instrument in [{start}, {cutoff}) that closed at or before the cutoff and were recorded
-- and received at or before {assembled} (a missing time counts). `usable` is 1 when every one of them is complete and
-- each is exactly 60 s after the one before it; `first_exit_at` is the open time of the first bar whose close, as a
-- return from the open of the first bar, is below {low} or above {high} (the 90-day normal range of the 15-minute
-- return), NULL when there is none or the first open is 0 or missing. `bars` counts the klines read.
-- Shape: one scan of mkt.klines_1m FINAL over the window, ordered by open time with one window function.
-- param iids = ['binance:spot:AVAXUSDT','bybit:spot:AVAXUSDT','coinbase:spot:AVAX-USD','kraken:spot:AVAX/USD','okx:spot:AVAX-USDT','binance:perp_linear:AVAXUSDT','okx:perp_linear:AVAX-USDT-SWAP','bybit:perp_linear:AVAXUSDT','hyperliquid:perp_linear:AVAX']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param start = 2026-11-09 14:35:00.000000
-- param low = -0.004690133348346806
-- param high = 0.00649031917586111
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {iids:Array(String)} AS iids,
    {low:Float64} AS low,
    {high:Float64} AS high
SELECT
    instrument_id,
    count() AS bars,
    toUInt8(countIf(NOT ifNull(complete, false)) = 0 AND countIf(gap_us != 60000000) = 0) AS usable,
    minIf(open_time, ifNull(p0 != 0 AND (c / p0 - 1 < low OR c / p0 - 1 > high), false)) AS first_exit_at
FROM
(
    SELECT
        instrument_id,
        open_time,
        complete,
        toFloat64OrNull(c) AS c,
        first_value(toFloat64OrNull(o)) OVER (PARTITION BY instrument_id ORDER BY open_time) AS p0,
        toUnixTimestamp64Micro(open_time)
            - lagInFrame(toUnixTimestamp64Micro(open_time), 1, toUnixTimestamp64Micro(open_time) - 60000000)
                OVER (PARTITION BY instrument_id ORDER BY open_time) AS gap_us
    FROM mkt.klines_1m FINAL
    WHERE instrument_id IN iids
      AND open_time >= {start:DateTime64(6, 'UTC')}
      AND open_time < cutoff
      AND open_time + toIntervalSecond(ifNull(interval_s, 60)) <= cutoff
      AND (recorded_at IS NULL OR recorded_at <= assembled)
      AND (received_ts IS NULL OR received_ts <= assembled)
)
GROUP BY instrument_id
ORDER BY instrument_id
