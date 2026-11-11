-- ticker_volumes: the native 24-hour volume of the latest ticker of every instrument (03 section 5.3, the volume a spot
-- venue excluded from the flow would have traded, estimated from its ticker).
-- One result row per instrument that has a ticker in [cutoff - {near_s} s, cutoff]: the time of the latest ticker at or
-- before the cutoff recorded and received at or before {assembled} (a missing time counts) and its native volume as
-- the stored decimal text, NULL when that ticker has none. The caller picks the rows of the venue it needs.
-- Shape: one scan of mkt.tickers_24h FINAL over {near_s} seconds.
-- param iids = ['binance:spot:AVAXUSDT','bybit:spot:AVAXUSDT','coinbase:spot:AVAX-USD','kraken:spot:AVAX/USD','okx:spot:AVAX-USDT','binance:perp_linear:AVAXUSDT','okx:perp_linear:AVAX-USDT-SWAP','bybit:perp_linear:AVAXUSDT','hyperliquid:perp_linear:AVAX']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param near_s = 900
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {iids:Array(String)} AS iids
SELECT
    instrument_id,
    latest.1 AS observed_at,
    latest.2 AS volume_native
FROM
(
    SELECT
        instrument_id,
        argMax((ts, volume_native), ts) AS latest
    FROM mkt.tickers_24h FINAL
    WHERE instrument_id IN iids
      AND ts >= cutoff - toIntervalSecond({near_s:UInt32})
      AND ts <= cutoff
      AND (recorded_at IS NULL OR recorded_at <= assembled)
      AND (received_ts IS NULL OR received_ts <= assembled)
    GROUP BY instrument_id
)
ORDER BY instrument_id
