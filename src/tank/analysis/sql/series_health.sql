-- series_health: what the data-quality rules read from the market series of a token (03 section 5.10): per series,
-- instrument and key the latest row, the latest valid row and the counts that say whether the series is fresh.
-- One result row per series and key. A row of a series counts when it is at or before the cutoff and was recorded and
-- received at or before {assembled} (a missing time counts); a one-minute bar (series klines and flow_bars) counts when
-- it also closed at or before the cutoff. The key is the instrument and, where the table has the column, `metric_key`
-- (books, oi, funding, liquidations) and `kind` (funding, tickers); an absent part is NULL. The rows of klines and
-- flow_bars are read over [cutoff - {look_s} s, cutoff], those of books and tickers over [cutoff - {near_s} s, cutoff]
-- (the newest books and the ticker next to the cutoff are all the rules read of them), the other series over
-- {look_s} seconds.
-- Columns: `rows` the rows that count; `last_at` the time of the latest one (of klines and flow_bars the open time of the
-- latest complete bar, NULL without one), `last_interval_s` the bar length, `last_prec`, `last_prec_reason`,
-- `last_timestamp_precision`, `last_bucket_size_s` and `last_bucket_anchor` the precision of that time as stored,
-- `last_interval_h` the funding interval of that row; `valid_at` the time (of a bar, its close) of the latest row that
-- is fit to be the series' freshness evidence: a complete bar, for klines with a non-zero close; a book with
-- integrity_ok, a non-zero mid and both sides' range_covered_pct; an open-interest row with a native value; a funding
-- row with a rate and a non-zero interval; NULL for liquidations and tickers. `usd_volume` is the USD volume of the
-- klines that closed in the 24 hours before the cutoff (the rows that have one), NULL for the other series.
-- Shape: one scan of each of the seven mkt tables FINAL, one aggregation per key.
-- param iids = ['binance:spot:AVAXUSDT','bybit:spot:AVAXUSDT','coinbase:spot:AVAX-USD','kraken:spot:AVAX/USD','okx:spot:AVAX-USDT','binance:perp_linear:AVAXUSDT','okx:perp_linear:AVAX-USDT-SWAP','bybit:perp_linear:AVAXUSDT','hyperliquid:perp_linear:AVAX']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param look_s = 172800
-- param near_s = 900
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {iids:Array(String)} AS iids,
    cutoff - toIntervalSecond(greatest({look_s:UInt32}, 86400)) AS low,
    cutoff - toIntervalSecond({look_s:UInt32}) AS look_low,
    cutoff - toIntervalSecond({near_s:UInt32}) AS near,
    cutoff - toIntervalSecond(86400) AS day,
    (recorded_at IS NULL OR recorded_at <= assembled) AND (received_ts IS NULL OR received_ts <= assembled) AS ok,
    CAST(NULL AS Nullable(String)) AS nils,
    CAST(NULL AS Nullable(Float64)) AS nilf,
    CAST(NULL AS Nullable(DateTime64(6, 'UTC'))) AS nilt
SELECT
    'klines' AS series,
    instrument_id,
    nils AS metric_key,
    nils AS kind,
    countIf(ok AND open_time + toIntervalSecond(ifNull(interval_s, 60)) <= cutoff) AS rows,
    nullIf(maxIf(open_time, ok AND complete = true AND open_time + toIntervalSecond(ifNull(interval_s, 60)) <= cutoff), toDateTime64(0, 6, 'UTC')) AS last_at,
    argMaxIf(ifNull(interval_s, 60), open_time, ok AND complete = true AND open_time + toIntervalSecond(ifNull(interval_s, 60)) <= cutoff) AS last_interval_s,
    argMaxIf(open_time_prec, open_time, ok AND complete = true AND open_time + toIntervalSecond(ifNull(interval_s, 60)) <= cutoff) AS last_prec,
    argMaxIf(open_time_prec_reason, open_time, ok AND complete = true AND open_time + toIntervalSecond(ifNull(interval_s, 60)) <= cutoff) AS last_prec_reason,
    nils AS last_timestamp_precision,
    nilf AS last_bucket_size_s,
    nils AS last_bucket_anchor,
    nilf AS last_interval_h,
    nullIf(maxIf(open_time + toIntervalSecond(ifNull(interval_s, 60)), ok AND complete = true AND ifNull(toFloat64OrNull(c), 0) != 0 AND open_time + toIntervalSecond(ifNull(interval_s, 60)) <= cutoff), toDateTime64(0, 6, 'UTC')) AS valid_at,
    sumKahanIf(toFloat64OrNull(quote_volume_usd), ok AND quote_volume_usd IS NOT NULL AND open_time >= day AND open_time + toIntervalSecond(ifNull(interval_s, 60)) <= cutoff) AS usd_volume
FROM mkt.klines_1m FINAL
WHERE instrument_id IN iids AND open_time >= low AND open_time <= cutoff
GROUP BY instrument_id
UNION ALL
SELECT
    'flow_bars',
    instrument_id,
    nils,
    nils,
    countIf(ok AND minute_start + toIntervalSecond(60) <= cutoff),
    nullIf(maxIf(minute_start, ok AND complete = true AND minute_start + toIntervalSecond(60) <= cutoff), toDateTime64(0, 6, 'UTC')),
    toInt64(60),
    argMaxIf(minute_start_prec, minute_start, ok AND complete = true AND minute_start + toIntervalSecond(60) <= cutoff),
    argMaxIf(minute_start_prec_reason, minute_start, ok AND complete = true AND minute_start + toIntervalSecond(60) <= cutoff),
    nils,
    nilf,
    nils,
    nilf,
    nullIf(maxIf(minute_start + toIntervalSecond(60), ok AND complete = true AND minute_start + toIntervalSecond(60) <= cutoff), toDateTime64(0, 6, 'UTC')),
    nilf
FROM mkt.flow_bars_1m FINAL
WHERE instrument_id IN iids AND minute_start >= look_low AND minute_start <= cutoff
GROUP BY instrument_id
UNION ALL
SELECT
    'books',
    instrument_id,
    metric_key,
    nils,
    countIf(ok),
    nullIf(maxIf(exchange_ts, ok), toDateTime64(0, 6, 'UTC')),
    toInt64(0),
    argMaxIf(exchange_ts_prec, exchange_ts, ok),
    argMaxIf(exchange_ts_prec_reason, exchange_ts, ok),
    nils,
    nilf,
    nils,
    nilf,
    nullIf(maxIf(exchange_ts, ok AND integrity_ok = true AND ifNull(toFloat64OrNull(mid), 0) != 0
        AND startsWith(ifNull(range_covered_pct, ''), '{')
        AND toFloat64OrNull(replaceAll(JSONExtractRaw(range_covered_pct, 'bid'), '"', '')) IS NOT NULL
        AND toFloat64OrNull(replaceAll(JSONExtractRaw(range_covered_pct, 'ask'), '"', '')) IS NOT NULL), toDateTime64(0, 6, 'UTC')),
    nilf
FROM mkt.book_summaries FINAL
WHERE instrument_id IN iids AND exchange_ts >= near AND exchange_ts <= cutoff
GROUP BY instrument_id, metric_key
HAVING countIf(ok) > 0
UNION ALL
SELECT
    'oi',
    instrument_id,
    metric_key,
    nils,
    countIf(ok),
    nullIf(maxIf(ts, ok), toDateTime64(0, 6, 'UTC')),
    toInt64(0),
    argMaxIf(ts_prec, ts, ok),
    argMaxIf(ts_prec_reason, ts, ok),
    argMaxIf(timestamp_precision, ts, ok),
    argMaxIf(toFloat64OrNull(ts_bucket_size_s), ts, ok),
    argMaxIf(ts_bucket_anchor, ts, ok),
    nilf,
    nullIf(maxIf(ts, ok AND oi_native IS NOT NULL), toDateTime64(0, 6, 'UTC')),
    nilf
FROM mkt.oi_obs FINAL
WHERE instrument_id IN iids AND ts >= look_low AND ts <= cutoff
GROUP BY instrument_id, metric_key
HAVING countIf(ok) > 0
UNION ALL
SELECT
    'funding',
    instrument_id,
    metric_key,
    kind,
    countIf(ok),
    nullIf(maxIf(as_of, ok), toDateTime64(0, 6, 'UTC')),
    toInt64(0),
    argMaxIf(as_of_prec, as_of, ok),
    argMaxIf(as_of_prec_reason, as_of, ok),
    nils,
    nilf,
    nils,
    argMaxIf(toFloat64OrNull(interval_h), as_of, ok),
    nullIf(maxIf(as_of, ok AND rate IS NOT NULL AND ifNull(toFloat64OrNull(interval_h), 0) != 0), toDateTime64(0, 6, 'UTC')),
    nilf
FROM mkt.funding_obs FINAL
WHERE instrument_id IN iids AND as_of >= look_low AND as_of <= cutoff
GROUP BY instrument_id, metric_key, kind
HAVING countIf(ok) > 0
UNION ALL
SELECT
    'liquidations',
    instrument_id,
    metric_key,
    nils,
    countIf(ok),
    nullIf(maxIf(ts, ok), toDateTime64(0, 6, 'UTC')),
    toInt64(0),
    argMaxIf(ts_prec, ts, ok),
    argMaxIf(ts_prec_reason, ts, ok),
    nils,
    nilf,
    nils,
    nilf,
    nilt,
    nilf
FROM mkt.liq_obs FINAL
WHERE instrument_id IN iids AND ts >= look_low AND ts <= cutoff
GROUP BY instrument_id, metric_key
HAVING countIf(ok) > 0
UNION ALL
SELECT
    'tickers',
    instrument_id,
    nils,
    kind,
    countIf(ok),
    nullIf(maxIf(ts, ok), toDateTime64(0, 6, 'UTC')),
    toInt64(0),
    argMaxIf(ts_prec, ts, ok),
    argMaxIf(ts_prec_reason, ts, ok),
    nils,
    nilf,
    nils,
    nilf,
    nilt,
    nilf
FROM mkt.tickers_24h FINAL
WHERE instrument_id IN iids AND ts >= near AND ts <= cutoff
GROUP BY instrument_id, kind
HAVING countIf(ok) > 0
