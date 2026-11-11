-- oi_history: the 90-day normal range of the open-interest change of every window length, for every perp venue
-- of a token and for the synchronised cross-venue composite, and the 90-day normal range of the basis of every
-- venue (03 section 0 normal range, 5.4 open interest and basis).
-- One result row per window and venue (series = the 1-based position in {iids}), plus one row per window for the
-- composite (series 0) that carries the cohort the statement decided for that window. A window is
-- [cutoff - length, cutoff); its history ends where the window starts, not at the event start.
-- Rules:
-- * an observation is a row of mkt.oi_obs recorded and received at or before {assembled} (a missing time
--   counts), one per instrument, metric key and time; one row per 300 s bucket before {recent_start} (the last
--   of the bucket) and every row from {recent_start} on, the same sampling for an assessment and a condition;
--   a row whose bucket edge is declared but unresolved is no observation;
-- * the change of an observation is its value divided by the value of the observation nearest to (time - window
--   length) minus one, native and USD (the stored oi_usd only), within the pairing tolerance of the venue and
--   window = max({sync_tol_s}, half the median positive gap between the venue's observations before the window
--   start), before the window start, the earlier one on a tie;
-- * the composite cohort of a window is decided per window: a candidate venue ({cand}, 1 for a venue the registry
--   allows) is a member when an observation lies within {sync_tol_s} of the window start and of the cutoff;
--   each past time of a candidate venue's observations (a window end) sums, per measure, the members observed at
--   both bounds of its window, a venue observed at the same row at both bounds not counting;
-- * the basis of an observation is (mark / index - 1) * 10000 where both prices exist and the index is not 0.
-- A history range is returned as its samples, the first and the last window end it reaches and its 0.025 and 0.975
-- quantiles; whether it spans the minimum number of days is decided by the caller.
-- Shape: mkt.oi_obs FINAL is read twice: a short scan of the longest window for the cohort flags, and one scan of
-- the look-back collected into one sorted array of (time, native, usd, basis) per venue (about 70,000 elements
-- for four venues) held as a scalar of the statement, which is never referenced from the outer select (that
-- re-reads the table). Everything else is derived from that scalar: nb = every observation with the one that
-- follows it; the venue branch joins every observation and window (ASOF) to nb a window earlier; the composite
-- branch joins every distinct observation time of the candidate venues (ASOF) to nb for every candidate venue,
-- keeps the windows whose cohort has the venue, joins (ASOF) to nb a window earlier and sums the venues
-- observed at both bounds per time.
-- param iids = ['binance:perp_linear:AVAXUSDT','okx:perp_linear:AVAX-USDT-SWAP','bybit:perp_linear:AVAXUSDT','hyperliquid:perp_linear:AVAX']
-- param cand = ['1','1','1','0']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param recent_start = 2026-11-08 12:39:00.000000
-- param names = ['W15M','W1H','W4H','W24H','EVENT']
-- param lens_us = ['900000000','3600000000','14400000000','86400000000','79500000000']
-- param lookback_s = 7776000
-- param sync_tol_s = 60
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {recent_start:DateTime64(6, 'UTC')} AS recent_start,
    {iids:Array(String)} AS iids,
    arrayMap(x -> toUInt8(x), {cand:Array(String)}) AS cand,
    toUInt8(length(iids)) AS nv,
    {names:Array(String)} AS names,
    arrayMap(x -> toInt64(x), {lens_us:Array(String)}) AS lens_us,
    toUnixTimestamp64Micro(cutoff) AS cutoff_us,
    arrayMap(w -> cutoff_us - w, lens_us) AS cuts_us,
    toInt64(300) * 1000000 AS sample_us,
    toInt64({sync_tol_s:UInt32}) * 1000000 AS sync_us,
    toFloat64(sync_us) AS min_tol_us,
    CAST(NULL AS Nullable(Float64)) AS nil,
    (
        SELECT groupArray(m)
        FROM
        (
            SELECT v, arrayMap(w -> toUInt8(cand[v] = 1 AND e > 0 AND s[w] > 0), arrayEnumerate(lens_us)) AS m
            FROM (SELECT toUInt8(arrayJoin(range(1, nv + 1))) AS v) AS a
            LEFT JOIN
            (
                SELECT
                    toUInt8(indexOf(iids, instrument_id)) AS vi,
                    countIf(ok AND ts_us >= cutoff_us - sync_us) AS e,
                    sumForEach(arrayMap(c -> toUInt64(ok AND abs(ts_us - c) <= sync_us), cuts_us)) AS s
                FROM
                (
                    SELECT
                        instrument_id,
                        toUnixTimestamp64Micro(ts) AS ts_us,
                        (recorded_at IS NULL OR recorded_at <= assembled)
                            AND (received_ts IS NULL OR received_ts <= assembled)
                            AND toFloat64OrNull(oi_native) IS NOT NULL
                            AND NOT (ts_bucket_anchor IS NULL AND ifNull(ts_prec, '') LIKE 'BUCKET(%')
                            AND ifNull(ts_bucket_anchor, '') != 'UNSPECIFIED' AS ok
                    FROM mkt.oi_obs FINAL
                    WHERE instrument_id IN iids
                      AND ts >= fromUnixTimestamp64Micro(cutoff_us - arrayMax(lens_us) - sync_us, 'UTC')
                      AND ts <= cutoff
                )
                GROUP BY vi
            ) AS b ON a.v = b.vi
            ORDER BY v
        )
    ) AS coh,
    (
        SELECT groupArray((vi, arrayMap(y -> y.1, s), arrayMap(y -> y.3, s), arrayMap(y -> y.4, s), arrayMap(y -> y.5, s)))
        FROM
        (
            SELECT
                vi,
                arraySort(y -> (y.1, y.2), groupArray((x.1, metric_key, x.2, x.3, x.5))) AS s0,
                arrayFilter((y, nx) -> nx != y.1, s0, arrayPushBack(arrayPopFront(arrayMap(y -> y.1, s0)), toInt64(0))) AS s
            FROM
            (
                SELECT
                    toUInt8(indexOf(iids, instrument_id)) AS vi,
                    metric_key,
                    if(ts < recent_start, intDiv(toUnixTimestamp64Micro(ts), sample_us) * sample_us, toUnixTimestamp64Micro(ts)) AS bucket,
                    argMaxIf(
                        (toUnixTimestamp64Micro(ts), toFloat64OrNull(oi_native), toFloat64OrNull(oi_usd),
                         NOT (ts_bucket_anchor IS NULL AND ifNull(ts_prec, '') LIKE 'BUCKET(%') AND ifNull(ts_bucket_anchor, '') != 'UNSPECIFIED',
                         if(ifNull(toFloat64OrNull(index_price), 0) != 0,
                            (toFloat64OrNull(mark_price) / toFloat64OrNull(index_price) - 1) * 10000, nil)),
                        ts,
                        (recorded_at IS NULL OR recorded_at <= assembled) AND (received_ts IS NULL OR received_ts <= assembled)) AS x,
                    countIf((recorded_at IS NULL OR recorded_at <= assembled) AND (received_ts IS NULL OR received_ts <= assembled)) AS seen
                FROM mkt.oi_obs FINAL
                WHERE instrument_id IN iids
                  AND ts >= cutoff - toIntervalSecond({lookback_s:UInt32})
                  AND ts < fromUnixTimestamp64Micro(cutoff_us - arrayMin(lens_us), 'UTC')
                GROUP BY vi, metric_key, bucket
                HAVING seen > 0 AND x.4
            )
            GROUP BY vi
        )
    ) AS hist,
    (
        SELECT groupArray(tl)
        FROM
        (
            SELECT v, if(empty(b.tl0), arrayMap(c -> min_tol_us, cuts_us), b.tl0) AS tl
            FROM (SELECT toUInt8(arrayJoin(range(1, nv + 1))) AS v) AS a
            LEFT JOIN
            (
                SELECT
                    h.1 AS vi,
                    arrayDifference(h.2) AS gp,
                    arrayMap(c -> arraySort(arrayFilter((g, t) -> g > 0 AND t < c, gp, h.2)), cuts_us) AS sg,
                    arrayMap(g -> greatest(min_tol_us, g[intDiv(length(g), 2) + 1] / 2), sg) AS tl0
                FROM (SELECT arrayJoin(hist) AS h)
            ) AS b ON a.v = b.vi
            ORDER BY v
        )
    ) AS tolm,
    nb AS
    (
        SELECT h.1 AS rv, r_ts, r_n, r_u, x_ts, x_n, x_u
        FROM (SELECT arrayJoin(hist) AS h)
        ARRAY JOIN
            arrayPushFront(h.2, toInt64(0)) AS r_ts,
            arrayPushFront(h.3, nil) AS r_n,
            arrayPushFront(h.4, nil) AS r_u,
            arrayPushBack(h.2, toInt64(0)) AS x_ts,
            arrayPushBack(h.3, nil) AS x_n,
            arrayPushBack(h.4, nil) AS x_u
    ),
    vw AS
    (
        SELECT
            v, w,
            lens_us[w] AS len_us,
            cuts_us[w] AS cut,
            tolm[v][w] AS tol,
            coh[v][w] = 1 AS member
        FROM
        (
            SELECT toUInt8(arrayJoin(range(1, nv + 1))) AS v, toUInt8(arrayJoin(arrayEnumerate(lens_us))) AS w
        )
    )
SELECT
    names[win] AS window,
    series,
    if(series = 0, 'composite', iids[series]) AS instrument_id,
    if(series = 0, arrayStringConcat(arrayMap(v -> iids[v], arrayFilter(v -> coh[v][win] = 1, range(1, nv + 1))), ','), '') AS cohort,
    observations,
    if(observations > 0, first_observation, NULL) AS first_observation,
    if(observations > 0, last_observation, NULL) AS last_observation,
    native_samples,
    if(native_samples > 0, native_range[1], nil) AS native_low,
    if(native_samples > 0, native_range[2], nil) AS native_high,
    if(native_samples > 0, native_first_end, NULL) AS native_first_end,
    if(native_samples > 0, native_last_end, NULL) AS native_last_end,
    usd_samples,
    if(usd_samples > 0, usd_range[1], nil) AS usd_low,
    if(usd_samples > 0, usd_range[2], nil) AS usd_high,
    if(usd_samples > 0, usd_first_end, NULL) AS usd_first_end,
    if(usd_samples > 0, usd_last_end, NULL) AS usd_last_end,
    pair_tolerance_s,
    basis_samples,
    if(basis_samples > 0, basis_range[1], nil) AS basis_low,
    if(basis_samples > 0, basis_range[2], nil) AS basis_high,
    if(basis_samples > 0, basis_first, NULL) AS basis_first,
    if(basis_samples > 0, basis_last, NULL) AS basis_last
FROM
(
    SELECT
        win, pv AS series,
        count() AS observations,
        fromUnixTimestamp64Micro(min(t), 'UTC') AS first_observation,
        fromUnixTimestamp64Micro(max(t), 'UTC') AS last_observation,
        countIf(dn IS NOT NULL) AS native_samples,
        quantilesExactInclusiveIf(0.025, 0.975)(dn, dn IS NOT NULL) AS native_range,
        fromUnixTimestamp64Micro(minIf(t, dn IS NOT NULL), 'UTC') AS native_first_end,
        fromUnixTimestamp64Micro(maxIf(t, dn IS NOT NULL), 'UTC') AS native_last_end,
        countIf(du IS NOT NULL) AS usd_samples,
        quantilesExactInclusiveIf(0.025, 0.975)(du, du IS NOT NULL) AS usd_range,
        fromUnixTimestamp64Micro(minIf(t, du IS NOT NULL), 'UTC') AS usd_first_end,
        fromUnixTimestamp64Micro(maxIf(t, du IS NOT NULL), 'UTC') AS usd_last_end,
        max(tol) / 1000000 AS pair_tolerance_s,
        countIf(b IS NOT NULL) AS basis_samples,
        quantilesExactInclusiveIf(0.025, 0.975)(b, b IS NOT NULL) AS basis_range,
        fromUnixTimestamp64Micro(minIf(t, b IS NOT NULL), 'UTC') AS basis_first,
        fromUnixTimestamp64Micro(maxIf(t, b IS NOT NULL), 'UTC') AS basis_last
    FROM
    (
        SELECT
            t, pv, win, tol, b,
            if(o.r_ts != 0 AND at - o.r_ts <= tol AND (o.x_ts = 0 OR o.x_ts >= cut OR o.x_ts - at > tol OR at - o.r_ts <= o.x_ts - at), 1,
               if(o.x_ts != 0 AND o.x_ts < cut AND o.x_ts - at <= tol, 2, 0)) AS opick,
            multiIf(opick = 1, o.r_n, opick = 2, o.x_n, nil) AS old_n,
            multiIf(opick = 1, o.r_u, opick = 2, o.x_u, nil) AS old_u,
            if(opick != 0 AND new_n IS NOT NULL AND old_n != 0, new_n / old_n - 1, nil) AS dn,
            if(opick != 0 AND new_u IS NOT NULL AND old_u != 0, new_u / old_u - 1, nil) AS du
        FROM
        (
            SELECT
                e.t AS t, e.pv AS pv, e.n AS new_n, e.u AS new_u, e.b AS b,
                g.w AS win, g.tol AS tol, g.cut AS cut, e.t - g.len_us AS at
            FROM
            (
                SELECT h.1 AS pv, t, n, u, b
                FROM (SELECT arrayJoin(hist) AS h)
                ARRAY JOIN h.2 AS t, h.3 AS n, h.4 AS u, h.5 AS b
            ) AS e
            INNER JOIN vw AS g ON e.pv = g.v
            WHERE e.t < g.cut
        ) AS p
        ASOF LEFT JOIN nb AS o ON p.pv = o.rv AND p.at >= o.r_ts
    )
    GROUP BY win, pv
    UNION ALL
    SELECT
        win, toUInt8(0) AS series,
        countIf(in_union) AS observations,
        fromUnixTimestamp64Micro(minIf(t, in_union), 'UTC') AS first_observation,
        fromUnixTimestamp64Micro(maxIf(t, in_union), 'UTC') AS last_observation,
        countIf(dn IS NOT NULL) AS native_samples,
        quantilesExactInclusiveIf(0.025, 0.975)(dn, dn IS NOT NULL) AS native_range,
        fromUnixTimestamp64Micro(minIf(t, dn IS NOT NULL), 'UTC') AS native_first_end,
        fromUnixTimestamp64Micro(maxIf(t, dn IS NOT NULL), 'UTC') AS native_last_end,
        countIf(du IS NOT NULL) AS usd_samples,
        quantilesExactInclusiveIf(0.025, 0.975)(du, du IS NOT NULL) AS usd_range,
        fromUnixTimestamp64Micro(minIf(t, du IS NOT NULL), 'UTC') AS usd_first_end,
        fromUnixTimestamp64Micro(maxIf(t, du IS NOT NULL), 'UTC') AS usd_last_end,
        toFloat64(0) AS pair_tolerance_s,
        toUInt64(0) AS basis_samples,
        CAST([], 'Array(Float64)') AS basis_range,
        CAST(NULL AS Nullable(DateTime64(6, 'UTC'))) AS basis_first,
        CAST(NULL AS Nullable(DateTime64(6, 'UTC'))) AS basis_last
    FROM
    (
        SELECT
            t, win,
            max(own) AS in_union,
            countIf(both AND new_n IS NOT NULL AND old_n != 0) AS cn,
            sumIf(old_n, both AND new_n IS NOT NULL AND old_n != 0) AS n0,
            sumIf(new_n, both AND new_n IS NOT NULL AND old_n != 0) AS n1,
            countIf(both AND new_u IS NOT NULL AND old_u != 0) AS cu,
            sumIf(old_u, both AND new_u IS NOT NULL AND old_u != 0) AS u0,
            sumIf(new_u, both AND new_u IS NOT NULL AND old_u != 0) AS u1,
            if(in_union AND cn > 0, n1 / n0 - 1, nil) AS dn,
            if(in_union AND cu > 0, u1 / u0 - 1, nil) AS du
        FROM
        (
            SELECT
                t, win,
                if(np_ts != 0 AND t - np_ts <= tol AND (nn_ts = 0 OR nn_ts >= cut OR nn_ts - t > tol OR t - np_ts <= nn_ts - t), 1,
                   if(nn_ts != 0 AND nn_ts < cut AND nn_ts - t <= tol, 2, 0)) AS npick,
                multiIf(npick = 1, np_ts, npick = 2, nn_ts, toInt64(0)) AS new_ts,
                multiIf(npick = 1, np_n, npick = 2, nn_n, nil) AS new_n,
                multiIf(npick = 1, np_u, npick = 2, nn_u, nil) AS new_u,
                if(o.r_ts != 0 AND at - o.r_ts <= tol AND (o.x_ts = 0 OR o.x_ts >= cut OR o.x_ts - at > tol OR at - o.r_ts <= o.x_ts - at), 1,
                   if(o.x_ts != 0 AND o.x_ts < cut AND o.x_ts - at <= tol, 2, 0)) AS opick,
                multiIf(opick = 1, o.r_ts, opick = 2, o.x_ts, toInt64(0)) AS old_ts,
                multiIf(opick = 1, o.r_n, opick = 2, o.x_n, nil) AS old_n,
                multiIf(opick = 1, o.r_u, opick = 2, o.x_u, nil) AS old_u,
                new_ts = t AS own,
                old_ts != 0 AND new_ts != 0 AND old_ts != new_ts AS both
            FROM
            (
                SELECT
                    n.t AS t, n.pv AS pv, g.w AS win, g.tol AS tol, g.cut AS cut,
                    n.t - g.len_us AS at,
                    np_ts, np_n, np_u, nn_ts, nn_n, nn_u
                FROM
                (
                    SELECT
                        q.t AS t, q.pv AS pv,
                        f.r_ts AS np_ts, f.r_n AS np_n, f.r_u AS np_u,
                        f.x_ts AS nn_ts, f.x_n AS nn_n, f.x_u AS nn_u
                    FROM
                    (
                        SELECT t, toUInt8(arrayJoin(arrayFilter(v -> cand[v] = 1, range(1, nv + 1)))) AS pv
                        FROM
                        (
                            SELECT DISTINCT t
                            FROM (SELECT arrayJoin(hist) AS h)
                            ARRAY JOIN h.2 AS t
                            WHERE cand[h.1] = 1
                        )
                    ) AS q
                    ASOF LEFT JOIN nb AS f ON q.pv = f.rv AND q.t >= f.r_ts
                ) AS n
                INNER JOIN vw AS g ON n.pv = g.v
                WHERE n.t < g.cut AND g.member
            ) AS p
            ASOF LEFT JOIN nb AS o ON p.pv = o.rv AND p.at >= o.r_ts
        )
        GROUP BY t, win
    )
    GROUP BY win
)
ORDER BY win, series
