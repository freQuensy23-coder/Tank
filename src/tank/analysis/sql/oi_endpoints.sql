-- oi_endpoints: the open-interest endpoints of the current window of every window length, per perp venue and for the
-- synchronised composite, with the basis at the window start and at the cutoff (03 section 5.4 open interest and
-- basis, 2.5 temporal admissibility).
-- One result row per window and venue (series = the 1-based position in {iids}; `start_*` precision columns are those of
-- the start observation as stored), plus one row per window for the
-- composite (series 0, the cohort in `cohort`). A window is [cutoff - length, cutoff].
-- Rules:
-- * an observation is a row of mkt.oi_obs recorded and received at or before {assembled} (a missing time counts)
--   with a native value; one whose bucket edge is declared but unresolved is no endpoint (admissible = 0) and is
--   counted in `excluded` instead, with the latest of them;
-- * the endpoint of a venue at a bound is the admissible observation nearest to it within {sync_tol_s} and not after
--   the cutoff, the earlier one on a tie; its USD value is the stored oi_usd, else the native value times the
--   observation's own mark, else the native value times the mark of the instrument's mkt.tickers_24h row nearest to
--   it within {sync_tol_s};
-- * the composite cohort of a window is the candidate venues ({cand}, 1 for a venue the registry allows) with an
--   endpoint at both bounds (the rule of oi_history.sql);
-- * the composite exists at a time T (a time of an observation of a cohort venue) when every cohort venue has an
--   observation within {sync_tol_s} of T with a native value; its value is the sum of the observations nearest to
--   T, its USD value exists only when every one has one; the composite endpoint at a bound is the composite at the
--   T nearest to it within {sync_tol_s}, the earlier one on a tie;
-- * the basis of an observation is (mark / index - 1) * 10000 where both prices exist and the index is not 0;
--   basis_start_bps is that of the admissible observation with a basis nearest to the window start within
--   {sync_tol_s}, basis_bps that of the latest admissible observation not older than the longer of three
--   tolerances and the shortest window (NULL when it has none).
-- Shape: one scan of mkt.oi_obs FINAL and one of mkt.tickers_24h FINAL over the longest window plus three tolerances,
-- the rows near each bound collected per window into small arrays (a few dozen elements), and the rules above
-- run on those arrays, one rule per subquery level (an alias that holds a lambda is analysed again at every
-- reference: the same rules written as aliases of one SELECT took 3,652 ms, almost all of it query analysis).
-- param iids = ['binance:perp_linear:AVAXUSDT','okx:perp_linear:AVAX-USDT-SWAP','bybit:perp_linear:AVAXUSDT','hyperliquid:perp_linear:AVAX']
-- param cand = ['1','1','1','0']
-- param cutoff = 2026-11-10 12:40:00.000000
-- param assembled = 2026-11-10 12:46:52.362322
-- param names = ['W15M','W1H','W4H','W24H','EVENT']
-- param lens_us = ['900000000','3600000000','14400000000','86400000000','79500000000']
-- param sync_tol_s = 60
WITH
    {cutoff:DateTime64(6, 'UTC')} AS cutoff,
    {assembled:DateTime64(6, 'UTC')} AS assembled,
    {iids:Array(String)} AS iids,
    arrayMap(x -> toUInt8(x), {cand:Array(String)}) AS cand,
    toUInt8(length(iids)) AS nv,
    {names:Array(String)} AS names,
    arrayMap(x -> toInt64(x), {lens_us:Array(String)}) AS lens_us,
    toUnixTimestamp64Micro(cutoff) AS cutoff_us,
    arrayMap(l -> cutoff_us - l, lens_us) AS cuts_us,
    toInt64({sync_tol_s:UInt32}) * 1000000 AS sync_us,
    greatest(3 * sync_us, arrayMin(lens_us)) AS wide_us,
    fromUnixTimestamp64Micro(cutoff_us - arrayMax(lens_us) - 3 * sync_us, 'UTC') AS scan_from,
    CAST(NULL AS Nullable(Float64)) AS nil,
    CAST(NULL AS Nullable(String)) AS nils,
    CAST(NULL AS Nullable(Int64)) AS nili
SELECT
    names[w] AS window,
    r.1 AS series,
    r.2 AS instrument_id,
    r.3 AS cohort,
    if(r.4 = 0, NULL, fromUnixTimestamp64Micro(r.4, 'UTC')) AS start_observed_at,
    r.5 AS oi_native_start,
    r.6 AS oi_usd_start,
    if(r.7 = 0, NULL, fromUnixTimestamp64Micro(r.7, 'UTC')) AS end_observed_at,
    r.8 AS oi_native_end,
    r.9 AS oi_usd_end,
    r.10 AS end_mark,
    r.11 AS end_ts_prec,
    r.12 AS end_timestamp_precision,
    r.13 AS end_ts_bucket_size_s,
    r.14 AS basis_bps,
    r.15 AS basis_start_bps,
    r.16 AS excluded,
    if(r.17 = 0, NULL, fromUnixTimestamp64Micro(r.17, 'UTC')) AS excluded_at,
    r.18 AS excluded_oi_native,
    r.19 AS excluded_oi_usd,
    r.20 AS excluded_ts_prec,
    r.21 AS excluded_timestamp_precision,
    r.22 AS excluded_ts_bucket_size_s,
    r.23 AS excluded_ts_bucket_anchor,
    r.24 AS start_ts_prec,
    r.25 AS start_timestamp_precision,
    r.26 AS start_ts_bucket_size_s
FROM
(
    SELECT
        w,
        arrayConcat(
            [(
                toUInt8(0), 'composite', arrayStringConcat(arrayMap(v -> iids[v], coh), ','),
                if(ends[1].5, toInt64(ends[1].2), toInt64(0)), if(ends[1].5, ends[1].3, nil), if(ends[1].5, ends[1].4, nil),
                if(ends[2].5, toInt64(ends[2].2), toInt64(0)), if(ends[2].5, ends[2].3, nil), if(ends[2].5, ends[2].4, nil),
                nil, nils, nils, nil, nil, nil,
                toUInt64(0), toInt64(0), nil, nil, nils, nils, nil, nils,
                nils, nils, nil
            )],
            arrayMap(v -> (
                toUInt8(v), iids[v], '',
                vs[v].2, vs[v].3, vs[v].4,
                ve[v].2, ve[v].3, ve[v].4,
                ve[v].5, ve[v].7, ve[v].8, ve[v].9,
                latest[v].6, startb[v],
                toUInt64(length(excl[v])), excl[v][1].2, excl[v][1].3, excl[v][1].4, excl[v][1].7, excl[v][1].8, excl[v][1].9, excl[v][1].10,
                vs[v].7, vs[v].8, vs[v].9
            ), range(1, nv + 1))
        ) AS out_rows
    FROM
    (
        SELECT
            w, coh, vs, ve, a0, rl, ex,
            arrayMap(v -> arraySort(x -> -x.2, arrayFilter(x -> x.1 = v, rl))[1], range(1, nv + 1)) AS latest,
            arrayMap(v -> arraySort(x -> (abs(x.2 - cuts_us[w]), x.2), arrayFilter(x -> x.1 = v AND x.6 IS NOT NULL AND abs(x.2 - cuts_us[w]) <= sync_us, a0))[1].6, range(1, nv + 1)) AS startb,
            arrayMap(v -> arraySort(x -> -x.2, arrayFilter(x -> x.1 = v, ex)), range(1, nv + 1)) AS excl,
            arrayMap(b -> arraySort(c -> (c.1, c.2),
                arrayFilter(c -> c.5,
                    arrayMap(
                        p -> (
                            p.1, p.2,
                            arrayReduce('sum', arrayMap(y -> y.3, p.3)),
                            if(arrayExists(y -> y.4 IS NULL, p.3), nil, arrayReduce('sum', arrayMap(y -> y.4, p.3))),
                            arrayAll(y -> y.1 != 0 AND y.3 IS NOT NULL, p.3)
                        ),
                        arrayMap(
                            T -> (
                                abs(T - b.1), T,
                                arrayMap(v -> arraySort(x -> (abs(x.2 - T), x.2), arrayFilter(x -> x.1 = v AND abs(x.2 - T) <= sync_us, b.2))[1], coh)
                            ),
                            arrayDistinct(arrayMap(x -> x.2, arrayFilter(x -> has(coh, x.1) AND abs(x.2 - b.1) <= sync_us, b.2)))
                        )
                    )
                ))[1], [(cuts_us[w], a0), (cutoff_us, a1)]) AS ends
        FROM
        (
            SELECT
                w, a0, a1, rl, ex,
                arrayMap(v -> arraySort(x -> (abs(x.2 - cuts_us[w]), x.2), arrayFilter(x -> x.1 = v AND abs(x.2 - cuts_us[w]) <= sync_us, a0))[1], range(1, nv + 1)) AS vs,
                arrayMap(v -> arraySort(x -> (abs(x.2 - cutoff_us), x.2), arrayFilter(x -> x.1 = v AND abs(x.2 - cutoff_us) <= sync_us, a1))[1], range(1, nv + 1)) AS ve,
                arrayFilter(v -> cand[v] = 1 AND vs[v].1 != 0 AND ve[v].1 != 0, range(1, nv + 1)) AS coh
            FROM
            (
                SELECT
                    w,
                    arrayMap(x -> (x.1, x.2, x.3, ifNull(x.4, x.3 * arraySort(k -> (abs(k.2 - x.2), k.2), arrayFilter(k -> k.1 = x.1 AND abs(k.2 - x.2) <= sync_us, k0))[1].3), x.5, x.6, x.7, x.8, x.9, x.10), r0) AS a0,
                    arrayMap(x -> (x.1, x.2, x.3, ifNull(x.4, x.3 * arraySort(k -> (abs(k.2 - x.2), k.2), arrayFilter(k -> k.1 = x.1 AND abs(k.2 - x.2) <= sync_us, k1))[1].3), x.5, x.6, x.7, x.8, x.9, x.10), r1) AS a1,
                    rl, ex
                FROM
                (
                    SELECT
                        w,
                        groupArrayIf((vi, ts_us, n, u, mk, bs, prec, tprec, bsz, anc), src = 0 AND which = 0 AND adm AND abs(ts_us - bound) <= 3 * sync_us) AS r0,
                        groupArrayIf((vi, ts_us, n, u, mk, bs, prec, tprec, bsz, anc), src = 0 AND which = 1 AND adm AND abs(ts_us - bound) <= 3 * sync_us) AS r1,
                        groupArrayIf((vi, ts_us, n, u, mk, bs, prec, tprec, bsz, anc), src = 0 AND which = 1 AND adm AND cutoff_us - ts_us <= wide_us) AS rl,
                        groupArrayIf((vi, ts_us, n, u, mk, bs, prec, tprec, bsz, anc), src = 0 AND NOT edge AND abs(ts_us - bound) <= 3 * sync_us) AS ex,
                        groupArrayIf((vi, ts_us, mk), src = 1 AND which = 0 AND abs(ts_us - bound) <= 3 * sync_us) AS k0,
                        groupArrayIf((vi, ts_us, mk), src = 1 AND which = 1 AND abs(ts_us - bound) <= 3 * sync_us) AS k1
                    FROM
                    (
                        SELECT
                            toUInt8(0) AS src,
                            toUInt8(indexOf(iids, instrument_id)) AS vi,
                            toUnixTimestamp64Micro(ts) AS ts_us,
                            toFloat64OrNull(oi_native) AS n,
                            ifNull(toFloat64OrNull(oi_usd), n * nullIf(toFloat64OrNull(mark_price), 0)) AS u,
                            toFloat64OrNull(mark_price) AS mk,
                            if(ifNull(toFloat64OrNull(index_price), 0) != 0, (mk / toFloat64OrNull(index_price) - 1) * 10000, nil) AS bs,
                            ts_prec AS prec,
                            timestamp_precision AS tprec,
                            toFloat64OrNull(ts_bucket_size_s) AS bsz,
                            ts_bucket_anchor AS anc,
                            NOT (ts_bucket_anchor IS NULL AND ifNull(ts_prec, '') LIKE 'BUCKET(%')
                                AND ifNull(ts_bucket_anchor, '') != 'UNSPECIFIED' AS edge,
                            edge AND n IS NOT NULL AS adm
                        FROM mkt.oi_obs FINAL
                        WHERE instrument_id IN iids
                          AND ts >= scan_from
                          AND ts <= cutoff
                          AND (recorded_at IS NULL OR recorded_at <= assembled)
                          AND (received_ts IS NULL OR received_ts <= assembled)
                        UNION ALL
                        SELECT
                            toUInt8(1) AS src,
                            toUInt8(indexOf(iids, instrument_id)) AS vi,
                            toUnixTimestamp64Micro(ts) AS ts_us,
                            nil AS n,
                            nil AS u,
                            toFloat64OrNull(mark_price) AS mk,
                            nil AS bs,
                            nils AS prec,
                            nils AS tprec,
                            nil AS bsz,
                            nils AS anc,
                            toUInt8(1) AS edge,
                            toUInt8(1) AS adm
                        FROM mkt.tickers_24h FINAL
                        WHERE instrument_id IN iids
                          AND ts >= scan_from
                          AND ts <= cutoff
                          AND (recorded_at IS NULL OR recorded_at <= assembled)
                          AND (received_ts IS NULL OR received_ts <= assembled)
                          AND toFloat64OrNull(mark_price) != 0
                    )
                    ARRAY JOIN
                        arrayConcat(arrayEnumerate(lens_us), arrayEnumerate(lens_us)) AS w,
                        arrayConcat(arrayMap(x -> toUInt8(0), lens_us), arrayMap(x -> toUInt8(1), lens_us)) AS which,
                        arrayConcat(cuts_us, arrayMap(x -> cutoff_us, lens_us)) AS bound
                    WHERE abs(ts_us - bound) <= 3 * sync_us OR (which = 1 AND src = 0 AND cutoff_us - ts_us <= wide_us)
                    GROUP BY w
                )
            )
        )
    )
)
ARRAY JOIN out_rows AS r
ORDER BY w, series
