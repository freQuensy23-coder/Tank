from tank.clickhouse_market import parameter_string
from tank.util import canonical_json


def _object(fields):
    parts = []
    for key, value in fields.items():
        parts.extend([repr(("," if parts else "") + '"' + key + '":'), value])
    return "concat('{', " + ", ".join(parts) + ", '}')"


def _json(value):
    return f"toJSONString({value})"


def _array(value):
    return f"concat('[', arrayStringConcat({value}, ','), ']')"


def ctes():
    venue_json = _object(
        {
            "venue": _json("v.1"),
            "complete": "'true'",
            "volume_native": _json("toString(v.2)"),
            "buy_qty": _json("toString(v.3)"),
            "sell_qty": _json("toString(v.4)"),
            "cvd": _json("toString(v.5)"),
            "buy_share": _json("spot_ratio(v.3, v.2)"),
            "bs_ratio": _json("spot_ratio(v.3, v.4)"),
            "sign": _json("v.7"),
            "sum_sq_qty": _json("toString(v.6)"),
            "boundary_segments": "v.8",
            "volume_share": _json("spot_ratio(v.2, spot_volume)"),
        }
    )
    segment_json = _object(
        {
            "start": _json("spot_iso(p.1)"),
            "cutoff": _json("spot_iso(p.2)"),
            "trade_count": (
                "toString(arraySum(arrayMap(r -> JSONExtractInt(r, 'n_buy') + "
                "JSONExtractInt(r, 'n_sell'), p.3)))"
            ),
            "aggregates": _array("p.3"),
        }
    )
    coverage_exclusion = _object(
        {
            "venue": _json("a.1"),
            "reason": "'\"INCOMPLETE\"'",
            "detail": _json("if(a.4, 'BOUNDARY_AGGREGATE_UNAVAILABLE', 'FULL_MINUTE_COVERAGE_INCOMPLETE')"),
            "source_reasons": _json(
                "arraySort(arrayDistinct(arrayMap(r -> spot_reason(r), "
                "arrayFilter(r -> has(a.2, JSONExtractString(r, 'instrument_id')) "
                "AND spot_reason(r) != '', arrayConcat(a.3, spot_pieces)))))"
            ),
        }
    )
    field_exclusion = _object(
        {
            "venue": _json("a.1"),
            "reason": "'\"INCOMPLETE\"'",
            "detail": "'\"FLOW_AGGREGATE_FIELDS_MISSING\"'",
            "missing_metrics": _json(
                "arrayFilter(k -> arrayExists(r -> JSONExtractRaw(r, k) IN ('', 'null'), a.5), "
                "['buy_qty', 'sell_qty', 'sum_sq_qty'])"
            ),
        }
    )
    composite_json = _object(
        {
            "cvd": _json("if(empty(vs), NULL, toString(spot_sum_tuple(vs, 'cvd')))"),
            "volume": _json("if(empty(vs), NULL, toString(spot_sum_tuple(vs, 'volume')))"),
            "buy_share": _json(
                "spot_ratio(spot_sum_tuple(vs, 'volume') + spot_sum_tuple(vs, 'cvd'), "
                "2 * spot_sum_tuple(vs, 'volume'))"
            ),
            "bs_ratio": _json(
                "spot_ratio(spot_sum_tuple(vs, 'volume') + spot_sum_tuple(vs, 'cvd'), "
                "spot_sum_tuple(vs, 'volume') - spot_sum_tuple(vs, 'cvd'))"
            ),
            "venues": _json("arrayMap(v -> v.1, vs)"),
            "sign": _json(
                "spot_sign_for(spot_sum_tuple(vs, 'cvd'), spot_sum_tuple(vs, 'q'), spot_sigma_value)"
            ),
        }
    )
    sql = r"""
(x -> if(startsWith(x, '"'), JSONExtractString(concat('{"v":', x, '}'), 'v'), x)) AS spot_text,
((r, k) -> if(startsWith(JSONExtractRaw(r, k), '{'),
    JSONExtractRaw(JSONExtractRaw(r, k), 'value'), JSONExtractRaw(r, k))) AS spot_raw,
(x -> replaceRegexpOne(x, '^[+-]', '')) AS spot_unsigned,
(x -> (if(startsWith(x, '-'), '-', ''),
    replaceAll(spot_unsigned(splitByRegexp('[eE]', x)[1]), '.', ''),
    if(position(spot_unsigned(splitByRegexp('[eE]', x)[1]), '.') = 0,
        toInt64(length(spot_unsigned(splitByRegexp('[eE]', x)[1]))),
        toInt64(position(spot_unsigned(splitByRegexp('[eE]', x)[1]), '.')) - 1)
        + toInt32OrZero(splitByRegexp('[eE]', x)[2]))) AS spot_scientific,
(p -> concat(p.1, multiIf(p.3 <= 0,
    concat('0.', repeat('0', greatest(least(-p.3, 48), 0)), p.2),
    p.3 >= length(p.2), concat(p.2, repeat('0', greatest(least(p.3 - toInt64(length(p.2)), 48), 0))),
    concat(substring(p.2, 1, greatest(p.3, 0)), '.', substring(p.2, greatest(p.3 + 1, 1)))))) AS spot_fixed,
(x -> if(match(x, '^[+-]?[0-9]{1,24}([.][0-9]{1,24})?[eE][+-]?[0-9]{1,2}$')
    AND abs(toInt32OrZero(splitByRegexp('[eE]', x)[2])) <= 48,
    spot_fixed(spot_scientific(x)), x)) AS spot_decimal_text,
(x -> if(match(spot_text(x), '^[0-9]+([.][0-9]+)?$'),
    toInt64(multiplyDecimal(toDecimal256OrZero(spot_text(x), 6),
        toDecimal256(multiIf(toDecimal256OrZero(spot_text(x), 6) > 100000000000000, 1,
            toDecimal256OrZero(spot_text(x), 6) > 100000000000, 1000, 1000000), 6), 6)),
    toUnixTimestamp64Micro(parseDateTime64BestEffortOrNull(spot_text(x), 6, 'UTC')))) AS spot_timestamp,
(r -> spot_timestamp(arrayFirst(x -> x NOT IN ('', 'null'),
    arrayMap(k -> JSONExtractRaw(r, k), ['minute_start', 'open_time', 'exchange_ts', 'ts',
        'as_of', 'settlement_time', 'funding_time', 'observed_at'])))) AS spot_time,
(t -> concat(formatDateTime(fromUnixTimestamp64Micro(t, 'UTC'),
    '%Y-%m-%dT%H:%i:%S', 'UTC'), if(modulo(t, 1000000) = 0, '',
    concat('.', formatDateTime(fromUnixTimestamp64Micro(t, 'UTC'), '%f', 'UTC'))), 'Z')) AS spot_iso,
(r -> if(JSONExtractString(r, 'venue') != '', JSONExtractString(r, 'venue'),
    arrayElement(splitByChar(':', JSONExtractString(r, 'instrument_id')), 1))) AS spot_venue,
(r -> if(JSONExtractString(r, 'market_type') != '', JSONExtractString(r, 'market_type'),
    if(position(JSONExtractString(r, 'instrument_id'), ':spot:') > 0, 'SPOT', 'PERP_LINEAR'))) AS spot_market,
((r, k) -> if(spot_raw(r, k) IN ('', 'null'), CAST(NULL, 'Nullable(Decimal256(24))'),
    toDecimal256OrZero(spot_decimal_text(spot_text(spot_raw(r, k))), 24) + toDecimal256(throwIf(
        spot_raw(r, k) NOT IN ('', 'null')
        AND NOT match(spot_decimal_text(spot_text(spot_raw(r, k))), '^[+-]?0*[0-9]{1,24}([.][0-9]{1,24})?$'),
        'Market Truth spot financial input outside native range'), 24))) AS spot_money,
toDecimal256(0, 24) AS spot_zero,
(x -> x + toDecimal256(throwIf(abs(ifNull(x, spot_zero)) >= toDecimal256('1000000000000000000000000', 24),
    'Market Truth spot aggregate outside native financial range'), 24)) AS spot_bound,
(x -> x + toDecimal256(throwIf(abs(ifNull(x, spot_zero)) >= toDecimal256('1000000000000000000000000', 24),
    'Market Truth spot squared quantity exceeds native range'), 24)) AS spot_qbound,
((a, b) -> if(isNull(a) OR isNull(b) OR b = 0, CAST(NULL, 'Nullable(Float64)'),
    toFloat64(divideDecimal(ifNull(a, spot_zero), if(ifNull(b, spot_zero) = 0,
        toDecimal256(1, 24), ifNull(b, spot_zero)), 24)))) AS spot_ratio,
((a, b) -> if(isNull(a) OR isNull(b) OR b <= 0, CAST(NULL, 'Nullable(Bool)'),
    CAST(2 * ifNull(a, spot_zero) > ifNull(b, spot_zero), 'Bool'))) AS spot_majority,
((rs, k) -> if(empty(rs) OR arrayExists(r -> isNull(spot_money(r, k)), rs),
    CAST(NULL, 'Nullable(Decimal256(24))'), arrayReduce('sum', arrayMap(r -> ifNull(spot_money(r, k), spot_zero), rs)))) AS spot_sum,
((vs, k) -> arrayReduce('sum', arrayMap(v -> multiIf(k = 'volume', v.2, k = 'cvd', v.5, v.6), vs))) AS spot_sum_tuple,
(toDecimal256OrZero(spot_decimal_text({spot_sigma:String}), 24) + toDecimal256(throwIf(
    NOT match(spot_decimal_text({spot_sigma:String}), '^[+]?[0-9]{1,3}([.][0-9]{1,12})?$')
    OR toDecimal256OrZero(spot_decimal_text({spot_sigma:String}), 24) > 100,
    'Market Truth spot sigma outside exact native range'), 24)) AS spot_sigma,
((c, q, sigma) -> if(c = 0, toInt8(0), if(
    multiplyDecimal(spot_bound(c), spot_bound(c), 24) >=
    multiplyDecimal(multiplyDecimal(sigma, sigma, 24), spot_qbound(q), 24),
    if(c > 0, toInt8(1), toInt8(-1)), toInt8(0)))) AS spot_sign_for,
((c, q) -> spot_sign_for(c, q, spot_sigma)) AS spot_sign,
(r -> if(JSONExtractString(r, 'gap_reason') != '', JSONExtractString(r, 'gap_reason'),
    JSONExtractString(r, 'coverage_reason'))) AS spot_reason,
((r, assembled) -> assembled IS NULL OR JSONExtractRaw(r, 'received_ts') IN ('', 'null')
    OR spot_timestamp(JSONExtractRaw(r, 'received_ts')) <= assembled) AS spot_admitted,
((r, assembled) -> assembled IS NULL OR JSONExtractRaw(r, 'observed_at') IN ('', 'null')
    OR spot_timestamp(JSONExtractRaw(r, 'observed_at')) <= assembled) AS spot_observed,
(r -> (JSONExtractString(r, 'instrument_id'), JSONExtractString(r, 'start'), JSONExtractString(r, 'end'))) AS spot_piece_key,
((pieces, iid, lo, hi, assembled) -> arrayFold((state, r) ->
    if(spot_timestamp(JSONExtractRaw(r, 'start')) = state.1,
        (assumeNotNull(spot_timestamp(JSONExtractRaw(r, 'end'))), arrayPushBack(state.2, r)), state),
    arraySort(r -> (spot_timestamp(JSONExtractRaw(r, 'start')), -spot_timestamp(JSONExtractRaw(r, 'end')),
        indexOf(pieces, r)),
        arrayFilter(r -> JSONExtractString(r, 'instrument_id') = iid
            AND JSONExtractRaw(r, 'complete') = 'true'
            AND lo <= spot_timestamp(JSONExtractRaw(r, 'start'))
            AND spot_timestamp(JSONExtractRaw(r, 'start')) < spot_timestamp(JSONExtractRaw(r, 'end'))
            AND spot_timestamp(JSONExtractRaw(r, 'end')) <= hi
            AND spot_admitted(r, assembled) AND spot_observed(r, assembled), pieces)),
    (toInt64(lo), CAST([], 'Array(String)')))) AS spot_cover,
(vs -> if(empty(vs),
    '{"cvd":null,"volume":null,"buy_share":null,"bs_ratio":null,"venues":[],"sign":0,"reason":"INCOMPLETE"}',
    __COMPOSITE__)) AS spot_composite,
(bars -> arrayFlatten(arrayMap(b -> arrayMap(p -> concat('{', arrayStringConcat(arrayConcat(
    arrayMap(kv -> concat(toJSONString(kv.1), ':', kv.2),
        arrayFilter(kv -> kv.1 NOT IN ('instrument_id', 'token_id', 'venue', 'market_type'),
            JSONExtractKeysAndValuesRaw(p))),
    [concat('"instrument_id":', toJSONString(JSONExtractString(b, 'instrument_id'))),
     concat('"token_id":', if(JSONHas(b, 'token_id'), JSONExtractRaw(b, 'token_id'), 'null')),
     concat('"venue":', toJSONString(spot_venue(b))),
     concat('"market_type":', toJSONString(spot_market(b)))]), ','), '}'),
    JSONExtractArrayRaw(b, 'boundary_segments')), bars))) AS spot_embedded_for,
spot_source AS (SELECT {spot_snapshot:String} AS spot_document),
spot_document_fields AS (
    SELECT s.spot_document, spot_sigma AS spot_sigma_value,
        spot_timestamp(JSONExtractRaw(s.spot_document, 'cutoff')) AS spot_cutoff,
        spot_timestamp(JSONExtractRaw(s.spot_document, 'assembled_at')) AS spot_assembled,
        if(startsWith(JSONExtractRaw(s.spot_document, 'anchor'), '{'),
            JSONExtractRaw(JSONExtractRaw(s.spot_document, 'anchor'), 'ts'),
            JSONExtractRaw(s.spot_document, 'anchor')) AS spot_anchor_text,
        JSONExtractArrayRaw(s.spot_document, 'flow_bars') AS spot_all_bars,
        arrayFilter(r -> (JSONExtractRaw(r, 'token_id') IN ('', 'null')
            OR JSONExtractRaw(r, 'token_id') = JSONExtractRaw(s.spot_document, 'token_id')),
            JSONExtractArrayRaw(s.spot_document, 'instruments')) AS spot_instruments,
        JSONExtractArrayRaw(s.spot_document, 'tickers') AS spot_all_tickers,
        JSONExtractArrayRaw(s.spot_document, 'flow_segments') AS spot_explicit_pieces
    FROM spot_source AS s
),
spot_source_records AS (
    SELECT s.*,
        s.spot_anchor_text NOT IN ('', 'null', '""', '0', 'false') AS spot_anchor_present,
        arrayMap(r -> (r, spot_time(r), spot_venue(r), spot_market(r),
            JSONExtractString(r, 'instrument_id'), spot_admitted(r, s.spot_assembled),
            if(JSONHas(r, 'interval_s'), JSONExtractInt(r, 'interval_s'), 60)),
            s.spot_all_bars) AS spot_all_bar_records,
        arrayMap((r, idx) -> (r, spot_time(r), spot_venue(r), spot_admitted(r, s.spot_assembled), idx),
            s.spot_all_tickers, arrayEnumerate(s.spot_all_tickers)) AS spot_all_ticker_records,
        arrayMap(r -> (spot_venue(r), JSONExtractString(r, 'instrument_id')),
            arrayFilter(r -> spot_market(r) = 'SPOT', s.spot_instruments)) AS spot_registry_records,
        spot_embedded_for(s.spot_all_bars) AS spot_embedded_pieces
    FROM spot_document_fields AS s
),
spot_bounded_records AS (
    SELECT s.*,
        if(NOT s.spot_anchor_present, s.spot_cutoff - 14400000000,
            spot_timestamp(s.spot_anchor_text)) AS spot_start,
        arrayFilter(r -> r.2 + 1000000 * r.7 <= s.spot_cutoff AND r.6,
            s.spot_all_bar_records) AS spot_bar_records,
        arrayFilter(r -> r.2 <= s.spot_cutoff AND r.4,
            s.spot_all_ticker_records) AS spot_ticker_records,
        arrayConcat(s.spot_explicit_pieces, s.spot_embedded_pieces) AS spot_piece_candidates
    FROM spot_source_records AS s
),
spot_piece_keys_stage AS (
    SELECT s.*, arrayDistinct(arrayMap(r -> spot_piece_key(r), s.spot_piece_candidates)) AS spot_piece_keys,
        arrayMap(r -> r.1, s.spot_bar_records) AS spot_bars,
        arrayMap(r -> r.1, s.spot_ticker_records) AS spot_tickers
    FROM spot_bounded_records AS s
),
spot_piece_selection AS (
    SELECT s.*,
        arrayMap(key -> if(arrayExists(r -> spot_piece_key(r) = key, s.spot_explicit_pieces),
            arrayElement(arrayReverse(arrayFilter(r -> spot_piece_key(r) = key, s.spot_explicit_pieces)), 1),
            arrayFirst(r -> spot_piece_key(r) = key, s.spot_embedded_pieces)), s.spot_piece_keys) AS spot_unique_pieces,
        spot_embedded_for(s.spot_bars) AS spot_retained_embedded
    FROM spot_piece_keys_stage AS s
),
spot_piece_bounds AS (
    SELECT s.*,
        arrayFilter(r -> spot_timestamp(JSONExtractRaw(r, 'end')) <= s.spot_cutoff
            AND spot_admitted(r, s.spot_assembled), s.spot_unique_pieces) AS spot_bounded_pieces,
        arrayDistinct(arrayMap(r -> spot_piece_key(r), s.spot_retained_embedded)) AS spot_retained_keys
    FROM spot_piece_selection AS s
),
spot_restored_pieces AS (
    SELECT s.*,
        arrayConcat(s.spot_bounded_pieces, arrayMap(key -> arrayFirst(r -> spot_piece_key(r) = key,
            s.spot_retained_embedded), arrayFilter(key -> NOT arrayExists(r -> spot_piece_key(r) = key,
                s.spot_bounded_pieces), s.spot_retained_keys))) AS spot_pieces
    FROM spot_piece_bounds AS s
),
spot_input AS (
    SELECT s.spot_document, s.spot_sigma_value, s.spot_cutoff, s.spot_assembled, s.spot_anchor_text,
        s.spot_anchor_present, s.spot_start, s.spot_all_bars, s.spot_instruments,
        s.spot_tickers, s.spot_bars, s.spot_pieces, s.spot_bar_records,
        s.spot_ticker_records, s.spot_registry_records,
        arrayMap((r, idx) -> (r, spot_timestamp(JSONExtractRaw(r, 'start')),
            spot_timestamp(JSONExtractRaw(r, 'end')), JSONExtractString(r, 'instrument_id'),
            JSONExtractRaw(r, 'complete') = 'true' AND spot_admitted(r, s.spot_assembled)
                AND spot_observed(r, s.spot_assembled), idx),
            s.spot_pieces, arrayEnumerate(s.spot_pieces)) AS spot_piece_records,
        [('W15M', toInt64(900000000)), ('W1H', toInt64(3600000000)),
            ('W4H', toInt64(14400000000)), ('W24H', toInt64(86400000000)),
            ('EVENT', assumeNotNull(spot_cutoff - spot_start))] AS spot_window_defs,
        if(s.spot_anchor_present AND s.spot_cutoff - s.spot_start BETWEEN 900000000 AND 86400000000,
            'EVENT', 'W4H') AS spot_primary
    FROM spot_restored_pieces AS s
    WHERE throwIf(spot_start >= spot_cutoff OR spot_start IS NULL OR spot_cutoff IS NULL,
        'Market Truth anchor must precede cutoff') = 0
),
spot_period_rows AS (
    SELECT s.*, spot_window_period
    FROM spot_input AS s
    ARRAY JOIN arrayFlatten(arrayMap(w -> [(w, toUInt8(0)), (w, toUInt8(1))],
        s.spot_window_defs)) AS spot_window_period
),
spot_period_fields AS (
    SELECT s.*, s.spot_window_period.1 AS spot_window, s.spot_window_period.2 AS spot_period
    FROM spot_period_rows AS s
),
spot_period_names AS (
    SELECT s.*, s.spot_window.1 AS spot_name, s.spot_window.2 AS spot_duration
    FROM spot_period_fields AS s
),
spot_period_ends AS (
    SELECT s.*, assumeNotNull(s.spot_cutoff - s.spot_period * s.spot_duration) AS spot_hi
    FROM spot_period_names AS s
),
spot_period_starts AS (
    SELECT s.*, s.spot_hi - s.spot_duration AS spot_lo,
        intDiv(s.spot_hi, 60000000) * 60000000 AS spot_floor_hi
    FROM spot_period_ends AS s
),
spot_period_floors AS (
    SELECT s.*, intDiv(s.spot_lo, 60000000) * 60000000 AS spot_floor_lo,
        arrayFilter(r -> r.4 = 'SPOT' AND r.2 < s.spot_hi
            AND r.2 + 60000000 > s.spot_lo, s.spot_bar_records) AS spot_overlapping_records
    FROM spot_period_starts AS s
),
spot_period_ceils AS (
    SELECT s.*, s.spot_floor_lo + if(s.spot_lo = s.spot_floor_lo, 0, 60000000) AS spot_ceil_lo,
        arrayFilter(r -> s.spot_lo <= r.2 AND r.2 + 60000000 <= s.spot_hi,
            s.spot_overlapping_records) AS spot_full_records,
        arrayConcat(s.spot_registry_records, arrayMap(r -> (r.3, r.5),
            s.spot_overlapping_records)) AS spot_registration
    FROM spot_period_floors AS s
),
spot_periods AS (
    SELECT s.*,
        greatest(toInt64(0), intDiv(s.spot_floor_hi - s.spot_ceil_lo, 60000000)) AS spot_expected,
        arrayFilter(p -> p.1 < p.2, arrayConcat(
            if(s.spot_lo != s.spot_floor_lo, [(s.spot_lo, least(s.spot_ceil_lo, s.spot_hi))], []),
            if(s.spot_hi != s.spot_floor_hi AND NOT (s.spot_lo != s.spot_floor_lo AND s.spot_floor_hi = s.spot_floor_lo),
                [(greatest(s.spot_lo, s.spot_floor_hi), s.spot_hi)], []))) AS spot_partial,
        arraySort(arrayDistinct(arrayMap(r -> r.1, s.spot_registration))) AS spot_registered
    FROM spot_period_ceils AS s
),
spot_grouped AS (
    SELECT s.*, arrayMap(ex -> (ex,
        arraySort(arrayDistinct(arrayMap(r -> r.2, arrayFilter(r -> r.1 = ex, s.spot_registration)))),
        arrayMap(r -> r.1, arrayFilter(r -> r.3 = ex, s.spot_full_records)),
        arrayMap(r -> r.2, arrayFilter(r -> r.3 = ex, s.spot_full_records))),
        s.spot_registered) AS spot_groups
    FROM spot_periods AS s
),
spot_boundaries AS (
    SELECT s.*, arrayMap(g -> (g.1, g.2, g.3,
        arrayMap(p -> (p.1, p.2,
            arrayMap(iid -> arrayFold((state, r) -> if(r.2 = state.1,
                (assumeNotNull(r.3), arrayPushBack(state.2, r.1)), state),
                arraySort(r -> (r.2, -r.3, r.6), arrayFilter(r -> r.4 = iid AND r.5
                    AND p.1 <= r.2 AND r.2 < r.3 AND r.3 <= p.2, s.spot_piece_records)),
                (toInt64(p.1), CAST([], 'Array(String)'))), g.2)), s.spot_partial), g.4),
        s.spot_groups) AS spot_covers
    FROM spot_grouped AS s
),
spot_admission AS (
    SELECT s.*, arrayMap(g -> (g.1, g.2, g.3,
        arrayExists(p -> arrayExists(c -> c.1 != p.2, p.3), g.4),
        arrayConcat(g.3, arrayFlatten(arrayMap(p -> arrayFlatten(arrayMap(c -> c.2, p.3)), g.4))),
        arrayMap(p -> (p.1, p.2, arrayFlatten(arrayMap(c -> c.2, p.3))), g.4),
        length(arrayDistinct(g.5)) >= s.spot_expected), s.spot_covers) AS spot_admissions
    FROM spot_boundaries AS s
),
spot_eligibility AS (
    SELECT s.*, arrayMap(a -> NOT a.4 AND a.7
        AND arrayAll(r -> JSONExtractRaw(r, 'complete') = 'true', a.5),
        s.spot_admissions) AS spot_complete
    FROM spot_admission AS s
),
spot_eligible AS (
    SELECT s.*, arrayMap((a, complete) -> if(complete, a.5, []),
        s.spot_admissions, s.spot_complete) AS spot_eligible_rows
    FROM spot_eligibility AS s
),
spot_numeric_raw AS (
    SELECT s.*, arrayMap(rs -> arrayMap(r -> arrayMap(k -> spot_raw(r, k),
        ['buy_qty', 'sell_qty', 'sum_sq_qty']), rs), s.spot_eligible_rows) AS spot_raw_numbers
    FROM spot_eligible AS s
),
spot_numeric_text AS (
    SELECT s.*, arrayMap(rs -> arrayMap(r -> arrayMap(x -> spot_text(x), r), rs),
        s.spot_raw_numbers) AS spot_number_texts
    FROM spot_numeric_raw AS s
),
spot_numeric_fixed AS (
    SELECT s.*, arrayMap(rs -> arrayMap(r -> arrayMap(x -> spot_decimal_text(x), r), rs),
        s.spot_number_texts) AS spot_fixed_numbers
    FROM spot_numeric_text AS s
),
spot_numeric_values AS (
    SELECT s.*, arrayMap((rs, raw_rs) -> arrayMap((r, raw_r) -> arrayMap((x, raw) ->
        if(raw IN ('', 'null'), CAST(NULL, 'Nullable(Decimal256(24))'),
            toDecimal256OrZero(x, 24) + toDecimal256(throwIf(
                raw NOT IN ('', 'null') AND NOT match(x, '^[+-]?0*[0-9]{1,24}([.][0-9]{1,24})?$'),
                'Market Truth spot financial input outside native range'), 24)), r, raw_r), rs, raw_rs),
        s.spot_fixed_numbers, s.spot_raw_numbers) AS spot_values
    FROM spot_numeric_fixed AS s
),
spot_numeric_sums AS (
    SELECT s.*, arrayMap(rs -> arrayMap(k -> if(empty(rs) OR arrayExists(r -> r[k] IS NULL, rs),
        CAST(NULL, 'Nullable(Decimal256(24))'),
        arrayReduce('sum', arrayMap(r -> ifNull(r[k], spot_zero), rs))), [1, 2, 3]),
        s.spot_values) AS spot_sums
    FROM spot_numeric_values AS s
),
spot_aggregated AS (
    SELECT s.*, arrayMap((a, sums, complete) -> (a.1,
        spot_bound(sums[1] + sums[2]), spot_bound(sums[1]), spot_bound(sums[2]),
        spot_bound(sums[1] - sums[2]), spot_qbound(sums[3]), complete, __SEGMENTS__),
        s.spot_admissions, s.spot_sums, s.spot_complete) AS spot_aggregates
    FROM spot_numeric_sums AS s
),
spot_results AS (
    SELECT s.*, arrayMap(v -> (v.1, assumeNotNull(v.2), assumeNotNull(v.3), assumeNotNull(v.4),
        assumeNotNull(v.5), assumeNotNull(v.6), spot_sign_for(assumeNotNull(v.5), assumeNotNull(v.6), s.spot_sigma_value), v.8),
        arrayFilter(v -> v.7 AND v.2 IS NOT NULL AND v.6 IS NOT NULL, s.spot_aggregates)) AS spot_venues,
        arrayMap((a, v) -> if(NOT v.7, __COVERAGE_EXCLUSION__, __FIELD_EXCLUSION__),
            arrayFilter((a, v) -> NOT v.7 OR v.2 IS NULL OR v.6 IS NULL, s.spot_admissions, s.spot_aggregates),
            arrayFilter(v -> NOT v.7 OR v.2 IS NULL OR v.6 IS NULL, s.spot_aggregates)) AS spot_excluded,
        arrayMap(v -> v.1, arrayFilter(v -> NOT v.7 OR v.2 IS NULL OR v.6 IS NULL,
            s.spot_aggregates)) AS spot_excluded_names
    FROM spot_aggregated AS s
),
spot_totals AS (
    SELECT s.*, spot_bound(spot_sum_tuple(s.spot_venues, 'volume')) AS spot_volume,
        spot_bound(spot_sum_tuple(s.spot_venues, 'cvd')) AS spot_cvd,
        spot_qbound(spot_sum_tuple(s.spot_venues, 'q')) AS spot_q,
        arrayMap(ex -> arrayElement(arraySort(r -> (-r.2, r.5), arrayFilter(r -> r.3 = ex
            AND r.2 <= s.spot_hi, s.spot_ticker_records)), 1).1,
            s.spot_excluded_names) AS spot_estimate_tickers,
        arraySort(v -> (-v.2, v.1), s.spot_venues) AS spot_ranked
    FROM spot_results AS s
),
spot_direction_stage AS (
    SELECT s.*, spot_sign_for(s.spot_cvd, s.spot_q, s.spot_sigma_value) AS spot_direction,
        arrayMap(r -> if(r = '', CAST(NULL, 'Nullable(Decimal256(24))'),
            spot_money(r, if(JSONHas(r, 'volume_native'), 'volume_native', 'volume'))),
            s.spot_estimate_tickers) AS spot_daily_estimates,
        multiplyDecimal(s.spot_volume, toDecimal256('86400000000', 24), 24) AS spot_scaled_volume,
        arrayMap(idx -> arrayReduce('sum', arrayMap(v -> v.2, arraySlice(s.spot_ranked, 1, idx))),
            arrayEnumerate(s.spot_ranked)) AS spot_cumulative,
        arrayMap(v -> __VENUE__, s.spot_venues) AS spot_venue_json
    FROM spot_totals AS s
),
spot_contribution_stage AS (
    SELECT s.*, arrayMap(v -> greatest(spot_zero, s.spot_direction * v.5), s.spot_venues) AS spot_contributions,
        if(arrayExists(x -> x IS NULL, s.spot_daily_estimates), CAST(NULL, 'Nullable(Decimal256(24))'),
            spot_qbound(arrayReduce('sum', arrayMap(x -> ifNull(x, spot_zero), s.spot_daily_estimates)))) AS spot_daily_estimated,
        if(empty(s.spot_ranked), toUInt64(0), if(arrayExists(x -> 2 * x > s.spot_volume, s.spot_cumulative),
            arrayFirstIndex(x -> 2 * x > s.spot_volume, s.spot_cumulative), length(s.spot_ranked))) AS spot_top_n,
        arrayReduce('sum', arrayMap(v -> if(v.7 = s.spot_direction, v.2, spot_zero), s.spot_venues)) AS spot_direction_volume
    FROM spot_direction_stage AS s
),
spot_contributor_stage AS (
    SELECT s.*, arrayReduce('sum', s.spot_contributions) AS spot_total_contribution,
        indexOf(s.spot_contributions, arrayMax(s.spot_contributions)) AS spot_contributor_index,
        multiplyDecimal(ifNull(s.spot_daily_estimated, spot_zero), toDecimal256(s.spot_duration, 24), 24)
            + if(s.spot_daily_estimated IS NULL, CAST(NULL, 'Nullable(Decimal256(24))'), spot_zero) AS spot_estimated_numerator,
        arraySlice(s.spot_ranked, 1, s.spot_top_n) AS spot_top,
        arrayFlatten(groupArrayIf(arrayMap(v -> v.1, s.spot_venues), s.spot_period = 0)
            OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) AS spot_cohort_names
    FROM spot_contribution_stage AS s
),
spot_cohort_set AS (
    SELECT s.*, arraySort(arrayFilter(ex -> countEqual(s.spot_cohort_names, ex) = 5,
        arrayDistinct(s.spot_cohort_names))) AS spot_cohort,
        arrayFilter((v, idx) -> idx != s.spot_contributor_index,
            s.spot_venues, arrayEnumerate(s.spot_venues)) AS spot_others
    FROM spot_contributor_stage AS s
),
spot_cohort_selected AS (
    SELECT s.*, arrayFilter(v -> has(s.spot_cohort, v.1), s.spot_venues) AS spot_selected,
        arrayReduce('sum', arrayMap(v -> v.2, s.spot_others)) AS spot_other_volume,
        arrayReduce('sum', arrayMap(v -> if(v.7 = s.spot_direction, v.2, spot_zero), s.spot_others)) AS spot_other_direction
    FROM spot_cohort_set AS s
),
spot_cohort_totals AS (
    SELECT s.*, spot_bound(spot_sum_tuple(s.spot_selected, 'volume')) AS spot_cohort_volume
    FROM spot_cohort_selected AS s
),
spot_cohort_majorities AS (
    SELECT s.*, spot_majority(s.spot_cohort_volume, s.spot_volume) AS spot_cohort_majority,
        groupArrayIf(tuple(s.spot_cohort_volume, length(s.spot_selected) = length(s.spot_cohort)),
            s.spot_period = 1) OVER (PARTITION BY s.spot_name
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS spot_previous_values
    FROM spot_cohort_totals AS s
),
spot_cohorts AS (
    SELECT s.*, length(s.spot_cohort) >= {spot_min_venues:UInt64}
            AND ifNull(s.spot_cohort_majority, false) AS spot_cohort_valid,
        s.spot_previous_values[1].1 AS spot_previous_volume,
        s.spot_previous_values[1].2 AS spot_previous_complete
    FROM spot_cohort_majorities AS s
),
spot_final AS (
    SELECT * FROM spot_cohorts WHERE spot_period = 0
)
"""
    replacements = {
        "__COMPOSITE__": composite_json,
        "__SEGMENTS__": _array(f"arrayMap(p -> {segment_json}, a.6)"),
        "__COVERAGE_EXCLUSION__": coverage_exclusion,
        "__FIELD_EXCLUSION__": field_exclusion,
        "__VENUE__": venue_json,
    }
    for marker, expression in replacements.items():
        sql = sql.replace(marker, expression)
    return sql.strip()


def select_sql():
    cohort = _object(
        {
            "cohort": _json("spot_cohort"),
            "share_of_max": _json("spot_ratio(spot_cohort_volume, spot_volume)"),
            "share_majority": _json("spot_cohort_majority"),
            "velocity": _json(
                "if(spot_previous_complete, spot_ratio(spot_cohort_volume, spot_previous_volume), NULL)"
            ),
            "reason": "'null'",
        }
    )
    composite_cohort = (
        "if(spot_cohort_valid, concat(substring(spot_composite(spot_selected), 1, "
        f"length(spot_composite(spot_selected)) - 1), ',', substring({cohort}, 2)), 'null')"
    )
    breadth = _object(
        {
            "n_pos": "toString(countEqual(arrayMap(v -> v.7, spot_venues), 1))",
            "n_neg": "toString(countEqual(arrayMap(v -> v.7, spot_venues), -1))",
            "n_neutral": "toString(countEqual(arrayMap(v -> v.7, spot_venues), 0))",
            "pct_pos": _json(
                "spot_ratio(toDecimal256(countEqual(arrayMap(v -> v.7, spot_venues), 1), 24), "
                "toDecimal256(length(spot_venues), 24))"
            ),
            "pct_neg": _json(
                "spot_ratio(toDecimal256(countEqual(arrayMap(v -> v.7, spot_venues), -1), 24), "
                "toDecimal256(length(spot_venues), 24))"
            ),
        }
    )
    directional_sums = {
        name: f"arrayReduce('sum', arrayMap(v -> if(v.7 = {sign}, v.2, spot_zero), spot_venues))"
        for name, sign in (("buy", 1), ("sell", -1), ("neutral", 0))
    }
    exact = _object(
        {
            **{
                name: _json(f"spot_majority({amount}, spot_volume)")
                for name, amount in directional_sums.items()
            },
            "covered": _json(
                "spot_majority(spot_scaled_volume, spot_scaled_volume + spot_estimated_numerator)"
            ),
        }
    )
    cw = _object(
        {name: _json(f"spot_ratio({amount}, spot_volume)") for name, amount in directional_sums.items()}
    )
    topn = _object(
        {
            "n": "toString(spot_top_n)",
            "venues": _json("arrayMap(v -> v.1, spot_top)"),
            "agreement": _json(
                "spot_ratio(toDecimal256(countEqual(arrayMap(v -> v.7, spot_top), spot_direction), 24), "
                "toDecimal256(length(spot_top), 24))"
            ),
        }
    )
    concentration = _object(
        {
            "hhi": _json(
                "if(spot_volume = 0, NULL, arraySum(arrayMap(v -> "
                "pow(ifNull(spot_ratio(v.2, spot_volume), 0), 2), spot_venues)))"
            ),
            "top_volume_venue": _json("if(empty(spot_ranked), NULL, spot_ranked[1].1)"),
            "top_volume_share": _json(
                "if(empty(spot_ranked), NULL, spot_ratio(spot_ranked[1].2, spot_volume))"
            ),
            "top_contrib_venue": _json("if(empty(spot_venues), NULL, spot_venues[spot_contributor_index].1)"),
            "top_contrib_share": _json("spot_ratio(arrayMax(spot_contributions), spot_total_contribution)"),
            "others_cwb_dir": _json("spot_ratio(spot_other_direction, spot_other_volume)"),
        }
    )
    coverage = _object(
        {
            "covered_share_est": _json(
                "spot_ratio(spot_scaled_volume, spot_scaled_volume + spot_estimated_numerator)"
            ),
            "exact_majority": _json(
                "spot_majority(spot_scaled_volume, spot_scaled_volume + spot_estimated_numerator)"
            ),
            "n_complete": "toString(length(spot_venues))",
            "excluded": _array("spot_excluded"),
        }
    )
    result = _object(
        {
            "venues": _array("spot_venue_json"),
            "composite_max": "spot_composite(spot_venues)",
            "composite_cohort": composite_cohort,
            "breadth": breadth,
            "cw_breadth": cw,
            "exact_majority": exact,
            "topn": topn,
            "concentration": concentration,
            "direction": _json(
                "if(empty(spot_venues), NULL, multiIf(spot_direction = 1, 'BUY', "
                "spot_direction = -1, 'SELL', 'NEUTRAL'))"
            ),
            "classification": _json(
                "multiIf(length(spot_venues) < {spot_min_venues:UInt64} OR spot_estimated_numerator IS NULL "
                "OR spot_scaled_volume + spot_estimated_numerator = 0 "
                "OR spot_scaled_volume <= spot_estimated_numerator, 'INSUFFICIENT_DATA', "
                "spot_total_contribution != 0 AND 2 * arrayMax(spot_contributions) > spot_total_contribution "
                "AND spot_other_volume != 0 AND 2 * spot_other_direction < spot_other_volume, 'CONCENTRATED', "
                "2 * spot_direction_volume > spot_volume AND spot_ranked[1].7 = spot_direction, "
                "'BROAD', 'DIVERGENT')"
            ),
            "coverage": coverage,
            "cohort_consistent": (
                "concat('{', arrayStringConcat(arrayMap(w -> concat(toJSONString(w.1), ':', "
                "toJSONString(CAST(spot_cohort_valid, 'Bool'))), "
                "arrayFilter(w -> w.1 != spot_name, spot_window_defs)), ','), '}')"
            ),
            "size_profile": "'null'",
            "window": _json("spot_name"),
            "start": _json("spot_iso(spot_lo)"),
            "cutoff": _json("spot_iso(spot_hi)"),
            "observed_at": _json("if(empty(spot_venues), NULL, spot_iso(spot_hi))"),
            "precision": _json("if(empty(spot_venues), NULL, 'BUCKET(60s)')"),
        }
    )
    return (
        "SELECT concat('{\"spots\":{', arrayStringConcat(groupArray(concat(toJSONString(spot_name), ':', "
        + result
        + ")), ','), '},\"windows\":{', arrayStringConcat(any(arrayMap(w -> concat(toJSONString(w.1), ':', "
        "toJSONString(toFloat64(w.2) / 1000000)), spot_window_defs)), ','), "
        "'},\"primary\":', toJSONString(any(spot_primary)), ',\"instruments\":', "
        "concat('[', arrayStringConcat(any(spot_instruments), ','), ']'), '}') AS result FROM spot_final"
    )


def query(snapshot, config=None):
    config = config or {}
    parameters = config.get("parameters", config)
    return "WITH " + ctes() + "\n" + select_sql(), {
        "spot_snapshot": (parameter_string(canonical_json(snapshot)), "String"),
        "spot_sigma": (str(parameters.get("significant_flow_sigma", 2)), "String"),
        "spot_min_venues": (parameters.get("min_spot_venues", 2), "UInt64"),
    }
