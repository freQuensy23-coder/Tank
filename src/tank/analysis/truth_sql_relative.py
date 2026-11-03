import json
from array import array

from tank.util import iso, jsonable


def _transport(value):
    if isinstance(value, dict):
        return {key: _transport(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, array, bytes, bytearray)):
        return [_transport(item) for item in value]
    return value


def _object(**fields):
    parts = []
    for key, expression in fields.items():
        parts.extend([repr(("," if parts else "") + '"' + key + '":'), expression])
    return "concat('{', " + ", ".join(parts) + ", '}')"


def _j(expression):
    return f"toJSONString({expression})"


def _bool(expression):
    return f"if({expression}, 'true', 'false')"


def _money(expression):
    return f"toJSONString(toString({expression}))"


_CORE = r"""
{rel_snapshot:String} AS rel_snapshot,
{rel_start:String} AS rel_start_text,
{rel_end:String} AS rel_end_text,
{rel_lookback:Float64} AS rel_lookback,
{rel_min_days:Float64} AS rel_min_days,
{rel_beta_weight:Float64} AS rel_beta_weight,
{rel_quantile_lo:Float64} AS rel_quantile_lo,
{rel_quantile_hi:Float64} AS rel_quantile_hi,
{rel_include_outcome:UInt8} AS rel_include_outcome,
rel_s -> if(match(rel_s, '^[+-]?[0-9]+([.][0-9]+)?$'),
    toNullable(toInt64(round(toFloat64OrZero(rel_s) * multiIf(abs(toFloat64OrZero(rel_s)) > 1e14, 1.,
        abs(toFloat64OrZero(rel_s)) > 1e11, 1000., 1000000.)))),
    toUnixTimestamp64Micro(parseDateTime64BestEffortOrNull(nullIf(rel_s, ''), 6, 'UTC'))) AS rel_ts,
(rel_r, rel_k) -> rel_ts(if(JSONType(rel_r, rel_k) = 'String', JSONExtractString(rel_r, rel_k),
    JSONExtractRaw(rel_r, rel_k))) AS rel_field_ts,
rel_t -> if(isNull(rel_t), NULL,
    concat(formatDateTime(fromUnixTimestamp64Micro(assumeNotNull(rel_t), 'UTC'), '%Y-%m-%dT%H:%i:%S', 'UTC'),
    if(modulo(assumeNotNull(rel_t), 1000000) = 0, '',
       formatDateTime(fromUnixTimestamp64Micro(assumeNotNull(rel_t), 'UTC'), '.%f', 'UTC')), 'Z')) AS rel_iso,
rel_ts(rel_start_text) AS rel_start,
rel_ts(rel_end_text) AS rel_end,
rel_field_ts(rel_snapshot, 'cutoff') AS rel_cutoff,
rel_field_ts(rel_snapshot, 'assembled_at') AS rel_assembled,
JSONExtractString(rel_snapshot, 'token_id') AS rel_token,
JSONExtractArrayRaw(rel_snapshot, 'klines') AS rel_klines,
JSONExtractArrayRaw(rel_snapshot, 'instruments') AS rel_instruments,
JSONExtractArrayRaw(rel_snapshot, 'tokens') AS rel_tokens,
JSONExtractArrayRaw(rel_snapshot, 'flow_bars') AS rel_flow,
JSONExtractArrayRaw(rel_snapshot, 'sector_baskets') AS rel_baskets,
JSONExtractKeysAndValuesRaw(rel_snapshot, 'stat_price_series') AS rel_projected,
rel_r -> JSONType(rel_r, 'complete') = 'Bool' AND JSONExtractBool(rel_r, 'complete') AS rel_complete,
rel_r -> coalesce(rel_field_ts(rel_r, 'minute_start'),
    rel_field_ts(rel_r, 'open_time'), rel_field_ts(rel_r, 'exchange_ts'),
    rel_field_ts(rel_r, 'ts'), rel_field_ts(rel_r, 'as_of'),
    rel_field_ts(rel_r, 'settlement_time'), rel_field_ts(rel_r, 'funding_time'),
    rel_field_ts(rel_r, 'observed_at')) AS rel_time,
rel_r -> if(JSONExtractString(rel_r, 'market_type') != '', JSONExtractString(rel_r, 'market_type'),
    if(position(JSONExtractString(rel_r, 'instrument_id'), ':spot:') > 0, 'SPOT', 'PERP_LINEAR')) AS rel_market,
rel_r -> if(JSONExtractString(rel_r, 'venue') != '', JSONExtractString(rel_r, 'venue'),
    arrayElement(splitByChar(':', JSONExtractString(rel_r, 'instrument_id')), 1)) AS rel_venue,
rel_r -> coalesce(rel_field_ts(rel_r, 'open_time') +
    if(JSONHas(rel_r, 'interval_s'), JSONExtractInt(rel_r, 'interval_s'), 60) * 1000000,
    rel_field_ts(rel_r, 'close_time'),
    rel_time(rel_r) + if(JSONHas(rel_r, 'interval_s'), JSONExtractInt(rel_r, 'interval_s'), 60) * 1000000) AS rel_close_time,
rel_r -> if(JSONType(rel_r) = 'Object', JSONExtractRaw(rel_r, 'value'), rel_r) AS rel_unwrap,
rel_r -> if(JSONType(rel_unwrap(rel_r)) = 'String', JSONExtractString(rel_unwrap(rel_r)),
    rel_unwrap(rel_r)) AS rel_numeric_text,
rel_r -> toFloat64OrNull(nullIf(rel_numeric_text(rel_r), 'null')) AS rel_number,
rel_x -> replaceRegexpOne(rel_x, '^[+-]', '') AS rel_unsigned,
rel_x -> (if(startsWith(rel_x, '-'), '-', ''),
    replaceAll(rel_unsigned(splitByRegexp('[eE]', rel_x)[1]), '.', ''),
    toInt64(if(position(rel_unsigned(splitByRegexp('[eE]', rel_x)[1]), '.') = 0,
        length(rel_unsigned(splitByRegexp('[eE]', rel_x)[1])),
        position(rel_unsigned(splitByRegexp('[eE]', rel_x)[1]), '.') - 1))
        + toInt32OrZero(splitByRegexp('[eE]', rel_x)[2])) AS rel_scientific,
rel_p -> concat(rel_p.1, multiIf(rel_p.3 <= 0,
    concat('0.', repeat('0', greatest(least(-rel_p.3, 48), 0)), rel_p.2),
    rel_p.3 >= length(rel_p.2), concat(rel_p.2, repeat('0', greatest(least(rel_p.3 - toInt64(length(rel_p.2)), 48), 0))),
    concat(substring(rel_p.2, 1, greatest(rel_p.3, 0)), '.', substring(rel_p.2, greatest(rel_p.3 + 1, 1))))) AS rel_fixed,
rel_x -> if(match(rel_x, '^[+-]?[0-9]{1,24}([.][0-9]{1,24})?[eE][+-]?[0-9]{1,2}$')
    AND abs(toInt32OrZero(splitByRegexp('[eE]', rel_x)[2])) <= 48,
    rel_fixed(rel_scientific(rel_x)), rel_x) AS rel_decimal_text,
rel_r -> if(throwIf(rel_numeric_text(rel_r) NOT IN ('', 'null')
    AND NOT match(rel_decimal_text(rel_numeric_text(rel_r)), '^[+-]?0*[0-9]{1,24}(\\.[0-9]{1,24})?$'),
    'Market Truth decimal input outside native financial range') = 0,
    toDecimal256OrNull(nullIf(rel_decimal_text(rel_numeric_text(rel_r)), 'null'), 24),
    CAST(NULL, 'Nullable(Decimal256(24))')) AS rel_decimal,
rel_r -> if(JSONHas(rel_r, 'c'), JSONExtractRaw(rel_r, 'c'), JSONExtractRaw(rel_r, 'close')) AS rel_close_raw,
rel_r -> (isNull(rel_assembled) OR
    coalesce(rel_field_ts(rel_r, 'received_ts') <= rel_assembled, 1)) AS rel_available,
rel_r -> rel_available(rel_r) AND (isNull(rel_assembled) OR
    coalesce(rel_field_ts(rel_r, 'observed_at') <= rel_assembled, 1)) AS rel_observed_available,
rel_kline_rows AS (
    SELECT JSONExtractString(rel_row, 'instrument_id') AS rel_iid,
        rel_close_time(rel_row) AS rel_t, rel_number(rel_close_raw(rel_row)) AS rel_p,
        rel_decimal(rel_close_raw(rel_row)) AS rel_exact_p,
        rel_row, toUInt64(rel_index) AS rel_order
    FROM (SELECT arrayJoin(arrayEnumerate(rel_klines)) AS rel_index,
        arrayElement(rel_klines, rel_index) AS rel_row)
    WHERE rel_complete(rel_row) AND rel_market(rel_row) = 'SPOT'
      AND rel_p != 0 AND rel_t <= rel_cutoff
),
rel_projected_rows AS (
    SELECT rel_entry.1 AS rel_iid, toInt64(rel_pair.1) AS rel_t,
        rel_number(rel_pair.2) AS rel_p,
        toUInt64(length(rel_klines)) + bitShiftLeft(toUInt64(rel_index), 32) + rel_position AS rel_order
    FROM (
        SELECT rel_index, arrayElement(rel_projected, rel_index) AS rel_entry,
            arrayJoin(arrayEnumerate(arraySlice(JSONExtractArrayRaw(rel_entry.2, 'times_us'), 1,
                least(length(JSONExtractArrayRaw(rel_entry.2, 'times_us')),
                      length(JSONExtractArrayRaw(rel_entry.2, 'values')))))) AS rel_position,
            tuple(arrayElement(JSONExtractArrayRaw(rel_entry.2, 'times_us'), rel_position),
                arrayElement(JSONExtractArrayRaw(rel_entry.2, 'values'), rel_position)) AS rel_pair
        FROM (SELECT arrayJoin(arrayEnumerate(rel_projected)) AS rel_index)
        WHERE JSONExtractString(rel_entry.2, 'time_basis') = 'CLOSE_TIME'
    )
    WHERE rel_p > 0 AND rel_t <= rel_cutoff
),
rel_price_points AS (
    SELECT rel_iid, assumeNotNull(rel_t) AS rel_t,
        argMax(tuple(assumeNotNull(rel_p)), rel_order).1 AS rel_p, min(rel_order) AS rel_order
    FROM (
        SELECT rel_iid, rel_t, rel_p, rel_order FROM rel_kline_rows
        UNION ALL
        SELECT rel_iid, rel_t, rel_p, rel_order FROM rel_projected_rows
    )
    GROUP BY rel_iid, rel_t
),
rel_series AS (
    SELECT rel_iid, arraySort(rel_x -> rel_x.1, groupArray(tuple(rel_t, rel_p))) AS rel_points,
        min(rel_order) AS rel_order
    FROM rel_price_points GROUP BY rel_iid
),
rel_i -> if(arrayExists(rel_x -> rel_x.1 = rel_i, rel_projected),
    JSONExtractString(arrayLast(rel_x -> rel_x.1 = rel_i, rel_projected).2, 'token_id'),
    coalesce(nullIf(JSONExtractString(arrayLast(rel_x -> JSONExtractString(rel_x, 'instrument_id') = rel_i
        AND JSONExtractString(rel_x, 'token_id') != '', rel_klines), 'token_id'), ''),
        JSONExtractString(arrayLast(rel_x -> JSONExtractString(rel_x, 'instrument_id') = rel_i,
            rel_instruments), 'token_id'))) AS rel_identity,
rel_id -> arrayFirst(rel_x -> JSONExtractString(rel_x, 'token_id') = rel_id
    AND (JSONExtractString(rel_x, 'base_asset') != '' OR JSONExtractString(rel_x, 'base_symbol') != ''),
    rel_instruments) AS rel_symbol_instrument,
rel_id -> coalesce(nullIf(upperUTF8(JSONExtractString(arrayLast(rel_x ->
    JSONExtractString(rel_x, 'token_id') = rel_id AND JSONExtractString(rel_x, 'symbol') != '',
    rel_tokens), 'symbol')), ''),
    nullIf(upperUTF8(if(JSONHas(rel_symbol_instrument(rel_id), 'base_asset'),
        JSONExtractString(rel_symbol_instrument(rel_id), 'base_asset'),
        JSONExtractString(rel_symbol_instrument(rel_id), 'base_symbol'))), ''),
    upperUTF8(arrayElement(splitByChar(':', rel_id), -1))) AS rel_symbol,
(rel_i, rel_n) -> if(JSONHas(JSONExtractRaw(rel_snapshot, 'series_coverage_counts'), rel_i),
    JSONExtractFloat(JSONExtractRaw(rel_snapshot, 'series_coverage_counts'), rel_i),
    if(JSONHas(arrayLast(rel_x -> rel_x.1 = rel_i, rel_projected).2, 'coverage_count'),
        JSONExtractFloat(arrayLast(rel_x -> rel_x.1 = rel_i, rel_projected).2, 'coverage_count'), toFloat64(rel_n))) AS rel_count,
(SELECT arrayMap(rel_x -> tuple(rel_x.1, rel_identity(rel_x.1), rel_x.2, rel_count(rel_x.1, length(rel_x.2))),
    arraySort(rel_x -> rel_x.3, groupArray(tuple(rel_iid, rel_points, rel_order)))) FROM rel_series) AS rel_groups,
rel_id -> arrayFirst(rel_x -> rel_x.2 = rel_id AND rel_x.4 = arrayMax(arrayMap(rel_g -> rel_g.4,
    arrayFilter(rel_g -> rel_g.2 = rel_id, rel_groups))), rel_groups) AS rel_peer,
rel_sym -> arrayFirst(rel_x -> rel_symbol(rel_x.2) = rel_sym AND rel_x.4 = arrayMax(arrayMap(rel_g -> rel_g.4,
    arrayFilter(rel_g -> rel_symbol(rel_g.2) = rel_sym, rel_groups))), rel_groups) AS rel_benchmark,
rel_benchmark('BTC') AS rel_btc_group,
rel_benchmark('ETH') AS rel_eth_group,
rel_btc_group.3 AS rel_btc,
rel_eth_group.3 AS rel_eth,
rel_symbol(rel_token) = 'BTC' AS rel_is_benchmark,
arrayFilter(rel_x -> rel_x.2 = rel_token, rel_groups) AS rel_candidates,
arrayConcat(arrayReverse(JSONExtractArrayRaw(rel_snapshot, 'flow_segments')), arrayFlatten(arrayMap(rel_bar ->
    arrayMap(rel_piece -> JSONMergePatch(rel_piece, toJSONString(map('instrument_id',
        JSONExtractString(rel_bar, 'instrument_id'), 'token_id', JSONExtractString(rel_bar, 'token_id'),
        'venue', rel_venue(rel_bar), 'market_type', rel_market(rel_bar)))),
        JSONExtractArrayRaw(rel_bar, 'boundary_segments')), rel_flow))) AS rel_segment_inputs,
arrayMap(rel_key -> arrayFirst(rel_r -> tuple(JSONExtractString(rel_r, 'instrument_id'),
    JSONExtractString(rel_r, 'start'), JSONExtractString(rel_r, 'end')) = rel_key, rel_segment_inputs),
    arrayDistinct(arrayMap(rel_r -> tuple(JSONExtractString(rel_r, 'instrument_id'),
        JSONExtractString(rel_r, 'start'), JSONExtractString(rel_r, 'end')), rel_segment_inputs))) AS rel_segments,
(rel_i, rel_l, rel_z) -> arrayFilter(rel_r -> JSONExtractString(rel_r, 'instrument_id') = rel_i
    AND rel_complete(rel_r) AND rel_field_ts(rel_r, 'start') >= rel_l
    AND rel_field_ts(rel_r, 'start') < rel_field_ts(rel_r, 'end')
    AND rel_field_ts(rel_r, 'end') <= rel_z AND rel_observed_available(rel_r), rel_segments) AS rel_segment_choices,
(rel_i, rel_l, rel_z) -> arrayFold((rel_state, rel_unused) ->
    if(rel_state.1 >= rel_z OR NOT arrayExists(rel_r -> rel_field_ts(rel_r, 'start') = rel_state.1,
        rel_segment_choices(rel_i, rel_l, rel_z)), rel_state,
        tuple(assumeNotNull(rel_ts(JSONExtractString(arrayFirst(rel_r ->
            rel_field_ts(rel_r, 'start') = rel_state.1,
            arrayReverseSort(rel_r -> rel_field_ts(rel_r, 'end'),
                rel_segment_choices(rel_i, rel_l, rel_z))), 'end'))),
            arrayPushBack(rel_state.2, arrayFirst(rel_r -> rel_field_ts(rel_r, 'start') = rel_state.1,
                arrayReverseSort(rel_r -> rel_field_ts(rel_r, 'end'),
                    rel_segment_choices(rel_i, rel_l, rel_z)))))),
    range(length(rel_segment_choices(rel_i, rel_l, rel_z)) + 1),
    tuple(assumeNotNull(rel_l), CAST([], 'Array(String)'))) AS rel_segment_walk,
rel_end - 86400000000 AS rel_coverage_start,
intDiv(assumeNotNull(rel_coverage_start), 60000000) * 60000000 AS rel_floor_start,
intDiv(assumeNotNull(rel_end), 60000000) * 60000000 AS rel_floor_end,
rel_floor_start + if(rel_floor_start < rel_coverage_start, 60000000, 0) AS rel_ceil_start,
arrayFilter(rel_x -> rel_x.2 > rel_x.1,
    [tuple(assumeNotNull(rel_coverage_start), least(rel_ceil_start, assumeNotNull(rel_end))),
     tuple(greatest(rel_floor_end, rel_ceil_start), assumeNotNull(rel_end))]) AS rel_partial,
rel_i -> arrayFilter(rel_r -> JSONExtractString(rel_r, 'instrument_id') = rel_i AND rel_market(rel_r) = 'SPOT'
    AND rel_time(rel_r) < rel_end AND rel_time(rel_r) + 60000000 > rel_coverage_start, rel_flow) AS rel_overlapping,
(rel_i, rel_v) -> arrayFilter(rel_r -> rel_venue(rel_r) = rel_v AND rel_time(rel_r) >= rel_coverage_start
    AND rel_time(rel_r) + 60000000 <= rel_end, rel_overlapping(rel_i)) AS rel_whole_bars,
rel_i -> arraySort(arrayDistinct(arrayConcat(arrayMap(rel_r -> rel_venue(rel_r), rel_overlapping(rel_i)),
    arrayMap(rel_r -> rel_venue(rel_r), arrayFilter(rel_r -> rel_market(rel_r) = 'SPOT',
        arraySlice(arrayFilter(rel_r -> JSONExtractString(rel_r, 'instrument_id') = rel_i, rel_instruments), 1, 1))),
    if(NOT arrayExists(rel_r -> JSONExtractString(rel_r, 'instrument_id') = rel_i, rel_instruments),
        [arrayElement(splitByChar(':', rel_i), 1)], [])))) AS rel_registered,
(rel_i, rel_v) -> arrayExists(rel_x -> rel_segment_walk(rel_i, rel_x.1, rel_x.2).1 != rel_x.2, rel_partial) AS rel_boundary_failed,
(rel_i, rel_v) -> arrayConcat(rel_whole_bars(rel_i, rel_v), arrayFlatten(arrayMap(rel_x ->
    rel_segment_walk(rel_i, rel_x.1, rel_x.2).2, rel_partial))) AS rel_covered_bars,
(rel_i, rel_v) -> length(arrayDistinct(arrayMap(rel_r -> rel_time(rel_r), rel_whole_bars(rel_i, rel_v))))
    < greatest(0, intDiv(rel_floor_end - rel_ceil_start, 60000000))
    OR NOT arrayAll(rel_r -> rel_complete(rel_r), rel_covered_bars(rel_i, rel_v))
    OR rel_boundary_failed(rel_i, rel_v) AS rel_incomplete,
(rel_i, rel_v) -> arrayFilter(rel_k -> arrayExists(rel_r -> JSONExtractRaw(rel_r, rel_k) IN ('', 'null'),
    rel_covered_bars(rel_i, rel_v)), ['buy_qty', 'sell_qty', 'sum_sq_qty']) AS rel_missing_metrics,
(rel_i, rel_v) -> empty(rel_covered_bars(rel_i, rel_v)) OR arrayExists(rel_k ->
    arrayExists(rel_r -> isNull(rel_decimal(JSONExtractRaw(rel_r, rel_k))), rel_covered_bars(rel_i, rel_v)),
    ['buy_qty', 'sell_qty', 'sum_sq_qty']) AS rel_aggregate_missing,
(rel_i, rel_v) -> if(rel_incomplete(rel_i, rel_v) OR rel_aggregate_missing(rel_i, rel_v),
    CAST(NULL, 'Nullable(Decimal256(24))'), arraySum(arrayMap(rel_r ->
        assumeNotNull(rel_decimal(JSONExtractRaw(rel_r, 'buy_qty'))) +
        assumeNotNull(rel_decimal(JSONExtractRaw(rel_r, 'sell_qty'))), rel_covered_bars(rel_i, rel_v)))) AS rel_volume,
(rel_i, rel_v) -> arraySort(arrayDistinct(arrayFilter(rel_s -> rel_s != '', arrayMap(rel_r ->
    coalesce(nullIf(JSONExtractString(rel_r, 'gap_reason'), ''), JSONExtractString(rel_r, 'coverage_reason')),
    arrayFilter(rel_r -> JSONExtractString(rel_r, 'instrument_id') = rel_i,
        arrayConcat(rel_whole_bars(rel_i, rel_v), rel_segments)))))) AS rel_source_reasons,
(rel_p, rel_t) -> arrayLastIndex(rel_x -> rel_x.1 <= rel_t, rel_p) AS rel_at_index,
(rel_p, rel_t) -> if(rel_at_index(rel_p, rel_t) = 0, CAST(NULL, 'Nullable(Float64)'),
    arrayElement(rel_p, rel_at_index(rel_p, rel_t)).2) AS rel_price,
(rel_p, rel_t) -> if(rel_at_index(rel_p, rel_t) = 0 OR
    rel_t - arrayElement(rel_p, rel_at_index(rel_p, rel_t)).1 > greatest(60000000,
        if(rel_at_index(rel_p, rel_t) > 1,
           arrayElement(rel_p, rel_at_index(rel_p, rel_t)).1 - arrayElement(rel_p, rel_at_index(rel_p, rel_t) - 1).1,
           60000000)), CAST(NULL, 'Nullable(Float64)'), rel_price(rel_p, rel_t)) AS rel_fresh,
(rel_p, rel_a, rel_z) -> rel_price(rel_p, rel_z) / nullIf(rel_price(rel_p, rel_a), 0) - 1 AS rel_return,
(rel_p, rel_a, rel_z) -> rel_fresh(rel_p, rel_z) / nullIf(rel_fresh(rel_p, rel_a), 0) - 1 AS rel_bench_return,
(rel_values, rel_q) -> if(empty(rel_values), CAST(NULL, 'Nullable(Float64)'),
    arrayElement(arraySort(rel_values), toUInt64(floor((length(rel_values) - 1) * rel_q)) + 1)
    + (arrayElement(arraySort(rel_values), least(length(rel_values),
        toUInt64(floor((length(rel_values) - 1) * rel_q)) + 2))
        - arrayElement(arraySort(rel_values), toUInt64(floor((length(rel_values) - 1) * rel_q)) + 1))
        * ((length(rel_values) - 1) * rel_q - floor((length(rel_values) - 1) * rel_q))) AS rel_quantile,
rel_values -> NOT empty(rel_values) AND abs(rel_quantile(rel_values, rel_quantile_hi)
    - rel_quantile(rel_values, rel_quantile_lo)) > 1e-12 * greatest(1.,
        abs(rel_quantile(rel_values, rel_quantile_lo)), abs(rel_quantile(rel_values, rel_quantile_hi))) AS rel_nondegenerate,
rel_values -> if(empty(rel_values), 'null', toJSONString([
    rel_quantile(rel_values, rel_quantile_lo), rel_quantile(rel_values, rel_quantile_hi)])) AS rel_normal_json,
rel_values -> if(rel_nondegenerate(rel_values), rel_normal_json(rel_values), 'null') AS rel_float_normal_json,
rel_values -> if(empty(rel_values), 'INCOMPLETE', if(rel_nondegenerate(rel_values), NULL, 'DEGENERATE_HISTORY')) AS rel_range_reason,
(rel_value, rel_values) -> if(isNull(rel_value) OR NOT rel_nondegenerate(rel_values), 'null',
    if(rel_value < rel_quantile(rel_values, rel_quantile_lo) OR rel_value > rel_quantile(rel_values, rel_quantile_hi),
        'true', 'false')) AS rel_beyond_json,
(rel_value, rel_values) -> if(isNull(rel_value) OR empty(rel_values), CAST(NULL, 'Nullable(Float64)'),
    countEqual(arrayMap(rel_x -> rel_x <= rel_value, rel_values), 1) / length(rel_values)) AS rel_percent_rank,
rel_p -> arrayMap(rel_x -> tuple(rel_x.1, log(rel_x.2 / assumeNotNull(rel_price(rel_p, rel_x.1 - 3600000000)))),
    arrayFilter(rel_x -> modulo(rel_x.1, 3600000000) < 1000000 AND rel_x.1 < rel_start
        AND rel_x.1 >= rel_start - rel_lookback * 86400000000
        AND arrayExists(rel_y -> rel_y.1 = rel_x.1 - 3600000000, rel_p), rel_p)) AS rel_hourly,
(rel_y, rel_x) -> arrayMap(rel_pair -> tuple(rel_pair.2, arrayFirst(rel_other -> rel_other.1 = rel_pair.1, rel_y).2),
    arrayFilter(rel_pair -> arrayExists(rel_other -> rel_other.1 = rel_pair.1, rel_y), rel_x)) AS rel_pairs,
rel_pairs_arg -> if(length(rel_pairs_arg) < toInt64(rel_min_days) * 24, tuple(1., CAST('INSUFFICIENT_HISTORY', 'Nullable(String)')),
    if(arraySum(arrayMap(rel_pair -> pow(rel_pair.1 - arrayAvg(arrayMap(rel_q -> rel_q.1, rel_pairs_arg)), 2), rel_pairs_arg)) = 0,
        tuple(1., CAST('NOT_ESTIMABLE', 'Nullable(String)')),
        tuple(rel_beta_weight * arraySum(arrayMap(rel_pair ->
            (rel_pair.1 - arrayAvg(arrayMap(rel_q -> rel_q.1, rel_pairs_arg))) *
            (rel_pair.2 - arrayAvg(arrayMap(rel_q -> rel_q.2, rel_pairs_arg))), rel_pairs_arg)) /
            arraySum(arrayMap(rel_pair -> pow(rel_pair.1 - arrayAvg(arrayMap(rel_q -> rel_q.1, rel_pairs_arg)), 2), rel_pairs_arg))
            + (1 - rel_beta_weight), CAST(NULL, 'Nullable(String)')))) AS rel_beta,
rel_p -> if(length(rel_p) < 2, CAST(NULL, 'Nullable(Float64)'),
    sqrt(arraySum(arrayMap(rel_x -> pow(rel_x - arrayAvg(rel_p), 2), rel_p)) / (length(rel_p) - 1))) AS rel_stdev,
rel_id -> coalesce(nullIf(JSONExtractString(arrayLast(rel_b -> JSONExtractString(rel_b, 'weighting') = 'EQUAL'
    AND has(JSONExtract(rel_b, 'constituents', 'Array(String)'), rel_id), rel_baskets), 'sector'), ''),
    nullIf(JSONExtractString(arrayLast(rel_t -> JSONExtractString(rel_t, 'token_id') = rel_id
        AND JSONExtractString(rel_t, 'sector') != '', rel_tokens), 'sector'), '')) AS rel_sector_for,
rel_sector_for(rel_token) AS rel_sector,
rel_sec -> if(arrayExists(rel_b -> JSONExtractString(rel_b, 'sector') = rel_sec
    AND JSONExtractString(rel_b, 'weighting') = 'EQUAL', rel_baskets),
    JSONExtract(arrayLast(rel_b -> JSONExtractString(rel_b, 'sector') = rel_sec
        AND JSONExtractString(rel_b, 'weighting') = 'EQUAL', rel_baskets), 'constituents', 'Array(String)'),
    arrayMap(rel_t -> JSONExtractString(rel_t, 'token_id'), arrayFilter(rel_t ->
        JSONExtractString(rel_t, 'sector') = rel_sec AND JSONExtractString(rel_t, 'token_id') != '', rel_tokens))) AS rel_members,
arrayFilter(rel_id -> rel_id != rel_token AND NOT empty(rel_peer(rel_id).3), rel_members(ifNull(rel_sector, ''))) AS rel_direct_peers,
if(length(rel_direct_peers) >= 2, rel_direct_peers,
    arrayFilter(rel_id -> rel_id != rel_token AND NOT empty(rel_peer(rel_id).3),
        arrayFlatten(arrayMap(rel_sec -> rel_members(rel_sec),
        ['DEX_EXECUTION', 'LENDING_CREDIT', 'PERP_DEX', 'RWA', 'INTEROPERABILITY', 'SYNTHETIC_DOLLAR'])))) AS rel_tradable,
rel_length -> arrayFilter(rel_x -> isNotNull(rel_x), arrayMap(rel_id ->
    rel_bench_return(rel_peer(rel_id).3, rel_end - rel_length * 1000000, rel_end), rel_tradable)) AS rel_peer_returns,
arrayConcat(JSONExtractArrayRaw(rel_snapshot, 'reference_points'), arrayFlatten(arrayMap(rel_bar ->
    arrayMap(rel_point -> JSONMergePatch(rel_point, toJSONString(map('instrument_id', JSONExtractString(rel_bar, 'instrument_id')))),
        JSONExtractArrayRaw(rel_bar, 'reference_points')), rel_flow))) AS rel_explicit_references,
(rel_i, rel_boundary) -> arrayConcat(rel_explicit_references,
    arrayMap(rel_bar -> concat('{"instrument_id":', toJSONString(rel_i), ',"boundary":', toJSONString(rel_iso(rel_boundary)),
        ',"complete":true,"last_trade":', if(JSONHas(rel_bar, 'last_trade'), JSONExtractRaw(rel_bar, 'last_trade'), 'null'), '}'),
    arrayFilter(rel_bar -> JSONExtractString(rel_bar, 'instrument_id') = rel_i AND rel_complete(rel_bar)
        AND rel_time(rel_bar) + 60000000 = rel_boundary, rel_flow))) AS rel_reference_inputs,
(rel_i, rel_boundary) -> arrayMap(rel_point -> JSONExtractRaw(rel_point, 'last_trade'),
    arrayFilter(rel_point -> JSONExtractString(rel_point, 'instrument_id') = rel_i
        AND rel_field_ts(rel_point, 'boundary') = rel_boundary AND rel_complete(rel_point)
        AND rel_observed_available(rel_point) AND isNotNull(rel_time(JSONExtractRaw(rel_point, 'last_trade')))
        AND rel_time(JSONExtractRaw(rel_point, 'last_trade')) < rel_boundary
        AND rel_available(JSONExtractRaw(rel_point, 'last_trade'))
        AND isNotNull(rel_decimal(JSONExtractRaw(JSONExtractRaw(rel_point, 'last_trade'), 'price'))),
        rel_reference_inputs(rel_i, rel_boundary))) AS rel_reference_trades,
(rel_i, rel_boundary) -> arrayFirst(rel_x -> 1,
    arrayFilter(rel_x -> rel_time(rel_x) = arrayMax(arrayMap(rel_r -> rel_time(rel_r),
        rel_reference_trades(rel_i, rel_boundary))), rel_reference_trades(rel_i, rel_boundary))) AS rel_reference,
(SELECT groupArray(tuple(rel_iid, assumeNotNull(rel_t), rel_exact_p, rel_order)) FROM rel_kline_rows) AS rel_exact_rows,
rel_i -> arrayFilter(rel_x -> rel_x.1 = rel_i, rel_exact_rows) AS rel_exact_for,
rel_i -> arrayFilter(rel_x -> rel_x.2 = arrayMax(arrayMap(rel_y -> rel_y.2, rel_exact_for(rel_i)))
    OR rel_x.2 = arrayMax(arrayMap(rel_y -> rel_y.2, arrayFilter(rel_y -> modulo(rel_y.2, 3600000000) < 1000000,
        rel_exact_for(rel_i)))), rel_exact_for(rel_i)) AS rel_exact_references,
(rel_i, rel_bound) -> arrayFirst(rel_x -> rel_x.2 <= rel_bound,
    arrayReverseSort(rel_x -> tuple(rel_x.2, rel_x.4), rel_exact_references(rel_i))) AS rel_exact_at,
rel_direction -> if(rel_direction IN ('"BEARISH"', '"LOSER"', '-1'), -1, 1) AS rel_direction_sign,
rel_direction_sign(JSONExtractRaw(rel_snapshot, 'expected_direction')) AS rel_sign
"""


def _coverage_ctes():
    boundary = _object(
        start=_j("rel_iso(rel_part.1)"),
        cutoff=_j("rel_iso(rel_part.2)"),
        trade_count=_j(
            "arraySum(arrayMap(rel_r -> JSONExtractInt(rel_r, 'n_buy') + "
            "JSONExtractInt(rel_r, 'n_sell'), rel_segment_walk(rel_i, rel_part.1, rel_part.2).2))"
        ),
        aggregates="concat('[', arrayStringConcat(rel_segment_walk(rel_i, rel_part.1, rel_part.2).2, ','), ']')",
    )
    excluded_coverage = _object(
        venue=_j("rel_v"),
        reason="'\"INCOMPLETE\"'",
        detail=_j(
            "if(rel_boundary_failed(rel_i, rel_v), 'BOUNDARY_AGGREGATE_UNAVAILABLE', "
            "'FULL_MINUTE_COVERAGE_INCOMPLETE')"
        ),
        source_reasons=_j("rel_source_reasons(rel_i, rel_v)"),
    )
    excluded_metrics = _object(
        venue=_j("rel_v"),
        reason="'\"INCOMPLETE\"'",
        detail="'\"FLOW_AGGREGATE_FIELDS_MISSING\"'",
        missing_metrics=_j("rel_missing_metrics(rel_i, rel_v)"),
    )
    coverage = _object(
        complete=_bool("NOT empty(rel_observed_venues(rel_i))"),
        expected_duration_s="'86400'",
        boundary_segments=("if(empty(rel_observed_venues(rel_i)), '[]', rel_boundary_json(rel_i))"),
        excluded=(
            "concat('[', arrayStringConcat(arrayMap(rel_v -> "
            f"if(rel_incomplete(rel_i, rel_v), {excluded_coverage}, {excluded_metrics}), "
            "arrayFilter(rel_v -> isNull(rel_volume(rel_i, rel_v)), rel_registered(rel_i))), ','), ']')"
        ),
        volume_unit="'\"NATIVE_TOKEN\"'",
        covered_volume_native=_money("rel_observed_volume(rel_i)"),
    )
    return f"""
rel_i -> concat('[', arrayStringConcat(arrayMap(rel_part -> {boundary}, rel_partial), ','), ']') AS rel_boundary_json,
rel_i -> arrayFilter(rel_v -> isNotNull(rel_volume(rel_i, rel_v)), rel_registered(rel_i)) AS rel_observed_venues,
rel_i -> if(empty(rel_observed_venues(rel_i)), CAST(NULL, 'Nullable(Decimal256(24))'),
    rel_volume(rel_i, arrayElement(rel_observed_venues(rel_i), 1))) AS rel_observed_volume,
rel_i -> {coverage} AS rel_coverage_json,
arrayMap(rel_group -> tuple(rel_group.1, rel_observed_volume(rel_group.1), rel_coverage_json(rel_group.1)), rel_candidates) AS rel_coverage,
arrayFilter(rel_x -> isNotNull(rel_x.2), rel_coverage) AS rel_complete_candidates,
if(empty(rel_complete_candidates), CAST(NULL, 'Nullable(String)'),
    arrayFirst(rel_x -> rel_x.2 = arrayMax(arrayMap(rel_r -> rel_r.2, rel_complete_candidates)),
        rel_complete_candidates).1) AS rel_ref,
arrayFirst(rel_x -> rel_x.1 = rel_ref, rel_groups).3 AS rel_prices,
rel_reference(ifNull(rel_ref, ''), rel_start) AS rel_anchor_row,
rel_reference(ifNull(rel_ref, ''), rel_end) AS rel_cutoff_row,
rel_decimal(JSONExtractRaw(rel_anchor_row, 'price')) AS rel_anchor_price,
rel_beta(rel_pairs(rel_hourly(rel_prices), rel_hourly(rel_btc))) AS rel_beta_btc,
rel_beta(rel_pairs(rel_hourly(rel_prices), rel_hourly(rel_eth))) AS rel_beta_eth,
arrayFilter(rel_x -> rel_x.1 < rel_start, rel_prices) AS rel_historical,
arrayMap(rel_i -> (arrayElement(rel_historical, rel_i + 1).1 - arrayElement(rel_historical, rel_i).1) / 1000000.,
    range(toUInt64(1), toUInt64(length(rel_historical)))) AS rel_history_deltas,
if(empty(rel_history_deltas), CAST(NULL, 'Nullable(Int64)'), toInt64(rel_quantile(rel_history_deltas, 0.5))) AS rel_resolution,
NOT empty(rel_historical) AND (arrayElement(rel_historical, -1).1 - arrayElement(rel_historical, 1).1)
    >= rel_min_days * 86400000000 AS rel_history_admitted,
rel_length -> arrayMap(rel_x -> tuple(rel_x,
    arrayLast(rel_y -> rel_y.1 <= rel_x.1 - rel_length * 1000000, rel_historical)), rel_historical) AS rel_history_pairs,
rel_length -> arrayFilter(rel_pair -> rel_history_admitted
    AND arrayExists(rel_y -> rel_y.1 <= rel_pair.1.1 - rel_length * 1000000, rel_historical)
    AND rel_pair.2.1 >= rel_start - rel_lookback * 86400000000
    AND (rel_pair.1.1 - rel_length * 1000000 - rel_pair.2.1) / 1000000 < rel_resolution
    AND (rel_is_benchmark OR (arrayExists(rel_x -> rel_x.1 = rel_pair.1.1, rel_btc)
        AND arrayExists(rel_x -> rel_x.1 = rel_pair.2.1, rel_btc))), rel_history_pairs(rel_length)) AS rel_history_selected,
rel_length -> arrayMap(rel_pair -> tuple(rel_pair.1.2 / rel_pair.2.2 - 1 - if(rel_is_benchmark, 0.,
    rel_beta_btc.1 * (assumeNotNull(rel_price(rel_btc, rel_pair.1.1)) /
    assumeNotNull(rel_price(rel_btc, rel_pair.2.1)) - 1)),
    (rel_pair.1.1 - rel_pair.2.1) / 1000000.,
    (rel_pair.1.1 - rel_length * 1000000 - rel_pair.2.1) / 1000000., rel_pair.1.1),
    rel_history_selected(rel_length)) AS rel_history_rows,
rel_length -> arrayMap(rel_x -> rel_x.1, rel_history_rows(rel_length)) AS rel_hist,
rel_length -> if(rel_length = (rel_end - rel_start) / 1000000.,
    if(isNull(rel_anchor_price) OR rel_anchor_price = 0 OR (rel_cutoff_row = '' AND isNull(rel_price(rel_prices, rel_end))),
        CAST(NULL, 'Nullable(Float64)'),
        toFloat64(divideDecimal(assumeNotNull(if(rel_cutoff_row != '',
            rel_decimal(if(JSONHas(rel_cutoff_row, 'price'), JSONExtractRaw(rel_cutoff_row, 'price'),
                JSONExtractRaw(rel_cutoff_row, 'p'))), rel_exact_at(ifNull(rel_ref, ''), rel_end).3)),
            ifNull(nullIf(rel_anchor_price, 0), toDecimal256(1, 24)), 24) - toDecimal256(1, 24))),
    rel_return(rel_prices, rel_end - rel_length * 1000000, rel_end)) AS rel_raw_event_or_window,
(rel_length, rel_event) -> if(rel_event, rel_raw_event_or_window((rel_end - rel_start) / 1000000.),
    rel_return(rel_prices, rel_end - rel_length * 1000000, rel_end)) AS rel_raw,
(rel_length, rel_event) -> if(rel_is_benchmark, rel_raw(rel_length, rel_event),
    rel_raw(rel_length, rel_event) - rel_beta_btc.1 * rel_bench_return(rel_btc, rel_end - rel_length * 1000000, rel_end)) AS rel_measure,
(rel_length, rel_event) -> if(isNull(rel_sector) OR rel_sector = 'IDENTITY_PAYMENTS' OR length(rel_peer_returns(rel_length)) < 2,
    CAST(NULL, 'Nullable(Float64)'), rel_raw(rel_length, rel_event) - arrayAvg(rel_peer_returns(rel_length))) AS rel_sector_return,
arrayFilter(rel_x -> modulo(rel_x.1, 3600000000) = 0, rel_btc) AS rel_btc_hourly,
rel_t -> arrayMap(rel_x -> log(rel_x.2 / assumeNotNull(rel_price(rel_btc_hourly, rel_x.1 - 3600000000))),
    arrayFilter(rel_x -> rel_x.1 > rel_t - 86400000000 AND rel_x.1 <= rel_t
        AND arrayExists(rel_y -> rel_y.1 = rel_x.1 - 3600000000, rel_btc_hourly), rel_btc_hourly)) AS rel_btc_lr,
arrayFilter(rel_x -> rel_x.1 < rel_start AND rel_x.1 >= rel_start - rel_lookback * 86400000000, rel_btc_hourly) AS rel_btc_baseline,
NOT empty(rel_btc) AND (arrayElement(rel_btc, -1).1 - arrayElement(rel_btc, 1).1)
    >= rel_min_days * 86400000000 AS rel_btc_history_admitted,
if(rel_btc_history_admitted,
    arrayMap(rel_x -> rel_x.2 / assumeNotNull(rel_price(rel_btc, rel_x.1 - 86400000000)) - 1,
        arrayFilter(rel_x -> arrayExists(rel_y -> rel_y.1 = rel_x.1 - 86400000000, rel_btc), rel_btc_baseline)),
    CAST([], 'Array(Float64)')) AS rel_btc_return_history,
if(rel_btc_history_admitted, arrayMap(rel_x -> assumeNotNull(rel_stdev(rel_btc_lr(rel_x.1))),
    arrayFilter(rel_x -> length(rel_btc_lr(rel_x.1)) = 24, rel_btc_baseline)), CAST([], 'Array(Float64)')) AS rel_btc_rv_history,
intDiv(assumeNotNull(rel_end), 3600000000) * 3600000000 AS rel_rv_cutoff,
if(length(rel_btc_lr(rel_rv_cutoff)) = 24, rel_stdev(rel_btc_lr(rel_rv_cutoff)), CAST(NULL, 'Nullable(Float64)')) AS rel_rv,
rel_bench_return(rel_btc, rel_end - 86400000000, rel_end) AS rel_btc_day_return,
(rel_r, rel_vol, rel_returns, rel_vols) ->
    if(isNull(rel_r) OR isNull(rel_vol) OR empty(rel_returns) OR empty(rel_vols),
    CAST(NULL, 'Nullable(String)'), if(rel_r < rel_quantile(rel_returns, rel_quantile_lo)
        OR rel_vol > rel_quantile(rel_vols, rel_quantile_hi), 'STRESS',
        if(rel_r < rel_quantile(rel_returns, 0.25), 'HEADWIND', 'NEUTRAL'))) AS rel_market_context_for,
rel_market_context_for(rel_btc_day_return, rel_rv, rel_btc_return_history, rel_btc_rv_history) AS rel_context,
if(rel_is_benchmark, rel_return(rel_prices, rel_start - 86400000000, rel_start),
    rel_return(rel_prices, rel_start - 86400000000, rel_start) - rel_beta_btc.1 *
    rel_bench_return(rel_btc, rel_start - 86400000000, rel_start)) AS rel_pre_runup,
rel_measure((rel_end - rel_start) / 1000000., 1) * rel_sign AS rel_signed_event,
arrayMap(rel_x -> rel_x * rel_sign, rel_hist((rel_end - rel_start) / 1000000.)) AS rel_signed_event_history,
arrayMap(rel_x -> rel_x * rel_sign, rel_hist(86400.)) AS rel_signed_day_history,
(rel_value, rel_eh, rel_dh) -> if(isNull(rel_value), CAST(NULL, 'Nullable(String)'),
    if(rel_nondegenerate(rel_dh) AND rel_value > rel_quantile(rel_dh, rel_quantile_hi),
        'HEAVY', if(NOT rel_nondegenerate(rel_eh), NULL,
        if(rel_value < rel_quantile(rel_eh, rel_quantile_lo), 'NEGATIVE',
        if(rel_value > rel_quantile(rel_eh, rel_quantile_hi), 'SUBSTANTIAL',
        if(rel_value > rel_quantile(rel_eh, 0.75), 'EARLY', 'UNPRICED')))))) AS rel_repricing_for,
rel_repricing_for(rel_signed_event, rel_signed_event_history, rel_signed_day_history) AS rel_repricing,
rel_reference(rel_btc_group.1, rel_end) AS rel_btc_last,
if(rel_btc_last != '', rel_decimal(JSONExtractRaw(rel_btc_last, 'price')),
    rel_exact_at(rel_btc_group.1, rel_end).3) AS rel_btc_entry
"""


def _history_json(length, *, incomplete=False):
    rows = f"rel_history_rows({length})"
    diagnostic = _object(
        target_window_duration_s=_j(length),
        observed_duration_bounds_s=(
            f"if(empty({rows}), 'null', toJSONString(["
            f"arrayMin(arrayMap(rel_x -> rel_x.2, {rows})), "
            f"arrayMax(arrayMap(rel_x -> rel_x.2, {rows}))]))"
        ),
        endpoint_staleness_bounds_s=(
            f"if(empty({rows}), 'null', toJSONString(["
            f"arrayMin(arrayMap(rel_x -> rel_x.3, {rows})), "
            f"arrayMax(arrayMap(rel_x -> rel_x.3, {rows}))]))"
        ),
        exact=_bool(f"NOT empty({rows}) AND arrayMax(arrayMap(rel_x -> rel_x.3, {rows})) = 0"),
        resolution_s=_j("rel_resolution"),
        alignment="'\"AS_OF_AT_EXACT_TARGET_BOUNDS\"'",
        sample_count=_j(f"length({rows})"),
        latest_observed_at=_j(
            f"if(empty({rows}), NULL, rel_iso(arrayMax(arrayMap(rel_x -> rel_x.4, {rows}))))"
        ),
    )
    missing = (
        _object(
            exact="'false'",
            reason="'\"INCOMPLETE\"'",
            target_window_duration_s=_j(length),
        )
        if incomplete
        else "'{}'"
    )
    return f"if(rel_history_admitted, {diagnostic}, {missing})"


def _window_json():
    length = "rel_w.2"
    event = "(rel_w.1 = 'EVENT')"
    raw = f"rel_raw({length}, {event})"
    adj = (
        f"if(rel_is_benchmark, CAST(NULL, 'Nullable(Float64)'), {raw} - rel_beta_btc.1 * "
        f"rel_bench_return(rel_btc, rel_end - {length} * 1000000, rel_end))"
    )
    hist = f"rel_hist({length})"
    measure = f"rel_measure({length}, {event})"
    return _object(
        window=_j("rel_w.1"),
        start=_j(f"rel_iso(rel_end - toInt64({length} * 1000000))"),
        cutoff=_j("rel_iso(rel_end)"),
        observed_at=_j("if(empty(rel_prices), NULL, rel_iso(arrayElement(rel_prices, -1).1))"),
        precision=_j(
            "if(empty(rel_prices), NULL, concat('BUCKET(', "
            "toString(if(JSONHas(arrayLast(rel_r -> JSONExtractString(rel_r, 'instrument_id') = rel_ref, rel_klines), 'interval_s'), "
            "JSONExtractInt(arrayLast(rel_r -> JSONExtractString(rel_r, 'instrument_id') = rel_ref, rel_klines), 'interval_s'), 60)), 's)'))"
        ),
        coverage=_object(reference_venue=_j("rel_ref"), complete_24h=_bool("isNotNull(rel_ref)")),
        r_raw=_j(raw),
        r_btc_adj=_j(adj),
        r_btc_adj_reason=_j("if(rel_is_benchmark, 'NOT_APPLICABLE_BENCHMARK', NULL)"),
        relative_measure=_j("if(rel_is_benchmark, 'R_RAW', 'R_BTC_ADJ')"),
        relative_return=_j(measure),
        r_eth_adj=_j(
            f"{raw} - rel_beta_eth.1 * rel_bench_return(rel_eth, rel_end - {length} * 1000000, rel_end)"
        ),
        r_sector_adj=_j(f"rel_sector_return({length}, {event})"),
        sector_reason=_j(
            f"if(rel_sector = 'IDENTITY_PAYMENTS', 'NOT_APPLICABLE', "
            f"if(isNotNull(rel_sector_return({length}, {event})), NULL, 'NOT_ESTIMABLE'))"
        ),
        pctile=_j(f"rel_percent_rank({measure}, {hist})"),
        normal_range=f"rel_float_normal_json({hist})",
        normal_range_reason=_j(f"rel_range_reason({hist})"),
        beyond_normal=f"rel_beyond_json({measure}, {hist})",
        historical_window_resolution_s=_j("rel_resolution"),
        historical_window_duration_s=_j(length),
        requested_window_duration_s=_j(length),
        historical_alignment="'\"AS_OF_AT_EXACT_TARGET_BOUNDS\"'",
        historical_measurement=_history_json(length, incomplete=True),
    )


def _result_ctes():
    horizon = _object(
        value="rel_float_normal_json(rel_hist(rel_h.2))",
        unit="'\"DECIMAL_RETURN\"'",
        window=_j("rel_h.1"),
        start=_j("rel_iso(rel_start - toInt64(rel_lookback * 86400000000))"),
        cutoff=_j("rel_iso(rel_start)"),
        observed_at=_j(
            "if(empty(rel_history_rows(rel_h.2)), NULL, "
            "rel_iso(arrayMax(arrayMap(rel_x -> rel_x.4, rel_history_rows(rel_h.2)))))"
        ),
        precision=_j(
            "if(isNull(rel_resolution) OR rel_resolution = 0, NULL, concat('BUCKET(', toString(rel_resolution), 's)'))"
        ),
        coverage=_object(sample_count=_j("length(rel_hist(rel_h.2))")),
        reason=_j("if(empty(rel_hist(rel_h.2)), 'NOT_ESTIMABLE', NULL)"),
        historical_measurement=_history_json("rel_h.2"),
    )
    outcome_btc = _object(
        instrument_id=_j("nullIf(rel_btc_group.1, '')"),
        entry_ref_price=_money("rel_btc_entry"),
        observed_at=_j(
            "if(rel_btc_last != '', rel_iso(rel_time(rel_btc_last)), "
            "if(empty(rel_btc), NULL, rel_iso(arrayElement(rel_btc, -1).1)))"
        ),
        price_source=_j("if(rel_btc_last != '', 'LAST_TRADE', if(empty(rel_btc), NULL, 'KLINE_CLOSE'))"),
        reason=_j("if(isNull(rel_btc_entry), 'INCOMPLETE', NULL)"),
    )
    anchor = _object(
        type=_j(
            "if(JSONType(JSONExtractRaw(rel_snapshot, 'anchor')) = 'Object' "
            "AND JSONHas(JSONExtractRaw(rel_snapshot, 'anchor'), 'type'), "
            "JSONExtractString(rel_snapshot, 'anchor', 'type'), 'KNOWN_TO_US_AT')"
        ),
        ts=_j("rel_iso(rel_start)"),
        ref_price=_money("rel_anchor_price"),
        price_observed_at=_j("rel_iso(rel_time(rel_anchor_row))"),
        evidence=(
            "if(rel_anchor_row = '', 'null', JSONMergePatch(rel_anchor_row, "
            "toJSONString(map('instrument_id', ifNull(rel_ref, '')))))"
        ),
        price_reason=_j("if(rel_anchor_row = '', 'INCOMPLETE', NULL)"),
        price_source=_j("if(rel_anchor_row = '', NULL, 'LAST_TRADE')"),
    )
    pre = _object(
        window="'\"PRE_EVENT_W24H\"'",
        start=_j("rel_iso(rel_start - 86400000000)"),
        cutoff=_j("rel_iso(rel_start)"),
        ar=_j("rel_pre_runup"),
        pctile=_j("rel_percent_rank(rel_pre_runup, rel_hist(86400.))"),
        flag="rel_beyond_json(rel_pre_runup, rel_hist(86400.))",
    )
    context = _object(
        btc_return_24h=_j("rel_btc_day_return"),
        rv24=_j("rel_rv"),
        rv24_observed_cutoff=_j("rel_iso(rel_rv_cutoff)"),
        rv24_resolution_s="'3600'",
        return_normal_range="rel_normal_json(rel_btc_return_history)",
        return_lower_quartile=_j("rel_quantile(rel_btc_return_history, 0.25)"),
        rv_normal_range="rel_normal_json(rel_btc_rv_history)",
        rule="'\"normal_range\"'",
    )
    result = _object(
        expected_direction="if(JSONHas(rel_snapshot, 'expected_direction'), JSONExtractRaw(rel_snapshot, 'expected_direction'), 'null')",
        repricing_sign=_j("rel_sign"),
        outcome_btc_reference=outcome_btc,
        normal_ranges="rel_horizon_json",
        entry_ref_price=_money(
            "if(rel_cutoff_row != '', rel_decimal(if(JSONHas(rel_cutoff_row, 'price'), "
            "JSONExtractRaw(rel_cutoff_row, 'price'), JSONExtractRaw(rel_cutoff_row, 'p'))), "
            "rel_exact_at(ifNull(rel_ref, ''), rel_end).3)"
        ),
        entry_price_source=_j("if(rel_cutoff_row != '', 'LAST_TRADE', 'KLINE_CLOSE')"),
        entry_price_observed_at=_j(
            "if(rel_cutoff_row != '', rel_iso(rel_time(rel_cutoff_row)), "
            "if(empty(rel_prices), NULL, rel_iso(arrayElement(rel_prices, -1).1)))"
        ),
        close_1h_price=_money(
            "arrayFirst(rel_x -> 1, arrayReverseSort(rel_x -> tuple(rel_x.2, rel_x.4), "
            "arrayFilter(rel_x -> rel_x.2 <= rel_end AND modulo(rel_x.2, 3600000000) < 1000000, "
            "rel_exact_references(ifNull(rel_ref, ''))))).3"
        ),
        ref_venue=_j("rel_ref"),
        reference_coverage=(
            "concat('{', arrayStringConcat(arrayMap(rel_x -> concat(toJSONString(rel_x.1), ':', rel_x.3), rel_coverage), ','), '}')"
        ),
        reference_reason=_j("if(isNull(rel_ref), 'INCOMPLETE', NULL)"),
        anchor=anchor,
        windows="rel_windows_json",
        beta_btc=_j("rel_beta_btc.1"),
        beta_eth=_j("rel_beta_eth.1"),
        beta_flags=_object(btc=_j("rel_beta_btc.2"), eth=_j("rel_beta_eth.2")),
        pre_event_runup=pre,
        repricing_band=_j("rel_repricing"),
        market_context=_j("rel_context"),
        context_inputs=context,
    )
    return f"""
[tuple('W15M', 900.), tuple('W1H', 3600.), tuple('W4H', 14400.), tuple('W24H', 86400.),
    tuple('EVENT', (rel_end - rel_start) / 1000000.)] AS rel_windows,
concat('{{', arrayStringConcat(arrayMap(rel_w -> concat(toJSONString(rel_w.1), ':', {_window_json()}), rel_windows), ','), '}}') AS rel_windows_json,
if(rel_include_outcome, concat('{{', arrayStringConcat(arrayMap(rel_h -> concat(toJSONString(rel_h.1), ':', {horizon}),
    [tuple('W1H', 3600.), tuple('W4H', 14400.), tuple('W12H', 43200.), tuple('W24H', 86400.),
     tuple('W3D', 259200.), tuple('W7D', 604800.)]), ','), '}}'), '{{}}') AS rel_horizon_json,
{result} AS rel_result
"""


def ctes():
    return _CORE + "," + _coverage_ctes() + "," + _result_ctes()


def select_sql():
    return "SELECT rel_result AS result"


def query(snapshot, config=None):
    config = config or {}
    anchor = snapshot.get("anchor")
    start = config.get("start") or (anchor.get("ts") if isinstance(anchor, dict) else None)
    if start is None:
        raise ValueError("relative analytics requires an explicit start or anchor.ts")
    end = config.get("end") or snapshot["cutoff"]
    quantiles = config.get("normal_range_quantiles", [0.025, 0.975])
    params = {
        "rel_snapshot": (
            json.dumps(
                jsonable(_transport(snapshot)), separators=(",", ":"), ensure_ascii=False, allow_nan=False
            ),
            "String",
        ),
        "rel_start": (iso(start), "String"),
        "rel_end": (iso(end), "String"),
        "rel_lookback": (
            config.get("historical_lookback_days", config.get("base_retention_days", 90)),
            "Float64",
        ),
        "rel_min_days": (config.get("history_min_days", 7), "Float64"),
        "rel_beta_weight": (config.get("beta_blume_weight", 0.6666666666666666), "Float64"),
        "rel_quantile_lo": (quantiles[0], "Float64"),
        "rel_quantile_hi": (quantiles[1], "Float64"),
        "rel_include_outcome": (int(config.get("include_outcome", True)), "UInt8"),
    }
    return "WITH " + ctes() + "\n" + select_sql(), params


def benchmark_ctes():
    result = _object(
        r_raw=_j("rel_reaction_return"),
        r_raw_reason=_j("if(isNull(rel_reaction_return), 'INCOMPLETE', NULL)"),
        r_raw_method="'\"BASE_KLINE_CLOSE_LARGEST_SPOT_VENUE\"'",
        r_raw_venue=_j("rel_reaction_venue"),
        price_start=_money("rel_reaction_first.3"),
        price_start_observed_at=_j("if(isNull(rel_reaction_first.3), NULL, rel_iso(rel_reaction_first.2))"),
        price_end=_money("rel_reaction_last.3"),
        price_end_observed_at=_j("if(isNull(rel_reaction_last.3), NULL, rel_iso(rel_reaction_last.2))"),
        not_applicable='\'["r_btc_adj","pctile","spot_cvd","oi","funding","liquidations"]\'',
        reason="'\"NOT_APPLICABLE\"'",
    )
    return (
        _CORE
        + f""",
arrayFilter(rel_r -> rel_market(rel_r) = 'SPOT' AND rel_complete(rel_r)
    AND (NOT JSONHas(rel_r, 'token_id') OR JSONExtractString(rel_r, 'token_id') = rel_token)
    AND coalesce(rel_number(rel_close_raw(rel_r)) != 0, 0), rel_klines) AS rel_reaction_rows,
arrayDistinct(arrayMap(rel_r -> JSONExtractString(rel_r, 'instrument_id'),
    arrayFilter(rel_r -> rel_close_time(rel_r) > rel_end - 86400000000 AND rel_close_time(rel_r) <= rel_end,
        rel_reaction_rows))) AS rel_reaction_venues,
rel_i -> arraySum(arrayMap(rel_r -> ifNull(rel_decimal(JSONExtractRaw(rel_r, 'quote_volume_usd')),
    toDecimal256(0, 24)), arrayFilter(rel_r -> JSONExtractString(rel_r, 'instrument_id') = rel_i
        AND rel_close_time(rel_r) > rel_end - 86400000000 AND rel_close_time(rel_r) <= rel_end,
        rel_reaction_rows))) AS rel_reaction_volume,
arrayMap(rel_i -> tuple(rel_i, rel_reaction_volume(rel_i)), rel_reaction_venues) AS rel_reaction_volumes,
if(empty(rel_reaction_volumes) OR arrayMax(arrayMap(rel_x -> rel_x.2, rel_reaction_volumes)) <= 0,
    CAST(NULL, 'Nullable(String)'), arrayFirst(rel_x -> rel_x.2 =
        arrayMax(arrayMap(rel_y -> rel_y.2, rel_reaction_volumes)), rel_reaction_volumes).1) AS rel_reaction_venue,
rel_bound -> arrayFirst(rel_r -> rel_close_time(rel_r) = arrayMax(arrayMap(rel_q -> rel_close_time(rel_q),
    arrayFilter(rel_q -> JSONExtractString(rel_q, 'instrument_id') = rel_reaction_venue
        AND rel_close_time(rel_q) <= rel_bound, rel_reaction_rows))),
    arrayFilter(rel_r -> JSONExtractString(rel_r, 'instrument_id') = rel_reaction_venue
        AND rel_close_time(rel_r) <= rel_bound, rel_reaction_rows)) AS rel_reaction_latest,
rel_bound -> if(rel_reaction_latest(rel_bound) = '' OR
    rel_bound - rel_close_time(rel_reaction_latest(rel_bound)) > 1000000 *
        if(JSONHas(rel_reaction_latest(rel_bound), 'interval_s'),
            JSONExtractInt(rel_reaction_latest(rel_bound), 'interval_s'), 60),
    tuple('', CAST(NULL, 'Nullable(Int64)'), CAST(NULL, 'Nullable(Decimal256(24))')),
    tuple(rel_reaction_latest(rel_bound), rel_close_time(rel_reaction_latest(rel_bound)),
        rel_decimal(rel_close_raw(rel_reaction_latest(rel_bound))))) AS rel_reaction_close,
rel_reaction_close(rel_start) AS rel_reaction_first,
rel_reaction_close(rel_end) AS rel_reaction_last,
if(isNull(rel_reaction_first.3) OR isNull(rel_reaction_last.3)
    OR rel_reaction_first.3 = 0 OR rel_reaction_last.3 = 0, CAST(NULL, 'Nullable(Float64)'),
    toFloat64(divideDecimal(assumeNotNull(rel_reaction_last.3),
        ifNull(nullIf(rel_reaction_first.3, 0), toDecimal256(1, 24)), 24) - toDecimal256(1, 24))) AS rel_reaction_return,
{result} AS rel_benchmark_result
"""
    )


def benchmark_select_sql():
    return "SELECT rel_benchmark_result AS result"


def benchmark_query(snapshot, config=None):
    _, params = query(snapshot, config)
    return "WITH " + benchmark_ctes() + "\n" + benchmark_select_sql(), params
