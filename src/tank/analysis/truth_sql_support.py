import json
from array import array

from tank.clickhouse_market import parameter_string
from tank.util import jsonable


def _transport(value):
    if isinstance(value, dict):
        return {key: _transport(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, array, bytes, bytearray, memoryview)):
        return [_transport(item) for item in value]
    return jsonable(value)


def _object(**fields):
    parts = []
    for name, expression in fields.items():
        if parts:
            parts.append("','")
        parts.extend(("'\"" + name + "\":'", expression))
    return "concat('{', " + ", ".join(parts) + ", '}')"


def _json(expression):
    return f"toJSONString({expression})"


def _raw(expression):
    return f"sup_raw({expression})"


def _money(expression):
    return f"sup_money({expression})"


def _bool(expression):
    return f"toJSONString(CAST(({expression}), 'Nullable(Bool)'))"


def _patch(expression, **fields):
    keys = ", ".join("'" + key + "'" for key in fields)
    values = ", ".join(fields.values())
    return f"sup_patch({expression}, [{keys}], [{values}])"


def ctes():
    return ",\n".join((_core(), _supply(), _freshness(), _confidence(), _support(), _output()))


def _core():
    return r"""
{sup_snapshot:String} AS sup_snapshot,
{sup_config:String} AS sup_config,
{sup_spots:String} AS sup_spots,
{sup_derivatives:String} AS sup_derivatives,
{sup_liquidity:String} AS sup_liquidity,
{sup_relative:String} AS sup_relative,
{sup_supply_preimages:String} AS sup_supply_preimages,
toDecimal256(0, 24) AS sup_zero,
toDecimal256(1, 24) AS sup_one,
(x -> if(JSONType(x) = 'Object', JSONExtractRaw(x, 'value'), x)) AS sup_unwrap,
(x -> if(JSONType(x) = 'String', JSONExtractString(x), x)) AS sup_text,
(x -> if(x IN ('', 'null'), 'null', x)) AS sup_raw,
(x -> if(isNull(x), 'null', toJSONString(toString(x)))) AS sup_money,
(x -> concat('[', arrayStringConcat(x, ','), ']')) AS sup_array,
((x, keys, vals) -> concat('{', arrayStringConcat(arrayConcat(
    arrayMap(k -> concat(toJSONString(k.1), ':', k.2), arrayFilter(k -> NOT has(keys, k.1), JSONExtractKeysAndValuesRaw(x))),
    arrayMap((k, v) -> concat(toJSONString(k), ':', v), keys, vals)), ','), '}')) AS sup_patch,
(x -> arrayMap(text -> if(startsWith(text, '.'), concat('0', text),
    if(startsWith(text, '-.') OR startsWith(text, '+.'), concat(substring(text, 1, 1), '0', substring(text, 2)), text)),
    [sup_text(sup_unwrap(x))])[1]) AS sup_decimal_input,
(x -> arrayMap(text -> arrayMap(parts -> arrayMap(digits -> arrayMap(point ->
    if(length(parts) = 2,
        if(throwIf(length(parts) = 2 AND (NOT match(text, '^[+-]?[0-9]+([.][0-9]*)?[eE][+-]?[0-9]+$')
            OR isNull(toInt32OrNull(parts[2])) OR abs(ifNull(toInt32OrNull(parts[2]), 100)) > 48),
            'Market Truth decimal input outside native financial range') = 0,
            concat(if(startsWith(parts[1], '-'), '-', ''), multiIf(
                point <= 0, concat('0.', repeat('0', toUInt64(greatest(-point, 0))), digits),
                point >= toInt64(length(digits)), concat(digits, repeat('0', toUInt64(greatest(point - toInt64(length(digits)), 0)))),
                concat(substring(digits, 1, toUInt64(greatest(point, 0))), '.', substring(digits, toUInt64(greatest(point + 1, 1)))))), ''),
        if(endsWith(text, '.'), substring(text, 1, length(text) - 1), text)),
    [if(position(parts[1], '.') > 0, toInt64(position(parts[1], '.')) - 1 - startsWith(parts[1], '-') - startsWith(parts[1], '+'),
        toInt64(length(digits))) + ifNull(toInt32OrNull(parts[2]), 0)])[1],
    [replaceRegexpAll(parts[1], '[^0-9]', '')])[1], [splitByRegexp('[eE]', text)])[1], [sup_decimal_input(x)])[1]) AS sup_decimal_text,
(x -> arrayMap(text -> if(text IN ('', 'null'), CAST(NULL, 'Nullable(Decimal256(24))'),
    if(throwIf(text NOT IN ('', 'null') AND NOT match(text, '^[+-]?0*[0-9]{1,24}([.][0-9]{1,24})?$'),
        'Market Truth decimal input outside native financial range') = 0, toDecimal256OrNull(text, 24), NULL)), [sup_decimal_text(x)])[1]) AS sup_dec,
(x -> arrayMap(text -> if(text IN ('', 'null'), CAST(NULL, 'Nullable(Decimal256(24))'),
    if(throwIf(text NOT IN ('', 'null') AND NOT match(text, '^[+-]?0*[0-9]{1,48}([.][0-9]{1,24})?$'),
        'Market Truth result outside native financial range') = 0, toDecimal256OrNull(text, 24), NULL)), [sup_decimal_text(x)])[1]) AS sup_result_dec,
(x -> if(JSONType(sup_unwrap(x)) = 'Bool', if(sup_unwrap(x) = 'true', 1., 0.), toFloat64OrNull(sup_text(sup_unwrap(x))))) AS sup_number,
(x -> if(sup_text(sup_unwrap(x)) IN ('', 'null', 'NaN', 'sNaN', 'Infinity', '-Infinity', 'inf', '-inf')
        OR JSONType(sup_unwrap(x)) = 'Bool', CAST(NULL, 'Nullable(Decimal256(24))'),
    if(match(sup_decimal_input(x), '^[+-]?[0-9]+([.][0-9]*)?([eE][+-]?[0-9]+)?$'), sup_dec(x), NULL))) AS sup_fundamental_dec,
(x -> if(JSONType(x) = 'Object', coalesce(nullIf(JSONExtractRaw(x, 'value'), ''), JSONExtractRaw(x, 'ts')), x)) AS sup_time_unwrap,
(x -> if(JSONType(sup_time_unwrap(x)) = 'Bool', if(sup_time_unwrap(x) = 'true', 1., 0.), toFloat64OrNull(sup_text(sup_time_unwrap(x))))) AS sup_timestamp_number,
(x -> arrayMap(raw -> if(JSONType(raw) IN ('Int64', 'UInt64', 'Float64', 'Bool') OR match(sup_text(raw), '^[0-9]+([.][0-9]+)?$'),
    arrayMap(number -> fromUnixTimestamp64Micro(toInt64(roundBankers(multiIf(abs(number) > 1e14,
        number, abs(number) > 1e11, number * 1000, number * 1000000), 0)), 'UTC'),
        [if(JSONType(raw) = 'Bool', if(raw = 'true', 1., 0.), toFloat64OrNull(sup_text(raw)))])[1],
    parseDateTime64BestEffortOrNull(sup_text(raw), 6, 'UTC')), [sup_time_unwrap(x)])[1]) AS sup_parse,
['minute_start', 'open_time', 'exchange_ts', 'ts', 'as_of', 'settlement_time', 'funding_time', 'observed_at'] AS sup_time_fields,
(x -> arrayFirst(k -> JSONExtractRaw(x, k) NOT IN ('', 'null'), sup_time_fields)) AS sup_time_field,
(x -> sup_parse(JSONExtractRaw(x, sup_time_field(x)))) AS sup_time,
(x -> if(isNull(x), CAST(NULL, 'Nullable(String)'), concat(formatDateTime(x, '%Y-%m-%dT%H:%i:%S', 'UTC'),
    if(positiveModulo(toUnixTimestamp64Micro(x), 1000000) = 0, '', concat('.', leftPad(toString(positiveModulo(toUnixTimestamp64Micro(x), 1000000)), 6, '0'))), 'Z'))) AS sup_iso,
((a, b) -> dateDiff('microsecond', a, b) / 1000000.) AS sup_seconds,
((t, s) -> t + toIntervalMicrosecond(toInt64(roundBankers(s * 1000000, 0)))) AS sup_offset,
((k, v) -> if(JSONHas(sup_config, k), JSONExtractRaw(sup_config, k), v)) AS sup_parameter,
sup_number(sup_parameter('history_min_days', '7')) * 86400 AS sup_history_min,
sup_number(sup_parameter('historical_lookback_days', sup_parameter('base_retention_days', '90'))) * 86400 AS sup_lookback,
JSONExtractArrayRaw(sup_parameter('normal_range_quantiles', '[0.025,0.975]')) AS sup_quantiles,
((a, q) -> if(empty(a), CAST(NULL, 'Nullable(Float64)'), arraySort(a)[toUInt64(floor((length(a) - 1) * q)) + 1]
    + (arraySort(a)[toUInt64(ceil((length(a) - 1) * q)) + 1] - arraySort(a)[toUInt64(floor((length(a) - 1) * q)) + 1])
    * ((length(a) - 1) * q - floor((length(a) - 1) * q)))) AS sup_quantile,
(a -> if(empty(a), 'null', toJSONString(arrayMap(q -> sup_quantile(a, sup_number(q)), sup_quantiles)))) AS sup_normal,
((a, q) -> if(empty(a), CAST(NULL, 'Nullable(Decimal256(24))'), arraySort(a)[toUInt64(floor((length(a) - 1) * q)) + 1]
    + multiplyDecimal(arraySort(a)[toUInt64(ceil((length(a) - 1) * q)) + 1] - arraySort(a)[toUInt64(floor((length(a) - 1) * q)) + 1],
        sup_dec(toJSONString((length(a) - 1) * q - floor((length(a) - 1) * q))), 24))) AS sup_decimal_quantile,
(a -> if(empty(a), 'null', sup_array(arrayMap(q -> sup_money(sup_decimal_quantile(a, sup_number(q))), sup_quantiles)))) AS sup_decimal_normal,
((a, b) -> toFloat64(divideDecimal(a, nullIf(b, sup_zero), 24))) AS sup_ratio,
(a -> if(empty(a) OR arrayExists(x -> isNull(x), a), CAST(NULL, 'Nullable(Decimal256(24))'),
    if(throwIf(toDecimal256(length(a), 0) * ifNull(arrayMax(arrayMap(x -> abs(x), a)), sup_zero) >= toDecimal256('1000000000000000000000000000000000000000000000000', 24),
        'Market Truth aggregate outside native financial range') = 0, arraySum(arrayMap(x -> ifNull(x, sup_zero), a)), NULL))) AS sup_sum,
(x -> if(JSONExtractString(x, 'venue') != '', JSONExtractString(x, 'venue'), splitByChar(':', JSONExtractString(x, 'instrument_id'))[1])) AS sup_venue,
(x -> if(JSONExtractString(x, 'market_type') != '', JSONExtractString(x, 'market_type') = 'SPOT', position(JSONExtractString(x, 'instrument_id'), ':spot:') > 0)) AS sup_is_spot,
(x -> if(JSONHas(x, 'kind'), JSONExtractString(x, 'kind'), JSONExtractString(x, 'type'))) AS sup_kind,
(x -> JSONExtractRaw(x, 'available') != 'false') AS sup_available,
(x -> JSONExtractString(x, 'instrument_id')) AS sup_iid,
(x -> tuple(sup_raw(JSONExtractRaw(x, 'instrument_id')), sup_raw(JSONExtractRaw(x, 'metric_key')), sup_raw(JSONExtractRaw(x, 'kind')),
    sup_raw(JSONExtractRaw(x, 'chain')))) AS sup_field_key,
(x -> arrayMap(p -> p.2, arraySort(p -> tuple(sup_time(p.2), p.1), arrayMap((r, i) -> tuple(i, r), x, arrayEnumerate(x))))) AS sup_sorted,
(x -> arrayFirst(r -> sup_time(r) = arrayMax(arrayMap(sup_time, x)), x)) AS sup_maxrow,
((a, target, age) -> sup_maxrow(arrayFilter(r -> sup_time(r) <= target AND sup_seconds(sup_time(r), target) <= age, a))) AS sup_asof,
JSONExtractString(sup_snapshot, 'token_id') AS sup_token,
sup_parse(JSONExtractRaw(sup_snapshot, 'cutoff')) AS sup_end,
sup_parse(JSONExtractRaw(sup_snapshot, 'assembled_at')) AS sup_assembled,
coalesce(sup_parse(JSONExtractRaw(sup_snapshot, '_event_anchor')), sup_parse(JSONExtractRaw(sup_snapshot, 'anchor', 'ts')),
    sup_parse(JSONExtractRaw(sup_snapshot, 'anchor'))) AS sup_anchor,
if(JSONHas(sup_snapshot, '_event_anchor'), isNotNull(sup_parse(JSONExtractRaw(sup_snapshot, '_event_anchor'))), isNotNull(sup_anchor)) AS sup_has_anchor,
if(JSONExtractString(sup_snapshot, '_primary') != '', JSONExtractString(sup_snapshot, '_primary'),
    if(sup_has_anchor AND sup_seconds(sup_anchor, sup_end) >= 900 AND sup_seconds(sup_anchor, sup_end) <= 86400, 'EVENT', 'W4H')) AS sup_primary,
((w) -> if(w = 'EVENT', coalesce(sup_anchor, sup_offset(sup_end, -14400.)),
    sup_offset(sup_end, -multiIf(w = 'W15M', 900., w = 'W1H', 3600., w = 'W24H', 86400., 14400.)))) AS sup_start,
['W15M', 'W1H', 'W4H', 'W24H', 'EVENT'] AS sup_windows,
(x -> isNull(sup_assembled) OR JSONExtractRaw(x, 'received_ts') IN ('', 'null') OR sup_parse(JSONExtractRaw(x, 'received_ts')) <= sup_assembled) AS sup_received,
JSONExtractArrayRaw(sup_snapshot, 'instruments') AS sup_instrument_rows,
JSONExtractArrayRaw(sup_snapshot, 'semantics') AS sup_semantics,
JSONExtractRaw(sup_snapshot, 'capture_metadata') AS sup_metadata,
((c) -> JSONExtractRaw(sup_metadata, 'collections', c)) AS sup_metadata_entry,
((c, iid) -> if(JSONType(JSONExtractRaw(sup_metadata_entry(c), 'observed_through')) = 'Object',
    JSONExtractRaw(sup_metadata_entry(c), 'observed_through', iid), JSONExtractRaw(sup_metadata_entry(c), 'observed_through'))) AS sup_watermark_raw,
((c, iid) -> sup_parse(sup_watermark_raw(c, iid))) AS sup_watermark,
(x -> arrayLast(r -> sup_iid(r) = sup_iid(x), sup_instrument_rows)) AS sup_instrument,
(x -> arrayFirst(r -> sup_iid(r) = sup_iid(x), sup_instrument_rows)) AS sup_cadence_instrument,
(x -> if(JSONHas(x, 'token_id'), JSONExtractString(x, 'token_id'),
    if(JSONExtractString(sup_instrument(x), 'token_id') != '', JSONExtractString(sup_instrument(x), 'token_id'), sup_token))) AS sup_owner,
(x -> if(JSONHas(x, 'token_id'), JSONExtractString(x, 'token_id'), coalesce(nullIf(JSONExtractString(arrayLast(r -> sup_iid(r) = sup_iid(x)
    AND JSONExtractString(r, 'token_id') != '', sup_instrument_rows), 'token_id'), ''), sup_token))) AS sup_fresh_owner,
((c, x) -> JSONExtractRaw(sup_metadata_entry(c), 'cadence_s_by_metric', JSONExtractString(x, 'metric_key'))) AS sup_metric_cadence,
((c, x) -> if(JSONHas(sup_metadata_entry(c), 'by_kind', sup_kind(x)), JSONExtractRaw(sup_metadata_entry(c), 'by_kind', sup_kind(x)),
    JSONExtractRaw(sup_metadata_entry(c), 'cadence_s'))) AS sup_explicit_cadence,
((c, x) -> sup_number(if(JSONType(sup_explicit_cadence(c, x)) = 'Object', JSONExtractRaw(sup_explicit_cadence(c, x), 'cadence_s'), sup_explicit_cadence(c, x)))) AS sup_explicit_number,
(x -> arrayFirst(s -> JSONExtractString(x, 'metric_key') != '' AND JSONExtractString(s, 'metric_key') = JSONExtractString(x, 'metric_key')
    AND (JSONExtractString(s, 'instrument_id') = '' OR sup_iid(s) = sup_iid(x)) AND JSONExtractRaw(s, 'cadence_s') NOT IN ('', 'null'), sup_semantics)) AS sup_cadence_semantics,
(x -> upper(coalesce(nullIf(JSONExtractString(sup_metadata, 'tier'), ''), nullIf(JSONExtractString(sup_snapshot, 'capture_tier'), ''),
    nullIf(JSONExtractString(sup_cadence_instrument(x), 'capture_tier'), ''), nullIf(JSONExtractString(sup_cadence_instrument(x), 'tier'), ''), 'BASE'))) AS sup_tier,
((c, x) -> multiIf(JSONType(sup_metadata_entry(c)) IN ('Int64', 'UInt64', 'Float64', 'Bool'), tuple(sup_number(sup_metadata_entry(c)), 'CAPTURE_METADATA'),
    upper(coalesce(nullIf(JSONExtractString(sup_metadata, 'tier'), ''), JSONExtractString(sup_snapshot, 'capture_tier'))) = 'UNKNOWN'
        AND (c IN ('oi', 'funding') OR c = 'supply' AND sup_kind(x) IN ('EXCHANGE_INVENTORY', 'WALLET_FLOWS')),
        tuple(CAST(NULL, 'Nullable(Float64)'), 'CAPTURE_TIER_HISTORY_UNAVAILABLE'),
    ifNull(sup_number(sup_metric_cadence(c, x)), 0.) != 0, tuple(sup_number(sup_metric_cadence(c, x)), 'CAPTURE_METADATA_BY_METRIC'),
    sup_explicit_number(c, x) > 0, tuple(sup_explicit_number(c, x), 'CAPTURE_METADATA'),
    sup_cadence_semantics(x) != '', tuple(sup_number(JSONExtractRaw(sup_cadence_semantics(x), 'cadence_s')), 'SCOPED_METRIC_SEMANTICS'),
    c IN ('oi', 'funding'), tuple(if(sup_tier(x) = 'HOT', 60., 300.), concat('TDD_5_1_', sup_tier(x))),
    c IN ('flow_bars', 'klines'), tuple(60., 'TDD_5_1'), c = 'books', tuple(10., 'TDD_5_1'), c = 'tickers', tuple(60., 'BASE_TICKER_POLL'),
    c = 'supply' AND sup_kind(x) IN ('EXCHANGE_INVENTORY', 'WALLET_FLOWS', 'UNLOCKS', 'LENDING', 'CHAIN_LIQUIDITY', 'STABLECOIN_CHAIN', 'FUNDAMENTALS'),
        tuple(multiIf(sup_kind(x) IN ('EXCHANGE_INVENTORY', 'WALLET_FLOWS'), if(sup_tier(x) = 'HOT', 900., 21600.),
            sup_kind(x) = 'UNLOCKS', 86400., sup_kind(x) = 'LENDING', 1800., 21600.), concat('TDD_5_6_', sup_tier(x))),
    tuple(CAST(NULL, 'Nullable(Float64)'), 'CADENCE_NOT_CONFIGURED'))) AS sup_cadence,
(x -> arrayLast(s -> JSONExtractString(x, 'metric_key') != '' AND JSONExtractString(s, 'metric_key') = JSONExtractString(x, 'metric_key') AND sup_iid(s) = sup_iid(x)
    AND ifNull(sup_number(JSONExtractRaw(s, 'source_interval_s')), 0.) != 0., sup_semantics)) AS sup_scoped_interval,
(x -> if(sup_scoped_interval(x) != '', sup_scoped_interval(x), arrayLast(s -> JSONExtractString(s, 'metric_key') = JSONExtractString(x, 'metric_key')
    AND sup_iid(s) = '' AND JSONExtractString(s, 'metric_key') != '' AND ifNull(sup_number(JSONExtractRaw(s, 'source_interval_s')), 0.) != 0., sup_semantics))) AS sup_interval_semantics,
(x -> sup_number(JSONExtractRaw(sup_interval_semantics(x), 'source_interval_s'))) AS sup_interval,
(x -> 2 * sup_interval(x) + ifNull(sup_number(JSONExtractRaw(sup_interval_semantics(x), 'publish_lag_s')), 0.)) AS sup_interval_allowed,
(x -> sup_offset(sup_time(x), if(JSONExtractString(sup_interval_semantics(x), 'timestamp_semantics') = 'BUCKET_END', -ifNull(sup_interval(x), 0.), 0.))) AS sup_interval_start,
'{"value":null,"unit":null,"window":null,"observed_at":null,"precision":null,"coverage":null,"reason":"INCOMPLETE"}' AS sup_missing
""".strip()


def _freshness():
    observed = _object(
        observed_at=_json("sup_iso(stamp)"),
        observed_at_prec=_json("if(sup_offset(sup_time(r), offset) = stamp, sup_precision(r), NULL)"),
        observed_at_prec_reason="if(sup_offset(sup_time(r), offset) = stamp AND isNotNull(sup_precision(r)), 'null', toJSONString(if(sup_offset(sup_time(r), offset) = stamp, sup_precision_reason(r), 'INCOMPLETE')))",
    )
    check = _patch(
        "sup_observed(sup_fresh_row, sup_fresh_stamp, if(sup_collection IN ('flow_bars', 'klines'), sup_bar_interval(sup_fresh_row), 0.))",
        collection=_json("sup_collection"),
        instrument_id=_raw("sup_fresh_key.1"),
        metric_key=_raw("sup_fresh_key.2"),
        kind=_raw("sup_fresh_key.3"),
        chain=_raw("sup_fresh_key.4"),
        cadence_s=_json("sup_fresh_cadence"),
        cadence_source=_json("sup_fresh_source"),
        age_s=_json("sup_fresh_age"),
        fresh=_bool("sup_fresh_flag"),
        reason="if(sup_fresh_flag, 'null', if(isNull(sup_fresh_cadence), '\"NOT_ESTIMABLE\"', '\"INCOMPLETE\"'))",
    )
    return (
        r"""
['flow_bars', 'klines', 'books', 'oi', 'funding', 'liquidations', 'tickers', 'supply'] AS sup_collections,
arrayFlatten(arrayMap((c, i) -> arrayMap((r, n) -> tuple(i, c, n, r), JSONExtractArrayRaw(sup_snapshot, c), arrayEnumerate(JSONExtractArrayRaw(sup_snapshot, c))), sup_collections, arrayEnumerate(sup_collections))) AS sup_all_rows,
(r -> ifNull(sup_number(JSONExtractRaw(r, 'interval_s')), 60.)) AS sup_bar_interval,
(r -> sup_offset(sup_time(r), sup_bar_interval(r))) AS sup_bar_close,
(r -> arrayFilter(p -> p NOT IN ('', 'null'), arrayMap(k -> JSONExtractRaw(r, k),
    [concat(sup_time_field(r), '_prec'), concat(sup_time_field(r), '_precision'), 'timestamp_precision', 'precision']))) AS sup_outer_precisions,
(r -> arrayConcat(arrayFilter(p -> p NOT IN ('', 'null'), [JSONExtractRaw(r, sup_time_field(r), 'precision'), JSONExtractRaw(r, sup_time_field(r), 'prec')]), sup_outer_precisions(r))) AS sup_precision_declarations,
(r -> sup_text(JSONExtractRaw(r, concat(sup_time_field(r), '_bucket_size_s')))) AS sup_bucket_size,
((p, r) -> multiIf(p IN ('', 'UNKNOWN', 'null'), CAST(NULL, 'Nullable(String)'),
    p IN ('EXACT', 'COLLECTION_ONLY'), if(throwIf(sup_bucket_size(r) NOT IN ('', 'null'), 'Non-bucket precision cannot carry bucket size') = 0, p, NULL),
    p IN ('HOUR', 'BUCKET_1H'), 'BUCKET(1h)', p IN ('DAY', 'BUCKET_1D'), 'BUCKET(1d)', p = 'BUCKET_1M', 'BUCKET(1m)',
    p = 'BUCKET', if(sup_bucket_size(r) IN ('', 'null'), NULL, concat('BUCKET(', sup_bucket_size(r), 's)')),
    match(p, '^BUCKET[(][0-9]+([.][0-9]+)?(us|ms|s|m|h|d|w)[)]$'), p,
    if(throwIf(p NOT IN ('', 'UNKNOWN', 'null', 'EXACT', 'COLLECTION_ONLY', 'HOUR', 'BUCKET_1H', 'DAY', 'BUCKET_1D', 'BUCKET_1M', 'BUCKET')
        AND NOT match(p, '^BUCKET[(][0-9]+([.][0-9]+)?(us|ms|s|m|h|d|w)[)]$'), 'Unsupported timestamp precision') = 0, NULL, NULL))) AS sup_normal_precision,
(p -> toFloat64OrNull(extract(ifNull(p, ''), '[0-9]+(?:[.][0-9]+)?')) * multiIf(endsWith(ifNull(p, ''), 'us)'), 0.000001,
    endsWith(ifNull(p, ''), 'ms)'), 0.001, endsWith(ifNull(p, ''), 'm)'), 60., endsWith(ifNull(p, ''), 'h)'), 3600.,
    endsWith(ifNull(p, ''), 'd)'), 86400., endsWith(ifNull(p, ''), 'w)'), 604800., 1.)) AS sup_precision_seconds,
(r -> arrayMap(p -> tuple(sup_normal_precision(sup_text(p), r)), sup_precision_declarations(r))) AS sup_precisions,
(r -> if(throwIf(length(arrayDistinct(arrayMap(p -> tuple(if(startsWith(ifNull(p.1, ''), 'BUCKET('), 'BUCKET', ifNull(p.1, '')), sup_precision_seconds(p.1)), sup_precisions(r)))) > 1
    OR arrayExists(p -> startsWith(ifNull(p.1, ''), 'BUCKET(') AND (sup_precision_seconds(p.1) <= 0 OR sup_bucket_size(r) NOT IN ('', 'null') AND sup_precision_seconds(p.1) != toFloat64OrNull(sup_bucket_size(r))), sup_precisions(r)),
    'Conflicting timestamp precision declarations') = 0, if(empty(sup_precisions(r)), CAST(NULL, 'Nullable(String)'), sup_precisions(r)[1].1), NULL)) AS sup_precision,
(r -> coalesce(nullIf(JSONExtractString(r, sup_time_field(r), 'precision_reason'), ''),
    nullIf(JSONExtractString(r, concat(sup_time_field(r), '_prec_reason')), ''), nullIf(JSONExtractString(r, concat(sup_time_field(r), '_precision_reason')), ''),
    nullIf(JSONExtractString(r, 'timestamp_precision_reason'), ''), nullIf(JSONExtractString(r, 'precision_reason'), ''), 'INCOMPLETE')) AS sup_precision_reason,
(r -> arrayFilter(p -> p NOT IN ('', 'null'), [JSONExtractRaw(r, sup_time_field(r), 'bucket_anchor'), JSONExtractRaw(r, concat(sup_time_field(r), '_bucket_anchor'))])) AS sup_anchor_declarations,
(r -> if(throwIf(length(arrayDistinct(sup_anchor_declarations(r))) > 1 OR arrayExists(a -> sup_text(a) NOT IN ('START', 'END', 'UNSPECIFIED'), sup_anchor_declarations(r))
    OR notEmpty(sup_anchor_declarations(r)) AND NOT startsWith(ifNull(sup_precision(r), ''), 'BUCKET('), 'Conflicting timestamp bucket anchors') = 0,
    if(notEmpty(sup_anchor_declarations(r)), sup_text(sup_anchor_declarations(r)[1]),
        if(startsWith(ifNull(sup_precision(r), ''), 'BUCKET(') AND (JSONHas(r, concat(sup_time_field(r), '_bucket_anchor')) OR JSONHas(r, sup_time_field(r), 'bucket_anchor')), 'UNSPECIFIED', CAST(NULL, 'Nullable(String)'))), NULL)) AS sup_precision_anchor,
((r, stamp, offset) -> if(sup_offset(sup_time(r), offset) = stamp AND isNotNull(sup_precision_anchor(r)),
    sup_patch(__OBSERVED__, ['observed_at_bucket_anchor'], [toJSONString(sup_precision_anchor(r))]), __OBSERVED__)) AS sup_observed,
sup_fresh_groups AS (
    SELECT sup_item.1 AS sup_collection_order, sup_item.2 AS sup_collection,
        tuple(sup_raw(JSONExtractRaw(sup_item.4, 'instrument_id')), sup_raw(JSONExtractRaw(sup_item.4, 'metric_key')),
            if(JSONHas(sup_item.4, 'kind'), sup_raw(JSONExtractRaw(sup_item.4, 'kind')), sup_raw(JSONExtractRaw(sup_item.4, 'type'))), sup_raw(JSONExtractRaw(sup_item.4, 'chain'))) AS sup_fresh_key,
        arrayMap(p -> p.2, arraySort(p -> p.1, groupArray(tuple(sup_item.3, sup_item.4)))) AS sup_fresh_rows
    FROM (SELECT arrayJoin(sup_all_rows) AS sup_item)
    WHERE sup_fresh_owner(sup_item.4) = sup_token AND sup_available(sup_item.4) AND isNotNull(sup_time(sup_item.4)) AND sup_time(sup_item.4) <= sup_end AND sup_received(sup_item.4)
    GROUP BY sup_collection_order, sup_collection, sup_fresh_key
),
sup_fresh_selected AS (
    SELECT *, sup_maxrow(sup_fresh_rows) AS sup_fresh_latest,
        arrayFilter(r -> JSONExtractRaw(r, 'complete') = 'true' AND sup_bar_close(r) <= sup_end, sup_fresh_rows) AS sup_fresh_completed,
        if(sup_collection IN ('flow_bars', 'klines'), arrayFirst(r -> sup_bar_close(r) = arrayMax(arrayMap(sup_bar_close, sup_fresh_completed)), sup_fresh_completed), sup_fresh_latest) AS sup_fresh_row,
        sup_cadence(sup_collection, sup_fresh_latest) AS sup_fresh_base_cadence,
        sup_watermark(sup_collection, sup_iid(sup_fresh_row)) AS sup_fresh_watermark,
        if(sup_collection IN ('flow_bars', 'klines'), if(sup_fresh_watermark > sup_bar_close(sup_fresh_row) AND sup_fresh_watermark <= sup_end,
            sup_fresh_watermark, sup_bar_close(sup_fresh_row)), if(sup_collection = 'liquidations' AND sup_watermark_raw(sup_collection, sup_iid(sup_fresh_row)) NOT IN ('', 'null'), sup_fresh_watermark, sup_time(sup_fresh_row))) AS sup_fresh_stamp
    FROM sup_fresh_groups
),
sup_fresh_checks AS (
    SELECT *, sup_collection NOT IN ('flow_bars', 'klines') AND sup_interval_semantics(sup_fresh_row) != '' AS sup_fresh_published,
        if(sup_fresh_published, sup_interval_allowed(sup_fresh_row), if(sup_collection = 'funding' AND JSONExtractString(sup_fresh_row, 'kind') = 'SETTLED'
            AND ifNull(sup_number(JSONExtractRaw(sup_fresh_row, 'interval_h')), 0.) != 0., sup_number(JSONExtractRaw(sup_fresh_row, 'interval_h')) * 3600., sup_fresh_base_cadence.1)) AS sup_fresh_cadence,
        if(sup_fresh_published, 'SOURCE_INTERVAL_PLUS_PUBLISH_LAG', if(sup_collection = 'funding' AND JSONExtractString(sup_fresh_row, 'kind') = 'SETTLED'
            AND ifNull(sup_number(JSONExtractRaw(sup_fresh_row, 'interval_h')), 0.) != 0., 'SETTLEMENT_INTERVAL', sup_fresh_base_cadence.2)) AS sup_fresh_source,
        sup_seconds(if(sup_fresh_published, sup_interval_start(sup_fresh_row), sup_fresh_stamp), sup_end) AS sup_fresh_age,
        isNotNull(sup_fresh_cadence) AND sup_fresh_age >= 0 AND sup_fresh_age <= sup_fresh_cadence AS sup_fresh_flag,
        __CHECK__ AS sup_fresh_base_json,
        if(sup_collection = 'liquidations', sup_patch(sup_fresh_base_json, ['freshness_basis', 'coverage_status', 'detail'], [
            toJSONString(if(sup_watermark_raw(sup_collection, sup_iid(sup_fresh_row)) NOT IN ('', 'null'), 'STREAM_WATERMARK', 'LAST_EVENT')),
            sup_raw(JSONExtractRaw(sup_metadata_entry(sup_collection), 'coverage_status_by_instrument', sup_iid(sup_fresh_row))),
            if(sup_fresh_flag OR sup_watermark_raw(sup_collection, sup_iid(sup_fresh_row)) NOT IN ('', 'null'), 'null', '"STREAM_CONNECTION_UNPROVEN"')]), sup_fresh_base_json) AS sup_fresh_json
    FROM sup_fresh_selected WHERE sup_collection NOT IN ('flow_bars', 'klines') OR notEmpty(sup_fresh_completed)
),
sup_fresh_output AS (
    SELECT arrayMap(p -> p.3, arraySort(p -> tuple(p.1, arrayMap(x -> if(x = 'null', '', sup_text(x)), [p.2.1, p.2.2, p.2.3, p.2.4])),
        groupArray(tuple(sup_collection_order, sup_fresh_key, sup_fresh_json)))) AS sup_freshness
    FROM sup_fresh_checks
)
""".strip()
        .replace("__OBSERVED__", observed)
        .replace("__CHECK__", check)
    )


def _confidence():
    required_failure = _object(
        condition="'\"freshness\"'",
        reason="'\"INCOMPLETE\"'",
        collection=_json("q.3"),
        instrument_id=_json("q.1"),
        metric_family=_json("q.2"),
        cadence_s=_json("sup_required_cadence(q).1"),
        cadence_source=_json("sup_required_cadence(q).2"),
    )
    confidence = _object(
        class_=_json("if(sup_conf_low, 'LOW', if(notEmpty(sup_conf_failed), 'MEDIUM', 'HIGH'))"),
        failed="sup_array(sup_conf_failed)",
        limiting="if(empty(sup_conf_failed), 'null', sup_conf_failed[1])",
        derivatives_native_coverage_ready=_bool("NOT sup_derivative_coverage_failed"),
        derivatives_classification_ready=_bool(
            "empty(sup_perp_ids) OR JSONExtractBool(sup_primary_derivative, 'synchronized', 'classification_ready')"
        ),
        freshness="sup_array(sup_freshness)",
    )
    return (
        r"""
if(JSONHas(sup_spots, sup_primary), JSONExtractRaw(sup_spots, sup_primary), '{}') AS sup_primary_spot,
coalesce(nullIf(if(JSONHas(sup_derivatives, 'windows'), JSONExtractRaw(sup_derivatives, 'windows', sup_primary), JSONExtractRaw(sup_derivatives, sup_primary)), ''), '{}') AS sup_primary_derivative,
arrayFilter(r -> sup_iid(r) != '' AND (NOT JSONHas(r, 'token_id') OR JSONExtractString(r, 'token_id') = sup_token), sup_instrument_rows) AS sup_owned_instruments,
arrayFlatten(arrayMap(c -> arrayFilter(r -> sup_iid(r) != '' AND sup_owner(r) = sup_token, JSONExtractArrayRaw(sup_snapshot, c)), ['flow_bars', 'books', 'oi', 'funding', 'klines'])) AS sup_supplemental_instruments,
arrayDistinct(arrayMap(sup_iid, arrayConcat(sup_owned_instruments, sup_supplemental_instruments))) AS sup_conf_instrument_ids,
arrayMap(iid -> if(arrayExists(r -> sup_iid(r) = iid, sup_owned_instruments), arrayLast(r -> sup_iid(r) = iid, sup_owned_instruments),
    arrayFirst(r -> sup_iid(r) = iid, sup_supplemental_instruments)), sup_conf_instrument_ids) AS sup_conf_instruments,
arrayMap(v -> JSONExtractString(v, 'venue'), JSONExtractArrayRaw(sup_primary_spot, 'venues')) AS sup_complete_venues,
arrayFlatten(arrayMap(r -> if(sup_is_spot(r), if(has(sup_complete_venues, sup_venue(r)), arrayMap(p -> tuple(sup_iid(r), p.1, p.2), [('aggressor', 'flow_bars'), ('depth', 'books'), ('price', 'klines')]), []),
    arrayMap(p -> tuple(sup_iid(r), p.1, p.2), [('oi', 'oi'), ('funding', 'funding')])), sup_conf_instruments)) AS sup_required,
arrayMap(sup_iid, arrayFilter(r -> NOT sup_is_spot(r), sup_conf_instruments)) AS sup_perp_ids,
arrayFilter(r -> has(sup_perp_ids, sup_iid(r)) AND sup_time(r) >= sup_offset(sup_end, -86400.) AND sup_time(r) < sup_end
    AND JSONExtractRaw(r, 'quote_volume_usd') NOT IN ('', 'null'), JSONExtractArrayRaw(sup_snapshot, 'klines')) AS sup_perp_volume_rows,
arrayMap(sup_text, JSONExtractArrayRaw(sup_primary_derivative, 'synchronized', 'venues')) AS sup_synchronized_venues,
notEmpty(sup_perp_ids) AND (JSONExtractRaw(sup_primary_derivative, 'synchronized') IN ('', 'null', '{}', 'false') OR empty(sup_perp_volume_rows)
    OR 2 * ifNull(sup_sum(arrayMap(r -> sup_dec(JSONExtractRaw(r, 'quote_volume_usd')), arrayFilter(r -> has(sup_synchronized_venues, sup_venue(r)), sup_perp_volume_rows))), sup_zero)
        <= sup_sum(arrayMap(r -> sup_dec(JSONExtractRaw(r, 'quote_volume_usd')), sup_perp_volume_rows))) AS sup_derivative_coverage_failed,
JSONExtractInt(sup_primary_spot, 'coverage', 'n_complete') < toInt64(sup_number(sup_parameter('min_spot_venues', '2'))) AS sup_conf_low,
sup_conf_low OR if(JSONHas(sup_primary_spot, 'coverage', 'exact_majority'), JSONExtractRaw(sup_primary_spot, 'coverage', 'exact_majority') != 'true',
    isNull(sup_dec(JSONExtractRaw(sup_primary_spot, 'coverage', 'covered_share_est'))) OR sup_dec(JSONExtractRaw(sup_primary_spot, 'coverage', 'covered_share_est')) <= toDecimal256('0.5', 24)) AS sup_spot_coverage_failed,
(q -> arrayFilter(r -> sup_iid(r) = q.1 AND isNotNull(sup_time(r)) AND sup_time(r) <= sup_end AND sup_received(r), JSONExtractArrayRaw(sup_snapshot, q.3))) AS sup_required_rows,
(q -> sup_cadence(q.3, if(empty(sup_required_rows(q)), concat('{"instrument_id":', toJSONString(q.1), '}'), sup_required_rows(q)[-1]))) AS sup_required_cadence,
((q, r) -> multiIf(q.3 IN ('flow_bars', 'klines'), JSONExtractRaw(r, 'complete') = 'true' AND sup_bar_close(r) <= sup_end
        AND (q.3 != 'klines' OR ifNull(sup_number(if(JSONHas(r, 'c'), JSONExtractRaw(r, 'c'), JSONExtractRaw(r, 'close'))), 0.) != 0.),
    q.3 = 'books', JSONExtractRaw(r, 'integrity_ok') = 'true' AND ifNull(sup_number(JSONExtractRaw(r, 'mid')), 0.) != 0.
        AND isNotNull(sup_number(JSONExtractRaw(r, 'range_covered_pct', 'bid'))) AND isNotNull(sup_number(JSONExtractRaw(r, 'range_covered_pct', 'ask'))),
    q.3 = 'oi', isNotNull(sup_dec(JSONExtractRaw(r, 'oi_native'))), q.3 = 'funding', isNotNull(sup_number(JSONExtractRaw(r, 'rate')))
        AND ifNull(sup_number(JSONExtractRaw(r, 'interval_h')), 0.) != 0., true)) AS sup_required_valid,
(q -> arrayFilter(r -> sup_required_valid(q, r), sup_required_rows(q))) AS sup_required_valid_rows,
((q, r) -> if(q.3 IN ('flow_bars', 'klines'), sup_bar_close(r), sup_time(r))) AS sup_required_stamp,
(q -> arrayFilter(t -> isNotNull(t), arrayConcat(arrayMap(r -> if(q.3 = 'oi' AND sup_interval_semantics(r) != '', sup_offset(sup_interval_start(r), sup_interval_allowed(r)),
    sup_offset(sup_required_stamp(q, r), sup_required_cadence(q).1)), sup_required_valid_rows(q)),
    if(q.3 = 'klines' AND notEmpty(sup_required_valid_rows(q)) AND sup_watermark(q.3, q.1) > arrayMax(arrayMap(r -> sup_required_stamp(q, r), sup_required_valid_rows(q)))
        AND sup_watermark(q.3, q.1) <= sup_end, [sup_offset(sup_watermark(q.3, q.1), sup_required_cadence(q).1)], [])))) AS sup_required_deadlines,
(q -> __REQUIRED_FAILURE__) AS sup_required_failure,
arrayMap(sup_required_failure, arrayFilter(q -> empty(sup_required_deadlines(q)) OR arrayMax(sup_required_deadlines(q)) < sup_end, sup_required)) AS sup_required_failed,
sup_conf_output AS (
    SELECT *, arrayConcat(if(sup_spot_coverage_failed, ['{"condition":"spot_coverage","reason":"INCOMPLETE"}'], []),
        if(sup_derivative_coverage_failed, ['{"condition":"derivatives_coverage","reason":"INCOMPLETE"}'], []), sup_required_failed,
        arrayMap(c -> sup_patch(c, ['condition'], ['"freshness"']), arrayMap(c -> concat('{', arrayStringConcat(arrayMap(k -> concat(toJSONString(k.1), ':', k.2),
            arrayFilter(k -> k.1 != 'fresh', JSONExtractKeysAndValuesRaw(c))), ','), '}'), arrayFilter(c -> NOT JSONExtractBool(c, 'fresh'), sup_freshness)))) AS sup_conf_failed,
        __CONFIDENCE__ AS sup_confidence
    FROM sup_fresh_output
)
""".strip()
        .replace("__REQUIRED_FAILURE__", required_failure)
        .replace("__CONFIDENCE__", confidence)
    )


def _support():
    volume = _object(
        value=_money("sup_volume_total(market, a, sup_end)"),
        previous=_money("sup_volume_total(market, sup_offset(a, -sup_seconds(a, sup_end)), a)"),
        normal_range="sup_decimal_normal(sup_volume_history(market, a))",
        unit="'\"NATIVE_TOKEN\"'",
        window=_json("w"),
        reason="if(isNotNull(sup_volume_total(market, a, sup_end)) AND notEmpty(sup_volume_history(market, a)), 'null', '\"NOT_ESTIMABLE\"')",
    )
    per_venue = _patch(
        "p",
        perp_taker_buy_share=_json(
            "if(sup_perp_flow_complete(p, a), sup_ratio(sup_flow_sum(sup_iid(p), a, 'buy_qty'), sup_flow_sum(sup_iid(p), a, 'buy_qty') + sup_flow_sum(sup_iid(p), a, 'sell_qty')), NULL)"
        ),
        perp_volume_usd=_money("sup_usd_volume(sup_iid(p), a)"),
        perp_volume_reason="if(isNotNull(sup_usd_volume(sup_iid(p), a)), 'null', '\"INCOMPLETE\"')",
        basis_normal_range="sup_decimal_normal(sup_basis_history(sup_iid(p), a))",
        basis_vs_normal="toJSONString(if(isNotNull(sup_result_dec(JSONExtractRaw(p, 'basis_bps'))) AND notEmpty(sup_basis_history(sup_iid(p), a)), multiIf(sup_result_dec(JSONExtractRaw(p, 'basis_bps')) < sup_decimal_quantile(sup_basis_history(sup_iid(p), a), sup_number(sup_quantiles[1])), 'BELOW', sup_result_dec(JSONExtractRaw(p, 'basis_bps')) > sup_decimal_quantile(sup_basis_history(sup_iid(p), a), sup_number(sup_quantiles[2])), 'ABOVE', 'WITHIN'), NULL))",
        basis_widening=_bool(
            "sup_result_dec(JSONExtractRaw(p, 'basis_bps')) > sup_initial_basis(sup_iid(p), a)"
        ),
        funding_rising=_bool(
            "if(isNotNull(sup_dec(JSONExtractRaw(p, 'funding_start'))) AND ifNull(sup_dec(JSONExtractRaw(p, 'funding_start_interval_h')), sup_zero) != sup_zero, sup_result_dec(JSONExtractRaw(p, 'funding_8h_eq')) > divideDecimal(multiplyDecimal(sup_dec(JSONExtractRaw(p, 'funding_start')), toDecimal256(8, 24), 24), nullIf(sup_dec(JSONExtractRaw(p, 'funding_start_interval_h')), sup_zero), 24), NULL)"
        ),
    )
    features = _object(
        spot_volume_up_beyond_normal=_bool("sup_volume_up('SPOT', sup_window_start)"),
        spot_volume_within_normal=_bool("sup_volume_within('SPOT', sup_window_start)"),
        spot_volume="sup_volume_json('SPOT', sup_window_start, sup_window)",
        perp_volume_up_beyond_normal=_bool("sup_volume_up('PERP', sup_window_start)"),
        perp_volume_within_normal=_bool("sup_volume_within('PERP', sup_window_start)"),
        perp_volume="sup_volume_json('PERP', sup_window_start, sup_window)",
        perp_taker_flow_buy=_bool(
            "if(notEmpty(sup_per_venue) AND arrayAll(p -> sup_perp_flow_complete(p, sup_window_start), sup_per_venue), sup_perp_cvd > sup_zero AND multiplyDecimal(sup_perp_cvd, sup_perp_cvd, 24) >= multiplyDecimal(multiplyDecimal(sup_dec(sup_parameter('significant_flow_sigma', '2')), sup_dec(sup_parameter('significant_flow_sigma', '2')), 24), sup_perp_q, 24), NULL)"
        ),
        basis_within_normal=_bool(
            "if(arrayAll(p -> JSONExtractRaw(p, 'basis_vs_normal') NOT IN ('', 'null'), sup_per_venue), notEmpty(sup_per_venue) AND arrayAll(p -> JSONExtractString(p, 'basis_vs_normal') = 'WITHIN', sup_per_venue), NULL)"
        ),
        basis_widening=_bool(
            "if(arrayExists(p -> JSONExtractRaw(p, 'basis_widening') NOT IN ('', 'null'), sup_per_venue), arrayExists(p -> JSONExtractRaw(p, 'basis_widening') = 'true', sup_per_venue), NULL)"
        ),
        funding_rising=_bool(
            "if(arrayExists(p -> JSONExtractRaw(p, 'funding_rising') NOT IN ('', 'null'), sup_per_venue), arrayExists(p -> JSONExtractRaw(p, 'funding_rising') = 'true', sup_per_venue), NULL)"
        ),
        liquidations_first_half_majority=_bool(
            "if(isNotNull(sup_liquidation_total) AND sup_liquidation_total > sup_zero, 2 * ifNull(sup_liquidation_first, sup_zero) > sup_liquidation_total, NULL)"
        ),
        large_ticket_buy_majority=_raw(
            "JSONExtractRaw(sup_window_spot, 'size_profile', 'large_ticket_buy_majority')"
        ),
        large_tickets_minority=_raw(
            "JSONExtractRaw(sup_window_spot, 'size_profile', 'large_tickets_minority')"
        ),
        large_ticket_sell_share_rising=_raw(
            "JSONExtractRaw(sup_window_spot, 'size_profile', 'sell_share_rising')"
        ),
        exchange_inflows=_bool(
            "if(isNotNull(sup_result_dec(JSONExtractRaw(sup_supply, 'exchange_inventory', 'chg_native_24h'))), sup_result_dec(JSONExtractRaw(sup_supply, 'exchange_inventory', 'chg_native_24h')) > sup_zero, if(notEmpty(JSONExtractArrayRaw(sup_supply, 'flows')), arrayExists(f -> JSONExtractString(f, 'class') IN ('TREASURY_INTERNAL', 'MM_INVENTORY', 'UNLOCK_DISTRIBUTION') AND (JSONExtractRaw(f, 'reaches_exchange') = 'true' OR JSONExtractRaw(f, 'to_exchange') = 'true'), JSONExtractArrayRaw(sup_supply, 'flows')), NULL))"
        ),
        localized_inventory_transfer=_bool(
            "if(notEmpty(JSONExtractArrayRaw(sup_supply, 'flows')), arrayExists(f -> JSONExtractString(f, 'class') = 'CEX_CUSTODY_MIGRATION', JSONExtractArrayRaw(sup_supply, 'flows')), NULL)"
        ),
        ask_depth_building=_bool(
            "if(notEmpty(sup_ask_builds(sup_window_start)), arrayExists(x -> x, sup_ask_builds(sup_window_start)), NULL)"
        ),
        ask_replenishment=_bool(
            "if(notEmpty(sup_ask_tests(sup_window_start)), arrayExists(p -> sup_ask_recovered(p.1, p.2), sup_ask_tests(sup_window_start)), NULL)"
        ),
    )
    return (
        r"""
JSONExtractArrayRaw(sup_snapshot, 'klines') AS sup_klines,
JSONExtractArrayRaw(sup_snapshot, 'flow_bars') AS sup_flow_bars,
JSONExtractArrayRaw(sup_snapshot, 'oi') AS sup_oi_rows,
JSONExtractKeysAndValuesRaw(JSONExtractRaw(sup_snapshot, 'stat_volume_series')) AS sup_volume_projections,
arrayFilter(r -> NOT JSONHas(r, 'token_id') OR JSONExtractString(r, 'token_id') = sup_token, sup_klines) AS sup_owned_klines,
arrayDistinct(arrayConcat(arrayMap(r -> tuple(if(sup_is_spot(r), 'SPOT', 'PERP'), sup_iid(r)), sup_owned_klines),
    arrayMap(p -> tuple(JSONExtractString(p.2, 'market_type'), p.1), arrayFilter(p -> JSONExtractString(p.2, 'token_id') = sup_token
        AND JSONExtractString(p.2, 'market_type') IN ('SPOT', 'PERP') AND JSONExtractString(p.2, 'time_basis') = 'OPEN_TIME', sup_volume_projections)))) AS sup_volume_instruments,
((k) -> arrayFirst(p -> p.1 = k.2 AND JSONExtractString(p.2, 'token_id') = sup_token AND JSONExtractString(p.2, 'market_type') = k.1
    AND JSONExtractString(p.2, 'time_basis') = 'OPEN_TIME', sup_volume_projections).2) AS sup_volume_projection,
((p) -> arrayMap(i -> tuple(fromUnixTimestamp64Micro(toInt64(sup_text(JSONExtractRaw(p, 'times_us', i))), 'UTC'),
    ifNull(sup_number(JSONExtractRaw(p, 'interval_s')), 60.), sup_dec(JSONExtractRaw(p, 'values', i)), JSONExtractRaw(p, 'complete', i) IN ('true', '1')),
    arrayEnumerate(JSONExtractArrayRaw(p, 'times_us')))) AS sup_projected_volume_rows,
((k) -> if(sup_volume_projection(k) != '', sup_projected_volume_rows(sup_volume_projection(k)), arrayMap(r -> tuple(sup_time(r), sup_bar_interval(r),
    sup_dec(JSONExtractRaw(r, 'volume_native')), JSONExtractRaw(r, 'complete') = 'true'), sup_sorted(arrayFilter(r -> sup_iid(r) = k.2
        AND if(sup_is_spot(r), 'SPOT', 'PERP') = k.1 AND sup_bar_close(r) <= sup_end, sup_owned_klines))))) AS sup_volume_rows,
((state, p) -> if(NOT p.4 OR isNull(p.3), tuple(state.1, state.2, state.3 + 1, CAST(NULL, 'Nullable(DateTime64(6, \'UTC\'))')),
    tuple(arrayConcat(state.1, [tuple(p.1, state.2, state.3 + toUInt64(ifNull(state.4 != p.1, true))),
        tuple(sup_offset(p.1, p.2), state.2 + ifNull(p.3, sup_zero), state.3 + toUInt64(ifNull(state.4 != p.1, true)))]),
        state.2 + ifNull(p.3, sup_zero), state.3 + toUInt64(ifNull(state.4 != p.1, true)), sup_offset(p.1, p.2)))) AS sup_volume_step,
(k -> arrayFold(sup_volume_step, sup_volume_rows(k), tuple(CAST([], 'Array(Tuple(Nullable(DateTime64(6, \'UTC\')), Decimal256(24), UInt64))'),
    sup_zero, toUInt64(0), CAST(NULL, 'Nullable(DateTime64(6, \'UTC\'))'))).1) AS sup_volume_bounds,
arrayMap(k -> tuple(k.1, k.2, sup_volume_bounds(k)), sup_volume_instruments) AS sup_volume_indexes,
((bounds, t) -> arrayLast(p -> p.1 = t, bounds)) AS sup_bound,
((market, a, z) -> arrayMap(indexes -> if(notEmpty(indexes) AND arrayAll(k -> arrayExists(p -> p.1 = a, k.3)
    AND arrayExists(p -> p.1 = z, k.3) AND sup_bound(k.3, a).3 = sup_bound(k.3, z).3, indexes),
    sup_sum(arrayMap(k -> sup_bound(k.3, z).2 - sup_bound(k.3, a).2, indexes)), NULL),
    [arrayFilter(k -> k.1 = market, sup_volume_indexes)])[1]) AS sup_volume_total,
((market) -> arrayFilter(t -> arrayAll(k -> arrayExists(p -> p.1 = t, k.3), arrayFilter(k -> k.1 = market, sup_volume_indexes)),
    arrayDistinct(arrayMap(p -> p.1, arrayFirst(k -> k.1 = market, sup_volume_indexes).3)))) AS sup_volume_endpoints,
((market, a) -> arrayFilter(p -> isNotNull(p.2), arrayMap(t -> tuple(t, sup_volume_total(market, sup_offset(t, -sup_seconds(a, sup_end)), t)),
    arrayFilter(t -> sup_offset(t, -sup_seconds(a, sup_end)) >= sup_offset(a, -sup_lookback) AND t < a, sup_volume_endpoints(market))))) AS sup_volume_history_points,
((market, a) -> arrayMap(points -> if(notEmpty(points) AND sup_seconds(sup_offset(arrayMin(arrayMap(p -> p.1, points)), -sup_seconds(a, sup_end)),
    arrayMax(arrayMap(p -> p.1, points))) >= sup_history_min, arrayMap(p -> p.2, points), []), [sup_volume_history_points(market, a)])[1]) AS sup_volume_history,
((market, a) -> if(notEmpty(sup_volume_history(market, a)), sup_volume_total(market, a, sup_end) > sup_volume_total(market, sup_offset(a, -sup_seconds(a, sup_end)), a)
    AND sup_volume_total(market, a, sup_end) > sup_decimal_quantile(sup_volume_history(market, a), sup_number(sup_quantiles[2])), CAST(NULL, 'Nullable(Bool)'))) AS sup_volume_up,
((market, a) -> if(notEmpty(sup_volume_history(market, a)), sup_volume_total(market, a, sup_end) >= sup_decimal_quantile(sup_volume_history(market, a), sup_number(sup_quantiles[1]))
    AND sup_volume_total(market, a, sup_end) <= sup_decimal_quantile(sup_volume_history(market, a), sup_number(sup_quantiles[2])), CAST(NULL, 'Nullable(Bool)'))) AS sup_volume_within,
((market, a, w) -> __VOLUME__) AS sup_volume_json,
arrayFlatten(arrayMap(b -> arrayMap(p -> sup_patch(p, ['instrument_id', 'token_id', 'venue', 'market_type'],
    [toJSONString(sup_iid(b)), sup_raw(JSONExtractRaw(b, 'token_id')), toJSONString(sup_venue(b)), toJSONString(if(sup_is_spot(b), 'SPOT', 'PERP_LINEAR'))]), JSONExtractArrayRaw(b, 'boundary_segments')), sup_flow_bars)) AS sup_nested_pieces,
JSONExtractArrayRaw(sup_snapshot, 'flow_segments') AS sup_direct_pieces,
(p -> tuple(sup_iid(p), JSONExtractRaw(p, 'start'), JSONExtractRaw(p, 'end'))) AS sup_piece_key,
arrayDistinct(arrayMap(sup_piece_key, arrayConcat(sup_direct_pieces, sup_nested_pieces))) AS sup_piece_keys,
arrayMap(k -> if(arrayExists(p -> sup_piece_key(p) = k, sup_direct_pieces), arrayLast(p -> sup_piece_key(p) = k, sup_direct_pieces),
    arrayFirst(p -> sup_piece_key(p) = k, sup_nested_pieces)), sup_piece_keys) AS sup_pieces,
((iid, a, z) -> arrayFilter(p -> sup_iid(p) = iid AND JSONExtractRaw(p, 'complete') = 'true' AND sup_parse(JSONExtractRaw(p, 'start')) >= a
    AND sup_parse(JSONExtractRaw(p, 'start')) < sup_parse(JSONExtractRaw(p, 'end')) AND sup_parse(JSONExtractRaw(p, 'end')) <= z AND sup_received(p)
    AND (isNull(sup_assembled) OR JSONExtractRaw(p, 'observed_at') IN ('', 'null') OR sup_parse(JSONExtractRaw(p, 'observed_at')) <= sup_assembled), sup_pieces)) AS sup_eligible_pieces,
((pieces, at) -> arrayFirst(p -> sup_parse(JSONExtractRaw(p, 'start')) = at AND sup_parse(JSONExtractRaw(p, 'end'))
    = arrayMax(arrayMap(q -> sup_parse(JSONExtractRaw(q, 'end')), arrayFilter(q -> sup_parse(JSONExtractRaw(q, 'start')) = at, pieces))), pieces)) AS sup_next_piece,
((pieces, a) -> arrayFold((state, unused) -> if(sup_next_piece(pieces, state.1) = '', state,
    tuple(sup_parse(JSONExtractRaw(sup_next_piece(pieces, state.1), 'end')), arrayPushBack(state.2, sup_next_piece(pieces, state.1)))),
    range(length(pieces)), tuple(a, CAST([], 'Array(String)')))) AS sup_cover_fold,
((iid, a, z) -> sup_cover_fold(sup_eligible_pieces(iid, a, z), a)) AS sup_cover,
((a) -> if(toDateTime64(toStartOfMinute(a), 6, 'UTC') < a, sup_offset(toDateTime64(toStartOfMinute(a), 6, 'UTC'), 60.), a)) AS sup_ceil_minute,
((a) -> arrayConcat(if(sup_ceil_minute(a) > a, [tuple(a, least(sup_ceil_minute(a), sup_end))], []),
    if(toDateTime64(toStartOfMinute(sup_end), 6, 'UTC') < sup_end AND (sup_ceil_minute(a) = a OR toDateTime64(toStartOfMinute(sup_end), 6, 'UTC') >= least(sup_ceil_minute(a), sup_end)),
        [tuple(greatest(toDateTime64(toStartOfMinute(sup_end), 6, 'UTC'), a), sup_end)], []))) AS sup_boundary_ranges,
((iid, a) -> arrayFilter(r -> sup_iid(r) = iid AND sup_time(r) >= a AND sup_offset(sup_time(r), 60.) <= sup_end, sup_flow_bars)) AS sup_full_flow_rows,
((iid, a) -> tuple(length(arrayDistinct(arrayMap(sup_time, sup_full_flow_rows(iid, a)))) = greatest(0, toInt64(sup_seconds(sup_ceil_minute(a), toDateTime64(toStartOfMinute(sup_end), 6, 'UTC')) / 60))
    AND arrayAll(r -> JSONExtractRaw(r, 'complete') = 'true', sup_full_flow_rows(iid, a)) AND arrayAll(p -> sup_cover(iid, p.1, p.2).1 = p.2, sup_boundary_ranges(a)),
    arrayConcat(sup_full_flow_rows(iid, a), arrayFlatten(arrayMap(p -> sup_cover(iid, p.1, p.2).2, sup_boundary_ranges(a)))))) AS sup_flow_window,
((iid, a, key) -> if(sup_flow_window(iid, a).1, sup_sum(arrayMap(r -> sup_dec(JSONExtractRaw(r, key)), sup_flow_window(iid, a).2)), NULL)) AS sup_flow_sum,
((p, a) -> arrayExists(s -> JSONExtractString(s, 'metric_family') = 'aggressor' AND JSONExtractString(s, 'venue') = JSONExtractString(p, 'venue')
    AND JSONExtractString(s, 'market_type') != 'SPOT' AND (sup_iid(s) = '' OR sup_iid(s) = sup_iid(p)), sup_semantics) AND sup_flow_window(sup_iid(p), a).1
    AND arrayAll(k -> isNotNull(sup_flow_sum(sup_iid(p), a, k)), ['buy_qty', 'sell_qty', 'sum_sq_qty'])) AS sup_perp_flow_complete,
((iid, a) -> sup_sorted(arrayFilter(r -> sup_iid(r) = iid AND sup_time(r) >= a AND sup_time(r) < sup_end, sup_klines))) AS sup_usd_rows,
((iid, a) -> arrayFold((state, r) -> if(state.3 AND sup_time(r) = state.1 AND sup_bar_close(r) <= sup_end AND JSONExtractRaw(r, 'complete') = 'true'
    AND JSONExtractRaw(r, 'quote_volume_usd') NOT IN ('', 'null'), tuple(sup_bar_close(r), state.2 + ifNull(sup_dec(JSONExtractRaw(r, 'quote_volume_usd')), sup_zero), true),
        tuple(state.1, state.2, false)), sup_usd_rows(iid, a), tuple(a, sup_zero, true))) AS sup_usd_fold,
((iid, a) -> if(sup_usd_fold(iid, a).1 = sup_end AND notEmpty(sup_usd_rows(iid, a)), sup_usd_fold(iid, a).2,
    sup_flow_sum(iid, a, 'buy_notional') + sup_flow_sum(iid, a, 'sell_notional'))) AS sup_usd_volume,
(r -> multiplyDecimal(divideDecimal(sup_dec(JSONExtractRaw(r, 'mark_price')), nullIf(sup_dec(JSONExtractRaw(r, 'index_price')), sup_zero), 24) - sup_one, toDecimal256(10000, 24), 24)) AS sup_basis,
((iid) -> arrayFilter(r -> sup_iid(r) = iid AND sup_time(r) <= sup_end AND isNotNull(sup_dec(JSONExtractRaw(r, 'mark_price')))
    AND ifNull(sup_dec(JSONExtractRaw(r, 'index_price')), sup_zero) != sup_zero, sup_oi_rows)) AS sup_basis_rows,
((iid, a) -> arrayFilter(r -> sup_time(r) < a, sup_basis_rows(iid))) AS sup_historical_basis_rows,
((iid, a) -> if(notEmpty(sup_historical_basis_rows(iid, a)) AND sup_seconds(arrayMin(arrayMap(sup_time, sup_historical_basis_rows(iid, a))),
    arrayMax(arrayMap(sup_time, sup_historical_basis_rows(iid, a)))) >= sup_history_min, arrayMap(sup_basis, sup_historical_basis_rows(iid, a)), [])) AS sup_basis_history,
((iid, a) -> arrayFilter(r -> abs(sup_seconds(sup_time(r), a)) <= sup_number(sup_parameter('oi_sync_tolerance_s', '60')), sup_basis_rows(iid))) AS sup_initial_basis_rows,
((iid, a) -> sup_basis(arrayFirst(r -> abs(sup_seconds(sup_time(r), a)) = arrayMin(arrayMap(q -> abs(sup_seconds(sup_time(q), a)), sup_initial_basis_rows(iid, a))), sup_initial_basis_rows(iid, a)))) AS sup_initial_basis,
((p, a) -> __PER_VENUE__) AS sup_support_per_venue,
arrayFilter(b -> sup_is_spot(b) AND JSONExtractRaw(b, 'integrity_ok') = 'true', JSONExtractArrayRaw(sup_snapshot, 'books')) AS sup_support_books,
arrayDistinct(arrayMap(sup_venue, sup_support_books)) AS sup_book_venues,
((venue) -> sup_sorted(arrayFilter(b -> sup_venue(b) = venue, sup_support_books))) AS sup_venue_books,
(b -> sup_dec(JSONExtractRaw(b, 'depth', 'ask', 'p1', 'usd'))) AS sup_ask,
sup_number(sup_parameter('book_interval_s', '10')) AS sup_book_interval,
sup_number(sup_parameter('refill_window_s', '60')) AS sup_refill,
((venue, at, strict) -> arrayLast(b -> if(strict, sup_time(b) < at, sup_time(b) <= at), sup_venue_books(venue))) AS sup_book_before,
((venue, a) -> sup_book_before(venue, a, false)) AS sup_ask_start_book,
((venue) -> sup_book_before(venue, sup_end, false)) AS sup_ask_end_book,
((a) -> arrayMap(venue -> sup_ask(sup_ask_end_book(venue)) > sup_ask(sup_ask_start_book(venue, a)), arrayFilter(venue -> sup_time(sup_ask_start_book(venue, a)) >= sup_offset(a, -sup_book_interval)
    AND isNotNull(sup_ask(sup_ask_start_book(venue, a))) AND isNotNull(sup_ask(sup_ask_end_book(venue))), sup_book_venues))) AS sup_ask_builds,
((a) -> arrayMap(f -> tuple(f, sup_book_before(sup_venue(f), sup_time(f), true)), arrayFilter(f -> sup_is_spot(f) AND sup_time(f) >= a AND sup_time(f) < sup_end
    AND JSONExtractRaw(f, 'complete') = 'true' AND has(sup_book_venues, sup_venue(f)) AND isNotNull(sup_dec(JSONExtractRaw(f, 'buy_notional')))
    AND isNotNull(sup_dec(JSONExtractRaw(f, 'sell_notional'))) , sup_flow_bars))) AS sup_ask_candidates,
((a) -> arrayFilter(p -> p.2 != '' AND sup_time(p.2) >= sup_offset(sup_time(p.1), -sup_book_interval) AND isNotNull(sup_ask(p.2))
    AND sup_dec(JSONExtractRaw(p.1, 'buy_notional')) - sup_dec(JSONExtractRaw(p.1, 'sell_notional')) >= sup_ask(p.2)
    AND sup_offset(sup_time(p.1), sup_refill) <= sup_end, sup_ask_candidates(a))) AS sup_ask_tests,
((flow, pre) -> arrayExists(b -> sup_time(b) > sup_time(flow) AND sup_time(b) <= sup_offset(sup_time(flow), sup_refill)
    AND isNotNull(sup_ask(b)) AND sup_ask(b) >= sup_ask(pre), sup_venue_books(sup_venue(flow)))) AS sup_ask_recovered,
sup_support_stage AS (
    SELECT *, JSONExtractRaw(sup_spots, sup_window) AS sup_window_spot,
        coalesce(nullIf(if(JSONHas(sup_derivatives, 'windows'), JSONExtractRaw(sup_derivatives, 'windows', sup_window), JSONExtractRaw(sup_derivatives, sup_window)), ''), '{}') AS sup_window_derivative,
        arrayMap(p -> sup_support_per_venue(p, sup_window_start), JSONExtractArrayRaw(sup_window_derivative, 'per_venue')) AS sup_per_venue,
        ifNull(sup_sum(arrayMap(p -> sup_flow_sum(sup_iid(p), sup_window_start, 'buy_qty') - sup_flow_sum(sup_iid(p), sup_window_start, 'sell_qty'),
            arrayFilter(p -> sup_perp_flow_complete(p, sup_window_start), sup_per_venue))), sup_zero) AS sup_perp_cvd,
        ifNull(sup_sum(arrayMap(p -> sup_flow_sum(sup_iid(p), sup_window_start, 'sum_sq_qty'), arrayFilter(p -> sup_perp_flow_complete(p, sup_window_start), sup_per_venue))), sup_zero) AS sup_perp_q,
        arrayFilter(r -> sup_time(r) >= sup_window_start AND sup_time(r) < sup_end AND has(arrayMap(sup_iid, arrayFilter(p -> JSONExtractString(p, 'liq_coverage') = 'COMPLETE', sup_per_venue)), sup_iid(r)),
            JSONExtractArrayRaw(sup_snapshot, 'liquidations')) AS sup_window_liquidations,
        sup_sum(arrayMap(r -> sup_dec(JSONExtractRaw(r, 'notional_usd')), sup_window_liquidations)) AS sup_liquidation_total,
        sup_sum(arrayMap(r -> sup_dec(JSONExtractRaw(r, 'notional_usd')), arrayFilter(r -> sup_time(r) < sup_offset(sup_window_start, sup_seconds(sup_window_start, sup_end) / 2.), sup_window_liquidations))) AS sup_liquidation_first,
        __FEATURES__ AS sup_features,
        sup_patch(sup_window_derivative, ['per_venue', 'support_features'], [sup_array(sup_per_venue), sup_features]) AS sup_supported_derivative
    FROM sup_supply_stage
)
""".strip()
        .replace("__VOLUME__", volume)
        .replace("__PER_VENUE__", per_venue)
        .replace("__FEATURES__", features)
    )


def _output():
    return r"""
sup_window_output AS (
    SELECT sup_window,
        if(sup_window = 'EVENT' AND NOT sup_has_anchor, concat('{"window":"EVENT","start":null,"cutoff":', toJSONString(sup_iso(sup_end)), ',"reason":"INCOMPLETE"}'),
            sup_patch(sup_supply, ['window', 'start', 'cutoff'], [toJSONString(sup_window), toJSONString(sup_iso(sup_window_start)), toJSONString(sup_iso(sup_end))])) AS sup_supply_json,
        if(sup_window = 'EVENT' AND NOT sup_has_anchor, 'null', sup_features) AS sup_features_json,
        if(sup_window = 'EVENT' AND NOT sup_has_anchor, sup_window_derivative, sup_supported_derivative) AS sup_derivative_json
    FROM sup_support_stage
),
sup_output AS (
    SELECT sup_patch(arrayFirst(p -> p.1 = sup_primary, sup_packs).2, ['windows'], [concat('{', arrayStringConcat(arrayMap(p -> concat(toJSONString(p.1), ':', p.2), sup_packs), ','), '}')]) AS sup_supply_result,
        sup_patch(arrayFirst(p -> p.1 = sup_primary, sup_packs).4, ['windows'], [concat('{', arrayStringConcat(arrayMap(p -> concat(toJSONString(p.1), ':', p.4), sup_packs), ','), '}')]) AS sup_derivatives_result,
        concat('{', arrayStringConcat(arrayMap(p -> concat(toJSONString(p.1), ':', p.3), sup_packs), ','), '}') AS sup_features_result
    FROM (SELECT arraySort(p -> indexOf(sup_windows, p.1), groupArray(tuple(sup_window, sup_supply_json, sup_features_json, sup_derivative_json))) AS sup_packs FROM sup_window_output)
)
""".strip()


def select_sql():
    result = _object(
        supply="sup_supply_result",
        derivatives="sup_derivatives_result",
        support_features="sup_features_result",
        data_confidence="sup_confidence",
    )
    return f"SELECT {result} AS result FROM sup_output CROSS JOIN sup_conf_output"


def query(snapshot, config=None, *, spots=None, derivatives=None, liquidity=None, relative=None):
    inputs = {
        "sup_snapshot": snapshot,
        "sup_config": config or {},
        "sup_spots": spots if spots is not None else snapshot.get("_spots", {}),
        "sup_derivatives": derivatives if derivatives is not None else snapshot.get("_derivatives", {}),
        "sup_liquidity": liquidity if liquidity is not None else snapshot.get("_liquidity", {}),
        "sup_relative": relative if relative is not None else snapshot.get("_relative", {}),
    }
    parameters = {
        name: (
            parameter_string(json.dumps(_transport(value), ensure_ascii=False, allow_nan=False)),
            "String",
        )
        for name, value in inputs.items()
    }
    parameters["sup_supply_preimages"] = (
        parameter_string(
            json.dumps([json.dumps(_transport(row), sort_keys=True) for row in snapshot.get("supply", [])])
        ),
        "String",
    )
    return f"WITH {ctes()}\n{select_sql()}", parameters


def _supply():
    inventory_per = _object(
        venue=_raw("JSONExtractRaw(n, 'venue')"),
        chainid=_raw("JSONExtractRaw(n, 'chainid')"),
        contract=_raw("JSONExtractRaw(n, 'contract')"),
        qty_native=_money("sup_dec(JSONExtractRaw(n, 'qty_native'))"),
        chg_native_24h=_money(
            "sup_dec(JSONExtractRaw(n, 'qty_native')) - sup_dec(JSONExtractRaw(o, 'qty_native'))"
        ),
        chg_pct=_json(
            "sup_ratio(sup_dec(JSONExtractRaw(n, 'qty_native')) - sup_dec(JSONExtractRaw(o, 'qty_native')), sup_dec(JSONExtractRaw(o, 'qty_native')))"
        ),
    )
    inventory = _object(
        chg_native_24h=_money("sup_inventory_change"),
        chg_pct=_json("sup_inventory_pct"),
        normal_range="sup_normal(sup_inventory_history)",
        per_venue="sup_array(arrayMap(p -> sup_inventory_per(p.1, p.2), sup_inventory_pairs))",
        coverage=_object(
            complete_balances=_json("length(sup_balances(sup_inventory_current))"),
            observed_balances=_json("length(JSONExtractArrayRaw(sup_inventory_current, 'balances'))"),
            comparable_balances=_json("length(sup_inventory_cohort)"),
            coverage_errors="if(JSONHas(sup_inventory_current, 'coverage_errors'), sup_raw(JSONExtractRaw(sup_inventory_current, 'coverage_errors')), '[]')",
            unsupported_venues="if(JSONHas(sup_inventory_current, 'unsupported_venues'), sup_raw(JSONExtractRaw(sup_inventory_current, 'unsupported_venues')), '[]')",
        ),
        reason="if(isNotNull(sup_inventory_change), 'null', '\"INCOMPLETE\"')",
        observed_at=_json("sup_iso(sup_time(sup_inventory_current))"),
    )
    pool = _patch(
        "p",
        borrow_apr="'null'",
        borrow_apr_reason="'\"NOT_ESTIMABLE\"'",
        chg_24h=_json(
            "sup_number(JSONExtractRaw(p, 'utilization')) - sup_number(JSONExtractRaw(o, 'utilization'))"
        ),
    )
    chain = _object(
        chain=_raw("JSONExtractRaw(r, 'chain')"),
        supply_usd=_money("sup_chain_value(r, sup_end)"),
        supply_chg_24h=_money(
            "sup_chain_value(r, sup_end) - sup_chain_value(r, sup_offset(sup_end, -86400.))"
        ),
        supply_chg_7d=_money(
            "sup_chain_value(r, sup_end) - sup_chain_value(r, sup_offset(sup_end, -604800.))"
        ),
        reason="if(isNotNull(sup_chain_value(r, sup_end)), 'null', '\"INCOMPLETE\"')",
    )
    chart = _object(
        provider_timestamp=_json("sup_text(JSONExtractRaw(p, 1))"),
        value=_money("sup_fundamental_dec(JSONExtractRaw(p, 2))"),
        provider_period="'\"UNSPECIFIED\"'",
    )
    period = _object(
        provider_period=_json("p.2"),
        value=_money(
            "if(sup_available(m) AND sup_available(sup_fundamental), sup_fundamental_dec(JSONExtractRaw(m, p.1)), NULL)"
        ),
        status="if(sup_available(m) AND sup_available(sup_fundamental) AND isNotNull(sup_fundamental_dec(JSONExtractRaw(m, p.1))), '\"OBSERVED\"', '\"UNAVAILABLE\"')",
    )
    protocol = _object(
        protocol=_json("slug"),
        provider="'\"DefiLlama\"'",
        unit="'\"USD\"'",
        source_url=_raw("JSONExtractRaw(m, 'source_url')"),
        periods="sup_array(arrayMap(p -> sup_period(m, p), [('total24h', '24h'), ('total30d', '30d')]))",
        chart_available=_bool("notEmpty(JSONExtractArrayRaw(m, 'totalDataChart'))"),
        latest_chart_point="sup_latest_chart(m)",
        reason=_raw("JSONExtractRaw(m, 'reason')"),
    )
    coverage = _object(
        observed=_json("arrayCount(p -> JSONExtractRaw(p, 'periods', i, 'value') != 'null', sup_protocols)"),
        expected=_json("length(sup_protocols)"),
        complete=_bool(
            "notEmpty(sup_protocols) AND arrayAll(p -> JSONExtractRaw(p, 'periods', i, 'value') != 'null', sup_protocols)"
        ),
    )
    holders = _object(
        status="toJSONString(if(empty(sup_protocols), 'UNAVAILABLE', if(isNotNull(sup_cadence('supply', sup_fundamental).1) AND sup_seconds(sup_time(sup_fundamental), sup_end) > sup_cadence('supply', sup_fundamental).1, 'STALE', 'OBSERVED')))",
        availability="toJSONString(multiIf(arrayAll(i -> JSONExtractBool(sup_revenue_coverage(i), 'complete'), [1, 2]), 'COMPLETE', arrayExists(p -> JSONExtractRaw(p, 'latest_chart_point') != 'null' OR arrayExists(v -> JSONExtractRaw(v, 'value') != 'null', JSONExtractArrayRaw(p, 'periods')), sup_protocols), 'PARTIAL', 'UNAVAILABLE'))",
        protocols="sup_array(sup_protocols)",
        coverage=_object(**{"24h": "sup_revenue_coverage(1)", "30d": "sup_revenue_coverage(2)"}),
        as_of=_json("sup_iso(sup_time(sup_fundamental))"),
        capture_received_at=_raw("JSONExtractRaw(sup_fundamental, 'received_ts')"),
        capture_availability="if(JSONExtractRaw(sup_fundamental, 'received_ts') IN ('', 'null'), '\"UNKNOWN\"', '\"KNOWN\"')",
        age_s=_json("sup_seconds(sup_time(sup_fundamental), sup_end)"),
        cadence_s=_json("sup_cadence('supply', sup_fundamental).1"),
        cadence_basis=_json("sup_cadence('supply', sup_fundamental).2"),
        identity_evidence=_raw("JSONExtractRaw(sup_fundamental, 'identity_evidence')"),
        source_observation_sha256=_json(
            "lower(hex(SHA256(JSONExtractString(sup_supply_preimages, toUInt64(sup_fundamental_index)))))"
        ),
        reason=_raw("JSONExtractRaw(sup_fundamental, 'reason')"),
        inference_limits='\'["PROVIDER_REVENUE_IS_NOT_PROOF_OF_TOKEN_CAPTURE","PER_PROTOCOL_VALUES_NOT_SUMMED_PARENT_CHILD_SCOPE_MAY_OVERLAP"]\'',
    )
    flow_wrapper = _patch(
        "f",
        **{
            "class": "if(JSONHas(f, 'classification'), sup_raw(JSONExtractRaw(f, 'classification')), '\"AMBIGUOUS\"')",
            "qty": _money("sup_dec(JSONExtractRaw(f, 'qty_native'))"),
        },
    )
    direct_flow = _patch(
        "r", **{"class": "if(JSONHas(r, 'class'), sup_raw(JSONExtractRaw(r, 'class')), '\"AMBIGUOUS\"')"}
    )
    unlock = _patch(
        "u",
        inference="'\"POTENTIAL_SUPPLY_IS_NOT_A_SALE\"'",
        pct_circulating=_raw("JSONExtractRaw(u, 'pct_circulating')"),
        recipient_class=_raw("JSONExtractRaw(u, 'recipient_class')"),
        transferable=_raw("JSONExtractRaw(u, 'transferable')"),
    )
    supply = _object(
        holders_revenue="sup_holders_revenue",
        exchange_inventory="if(sup_inventory_current != '' AND sup_available(sup_inventory_current), "
        + _patch("sup_inventory", window="'\"W24H\"'")
        + ", sup_missing)",
        flows="sup_array(sup_flows(sup_window_start))",
        unlocks_next_14d="sup_array(sup_unlocks)",
        lending="sup_lending",
        stablecoin_chain="if(empty(sup_chains), sup_missing, sup_chains[1])",
        chain_contexts="sup_array(sup_chains)",
        unavailable="sup_unavailable",
        supply_in_evidence="toJSONString(if(sup_inventory_evidence OR sup_flow_evidence(sup_window_start), 'TRUE', if(sup_supply_available(sup_window_start), 'FALSE', 'UNKNOWN')))",
    )
    return (
        r"""
sup_sorted(arrayFilter(r -> isNotNull(sup_time(r)) AND sup_time(r) <= sup_end, JSONExtractArrayRaw(sup_snapshot, 'supply'))) AS sup_supply_rows,
((k) -> arrayLast(r -> has(k, sup_kind(r)), sup_supply_rows)) AS sup_latest_kind,
arrayFilter(r -> sup_kind(r) IN ('INVENTORY', 'EXCHANGE_INVENTORY'), sup_supply_rows) AS sup_inventories,
sup_inventories[-1] AS sup_inventory_current,
sup_asof(sup_inventories, sup_offset(sup_end, -86400.), 21600.) AS sup_inventory_previous,
(x -> tuple(sup_raw(JSONExtractRaw(x, 'venue')), sup_raw(JSONExtractRaw(x, 'chainid')), sup_raw(JSONExtractRaw(x, 'contract')), sup_raw(JSONExtractRaw(x, 'wallet_list_commit')))) AS sup_balance_key,
(x -> arrayFilter(b -> JSONExtractRaw(b, 'complete') = 'true' AND isNotNull(sup_dec(JSONExtractRaw(b, 'qty_native'))), JSONExtractArrayRaw(x, 'balances'))) AS sup_complete_balances,
(x -> arrayMap(k -> arrayLast(b -> sup_balance_key(b) = k, sup_complete_balances(x)), arrayDistinct(arrayMap(sup_balance_key, sup_complete_balances(x))))) AS sup_balances,
(x -> if(x = 'null', 'None', if(JSONType(x) = 'String', concat('\'', sup_text(x), '\''), x))) AS sup_python_key,
(x -> concat('(', arrayStringConcat(arrayMap(sup_python_key, [x.1, x.2, x.3, x.4]), ', '), ')')) AS sup_balance_sort,
arraySort(sup_balance_sort, arrayFilter(k -> sup_inventory_previous != '' AND sup_available(sup_inventory_previous) AND has(arrayMap(sup_balance_key, sup_balances(sup_inventory_previous)), k), arrayMap(sup_balance_key, sup_balances(sup_inventory_current)))) AS sup_inventory_cohort,
((r, keys) -> if(notEmpty(keys) AND arrayAll(k -> arrayExists(b -> sup_balance_key(b) = k, sup_balances(r)), keys),
    sup_sum(arrayMap(k -> sup_dec(JSONExtractRaw(arrayLast(b -> sup_balance_key(b) = k, sup_balances(r)), 'qty_native')), keys)), NULL)) AS sup_balance_total,
arrayMap(k -> tuple(arrayLast(b -> sup_balance_key(b) = k, sup_balances(sup_inventory_current)),
    arrayLast(b -> sup_balance_key(b) = k, sup_balances(sup_inventory_previous))), sup_inventory_cohort) AS sup_inventory_pairs,
sup_balance_total(sup_inventory_current, sup_inventory_cohort) - if(sup_available(sup_inventory_previous), sup_balance_total(sup_inventory_previous, sup_inventory_cohort), NULL) AS sup_inventory_change,
sup_ratio(sup_inventory_change, sup_balance_total(sup_inventory_previous, sup_inventory_cohort)) AS sup_inventory_pct,
((a) -> if(sup_seconds(sup_time(sup_inventories[1]), sup_time(sup_inventory_current)) >= sup_history_min,
    arrayMap(p -> sup_ratio(p.1, p.2) - 1, arrayFilter(p -> isNotNull(p.1) AND isNotNull(p.2) AND p.2 != sup_zero,
        arrayMap(r -> tuple(sup_balance_total(r, sup_inventory_cohort), sup_balance_total(sup_asof(sup_inventories, sup_offset(sup_time(r), -86400.), 21600.), sup_inventory_cohort)),
            arrayFilter(r -> sup_time(r) >= sup_offset(sup_end, -sup_lookback) AND sup_time(r) < a, sup_inventories)))), [])) AS sup_inventory_history_at,
((n, o) -> __INVENTORY_PER__) AS sup_inventory_per,
sup_latest_kind(['WALLET_FLOWS']) AS sup_wallet,
((f) -> __FLOW_WRAPPER__) AS sup_wrapped_flow,
((r) -> __DIRECT_FLOW__) AS sup_direct_flow,
((a) -> if(sup_wallet != '' AND sup_available(sup_wallet), arrayMap(sup_wrapped_flow,
    arrayFilter(f -> sup_parse(JSONExtractRaw(f, 'ts')) >= a AND sup_parse(JSONExtractRaw(f, 'ts')) < sup_end, JSONExtractArrayRaw(sup_wallet, 'flows'))), [])) AS sup_wrapped_flows,
((a) -> arrayConcat(sup_wrapped_flows(a),
    arrayMap(sup_direct_flow, arrayFilter(r -> sup_kind(r) IN ('FLOW', 'WALLET_FLOW') AND sup_available(r) AND sup_time(r) >= a AND sup_time(r) < sup_end, sup_supply_rows)))) AS sup_flows,
((a) -> sup_wallet != '' AND sup_available(sup_wallet) AND arrayExists(f -> sup_parse(JSONExtractRaw(f, 'ts')) >= a AND sup_parse(JSONExtractRaw(f, 'ts')) < sup_end
    AND JSONExtractString(f, 'classification') IN ('UNLOCK_DISTRIBUTION', 'TREASURY_INTERNAL', 'MM_INVENTORY') AND JSONExtractRaw(f, 'reaches_exchange') = 'true', JSONExtractArrayRaw(sup_wallet, 'flows'))
    OR arrayExists(r -> sup_kind(r) IN ('FLOW', 'WALLET_FLOW') AND sup_available(r) AND sup_time(r) >= a AND sup_time(r) < sup_end
        AND JSONExtractString(r, 'class') IN ('UNLOCK_DISTRIBUTION', 'TREASURY_INTERNAL', 'MM_INVENTORY') AND JSONExtractRaw(r, 'to_exchange') = 'true', sup_supply_rows)) AS sup_flow_evidence,
sup_latest_kind(['UNLOCKS']) AS sup_unlock_wrapper,
(u -> sup_parse(if(JSONHas(u, 'date'), JSONExtractRaw(u, 'date'), JSONExtractRaw(u, 'ts')))) AS sup_unlock_time,
arrayConcat(if(sup_unlock_wrapper != '' AND sup_available(sup_unlock_wrapper), JSONExtractArrayRaw(sup_unlock_wrapper, 'unlocks'), []),
    arrayFilter(r -> sup_kind(r) = 'UNLOCK' AND sup_available(r), sup_supply_rows)) AS sup_schedules,
arrayMap(u -> __UNLOCK__, arrayFilter(u -> sup_unlock_time(u) >= sup_end AND sup_unlock_time(u) <= sup_offset(sup_end, 1209600.), sup_schedules)) AS sup_unlocks,
sup_latest_kind(['LENDING']) AS sup_lend,
sup_asof(arrayFilter(r -> sup_kind(r) = 'LENDING' AND sup_available(r), sup_supply_rows), sup_offset(sup_end, -86400.), 1800.) AS sup_lend_previous,
((p, o) -> __POOL__) AS sup_pool,
if(sup_lend != '' AND sup_available(sup_lend),
    if(JSONHas(sup_lend, 'pools'), sup_patch(sup_lend, ['pools', 'borrow_apr', 'reason', 'window'],
        [sup_array(arrayMap(p -> sup_pool(p, arrayLast(o -> JSONExtractRaw(o, 'pool_id') = JSONExtractRaw(p, 'pool_id'), JSONExtractArrayRaw(sup_lend_previous, 'pools'))), JSONExtractArrayRaw(sup_lend, 'pools'))),
            'null', '"APY_IS_NOT_APR"', '"W24H"']), sup_patch(sup_lend, ['window'], ['"W24H"'])), sup_missing) AS sup_lending,
arrayFilter(r -> sup_kind(r) IN ('CHAIN_LIQUIDITY', 'STABLECOIN_CHAIN'), sup_supply_rows) AS sup_chain_rows,
arrayMap(k -> arrayLast(r -> sup_raw(JSONExtractRaw(r, 'chain')) = k, sup_chain_rows), arrayDistinct(arrayMap(r -> sup_raw(JSONExtractRaw(r, 'chain')), sup_chain_rows))) AS sup_chain_latest,
((r, t) -> sup_dec(JSONExtractRaw(sup_sorted(arrayFilter(p -> sup_time(p) <= t AND sup_seconds(sup_time(p), t) <= 86400., JSONExtractArrayRaw(r, 'series')))[-1], 'supply_usd'))) AS sup_chain_value,
arrayMap(r -> if(JSONHas(r, 'series'), __CHAIN__, r), arrayFilter(sup_available, sup_chain_latest)) AS sup_chains,
arrayLast(p -> sup_kind(p.2) = 'FUNDAMENTALS' AND isNotNull(sup_time(p.2)) AND sup_time(p.2) <= sup_end
    AND JSONType(JSONExtractRaw(p.2, 'ts')) != 'Bool' AND (JSONExtractRaw(p.2, 'received_ts') IN ('', 'null')
        OR JSONType(JSONExtractRaw(p.2, 'received_ts')) != 'Bool' AND sup_parse(JSONExtractRaw(p.2, 'received_ts')) <= sup_end),
    arraySort(p -> tuple(sup_time(p.2), p.1), arrayMap((r, i) -> tuple(i, r), JSONExtractArrayRaw(sup_snapshot, 'supply'), arrayEnumerate(JSONExtractArrayRaw(sup_snapshot, 'supply'))))) AS sup_fundamental_pair,
sup_fundamental_pair.1 AS sup_fundamental_index,
sup_fundamental_pair.2 AS sup_fundamental,
((m, p) -> __PERIOD__) AS sup_period,
((p) -> __CHART__) AS sup_chart,
((m) -> arrayFilter(p -> length(JSONExtractArrayRaw(p)) = 2 AND JSONType(JSONExtractRaw(p, 1)) != 'Bool' AND JSONType(JSONExtractRaw(p, 2)) != 'Bool'
    AND isNotNull(sup_fundamental_dec(JSONExtractRaw(p, 2))) AND sup_parse(JSONExtractRaw(p, 1)) <= sup_end, JSONExtractArrayRaw(m, 'totalDataChart'))) AS sup_chart_points,
((m) -> if(sup_available(m) AND sup_available(sup_fundamental) AND notEmpty(sup_chart_points(m)),
    sup_chart(arrayFirst(p -> sup_parse(JSONExtractRaw(p, 1)) = arrayMax(arrayMap(q -> sup_parse(JSONExtractRaw(q, 1)), sup_chart_points(m))), sup_chart_points(m))), 'null')) AS sup_latest_chart,
((slug, m) -> __PROTOCOL__) AS sup_protocol,
arrayMap(p -> sup_protocol(p.1, JSONExtractRaw(p.2, 'dailyHoldersRevenue')), arraySort(p -> p.1, JSONExtractKeysAndValuesRaw(JSONExtractRaw(sup_fundamental, 'protocols')))) AS sup_protocols,
((i) -> __COVERAGE__) AS sup_revenue_coverage,
if(sup_fundamental != '', __HOLDERS__, concat('{"status":"UNAVAILABLE","protocols":[],"reason":',
    if(arrayExists(r -> sup_kind(r) = 'FUNDAMENTALS' AND sup_time(r) > sup_end, JSONExtractArrayRaw(sup_snapshot, 'supply')), '"ONLY_FUTURE_FUNDAMENTALS"', '"NO_CAPTURED_FUNDAMENTALS"'), '}')) AS sup_holders_revenue,
concat('{', arrayStringConcat(arrayFilter(x -> x != '', [
    if(sup_inventory_current = '' OR NOT sup_available(sup_inventory_current), concat('"exchange_inventory":', if(sup_inventory_current != '', coalesce(nullIf(JSONExtractRaw(sup_inventory_current, 'reason'), ''), '"INCOMPLETE"'), '"INCOMPLETE"')), ''),
    if(sup_wallet != '' AND NOT sup_available(sup_wallet), concat('"flows":', coalesce(nullIf(JSONExtractRaw(sup_wallet, 'reason'), ''), '"INCOMPLETE"')),
        if(sup_wallet = '' AND NOT arrayExists(r -> sup_kind(r) IN ('FLOW', 'WALLET_FLOW'), sup_supply_rows), '"flows":"INCOMPLETE"', '')),
    if(sup_unlock_wrapper != '' AND NOT sup_available(sup_unlock_wrapper), concat('"unlocks":', coalesce(nullIf(JSONExtractRaw(sup_unlock_wrapper, 'reason'), ''), '"INCOMPLETE"')),
        if(sup_unlock_wrapper = '' AND NOT arrayExists(r -> sup_kind(r) = 'UNLOCK', sup_supply_rows), '"unlocks":"INCOMPLETE"', '')),
    if(sup_lend = '' OR NOT sup_available(sup_lend), concat('"lending":', coalesce(nullIf(JSONExtractRaw(sup_lend, 'reason'), ''), '"INCOMPLETE"')), ''),
    if(empty(sup_chain_latest), '"stablecoin_chain":"INCOMPLETE"', if(arrayExists(r -> NOT sup_available(r), sup_chain_latest),
        concat('"stablecoin_chain":', coalesce(nullIf(JSONExtractRaw(arrayLast(r -> NOT sup_available(r), sup_chain_latest), 'reason'), ''), '"INCOMPLETE"')), ''))]), ','), '}') AS sup_unavailable,
((a) -> sup_inventory_current != '' AND sup_available(sup_inventory_current) OR sup_wallet != '' AND sup_available(sup_wallet)
    OR sup_unlock_wrapper != '' AND sup_available(sup_unlock_wrapper) OR notEmpty(sup_unlocks) OR sup_lend != '' AND sup_available(sup_lend)
    OR notEmpty(sup_chains) OR arrayExists(r -> sup_kind(r) IN ('FLOW', 'WALLET_FLOW') AND sup_available(r) AND sup_time(r) >= a AND sup_time(r) < sup_end, sup_supply_rows)) AS sup_supply_available,
sup_supply_stage AS (
    SELECT sup_window, sup_window_start,
        if(sup_window = 'EVENT' AND NOT sup_has_anchor, 0, length(sup_wrapped_flows(sup_window_start))) AS sup_wrapped_flow_count,
        sup_inventory_history_at(sup_window_start) AS sup_inventory_history,
        if(sup_inventory_current != '' AND sup_available(sup_inventory_current),
            if(JSONHas(sup_inventory_current, 'balances'), __INVENTORY__, sup_inventory_current), sup_missing) AS sup_inventory,
        if(sup_inventory_current != '' AND sup_available(sup_inventory_current),
            if(JSONHas(sup_inventory_current, 'balances'), isNotNull(sup_inventory_pct) AND notEmpty(sup_inventory_history)
                AND sup_inventory_pct > sup_quantile(sup_inventory_history, sup_number(sup_quantiles[2])),
                sup_number(JSONExtractRaw(sup_inventory_current, 'chg_pct')) > 0 AND length(JSONExtractArrayRaw(sup_inventory_current, 'normal_range')) = 2
                AND (sup_number(JSONExtractRaw(sup_inventory_current, 'chg_pct')) < sup_number(JSONExtractRaw(sup_inventory_current, 'normal_range', 1))
                    OR sup_number(JSONExtractRaw(sup_inventory_current, 'chg_pct')) > sup_number(JSONExtractRaw(sup_inventory_current, 'normal_range', 2)))), false) AS sup_inventory_evidence,
        __SUPPLY__ AS sup_supply
    FROM (SELECT arrayJoin(sup_windows) AS sup_window, sup_start(sup_window) AS sup_window_start)
)
""".strip()
        .replace("__INVENTORY_PER__", inventory_per)
        .replace("__INVENTORY__", inventory)
        .replace("__POOL__", pool)
        .replace("__CHAIN__", chain)
        .replace("__PERIOD__", period)
        .replace("__CHART__", chart)
        .replace("__PROTOCOL__", protocol)
        .replace("__COVERAGE__", coverage)
        .replace("__HOLDERS__", holders)
        .replace("__FLOW_WRAPPER__", flow_wrapper)
        .replace("__DIRECT_FLOW__", direct_flow)
        .replace("__UNLOCK__", unlock)
        .replace("__SUPPLY__", supply)
    )
