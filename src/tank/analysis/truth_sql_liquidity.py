from decimal import Decimal

from tank.clickhouse_market import parameter_string
from tank.util import canonical_json, jsonable


def _transport(value):
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {key: _transport(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
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


def _money(expression):
    return f"if(isNull({expression}), 'null', toJSONString(toString({expression})))"


def _bool(expression):
    return f"toJSONString(CAST(({expression}), 'Nullable(Bool)'))"


def _raw(expression):
    return f"if({expression} IN ('', 'null'), 'null', {expression})"


def _input_ctes():
    return r"""
lq_input AS (
    SELECT lq_snapshot AS input_snapshot, lq_config AS input_config,
        lq_spot AS input_spot, lq_relative AS input_relative
),
lq_input_fields AS (
    SELECT JSONExtractString(i.input_snapshot, 'token_id') AS ctx_token,
        if(JSONExtractString(i.input_snapshot, '_primary') = '', 'W4H', JSONExtractString(i.input_snapshot, '_primary')) AS ctx_primary,
        JSONExtractRaw(i.input_snapshot, 'cutoff') AS ctx_end_raw,
        JSONExtractRaw(i.input_snapshot, '_start') AS ctx_private_start_raw,
        JSONExtractRaw(i.input_snapshot, 'start') AS ctx_start_raw,
        JSONExtractRaw(i.input_snapshot, '_event_anchor') AS ctx_anchor_raw,
        JSONHas(i.input_snapshot, '_event_anchor') AS ctx_has_anchor,
        JSONExtractRaw(i.input_snapshot, 'anchor', 'ts') AS ctx_event_raw,
        JSONExtractArrayRaw(i.input_snapshot, 'books') AS ctx_all_books,
        JSONExtractArrayRaw(i.input_snapshot, 'flow_bars') AS ctx_all_flows,
        JSONExtractArrayRaw(i.input_spot, 'venues') AS ctx_spot_venues,
        i.input_relative AS ctx_relative,
        if(JSONHas(i.input_config, 'depth_band_pct'), JSONExtractRaw(i.input_config, 'depth_band_pct'), '1') AS ctx_band_raw,
        if(JSONHas(i.input_config, 'book_interval_s'), JSONExtractRaw(i.input_config, 'book_interval_s'), '10') AS ctx_cadence_raw,
        if(JSONHas(i.input_config, 'refill_window_s'), JSONExtractRaw(i.input_config, 'refill_window_s'), '60') AS ctx_refill_raw,
        if(JSONHas(i.input_config, 'resilience_window_min'), JSONExtractRaw(i.input_config, 'resilience_window_min'), '15') AS ctx_resilience_raw,
        if(JSONHas(i.input_config, 'impact_tiers_usd'), JSONExtractRaw(i.input_config, 'impact_tiers_usd'), '[100000,1000000,5000000]') AS ctx_tiers_raw
    FROM lq_input AS i
),
lq_context_values AS (
    SELECT f.*, lq_parse(f.ctx_end_raw) AS ctx_end,
        lq_number(f.ctx_band_raw) AS ctx_band,
        lq_number(f.ctx_cadence_raw) AS ctx_cadence,
        lq_number(f.ctx_refill_raw) AS ctx_refill,
        lq_number(f.ctx_resilience_raw) * 60 AS ctx_resilience,
        arrayDistinct(arrayMap(v -> JSONExtractString(v, 'venue'), f.ctx_spot_venues)) AS ctx_eligible,
        arrayMap(x -> lq_dec(x), JSONExtractArrayRaw(f.ctx_tiers_raw)) AS ctx_base_tiers
    FROM lq_input_fields AS f
),
lq_context_start AS (
    SELECT v.*, coalesce(lq_parse(v.ctx_private_start_raw), lq_parse(v.ctx_start_raw),
        if(v.ctx_primary = 'EVENT', coalesce(lq_parse(v.ctx_anchor_raw), lq_parse(v.ctx_event_raw)),
            v.ctx_end - toIntervalSecond(multiIf(v.ctx_primary = 'W15M', 900, v.ctx_primary = 'W1H', 3600,
                v.ctx_primary = 'W24H', 86400, 14400)))) AS ctx_start,
        multiIf(v.ctx_band = 0.5, 'p0_5', v.ctx_band = 1, 'p1', v.ctx_band = 2, 'p2', '') AS ctx_band_key
    FROM lq_context_values AS v
),
lq_static_context AS (
    SELECT s.ctx_token, s.ctx_primary, s.ctx_end, s.ctx_start,
        if(s.ctx_has_anchor, lq_parse(s.ctx_anchor_raw), s.ctx_start) AS ctx_anchor,
        s.ctx_band, s.ctx_band_key, s.ctx_band_raw, s.ctx_cadence, s.ctx_refill, s.ctx_resilience,
        s.ctx_all_books, s.ctx_all_flows, s.ctx_eligible, s.ctx_base_tiers, s.ctx_relative,
        s.ctx_spot_venues
    FROM lq_context_start AS s
),
"""


def _book_ctes():
    return r"""
lq_flow_context AS (
    SELECT s.ctx_token, s.ctx_primary, s.ctx_end, s.ctx_start, s.ctx_anchor,
        s.ctx_band, s.ctx_band_key, s.ctx_band_raw, s.ctx_cadence, s.ctx_refill,
        s.ctx_resilience, s.ctx_all_books, s.ctx_all_flows, s.ctx_relative, s.lq_tiers,
        arrayConcat(arrayFilter(flow -> if(lq_is_spot(flow) AND has(s.ctx_eligible, lq_venue(flow)),
            lq_time(flow) >= s.ctx_start AND lq_time(flow) + INTERVAL 1 MINUTE <= s.ctx_end
                AND JSONExtractRaw(flow, 'complete') = 'true', false), s.ctx_all_flows),
            arrayFlatten(arrayMap(v -> arrayFlatten(arrayMap(segment -> arrayMap(row ->
                JSONMergePatch(row, concat('{"minute_start":', JSONExtractRaw(row, 'start'),
                    ',"segment_end":', JSONExtractRaw(row, 'end'), ',"venue":', toJSONString(JSONExtractString(v, 'venue')), '}')),
                JSONExtractArrayRaw(segment, 'aggregates')), JSONExtractArrayRaw(v, 'boundary_segments'))), s.ctx_spot_venues))) AS ctx_flows
    FROM lq_tier_values AS s
),
lq_book_metadata AS (
    SELECT c.ctx_token, c.ctx_primary, c.ctx_end, c.ctx_start, c.ctx_anchor,
        c.ctx_band, c.ctx_band_key, c.ctx_band_raw, c.ctx_cadence, c.ctx_refill,
        c.ctx_resilience, c.ctx_all_books, c.ctx_all_flows, c.ctx_relative, c.lq_tiers, c.ctx_flows,
        arrayMap(book -> tuple(book, lq_venue(book), lq_is_spot(book),
        NOT JSONHas(book, 'token_id') OR JSONExtractString(book, 'token_id') = c.ctx_token,
        JSONExtractRaw(book, 'integrity_ok'), lq_time_raw(book)), c.ctx_all_books) AS book_metadata
    FROM lq_flow_context AS c
),
lq_book_times AS (
    SELECT m.ctx_token, m.ctx_primary, m.ctx_end, m.ctx_start, m.ctx_anchor,
        m.ctx_band, m.ctx_band_key, m.ctx_band_raw, m.ctx_cadence, m.ctx_refill,
        m.ctx_resilience, m.ctx_all_books, m.ctx_all_flows, m.ctx_relative, m.lq_tiers, m.ctx_flows,
        arrayMap(book -> tuple(book.1, book.2,
        if(book.3 AND book.4, lq_required_time(book.6), CAST(NULL, 'Nullable(DateTime64(6, \'UTC\'))')),
        book.3 AND book.4, book.5), m.book_metadata) AS book_times
    FROM lq_book_metadata AS m
),
lq_book_admitted AS (
    SELECT t.ctx_token, t.ctx_primary, t.ctx_end, t.ctx_start, t.ctx_anchor,
        t.ctx_band, t.ctx_band_key, t.ctx_band_raw, t.ctx_cadence, t.ctx_refill,
        t.ctx_resilience, t.ctx_all_books, t.ctx_all_flows, t.ctx_relative, t.lq_tiers, t.ctx_flows,
        arrayFilter(book -> book.4 AND book.3 <= t.ctx_end AND book.5 = 'true', t.book_times) AS admitted_books
    FROM lq_book_times AS t
),
lq_book_ordinals AS (
    SELECT a.ctx_token, a.ctx_primary, a.ctx_end, a.ctx_start, a.ctx_anchor,
        a.ctx_band, a.ctx_band_key, a.ctx_band_raw, a.ctx_cadence, a.ctx_refill,
        a.ctx_resilience, a.ctx_all_books, a.ctx_all_flows, a.ctx_relative, a.lq_tiers, a.ctx_flows,
        arrayMap((book, ordinal) -> tuple(book.3, ordinal, book.1, book.2),
            a.admitted_books, arrayEnumerate(a.admitted_books)) AS ordered_books
    FROM lq_book_admitted AS a
),
"""


def ctes():
    per_venue = _object(
        venue=_json("venue"),
        coverage=_object(
            venue=_json("venue"),
            range_covered_pct=_raw("JSONExtractRaw(current.3, 'range_covered_pct')"),
            integrity_ok="'true'",
        ),
        observed_at=_json("lq_iso(current.1)"),
        precision=_json("lq_precision(current.3)"),
        spread_bps=_json("spread"),
        depth_p1_usd=_money("current_total"),
        bid_depth_p1_usd=_money("bid"),
        ask_depth_p1_usd=_money("ask"),
        range_covered_pct=_raw("JSONExtractRaw(current.3, 'range_covered_pct')"),
        tier_notional=_object(
            **{name: _money(f"lq_tiers[{i}]") for i, name in enumerate(("T1", "T2", "T3"), 1)}
        ),
        slippage_bps=_object(
            **{
                name: _json(f"lq_walk_book(current.3, lq_tiers[{i}], 'ask')")
                for i, name in enumerate(("T1", "T2", "T3"), 1)
            }
        ),
    )
    observation = _object(
        venue=_json("venue"),
        ts=_json("lq_iso(lq_time(flow))"),
        replenished=_bool(
            "if(lq_time(flow) + toIntervalMicrosecond(toInt64(ctx_refill * 1000000)) "
            "<= ctx_end, notEmpty(recovery), NULL)"
        ),
        recovery_s=_json(
            "if(notEmpty(recovery), dateDiff('microsecond', lq_time(flow), recovery[1].1) / 1000000., NULL)"
        ),
        resilient=_bool(
            "if(notEmpty(future_lows) AND isNotNull(lq_dec(JSONExtractRaw(flow, 'low'))) "
            "AND lq_time(flow) + toIntervalMicrosecond(toInt64(ctx_resilience * 1000000)) "
            "<= ctx_end, arrayMin(future_lows) >= lq_dec(JSONExtractRaw(flow, 'low')), NULL)"
        ),
    )
    root_observation = (
        observation.replace("flow", "burst.1")
        .replace("recovery)", "burst.3)")
        .replace("recovery[", "burst.3[")
        .replace("future_lows", "burst.4")
    )
    return (
        r"""
{lq_snapshot:String} AS lq_snapshot,
{lq_config:String} AS lq_config,
{lq_spot:String} AS lq_spot,
{lq_relative:String} AS lq_relative,
toDecimal256(0, 24) AS lq_zero,
toDecimal256(1, 24) AS lq_one,
(x -> if(JSONType(x) = 'String', JSONExtractString(x), x)) AS lq_text,
(x -> if(JSONType(x) = 'Object', JSONExtractRaw(x, 'value'), x)) AS lq_unwrap,
(x -> arrayMap(text -> arrayMap(parts -> arrayMap(digits -> arrayMap(point ->
    if(length(parts) = 2,
        if(throwIf(length(parts) = 2 AND (NOT match(text, '^[+-]?[0-9]+([.][0-9]+)?[eE][+-]?[0-9]+$')
            OR isNull(toInt32OrNull(parts[2])) OR abs(ifNull(toInt32OrNull(parts[2]), 100)) > 48),
            'Market Truth decimal input outside native financial range') = 0,
            concat(if(startsWith(parts[1], '-'), '-', ''), multiIf(
                point <= 0, concat('0.', repeat('0', toUInt64(greatest(-point, 0))), digits),
                point >= toInt64(length(digits)), concat(digits, repeat('0', toUInt64(greatest(point - toInt64(length(digits)), 0)))),
                concat(substring(digits, 1, toUInt64(greatest(point, 0))), '.', substring(digits, toUInt64(greatest(point + 1, 1)))))), ''), text),
    [if(position(parts[1], '.') > 0, toInt64(position(parts[1], '.')) - 1 - startsWith(parts[1], '-') - startsWith(parts[1], '+'),
        toInt64(length(digits))) + ifNull(toInt32OrNull(parts[2]), 0)])[1],
    [replaceRegexpAll(parts[1], '[^0-9]', '')])[1], [splitByRegexp('[eE]', text)])[1], [lq_text(lq_unwrap(x))])[1]) AS lq_decimal_text,
(x -> arrayMap(text -> if(text IN ('', 'null'), CAST(NULL, 'Nullable(Decimal256(24))'),
    if(throwIf(text NOT IN ('', 'null') AND NOT match(text, '^[+-]?0*[0-9]{1,24}([.][0-9]{1,24})?$'),
        'Market Truth decimal input outside native financial range') = 0,
        toDecimal256OrNull(text, 24), NULL)), [lq_decimal_text(x)])[1]) AS lq_dec,
(x -> toFloat64OrNull(lq_text(lq_unwrap(x)))) AS lq_number,
(x -> toDecimal256OrNull(lq_decimal_text(x), 24)) AS lq_timestamp_number,
(x -> if(JSONType(x) IN ('Int64', 'UInt64', 'Float64') OR match(lq_text(x), '^[0-9]+([.][0-9]+)?$'),
    arrayMap(number -> fromUnixTimestamp64Micro(toInt64(roundBankers(multiIf(abs(number) > toDecimal256('100000000000000', 24), number,
        abs(number) > toDecimal256('100000000000', 24), number * 1000,
        number * 1000000), 0)), 'UTC'), [lq_timestamp_number(x)])[1],
    parseDateTime64BestEffortOrNull(lq_text(x), 6, 'UTC'))) AS lq_parse,
(x -> arrayFirst(raw -> raw NOT IN ('', 'null'), [
    JSONExtractRaw(x, 'minute_start'), JSONExtractRaw(x, 'open_time'),
    JSONExtractRaw(x, 'exchange_ts'), JSONExtractRaw(x, 'ts'), JSONExtractRaw(x, 'as_of'),
    JSONExtractRaw(x, 'settlement_time'), JSONExtractRaw(x, 'funding_time'), JSONExtractRaw(x, 'observed_at')])) AS lq_time_raw,
(raw -> arrayMap(parsed -> if(throwIf(isNull(parsed), 'Invalid demanded observation timestamp') = 0,
    parsed, CAST(NULL, 'Nullable(DateTime64(6, \'UTC\'))')), [lq_parse(raw)])[1]) AS lq_required_time,
(x -> if(lq_time_raw(x) = '', CAST(NULL, 'Nullable(DateTime64(6, \'UTC\'))'), lq_required_time(lq_time_raw(x)))) AS lq_time,
(x -> if(isNull(x), CAST(NULL, 'Nullable(String)'),
    concat(formatDateTime(x, '%Y-%m-%dT%H:%i:%S', 'UTC'),
        if(positiveModulo(toUnixTimestamp64Micro(x), 1000000) = 0, '', concat('.', leftPad(toString(positiveModulo(toUnixTimestamp64Micro(x), 1000000)), 6, '0'))), 'Z'))) AS lq_iso,
(x -> if(JSONExtractString(x, 'venue') != '', JSONExtractString(x, 'venue'), splitByChar(':', JSONExtractString(x, 'instrument_id'))[1])) AS lq_venue,
(x -> if(JSONExtractString(x, 'market_type') != '', JSONExtractString(x, 'market_type') = 'SPOT', position(JSONExtractString(x, 'instrument_id'), ':spot:') > 0)) AS lq_is_spot,
((book, side, unit, band, band_key) -> if(
    if(JSONType(JSONExtractRaw(book, 'range_covered_pct')) = 'Object',
        lq_number(JSONExtractRaw(book, 'range_covered_pct', side)), lq_number(JSONExtractRaw(book, 'range_covered_pct'))) >= band,
    lq_dec(JSONExtractRaw(book, 'depth', side, band_key, unit)), NULL)) AS lq_depth_context,
(values -> arrayMap(items -> if(notEmpty(items) AND arrayAll(x -> isNotNull(x), items),
    arraySum(arrayMap(x -> ifNull(x, lq_zero), items)), CAST(NULL, 'Nullable(Decimal256(24))')), [values])[1]) AS lq_sum,
(values -> arrayMap(items -> divideDecimal(lq_sum(items), toDecimal256(greatest(length(items), 1), 24), 24), [values])[1]) AS lq_mean,
(values -> arrayMap(items -> if(notEmpty(items), arraySum(x -> ifNull(x, 0.), items) / greatest(length(items), 1), CAST(NULL, 'Nullable(Float64)')), [values])[1]) AS lq_float_mean,
((values, q) -> if(empty(values), CAST(NULL, 'Nullable(Float64)'),
    arraySort(values)[toUInt64(floor((length(values) - 1) * q)) + 1]
    + (arraySort(values)[toUInt64(ceil((length(values) - 1) * q)) + 1]
        - arraySort(values)[toUInt64(floor((length(values) - 1) * q)) + 1])
        * ((length(values) - 1) * q - floor((length(values) - 1) * q)))) AS lq_quantile,
(book -> arrayFilter(p -> p NOT IN ('', 'null'), arrayMap(raw -> lq_text(raw), [
    JSONExtractRaw(book, 'exchange_ts_prec'), JSONExtractRaw(book, 'exchange_ts_precision'),
    JSONExtractRaw(book, 'timestamp_precision')]))) AS lq_precision_declarations,
((p, book) -> multiIf(p = 'UNKNOWN', CAST(NULL, 'Nullable(String)'),
    p IN ('HOUR', 'BUCKET_1H'), 'BUCKET(1h)', p IN ('DAY', 'BUCKET_1D'), 'BUCKET(1d)',
    p = 'BUCKET_1M', 'BUCKET(1m)', p = 'BUCKET',
        if(JSONExtractRaw(book, 'exchange_ts_bucket_size_s') IN ('', 'null'), NULL,
            concat('BUCKET(', lq_text(JSONExtractRaw(book, 'exchange_ts_bucket_size_s')), 's)')),
    p IN ('EXACT', 'COLLECTION_ONLY') OR match(p, '^BUCKET[(][0-9]+([.][0-9]+)?(us|ms|s|m|h|d|w)[)]$'), p,
    if(throwIf(p NOT IN ('', 'UNKNOWN', 'BUCKET'), 'Unsupported timestamp precision') = 0, NULL, NULL))) AS lq_normal_precision,
(p -> multiplyDecimal(lq_dec(extract(ifNull(p, ''), '[0-9]+(?:[.][0-9]+)?')),
    multiIf(endsWith(ifNull(p, ''), 'us)'), toDecimal256('0.000001', 24),
        endsWith(ifNull(p, ''), 'ms)'), toDecimal256('0.001', 24),
        endsWith(ifNull(p, ''), 'm)'), toDecimal256(60, 24),
        endsWith(ifNull(p, ''), 'h)'), toDecimal256(3600, 24),
        endsWith(ifNull(p, ''), 'd)'), toDecimal256(86400, 24),
        endsWith(ifNull(p, ''), 'w)'), toDecimal256(604800, 24), lq_one), 24)) AS lq_precision_size,
(p -> tuple(if(startsWith(ifNull(p, ''), 'BUCKET('), 'BUCKET', ifNull(p, '')), lq_precision_size(p))) AS lq_precision_key,
(book -> arrayMap(p -> tuple(lq_normal_precision(p, book)), lq_precision_declarations(book))) AS lq_normal_precisions,
(book -> if(throwIf(length(arrayDistinct(arrayMap(p -> lq_precision_key(p.1), lq_normal_precisions(book)))) > 1
    OR arrayExists(p -> startsWith(ifNull(p.1, ''), 'BUCKET(') AND ifNull(lq_precision_size(p.1), lq_zero) <= 0, lq_normal_precisions(book))
    OR (JSONExtractRaw(book, 'exchange_ts_bucket_size_s') NOT IN ('', 'null') AND arrayExists(p -> isNotNull(p.1)
        AND (NOT startsWith(p.1, 'BUCKET(') OR lq_precision_size(p.1) != lq_dec(JSONExtractRaw(book, 'exchange_ts_bucket_size_s'))), lq_normal_precisions(book))),
    'Conflicting timestamp precision declarations') = 0,
    if(empty(lq_normal_precisions(book)), CAST(NULL, 'Nullable(String)'), lq_normal_precisions(book)[1].1), NULL)) AS lq_precision,
"""
        + _input_ctes()
        + r"""
lq_volume_population AS (
    SELECT c.*,
        arrayFilter(flow -> if(lq_is_spot(flow), lq_time(flow) >= c.ctx_end - INTERVAL 1 DAY
            AND lq_time(flow) < c.ctx_end, false), c.ctx_all_flows) AS lq_volume_flows
    FROM lq_static_context AS c
),
lq_volume_bucket_keys AS (
    SELECT p.*, arrayDistinct(arrayMap(flow -> intDiv(toUnixTimestamp(lq_time(flow)), 900), p.lq_volume_flows)) AS lq_buckets
    FROM lq_volume_population AS p
),
lq_volume_bucket_groups AS (
    SELECT k.*,
        arrayMap(bucket -> arrayFilter(flow -> has(k.ctx_eligible, lq_venue(flow)) AND intDiv(toUnixTimestamp(lq_time(flow)), 900) = bucket, k.lq_volume_flows), k.lq_buckets) AS lq_bucket_rows
    FROM lq_volume_bucket_keys AS k
),
lq_volume_coverage AS (
    SELECT g.*, arrayFilter(rows -> notEmpty(g.ctx_eligible) AND arrayAll(row -> JSONExtractRaw(row, 'complete') = 'true', rows)
        AND arrayAll(venue -> length(arrayDistinct(arrayMap(row -> lq_time(row), arrayFilter(row -> lq_venue(row) = venue, rows)))) = 15, g.ctx_eligible)
        AND isNotNull(lq_sum(arrayMap(row -> lq_dec(JSONExtractRaw(row, 'buy_notional')), rows)))
        AND isNotNull(lq_sum(arrayMap(row -> lq_dec(JSONExtractRaw(row, 'sell_notional')), rows))), g.lq_bucket_rows) AS lq_covered_buckets
    FROM lq_volume_bucket_groups AS g
),
lq_volume_totals AS (
    SELECT c.*, arrayMap(rows -> lq_sum(arrayMap(row -> lq_dec(JSONExtractRaw(row, 'buy_notional')), rows))
        + lq_sum(arrayMap(row -> lq_dec(JSONExtractRaw(row, 'sell_notional')), rows)), c.lq_covered_buckets) AS lq_volumes
    FROM lq_volume_coverage AS c
),
lq_volume_average AS (
    SELECT t.*, lq_mean(t.lq_volumes) AS lq_volume_mean FROM lq_volume_totals AS t
),
lq_tier_values AS (
    SELECT a.*, arrayMap(n -> if(a.lq_volume_mean >= a.ctx_base_tiers[3], n,
        divideDecimal(multiplyDecimal(n, a.lq_volume_mean, 24), nullIf(a.ctx_base_tiers[3], lq_zero), 24)), a.ctx_base_tiers) AS lq_tiers
    FROM lq_volume_average AS a
),
"""
        + _book_ctes()
        + r"""
(level -> if(JSONType(level) = 'Object', lq_dec(JSONExtractRaw(level, 'price')), lq_dec(JSONExtractRaw(level, 1)))) AS lq_level_price,
(level -> if(JSONType(level) = 'Object', lq_dec(JSONExtractRaw(level, 'qty')), lq_dec(JSONExtractRaw(level, 2)))) AS lq_level_qty,
((book, side) -> if(JSONHas(book, if(side = 'ask', 'asks', 'bids')),
    JSONExtractArrayRaw(book, if(side = 'ask', 'asks', 'bids')),
    JSONExtractArrayRaw(book, 'levels', if(side = 'ask', 'asks', 'bids')))) AS lq_levels,
((levels, notional) -> arrayFold((state, level) ->
    if(state.1 <= toDecimal256('0.00000001', 24) AND state.4, state,
        arrayMap(parsed -> tuple(
            state.1 - least(state.1, multiplyDecimal(ifNull(parsed.1, lq_zero), ifNull(parsed.2, lq_zero), 24)),
            state.2 + least(state.1, multiplyDecimal(ifNull(parsed.1, lq_zero), ifNull(parsed.2, lq_zero), 24)),
            state.3 + divideDecimal(least(state.1, multiplyDecimal(ifNull(parsed.1, lq_zero), ifNull(parsed.2, lq_zero), 24)),
                nullIf(ifNull(parsed.1, lq_zero), lq_zero), 24), toUInt8(1)),
            [tuple(lq_level_price(level), lq_level_qty(level))])[1]), levels,
    tuple(ifNull(notional, lq_zero), lq_zero, CAST(lq_zero, 'Nullable(Decimal256(24))'), toUInt8(0)))) AS lq_walk_state,
((book, n, side) -> if(JSONExtractString(book, 'quote_asset') = 'USD', lq_one, lq_dec(JSONExtractRaw(book, 'quote_usd_rate')))) AS lq_fx,
((book, n, side) -> lq_walk_state(lq_levels(book, side), divideDecimal(n, nullIf(lq_fx(book, n, side), lq_zero), 24))) AS lq_fill,
((book, n, side) -> if(isNotNull(n) AND n != 0 AND lq_dec(JSONExtractRaw(book, 'mid')) != 0
    AND lq_fx(book, n, side) != 0 AND notEmpty(lq_levels(book, side)),
    arrayMap(fill -> if(fill.3 != 0 AND fill.1 <= toDecimal256('0.00000001', 24),
        toFloat64((divideDecimal(divideDecimal(fill.2, nullIf(fill.3, lq_zero), 24),
            nullIf(lq_dec(JSONExtractRaw(book, 'mid')), lq_zero), 24) - lq_one) * 10000), CAST(NULL, 'Nullable(Float64)')),
        [lq_fill(book, n, side)])[1], CAST(NULL, 'Nullable(Float64)'))) AS lq_walk_book,
lq_venue_books AS (
    SELECT item.4 AS venue, min(item.2) AS first_ordinal,
        arraySort(x -> tuple(x.1, x.2), groupArrayIf(tuple(item.1, item.2, item.3), item.2 > 0)) AS series,
        any(tuple(o.ctx_start, o.ctx_anchor, o.ctx_band, o.ctx_band_key)) AS venue_context,
        any(tuple(o.ctx_primary, o.ctx_end, o.ctx_band_raw, o.ctx_cadence, o.ctx_refill,
            o.ctx_resilience, o.ctx_relative, o.ctx_flows, o.lq_tiers)) AS flow_tier_context,
        any(tuple(o.ctx_token, o.ctx_all_books, o.ctx_all_flows,
            arrayMap(book -> book.3, o.ordered_books))) AS source_context
    FROM lq_book_ordinals AS o LEFT ARRAY JOIN o.ordered_books AS item
    GROUP BY venue
),
lq_venue_context AS (
    SELECT b.venue, b.first_ordinal, b.series,
        b.venue_context.1 AS ctx_start, b.venue_context.2 AS ctx_anchor,
        b.venue_context.3 AS ctx_band, b.venue_context.4 AS ctx_band_key,
        b.flow_tier_context.1 AS ctx_primary, b.flow_tier_context.2 AS ctx_end,
        b.flow_tier_context.3 AS ctx_band_raw, b.flow_tier_context.4 AS ctx_cadence,
        b.flow_tier_context.5 AS ctx_refill, b.flow_tier_context.6 AS ctx_resilience,
        b.flow_tier_context.7 AS ctx_relative, b.flow_tier_context.8 AS ctx_flows,
        b.flow_tier_context.9 AS lq_tiers,
        b.source_context.1 AS ctx_token, b.source_context.2 AS ctx_all_books,
        b.source_context.3 AS ctx_all_flows, b.source_context.4 AS lq_books
    FROM lq_venue_books AS b
),
lq_venue_base AS (
    SELECT v.*, v.series[-1] AS current,
        arrayFilter(book -> book.1 >= v.ctx_anchor - INTERVAL 1 HOUR AND book.1 < v.ctx_anchor, v.series) AS previous
    FROM lq_venue_context AS v
),
lq_venue_metric_demand AS (
    SELECT b.*, arrayMap(book -> tuple(book,
        ifNull(book.2 = b.current.2 OR (book.1 >= b.ctx_anchor - INTERVAL 1 HOUR AND book.1 < b.ctx_anchor)
            OR book.1 >= b.ctx_start, false),
        ifNull(book.2 = b.current.2 OR (book.1 >= b.ctx_anchor - INTERVAL 1 HOUR AND book.1 < b.ctx_anchor), false),
        ifNull(book.1 >= b.ctx_anchor - INTERVAL 1 HOUR AND book.1 < b.ctx_anchor, false),
        ifNull(book.1 >= b.ctx_start, false)), b.series) AS metric_demand
    FROM lq_venue_base AS b
),
lq_venue_metrics AS (
    SELECT d.*, arrayMap(demand -> tuple(demand.1.1, demand.1.2,
        if(demand.2, lq_depth_context(demand.1.3, 'bid', 'usd', d.ctx_band, d.ctx_band_key), CAST(NULL, 'Nullable(Decimal256(24))')),
        if(demand.2, lq_depth_context(demand.1.3, 'ask', 'usd', d.ctx_band, d.ctx_band_key), CAST(NULL, 'Nullable(Decimal256(24))')),
        if(demand.3, lq_depth_context(demand.1.3, 'bid', 'native', d.ctx_band, d.ctx_band_key), CAST(NULL, 'Nullable(Decimal256(24))')),
        if(demand.3, lq_depth_context(demand.1.3, 'ask', 'native', d.ctx_band, d.ctx_band_key), CAST(NULL, 'Nullable(Decimal256(24))')),
        if(demand.3, lq_number(JSONExtractRaw(demand.1.3, 'spread_bps')), CAST(NULL, 'Nullable(Float64)')),
        demand.4, demand.5), d.metric_demand) AS book_metrics
    FROM lq_venue_metric_demand AS d
),
lq_venue_depth AS (
    SELECT b.venue, b.first_ordinal, b.series, b.ctx_start, b.ctx_anchor,
        b.ctx_band, b.ctx_band_key, b.ctx_primary, b.ctx_end, b.ctx_band_raw,
        b.ctx_cadence, b.ctx_refill, b.ctx_resilience, b.ctx_relative, b.ctx_flows,
        b.lq_tiers, b.ctx_token, b.ctx_all_books, b.ctx_all_flows, b.lq_books,
        b.current, b.previous,
        b.book_metrics[-1].3 AS bid, b.book_metrics[-1].4 AS ask,
        b.book_metrics[-1].5 AS bid_native, b.book_metrics[-1].6 AS ask_native,
        lq_mean(arrayFilter(x -> isNotNull(x), arrayMap(metric -> metric.3, arrayFilter(metric -> metric.8, b.book_metrics)))) AS pre_bid,
        lq_mean(arrayFilter(x -> isNotNull(x), arrayMap(metric -> metric.4, arrayFilter(metric -> metric.8, b.book_metrics)))) AS pre_ask,
        lq_mean(arrayMap(metric -> metric.5, arrayFilter(metric -> metric.8, b.book_metrics))) AS pre_bid_native,
        lq_mean(arrayMap(metric -> metric.6, arrayFilter(metric -> metric.8, b.book_metrics))) AS pre_ask_native,
        lq_mean(arrayFilter(x -> isNotNull(x), arrayMap(metric -> metric.3 + metric.4, arrayFilter(metric -> metric.8, b.book_metrics)))) AS pre_total,
        b.book_metrics[-1].7 AS spread,
        lq_float_mean(arrayFilter(x -> isNotNull(x), arrayMap(metric -> metric.7, arrayFilter(metric -> metric.8, b.book_metrics)))) AS pre_spread,
        lq_mean(arrayFilter(x -> isNotNull(x), arrayMap(metric -> metric.3, arrayFilter(metric -> metric.9, b.book_metrics)))) AS avg_bid,
        lq_mean(arrayFilter(x -> isNotNull(x), arrayMap(metric -> metric.4, arrayFilter(metric -> metric.9, b.book_metrics)))) AS avg_ask
    FROM lq_venue_metrics AS b
),
lq_venue_stats AS (
    SELECT d.*, d.bid + d.ask AS current_total FROM lq_venue_depth AS d
),
"""
        + f"""
lq_venues AS (
    SELECT *, {per_venue} AS venue_json FROM lq_venue_stats WHERE first_ordinal > 0
),
lq_burst_inputs AS (
    SELECT v.venue, v.first_ordinal, v.series, v.ctx_band, v.ctx_band_key,
        v.ctx_end, v.ctx_cadence, v.ctx_refill, v.ctx_resilience,
        arrayFilter(flow -> lq_venue(flow) = v.venue, v.ctx_flows) AS venue_flows
    FROM lq_venue_base AS v WHERE v.first_ordinal > 0
),
lq_burst_base AS (
    SELECT b.*, flow, arrayFilter(book -> book.1 < lq_time(flow) AND dateDiff('microsecond', book.1, lq_time(flow)) <= b.ctx_cadence * 1000000, b.series) AS baselines
    FROM lq_burst_inputs AS b ARRAY JOIN b.venue_flows AS flow
),
lq_burst_selected AS (
    SELECT b.*, b.baselines[-1] AS baseline
    FROM lq_burst_base AS b WHERE notEmpty(b.baselines)
        AND isNotNull(lq_dec(JSONExtractRaw(flow, 'sell_notional')))
        AND isNotNull(lq_dec(JSONExtractRaw(flow, 'buy_notional')))
        AND lq_dec(JSONExtractRaw(flow, 'sell_notional')) - lq_dec(JSONExtractRaw(flow, 'buy_notional')) >= lq_depth_context(b.baselines[-1].3, 'bid', 'usd', b.ctx_band, b.ctx_band_key)
),
lq_burst_recovery AS (
    SELECT b.*, arrayFilter(book -> book.1 > lq_time(b.flow)
        AND book.1 <= least(lq_time(b.flow) + toIntervalMicrosecond(toInt64(b.ctx_refill * 1000000)), b.ctx_end)
        AND lq_depth_context(book.3, 'bid', 'usd', b.ctx_band, b.ctx_band_key)
            >= lq_depth_context(b.baseline.3, 'bid', 'usd', b.ctx_band, b.ctx_band_key), b.series) AS recovery,
        arrayFilter(x -> isNotNull(x), arrayMap(row -> lq_dec(JSONExtractRaw(row, 'low')),
            arrayFilter(row -> lq_time(row) > lq_time(b.flow)
                AND lq_time(row) <= lq_time(b.flow) + toIntervalMicrosecond(toInt64(b.ctx_resilience * 1000000)), b.venue_flows))) AS future_lows
    FROM lq_burst_selected AS b
),
lq_burst_results AS (
    SELECT first_ordinal, indexOf(venue_flows, flow) AS flow_ordinal, {observation} AS observation
    FROM lq_burst_recovery
),
lq_collected AS (
    SELECT arraySort(v -> v.17, groupArray(tuple(venue, bid, ask, bid_native, ask_native,
        pre_bid, pre_ask, pre_bid_native, pre_ask_native, current_total, pre_total, spread,
        pre_spread, avg_bid, avg_ask, venue_json, first_ordinal, current.3))) AS venues FROM lq_venues
),
lq_observed AS (
    SELECT arrayMap(x -> x.3, arraySort(x -> tuple(x.1, x.2), groupArray(tuple(first_ordinal, flow_ordinal, observation)))) AS observations
    FROM lq_burst_results
),
lq_root_inputs AS (
    SELECT v.venue AS venue, v.first_ordinal AS first_ordinal, v.series AS series,
        v.current AS current, v.ctx_band_key AS ctx_band_key,
        v.bid AS bid, v.ask AS ask, v.bid_native AS bid_native, v.ask_native AS ask_native,
        v.pre_bid AS pre_bid, v.pre_ask AS pre_ask,
        v.pre_bid_native AS pre_bid_native, v.pre_ask_native AS pre_ask_native,
        v.current_total AS current_total, v.pre_total AS pre_total,
        v.spread AS spread, v.pre_spread AS pre_spread, v.avg_bid AS avg_bid, v.avg_ask AS avg_ask,
        v.lq_tiers AS lq_tiers,
        v.ctx_primary AS ctx_primary, v.ctx_end AS ctx_end, v.ctx_start AS ctx_start,
        v.ctx_anchor AS ctx_anchor, v.ctx_band AS ctx_band, v.ctx_band_raw AS ctx_band_raw,
        v.ctx_cadence AS ctx_cadence, v.ctx_refill AS ctx_refill,
        v.ctx_resilience AS ctx_resilience, v.ctx_relative AS ctx_relative, v.ctx_flows AS ctx_flows,
        v.ctx_token AS ctx_token, v.ctx_all_books AS ctx_all_books,
        v.ctx_all_flows AS ctx_all_flows, v.lq_books AS lq_books,
        arrayFilter(flow -> v.first_ordinal > 0 AND lq_venue(flow) = v.venue, v.ctx_flows) AS venue_flows
    FROM lq_venue_stats AS v
),
lq_root_baselines AS (
    SELECT r.*, arrayMap(flow -> tuple(flow, indexOf(r.venue_flows, flow),
        arrayFilter(book -> book.1 < lq_time(flow)
            AND dateDiff('microsecond', book.1, lq_time(flow)) <= r.ctx_cadence * 1000000,
            r.series)), r.venue_flows) AS burst_baselines
    FROM lq_root_inputs AS r
),
lq_root_selected AS (
    SELECT r.*, arrayFilter(burst -> if(notEmpty(burst.3),
        isNotNull(lq_dec(JSONExtractRaw(burst.1, 'sell_notional')))
        AND isNotNull(lq_dec(JSONExtractRaw(burst.1, 'buy_notional')))
        AND lq_dec(JSONExtractRaw(burst.1, 'sell_notional')) - lq_dec(JSONExtractRaw(burst.1, 'buy_notional'))
            >= lq_depth_context(burst.3[-1].3, 'bid', 'usd', r.ctx_band, r.ctx_band_key), false),
        r.burst_baselines) AS selected_bursts
    FROM lq_root_baselines AS r
),
lq_root_recovery AS (
    SELECT r.*, arrayMap(burst -> tuple(burst.1, burst.2,
        arrayFilter(book -> book.1 > lq_time(burst.1)
            AND book.1 <= least(lq_time(burst.1) + toIntervalMicrosecond(toInt64(r.ctx_refill * 1000000)), r.ctx_end)
            AND lq_depth_context(book.3, 'bid', 'usd', r.ctx_band, r.ctx_band_key)
                >= lq_depth_context(burst.3[-1].3, 'bid', 'usd', r.ctx_band, r.ctx_band_key), r.series),
        arrayFilter(x -> isNotNull(x), arrayMap(row -> lq_dec(JSONExtractRaw(row, 'low')),
            arrayFilter(row -> lq_time(row) > lq_time(burst.1)
                AND lq_time(row) <= lq_time(burst.1) + toIntervalMicrosecond(toInt64(r.ctx_resilience * 1000000)),
                r.venue_flows)))), r.selected_bursts) AS recovered_bursts
    FROM lq_root_selected AS r
),
lq_root_results AS (
    SELECT r.*, if(first_ordinal > 0, {per_venue}, '') AS venue_json,
        arrayMap(burst -> tuple(first_ordinal, burst.2, {root_observation}), recovered_bursts) AS root_observations
    FROM lq_root_recovery AS r
),
lq_root_publication AS (
    SELECT arraySort(v -> v.17, groupArrayIf(tuple(venue, bid, ask, bid_native, ask_native,
        pre_bid, pre_ask, pre_bid_native, pre_ask_native, current_total, pre_total, spread,
        pre_spread, avg_bid, avg_ask, venue_json, first_ordinal, current.3), first_ordinal > 0)) AS venues,
        arrayMap(x -> x.3, arraySort(x -> tuple(x.1, x.2), arrayFlatten(groupArray(root_observations)))) AS observations,
        any(tuple(lq_tiers, ctx_anchor, ctx_band, ctx_band_raw, ctx_start, ctx_end, ctx_primary,
            ctx_flows, ctx_relative, ctx_token, ctx_all_books, ctx_all_flows, ctx_cadence, lq_books)) AS root_context
    FROM lq_root_results
),
lq_root_context AS (
    SELECT p.venues, p.observations, p.root_context.1 AS lq_tiers,
        p.root_context.2 AS ctx_anchor, p.root_context.3 AS ctx_band,
        p.root_context.4 AS ctx_band_raw, p.root_context.5 AS ctx_start,
        p.root_context.6 AS ctx_end, p.root_context.7 AS ctx_primary,
        p.root_context.8 AS ctx_flows, p.root_context.9 AS ctx_relative,
        p.root_context.10 AS ctx_token, p.root_context.11 AS ctx_all_books,
        p.root_context.12 AS ctx_all_flows, p.root_context.13 AS ctx_cadence,
        p.root_context.14 AS lq_books
    FROM lq_root_publication AS p
),
"""
        + _state_ctes()
    )


def _state_ctes():
    return r"""
((v, side, native, previous) -> if(previous,
    if(native, if(side = 'bid', v.8, v.9), if(side = 'bid', v.6, v.7)),
    if(native, if(side = 'bid', v.4, v.5), if(side = 'bid', v.2, v.3)))) AS lq_side_value,
lq_state_inputs AS (
    SELECT venues AS lq_venues_array, observations AS lq_observations, lq_tiers,
        c.ctx_anchor, c.ctx_band, c.ctx_band_raw, c.ctx_start, c.ctx_end, c.ctx_primary,
        c.ctx_token, c.ctx_all_books, c.ctx_all_flows, c.ctx_cadence, c.lq_books,
        lq_sum(arrayMap(flow -> lq_dec(JSONExtractRaw(flow, 'buy_notional')), c.ctx_flows))
            - lq_sum(arrayMap(flow -> lq_dec(JSONExtractRaw(flow, 'sell_notional')), c.ctx_flows)) AS lq_net_usd,
        lq_sum(arrayMap(flow -> lq_dec(JSONExtractRaw(flow, 'buy_qty')), c.ctx_flows))
            - lq_sum(arrayMap(flow -> lq_dec(JSONExtractRaw(flow, 'sell_qty')), c.ctx_flows)) AS lq_net_native,
        JSONExtractRaw(c.ctx_relative, 'windows', c.ctx_primary) AS lq_relative_window,
        notEmpty(venues) AND notEmpty(c.ctx_flows) AS lq_replenishment_available
    FROM lq_root_context AS c
),
lq_state_flow AS (
    SELECT *, if(isNotNull(lq_net_native), lq_net_native, lq_net_usd) AS lq_net,
        if(isNotNull(lq_net_native), 'NATIVE', 'USD') AS lq_unit,
        lq_number(JSONExtractRaw(lq_relative_window, 'r_raw')) AS lq_raw_return,
        if(JSONExtractRaw(lq_relative_window, 'beyond_normal') IN ('true', 'false'),
            JSONExtractRaw(lq_relative_window, 'beyond_normal') = 'true', CAST(NULL, 'Nullable(Bool)')) AS lq_beyond
    FROM lq_state_inputs
),
lq_state_direction AS (
    SELECT *, multiIf(lq_net > 0, 'ask', lq_net < 0, 'bid', CAST(NULL, 'Nullable(String)')) AS lq_hit,
        if(lq_net != 0 AND isNotNull(lq_raw_return),
            if(lq_net > 0, lq_raw_return, -lq_raw_return) < ctx_band / 100, CAST(NULL, 'Nullable(Bool)')) AS lq_small_price
    FROM lq_state_flow
),
lq_state_coverage_rows AS (
    SELECT d.*,
        arrayMap(spec -> arrayMap(v -> tuple(v.1, lq_side_value(v, spec.1, spec.2, spec.3)),
            arrayFilter(v -> isNotNull(lq_side_value(v, spec.1, spec.2, spec.3))
                AND isNotNull(lq_side_value(v, spec.1, 0, spec.3)), d.lq_venues_array)),
            [('bid', 0, 0), ('ask', 0, 0), ('bid', 1, 0), ('ask', 1, 0),
             ('bid', 0, 1), ('ask', 0, 1), ('bid', 1, 1), ('ask', 1, 1)]) AS covered_rows,
        arrayMap(side -> arrayFilter(v -> isNotNull(lq_side_value(v, side, 0, 0))
            AND isNotNull(lq_side_value(v, side, 0, 1)), d.lq_venues_array), ['bid', 'ask']) AS cohort_rows,
        arrayMap(spec -> lq_sum(arrayMap(v -> lq_side_value(v, spec.1, 0, spec.2), d.lq_venues_array)),
            [('bid', 0), ('ask', 0), ('bid', 1), ('ask', 1)]) AS side_totals
    FROM lq_state_direction AS d
),
lq_state_coverage AS (
    SELECT r.*, arrayMap(rows -> tuple(arraySort(arrayMap(v -> v.1, rows)),
        lq_sum(arrayMap(v -> v.2, rows)), notEmpty(r.lq_venues_array)
            AND length(rows) = length(r.lq_venues_array)), r.covered_rows) AS covered,
        arrayMap((rows, side) -> tuple(arraySort(arrayMap(v -> v.1, rows)),
            arraySum(arrayMap(v -> ifNull(lq_side_value(v, side, 0, 0), lq_zero), rows)),
            arraySum(arrayMap(v -> ifNull(lq_side_value(v, side, 0, 1), lq_zero), rows))),
            r.cohort_rows, ['bid', 'ask']) AS cohort_totals
    FROM lq_state_coverage_rows AS r
),
lq_state_cohort_totals AS (
    SELECT c.*, arrayMap(t -> if(notEmpty(t.1) AND t.3 != 0,
        toFloat64(lq_one - divideDecimal(t.2, nullIf(t.3, lq_zero), 24)),
        CAST(NULL, 'Nullable(Float64)')), c.cohort_totals) AS depth_drops,
        arrayMap(t -> multiIf(isNull(t.2) OR isNull(c.lq_tiers[2]), CAST(NULL, 'Nullable(Bool)'),
            t.3, t.2 < c.lq_tiers[2], t.2 >= c.lq_tiers[2], false,
            CAST(NULL, 'Nullable(Bool)')), arraySlice(c.covered, 1, 2)) AS thin_sides
    FROM lq_state_coverage AS c
),
lq_state_cohort AS (
    SELECT c.*, if(c.lq_hit = 'bid', 1, 2) AS hit_index,
        if(c.lq_hit = 'bid', 1, 2) + if(c.lq_unit = 'NATIVE', 2, 0) AS coverage_index
    FROM lq_state_cohort_totals AS c
),
lq_state_depth AS (
    SELECT *,
        if(isNotNull(lq_hit), covered[coverage_index].2, CAST(NULL, 'Nullable(Decimal256(24))')) AS lq_bound,
        if(isNotNull(lq_hit), covered[coverage_index + 4].2, CAST(NULL, 'Nullable(Decimal256(24))')) AS lq_pre_bound,
        if(isNotNull(lq_hit), covered[coverage_index].3, false) AS lq_bound_complete,
        if(isNotNull(lq_hit), covered[coverage_index + 4].3, false) AS lq_pre_complete,
        if(isNotNull(lq_hit) AND notEmpty(cohort_totals[hit_index].1) AND cohort_totals[hit_index].3 != 0,
            cohort_totals[hit_index].2 * 2 < cohort_totals[hit_index].3, CAST(NULL, 'Nullable(Bool)')) AS lq_majority_lost,
        if(isNotNull(lq_hit), lq_sum(arrayMap(v -> if(lq_hit = 'bid', v.14, v.15), lq_venues_array)), CAST(NULL, 'Nullable(Decimal256(24))')) AS lq_average_hit
    FROM lq_state_cohort
),
lq_state_response AS (
    SELECT *,
        multiIf(isNull(lq_net) OR lq_net = 0 OR isNull(lq_bound), CAST(NULL, 'Nullable(Bool)'),
            abs(lq_net) < lq_bound, false, lq_bound_complete, true, CAST(NULL, 'Nullable(Bool)')) AS lq_absorbed,
        multiIf(lq_beyond = false, false, lq_beyond = true AND isNotNull(lq_net) AND isNotNull(lq_pre_bound),
            multiIf(abs(lq_net) < lq_pre_bound, true, lq_pre_complete, false, CAST(NULL, 'Nullable(Bool)')), CAST(NULL, 'Nullable(Bool)')) AS lq_excess
    FROM lq_state_depth
),
lq_state AS (
    SELECT *,
        multiIf(isNull(lq_net) OR lq_net = 0, CAST(NULL, 'Nullable(Bool)'),
            lq_absorbed = false OR lq_small_price = false, false, lq_absorbed AND lq_small_price, true, CAST(NULL, 'Nullable(Bool)')) AS lq_weak,
        if(isNotNull(depth_drops[hit_index]) AND isNotNull(lq_excess) AND isNotNull(lq_beyond),
            lq_beyond AND ifNull(lq_majority_lost, false) AND lq_excess, CAST(NULL, 'Nullable(Bool)')) AS lq_vacuum,
        multiIf(thin_sides[1] = true OR thin_sides[2] = true, true,
            thin_sides[1] = false AND thin_sides[2] = false, false, CAST(NULL, 'Nullable(Bool)')) AS lq_thin
    FROM lq_state_response
)
"""


def _base_fields():
    replenishment = _object(
        available=_bool("lq_replenishment_available"),
        bursts=_json("if(lq_replenishment_available, length(lq_observations), NULL)"),
        replenished=_json(
            "if(lq_replenishment_available, arrayCount(x -> "
            "JSONExtractRaw(x, 'replenished') = 'true', lq_observations), NULL)"
        ),
        resilient_bursts=_json(
            "if(lq_replenishment_available, arrayCount(x -> "
            "JSONExtractRaw(x, 'replenished') = 'true' AND "
            "JSONExtractRaw(x, 'resilient') = 'true', lq_observations), NULL)"
        ),
        median_recovery_s=_json(
            "lq_quantile(arrayFilter(x -> isNotNull(x), arrayMap(x -> "
            "lq_number(JSONExtractRaw(x, 'recovery_s')), lq_observations)), 0.5)"
        ),
        observations="concat('[', arrayStringConcat(lq_observations, ','), ']')",
    )
    elasticity = _object(
        bps_per_depth_unit=_json(
            "if(isNotNull(lq_raw_return) AND lq_net_usd != 0 AND lq_average_hit != 0, "
            "lq_raw_return * 10000 / nullIf(toFloat64(divideDecimal(abs(lq_net_usd), "
            "nullIf(lq_average_hit, lq_zero), 24)), 0), NULL)"
        ),
        weak_response=_bool("lq_weak"),
        excess_response=_bool("lq_excess"),
        hit_side=_json("lq_hit"),
        depth_unit=_json("lq_unit"),
        net_flow=_money("lq_net"),
        hit_side_depth_lower_bound=_money("lq_bound"),
        hit_side_covered_venues=_json("if(isNull(lq_hit), [], covered[coverage_index].1)"),
        hit_side_coverage_complete=_bool("lq_bound_complete"),
        pre_event_hit_side_depth_lower_bound=_money("lq_pre_bound"),
        pre_event_hit_side_covered_venues=_json("if(isNull(lq_hit), [], covered[coverage_index + 4].1)"),
        hit_side_depth_p1_usd=_money("if(isNull(lq_hit), NULL, side_totals[hit_index])"),
        pre_event_hit_side_depth_p1_usd=_money("if(isNull(lq_hit), NULL, side_totals[hit_index + 2])"),
        avg_hit_side_depth_p1_usd=_money("lq_average_hit"),
    )
    pre_window = _object(
        start=_json("lq_iso(ctx_anchor - INTERVAL 1 HOUR)"),
        cutoff=_json("lq_iso(ctx_anchor)"),
        anchor_type="'\"EVENT\"'",
    )
    absent_window = _object(start="'null'", cutoff="'null'", reason="'\"INCOMPLETE\"'")
    return dict(
        depth_measurement_band_pct="ctx_band_raw",
        per_venue=("concat('[', arrayStringConcat(arrayMap(v -> v.16, lq_venues_array), ','), ']')"),
        aggregate_depth_p1_usd=_money("lq_sum(arrayMap(v -> v.10, lq_venues_array))"),
        aggregate_bid_depth_p1_usd=_money("side_totals[1]"),
        aggregate_ask_depth_p1_usd=_money("side_totals[2]"),
        pre_event_depth_p1_usd=_money("lq_sum(arrayMap(v -> v.11, lq_venues_array))"),
        pre_event_depth_window=f"if(isNotNull(ctx_anchor), {pre_window}, {absent_window})",
        pre_event_side_depth_p1_usd=_object(bid=_money("side_totals[3]"), ask=_money("side_totals[4]")),
        depth_drop_by_side=_object(bid=_json("depth_drops[1]"), ask=_json("depth_drops[2]")),
        depth_drop=_json("if(isNull(lq_hit), NULL, depth_drops[hit_index])"),
        depth_majority_lost=_bool("lq_majority_lost"),
        depth_drop_cohort=_object(
            bid=_json("cohort_totals[1].1"),
            ask=_json("cohort_totals[2].1"),
        ),
        spread_ratio=_json(
            "lq_float_mean(arrayFilter(x -> isNotNull(x), "
            "arrayMap(v -> v.12, lq_venues_array))) / "
            "nullIf(lq_float_mean(arrayFilter(x -> isNotNull(x), "
            "arrayMap(v -> v.13, lq_venues_array))), 0)"
        ),
        replenishment=replenishment,
        elasticity=elasticity,
        vol_compression=_object(
            rv24="'null'", normal_range="'null'", state="'null'", reason="'\"INCOMPLETE\"'"
        ),
        liquidity_state=_json(
            "multiIf(lq_vacuum = true, 'VACUUM', lq_thin = true, 'THIN', "
            "lq_thin = false, 'NORMAL', CAST(NULL, 'Nullable(String)'))"
        ),
    )


def select_sql():
    return "SELECT " + _object(**_base_fields()) + " AS result FROM lq_state"


def enrichment_ctes():
    return r"""
{lq_deriv:String} AS lq_deriv,
((key, fallback) -> if(JSONHas(lq_config, key), JSONExtractRaw(lq_config, key), fallback)) AS lq_parameter,
ctx_end - INTERVAL 15 MINUTE AS lq_frag_start,
lq_number(lq_parameter('history_min_days', '7')) * 86400 AS lq_min_history_s,
lq_number(lq_parameter('historical_lookback_days', lq_parameter('base_retention_days', '90'))) AS lq_lookback_days,
lq_parse(JSONExtractRaw(lq_snapshot, 'assembled_at')) AS lq_assembled,
(row -> isNull(lq_assembled) OR isNull(lq_parse(JSONExtractRaw(row, 'received_ts')))
    OR lq_parse(JSONExtractRaw(row, 'received_ts')) <= lq_assembled) AS lq_available,
JSONExtractArrayRaw(lq_snapshot, 'instruments') AS lq_instruments,
arrayConcat(arrayReverse(JSONExtractArrayRaw(lq_snapshot, 'flow_segments')),
    arrayFlatten(arrayMap(bar -> arrayMap(piece -> JSONMergePatch(piece, concat('{"instrument_id":',
        toJSONString(JSONExtractString(bar, 'instrument_id')), '}')), JSONExtractArrayRaw(bar, 'boundary_segments')), ctx_all_flows))) AS lq_segment_inputs,
(row -> tuple(JSONExtractString(row, 'instrument_id'), JSONExtractString(row, 'start'), JSONExtractString(row, 'end'))) AS lq_segment_identity,
arrayMap(key -> lq_segment_inputs[indexOf(arrayMap(lq_segment_identity, lq_segment_inputs), key)],
    arrayDistinct(arrayMap(lq_segment_identity, lq_segment_inputs))) AS lq_segments,
((iid, left, right) -> arrayFilter(row -> JSONExtractString(row, 'instrument_id') = iid
    AND JSONExtractRaw(row, 'complete') = 'true' AND lq_parse(JSONExtractRaw(row, 'start')) >= left
    AND lq_parse(JSONExtractRaw(row, 'start')) < lq_parse(JSONExtractRaw(row, 'end'))
    AND lq_parse(JSONExtractRaw(row, 'end')) <= right AND lq_available(row)
    AND (isNull(lq_assembled) OR isNull(lq_parse(JSONExtractRaw(row, 'observed_at')))
        OR lq_parse(JSONExtractRaw(row, 'observed_at')) <= lq_assembled), lq_segments)) AS lq_segment_choices,
((choices, cursor) -> arraySort(row -> lq_parse(JSONExtractRaw(row, 'end')),
    arrayFilter(row -> lq_parse(JSONExtractRaw(row, 'start')) = cursor, choices))) AS lq_next_segments,
((iid, left, right) -> arrayFold((state, ignored) ->
    if(state.1 >= right OR empty(lq_next_segments(lq_segment_choices(iid, left, right), state.1)), state,
        tuple(lq_parse(JSONExtractRaw(lq_next_segments(lq_segment_choices(iid, left, right), state.1)[-1], 'end')),
            arrayPushBack(state.2, lq_next_segments(lq_segment_choices(iid, left, right), state.1)[-1]))),
    range(length(lq_segments) + 1), tuple(CAST(left, 'Nullable(DateTime64(6, \'UTC\'))'), CAST([], 'Array(String)')))) AS lq_segment_cover,
((left, right) -> arrayFilter(pair -> pair.1 < pair.2,
    arrayConcat(if(left != toStartOfMinute(left), [tuple(left, least(toStartOfMinute(left) + INTERVAL 1 MINUTE, right))], []),
        if(right != toStartOfMinute(right) AND NOT (left != toStartOfMinute(left) AND toStartOfMinute(left) = toStartOfMinute(right)),
            [tuple(greatest(left, toStartOfMinute(right)), right)], [])))) AS lq_partial_ranges,
((left, right) -> greatest(0, dateDiff('second', if(left = toStartOfMinute(left), left, toStartOfMinute(left) + INTERVAL 1 MINUTE), toStartOfMinute(right))) / 60) AS lq_expected_minutes,
((iid, left, right) -> arrayFilter(row -> JSONExtractString(row, 'instrument_id') = iid
    AND lq_time(row) >= left AND lq_time(row) + INTERVAL 1 MINUTE <= right, ctx_all_flows)) AS lq_instrument_bars,
((iid, left, right) -> length(arrayDistinct(arrayMap(lq_time, lq_instrument_bars(iid, left, right)))) = lq_expected_minutes(left, right)
    AND arrayAll(row -> JSONExtractRaw(row, 'complete') = 'true', lq_instrument_bars(iid, left, right))
    AND arrayAll(pair -> lq_segment_cover(iid, pair.1, pair.2).1 = pair.2, lq_partial_ranges(left, right))) AS lq_flow_complete,
((venue, left, right) -> arrayDistinct(arrayConcat(
    arrayMap(row -> JSONExtractString(row, 'instrument_id'), arrayFilter(row -> lq_venue(row) = venue AND lq_is_spot(row)
        AND (NOT JSONHas(row, 'token_id') OR JSONExtractString(row, 'token_id') = ctx_token), lq_instruments)),
    arrayMap(row -> JSONExtractString(row, 'instrument_id'), arrayFilter(row -> lq_is_spot(row) AND lq_venue(row) = venue
        AND lq_time(row) < right AND lq_time(row) + INTERVAL 1 MINUTE > left, ctx_all_flows))))) AS lq_venue_ids,
((venue, left, right) -> arrayFilter(row -> lq_is_spot(row) AND lq_venue(row) = venue
    AND lq_time(row) >= left AND lq_time(row) + INTERVAL 1 MINUTE <= right, ctx_all_flows)) AS lq_spot_bars,
((venue, left, right) -> arrayConcat(lq_spot_bars(venue, left, right),
    arrayFlatten(arrayMap(pair -> arrayFlatten(arrayMap(iid -> lq_segment_cover(iid, pair.1, pair.2).2,
        lq_venue_ids(venue, left, right))), lq_partial_ranges(left, right))))) AS lq_spot_rows,
((venue, left, right) -> notEmpty(lq_venue_ids(venue, left, right))
    AND length(arrayDistinct(arrayMap(lq_time, lq_spot_bars(venue, left, right)))) >= lq_expected_minutes(left, right)
    AND arrayAll(row -> JSONExtractRaw(row, 'complete') = 'true', lq_spot_rows(venue, left, right))
    AND arrayAll(pair -> arrayAll(iid -> lq_segment_cover(iid, pair.1, pair.2).1 = pair.2, lq_venue_ids(venue, left, right)), lq_partial_ranges(left, right))
    AND arrayAll(key -> isNotNull(lq_sum(arrayMap(row -> lq_dec(JSONExtractRaw(row, key)), lq_spot_rows(venue, left, right)))),
        ['buy_qty', 'sell_qty', 'sum_sq_qty'])) AS lq_spot_complete,
arrayDistinct(arrayConcat(arrayMap(row -> lq_venue(row), arrayFilter(row -> lq_is_spot(row)
    AND (NOT JSONHas(row, 'token_id') OR JSONExtractString(row, 'token_id') = ctx_token), lq_instruments)),
    arrayMap(row -> lq_venue(row), arrayFilter(row -> lq_is_spot(row) AND lq_time(row) < ctx_end
        AND lq_time(row) + INTERVAL 1 MINUTE > lq_frag_start, ctx_all_flows)))) AS lq_capture_registered,
arraySort(arrayFilter(venue -> lq_spot_complete(venue, lq_frag_start, ctx_end), lq_capture_registered)) AS lq_capture_venues,
arrayDistinct(JSONExtract(lq_spot, 'topn', 'venues', 'Array(String)')) AS lq_top_venues,
arrayMap(index -> tuple(ctx_start + toIntervalSecond(index * 900), ctx_start + toIntervalSecond((index + 1) * 900)),
    range(toUInt64(ifNull(greatest(0, dateDiff('microsecond', ctx_start, ctx_end)), 0) / 900000000))) AS lq_quarters,
((venue, left, right) -> lq_sum(arrayMap(row -> lq_dec(JSONExtractRaw(row, 'buy_qty')), lq_spot_rows(venue, left, right)))
    - lq_sum(arrayMap(row -> lq_dec(JSONExtractRaw(row, 'sell_qty')), lq_spot_rows(venue, left, right)))) AS lq_quarter_net,
((venue, left, right) -> if(lq_spot_complete(venue, left, right),
    if(multiplyDecimal(lq_quarter_net(venue, left, right), lq_quarter_net(venue, left, right), 24)
        >= multiplyDecimal(multiplyDecimal(lq_dec(lq_parameter('significant_flow_sigma', '2')),
            lq_dec(lq_parameter('significant_flow_sigma', '2')), 24),
            lq_sum(arrayMap(row -> lq_dec(JSONExtractRaw(row, 'sum_sq_qty')), lq_spot_rows(venue, left, right))), 24),
        sign(lq_quarter_net(venue, left, right)), 0), CAST(NULL, 'Nullable(Int8)'))) AS lq_quarter_sign,
arrayMap(venue -> arrayMap(pair -> lq_quarter_sign(venue, pair.1, pair.2), lq_quarters), lq_top_venues) AS lq_top_signs,
(values -> arrayFilter(x -> x != 0, values)) AS lq_directional_signs,
if(notEmpty(lq_top_venues) AND arrayAll(values -> length(values) >= 3 AND arrayAll(x -> isNotNull(x), values), lq_top_signs),
    arrayExists(values -> arrayCount(pair -> pair.1 != pair.2,
        arrayZip(arrayPopBack(lq_directional_signs(values)), arrayPopFront(lq_directional_signs(values)))) > 1, lq_top_signs),
    CAST(NULL, 'Nullable(Bool)')) AS lq_top_flips,
arrayFilter(book -> lq_is_spot(book) AND (NOT JSONHas(book, 'token_id') OR JSONExtractString(book, 'token_id') = ctx_token)
    AND JSONExtractRaw(book, 'integrity_ok') = 'true', ctx_all_books) AS lq_split_books,
arrayDistinct(arrayMap(lq_venue, lq_split_books)) AS lq_split_venues,
arrayMap(venue -> arraySort(book -> tuple(lq_time(book), -toInt64(indexOf(ctx_all_books, book))), arrayFilter(book -> lq_venue(book) = venue, lq_split_books))[-1], lq_split_venues) AS lq_split_latest,
lq_sum(arrayMap(book -> lq_dec(JSONExtractRaw(book, 'depth', 'ask', 'p1', 'usd')), lq_split_latest)) AS lq_split_total,
((tier, book) -> divideDecimal(multiplyDecimal(lq_tiers[tier], lq_dec(JSONExtractRaw(book, 'depth', 'ask', 'p1', 'usd')), 24), nullIf(lq_split_total, lq_zero), 24)) AS lq_allocation,
(tier -> arrayMap(book -> tuple(lq_allocation(tier, book), lq_walk_book(book, lq_allocation(tier, book), 'ask')), lq_split_latest)) AS lq_split_fills,
(tier -> if(lq_tiers[tier] != 0 AND lq_split_total != 0 AND notEmpty(lq_split_latest)
    AND arrayAll(fill -> isNotNull(fill.2), lq_split_fills(tier)),
    arraySum(arrayMap(fill -> toFloat64(divideDecimal(fill.1, nullIf(lq_tiers[tier], lq_zero), 24)) * fill.2, lq_split_fills(tier))), CAST(NULL, 'Nullable(Float64)'))) AS lq_split_impact,
(tier -> if(empty(lq_venues_array), CAST(NULL, 'Nullable(Float64)'),
    arrayMin(arrayFilter(x -> isNotNull(x), arrayMap(v -> lq_number(JSONExtractRaw(v.16, 'slippage_bps', tier)), lq_venues_array))))) AS lq_best_impact,
arraySort(arrayDistinct(arrayMap(lq_venue, lq_books))) AS lq_frag_venues,
arrayFlatten(arrayMap((venue, index) -> arrayMap(other -> tuple(venue, other), arraySlice(lq_frag_venues, index + 1)), lq_frag_venues, arrayEnumerate(lq_frag_venues))) AS lq_pairs,
((venue, at) -> arrayFilter(book -> lq_venue(book) = venue AND lq_time(book) <= at
    AND dateDiff('microsecond', lq_time(book), at) <= ctx_cadence * 1000000, lq_books)) AS lq_live_books,
((venue, at) -> arraySort(book -> tuple(lq_time(book), indexOf(ctx_all_books, book)), lq_live_books(venue, at))[-1]) AS lq_book_at,
(book -> upper(JSONExtractString(book, 'quote_asset'))) AS lq_quote,
(book -> if(lq_quote(book) = 'USD', lq_one, lq_dec(JSONExtractRaw(book, 'quote_usd_rate')))) AS lq_usd_rate,
lq_dec(lq_parameter('quote_conversion_uncertainty_bps', '2')) AS lq_conversion_bps,
((a, b) -> lq_quote(a) = lq_quote(b)) AS lq_same_quote,
((a, b, value) -> if(lq_same_quote(a, b), value, multiplyDecimal(value, nullIf(lq_usd_rate(a), lq_zero), 24))) AS lq_comparable_mid,
((a, b) -> isNotNull(lq_dec(JSONExtractRaw(a, 'mid'))) AND isNotNull(lq_dec(JSONExtractRaw(b, 'mid')))
    AND lq_dec(JSONExtractRaw(b, 'mid')) != 0 AND isNotNull(lq_dec(JSONExtractRaw(a, 'spread_bps')))
    AND isNotNull(lq_dec(JSONExtractRaw(b, 'spread_bps')))) AS lq_pair_prices,
((a, b) -> isNotNull(lq_comparable_mid(a, b, lq_dec(JSONExtractRaw(a, 'mid'))))
    AND lq_comparable_mid(b, a, lq_dec(JSONExtractRaw(b, 'mid'))) != 0) AS lq_pair_fx,
((a, b, am, bm, asp, bsp) -> abs(lq_comparable_mid(a, b, am) - lq_comparable_mid(b, a, bm)) * 10000
    > multiplyDecimal(asp + bsp + if(lq_same_quote(a, b), lq_zero, lq_conversion_bps), lq_comparable_mid(b, a, bm), 24)) AS lq_gap_values,
((a, b) -> lq_gap_values(a, b, lq_dec(JSONExtractRaw(a, 'mid')), lq_dec(JSONExtractRaw(b, 'mid')),
    lq_dec(JSONExtractRaw(a, 'spread_bps')), lq_dec(JSONExtractRaw(b, 'spread_bps')))) AS lq_gap,
(pair -> arraySort(arrayDistinct(arrayConcat([lq_frag_start, ctx_end], arrayMap(lq_time,
    arrayFilter(book -> has([pair.1, pair.2], lq_venue(book)) AND lq_time(book) >= lq_frag_start AND lq_time(book) < ctx_end, lq_books)))))) AS lq_pair_times,
(pair -> arrayMap(at -> tuple(at, lq_book_at(pair.1, at), lq_book_at(pair.2, at),
    notEmpty(lq_live_books(pair.1, at)) AND notEmpty(lq_live_books(pair.2, at))), lq_pair_times(pair))) AS lq_pair_states,
(state -> state.4 AND lq_pair_prices(state.2, state.3) AND lq_pair_fx(state.2, state.3)) AS lq_valid_pair_state,
(book -> JSONExtractRaw(book, 'independent_rest')) AS lq_rest,
(book -> JSONExtractRaw(book, 'second_path_confirmed') = 'true' AND JSONExtractString(lq_rest(book), 'source') = 'PUBLIC_REST'
    AND isNotNull(lq_parse(JSONExtractRaw(lq_rest(book), 'ts'))) AND lq_parse(JSONExtractRaw(lq_rest(book), 'ts')) <= ctx_end
    AND lq_available(lq_rest(book)) AND abs(dateDiff('microsecond', lq_parse(JSONExtractRaw(lq_rest(book), 'ts')), lq_time(book))) <= ctx_cadence * 1000000
    AND lq_dec(JSONExtractRaw(lq_rest(book), 'mid')) > 0 AND isNotNull(lq_dec(JSONExtractRaw(lq_rest(book), 'spread_bps')))) AS lq_rest_good,
(state -> lq_rest_good(state.2) AND lq_rest_good(state.3)
    AND ifNull(lq_gap_values(state.2, state.3, lq_dec(JSONExtractRaw(lq_rest(state.2), 'mid')),
        lq_dec(JSONExtractRaw(lq_rest(state.3), 'mid')), lq_dec(JSONExtractRaw(lq_rest(state.2), 'spread_bps')),
        lq_dec(JSONExtractRaw(lq_rest(state.3), 'spread_bps'))), false)) AS lq_second_gap,
(pair -> arrayAll(lq_valid_pair_state, lq_pair_states(pair))
    AND arrayAll(state -> lq_gap(state.2, state.3), arrayPopBack(lq_pair_states(pair)))) AS lq_persistent_pair,
arrayFilter(lq_persistent_pair, lq_pairs) AS lq_persistent_pairs,
arrayExists(pair -> arrayAll(lq_valid_pair_state, lq_pair_states(pair))
    AND lq_gap(lq_pair_states(pair)[-2].2, lq_pair_states(pair)[-2].3)
    AND NOT lq_gap(lq_pair_states(pair)[-1].2, lq_pair_states(pair)[-1].3), lq_pairs) AS lq_closed_gap,
arrayExists(pair -> hasAll(lq_capture_venues, [pair.1, pair.2])
    AND arrayExists(lq_second_gap, lq_pair_states(pair)), lq_persistent_pairs) AS lq_price_validated,
arrayFilter(pair -> arrayExists(index -> lq_pair_states(pair)[index].4 AND lq_pair_prices(lq_pair_states(pair)[index].2, lq_pair_states(pair)[index].3)
    AND NOT lq_pair_fx(lq_pair_states(pair)[index].2, lq_pair_states(pair)[index].3)
    AND arrayAll(lq_valid_pair_state, arraySlice(lq_pair_states(pair), 1, index - 1)), arrayEnumerate(lq_pair_states(pair))), lq_pairs) AS lq_unevaluated_pairs,
arrayDistinct(arrayMap(row -> JSONExtractString(row, 'instrument_id'), arrayFilter(row -> JSONExtractRaw(row, 'funding_8h_eq') NOT IN ('', 'null'), JSONExtractArrayRaw(lq_deriv, 'per_venue')))) AS lq_funding_ids,
arraySort(row -> tuple(lq_time(row.1), row.2), arrayFilter(row -> has(lq_funding_ids, JSONExtractString(row.1, 'instrument_id'))
    AND JSONExtractString(row.1, 'kind') = 'SETTLED' AND lq_time(row.1) < ctx_start
    AND lq_time(row.1) >= ctx_start - toIntervalSecond(toInt64(lq_lookback_days * 86400))
    AND isNotNull(lq_dec(JSONExtractRaw(row.1, 'rate'))) AND lq_dec(JSONExtractRaw(row.1, 'interval_h')) != 0,
    arrayZip(JSONExtractArrayRaw(lq_snapshot, 'funding'), arrayEnumerate(JSONExtractArrayRaw(lq_snapshot, 'funding'))))) AS lq_funding_history,
((iid, index) -> arrayFilter(row -> JSONExtractString(row.1, 'instrument_id') = iid, arraySlice(lq_funding_history, 1, index))) AS lq_funding_states,
(row -> divideDecimal(lq_dec(JSONExtractRaw(row.1, 'rate')) * 8, nullIf(lq_dec(JSONExtractRaw(row.1, 'interval_h')), lq_zero), 24)) AS lq_funding_rate,
arrayFilter(row -> length(lq_funding_ids) >= 2 AND arrayAll(iid -> notEmpty(lq_funding_states(iid, row.2)), lq_funding_ids),
    arrayMap(index -> tuple(lq_funding_history[index].1, index), arrayEnumerate(lq_funding_history))) AS lq_history_complete,
arrayMap(row -> tuple(lq_time(row.1), arrayMax(arrayMap(iid -> lq_funding_rate(lq_funding_states(iid, row.2)[-1]), lq_funding_ids))
    - arrayMin(arrayMap(iid -> lq_funding_rate(lq_funding_states(iid, row.2)[-1]), lq_funding_ids))), lq_history_complete) AS lq_dispersions,
arrayMap(x -> lq_number(x), JSONExtractArrayRaw(lq_parameter('normal_range_quantiles', '[0.025,0.975]'))) AS lq_quantiles,
((values, q) -> if(empty(values), CAST(NULL, 'Nullable(Decimal256(24))'),
    arraySort(values)[toUInt64(floor((length(values) - 1) * q)) + 1]
    + multiplyDecimal(arraySort(values)[toUInt64(ceil((length(values) - 1) * q)) + 1] - arraySort(values)[toUInt64(floor((length(values) - 1) * q)) + 1],
        toDecimal256(toString((length(values) - 1) * q - floor((length(values) - 1) * q)), 24), 24))) AS lq_money_quantile,
if(notEmpty(lq_dispersions) AND dateDiff('microsecond', lq_dispersions[1].1, lq_dispersions[-1].1) >= lq_min_history_s * 1000000,
    arrayMap(q -> lq_money_quantile(arrayMap(row -> row.2, lq_dispersions), q), lq_quantiles), CAST([], 'Array(Nullable(Decimal256(24)))')) AS lq_funding_normal,
lq_dec(JSONExtractRaw(lq_deriv, 'funding_dispersion_8h')) AS lq_funding_dispersion,
if(length(lq_funding_normal) = 2 AND isNotNull(lq_funding_dispersion),
    lq_funding_dispersion < lq_funding_normal[1] OR lq_funding_dispersion > lq_funding_normal[2], CAST(NULL, 'Nullable(Bool)')) AS lq_funding_beyond,
arrayFilter(row -> has(lq_funding_ids, JSONExtractString(row, 'instrument_id')) AND lq_time(row) <= ctx_end AND lq_available(row)
    AND JSONExtractRaw(row, 'independent_path_verified') = 'true' AND JSONExtractString(row, 'source_path') != ''
    AND JSONExtractString(row, 'primary_source_path') != '' AND JSONExtractString(row, 'source_path') != JSONExtractString(row, 'primary_source_path')
    AND isNotNull(lq_dec(JSONExtractRaw(row, 'rate_8h_eq'))), JSONExtractArrayRaw(lq_snapshot, 'funding_confirmations')) AS lq_confirmations,
arrayMap(iid -> arrayFilter(row -> JSONExtractString(row, 'instrument_id') = iid, lq_confirmations), lq_funding_ids) AS lq_secondary_rows,
arrayMap(rows -> lq_dec(JSONExtractRaw(rows[-1], 'rate_8h_eq')), lq_secondary_rows) AS lq_secondary_rates,
if(length(lq_funding_ids) >= 2 AND arrayAll(rows -> notEmpty(rows), lq_secondary_rows),
    arrayMax(lq_secondary_rates) - arrayMin(lq_secondary_rates), CAST(NULL, 'Nullable(Decimal256(24))')) AS lq_secondary_dispersion,
ifNull(lq_funding_beyond = true AND (lq_secondary_dispersion < lq_funding_normal[1] OR lq_secondary_dispersion > lq_funding_normal[2])
    AND arrayAll(iid -> lq_flow_complete(iid, lq_frag_start, ctx_end), lq_funding_ids), false) AS lq_funding_validated,
JSONExtractString(lq_relative, 'ref_venue') AS lq_reference_id,
arrayFilter(row -> JSONExtractString(row, 'instrument_id') = lq_reference_id AND JSONExtractRaw(row, 'complete') = 'true'
    AND lq_is_spot(row), JSONExtractArrayRaw(lq_snapshot, 'klines')) AS lq_reference_klines,
(row -> if(isNotNull(lq_parse(JSONExtractRaw(row, 'open_time'))), lq_parse(JSONExtractRaw(row, 'open_time'))
    + toIntervalSecond(if(JSONHas(row, 'interval_s'), JSONExtractInt(row, 'interval_s'), 60)),
    coalesce(lq_parse(JSONExtractRaw(row, 'close_time')), lq_time(row) + toIntervalSecond(if(JSONHas(row, 'interval_s'), JSONExtractInt(row, 'interval_s'), 60))))) AS lq_kline_close,
JSONExtract(lq_snapshot, 'stat_price_series', lq_reference_id, 'times_us', 'Array(Int64)') AS lq_projected_times,
JSONExtractArrayRaw(lq_snapshot, 'stat_price_series', lq_reference_id, 'values') AS lq_projected_values,
least(length(lq_projected_times), length(lq_projected_values)) AS lq_projected_count,
arrayConcat(arrayMap(row -> tuple(lq_kline_close(row), lq_number(if(JSONHas(row, 'c'), JSONExtractRaw(row, 'c'), JSONExtractRaw(row, 'close')))), lq_reference_klines),
    if(JSONExtractString(lq_snapshot, 'stat_price_series', lq_reference_id, 'time_basis') = 'CLOSE_TIME',
        arrayFilter(point -> point.2 > 0, arrayMap((micros, value) -> tuple(fromUnixTimestamp64Micro(micros, 'UTC'), lq_number(value)),
            arraySlice(lq_projected_times, 1, lq_projected_count), arraySlice(lq_projected_values, 1, lq_projected_count))), [])) AS lq_reference_points_raw,
arrayFilter(point -> point.1 <= ctx_end AND point.2 != 0 AND toMinute(point.1) = 0 AND toSecond(point.1) = 0, lq_reference_points_raw) AS lq_hourly_inputs,
arraySort(point -> point.1, arrayMap(at -> arrayFilter(point -> point.1 = at, lq_hourly_inputs)[-1],
    arrayDistinct(arrayMap(point -> point.1, lq_hourly_inputs)))) AS lq_hourly,
arrayFilter(index -> index > 1 AND lq_hourly[index - 1].1 = lq_hourly[index].1 - INTERVAL 1 HOUR, arrayEnumerate(lq_hourly)) AS lq_hourly_pairs,
arrayMap(index -> tuple(lq_hourly[index].1, log(lq_hourly[index].2 / lq_hourly[index - 1].2)), lq_hourly_pairs) AS lq_hourly_returns,
(at -> arrayMap(row -> row.2, arrayFilter(row -> row.1 > at - INTERVAL 24 HOUR AND row.1 <= at, lq_hourly_returns))) AS lq_rv_inputs,
(values -> sqrt(arraySum(arrayMap(x -> pow(x - lq_float_mean(values), 2), values)) / greatest(length(values) - 1, 1))) AS lq_sample_stdev,
arrayMap(point -> tuple(point.1, lq_sample_stdev(lq_rv_inputs(point.1))), arrayFilter(point -> length(lq_rv_inputs(point.1)) = 24, lq_hourly)) AS lq_rv_points,
if(empty(lq_rv_points), CAST(NULL, 'Nullable(Float64)'), lq_rv_points[-1].2) AS lq_current_rv,
if(notEmpty(lq_hourly) AND dateDiff('microsecond', lq_hourly[1].1, lq_hourly[-1].1) >= lq_min_history_s * 1000000,
    arrayMap(row -> row.2, arrayFilter(row -> row.1 >= ctx_start - toIntervalSecond(toInt64(lq_lookback_days * 86400)) AND row.1 < ctx_start, lq_rv_points)), []) AS lq_rv_history,
if(notEmpty(lq_rv_history), arrayMap(q -> lq_quantile(lq_rv_history, q), lq_quantiles), CAST([], 'Array(Nullable(Float64))')) AS lq_rv_normal
"""


def window_select_sql():
    fields = _base_fields()
    fields.update(
        window=_json("ctx_primary"),
        start=_json("lq_iso(ctx_start)"),
        cutoff=_json("lq_iso(ctx_end)"),
        best_single_venue_slippage_bps=_object(
            **{tier: _json(f"lq_best_impact('{tier}')") for tier in ("T1", "T2", "T3")}
        ),
        split_slippage_bps=_object(
            **{tier: _json(f"lq_split_impact({i})") for i, tier in enumerate(("T1", "T2", "T3"), 1)}
        ),
        vol_compression=_object(
            window="'\"W24H\"'",
            cutoff=_json("lq_iso(ctx_end)"),
            rv24=_json("lq_current_rv"),
            normal_range="if(empty(lq_rv_normal), 'null', toJSONString(lq_rv_normal))",
            state=_json(
                "multiIf(isNull(lq_current_rv) OR empty(lq_rv_normal), CAST(NULL, 'Nullable(String)'), lq_current_rv < lq_rv_normal[1], 'COMPRESSED', lq_current_rv > lq_rv_normal[2], 'EXPANDED', 'NORMAL')"
            ),
            reason=_json(
                "if(isNotNull(lq_current_rv) AND notEmpty(lq_rv_normal), CAST(NULL, 'Nullable(String)'), 'INCOMPLETE')"
            ),
        ),
        fragmentation=_object(
            window="'\"W15M\"'",
            cutoff=_json("lq_iso(ctx_end)"),
            persistent_gap=_bool("if(notEmpty(lq_books), notEmpty(lq_persistent_pairs), NULL)"),
            validated=_bool("lq_price_validated OR lq_funding_validated"),
            price_path_validated=_bool("lq_price_validated"),
            funding_path_validated=_bool("lq_funding_validated"),
            funding_beyond_normal=_bool("lq_funding_beyond"),
            funding_normal_range_8h="if(empty(lq_funding_normal), 'null', concat('[', arrayStringConcat(arrayMap(x -> if(isNull(x), 'null', toJSONString(toString(x))), lq_funding_normal), ','), ']'))",
            top_sign_flips=_bool("lq_top_flips"),
            gap_closed_quickly=_bool("lq_closed_gap"),
            venues=_json("arrayMap(pair -> [pair.1, pair.2], lq_persistent_pairs)"),
            cross_quote_not_evaluated=_json("arrayMap(pair -> [pair.1, pair.2], lq_unevaluated_pairs)"),
            quote_conversion_uncertainty_bps=_money("lq_conversion_bps"),
            complete_venues=_json("lq_capture_venues"),
            reason=_json(
                "if(notEmpty(lq_books) OR isNotNull(lq_funding_dispersion), CAST(NULL, 'Nullable(String)'), 'INCOMPLETE')"
            ),
        ),
    )
    timing = _object(
        window=_json("ctx_primary"), start=_json("lq_iso(ctx_start)"), cutoff=_json("lq_iso(ctx_end)")
    )
    for name in ("elasticity", "replenishment"):
        fields[name] = (
            f"concat(substring({fields[name]}, 1, length({fields[name]}) - 1), ',', substring({timing}, 2))"
        )
    result = _object(**fields)
    with_coverage = _object(
        coverage=_object(venues=_json("arrayMap(v -> v.1, lq_venues_array)")),
        observed_at=_json("lq_iso(arrayMin(arrayMap(v -> lq_time(v.18), lq_venues_array)))"),
    )
    result = f"if(empty(lq_venues_array), {result}, concat(substring({result}, 1, length({result}) - 1), ',', substring({with_coverage}, 2)))"
    unavailable = _object(
        window=_json("ctx_primary"),
        start="'null'",
        cutoff=_json("lq_iso(ctx_end)"),
        reason="'\"INCOMPLETE\"'",
    )
    result = (
        "if(ctx_primary = 'EVENT' AND JSONHas(lq_snapshot, '_event_anchor') "
        "AND JSONExtractRaw(lq_snapshot, '_event_anchor') = 'null', " + unavailable + ", " + result + ")"
    )
    return "SELECT " + result + " AS result FROM lq_state"


def window_query(snapshot, config=None, *, spot=None, relative=None, deriv=None):
    _, parameters = query(snapshot, config, spot=spot, relative=relative)
    parameters["lq_deriv"] = (parameter_string(canonical_json(_transport(deriv or {}))), "String")
    return "WITH " + ctes() + ",\n" + enrichment_ctes() + "\n" + window_select_sql(), parameters


def windows_query(snapshot, config=None, *, spots=None, relative=None, deriv=None):
    _, parameters = query(snapshot, config, spot=spots, relative=relative)
    parameters["lq_deriv"] = (parameter_string(canonical_json(_transport(deriv or {}))), "String")
    selections = []
    for name in ("W15M", "W1H", "W4H", "W24H", "EVENT"):
        scoped_ctes = ctes().replace(
            "{lq_snapshot:String} AS lq_snapshot",
            "JSONMergePatch({lq_snapshot:String}, "
            + _object(_primary=f"'\"{name}\"'", _start="'null'", start="'null'")
            + ") AS lq_snapshot",
        )
        scoped_ctes = scoped_ctes.replace(
            "{lq_spot:String} AS lq_spot",
            f"JSONExtractRaw({{lq_spot:String}}, '{name}') AS lq_spot",
        )
        scoped_enrichment = enrichment_ctes().replace(
            "{lq_deriv:String} AS lq_deriv",
            f"JSONExtractRaw({{lq_deriv:String}}, '{name}') AS lq_deriv",
        )
        selections.append(
            "SELECT '"
            + name
            + "' AS name, result FROM (WITH "
            + scoped_ctes
            + ",\n"
            + scoped_enrichment
            + "\n"
            + window_select_sql()
            + ")"
        )
    statement = (
        "WITH lq_window_rows AS (" + " UNION ALL ".join(selections) + "), "
        "lq_window_collection AS (SELECT groupArray(tuple(name, result)) AS items "
        "FROM lq_window_rows) SELECT concat("
        "substring(arrayFirst(item -> item.1 = if(JSONExtractString({lq_snapshot:String}, "
        "'_primary') = '', 'W4H', JSONExtractString({lq_snapshot:String}, '_primary')), "
        "items).2, 1, length(arrayFirst(item -> item.1 = "
        "if(JSONExtractString({lq_snapshot:String}, '_primary') = '', 'W4H', "
        "JSONExtractString({lq_snapshot:String}, '_primary')), items).2) - 1), "
        "',\"windows\":{', arrayStringConcat(arrayMap(item -> "
        "concat(toJSONString(item.1), ':', item.2), items), ','), '}}') "
        "AS result FROM lq_window_collection"
    )
    return statement, parameters


def query(snapshot, config=None, *, spot=None, relative=None):
    inputs = {
        "lq_snapshot": snapshot,
        "lq_config": config or {},
        "lq_spot": spot if spot is not None else snapshot.get("_spot", {"venues": []}),
        "lq_relative": (relative if relative is not None else snapshot.get("_relative", {"windows": {}})),
    }
    parameters = {
        name: (parameter_string(canonical_json(_transport(value))), "String")
        for name, value in inputs.items()
    }
    return "WITH " + ctes() + "\n" + select_sql(), parameters
