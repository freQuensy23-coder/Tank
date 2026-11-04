import json
from array import array

from tank.analysis import truth_sql_derivatives, truth_sql_relative, truth_sql_spot, truth_sql_support
from tank.clickhouse_market import parameter_string
from tank.util import jsonable

SPOT_METRICS = frozenset(
    {"spot.cohort.direction", "spot.cohort.cwb_buy", "spot.cohort.cwb_sell", "spot.classification"}
)
OI_METRICS = frozenset(
    {"deriv.native_oi_chg_pct", "deriv.native_oi_vs_normal", "deriv.oi_class"}
)
STATE_METRICS = frozenset({"mt.move_class", "mt.divergence_state"})
RELATIVE_METRICS = frozenset({"price.repricing_band", "price.vs_anchor_pct"})
DOMAIN_FIELDS = {
    "spot": ("flow_bars", "flow_segments", "tickers", "instruments"),
    "oi": ("oi", "instruments", "semantics", "capture_metadata"),
    "funding": ("funding", "instruments", "semantics", "capture_metadata"),
    "close": ("flow_bars", "flow_segments", "klines", "instruments"),
    "dc": (
        "flow_bars", "flow_segments", "tickers", "instruments", "semantics", "capture_metadata",
        "oi", "funding", "klines", "books", "liquidations", "supply", "anchor",
    ),
    "depth": ("books", "instruments", "anchor", "capture_metadata"),
    "state": (
        "flow_bars", "flow_segments", "reference_points", "tickers", "instruments", "tokens",
        "sector_baskets", "semantics", "capture_metadata", "oi", "funding", "klines", "books",
        "liquidations", "supply", "anchor", "expected_direction", "funding_confirmations",
        "stat_price_series", "stat_volume_series", "series_coverage_counts",
        "relative_history_dependency_fingerprint",
    ),
    "relative": (
        "flow_bars", "flow_segments", "reference_points", "instruments", "tokens", "sector_baskets",
        "klines", "anchor", "expected_direction", "funding_confirmations", "stat_price_series",
        "series_coverage_counts", "relative_history_dependency_fingerprint",
    ),
}

STATE_NODE_FIELDS = {
    "spot": ("flow_bars", "flow_segments", "tickers", "instruments", "anchor"),
    "relative": (
        "klines", "stat_price_series", "flow_bars", "flow_segments", "reference_points", "tokens",
        "sector_baskets", "instruments", "expected_direction",
    ),
    "derivatives": ("oi", "funding", "liquidations", "semantics", "instruments", "klines"),
    "supply": ("supply", "anchor"),
}


def _transport(value):
    if isinstance(value, dict):
        return {key: _transport(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, array, bytes, bytearray, memoryview)):
        return [_transport(item) for item in value]
    return jsonable(value)


def _encode(value, *, canonical=False):
    if canonical:
        return json.dumps(
            jsonable(value), separators=(",", ":"), sort_keys=True, allow_nan=False,
            default=lambda item: item.tolist() if isinstance(item, array) else str(item),
        )
    return json.dumps(
        _transport(value), separators=(",", ":"), ensure_ascii=False, allow_nan=False,
    )


def _object(**fields):
    return "concat('{', " + ", ".join(
        part
        for index, (key, value) in enumerate(fields.items())
        for part in (("','",) if index else ()) + (repr('"' + key + '":'), value)
    ) + ", '}')"


def _core():
    core = truth_sql_support._core().replace("sup_", "con_")
    replacements = {
        "{con_snapshot:String} AS con_snapshot": "{con_snapshot:String} AS con_input_snapshot",
        "{con_spots:String}": "'{}'",
        "{con_derivatives:String}": "'{}'",
        "{con_liquidity:String}": "'{}'",
        "{con_relative:String}": "'{}'",
        "{con_supply_preimages:String}": "'[]'",
    }
    for old, new in replacements.items():
        core = core.replace(old, new)
    core = core.replace(
        "con_parse(JSONExtractRaw(con_snapshot, 'cutoff')) AS con_end",
        "con_parse(JSONExtractRaw(con_input_snapshot, 'cutoff')) AS con_end",
    ).replace(
        "con_parse(JSONExtractRaw(con_snapshot, 'assembled_at')) AS con_assembled",
        "con_parse(JSONExtractRaw(con_input_snapshot, 'assembled_at')) AS con_assembled",
    )
    core = core.replace("AS con_parse,", "AS con_unchecked_parse,")
    return core + r""",
(x -> if(con_text(con_time_unwrap(x)) IN ('', 'null'), CAST(NULL, 'Nullable(DateTime64(6, \'UTC\'))'),
    if(throwIf(isNull(con_unchecked_parse(x)) OR match(con_text(con_time_unwrap(x)), '^[0-9]{4}-')
        AND toInt32OrZero(substring(con_text(con_time_unwrap(x)), 1, 4)) NOT BETWEEN 1900 AND 2299,
        'Market Truth timestamp outside native supported range') = 0, con_unchecked_parse(x), NULL))) AS con_parse,
{con_requirements:String} AS con_requirements,
{con_canonical_snapshot:String} AS con_canonical_input,
{con_canonical_config:String} AS con_canonical_config,
{con_result:String} AS con_full_result,
arrayMap(r -> tuple(JSONExtractString(r, 'key'), JSONExtractRaw(r, 'value')), JSONExtractArrayRaw({con_cache_records:String})) AS con_initial_cache,
((c, k) -> arrayExists(r -> r.1 = k, c)) AS con_cache_has,
((c, k) -> arrayFirst(r -> r.1 = k, c).2) AS con_cache_get,
((c, k, v) -> if(con_cache_has(c, k), c, arraySlice(arrayPushBack(c, tuple(k, assumeNotNull(v))), -128))) AS con_cache_store,
(c -> con_array(arrayMap(r -> concat('{"key":', toJSONString(r.1), ',"value":', r.2, '}'), c))) AS con_cache_json,
arrayExists(r -> con_domain(r.1) = 'depth', con_supported_requests) AND con_number(con_parameter('depth_band_pct', '1')) NOT IN (0.5, 1., 2.) AS con_depth_key_guard,
(x -> concat('{', arrayStringConcat(arrayMap(p -> concat(toJSONString(p.1), ':', p.2), x), ','), '}')) AS con_pairs,
((x, keys) -> con_pairs(arrayFilter(p -> NOT has(keys, p.1), JSONExtractKeysAndValuesRaw(x)))) AS con_strip,
((x, y) -> con_patch(x, arrayMap(p -> p.1, JSONExtractKeysAndValuesRaw(y)), arrayMap(p -> p.2, JSONExtractKeysAndValuesRaw(y)))) AS con_merge,
(x -> lower(hex(SHA256(x)))) AS con_hash,
(x -> con_pairs(arraySort(p -> p.1, x))) AS con_sorted_object,
((c, r) -> isNotNull(con_time(r)) AND con_time(r) <= con_end
    AND (c NOT IN ('klines', 'flow_bars') OR con_offset(con_time(r), ifNull(con_number(JSONExtractRaw(r, 'interval_s')), 60.)) <= con_end)
    AND (isNull(con_assembled) OR JSONExtractRaw(r, 'received_ts') IN ('', 'null', 'false', '0', '""')
        OR con_parse(JSONExtractRaw(r, 'received_ts')) <= con_assembled)) AS con_row_admitted,
['flow_bars', 'klines', 'books', 'oi', 'funding', 'liquidations', 'tickers', 'supply'] AS con_bounded_collections,
(x -> tuple(JSONExtractString(x, 'instrument_id'), JSONExtractRaw(x, 'start'), JSONExtractRaw(x, 'end'))) AS con_segment_key,
(x -> arrayConcat(JSONExtractArrayRaw(x, 'flow_segments'), arrayFlatten(arrayMap(b -> arrayMap(p -> con_patch(p,
    ['instrument_id', 'token_id', 'venue', 'market_type'], [con_raw(JSONExtractRaw(b, 'instrument_id')),
    con_raw(JSONExtractRaw(b, 'token_id')), toJSONString(con_venue(b)),
    if(JSONExtractString(b, 'market_type') != '', JSONExtractRaw(b, 'market_type'), toJSONString(if(con_is_spot(b), 'SPOT', 'PERP_LINEAR')))]),
    JSONExtractArrayRaw(b, 'boundary_segments')), JSONExtractArrayRaw(x, 'flow_bars'))))) AS con_segment_candidates,
(x -> arrayMap(k -> if(arrayExists(r -> con_segment_key(r) = k, JSONExtractArrayRaw(x, 'flow_segments')),
    arrayLast(r -> con_segment_key(r) = k, JSONExtractArrayRaw(x, 'flow_segments')),
    arrayFirst(r -> con_segment_key(r) = k, con_segment_candidates(x))), arrayDistinct(arrayMap(con_segment_key, con_segment_candidates(x))))) AS con_segments,
(x -> arrayFilter(r -> con_parse(JSONExtractRaw(r, 'end')) <= con_end
    AND (isNull(con_assembled) OR JSONExtractRaw(r, 'received_ts') IN ('', 'null') OR con_parse(JSONExtractRaw(r, 'received_ts')) <= con_assembled), con_segments(x))) AS con_bounded_segments,
(x -> arrayConcat(JSONExtractArrayRaw(x, 'reference_points'), arrayFlatten(arrayMap(b -> arrayMap(p -> con_patch(p,
    ['instrument_id'], [con_raw(JSONExtractRaw(b, 'instrument_id'))]), arrayFilter(p -> JSONExtractRaw(p, 'boundary') NOT IN ('', 'null')
    AND con_parse(JSONExtractRaw(p, 'boundary')) <= con_end, JSONExtractArrayRaw(b, 'reference_points'))), JSONExtractArrayRaw(x, 'flow_bars'))))) AS con_references,
(x -> con_patch(x, arrayConcat(con_bounded_collections, ['flow_segments', 'reference_points']),
    arrayConcat(arrayMap(c -> con_array(arrayFilter(r -> con_row_admitted(c, r), JSONExtractArrayRaw(x, c))), con_bounded_collections),
        [con_array(con_bounded_segments(x)), con_array(con_references(x))]))) AS con_bound,
con_strip(con_bound(con_input_snapshot), ['_primary', '_event_anchor']) AS con_snapshot,
con_patch(con_bound(con_canonical_input), ['flow_segments', 'reference_points'],
    [con_array(arrayMap(r -> con_sorted_object(JSONExtractKeysAndValuesRaw(r)), con_bounded_segments(con_canonical_input))),
    con_array(arrayMap(r -> con_sorted_object(JSONExtractKeysAndValuesRaw(r)), con_references(con_canonical_input)))]) AS con_canonical_snapshot,
arraySort(arrayDistinct(arrayMap(r -> tuple(JSONExtractString(r, 'metric'),
    if(JSONHas(r, 'window'), JSONExtractString(r, 'window'), 'W1H')), JSONExtractArrayRaw(con_requirements)))) AS con_requested,
((m) -> multiIf(startsWith(m, 'spot.') AND has(['spot.cohort.direction', 'spot.cohort.cwb_buy', 'spot.cohort.cwb_sell', 'spot.classification'], m), 'spot',
    has(['deriv.native_oi_chg_pct', 'deriv.native_oi_vs_normal', 'deriv.oi_class'], m), 'oi', m = 'deriv.funding_vs_normal', 'funding',
    m = 'price.close_vs_level', 'close', m = 'dc.class', 'dc', has(['liq.depth_p1_vs_pre', 'liq.spread_ratio'], m), 'depth',
    has(['mt.move_class', 'mt.divergence_state'], m), 'state', has(['price.repricing_band', 'price.vs_anchor_pct'], m), 'relative', '')) AS con_domain,
(w -> multiIf(w = 'EVENT' AND con_has_anchor, con_seconds(con_anchor, con_end), w = 'W15M', 900., w = 'W1H', 3600., w = 'W4H', 14400., w = 'W24H', 86400., 0.)) AS con_duration,
(r -> con_duration(r.2) > 0 AND con_domain(r.1) != '') AS con_supported,
arrayFilter(con_supported, con_requested) AS con_supported_requests,
arrayDistinct(arrayMap(r -> tuple(con_domain(r.1), r.2), con_supported_requests)) AS con_domain_windows,
con_parse(JSONExtractRaw(con_input_snapshot, 'cutoff')) AS con_input_end,
con_parse(JSONExtractRaw(con_input_snapshot, 'assembled_at')) AS con_input_assembled
"""


def _timestamp_ctes():
    freshness = truth_sql_support._freshness().replace("sup_", "con_")
    return freshness + r""",
((r, field, aliases) -> con_patch(r, ['ts', 'ts_prec', 'ts_precision', 'ts_prec_reason', 'ts_bucket_size_s', 'ts_bucket_anchor', 'timestamp_precision', 'precision', 'timestamp_precision_reason', 'precision_reason'],
    [con_raw(JSONExtractRaw(r, field)), con_raw(JSONExtractRaw(r, concat(field, '_prec'))), con_raw(JSONExtractRaw(r, concat(field, '_precision'))),
    coalesce(nullIf(JSONExtractRaw(r, concat(field, '_prec_reason')), ''), nullIf(JSONExtractRaw(r, concat(field, '_precision_reason')), ''), 'null'), con_raw(JSONExtractRaw(r, concat(field, '_bucket_size_s'))),
    con_raw(JSONExtractRaw(r, concat(field, '_bucket_anchor'))), if(has(aliases, 'timestamp_precision'), con_raw(JSONExtractRaw(r, 'timestamp_precision')), 'null'),
    if(has(aliases, 'precision'), con_raw(JSONExtractRaw(r, 'precision')), 'null'), if(has(aliases, 'timestamp_precision'), con_raw(JSONExtractRaw(r, 'timestamp_precision_reason')), 'null'),
    if(has(aliases, 'precision'), con_raw(JSONExtractRaw(r, 'precision_reason')), 'null')])) AS con_timestamp_row_raw,
((r, field, aliases) -> con_pairs(arrayFilter(p -> has(['ts', 'ts_prec', 'ts_precision', 'ts_prec_reason', 'ts_bucket_size_s', 'ts_bucket_anchor', 'timestamp_precision', 'precision', 'timestamp_precision_reason', 'precision_reason'], p.1)
    AND (p.1 != 'ts_bucket_anchor' OR JSONHas(r, concat(field, '_bucket_anchor'))), JSONExtractKeysAndValuesRaw(con_timestamp_row_raw(r, field, aliases))))) AS con_timestamp_row,
((r, field, output, offset, aliases) -> con_timestamp_fields(con_timestamp_row(r, field, aliases), output, offset)) AS con_timestamp,
((r, output, offset) -> con_patch('{}', [output, concat(output, '_prec'), concat(output, '_prec_reason')],
    [toJSONString(con_iso(con_offset(con_time(r), offset))), toJSONString(if(isNull(con_time(r)) AND con_precision(r) != 'COLLECTION_ONLY', NULL, con_precision(r))),
    if(isNotNull(con_time(r)) AND isNotNull(con_precision(r)), 'null', toJSONString(con_precision_reason(r)))])) AS con_timestamp_base,
((r, output, offset) -> if(isNotNull(con_time(r)) AND isNotNull(con_precision_anchor(r)), con_patch(con_timestamp_base(r, output, offset),
    [concat(output, '_bucket_anchor')], [toJSONString(con_precision_anchor(r))]), con_timestamp_base(r, output, offset))) AS con_timestamp_fields
"""


def _dependency_ctes():
    cases = []
    for domain, fields in DOMAIN_FIELDS.items():
        names = "[" + ", ".join(repr(field) for field in sorted(fields)) + "]"
        cases.extend((repr(domain), names))
    branches = ", ".join(
        f"d = {domain}, {fields}" for domain, fields in zip(cases[::2], cases[1::2], strict=True)
    )
    node_branches = ", ".join(
        f"d = {domain!r}, [" + ", ".join(repr(field) for field in sorted(fields)) + "]"
        for domain, fields in STATE_NODE_FIELDS.items()
    )
    return r"""
(d -> multiIf(__BRANCHES__, CAST([], 'Array(String)'))) AS con_domain_fields,
(d -> con_raw(JSONExtractRaw(con_canonical_snapshot, 'domain_dependency_fingerprints', d))) AS con_explicit_dependency,
(d -> if(con_explicit_dependency(d) = 'null', con_hash(con_pairs(arrayMap(k -> tuple(k, con_raw(JSONExtractRaw(con_canonical_snapshot, k))), con_domain_fields(d)))),
    if(JSONType(con_explicit_dependency(d)) = 'String', con_text(con_explicit_dependency(d)), con_hash(con_explicit_dependency(d))))) AS con_dependency,
(d -> con_patch(con_pairs(arrayMap(k -> tuple(k, if(JSONHas(con_snapshot, k), JSONExtractRaw(con_snapshot, k), '[]')), con_domain_fields(d))),
    ['token_id', 'cutoff', 'assembled_at', 'domain_dependency_fingerprints'],
    [con_raw(JSONExtractRaw(con_snapshot, 'token_id')), toJSONString(con_iso(con_end)), con_raw(JSONExtractRaw(con_snapshot, 'assembled_at')),
    if(JSONHas(con_snapshot, 'domain_dependency_fingerprints'), con_raw(JSONExtractRaw(con_snapshot, 'domain_dependency_fingerprints')), '{}')])) AS con_selected_snapshot,
((d, w) -> toJSONString(tuple(d, w, if(d IN ('state', 'dc'), con_seconds(con_start(con_primary), con_end), con_duration(w)),
    con_iso(con_end), con_dependency(d), con_hash(con_canonical_config), con_hash(con_raw(JSONExtractRaw(con_canonical_snapshot, 'expected_direction')))))) AS con_cache_key,
(d -> multiIf(__NODE_BRANCHES__, CAST([], 'Array(String)'))) AS con_node_fields,
(k -> if(JSONHas(con_canonical_snapshot, k), JSONExtractRaw(con_canonical_snapshot, k), '[]')) AS con_node_field,
(d -> if(con_explicit_dependency(d) = 'null', con_hash(con_pairs(arrayMap(k -> tuple(k, con_node_field(k)), con_node_fields(d)))),
    if(JSONType(con_explicit_dependency(d)) = 'String', con_text(con_explicit_dependency(d)), con_hash(con_explicit_dependency(d))))) AS con_node_dependency,
(d -> toJSONString(tuple('condition-state-node', d, con_iso(coalesce(con_anchor, con_offset(con_end, -14400.))), con_iso(con_end),
    con_node_dependency(d), con_hash(con_canonical_config), con_hash(con_node_field('expected_direction'))))) AS con_node_key
""".replace("__BRANCHES__", branches).replace("__NODE_BRANCHES__", node_branches)


def _spot_ctes(*, close=False):
    sql = truth_sql_spot.ctes().replace("spot_", "con_flow_")
    sql = sql.replace("{con_flow_snapshot:String}", "con_snapshot")
    sql = sql.replace("{con_flow_sigma:String}", "con_text(con_parameter('significant_flow_sigma', '2'))")
    sql = sql.replace("{con_flow_min_venues:UInt64}", "toUInt64(con_number(con_parameter('min_spot_venues', '2')))")
    sql = sql.replace(
        "('EVENT', assumeNotNull(con_flow_cutoff - con_flow_start))] AS con_flow_window_defs",
        "('EVENT', assumeNotNull(con_flow_cutoff - con_flow_start))]) AS con_flow_window_defs",
    )
    sql = sql.replace("[('W15M', toInt64(900000000))", "arrayFilter(w -> has(con_spot_windows, w.1), [('W15M', toInt64(900000000))")

    sql = sql.replace("con_flow_start >= con_flow_cutoff OR ", "")
    selected = truth_sql_spot.select_sql().replace("spot_", "con_flow_")
    selected = selected.replace("{con_flow_sigma:String}", "con_text(con_parameter('significant_flow_sigma', '2'))")
    selected = selected.replace("{con_flow_min_venues:UInt64}", "toUInt64(con_number(con_parameter('min_spot_venues', '2')))")
    result = r"""
arrayDistinct(arrayConcat(arrayMap(r -> r.2, arrayFilter(r -> con_domain(r.1) = 'spot', con_supported_requests)),
    if(arrayExists(r -> con_domain(r.1) = 'dc', con_supported_requests), [con_primary], []),
    if(arrayExists(r -> con_domain(r.1) = 'state', con_supported_requests), ['EVENT'], []))) AS con_spot_windows,
""" + sql + ",\ncon_spot_graph AS (" + selected + r"""),
(SELECT result FROM con_spot_graph) AS con_spot_graph_json,
(w -> con_patch(con_strip(JSONExtractRaw(con_spot_graph_json, 'spots', w), ['window', 'start', 'cutoff', 'observed_at', 'precision']),
    ['composite_cohort', 'cohort_consistent'], ['null', '{}'])) AS con_spot_value
"""
    if close:
        beginning = result.index("arrayDistinct(arrayConcat(")
        ending = result.index(" AS con_spot_windows,") + len(" AS con_spot_windows,")
        result = result[:beginning] + "['W24H'] AS con_spot_windows," + result[ending:]
        result = result.replace("con_flow_", "con_close_flow_").replace("con_spot_", "con_close_spot_")
        result = result.replace("SELECT con_snapshot AS con_close_flow_document", "SELECT con_close_snapshot AS con_close_flow_document")
    return result


def _derivative_ctes():
    sql = _derivative_helpers("con_oi_", "con_selected_snapshot('oi')")
    sql += ",\n" + _derivative_helpers("con_funding_", "con_selected_snapshot('funding')")
    return sql + r""",
((d, w) -> if(d = 'oi', con_oi_window(toUnixTimestamp64Micro(con_offset(con_end, -con_duration(w))), toUnixTimestamp64Micro(con_end)),
    con_funding_window(toUnixTimestamp64Micro(con_offset(con_end, -con_duration(w))), toUnixTimestamp64Micro(con_end)))) AS con_derivative_value,
((d, v) -> arrayFilter(p -> d = 'funding' OR has(arrayMap(con_text, JSONExtractArrayRaw(v, 'synchronized', 'venues')), JSONExtractString(p, 'venue')), JSONExtractArrayRaw(v, 'per_venue'))) AS con_derivative_observed_venues,
((d, iid) -> con_maxrow(arrayFilter(r -> con_iid(r) = iid, JSONExtractArrayRaw(con_snapshot, d)))) AS con_derivative_latest,
((d, p) -> con_merge(con_timestamp(con_derivative_latest(d, con_iid(p)), con_time_field(con_derivative_latest(d, con_iid(p))), 'observed_at', 0., ['timestamp_precision', 'precision']),
    con_patch('{}', ['instrument_id', 'cadence_s', 'cadence_source'], [con_raw(JSONExtractRaw(p, 'instrument_id')),
    toJSONString(con_cadence(d, con_derivative_latest(d, con_iid(p))).1), toJSONString(con_cadence(d, con_derivative_latest(d, con_iid(p))).2)]))) AS con_derivative_observation,
((d, p) -> con_derivative_latest(d, con_iid(p)) != '' AND isNotNull(con_cadence(d, con_derivative_latest(d, con_iid(p))).1)
    AND con_seconds(con_time(con_derivative_latest(d, con_iid(p))), con_end) <= con_cadence(d, con_derivative_latest(d, con_iid(p))).1) AS con_derivative_fresh,
((d, v) -> arrayAll(p -> con_derivative_fresh(d, p), con_derivative_observed_venues(d, v))) AS con_derivatives_fresh,
((d, v) -> if(d = 'oi', con_patch(v, ['synchronized'], [if(con_derivatives_fresh(d, v), con_raw(JSONExtractRaw(v, 'synchronized')), 'null')]),
    con_patch(v, ['per_venue'], [con_array(arrayMap(p -> if(con_derivative_fresh(d, p), p, con_patch(p, ['funding_vs_normal'], ['null'])), JSONExtractArrayRaw(v, 'per_venue')))]))) AS con_fresh_derivatives
"""


def _physical_spot_ctes():
    inputs = r"""
con_patch(con_selected_snapshot('spot'), ['anchor', 'instruments'], [con_raw(JSONExtractRaw(con_snapshot, 'anchor')),
    con_array(arrayMap(i -> con_strip(i, ['token_id']), con_instrument_rows))]) AS con_physical_spot_snapshot
"""
    sql = _spot_ctes()
    beginning = sql.index("arrayDistinct(arrayConcat(")
    ending = sql.index(" AS con_spot_windows,") + len(" AS con_spot_windows,")
    windows = "arrayDistinct(arrayMap(r -> r.2, arrayFilter(r -> con_domain(r.1) = 'spot', con_supported_requests))) AS con_spot_windows,"
    sql = sql[:beginning] + windows + sql[ending:]
    sql = sql.replace("con_flow_", "con_physical_flow_").replace("con_spot_", "con_physical_spot_")
    sql = sql.replace("SELECT con_snapshot AS con_physical_flow_document", "SELECT con_physical_spot_snapshot AS con_physical_flow_document")
    return inputs + ",\n" + sql


def _derivative_helpers(prefix, snapshot):
    sql = truth_sql_derivatives.ctes().replace("deriv_", prefix)
    sql = sql.replace("{" + prefix + "snapshot:String}", snapshot)
    sql = sql.replace("{" + prefix + "config:String}", "con_config")
    return sql[:sql.index("    " + prefix + "output AS (")].rstrip().removesuffix(",")


def _depth_ctes():
    observation = _object(
        venue="toJSONString(con_venue(r))", observed_at="toJSONString(con_iso(con_time(r)))",
        cadence_s="toJSONString(con_book_cadence(r).1)",
        cadence_source="toJSONString(con_book_cadence(r).2)",
        fresh="toJSONString(CAST(con_book_fresh(r), 'Bool'))",
    )
    return r"""
arrayFilter(r -> con_is_spot(r) AND (NOT JSONHas(r, 'token_id') OR JSONExtractString(r, 'token_id') = con_token)
    AND JSONExtractRaw(r, 'integrity_ok') = 'true', JSONExtractArrayRaw(con_snapshot, 'books')) AS con_books,
(r -> multiIf(JSONType(con_metadata_entry('books')) IN ('Int64', 'UInt64', 'Float64', 'Bool'), tuple(con_number(con_metadata_entry('books')), 'CAPTURE_METADATA'),
    ifNull(con_number(con_metric_cadence('books', r)), 0.) != 0, tuple(con_number(con_metric_cadence('books', r)), 'CAPTURE_METADATA_BY_METRIC'),
    con_explicit_number('books', r) > 0, tuple(con_explicit_number('books', r), 'CAPTURE_METADATA'), tuple(CAST(10., 'Nullable(Float64)'), 'TDD_5_1'))) AS con_book_cadence,
arrayDistinct(arrayMap(con_venue, con_books)) AS con_book_venues,
(v -> arrayFilter(r -> con_venue(r) = v, con_books)) AS con_venue_books,
(v -> con_maxrow(con_venue_books(v))) AS con_latest_book,
(v -> arrayFilter(r -> con_has_anchor AND con_time(r) >= con_offset(con_anchor, -3600.) AND con_time(r) < con_anchor, con_venue_books(v))) AS con_before_books,
con_number(con_parameter('depth_band_pct', '1')) AS con_depth_band,
transform(con_depth_band, [0.5, 1., 2.], ['p0_5', 'p1', 'p2'], '') AS con_depth_key,
((r, side) -> if(con_number(if(JSONType(JSONExtractRaw(r, 'range_covered_pct')) = 'Object', JSONExtractRaw(r, 'range_covered_pct', side), JSONExtractRaw(r, 'range_covered_pct'))) >= con_depth_band,
    con_dec(JSONExtractRaw(r, 'depth', side, con_depth_key, 'usd')), NULL)) AS con_book_side,
(r -> con_book_side(r, 'bid') + con_book_side(r, 'ask')) AS con_book_depth,
(a -> if(empty(a), CAST(NULL, 'Nullable(Decimal256(24))'), divideDecimal(con_sum(a), toDecimal256(length(a), 24), 24))) AS con_decimal_mean,
(a -> if(empty(a), CAST(NULL, 'Nullable(Float64)'), arraySum(a) / length(a))) AS con_float_mean,
(v -> con_decimal_mean(arrayFilter(x -> isNotNull(x), arrayMap(con_book_depth, con_before_books(v))))) AS con_previous_depth,
(v -> con_float_mean(arrayFilter(x -> isNotNull(x), arrayMap(r -> con_number(JSONExtractRaw(r, 'spread_bps')), con_before_books(v))))) AS con_previous_spread,
(r -> isNotNull(con_book_cadence(r).1) AND con_seconds(con_time(r), con_end) <= con_book_cadence(r).1) AS con_book_fresh,
arrayMap(v -> con_latest_book(v), con_book_venues) AS con_latest_books,
notEmpty(con_latest_books) AND arrayAll(con_book_fresh, con_latest_books) AS con_depth_fresh,
arrayMap(con_book_depth, con_latest_books) AS con_current_depths,
arrayMap(con_previous_depth, con_book_venues) AS con_previous_depths,
arrayFilter(x -> isNotNull(x), arrayMap(r -> con_number(JSONExtractRaw(r, 'spread_bps')), con_latest_books)) AS con_current_spreads,
arrayFilter(x -> isNotNull(x), arrayMap(con_previous_spread, con_book_venues)) AS con_previous_spreads,
if(notEmpty(con_current_depths) AND arrayAll(x -> isNotNull(x), con_current_depths), con_sum(con_current_depths), NULL) AS con_current_depth,
if(notEmpty(con_previous_depths) AND arrayAll(x -> isNotNull(x), con_previous_depths), con_sum(con_previous_depths), NULL) AS con_pre_depth,
if(notEmpty(con_current_spreads) AND notEmpty(con_previous_spreads),
    con_float_mean(con_current_spreads) / nullIf(con_float_mean(con_previous_spreads), 0.), NULL) AS con_spread_ratio,
arrayMap(r -> __OBSERVATION__, con_latest_books) AS con_depth_observations,
con_patch('{}', ['aggregate_depth_p1_usd', 'pre_event_depth_p1_usd', 'spread_ratio', 'observations'],
    [con_money(con_current_depth), con_money(con_pre_depth), toJSONString(con_spread_ratio), con_array(con_depth_observations)]) AS con_depth_value
""".replace("__OBSERVATION__", observation)


def _close_ctes():
    inputs = r"""
(r -> con_patch(r, ['venue'], [toJSONString(con_iid(r))])) AS con_close_identity,
con_patch(con_snapshot, ['instruments', 'flow_bars', 'flow_segments'],
    [con_array(arrayMap(con_close_identity, arrayFilter(r -> con_is_spot(r) AND JSONExtractString(r, 'token_id') = con_token, con_instrument_rows))),
    con_array(arrayMap(con_close_identity, JSONExtractArrayRaw(con_snapshot, 'flow_bars'))),
    con_array(arrayMap(con_close_identity, JSONExtractArrayRaw(con_snapshot, 'flow_segments')))]) AS con_close_snapshot
"""
    return inputs + ",\n" + _spot_ctes(close=True) + r""",
arrayFilter(r -> JSONExtractRaw(r, 'complete') = 'true' AND con_is_spot(r)
    AND isNotNull(con_dec(if(JSONHas(r, 'c'), JSONExtractRaw(r, 'c'), JSONExtractRaw(r, 'close'))))
    AND con_dec(if(JSONHas(r, 'c'), JSONExtractRaw(r, 'c'), JSONExtractRaw(r, 'close'))) != con_zero, JSONExtractArrayRaw(con_snapshot, 'klines')) AS con_price_rows,
(r -> coalesce(if(JSONExtractRaw(r, 'open_time') NOT IN ('', 'null'), con_offset(con_parse(JSONExtractRaw(r, 'open_time')), ifNull(con_number(JSONExtractRaw(r, 'interval_s')), 60.)), NULL),
    con_parse(JSONExtractRaw(r, 'close_time')), con_offset(con_time(r), ifNull(con_number(JSONExtractRaw(r, 'interval_s')), 60.)))) AS con_close_time,
arrayDistinct(arrayMap(con_iid, arrayFilter(r -> con_close_time(r) <= con_end, con_price_rows))) AS con_price_ids,
(iid -> arrayLast(v -> JSONExtractString(v, 'venue') = iid, JSONExtractArrayRaw(con_close_spot_value('W24H'), 'venues'))) AS con_close_venue,
arrayFilter(iid -> arrayExists(i -> con_iid(i) = iid AND con_is_spot(i) AND JSONExtractString(i, 'token_id') = con_token, con_instrument_rows)
    AND con_close_venue(iid) != '', con_price_ids) AS con_close_candidates,
arrayFirst(iid -> con_dec(JSONExtractRaw(con_close_venue(iid), 'volume_native')) = arrayMax(arrayMap(k -> con_dec(JSONExtractRaw(con_close_venue(k), 'volume_native')), con_close_candidates)), con_close_candidates) AS con_close_ref,
arrayFilter(r -> con_iid(r) = con_close_ref AND con_close_time(r) = toStartOfHour(con_end), con_price_rows) AS con_hourly_rows,
if(empty(con_hourly_rows), '', arrayLast(r -> con_iid(r) = con_close_ref AND JSONExtractRaw(r, 'complete') = 'true' AND con_close_time(r) = toStartOfHour(con_end), JSONExtractArrayRaw(con_snapshot, 'klines'))) AS con_hourly_source,
if(empty(con_hourly_rows), CAST(NULL, 'Nullable(Decimal256(24))'), con_dec(if(JSONHas(con_hourly_rows[-1], 'c'), JSONExtractRaw(con_hourly_rows[-1], 'c'), JSONExtractRaw(con_hourly_rows[-1], 'close')))) AS con_close_price,
con_merge(con_patch('{}', ['ref_venue', 'close_1h_price'], [if(con_close_ref = '', 'null', toJSONString(con_close_ref)), con_money(con_close_price)]),
    if(con_hourly_source = '', con_timestamp('{}', 'observed_at', 'close_1h_observed_at', 0., []),
        if(JSONExtractRaw(con_hourly_source, 'close_time') NOT IN ('', 'null') AND JSONExtractRaw(con_hourly_source, 'open_time') IN ('', 'null'),
            con_timestamp(con_hourly_source, 'close_time', 'close_1h_observed_at', 0., []),
            con_timestamp(con_hourly_source, 'open_time', 'close_1h_observed_at', ifNull(con_number(JSONExtractRaw(con_hourly_source, 'interval_s')), 60.), ['timestamp_precision'])))) AS con_close_value
"""


def _confidence_ctes():
    inputs = r"""
con_selected_snapshot('dc') AS con_confidence_snapshot,
con_patch(con_confidence_snapshot, ['oi'], [con_array(arrayFilter(r -> abs(con_seconds(con_time(r), con_start(con_primary))) <= con_number(con_parameter('oi_sync_tolerance_s', '60'))
    OR con_time(r) >= con_offset(con_end, -con_number(con_parameter('oi_sync_tolerance_s', '60'))) AND con_time(r) <= con_end, JSONExtractArrayRaw(con_confidence_snapshot, 'oi')))]) AS con_confidence_endpoints,
con_pairs([(con_primary, con_spot_value(con_primary))]) AS con_confidence_spots,
con_patch('{}', ['synchronized'], [con_dc_deriv_sync_json(arrayFilter(iid -> con_dc_deriv_sync_eligible(iid,
    toUnixTimestamp64Micro(con_start(con_primary)), toUnixTimestamp64Micro(con_end)), con_dc_deriv_instrument_ids),
    toUnixTimestamp64Micro(con_start(con_primary)), toUnixTimestamp64Micro(con_end))]) AS con_confidence_raw_derivatives,
con_pairs([(con_primary, con_confidence_raw_derivatives)]) AS con_confidence_derivatives
"""
    core = truth_sql_support._core().replace("sup_", "con_conf_")
    replacements = {
        "{con_conf_snapshot:String}": "con_confidence_snapshot",
        "{con_conf_config:String}": "con_config",
        "{con_conf_spots:String}": "con_confidence_spots",
        "{con_conf_derivatives:String}": "con_confidence_derivatives",
        "{con_conf_liquidity:String}": "'{}'",
        "{con_conf_relative:String}": "'{}'",
        "{con_conf_supply_preimages:String}": "'[]'",
    }
    for old, new in replacements.items():
        core = core.replace(old, new)
    return ",\n".join((
        inputs,
        _derivative_helpers("con_dc_deriv_", "con_confidence_endpoints"),
        core,
        truth_sql_support._freshness().replace("sup_", "con_conf_"),
        truth_sql_support._confidence().replace("sup_", "con_conf_"),
        r"""
(SELECT con_patch(con_strip(con_conf_confidence, ['class_']), ['class', 'derivatives_classification_ready', 'derivatives_classification_ready_reason', 'analysis_window'],
    [con_raw(JSONExtractRaw(con_conf_confidence, 'class_')), 'null', '"NOT_ESTIMABLE"',
    con_patch('{}', ['window', 'start', 'cutoff'], [toJSONString(con_primary), toJSONString(con_iso(con_start(con_primary))), toJSONString(con_iso(con_end))])]) FROM con_conf_conf_output) AS con_confidence_value
""",
    ))


def _relative_ctes():
    sql = truth_sql_relative.ctes().replace("rel_", "con_rel_")
    inputs = {
        "snapshot:String": "con_selected_snapshot('relative')", "start:String": "con_iso(coalesce(con_anchor, con_offset(con_end, -14400.)))",
        "end:String": "con_iso(con_end)", "lookback:Float64": "con_number(con_parameter('historical_lookback_days', con_parameter('base_retention_days', '90')))",
        "min_days:Float64": "con_number(con_parameter('history_min_days', '7'))",
        "beta_weight:Float64": "con_number(con_parameter('beta_blume_weight', '0.6666666666666666'))",
        "quantile_lo:Float64": "con_number(JSONExtractRaw(con_parameter('normal_range_quantiles', '[0.025,0.975]'), 1))",
        "quantile_hi:Float64": "con_number(JSONExtractRaw(con_parameter('normal_range_quantiles', '[0.025,0.975]'), 2))",
        "include_outcome:UInt8": "toUInt8(0)",
    }
    for name, expression in inputs.items():
        sql = sql.replace("{con_rel_" + name + "}", expression)
    return sql + r""",
if(con_has_anchor, con_rel_result, con_patch(con_rel_result, ['repricing_band', 'windows'], ['null',
    con_patch(JSONExtractRaw(con_rel_result, 'windows'), ['EVENT'], [con_patch(JSONExtractRaw(con_rel_result, 'windows', 'EVENT'), ['r_raw'], ['null'])])])) AS con_relative_value
"""


def _state_ctes():
    return r"""
con_pairs(arrayFilter(p -> has(['eligible_before_divergence', 'eligible_support_before_divergence', 'move_class', 'support', 'alternatives', 'divergence', 'class_confidence', 'flags'], p.1), JSONExtractKeysAndValuesRaw(con_full_result))) AS con_selected_state,
con_pairs(arrayMap(p -> tuple(p.1, if(NOT con_has_anchor AND p.1 = 'EVENT', con_patch(con_strip(p.2, ['reason']),
    ['classification', 'size_profile'], [con_raw(JSONExtractRaw(con_spot_value('EVENT'), 'classification')), 'null']),
    if(p.1 = con_primary, p.2, con_patch(p.2, ['size_profile'], ['null'])))), JSONExtractKeysAndValuesRaw(con_full_result, 'spot'))) AS con_state_spots,
con_strip(JSONExtractRaw(con_full_result, 'derivatives', 'windows', con_primary), ['window', 'start', 'cutoff']) AS con_state_derivative_outer,
con_patch(con_state_derivative_outer, ['synchronized'], [if(JSONExtractRaw(con_state_derivative_outer, 'synchronized') IN ('', 'null'), 'null', con_strip(JSONExtractRaw(con_state_derivative_outer, 'synchronized'), ['coverage']))]) AS con_state_derivative,
con_strip(JSONExtractRaw(con_full_result, 'liquidity', 'windows', con_primary), ['windows', 'window', 'start', 'cutoff', 'coverage', 'observed_at']) AS con_state_liquidity_outer,
con_patch(con_state_liquidity_outer, ['elasticity', 'replenishment'], [con_strip(JSONExtractRaw(con_state_liquidity_outer, 'elasticity'), ['window', 'start', 'cutoff']), con_strip(JSONExtractRaw(con_state_liquidity_outer, 'replenishment'), ['window', 'start', 'cutoff'])]) AS con_state_liquidity,
if(con_has_anchor AND con_anchor >= con_end, '{"move_class":"UNKNOWN","divergence":{"state":"UNKNOWN"},"data_confidence":{"class":"LOW"}}',
    con_merge(con_selected_state, con_patch('{}', ['spot', 'derivatives', 'liquidity', 'supply', 'relative', 'data_confidence', 'primary_window'],
    [con_state_spots, concat('{"windows":', con_pairs([(con_primary, con_state_derivative)]), '}'), con_state_liquidity,
    con_strip(JSONExtractRaw(con_full_result, 'supply', 'windows', con_primary), ['windows', 'window', 'start', 'cutoff']), JSONExtractRaw(con_full_result, 'relative'),
    con_strip(JSONExtractRaw(con_full_result, 'data_confidence'), ['capture_readiness']), toJSONString(con_primary)]))) AS con_state_value
"""


def _ordered_requirements(requirements):
    return sorted({(row["metric"], row.get("window", "W1H")) for row in requirements or []})


def _cache_steps(requirements):
    chunks = [r"""
((d, w) -> multiIf(d = 'spot', con_physical_spot_value(w), d IN ('oi', 'funding'), con_derivative_value(d, w),
    d = 'depth', con_depth_value, d = 'close', con_close_value, d = 'dc', con_confidence_value,
    d = 'relative', con_relative_value, '{}')) AS con_calculate,
((c, r, k) -> tuple(toUInt64(indexOf(con_requested, r)), r.1, r.2, con_domain(r.1), k, CAST(con_cache_has(c, k), 'Bool'),
    assumeNotNull(if(con_cache_has(c, k), con_cache_get(c, k), con_calculate(con_domain(r.1), r.2))))) AS con_prepare_key,
((c, r) -> con_prepare_key(c, r, con_cache_key(con_domain(r.1), r.2))) AS con_prepare,
((acc, r) -> tuple(con_cache_store(acc.1, r.5, r.7), arrayPushBack(acc.2, tuple(r.1, r.2, r.3, r.4, r.7, r.6)))) AS con_accumulate,
con_step_0 AS (SELECT tuple(con_initial_cache, CAST([], 'Array(Tuple(UInt64, String, String, String, String, Bool))')) AS con_packet)
"""]
    ordered = _ordered_requirements(requirements)
    previous = 0
    while previous < len(ordered):
        index = previous + 1
        metric, window = ordered[previous]
        is_state = metric in STATE_METRICS and window in {"W15M", "W1H", "W4H", "W24H", "EVENT"}
        if not is_state:
            last = index
            while last < len(ordered):
                next_metric, next_window = ordered[last]
                if next_metric in STATE_METRICS and next_window in {"W15M", "W1H", "W4H", "W24H", "EVENT"}:
                    break
                last += 1
            chunks.append(
                f"con_step_{last} AS (SELECT arrayFold((acc, r) -> if(con_supported(r), "
                "con_accumulate(acc, con_prepare(acc.1, r)), acc), "
                f"arraySlice(con_requested, {index}, {last - previous}), prior.con_packet) AS con_packet FROM con_step_{previous} AS prior)"
            )
            previous = last
            continue
        bundle = f"con_state_bundle_{index}"
        chunks.append(f"{{con_state_result_{index}:String}} AS {bundle}")
        chunks.append(_state_ctes().replace("con_state_", f"con_state_{index}_").replace("con_selected_state", f"con_selected_state_{index}").replace("con_full_result", bundle))
        node_cache = (
            f"if(con_has_anchor AND con_anchor >= con_end, con_cache, if(JSONHas({bundle}, '_native_condition_cache'), "
            f"arrayMap(r -> tuple(JSONExtractString(r, 'key'), JSONExtractRaw(r, 'value')), "
            f"JSONExtractArrayRaw({bundle}, '_native_condition_cache')), con_cache))"
        )
        chunks.append(
            f"con_step_{index}_key AS (SELECT con_packet, con_packet.1 AS con_cache, con_requested[{index}] AS con_request, "
            "con_request.1 AS con_metric, con_request.2 AS con_window, con_domain(con_metric) AS con_domain_name, "
            f"toUInt64({index}) AS con_order, con_supported(con_request) AS con_admitted, "
            "con_cache_key(con_domain_name, con_window) AS con_key, "
            f"CAST(con_cache_has(con_cache, con_key), 'Bool') AS con_reused FROM con_step_{previous})"
        )
        chunks.append(
            f"con_step_{index}_raw AS (SELECT *, assumeNotNull(if(con_reused, con_cache_get(con_cache, con_key), "
            f"con_state_{index}_value)) AS con_raw_value FROM con_step_{index}_key WHERE " + _state_guard(bundle) + ")"
        )
        chunks.append(
            f"con_step_{index} AS (SELECT if(con_admitted, tuple(if(con_reused, con_cache, "
            f"con_cache_store({node_cache}, con_key, con_raw_value)), arrayPushBack(raw.con_packet.2, "
            "tuple(con_order, con_metric, con_window, con_domain_name, con_raw_value, con_reused))), raw.con_packet) "
            f"AS con_packet FROM con_step_{index}_raw AS raw)"
        )
        previous = index
    chunks.append(
        "con_evaluation_inputs AS (SELECT con_row.1 AS con_order, con_row.2 AS con_metric, con_row.3 AS con_window, "
        "con_row.4 AS con_domain_name, con_row.5 AS con_raw_value, con_row.6 AS con_reused "
        f"FROM (SELECT arrayJoin(con_packet.2) AS con_row FROM con_step_{previous}))"
    )
    chunks.append(f"con_final_cache AS (SELECT con_packet.1 AS con_cache FROM con_step_{previous})")
    return ",\n".join(chunks)


def _evaluation_ctes(requirements):
    proof = _object(
        metric="toJSONString(con_metric)", window="toJSONString(con_window)",
        dependency_fingerprint="toJSONString(con_dependency(con_domain_name))",
        reused="toJSONString(CAST(con_reused, 'Bool'))",
        reason="if(con_available AND con_fresh, 'null', '\"INCOMPLETE\"')",
        observations="con_array(con_observations)", available="toJSONString(CAST(con_available, 'Bool'))",
        coverage="multiIf(con_domain_name = 'spot', con_raw(JSONExtractRaw(con_value, 'coverage')), con_domain_name = 'oi', concat('{\"venues\":', con_raw(coalesce(nullIf(JSONExtractRaw(con_value, 'synchronized', 'venues'), ''), '[]')), '}'), 'null')",
    )
    pending = _object(metric="toJSONString(r.1)", window="toJSONString(r.2)", reason="'\"NOT_SUPPORTED\"'")
    incomplete = _object(metric="toJSONString(con_metric)", window="toJSONString(con_window)", reason="'\"INCOMPLETE\"'")
    return _cache_steps(requirements) + r""",
con_evaluation_values AS (
    SELECT *, multiIf(con_domain_name IN ('oi', 'funding'), con_fresh_derivatives(con_domain_name, con_raw_value),
        con_domain_name = 'depth', con_patch(con_strip(con_raw_value, ['observations']), ['aggregate_depth_p1_usd', 'spread_ratio'],
            [if(con_fresh, con_raw(JSONExtractRaw(con_raw_value, 'aggregate_depth_p1_usd')), 'null'), if(con_fresh, con_raw(JSONExtractRaw(con_raw_value, 'spread_ratio')), 'null')]), con_raw_value) AS con_value,
        multiIf(con_domain_name IN ('oi', 'funding'), con_derivatives_fresh(con_domain_name, con_raw_value), con_domain_name = 'depth',
            notEmpty(con_observations) AND arrayAll(r -> JSONExtractBool(r, 'fresh'), con_observations), true) AS con_fresh,
        multiIf(con_domain_name IN ('oi', 'funding'), arrayMap(p -> con_derivative_observation(con_domain_name, p), con_derivative_observed_venues(con_domain_name, con_raw_value)),
            con_domain_name = 'depth', JSONExtractArrayRaw(con_raw_value, 'observations'), CAST([], 'Array(String)')) AS con_observations
    FROM con_evaluation_inputs
),
con_evaluation_rows AS (
    SELECT *, multiIf(con_domain_name = 'spot', JSONExtractRaw(con_value, 'direction') NOT IN ('', 'null'),
        con_domain_name = 'close', JSONExtractRaw(con_value, 'close_1h_price') NOT IN ('', 'null'),
        con_domain_name = 'dc', JSONExtractRaw(con_value, 'class') NOT IN ('', 'null'),
        con_domain_name = 'depth', if(con_metric = 'liq.spread_ratio', JSONExtractRaw(con_value, 'spread_ratio') NOT IN ('', 'null'),
            JSONExtractRaw(con_value, 'aggregate_depth_p1_usd') NOT IN ('', 'null') AND ifNull(con_result_dec(JSONExtractRaw(con_value, 'pre_event_depth_p1_usd')), con_zero) != con_zero),
        con_domain_name = 'state', JSONExtractRaw(con_value, 'move_class') NOT IN ('', 'null'),
        con_domain_name = 'relative', if(con_metric = 'price.repricing_band', JSONExtractRaw(con_value, 'repricing_band') NOT IN ('', 'null'), JSONExtractRaw(con_value, 'windows', 'EVENT', 'r_raw') NOT IN ('', 'null')),
        con_domain_name = 'funding', notEmpty(JSONExtractArrayRaw(con_value, 'per_venue')) AND arrayAll(p -> JSONExtractRaw(p, 'funding_vs_normal') NOT IN ('', 'null'), JSONExtractArrayRaw(con_value, 'per_venue')),
        JSONExtractRaw(con_value, 'synchronized', transform(con_metric, ['deriv.native_oi_chg_pct', 'deriv.native_oi_vs_normal', 'deriv.oi_class'], ['oi_native_chg_pct', 'native_oi_vs_normal', 'oi_class'], '')) NOT IN ('', 'null')) AS con_available,
        __PROOF__ AS con_proof, if(con_available, '', __INCOMPLETE__) AS con_pending
    FROM con_evaluation_values
),
con_evaluation_collected AS (
    SELECT arraySort(r -> r.1, groupArray(tuple(con_order, con_metric, con_window, con_domain_name, con_value, con_proof, con_pending))) AS con_rows
    FROM con_evaluation_rows
),
con_evaluation_output AS (
    SELECT *, arrayMap(r -> r.6, con_rows) AS con_proofs,
        arrayMap(p -> p.2, arraySort(p -> p.1, arrayConcat(
            arrayMap(r -> tuple(r.1, r.7), arrayFilter(r -> r.7 != '', con_rows)),
            arrayMap(r -> tuple(indexOf(con_requested, r), __UNSUPPORTED__), arrayFilter(r -> NOT con_supported(r), con_requested))))) AS con_pending_metrics,
        arrayFold((acc, r) -> con_apply(acc, r.2, r.3, r.4, r.5), con_rows, '{"spot":{},"derivatives":{"windows":{}},"relative":{}}') AS con_body
    FROM con_evaluation_collected
)
""".replace("__PROOF__", proof).replace("__INCOMPLETE__", incomplete).replace("__UNSUPPORTED__", pending)


def _merge_ctes():
    return r"""
((a, b) -> con_patch(a, ['windows'], [con_merge(JSONExtractRaw(a, 'windows'), JSONExtractRaw(b, 'windows'))])) AS con_merge_derivatives,
((acc, w, d, v) -> con_patch(acc, ['derivatives'], [con_patch(JSONExtractRaw(acc, 'derivatives'), ['windows'],
    [con_patch(JSONExtractRaw(acc, 'derivatives', 'windows'), [w], [con_patch(if(JSONHas(acc, 'derivatives', 'windows', w), JSONExtractRaw(acc, 'derivatives', 'windows', w), '{"per_venue":[],"synchronized":null}'),
    [if(d = 'oi', 'synchronized', 'per_venue')], [con_raw(JSONExtractRaw(v, if(d = 'oi', 'synchronized', 'per_venue')))])])])])) AS con_apply_derivative,
((acc, w, v) -> con_patch(v, ['derivatives'], [con_patch(JSONExtractRaw(v, 'derivatives'), ['windows'], [con_merge(JSONExtractRaw(acc, 'derivatives', 'windows'),
    con_pairs(arrayMap(p -> tuple(p.1, con_patch(p.2, arrayConcat(
        if(arrayExists(r -> r.2 = p.1 AND con_domain(r.1) = 'oi', con_requested), ['synchronized'], []),
        if(has(con_requested, tuple('deriv.funding_vs_normal', p.1)), ['per_venue'], [])), arrayConcat(
        if(arrayExists(r -> r.2 = p.1 AND con_domain(r.1) = 'oi', con_requested), [con_raw(JSONExtractRaw(acc, 'derivatives', 'windows', p.1, 'synchronized'))], []),
        if(has(con_requested, tuple('deriv.funding_vs_normal', p.1)), [coalesce(nullIf(JSONExtractRaw(acc, 'derivatives', 'windows', p.1, 'per_venue'), ''), '[]')], [])))), JSONExtractKeysAndValuesRaw(v, 'derivatives', 'windows'))))])])) AS con_state_merge,
((acc, metric, w, d, v) -> multiIf(d = 'spot', con_patch(acc, ['spot'], [con_patch(JSONExtractRaw(acc, 'spot'), [w], [v])]),
    d IN ('close', 'relative'), con_patch(acc, ['relative'], [con_merge(JSONExtractRaw(acc, 'relative'), v)]),
    d = 'dc', con_patch(acc, ['data_confidence'], [v]), d = 'depth', con_patch(acc, ['liquidity'], [v]),
    d = 'state', con_merge(acc, if(JSONHas(v, 'derivatives'), con_state_merge(acc, w, v), v)), con_apply_derivative(acc, w, d, v))) AS con_apply
"""


def ctes(requirements=None):
    requested = None if requirements is None else {row["metric"] for row in requirements}

    def needs(metrics):
        return requested is None or bool(requested & metrics)

    chunks = [_core(), _timestamp_ctes(), _dependency_ctes()]
    if needs(STATE_METRICS | {"dc.class"}):
        chunks.append(_spot_ctes())
    else:
        chunks.append("(w -> '{}') AS con_spot_value")
    if needs(SPOT_METRICS):
        chunks.append(_physical_spot_ctes())
    else:
        chunks.append("(w -> '{}') AS con_physical_spot_value")
    if needs(OI_METRICS | {"deriv.funding_vs_normal"}):
        chunks.append(_derivative_ctes())
    else:
        chunks.append("((d, w) -> '{}') AS con_derivative_value, ((d, v) -> v) AS con_fresh_derivatives, ((d, v) -> true) AS con_derivatives_fresh, ((d, v) -> CAST([], 'Array(String)')) AS con_derivative_observed_venues, ((d, p) -> '{}') AS con_derivative_observation")
    chunks.append(_depth_ctes() if needs({"liq.depth_p1_vs_pre", "liq.spread_ratio"}) else "'{}' AS con_depth_value, true AS con_depth_fresh, CAST([], 'Array(String)') AS con_depth_observations")
    chunks.append(_close_ctes() if needs({"price.close_vs_level"}) else "'{}' AS con_close_value")
    chunks.append(_confidence_ctes() if needs({"dc.class"}) else "'{}' AS con_confidence_value")
    chunks.append(_relative_ctes() if needs(RELATIVE_METRICS) else "'{}' AS con_relative_value")
    chunks.extend((_merge_ctes(), _evaluation_ctes(requirements)))
    return ",\n".join(chunks)


def _state_guard(body):
    active = "con_admitted AND NOT con_reused AND NOT (con_has_anchor AND con_anchor >= con_end)"
    return (
        f"throwIf({active} AND JSONExtractRaw({body}, 'move_class') IN ('', 'null'), "
        "'Market Truth composite conditions require server-side native engine composition') = 0 AND "
        f"throwIf({active} AND (arrayExists(k -> JSONType({body}, k) != 'Object', "
        "['spot', 'derivatives', 'liquidity', 'supply', 'relative', 'data_confidence']) "
        f"OR JSONExtractString({body}, 'primary_window') != con_primary "
        f"OR JSONType({body}, 'derivatives', 'windows', con_primary) != 'Object' "
        f"OR JSONType({body}, 'liquidity', 'windows', con_primary) != 'Object' "
        f"OR JSONType({body}, 'supply', 'windows', con_primary) != 'Object'), "
        "'Market Truth composite conditions require a complete native MarketTruth body') = 0"
    )


def select_sql(*, cache_transport=False):
    request_key = "con_hash(con_sorted_object([('token_id', con_raw(JSONExtractRaw(con_canonical_snapshot, 'token_id'))), ('requirements', toJSONString(con_requested)), ('dependencies', if(JSONHas(con_canonical_snapshot, 'dependency_fingerprint'), con_raw(JSONExtractRaw(con_canonical_snapshot, 'dependency_fingerprint')), toJSONString(arrayMap(p -> JSONExtractString(p, 'dependency_fingerprint'), con_proofs)))), ('anchor', con_raw(JSONExtractRaw(con_canonical_snapshot, 'anchor'))), ('config', con_canonical_config)]))"
    evaluation = _object(
        requires_market_truth="'[]'", unavailable_metrics="con_array(con_pending_metrics)",
        request_key=request_key, proofs="con_array(con_proofs)", transient="'true'",
    )
    return (
        "SELECT con_merge(con_patch(con_body, ['_condition_evaluation'], [" + evaluation + "]), "
        "con_timestamp(con_input_snapshot, 'cutoff', 'cutoff', 0., [])) AS result"
        + (", (SELECT con_cache_json(con_cache) FROM con_final_cache) AS condition_cache" if cache_transport else "")
        + " FROM con_evaluation_output "
        "WHERE throwIf(isNull(con_end), 'Market Truth condition cutoff is invalid') = 0 "
        "AND throwIf(con_depth_key_guard, 'Unsupported depth band') = 0"
    )


def query(snapshot, requirements, config=None, *, result=None, cache_records=None, cache_transport=False):
    config = config or {}
    requirements = list(requirements)
    if result is None and any(
        metric in STATE_METRICS and window in {"W15M", "W1H", "W4H", "W24H", "EVENT"}
        for metric, window in _ordered_requirements(requirements)
    ):
        from tank.analysis import truth_sql_engine

        return truth_sql_engine.condition_query(
            snapshot, requirements, config, cache_records=cache_records, cache_transport=cache_transport
        )
    parameters = {
        "con_snapshot": (parameter_string(_encode(snapshot)), "String"),
        "con_canonical_snapshot": (parameter_string(_encode(snapshot, canonical=True)), "String"),
        "con_config": (parameter_string(_encode(config)), "String"),
        "con_canonical_config": (parameter_string(_encode(config, canonical=True)), "String"),
        "con_requirements": (parameter_string(_encode(requirements)), "String"),
        "con_result": (parameter_string(_encode(result if result is not None else {})), "String"),
        "con_cache_records": (parameter_string(_encode(cache_records or [])), "String"),
    }
    for index, (metric, window) in enumerate(_ordered_requirements(requirements), 1):
        if metric in STATE_METRICS and window in {"W15M", "W1H", "W4H", "W24H", "EVENT"}:
            parameters[f"con_state_result_{index}"] = parameters["con_result"]
    sql = "WITH " + ctes(requirements) + "\n" + select_sql(cache_transport=cache_transport)
    return sql, parameters
