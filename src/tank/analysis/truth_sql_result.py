from array import array

from tank.analysis import truth_sql_spot
from tank.clickhouse_market import parameter_string
from tank.util import canonical_json, jsonable


def _transport(value):
    if isinstance(value, dict):
        return {key: _transport(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, array, bytes, bytearray, memoryview)):
        return [_transport(item) for item in value]
    return jsonable(value)


def _object(**fields):
    return (
        "concat('{', "
        + ", ".join(
            part
            for index, (name, expression) in enumerate(fields.items())
            for part in (("','",) if index else ()) + ("'\"" + name + "\":'", expression)
        )
        + ", '}')"
    )


def _json(expression):
    return f"toJSONString({expression})"


def _bool(expression):
    return f"toJSONString(CAST(({expression}), 'Nullable(Bool)'))"


def _array(expression):
    return f"concat('[', arrayStringConcat({expression}, ','), ']')"


def _money(expression):
    return f"if(isNull({expression}), 'null', toJSONString(toString({expression})))"


def _raw(expression):
    return f"if({expression} IN ('', 'null'), 'null', {expression})"


def _patch(original, **fields):
    return f"res_put({original}, {_object(**fields)})"


def _core_ctes():
    composite = _object(
        cvd=_money("res_vsum(vs, 'cvd')"),
        volume=_money("res_vsum(vs, 'volume_native')"),
        buy_share=_json(
            "res_flow_ratio(res_vsum(vs, 'volume_native') + res_vsum(vs, 'cvd'), 2 * res_vsum(vs, 'volume_native'))"
        ),
        bs_ratio=_json(
            "res_flow_ratio(res_vsum(vs, 'volume_native') + res_vsum(vs, 'cvd'), res_vsum(vs, 'volume_native') - res_vsum(vs, 'cvd'))"
        ),
        venues=_json("arrayMap(v -> JSONExtractString(v, 'venue'), vs)"),
        sign="toString(res_flow_sign(res_vsum(vs, 'cvd'), res_vsum(vs, 'sum_sq_qty')))",
    )
    comparison = _object(
        cohort=_json("arraySort(arrayMap(v -> JSONExtractString(v, 'venue'), a))"),
        current="if(valid, res_composite(a), 'null')",
        other="if(valid, res_composite(b), 'null')",
        share_of_max=_json(
            "res_flow_ratio(res_volume(a), res_money(res_get(left, 'composite_max'), 'volume'))"
        ),
        share_majority=_bool(
            "res_flow_majority(res_volume(a), res_money(res_get(left, 'composite_max'), 'volume'))"
        ),
        reason="if(valid, 'null', '\"NOT_COHORT_CONSISTENT\"')",
    )
    size = _object(
        large_ticket_buy_share=_json("res_flow_ratio(res_large_buy, res_large_buy + res_large_sell)"),
        large_ticket_sell_share=_json("res_flow_ratio(res_large_sell, res_large_buy + res_large_sell)"),
        large_ticket_buy_majority=_bool("res_flow_majority(res_large_buy, res_large_buy + res_large_sell)"),
        large_tickets_minority=_bool(
            "if(res_size_total = 0 OR isNull(res_size_total), NULL, 2 * (res_large_buy + res_large_sell) < res_size_total)"
        ),
        large_ticket_buy_notional=_money("res_large_buy"),
        large_ticket_sell_notional=_money("res_large_sell"),
        sell_share_rising=_bool(
            "if(res_previous_valid AND res_previous_buy + res_previous_sell != 0 AND res_large_buy + res_large_sell != 0, multiplyDecimal(res_large_sell, res_previous_buy + res_previous_sell, 24) > multiplyDecimal(res_previous_sell, res_large_buy + res_large_sell, 24), NULL)"
        ),
        compatible_venues=_json("res_compatible"),
    )
    lead = _object(
        window=_json("res_lead_window"),
        start=_json("res_flow_iso(res_lead_start)"),
        cutoff=_json("res_flow_iso(res_end)"),
        first_mover=_json("res_first_mover"),
        spot_confirmed=_bool(
            "if(isNull(res_lead_return) OR res_get(res_lead_spot, 'composite_cohort') = 'null', NULL, JSONExtractString(res_lead_spot, 'direction') = multiIf(res_lead_return > 0, 'BUY', res_lead_return < 0, 'SELL', 'NEUTRAL'))"
        ),
        sequence=_array("res_sequence"),
    )
    sequence = _object(
        venue=_json("res_flow_venue(r)"),
        market=_json("if(res_flow_market(r) = 'SPOT', 'SPOT', 'PERP')"),
        t_first=_json("res_flow_iso(res_flow_time(r))"),
    )
    spot = _patch(
        "res_spot_original",
        cohort_comparisons="concat('{', arrayStringConcat(arrayMap(pair -> concat(toJSONString(pair.1), ':', pair.2), res_comparisons), ','), '}')",
        cohort_consistent="concat('{', arrayStringConcat(arrayMap(pair -> concat(toJSONString(pair.1), ':', toJSONString(CAST(JSONExtractRaw(pair.2, 'reason') = 'null', 'Bool'))), res_comparisons), ','), '}')",
        size_profile=f"if(res_size_valid, {size}, res_get(res_spot_original, 'size_profile'))",
    )
    deriv = _patch(
        "res_deriv_original",
        window=_json("res_name"),
        start=_json("res_flow_iso(res_wstart)"),
        cutoff=_json("res_flow_iso(res_end)"),
        synchronized="if(res_get(res_deriv_original, 'synchronized') = 'null', 'null', res_put(res_get(res_deriv_original, 'synchronized'), concat('{\"coverage\":{\"venues\":', res_get(res_get(res_deriv_original, 'synchronized'), 'venues'), '}}')))",
        lead_lag=lead,
        funding_dispersion_normal_range_8h="res_get(res_get(res_liq, 'fragmentation'), 'funding_normal_range_8h')",
        funding_dispersion_beyond_normal="res_get(res_get(res_liq, 'fragmentation'), 'funding_beyond_normal')",
    )
    return (
        r"""
{res_snapshot:String} AS res_snapshot,
{res_config:String} AS res_config,
{res_spots:String} AS res_spots_input,
{res_derivatives:String} AS res_derivatives_input,
{res_liquidity:String} AS res_liquidity_input,
{res_supply:String} AS res_supply_input,
{res_relative:String} AS res_relative_input,
{res_confidence:String} AS res_confidence_input,
((o, p) -> concat('{', arrayStringConcat(arrayConcat(
    arrayMap(kv -> concat(toJSONString(kv.1), ':', kv.2), arrayFilter(kv -> NOT has(JSONExtractKeys(p), kv.1), JSONExtractKeysAndValuesRaw(o))),
    arrayMap(kv -> concat(toJSONString(kv.1), ':', kv.2), JSONExtractKeysAndValuesRaw(p))), ','), '}')) AS res_put,
((o, k) -> if(JSONHas(o, k), JSONExtractRaw(o, k), 'null')) AS res_get,
((o, k) -> res_flow_money(o, k)) AS res_money,
((o, k) -> toFloat64OrNull(res_flow_text(res_get(o, k)))) AS res_number,
((o, k) -> JSONExtractRaw(o, k) = 'true') AS res_true,
((o, k) -> if(JSONExtractRaw(o, k) IN ('true', 'false'), JSONExtractRaw(o, k) = 'true', CAST(NULL, 'Nullable(Bool)'))) AS res_boolean,
((v, r) -> if(isNull(v) OR r IN ('', 'null'), CAST(NULL, 'Nullable(Bool)'), v < JSONExtract(r, 'Array(Float64)')[1] OR v > JSONExtract(r, 'Array(Float64)')[2])) AS res_beyond,
((v, r) -> if(isNull(v) OR r IN ('', 'null'), CAST(NULL, 'Nullable(Bool)'), v >= JSONExtract(r, 'Array(Float64)')[1] AND v <= JSONExtract(r, 'Array(Float64)')[2])) AS res_within,
((vs, k) -> arrayReduce('sum', arrayMap(v -> assumeNotNull(res_money(v, k)), vs))) AS res_vsum,
(vs -> if(empty(vs), CAST(NULL, 'Nullable(Decimal256(24))'), res_vsum(vs, 'volume_native'))) AS res_volume,
(vs -> if(empty(vs), '{"cvd":null,"volume":null,"buy_share":null,"bs_ratio":null,"venues":[],"sign":0,"reason":"INCOMPLETE"}', __COMPOSITE__)) AS res_composite,
((left, right) -> arrayFilter(v -> has(arrayMap(x -> JSONExtractString(x, 'venue'), JSONExtractArrayRaw(right, 'venues')), JSONExtractString(v, 'venue')), JSONExtractArrayRaw(left, 'venues'))) AS res_common,
((left, a, b, valid) -> __COMPARISON__) AS res_comparison,
((iid, lo, hi, bars, pieces, assembled) -> (
    arrayFilter(r -> JSONExtractString(r, 'instrument_id') = iid AND lo <= res_flow_time(r) AND res_flow_time(r) + 60000000 <= hi, bars),
    arrayMap(p -> (p.2, res_flow_cover(pieces, iid, p.1, p.2, assembled)), arrayFilter(p -> p.1 < p.2,
        arrayConcat(if(modulo(lo, 60000000) != 0, [(lo, least(intDiv(lo, 60000000) * 60000000 + 60000000, hi))], []),
        if(modulo(hi, 60000000) != 0 AND NOT(modulo(lo, 60000000) != 0 AND intDiv(lo, 60000000) = intDiv(hi, 60000000)), [(greatest(intDiv(hi, 60000000) * 60000000, lo), hi)], [])))),
    greatest(toInt64(0), intDiv(intDiv(hi, 60000000) * 60000000 - (intDiv(lo, 60000000) * 60000000 + if(modulo(lo, 60000000) = 0, 0, 60000000)), 60000000))
)) AS res_flow_parts,
(p -> (length(arrayDistinct(arrayMap(r -> res_flow_time(r), p.1))) = p.3 AND arrayAll(r -> JSONExtractRaw(r, 'complete') = 'true', p.1) AND arrayAll(c -> c.2.1 = c.1, p.2), arrayConcat(p.1, arrayFlatten(arrayMap(c -> c.2.2, p.2))))) AS res_flow_complete,
((rows, keys, side) -> arrayReduce('sum', arrayFlatten(arrayMap(r -> arrayMap(k -> assumeNotNull(res_money(res_get(res_get(r, 'size_buckets'), k), side)), keys), rows)))) AS res_bucket_sum,
((rows, keys) -> notEmpty(rows) AND arrayAll(r -> res_get(r, 'size_buckets') NOT IN ('null', '{}') AND arrayAll(k -> isNotNull(res_money(res_get(res_get(r, 'size_buckets'), k), 'buy_notional')) AND isNotNull(res_money(res_get(res_get(r, 'size_buckets'), k), 'sell_notional')), keys), rows)) AS res_buckets_valid,
res_base AS (
    SELECT *, res_flow_cutoff AS res_end, res_flow_start AS res_start,
        res_flow_anchor_present AS res_has_event, res_flow_primary AS res_primary,
        res_flow_window_defs AS res_windows,
        if(JSONHas(res_spots_input, 'spots'), JSONExtractRaw(res_spots_input, 'spots'), res_spots_input) AS res_spot_map,
        if(JSONHas(res_derivatives_input, 'windows'), JSONExtractRaw(res_derivatives_input, 'windows'), res_derivatives_input) AS res_deriv_map,
        if(JSONHas(res_liquidity_input, 'windows'), JSONExtractRaw(res_liquidity_input, 'windows'), res_liquidity_input) AS res_liq_map,
        if(JSONHas(res_supply_input, 'windows'), JSONExtractRaw(res_supply_input, 'windows'), res_supply_input) AS res_supply_map,
        if(JSONHas(res_config, 'parameters'), JSONExtractRaw(res_config, 'parameters'), res_config) AS res_parameters,
        if(JSONHas(res_parameters, 'min_spot_venues'), JSONExtractInt(res_parameters, 'min_spot_venues'), 2) AS res_min_venues,
        if(JSONHas(res_parameters, 'absorption_min_bursts'), JSONExtractInt(res_parameters, 'absorption_min_bursts'), 2) AS res_min_bursts,
        arrayFilter(r -> res_flow_time(r) + 1000000 * if(JSONHas(r, 'interval_s'), JSONExtractInt(r, 'interval_s'), 60) <= res_end AND res_flow_admitted(r, res_flow_assembled), JSONExtractArrayRaw(res_snapshot, 'klines')) AS res_klines,
        JSONExtractArrayRaw(res_snapshot, 'semantics') AS res_semantics,
        if(JSONHas(res_parameters, 'large_ticket_usd'), res_money(res_parameters, 'large_ticket_usd'), toDecimal256(100000, 24)) AS res_large_threshold,
        if(JSONHas(res_parameters, 'size_buckets_usd'), JSONExtractArrayRaw(res_parameters, 'size_buckets_usd'), ['1000','10000','100000','1000000']) AS res_bucket_edges,
        arrayFilter(k -> res_flow_money(concat('{"v":', res_bucket_edges[k], '}'), 'v') >= res_large_threshold, arrayEnumerate(res_bucket_edges)) AS res_large_indices,
        arrayMap(k -> concat('B', toString(k + 1)), res_large_indices) AS res_large_buckets,
        res_flow_timestamp(if(JSONHas(res_snapshot, '_lead_start'), JSONExtractRaw(res_snapshot, '_lead_start'), toJSONString(res_flow_iso(res_start)))) AS res_lead_start
    FROM res_flow_input
    WHERE throwIf(NOT arrayAll(w -> JSONHas(res_spot_map, w.1) AND JSONHas(res_deriv_map, w.1) AND JSONHas(res_liq_map, w.1) AND JSONHas(res_supply_map, w.1)
        AND JSONHas(res_get(res_spot_map, w.1), 'venues') AND JSONHas(res_get(res_spot_map, w.1), 'composite_max') AND JSONHas(res_get(res_spot_map, w.1), 'classification')
        AND JSONHas(res_get(res_deriv_map, w.1), 'synchronized') AND JSONHas(res_get(res_deriv_map, w.1), 'per_venue')
        AND (w.1 = 'EVENT' AND NOT res_flow_anchor_present OR JSONHas(res_get(res_liq_map, w.1), 'elasticity') AND JSONHas(res_get(res_liq_map, w.1), 'replenishment') AND JSONHas(res_get(res_supply_map, w.1), 'supply_in_evidence'))
        AND JSONHas(res_get(res_relative_input, 'windows'), w.1), res_flow_window_defs)
        OR NOT JSONHas(res_relative_input, 'windows') OR NOT (JSONHas(res_confidence_input, 'class') OR JSONHas(res_confidence_input, 'class_')), 'Market Truth result requires complete native domain inputs') = 0
),
res_windows_source AS (
    SELECT *, res_w.1 AS res_name, res_end - res_w.2 AS res_wstart,
        res_get(res_spot_map, res_name) AS res_spot_original,
        res_get(res_deriv_map, res_name) AS res_deriv_original,
        res_get(res_liq_map, res_name) AS res_liq_window,
        concat('{"window":', toJSONString(res_name), ',"start":', toJSONString(res_flow_iso(res_wstart)), ',"cutoff":', toJSONString(res_flow_iso(res_end)), '}') AS res_window_timing,
        res_put(res_put(res_liq_window, res_window_timing), concat('{"elasticity":', res_put(res_get(res_liq_window, 'elasticity'), res_window_timing), ',"replenishment":', res_put(res_get(res_liq_window, 'replenishment'), res_window_timing),
            if(empty(JSONExtractArrayRaw(res_liq_window, 'per_venue')), '', concat(',"coverage":{"venues":', toJSONString(arrayMap(v -> JSONExtractString(v, 'venue'), JSONExtractArrayRaw(res_liq_window, 'per_venue'))), '},"observed_at":', toJSONString(arrayMin(arrayMap(v -> JSONExtractString(v, 'observed_at'), JSONExtractArrayRaw(res_liq_window, 'per_venue')))))), '}')) AS res_liq,
        res_get(res_supply_map, res_name) AS res_su,
        arrayMap(other -> (other.1, res_comparison(res_spot_original,
            res_common(res_spot_original, res_get(res_spot_map, other.1)),
            res_common(res_get(res_spot_map, other.1), res_spot_original),
            length(res_common(res_spot_original, res_get(res_spot_map, other.1))) >= res_min_venues AND ifNull(res_flow_majority(res_volume(res_common(res_spot_original, res_get(res_spot_map, other.1))), res_money(res_get(res_spot_original, 'composite_max'), 'volume')), false))),
            arrayFilter(other -> other.1 != res_name, res_windows)) AS res_comparisons,
        arrayMap(v -> (JSONExtractString(v, 'venue'), arrayLast(s -> JSONExtractString(s, 'venue') = JSONExtractString(v, 'venue') AND res_get(s, 'trade_aggregation') NOT IN ('null', '\"N_A\"'), res_semantics)), JSONExtractArrayRaw(res_spot_original, 'venues')) AS res_venue_semantics,
        arrayDistinct(arrayMap(v -> JSONExtractString(v.2, 'trade_aggregation'), arrayFilter(v -> v.2 != '', res_venue_semantics))) AS res_aggregation_classes,
        arrayMap(c -> arrayMap(v -> v.1, arrayFilter(v -> JSONExtractString(v.2, 'trade_aggregation') = c, res_venue_semantics)), res_aggregation_classes) AS res_classes,
        arrayElement(res_classes, indexOf(arrayMap(c -> length(c), res_classes), arrayMax(arrayMap(c -> length(c), res_classes)))) AS res_compatible,
        arrayDistinct(arrayMap(r -> JSONExtractString(r, 'instrument_id'), arrayFilter(r -> has(res_compatible, res_flow_venue(r)) AND res_flow_market(r) = 'SPOT', res_flow_bars))) AS res_size_ids,
        arrayMap(iid -> res_flow_complete(res_flow_parts(iid, res_wstart, res_end, res_flow_bars, res_flow_pieces, res_flow_assembled)), res_size_ids) AS res_size_populations,
        arrayMap(iid -> res_flow_complete(res_flow_parts(iid, res_wstart - res_w.2, res_wstart, res_flow_bars, res_flow_pieces, res_flow_assembled)), res_size_ids) AS res_previous_populations,
        if(arrayAll(p -> p.1, res_size_populations), arrayFlatten(arrayMap(p -> p.2, res_size_populations)), []) AS res_size_rows,
        if(arrayAll(p -> p.1, res_previous_populations), arrayFlatten(arrayMap(p -> p.2, res_previous_populations)), []) AS res_previous_rows,
        notEmpty(res_classes) AND arrayExists(e -> res_flow_money(concat('{"v":', e, '}'), 'v') = res_large_threshold, res_bucket_edges) AND res_buckets_valid(res_size_rows, res_large_buckets) AS res_size_valid,
        res_buckets_valid(res_previous_rows, res_large_buckets) AS res_previous_valid,
        res_flow_bound(res_bucket_sum(res_size_rows, res_large_buckets, 'buy_notional')) AS res_large_buy,
        res_flow_bound(res_bucket_sum(res_size_rows, res_large_buckets, 'sell_notional')) AS res_large_sell,
        res_flow_bound(res_bucket_sum(res_previous_rows, res_large_buckets, 'buy_notional')) AS res_previous_buy,
        res_flow_bound(res_bucket_sum(res_previous_rows, res_large_buckets, 'sell_notional')) AS res_previous_sell,
        res_flow_sum(res_size_rows, 'buy_notional') + res_flow_sum(res_size_rows, 'sell_notional') AS res_size_total,
        arrayFilter(r -> (res_get(r, 'token_id') = 'null' OR res_get(r, 'token_id') = res_get(res_snapshot, 'token_id')) AND res_lead_start <= res_flow_time(r), res_klines) AS res_lead_rows,
        arrayDistinct(arrayMap(r -> JSONExtractString(r, 'instrument_id'), res_lead_rows)) AS res_lead_ids,
        arrayMap(iid -> arraySort(r -> (res_flow_time(r), indexOf(res_lead_rows, r)), arrayFilter(r -> JSONExtractString(r, 'instrument_id') = iid, res_lead_rows)), res_lead_ids) AS res_lead_groups,
        arrayAll(rs -> arrayAll(r -> JSONExtractRaw(r, 'complete') = 'true', rs) AND arrayAll(i -> res_flow_time(rs[i + 1]) - res_flow_time(rs[i]) = 60000000, range(1, length(rs))), res_lead_groups) AS res_lead_complete,
        res_get(res_get(res_get(res_relative_input, 'windows'), 'W15M'), 'normal_range') AS res_lead_normal,
        arrayMap(rs -> arrayFirst(r -> isNotNull(res_number(r, 'c')) AND ifNull(res_beyond(res_number(r, 'c') / nullIf(res_number(rs[1], 'o'), 0) - 1, res_lead_normal), false), rs), arrayFilter(rs -> arrayAll(r -> JSONExtractRaw(r, 'complete') = 'true', rs) AND arrayAll(i -> res_flow_time(rs[i + 1]) - res_flow_time(rs[i]) = 60000000, range(1, length(rs))) AND ifNull(res_number(rs[1], 'o'), 0) != 0, res_lead_groups)) AS res_crossings,
        arrayMap(r -> __SEQUENCE__, arrayFilter(r -> r != '', res_crossings)) AS res_sequence,
        arrayFilter(r -> JSONExtractString(r, 'market') = 'SPOT', res_sequence) AS res_spot_sequence,
        arrayFilter(r -> JSONExtractString(r, 'market') = 'PERP', res_sequence) AS res_perp_sequence,
        arrayMin(arrayMap(r -> res_flow_timestamp(JSONExtractRaw(r, 't_first')), res_spot_sequence)) AS res_first_spot,
        arrayMin(arrayMap(r -> res_flow_timestamp(JSONExtractRaw(r, 't_first')), res_perp_sequence)) AS res_first_perp,
        arrayDistinct(arrayMap(r -> if(res_flow_market(r) = 'SPOT', 'SPOT', 'PERP'), res_lead_rows)) AS res_lead_markets,
        multiIf(NOT res_lead_complete, 'UNKNOWN', notEmpty(res_spot_sequence) AND notEmpty(res_perp_sequence), multiIf(res_first_perp - res_first_spot >= 60000000, 'SPOT', res_first_perp - res_first_spot <= -60000000, 'PERP', 'SIMULTANEOUS'), notEmpty(res_sequence) AND hasAll(res_lead_markets, ['SPOT','PERP']), if(notEmpty(res_spot_sequence), 'SPOT','PERP'), 'UNKNOWN') AS res_first_mover,
        if(res_has_event, 'EVENT', res_name) AS res_lead_window,
        res_get(res_spot_map, res_lead_window) AS res_lead_spot,
        res_number(res_get(res_get(res_relative_input, 'windows'), res_lead_window), 'r_raw') AS res_lead_return
    FROM res_base ARRAY JOIN res_windows AS res_w
),
res_enriched AS (
    SELECT *, __SPOT__ AS res_spot,
        __DERIV__ AS res_deriv
    FROM res_windows_source
)
""".replace("__COMPOSITE__", composite)
        .replace("__COMPARISON__", comparison)
        .replace("__SEQUENCE__", sequence)
        .replace("__SPOT__", spot)
        .replace("__DERIV__", deriv)
    )


def _selection_ctes():
    gates = [
        (
            "SPOT_LED_ACCUMULATION",
            "res_direction = 'BUY' AND res_broad AND res_notdown AND res_boolean(res_elasticity, 'weak_response') = false AND res_nwithin AND res_fundok",
            [
                "res_feature('spot_volume_up_beyond_normal')",
                "res_lead = 'SPOT'",
                "res_spotmost",
                "res_feature('large_ticket_buy_majority')",
                "res_drop <= 0",
            ],
        ),
        (
            "ORGANIC_MIXED",
            "res_direction = 'BUY' AND res_class IN ('BROAD','CONCENTRATED') AND res_dn > 0 AND res_nwithin AND JSONExtractString(res_oi, 'oi_class') IN ('REAL_NATIVE','MIXED') AND res_fundok",
            [
                "res_feature('spot_volume_up_beyond_normal') AND res_feature('perp_volume_up_beyond_normal')",
                "res_feature('basis_within_normal')",
            ],
        ),
        (
            "PERP_LED_CHASE",
            "res_up AND res_nup AND (res_perpmost OR res_lead = 'PERP') AND NOT (res_direction = 'BUY' AND res_broad)",
            [
                "has(res_funding, 'ABOVE')",
                "res_feature('basis_widening')",
                "res_direction IN ('SELL','NEUTRAL')",
                "res_feature('perp_taker_flow_buy')",
            ],
        ),
        (
            "SHORT_SQUEEZE",
            "res_up AND (res_ndown OR res_money(res_liqs, 'short') > res_money(res_liqs, 'long')) AND res_fundnegative",
            ["res_feature('liquidations_first_half_majority')", "res_feature('perp_taker_flow_buy')"],
        ),
        (
            "SELL_SIDE_ABSORPTION",
            "res_direction = 'BUY' AND res_true(res_elasticity, 'weak_response')",
            ["res_feature('ask_replenishment')", "res_supplyyes", "res_native_not_up"],
        ),
        (
            "DISTRIBUTION",
            "res_notdown AND (res_buy_to_sell OR (res_direction = 'SELL' AND res_sell_majority)) AND res_supplyyes",
            [
                "res_feature('large_ticket_sell_share_rising')",
                "res_feature('ask_depth_building')",
                "res_feature('exchange_inflows')",
            ],
        ),
        (
            "LIQUIDITY_VACUUM",
            "res_moved AND res_depth_lost AND res_true(res_elasticity, 'excess_response')",
            ["res_feature('large_tickets_minority')", "res_feature('spot_volume_within_normal')"],
        ),
        (
            "REAL_MARKET_FRAGMENTATION",
            "res_true(res_fragmentation, 'validated')",
            [
                "length(arrayDistinct(arrayMap(v -> JSONExtractInt(v, 'sign'), arrayFilter(v -> JSONExtractInt(v, 'sign') != 0, JSONExtractArrayRaw(res_spot, 'venues'))))) > 1",
                "res_true(res_fragmentation, 'gap_closed_quickly')",
            ],
        ),
    ]
    candidates = [
        (
            "PERP_LED_MARKUP",
            "res_nup OR res_perpmost",
            [
                "res_lead = 'PERP'",
                "res_feature('funding_rising') OR res_feature('basis_widening')",
                "res_class IN ('CONCENTRATED','DIVERGENT')",
                "res_feature('perp_taker_flow_buy')",
            ],
            3,
        ),
        (
            "if(res_absorption_confirmed, 'ABSORPTION_CONFIRMED','ABSORPTION_CANDIDATE')",
            "NOT res_absorption_refuted",
            [
                "res_number(res_liq, 'spread_ratio') <= 1",
                "res_number(res_get(res_spot, 'size_profile'), 'large_ticket_sell_share') > 0.5 AND res_true(res_elasticity, 'weak_response')",
                "res_native_not_up",
            ],
            "if(res_absorption_confirmed, 2, 1)",
        ),
        (
            "LIQUIDITY_VACUUM",
            "res_depth_lost OR res_true(res_elasticity, 'excess_response')",
            [
                "JSONExtractString(res_liq, 'liquidity_state') = 'THIN'",
                "res_true(res_elasticity, 'excess_response')",
            ],
            4,
        ),
        (
            "FRAGMENTATION",
            "(res_true(res_fragmentation, 'persistent_gap') OR res_true(res_fragmentation, 'funding_beyond_normal') OR (res_class = 'DIVERGENT' AND res_true(res_fragmentation, 'top_sign_flips'))) AND NOT res_broad",
            [
                "res_feature('localized_inventory_transfer')",
                "res_true(res_fragmentation, 'gap_closed_quickly')",
            ],
            0,
        ),
    ]

    def score(expressions):
        return " + ".join(f"toInt64(ifNull(({expression}), false))" for expression in expressions)

    eligible = ",\n".join(
        f"('{name}', ifNull(({gate}), false), {score(features)})" for name, gate, features in gates
    )
    candidate = ",\n".join(
        f"({name if name.startswith('if(') else repr(name)}, ifNull(({gate}), false), {score(features)}, toInt64({rank}))"
        for name, gate, features, rank in candidates
    )
    divergence = _object(
        state=_json(
            "if(res_low, if(res_triggered, 'UNKNOWN','NONE'), if(res_triggered, if(empty(res_candidates), 'UNKNOWN', res_chosen.1), 'NONE'))"
        ),
        triggered=_bool("res_triggered"),
        candidates=_array("if(res_triggered, res_candidate_details, [])"),
        unresolved=_json(
            "if(res_triggered, arrayMap(c -> c.1, arrayFilter(c -> c.1 != res_chosen.1, res_candidates)), [])"
        ),
        confirmed=_bool("res_triggered AND res_chosen.1 = 'ABSORPTION_CONFIRMED'"),
    )
    candidate_detail = _object(
        state=_json("c.1"),
        eligible=_bool("c.2"),
        refuted=_bool(
            "multiIf(c.1 IN ('ABSORPTION_CANDIDATE','ABSORPTION_CONFIRMED'), res_absorption_refuted, c.1 = 'FRAGMENTATION', res_broad, false)"
        ),
        testable=_bool(
            "multiIf(c.1 IN ('ABSORPTION_CANDIDATE','ABSORPTION_CONFIRMED'), true, c.1 = 'PERP_LED_MARKUP', isNotNull(res_nb) OR res_inc NOT IN ('null','{}'), c.1 = 'LIQUIDITY_VACUUM', isNotNull(res_drop) OR isNotNull(res_boolean(res_elasticity, 'excess_response')), res_fragmentation NOT IN ('null','{}'))"
        ),
        support="toString(if(c.2, c.3, 0))",
    )
    selected = _object(
        eligible_before_divergence=_json("arraySort(arrayMap(c -> c.1, res_eligible_before))"),
        eligible_support_before_divergence="concat('{', arrayStringConcat(arrayMap(c -> concat(toJSONString(c.1), ':', toString(c.3)), res_eligible_before), ','), '}')",
        move_class=_json("res_move"),
        support="toString(if(res_move = 'UNKNOWN', if(notEmpty(res_ordered) AND res_ordered[1].1 = 'UNKNOWN', res_ordered[1].2, 0), res_ordered[1].2))",
        alternatives=_json(
            "arrayMap(c -> c.1, if(res_move = 'UNKNOWN', res_ordered, arraySlice(res_ordered, 2)))"
        ),
        divergence=divergence,
        class_confidence=_json(
            "multiIf(res_move = 'UNKNOWN', 'LOW', length(res_ordered) > 1 OR (res_triggered AND length(res_candidates) > 1), 'MEDIUM', 'HIGH')"
        ),
        flags=_json(
            "arrayConcat(if(arrayExists(o -> JSONExtractString(o, 'oi_class') = 'PRICE_MARKUP' AND res_number(o, 'oi_usd_chg_pct') > 0, arrayPushFront(res_per_venue, res_oi)), ['PRICE_MARKUP_OI'], []), if(arrayExists(kv -> kv.2 = 'false', JSONExtractKeysAndValuesRaw(res_get(res_spot, 'cohort_consistent'))), ['NOT_COHORT_CONSISTENT'], []), if(res_true(res_get(res_relative_input, 'pre_event_runup'), 'flag'), ['PRE_EVENT_RUNUP'], []), if(res_triggered AND arrayExists(c -> c.1 = 'FRAGMENTATION' AND (c.1 != res_chosen.1 OR NOT res_low), res_candidates) AND NOT res_true(res_fragmentation, 'validated'), ['FRAGMENTATION_INVESTIGATE'], []))"
        ),
    )
    return (
        r"""
res_gate_inputs AS (
    SELECT *, res_get(res_get(res_relative_input, 'windows'), res_name) AS res_relative_window,
        if(JSONHas(res_relative_window, 'relative_return'), res_number(res_relative_window, 'relative_return'), res_number(res_relative_window, 'r_btc_adj')) AS res_return,
        res_get(res_relative_window, 'normal_range') AS res_normal,
        res_return > JSONExtract(res_normal, 'Array(Float64)')[2] AND res_normal != 'null' AS res_up,
        res_return >= JSONExtract(res_normal, 'Array(Float64)')[1] AND res_normal != 'null' AS res_notdown,
        ifNull(res_beyond(res_return, res_normal), false) AS res_moved,
        res_get(res_deriv, 'synchronized') AS res_oi,
        res_number(res_oi, 'oi_native_chg_pct') AS res_dn,
        res_get(res_oi, 'native_normal_range') AS res_native_range,
        res_beyond(res_dn, res_native_range) AS res_nb,
        ifNull(res_nb, false) AND res_dn > 0 AS res_nup,
        ifNull(res_nb, false) AND res_dn < 0 AS res_ndown,
        ifNull(res_within(res_dn, res_native_range), false) AS res_nwithin,
        res_nb = false OR (res_nb = true AND res_dn < 0) AS res_native_not_up,
        JSONExtractArrayRaw(res_deriv, 'per_venue') AS res_per_venue,
        arrayMap(v -> JSONExtractString(v, 'funding_vs_normal'), res_per_venue) AS res_funding,
        notEmpty(res_funding) AND arrayAll(x -> x IN ('WITHIN','BELOW'), res_funding) AS res_fundok,
        notEmpty(res_per_venue) AND arrayAll(v -> isNotNull(res_money(v, 'funding_start')) AND res_money(v, 'funding_start') <= 0, res_per_venue) AS res_fundnegative,
        res_get(res_deriv, 'incremental_share') AS res_inc,
        res_get(res_inc, 'increment_usd') AS res_increments,
        arrayReduce('sum', arrayMap(kv -> assumeNotNull(res_money(res_increments, kv.1)), JSONExtractKeysAndValuesRaw(res_increments))) AS res_increment_total,
        if(res_increments NOT IN ('null','{}'), 2 * res_money(res_increments, 'perp') > res_increment_total, ifNull(res_number(res_inc, 'perp'), 0) > 0.5) AS res_perpmost,
        if(res_increments NOT IN ('null','{}'), 2 * res_money(res_increments, 'spot') > res_increment_total, ifNull(res_number(res_inc, 'spot'), 0) > 0.5) AS res_spotmost,
        JSONExtractString(res_get(res_deriv, 'lead_lag'), 'first_mover') AS res_lead,
        res_get(res_liq, 'elasticity') AS res_elasticity,
        res_number(res_liq, 'depth_drop') AS res_drop,
        if(JSONHas(res_liq, 'depth_majority_lost'), res_true(res_liq, 'depth_majority_lost'), ifNull(res_drop > 0.5, false)) AS res_depth_lost,
        JSONExtractString(res_spot, 'direction') AS res_direction,
        JSONExtractString(res_spot, 'classification') AS res_class,
        res_class = 'BROAD' AS res_broad,
        JSONExtractString(res_su, 'supply_in_evidence') = 'TRUE' AS res_supplyyes,
        res_get(res_get(res_deriv, 'liquidations'), 'complete_venues_total') AS res_liqs,
        res_get(res_deriv, 'support_features') AS res_features,
        res_get(res_liq, 'fragmentation') AS res_fragmentation,
        if(JSONHas(res_get(res_spot, 'exact_majority'), 'sell'), res_true(res_get(res_spot, 'exact_majority'), 'sell'), res_number(res_get(res_spot, 'cw_breadth'), 'sell') > 0.5) AS res_sell_majority,
        arrayExists(kv -> multiIf(kv.1 = 'W15M', 900000000, kv.1 = 'W1H', 3600000000, kv.1 = 'W4H', 14400000000, kv.1 = 'W24H', 86400000000, 0) > res_w.2 AND JSONExtractInt(res_get(kv.2, 'current'), 'sign') = -1 AND JSONExtractInt(res_get(kv.2, 'other'), 'sign') = 1 AND res_true(kv.2, 'share_majority'), JSONExtractKeysAndValuesRaw(res_get(res_spot, 'cohort_comparisons'))) AS res_buy_to_sell,
        res_get(res_liq, 'replenishment') AS res_refill,
        res_number(res_refill, 'bursts') AS res_bursts,
        res_number(res_refill, 'replenished') AS res_replenished,
        ifNull((res_bursts >= res_min_bursts AND res_replenished < res_bursts / 2) OR res_depth_lost, false) AS res_absorption_refuted,
        ifNull(res_bursts >= res_min_bursts AND res_replenished = res_bursts AND res_number(res_refill, 'resilient_bursts') = res_bursts AND res_native_not_up, false) AS res_absorption_confirmed,
        ifNull(res_notdown AND res_direction = 'SELL', false) AS res_triggered,
        if(JSONHas(res_confidence_input, 'class'), JSONExtractString(res_confidence_input, 'class'), JSONExtractString(res_confidence_input, 'class_')) = 'LOW' AS res_low
    FROM res_enriched
),
res_eligible_rows AS (
    WITH (k -> res_true(res_features, k)) AS res_feature
    SELECT *, arrayFilter(c -> c.2, [__ELIGIBLE__]) AS res_eligible_initial,
        arrayConcat(res_eligible_initial, if(empty(res_eligible_initial) AND JSONExtractString(res_oi, 'oi_class') = 'PRICE_MARKUP' AND res_number(res_oi, 'oi_usd_chg_pct') > 0 AND res_true(res_oi, 'usd_beyond'), [('PRICE_MARKUP_FAKE_OI', true, toInt64(0))], [])) AS res_eligible_before,
        [__CANDIDATES__] AS res_all_candidates,
        arrayFilter(c -> c.2, res_all_candidates) AS res_candidates,
        arrayMap(c -> __CANDIDATE_DETAIL__, res_all_candidates) AS res_candidate_details,
        arrayElement(arrayReverseSort(c -> (c.3, c.4), res_candidates), 1) AS res_chosen,
        multiIf(res_chosen.1 = 'PERP_LED_MARKUP', 'PERP_LED_CHASE', res_chosen.1 = 'LIQUIDITY_VACUUM', 'LIQUIDITY_VACUUM', res_chosen.1 IN ('ABSORPTION_CONFIRMED','ABSORPTION_CANDIDATE'), 'PASSIVE_SPOT_ABSORPTION', res_chosen.1 = 'FRAGMENTATION' AND res_true(res_fragmentation, 'validated'), 'REAL_MARKET_FRAGMENTATION', 'UNKNOWN') AS res_divergence_move,
        if(res_triggered, if(empty(res_candidates), CAST([], 'Array(Tuple(String, Int64))'), [(res_divergence_move, res_chosen.3)]), arrayMap(c -> (c.1, c.3), res_eligible_before)) AS res_final_eligible,
        ['LIQUIDITY_VACUUM','SHORT_SQUEEZE','PERP_LED_CHASE','DISTRIBUTION','SELL_SIDE_ABSORPTION','ORGANIC_MIXED','SPOT_LED_ACCUMULATION','REAL_MARKET_FRAGMENTATION','PRICE_MARKUP_FAKE_OI'] AS res_risk_order,
        arraySort(c -> (-c.2, if(has(res_risk_order, c.1), indexOf(res_risk_order, c.1), 99)), res_final_eligible) AS res_ordered,
        if(res_low OR empty(res_ordered), 'UNKNOWN', res_ordered[1].1) AS res_move
    FROM res_gate_inputs
),
res_selected AS (
    SELECT *, __SELECTED__ AS res_selected_json
    FROM res_eligible_rows
)
""".replace("__ELIGIBLE__", eligible)
        .replace("__CANDIDATES__", candidate)
        .replace("__CANDIDATE_DETAIL__", candidate_detail)
        .replace("__SELECTED__", selected)
    )


def _fact_ctes():
    def evidence(collection, instruments, *, venue=False):
        field = "venue" if venue else "instrument_id"
        return _array(
            "arrayMap(i -> "
            + _object(
                collection=repr('"' + collection + '"'),
                **{field: _json("i")},
                start=_json("res_flow_iso(res_wstart)"),
                cutoff=_json("res_flow_iso(res_end)"),
            )
            + ", "
            + instruments
            + ")"
        )

    def fact(metric, value, unit, text, sources, coverage, *, window=None, start=None):
        return _object(
            metric=_json(metric),
            value=value,
            unit=repr('"' + unit + '"'),
            window=_json(window or "res_name"),
            start=_json(start or "res_flow_iso(res_wstart)"),
            cutoff=_json("res_flow_iso(res_end)"),
            text=_json(text),
            evidence=sources,
            coverage=coverage,
        )

    spot_fact = fact(
        "'spot.cvd'",
        _money("res_cvd"),
        "NATIVE_TOKEN",
        "concat(res_name, ' spot flow ', if(res_direction = '', 'unknown', res_direction), ': CVD ', res_signed_native(res_cvd), ' ', arrayElement(splitByChar(':', JSONExtractString(res_snapshot, 'token_id')), -1), ', ', res_class, '; ', res_count(JSONExtractInt(res_coverage, 'n_complete'), 'complete venue'), '.')",
        evidence(
            "mkt.flow_bars_1m",
            "JSONExtract(res_get(res_get(res_spot, 'composite_max'), 'venues'), 'Array(String)')",
            venue=True,
        ),
        "res_coverage",
    )
    oi_fact = fact(
        "'derivatives.oi_native_chg_pct'",
        _raw("res_get(res_oi, 'oi_native_chg_pct')"),
        "DECIMAL_RETURN",
        "concat(res_name, ' synchronized native OI ', res_pct(res_dn, 2), ', ', if(JSONExtractString(res_oi, 'native_oi_vs_normal') = '', 'normal range unavailable', JSONExtractString(res_oi, 'native_oi_vs_normal')), '; class ', res_oi_class_text, '.')",
        evidence(
            "mkt.oi_obs",
            "arrayMap(p -> JSONExtractString(p, 'instrument_id'), arrayFilter(p -> has(JSONExtract(res_get(res_oi, 'venues'), 'Array(String)'), JSONExtractString(p, 'venue')), res_per_venue))",
        ),
        _object(
            venues="res_get(res_oi, 'venues')", classification_ready="res_get(res_oi, 'classification_ready')"
        ),
    )
    relative_fact = fact(
        "if(JSONExtractString(res_relative_window, 'relative_measure') = 'R_RAW', 'relative.r_raw', 'relative.r_btc_adj')",
        _json("res_return"),
        "DECIMAL_RETURN",
        "concat(res_name, if(JSONExtractString(res_relative_window, 'relative_measure') = 'R_RAW', ' price move (benchmark, raw) ', ' BTC-adjusted price move '), res_pct(res_return, 2), '; ', if(res_normal NOT IN ('null','[]'), concat('normal range ', res_pct(JSONExtract(res_normal, 'Array(Float64)')[1], 2), ' to ', res_pct(JSONExtract(res_normal, 'Array(Float64)')[2], 2), '.'), concat('normal range unavailable (', if(JSONExtractString(res_relative_window, 'normal_range_reason') = '', 'INCOMPLETE', JSONExtractString(res_relative_window, 'normal_range_reason')), ').')))",
        evidence(
            "mkt.klines_1m",
            "if(res_get(res_relative_input, 'ref_venue') = 'null', [], [JSONExtractString(res_relative_input, 'ref_venue')])",
        ),
        "if(JSONHas(res_relative_input, 'reference_coverage'), res_get(res_relative_input, 'reference_coverage'), '{}')",
    )
    depth_fact = fact(
        "'liquidity.depth_drop'",
        _json("res_drop"),
        "DECIMAL_FRACTION",
        "concat(if(JSONExtractString(res_elasticity, 'hit_side') = '', 'unknown', JSONExtractString(res_elasticity, 'hit_side')), ' depth within 1% changed ', res_pct(-res_drop, 2), '; excess response ', res_python_bool(res_get(res_elasticity, 'excess_response')), '; weak response ', res_python_bool(res_get(res_elasticity, 'weak_response')), '.')",
        evidence(
            "mkt.book_summaries",
            "arrayMap(v -> JSONExtractString(v, 'venue'), JSONExtractArrayRaw(res_liq, 'per_venue'))",
            venue=True,
        ),
        _object(
            hit_side="res_get(res_elasticity, 'hit_side')",
            depth_usd="res_get(res_elasticity, 'hit_side_depth_p1_usd')",
        ),
    )
    fragmentation_fact = fact(
        "'liquidity.fragmentation'",
        "'true'",
        "BOOLEAN",
        "'Venue price gap exceeded both spreads for 15 minutes and was confirmed by independent public REST books.'",
        _array(
            "arrayMap(pair -> "
            + _object(collection="'\"mkt.book_summaries\"'", venues="pair")
            + ", JSONExtractArrayRaw(res_fragmentation, 'venues'))"
        ),
        '\'{"independent_path":"PUBLIC_REST","validated":true}\'',
        window="'W15M'",
        start="res_flow_iso(res_end - 900000000)",
    )
    funding_value = (
        "concat('{', arrayStringConcat(arrayMap(v -> concat(toJSONString(v), ':', "
        + _object(
            rate="res_get(arrayLast(p -> JSONExtractString(p, 'venue') = v, res_funding_start_rows), 'funding_start')",
            interval_h="res_get(arrayLast(p -> JSONExtractString(p, 'venue') = v, res_funding_start_rows), 'funding_start_interval_h')",
            observed_at="res_get(arrayLast(p -> JSONExtractString(p, 'venue') = v, res_funding_start_rows), 'funding_start_observed_at')",
        )
        + "), res_funding_venues), ','), '}')"
    )
    funding_fact = fact(
        "'derivatives.funding_start'",
        funding_value,
        "RATE_PER_INTERVAL",
        "concat('Funding at window start: ', arrayStringConcat(arrayMap(v -> concat(v, ' ', res_pct(toFloat64(res_money(arrayLast(p -> JSONExtractString(p, 'venue') = v, res_funding_start_rows), 'funding_start')), 4), ' per ', res_flow_text(res_get(arrayLast(p -> JSONExtractString(p, 'venue') = v, res_funding_start_rows), 'funding_start_interval_h')), 'h'), arraySort(res_funding_venues)), '; '), '.')",
        _array(
            "arrayMap(p -> "
            + _object(
                collection="'\"mkt.funding_obs\"'",
                instrument_id=_json("JSONExtractString(p, 'instrument_id')"),
                ts="res_get(p, 'funding_start_observed_at')",
            )
            + ", res_funding_start_rows)"
        ),
        _object(venues=_json("res_funding_venues")),
    )
    supply_fact = fact(
        "'supply.supply_in_evidence'",
        "'true'",
        "BOOLEAN",
        "concat('Supply in evidence: inventory change ', if(isNull(res_number(res_inventory, 'chg_pct')), 'unknown', res_pct(res_number(res_inventory, 'chg_pct'), 2)), '; labeled exchange transfers ', toString(length(res_classified_flows)), '.')",
        _array(
            "["
            + _object(
                collection="'\"mkt.supply_context\"'",
                observed_at="res_get(res_inventory, 'observed_at')",
                flows=_array("arrayMap(f -> res_get(f, 'tx_hash'), res_classified_flows)"),
            )
            + "]"
        ),
        "if(JSONHas(res_inventory, 'coverage'), res_get(res_inventory, 'coverage'), concat('{\"classified_flows\":', toString(length(res_classified_flows)), '}'))",
    )
    canonical = _patch(
        "f",
        text=_json(
            "concat(JSONExtractString(f, 'text'), ' [', JSONExtractString(f, 'window'), '; ', JSONExtractString(f, 'unit'), '; ', res_coverage_label(res_get(f, 'coverage')), ']')"
        ),
    )
    return (
        r"""
((n, label) -> concat(toString(n), ' ', label, if(n = 1, '', 's'))) AS res_count,
(v -> multiIf(v = 'true', 'True', v = 'false', 'False', 'None')) AS res_python_bool,
((x, n) -> concat(if(x < 0, '-', '+'), splitByChar('.', toString(toDecimal256(roundBankers(abs(ifNull(x, 0)), n), 8)))[1], if(n = 0, '', concat('.', substring(arrayElement(splitByChar('.', toString(toDecimal256(roundBankers(abs(ifNull(x, 0)), n), 8))), 2), 1, n))))) AS res_fixed,
((x, n) -> res_fixed(100 * x, n)) AS res_pct,
(s -> arrayStringConcat(arrayReverse(arrayMap(i -> reverse(substring(reverse(s), i * 3 + 1, 3)), range(toUInt64(intDiv(length(s) + 2, 3))))), ',')) AS res_commas,
(s -> concat(res_commas(splitByChar('.', s)[1]), if(position(s, '.') = 0, '', concat('.', splitByChar('.', s)[2])))) AS res_grouped,
(x -> if(abs(x) >= 1, concat(if(x < 0, '-', '+'), res_grouped(toString(toDecimal256(roundBankers(abs(x), 2), 2)))),
    if(x = 0, '+0', concat(if(x < 0, '-', '+'), res_small_native(abs(x)))))) AS res_signed_native,
(x -> (replaceRegexpOne(splitByChar('.', toString(x))[2], '^0*', ''), toInt64(length(splitByChar('.', toString(x))[2])) - toInt64(length(replaceRegexpOne(splitByChar('.', toString(x))[2], '^0*', ''))))) AS res_small_parts,
(p -> if(p.2 <= 5, concat('0.', repeat('0', p.2), substring(p.1, 1, 3)), concat(substring(p.1, 1, 1), if(length(p.1) >= 2, concat('.', substring(p.1, 2, 2)), ''), 'e-', leftPad(toString(p.2 + 1), 2, '0')))) AS res_small_render,
(x -> if(roundBankers(x, least(toInt64(24), res_small_parts(x).2 + 3)) >= 1, '1.00', res_small_render(res_small_parts(roundBankers(x, least(toInt64(24), res_small_parts(x).2 + 3)))))) AS res_small_native,
(c -> multiIf(JSONHas(c, 'n_complete'), concat(res_count(JSONExtractInt(c, 'n_complete'), 'complete spot venue'), if(res_get(c, 'covered_share_est') = 'null', '', concat(', ', substring(res_pct(res_number(c, 'covered_share_est'), 1), 2), ' estimated volume coverage'))),
    JSONHas(c, 'venues'), res_count(length(JSONExtractArrayRaw(c, 'venues')), 'covered venue'),
    JSONHas(c, 'hit_side'), concat(if(JSONExtractString(c, 'hit_side') = '', 'unknown side', JSONExtractString(c, 'hit_side')), ' depth ', if(res_get(c, 'depth_usd') = 'null', 'unknown', concat(res_grouped(toString(toDecimal256(roundBankers(abs(ifNull(res_number(c, 'depth_usd'), 0)), 0), 0))), ' USD'))),
    JSONHas(c, 'complete_balances'), concat(res_count(JSONExtractInt(c, 'complete_balances'), 'complete balance'), ', ', toString(JSONExtractInt(c, 'comparable_balances')), ' comparable'),
    res_true(c, 'validated'), 'independent public REST confirmation',
    c NOT IN ('null','{}') AND arrayAll(kv -> startsWith(kv.2, '{') AND JSONHas(kv.2, 'complete'), JSONExtractKeysAndValuesRaw(c)), res_count(countEqual(arrayMap(kv -> res_true(kv.2, 'complete'), JSONExtractKeysAndValuesRaw(c)), true), 'complete trailing-24h reference venue'),
    JSONHas(c, 'classified_flows'), res_count(JSONExtractInt(c, 'classified_flows'), 'classified flow'), 'coverage unavailable')) AS res_coverage_label,
res_fact_inputs AS (
    SELECT *, res_get(res_spot, 'coverage') AS res_coverage,
        res_money(res_get(res_spot, 'composite_max'), 'cvd') AS res_cvd,
        arrayFilter(p -> isNotNull(res_money(p, 'funding_start')), res_per_venue) AS res_funding_start_rows,
        arrayDistinct(arrayMap(p -> JSONExtractString(p, 'venue'), res_funding_start_rows)) AS res_funding_venues,
        res_get(res_su, 'exchange_inventory') AS res_inventory,
        arrayFilter(f -> JSONExtractString(f, 'class') IN ('UNLOCK_DISTRIBUTION','TREASURY_INTERNAL','MM_INVENTORY'), JSONExtractArrayRaw(res_su, 'flows')) AS res_classified_flows,
        res_get(res_oi, 'decomposition') AS res_decomposition,
        arrayMap(p -> concat(JSONExtractString(p, 'venue'), ' ', JSONExtractString(p, 'oi_class'), if(res_get(p, 'oi_native_chg_pct') = 'null', ' (', concat(' (native ', res_pct(res_number(p, 'oi_native_chg_pct'), 2))), ', position share ', substring(res_pct(res_number(res_get(p, 'decomposition'), 'pos_share'), 1), 2), if(res_get(res_get(p, 'decomposition'), 'markup') = 'null', '', concat(', price-markup component ', if(res_money(res_get(p, 'decomposition'), 'markup') < 0, '-', '+'), res_grouped(toString(toDecimal256(roundBankers(abs(res_money(res_get(p, 'decomposition'), 'markup')), 0), 0))), ' USD')), ')'), arrayFilter(p -> res_get(p, 'oi_class') != 'null' AND res_get(res_get(p, 'decomposition'), 'pos_share') != 'null', res_per_venue)) AS res_venue_classes,
        if(res_get(res_oi, 'oi_class') != 'null', JSONExtractString(res_oi, 'oi_class'), concat('unavailable (', arrayFirst(s -> s != '', [JSONExtractString(res_decomposition, 'detail'), JSONExtractString(res_decomposition, 'reason'), JSONExtractString(res_oi, 'oi_usd_detail'), 'INCOMPLETE']), ')', if(empty(res_venue_classes), '', concat('; by venue: ', arrayStringConcat(res_venue_classes, ', '))))) AS res_oi_class_text
    FROM res_selected
),
res_fact_rows AS (
    SELECT *, arrayConcat(
        if(res_move = 'DISTRIBUTION' AND res_supplyyes, [__SUPPLY_FACT__], []),
        if(res_move = 'SHORT_SQUEEZE' AND notEmpty(res_funding_venues), [__FUNDING_FACT__], []),
        if(res_move = 'REAL_MARKET_FRAGMENTATION' AND res_true(res_fragmentation, 'validated'), [__FRAGMENTATION_FACT__], []),
        if(res_move IN ('LIQUIDITY_VACUUM','SELL_SIDE_ABSORPTION','PASSIVE_SPOT_ABSORPTION') AND isNotNull(res_drop), [__DEPTH_FACT__], []),
        if(isNotNull(res_cvd), [__SPOT_FACT__], []),
        if(isNotNull(res_dn), [__OI_FACT__], []),
        if(isNotNull(res_return), [__RELATIVE_FACT__], [])) AS res_unsorted_facts,
        if(res_move IN ('SHORT_SQUEEZE','PERP_LED_CHASE','PRICE_MARKUP_FAKE_OI'), arrayMap(pair -> pair.1, arraySort(pair -> (multiIf(startsWith(JSONExtractString(pair.1, 'metric'), 'derivatives'), 0, startsWith(JSONExtractString(pair.1, 'metric'), 'relative'), 1, 2), pair.2), arrayZip(res_unsorted_facts, arrayEnumerate(res_unsorted_facts)))), res_unsorted_facts) AS res_sorted_facts,
        arrayMap(f -> __CANONICAL__, arraySlice(res_sorted_facts, 1, 3)) AS res_facts
    FROM res_fact_inputs
)
""".replace("__SPOT_FACT__", spot_fact)
        .replace("__OI_FACT__", oi_fact)
        .replace("__RELATIVE_FACT__", relative_fact)
        .replace("__DEPTH_FACT__", depth_fact)
        .replace("__FRAGMENTATION_FACT__", fragmentation_fact)
        .replace("__FUNDING_FACT__", funding_fact)
        .replace("__SUPPLY_FACT__", supply_fact)
        .replace("__CANONICAL__", canonical)
    )


def _result_ctes():
    unavailable = _object(
        window=_json("res_name"),
        start="'null'",
        cutoff=_json("res_flow_iso(res_end)"),
        reason="'\"INCOMPLETE\"'",
    )
    empty_deriv = _patch(
        "res_get(res_empty_map, 'EVENT')",
        window="'\"EVENT\"'",
        start="'null'",
        cutoff=_json("res_flow_iso(res_end)"),
        reason="'\"INCOMPLETE\"'",
    )
    readiness = _object(
        spot_ready=_bool("JSONExtractString(res_final_spot, 'classification') != 'INSUFFICIENT_DATA'"),
        derivatives_native_ready=_bool(
            "NOT res_has_perps OR res_get(res_final_deriv, 'synchronized') != 'null'"
        ),
        derivatives_classification_ready=_bool(
            "NOT res_has_perps OR res_true(res_get(res_final_deriv, 'synchronized'), 'classification_ready')"
        ),
        book_depth_ready=_bool(
            "notEmpty(JSONExtractArrayRaw(res_final_liq, 'per_venue')) AND arrayAll(v -> res_get(v, 'bid_depth_p1_usd') != 'null' AND res_get(v, 'ask_depth_p1_usd') != 'null', JSONExtractArrayRaw(res_final_liq, 'per_venue'))"
        ),
        reference_ready=_bool(
            "res_get(res_relative_final, 'ref_venue') != 'null' AND res_get(res_get(res_relative_final, 'anchor'), 'ref_price') != 'null'"
        ),
    )
    backfill = _object(
        **{
            key: f"res_get(r, '{key}')"
            for key in ("backfill_id", "status", "started_at", "finished_at", "anchor")
        }
    )
    detail = _object(
        capture_status="res_capture",
        backfill_runs=_array(
            f"arrayMap(r -> {backfill}, JSONExtractArrayRaw(res_snapshot, 'backfill_runs'))"
        ),
        windows="res_readiness_windows",
        incomplete_windows=_json("res_incomplete"),
        primary_window=_json("res_primary"),
        primary_window_ready=_bool("NOT has(res_incomplete, res_primary)"),
        prerequisite_pending=_bool(
            "JSONExtractString(res_capture, 'status') IN ('PENDING','RUNNING','PARTIAL','FAILED')"
        ),
    )
    return (
        r"""
res_empty_derivative_map AS (SELECT res_empty_result AS res_empty_map FROM res_empty_output),
res_final_windows AS (
    SELECT *, if(res_name = 'EVENT' AND NOT res_has_event,
            res_put(res_spot, '{"reason":"INCOMPLETE","classification":"INSUFFICIENT_DATA"}'), res_spot) AS res_final_spot,
        if(res_name = 'EVENT' AND NOT res_has_event, __EMPTY_DERIV__, res_deriv) AS res_final_deriv,
        if(res_name = 'EVENT' AND NOT res_has_event, __UNAVAILABLE__, res_liq) AS res_final_liq,
        if(res_name = 'EVENT' AND NOT res_has_event, __UNAVAILABLE__, res_put(res_su, concat('{"window":', toJSONString(res_name), ',"start":', toJSONString(res_flow_iso(res_wstart)), ',"cutoff":', toJSONString(res_flow_iso(res_end)), '}'))) AS res_final_su,
        if(res_has_event, res_relative_input,
            res_put(res_relative_input, concat('{"anchor":{"type":null,"ts":null,"ref_price":null,"reason":"INCOMPLETE"},"repricing_band":null,"windows":',
            res_put(res_get(res_relative_input, 'windows'), concat('{"EVENT":',
                res_put(concat('{', arrayStringConcat(arrayMap(k -> concat(toJSONString(k), ':null'), JSONExtractKeys(res_get(res_get(res_relative_input, 'windows'), 'EVENT'))), ','), '}'), '{"reason":"INCOMPLETE"}'), '}')), '}'))) AS res_relative_final,
        arrayExists(i -> res_flow_market(i) != 'SPOT', res_flow_instruments) AS res_has_perps,
        __READINESS__ AS res_window_ready
    FROM res_fact_rows CROSS JOIN res_empty_derivative_map
),
res_collected AS (
    SELECT arraySort(i -> indexOf(['W15M','W1H','W4H','W24H','EVENT'], i.1), groupArray((res_name, res_final_spot, res_final_deriv, res_final_liq, res_final_su, res_window_ready, res_selected_json, res_facts))) AS res_items,
        any(res_primary) AS res_primary, any(res_end) AS res_end, any(res_start) AS res_start,
        any(res_relative_final) AS res_relative_final
    FROM res_final_windows
),
res_assembly AS (
    SELECT *, arrayFirst(i -> i.1 = res_primary, res_items) AS res_primary_item,
        concat('{', arrayStringConcat(arrayMap(i -> concat(toJSONString(i.1), ':', i.2), res_items), ','), '}') AS res_final_spots,
        concat('{', arrayStringConcat(arrayMap(i -> concat(toJSONString(i.1), ':', i.3), res_items), ','), '}') AS res_final_derivatives,
        concat('{', arrayStringConcat(arrayMap(i -> concat(toJSONString(i.1), ':', i.4), res_items), ','), '}') AS res_final_liquidity,
        concat('{', arrayStringConcat(arrayMap(i -> concat(toJSONString(i.1), ':', i.5), res_items), ','), '}') AS res_final_supply,
        concat('{', arrayStringConcat(arrayMap(i -> concat(toJSONString(i.1), ':', i.6), res_items), ','), '}') AS res_readiness_windows,
        arrayMap(i -> i.1, arrayFilter(i -> NOT arrayAll(kv -> kv.2 = 'true', JSONExtractKeysAndValuesRaw(i.6)), res_items)) AS res_incomplete,
        if(res_get(res_snapshot, 'capture_status') IN ('null','{}'), '{"status":"UNKNOWN","reason":"CAPTURE_STATUS_NOT_PROVIDED"}', res_get(res_snapshot, 'capture_status')) AS res_capture,
        __DETAIL__ AS res_detail
    FROM res_collected
)
""".replace("__UNAVAILABLE__", unavailable)
        .replace("__EMPTY_DERIV__", empty_deriv)
        .replace("__READINESS__", readiness)
        .replace("__DETAIL__", detail)
    )


def enrichment_ctes():
    return truth_sql_spot.ctes().replace("spot_", "res_flow_") + ",\n" + _core_ctes()


def selection_ctes():
    return ",\n".join((_selection_ctes(), _fact_ctes()))


def enrichment_select_sql():
    return (
        "SELECT concat('{\"spots\":{', arrayStringConcat(groupArray(concat(toJSONString(res_name), ':', res_spot)), ','),"
        " '},\"derivatives\":{', arrayStringConcat(groupArray(concat(toJSONString(res_name), ':', res_deriv)), ','), '}}') AS result FROM res_enriched"
    )


def assembly_ctes():
    from tank.analysis import truth_sql_derivatives

    empty_derivatives = truth_sql_derivatives.ctes().replace("deriv_", "res_empty_")
    empty_derivatives = empty_derivatives.replace(
        "{res_empty_snapshot:String}",
        'JSONMergePatch({res_snapshot:String}, \'{"oi":[],"funding":[],"liquidations":[],"klines":[]}\')',
    ).replace("{res_empty_config:String}", "{res_config:String}")
    return empty_derivatives + ",\n" + _result_ctes()


def ctes():
    return ",\n".join((enrichment_ctes(), selection_ctes(), assembly_ctes()))


def select_sql():
    confidence = "concat('{', arrayStringConcat(arrayMap(kv -> concat(toJSONString(if(kv.1 = 'class_', 'class', kv.1)), ':', kv.2), arrayFilter(kv -> kv.1 != 'capture_readiness', JSONExtractKeysAndValuesRaw(res_confidence_input))), ','), '}')"
    result = _object(
        assembled_at="res_get(res_snapshot, 'assembled_at')",
        time_policy="'\"SEMANTIC_MARKET_TIME_WITH_ASSEMBLY_AVAILABILITY\"'",
        token_id="res_get(res_snapshot, 'token_id')",
        event_id="res_get(res_snapshot, 'event_id')",
        cutoff=_json("res_flow_iso(res_end)"),
        requested_cutoff="if(JSONHas(res_snapshot, 'requested_cutoff'), res_get(res_snapshot, 'requested_cutoff'), res_get(res_snapshot, 'cutoff'))",
        event_public_at="res_get(res_snapshot, 'event_public_at')",
        primary_window=_json("res_primary"),
        analysis_window=_object(
            window=_json("res_primary"),
            start=_json("res_flow_iso(res_start)"),
            end=_json("res_flow_iso(res_end)"),
        ),
        decisive_facts=_json("arrayMap(f -> JSONExtractString(f, 'text'), res_primary_item.8)"),
        decisive_fact_details=_array("res_primary_item.8"),
        decisive_facts_reason="if(length(res_primary_item.8) >= 2, 'null', '\"INCOMPLETE\"')",
        detail="res_detail",
        spot="res_final_spots",
        derivatives=_patch("res_primary_item.3", windows="res_final_derivatives"),
        liquidity=_patch("res_primary_item.4", windows="res_final_liquidity"),
        supply=_patch("res_primary_item.5", windows="res_final_supply"),
        relative="res_relative_final",
        data_confidence=_patch(confidence, capture_readiness="res_detail"),
        config_version="if(JSONHas(res_config, 'config_version'), res_get(res_config, 'config_version'), if(JSONHas(res_snapshot, 'config_version'), res_get(res_snapshot, 'config_version'), '\"unversioned\"'))",
        code_version="if(JSONHas(res_config, 'code_version'), res_get(res_config, 'code_version'), if(JSONHas(res_snapshot, 'code_version'), res_get(res_snapshot, 'code_version'), '\"unversioned\"'))",
    )
    context = _object(
        cutoff=_json("res_flow_iso(res_end)"),
        window_inheritance="'\"Nearest enclosing explicit window; canonical map keys override; anchor is EVENT\"'",
        shape_policy="'\"Literal TDD §2 bare fields retained; all quantitative and null derived leaves described in measurement_metadata\"'",
        unknown_precision="'\"Null with NOT_ESTIMABLE; no fabricated timestamp precision\"'",
        time_policy="'\"SEMANTIC_MARKET_TIME_WITH_ASSEMBLY_AVAILABILITY\"'",
        numeric_policy="'\"Native Decimal256(24) financial inputs admit magnitude below 1e24 and at most 24 fractional digits; each multiplication and division truncates toward zero at scale 24 with local error below 1e-24; significance and sell-share comparisons use these native rounded products; overflow and outside-range inputs are refused; dimensionless returns, beta, percentiles and display ratios use Float64 statistics\"'",
    )
    return (
        "SELECT res_put(res_put("
        + result
        + ", res_primary_item.7), "
        + _object(measurement_context=context)
        + ") AS result FROM res_assembly"
    )


def query(
    snapshot,
    config=None,
    *,
    spots=None,
    derivatives=None,
    liquidity=None,
    supply=None,
    relative=None,
    confidence=None,
):
    inputs = {
        "res_spots": spots,
        "res_derivatives": derivatives,
        "res_liquidity": liquidity,
        "res_supply": supply,
        "res_relative": relative,
        "res_confidence": confidence,
    }
    missing = [name.removeprefix("res_") for name, value in inputs.items() if value is None]
    if missing:
        raise ValueError("native Market Truth domains required: " + ", ".join(missing))
    _, flow_parameters = truth_sql_spot.query(_transport(snapshot), config)
    parameters = {name.replace("spot_", "res_flow_"): value for name, value in flow_parameters.items()}
    inputs.update(res_snapshot=snapshot, res_config=config or {})
    parameters.update(
        {name: (parameter_string(canonical_json(_transport(value))), "String") for name, value in inputs.items()}
    )
    return "WITH " + ctes() + "\n" + select_sql(), parameters
