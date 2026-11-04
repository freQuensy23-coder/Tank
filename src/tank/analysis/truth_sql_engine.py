import json
import math
import re
from array import array
from copy import deepcopy
from datetime import datetime
from decimal import Decimal

from tank.analysis import (
    truth_sql_derivatives,
    truth_sql_liquidity,
    truth_sql_relative,
    truth_sql_result,
    truth_sql_spot,
    truth_sql_support,
)
from tank.clickhouse_market import parameter_string
from tank.util import jsonable

NUMERIC_POLICY = (
    "Native Decimal256(24) quantities, USD amounts and funding rates with explicit range admission; "
    "dimensionless returns, beta, percentiles and ratios use FLOAT64 statistics; significance compares "
    "native Decimal products at scale 24 with explicit per-product truncation"
)

_DECIMAL_FIELDS = frozenset(
    [
        "cvd",
        "volume",
        "volume_native",
        "sum_sq_qty",
        "buy_qty",
        "sell_qty",
        "buy_notional",
        "sell_notional",
        "oi_native",
        "oi_native_start",
        "oi_native_end",
        "oi_usd",
        "oi_usd_start",
        "oi_usd_end",
        "position",
        "markup",
        "interaction",
        "basis_bps",
        "funding_8h_eq",
        "funding_dispersion_8h",
        "funding_estimate",
        "funding_estimate_8h_eq",
        "funding_estimate_interval_h",
        "funding_settled_8h_eq",
        "funding_settled_interval_h",
        "funding_settled_last",
        "funding_start",
        "funding_start_interval_h",
        "interval_h",
        "liq_long_native",
        "liq_long_usd",
        "liq_short_native",
        "liq_short_usd",
        "long",
        "short",
        "spot",
        "perp",
        "aggregate_ask_depth_p1_usd",
        "aggregate_bid_depth_p1_usd",
        "aggregate_depth_p1_usd",
        "ask_depth_p1_usd",
        "bid_depth_p1_usd",
        "avg_hit_side_depth_p1_usd",
        "depth_p1_usd",
        "hit_side_depth_lower_bound",
        "hit_side_depth_p1_usd",
        "net_flow",
        "pre_event_depth_p1_usd",
        "pre_event_hit_side_depth_lower_bound",
        "pre_event_hit_side_depth_p1_usd",
        "quote_conversion_uncertainty_bps",
        "close_1h_price",
        "covered_volume_native",
        "entry_ref_price",
        "price_end",
        "price_start",
        "ref_price",
        "large_ticket_buy_notional",
        "large_ticket_sell_notional",
        "chg_native_24h",
        "perp_volume_usd",
        "previous",
        "qty_native",
        "supply_chg_24h",
        "supply_chg_7d",
        "supply_usd",
        "value",
        "bid",
        "ask",
        "usd",
        "native",
        "tier_notional",
        "tier_notional_usd",
        "normal_range",
        "basis_normal_range",
        "funding_dispersion_normal_range_8h",
        "funding_normal_range_8h",
    ]
)
_DECIMAL_TEXT = re.compile(r"-?[0-9]+[.][0-9]{24}\Z")
_PARAMETER = re.compile(r"\{([A-Za-z_][A-Za-z_0-9]*):([^{}]+)\}")


def _transport(value):
    if isinstance(value, dict):
        return {key: _transport(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, array, bytes, bytearray, memoryview)):
        return [_transport(item) for item in value]
    return jsonable(value)


def _encode(value):
    return json.dumps(_transport(value), ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _bind(statement, bindings):
    return _PARAMETER.sub(lambda match: bindings.get(match[1], match[0]), statement)


def _window_map(table, name, value):
    return (
        "(SELECT concat('{', arrayStringConcat(arrayMap(p -> concat(toJSONString(p.1), ':', p.2), "
        f"arraySort(p -> indexOf(['W15M','W1H','W4H','W24H','EVENT'], p.1), "
        f"groupArray(tuple({name}, {value})))), ','), '}}') FROM {table})"
    )


def _input_ctes():
    return r"""
{eng_snapshot:String} AS eng_raw,
{eng_config:String} AS eng_config,
((o, p) -> concat('{', arrayStringConcat(arrayConcat(
    arrayMap(kv -> concat(toJSONString(kv.1), ':', kv.2), arrayFilter(kv -> NOT has(JSONExtractKeys(p), kv.1), JSONExtractKeysAndValuesRaw(o))),
    arrayMap(kv -> concat(toJSONString(kv.1), ':', kv.2), JSONExtractKeysAndValuesRaw(p))), ','), '}')) AS eng_put,
(x -> if(JSONType(x) = 'String', JSONExtractString(x), x)) AS eng_text,
(x -> if(JSONType(x) = 'Object', coalesce(nullIf(JSONExtractRaw(x, 'value'), ''), JSONExtractRaw(x, 'ts')), x)) AS eng_unwrap,
(x -> if(JSONType(eng_unwrap(x)) = 'Bool', if(eng_unwrap(x) = 'true', 1., 0.), toFloat64OrNull(eng_text(eng_unwrap(x))))) AS eng_timestamp_number,
(x -> if(JSONType(eng_unwrap(x)) IN ('Int64', 'UInt64', 'Float64', 'Bool') OR match(eng_text(eng_unwrap(x)), '^[0-9]+([.][0-9]+)?$'),
    fromUnixTimestamp64Micro(toInt64(roundBankers(multiIf(abs(eng_timestamp_number(x)) > 1e14, eng_timestamp_number(x), abs(eng_timestamp_number(x)) > 1e11,
        eng_timestamp_number(x) * 1000, eng_timestamp_number(x) * 1000000), 0)), 'UTC'),
    parseDateTime64BestEffortOrNull(eng_text(eng_unwrap(x)), 6, 'UTC'))) AS eng_unchecked_time,
(x -> if(eng_text(eng_unwrap(x)) IN ('', 'null'), CAST(NULL, 'Nullable(DateTime64(6, \'UTC\'))'),
    if(throwIf(isNull(eng_unchecked_time(x)) OR match(eng_text(eng_unwrap(x)), '^[0-9]{4}-')
        AND toInt32OrZero(substring(eng_text(eng_unwrap(x)), 1, 4)) NOT BETWEEN 1900 AND 2299,
        'Market Truth timestamp outside native supported range') = 0, eng_unchecked_time(x), NULL))) AS eng_time,
(x -> arrayFirst(k -> JSONExtractRaw(x, k) NOT IN ('', 'null'), ['minute_start','open_time','exchange_ts','ts','as_of','settlement_time','funding_time','observed_at'])) AS eng_time_field,
(x -> eng_time(JSONExtractRaw(x, eng_time_field(x)))) AS eng_observed,
(x -> formatDateTime(x, '%Y-%m-%dT%H:%i:%S', 'UTC') || if(positiveModulo(toUnixTimestamp64Micro(x), 1000000) = 0, '', concat('.', leftPad(toString(positiveModulo(toUnixTimestamp64Micro(x), 1000000)), 6, '0'))) || 'Z') AS eng_iso,
eng_time(JSONExtractRaw(eng_raw, 'cutoff')) AS eng_end,
eng_time(JSONExtractRaw(eng_raw, 'assembled_at')) AS eng_assembled,
if(JSONType(eng_raw, 'anchor') = 'Object', JSONExtractRaw(eng_raw, 'anchor', 'ts'), JSONExtractRaw(eng_raw, 'anchor')) AS eng_anchor_raw,
eng_anchor_raw NOT IN ('', 'null', '""', 'false', '0') AS eng_has_event,
if(eng_has_event, eng_time(eng_anchor_raw), eng_end - toIntervalHour(4)) AS eng_start,
if(eng_has_event AND dateDiff('microsecond', eng_start, eng_end) BETWEEN 900000000 AND 86400000000, 'EVENT', 'W4H') AS eng_primary,
(x -> isNull(eng_assembled) OR JSONExtractRaw(x, 'received_ts') IN ('', 'null', '""') OR eng_time(JSONExtractRaw(x, 'received_ts')) <= eng_assembled) AS eng_available,
((x, c) -> isNotNull(eng_observed(x)) AND eng_observed(x) <= eng_end AND eng_available(x)
    AND (c NOT IN ('flow_bars', 'klines') OR eng_observed(x) + toIntervalSecond(if(JSONHas(x, 'interval_s'), toInt64OrZero(eng_text(JSONExtractRaw(x, 'interval_s'))), 60)) <= eng_end)) AS eng_admitted,
(x -> coalesce(nullIf(JSONExtractString(x, 'venue'), ''), splitByChar(':', JSONExtractString(x, 'instrument_id'))[1])) AS eng_venue,
(x -> coalesce(nullIf(JSONExtractString(x, 'market_type'), ''), if(position(JSONExtractString(x, 'instrument_id'), ':spot:') > 0, 'SPOT', 'PERP_LINEAR'))) AS eng_market,
arrayFlatten(arrayMap(b -> arrayMap(p -> eng_put(p, concat('{"instrument_id":', JSONExtractRaw(b, 'instrument_id'), ',"token_id":', if(JSONHas(b, 'token_id'), JSONExtractRaw(b, 'token_id'), 'null'), ',"venue":', toJSONString(eng_venue(b)), ',"market_type":', toJSONString(eng_market(b)), '}')), JSONExtractArrayRaw(b, 'boundary_segments')), JSONExtractArrayRaw(eng_raw, 'flow_bars'))) AS eng_nested_segments,
arrayConcat(JSONExtractArrayRaw(eng_raw, 'flow_segments'), eng_nested_segments) AS eng_all_segments,
(p -> tuple(JSONExtractRaw(p, 'instrument_id'), JSONExtractRaw(p, 'start'), JSONExtractRaw(p, 'end'))) AS eng_segment_key,
arrayMap(k -> if(arrayExists(p -> eng_segment_key(p) = k, JSONExtractArrayRaw(eng_raw, 'flow_segments')),
    arrayLast(p -> eng_segment_key(p) = k, JSONExtractArrayRaw(eng_raw, 'flow_segments')),
    arrayFirst(p -> eng_segment_key(p) = k, eng_nested_segments)), arrayDistinct(arrayMap(eng_segment_key, eng_all_segments))) AS eng_segments,
arrayConcat(JSONExtractArrayRaw(eng_raw, 'reference_points'), arrayFlatten(arrayMap(b -> arrayMap(p -> eng_put(p, concat('{"instrument_id":', JSONExtractRaw(b, 'instrument_id'), '}')),
    arrayFilter(p -> JSONExtractRaw(p, 'boundary') NOT IN ('', 'null') AND eng_time(JSONExtractRaw(p, 'boundary')) <= eng_end, JSONExtractArrayRaw(b, 'reference_points'))), JSONExtractArrayRaw(eng_raw, 'flow_bars')))) AS eng_reference_points,
eng_input AS (
    SELECT eng_put(eng_put(eng_raw, concat('{', arrayStringConcat(arrayMap(c -> concat(toJSONString(c), ':[', arrayStringConcat(arrayFilter(r -> eng_admitted(r, c), JSONExtractArrayRaw(eng_raw, c)), ','), ']'),
        ['flow_bars','klines','books','oi','funding','liquidations','tickers','supply']), ','), '}')),
        concat('{"flow_segments":[', arrayStringConcat(arrayFilter(p -> eng_time(JSONExtractRaw(p, 'end')) <= eng_end AND eng_available(p), eng_segments), ','),
        '],"reference_points":[', arrayStringConcat(eng_reference_points, ','), '],"_primary":', toJSONString(eng_primary), ',"_has_event":', toJSONString(eng_has_event),
        ',"_lead_start":', toJSONString(eng_iso(eng_start)), ',"_event_anchor":', if(eng_has_event, toJSONString(eng_iso(eng_start)), 'null'), '}')) AS eng_snapshot,
        toJSONString(arrayMap(i -> JSONExtractString({eng_supply_preimages:String}, i), arrayFilter(i -> eng_admitted(JSONExtractRaw(eng_raw, 'supply', i), 'supply'), arrayEnumerate(JSONExtractArrayRaw(eng_raw, 'supply'))))) AS eng_supply_preimages
    WHERE throwIf(isNull(eng_end) OR isNull(eng_start) OR eng_start >= eng_end, 'anchor must precede cutoff') = 0
)
""".strip()


def _assessment_select():
    return truth_sql_result.select_sql().replace(
        "significance uses exact squared comparisons within admitted native ranges",
        "significance compares native Decimal products at scale 24 with explicit per-product truncation",
    )


def _provenance_select():
    statement = _assessment_select()
    suffix = " FROM res_assembly"
    if not statement.endswith(suffix):
        raise ValueError("native Market Truth assembly phase contract changed")
    return statement.removesuffix(suffix) + (
        ", (SELECT eng_snapshot FROM eng_input) AS provenance_snapshot FROM res_assembly"
    )


def _state_select(*, cache_transport=False):
    statement = _assessment_select()
    prefix, suffix = "SELECT ", " AS result FROM res_assembly"
    if not statement.startswith(prefix) or not statement.endswith(suffix):
        raise ValueError("native Market Truth state assembly contract changed")
    body = statement.removeprefix(prefix).removesuffix(suffix)
    spots = _window_map(
        "res_enriched", "res_name",
        "if(res_name = res_primary, res_spot, res_put(res_spot, "
        "concat('{\"size_profile\":', res_get(res_spot_original, 'size_profile'), '}')))",
    )
    relative = (
        "if(eng_has_event, (SELECT result FROM eng_relative), "
        "res_put((SELECT result FROM eng_relative), concat('{\"repricing_band\":null,\"windows\":', "
        "res_put(JSONExtractRaw((SELECT result FROM eng_relative), 'windows'), "
        "concat('{\"EVENT\":', res_put(JSONExtractRaw((SELECT result FROM eng_relative), 'windows', 'EVENT'), "
        "'{\"r_raw\":null}'), '}')), '}')))"
    )
    cache = "',\"_native_condition_cache\":', con_cache_json(eng_node_cache_4), " if cache_transport else ""
    return (
        f"SELECT res_put({body}, concat('{{\"spot\":', {spots}, ',\"relative\":', {relative}, "
        + cache + "'}')) AS result FROM res_assembly"
    )


def query(snapshot, config=None):
    return _query(snapshot, config, _assessment_select)


def state_query(snapshot, config=None, *, node_cache=None):
    return _query(
        snapshot, config, lambda: _state_select(cache_transport=node_cache is not None), node_cache=node_cache,
        condition_state=True,
    )


def _query(snapshot, config, projection, *, node_cache=None, condition_state=False):
    config = config or {}
    if config.get("market_context_rule", "normal_range") != "normal_range":
        raise ValueError("Only the approved §5.7 normal_range market context rule is supported")
    packet = _transport(snapshot)
    parameters = {
        "eng_snapshot": (parameter_string(_encode(packet)), "String"),
        "eng_config": (parameter_string(_encode(config)), "String"),
    }
    bound_snapshot = "(SELECT eng_snapshot FROM eng_input)"
    spot_statement, spot_parameters = truth_sql_spot.query(packet, config)
    derivative_statement, derivative_parameters = truth_sql_derivatives.query(packet, config)
    relative_statement, relative_parameters = truth_sql_relative.query(
        packet, dict(config, start=packet["cutoff"], end=packet["cutoff"], include_outcome=not condition_state)
    )
    liquidity_statement, liquidity_parameters = truth_sql_liquidity.windows_query(packet, config)
    _, support_parameters = truth_sql_support.query(packet, config)
    _, result_parameters = truth_sql_result.query(
        packet, config, spots={}, derivatives={}, liquidity={}, supply={}, relative={}, confidence={}
    )
    parameters["eng_supply_preimages"] = support_parameters["sup_supply_preimages"]
    sources = (
        spot_parameters, derivative_parameters, relative_parameters,
        liquidity_parameters, support_parameters, result_parameters,
    )
    bindings = {
        name: bound_snapshot
        for source in sources for name in source if name.endswith("snapshot")
    }
    bindings.update({name: "eng_config" for source in sources for name in source if name.endswith("config")})
    bindings.update({
        "rel_start": "eng_iso(eng_start)",
        "rel_end": "eng_iso(eng_end)",
        "lq_spot": "JSONExtractRaw((SELECT result FROM eng_spot), 'spots')",
        "lq_relative": "(SELECT result FROM eng_relative)",
        "lq_deriv": "(SELECT result FROM eng_derivatives)",
        "sup_spots": _window_map("eng_enriched", "res_name", "res_spot"),
        "sup_derivatives": _window_map("eng_enriched", "res_name", "res_deriv"),
        "sup_liquidity": "(SELECT result FROM eng_liquidity)",
        "sup_relative": "(SELECT result FROM eng_relative)",
        "sup_supply_preimages": "(SELECT eng_supply_preimages FROM eng_input)",
        "res_spots": "(SELECT result FROM eng_spot)",
        "res_derivatives": "(SELECT result FROM eng_derivatives)",
        "res_liquidity": "(SELECT result FROM eng_liquidity)",
        "res_supply": _window_map("pre_supply_stage", "pre_window", "pre_supply"),
        "res_relative": "(SELECT result FROM eng_relative)",
        "res_confidence": "(SELECT pre_confidence FROM pre_conf_output)",
    })
    independent = ",\n".join((
        truth_sql_support._core(), truth_sql_support._supply(),
        truth_sql_support._freshness(), truth_sql_support._confidence(),
    )).replace("sup_", "pre_")
    bindings.update({
        "pre_snapshot": bound_snapshot,
        "pre_config": "eng_config",
        "pre_spots": "JSONExtractRaw((SELECT result FROM eng_spot), 'spots')",
        "pre_derivatives": "(SELECT result FROM eng_derivatives)",
        "pre_liquidity": "(SELECT result FROM eng_liquidity)",
        "pre_relative": "(SELECT result FROM eng_relative)",
        "pre_supply_preimages": "(SELECT eng_supply_preimages FROM eng_input)",
    })
    enrichment = truth_sql_result.enrichment_ctes()
    if enrichment.count("res_enriched AS (") != 1:
        raise ValueError("native Market Truth residual phase contract changed")
    enrichment = enrichment.replace("res_enriched AS (", "eng_enriched AS (")
    support = ",\n".join((
        truth_sql_support._core(), truth_sql_support._freshness(),
        "sup_supply_stage AS (SELECT pre_window AS sup_window, pre_window_start AS sup_window_start, "
        "pre_inventory_history AS sup_inventory_history, pre_inventory AS sup_inventory, "
        "pre_inventory_evidence AS sup_inventory_evidence, pre_supply AS sup_supply FROM pre_supply_stage)",
        truth_sql_support._support(), truth_sql_support._output(),
    ))
    supported = (
        "res_enriched AS (SELECT e.* EXCEPT (res_deriv), s.sup_derivative_json AS res_deriv "
        "FROM eng_enriched AS e INNER JOIN sup_window_output AS s ON e.res_name = s.sup_window)"
    )
    spot_cte = "eng_spot AS (" + spot_statement + ")"
    relative_cte = "eng_relative AS (" + relative_statement + ")"
    derivative_cte = "eng_derivatives AS (" + derivative_statement + ")"
    if node_cache is not None:
        spot_cte = (
            f"{node_cache} AS eng_node_cache_0, eng_spot_actual AS (SELECT result FROM ({spot_statement}) "
            "WHERE NOT con_cache_has(eng_node_cache_0, con_node_key('spot'))), "
            "eng_spot AS (SELECT if(con_cache_has(eng_node_cache_0, con_node_key('spot')), "
            "con_cache_get(eng_node_cache_0, con_node_key('spot')), assumeNotNull((SELECT result FROM eng_spot_actual))) AS result), "
            "con_cache_store(eng_node_cache_0, con_node_key('spot'), (SELECT result FROM eng_spot)) AS eng_node_cache_1"
        )
        relative_cte = (
            f"eng_relative_actual AS (SELECT result FROM ({relative_statement}) "
            "WHERE NOT con_cache_has(eng_node_cache_1, con_node_key('relative'))), "
            "eng_relative AS (SELECT if(con_cache_has(eng_node_cache_1, con_node_key('relative')), "
            "con_cache_get(eng_node_cache_1, con_node_key('relative')), assumeNotNull((SELECT result FROM eng_relative_actual))) AS result), "
            "con_cache_store(eng_node_cache_1, con_node_key('relative'), (SELECT result FROM eng_relative)) AS eng_node_cache_2"
        )
        derivative_call = "deriv_window(deriv_cutoff - deriv_w.2, deriv_cutoff)"
        if derivative_statement.count(derivative_call) != 1:
            raise ValueError("native condition derivative cache phase contract changed")
        derivative_cached = derivative_statement.replace(
            derivative_call,
            "if(deriv_w.1 = eng_primary AND con_cache_has(eng_node_cache_2, con_node_key('derivatives')), "
            "con_cache_get(eng_node_cache_2, con_node_key('derivatives')), " + derivative_call + ")",
        )
        derivative_cte = (
            f"eng_derivatives AS ({derivative_cached}), "
            "con_cache_store(eng_node_cache_2, con_node_key('derivatives'), "
            "JSONExtractRaw((SELECT result FROM eng_derivatives), eng_primary)) AS eng_node_cache_3"
        )
        supply_prefix = " AS pre_inventory_evidence,\n        "
        supply_suffix = " AS pre_supply\n"
        if independent.count(supply_prefix) != 1 or independent.count(supply_suffix) != 1:
            raise ValueError("native condition supply cache phase contract changed")
        supply_head, _, supply_tail = independent.partition(supply_prefix)
        supply_value, _, supply_rest = supply_tail.partition(supply_suffix)
        independent = supply_head + supply_prefix + (
            "if(pre_window = eng_primary AND con_cache_has(eng_node_cache_3, con_node_key('supply')), "
            "con_cache_get(eng_node_cache_3, con_node_key('supply')), " + supply_value + ")"
        ) + supply_suffix + supply_rest + (
            ", con_cache_store(eng_node_cache_3, con_node_key('supply'), "
            "(SELECT pre_supply FROM pre_supply_stage WHERE pre_window = eng_primary)) AS eng_node_cache_4"
        )
    final_select = projection()
    if not final_select.endswith(" FROM res_assembly"):
        raise ValueError("native Market Truth assembly phase contract changed")
    statement = "WITH " + ",\n".join((
        _input_ctes(), spot_cte, relative_cte, derivative_cte,
        "eng_liquidity AS (" + liquidity_statement + ")",
        independent, enrichment, support, supported,
        truth_sql_result.selection_ctes(), truth_sql_result.assembly_ctes(),
    )) + "\n" + final_select
    statement = _bind(statement, bindings)
    referenced = {match[1] for match in _PARAMETER.finditer(statement)}
    for source in sources:
        for name, value in source.items():
            if name in referenced and name not in parameters:
                parameters[name] = value
    if referenced != set(parameters):
        raise ValueError("native Market Truth parameter composition contract changed")
    return statement, parameters


def condition_query(snapshot, requirements, config=None, *, cache_records=None, cache_transport=False):
    from tank.analysis import truth_sql_conditions

    requirements = list(requirements)
    statement, parameters = truth_sql_conditions.query(
        snapshot, requirements, config, result={}, cache_records=cache_records, cache_transport=cache_transport
    )
    state_snapshot = {field: snapshot.get(field, []) for field in truth_sql_conditions.DOMAIN_FIELDS["state"]}
    state_snapshot.update(
        token_id=snapshot.get("token_id"), cutoff=snapshot["cutoff"],
        assembled_at=snapshot.get("assembled_at"),
        domain_dependency_fingerprints=snapshot.get("domain_dependency_fingerprints", {}),
        anchor=snapshot.get("anchor") or None,
    )
    for index, (metric, window) in enumerate(truth_sql_conditions._ordered_requirements(requirements), 1):
        if metric not in truth_sql_conditions.STATE_METRICS or window not in {"W15M", "W1H", "W4H", "W24H", "EVENT"}:
            continue
        engine_statement, engine_parameters = state_query(
            state_snapshot, config, node_cache=f"(SELECT con_packet.1 FROM con_step_{index - 1})"
        )
        admitted = (
            "if(eng_time(if(JSONType({eng_snapshot:String}, 'anchor') = 'Object', "
            "JSONExtractRaw({eng_snapshot:String}, 'anchor', 'ts'), "
            "JSONExtractRaw({eng_snapshot:String}, 'anchor'))) >= "
            "eng_time(JSONExtractRaw({eng_snapshot:String}, 'cutoff')), "
            "eng_put({eng_snapshot:String}, '{\"anchor\":null}'), {eng_snapshot:String})"
        )
        engine_statement = engine_statement.replace(
            "{eng_snapshot:String} AS eng_raw", admitted + " AS eng_raw"
        )
        bound = (
            f"if((SELECT NOT con_admitted OR con_reused FROM con_step_{index}_key) "
            "OR (con_has_anchor AND con_anchor >= con_end), '{}', (" + engine_statement + "))"
        )
        statement = _bind(statement, {f"con_state_result_{index}": bound})
        parameters.pop(f"con_state_result_{index}")
        for name, value in engine_parameters.items():
            if name in parameters and parameters[name] != value:
                raise ValueError("native Market Truth condition parameter namespace collision")
            parameters[name] = value
    return statement, parameters


async def evaluate_conditions(client, snapshot, requirements, config=None, *, cache=None):
    if client is None or not callable(getattr(client, "rows", None)):
        raise RuntimeError("Market Truth conditions require the repository ClickHouse native provider")
    requirements = list(requirements)
    records = [] if cache is None else [{"key": key, "value": deepcopy(value)} for key, value in cache.items()]
    statement, parameters = condition_query(
        snapshot, requirements, config, cache_records=records, cache_transport=True
    )
    rows = await client.rows(statement, parameters)
    if len(rows) != 1 or set(rows[0]) != {"result", "condition_cache"}:
        raise ValueError("native Market Truth conditions require one complete result row")
    result = json.loads(rows[0]["result"], object_pairs_hook=_condition_pairs)
    records = json.loads(rows[0]["condition_cache"], object_pairs_hook=_condition_pairs)
    _validate_condition_json(result)
    _validate_condition_json(records)
    _validate_conditions(result, snapshot, requirements)
    replacement = deepcopy(_validate_condition_cache(records))
    if cache is not None:
        cache.clear()
        cache.update(replacement)
    return result


_CONDITION_WINDOWS = {"W15M", "W1H", "W4H", "W24H", "EVENT"}
_CONDITION_TEXT = {"type": "string"}
_CONDITION_NULL_TEXT = {"type": ["string", "null"]}
_CONDITION_NUMBER = {"type": ["number", "null"]}
_CONDITION_BOOL = {"type": ["boolean", "null"]}
_CONDITION_MONEY = {"type": ["string", "null"], "pattern": r"^-?[0-9]+[.][0-9]{24}$"}
_CONDITION_OBJECT = {"type": "object"}
_CONDITION_ROWS = {"type": "array", "items": _CONDITION_OBJECT}
_CONDITION_STRINGS = {"type": "array", "items": _CONDITION_TEXT}


def _condition_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("native Market Truth conditions returned duplicate object fields")
        result[key] = value
    return result


def _validate_condition_json(value):
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("native Market Truth conditions returned non-finite JSON")
    if isinstance(value, dict):
        for item in value.values():
            _validate_condition_json(item)
    elif isinstance(value, list):
        for item in value:
            _validate_condition_json(item)


def _condition_fields(value, properties, *, required=None, exact=False):
    from tank.event_tools import validate_schema

    validate_schema(value, {
        "type": "object", "properties": properties,
        "required": list(properties) if required is None else required,
        "additionalProperties": not exact,
    }, "native Market Truth conditions")


def _condition_time(value, *, nullable=False):
    if value is None and nullable:
        return
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:[.][0-9]{6})?Z", value):
        raise ValueError("native Market Truth conditions returned an invalid timestamp")
    try:
        stamp = datetime.fromisoformat(value)
    except ValueError:
        raise ValueError("native Market Truth conditions returned an invalid timestamp") from None
    if not 1900 <= stamp.year <= 2299:
        raise ValueError("native Market Truth conditions returned an unsupported timestamp")


def _condition_timestamp(value, field):
    from tank.time_precision import Timestamp

    _condition_fields(value, {
        field: _CONDITION_NULL_TEXT, field + "_prec": _CONDITION_NULL_TEXT,
        field + "_prec_reason": _CONDITION_NULL_TEXT,
        field + "_bucket_anchor": _CONDITION_NULL_TEXT,
    }, required=[field, field + "_prec", field + "_prec_reason"])
    _condition_time(value[field], nullable=True)
    Timestamp(value[field], value[field + "_prec"], value[field + "_prec_reason"], value.get(field + "_bucket_anchor"))


def _condition_observations(observations, *, depth):
    if not isinstance(observations, list):
        raise ValueError("native Market Truth conditions returned invalid observations")
    for row in observations:
        properties = {
            "venue" if depth else "instrument_id": _CONDITION_TEXT,
            "observed_at": _CONDITION_NULL_TEXT, "cadence_s": _CONDITION_NUMBER,
            "cadence_source": _CONDITION_TEXT,
        }
        if depth:
            properties["fresh"] = {"type": "boolean"}
        else:
            properties.update({
                "observed_at_prec": _CONDITION_NULL_TEXT, "observed_at_prec_reason": _CONDITION_NULL_TEXT,
                "observed_at_bucket_anchor": _CONDITION_NULL_TEXT,
            })
        _condition_fields(row, properties, required=[key for key in properties if key != "observed_at_bucket_anchor"], exact=True)
        _condition_time(row["observed_at"], nullable=not depth)
        if not depth:
            _condition_timestamp(row, "observed_at")


def _condition_spot_coverage(value):
    _condition_fields(value, {
        "covered_share_est": _CONDITION_NUMBER, "exact_majority": _CONDITION_BOOL,
        "n_complete": {"type": "integer", "minimum": 0}, "excluded": _CONDITION_ROWS,
    }, exact=True)
    for row in value["excluded"]:
        _condition_fields(row, {
            "venue": _CONDITION_TEXT, "reason": _CONDITION_TEXT, "detail": _CONDITION_TEXT,
            "source_reasons": _CONDITION_STRINGS, "missing_metrics": _CONDITION_STRINGS,
        }, required=["venue", "reason", "detail"], exact=True)


def _condition_spot(value):
    _condition_fields(value, {
        "venues": _CONDITION_ROWS, "composite_max": _CONDITION_OBJECT,
        "composite_cohort": {"type": ["object", "null"]}, "breadth": _CONDITION_OBJECT,
        "cw_breadth": _CONDITION_OBJECT, "exact_majority": _CONDITION_OBJECT,
        "topn": _CONDITION_OBJECT, "concentration": _CONDITION_OBJECT,
        "direction": {"enum": [None, "BUY", "SELL", "NEUTRAL"]},
        "classification": {"enum": ["BROAD", "DIVERGENT", "CONCENTRATED", "INSUFFICIENT_DATA"]},
        "coverage": _CONDITION_OBJECT, "cohort_consistent": _CONDITION_OBJECT,
        "size_profile": {"type": ["object", "null"]},
    })
    _condition_spot_coverage(value["coverage"])
    _condition_fields(value, {
        "window": _CONDITION_TEXT, "start": _CONDITION_NULL_TEXT, "cutoff": _CONDITION_TEXT,
        "observed_at": _CONDITION_NULL_TEXT, "precision": _CONDITION_NULL_TEXT,
    }, required=[])
    for pack in [value["composite_max"], *([value["composite_cohort"]] if value["composite_cohort"] is not None else [])]:
        _condition_fields(pack, {
            "volume": _CONDITION_MONEY, "cvd": _CONDITION_MONEY, "venues": _CONDITION_STRINGS,
            "buy_share": _CONDITION_NUMBER, "bs_ratio": _CONDITION_NUMBER, "sign": {"type": "integer"},
        })
    for row in value["venues"]:
        _condition_fields(row, {
            "venue": _CONDITION_TEXT, "complete": {"type": "boolean"},
            "volume_native": _CONDITION_MONEY, "buy_qty": _CONDITION_MONEY, "sell_qty": _CONDITION_MONEY,
            "cvd": _CONDITION_MONEY, "sum_sq_qty": _CONDITION_MONEY,
            "buy_share": _CONDITION_NUMBER, "bs_ratio": _CONDITION_NUMBER,
            "sign": {"type": "integer"}, "boundary_segments": _CONDITION_ROWS,
            "volume_share": _CONDITION_NUMBER,
        })
        for segment in row["boundary_segments"]:
            _condition_fields(segment, {
                "start": _CONDITION_TEXT, "cutoff": _CONDITION_TEXT,
                "trade_count": {"type": "integer"}, "aggregates": _CONDITION_ROWS,
            })
    _condition_fields(value["breadth"], {
        **{key: {"type": "integer"} for key in ("n_pos", "n_neg", "n_neutral")},
        "pct_pos": _CONDITION_NUMBER, "pct_neg": _CONDITION_NUMBER,
    }, exact=True)
    _condition_fields(value["cw_breadth"], dict.fromkeys(("buy", "sell", "neutral"), _CONDITION_NUMBER), exact=True)
    _condition_fields(value["exact_majority"], dict.fromkeys(("buy", "sell", "neutral", "covered"), _CONDITION_BOOL), exact=True)
    _condition_fields(value["topn"], {"n": {"type": "integer"}, "venues": _CONDITION_STRINGS, "agreement": _CONDITION_NUMBER}, exact=True)
    _condition_fields(value["concentration"], {
        **dict.fromkeys(("hhi", "top_volume_share", "top_contrib_share", "others_cwb_dir"), _CONDITION_NUMBER),
        "top_volume_venue": _CONDITION_NULL_TEXT, "top_contrib_venue": _CONDITION_NULL_TEXT,
    }, exact=True)
    if not value["cohort_consistent"].keys() <= _CONDITION_WINDOWS or any(type(item) is not bool for item in value["cohort_consistent"].values()):
        raise ValueError("native Market Truth conditions returned invalid cohort consistency")
    if value["composite_cohort"] is not None:
        _condition_fields(value["composite_cohort"], {
            "cohort": _CONDITION_STRINGS, "share_of_max": _CONDITION_NUMBER,
            "share_majority": _CONDITION_BOOL, "velocity": _CONDITION_NUMBER, "reason": _CONDITION_NULL_TEXT,
        })
    if value["size_profile"] is not None:
        _condition_fields(value["size_profile"], {
            "large_ticket_buy_share": _CONDITION_NUMBER, "large_ticket_sell_share": _CONDITION_NUMBER,
            "large_ticket_buy_majority": _CONDITION_BOOL, "large_tickets_minority": _CONDITION_BOOL,
            "large_ticket_buy_notional": _CONDITION_MONEY, "large_ticket_sell_notional": _CONDITION_MONEY,
            "sell_share_rising": _CONDITION_BOOL, "compatible_venues": _CONDITION_STRINGS,
        })
    if "cohort_comparisons" in value:
        _condition_fields(value, {"cohort_comparisons": _CONDITION_OBJECT})
        for pack in value["cohort_comparisons"].values():
            _condition_fields(pack, {
                "cohort": _CONDITION_STRINGS, "current": {"type": ["object", "null"]},
                "other": {"type": ["object", "null"]}, "share_of_max": _CONDITION_NUMBER,
                "share_majority": _CONDITION_BOOL, "reason": _CONDITION_NULL_TEXT,
            })
            for composite in (pack["current"], pack["other"]):
                if composite is not None:
                    _condition_fields(composite, {
                        "volume": _CONDITION_MONEY, "cvd": _CONDITION_MONEY, "venues": _CONDITION_STRINGS,
                        "buy_share": _CONDITION_NUMBER, "bs_ratio": _CONDITION_NUMBER, "sign": {"type": "integer"},
                    })


def _condition_derivative(value, *, raw):
    _condition_fields(value, {"per_venue": _CONDITION_ROWS, "synchronized": {"type": ["object", "null"]}})
    if raw:
        _condition_fields(value, {
            "funding_dispersion_8h": _CONDITION_MONEY, "liquidations": _CONDITION_OBJECT,
            "incremental_share": {"type": ["object", "null"]},
            "incremental_volume_coverage": _CONDITION_OBJECT, "lead_lag": _CONDITION_OBJECT,
        })
        _condition_fields(value["lead_lag"], {"first_mover": _CONDITION_TEXT, "spot_confirmed": _CONDITION_BOOL, "sequence": _CONDITION_ROWS})
        for key in ("complete_venues_total", "sampled_venues_min", "complete_venues_total_reasons", "sampled_venues_min_reasons"):
            _condition_fields(value["liquidations"], {key: _CONDITION_OBJECT})
        for key in ("spot", "perp"):
            _condition_fields(value["incremental_volume_coverage"], {key: _CONDITION_OBJECT})
            _condition_fields(value["incremental_volume_coverage"][key], {
                "instruments": _CONDITION_STRINGS, "complete": {"type": "boolean"},
                "unit": _CONDITION_TEXT, "reason": _CONDITION_NULL_TEXT,
            })
    for pack in [*value["per_venue"], *([value["synchronized"]] if value["synchronized"] is not None else [])]:
        _condition_fields(pack, {
            "oi_class": {"enum": [None, "REAL_NATIVE", "PRICE_MARKUP", "MIXED"]}, "oi_native_start": _CONDITION_MONEY,
            "oi_native_end": _CONDITION_MONEY, "oi_native_chg_pct": _CONDITION_NUMBER,
            "oi_usd_start": _CONDITION_MONEY, "oi_usd_end": _CONDITION_MONEY,
            "oi_usd_chg_pct": _CONDITION_NUMBER,
        })
        if "decomposition" in pack:
            _condition_fields(pack, {
                "requested_window_duration_s": _CONDITION_NUMBER, "observed_window_duration_s": _CONDITION_NUMBER,
                "native_oi_vs_normal": _CONDITION_NULL_TEXT, "precision": _CONDITION_NULL_TEXT,
                "oi_usd_reason": _CONDITION_NULL_TEXT, "oi_usd_detail": _CONDITION_NULL_TEXT,
                "decomposition": _CONDITION_OBJECT, "classification_ready": {"type": "boolean"},
                "native_normal_range": {"type": ["array", "null"], "items": {"type": "number"}},
                "usd_normal_range": {"type": ["array", "null"], "items": {"type": "number"}},
                "native_beyond": _CONDITION_BOOL, "usd_beyond": _CONDITION_BOOL,
                "start_observed_at": _CONDITION_NULL_TEXT, "end_observed_at": _CONDITION_NULL_TEXT,
            })
            _condition_fields(pack["decomposition"], {
                "position": _CONDITION_MONEY, "markup": _CONDITION_MONEY,
                "interaction": _CONDITION_MONEY, "pos_share": _CONDITION_NUMBER,
            })
        else:
            _condition_fields(pack, {"reason": _CONDITION_TEXT, "detail": _CONDITION_NULL_TEXT})
        if pack is value["synchronized"]:
            _condition_fields(pack, {"venues": _CONDITION_STRINGS})
        else:
            _condition_fields(pack, {
                "venue": _CONDITION_TEXT, "instrument_id": _CONDITION_TEXT,
                "coverage": _CONDITION_OBJECT, "funding_vs_normal": _CONDITION_NULL_TEXT,
                "funding_8h_kind": _CONDITION_TEXT, "liq_capture_window": _CONDITION_OBJECT,
                "liq_long_count": {"type": "integer"}, "liq_short_count": {"type": "integer"},
                "liq_long_usd_reason": _CONDITION_NULL_TEXT, "liq_short_usd_reason": _CONDITION_NULL_TEXT,
                "liq_coverage": _CONDITION_TEXT, "liq_source_coverage": _CONDITION_TEXT,
                **dict.fromkeys((
                    "funding_settled_last", "funding_settled_interval_h", "funding_estimate",
                    "funding_estimate_interval_h", "interval_h", "funding_8h_eq", "funding_settled_8h_eq",
                    "funding_estimate_8h_eq", "funding_start", "funding_start_interval_h", "basis_bps",
                    "liq_long_usd", "liq_long_native", "liq_short_usd", "liq_short_native",
                ), _CONDITION_MONEY),
                **dict.fromkeys(("funding_settled_last_observed_at", "funding_estimate_observed_at", "funding_start_observed_at"), _CONDITION_NULL_TEXT),
            })
            _condition_fields(pack, {
                "perp_taker_buy_share": _CONDITION_NUMBER, "perp_volume_usd": _CONDITION_MONEY,
                "perp_volume_reason": _CONDITION_NULL_TEXT, "basis_vs_normal": _CONDITION_NULL_TEXT,
                "basis_widening": _CONDITION_BOOL, "funding_rising": _CONDITION_BOOL,
                "basis_normal_range": {"type": ["array", "null"], "items": _CONDITION_MONEY},
            }, required=[])
            _condition_fields(pack["coverage"], {"venue": _CONDITION_TEXT})
            _condition_fields(pack["liq_capture_window"], {
                "start": {}, "observed_through": {}, "complete": {"type": "boolean"},
                "reason": _CONDITION_NULL_TEXT,
            })
            if "oi_temporal_exclusions" in pack:
                _condition_fields(pack["oi_temporal_exclusions"], {
                    "count": {"type": "integer"}, "reason": _CONDITION_TEXT,
                    "detail": _CONDITION_TEXT, "latest_observation": _CONDITION_OBJECT,
                }, exact=True)
                latest = pack["oi_temporal_exclusions"]["latest_observation"]
                _condition_fields(latest, {
                    "ts": _CONDITION_NULL_TEXT, "ts_prec": _CONDITION_NULL_TEXT,
                    "ts_prec_reason": _CONDITION_NULL_TEXT, "ts_bucket_anchor": _CONDITION_NULL_TEXT,
                    "oi_native": _CONDITION_MONEY, "oi_usd": _CONDITION_MONEY,
                    "scope": {"enum": ["SUPPLEMENTAL_PROVIDER_OBSERVATION_NOT_WINDOW_ENDPOINT"]},
                }, required=["ts", "ts_prec", "ts_prec_reason", "oi_native", "oi_usd", "scope"], exact=True)
                if latest["ts_prec"] is not None and latest["ts_prec"].startswith("BUCKET("):
                    _condition_fields(latest, {"ts_bucket_anchor": _CONDITION_TEXT})
                _condition_timestamp(latest, "ts")
    if "support_features" in value:
        features = value["support_features"]
        _condition_fields(features, {
            **dict.fromkeys(("spot_volume_up_beyond_normal", "spot_volume_within_normal", "perp_volume_up_beyond_normal",
                "perp_volume_within_normal", "perp_taker_flow_buy", "basis_within_normal", "basis_widening", "funding_rising",
                "liquidations_first_half_majority", "large_ticket_buy_majority", "large_tickets_minority",
                "large_ticket_sell_share_rising", "exchange_inflows", "localized_inventory_transfer",
                "ask_depth_building", "ask_replenishment"), _CONDITION_BOOL),
            "spot_volume": _CONDITION_OBJECT, "perp_volume": _CONDITION_OBJECT,
        })
        for key in ("spot_volume", "perp_volume"):
            _condition_fields(features[key], {
                "value": _CONDITION_MONEY, "previous": _CONDITION_MONEY,
                "normal_range": {"type": ["array", "null"], "items": _CONDITION_MONEY},
                "unit": _CONDITION_TEXT, "window": _CONDITION_TEXT, "reason": _CONDITION_NULL_TEXT,
            })
    if raw:
        for row in value["lead_lag"]["sequence"]:
            _condition_fields(row, {"venue": _CONDITION_TEXT, "market": _CONDITION_TEXT, "t_first": _CONDITION_TEXT})
        for key, pack in value["liquidations"].items():
            _condition_fields(pack, dict.fromkeys(("long", "short"), _CONDITION_NULL_TEXT if key.endswith("_reasons") else _CONDITION_MONEY), exact=True)
        if value["incremental_share"] is not None:
            _condition_fields(value["incremental_share"], {
                "spot": _CONDITION_NUMBER, "perp": _CONDITION_NUMBER, "increment_usd": _CONDITION_OBJECT,
            })
            _condition_fields(value["incremental_share"]["increment_usd"], dict.fromkeys(("spot", "perp"), _CONDITION_MONEY), exact=True)


def _condition_relative(value):
    _condition_fields(value, {
        "expected_direction": _CONDITION_NULL_TEXT, "repricing_sign": _CONDITION_NUMBER,
        "outcome_btc_reference": _CONDITION_OBJECT, "normal_ranges": _CONDITION_OBJECT,
        "entry_ref_price": _CONDITION_MONEY, "entry_price_source": _CONDITION_TEXT,
        "entry_price_observed_at": _CONDITION_NULL_TEXT, "close_1h_price": _CONDITION_MONEY,
        "ref_venue": _CONDITION_NULL_TEXT, "reference_coverage": _CONDITION_OBJECT,
        "reference_reason": _CONDITION_NULL_TEXT, "anchor": _CONDITION_OBJECT,
        "windows": _CONDITION_OBJECT, "beta_btc": _CONDITION_NUMBER, "beta_eth": _CONDITION_NUMBER,
        "beta_flags": _CONDITION_OBJECT, "pre_event_runup": _CONDITION_OBJECT,
        "repricing_band": _CONDITION_NULL_TEXT, "market_context": _CONDITION_NULL_TEXT,
        "context_inputs": _CONDITION_OBJECT,
    })
    _condition_window_map(value["windows"])
    if set(value["windows"]) != _CONDITION_WINDOWS or value["normal_ranges"] != {}:
        raise ValueError("native Market Truth conditions returned incomplete relative windows")
    for pack in value["windows"].values():
        _condition_fields(pack, {
            **dict.fromkeys(("window", "start", "cutoff", "relative_measure", "historical_alignment"), _CONDITION_TEXT),
            **dict.fromkeys(("observed_at", "precision", "r_btc_adj_reason", "sector_reason", "normal_range_reason"), _CONDITION_NULL_TEXT),
            **dict.fromkeys(("r_raw", "r_btc_adj", "r_eth_adj", "r_sector_adj", "relative_return", "pctile",
                "historical_window_resolution_s", "historical_window_duration_s", "requested_window_duration_s"), _CONDITION_NUMBER),
            "normal_range": {"type": ["array", "null"], "items": {"type": "number"}},
            "beyond_normal": _CONDITION_BOOL, "historical_measurement": _CONDITION_OBJECT,
            "coverage": _CONDITION_OBJECT,
        })
        _condition_fields(pack["coverage"], {"reference_venue": _CONDITION_NULL_TEXT, "complete_24h": {"type": "boolean"}})
        history = pack["historical_measurement"]
        _condition_fields(history, {"exact": {"type": "boolean"}, "target_window_duration_s": _CONDITION_NUMBER})
        if "alignment" in history:
            _condition_fields(history, {
                "resolution_s": _CONDITION_NUMBER, "alignment": _CONDITION_TEXT,
                "sample_count": {"type": "integer"}, "latest_observed_at": _CONDITION_NULL_TEXT,
            })
        else:
            _condition_fields(history, {"reason": _CONDITION_TEXT})
    _condition_fields(value["anchor"], {
        "type": _CONDITION_TEXT, "ts": _CONDITION_TEXT, "ref_price": _CONDITION_MONEY,
        "price_observed_at": _CONDITION_NULL_TEXT, "evidence": {"type": ["object", "null"]},
        "price_reason": _CONDITION_NULL_TEXT, "price_source": _CONDITION_NULL_TEXT,
    })
    _condition_fields(value["outcome_btc_reference"], {
        "instrument_id": _CONDITION_NULL_TEXT, "entry_ref_price": _CONDITION_MONEY,
        "observed_at": _CONDITION_NULL_TEXT, "price_source": _CONDITION_NULL_TEXT, "reason": _CONDITION_NULL_TEXT,
    })
    _condition_fields(value["beta_flags"], dict.fromkeys(("btc", "eth"), _CONDITION_NULL_TEXT), exact=True)
    _condition_fields(value["pre_event_runup"], {
        "window": _CONDITION_TEXT, "start": _CONDITION_TEXT, "cutoff": _CONDITION_TEXT,
        "ar": _CONDITION_NUMBER, "pctile": _CONDITION_NUMBER, "flag": _CONDITION_BOOL,
    })
    _condition_fields(value["context_inputs"], {
        **dict.fromkeys(("btc_return_24h", "rv24", "rv24_resolution_s", "return_lower_quartile"), _CONDITION_NUMBER),
        "rv24_observed_cutoff": _CONDITION_NULL_TEXT, "rule": _CONDITION_TEXT,
        **dict.fromkeys(("return_normal_range", "rv_normal_range"), {"type": ["array", "null"], "items": {"type": "number"}}),
    })
    for pack in value["reference_coverage"].values():
        _condition_fields(pack, {
            "complete": {"type": "boolean"}, "expected_duration_s": {"type": "number"},
            "boundary_segments": _CONDITION_ROWS, "excluded": _CONDITION_ROWS,
            "volume_unit": _CONDITION_TEXT, "covered_volume_native": _CONDITION_MONEY,
        })
        for segment in pack["boundary_segments"]:
            _condition_fields(segment, {
                "start": _CONDITION_TEXT, "cutoff": _CONDITION_TEXT,
                "trade_count": {"type": "integer"}, "aggregates": _CONDITION_ROWS,
            })
        for row in pack["excluded"]:
            _condition_fields(row, {
                "venue": _CONDITION_TEXT, "reason": _CONDITION_TEXT, "detail": _CONDITION_TEXT,
                "source_reasons": _CONDITION_STRINGS, "missing_metrics": _CONDITION_STRINGS,
            }, required=["venue", "reason", "detail"])


def _condition_confidence(value, *, minimal=False):
    _condition_fields(value, {"class": {"enum": ["LOW", "MEDIUM", "HIGH"]}})
    if minimal:
        if value != {"class": "LOW"}:
            raise ValueError("native Market Truth conditions returned invalid minimal confidence")
        return
    _condition_fields(value, {
        "failed": _CONDITION_ROWS, "limiting": {"type": ["object", "null"]},
        "freshness": _CONDITION_ROWS, "derivatives_native_coverage_ready": _CONDITION_BOOL,
        "derivatives_classification_ready": _CONDITION_BOOL,
    })
    stream_fields = {
        "freshness_basis": {"enum": ["LAST_EVENT", "STREAM_WATERMARK"]},
        "coverage_status": _CONDITION_NULL_TEXT,
        "detail": {"enum": [None, "STREAM_CONNECTION_UNPROVEN"]},
    }
    freshness_fields = {
        "collection": _CONDITION_TEXT,
        **dict.fromkeys(("instrument_id", "metric_key", "kind", "chain"), _CONDITION_NULL_TEXT),
        "cadence_s": _CONDITION_NUMBER, "cadence_source": _CONDITION_TEXT, "age_s": _CONDITION_NUMBER,
        "observed_at": _CONDITION_NULL_TEXT, "observed_at_prec": _CONDITION_NULL_TEXT,
        "observed_at_prec_reason": _CONDITION_NULL_TEXT,
    }
    required_collections = {"aggressor": "flow_bars", "depth": "books", "price": "klines", "oi": "oi", "funding": "funding"}
    failures = [*value["failed"], *([value["limiting"]] if value["limiting"] is not None else [])]
    for row in failures:
        _condition_fields(row, {"condition": {"enum": ["spot_coverage", "derivatives_coverage", "freshness"]}})
        if row["condition"] != "freshness":
            _condition_fields(row, {
                "condition": {"enum": ["spot_coverage", "derivatives_coverage"]}, "reason": _CONDITION_TEXT,
            }, exact=True)
        elif "metric_family" in row:
            _condition_fields(row, {"metric_family": {"enum": list(required_collections)}})
            _condition_fields(row, {
                "condition": {"enum": ["freshness"]}, "reason": _CONDITION_TEXT,
                "collection": {"enum": [required_collections[row["metric_family"]]]},
                "instrument_id": _CONDITION_NULL_TEXT, "metric_family": {"enum": list(required_collections)},
                "cadence_s": _CONDITION_NUMBER, "cadence_source": _CONDITION_TEXT,
            }, exact=True)
        else:
            properties = {
                "condition": {"enum": ["freshness"]}, "reason": _CONDITION_TEXT,
                **freshness_fields,
                **(stream_fields if row.get("collection") == "liquidations" else {}),
            }
            _condition_fields(row, {
                **properties, "observed_at_bucket_anchor": _CONDITION_NULL_TEXT,
            }, required=list(properties), exact=True)
            _condition_timestamp(row, "observed_at")
    for row in value["freshness"]:
        _condition_fields(row, {
            "collection": _CONDITION_TEXT, "cadence_s": _CONDITION_NUMBER,
            "cadence_source": _CONDITION_TEXT, "age_s": _CONDITION_NUMBER,
            "fresh": {"type": "boolean"}, "reason": _CONDITION_NULL_TEXT,
            **dict.fromkeys(("instrument_id", "metric_key", "kind", "chain"), _CONDITION_NULL_TEXT),
            "observed_at": _CONDITION_NULL_TEXT, "observed_at_prec": _CONDITION_NULL_TEXT,
            "observed_at_prec_reason": _CONDITION_NULL_TEXT, "observed_at_bucket_anchor": _CONDITION_NULL_TEXT,
            **(stream_fields if row.get("collection") == "liquidations" else {}),
        }, required=["collection", "cadence_s", "cadence_source", "age_s", "fresh", "reason", "instrument_id", "metric_key", "kind", "chain", "observed_at", "observed_at_prec", "observed_at_prec_reason", *(stream_fields if row.get("collection") == "liquidations" else [])], exact=True)
        _condition_timestamp(row, "observed_at")
    if "analysis_window" in value:
        _condition_fields(value["analysis_window"], dict.fromkeys(("window", "start", "cutoff"), _CONDITION_TEXT))


def _condition_supply(value):
    _condition_fields(value, {
        "holders_revenue": _CONDITION_OBJECT, "exchange_inventory": _CONDITION_OBJECT,
        "flows": _CONDITION_ROWS, "unlocks_next_14d": _CONDITION_ROWS,
        "lending": _CONDITION_OBJECT, "stablecoin_chain": _CONDITION_OBJECT,
        "chain_contexts": _CONDITION_ROWS, "unavailable": _CONDITION_OBJECT,
        "supply_in_evidence": {"enum": ["TRUE", "FALSE", "UNKNOWN"]},
    })
    _condition_fields(value["holders_revenue"], {"status": _CONDITION_TEXT, "protocols": _CONDITION_ROWS, "reason": _CONDITION_NULL_TEXT})
    revenue = value["holders_revenue"]
    if "availability" in revenue:
        _condition_fields(revenue, {
            "availability": _CONDITION_TEXT, "coverage": _CONDITION_OBJECT, "as_of": _CONDITION_NULL_TEXT,
            "capture_received_at": {}, "capture_availability": _CONDITION_TEXT,
            "age_s": _CONDITION_NUMBER, "cadence_s": _CONDITION_NUMBER, "cadence_basis": _CONDITION_TEXT,
            "identity_evidence": {}, "source_observation_sha256": _CONDITION_TEXT,
            "inference_limits": _CONDITION_STRINGS,
        })
        for period in ("24h", "30d"):
            _condition_fields(revenue["coverage"], {period: _CONDITION_OBJECT})
            _condition_fields(revenue["coverage"][period], {
                "observed": {"type": "integer"}, "expected": {"type": "integer"}, "complete": {"type": "boolean"},
            })
    for protocol in revenue["protocols"]:
        _condition_fields(protocol, {
            "protocol": _CONDITION_TEXT, "provider": _CONDITION_TEXT, "unit": _CONDITION_TEXT,
            "source_url": _CONDITION_NULL_TEXT, "periods": _CONDITION_ROWS,
            "chart_available": {"type": "boolean"}, "latest_chart_point": {"type": ["object", "null"]},
            "reason": _CONDITION_NULL_TEXT,
        })
        for period in protocol["periods"]:
            _condition_fields(period, {"provider_period": _CONDITION_TEXT, "value": _CONDITION_MONEY, "status": _CONDITION_TEXT})
        if protocol["latest_chart_point"] is not None:
            _condition_fields(protocol["latest_chart_point"], {"provider_timestamp": _CONDITION_TEXT, "value": _CONDITION_MONEY, "provider_period": _CONDITION_TEXT})
    for name in ("exchange_inventory", "lending", "stablecoin_chain"):
        pack = value[name]
        if "value" in pack:
            _condition_fields(pack, {
                "value": {"type": "null"}, "unit": _CONDITION_NULL_TEXT, "window": _CONDITION_NULL_TEXT,
                "observed_at": _CONDITION_NULL_TEXT, "precision": _CONDITION_NULL_TEXT,
                "coverage": {"type": ["object", "null"]}, "reason": _CONDITION_TEXT,
            })
        elif "kind" in pack or "type" in pack:
            _condition_fields(pack, {"kind" if "kind" in pack else "type": _CONDITION_TEXT})
        elif name == "exchange_inventory":
            _condition_fields(pack, {
                "chg_native_24h": _CONDITION_MONEY, "chg_pct": _CONDITION_NUMBER,
                "normal_range": {"type": ["array", "null"], "items": {"type": "number"}},
                "per_venue": _CONDITION_ROWS, "coverage": _CONDITION_OBJECT,
                "reason": _CONDITION_NULL_TEXT, "observed_at": _CONDITION_NULL_TEXT, "window": _CONDITION_TEXT,
            })
            for row in pack["per_venue"]:
                _condition_fields(row, {"venue": _CONDITION_NULL_TEXT, "chainid": {}, "contract": _CONDITION_NULL_TEXT,
                    "qty_native": _CONDITION_MONEY, "chg_native_24h": _CONDITION_MONEY, "chg_pct": _CONDITION_NUMBER})
        elif name == "stablecoin_chain":
            _condition_fields(pack, {"chain": _CONDITION_NULL_TEXT, "supply_usd": _CONDITION_MONEY,
                "supply_chg_24h": _CONDITION_MONEY, "supply_chg_7d": _CONDITION_MONEY, "reason": _CONDITION_NULL_TEXT})
        else:
            raise ValueError("native Market Truth conditions returned invalid lending data")
    for pack in value["chain_contexts"]:
        if "kind" not in pack and "type" not in pack:
            _condition_fields(pack, {"chain": _CONDITION_NULL_TEXT, "supply_usd": _CONDITION_MONEY,
                "supply_chg_24h": _CONDITION_MONEY, "supply_chg_7d": _CONDITION_MONEY, "reason": _CONDITION_NULL_TEXT})
    for flow in value["flows"]:
        _condition_fields(flow, {"class": _CONDITION_TEXT})
    for unlock in value["unlocks_next_14d"]:
        _condition_fields(unlock, {"inference": _CONDITION_TEXT, "recipient_class": _CONDITION_NULL_TEXT,
            "transferable": _CONDITION_BOOL})
    if "pools" in value["lending"]:
        _condition_fields(value["lending"], {"pools": _CONDITION_ROWS})
        for pool in value["lending"]["pools"]:
            _condition_fields(pool, {"borrow_apr": {"type": "null"}, "borrow_apr_reason": _CONDITION_TEXT,
                "chg_24h": _CONDITION_NUMBER})
    if any(not isinstance(reason, str) and reason is not None for reason in value["unavailable"].values()):
        raise ValueError("native Market Truth conditions returned invalid supply availability")


def _condition_liquidity(value):
    _condition_fields(value, {
        "per_venue": _CONDITION_ROWS, "aggregate_depth_p1_usd": _CONDITION_MONEY,
        "pre_event_depth_p1_usd": _CONDITION_MONEY, "spread_ratio": _CONDITION_NUMBER,
        "replenishment": _CONDITION_OBJECT, "elasticity": _CONDITION_OBJECT,
        "vol_compression": _CONDITION_OBJECT, "liquidity_state": _CONDITION_NULL_TEXT,
        "depth_measurement_band_pct": {"type": "number"},
        "aggregate_bid_depth_p1_usd": _CONDITION_MONEY, "aggregate_ask_depth_p1_usd": _CONDITION_MONEY,
        "pre_event_depth_window": _CONDITION_OBJECT, "pre_event_side_depth_p1_usd": _CONDITION_OBJECT,
        "depth_drop_by_side": _CONDITION_OBJECT, "depth_drop": _CONDITION_NUMBER,
        "depth_majority_lost": _CONDITION_BOOL, "depth_drop_cohort": _CONDITION_OBJECT,
        "best_single_venue_slippage_bps": _CONDITION_OBJECT, "split_slippage_bps": _CONDITION_OBJECT,
        "fragmentation": _CONDITION_OBJECT,
    }, exact=True)
    for key, schema in (("pre_event_side_depth_p1_usd", _CONDITION_MONEY), ("depth_drop_by_side", _CONDITION_NUMBER), ("depth_drop_cohort", _CONDITION_STRINGS)):
        _condition_fields(value[key], dict.fromkeys(("bid", "ask"), schema), exact=True)
    for key in ("best_single_venue_slippage_bps", "split_slippage_bps"):
        _condition_fields(value[key], dict.fromkeys(("T1", "T2", "T3"), _CONDITION_NUMBER), exact=True)
    pre_window = value["pre_event_depth_window"]
    _condition_fields(pre_window, {
        "start": _CONDITION_TEXT if "anchor_type" in pre_window else {"type": "null"},
        "cutoff": _CONDITION_TEXT if "anchor_type" in pre_window else {"type": "null"},
        **({"anchor_type": {"enum": ["EVENT"]}} if "anchor_type" in pre_window else {"reason": {"enum": ["INCOMPLETE"]}}),
    }, exact=True)
    _condition_time(pre_window["start"], nullable=True)
    _condition_time(pre_window["cutoff"], nullable=True)
    _condition_fields(value["replenishment"], {
        "bursts": {"type": ["integer", "null"]}, "replenished": {"type": ["integer", "null"]},
        "median_recovery_s": _CONDITION_NUMBER, "observations": _CONDITION_ROWS,
        "available": {"type": "boolean"}, "resilient_bursts": {"type": ["integer", "null"]},
    }, exact=True)
    for row in value["replenishment"]["observations"]:
        _condition_fields(row, {"venue": _CONDITION_TEXT, "ts": _CONDITION_TEXT,
            "replenished": _CONDITION_BOOL, "recovery_s": _CONDITION_NUMBER, "resilient": _CONDITION_BOOL}, exact=True)
        _condition_time(row["ts"])
    for row in value["per_venue"]:
        _condition_fields(row, {
            "venue": _CONDITION_TEXT, "coverage": _CONDITION_OBJECT, "observed_at": _CONDITION_NULL_TEXT,
            "precision": _CONDITION_NULL_TEXT, "spread_bps": _CONDITION_NUMBER,
            "depth_p1_usd": _CONDITION_MONEY, "bid_depth_p1_usd": _CONDITION_MONEY, "ask_depth_p1_usd": _CONDITION_MONEY,
            "range_covered_pct": {"type": ["object", "number", "string", "null"]},
            "tier_notional": _CONDITION_OBJECT, "slippage_bps": _CONDITION_OBJECT,
        }, exact=True)
        _condition_time(row["observed_at"], nullable=True)
        _condition_fields(row["coverage"], {"venue": _CONDITION_TEXT,
            "range_covered_pct": {"type": ["object", "number", "string", "null"]}, "integrity_ok": {"type": "boolean"}}, exact=True)
        _condition_fields(row["tier_notional"], dict.fromkeys(("T1", "T2", "T3"), _CONDITION_MONEY), exact=True)
        _condition_fields(row["slippage_bps"], dict.fromkeys(("T1", "T2", "T3"), _CONDITION_NUMBER), exact=True)
    _condition_fields(value["elasticity"], {
        "bps_per_depth_unit": _CONDITION_NUMBER, "weak_response": _CONDITION_BOOL,
        "excess_response": _CONDITION_BOOL, "hit_side": _CONDITION_NULL_TEXT,
        "net_flow": _CONDITION_MONEY, "depth_unit": _CONDITION_NULL_TEXT,
        **dict.fromkeys(("hit_side_depth_lower_bound", "pre_event_hit_side_depth_lower_bound", "hit_side_depth_p1_usd",
            "pre_event_hit_side_depth_p1_usd", "avg_hit_side_depth_p1_usd"), _CONDITION_MONEY),
        "hit_side_covered_venues": _CONDITION_STRINGS, "pre_event_hit_side_covered_venues": _CONDITION_STRINGS,
        "hit_side_coverage_complete": _CONDITION_BOOL,
    }, exact=True)
    _condition_fields(value["vol_compression"], {
        "rv24": _CONDITION_NUMBER, "normal_range": {"type": ["array", "null"], "items": {"type": "number"}},
        "state": _CONDITION_NULL_TEXT, "reason": _CONDITION_NULL_TEXT,
        "window": _CONDITION_TEXT, "cutoff": _CONDITION_TEXT,
    }, exact=True)
    _condition_time(value["vol_compression"]["cutoff"])
    if "fragmentation" in value:
        _condition_fields(value["fragmentation"], {
            **dict.fromkeys(("persistent_gap", "validated", "price_path_validated", "funding_path_validated",
                "funding_beyond_normal", "top_sign_flips", "gap_closed_quickly"), _CONDITION_BOOL),
            "funding_normal_range_8h": {"type": ["array", "null"], "items": _CONDITION_MONEY},
            "venues": {"type": "array", "items": _CONDITION_STRINGS},
            "cross_quote_not_evaluated": {"type": "array", "items": _CONDITION_STRINGS},
            "quote_conversion_uncertainty_bps": _CONDITION_MONEY, "complete_venues": _CONDITION_STRINGS,
            "reason": _CONDITION_NULL_TEXT,
            "window": _CONDITION_TEXT, "cutoff": _CONDITION_TEXT,
        }, exact=True)
        _condition_time(value["fragmentation"]["cutoff"])
        for key in ("venues", "cross_quote_not_evaluated"):
            if any(len(pair) != 2 for pair in value["fragmentation"][key]):
                raise ValueError("native Market Truth conditions returned invalid liquidity venue pairs")


def _condition_window_map(value):
    if not isinstance(value, dict) or not value.keys() <= _CONDITION_WINDOWS or any(not isinstance(pack, dict) for pack in value.values()):
        raise ValueError("native Market Truth conditions returned invalid window packs")


def _condition_state(value):
    minimal = {"move_class": "UNKNOWN", "divergence": {"state": "UNKNOWN"}, "data_confidence": {"class": "LOW"}}
    if value == minimal:
        return
    _condition_fields(value, {
        "spot": _CONDITION_OBJECT, "derivatives": _CONDITION_OBJECT, "liquidity": _CONDITION_OBJECT,
        "supply": _CONDITION_OBJECT, "relative": _CONDITION_OBJECT, "data_confidence": _CONDITION_OBJECT,
        "primary_window": {"enum": ["W4H", "EVENT"]}, "move_class": _CONDITION_TEXT,
        "divergence": _CONDITION_OBJECT, "support": {"type": "integer"},
        "eligible_before_divergence": _CONDITION_STRINGS, "eligible_support_before_divergence": _CONDITION_OBJECT,
        "alternatives": _CONDITION_STRINGS, "class_confidence": _CONDITION_TEXT, "flags": _CONDITION_STRINGS,
    })
    _condition_window_map(value["spot"])
    if set(value["spot"]) != _CONDITION_WINDOWS:
        raise ValueError("native Market Truth conditions returned incomplete state spot windows")
    for pack in value["spot"].values():
        _condition_spot(pack)
    _condition_fields(value["derivatives"], {"windows": _CONDITION_OBJECT})
    _condition_window_map(value["derivatives"]["windows"])
    if value["primary_window"] not in value["derivatives"]["windows"]:
        raise ValueError("native Market Truth conditions returned missing primary derivatives")
    for window, pack in value["derivatives"]["windows"].items():
        _condition_derivative(pack, raw=window == value["primary_window"])
    _condition_liquidity(value["liquidity"])
    _condition_supply(value["supply"])
    _condition_relative(value["relative"])
    _condition_confidence(value["data_confidence"])
    _condition_fields(value["divergence"], {
        "state": _CONDITION_TEXT, "triggered": {"type": "boolean"}, "candidates": _CONDITION_ROWS,
        "unresolved": _CONDITION_STRINGS, "confirmed": {"type": "boolean"},
    })
    if any(type(support) is not int for support in value["eligible_support_before_divergence"].values()):
        raise ValueError("native Market Truth conditions returned invalid support scores")
    for candidate in value["divergence"]["candidates"]:
        _condition_fields(candidate, {
            "state": _CONDITION_TEXT, "eligible": {"type": "boolean"}, "refuted": {"type": "boolean"},
            "testable": {"type": "boolean"}, "support": {"type": "integer"},
        })


def _condition_domain(value, domain):
    if domain == "spot":
        _condition_spot(value)
    elif domain in {"oi", "funding", "derivatives"}:
        _condition_derivative(value, raw=True)
    elif domain == "relative":
        _condition_relative(value)
    elif domain == "state":
        _condition_state(value)
    elif domain == "supply":
        _condition_supply(value)
    elif domain == "dc":
        _condition_confidence(value)
        _condition_fields(value, {"analysis_window": _CONDITION_OBJECT, "derivatives_classification_ready_reason": _CONDITION_TEXT})
    elif domain == "close":
        _condition_fields(value, {"ref_venue": _CONDITION_NULL_TEXT, "close_1h_price": _CONDITION_MONEY})
        _condition_timestamp(value, "close_1h_observed_at")
    elif domain == "depth":
        _condition_fields(value, {
            "aggregate_depth_p1_usd": _CONDITION_MONEY, "pre_event_depth_p1_usd": _CONDITION_MONEY,
            "spread_ratio": _CONDITION_NUMBER, "observations": _CONDITION_ROWS,
        }, exact=True)
        _condition_observations(value["observations"], depth=True)
    else:
        raise ValueError("native Market Truth conditions returned an invalid raw domain")


def _validate_conditions(result, snapshot, requirements):
    from tank.analysis import truth_sql_conditions
    from tank.util import parse_time

    _condition_fields(result, {
        "cutoff": _CONDITION_TEXT, "spot": _CONDITION_OBJECT, "derivatives": _CONDITION_OBJECT,
        "relative": _CONDITION_OBJECT, "_condition_evaluation": _CONDITION_OBJECT,
        "cutoff_prec": _CONDITION_NULL_TEXT, "cutoff_prec_reason": _CONDITION_NULL_TEXT,
        "cutoff_bucket_anchor": _CONDITION_NULL_TEXT, "liquidity": _CONDITION_OBJECT,
        "supply": _CONDITION_OBJECT, "data_confidence": _CONDITION_OBJECT,
        "primary_window": {"enum": ["EVENT", "W4H"]}, "move_class": _CONDITION_TEXT,
        "divergence": _CONDITION_OBJECT, "support": {"type": "integer"},
        "eligible_before_divergence": _CONDITION_STRINGS, "eligible_support_before_divergence": _CONDITION_OBJECT,
        "alternatives": _CONDITION_STRINGS, "class_confidence": _CONDITION_TEXT, "flags": _CONDITION_STRINGS,
    }, required=["cutoff", "spot", "derivatives", "relative", "_condition_evaluation"], exact=True)
    _condition_timestamp(result, "cutoff")
    _condition_window_map(result["spot"])
    for pack in result["spot"].values():
        _condition_spot(pack)
    _condition_fields(result["derivatives"], {"windows": _CONDITION_OBJECT})
    _condition_window_map(result["derivatives"]["windows"])
    for pack in result["derivatives"]["windows"].values():
        _condition_derivative(pack, raw=False)
    evaluation = result["_condition_evaluation"]
    _condition_fields(evaluation, {
        "requires_market_truth": {"enum": [[]]}, "unavailable_metrics": _CONDITION_ROWS,
        "request_key": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
        "proofs": _CONDITION_ROWS, "transient": {"enum": [True], "type": "boolean"},
    }, exact=True)
    requested = truth_sql_conditions._ordered_requirements(requirements)
    known = truth_sql_conditions.SPOT_METRICS | truth_sql_conditions.OI_METRICS | truth_sql_conditions.STATE_METRICS | truth_sql_conditions.RELATIVE_METRICS | {
        "deriv.funding_vs_normal", "price.close_vs_level", "dc.class", "liq.depth_p1_vs_pre", "liq.spread_ratio",
    }
    anchor = snapshot.get("anchor") or {}
    anchor = anchor.get("ts") if isinstance(anchor, dict) else anchor
    event_supported = bool(anchor) and parse_time(anchor) < parse_time(snapshot["cutoff"])
    supported = [(metric, window) for metric, window in requested if metric in known and window in _CONDITION_WINDOWS and (window != "EVENT" or event_supported)]
    minimal_state = result.get("data_confidence") == {"class": "LOW"} and any(metric in truth_sql_conditions.STATE_METRICS for metric, _ in supported)
    proof_pairs = []
    for proof in evaluation["proofs"]:
        _condition_fields(proof, {
            "metric": _CONDITION_TEXT, "window": _CONDITION_TEXT,
            "dependency_fingerprint": _CONDITION_TEXT, "reused": {"type": "boolean"},
            "reason": {"enum": [None, "INCOMPLETE"]}, "observations": _CONDITION_ROWS,
            "available": {"type": "boolean"}, "coverage": {"type": ["object", "null"]},
        }, exact=True)
        metric, window = proof["metric"], proof["window"]
        proof_pairs.append((metric, window))
        depth = metric in {"liq.depth_p1_vs_pre", "liq.spread_ratio"}
        if depth or metric in truth_sql_conditions.OI_METRICS or metric == "deriv.funding_vs_normal":
            _condition_observations(proof["observations"], depth=depth)
        elif proof["observations"]:
            raise ValueError("native Market Truth conditions returned unexpected observations")
        if metric in truth_sql_conditions.SPOT_METRICS:
            _condition_spot_coverage(proof["coverage"])
            if window not in result["spot"]:
                raise ValueError("native Market Truth conditions omitted a requested spot window")
        elif metric in truth_sql_conditions.OI_METRICS:
            _condition_fields(proof["coverage"], {"venues": _CONDITION_STRINGS}, exact=True)
        elif proof["coverage"] is not None:
            raise ValueError("native Market Truth conditions returned unexpected coverage")
        if (
            (metric in truth_sql_conditions.OI_METRICS or metric == "deriv.funding_vs_normal")
            and window not in result["derivatives"]["windows"]
        ):
            raise ValueError("native Market Truth conditions omitted a requested derivative window")
        if not proof["available"] and proof["reason"] != "INCOMPLETE":
            raise ValueError("native Market Truth conditions omitted an unavailable proof reason")
        if minimal_state and metric in truth_sql_conditions.STATE_METRICS and (not proof["available"] or proof["reason"] is not None):
            raise ValueError("native Market Truth conditions returned invalid minimal state availability")
    if proof_pairs != supported:
        raise ValueError("native Market Truth conditions omitted or duplicated requested proofs")
    pending_pairs = []
    for pending in evaluation["unavailable_metrics"]:
        _condition_fields(pending, {
            "metric": _CONDITION_TEXT, "window": _CONDITION_TEXT,
            "reason": {"enum": ["NOT_SUPPORTED", "INCOMPLETE"]},
        }, exact=True)
        pending_pairs.append((pending["metric"], pending["window"]))
    incomplete = {(proof["metric"], proof["window"]) for proof in evaluation["proofs"] if not proof["available"]}
    expected_pending = [pair for pair in requested if pair not in supported or pair in incomplete]
    if pending_pairs != expected_pending or any(row["reason"] != ("INCOMPLETE" if pair in supported else "NOT_SUPPORTED") for pair, row in zip(pending_pairs, evaluation["unavailable_metrics"], strict=True)):
        raise ValueError("native Market Truth conditions returned invalid pending coverage")
    metrics = {metric for metric, _ in supported}
    if not metrics & truth_sql_conditions.STATE_METRICS or minimal_state:
        spot_windows = {window for metric, window in supported if metric in truth_sql_conditions.SPOT_METRICS}
        derivative_windows = {window for metric, window in supported if metric in truth_sql_conditions.OI_METRICS or metric == "deriv.funding_vs_normal"}
        if set(result["spot"]) != spot_windows or set(result["derivatives"]["windows"]) != derivative_windows:
            raise ValueError("native Market Truth conditions returned unexpected domain windows")
        state_fields = {"primary_window", "move_class", "divergence", "support", "eligible_before_divergence", "eligible_support_before_divergence", "alternatives", "class_confidence", "flags"}
        if minimal_state:
            state_fields -= {"move_class", "divergence"}
        if "supply" in result or state_fields & result.keys():
            raise ValueError("native Market Truth conditions returned unexpected state fields")
        if "data_confidence" in result and "dc.class" not in metrics and not minimal_state:
            raise ValueError("native Market Truth conditions returned unexpected confidence")
        if "liquidity" in result and not metrics & {"liq.depth_p1_vs_pre", "liq.spread_ratio"}:
            raise ValueError("native Market Truth conditions returned unexpected liquidity")
        if result["relative"] and not metrics & (truth_sql_conditions.RELATIVE_METRICS | {"price.close_vs_level"}):
            raise ValueError("native Market Truth conditions returned unexpected relative prices")
    if metrics & truth_sql_conditions.RELATIVE_METRICS:
        _condition_relative(result["relative"])
    elif "price.close_vs_level" in metrics:
        _condition_domain(result["relative"], "close")
    if "dc.class" in metrics and not metrics & truth_sql_conditions.STATE_METRICS:
        _condition_domain(result.get("data_confidence"), "dc")
    if metrics & {"liq.depth_p1_vs_pre", "liq.spread_ratio"}:
        _condition_fields(result.get("liquidity"), {
            "aggregate_depth_p1_usd": _CONDITION_MONEY, "pre_event_depth_p1_usd": _CONDITION_MONEY,
            "spread_ratio": _CONDITION_NUMBER,
        })
    if metrics & truth_sql_conditions.STATE_METRICS:
        if minimal_state:
            _condition_fields(result, {"move_class": {"enum": ["UNKNOWN"]}, "divergence": _CONDITION_OBJECT})
            if result["divergence"] != {"state": "UNKNOWN"}:
                raise ValueError("native Market Truth conditions returned invalid minimal state")
            _condition_confidence(result["data_confidence"], minimal=True)
        else:
            _condition_state(result)


def _validate_condition_cache(records):
    if not isinstance(records, list) or len(records) > 128:
        raise ValueError("native Market Truth condition cache exceeded its bounded contract")
    replacement = {}
    for record in records:
        if not isinstance(record, dict) or set(record) != {"key", "value"}:
            raise ValueError("native Market Truth condition cache returned invalid records")
        key, value = record["key"], record["value"]
        if not isinstance(key, str) or not isinstance(value, dict) or key in replacement:
            raise ValueError("native Market Truth condition cache returned invalid keys or values")
        parts = json.loads(key)
        if (
            not isinstance(parts, list) or len(parts) != 7
            or any(not isinstance(parts[index], str) for index in (0, 1, 3, 4, 5, 6))
            or any(not re.fullmatch(r"[0-9a-f]{64}", parts[index]) for index in (5, 6))
        ):
            raise ValueError("native Market Truth condition cache returned an invalid native key")
        _condition_time(parts[3])
        domain = parts[0]
        if domain == "condition-state-node":
            if parts[1] not in {"spot", "relative", "derivatives", "supply"}:
                raise ValueError("native Market Truth condition cache returned an invalid node key")
            _condition_time(parts[2])
            if parts[1] == "spot":
                _condition_fields(value, {
                    "spots": _CONDITION_OBJECT, "windows": _CONDITION_OBJECT,
                    "primary": {"enum": ["EVENT", "W4H"]}, "instruments": _CONDITION_ROWS,
                }, exact=True)
                _condition_window_map(value["spots"])
                if set(value["spots"]) != _CONDITION_WINDOWS or set(value["windows"]) != _CONDITION_WINDOWS:
                    raise ValueError("native Market Truth condition cache returned incomplete spot graph windows")
                for pack in value["spots"].values():
                    _condition_spot(pack)
                if any(type(seconds) not in (int, float) or not math.isfinite(seconds) for seconds in value["windows"].values()):
                    raise ValueError("native Market Truth condition cache returned invalid graph durations")
            else:
                _condition_domain(value, parts[1])
        else:
            if (
                domain not in {"spot", "oi", "funding", "close", "dc", "depth", "state", "relative"}
                or parts[1] not in _CONDITION_WINDOWS
                or type(parts[2]) not in (int, float) or not math.isfinite(parts[2]) or parts[2] <= 0
            ):
                raise ValueError("native Market Truth condition cache returned an invalid domain key")
            _condition_domain(value, domain)
        replacement[key] = value
    return replacement


def decode_decimals(value, field=None):
    if field in {"aggregates", "identity_evidence"}:
        return value
    if field == "exchange_inventory" and isinstance(value, dict) and ("kind" in value or "type" in value):
        return value
    if isinstance(value, dict):
        return {key: decode_decimals(item, key) for key, item in value.items()}
    if isinstance(value, list):
        return [decode_decimals(item, field) for item in value]
    if isinstance(value, str) and field in _DECIMAL_FIELDS and _DECIMAL_TEXT.fullmatch(value):
        return Decimal(value)
    return value


async def evaluate(client, snapshot, config=None):
    if client is None or not callable(getattr(client, "rows", None)):
        raise RuntimeError("Market Truth requires the repository ClickHouse native provider")
    statement, parameters = _query(snapshot, config, _provenance_select)
    rows = await client.rows(statement, parameters)
    if len(rows) != 1 or set(rows[0]) != {"result", "provenance_snapshot"}:
        raise ValueError("native Market Truth requires one complete result row")
    result = json.loads(rows[0]["result"])
    provenance = json.loads(rows[0]["provenance_snapshot"])
    required = {"spot", "relative", "derivatives", "liquidity", "supply", "data_confidence", "move_class", "cutoff", "primary_window", "measurement_context"}
    if not isinstance(result, dict) or not required <= result.keys() or not isinstance(provenance, dict):
        raise ValueError("native Market Truth returned an incomplete business result")
    if not all(isinstance(result[name], dict) for name in (
        "spot", "relative", "derivatives", "liquidity", "supply", "data_confidence", "measurement_context",
    )) or not {"W15M", "W1H", "W4H", "W24H", "EVENT"} <= result["spot"].keys():
        raise ValueError("native Market Truth returned incomplete domain shapes")
    from tank.analysis.truth import measurement_metadata, timestamp_companions

    result = timestamp_companions(decode_decimals(result), provenance)
    result["measurement_metadata"] = measurement_metadata(result)
    return jsonable(result)
