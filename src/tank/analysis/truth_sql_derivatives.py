from tank.clickhouse_market import parameter_string
from tank.util import canonical_json

DECIMAL_SCALE = 24
INPUT_INTEGER_DIGITS = 24
INPUT_FRACTION_DIGITS = 24
NATIVE_INTEGER_DIGITS = 52
INTERMEDIATE_INTEGER_DIGITS = 48
ROUNDING_POLICY = "TRUNCATE_TOWARD_ZERO_AT_EACH_MULTIPLICATION_AND_DIVISION"


def _json(value):
    return f"toJSONString({value})"


def _money(value):
    return f"deriv_money_json({value})"


def _object(**fields):
    parts = []
    for name, expression in fields.items():
        parts.extend([repr(("," if parts else "") + '"' + name + '":'), expression])
    return "concat('{', " + ", ".join(parts) + ", '}')"


def _array(expression):
    return f"concat('[', arrayStringConcat({expression}, ','), ']')"


def ctes():
    decomposition = _object(
        position=_money("deriv_position"),
        markup=_money("deriv_markup"),
        interaction=_money("deriv_interaction"),
        pos_share=_json("deriv_position_share"),
    )
    incomplete_decomposition = _object(
        position="'null'",
        markup="'null'",
        interaction="'null'",
        pos_share="'null'",
        reason="'\"INCOMPLETE\"'",
        detail="'\"SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE\"'",
    )
    missing_stats = _object(
        oi_class="'null'",
        oi_native_start="'null'",
        oi_native_end="'null'",
        oi_native_chg_pct="'null'",
        oi_usd_start="'null'",
        oi_usd_end="'null'",
        oi_usd_chg_pct="'null'",
        reason="'\"INCOMPLETE\"'",
        detail=_json("if(deriv_unresolved, 'OI_BUCKET_EDGE_UNSPECIFIED', NULL)"),
    )
    stats = _object(
        requested_window_duration_s=_json("(deriv_end - deriv_start) / 1000000."),
        observed_window_duration_s=_json("(deriv_z.3 - deriv_a.3) / 1000000."),
        native_oi_vs_normal=_json("deriv_band(deriv_dn, deriv_nr)"),
        precision=_json("deriv_z.7"),
        oi_native_start=_money("deriv_a.4"),
        oi_native_end=_money("deriv_z.4"),
        oi_native_chg_pct=_json("deriv_dn"),
        oi_usd_start=_money("deriv_a.5"),
        oi_usd_end=_money("deriv_z.5"),
        oi_usd_chg_pct=_json("deriv_du"),
        oi_usd_reason=_json("if(deriv_usd_ready, NULL, 'INCOMPLETE')"),
        oi_usd_detail=_json("if(deriv_usd_ready, NULL, 'SYNCHRONIZED_MARK_OR_QUOTE_RATE_UNAVAILABLE')"),
        decomposition=f"if(deriv_decomp_ready, {decomposition}, {incomplete_decomposition})",
        oi_class=_json("deriv_classification"),
        classification_ready=_json("toBool(isNotNull(deriv_classification))"),
        native_normal_range="deriv_range_json(deriv_nr)",
        usd_normal_range="deriv_range_json(deriv_ur)",
        native_beyond=_json("deriv_beyond(deriv_dn, deriv_nr)"),
        usd_beyond=_json("deriv_beyond(deriv_du, deriv_ur)"),
        start_observed_at=_json("deriv_iso(deriv_a.3)"),
        end_observed_at=_json("deriv_iso(deriv_z.3)"),
    )
    funding = _object(
        funding_settled_last=_money("deriv_dec(deriv_last, 'rate')"),
        funding_settled_interval_h=_money("deriv_dec(deriv_last, 'interval_h')"),
        funding_settled_last_observed_at=_json("deriv_iso(deriv_time(deriv_last))"),
        funding_estimate_observed_at=_json("deriv_iso(deriv_time(deriv_estimate))"),
        funding_estimate=_money("deriv_dec(deriv_estimate, 'rate')"),
        funding_estimate_interval_h=_money("deriv_dec(deriv_estimate, 'interval_h')"),
        interval_h=_money(
            "if(isNotNull(deriv_estimate_eq), deriv_dec(deriv_estimate, 'interval_h'), "
            "deriv_dec(deriv_last, 'interval_h'))"
        ),
        funding_8h_eq=_money("coalesce(deriv_estimate_eq, deriv_settled_eq)"),
        funding_8h_kind=_json("if(isNotNull(deriv_estimate_eq), 'CURRENT_ESTIMATE', 'SETTLED')"),
        funding_settled_8h_eq=_money("deriv_settled_eq"),
        funding_estimate_8h_eq=_money("deriv_estimate_eq"),
        funding_vs_normal=_json(
            "deriv_decimal_band(coalesce(deriv_estimate_eq, deriv_settled_eq), deriv_fr)"
        ),
        funding_start=_money("deriv_dec(deriv_start_fund, 'rate')"),
        funding_start_interval_h=_money("deriv_dec(deriv_start_fund, 'interval_h')"),
        funding_start_observed_at=_json("deriv_iso(deriv_time(deriv_start_fund))"),
    )
    liquidation = _object(
        liq_long_usd=_money("deriv_liq_side(deriv_liqs, 'LONG_LIQ', 'notional_usd', deriv_observed)"),
        liq_long_usd_reason=_json(
            "if(isNull(deriv_liq_side(deriv_liqs, 'LONG_LIQ', 'notional_usd', deriv_observed)), "
            "'INCOMPLETE', NULL)"
        ),
        liq_long_count=_json(
            "length(arrayFilter(deriv_l -> deriv_get(deriv_l, 'side') = 'LONG_LIQ', deriv_liqs))"
        ),
        liq_long_native=_money("deriv_liq_side(deriv_liqs, 'LONG_LIQ', 'qty_native', deriv_observed)"),
        liq_short_usd=_money("deriv_liq_side(deriv_liqs, 'SHORT_LIQ', 'notional_usd', deriv_observed)"),
        liq_short_usd_reason=_json(
            "if(isNull(deriv_liq_side(deriv_liqs, 'SHORT_LIQ', 'notional_usd', deriv_observed)), "
            "'INCOMPLETE', NULL)"
        ),
        liq_short_count=_json(
            "length(arrayFilter(deriv_l -> deriv_get(deriv_l, 'side') = 'SHORT_LIQ', deriv_liqs))"
        ),
        liq_short_native=_money("deriv_liq_side(deriv_liqs, 'SHORT_LIQ', 'qty_native', deriv_observed)"),
        liq_coverage=_json("deriv_cov"),
        liq_source_coverage=_json("deriv_source_cov"),
        liq_capture_window=_object(
            start="deriv_raw_or_null(deriv_liq_meta, 'coverage_start', deriv_iid)",
            observed_through="deriv_raw_or_null(deriv_liq_meta, 'observed_through', deriv_iid)",
            complete=_json("toBool(deriv_liq_complete)"),
            reason=_json("if(deriv_liq_complete, NULL, 'INCOMPLETE')"),
        ),
    )
    exclusions = _object(
        oi_temporal_exclusions=_object(
            count=_json("length(deriv_excluded)"),
            reason="'\"INCOMPLETE\"'",
            detail="'\"OI_BUCKET_EDGE_UNSPECIFIED\"'",
            latest_observation="deriv_exclusion_json(arrayElement(deriv_excluded, -1))",
        )
    )
    exclusion_observation = _object(
        ts=_json("deriv_iso(deriv_row.3)"),
        ts_prec=_json("deriv_row.7"),
        ts_prec_reason=_json(
            "if(isNotNull(deriv_row.7), NULL, "
            "coalesce(deriv_get(deriv_row.1, 'ts_prec_reason'), "
            "deriv_get(deriv_row.1, 'ts_precision_reason'), "
            "deriv_get(deriv_row.1, 'timestamp_precision_reason'), "
            "deriv_get(deriv_row.1, 'precision_reason'), "
            "deriv_get(JSONExtractRaw(deriv_row.1, 'ts'), 'precision_reason'), 'INCOMPLETE'))"
        ),
        ts_bucket_anchor=_json("deriv_row.8"),
        oi_native=_money("deriv_row.4"),
        oi_usd=_money("deriv_row.5"),
        scope="'\"SUPPLEMENTAL_PROVIDER_OBSERVATION_NOT_WINDOW_ENDPOINT\"'",
    )
    per_venue = _object(
        venue=_json("deriv_venue(deriv_instrument)"),
        instrument_id=_json("deriv_iid"),
        coverage=_object(venue=_json("deriv_venue(deriv_instrument)")),
    )
    totals = _object(
        complete_venues_total="deriv_liq_total(deriv_per, 'COMPLETE', 0)",
        sampled_venues_min="deriv_liq_total(deriv_per, 'SAMPLED', 0)",
        complete_venues_total_reasons="deriv_liq_total(deriv_per, 'COMPLETE', 1)",
        sampled_venues_min_reasons="deriv_liq_total(deriv_per, 'SAMPLED', 1)",
    )
    total_sides = _object(
        long="deriv_liq_total_side(deriv_eligible, 'liq_long_usd', deriv_reasons)",
        short="deriv_liq_total_side(deriv_eligible, 'liq_short_usd', deriv_reasons)",
    )
    coverage = _object(
        instruments=_json("arraySort(deriv_ids)"),
        complete=_json("toBool(deriv_complete)"),
        unit="'\"USD\"'",
        reason=_json("if(deriv_complete, NULL, 'INCOMPLETE')"),
    )
    increments = _object(
        spot=_json("toFloat64(deriv_div(deriv_spot.1, deriv_spot.1 + deriv_perp.1))"),
        perp=_json("toFloat64(deriv_div(deriv_perp.1, deriv_spot.1 + deriv_perp.1))"),
        increment_usd=_object(spot=_money("deriv_spot.1"), perp=_money("deriv_perp.1")),
    )
    domain = _object(
        per_venue=_array("deriv_per"),
        synchronized="deriv_sync_json(deriv_cohort, deriv_start, deriv_end)",
        funding_dispersion_8h=_money(
            "if(length(deriv_eqs) > 1, arrayMax(deriv_eqs) - arrayMin(deriv_eqs), deriv_null_decimal)"
        ),
        liquidations=totals,
        incremental_share=(
            f"if(isNotNull(deriv_spot.1) AND isNotNull(deriv_perp.1) "
            f"AND deriv_spot.1 + deriv_perp.1 != 0, {increments}, 'null')"
        ),
        incremental_volume_coverage=_object(spot="deriv_spot.2", perp="deriv_perp.2"),
        lead_lag='\'{"first_mover":"UNKNOWN","spot_confirmed":null,"sequence":[]}\'',
    )
    return f"""
    {{deriv_snapshot:String}} AS deriv_snapshot,
    {{deriv_config:String}} AS deriv_config,
    toDecimal256(0, 24) AS deriv_zero,
    toDecimal256(1, 24) AS deriv_one,
    toDecimal256(concat('1', repeat('0', 48)), 24) AS deriv_financial_limit,
    CAST(NULL, 'Nullable(Decimal256(24))') AS deriv_null_decimal,
    (deriv_value) -> if(isNull(deriv_value), 'null', toJSONString(toString(deriv_checked_decimal(deriv_value)))) AS deriv_money_json,
    (deriv_raw) -> if(JSONType(deriv_raw) = 'Object',
        coalesce(nullIf(JSONExtractRaw(deriv_raw, 'value'), ''), JSONExtractRaw(deriv_raw, 'ts')),
        deriv_raw) AS deriv_unwrap,
    (deriv_raw) -> if(deriv_raw IN ('', 'null'), NULL,
        if(JSONType(deriv_raw) = 'String', JSONExtractString(deriv_raw), deriv_raw)) AS deriv_text,
    (deriv_raw, deriv_key) -> deriv_text(deriv_unwrap(JSONExtractRaw(deriv_raw, deriv_key))) AS deriv_get,
    (deriv_raw, deriv_key, deriv_id) -> if(JSONExtractRaw(deriv_raw, deriv_key, deriv_id) IN ('', 'null'),
        'null', JSONExtractRaw(deriv_raw, deriv_key, deriv_id)) AS deriv_raw_or_null,
    (deriv_text_value) -> if(positionCaseInsensitive(ifNull(deriv_text_value, ''), 'e') = 0,
        deriv_text_value,
        if(match(ifNull(deriv_text_value, ''), '^[+-]?[0-9]+([.][0-9]+)?[eE][+-]?[0-9]+$'),
            deriv_expand_scientific(splitByChar('e', lowerASCII(ifNull(deriv_text_value, '')))),
            'INVALID_DECIMAL')) AS deriv_plain,
    (deriv_parts) -> deriv_expand_parts(deriv_parts[1], toInt32OrNull(deriv_parts[2])) AS deriv_expand_scientific,
    (deriv_mantissa, deriv_exponent) -> deriv_expand_digits(
        replaceRegexpAll(deriv_mantissa, '[+.-]', ''),
        position(deriv_mantissa, '-') = 1,
        length(replaceRegexpAll(splitByChar('.', deriv_mantissa)[1], '[+-]', ''))
            + ifNull(deriv_exponent, 1000)) AS deriv_expand_parts,
    (deriv_digits, deriv_negative, deriv_point) -> if(abs(deriv_point) > 100,
        'INVALID_DECIMAL', concat(if(deriv_negative, '-', ''),
        multiIf(deriv_point <= 0,
            concat('0.', repeat('0', toUInt64(greatest(0, least(100, -deriv_point)))), deriv_digits),
            deriv_point >= length(deriv_digits),
            concat(deriv_digits, repeat('0', toUInt64(greatest(0, least(100, deriv_point - length(deriv_digits)))))),
            concat(substring(deriv_digits, 1, toUInt64(greatest(1, deriv_point))), '.',
                substring(deriv_digits, toUInt64(greatest(1, deriv_point + 1))))))) AS deriv_expand_digits,
    (deriv_value) -> if(isNull(deriv_value), deriv_null_decimal,
        toDecimal256(if(throwIf(isNotNull(deriv_value) AND NOT match(ifNull(deriv_plain(deriv_value), ''),
            '^[+-]?0*[0-9]{{1,24}}([.][0-9]{{1,24}})?$'),
            'Market Truth decimal input outside native financial range') = 0,
            ifNull(deriv_plain(deriv_value), '0'), '0'), 24)) AS deriv_parse_decimal,
    (deriv_raw, deriv_key) -> deriv_parse_decimal(deriv_get(deriv_raw, deriv_key)) AS deriv_dec,
    (deriv_value) -> if(throwIf(ifNull(abs(deriv_value) >= deriv_financial_limit, 0),
        'Market Truth financial intermediate outside admitted range') = 0,
        deriv_value, deriv_null_decimal) AS deriv_checked_decimal,
    (deriv_x, deriv_y) -> deriv_checked_decimal(multiplyDecimal(deriv_x, deriv_y, 24)) AS deriv_mul,
    (deriv_x, deriv_y) -> if(isNull(deriv_x) OR isNull(deriv_y) OR deriv_y = 0,
        deriv_null_decimal,
        deriv_checked_decimal(divideDecimal(ifNull(deriv_x, deriv_zero),
            ifNull(nullIf(deriv_y, deriv_zero), deriv_one), 24))) AS deriv_div,
    (deriv_values) -> if(empty(deriv_values) OR arrayExists(deriv_v -> isNull(deriv_v), deriv_values),
        deriv_null_decimal, deriv_safe_sum(deriv_values)) AS deriv_sum,
    (deriv_values) -> if(throwIf(ifNull(arrayMax(arrayMap(deriv_v -> abs(deriv_v), deriv_values)), deriv_zero)
        >= divideDecimal(deriv_financial_limit, toDecimal256(greatest(toUInt64(1), length(deriv_values)), 24), 24),
        'Market Truth aggregate outside admitted financial population range') = 0,
        deriv_checked_decimal(arraySum(deriv_values)), deriv_null_decimal) AS deriv_safe_sum,
    (deriv_value) -> toUnixTimestamp64Micro(parseDateTime64BestEffortOrNull(deriv_value, 6, 'UTC')) AS deriv_parse_time,
    (deriv_raw) -> deriv_parse_time(coalesce(
        deriv_get(deriv_raw, 'minute_start'), deriv_get(deriv_raw, 'open_time'),
        deriv_get(deriv_raw, 'exchange_ts'), deriv_get(deriv_raw, 'ts'), deriv_get(deriv_raw, 'as_of'),
        deriv_get(deriv_raw, 'settlement_time'), deriv_get(deriv_raw, 'funding_time'),
        deriv_get(deriv_raw, 'observed_at'))) AS deriv_time,
    (deriv_time_value) -> if(isNull(deriv_time_value), NULL,
        concat(formatDateTime(fromUnixTimestamp64Micro(ifNull(deriv_time_value, 0), 'UTC'), '%Y-%m-%dT%H:%i:%S'),
        if(modulo(ifNull(deriv_time_value, 0), 1000000) = 0, '',
            concat('.', leftPad(toString(modulo(ifNull(deriv_time_value, 0), 1000000)), 6, '0'))), 'Z')) AS deriv_iso,
    (deriv_raw) -> coalesce(deriv_get(deriv_raw, 'venue'),
        splitByChar(':', ifNull(deriv_get(deriv_raw, 'instrument_id'), ''))[1]) AS deriv_venue,
    (deriv_raw) -> coalesce(deriv_get(deriv_raw, 'market_type'),
        if(position(ifNull(deriv_get(deriv_raw, 'instrument_id'), ''), ':spot:') > 0,
            'SPOT', 'PERP_LINEAR')) AS deriv_market,
    (deriv_raw) -> coalesce(deriv_get(deriv_raw, 'token_id'), deriv_get(deriv_snapshot, 'token_id'), '')
        = ifNull(deriv_get(deriv_snapshot, 'token_id'), '') AS deriv_target,
    (deriv_name) -> JSONExtractArrayRaw(deriv_snapshot, deriv_name) AS deriv_collection,
    (deriv_name, deriv_default) -> coalesce(deriv_get(deriv_config, deriv_name), deriv_default) AS deriv_parameter,
    toFloat64(deriv_parameter('oi_sync_tolerance_s', '60')) * 1000000 AS deriv_tolerance,
    toFloat64(deriv_parameter('history_min_days', '7')) * 86400000000 AS deriv_min_history,
    toFloat64(deriv_parameter('historical_lookback_days', deriv_parameter('base_retention_days', '90')))
        * 86400000000 AS deriv_lookback,
    JSONExtractArrayRaw(deriv_config, 'normal_range_quantiles') AS deriv_quantiles,
    if(empty(deriv_quantiles), 0.025, toFloat64(deriv_text(deriv_quantiles[1]))) AS deriv_q_low,
    if(empty(deriv_quantiles), 0.975, toFloat64(deriv_text(deriv_quantiles[2]))) AS deriv_q_high,
    (deriv_raw) -> coalesce(deriv_get(deriv_raw, 'ts_prec'), deriv_get(deriv_raw, 'ts_precision'),
        deriv_get(deriv_raw, 'timestamp_precision'), deriv_get(deriv_raw, 'precision'),
        deriv_get(JSONExtractRaw(deriv_raw, 'ts'), 'precision'),
        deriv_get(JSONExtractRaw(deriv_raw, 'ts'), 'prec')) AS deriv_precision_raw,
    (deriv_value, deriv_bucket_size) -> multiIf(deriv_value IN ('UNKNOWN', ''), NULL,
        deriv_value IN ('HOUR', 'BUCKET_1H'), 'BUCKET(1h)',
        deriv_value IN ('DAY', 'BUCKET_1D'), 'BUCKET(1d)',
        deriv_value = 'BUCKET_1M', 'BUCKET(1m)',
        deriv_value = 'BUCKET', if(isNull(deriv_bucket_size), NULL, concat('BUCKET(', deriv_bucket_size, 's)')),
        deriv_value) AS deriv_normalize_precision,
    (deriv_raw) -> arrayMap(deriv_v -> deriv_normalize_precision(deriv_v, deriv_get(deriv_raw, 'ts_bucket_size_s')),
        arrayFilter(deriv_v -> isNotNull(deriv_v), [deriv_get(JSONExtractRaw(deriv_raw, 'ts'), 'precision'),
            deriv_get(JSONExtractRaw(deriv_raw, 'ts'), 'prec'), deriv_get(deriv_raw, 'ts_prec'),
            deriv_get(deriv_raw, 'ts_precision'), deriv_get(deriv_raw, 'timestamp_precision'),
            deriv_get(deriv_raw, 'precision')])) AS deriv_precision_declarations,
    (deriv_value) -> toFloat64OrZero(extract(ifNull(deriv_value, ''), '^BUCKET[(]([0-9]+(?:[.][0-9]+)?)'))
        * multiIf(endsWith(ifNull(deriv_value, ''), 'us)'), 0.000001,
            endsWith(ifNull(deriv_value, ''), 'ms)'), 0.001,
            endsWith(ifNull(deriv_value, ''), 's)'), 1,
            endsWith(ifNull(deriv_value, ''), 'm)'), 60,
            endsWith(ifNull(deriv_value, ''), 'h)'), 3600,
            endsWith(ifNull(deriv_value, ''), 'd)'), 86400,
            endsWith(ifNull(deriv_value, ''), 'w)'), 604800, 0) AS deriv_bucket_seconds,
    (deriv_value) -> if(startsWith(ifNull(deriv_value, ''), 'BUCKET('),
        toString(deriv_bucket_seconds(deriv_value)), ifNull(deriv_value, 'UNKNOWN')) AS deriv_precision_signature,
    (deriv_raw) -> deriv_validate_precision(deriv_precision_declarations(deriv_raw),
        deriv_get(deriv_raw, 'ts_bucket_size_s')) AS deriv_precision,
    (deriv_values, deriv_bucket_size) -> if(throwIf(arrayExists(deriv_v ->
            deriv_precision_signature(deriv_v) != deriv_precision_signature(deriv_values[1])
            OR (isNotNull(deriv_v) AND deriv_v NOT IN ('EXACT', 'COLLECTION_ONLY')
                AND (NOT match(ifNull(deriv_v, ''), '^BUCKET[(][0-9]+([.][0-9]+)?(us|ms|s|m|h|d|w)[)]$')
                    OR deriv_bucket_seconds(deriv_v) <= 0))
            OR (isNotNull(deriv_bucket_size) AND isNotNull(deriv_v)
                AND (deriv_v IN ('EXACT', 'COLLECTION_ONLY')
                    OR deriv_bucket_seconds(deriv_v) != toFloat64OrZero(ifNull(deriv_bucket_size, '0')))), deriv_values),
            'Conflicting or unsupported OI timestamp precision') = 0, deriv_values[1], NULL) AS deriv_validate_precision,
    (deriv_raw) -> if(throwIf((isNotNull(deriv_anchor_value(deriv_raw))
        AND (deriv_anchor_value(deriv_raw) NOT IN ('START', 'END', 'UNSPECIFIED')
            OR NOT startsWith(ifNull(deriv_precision(deriv_raw), ''), 'BUCKET(')))
        OR (isNotNull(deriv_get(deriv_raw, 'ts_bucket_anchor'))
        AND isNotNull(deriv_get(JSONExtractRaw(deriv_raw, 'ts'), 'bucket_anchor'))
        AND deriv_get(deriv_raw, 'ts_bucket_anchor') != deriv_get(JSONExtractRaw(deriv_raw, 'ts'), 'bucket_anchor')),
        'Conflicting OI timestamp bucket anchors') = 0,
        deriv_anchor_value(deriv_raw), NULL) AS deriv_anchor,
    (deriv_raw) -> coalesce(deriv_get(deriv_raw, 'ts_bucket_anchor'),
        deriv_get(JSONExtractRaw(deriv_raw, 'ts'), 'bucket_anchor'),
        if((JSONHas(deriv_raw, 'ts_bucket_anchor') OR JSONHas(deriv_raw, 'ts', 'bucket_anchor'))
            AND startsWith(ifNull(deriv_precision(deriv_raw), ''), 'BUCKET('), 'UNSPECIFIED', NULL)) AS deriv_anchor_value,
    (deriv_raw) -> ifNull(deriv_anchor(deriv_raw) != 'UNSPECIFIED', 1) AS deriv_admissible,
    (deriv_raw, deriv_seq, deriv_usd, deriv_source) -> tuple(deriv_raw, deriv_seq, deriv_time(deriv_raw),
        deriv_dec(deriv_raw, 'oi_native'), deriv_usd,
        if(isNotNull(nullIf(deriv_source, '')), deriv_null_decimal, deriv_dec(deriv_raw, 'oi_usd')),
        deriv_precision(deriv_raw), deriv_anchor(deriv_raw), deriv_source) AS deriv_oi_tuple,
    (deriv_raw) -> arrayElement(arraySort(deriv_m -> tuple(abs(deriv_m.1 - deriv_time(deriv_raw)), deriv_m.1,
        if(deriv_m.1 < deriv_time(deriv_raw), -deriv_m.2, deriv_m.2)),
        arrayMap(deriv_t -> tuple(deriv_time(deriv_t), deriv_dec(deriv_t, 'mark_price')),
            arrayFilter(deriv_t -> deriv_get(deriv_t, 'instrument_id') = deriv_get(deriv_raw, 'instrument_id')
                AND isNotNull(deriv_time(deriv_t)) AND ifNull(deriv_dec(deriv_t, 'mark_price') != 0, 0)
                AND abs(deriv_time(deriv_t) - deriv_time(deriv_raw)) <= deriv_tolerance,
                deriv_collection('tickers')))), 1).2 AS deriv_ticker_mark,
    (deriv_raw, deriv_seq) -> deriv_derive_oi(deriv_raw, deriv_seq,
        coalesce(nullIf(deriv_dec(deriv_raw, 'mark_price'), deriv_zero), deriv_ticker_mark(deriv_raw))) AS deriv_prepare_oi,
    (deriv_raw, deriv_seq, deriv_mark) -> deriv_oi_tuple(deriv_raw, deriv_seq,
        coalesce(deriv_dec(deriv_raw, 'oi_usd'), deriv_mul(deriv_dec(deriv_raw, 'oi_native'), deriv_mark)),
        if(isNotNull(deriv_dec(deriv_raw, 'oi_usd')) OR isNull(deriv_dec(deriv_raw, 'oi_native')) OR isNull(deriv_mark),
            deriv_get(deriv_raw, 'oi_usd_source'),
            if(ifNull(deriv_dec(deriv_raw, 'mark_price') != 0, 0),
                'OI_NATIVE_X_ROW_MARK', 'OI_NATIVE_X_TICKER_MARK'))) AS deriv_derive_oi,
    arrayMap((deriv_raw, deriv_seq) -> deriv_prepare_oi(deriv_raw, deriv_seq),
        deriv_collection('oi'), arrayEnumerate(deriv_collection('oi'))) AS deriv_oi,
    (deriv_rows, deriv_at, deriv_bound, deriv_tol, deriv_mode) -> arrayElement(arraySort(
        deriv_r -> tuple(abs(deriv_r.3 - deriv_at),
            if(deriv_mode = 0, toInt64(deriv_r.2), ifNull(deriv_r.3, 0)),
            if(deriv_mode = 2 AND deriv_r.3 < deriv_at, -toInt64(deriv_r.2), toInt64(deriv_r.2))),
        arrayFilter(deriv_r -> deriv_r.3 <= deriv_bound AND abs(deriv_r.3 - deriv_at) <= deriv_tol,
            deriv_rows)), 1) AS deriv_nearest,
    (deriv_rows, deriv_start) -> arrayMap(deriv_at -> arrayElement(arrayFilter(
        deriv_r -> deriv_r.3 = deriv_at, arrayReverse(deriv_rows)), 1),
        arraySort(arrayDistinct(arrayMap(deriv_r -> deriv_r.3, arrayFilter(deriv_r ->
            deriv_r.3 >= deriv_start - deriv_lookback AND deriv_r.3 < deriv_start
                AND ifNull(deriv_r.8 != 'UNSPECIFIED', 1), deriv_rows))))) AS deriv_history_rows,
    (deriv_rows) -> arraySort(arrayFilter(deriv_gap -> deriv_gap > 0,
        arrayMap((deriv_r, deriv_previous) -> deriv_r.3 - deriv_previous.3,
            arraySlice(deriv_rows, 2), arrayPopBack(deriv_rows)))) AS deriv_history_gaps,
    (deriv_rows) -> greatest(deriv_tolerance,
        if(empty(deriv_history_gaps(deriv_rows)), 0,
            deriv_history_gaps(deriv_rows)[intDiv(length(deriv_history_gaps(deriv_rows)), 2) + 1] / 2.)) AS deriv_pair_tolerance,
    (deriv_rows, deriv_length) -> arrayMap(deriv_r -> tuple(deriv_r,
        deriv_nearest(deriv_rows, deriv_r.3 - deriv_length, 9223372036854775807,
            deriv_pair_tolerance(deriv_rows), 1)), deriv_rows) AS deriv_history_pairs,
    (deriv_pairs, deriv_index, deriv_length) -> arrayMap(deriv_pair -> tuple(deriv_pair.1.3,
        toFloat64(deriv_div(tupleElement(deriv_pair.1, deriv_index),
            tupleElement(deriv_pair.2, deriv_index)) - deriv_one)),
        arrayFilter(deriv_pair -> isNotNull(deriv_pair.2.3)
            AND isNotNull(tupleElement(deriv_pair.1, deriv_index))
            AND ifNull(tupleElement(deriv_pair.2, deriv_index) != 0, 0), deriv_pairs)) AS deriv_history_changes,
    (deriv_points, deriv_q) -> if(empty(deriv_points), CAST(NULL, 'Nullable(Float64)'),
        deriv_sorted_quantile(arraySort(deriv_points), deriv_q)) AS deriv_quantile,
    (deriv_sorted, deriv_q) -> deriv_sorted[toUInt64(floor((length(deriv_sorted) - 1) * deriv_q)) + 1]
        + (deriv_sorted[toUInt64(ceil((length(deriv_sorted) - 1) * deriv_q)) + 1]
            - deriv_sorted[toUInt64(floor((length(deriv_sorted) - 1) * deriv_q)) + 1])
        * ((length(deriv_sorted) - 1) * deriv_q - floor((length(deriv_sorted) - 1) * deriv_q)) AS deriv_sorted_quantile,
    (deriv_changes, deriv_length, deriv_span) -> if(empty(deriv_changes) OR deriv_span < deriv_min_history
        OR deriv_changes[-1].1 - deriv_changes[1].1 + deriv_length < deriv_min_history,
        CAST([], 'Array(Nullable(Float64))'),
        [deriv_quantile(arrayMap(deriv_c -> deriv_c.2, deriv_changes), deriv_q_low),
         deriv_quantile(arrayMap(deriv_c -> deriv_c.2, deriv_changes), deriv_q_high)]) AS deriv_history_range,
    (deriv_values) -> if(empty(deriv_values), 'null', toJSONString(deriv_values)) AS deriv_range_json,
    (deriv_value, deriv_range) -> if(isNull(deriv_value) OR empty(deriv_range), CAST(NULL, 'Nullable(Bool)'),
        toBool(deriv_value < deriv_range[1] OR deriv_value > deriv_range[2])) AS deriv_beyond,
    (deriv_value, deriv_range) -> if(isNull(deriv_value) OR empty(deriv_range), NULL,
        if(deriv_value < deriv_range[1], 'BELOW', if(deriv_value > deriv_range[2], 'ABOVE', 'WITHIN'))) AS deriv_band,
    (deriv_value, deriv_range) -> if(isNull(deriv_value) OR empty(deriv_range), NULL,
        if(deriv_value < deriv_range[1], 'BELOW', if(deriv_value > deriv_range[2], 'ABOVE', 'WITHIN'))) AS deriv_decimal_band,
    (deriv_rows, deriv_start, deriv_end, deriv_histories) -> deriv_stats_endpoints(
        deriv_nearest(arrayFilter(deriv_r -> ifNull(deriv_r.8 != 'UNSPECIFIED', 1), deriv_rows), deriv_start, deriv_end, deriv_tolerance, 0),
        deriv_nearest(arrayFilter(deriv_r -> ifNull(deriv_r.8 != 'UNSPECIFIED', 1), deriv_rows), deriv_end, deriv_end, deriv_tolerance, 0),
        deriv_start, deriv_end, deriv_histories,
        arrayExists(deriv_r -> deriv_r.8 = 'UNSPECIFIED' AND deriv_r.3 <= deriv_end, deriv_rows)) AS deriv_stats,
    (deriv_a, deriv_z, deriv_start, deriv_end, deriv_histories, deriv_unresolved) -> deriv_stats_ranges(
        deriv_a, deriv_z, deriv_start, deriv_end,
        deriv_baseline(deriv_histories, deriv_start, deriv_end - deriv_start, 4, deriv_a.1 = ''),
        deriv_baseline(deriv_histories, deriv_start, deriv_end - deriv_start, 6, deriv_a.1 = ''),
        deriv_unresolved) AS deriv_stats_endpoints,
    (deriv_a, deriv_z, deriv_start, deriv_end, deriv_nr, deriv_ur, deriv_unresolved) ->
        deriv_stats_values(deriv_a, deriv_z, deriv_start, deriv_end, deriv_nr, deriv_ur, deriv_unresolved,
            toFloat64(deriv_div(deriv_z.4, deriv_a.4) - deriv_one),
            if(deriv_a.5 > 0 AND isNotNull(deriv_z.5),
                toFloat64(deriv_div(deriv_z.5, deriv_a.5) - deriv_one), NULL),
            deriv_div(deriv_a.5, deriv_a.4),
            if(deriv_z.4 != 0, deriv_div(deriv_z.5, deriv_z.4),
                if(JSONHas(deriv_z.1, 'mark_price'), deriv_dec(deriv_z.1, 'mark_price'), deriv_dec(deriv_z.1, 'mark')))) AS deriv_stats_ranges,
    (deriv_a, deriv_z, deriv_start, deriv_end, deriv_nr, deriv_ur, deriv_unresolved,
        deriv_dn, deriv_du, deriv_p0, deriv_p1) -> deriv_stats_terms(
        deriv_a, deriv_z, deriv_start, deriv_end, deriv_nr, deriv_ur, deriv_unresolved, deriv_dn, deriv_du,
        deriv_mul(deriv_z.4 - deriv_a.4, deriv_p0),
        deriv_mul(deriv_a.4, deriv_p1 - deriv_p0),
        deriv_mul(deriv_z.4 - deriv_a.4, deriv_p1 - deriv_p0)) AS deriv_stats_values,
    (deriv_a, deriv_z, deriv_start, deriv_end, deriv_nr, deriv_ur, deriv_unresolved, deriv_dn, deriv_du,
        deriv_position, deriv_markup, deriv_interaction) -> deriv_stats_finish(
        deriv_a, deriv_z, deriv_start, deriv_end, deriv_nr, deriv_ur, deriv_unresolved, deriv_dn, deriv_du,
        deriv_position, deriv_markup, deriv_interaction,
        toFloat64(deriv_div(abs(deriv_position), deriv_sum([abs(deriv_position), abs(deriv_markup), abs(deriv_interaction)]))),
        isNotNull(deriv_position) AND isNotNull(deriv_markup) AND isNotNull(deriv_interaction),
        isNotNull(deriv_du),
        if(isNull(deriv_beyond(deriv_dn, deriv_nr)), NULL,
            if(deriv_beyond(deriv_dn, deriv_nr),
                if(isNull(deriv_div(abs(deriv_position), deriv_sum([abs(deriv_position), abs(deriv_markup), abs(deriv_interaction)]))), NULL,
                    if(abs(deriv_position) > deriv_sum([abs(deriv_markup), abs(deriv_interaction)]), 'REAL_NATIVE', 'MIXED')),
                if(isNull(deriv_beyond(deriv_du, deriv_ur)), NULL,
                    if(deriv_beyond(deriv_du, deriv_ur), 'PRICE_MARKUP', 'MIXED'))))) AS deriv_stats_terms,
    (deriv_a, deriv_z, deriv_start, deriv_end, deriv_nr, deriv_ur, deriv_unresolved, deriv_dn, deriv_du,
        deriv_position, deriv_markup, deriv_interaction, deriv_position_share, deriv_decomp_ready,
        deriv_usd_ready, deriv_classification) ->
        if(isNull(deriv_a.3) OR isNull(deriv_z.3) OR isNull(deriv_a.4) OR isNull(deriv_z.4)
            OR deriv_a.4 <= 0 OR deriv_z.4 < 0, {missing_stats}, {stats}) AS deriv_stats_finish,
    (deriv_histories, deriv_start, deriv_length, deriv_index, deriv_composite) ->
        if(length(deriv_histories) = 1 AND NOT deriv_composite,
            deriv_single_baseline(deriv_history_rows(deriv_histories[1], deriv_start), deriv_length, deriv_index),
            deriv_composite_baseline(arrayMap(deriv_rows -> deriv_cohort_history_rows(deriv_rows, deriv_start), deriv_histories),
                deriv_length, deriv_index)) AS deriv_baseline,
    (deriv_rows, deriv_start) -> arraySort(deriv_r -> tuple(deriv_r.3, deriv_r.2),
        arrayFilter(deriv_r -> deriv_r.3 >= deriv_start - deriv_lookback AND deriv_r.3 < deriv_start
            AND ifNull(deriv_r.8 != 'UNSPECIFIED', 1), deriv_rows)) AS deriv_cohort_history_rows,
    (deriv_rows, deriv_length, deriv_index) -> deriv_history_range(
        deriv_history_changes(deriv_history_pairs(deriv_rows, deriv_length), deriv_index, deriv_length),
        deriv_length, if(empty(deriv_rows), 0, deriv_rows[-1].3 - deriv_rows[1].3)) AS deriv_single_baseline,
    (deriv_histories, deriv_length, deriv_index) -> deriv_composite_changes(deriv_histories,
        arraySort(arrayDistinct(arrayFlatten(arrayMap(deriv_rows -> arrayMap(deriv_r -> deriv_r.3, deriv_rows), deriv_histories)))),
        deriv_length, deriv_index) AS deriv_composite_baseline,
    (deriv_histories, deriv_times, deriv_length, deriv_index) -> deriv_composite_range(
        arrayMap(deriv_at -> tuple(deriv_at, deriv_composite_pair(deriv_histories, deriv_at, deriv_length, deriv_index)), deriv_times),
        deriv_length, if(empty(deriv_times), 0, deriv_times[-1] - deriv_times[1])) AS deriv_composite_changes,
    (deriv_points, deriv_length, deriv_span) -> deriv_history_range(
        arrayFilter(deriv_p -> isNotNull(deriv_p.2), deriv_points), deriv_length, deriv_span) AS deriv_composite_range,
    (deriv_histories, deriv_at, deriv_length, deriv_index) -> deriv_composite_pair_sum(
        arrayFilter(deriv_pair -> isNotNull(deriv_pair.1.3) AND isNotNull(deriv_pair.2.3)
            AND deriv_pair.1.2 != deriv_pair.2.2
            AND ifNull(tupleElement(deriv_pair.1, deriv_index) != 0, 0)
            AND isNotNull(tupleElement(deriv_pair.2, deriv_index)),
        arrayMap(deriv_rows -> tuple(
            deriv_nearest(deriv_rows, deriv_at - deriv_length, 9223372036854775807, deriv_pair_tolerance(deriv_rows), 2),
            deriv_nearest(deriv_rows, deriv_at, 9223372036854775807, deriv_pair_tolerance(deriv_rows), 2)), deriv_histories)),
        deriv_index) AS deriv_composite_pair,
    (deriv_pairs, deriv_index) -> if(empty(deriv_pairs), CAST(NULL, 'Nullable(Float64)'),
        if(throwIf(notEmpty(deriv_pairs) AND
            deriv_sum(arrayMap(deriv_p -> tupleElement(deriv_p.1, deriv_index), deriv_pairs)) = 0,
            'Zero synchronized OI history denominator') = 0,
            toFloat64(deriv_div(deriv_sum(arrayMap(deriv_p -> tupleElement(deriv_p.2, deriv_index), deriv_pairs)),
                deriv_sum(arrayMap(deriv_p -> tupleElement(deriv_p.1, deriv_index), deriv_pairs))) - deriv_one), NULL)) AS deriv_composite_pair_sum,
    (deriv_row) -> {exclusion_observation} AS deriv_exclusion_json,
    (deriv_rows) -> arrayElement(arraySort(deriv_f -> tuple(deriv_time(deriv_f.1), deriv_f.2),
        arrayZip(deriv_rows, arrayEnumerate(deriv_rows))), -1).1 AS deriv_latest,
    (deriv_f) -> deriv_div(deriv_mul(deriv_dec(deriv_f, 'rate'), toDecimal256(8, 24)),
        deriv_dec(deriv_f, 'interval_h')) AS deriv_funding_eq,
    (deriv_rows, deriv_start) -> arraySort(deriv_f -> deriv_time(deriv_f), arrayFilter(deriv_f -> deriv_time(deriv_f) < deriv_start
        AND ifNull(deriv_dec(deriv_f, 'interval_h') > 0, 0)
        AND isNotNull(deriv_dec(deriv_f, 'rate')), deriv_rows)) AS deriv_funding_history,
    (deriv_rows) -> if(empty(deriv_rows) OR deriv_time(deriv_rows[-1]) - deriv_time(deriv_rows[1]) < deriv_min_history,
        CAST([], 'Array(Nullable(Decimal256(24)))'),
        deriv_funding_range(arraySort(arrayMap(deriv_f -> coalesce(deriv_dec(deriv_f, 'rate_8h_eq'), deriv_funding_eq(deriv_f)), deriv_rows)))) AS deriv_funding_normal,
    (deriv_values) -> [deriv_funding_quantile(deriv_values, deriv_q_low),
        deriv_funding_quantile(deriv_values, deriv_q_high)] AS deriv_funding_range,
    (deriv_values, deriv_q) -> deriv_values[toUInt64(floor((length(deriv_values) - 1) * deriv_q)) + 1]
        + deriv_mul(deriv_values[toUInt64(ceil((length(deriv_values) - 1) * deriv_q)) + 1]
            - deriv_values[toUInt64(floor((length(deriv_values) - 1) * deriv_q)) + 1],
            toDecimal256(deriv_plain(toString((length(deriv_values) - 1) * deriv_q - floor((length(deriv_values) - 1) * deriv_q))), 24)) AS deriv_funding_quantile,
    (deriv_settled, deriv_estimates, deriv_start) -> deriv_funding_json(
        deriv_latest(deriv_settled), deriv_latest(deriv_estimates),
        deriv_latest(arrayFilter(deriv_f -> deriv_time(deriv_f) <= deriv_start, deriv_settled)),
        deriv_funding_eq(deriv_latest(deriv_settled)), deriv_funding_eq(deriv_latest(deriv_estimates)),
        deriv_funding_normal(deriv_funding_history(deriv_settled, deriv_start))) AS deriv_funding,
    (deriv_last, deriv_estimate, deriv_start_fund, deriv_settled_eq, deriv_estimate_eq, deriv_fr) -> {funding} AS deriv_funding_json,
    JSONExtractRaw(deriv_snapshot, 'capture_metadata', 'collections', 'liquidations') AS deriv_liq_meta,
    (deriv_liqs, deriv_side, deriv_key, deriv_observed) -> if(
        empty(arrayFilter(deriv_l -> deriv_get(deriv_l, 'side') = deriv_side, deriv_liqs)),
        if(deriv_observed, deriv_zero, deriv_null_decimal),
        deriv_sum(arrayMap(deriv_l -> deriv_dec(deriv_l, deriv_key),
            arrayFilter(deriv_l -> deriv_get(deriv_l, 'side') = deriv_side, deriv_liqs)))) AS deriv_liq_side,
    (deriv_instrument, deriv_rows) -> coalesce(nullIf(deriv_get(if(empty(deriv_rows), deriv_instrument, deriv_rows[-1].1), 'liq_coverage'), ''),
        multiIf(deriv_venue(deriv_instrument) = 'bybit', 'COMPLETE',
            deriv_venue(deriv_instrument) IN ('binance', 'okx'), 'SAMPLED', 'NOT_SUPPORTED')) AS deriv_liq_source_cov,
    (deriv_iid, deriv_start, deriv_end) -> ifNull(
        deriv_parse_time(deriv_text(JSONExtractRaw(deriv_liq_meta, 'coverage_start', deriv_iid))) <= deriv_start
        AND deriv_parse_time(deriv_text(JSONExtractRaw(deriv_liq_meta, 'observed_through', deriv_iid))) >= deriv_end, 0) AS deriv_liq_complete,
    (deriv_iid, deriv_liqs, deriv_source_cov, deriv_liq_complete) -> deriv_liq_json(deriv_iid, deriv_liqs,
        deriv_source_cov, if(deriv_source_cov = 'COMPLETE' AND NOT deriv_liq_complete, 'UNKNOWN', deriv_source_cov),
        deriv_liq_complete) AS deriv_liquidation,
    (deriv_iid, deriv_liqs, deriv_source_cov, deriv_cov, deriv_liq_complete) -> deriv_liq_finish(deriv_iid, deriv_liqs,
        deriv_source_cov, deriv_cov, deriv_liq_complete,
        notEmpty(deriv_liqs) OR (deriv_liq_complete AND deriv_cov IN ('COMPLETE', 'SAMPLED'))) AS deriv_liq_json,
    (deriv_iid, deriv_liqs, deriv_source_cov, deriv_cov, deriv_liq_complete, deriv_observed) -> {liquidation} AS deriv_liq_finish,
    (deriv_per, deriv_cov, deriv_reasons) -> deriv_liq_total_json(
        arrayFilter(deriv_p -> deriv_get(deriv_p, 'liq_coverage') = deriv_cov, deriv_per), deriv_reasons) AS deriv_liq_total,
    (deriv_eligible, deriv_reasons) -> {total_sides} AS deriv_liq_total_json,
    (deriv_eligible, deriv_key, deriv_reasons) -> if(deriv_reasons,
        toJSONString(if(isNull(deriv_sum(arrayMap(deriv_p -> deriv_dec_result(deriv_p, deriv_key), deriv_eligible))), 'INCOMPLETE', NULL)),
        deriv_money_json(deriv_sum(arrayMap(deriv_p -> deriv_dec_result(deriv_p, deriv_key), deriv_eligible)))) AS deriv_liq_total_side,
    (deriv_raw, deriv_key) -> toDecimal256OrNull(deriv_get(deriv_raw, deriv_key), 24) AS deriv_dec_result,
    (deriv_left, deriv_right) -> concat(substring(deriv_left, 1, length(deriv_left) - 1),
        if(deriv_right = '{{}}', '', concat(',', substring(deriv_right, 2, length(deriv_right) - 2))), '}}') AS deriv_merge,
    arrayFilter(deriv_r -> deriv_target(deriv_r) AND deriv_market(deriv_r) != 'SPOT'
        AND isNotNull(deriv_get(deriv_r, 'instrument_id')),
        arrayConcat(deriv_collection('instruments'), deriv_collection('oi'),
            deriv_collection('funding'), deriv_collection('liquidations'))) AS deriv_discovery,
    arrayDistinct(arrayMap(deriv_r -> deriv_get(deriv_r, 'instrument_id'), deriv_discovery)) AS deriv_instrument_ids,
    (deriv_iid) -> arrayElement(arrayFilter(deriv_r -> deriv_get(deriv_r, 'instrument_id') = deriv_iid,
        deriv_discovery), 1) AS deriv_instrument,
    (deriv_iid) -> arrayFilter(deriv_r -> deriv_get(deriv_r.1, 'instrument_id') = deriv_iid, deriv_oi) AS deriv_instrument_oi,
    (deriv_iid, deriv_start, deriv_end) -> deriv_per_json(deriv_iid, deriv_instrument(deriv_iid),
        deriv_instrument_oi(deriv_iid), deriv_start, deriv_end) AS deriv_per_venue,
    (deriv_iid, deriv_instrument, deriv_rows, deriv_start, deriv_end) -> deriv_per_parts(deriv_iid, deriv_instrument, deriv_rows,
        deriv_start, deriv_end, arraySort(deriv_r -> tuple(deriv_r.3, -toInt64(deriv_r.2)),
            arrayFilter(deriv_r -> deriv_r.8 = 'UNSPECIFIED' AND deriv_r.3 <= deriv_end, deriv_rows)),
        arrayFilter(deriv_f -> deriv_get(deriv_f, 'instrument_id') = deriv_iid
            AND deriv_time(deriv_f) <= deriv_end, deriv_collection('funding')),
        arrayFilter(deriv_l -> deriv_get(deriv_l, 'instrument_id') = deriv_iid
            AND deriv_time(deriv_l) >= deriv_start AND deriv_time(deriv_l) < deriv_end,
            deriv_collection('liquidations')),
        arrayElement(arraySort(deriv_r -> tuple(-deriv_r.3, deriv_r.2), deriv_rows), 1).1) AS deriv_per_json,
    (deriv_iid, deriv_instrument, deriv_rows, deriv_start, deriv_end, deriv_excluded, deriv_funds, deriv_liqs, deriv_lastoi) ->
        deriv_merge(deriv_merge(deriv_merge(deriv_merge({per_venue},
        deriv_stats(deriv_rows, deriv_start, deriv_end, [deriv_rows])),
        deriv_funding(arrayFilter(deriv_f -> deriv_get(deriv_f, 'kind') = 'SETTLED', deriv_funds),
            arrayFilter(deriv_f -> deriv_get(deriv_f, 'kind') = 'CURRENT_ESTIMATE', deriv_funds), deriv_start)),
        deriv_liquidation(deriv_iid, deriv_liqs, deriv_liq_source_cov(deriv_instrument, deriv_rows),
            deriv_liq_complete(deriv_iid, deriv_start, deriv_end))),
        deriv_merge(if(empty(deriv_excluded), '{{}}', {exclusions}),
            concat('{{\"basis_bps\":', deriv_money_json(deriv_mul(
                deriv_div(if(JSONHas(deriv_lastoi, 'mark_price'), deriv_dec(deriv_lastoi, 'mark_price'), deriv_dec(deriv_lastoi, 'mark')),
                    if(JSONHas(deriv_lastoi, 'index_price'), deriv_dec(deriv_lastoi, 'index_price'), deriv_dec(deriv_lastoi, 'index')))
                - deriv_one, toDecimal256(10000, 24))), '}}'))) AS deriv_per_parts,
    (deriv_row) -> arrayElement(arrayReverse(arrayFilter(deriv_s ->
        deriv_get(deriv_s, 'metric_key') = deriv_get(deriv_row.1, 'metric_key')
            AND deriv_get(deriv_s, 'instrument_id') = deriv_get(deriv_row.1, 'instrument_id'), deriv_collection('semantics'))), 1) AS deriv_specific_semantics,
    (deriv_row) -> if(deriv_specific_semantics(deriv_row) != '', deriv_specific_semantics(deriv_row),
        arrayElement(arrayReverse(arrayFilter(deriv_s ->
            deriv_get(deriv_s, 'metric_key') = deriv_get(deriv_row.1, 'metric_key')
                AND isNull(deriv_get(deriv_s, 'instrument_id')), deriv_collection('semantics'))), 1)) AS deriv_semantics,
    (deriv_iid, deriv_start, deriv_end) -> arrayFilter(deriv_r -> deriv_r.3 >= deriv_start
        AND deriv_r.3 <= deriv_end, deriv_instrument_oi(deriv_iid)) AS deriv_window_semantics_rows,
    (deriv_iid, deriv_start, deriv_end) -> notEmpty(deriv_window_semantics_rows(deriv_iid, deriv_start, deriv_end))
        AND arrayAll(deriv_r -> ifNull(deriv_get(deriv_semantics(deriv_r), 'sidedness') = 'SINGLE_SIDED', 0),
            deriv_window_semantics_rows(deriv_iid, deriv_start, deriv_end)) AS deriv_semantics_ready,
    (deriv_iid, deriv_start, deriv_end) -> deriv_get(deriv_semantics(
        deriv_window_semantics_rows(deriv_iid, deriv_start, deriv_end)[1]), 'aggregation_class') AS deriv_category,
    (deriv_iid, deriv_start, deriv_end) -> deriv_venue(deriv_instrument(deriv_iid)) IN ('binance', 'okx', 'bybit')
        AND deriv_semantics_ready(deriv_iid, deriv_start, deriv_end)
        AND deriv_sync_endpoints(
            deriv_nearest(arrayFilter(deriv_r -> ifNull(deriv_r.8 != 'UNSPECIFIED', 1), deriv_instrument_oi(deriv_iid)),
                deriv_start, deriv_end, deriv_tolerance, 0),
            deriv_nearest(arrayFilter(deriv_r -> ifNull(deriv_r.8 != 'UNSPECIFIED', 1), deriv_instrument_oi(deriv_iid)),
                deriv_end, deriv_end, deriv_tolerance, 0)) AS deriv_sync_eligible,
    (deriv_a, deriv_z) -> ifNull(isNotNull(deriv_a.3) AND isNotNull(deriv_z.3)
        AND deriv_a.4 > 0 AND deriv_z.4 >= 0, 0) AS deriv_sync_endpoints,
    (deriv_cohort, deriv_start, deriv_end) -> if(empty(deriv_cohort)
        OR isNull(deriv_category(deriv_cohort[1], deriv_start, deriv_end))
        OR NOT arrayAll(deriv_iid -> ifNull(deriv_category(deriv_iid, deriv_start, deriv_end)
            = deriv_category(deriv_cohort[1], deriv_start, deriv_end), 0), deriv_cohort), 'null',
        deriv_synchronized(deriv_cohort, deriv_start, deriv_end)) AS deriv_sync_json,
    (deriv_cohort, deriv_start, deriv_end) -> deriv_sync_combined(deriv_cohort, deriv_start, deriv_end,
        arrayMap(deriv_iid -> arrayFilter(deriv_r -> ifNull(deriv_r.8 != 'UNSPECIFIED', 1)
            AND deriv_r.3 <= deriv_end, deriv_instrument_oi(deriv_iid)), deriv_cohort)) AS deriv_synchronized,
    (deriv_cohort, deriv_start, deriv_end, deriv_histories) -> deriv_sync_stats(deriv_cohort, deriv_start, deriv_end,
        deriv_histories, deriv_combined_rows(deriv_histories, deriv_end)) AS deriv_sync_combined,
    (deriv_histories, deriv_end) -> arrayFilter(deriv_r -> isNotNull(deriv_r.4),
        arrayMap((deriv_at, deriv_seq) -> deriv_combine_point(deriv_histories, deriv_at, deriv_seq, deriv_end),
            arraySort(arrayDistinct(arrayFlatten(arrayMap(deriv_rs -> arrayMap(deriv_r -> deriv_r.3, deriv_rs), deriv_histories)))),
            arrayEnumerate(arraySort(arrayDistinct(arrayFlatten(arrayMap(deriv_rs -> arrayMap(deriv_r -> deriv_r.3, deriv_rs), deriv_histories))))))) AS deriv_combined_rows,
    (deriv_histories, deriv_at, deriv_seq, deriv_end) -> deriv_combined_tuple(deriv_at, deriv_seq,
        arrayMap(deriv_rows -> deriv_nearest(deriv_rows, deriv_at, deriv_end, deriv_tolerance, 2), deriv_histories)) AS deriv_combine_point,
    (deriv_at, deriv_seq, deriv_selected) -> tuple('', deriv_seq, deriv_at,
        deriv_sum(arrayMap(deriv_r -> deriv_r.4, deriv_selected)),
        deriv_sum(arrayMap(deriv_r -> deriv_r.5, deriv_selected)),
        deriv_sum(arrayMap(deriv_r -> deriv_r.6, deriv_selected)),
        CAST(NULL, 'Nullable(String)'), CAST(NULL, 'Nullable(String)'), CAST(NULL, 'Nullable(String)')) AS deriv_combined_tuple,
    (deriv_cohort, deriv_start, deriv_end, deriv_histories, deriv_combined) -> deriv_sync_finish(deriv_cohort,
        deriv_stats(deriv_combined, deriv_start, deriv_end, deriv_histories)) AS deriv_sync_stats,
    (deriv_cohort, deriv_stats_json) -> if(isNull(deriv_get(deriv_stats_json, 'oi_native_start')), 'null',
        deriv_merge(deriv_stats_json, concat('{{\"venues\":', toJSONString(arrayMap(deriv_iid ->
            deriv_venue(deriv_instrument(deriv_iid)), deriv_cohort)), '}}'))) AS deriv_sync_finish,
    (deriv_iid, deriv_start, deriv_end) -> arraySort(deriv_r -> deriv_time(deriv_r),
        arrayFilter(deriv_r -> deriv_get(deriv_r, 'instrument_id') = deriv_iid
            AND deriv_time(deriv_r) >= deriv_start AND deriv_time(deriv_r) < deriv_end,
            deriv_collection('klines'))) AS deriv_volume_candles,
    (deriv_rows, deriv_start, deriv_end) -> notEmpty(deriv_rows)
        AND deriv_time(deriv_rows[1]) = deriv_start
        AND deriv_time(deriv_rows[-1]) + toInt64(coalesce(deriv_get(deriv_rows[-1], 'interval_s'), '60')) * 1000000 = deriv_end
        AND arrayAll(deriv_r -> JSONExtractBool(deriv_r, 'complete') AND isNotNull(deriv_dec(deriv_r, 'quote_volume_usd')),
            deriv_rows)
        AND arrayAll((deriv_r, deriv_next) -> deriv_time(deriv_r)
            + toInt64(coalesce(deriv_get(deriv_r, 'interval_s'), '60')) * 1000000 = deriv_time(deriv_next),
            arrayPopBack(deriv_rows), arraySlice(deriv_rows, 2)) AS deriv_candles_complete,
    arrayConcat(arrayReverse(deriv_collection('flow_segments')), arrayFlatten(arrayMap(deriv_bar -> arrayMap(deriv_piece ->
        deriv_merge(deriv_piece, concat('{{\"instrument_id\":', toJSONString(deriv_get(deriv_bar, 'instrument_id')), '}}')),
        JSONExtractArrayRaw(deriv_bar, 'boundary_segments')), deriv_collection('flow_bars')))) AS deriv_segments_raw,
    (deriv_piece) -> tuple(deriv_get(deriv_piece, 'instrument_id'), deriv_get(deriv_piece, 'start'),
        deriv_get(deriv_piece, 'end')) AS deriv_segment_key,
    arrayMap(deriv_key -> arrayFirst(deriv_piece -> deriv_segment_key(deriv_piece) = deriv_key, deriv_segments_raw),
        arrayDistinct(arrayMap(deriv_piece -> deriv_segment_key(deriv_piece), deriv_segments_raw))) AS deriv_segments,
    (deriv_iid, deriv_start, deriv_end) -> arrayFilter(deriv_r ->
        deriv_get(deriv_r, 'instrument_id') = deriv_iid AND JSONExtractBool(deriv_r, 'complete')
        AND deriv_parse_time(deriv_get(deriv_r, 'start')) >= deriv_start
        AND deriv_parse_time(deriv_get(deriv_r, 'end')) <= deriv_end
        AND deriv_parse_time(deriv_get(deriv_r, 'start')) < deriv_parse_time(deriv_get(deriv_r, 'end'))
        AND (isNull(deriv_get(deriv_snapshot, 'assembled_at'))
            OR (ifNull(deriv_parse_time(deriv_get(deriv_r, 'received_ts')) <= deriv_parse_time(deriv_get(deriv_snapshot, 'assembled_at')), 1)
                AND ifNull(deriv_parse_time(deriv_get(deriv_r, 'observed_at')) <= deriv_parse_time(deriv_get(deriv_snapshot, 'assembled_at')), 1))),
        deriv_segments) AS deriv_volume_segments,
    (deriv_iid, deriv_start, deriv_end) -> deriv_segment_fold(
        arraySort(deriv_piece -> tuple(deriv_parse_time(deriv_get(deriv_piece, 'start')),
            -deriv_parse_time(deriv_get(deriv_piece, 'end'))), deriv_volume_segments(deriv_iid, deriv_start, deriv_end)),
        deriv_start, deriv_end) AS deriv_segment_volume,
    (deriv_pieces, deriv_start, deriv_end) -> arrayFold((deriv_acc, deriv_piece) ->
        if(deriv_parse_time(deriv_get(deriv_piece, 'start')) = deriv_acc.1,
            tuple(assumeNotNull(deriv_parse_time(deriv_get(deriv_piece, 'end'))),
                deriv_acc.2 + deriv_dec(deriv_piece, 'buy_notional') + deriv_dec(deriv_piece, 'sell_notional'), deriv_acc.3 + 1),
            deriv_acc), deriv_pieces, tuple(toInt64(deriv_start), CAST(deriv_zero, 'Nullable(Decimal256(24))'), toUInt64(0))) AS deriv_segment_fold,
    (deriv_iid, deriv_start, deriv_end) -> arrayFilter(deriv_r -> deriv_get(deriv_r, 'instrument_id') = deriv_iid
        AND deriv_time(deriv_r) >= deriv_start AND deriv_time(deriv_r) + 60000000 <= deriv_end,
        deriv_collection('flow_bars')) AS deriv_volume_bars,
    (deriv_iid, deriv_start, deriv_end) -> deriv_flow_volume(deriv_iid, deriv_start, deriv_end,
        intDiv(deriv_start + 59999999, 60000000) * 60000000,
        intDiv(deriv_end, 60000000) * 60000000,
        deriv_volume_bars(deriv_iid, deriv_start, deriv_end)) AS deriv_fallback_volume,
    (deriv_iid, deriv_start, deriv_end, deriv_first, deriv_last, deriv_bars) -> deriv_flow_volume_parts(
        deriv_start, deriv_end, deriv_first, deriv_last, deriv_bars,
        deriv_segment_volume(deriv_iid, deriv_start, least(deriv_first, deriv_end)),
        deriv_segment_volume(deriv_iid, greatest(deriv_last, deriv_start), deriv_end)) AS deriv_flow_volume,
    (deriv_start, deriv_end, deriv_first, deriv_last, deriv_bars, deriv_left, deriv_right) -> if(
        length(arrayDistinct(arrayMap(deriv_r -> deriv_time(deriv_r), deriv_bars))) = greatest(0, intDiv(deriv_last - deriv_first, 60000000))
        AND arrayAll(deriv_r -> JSONExtractBool(deriv_r, 'complete'), deriv_bars)
        AND (deriv_first = deriv_start OR deriv_left.1 = least(deriv_first, deriv_end))
        AND (deriv_last = deriv_end OR deriv_last < deriv_first OR deriv_right.1 = deriv_end),
        if(empty(deriv_bars) AND deriv_start = deriv_first AND deriv_end = deriv_last, deriv_null_decimal,
            if(empty(deriv_bars), deriv_zero,
                deriv_sum(arrayMap(deriv_r -> deriv_dec(deriv_r, 'buy_notional'), deriv_bars))
                + deriv_sum(arrayMap(deriv_r -> deriv_dec(deriv_r, 'sell_notional'), deriv_bars)))
            + if(deriv_first = deriv_start, deriv_zero, deriv_left.2)
            + if(deriv_last = deriv_end OR deriv_last < deriv_first, deriv_zero, deriv_right.2)),
        deriv_null_decimal) AS deriv_flow_volume_parts,
    (deriv_iid, deriv_start, deriv_end) -> deriv_observed_volume(deriv_iid, deriv_start, deriv_end,
        deriv_volume_candles(deriv_iid, deriv_start, deriv_end)) AS deriv_volume,
    (deriv_iid, deriv_start, deriv_end, deriv_candles) -> if(deriv_candles_complete(deriv_candles, deriv_start, deriv_end),
        deriv_sum(arrayMap(deriv_r -> deriv_dec(deriv_r, 'quote_volume_usd'), deriv_candles)),
        deriv_fallback_volume(deriv_iid, deriv_start, deriv_end)) AS deriv_observed_volume,
    (deriv_spot) -> arraySort(arrayDistinct(arrayMap(deriv_r -> deriv_get(deriv_r, 'instrument_id'),
        arrayFilter(deriv_r -> deriv_target(deriv_r) AND (deriv_market(deriv_r) = 'SPOT') = deriv_spot,
            arrayConcat(deriv_collection('instruments'), deriv_collection('klines'), deriv_collection('flow_bars')))))) AS deriv_volume_ids,
    (deriv_spot, deriv_start, deriv_end) -> deriv_increment_values(deriv_volume_ids(deriv_spot),
        arrayMap(deriv_iid -> deriv_volume(deriv_iid, deriv_start, deriv_end), deriv_volume_ids(deriv_spot)),
        arrayMap(deriv_iid -> deriv_volume(deriv_iid, deriv_start - (deriv_end - deriv_start), deriv_start), deriv_volume_ids(deriv_spot))) AS deriv_increment,
    (deriv_ids, deriv_current, deriv_previous) -> deriv_increment_json(deriv_ids,
        notEmpty(deriv_ids) AND NOT arrayExists(deriv_v -> isNull(deriv_v), arrayConcat(deriv_current, deriv_previous)),
        deriv_sum(deriv_current), deriv_sum(deriv_previous)) AS deriv_increment_values,
    (deriv_ids, deriv_complete, deriv_current, deriv_previous) -> tuple(
        if(deriv_complete, greatest(deriv_zero, deriv_current - deriv_previous), deriv_null_decimal),
        {coverage}) AS deriv_increment_json,
    (deriv_start, deriv_end) -> deriv_window_parts(deriv_start, deriv_end,
        arrayMap(deriv_iid -> deriv_per_venue(deriv_iid, deriv_start, deriv_end), deriv_instrument_ids),
        arrayFilter(deriv_iid -> deriv_sync_eligible(deriv_iid, deriv_start, deriv_end), deriv_instrument_ids),
        deriv_increment(1, deriv_start, deriv_end), deriv_increment(0, deriv_start, deriv_end)) AS deriv_window,
    (deriv_start, deriv_end, deriv_per, deriv_cohort, deriv_spot, deriv_perp) -> deriv_window_json(
        deriv_start, deriv_end, deriv_per, deriv_cohort, deriv_spot, deriv_perp,
        arrayFilter(deriv_v -> isNotNull(deriv_v), arrayMap(deriv_p -> deriv_dec_result(deriv_p, 'funding_8h_eq'), deriv_per))) AS deriv_window_parts,
    (deriv_start, deriv_end, deriv_per, deriv_cohort, deriv_spot, deriv_perp, deriv_eqs) -> {domain} AS deriv_window_json,
    deriv_time(concat('{{\"ts\":', JSONExtractRaw(deriv_snapshot, 'cutoff'), '}}')) AS deriv_cutoff,
    coalesce(deriv_parse_time(deriv_get(JSONExtractRaw(deriv_snapshot, 'anchor'), 'ts')),
        deriv_parse_time(if(JSONType(deriv_snapshot, 'anchor') != 'Object',
            deriv_text(JSONExtractRaw(deriv_snapshot, 'anchor')), NULL)),
        deriv_cutoff - 14400000000) AS deriv_event_start,
    deriv_output AS (
        SELECT concat('{{', arrayStringConcat(arrayMap(deriv_w -> concat(toJSONString(deriv_w.1), ':',
            deriv_window(deriv_cutoff - deriv_w.2, deriv_cutoff)),
            [('W15M', toInt64(900000000)), ('W1H', toInt64(3600000000)),
             ('W4H', toInt64(14400000000)), ('W24H', toInt64(86400000000)),
             ('EVENT', deriv_cutoff - deriv_event_start)]), ','), '}}') AS deriv_result
    )
    """


def select_sql():
    return "SELECT deriv_result AS result FROM deriv_output"


def query(snapshot, config=None):
    return "WITH " + ctes() + "\n" + select_sql(), {
        "deriv_snapshot": (parameter_string(canonical_json(snapshot)), "String"),
        "deriv_config": (parameter_string(canonical_json(config or {})), "String"),
    }
