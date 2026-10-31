from decimal import Decimal, localcontext
from importlib.machinery import ExtensionFileLoader

from tank import clickhouse_market, util
from tank.analysis import truth


def test_truth_executes_native_code_without_changing_decimal_representation_or_precision():
    marker = truth._ASSESSMENT_INPUTS.set({})
    try:
        with localcontext() as context:
            context.prec = 128
            first = truth.decimal("3.000")
            context.prec = 9
            second = truth.decimal("3.000")
            assert first.as_tuple() == second.as_tuple() == Decimal("3.000").as_tuple()
            assert context.prec == 40
        assert isinstance(truth.__loader__, ExtensionFileLoader)
    finally:
        truth._ASSESSMENT_INPUTS.reset(marker)


def test_native_wire_utilities_preserve_exact_decimal_text_and_utc_time():
    stamp = "2026-10-19T00:00:00.123456Z"
    assert util.jsonable([Decimal("-0.000"), Decimal("1e-900"), util.parse_time(stamp)]) == [
        "-0.000",
        "1E-900",
        stamp,
    ]
    assert isinstance(util.__loader__, ExtensionFileLoader)


def test_native_column_decoder_preserves_money_text_and_row_independence():
    columns = {
        "instrument_id": ["A:perp:T", "A:perp:T"],
        "ts": ["2026-10-19T00:00:00Z", "2026-10-19T00:01:00Z"],
        "oi_native": ["-0.000", "1e-900"],
        "oi_usd": ["3.000", None],
        "storage_key": ["first", "second"],
    }
    rows = [row[1] for row in clickhouse_market._wide_column_records("mkt.oi_obs", columns, "ts")]
    assert [row["oi_native"] for row in rows] == ["-0.000", "1E-900"]
    assert [row["oi_usd"] for row in rows] == ["3.000", None]
    rows[0]["oi_native"] = "caller-local"
    assert rows[1]["oi_native"] == "1E-900"
    assert isinstance(clickhouse_market.__loader__, ExtensionFileLoader)
