from decimal import Decimal, localcontext
from importlib import util as importlib_util
from pathlib import Path

import pytest

from tank import util


@pytest.fixture(scope="module")
def interpreted_util():
    source = Path(__file__).resolve().parents[1] / "src/tank/util.py"
    spec = importlib_util.spec_from_file_location("market_truth_interpreted_util", source)
    module = importlib_util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "value",
    [0, -0.0, 0.000001, -0.000001, 1e11, 1e14, Decimal("1.000000000000000000001"), "0.000001"],
)
def test_native_timestamp_inference_matches_interpreted_source_and_decimal_flags(interpreted_util, value):
    with localcontext() as context:
        context.prec = 128
        context.clear_flags()
        expected = interpreted_util.parse_time(value)
        flags = dict(context.flags)
        context.clear_flags()
        assert util.parse_time(value) == expected
        assert dict(context.flags) == flags
        assert util.iso(value) == interpreted_util.iso(value)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), 10**100])
def test_native_timestamp_inference_keeps_interpreted_source_errors(interpreted_util, value):
    with pytest.raises((ValueError, OverflowError)) as expected:
        interpreted_util.parse_time(value)
    with pytest.raises(type(expected.value)) as actual:
        util.parse_time(value)
    assert str(actual.value) == str(expected.value)


@pytest.mark.parametrize("entropy", [0, 2**79, 2**80 - 1])
def test_native_identifier_inference_keeps_80_bit_integer_arithmetic(interpreted_util, entropy):
    timestamp = "2026-10-19T00:00:00.123456Z"
    expected = interpreted_util.new_id("CALL", timestamp=timestamp, entropy=entropy)
    assert util.new_id("CALL", timestamp=timestamp, entropy=entropy) == expected


def test_native_json_inference_keeps_canonical_money_text_and_float_bits(interpreted_util):
    value = {
        "money": [Decimal("-0.000"), Decimal("1e-900")],
        "numbers": [2**80 - 1, -0.0, 1e-300, 1e300],
    }
    assert util.canonical_json(value) == interpreted_util.canonical_json(value)
    assert util.sha256(value) == interpreted_util.sha256(value)
