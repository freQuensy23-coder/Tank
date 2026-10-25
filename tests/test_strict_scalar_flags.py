from enum import IntEnum
from typing import Annotated, Literal

import pytest
from pydantic import BeforeValidator, JsonValue, TypeAdapter, ValidationError

from tank.analysis.statistics_contracts import UnavailableHistoryDiagnostics
from tank.contracts import DomainModel
from tank.contracts.json import JsonDocument
from tank.contracts.repository import Collection
from tank.contracts.scalars import FalseFlag, OneInt, TrueFlag, ZeroInt, require_integer, require_number


class CapturedAssertion(DomainModel):
    complete: TrueFlag
    exact: FalseFlag


@pytest.mark.parametrize("invalid", [0, 1, 0.0, 1.0, "true", "false", None, [], {}])
def test_fixed_boolean_assertions_refuse_non_boolean_wire_values(invalid: JsonValue) -> None:
    with pytest.raises(ValidationError, match="boolean"):
        CapturedAssertion.model_validate({"complete": invalid, "exact": False})
    with pytest.raises(ValidationError, match="boolean"):
        CapturedAssertion.model_validate({"complete": True, "exact": invalid})


def test_fixed_booleans_preserve_boolean_wire_schema_and_serialization() -> None:
    assertion = CapturedAssertion(complete=True, exact=False)
    assert assertion.model_dump_json() == '{"complete":true,"exact":false}'
    restored = CapturedAssertion.model_validate_json(assertion.model_dump_json(), strict=True)
    assert restored.complete is True and restored.exact is False
    complete = JsonDocument.validate_value(TypeAdapter(TrueFlag).json_schema())
    exact = JsonDocument.validate_value(TypeAdapter(FalseFlag).json_schema())
    assert isinstance(complete, dict) and isinstance(exact, dict)
    assert complete["type"] == "boolean" and complete["const"] is True
    assert exact["type"] == "boolean" and exact["const"] is False
    for complete_value, exact_value in ((False, False), (True, True)):
        with pytest.raises(ValidationError):
            CapturedAssertion.model_validate({"complete": complete_value, "exact": exact_value})
    with pytest.raises(ValidationError):
        CapturedAssertion.model_validate(assertion.model_copy(update={"complete": 1}))


@pytest.mark.parametrize("invalid", [0, 0.0, 1, 1.0, "false", None])
def test_unavailable_history_cannot_admit_numeric_precision_assertion(invalid: JsonValue) -> None:
    fields: dict[str, JsonValue] = {
        "target_window_duration_s": 3600.0,
        "exact": invalid,
        "reason": "INCOMPLETE",
        "resolution_s": None,
        "sample_count": 0,
    }
    with pytest.raises(ValidationError, match="boolean"):
        UnavailableHistoryDiagnostics.model_validate(fields)


def test_unavailable_history_retains_real_false_and_known_zero_sample_count() -> None:
    history = UnavailableHistoryDiagnostics(
        target_window_duration_s=3600.0, exact=False, reason="INCOMPLETE", resolution_s=None, sample_count=0
    )
    assert history.exact is False and history.sample_count == 0


class FixedIntegerProtocol(DomainModel):
    zero: ZeroInt
    one: OneInt


class IntegerProtocolCode(IntEnum):
    ZERO = 0
    ONE = 1


@pytest.mark.parametrize("invalid", [False, True, 0.0, 1.0, "0", "1", None, [], {}])
def test_fixed_integer_protocol_refuses_boolean_or_integral_float_substitution(invalid: JsonValue) -> None:
    with pytest.raises(ValidationError, match="integer"):
        FixedIntegerProtocol.model_validate({"zero": invalid, "one": 1})
    with pytest.raises(ValidationError, match="integer"):
        FixedIntegerProtocol.model_validate({"zero": 0, "one": invalid})


def test_fixed_integer_protocol_preserves_valid_numbers_and_numeric_wire_shape() -> None:
    numbers = FixedIntegerProtocol(zero=0, one=1)
    restored = FixedIntegerProtocol.model_validate_json(numbers.model_dump_json(), strict=True)
    assert type(restored.zero) is int and type(restored.one) is int
    assert restored.model_dump_json() == '{"zero":0,"one":1}'
    codes = FixedIntegerProtocol.model_validate(
        {"zero": IntegerProtocolCode.ZERO, "one": IntegerProtocolCode.ONE}
    )
    assert codes.model_dump_json() == '{"zero":0,"one":1}'
    zero_schema = JsonDocument.validate_value(TypeAdapter(ZeroInt).json_schema())
    one_schema = JsonDocument.validate_value(TypeAdapter(OneInt).json_schema())
    assert isinstance(zero_schema, dict) and isinstance(one_schema, dict)
    assert zero_schema["type"] == "integer" and type(zero_schema["const"]) is int
    assert one_schema["type"] == "integer" and type(one_schema["const"]) is int
    with pytest.raises(ValidationError):
        FixedIntegerProtocol.model_validate({"zero": 1, "one": 1})
    with pytest.raises(ValidationError):
        FixedIntegerProtocol.model_validate({"zero": 0, "one": 0})


@pytest.mark.parametrize("invalid", [False, True, 0.0, 1.0, 1, "0", None])
def test_unavailable_history_requires_an_actual_integer_zero_sample_count(invalid: JsonValue) -> None:
    with pytest.raises(ValidationError):
        UnavailableHistoryDiagnostics.model_validate(
            {
                "target_window_duration_s": 3600.0,
                "exact": False,
                "reason": "INCOMPLETE",
                "resolution_s": None,
                "sample_count": invalid,
            }
        )


class OtherIntegerProtocol(DomainModel):
    format_version: Annotated[Literal[2], BeforeValidator(require_integer)]
    original_http_status: Annotated[Literal[404], BeforeValidator(require_integer)]


def test_other_integer_literals_can_require_integer_type_without_rejecting_valid_protocol_values() -> None:
    value = OtherIntegerProtocol(format_version=2, original_http_status=404)
    assert value.model_dump_json() == '{"format_version":2,"original_http_status":404}'
    with pytest.raises(ValidationError, match="integer"):
        OtherIntegerProtocol.model_validate({"format_version": 2.0, "original_http_status": 404})
    with pytest.raises(ValidationError, match="integer"):
        OtherIntegerProtocol.model_validate({"format_version": 2, "original_http_status": 404.0})


def test_persistence_codec_refuses_copied_models_with_cross_type_fixed_scalars() -> None:
    assertion = CapturedAssertion(complete=True, exact=False)
    numbers = FixedIntegerProtocol(zero=0, one=1)
    with pytest.raises(ValidationError):
        Collection("assertions", CapturedAssertion).encode(assertion.model_copy(update={"complete": 1}))
    with pytest.raises(ValidationError):
        Collection("numbers", FixedIntegerProtocol).encode(numbers.model_copy(update={"zero": False}))


class NumericCoefficientProtocol(DomainModel):
    multiplier: Annotated[Literal[1], BeforeValidator(require_number)]


@pytest.mark.parametrize("value", [1, 1.0])
def test_numeric_constant_contract_preserves_valid_integral_float_representation(value: int | float) -> None:
    coefficient = NumericCoefficientProtocol.model_validate({"multiplier": value})
    assert coefficient.model_dump_json() == '{"multiplier":1}'


@pytest.mark.parametrize("value", [False, True, float("nan"), float("inf"), "1", None])
def test_numeric_constant_contract_still_refuses_boolean_nonfinite_and_text_substitution(
    value: JsonValue,
) -> None:
    with pytest.raises(ValidationError, match="number"):
        NumericCoefficientProtocol.model_validate({"multiplier": value})
