"""Shared tool wire validation enforces declared bounds without coercing JSON types."""

import pytest
from pydantic import JsonValue

from tank.contracts.tool_schema import ToolSchemaError, validate_schema


@pytest.mark.parametrize("value", [True, False, "1"])
def test_numbers_do_not_accept_boolean_or_string_values(value: JsonValue) -> None:
    with pytest.raises(ValueError, match="invalid type"):
        validate_schema(
            {"score": value},
            {
                "type": "object",
                "properties": {"score": {"type": "number"}},
                "required": ["score"],
            },
        )


def test_declared_maximum_array_length_is_enforced() -> None:
    schema: dict[str, JsonValue] = {"type": "array", "items": {"type": "string"}, "maxItems": 2}
    validate_schema(["one", "two"], schema)
    with pytest.raises(ValueError, match="more than 2 items"):
        validate_schema(["one", "two", "three"], schema)


def test_additional_property_schema_is_validated_at_the_actual_key() -> None:
    schema: dict[str, JsonValue] = {
        "type": "object",
        "additionalProperties": {"type": "integer", "minimum": 1},
    }
    validate_schema({"one": 1}, schema)
    with pytest.raises(ValueError, match=r"\$\.invalid: violates minimum"):
        validate_schema({"invalid": 0}, schema)


@pytest.mark.parametrize("number", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_numbers_are_invalid_even_in_unconstrained_properties(number: float) -> None:
    with pytest.raises(ValueError, match="invalid number"):
        validate_schema({"nested": [{"amount": number}]}, {"type": "object"})


def test_boolean_and_numeric_enum_values_are_distinct() -> None:
    with pytest.raises(ValueError, match="invalid enum"):
        validate_schema(True, {"enum": [1]})
    validate_schema(1.0, {"enum": [1]})


@pytest.mark.parametrize("schema", [{"type": None}, {"anyOf": None}, {"properties": None}])
def test_malformed_optional_keywords_do_not_act_as_absent(schema: dict[str, JsonValue]) -> None:
    with pytest.raises(ToolSchemaError):
        validate_schema({}, schema)


def test_nested_union_keeps_required_field_and_unknown_field_diagnostics() -> None:
    schema: dict[str, JsonValue] = {
        "anyOf": [
            {
                "type": "object",
                "required": ["item_id"],
                "properties": {"item_id": {"type": "string"}},
                "additionalProperties": False,
            },
            {
                "type": "object",
                "required": ["event_id"],
                "properties": {"event_id": {"type": "string"}},
                "additionalProperties": False,
            },
        ]
    }
    validate_schema({"item_id": "item"}, schema)
    with pytest.raises(ValueError, match="matches none of the allowed forms"):
        validate_schema({"unregistered": "value"}, schema)
