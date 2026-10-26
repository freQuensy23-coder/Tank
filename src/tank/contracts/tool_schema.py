"""Strict validation of the JSON-schema wire subset declared by Tank tools."""

import math
import re
from typing import Literal

from pydantic import Field, JsonValue

from tank.contracts import DomainModel

type JsonType = Literal["object", "array", "string", "boolean", "integer", "number", "null"]
type Number = int | float


class ToolDefinition(DomainModel):
    name: str = Field(min_length=1)
    description: str
    input_schema: dict[str, JsonValue]


def parse_tool_definitions(value: JsonValue) -> tuple[ToolDefinition, ...]:
    if not isinstance(value, list):
        raise ToolSchemaError("Configured tools must be a JSON array")
    tools = tuple(ToolDefinition.model_validate(row) for row in value)
    names = [tool.name for tool in tools]
    if len(names) != len(set(names)):
        raise ToolSchemaError("Configured tools contain duplicate names")
    return tools


SCOPED_TOOLS = frozenset(
    {
        "add_observation",
        "set_assessment",
        "set_beneficiaries",
        "get_market_truth",
        "get_decision",
        "publish_card",
        "schedule_milestone",
        "set_watch_condition",
    }
)


class ToolSchemaError(ValueError):
    """The configured wire schema is malformed rather than the incoming value."""


def _type_name(value: JsonValue) -> JsonType:
    match value:
        case "object" | "array" | "string" | "boolean" | "integer" | "number" | "null":
            return value
        case _:
            raise ToolSchemaError("Configured tool schema declares an unsupported JSON type")


def _matches(value: JsonValue, kind: JsonType) -> bool:
    match kind:
        case "object":
            return isinstance(value, dict)
        case "array":
            return isinstance(value, list)
        case "string":
            return isinstance(value, str)
        case "boolean":
            return isinstance(value, bool)
        case "integer":
            return isinstance(value, int) and not isinstance(value, bool)
        case "number":
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        case "null":
            return value is None


def _array(schema: dict[str, JsonValue], key: str) -> list[JsonValue] | None:
    if key not in schema:
        return None
    values = schema[key]
    if not isinstance(values, list):
        raise ToolSchemaError(f"Configured tool schema {key} must be an array")
    return values


def _number(schema: dict[str, JsonValue], key: str) -> Number | None:
    if key not in schema:
        return None
    value = schema[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ToolSchemaError(f"Configured tool schema {key} must be numeric")
    if isinstance(value, float) and not math.isfinite(value):
        raise ToolSchemaError(f"Configured tool schema {key} must be finite")
    return value


def _length(schema: dict[str, JsonValue], key: str) -> int | None:
    if key not in schema:
        return None
    value = schema[key]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ToolSchemaError(f"Configured tool schema {key} must be a nonnegative integer")
    return value


def _finite(value: JsonValue, path: str) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{path}: invalid number")
    if isinstance(value, dict):
        for key, child in value.items():
            _finite(child, path + "." + key)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _finite(child, f"{path}[{index}]")


def _equal(left: JsonValue, right: JsonValue) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left == right
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(_equal(value, right[key]) for key, value in left.items())
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(_equal(a, b) for a, b in zip(left, right, strict=True))
    return left == right


def _accepts(value: JsonValue, schema: JsonValue, path: str) -> bool:
    try:
        _validate(value, schema, path)
    except ToolSchemaError:
        raise
    except ValueError:
        return False
    return True


def _validate(value: JsonValue, schema: JsonValue, path: str) -> None:
    if isinstance(schema, bool):
        if not schema:
            raise ValueError(f"{path}: forbidden by schema")
        return
    if not isinstance(schema, dict):
        raise ToolSchemaError("Configured tool schema must be an object or boolean")
    conjunction = _array(schema, "allOf")
    if conjunction is not None and not conjunction:
        raise ToolSchemaError("Configured tool schema allOf must not be empty")
    for keyword in ("if", "then", "else"):
        if keyword in schema and not isinstance(schema[keyword], (bool, dict)):
            raise ToolSchemaError(f"Configured tool schema {keyword} must be an object or boolean")
    if "type" in schema:
        raw = schema["type"]
        if isinstance(raw, str):
            declared = [_type_name(raw)]
        elif isinstance(raw, list) and raw:
            declared = [_type_name(kind) for kind in raw]
        else:
            raise ToolSchemaError("Configured tool schema type must be a name or nonempty array")
        if not any(_matches(value, kind) for kind in declared):
            raise ValueError(f"{path}: invalid type")
    choices = _array(schema, "enum")
    if choices is not None and not any(_equal(value, choice) for choice in choices):
        raise ValueError(f"{path}: invalid enum; allowed: {choices}")
    if "const" in schema and not _equal(value, schema["const"]):
        raise ValueError(f"{path}: does not match the required constant")
    if conjunction is not None:
        matches = sum(_accepts(value, option, path) for option in conjunction)
        if matches != len(conjunction):
            raise ValueError(f"{path}: does not satisfy every required form")
    if "if" in schema:
        selected = "then" if _accepts(value, schema["if"], path) else "else"
        selected_matches = True
        for keyword in ("then", "else"):
            if keyword in schema:
                matches_branch = _accepts(value, schema[keyword], path)
                if keyword == selected:
                    selected_matches = matches_branch
        if not selected_matches:
            raise ValueError(f"{path}: does not satisfy its conditional form")
    if "not" in schema and _accepts(value, schema["not"], path):
        raise ValueError(f"{path}: matches a forbidden form")
    exclusive_forms = _array(schema, "oneOf")
    if exclusive_forms is not None:
        if not exclusive_forms:
            raise ToolSchemaError("Configured tool schema oneOf must not be empty")
        matches = sum(_accepts(value, option, path) for option in exclusive_forms)
        if matches != 1:
            raise ValueError(f"{path}: must match exactly one allowed form")
    alternatives = _array(schema, "anyOf")
    if alternatives is not None:
        if not alternatives:
            raise ToolSchemaError("Configured tool schema anyOf must not be empty")
        errors: list[str] = []
        for option in alternatives:
            try:
                _validate(value, option, path)
                break
            except ToolSchemaError:
                raise
            except ValueError as error:
                errors.append(str(error).removeprefix(path + ": "))
        else:
            raise ValueError(f"{path}: matches none of the allowed forms: " + " | ".join(errors))
    if isinstance(value, str):
        if "pattern" in schema:
            pattern = schema["pattern"]
            if not isinstance(pattern, str):
                raise ToolSchemaError("Configured tool schema pattern must be text")
            try:
                matched = re.search(pattern, value)
            except re.error:
                raise ToolSchemaError("Configured tool schema pattern is invalid") from None
            if matched is None:
                raise ValueError(f"{path}: does not match pattern {pattern}")
        minimum = _length(schema, "minLength")
        maximum = _length(schema, "maxLength")
        if minimum is not None and len(value) < minimum:
            raise ValueError(f"{path}: shorter than {minimum}")
        if maximum is not None and len(value) > maximum:
            raise ValueError(f"{path}: {len(value)} characters, at most {maximum}")
    if isinstance(value, list):
        minimum = _length(schema, "minItems")
        maximum = _length(schema, "maxItems")
        if minimum is not None and len(value) < minimum:
            raise ValueError(f"{path}: fewer than {minimum} items")
        if maximum is not None and len(value) > maximum:
            raise ValueError(f"{path}: more than {maximum} items")
        if "items" in schema:
            for index, child in enumerate(value):
                _validate(child, schema["items"], f"{path}[{index}]")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric_minimum = _number(schema, "minimum")
        numeric_maximum = _number(schema, "maximum")
        exclusive = _number(schema, "exclusiveMinimum")
        if numeric_minimum is not None and value < numeric_minimum:
            raise ValueError(f"{path}: violates minimum")
        if numeric_maximum is not None and value > numeric_maximum:
            raise ValueError(f"{path}: violates maximum")
        if exclusive is not None and value <= exclusive:
            raise ValueError(f"{path}: must be greater than {exclusive}")
    if isinstance(value, dict):
        required = _array(schema, "required")
        if required is not None:
            for name in required:
                if not isinstance(name, str):
                    raise ToolSchemaError("Configured tool schema required entries must be strings")
                if name not in value:
                    raise ValueError(f"{path}: missing {name}")
        if "properties" in schema:
            properties = schema["properties"]
            if not isinstance(properties, dict):
                raise ToolSchemaError("Configured tool schema properties must be an object")
        else:
            properties = {}
        additional = schema.get("additionalProperties", True)
        if not isinstance(additional, (bool, dict)):
            raise ToolSchemaError("Configured tool schema additionalProperties must be a schema or boolean")
        unknown = set(value) - properties.keys()
        if additional is False and unknown:
            raise ValueError(f"{path}: unknown fields {sorted(unknown)}")
        for name, child in value.items():
            if name in properties:
                _validate(child, properties[name], path + "." + name)
            elif isinstance(additional, dict):
                _validate(child, additional, path + "." + name)


def validate_schema(value: JsonValue, schema: JsonValue, path: str = "$") -> None:
    """Validate strict JSON types, finite numbers and supported tool-schema constraints."""
    _finite(value, path)
    _validate(value, schema, path)
