"""Redact projected tool output before serializing its actual wire types."""

from pydantic import JsonValue

from tank.agents.context import clean_value
from tank.util import JsonInput, jsonable


def tool_wire_result(result: JsonInput) -> JsonValue:
    return jsonable(clean_value(result))
