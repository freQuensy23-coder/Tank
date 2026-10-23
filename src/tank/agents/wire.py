"""Last-pass redaction for already projected model tool results."""

from tank.agents.context import clean_value


def tool_wire_result(tool, result, *, session_id, call_id):
    return clean_value(result)
