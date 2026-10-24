"""Finite decimal decoding at market wire boundaries."""

from decimal import Decimal, InvalidOperation

from pydantic import JsonValue

from tank.market.errors import SemanticError


def decimal(value: JsonValue | Decimal) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise SemanticError("missing or invalid decimal")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise SemanticError("missing or invalid decimal") from exc
    if not result.is_finite():
        raise SemanticError("nonfinite decimal")
    return result
