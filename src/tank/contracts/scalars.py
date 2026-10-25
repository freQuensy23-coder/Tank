"""Strict fixed scalar assertions at raw JSON validation boundaries."""

from math import isfinite
from typing import Annotated, Literal

from pydantic import BeforeValidator, JsonValue


def require_boolean(value: JsonValue) -> bool:
    if not isinstance(value, bool):
        raise ValueError("Fixed scalar requires a boolean")
    return value


def require_integer(value: JsonValue) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("Fixed scalar requires an integer")
    return value


def require_number(value: JsonValue) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Fixed scalar requires a number")
    if isinstance(value, float) and not isfinite(value):
        raise ValueError("Fixed scalar requires a finite number")
    return value


type TrueFlag = Annotated[Literal[True], BeforeValidator(require_boolean)]
type FalseFlag = Annotated[Literal[False], BeforeValidator(require_boolean)]
type ZeroInt = Annotated[Literal[0], BeforeValidator(require_integer)]
type OneInt = Annotated[Literal[1], BeforeValidator(require_integer)]
