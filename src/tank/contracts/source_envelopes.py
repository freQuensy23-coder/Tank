"""Consumed announcement success facts, separate from retained response bytes."""

from collections.abc import Mapping
from typing import Annotated, Literal

from pydantic import BeforeValidator, JsonValue, ValidationError

from tank.contracts import DomainModel
from tank.contracts.scalars import require_number


class BybitAnnouncementResult(DomainModel):
    list: list[JsonValue]


class BybitAnnouncementSuccess(DomainModel):
    retCode: Annotated[Literal[0], BeforeValidator(require_number)]
    result: BybitAnnouncementResult


def bybit_announcement_success(value: Mapping[str, JsonValue]) -> BybitAnnouncementSuccess:
    """Admit only consumed fields after the adapter validates the original JSON tree."""
    if not isinstance(value, Mapping):
        raise ValueError("Bybit API error: invalid or unsuccessful announcement response")
    result = value.get("result")
    if not isinstance(result, dict):
        raise ValueError("Bybit API error: invalid or unsuccessful announcement response")
    try:
        return BybitAnnouncementSuccess.model_validate(
            {
                **({"retCode": value["retCode"]} if "retCode" in value else {}),
                "result": {"list": result["list"]} if "list" in result else {},
            }
        )
    except ValidationError:
        raise ValueError("Bybit API error: invalid or unsuccessful announcement response") from None
