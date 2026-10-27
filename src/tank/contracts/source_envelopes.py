"""Consumed announcement success facts, separate from retained response bytes."""

from collections.abc import Mapping
from typing import Annotated, Literal

from pydantic import BeforeValidator, ConfigDict, Field, JsonValue, ValidationError

from tank.contracts import DomainModel
from tank.contracts.scalars import TrueFlag, require_number


class BybitAnnouncementResult(DomainModel):
    list: list[JsonValue]


class BybitAnnouncementStatus(DomainModel):
    retCode: Annotated[int | float, BeforeValidator(require_number)]


class BybitAnnouncementSuccess(DomainModel):
    retCode: Annotated[Literal[0], BeforeValidator(require_number)]
    result: BybitAnnouncementResult


def bybit_announcement_success(value: Mapping[str, JsonValue]) -> BybitAnnouncementSuccess:
    """Admit only consumed fields after the adapter validates the original JSON tree."""
    if not isinstance(value, Mapping):
        raise ValueError("Bybit API error: invalid or unsuccessful announcement response")
    try:
        status = BybitAnnouncementStatus.model_validate(
            {"retCode": value["retCode"]} if "retCode" in value else {}
        )
    except ValidationError:
        raise ValueError("Bybit API error: invalid or unsuccessful announcement response") from None
    if status.retCode != 0:
        raise ValueError("Bybit API error: " + str(status.retCode))
    result = value.get("result")
    if not isinstance(result, dict):
        raise ValueError("Bybit API error: invalid or unsuccessful announcement response")
    try:
        return BybitAnnouncementSuccess.model_validate(
            {
                "retCode": status.retCode,
                "result": {"list": result["list"]} if "list" in result else {},
            }
        )
    except ValidationError:
        raise ValueError("Bybit API error: invalid or unsuccessful announcement response") from None


class FederalRegisterResults(DomainModel):
    model_config = ConfigDict(extra="ignore")

    results: list[JsonValue]


class BinanceCmsCatalog(DomainModel):
    model_config = ConfigDict(extra="ignore")

    articles: list[JsonValue]


class BinanceCmsCatalogs(DomainModel):
    model_config = ConfigDict(extra="ignore")

    catalogs: list[BinanceCmsCatalog]


class BinanceCmsCatalogReply(DomainModel):
    """The captured website catalog operation, not an exchange trading reply."""

    model_config = ConfigDict(extra="ignore")

    code: Literal["000000"] | None = None
    success: TrueFlag
    data: BinanceCmsCatalogs


class OkxAnnouncementPage(DomainModel):
    model_config = ConfigDict(extra="ignore")

    details: list[JsonValue]


class OkxAnnouncementReply(DomainModel):
    model_config = ConfigDict(extra="ignore")

    code: Literal["0"]
    data: list[OkxAnnouncementPage]


class PressSearchHits(DomainModel):
    model_config = ConfigDict(extra="ignore")

    hits: list[JsonValue]


class PressSearchResponse(DomainModel):
    model_config = ConfigDict(extra="ignore")

    hits: PressSearchHits


class PressSearchResults(DomainModel):
    model_config = ConfigDict(extra="ignore")

    response: PressSearchResponse


class PressSearchReply(DomainModel):
    model_config = ConfigDict(extra="ignore")

    results: PressSearchResults


class SnapshotProposalData(DomainModel):
    model_config = ConfigDict(extra="ignore")

    proposals: list[JsonValue]


class SnapshotProposalReply(DomainModel):
    model_config = ConfigDict(extra="ignore")

    data: SnapshotProposalData


class SnapshotReportedErrors(DomainModel):
    errors: list[JsonValue]


class BinanceCmsStatus(DomainModel):
    model_config = ConfigDict(extra="ignore")

    success: bool


class OkxAnnouncementStatus(DomainModel):
    model_config = ConfigDict(extra="ignore")

    code: str = Field(pattern=r"^[0-9]+(?:_[0-9]+)*$")
