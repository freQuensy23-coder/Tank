"""Admitted backup inventory and its original recovery-root coordinates."""

from pathlib import PurePosixPath
from typing import Annotated, Self

from pydantic import (
    Field,
    JsonValue,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
    model_validator,
)

from tank.contracts import DomainModel
from tank.contracts.json import JsonDocument, decode_unique_json
from tank.contracts.scalars import OneInt
from tank.time_precision import Timestamp
from tank.util import parse_time

type BackupDigest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
type BackupText = Annotated[str, Field(min_length=1)]


class BackupManifest(DomainModel):
    version: OneInt
    created_at: str
    created_at_prec: str | None = None
    created_at_prec_reason: str | None = None
    reference_base: str
    roots: dict[str, str]
    files: dict[str, BackupDigest]
    database_dumps: list[str]
    excluded: list[BackupText]
    restore: BackupText
    scope: BackupText | None = None

    @field_validator("created_at")
    @classmethod
    def aware_creation(cls, value: str) -> str:
        parse_time(value)
        return value

    @field_validator("reference_base")
    @classmethod
    def absolute_base(cls, value: str) -> str:
        if not PurePosixPath(value).is_absolute():
            raise ValueError("Backup reference base must be absolute")
        return value

    @model_validator(mode="after")
    def inventory(self) -> Self:
        if {"created_at_prec", "created_at_prec_reason"} & self.model_fields_set:
            Timestamp(self.created_at, self.created_at_prec, self.created_at_prec_reason)
        if "scope" in self.model_fields_set and self.scope is None:
            raise ValueError("A declared backup scope requires text")
        if len(self.database_dumps) != 2 or set(self.database_dumps) != {"tank.dump", "oma.dump"}:
            raise ValueError("Backup requires both Tank and OpenMA database dumps")
        for label, source in self.roots.items():
            if not label or "/" in label or label in {".", ".."}:
                raise ValueError("Backup root labels must be single path components")
            if not PurePosixPath(source).is_absolute():
                raise ValueError("Backup source roots must be absolute")
        for relative in self.files:
            path = PurePosixPath(relative)
            if (
                not relative
                or path.is_absolute()
                or any(part in {"", ".", ".."} for part in relative.split("/"))
            ):
                raise ValueError("Backup inventory requires safe relative file paths")
            if relative not in self.database_dumps and path.parts[0] not in self.roots:
                raise ValueError("Backup evidence requires its declared original root")
        if any(name not in self.files for name in self.database_dumps):
            raise ValueError("Backup requires both Tank and OpenMA database dumps")
        return self

    @model_serializer(mode="wrap")
    def preserve_reported_fields(self, handler: SerializerFunctionWrapHandler) -> dict[str, JsonValue]:
        value = JsonDocument.validate_value(handler(self))
        if not isinstance(value, dict):
            raise TypeError("Backup manifest serialization requires an object")
        for field in ("created_at_prec", "created_at_prec_reason", "scope"):
            if field not in self.model_fields_set:
                value.pop(field, None)
        return value

    @classmethod
    def from_bytes(cls, payload: bytes) -> Self:
        return cls.model_validate(decode_unique_json(payload))
