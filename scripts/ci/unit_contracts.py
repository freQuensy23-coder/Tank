"""Exact unit case identities, measured file plans and shard declarations."""

import hashlib
import json
from collections import Counter
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal, Self

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from tank.contracts.json import decode_unique_json


def relative_file(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or str(path) != value or path.suffix != ".py":
        raise ValueError("A unit file must be a canonical relative Python path")
    return value


type UnitFile = Annotated[str, AfterValidator(relative_file)]
type Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
type SourceHead = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
type ShardIndex = Annotated[int, Field(ge=0, le=2)]


class UnitRecord(BaseModel):
    model_config = ConfigDict(
        strict=True, extra="forbid", frozen=True, allow_inf_nan=False, hide_input_in_errors=True
    )


class UnitCase(UnitRecord):
    classname: str = Field(min_length=1)
    name: str = Field(min_length=1)
    file: UnitFile

    @property
    def identity(self) -> tuple[str, str]:
        return self.classname, self.name


class UnitCollection(UnitRecord):
    schema_version: Literal["tank-unit-collection-v1"]
    source_head: SourceHead
    cases: list[UnitCase] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_cases(self) -> Self:
        identities = [case.identity for case in self.cases]
        if len(identities) != len(set(identities)):
            raise ValueError("Unit JUnit identities must be unique")
        return self

    @property
    def signature(self) -> str:
        rows = sorted((case.classname, case.name, case.file) for case in self.cases)
        return hashlib.sha256(json.dumps(rows, separators=(",", ":")).encode()).hexdigest()


class FileTiming(UnitRecord):
    file: UnitFile
    cases: int = Field(gt=0)
    seconds: Decimal = Field(ge=0, allow_inf_nan=False)


def measured_assignments(files: list[FileTiming]) -> dict[str, int]:
    duration = [Decimal(0), Decimal(0), Decimal(0)]
    counts = [0, 0, 0]
    result: dict[str, int] = {}
    for item in sorted(files, key=lambda item: (-item.seconds, item.file)):
        index = min(range(3), key=lambda index: (duration[index], counts[index], index))
        result[item.file] = index
        duration[index] += item.seconds
        counts[index] += item.cases
    return result


class UnitPlan(UnitRecord):
    schema_version: Literal["tank-unit-plan-v2"]
    shard_count: Literal[3]
    source_head: SourceHead
    collection_sha256: Digest
    reports_sha256: list[Digest] = Field(min_length=1)
    files: list[FileTiming] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_files(self) -> Self:
        paths = [item.file for item in self.files]
        if len(paths) != len(set(paths)):
            raise ValueError("Measured unit paths must be unique")
        return self


def shard_for(file: str, measured: dict[str, int]) -> int:
    if file in measured:
        return measured[file]
    return int.from_bytes(hashlib.sha256(file.encode()).digest()[:8]) % 3


class UnitShard(UnitRecord):
    index: ShardIndex
    count: int = Field(ge=3, le=3)


def parse_shard(value: str) -> UnitShard:
    if value not in {"0/3", "1/3", "2/3"}:
        raise ValueError("Unit shard must be 0/3, 1/3 or 2/3")
    return UnitShard(index=int(value[0]), count=3)


class WeightCoverage(UnitRecord):
    unmeasured_files: list[UnitFile]
    changed_case_count_files: list[UnitFile]


def weight_coverage(collection: UnitCollection, plan: UnitPlan) -> WeightCoverage:
    current = Counter(case.file for case in collection.cases)
    measured = {item.file: item.cases for item in plan.files}
    return WeightCoverage(
        unmeasured_files=sorted(current.keys() - measured.keys()),
        changed_case_count_files=sorted(
            file for file in current.keys() & measured.keys() if current[file] != measured[file]
        ),
    )


class UnitDeclaration(UnitRecord):
    schema_version: Literal["tank-unit-declaration-v2"]
    source_head: SourceHead
    shard: UnitShard
    collection_sha256: Digest
    plan_sha256: Digest
    coverage: WeightCoverage


class PassedUnitCase(UnitRecord):
    classname: str = Field(min_length=1)
    name: str = Field(min_length=1)
    seconds: Decimal = Field(ge=0, allow_inf_nan=False)

    @property
    def identity(self) -> tuple[str, str]:
        return self.classname, self.name


class UnitSummary(UnitRecord):
    cases: int = Field(gt=0)
    passed: int = Field(gt=0)
    skipped: int = Field(ge=0, le=0)
    p90_seconds: Decimal = Field(ge=0, allow_inf_nan=False)
    max_seconds: Decimal = Field(ge=0, allow_inf_nan=False)


def read_model[T: BaseModel](path: Path, model: type[T]) -> T:
    raw = path.read_bytes()
    decode_unique_json(raw)
    return model.model_validate_json(raw)
