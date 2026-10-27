"""Order measured lane-two file slots without inventing costs for unknown work."""

import hashlib
from collections import Counter
from pathlib import Path
from typing import Literal, Self

from pydantic import ConfigDict, Field, RootModel, model_validator

from scripts.ci.unit_contracts import (
    Digest,
    FileTiming,
    SourceHead,
    UnitCase,
    UnitFile,
    UnitRecord,
    read_model,
)
from tank.contracts.json import decode_unique_json

PLAN = Path(__file__).with_name("integration_order.json")
COLLECTION = Path(".test-results/integration-collection.json")
OUTPUT = Path(".test-results/integration-order.json")


class IntegrationOrderPlan(UnitRecord):
    schema_version: Literal["tank-integration-order-v1"]
    source_head: SourceHead
    checkout_sha: SourceHead
    source_tree: SourceHead
    collection_sha256: Digest
    report_sha256: Digest
    shard_index: int = Field(ge=2, le=2)
    shard_count: int = Field(ge=3, le=3)
    cases: int = Field(gt=0)
    files: list[FileTiming] = Field(min_length=1)

    @model_validator(mode="after")
    def complete(self) -> Self:
        names = [file.file for file in self.files]
        if len(names) != len(set(names)) or sum(file.cases for file in self.files) != self.cases:
            raise ValueError(
                "Integration ordering requires distinct files and its actual recorded case count"
            )
        return self


class IntegrationCollection(RootModel[list[UnitCase]]):
    model_config = ConfigDict(strict=True)
    root: list[UnitCase] = Field(min_length=1)

    @model_validator(mode="after")
    def unique(self) -> Self:
        if len({case.identity for case in self.root}) != len(self.root):
            raise ValueError("Integration collection contains repeated case identities")
        return self


class FileOrder(UnitRecord):
    files: tuple[UnitFile, ...] = Field(min_length=1)
    measured_files: tuple[UnitFile, ...]
    unmeasured_files: tuple[UnitFile, ...]
    changed_count_files: tuple[UnitFile, ...]
    absent_calibration_files: tuple[UnitFile, ...]


class NativeOrderReport(UnitRecord):
    kind: Literal["NATIVE_CASE_COUNT"]
    source_head: SourceHead
    collection_sha256: Digest
    shard_index: int = Field(ge=0, le=1)
    shard_count: int = Field(ge=3, le=3)
    cases: int = Field(gt=0)
    files: tuple[UnitFile, ...]


class MeasuredOrderReport(UnitRecord):
    kind: Literal["MEASURED_FILE_SLOTS"]
    source_head: SourceHead
    collection_sha256: Digest
    calibration_source_head: SourceHead
    calibration_checkout: SourceHead
    calibration_source_tree: SourceHead
    calibration_report_sha256: Digest
    calibration_collection_sha256: Digest
    calibration_is_historical: bool
    shard_index: int = Field(ge=2, le=2)
    shard_count: int = Field(ge=3, le=3)
    cases: int = Field(gt=0)
    order: FileOrder


def native_file_order(files: list[str]) -> tuple[str, ...]:
    counts = Counter(files)
    return tuple(sorted(counts, key=lambda file: -counts[file]))


def measured_file_order(files: list[str], plan: IntegrationOrderPlan) -> FileOrder:
    counts = Counter(files)
    native = native_file_order(files)
    calibrated = {item.file: item for item in plan.files}
    eligible = {
        file: item for file, item in calibrated.items() if file in counts and counts[file] == item.cases
    }
    native_positions = {file: index for index, file in enumerate(native)}
    ranked = iter(sorted(eligible, key=lambda file: (-eligible[file].seconds, native_positions[file])))
    ordered = tuple(next(ranked) if file in eligible else file for file in native)
    if len(ordered) != len(native) or set(ordered) != set(native):
        raise ValueError("Integration ordering must preserve every selected file exactly once")
    return FileOrder(
        files=ordered,
        measured_files=tuple(file for file in ordered if file in eligible),
        unmeasured_files=tuple(file for file in native if file not in calibrated),
        changed_count_files=tuple(file for file in native if file in calibrated and file not in eligible),
        absent_calibration_files=tuple(file for file in calibrated if file not in counts),
    )


def load_plan(path: Path) -> IntegrationOrderPlan:
    return read_model(path, IntegrationOrderPlan)


def read_collection(path: Path) -> tuple[IntegrationCollection, str]:
    raw = path.read_bytes()
    decode_unique_json(raw)
    return IntegrationCollection.model_validate_json(raw), hashlib.sha256(raw).hexdigest()
