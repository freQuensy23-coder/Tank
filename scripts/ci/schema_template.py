"""Allocate a fresh named CI schema without claiming it has been created or sealed."""

import os
import stat
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import Field

from scripts.ci.managed_clickhouse import CIContext, Record
from tank.contracts.json import decode_unique_json


class AllocatedSchemaTemplate(Record):
    schema_version: Literal["tank-ci-allocated-schema-template-v1"]
    phase: Literal["ALLOCATED_NOT_PREPARED"]
    owner: CIContext
    name: str = Field(pattern=r"^tank_test_ci_schema_[0-9a-f]{32}$")


def template_path(context: CIContext) -> Path:
    return context.root / ".test-results" / "schema-template-intent.json"


def allocate_template(context: CIContext) -> AllocatedSchemaTemplate:
    record = AllocatedSchemaTemplate(
        schema_version="tank-ci-allocated-schema-template-v1",
        phase="ALLOCATED_NOT_PREPARED",
        owner=context,
        name="tank_test_ci_schema_" + uuid4().hex,
    )
    path = template_path(context)
    if path.parent.is_symlink() or path.parent.stat().st_uid != os.geteuid():
        raise ValueError("Schema allocation requires its owned results directory")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(record.model_dump_json() + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return record


def read_template(context: CIContext) -> AllocatedSchemaTemplate:
    path = template_path(context)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        metadata = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise ValueError("Schema allocation requires its exact owned private file")
        raw = stream.read()
    decode_unique_json(raw)
    record = AllocatedSchemaTemplate.model_validate_json(raw)
    if record.owner != context:
        raise ValueError("Schema allocation belongs to another CI owner")
    return record
