"""Fresh-cache declarations and observed native identities stay distinct."""

import pytest
from pydantic import ValidationError

from tank.contracts.native_files import (
    NativeFileIdObserved,
    NativeFileMetadataVerified,
    NativeFileNotRequested,
    NativeFileSourceScope,
    NativeMountConfirmed,
    NativeMountIntent,
    NativeUploadIntent,
    NativeUploadUnconfirmed,
    NativeVerifiedFileMetadata,
)
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.time_precision import Timestamp


def source_scope() -> NativeFileSourceScope:
    return NativeFileSourceScope(
        file_key="file-key",
        question_id="question",
        session_id="session",
        cached_sha256="a" * 64,
        filename="report.txt",
        mime="text/plain",
        size=0,
    )


def clock() -> Timestamp:
    return Timestamp("2026-10-26T00:00:00Z", "EXACT", None)


def upload_intent() -> NativeUploadIntent:
    return NativeUploadIntent(
        scope=source_scope(),
        recorded_at=clock(),
        versions=ResolvedVersionIdentity(config_version="fixture-config", code_version="fixture-code"),
    )


def verified_file(upload: NativeFileIdObserved) -> NativeFileMetadataVerified:
    source = upload.intent.scope
    return NativeFileMetadataVerified(
        phase="FILE_METADATA_VERIFIED",
        upload=upload,
        recorded_at=clock(),
        metadata=NativeVerifiedFileMetadata(
            file_id=upload.file_id,
            filename=source.filename,
            mime_type=source.mime,
            size_bytes=source.size,
            scope_session_id=None,
        ),
    )


def test_fresh_declaration_has_no_native_identity_or_controller_version_invention() -> None:
    fresh = NativeFileNotRequested(phase="NOT_REQUESTED", scope=source_scope(), recorded_at=clock())
    assert set(fresh.model_dump()) == {"phase", "scope", "recorded_at"}
    pending = NativeUploadUnconfirmed(phase="UPLOAD_UNCONFIRMED", intent=upload_intent())
    assert "file_id" not in pending.model_dump()


def test_confirmed_mount_retains_actual_upload_and_controller_evidence() -> None:
    uploaded = NativeFileIdObserved(
        phase="FILE_ID_OBSERVED",
        intent=upload_intent(),
        file_id="native-file",
        recorded_at=clock(),
    )
    mounting = NativeMountIntent(
        upload=verified_file(uploaded),
        mount_path="/workspace/uploads/question/report.txt",
        recorded_at=clock(),
    )
    receipt = NativeMountConfirmed(
        phase="MOUNT_CONFIRMED",
        intent=mounting,
        resource_id="native-resource",
        recorded_at=clock(),
    )
    assert receipt.intent.upload.upload.file_id == "native-file"
    assert receipt.intent.upload.upload.intent.scope == source_scope()
    assert receipt.resource_id == "native-resource"


@pytest.mark.parametrize("field", ["question_id", "session_id", "file_key"])
def test_scope_rejects_constructed_missing_owner(field: str) -> None:
    values = source_scope().model_dump()
    values[field] = ""
    with pytest.raises(ValidationError):
        NativeFileSourceScope.model_validate(values)


def test_unknown_occurrence_cannot_become_new_controller_evidence() -> None:
    with pytest.raises(ValidationError):
        NativeFileNotRequested(
            phase="NOT_REQUESTED",
            scope=source_scope(),
            recorded_at=Timestamp(None, None, "not observed"),
        )


@pytest.mark.parametrize(
    "path", ["/tmp/report.txt", "/workspace/uploads/../report.txt", "/workspace/uploads//report.txt"]
)
def test_mount_intent_refuses_noncanonical_target(path: str) -> None:
    uploaded = NativeFileIdObserved(
        phase="FILE_ID_OBSERVED",
        intent=upload_intent(),
        file_id="native-file",
        recorded_at=clock(),
    )
    with pytest.raises(ValidationError):
        NativeMountIntent(upload=verified_file(uploaded), mount_path=path, recorded_at=clock())


def test_constructed_nested_scope_is_revalidated_before_upload_intent() -> None:
    mutated = source_scope().model_copy(update={"session_id": ""})
    with pytest.raises(ValidationError):
        NativeUploadIntent(
            scope=mutated,
            recorded_at=clock(),
            versions=ResolvedVersionIdentity(config_version="fixture-config", code_version="fixture-code"),
        )
