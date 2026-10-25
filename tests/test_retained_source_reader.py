from pathlib import Path

import pytest
import zstandard

from tank.fixture_reference import retained_source_payload
from tank.fixture_reference_sources import (
    RequiredRetainedInventory,
    RequiredRetainedRaw,
    RetainedInputOrigin,
    StagedRetainedInventory,
    StagedRetainedRaw,
    VerifiedRetainedSources,
)
from tank.util import sha256

ORIGINAL = "/historical/raw/content.json.zst"


def manifests(tmp_path: Path) -> tuple[Path, Path, Path, Path, bytes]:
    payload = b'{"original":"retained"}'
    transport = zstandard.ZstdCompressor(write_content_size=False).compress(payload)
    root = tmp_path / "staged"
    root.mkdir()
    location = root / "content.zst"
    location.write_bytes(transport)
    required = RequiredRetainedInventory(
        verified_input_packets=1,
        excluded_fixture="F-02_MANIFEST_MISMATCH",
        raw_inputs=[
            RequiredRetainedRaw(
                original_path=ORIGINAL,
                expected_payload_sha256=sha256(payload),
                hash_basis="DECOMPRESSED_RETAINED_SOURCE_BYTES",
                origins=[
                    RetainedInputOrigin(
                        fixture_id="F-01",
                        packet_path="inputs/one.json.zst",
                        compressed_sha256="a" * 64,
                        field_path="$.records[0].as_of_evidence.raw_ref",
                    )
                ],
            )
        ],
    )
    required_path = tmp_path / "required.json"
    required_bytes = required.model_dump_json().encode()
    required_path.write_bytes(required_bytes)
    staged = StagedRetainedInventory(
        scope="Exact named retained input bodies",
        input_manifest=str(required_path),
        owner_manifest_sha256=sha256(required_bytes),
        hash_basis="DECOMPRESSED_RETAINED_SOURCE_BYTES",
        counts={"verified_staged": 1},
        entries=[
            StagedRetainedRaw(
                original_path=ORIGINAL,
                expected_payload_sha256=sha256(payload),
                source_candidate="/historical/unavailable/application-original.zst",
                actual_payload_sha256=sha256(payload),
                transport_sha256=sha256(transport),
                transport_bytes=len(transport),
                payload_bytes=len(payload),
                staged_path=location,
                status="verified_staged",
            )
        ],
    )
    staged_path = tmp_path / "staged.json"
    staged_path.write_text(staged.model_dump_json())
    return required_path, staged_path, root, location, payload


def test_verified_reader_preserves_exact_body_without_accessing_original_location(tmp_path: Path) -> None:
    required, staged, root, _, expected = manifests(tmp_path)
    reader = VerifiedRetainedSources(required, staged, staged_root=root)
    assert retained_source_payload(ORIGINAL, raw_sources=reader) == expected
    with pytest.raises(ValueError, match="not listed"):
        reader.read_payload("/historical/unlisted.zst")


def test_verified_reader_rechecks_transport_after_construction(tmp_path: Path) -> None:
    required, staged, root, location, _ = manifests(tmp_path)
    reader = VerifiedRetainedSources(required, staged, staged_root=root)
    location.write_bytes(b"changed")
    with pytest.raises(ValueError, match="transport bytes differ"):
        reader.read_payload(ORIGINAL)


def test_verified_reader_refuses_a_staged_symlink_outside_its_declared_root(tmp_path: Path) -> None:
    required, staged, root, location, _ = manifests(tmp_path)
    external = tmp_path / "external.zst"
    location.rename(external)
    location.symlink_to(external)
    with pytest.raises(ValueError, match="escapes its declared root"):
        VerifiedRetainedSources(required, staged, staged_root=root)


def test_verified_reader_refuses_a_different_owner_inventory(tmp_path: Path) -> None:
    required, staged, root, _, _ = manifests(tmp_path)
    required.write_text(required.read_text() + "\n")
    with pytest.raises(ValueError, match="not bound"):
        VerifiedRetainedSources(required, staged, staged_root=root)


def test_verified_reader_checks_payload_even_when_transport_metadata_matches(tmp_path: Path) -> None:
    required, staged, root, location, _ = manifests(tmp_path)
    document = StagedRetainedInventory.model_validate_json(staged.read_bytes())
    original = document.entries[0]
    changed = b'{"original":"modified"}'
    transport = zstandard.ZstdCompressor().compress(changed)
    location.write_bytes(transport)
    altered = StagedRetainedInventory(
        scope=document.scope,
        input_manifest=document.input_manifest,
        owner_manifest_sha256=document.owner_manifest_sha256,
        hash_basis=document.hash_basis,
        counts=document.counts,
        entries=[
            StagedRetainedRaw(
                original_path=original.original_path,
                expected_payload_sha256=original.expected_payload_sha256,
                source_candidate=original.source_candidate,
                actual_payload_sha256=original.actual_payload_sha256,
                transport_sha256=sha256(transport),
                transport_bytes=len(transport),
                payload_bytes=len(changed),
                staged_path=location,
                status="verified_staged",
            )
        ],
    )
    staged.write_text(altered.model_dump_json())
    with pytest.raises(ValueError, match="payload differs"):
        VerifiedRetainedSources(required, staged, staged_root=root)
