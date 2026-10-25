import json

import pytest
from pydantic import JsonValue, ValidationError

from tank.contracts import DomainModel
from tank.contracts.progress import ProgressNotice, PublicationReceipt
from tank.contracts.source_checkpoints import CapturedInstrumentCoverage, InstrumentCoverage
from tank.contracts.source_items import PublisherCMSRecovery
from tank.contracts.sources import UnconfiguredSource


@pytest.fixture
def scalar_records() -> list[tuple[type[DomainModel], dict[str, JsonValue], str, int]]:
    return [
        (
            PublicationReceipt,
            {
                "contract_version": 1,
                "event_id": "event",
                "kind": None,
                "ems_id": "assessment",
                "ems_class": "UNASSESSED",
                "lifecycle": "ACTIVE",
                "freshness": None,
                "certainty": None,
                "map_version": None,
                "analysis_set": [],
                "decision_ids": [],
                "market_truth_ids": [],
                "previous_alert_id": None,
                "previous_alert_version": None,
                "disposition": "REFUSED",
                "reason": "NO_ACCEPTED_CARD",
                "alert_id": None,
                "alert_version": None,
            },
            "contract_version",
            1,
        ),
        (
            ProgressNotice,
            {
                "notice_id": "notice",
                "subject_id": "item",
                "stage": "RECEIVED",
                "event_id": None,
                "reason": "",
                "text": "Received",
                "format_version": 2,
                "details": {"source_provenance": "NOT_CAPTURED"},
                "created_at": "2026-10-25T00:00:00Z",
            },
            "format_version",
            2,
        ),
        (
            UnconfiguredSource,
            {
                "source_id": "S01",
                "items": 0,
                "errors": [],
            },
            "items",
            0,
        ),
        (
            PublisherCMSRecovery,
            {
                "status": "VERIFIED_SAME_CMS_ENTRY",
                "canonical_url": "https://example.test/post",
                "resolved_url": "https://example.test/cms/post",
                "original_http_status": 404,
                "original_response_url": "https://example.test/post",
                "original_payload_sha256": "a" * 64,
                "observed_at": "2026-10-25T00:00:00Z",
                "publisher_cms_identity": {
                    "entry_id": "entry",
                    "slug": "post",
                    "title": "Post",
                    "content_sha256": "b" * 64,
                },
            },
            "original_http_status",
            404,
        ),
        (
            InstrumentCoverage,
            {
                "etag": None,
                "last_modified": None,
                "instrument_snapshot_schema_version": 2,
            },
            "instrument_snapshot_schema_version",
            2,
        ),
        (
            CapturedInstrumentCoverage,
            {
                "instruments": {},
                "instrument_snapshot_schema_version": 1,
            },
            "instrument_snapshot_schema_version",
            1,
        ),
        (
            CapturedInstrumentCoverage,
            {
                "instruments": {},
                "instrument_snapshot_schema_version": 2,
            },
            "instrument_snapshot_schema_version",
            2,
        ),
    ]


def test_fixed_integer_boundary_preserves_real_records_and_rejects_numeric_aliases(
    scalar_records: list[tuple[type[DomainModel], dict[str, JsonValue], str, int]],
) -> None:
    for schema, record, field, expected in scalar_records:
        accepted = schema.model_validate(record)
        serialized = accepted.model_dump(mode="json")
        assert type(serialized[field]) is int
        assert serialized[field] == expected
        assert schema.model_validate_json(accepted.model_dump_json()) == accepted
        for invalid in (float(expected), bool(expected)):
            with pytest.raises(ValidationError, match="Fixed scalar requires an integer"):
                schema.model_validate({**record, field: invalid})
            with pytest.raises(ValidationError, match="Fixed scalar requires an integer"):
                schema.model_validate_json(json.dumps({**record, field: invalid}))


def test_historical_capture_can_explicitly_lack_schema_version() -> None:
    absent = CapturedInstrumentCoverage.model_validate({"instruments": {}})
    declared_unknown = CapturedInstrumentCoverage.model_validate(
        {
            "instruments": {},
            "instrument_snapshot_schema_version": None,
        }
    )
    assert absent.instrument_snapshot_schema_version is None
    assert "instrument_snapshot_schema_version" not in absent.model_dump(exclude_unset=True)
    assert declared_unknown.model_dump(exclude_unset=True)["instrument_snapshot_schema_version"] is None
