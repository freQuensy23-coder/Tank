"""Read envelopes preserve actual physical representation and public model identity."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.contracts.stored_documents import ImmutableDocument, NativeBackfillWitness
from tank.models import Record
from tank.storage import ImmutableDocument as StorageImmutableDocument


def test_existing_storage_document_model_is_the_same_inward_object() -> None:
    assert StorageImmutableDocument is ImmutableDocument
    value = ImmutableDocument(document={"id": "captured"}, immutable=True)
    assert value.model_dump(mode="json") == {"document": {"id": "captured"}, "immutable": True}


def test_native_backfill_retains_only_its_actual_decoded_fields() -> None:
    original: Record = {
        "backfill_id": "BF-captured",
        "status": "PARTIAL",
        "recorded_at": "2026-10-26T00:00:00.123456Z",
        "recorded_at_prec": None,
        "reason": "",
        "result": None,
    }
    value = NativeBackfillWitness(record=original)
    assert value.record == original
    assert value.model_dump(mode="json") == {"record": original}
    with pytest.raises(ValidationError):
        NativeBackfillWitness.model_validate({"record": original, "immutable": False})
    with pytest.raises(ValidationError):
        NativeBackfillWitness.model_validate({"record": original, "document": original})


def test_document_read_requires_a_real_boolean_immutability_flag() -> None:
    with pytest.raises(ValidationError):
        ImmutableDocument.model_validate({"document": {}, "immutable": 1})
    with pytest.raises(ValidationError):
        ImmutableDocument.model_validate({"document": {}})


def test_read_envelopes_do_not_share_mutable_nested_input() -> None:
    items: list[JsonValue] = [1, True, None]
    original: Record = {"state": {"items": items}}
    document = ImmutableDocument(document=original, immutable=False)
    native = NativeBackfillWitness(record=original)
    items.append("later caller mutation")
    assert document.document == {"state": {"items": [1, True, None]}}
    assert native.record == {"state": {"items": [1, True, None]}}
