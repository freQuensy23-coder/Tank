from typing import Literal

import pytest
from pydantic import JsonValue, ValidationError

from tank.contracts.json import DuplicateJsonKeyError, decode_json, decode_unique_json


@pytest.mark.parametrize(
    "payload",
    [
        b'{"retCode":1,"retCode":0,"result":{"list":[]}}',
        b'{"items":[{"category":"spot","category":"linear"}]}',
        b'{"telemetry":{"token":"fixture-secret"},"telemetry":{}}',
        b'{"to\\u006ben":"fixture-secret","token":null}',
        b'{"status":"active","status":"active"}',
    ],
)
def test_duplicate_facts_are_refused_before_they_can_be_collapsed(payload: bytes) -> None:
    original = bytearray(payload)
    with pytest.raises(DuplicateJsonKeyError) as error:
        decode_unique_json(original)
    assert str(error.value) == "JSON object keys must be unique"
    assert "fixture-secret" not in str(error.value)
    assert original == payload


@pytest.mark.parametrize(
    "payload",
    [
        b'{"items":[{"symbol":"A"},{"symbol":"B"}]}',
        b'{"first":{"value":0},"second":{"value":false}}',
        b'[null,true,0,0.0,-2,1e-20,"001.2300",{"a/b":"a\\nb"}]',
        b'"scalar"',
        b"null",
    ],
)
def test_unique_decoder_preserves_existing_numeric_text_and_array_scope(payload: bytes) -> None:
    original = bytes(payload)
    actual: JsonValue = decode_unique_json(payload)
    assert actual == decode_json(payload)
    assert payload == original
    if isinstance(actual, dict) and "first" in actual:
        first, second = actual["first"], actual["second"]
        assert isinstance(first, dict) and isinstance(second, dict)
        assert isinstance(first["value"], int) and not isinstance(first["value"], bool)
        assert isinstance(second["value"], bool)
        assert first["value"] == 0 and second["value"] is False


@pytest.mark.parametrize(
    "payload",
    [
        b'{"key":',
        b'{"secret":"fixture-secret", broken}',
        b'"\\ud800"',
        b"\xff",
    ],
)
def test_invalid_syntax_and_unicode_keep_sanitized_authoritative_validation(payload: bytes) -> None:
    with pytest.raises(ValidationError) as error:
        decode_unique_json(payload)
    assert "fixture-secret" not in str(error.value)
    assert "input_value" not in str(error.value)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", "1e999"])
def test_unique_decoder_does_not_relax_finite_number_admission(value: str) -> None:
    with pytest.raises(ValueError, match="finite"):
        decode_unique_json('{"value":' + value + "}")


@pytest.mark.parametrize("kind", ["str", "bytes", "bytearray"])
def test_unique_decoder_accepts_each_declared_raw_payload_type(
    kind: Literal["str", "bytes", "bytearray"],
) -> None:
    raw = '{"token":"kept-as-text","nested":[{"n":0}]}'
    payload: str | bytes | bytearray
    if kind == "str":
        payload = raw
    elif kind == "bytes":
        payload = raw.encode()
    else:
        payload = bytearray(raw.encode())
    assert decode_unique_json(payload) == {"token": "kept-as-text", "nested": [{"n": 0}]}


def test_duplicate_secret_field_names_and_escaped_values_are_not_echoed() -> None:
    values = (
        '{"fixture-secret":1,"fixture-secret":2}',
        '{"telemetry":{"token":"\\u0066ixture-secret"},"telemetry":{}}',
    )
    for raw in values:
        with pytest.raises(DuplicateJsonKeyError) as error:
            decode_unique_json(raw)
        assert error.value.args == ("JSON object keys must be unique",)
        assert error.value.__cause__ is None


def test_opaque_json_text_and_distinct_unicode_keys_are_not_rewritten() -> None:
    import json

    opaque = '{"status":1,"status":0}'
    payload = json.dumps({"raw": opaque, "é": 1, "e\u0301": 2}).encode()
    assert decode_unique_json(payload) == {"raw": opaque, "é": 1, "e\u0301": 2}


def test_raw_number_kinds_survive_before_integer_and_boolean_contracts() -> None:
    value = decode_unique_json('{"i":0,"f":0.0,"e":0e0,"b":false}')
    assert isinstance(value, dict)
    assert isinstance(value["i"], int) and not isinstance(value["i"], bool)
    assert isinstance(value["f"], float) and isinstance(value["e"], float)
    assert isinstance(value["b"], bool)
