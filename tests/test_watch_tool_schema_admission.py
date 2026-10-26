"""The actual host validator enforces the declared mutually exclusive watch requests."""

from pathlib import Path
from typing import Literal

import pytest
from pydantic import JsonValue

from tank.contracts.json import decode_unique_json
from tank.contracts.tool_schema import ToolSchemaError, validate_schema
from tank.util import canonical_json


def watch_schema() -> JsonValue:
    return decode_unique_json(
        (
            Path(__file__).resolve().parents[1]
            / "config/skills/tank-event-analyst/set_watch_condition.schema.json"
        ).read_bytes()
    )


def publish_schema() -> JsonValue:
    return decode_unique_json(
        (
            Path(__file__).resolve().parents[1] / "config/skills/tank-event-analyst/publish_card.schema.json"
        ).read_bytes()
    )


@pytest.mark.parametrize(
    "payload",
    [
        {"action": "DISARM", "cond_id": "CND-condition"},
        {
            "action": "ARM",
            "token_id": "world:WLD",
            "conditions": [
                {"metric": "event.state", "op": "==", "value": "LIVE"},
            ],
        },
        {
            "action": "ARM",
            "review_at": "2026-10-26T13:00:00Z",
            "review_question": "Has the partner confirmed?",
        },
    ],
)
def test_actual_watch_shapes_are_admitted_without_inserting_defaults(payload: dict[str, JsonValue]) -> None:
    before = canonical_json(payload)
    validate_schema(payload, watch_schema())
    assert canonical_json(payload) == before
    assert "expires_in_hours" not in payload


@pytest.mark.parametrize(
    "payload",
    [
        {"action": "DISARM", "cond_id": "CND-condition", "token_id": "world:WLD"},
        {"action": "DISARM", "cond_id": "CND-condition", "expires_in_hours": 72},
        {
            "action": "ARM",
            "review_at": "2026-10-26T13:00:00Z",
            "review_question": "Question",
            "text": "Other text",
        },
        {
            "action": "ARM",
            "review_at": "2026-10-26T13:00:00Z",
            "review_question": "Question",
            "token_id": "world:WLD",
        },
        {
            "action": "ARM",
            "token_id": "world:WLD",
            "conditions": [
                {"metric": "event.state", "op": "==", "value": "LIVE"},
            ],
            "review_question": "Question",
        },
        {"action": "ARM"},
        {"action": "DISARM", "review_at": "2026-10-26T13:00:00Z", "review_question": "Question"},
    ],
)
def test_actual_watch_shapes_reject_missing_or_mixed_modes(payload: dict[str, JsonValue]) -> None:
    with pytest.raises(ValueError):
        validate_schema(payload, watch_schema())


def test_branch_properties_and_required_apply_without_an_explicit_branch_object_type() -> None:
    schema: JsonValue = {
        "type": "object",
        "oneOf": [
            {"properties": {"action": {"const": "A"}}, "required": ["action", "a"]},
            {"properties": {"action": {"const": "B"}}, "required": ["action", "b"]},
        ],
    }
    validate_schema({"action": "A", "a": 0}, schema)
    with pytest.raises(ValueError):
        validate_schema({"action": "A", "b": 0}, schema)


def test_exactly_one_branch_must_match_instead_of_merely_one_or_more() -> None:
    with pytest.raises(ValueError, match="exactly one"):
        validate_schema({"present": 0}, {"oneOf": [{"required": ["present"]}, {"required": ["present"]}]})
    validate_schema("value", {"oneOf": [True, False]})


@pytest.mark.parametrize("value", [1, 0, {"flag": 1}, [1]])
def test_const_does_not_equate_boolean_facts_with_numbers(value: JsonValue) -> None:
    schema: JsonValue = {"const": True}
    if isinstance(value, dict):
        schema = {"const": {"flag": True}}
    elif isinstance(value, list):
        schema = {"const": [True]}
    with pytest.raises(ValueError):
        validate_schema(value, schema)
    validate_schema(True, {"const": True})


@pytest.mark.parametrize(
    "schema",
    [
        {"oneOf": []},
        {"oneOf": {}},
        {"oneOf": [True, 17]},
        {"not": 17},
        {"oneOf": [True, {"type": "unsupported"}]},
    ],
)
def test_malformed_schema_cannot_be_counted_as_an_instance_refusal(schema: JsonValue) -> None:
    with pytest.raises(ToolSchemaError):
        validate_schema("PRIVATE_REQUEST_VALUE", schema)


def test_negated_presence_and_exclusive_errors_do_not_echo_actual_input() -> None:
    private = "SYNTHETIC_PRIVATE_REVIEW_TEXT"
    with pytest.raises(ValueError) as rejected:
        validate_schema({"private": private}, {"not": {"required": ["private"]}})
    assert private not in str(rejected.value)
    with pytest.raises(ValueError) as exclusive:
        validate_schema(private, {"oneOf": [{"const": "A"}, {"const": "B"}]})
    assert private not in str(exclusive.value)


@pytest.mark.parametrize("kind", ["FULL", "PARTIAL", "UPDATE"])
def test_publish_condition_preserves_two_why_bullets_for_ordinary_cards(
    kind: Literal["FULL", "PARTIAL", "UPDATE"],
) -> None:
    private = "SYNTHETIC_PRIVATE_FACT"
    payload: dict[str, JsonValue] = {
        "kind": kind,
        "what": "A synthetic product shipped.",
        "verdict": "Synthetic launch confirmed.",
        "why": [private],
    }
    with pytest.raises(ValueError) as rejected:
        validate_schema(payload, publish_schema())
    assert not isinstance(rejected.value, ToolSchemaError)
    assert private not in str(rejected.value)
    payload["why"] = ["The release is published.", "The feature is available."]
    validate_schema(payload, publish_schema())
    with pytest.raises(ValueError):
        validate_schema({**payload, "unconfirmed": private}, publish_schema())


def test_publish_condition_allows_one_preliminary_fact_without_mutating_the_payload() -> None:
    payload: dict[str, JsonValue] = {
        "kind": "PRELIMINARY",
        "what": "The issuer announced a proposed integration.",
        "verdict": "A proposed integration is under review.",
        "why": ["The issuer published the proposed scope."],
        "unconfirmed": "The counterparty has not confirmed participation.",
    }
    before = canonical_json(payload)
    validate_schema(payload, publish_schema())
    assert canonical_json(payload) == before


def test_all_of_applies_every_typeless_object_constraint() -> None:
    schema: JsonValue = {
        "allOf": [
            {"required": ["count"]},
            {"properties": {"count": {"type": "integer", "minimum": 1}}},
        ]
    }
    validate_schema({"count": 1}, schema)
    with pytest.raises(ValueError):
        validate_schema({"count": 0}, schema)
    with pytest.raises(ValueError):
        validate_schema({}, schema)


def test_conditional_branch_selection_preserves_boolean_and_numeric_distinctions() -> None:
    schema: JsonValue = {
        "if": {"properties": {"flag": {"const": True}}, "required": ["flag"]},
        "then": {"required": ["yes"]},
        "else": {"required": ["no"]},
    }
    validate_schema({"flag": True, "yes": 0}, schema)
    validate_schema({"flag": 1, "no": 0}, schema)
    with pytest.raises(ValueError):
        validate_schema({"flag": 1, "yes": 0}, schema)
    with pytest.raises(ValueError):
        validate_schema({"flag": True, "no": 0}, schema)


def test_then_and_else_without_if_do_not_constrain_an_instance() -> None:
    validate_schema("unrelated instance", {"then": False, "else": False})
    validate_schema("unrelated instance", {"if": False})


@pytest.mark.parametrize(
    "schema",
    [
        {"allOf": []},
        {"allOf": {}},
        {"allOf": [False, 17]},
        {"if": 17},
        {"if": True, "then": 17},
        {"if": False, "else": 17},
        {"if": True, "then": True, "else": {"type": "unsupported"}},
        {"if": False, "then": {"type": "unsupported"}, "else": True},
    ],
)
def test_conditional_schema_defects_are_not_instance_mismatches(schema: JsonValue) -> None:
    with pytest.raises(ToolSchemaError):
        validate_schema("PRIVATE_REQUEST_VALUE", schema)
