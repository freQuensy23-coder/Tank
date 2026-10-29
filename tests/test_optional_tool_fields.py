"""Strict tool calls fill every declared field: null and "" in an optional field mean the field is absent."""

import json
from pathlib import Path

import pytest

from tank.agents.context import CONTEXT_VERSION, ModelContext
from tank.event_tools import absent_optionals, domain_schema, validate_schema
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, new_id, sha256

SKILLS = Path(__file__).parents[1] / "config/skills"


@pytest.fixture
async def boundary(tmp_path):
    repo = MemoryRepository()
    settings = Settings(data_dir=tmp_path, _env_file=None)
    item = {
        "item_id": new_id("itm"),
        "url": "https://example.org/launch",
        "title": "Product launch",
        "text": "Product is live.",
        "observed_at": iso(),
        "source_id": "S01",
        "source_class": "PRIMARY",
    }
    event = {"event_id": new_id("evt"), "statement": "World product is live", "lifecycle": "SCORED"}
    session = {
        "session_id": "session_optional_fields",
        "model_context_version": CONTEXT_VERSION,
        "item_id": item["item_id"],
        "event_id": event["event_id"],
        "prompt_version": sha256({"prompt": "test"}),
        "model_id": "test",
        "config_version": "test",
        "code_version": "test",
        "status": "OPEN",
        "last_payload": {"kind": "NEW_ITEM", "item_id": item["item_id"]},
    }
    await repo.put("ingest.source_items", item["item_id"], item)
    await repo.put("evt.events", event["event_id"], event)
    await repo.put("ops.agent_sessions", session["session_id"], session)
    return ModelContext(repo, settings, session), event


CARD = {"kind": "FULL", "what": "World launched", "verdict": "Live", "why": []}
MILESTONE = {"milestone": "Mainnet", "due_at": "2026-11-01T00:00:00Z", "source_id": "S15"}
CASES = [
    ("list_open_events", {"token_id": "", "entity_id": ""}, {}),
    ("list_open_events", {"token_id": None, "entity_id": "uniswap"}, {"entity_id": "uniswap"}),
    ("get_registry", {"token_id": "", "entity_id": None}, {}),
    ("get_registry", {"token_id": "uniswap:UNI", "entity_id": ""}, {"token_id": "uniswap:UNI"}),
    ("search_tank_items", {"query": "x", "days": "", "source_ids": None}, {"query": "x"}),
    (
        "search_tank_items",
        {"query": "x", "days": None, "source_ids": ["S14"]},
        {"query": "x", "source_ids": ["S14"]},
    ),
    ("fetch_page", {"url": "https://example.org/a", "timeout_s": ""}, {"url": "https://example.org/a"}),
    ("fetch_page", {"url": "https://example.org/a", "timeout_s": None}, {"url": "https://example.org/a"}),
    ("publish_card", {**CARD, "unconfirmed": ""}, CARD),
    ("publish_card", {**CARD, "unconfirmed": None}, CARD),
    ("schedule_milestone", {**MILESTONE, "due_at_prec": "", "due_at_bucket_size_s": ""}, MILESTONE),
    ("schedule_milestone", {**MILESTONE, "due_at_prec": None, "due_at_bucket_size_s": None}, MILESTONE),
]


@pytest.mark.parametrize(("tool", "sent", "kept"), CASES)
async def test_null_and_empty_optional_fields_are_absent(boundary, tool, sent, kept):
    context, event = boundary
    bound = {"event_id": event["event_id"]} if tool in {"publish_card", "schedule_milestone"} else {}
    assert await context.arguments(tool, sent) == {**kept, **bound}


@pytest.mark.parametrize(
    ("tool", "sent", "message"),
    [
        (
            "list_open_events",
            {"token_id": "nope:NOPE", "entity_id": ""},
            r"\$\.token_id: not a registry token_id",
        ),
        ("get_registry", {"entity_id": "Uniswap Labs", "token_id": None}, r"\$\.entity_id: not a registry"),
        ("search_tank_items", {"query": None, "days": None}, r"\$\.query: invalid type"),
        ("search_tank_items", {"days": ""}, r"\$: missing query"),
        ("search_tank_items", {"query": "x", "days": 0}, r"\$\.days: violates minimum"),
        (
            "fetch_page",
            {"url": "https://example.org/a", "timeout_s": 0},
            r"\$\.timeout_s: must be greater than 0",
        ),
        ("publish_card", {**CARD, "unconfirmed": "   "}, r"\$\.unconfirmed: does not match pattern"),
        ("publish_card", {**CARD, "what": ""}, r"\$\.what: does not match pattern"),
        (
            "schedule_milestone",
            {**MILESTONE, "due_at_bucket_size_s": -1},
            r"\$\.due_at_bucket_size_s: must be",
        ),
        ("schedule_milestone", {**MILESTONE, "source_id": ""}, r"\$\.source_id: not a registered source_id"),
    ],
)
async def test_required_and_bad_provided_values_are_still_refused(boundary, tool, sent, message):
    context, _ = boundary
    with pytest.raises(ValueError, match=message):
        await context.arguments(tool, sent)


def test_a_rejection_still_needs_its_reason_and_an_event_ignores_an_empty_one():
    from test_event_tools import extraction

    schema = domain_schema(Settings(_env_file=None), "record_event")
    value = extraction()
    validate_schema(absent_optionals({**value, "no_event_reason": ""}, schema), schema)
    validate_schema(absent_optionals({**value, "no_event_reason": None}, schema), schema)
    for reason in ("", None):
        rejected = absent_optionals({**value, "is_event": False, "no_event_reason": reason}, schema)
        with pytest.raises(ValueError, match="matches none of the allowed forms"):
            validate_schema(rejected, schema)


def test_nested_optional_fields_are_normalised_and_required_ones_kept():
    schema = {
        "type": "object",
        "required": ["claims"],
        "properties": {
            "claims": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["quote"],
                    "properties": {"quote": {"type": "string"}, "evidence": {"type": ["array", "null"]}},
                },
            },
        },
    }
    assert absent_optionals({"claims": [{"quote": "", "evidence": None}]}, schema) == {
        "claims": [{"quote": ""}]
    }


def optional_fields(schema, path="$"):
    required = set(schema.get("required", []))
    for key, field in schema.get("properties", {}).items():
        if key not in required:
            yield f"{path}.{key}", field
        yield from optional_fields(field, f"{path}.{key}")
        if isinstance(field.get("items"), dict):
            yield from optional_fields(field["items"], f"{path}.{key}[]")


def accepts_null(field):
    kinds = field.get("type", ["null"])
    return (
        "null" in ([kinds] if isinstance(kinds, str) else kinds)
        and None in field.get("enum", [None])
        and any(option.get("type") == "null" for option in field.get("anyOf", [{"type": "null"}]))
    )


@pytest.mark.parametrize("tools", sorted(SKILLS.glob("*/tools.json")), ids=lambda path: path.parent.name)
def test_every_optional_tool_field_is_declared_nullable_and_matches_its_schema_file(tools):
    for tool in json.loads(tools.read_text()):
        schema = tool["input_schema"]
        loose = [path for path, field in optional_fields(schema) if not accepts_null(field)]
        assert not loose, (tool["name"], loose)
        separate = tools.parent / f"{tool['name']}.schema.json"
        if separate.exists():
            declared = json.loads(separate.read_text())
            assert schema == {
                k: v for k, v in declared.items() if k not in {"$schema", "title", "description"}
            }
