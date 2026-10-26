"""Question prompts preserve reported context and do not invent missing native facts."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.contracts.telegram_state import QuestionOpsReference, QuestionUnreportedReference
from tank.question_prompts import question_message, question_prompt


def test_historical_unreported_kind_preserves_actual_text_and_reference_limit() -> None:
    reference = "x" * 1024 + "private-tail"
    prompt = question_prompt({"text": "А дальше?", "reference": {"text": reference}})
    assert isinstance(prompt.reference, QuestionUnreportedReference)
    assert prompt.reference.kind is None and prompt.reference.text == reference
    assert prompt.attachments is None
    text = question_message(prompt)
    assert "x" * 1024 in text and "private-tail" not in text and "А дальше?" in text
    assert "Tank Ops" not in text


def test_missing_reference_and_file_only_turn_do_not_invent_quote_or_mount() -> None:
    prompt = question_prompt(
        {
            "text": None,
            "reference": None,
            "attachments": [{"filename": "empty.csv", "size": 0}],
        }
    )
    assert prompt.reference is None and prompt.attachments is not None
    assert prompt.attachments[0].path is None and prompt.attachments[0].size == 0
    text = question_message(prompt)
    assert "(без текста)" in text and "empty.csv (0 байт): файл недоступен" in text
    assert "/workspace" not in text and "None" not in text


def test_ops_context_and_actual_mount_are_rendered_without_unconsumed_metadata() -> None:
    record: dict[str, JsonValue] = {
        "question_id": "q",
        "text": "  keep actual whitespace  ",
        "reference": {"kind": "OPS_CARD", "item_id": "itm", "text": "actual quote", "event_id": None},
        "attachments": [
            {
                "filename": "data.csv",
                "size": 7,
                "path": "/workspace/uploads/data.csv",
                "file_key": "private-key",
            }
        ],
    }
    prompt = question_prompt(record)
    assert isinstance(prompt.reference, QuestionOpsReference)
    text = question_message(prompt)
    assert "Tank Ops о материале itm" in text and "actual quote" in text
    assert "  keep actual whitespace  " in text and "/workspace/uploads/data.csv" in text
    assert "private-key" not in text and record["question_id"] == "q"


@pytest.mark.parametrize("kind", ["CARD", "ANSWER", "PROMPT", "USER_MESSAGE"])
def test_reported_nonops_reference_retains_kind_without_ops_claim(kind: str) -> None:
    prompt = question_prompt({"text": "", "reference": {"kind": kind, "text": "quote"}, "attachments": []})
    assert prompt.reference is not None and prompt.reference.kind == kind
    assert prompt.attachments == []
    assert "Tank Ops" not in question_message(prompt)


@pytest.mark.parametrize(
    "record",
    [
        {"reference": {"kind": "OPS_CARD", "text": "quote"}},
        {"reference": {"kind": "UNKNOWN", "text": "quote"}},
        {"attachments": [{"filename": "x"}]},
        {"attachments": [{"filename": "x", "size": None}]},
        {"attachments": [{"filename": "x", "size": True}]},
        {"attachments": [{"filename": "x", "size": 1, "path": ""}]},
    ],
)
def test_required_context_or_file_facts_refuse_instead_of_becoming_prompt(
    record: dict[str, JsonValue],
) -> None:
    with pytest.raises((ValueError, ValidationError)):
        question_prompt(record)
