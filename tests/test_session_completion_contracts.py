"""Known attempt ordering is required for failed-input supersession."""

import pytest
from pydantic import ValidationError

from tank.contracts.session_completion import (
    CompletionAttemptOwner,
    CompletionMessage,
    FailedToolAttempt,
    SourceCompletionEntry,
    SourceCompletionSnapshot,
)


@pytest.mark.parametrize(
    "previous,current,replacement,same_owner,expected",
    [
        (None, 1, "next", True, False),
        (0, None, "next", True, False),
        (0, 1, None, True, False),
        (0, 1, "next", False, False),
        (0, 0, "next", True, False),
        (1, 0, "next", True, False),
        (0, 1, "next", True, True),
        (1, 2, "next", True, True),
    ],
)
def test_failed_attempt_requires_actual_order_and_replacement(
    previous: int | None, current: int | None, replacement: str | None, same_owner: bool, expected: bool
) -> None:
    owner = CompletionAttemptOwner(message_id="owner", attempt=current, replacement_input_id=replacement)
    prior = FailedToolAttempt(
        input_id="prior", message_id="owner" if same_owner else "other", attempt=previous
    )
    assert prior.is_prior_to(owner) is expected


@pytest.mark.parametrize("invalid", [True, False, -1, 0.0, "0"])
def test_reported_attempt_requires_strict_nonnegative_integer(invalid: bool | int | float | str) -> None:
    with pytest.raises(ValidationError):
        CompletionAttemptOwner.model_validate(
            {"message_id": "owner", "attempt": invalid, "replacement_input_id": "input"}
        )
    with pytest.raises(ValidationError):
        FailedToolAttempt.model_validate({"input_id": "prior", "message_id": "owner", "attempt": invalid})
    with pytest.raises(ValidationError):
        CompletionMessage.from_record({"message_id": "owner", "retry_count": invalid})


def test_unreported_completion_is_distinct_from_known_zero_through_storage() -> None:
    entry = SourceCompletionEntry(
        version=4, item_id=None, native_input_id="actual", retry_attempt=None, outcome=None
    )
    snapshot = SourceCompletionSnapshot(entries={"owner": entry})
    stored = snapshot.storage_record()
    assert stored == {
        "owner": {
            "version": 4,
            "item_id": None,
            "native_input_id": "actual",
            "retry_attempt": None,
            "outcome": None,
        }
    }
    assert SourceCompletionEntry.model_validate(stored["owner"]).retry_attempt is None
    zero = SourceCompletionEntry(
        version=4, item_id=None, native_input_id="actual", retry_attempt=0, outcome="NOT_APPLICABLE"
    )
    assert zero.retry_attempt == 0 and zero != entry


def test_failed_input_cannot_be_its_own_replacement() -> None:
    owner = CompletionAttemptOwner(message_id="owner", attempt=1, replacement_input_id="same")
    prior = FailedToolAttempt(input_id="same", message_id="owner", attempt=0)
    assert prior.is_prior_to(owner) is False


@pytest.mark.parametrize("version", [4.0, "4", True, 3])
def test_completion_schema_version_is_an_actual_integer_four(version: float | str | bool | int) -> None:
    with pytest.raises(ValidationError):
        SourceCompletionEntry.model_validate(
            {
                "version": version,
                "item_id": None,
                "native_input_id": None,
                "retry_attempt": None,
                "outcome": None,
            }
        )
