"""Unknown exposure is not a zero estimate or an invalid sortable number."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.analysis.publication_ordering import exposure_priority_from_record


def test_known_zero_remains_a_known_score_ahead_of_unavailable_exposure() -> None:
    rows: list[dict[str, JsonValue]] = [
        {"exposure_score": None},
        {"exposure_score": 0},
        {"exposure_score": 7},
        {"exposure_score": 10},
        {"exposure_score": 0},
    ]
    ordered = sorted(rows, key=exposure_priority_from_record)
    assert [row["exposure_score"] for row in ordered] == [10, 7, 0, 0, None]
    assert rows[0]["exposure_score"] is None


@pytest.mark.parametrize("value", [True, "8", 8.1, {}, [], 11, -1])
def test_invalid_persisted_exposure_does_not_acquire_a_numeric_rank(value: JsonValue) -> None:
    with pytest.raises(ValidationError):
        exposure_priority_from_record({"exposure_score": value})


def test_missing_required_score_is_distinct_from_explicit_unavailable() -> None:
    with pytest.raises(ValueError, match="actual exposure score"):
        exposure_priority_from_record({})
    assert exposure_priority_from_record({"exposure_score": None}) == (True, None)
