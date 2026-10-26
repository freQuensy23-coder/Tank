"""Spot direction is a discrete integer classification, not a numeric coefficient."""

import json
from decimal import Decimal

import pytest
from pydantic import JsonValue, ValidationError

from tank.analysis.card_contracts import CardSpotVenue


@pytest.mark.parametrize("value", [True, False, -1.0, 0.0, 1.0, "1", 2, None])
def test_spot_sign_refuses_boolean_float_and_unknown_classes(value: JsonValue) -> None:
    with pytest.raises(ValidationError):
        CardSpotVenue.model_validate_json(
            json.dumps({"venue": "authored", "sign": value, "volume_share": "0.5"})
        )


@pytest.mark.parametrize("value", [-1, 0, 1])
def test_actual_integer_spot_signs_preserve_financial_share(value: int) -> None:
    original = CardSpotVenue.model_validate_json(
        json.dumps({"venue": "authored", "sign": value, "volume_share": "0.5"})
    )
    assert type(original.sign) is int and original.sign == value
    assert original.volume_share == Decimal("0.5")
    assert CardSpotVenue.model_validate_json(original.model_dump_json()) == original


def test_known_zero_direction_does_not_require_an_invented_volume_share() -> None:
    observed = CardSpotVenue(venue="authored", sign=0, volume_share=None)
    assert observed.sign == 0 and observed.volume_share is None
