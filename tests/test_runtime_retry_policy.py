"""Provider absence and application policy remain distinct retry facts."""

import pytest
from pydantic import JsonValue, ValidationError

from tank.delivery import DeliveryError
from tank.runtime_retry import (
    ApplicationBackoff,
    DeliveryRetryDelay,
    TelegramRetryDelay,
    worker_retry_schedule,
)
from tank.telegram_api import TelegramResponseError


@pytest.mark.parametrize(("attempts", "seconds"), [(0, 1), (1, 2), (5, 32), (6, 60), (10**9, 60)])
def test_backoff_uses_actual_count_and_saturates_without_exponentiating_unbounded_input(
    attempts: int, seconds: int
) -> None:
    error = TelegramResponseError("sendMessage", 400, retry_after=None)
    schedule = worker_retry_schedule(error, attempts)
    assert isinstance(schedule, ApplicationBackoff) and schedule.seconds == seconds
    assert error.retry_after is None


def test_reported_delay_and_delivery_zero_are_not_treated_as_absence() -> None:
    telegram = TelegramResponseError("sendMessage", 429, retry_after=7)
    reported = worker_retry_schedule(telegram, 1)
    assert isinstance(reported, TelegramRetryDelay) and reported.seconds == 7
    assert telegram.retry_after == 7
    delivery = worker_retry_schedule(DeliveryError("Known immediate retry", retry_after=0), 1)
    assert isinstance(delivery, DeliveryRetryDelay) and delivery.seconds == 0


@pytest.mark.parametrize("attempts", [None, True, "1", -1])
def test_invalid_attempt_cannot_acquire_a_default_counter(attempts: JsonValue) -> None:
    with pytest.raises(ValidationError):
        ApplicationBackoff.model_validate({"basis": "APPLICATION_BACKOFF", "attempts": attempts})
