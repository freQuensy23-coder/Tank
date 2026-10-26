"""Explicit worker delay policy preserves absent provider facts as application backoff."""

from typing import Literal

from pydantic import Field

from tank.contracts import DomainModel
from tank.delivery import DeliveryError
from tank.telegram_api import TelegramResponseError


class ApplicationBackoff(DomainModel):
    basis: Literal["APPLICATION_BACKOFF"]
    attempts: int = Field(ge=0)

    @property
    def seconds(self) -> float:
        return 60.0 if self.attempts >= 6 else float(2**self.attempts)


class TelegramRetryDelay(DomainModel):
    basis: Literal["TELEGRAM_RETRY_AFTER"]
    seconds: int = Field(gt=0)


class DeliveryRetryDelay(DomainModel):
    basis: Literal["DELIVERY_DIRECTIVE"]
    seconds: int | float = Field(ge=0)


type WorkerRetrySchedule = ApplicationBackoff | TelegramRetryDelay | DeliveryRetryDelay


def worker_retry_schedule(error: Exception, attempts: int) -> WorkerRetrySchedule:
    if isinstance(error, TelegramResponseError):
        if error.retry_after is not None:
            return TelegramRetryDelay(basis="TELEGRAM_RETRY_AFTER", seconds=error.retry_after)
    elif isinstance(error, DeliveryError):
        return DeliveryRetryDelay(basis="DELIVERY_DIRECTIVE", seconds=error.retry_after)
    return ApplicationBackoff(basis="APPLICATION_BACKOFF", attempts=attempts)
