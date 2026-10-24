"""Pure policy for permanent MAJOR+ evidence windows and benchmark identities."""

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

from pydantic import Field, field_validator

from tank.contracts import DomainModel

PROTECTED_EMS_CLASSES = ("MAJOR", "STRUCTURAL", "STRUCTURAL_SHOCK")
BENCHMARK_SYMBOLS = ("BTC", "ETH")
BASE_BENCHMARK_TOKENS = frozenset({"bitcoin:BTC", "ethereum:ETH", "BTC", "ETH"})
PRE_EVENT_WINDOW = timedelta(hours=24)
POST_EVENT_WINDOW = timedelta(hours=72)


class BenchmarkToken(DomainModel):
    token_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)


class RetentionEvidence(DomainModel):
    event_id: str = Field(min_length=1)
    anchors: tuple[datetime, ...]
    token_ids: frozenset[str]

    @field_validator("anchors")
    @classmethod
    def require_aware_anchors(cls, values: tuple[datetime, ...]) -> tuple[datetime, ...]:
        if any(value.tzinfo is None or value.utcoffset() is None for value in values):
            raise ValueError("Retention anchors require a declared timezone")
        return values


class RetentionWindow(DomainModel):
    event_id: str
    token_id: str
    start: datetime
    end: datetime


def benchmark_token_ids(tokens: Iterable[BenchmarkToken]) -> frozenset[str]:
    selected = set(BASE_BENCHMARK_TOKENS)
    selected.update(token.token_id for token in tokens if token.symbol in BENCHMARK_SYMBOLS)
    return frozenset(selected)


def protected_windows(events: Iterable[RetentionEvidence]) -> list[RetentionWindow]:
    windows: list[RetentionWindow] = []
    for event in events:
        if not event.anchors:
            continue
        anchors = tuple(value.astimezone(UTC) for value in event.anchors)
        start = min(anchors) - PRE_EVENT_WINDOW
        end = max(anchors) + POST_EVENT_WINDOW
        windows.extend(
            RetentionWindow(event_id=event.event_id, token_id=token, start=start, end=end)
            for token in event.token_ids
        )
    return windows
