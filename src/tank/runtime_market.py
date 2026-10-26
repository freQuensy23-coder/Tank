"""Market operations consumed by Runtime's capture and service lifecycle."""

import asyncio
from collections.abc import Sequence
from datetime import datetime
from typing import Protocol

from tank.market.capture_contracts import (
    CaptureReadiness,
    CaptureWaitOutcome,
    RepairOutcome,
    TokenEscalation,
)
from tank.time_precision import Timestamp


class RuntimeMarketPort(Protocol):
    async def escalate(self, event_id: str, tokens: Sequence[str | TokenEscalation]) -> None: ...

    async def capture_readiness(
        self, event_id: str, token_id: str, *, anchor: Timestamp | None
    ) -> CaptureReadiness: ...

    async def wait_capture(self, event_id: str, token_id: str, *, timeout_s: float) -> CaptureWaitOutcome: ...

    async def repair_windows(
        self, token_id: str, start: datetime, end: datetime, *, budget_s: float
    ) -> RepairOutcome: ...

    async def run(self, stop: asyncio.Event) -> None: ...

    async def close(self) -> None: ...
