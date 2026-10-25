"""Execute admitted Python requests with explicit redirect and retry feedback boundaries."""

from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime

import httpx

from tank.contracts.http_policy import HttpAdmission, HttpRefused
from tank.util import parse_iso_time

type RequestGuard = Callable[[httpx.Request], None]
type RequestStarted = Callable[[], Awaitable[None]]


class HttpAdmissionRefused(RuntimeError):
    def __init__(self, refusal: HttpRefused) -> None:
        self.refusal = refusal
        super().__init__(f"{refusal.host}: {refusal.reason}")


async def send_admitted(
    client: httpx.AsyncClient,
    admission: HttpAdmission,
    request: httpx.Request,
    guard: RequestGuard,
    *,
    stream: bool,
    on_start: RequestStarted | None = None,
) -> httpx.Response:
    for redirect in range(client.max_redirects + 1):
        guard(request)
        decision = await admission.admit(request)
        if decision.host != request.url.host:
            raise ValueError("HTTP admission destination differs from the actual request")
        if isinstance(decision, HttpRefused):
            raise HttpAdmissionRefused(decision)
        if redirect == 0 and on_start is not None:
            await on_start()
        response = await client.send(request, follow_redirects=False, stream=stream)
        if response.next_request is None:
            return response
        request = response.next_request
        await response.aclose()
    raise RuntimeError("HTTP redirect limit exceeded")


def retry_after_deadline(value: str | None, observed_at: datetime) -> datetime | None:
    now = parse_iso_time(observed_at)
    if value is None:
        return None
    try:
        seconds = Decimal(value)
    except InvalidOperation:
        try:
            deadline = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if deadline.tzinfo is None or deadline.utcoffset() is None:
            return None
        deadline = parse_iso_time(deadline)
        return deadline if deadline >= now else now
    if not seconds.is_finite() or seconds < 0:
        return None
    try:
        return now + timedelta(seconds=float(seconds))
    except OverflowError:
        return None
