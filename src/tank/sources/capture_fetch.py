"""Exchange HTTP adapter through the durable tank-capture queue."""

import asyncio
import base64
import binascii
import builtins
import time
from collections.abc import Mapping
from datetime import timedelta
from typing import Literal, Protocol, Self
from urllib.parse import urlsplit

import httpx
from pydantic import Field, JsonValue, model_validator

from tank.contracts import DomainModel
from tank.time_precision import Timestamp, collection_clock
from tank.util import iso, sha256, utcnow

EXCHANGE_FETCH_HOSTS = frozenset(
    {
        "api.binance.com",
        "fapi.binance.com",
        "www.binance.com",
        "data.binance.vision",
        "api.exchange.coinbase.com",
        "api.kraken.com",
        "www.okx.com",
        "openapi.okx.com",
        "api.bybit.com",
        "api.hyperliquid.xyz",
    }
)
FETCH_BUDGET_S = 45.0
FETCH_GRACE_S = 3.0
TERMINAL = ("COMPLETE", "FAILED", "EXPIRED")
CONDITIONAL_HEADERS = ("if-none-match", "if-modified-since")


class CaptureRepository(Protocol):
    async def get(self, collection: str, key: str) -> dict[str, JsonValue] | None: ...
    async def put(self, collection: str, key: str, record: dict[str, JsonValue]) -> dict[str, JsonValue]: ...
    async def delete(self, collection: str, key: str) -> None: ...


class FetchRequest(DomainModel):
    method: Literal["GET", "POST"]
    url: str = Field(min_length=1)
    source_id: str | None
    headers: dict[str, str] = Field(default_factory=dict)
    body: JsonValue = None

    @model_validator(mode="after")
    def allowed_destination(self) -> Self:
        parts = urlsplit(self.url)
        if parts.scheme != "https" or parts.hostname not in EXCHANGE_FETCH_HOSTS:
            raise ValueError("Capture fetch requires an allowed HTTPS exchange host")
        return self


class FetchResult(DomainModel):
    status_code: int = Field(ge=100, le=599)
    content_type: str
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    fetched_at: str
    bytes: int = Field(ge=0)
    source_id: str | None
    body: str | None = None
    body_b64: str | None = None

    @model_validator(mode="after")
    def body_encoding(self) -> Self:
        if (self.body is None) == (self.body_b64 is None):
            raise ValueError("Capture response requires exactly one body representation")
        content = self.content()
        if len(content) != self.bytes or sha256(content) != self.sha256:
            raise ValueError("Capture response body does not match retained byte count and digest")
        return self

    def content(self) -> builtins.bytes:
        if self.body_b64 is not None:
            try:
                return base64.b64decode(self.body_b64, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise ValueError("Capture response contains invalid base64") from exc
        if self.body is None:
            raise ValueError("Capture response body is absent")
        return self.body.encode()


class CaptureState(DomainModel):
    status: Literal["PENDING", "RUNNING", "COMPLETE", "FAILED", "EXPIRED"]
    reason: str | None
    result: JsonValue


def is_exchange_url(url: str) -> bool:
    return urlsplit(url).hostname in EXCHANGE_FETCH_HOSTS


async def _capture_request(
    repo: CaptureRepository, request: FetchRequest, budget_s: float
) -> tuple[str, CaptureState | None]:
    """Serialize at the queue boundary; a missing row remains a missing answer."""
    if budget_s <= 0:
        raise ValueError("Capture fetch budget must be positive")
    requested = utcnow()
    request_id = "CF-" + sha256(
        {"url": request.url, "method": request.method, "requested_at": iso(requested)}
    )
    pending = Timestamp(None, None, "PENDING")
    await repo.put(
        "mkt.capture_requests",
        request_id,
        {
            "request_id": request_id,
            "kind": "HTTP_FETCH",
            "request": request.model_dump(mode="json"),
            "budget_s": budget_s,
            **collection_clock(requested).fields("requested_at"),
            **collection_clock(requested + timedelta(seconds=budget_s)).fields("deadline"),
            **pending.fields("started_at"),
            **pending.fields("finished_at"),
            "status": "PENDING",
            "reason": None,
            "result": None,
        },
    )
    until = time.monotonic() + budget_s + FETCH_GRACE_S
    state = None
    while time.monotonic() < until:
        row = await repo.get("mkt.capture_requests", request_id)
        if row is not None:
            state = CaptureState.model_validate(
                {field: row[field] for field in ("status", "reason", "result")}
            )
            if state.status in TERMINAL:
                break
        await asyncio.sleep(0.2)
    return request_id, state


async def capture_fetch(
    repo: CaptureRepository,
    method: str,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    body: JsonValue = None,
    source_id: str | None = None,
    budget_s: float = FETCH_BUDGET_S,
) -> httpx.Response:
    """Validate the capture response before presenting it as an HTTP response."""
    request = FetchRequest.model_validate(
        {
            "method": method.upper(),
            "url": url,
            "source_id": source_id,
            "headers": {
                key: value for key, value in headers.items() if key.lower() not in CONDITIONAL_HEADERS
            }
            if headers is not None
            else {},
            "body": body,
        }
    )
    request_id, state = await _capture_request(repo, request, budget_s)
    http_request = httpx.Request(request.method, request.url)
    if state is None or state.status != "COMPLETE":
        if state is not None and state.status in TERMINAL:
            await repo.delete("mkt.capture_requests", request_id)
        reason = (
            "NO_ANSWER" if state is None else (state.reason if state.reason is not None else state.status)
        )
        raise httpx.TransportError(f"tank-capture fetch {request_id}: {reason}", request=http_request)
    result = FetchResult.model_validate(state.result)
    content = result.content()
    await repo.delete("mkt.capture_requests", request_id)
    response_headers = {"content-type": result.content_type} if result.content_type else {}
    return httpx.Response(result.status_code, content=content, headers=response_headers, request=http_request)
