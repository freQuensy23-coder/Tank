"""Pinned, typed Jev classification through OpenCode's System One endpoint (03 §3.1)."""

import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx

MODEL = "jev-1.13.0"


@dataclass(frozen=True)
class JevBinding:
    provider: str = "opencode"
    base_url: str = "https://opencode.ai/inference/systemone"
    request_model: str = "jev-1.13-free"
    resolved_models: tuple[str, ...] = ("jev-1.13-free",)
    semantic_model: str = MODEL


FREE = JevBinding()
KEYED = JevBinding(request_model="jev-1.13", resolved_models=("jev-1.13",))


def binding(api_key=None):
    return KEYED if api_key else FREE


def retry_after_seconds(value: str | None, now: datetime) -> float | None:
    if value is None:
        return None
    value = value.strip()
    if value.isascii() and value.isdecimal():
        digits = value.lstrip("0") or "0"
        return float(digits) if len(digits) <= 18 else 1e18
    try:
        date = parsedate_to_datetime(value)
        if date.tzinfo is None:
            return None
        return max(0, (date - now).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return None


class JevHTTPError(httpx.HTTPStatusError):
    """Provider status and correlation evidence without raw error bodies or credentials."""

    def __init__(self, response: httpx.Response, model: str, api_key: str | None):
        super().__init__(
            f"Jev HTTP error (HTTP {response.status_code})", request=response.request, response=response
        )
        self.http_status = response.status_code
        self.model = model
        self.retry_after_s = retry_after_seconds(response.headers.get("Retry-After"), datetime.now(UTC))
        request_id = response.headers.get("x-opencode-log-id") or response.headers.get("x-request-id") or ""
        self.request_id = (
            request_id
            if re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", request_id)
            and (not api_key or api_key not in request_id)
            else None
        )

    def diagnostics(self) -> dict:
        return {
            "provider": "opencode",
            "endpoint": FREE.base_url + "/v1/systemone",
            "model": self.model,
            "http_status": self.http_status,
            "message": httpx.codes.get_reason_phrase(self.http_status),
            "retry_after_s": self.retry_after_s,
            "request_id": self.request_id,
        }


class JevRateLimited(JevHTTPError):
    """Schedule a confirmed HTTP 429 durably instead of spending processing attempts."""


class JevBatchFailure(Exception):
    """A real routing failure whose retry must also honor concurrent rate-limit responses."""

    def __init__(self, error: Exception, retry_after_s: float):
        super().__init__("Jev routing batch failed")
        self.error = error
        self.retry_after_s = retry_after_s


async def classify(http, *, api_key=None, state, questions):
    pinned = binding(api_key)
    response = await http.post(
        pinned.base_url + "/v1/systemone",
        headers={"Authorization": "Bearer " + api_key} if api_key else {},
        json={"model": pinned.request_model, "state": state, "questions": questions},
    )
    if response.status_code == 429:
        raise JevRateLimited(response, pinned.request_model, api_key)
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError:
        raise JevHTTPError(response, pinned.request_model, api_key) from None
    data = response.json()
    if not isinstance(data, dict) or data.get("model") not in pinned.resolved_models:
        raise ValueError("Jev returned an unexpected resolved model for the pinned provider binding")
    if not isinstance(data.get("answers"), dict) or not isinstance(data.get("usage"), dict):
        raise TypeError("Jev returned an invalid decision response")
    for name in questions:
        answer = data["answers"].get(name)
        if (
            not isinstance(answer, dict)
            or answer.get("type") != "noul"
            or type(answer.get("noul")) not in (int, float)
            or not math.isfinite(answer["noul"])
            or not 0 <= answer["noul"] <= 1
        ):
            raise ValueError("Jev returned an invalid noul answer")
    return data
