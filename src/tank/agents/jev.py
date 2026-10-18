"""Official TypeSafe async SDK with Tank's existing injected HTTP transport.

SDK 0.7.5 uses httpx2; the rest of Tank uses httpx 0.28. This adapter bridges
only request/response I/O, leaving the official SDK responsible for Jev request
serialization, authentication, typed response validation and error handling.
"""

from dataclasses import dataclass

import httpx
import httpx2
from typesafe_sdk import AsyncTypeSafeClient, RetryPolicy

MODEL = "jev-1.13.0"
BASE_URL = "https://api.typesafe.ai"


@dataclass(frozen=True)
class JevBinding:
    provider: str
    base_url: str
    request_model: str
    resolved_models: tuple[str, ...]
    semantic_model: str = MODEL


_BINDINGS = {
    "typesafe": JevBinding("typesafe", BASE_URL, MODEL, (MODEL,)),
    "openrouter": JevBinding(
        "openrouter", "https://openrouter.ai/api", "jev-1.13", ("typesafe/jev-1.13-20260917",)
    ),
}


def provider_binding(provider):
    try:
        return _BINDINGS[provider]
    except KeyError as error:
        raise ValueError("Unsupported explicit Jev provider") from error


class _TankHttpTransport(httpx2.AsyncBaseTransport):
    def __init__(self, http):
        self.http = http

    async def handle_async_request(self, request):
        timeout = request.extensions.get("timeout")
        try:
            response = await self.http.request(
                request.method,
                str(request.url),
                headers=request.headers.multi_items(),
                content=await request.aread(),
                timeout=httpx.Timeout(**timeout) if timeout else self.http.timeout,
            )
        except httpx.TimeoutException as error:
            raise httpx2.ReadTimeout("TypeSafe request timed out", request=request) from error
        except httpx.TransportError as error:
            raise httpx2.TransportError("TypeSafe transport unavailable", request=request) from error
        # httpx has already decoded content encoding. Preserve all provider
        # metadata while preventing a second decompression in httpx2.Response.
        headers = [
            (name, value)
            for name, value in response.headers.multi_items()
            if name.lower() not in {"content-encoding", "content-length"}
        ]
        return httpx2.Response(
            response.status_code, headers=headers, content=response.content, request=request
        )

    async def aclose(self):
        # The caller owns the injected Tank client and its connection pool.
        return None


async def classify(http, *, api_key, state, questions, provider="typesafe"):
    binding = provider_binding(provider)
    timeout = httpx2.Timeout(
        connect=http.timeout.connect, read=http.timeout.read, write=http.timeout.write, pool=http.timeout.pool
    )
    async with AsyncTypeSafeClient(
        api_key=api_key,
        base_url=binding.base_url,
        model=binding.request_model,
        retry=RetryPolicy(max_retries=0),
        timeout=timeout,
        compression=False,
        transport=_TankHttpTransport(http),
    ) as sdk:
        result = await sdk.system_one(state=state, questions=questions, model=binding.request_model)
    data = result.model_dump(mode="json")
    raw = result.raw_http_response.json()
    # The SDK validates canonical decision fields but its Usage model ignores
    # OpenRouter's additional billing fields. Retain those authoritative values.
    if isinstance(raw.get("usage"), dict):
        data["usage"] = {**raw["usage"], **data["usage"]}
    for field in ("id", "provider"):
        if field in raw:
            data[field] = raw[field]
    if data["model"] not in binding.resolved_models:
        raise ValueError("Jev returned an unexpected resolved model for the pinned provider binding")
    return data
