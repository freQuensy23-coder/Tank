from datetime import UTC, datetime, timedelta

import httpx
import pytest
from source_support import FixtureBudgetClock, fixture_http_admission, fixture_request_budgets

from tank.http_transport import HttpAdmissionRefused, retry_after_deadline, send_admitted


def allow(request: httpx.Request) -> None:
    if request.url.scheme != "https":
        raise ValueError("HTTPS required")


async def test_each_redirect_admits_the_actual_request_and_calls_start_once() -> None:
    clock = FixtureBudgetClock()
    admission = fixture_http_admission(budgets=fixture_request_budgets(clock=clock))
    seen: list[str] = []
    starts: list[int] = []

    async def started() -> None:
        starts.append(len(seen))

    def response(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        if request.url.path == "/first":
            return httpx.Response(302, headers={"Location": "/second"})
        return httpx.Response(200, content=b"captured")

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        result = await send_admitted(
            client,
            admission,
            client.build_request("GET", "https://example.org/first"),
            allow,
            stream=False,
            on_start=started,
        )
    assert result.content == b"captured"
    assert seen == ["/first", "/second"] and starts == [0]
    assert clock.elapsed == 0.0


async def test_configured_admission_sees_client_query_credentials_before_each_attempt() -> None:
    clock = FixtureBudgetClock()
    admission = fixture_http_admission(budgets=fixture_request_budgets(clock=clock))
    seen: list[float] = []

    def response(request: httpx.Request) -> httpx.Response:
        assert request.url.params["apikey"] == "exact key"
        seen.append(clock.elapsed)
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(
        params={"apikey": "exact key"}, transport=httpx.MockTransport(response)
    ) as client:
        for _ in range(2):
            await send_admitted(
                client,
                admission,
                client.build_request("GET", "https://api.etherscan.io/v2/api?chainid=1"),
                allow,
                stream=False,
            )
    assert len(seen) == 2 and seen[1] > seen[0]


async def test_refused_redirect_never_reaches_transport() -> None:
    admission = fixture_http_admission()
    requests: list[str] = []

    def response(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.host)
        return httpx.Response(302, headers={"Location": "https://api.etherscan.io/unsupported"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        with pytest.raises(HttpAdmissionRefused, match="UNSUPPORTED_API_ROUTE"):
            await send_admitted(
                client,
                admission,
                client.build_request("GET", "https://example.org/start"),
                allow,
                stream=False,
            )
    assert requests == ["example.org"]


def test_retry_feedback_uses_actual_seconds_or_http_date_without_default_or_clamp() -> None:
    now = datetime(2026, 10, 1, tzinfo=UTC)
    assert retry_after_deadline("120", now) == now + timedelta(seconds=120)
    assert retry_after_deadline("Thu, 01 Oct 2026 00:02:00 GMT", now) == now + timedelta(seconds=120)
    assert retry_after_deadline("0", now) == now
    for invalid in (None, "NaN", "-1", "missing", "1e9999"):
        assert retry_after_deadline(invalid, now) is None
