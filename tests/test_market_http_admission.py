from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from scheduling_support import RecordingReactionScheduling
from source_support import (
    FixtureBudgetClock,
    fixture_http_admission,
    fixture_request_budgets,
    fixture_versions,
)

from tank.analysis.page_access import LivePageAccess, RecordedPageAccess, RecordedResultsOnlyPageReader
from tank.event_tools import EventToolService
from tank.http_admission import ConfiguredHttpAdmission
from tank.market.service import MarketService
from tank.settings import Settings
from tank.storage import MemoryRepository


async def test_market_retry_obeys_actual_long_feedback_on_the_same_admission_engine(tmp_path: Path) -> None:
    clock = FixtureBudgetClock()
    admission = fixture_http_admission(budgets=fixture_request_budgets(clock=clock))
    observed: list[float] = []
    repo = MemoryRepository()

    def response(request: httpx.Request) -> httpx.Response:
        observed.append(clock.elapsed)
        if len(observed) == 1:
            return httpx.Response(429, headers={"Retry-After": "120"})
        return httpx.Response(200, json={"observed": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        service = MarketService(
            repo,
            Settings(data_dir=tmp_path, _env_file=None),
            versions=fixture_versions(),
            admission=admission,
            http=client,
        )
        with patch("tank.market.service.utcnow", return_value=clock.now()):
            result = await service._request("GET", "https://example.org/metric")
        await service.close()
    assert result == {"observed": True}
    assert observed == [0.0, 120.0]


async def test_market_missing_retry_instruction_does_not_invent_a_host_block(tmp_path: Path) -> None:
    clock = FixtureBudgetClock()
    admission = fixture_http_admission(budgets=fixture_request_budgets(clock=clock))
    sends = 0

    def response(request: httpx.Request) -> httpx.Response:
        nonlocal sends
        sends += 1
        return httpx.Response(429)

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        service = MarketService(
            MemoryRepository(),
            Settings(data_dir=tmp_path, _env_file=None),
            versions=fixture_versions(),
            admission=admission,
            http=client,
        )
        with pytest.raises(RuntimeError, match="retry instruction unavailable"):
            await service._request("GET", "https://example.org/metric")
        await service.close()
    assert sends == 1 and clock.elapsed == 0.0


async def test_event_page_admits_actual_client_credentials_and_preserves_safe_evidence_url(
    tmp_path: Path,
) -> None:
    observed: list[str] = []
    clock = FixtureBudgetClock()
    admission = fixture_http_admission(budgets=fixture_request_budgets(clock=clock))

    def response(request: httpx.Request) -> httpx.Response:
        observed.append(request.url.params["apikey"])
        return httpx.Response(
            200, content=b"<html>Observed fact</html>", headers={"Content-Type": "text/html"}
        )

    repo = MemoryRepository()
    async with httpx.AsyncClient(
        params={"apikey": "actual key"}, transport=httpx.MockTransport(response)
    ) as client:
        tools = EventToolService(
            repo,
            Settings(data_dir=tmp_path, _env_file=None),
            None,
            None,
            versions=fixture_versions(),
            scheduler=RecordingReactionScheduling(),
            pages=LivePageAccess(admission),
            http=client,
        )
        result = await tools.fetch_page({"url": "https://api.etherscan.io/v2/api?chainid=1"}, {})
    assert observed == ["actual key"]
    assert "apikey" not in result["url"]
    assert result["page_read_provenance"]["mode"] == "LIVE_RESPONSE"


async def test_recorded_missing_raw_bytes_refuses_before_any_live_effect(tmp_path: Path) -> None:
    reader = RecordedResultsOnlyPageReader(input_sha256="a" * 64, captured_result_catalog_sha256="b" * 64)
    repo = MemoryRepository()
    tools = EventToolService(
        repo,
        Settings(data_dir=tmp_path, _env_file=None),
        None,
        None,
        versions=fixture_versions(),
        scheduler=RecordingReactionScheduling(),
        pages=RecordedPageAccess(reader),
    )
    admit = AsyncMock(side_effect=AssertionError("Live admission must not run"))
    block = AsyncMock(side_effect=AssertionError("Live feedback must not run"))
    sleep = AsyncMock(side_effect=AssertionError("Provider wait must not run"))
    with (
        patch.object(ConfiguredHttpAdmission, "admit", admit),
        patch.object(ConfiguredHttpAdmission, "block", block),
        patch.object(FixtureBudgetClock, "sleep", sleep),
        patch("httpx.AsyncClient", side_effect=AssertionError("Live HTTP client must not be created")),
        pytest.raises(ValueError, match="no original page response bytes"),
    ):
        await tools.fetch_page({"url": "https://api.etherscan.io/v2/api?chainid=1"}, {})
    admit.assert_not_awaited()
    block.assert_not_awaited()
    sleep.assert_not_awaited()
    assert await repo.list("ingest.source_items") == []
