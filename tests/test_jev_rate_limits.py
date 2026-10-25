"""Regression coverage for provider 429 responses, durable scheduling and explicit recovery."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from click.testing import CliRunner

import tank.cli as cli
import tank.jev_recovery as recovery
import tank.runtime as runtime_module
import tank.storage as storage_module
import tank.util as util
from tank.agents.jev import JevRateLimited, classify, retry_after_seconds
from tank.agents.pipeline import Pipeline
from tank.jev_recovery import JEV_RATE_LIMIT_ERROR, defer_jev_job, retry_jev_job
from tank.runtime import Runtime
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock, offset_schedule
from tank.util import parse_time

URL = "https://opencode.ai/inference/systemone/v1/systemone"
REQUEST_ID = "01a172e1-8e9f-7653-9bc2-f91997ff7190"
NOW = datetime(2026, 10, 25, 11, 31, 37, tzinfo=UTC)


@pytest.fixture
def clock(monkeypatch):
    current = [NOW]
    for module in (util, recovery, runtime_module, storage_module):
        monkeypatch.setattr(module, "utcnow", lambda: current[0])
    monkeypatch.setattr(recovery.random, "uniform", lambda low, high: 0)
    return current


def options():
    return SimpleNamespace(
        replay=False,
        opencode_api_key="PRIVATE_API_KEY",
        config_version="test",
        code_version="test",
        ops_progress_enabled=False,
        load_config=lambda name: {"filter_cutoff": 0.5},
    )


def item():
    return {
        "item_id": "bloomberg-item",
        "source": "Bloomberg",
        "author": "Bloomberg",
        "title": "IndiGo, Air India to Shift Some Flights to New Mumbai Airport",
        "text": "IndiGo and Air India will move some of its international operations to Navi Mumbai International Airport.",
        "source_id": "S31",
        "feed": "https://feeds.bloomberg.com/markets/news.rss",
        "external_id": "TNGIXPR24U8300",
        "observed_at": NOW.isoformat(),
        "published_prec": "COLLECTION_ONLY",
        "source_class": "SECONDARY",
        "raw_ref": "/test/raw/bloomberg.json.zst",
        "raw_sha256": "a" * 64,
        "content_hash": "b" * 64,
        "relevance": {"decision": None, "pending_reason": "AWAITING_FILTER"},
        "extraction_status": "PENDING",
    }


def provider_response(status, *, delay="13", score=0.01, request=None):
    if status != 200:
        return httpx.Response(
            status,
            headers={"Retry-After": delay, "x-opencode-log-id": REQUEST_ID},
            json={"error": {"message": "Rate limit exceeded PRIVATE_API_KEY private-input"}},
            request=request,
        )
    return httpx.Response(
        200,
        json={
            "model": "jev-1.13",
            "answers": {"relevant": {"type": "noul", "noul": score}},
            "usage": {"input_tokens": 470},
        },
        request=request,
    )


def rate_limit(delay="13"):
    request = httpx.Request("POST", URL, headers={"Authorization": "Bearer PRIVATE_API_KEY"})
    return JevRateLimited(provider_response(429, delay=delay, request=request), "jev-1.13", "PRIVATE_API_KEY")


async def seeded(repo, *, expires_at=None, kind="FILTER_ITEM"):
    await repo.put("ingest.source_items", item()["item_id"], item())
    return await repo.enqueue(
        kind, {"item_id": item()["item_id"]}, item()["item_id"], max_attempts=2, expires_at=expires_at
    )


async def run_once(repo, handler, monkeypatch, *, kind="FILTER_ITEM"):
    stop = asyncio.Event()
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.settings = options()
    runtime.handlers = {kind: handler}
    runtime.terminal_delivery_failure = AsyncMock()
    original = repo.claim_job

    async def claim():
        result = await original()
        stop.set()
        return result

    with monkeypatch.context() as patch:
        patch.setattr(repo, "claim_job", claim)
        await runtime.worker(stop)
    return runtime


@pytest.mark.parametrize(
    "value,expected",
    [
        ("13", 13),
        ("0", 0),
        (" 31 ", 31),
        ("Sun, 25 Oct 2026 11:32:00 GMT", 23),
        ("Sun, 25 Oct 2026 11:30:00 GMT", 0),
        (None, None),
        ("", None),
        ("-5", None),
        ("1.5", None),
        ("nan", None),
        ("inf", None),
        ("tomorrow", None),
        ("Sun, 25 Oct 2026 11:32:00", None),
        ("9" * 1000, 1e18),
    ],
)
def test_retry_after_accepts_delta_or_http_date_and_rejects_invalid_headers(value, expected):
    assert retry_after_seconds(value, NOW) == expected


async def test_classify_exposes_safe_diagnostics_without_sleep_or_raw_error_leaks():
    calls = []

    def transport(request):
        calls.append(request)
        return provider_response(429)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        with pytest.raises(JevRateLimited) as captured:
            await classify(http, api_key="PRIVATE_API_KEY", state={"private": "input"}, questions={})
    assert len(calls) == 1
    error = captured.value
    assert error.retry_after_s == 13
    assert error.diagnostics()["request_id"] == REQUEST_ID
    assert error.diagnostics()["http_status"] == 429
    assert "PRIVATE_API_KEY" not in json.dumps(error.diagnostics()) + str(error)
    assert "private-input" not in json.dumps(error.diagnostics()) + str(error)


@pytest.mark.parametrize("request_id", ["PRIVATE_API_KEY", "secret\nrequest", "a" * 1000, REQUEST_ID])
def test_request_id_cannot_echo_secret_or_arbitrary_provider_text(request_id):
    response = httpx.Response(
        429, headers={"x-opencode-log-id": request_id}, request=httpx.Request("POST", URL)
    )
    error = JevRateLimited(response, "jev-1.13", REQUEST_ID)
    assert error.request_id is None


@pytest.mark.parametrize("score,decision", [(0.01, "SKIP"), (0.99, "SEND")])
async def test_real_worker_recovers_after_two_429s_and_restart_without_duplicate_effects(
    clock, monkeypatch, score, decision
):
    repo = MemoryRepository()
    job = await seeded(repo)
    requests = []
    statuses = iter([429, 429, 200])

    def transport(request):
        requests.append(request)
        return provider_response(next(statuses), score=score)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        pipeline = Pipeline(repo, options(), http=http)
        for count in (1, 2):
            runtime = await run_once(repo, pipeline.filter_item, monkeypatch)
            row = await repo.get("ops.jobs", job["job_id"])
            assert row["status"] == "QUEUED" and row["attempts"] == 0
            assert row["jev_rate_limit_count"] == count
            assert row["last_error"] == JEV_RATE_LIMIT_ERROR
            assert row["jev_rate_limit"]["http_status"] == 429
            assert parse_time(row["run_after"]) == clock[0] + timedelta(seconds=13)
            assert row["expires_at"] == job["expires_at"]
            runtime.terminal_delivery_failure.assert_not_called()
            assert await repo.list("ops.llm_usage") == []
            assert (await repo.get("ingest.source_items", item()["item_id"]))["relevance"]["decision"] is None
            await run_once(repo, pipeline.filter_item, monkeypatch)
            assert len(requests) == count
            clock[0] = parse_time(row["run_after"])
        await run_once(repo, pipeline.filter_item, monkeypatch)
        row = await repo.get("ops.jobs", job["job_id"])
        assert row["status"] == "DONE" and row["attempts"] == 1
        assert row["last_error"] is None
        assert row["jev_rate_limit_count"] == 2
        await pipeline.filter_item(job["payload"])
        assert len(requests) == 3
    assert (await repo.get("ingest.source_items", item()["item_id"]))["relevance"]["decision"] == decision
    assert len(await repo.list("ops.llm_usage")) == 1
    assert len(await repo.list("ops.jobs", filters={"type": "ROUTE_ITEM"})) == (decision == "SEND")
    assert await repo.list("out.alerts") == []
    assert await repo.list("ops.agent_sessions") == []


async def test_429_only_refunds_its_own_attempt_and_does_not_mask_permanent_http_errors(clock, monkeypatch):
    repo = MemoryRepository()
    job = await seeded(repo)
    statuses = iter([429, 401, 429, 401])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: provider_response(next(statuses)))
    ) as http:
        pipeline = Pipeline(repo, options(), http=http)
        for attempts in (0, 1, 1, 2):
            runtime = await run_once(repo, pipeline.filter_item, monkeypatch)
            row = await repo.get("ops.jobs", job["job_id"])
            assert row["attempts"] == attempts
            clock[0] = parse_time(row["run_after"])
    assert row["status"] == "FAILED" and row["last_error"] == "JevHTTPError: HTTP 401"
    runtime.terminal_delivery_failure.assert_awaited_once()
    assert await repo.list("ops.llm_usage") == []


@pytest.mark.parametrize("delay", ["13", "invalid", "9" * 1000])
async def test_outage_expires_without_an_extra_provider_call_or_extended_deadline(clock, monkeypatch, delay):
    repo = MemoryRepository()
    deadline = offset_schedule(collection_clock(clock[0]), timedelta(seconds=1))
    job = await seeded(repo, expires_at=deadline)
    calls = []

    async def handle(payload):
        calls.append(payload)
        raise rate_limit(delay)

    await run_once(repo, handle, monkeypatch)
    row = await repo.get("ops.jobs", job["job_id"])
    assert row["run_after"] == job["expires_at"]
    clock[0] = parse_time(row["run_after"])
    await run_once(repo, handle, monkeypatch)
    row = await repo.get("ops.jobs", job["job_id"])
    assert row["status"] == "EXPIRED"
    assert len(calls) == 1


async def test_late_429_response_expires_immediately(clock, monkeypatch):
    repo = MemoryRepository()
    job = await seeded(repo)

    async def handle(payload):
        clock[0] = parse_time(job["expires_at"])
        raise rate_limit()

    runtime = await run_once(repo, handle, monkeypatch)
    assert (await repo.get("ops.jobs", job["job_id"]))["status"] == "EXPIRED"
    runtime.terminal_delivery_failure.assert_awaited_once()


async def test_fallback_backoff_increases_and_is_bounded(clock):
    repo = MemoryRepository()
    job = await seeded(repo)
    delays = []
    for _ in range(12):
        assert await repo.claim_job()
        row = await defer_jev_job(repo, job["job_id"], rate_limit("invalid"))
        delays.append((parse_time(row["run_after"]) - clock[0]).total_seconds())
        clock[0] = parse_time(row["run_after"])
    assert delays[:4] == [2, 4, 8, 16]
    assert delays[-2:] == [300, 300]


@pytest.mark.parametrize("kind", ["FILTER_ITEM", "ROUTE_ITEM"])
async def test_jev_job_types_both_defer_and_release_the_worker(clock, monkeypatch, kind):
    repo = MemoryRepository()
    job = await seeded(repo, kind=kind)

    async def handle(payload):
        raise rate_limit()

    await run_once(repo, handle, monkeypatch, kind=kind)
    row = await repo.get("ops.jobs", job["job_id"])
    assert row["status"] == "QUEUED" and row["attempts"] == 0


async def test_manual_recovery_preserves_original_evidence_and_does_not_duplicate_job(clock):
    repo = MemoryRepository()
    job = await seeded(repo)
    failure = {
        **job,
        "status": "FAILED",
        "attempts": 2,
        "last_error": "HTTPStatusError",
        "finished_at": util.iso(),
        "ops_progress_recorded_at": util.iso(),
    }
    await repo.put("ops.jobs", job["job_id"], failure)
    assert (await repo.enqueue(job["type"], job["payload"], job["idem_key"]))["status"] == "FAILED"
    result = await retry_jev_job(repo, job["job_id"])
    assert result["status"] == "QUEUED"
    row = await repo.get("ops.jobs", job["job_id"])
    assert row["attempts"] == 0 and row["last_error"] is None
    assert row["expires_at"] == failure["expires_at"] and row["created_at"] == failure["created_at"]
    assert row["manual_retries"][0]["last_error"] == "HTTPStatusError"
    assert row["manual_retries"][0]["attempts"] == 2
    assert "ops_progress_recorded_at" not in row
    assert len(await repo.list("ops.jobs")) == 1
    with pytest.raises(ValueError, match="Only failed"):
        await retry_jev_job(repo, job["job_id"])


@pytest.mark.parametrize("change", ["expired", "wrong_kind", "wrong_error", "completed_item", "missing_item"])
async def test_manual_recovery_refuses_unsafe_or_unrelated_cases_without_writes(clock, change):
    repo = MemoryRepository()
    job = await seeded(repo)
    failure = {**job, "status": "FAILED", "attempts": 2, "last_error": "HTTPStatusError"}
    if change == "expired":
        clock[0] = parse_time(job["expires_at"])
    elif change == "wrong_kind":
        failure["type"] = "DELIVER"
    elif change == "wrong_error":
        failure["last_error"] = "ValueError"
    elif change == "completed_item":
        await repo.put(
            "ingest.source_items", item()["item_id"], {**item(), "relevance": {"decision": "SKIP"}}
        )
    elif change == "missing_item":
        await repo.delete("ingest.source_items", item()["item_id"])
    await repo.put("ops.jobs", job["job_id"], failure)
    before = await repo.get("ops.jobs", job["job_id"])
    with pytest.raises(ValueError):
        await retry_jev_job(repo, job["job_id"])
    assert await repo.get("ops.jobs", job["job_id"]) == before


def test_operator_command_calls_targeted_recovery(monkeypatch):
    selected = []

    async def recover(repo, job_id):
        selected.append(job_id)
        return {"status": "QUEUED", "job_id": job_id}

    def run_db(fn):
        cli.output(asyncio.run(fn(object(), options())))

    monkeypatch.setattr(recovery, "retry_jev_job", recover)
    monkeypatch.setattr(cli, "run_db", run_db)
    result = CliRunner().invoke(cli.main, ["jobs", "retry-jev", "JOB-selected"])
    assert result.exit_code == 0, result.output
    assert selected == ["JOB-selected"]
    assert json.loads(result.output)["status"] == "QUEUED"


async def test_partial_routing_scores_survive_rate_limit_without_duplicate_agent_delivery(clock, monkeypatch):
    repo = MemoryRepository()
    job = await seeded(repo, kind="ROUTE_ITEM")
    await repo.put(
        "ingest.source_items",
        item()["item_id"],
        {**item(), "entities": ["airline"], "relevance": {"decision": "SEND"}},
    )
    for event in ("first", "second"):
        await repo.put(
            "evt.events",
            event,
            {"event_id": event, "entities": ["airline"], "statement": event, "lifecycle": "ANALYZING"},
        )
        await repo.put(
            "ops.agent_sessions",
            event,
            {"session_id": event, "event_id": event, "status": "OPEN", "last_item_at": util.iso()},
        )
    settings = options()
    settings.load_config = lambda name: (
        {"routing_cutoff": 0.5, "routing_window_days": 3} if name == "thresholds" else {}
    )
    calls = []

    def transport(request):
        state = json.loads(request.content)["state"]
        candidate = state["event"]["statement"]
        calls.append(candidate)
        if candidate == "second" and calls.count(candidate) == 1:
            return provider_response(429)
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13",
                "answers": {"same_event": {"type": "noul", "noul": 0.99 if candidate == "second" else 0.01}},
                "usage": {"input_tokens": 470},
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        pipeline = Pipeline(repo, settings, http=http)
        await run_once(repo, pipeline.route_item, monkeypatch, kind="ROUTE_ITEM")
        row = await repo.get("ops.jobs", job["job_id"])
        assert row["status"] == "QUEUED" and row["attempts"] == 0
        assert len(await repo.list("ops.llm_usage")) == 1
        assert await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"}) == []
        clock[0] = parse_time(row["run_after"])
        await run_once(repo, pipeline.route_item, monkeypatch, kind="ROUTE_ITEM")
        assert (await repo.get("ops.jobs", job["job_id"]))["status"] == "DONE"
        await pipeline.route_item(job["payload"])
    assert calls.count("first") == 1 and calls.count("second") == 2
    assert len(await repo.list("ops.llm_usage")) == 2
    deliveries = await repo.list("ops.jobs", filters={"type": "AGENT_MESSAGE"})
    assert len(deliveries) == 1
    assert deliveries[0]["payload"]["event_id"] == "second"


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422, 500, 503])
async def test_other_http_statuses_keep_the_failure_budget_and_record_the_actual_status(
    clock, monkeypatch, status
):
    repo = MemoryRepository()
    job = await seeded(repo)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: provider_response(status))) as http:
        pipeline = Pipeline(repo, options(), http=http)
        for attempts in (1, 2):
            await run_once(repo, pipeline.filter_item, monkeypatch)
            row = await repo.get("ops.jobs", job["job_id"])
            assert row["attempts"] == attempts
            clock[0] = parse_time(row["run_after"])
    assert row["status"] == "FAILED"
    assert row["last_error"] == f"JevHTTPError: HTTP {status}"
    assert "PRIVATE_API_KEY" not in json.dumps(row)
    assert "private-input" not in json.dumps(row)
