"""Actual PostgreSQL and HTTP response boundaries for Jev deferral and restart recovery."""

import os
from dataclasses import replace
from datetime import timedelta

import httpx
import psycopg
import pytest
from aiohttp import web
from test_jev_rate_limits import options, run_once, seeded

import tank.agents.jev as jev
from tank.agents.pipeline import Pipeline
from tank.jev_recovery import retry_jev_job
from tank.storage import PostgresRepository
from tank.time_precision import collection_clock, offset_schedule
from tank.util import parse_time, utcnow

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


@pytest.fixture
async def repository(postgres_template):
    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.fail("This integration requires its disposable PostgreSQL service")
    async with postgres_template.repository(url) as repo:
        yield repo


@pytest.mark.parametrize("score", [0.01, 0.99])
async def test_http_429s_survive_database_reopen_and_complete_once(repository, monkeypatch, score):
    repo = repository
    job = await seeded(repo)
    requests = []

    async def endpoint(request):
        requests.append(await request.json())
        if len(requests) <= 2:
            return web.json_response(
                {"error": {"type": "server_error", "message": "Rate limit exceeded"}},
                status=429,
                headers={"Retry-After": "120", "x-opencode-log-id": "01a172e1-8e9f-7653-9bc2-f91997ff7190"},
            )
        return web.json_response(
            {
                "model": "jev-1.13",
                "answers": {"relevant": {"type": "noul", "noul": score}},
                "usage": {"input_tokens": 470},
            }
        )

    app = web.Application()
    app.router.add_post("/v1/systemone", endpoint)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    monkeypatch.setattr(jev, "KEYED", replace(jev.KEYED, base_url=f"http://127.0.0.1:{port}"))
    reopened = None
    try:
        async with httpx.AsyncClient(timeout=2) as http:
            pipeline = Pipeline(repo, options(), http=http)
            await run_once(repo, pipeline.filter_item, monkeypatch)
            original = await repo.get("ops.jobs", job["job_id"])
            assert original["status"] == "QUEUED" and original["attempts"] == 0
            assert (parse_time(original["run_after"]) - utcnow()).total_seconds() > 100
            await repo.close()
            reopened = PostgresRepository(repo.database_url)
            await reopened.open()
            assert await reopened.get("ops.jobs", job["job_id"]) == original
            pipeline = Pipeline(reopened, options(), http=http)
            await run_once(reopened, pipeline.filter_item, monkeypatch)
            assert len(requests) == 1
            for expected_calls in (2, 3):
                row = await reopened.get("ops.jobs", job["job_id"])
                await reopened.put(
                    "ops.jobs",
                    job["job_id"],
                    {**row, **offset_schedule(collection_clock(), timedelta(seconds=-1)).fields("run_after")},
                )
                await run_once(reopened, pipeline.filter_item, monkeypatch)
                assert len(requests) == expected_calls
            result = await reopened.get("ops.jobs", job["job_id"])
            assert result["status"] == "DONE" and result["attempts"] == 1
            assert result["last_error"] is None and result["jev_rate_limit_count"] == 2
            assert result["expires_at"] == job["expires_at"]
            await pipeline.filter_item(job["payload"])
            assert len(requests) == 3
            assert requests[0] == requests[1] == requests[2]
            assert len(await reopened.list("ops.llm_usage")) == 1
            assert len(await reopened.list("ops.jobs", filters={"type": "ROUTE_ITEM"})) == (score >= 0.5)
            assert await reopened.list("out.alerts") == []
            assert await reopened.list("ops.agent_sessions") == []
            connection = await psycopg.AsyncConnection.connect(repo.database_url)
            try:
                cursor = await connection.execute(
                    "SELECT status,attempts,last_error,document FROM ops.jobs WHERE job_id=%s",
                    (job["job_id"],),
                )
                status, attempts, error, document = await cursor.fetchone()
                assert status == document["status"] == "DONE"
                assert attempts == document["attempts"] == 1
                assert error is document["last_error"] is None
            finally:
                await connection.close()
    finally:
        if reopened:
            await reopened.close()
        await runner.cleanup()


async def test_recovery_preserves_legacy_failure_and_cannot_recover_completed_job(repository):
    repo = repository
    job = await seeded(repo)
    await repo.put(
        "ops.jobs",
        job["job_id"],
        {**job, "status": "FAILED", "attempts": 2, "last_error": "HTTPStatusError"},
    )
    before = await repo.get("ops.jobs", job["job_id"])
    result = await retry_jev_job(repo, job["job_id"])
    assert result["status"] == "QUEUED"
    recovered = await repo.get("ops.jobs", job["job_id"])
    assert recovered["manual_retries"][0]["last_error"] == before["last_error"]
    assert recovered["manual_retries"][0]["finished_at"] == before["finished_at"]
    assert recovered["expires_at"] == before["expires_at"]
    assert recovered["finished_at"] is None
    with pytest.raises(ValueError, match="Only failed"):
        await retry_jev_job(repo, job["job_id"])
    assert len(await repo.list("ops.jobs")) == 1
