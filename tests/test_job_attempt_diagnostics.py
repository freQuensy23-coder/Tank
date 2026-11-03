import asyncio
import json
import os
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest

from tank import runtime as runtime_module
from tank.agents.sessions import AdmissionDeferred
from tank.job_diagnostics import MAX_CHAIN, MAX_HISTORY_BYTES, MAX_RECEIPT_BYTES
from tank.runtime import Runtime
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock, offset_schedule
from tank.util import iso, parse_time, utcnow

REQUEST_ID = "4864396e-205b-41c0-a078-4255bb529d08"
QUERY_ID = "06a4dd8d-a61d-430c-a978-19fb400abb96"
SECRET = "injected-private-credential"


@pytest.fixture(
    params=[
        "memory",
        pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres]),
    ]
)
async def diagnostic_repo(request, postgres_template):
    if request.param == "memory":
        yield MemoryRepository()
    else:
        url = os.environ.get("TANK_TEST_DATABASE_URL")
        assert url, "Requires the CI-owned PostgreSQL test connection"
        async with postgres_template.repository(url) as repo:
            yield repo


def make_runtime(repo):
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.settings = SimpleNamespace(
        code_version="a" * 64,
        config_version="dev." + "b" * 64,
        build_sha="c" * 40,
        configured_secrets=lambda: (SECRET,),
    )
    return runtime


async def run_attempt(runtime, handler):
    stop = asyncio.Event()

    async def handle(payload):
        try:
            return await handler(payload)
        finally:
            stop.set()

    runtime.handlers = {"RUN_MARKET_TRUTH": handle}
    await runtime.worker(stop)


def http_failure():
    request = httpx.Request(
        "POST",
        f"https://user:{SECRET}@example.test/{SECRET}?token={SECRET}",
        headers={"authorization": f"Bearer {SECRET}"},
        content=SECRET,
    )
    response = httpx.Response(
        503,
        request=request,
        headers={
            "x-request-id": REQUEST_ID,
            "x-clickhouse-query-id": QUERY_ID,
            "set-cookie": SECRET,
        },
        content=SECRET,
    )
    error = httpx.HTTPStatusError(SECRET, request=request, response=response)
    error.retry_after = 0
    return error


async def test_runtime_receipt_survives_retry_success_and_jsonb(diagnostic_repo):
    repo = diagnostic_repo
    expires = iso(utcnow() + timedelta(hours=1))
    payload = {"private": SECRET, "code_version": "d" * 64, "config_version": "e" * 40}
    job = await repo.enqueue("RUN_MARKET_TRUTH", payload, "diagnostic-retry", expires_at=expires)
    runtime = make_runtime(repo)

    async def fail(payload):
        try:
            raise httpx.ReadError(SECRET)
        except httpx.ReadError as cause:
            raise http_failure() from cause

    await run_attempt(runtime, fail)
    first = await repo.get("ops.jobs", job["job_id"])
    assert first["status"] == "QUEUED" and first["attempts"] == 1
    assert first["last_error"] == "HTTPStatusError"
    assert first["max_attempts"] == job["max_attempts"]
    receipt = first["attempt_diagnostics"][0]
    assert receipt["job_id"] == job["job_id"]
    assert receipt["stage"] == "RUN_MARKET_TRUTH" and receipt["phase"] == "handler"
    assert receipt["attempt"] == 1 and receipt["expires_at"] == expires
    assert receipt["runtime"] == {
        "code_version": "a" * 64,
        "config_version": "dev." + "b" * 64,
        "build_sha": "c" * 40,
    }
    assert receipt["exception_chain"] == [
        {
            "class": "httpx.HTTPStatusError",
            "http_status": 503,
            "request_id": REQUEST_ID,
            "query_id": QUERY_ID,
        },
        {"class": "httpx.ReadError"},
    ]
    assert not receipt["chain_truncated"]
    assert parse_time(receipt["started_at"]) <= parse_time(receipt["finished_at"])
    assert parse_time(receipt["handler_deadline_at"]) > parse_time(receipt["started_at"])
    assert SECRET not in json.dumps(receipt)
    assert "payload" not in receipt and "traceback" not in receipt

    async def succeed(payload):
        return {"completed": True}

    await run_attempt(runtime, succeed)
    completed = await repo.get("ops.jobs", job["job_id"])
    assert completed["status"] == "DONE" and completed["attempts"] == 2
    assert completed["result"] == {"completed": True}
    assert completed["payload"] == payload and completed["expires_at"] == expires
    assert completed["attempt_diagnostics"] == [receipt]
    duplicate = await repo.enqueue("RUN_MARKET_TRUTH", {}, "diagnostic-retry")
    assert duplicate["job_id"] == job["job_id"]
    if not isinstance(repo, MemoryRepository):
        async with repo.transaction():
            row = await (
                await repo._conn().execute(
                    "SELECT document->'attempt_diagnostics' AS receipts FROM ops.jobs WHERE job_id=%s",
                    (job["job_id"],),
                )
            ).fetchone()
        assert row["receipts"] == [receipt]
        assert SECRET not in json.dumps(row["receipts"])


async def test_runtime_keeps_each_failure_without_spending_extra_attempts(diagnostic_repo):
    repo = diagnostic_repo
    job = await repo.enqueue("RUN_MARKET_TRUTH", {}, "diagnostic-exhaustion", max_attempts=2)
    runtime = make_runtime(repo)

    async def fail(payload):
        raise http_failure()

    await run_attempt(runtime, fail)
    first = await repo.get("ops.jobs", job["job_id"])
    await run_attempt(runtime, fail)
    final = await repo.get("ops.jobs", job["job_id"])
    assert final["status"] == "FAILED" and final["attempts"] == 2
    assert [entry["attempt"] for entry in final["attempt_diagnostics"]] == [1, 2]
    assert final["attempt_diagnostics"][0] == first["attempt_diagnostics"][0]
    assert await repo.claim_job() is None


async def test_cancellation_retains_previous_failure_and_running_recovery(diagnostic_repo):
    repo = diagnostic_repo
    job = await repo.enqueue("RUN_MARKET_TRUTH", {}, "diagnostic-cancel")
    runtime = make_runtime(repo)

    async def fail(payload):
        raise http_failure()

    await run_attempt(runtime, fail)
    previous = await repo.get("ops.jobs", job["job_id"])
    started = asyncio.Event()

    async def blocked(payload):
        started.set()
        await asyncio.Event().wait()

    runtime.handlers = {"RUN_MARKET_TRUTH": blocked}
    task = asyncio.create_task(runtime.worker(asyncio.Event()))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    cancelled = await repo.get("ops.jobs", job["job_id"])
    assert cancelled["status"] == "RUNNING" and cancelled["attempts"] == 2
    assert cancelled["attempt_diagnostics"] == previous["attempt_diagnostics"]
    assert not runtime.active_jobs
    assert await repo.recover_jobs() == 1
    reclaimed = await repo.claim_job()
    assert reclaimed["job_id"] == job["job_id"] and reclaimed["attempts"] == 2
    assert reclaimed["attempt_diagnostics"] == previous["attempt_diagnostics"]


async def test_dependency_failure_receipt_does_not_run_handler(diagnostic_repo):
    repo = diagnostic_repo
    dependency = await repo.enqueue("ESCALATE_TOKENS", {}, "diagnostic-dependency")
    await repo.put("ops.jobs", dependency["job_id"], {**dependency, "status": "FAILED"})
    job = await repo.enqueue(
        "RUN_MARKET_TRUTH", {"_depends_on": [dependency["job_id"]]}, "diagnostic-dependent", max_attempts=1
    )
    runtime = make_runtime(repo)
    stop = asyncio.Event()

    async def handler(payload):
        pytest.fail("A failed dependency must prevent handler effects")

    async def settled(job, reason):
        stop.set()

    runtime.handlers = {"RUN_MARKET_TRUTH": handler}
    runtime.terminal_delivery_failure = settled
    await runtime.worker(stop)
    failed = await repo.get("ops.jobs", job["job_id"])
    receipt = failed["attempt_diagnostics"][0]
    assert failed["status"] == "FAILED" and failed["attempts"] == 1
    assert receipt["phase"] == "dependencies" and receipt["handler_deadline_at"] is None
    assert receipt["exception_chain"] == [{"class": "builtins.RuntimeError"}]


async def test_timeout_receipt_contains_cancelled_leaf_without_changing_timeout(diagnostic_repo, monkeypatch):
    repo = diagnostic_repo
    job = await repo.enqueue("RUN_MARKET_TRUTH", {}, "diagnostic-timeout", max_attempts=1)
    runtime = make_runtime(repo)
    monkeypatch.setitem(runtime_module.TIMEOUTS, "RUN_MARKET_TRUTH", 0.01)

    async def blocked(payload):
        await asyncio.Event().wait()

    await run_attempt(runtime, blocked)
    failed = await repo.get("ops.jobs", job["job_id"])
    assert failed["status"] == "FAILED" and failed["last_error"] == "TimeoutError"
    receipt = failed["attempt_diagnostics"][0]
    assert receipt["exception_chain"] == [
        {"class": "builtins.TimeoutError"},
        {"class": "asyncio.exceptions.CancelledError"},
    ]
    assert parse_time(receipt["finished_at"]) >= parse_time(receipt["handler_deadline_at"])


async def test_typed_metadata_rejects_secret_and_invalid_correlations(diagnostic_repo):
    repo = diagnostic_repo
    job = await repo.enqueue("RUN_MARKET_TRUTH", {}, "diagnostic-secret", max_attempts=1)
    runtime = make_runtime(repo)
    runtime.settings.configured_secrets = lambda: (SECRET, REQUEST_ID)
    runtime.settings.build_sha = SECRET

    async def fail(payload):
        error = http_failure()
        error.response.headers["x-clickhouse-query-id"] = SECRET + "\x00"
        error.response.headers["x-request-id"] = REQUEST_ID
        error.__cause__ = error
        raise error

    await run_attempt(runtime, fail)
    failed = await repo.get("ops.jobs", job["job_id"])
    receipt = failed["attempt_diagnostics"][0]
    assert receipt["runtime"]["build_sha"] is None
    assert receipt["exception_chain"] == [{"class": "httpx.HTTPStatusError", "http_status": 503}]
    assert receipt["chain_truncated"]
    assert SECRET not in json.dumps(receipt) and REQUEST_ID not in json.dumps(receipt)


@pytest.mark.parametrize("producer", ["http", "clickhouse"])
@pytest.mark.parametrize(
    "credential", ["foundry_api_key", "tank_db_password", "oma_db_password", "clickhouse_url"]
)
@pytest.mark.parametrize("spelling", ["exact", "normalized", "canonical-secret"])
async def test_worker_rejects_configured_credential_echo_after_jsonb(
    diagnostic_repo, producer, credential, spelling
):
    from tank.clickhouse_market import ClickHouse

    repo = diagnostic_repo
    job = await repo.enqueue("RUN_MARKET_TRUTH", {}, "diagnostic-credential-echo", max_attempts=1)
    runtime = make_runtime(repo)
    configured_id = REQUEST_ID.upper() if spelling == "canonical-secret" else REQUEST_ID
    settings_values = {credential: configured_id}
    if credential == "clickhouse_url":
        settings_values[credential] = f"http://default:{configured_id}@example.test"
    runtime.settings = Settings(
        _env_file=None,
        database_url="postgresql://tank:FAKE-pg-independent@postgres/tank_test_diagnostics",
        openma_database_url="postgresql://oma:FAKE-oma-independent@postgres/tank_test_diagnostics",
        code_version="a" * 64,
        config_version="dev." + "b" * 64,
        build_sha="c" * 40,
        **settings_values,
    )
    echoed = REQUEST_ID.upper() if spelling == "normalized" else REQUEST_ID
    secret_header = "x-request-id" if producer == "http" else "x-clickhouse-query-id"
    safe_header = "x-clickhouse-query-id" if producer == "http" else "x-request-id"
    safe_key = "query_id" if producer == "http" else "request_id"

    def transport(request):
        if producer == "clickhouse":
            assert request.content == b"SELECT 1 FORMAT JSONEachRow"
            expected_password = (
                configured_id if credential in {"tank_db_password", "clickhouse_url"} else ""
            )
            assert request.headers["x-clickhouse-key"] == expected_password
        return httpx.Response(
            503,
            request=request,
            headers={secret_header: echoed, safe_header: QUERY_ID},
            content=SECRET,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        client = ClickHouse(
            runtime.settings.clickhouse_url or "http://example.test",
            password=runtime.settings.tank_db_password,
            http=http,
        )

        async def fail(payload):
            if producer == "clickhouse":
                return await client.rows("SELECT 1")
            response = await http.post("https://example.test/decision", content=SECRET)
            response.raise_for_status()

        await run_attempt(runtime, fail)
    stored = await repo.get("ops.jobs", job["job_id"])
    receipts = stored["attempt_diagnostics"]
    if not isinstance(repo, MemoryRepository):
        async with repo.transaction():
            row = await (
                await repo._conn().execute(
                    "SELECT document->'attempt_diagnostics' AS receipts FROM ops.jobs WHERE job_id=%s",
                    (job["job_id"],),
                )
            ).fetchone()
        assert row["receipts"] == receipts
        receipts = row["receipts"]
    error_class = "HTTPStatusError" if producer == "http" else "RuntimeError"
    error_module = "httpx" if producer == "http" else "builtins"
    assert stored["status"] == "FAILED" and stored["attempts"] == 1
    assert stored["last_error"] == error_class
    assert len(receipts) == 1 and receipts[0]["phase"] == "handler"
    assert receipts[0]["exception_chain"] == [
        {"class": f"{error_module}.{error_class}", "http_status": 503, safe_key: QUERY_ID}
    ]
    serialized = json.dumps(receipts)
    assert REQUEST_ID not in serialized and echoed not in serialized and configured_id not in serialized
    assert SECRET not in serialized and "FAKE-" not in serialized
    assert await repo.claim_job() is None


async def test_exception_chain_and_retention_are_bounded(diagnostic_repo):
    repo = diagnostic_repo
    job = await repo.enqueue("RUN_MARKET_TRUTH", {}, "diagnostic-bounds", max_attempts=2)
    runtime = make_runtime(repo)

    async def fail(payload):
        error = http_failure()
        current = error
        for _ in range(20):
            current.__cause__ = ValueError(SECRET * 1000)
            current = current.__cause__
        raise error

    await run_attempt(runtime, fail)
    current = await repo.get("ops.jobs", job["job_id"])
    receipt = current["attempt_diagnostics"][0]
    assert len(receipt["exception_chain"]) == MAX_CHAIN and receipt["chain_truncated"]
    assert len(json.dumps(receipt).encode()) <= MAX_RECEIPT_BYTES
    assert SECRET not in json.dumps(receipt)
    claimed = await repo.claim_job()
    assert claimed["attempts"] == 2
    await repo.fail_job(job["job_id"], "ReadError", diagnostic={"oversized": "x" * MAX_HISTORY_BYTES})
    final = await repo.get("ops.jobs", job["job_id"])
    assert final["status"] == "FAILED" and final["attempts"] == 2
    assert final["attempt_diagnostics"] == [receipt]
    assert final["attempt_diagnostics_truncated"]


@pytest.mark.integration
@pytest.mark.postgres
async def test_actual_postgres_constraint_is_retained_after_handler_rollback(postgres_template):
    url = os.environ.get("TANK_TEST_DATABASE_URL")
    assert url, "Requires the CI-owned PostgreSQL test connection"
    async with postgres_template.repository(url) as repo:
        job = await repo.enqueue("RUN_MARKET_TRUTH", {}, "diagnostic-constraint", max_attempts=1)
        runtime = make_runtime(repo)

        async def fail(payload):
            async with repo.transaction():
                await repo._conn().execute(
                    "UPDATE ops.jobs SET priority=-1 WHERE job_id=%s", (job["job_id"],)
                )

        await run_attempt(runtime, fail)
        failed = await repo.get("ops.jobs", job["job_id"])
        assert failed["status"] == "FAILED" and failed["priority"] == 1
        assert failed["attempts"] == 1 and failed["last_error"] == "CheckViolation"
        receipt = failed["attempt_diagnostics"][0]
        assert receipt["exception_chain"] == [
            {
                "class": "psycopg.errors.CheckViolation",
                "db_constraint": "jobs_priority_check",
                "sqlstate": "23514",
            }
        ]
        assert "statement" not in receipt and "detail" not in receipt


async def test_failure_receipt_and_state_roll_back_together(diagnostic_repo, monkeypatch):
    repo = diagnostic_repo
    job = await repo.enqueue("RUN_MARKET_TRUTH", {}, "diagnostic-atomic")
    claimed = await repo.claim_job()
    original = repo.put

    async def interrupted(collection, key, record, **kwargs):
        if collection == "ops.jobs":
            await original(collection, key, record, **kwargs)
            raise asyncio.CancelledError
        return await original(collection, key, record, **kwargs)

    monkeypatch.setattr(repo, "put", interrupted)
    with pytest.raises(asyncio.CancelledError):
        async with repo.transaction():
            await repo.fail_job(job["job_id"], "ReadError", diagnostic={"attempt": 1})
    stored = await repo.get("ops.jobs", job["job_id"])
    assert stored == claimed
    assert stored["status"] == "RUNNING" and "attempt_diagnostics" not in stored


async def test_actual_clickhouse_transport_failure_reaches_worker_receipt(diagnostic_repo):
    from tank.clickhouse_market import ClickHouse

    repo = diagnostic_repo
    job = await repo.enqueue("RUN_MARKET_TRUTH", {}, "diagnostic-clickhouse", max_attempts=1)
    runtime = make_runtime(repo)

    async def transport(request):
        assert request.content == b"SELECT 1 FORMAT JSONEachRow"
        assert request.headers["x-clickhouse-key"] == SECRET
        raise httpx.ReadError(SECRET, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        client = ClickHouse("http://example.test", password=SECRET, http=http)

        async def fail(payload):
            return await client.rows("SELECT 1")

        await run_attempt(runtime, fail)
    stored = await repo.get("ops.jobs", job["job_id"])
    assert stored["status"] == "FAILED" and stored["last_error"] == "ReadError"
    receipt = stored["attempt_diagnostics"][0]
    assert receipt["exception_chain"] == [{"class": "httpx.ReadError"}]
    assert SECRET not in json.dumps(receipt) and "SELECT" not in json.dumps(receipt)


async def test_admission_deferral_keeps_attempt_refund_and_original_expiry(diagnostic_repo):
    repo = diagnostic_repo
    expires = iso(utcnow() + timedelta(hours=1))
    job = await repo.enqueue("AGENT_MESSAGE", {}, "diagnostic-admission", expires_at=expires)
    runtime = make_runtime(repo)
    stop = asyncio.Event()
    not_before = offset_schedule(collection_clock(), timedelta(seconds=5))

    async def admit(job):
        stop.set()
        raise AdmissionDeferred(not_before)

    async def handler(payload):
        pytest.fail("Admission deferral must prevent handler effects")

    runtime.sessions = SimpleNamespace(admit=admit)
    runtime.handlers = {"AGENT_MESSAGE": handler}
    await runtime.worker(stop)
    deferred = await repo.get("ops.jobs", job["job_id"])
    assert deferred["status"] == "QUEUED" and deferred["attempts"] == 0
    assert deferred["expires_at"] == expires
    assert deferred["run_after"] == not_before.value
    assert deferred["slot_waiting_since"] == job["created_at"]
    assert "attempt_diagnostics" not in deferred and deferred["last_error"] is None


async def test_actual_clickhouse_http_status_retains_query_id_without_body(diagnostic_repo):
    from tank.clickhouse_market import ClickHouse

    repo = diagnostic_repo
    job = await repo.enqueue("RUN_MARKET_TRUTH", {}, "diagnostic-clickhouse-status", max_attempts=1)
    runtime = make_runtime(repo)

    def transport(request):
        return httpx.Response(
            503,
            request=request,
            headers={"x-clickhouse-query-id": QUERY_ID, "authorization": SECRET},
            content=SECRET,
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        client = ClickHouse("http://example.test", password=SECRET, http=http)

        async def fail(payload):
            return await client.rows("SELECT 1")

        await run_attempt(runtime, fail)
    stored = await repo.get("ops.jobs", job["job_id"])
    assert stored["status"] == "FAILED" and stored["last_error"] == "RuntimeError"
    receipt = stored["attempt_diagnostics"][0]
    assert receipt["exception_chain"] == [
        {"class": "builtins.RuntimeError", "http_status": 503, "query_id": QUERY_ID}
    ]
    assert SECRET not in json.dumps(receipt)
