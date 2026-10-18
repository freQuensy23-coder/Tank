import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tank.runtime import Runtime
from tank.storage import MemoryRepository


@pytest.mark.asyncio
async def test_worker_survives_bad_job_and_persists_result():
    repo = MemoryRepository()
    stop = asyncio.Event()
    bad = await repo.enqueue("BAD", {}, "bad", max_attempts=1)
    good = await repo.enqueue("GOOD", {}, "good", max_attempts=1)
    runtime = object.__new__(Runtime)
    runtime.repo = repo

    async def handle(payload):
        stop.set()
        return {"evidence": "persisted"}

    runtime.handlers = {"GOOD": handle}
    import tank.runtime as module

    module.TIMEOUTS["GOOD"] = 1
    await runtime.worker(stop)
    assert (await repo.get("ops.jobs", bad["job_id"]))["status"] == "FAILED"
    assert (await repo.get("ops.jobs", good["job_id"]))["result"] == {"evidence": "persisted"}
    assert (await repo.get("ops.jobs", good["job_id"]))["status"] == "DONE"


@pytest.mark.asyncio
async def test_worker_refuses_failed_dependency():
    repo = MemoryRepository()
    stop = asyncio.Event()
    dep = await repo.enqueue("ESCALATE_TOKENS", {}, "dep", max_attempts=1)
    await repo.put("ops.jobs", dep["job_id"], {**dep, "status": "FAILED"})
    job = await repo.enqueue("RUN_MARKET_TRUTH", {"_depends_on": [dep["job_id"]]}, "truth", max_attempts=1)
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.handlers = {"RUN_MARKET_TRUTH": AsyncMock()}
    task = asyncio.create_task(runtime.worker(stop))
    for _ in range(100):
        if (await repo.get("ops.jobs", job["job_id"]))["status"] == "FAILED":
            break
        await asyncio.sleep(0.001)
    stop.set()
    await task
    assert (await repo.get("ops.jobs", job["job_id"]))["status"] == "FAILED"
    runtime.handlers["RUN_MARKET_TRUTH"].assert_not_called()


def test_doctor_distinguishes_syntax_from_deployment_and_never_prints_secrets(tmp_path):
    import json

    from tank.cli import readiness_checks
    from tank.settings import Settings

    settings = Settings(
        config_dir=__import__("pathlib").Path("config"),
        x_profile_dir=tmp_path / "profile",
        openma_api_key="sensitive-secret",
        x_list_id="REPLACE_WITH_LIST",
        user_agent="Tank contact not configured",
    )
    checks = readiness_checks(settings)
    by_name = {r["check"]: r["ok"] for r in checks}
    assert by_name["openma_syntax"]
    assert not by_name["openma_agent_id"] and not by_name["openma_scout_agent_id"]
    assert not by_name["openma_environment_id"] and not by_name["openma_replay_environment_id"]
    assert by_name["openma_api_key"]
    assert not by_name["user_agent_contact"] and not by_name["x_profile"] and not by_name["x_list_id"]
    assert "sensitive-secret" not in json.dumps(checks)


def test_source_check_network_probes_are_explicit(monkeypatch):
    from click.testing import CliRunner

    import tank.cli as cli
    import tank.sources

    calls = []

    class Source:
        def __init__(self, *args):
            pass

        async def check(self, **kwargs):
            calls.append(kwargs)
            return []

        async def close(self):
            pass

    monkeypatch.setattr(tank.sources, "SourceService", Source)
    monkeypatch.setattr(cli, "run_db", lambda fn: asyncio.run(fn(None, None)))
    assert CliRunner().invoke(cli.main, ["sources", "check"]).exit_code == 0
    assert calls[-1] == {"probe": False, "include_browser": False, "check_accounts": False}
    assert (
        CliRunner().invoke(cli.main, ["sources", "check", "--probe", "--browser", "--accounts"]).exit_code
        == 0
    )
    assert calls[-1] == {"probe": True, "include_browser": True, "check_accounts": True}


def test_manual_backup_defaults_to_no_retention_and_forwards_force(monkeypatch):
    from click.testing import CliRunner

    import tank.cli as cli
    import tank.scheduler

    calls = []

    class Scheduler:
        def __init__(self, *args):
            pass

        async def maintenance(self, now, **kwargs):
            calls.append(kwargs)
            return {"status": "created"}

    monkeypatch.setattr(tank.scheduler, "Scheduler", Scheduler)
    monkeypatch.setattr(cli, "run_db", lambda fn: asyncio.run(fn(None, None)))
    assert CliRunner().invoke(cli.main, ["backup", "--force"]).exit_code == 0
    assert calls[-1] == {"force": True, "prune": False}
    assert CliRunner().invoke(cli.main, ["backup", "--prune"]).exit_code == 0
    assert calls[-1] == {"force": False, "prune": True}


@pytest.mark.asyncio
async def test_delivery_rate_deferral_requeues_without_ttl_or_attempt_cost():
    from datetime import timedelta

    from tank.delivery import DeliveryDeferred
    from tank.util import iso, utcnow

    now = utcnow()
    repo = MemoryRepository()
    stop = asyncio.Event()
    expires = iso(now + timedelta(days=1))
    due = iso(now + timedelta(minutes=13))
    job = await repo.enqueue(
        "DELIVER", {"alert_id": "a", "version": 2}, "a:2", expires_at=expires, max_attempts=5
    )

    async def deferred(payload):
        stop.set()
        raise DeliveryDeferred(due)

    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.handlers = {"DELIVER": deferred}
    await runtime.worker(stop)
    row = await repo.get("ops.jobs", job["job_id"])
    assert row["status"] == "QUEUED" and row["run_after"] == due and row["attempts"] == 0
    assert row["expires_at"] == expires


@pytest.mark.asyncio
async def test_reload_rejects_candidate_then_drains_old_generation_before_swap():
    from tank.runtime import RuntimeSupervisor

    stop = asyncio.Event()
    reload_event = asyncio.Event()
    old_draining = asyncio.Event()
    allow_finish = asyncio.Event()
    started = []
    closed = []
    loaded = []
    old = SimpleNamespace(database_url="same-db", config_version="old", valid=True)
    invalid = SimpleNamespace(database_url="same-db", config_version="invalid", valid=False)
    new = SimpleNamespace(database_url="same-db", config_version="new", valid=True)
    candidates = iter([invalid, new])

    def loader():
        candidate = next(candidates)
        loaded.append(candidate.config_version)
        return candidate

    def validate(settings):
        if not settings.valid:
            raise ValueError("Invalid candidate configuration")

    class Generation:
        def __init__(self, repo, settings):
            self.settings = settings

        def validate(self):
            validate(self.settings)

        async def run(self, flag):
            started.append(self.settings.config_version)
            await flag.wait()
            if self.settings.config_version == "old":
                old_draining.set()
                await allow_finish.wait()
            await self.close()

        async def close(self):
            closed.append(self.settings.config_version)

    supervisor = RuntimeSupervisor(None, old, candidate_loader=loader, factory=Generation, validator=validate)
    task = asyncio.create_task(supervisor.run(stop, reload_event))

    async def until(predicate):
        async with asyncio.timeout(2):
            while not predicate():
                await asyncio.sleep(0.001)

    try:
        await until(lambda: started == ["old"])
        reload_event.set()
        await until(lambda: loaded == ["invalid"])
        await asyncio.sleep(0.01)
        assert supervisor.settings is old and not old_draining.is_set() and not task.done()
        reload_event.set()
        await old_draining.wait()
        assert started == ["old"] and supervisor.settings is old
        allow_finish.set()
        await until(lambda: started == ["old", "new"])
        assert closed == ["old"] and supervisor.settings is new
    finally:
        allow_finish.set()
        stop.set()
        await task
    assert closed == ["old", "new"]


@pytest.mark.asyncio
async def test_runtime_quiescence_drains_old_job_dependencies_and_preserves_session_provenance(monkeypatch):
    from datetime import timedelta

    import tank.runtime as module
    from tank.util import iso, utcnow

    repo = MemoryRepository()
    stop = asyncio.Event()
    started = asyncio.Event()
    release = asyncio.Event()

    class Service:
        def __init__(self):
            self.stopped = asyncio.Event()
            self.closed = False

        async def start(self):
            pass

        async def run(self, flag):
            await flag.wait()
            self.stopped.set()

        async def close(self):
            self.closed = True

    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.settings = SimpleNamespace(shutdown_grace_s=1, config_version="old")
    runtime.active_jobs = set()
    runtime.validate = lambda: None
    runtime.bootstrap = AsyncMock()
    runtime.sources = Service()
    runtime.scheduler = Service()
    runtime.market = Service()
    runtime.sessions = Service()
    runtime.pipeline = Service()
    runtime.delivery = Service()
    runtime.event_tools = Service()
    runtime.gateway = Service()
    future = await repo.enqueue("FUTURE", {}, "future", run_after=iso(utcnow() + timedelta(days=1)))
    await repo.put(
        "ops.agent_sessions",
        "session",
        {
            "session_id": "session",
            "status": "OPEN",
            "runtime_status": "IDLE",
            "config_version": "old",
            "last_event_seq": 7,
        },
    )
    await repo.put(
        "ops.tool_calls",
        "tool",
        {
            "call_id": "tool",
            "session_id": "session",
            "status": "DONE",
            "returned_at": iso(),
            "config_version": "old",
            "result": {"frozen": "old"},
        },
    )

    async def first(payload):
        started.set()
        await release.wait()
        dep = await repo.enqueue("DRAIN_DEP", {}, "dependency")
        while (await repo.get("ops.jobs", dep["job_id"]))["status"] != "DONE":
            await asyncio.sleep(0.001)
        return {"config_version": runtime.settings.config_version}

    async def dependency(payload):
        return {"config_version": runtime.settings.config_version}

    runtime.handlers = {"DRAIN_FIRST": first, "DRAIN_DEP": dependency}
    monkeypatch.setitem(module.TIMEOUTS, "DRAIN_FIRST", 2)
    monkeypatch.setitem(module.TIMEOUTS, "DRAIN_DEP", 2)
    job = await repo.enqueue("DRAIN_FIRST", {}, "first")
    task = asyncio.create_task(runtime.run(stop))
    await started.wait()
    stop.set()
    await asyncio.sleep(0.01)
    assert not runtime.market.stopped.is_set() and not runtime.sessions.stopped.is_set()
    assert (await repo.get("ops.jobs", job["job_id"]))["status"] == "RUNNING"
    release.set()
    await task
    assert (await repo.get("ops.jobs", job["job_id"]))["result"]["config_version"] == "old"
    assert (await repo.get("ops.jobs", future["job_id"]))["status"] == "QUEUED"
    assert (await repo.get("ops.agent_sessions", "session"))["last_event_seq"] == 7
    assert (await repo.get("ops.tool_calls", "tool"))["result"] == {"frozen": "old"}
    assert all(
        service.closed
        for service in [
            runtime.sources,
            runtime.market,
            runtime.sessions,
            runtime.pipeline,
            runtime.delivery,
            runtime.event_tools,
        ]
    )


def test_review_cli_forwards_period_paging_and_structured_checks(monkeypatch, tmp_path):
    import json

    from click.testing import CliRunner

    import tank.cli as cli
    import tank.review

    calls = []

    class Review:
        def __init__(self, *args):
            pass

        async def list(self, **kwargs):
            calls.append(("list", kwargs))
            return []

        async def show(self, subject, **kwargs):
            calls.append(("show", subject, kwargs))
            return {}

        async def report(self, start, end, **kwargs):
            calls.append(("report", start, end, kwargs))
            return {}

    monkeypatch.setattr(tank.review, "ReviewService", Review)
    monkeypatch.setattr(cli, "run_db", lambda fn: asyncio.run(fn(None, None)))
    runner = CliRunner()
    since = "2026-10-01T00:00:00Z"
    until = "2026-10-08T00:00:00Z"
    assert (
        runner.invoke(
            cli.main,
            ["review", "list", "--since", since, "--until", until, "--after", "cursor", "--limit", "20"],
        ).exit_code
        == 0
    )
    assert calls[-1] == (
        "list",
        {"start": since, "end": until, "limit": 20, "after": "cursor", "event_id": None},
    )
    assert (
        runner.invoke(
            cli.main, ["review", "show", "event", "--since", since, "--until", until, "--limit", "10"]
        ).exit_code
        == 0
    )
    assert calls[-1] == ("show", "event", {"start": since, "end": until, "limit": 10, "after": None})
    checks = [
        {
            "alert_id": "a",
            "version": 1,
            "reviewer": "operator",
            "reason": "inspected frozen evidence",
            "state_overstated": False,
        }
    ]
    path = tmp_path / "checks.json"
    path.write_text(json.dumps(checks))
    assert (
        runner.invoke(
            cli.main, ["review", "metrics", "--since", since, "--until", until, "--checks-file", str(path)]
        ).exit_code
        == 0
    )
    assert calls[-1] == ("report", since, until, {"checks": checks})


def test_review_label_records_latest_or_explicit_alert_version(monkeypatch):
    from click.testing import CliRunner

    import tank.cli as cli

    repo = MemoryRepository()

    async def seed():
        for version in (1, 2):
            await repo.put(
                "out.alerts",
                f"a:{version}",
                {"alert_id": "a", "version": version, "event_id": "e"},
                immutable=True,
            )

    asyncio.run(seed())
    monkeypatch.setattr(cli, "run_db", lambda fn: asyncio.run(fn(repo, None)))
    runner = CliRunner()
    assert runner.invoke(cli.main, ["review", "label", "a", "CATCH", "--reviewer", "operator"]).exit_code == 0
    assert asyncio.run(repo.list("qa.reviews"))[0]["version"] == 2
    assert (
        runner.invoke(
            cli.main, ["review", "label", "a", "LATE", "--reviewer", "operator", "--version", "1"]
        ).exit_code
        == 0
    )
    rows = asyncio.run(repo.list("qa.reviews"))
    assert len(rows) == 2 and rows[-1]["version"] == 1


@pytest.mark.asyncio
async def test_reload_uses_real_immutable_settings_snapshot_and_rejects_malformed_edit(tmp_path):
    from tank.runtime import RuntimeSupervisor
    from tank.settings import Settings, resolve_versions

    config = tmp_path / "config"
    config.mkdir()
    backup = tmp_path / "backup"
    backup.mkdir()
    for name in ("sources", "registry", "semantics", "dependency_map"):
        (config / f"{name}.yaml").write_text("{}\n")
    thresholds = config / "thresholds.yaml"
    thresholds.write_text("filter_cutoff: 0.4\nrouting_cutoff: 0.5\n")
    base = Settings(
        config_dir=config,
        data_dir=tmp_path / "data",
        backup_dir=backup,
        telegram_token="test-secret",
        telegram_alerts_chat="1",
        telegram_ops_chat="2",
    )
    original = resolve_versions(base)
    started = []
    loads = []
    stop = asyncio.Event()
    reload_event = asyncio.Event()

    class Generation:
        def __init__(self, repo, settings):
            self.settings = settings

        def validate(self):
            Runtime.validate_settings(self.settings)

        async def run(self, flag):
            started.append(
                (self.settings.config_version, self.settings.load_config("thresholds")["filter_cutoff"])
            )
            await flag.wait()

        async def close(self):
            pass

    def load():
        loads.append(True)
        return resolve_versions(base)

    supervisor = RuntimeSupervisor(None, original, candidate_loader=load, factory=Generation)
    task = asyncio.create_task(supervisor.run(stop, reload_event))

    async def until(predicate):
        async with asyncio.timeout(2):
            while not predicate():
                await asyncio.sleep(0.001)

    try:
        await until(lambda: len(started) == 1)
        thresholds.write_text("filter_cutoff: 0.8\nrouting_cutoff: 0.5\n")
        assert original.load_config("thresholds")["filter_cutoff"] == 0.4
        reload_event.set()
        await until(lambda: len(started) == 2)
        assert started[0][0] != started[1][0] and started[1][1] == 0.8
        assert original.code_version == supervisor.settings.code_version
        assert (base.data_dir / "config_versions" / f"{original.config_version}.json").exists()
        thresholds.write_text("filter_cutoff: [malformed\n")
        reload_event.set()
        await until(lambda: len(loads) == 2)
        await asyncio.sleep(0.01)
        assert len(started) == 2 and supervisor.settings.load_config("thresholds")["filter_cutoff"] == 0.8
    finally:
        stop.set()
        await task


@pytest.mark.asyncio
async def test_controlled_generation_timeout_preserves_last_job_attempt(monkeypatch):
    import tank.runtime as module

    repo = MemoryRepository()
    stop = asyncio.Event()
    started = asyncio.Event()

    class Service:
        async def start(self):
            pass

        async def run(self, flag):
            await flag.wait()

        async def close(self):
            pass

    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.settings = SimpleNamespace(shutdown_grace_s=0.01)
    runtime.active_jobs = set()
    runtime.validate = lambda: None
    runtime.bootstrap = AsyncMock()
    for name in (
        "sources",
        "scheduler",
        "market",
        "sessions",
        "pipeline",
        "delivery",
        "event_tools",
        "gateway",
    ):
        setattr(runtime, name, Service())

    async def unfinished(payload):
        started.set()
        await asyncio.Event().wait()

    runtime.handlers = {"LAST_ATTEMPT": unfinished}
    monkeypatch.setitem(module.TIMEOUTS, "LAST_ATTEMPT", 10)
    job = await repo.enqueue("LAST_ATTEMPT", {}, "last", max_attempts=1)
    task = asyncio.create_task(runtime.run(stop))
    await started.wait()
    stop.set()
    await task
    preserved = await repo.get("ops.jobs", job["job_id"])
    assert preserved["status"] == "QUEUED" and preserved["attempts"] == 0
    assert preserved["interruption_reason"] == "CONTROLLED_GENERATION_STOP"


def test_doctor_only_checks_fake_subscription_auth_presence_and_internal_key(monkeypatch, tmp_path):
    import json
    from pathlib import Path

    from tank.cli import readiness_checks

    auth = tmp_path / "fake-auth.json"
    auth.write_text("fake contents must not be read by doctor")
    original_bytes = Path.read_bytes
    original_text = Path.read_text

    def read_bytes(path, *args, **kwargs):
        if path == auth:
            raise AssertionError("Doctor must not read sign-in contents")
        return original_bytes(path, *args, **kwargs)

    def read_text(path, *args, **kwargs):
        if path == auth:
            raise AssertionError("Doctor must not read sign-in contents")
        return original_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    monkeypatch.setattr(Path, "read_text", read_text)
    settings = SimpleNamespace(
        codex_auth_path=auth,
        codex_gateway_key="fake-internal-secret",
        x_profile_dir=tmp_path / "profile",
        load_config=lambda name: {},
    )
    checks = readiness_checks(settings)
    indexed = {r["check"]: r["ok"] for r in checks}
    assert indexed["codex_auth_file_present"] and indexed["codex_gateway_key"]
    assert "fake-internal-secret" not in json.dumps(checks)


@pytest.mark.asyncio
async def test_gateway_starts_in_same_runtime_before_sources_and_closes_after_drain():
    repo = MemoryRepository()
    stop = asyncio.Event()
    binding = asyncio.Event()
    allow_bind = asyncio.Event()
    order = []

    class Service:
        def __init__(self, name):
            self.name = name
            self.started = asyncio.Event()

        async def run(self, flag):
            order.append("run:" + self.name)
            self.started.set()
            await flag.wait()

        async def close(self):
            order.append("close:" + self.name)

    class Gateway:
        async def start(self):
            order.append("gateway:binding")
            binding.set()
            await allow_bind.wait()
            order.append("gateway:ready")

        async def close(self):
            order.append("gateway:closed")

    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.settings = SimpleNamespace(shutdown_grace_s=1)
    runtime.active_jobs = set()
    runtime.validate = lambda: None
    runtime.bootstrap = AsyncMock()
    runtime.handlers = {}
    runtime.gateway = Gateway()
    for name in ("sources", "scheduler", "market", "sessions", "pipeline", "delivery", "event_tools"):
        setattr(runtime, name, Service(name))
    task = asyncio.create_task(runtime.run(stop))
    await binding.wait()
    assert not runtime.sources.started.is_set() and not runtime.sessions.started.is_set()
    allow_bind.set()
    await runtime.sources.started.wait()
    await runtime.sessions.started.wait()
    assert order.index("gateway:ready") < order.index("run:sources")
    stop.set()
    await task
    assert order[-1] == "gateway:closed" and order.index("close:sessions") < order.index("gateway:closed")


@pytest.mark.asyncio
async def test_unfinished_dependency_defers_without_error_or_attempt_cost(caplog):
    from datetime import timedelta

    from tank.util import iso, parse_time, utcnow

    repo = MemoryRepository()
    stop = asyncio.Event()
    dep = await repo.enqueue("ESCALATE_TOKENS", {}, "pending", run_after=iso(utcnow() + timedelta(hours=1)))
    child = await repo.enqueue("RUN_MARKET_TRUTH", {"_depends_on": [dep["job_id"]]}, "child", priority=0)
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.handlers = {"RUN_MARKET_TRUTH": AsyncMock()}
    task = asyncio.create_task(runtime.worker(stop))
    try:
        async with asyncio.timeout(1):
            while True:
                row = await repo.get("ops.jobs", child["job_id"])
                if row["status"] == "QUEUED" and parse_time(row["run_after"]) > parse_time(
                    child["run_after"]
                ):
                    break
                await asyncio.sleep(0.001)
        assert row["attempts"] == 0 and row["last_error"] is None
        runtime.handlers["RUN_MARKET_TRUTH"].assert_not_called()
        assert not [r for r in caplog.records if r.levelno >= 40]
    finally:
        stop.set()
        await task


@pytest.mark.asyncio
async def test_terminal_delivery_reconciliation_covers_expiry_restart_and_validation():
    from tank.delivery import DeliveryService
    from tank.util import iso

    repo = MemoryRepository()
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.delivery = DeliveryService(repo, SimpleNamespace(), http=SimpleNamespace())
    for key, status in [("expired", "EXPIRED"), ("validation", "FAILED")]:
        job = await repo.enqueue(
            "DELIVER", {"alert_id": key, "version": 1, "channel": "telegram:alerts"}, key, max_attempts=1
        )
        await repo.put(
            "ops.jobs",
            job["job_id"],
            {**job, "status": status, "finished_at": iso(), "last_error": "ValueError"},
        )
    await runtime.reconcile_delivery_failures()
    runtime._delivery_reconciled_at = None
    await runtime.reconcile_delivery_failures()
    ledger = await repo.list("out.deliveries")
    assert len(ledger) == 2 and all(row["status"] == "FAILED" and row["terminal"] for row in ledger)
    notices = await repo.list("out.ops_notices")
    assert len(notices) == 2 and all(row["kind"] == "DELIVERY_FAILED" for row in notices)
    assert len(await repo.list("ops.jobs", filters={"status": "QUEUED"})) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", ["PARTIAL", "FAILED"])
async def test_real_capture_producer_truth_worker_defers_then_uses_terminal_partial(
    tmp_path, terminal_status
):
    from datetime import timedelta

    import httpx

    from tank.analysis.truth import MarketTruthService
    from tank.market.service import MarketService
    from tank.util import iso, utcnow

    class CaptureRepository(MemoryRepository):
        def __init__(self):
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def put(self, collection, key, row, **kwargs):
            result = await super().put(collection, key, row, **kwargs)
            if collection == "mkt.backfill_runs" and row["status"] == "RUNNING":
                self.started.set()
                await self.release.wait()
                if terminal_status == "FAILED":
                    raise RuntimeError("Synthetic capture persistence failure")
            return result

    repo = CaptureRepository()
    now = utcnow()
    anchor = iso(now - timedelta(hours=1))
    await repo.put("evt.events", "e", {"event_id": "e", "known_to_us_at": anchor, "public_at": anchor})
    configs = {
        "registry": {"tokens": [{"token_id": "T", "symbol": "T"}], "instruments": []},
        "semantics": {"metrics": {}},
        "thresholds": {},
    }
    settings = SimpleNamespace(
        load_config=lambda name: configs[name],
        data_dir=tmp_path,
        config_version="fixture",
        code_version="fixture",
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda req: (_ for _ in ()).throw(AssertionError("No external requests"))
        )
    ) as http:
        market = MarketService(repo, settings, http=http)
        truth = MarketTruthService(repo, settings, market)
        original = truth.assess
        truth.assess = AsyncMock(wraps=original)
        runtime = object.__new__(Runtime)
        runtime.repo = repo
        runtime.market = market
        runtime.truth = truth
        runtime.handlers = {
            "RUN_MARKET_TRUTH": runtime.assess,
            "EVAL_CONDITIONS": __import__("tank.scheduler", fromlist=["Scheduler"])
            .Scheduler(repo, settings, market, truth)
            .eval_conditions,
        }
        runtime.active_jobs = set()
        # Actual durable producer, held at persisted RUNNING before acquisition.
        await market._schedule_event_backfill("e", "T", anchor)
        await asyncio.wait_for(repo.started.wait(), 1)
        job = await repo.enqueue(
            "RUN_MARKET_TRUTH",
            {"event_id": "e", "token_id": "T", "cutoff": iso(now)},
            "truth",
            max_attempts=2,
        )
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            async with asyncio.timeout(2):
                while True:
                    row = await repo.get("ops.jobs", job["job_id"])
                    if row.get("last_deferred_reason") == "PendingBackfill":
                        break
                    await asyncio.sleep(0.001)
            assert row["attempts"] == 0 and row["expires_at"] == job["expires_at"]
            truth.assess.assert_not_awaited()
            # A different anchor's completed request must not release this dependency.
            await repo.put(
                "mkt.backfill_runs",
                "different",
                {
                    "backfill_id": "different",
                    "event_id": "e",
                    "token_id": "T",
                    "anchor": iso(now - timedelta(days=1)),
                    "started_at": iso(now),
                    "status": "COMPLETE",
                },
            )
            await asyncio.sleep(0.02)
            truth.assess.assert_not_awaited()
            repo.release.set()
            async with asyncio.timeout(3):
                while (await repo.get("ops.jobs", job["job_id"]))["status"] != "DONE":
                    await asyncio.sleep(0.01)
            done = await repo.get("ops.jobs", job["job_id"])
            assert done["attempts"] == 1 and done["result"]["token_id"] == "T"
            assert (await market.capture_readiness("e", "T", anchor=anchor))["status"] == terminal_status
            assert done["result"]["data_confidence"]["class"] == "LOW"
            assert done["result"]["detail"]["capture_status"]["status"] == terminal_status
        finally:
            stop.set()
            repo.release.set()
            await worker
            await market.close()
