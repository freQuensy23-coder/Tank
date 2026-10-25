import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from agent_source_fixtures import declared_versions_fixture
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict

from tank.analysis.truth_contracts import MarketTruthAssessmentRef
from tank.contracts import DomainModel
from tank.contracts.json import decode_json, encode_json
from tank.contracts.progress import ProgressPreferences
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.market.capture_contracts import CaptureReadiness
from tank.runtime import Runtime
from tank.runtime_contracts import MarketTruthJob
from tank.settings import Settings, resolve_versions
from tank.storage import MemoryRepository, Repository
from tank.time_precision import Timestamp
from tank.util import JsonInput, clock_iso, parse_time


def run_fixture_database(operation: Callable[[Repository, Settings], Awaitable[JsonInput]]) -> None:
    async def invoke() -> None:
        repo = MemoryRepository()
        try:
            await operation(repo, resolve_versions(Settings()))
        finally:
            await repo.close()

    asyncio.run(invoke())


@pytest.mark.asyncio
async def test_timeout_reports_suspended_operation_without_exception_values(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import tank.runtime as module

    repo = MemoryRepository()
    stop = asyncio.Event()
    job = await repo.enqueue("RUN_MARKET_TRUTH", {}, "timeout-frames", max_attempts=1)
    runtime = object.__new__(Runtime)
    runtime.repo = repo

    async def wait_for_snapshot(payload):
        private_url = "https://example.test/private-token"
        try:
            await asyncio.Event().wait()
        finally:
            stop.set()
        return private_url

    monkeypatch.setitem(module.TIMEOUTS, "RUN_MARKET_TRUTH", 0.01)
    runtime.handlers = {"RUN_MARKET_TRUTH": wait_for_snapshot}
    await runtime.worker(stop)
    assert (await repo.get("ops.jobs", job["job_id"]))["status"] == "FAILED"
    assert "wait_for_snapshot" in caplog.text
    assert "TimeoutError" in caplog.text
    assert "private-token" not in caplog.text


@pytest.mark.asyncio
async def test_worker_survives_bad_job_and_persists_result() -> None:
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
async def test_worker_refuses_failed_dependency() -> None:
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


def test_doctor_distinguishes_syntax_from_deployment_and_never_prints_secrets(tmp_path: Path) -> None:
    import json

    from tank.cli import readiness_checks
    from tank.settings import Settings

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    for name in ("sources", "registry", "semantics", "dependency_map", "thresholds", "openma"):
        (config_dir / (name + ".yaml")).write_text("{}\n")
    (config_dir / "openma.yaml").write_text(
        json.dumps(
            {"agent_id": None, "scout_agent_id": None, "environment_id": None, "replay_environment_id": None}
        )
    )
    settings = Settings(
        config_dir=config_dir,
        openma_api_key="sensitive-secret",
        x_list_id="REPLACE_WITH_LIST",
        user_agent="Tank contact not configured",
    )
    checks = readiness_checks(settings)
    by_name = {r.check: r.ok for r in checks}
    assert by_name["openma_syntax"]
    assert not by_name["openma_agent_id"] and not by_name["openma_scout_agent_id"]
    assert not by_name["openma_environment_id"] and not by_name["openma_replay_environment_id"]
    assert by_name["openma_api_key"]
    assert not by_name["user_agent_contact"] and not by_name["apify_api_token"]
    assert "sensitive-secret" not in json.dumps([check.model_dump(mode="json") for check in checks])


def test_source_check_network_probes_are_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    from click.testing import CliRunner

    import tank.sources
    from tank import cli

    calls: list[dict[str, bool]] = []

    class Source:
        def __init__(
            self, repo: Repository, settings: Settings, *, versions: ResolvedVersionIdentity
        ) -> None:
            pass

        async def check(self, *, probe: bool, check_accounts: bool) -> list[JsonValue]:
            calls.append({"probe": probe, "check_accounts": check_accounts})
            return []

        async def close(self) -> None:
            pass

    monkeypatch.setattr(tank.sources, "SourceService", Source)
    monkeypatch.setattr(cli, "run_db", run_fixture_database)
    assert CliRunner().invoke(cli.main, ["sources", "check"]).exit_code == 0
    assert calls[-1] == {"probe": False, "check_accounts": False}
    assert CliRunner().invoke(cli.main, ["sources", "check", "--probe", "--accounts"]).exit_code == 0
    assert calls[-1] == {"probe": True, "check_accounts": True}


def test_manual_backup_defaults_to_no_retention_and_forwards_force(monkeypatch: pytest.MonkeyPatch) -> None:
    from click.testing import CliRunner

    import tank.scheduler
    from tank import cli

    calls: list[dict[str, bool]] = []

    class Scheduler:
        def __init__(
            self,
            repo: Repository,
            settings: Settings,
            market: None,
            truth: None,
            *,
            versions: ResolvedVersionIdentity,
        ) -> None:
            pass

        async def maintenance(self, now: datetime, *, force: bool, prune: bool) -> dict[str, JsonValue]:
            calls.append({"force": force, "prune": prune})
            return {"status": "created"}

    monkeypatch.setattr(tank.scheduler, "Scheduler", Scheduler)
    monkeypatch.setattr(cli, "run_db", run_fixture_database)
    assert CliRunner().invoke(cli.main, ["backup", "--force"]).exit_code == 0
    assert calls[-1] == {"force": True, "prune": False}
    assert CliRunner().invoke(cli.main, ["backup", "--prune"]).exit_code == 0
    assert calls[-1] == {"force": False, "prune": True}


@pytest.mark.asyncio
async def test_reload_rejects_candidate_then_drains_old_generation_before_swap() -> None:
    from tank.runtime_composition import RuntimeSupervisor

    stop = asyncio.Event()
    reload_event = asyncio.Event()
    old_draining = asyncio.Event()
    allow_finish = asyncio.Event()
    started = []
    closed = []
    loaded = []
    old = SimpleNamespace(
        database_url="same-db",
        openma_url="same-native",
        data_dir=Path("/fixture"),
        config_version="old",
        valid=True,
    )
    invalid = SimpleNamespace(
        database_url="same-db",
        openma_url="same-native",
        data_dir=Path("/fixture"),
        config_version="invalid",
        valid=False,
    )
    new = SimpleNamespace(
        database_url="same-db",
        openma_url="same-native",
        data_dir=Path("/fixture"),
        config_version="new",
        valid=True,
    )
    candidates = iter([invalid, new])

    def loader():
        candidate = next(candidates)
        loaded.append(candidate.config_version)
        return candidate

    def validate(settings) -> None:
        if not settings.valid:
            raise ValueError("Invalid candidate configuration")

    class Generation:
        def __init__(self, repo, settings) -> None:
            self.settings = settings

        def validate(self) -> None:
            validate(self.settings)

        async def prepare(self) -> None:
            self.validate()

        async def run(self, flag) -> None:
            started.append(self.settings.config_version)
            await flag.wait()
            if self.settings.config_version == "old":
                old_draining.set()
                await allow_finish.wait()
            await self.close()

        async def close(self) -> None:
            closed.append(self.settings.config_version)

    supervisor = RuntimeSupervisor(None, old, candidate_loader=loader, factory=Generation, validator=validate)
    task = asyncio.create_task(supervisor.run(stop, reload_event))

    async def until(predicate) -> None:
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
async def test_runtime_quiescence_drains_old_job_dependencies_and_preserves_session_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import timedelta

    import tank.runtime as module
    from tank.util import iso, utcnow

    repo = MemoryRepository()
    stop = asyncio.Event()
    started = asyncio.Event()
    release = asyncio.Event()

    class Service:
        def __init__(self) -> None:
            self.stopped = asyncio.Event()
            self.closed = False

        async def start(self) -> None:
            pass

        async def run(self, flag) -> None:
            await flag.wait()
            self.stopped.set()

        async def close(self) -> None:
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
    runtime.inbox = Service()
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
            "returned_at": clock_iso(),
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
            runtime.inbox,
        ]
    )


def test_review_cli_forwards_period_paging_and_structured_checks(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import json

    from click.testing import CliRunner

    import tank.review
    from tank import cli

    calls = []

    class Review:
        def __init__(self, *args) -> None:
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
            [
                "review",
                "list",
                "--since",
                since,
                "--until",
                until,
                "--after",
                '["2026-10-01T00:00:00Z", "a:1"]',
                "--limit",
                "20",
            ],
        ).exit_code
        == 0
    )
    assert calls[-1] == (
        "list",
        {
            "start": since,
            "end": until,
            "limit": 20,
            "after": ["2026-10-01T00:00:00Z", "a:1"],
            "event_id": None,
        },
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


def test_review_label_records_latest_or_explicit_alert_version(monkeypatch: pytest.MonkeyPatch) -> None:
    from click.testing import CliRunner

    from tank import cli

    repo = MemoryRepository()

    async def seed() -> None:
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
async def test_reload_uses_real_immutable_settings_snapshot_and_rejects_malformed_edit(
    tmp_path: Path,
) -> None:
    from tank.runtime_composition import RuntimeSupervisor, validate_runtime_settings
    from tank.settings import Settings, resolve_versions

    config = tmp_path / "config"
    config.mkdir()
    backup = tmp_path / "backup"
    backup.mkdir()
    import yaml

    class ReloadFixtureSettings(Settings):
        model_config = SettingsConfigDict(env_file=None)

    class GenerationStart(DomainModel):
        config_version: str
        filter_cutoff: float

    catalog_bytes = encode_json({"pools": []})
    (config / "reload-pools.json").write_bytes(catalog_bytes)
    registry_fixture: dict[str, JsonValue] = {
        "version": "reload-fixture",
        "tokens": [],
        "instruments": [],
        "entities": [
            {
                "entity_id": "reload-fixture",
                "kind": "OTHER",
                "names": ["Synthetic reload entity"],
                "aliases": ["Synthetic reload entity"],
                "tokens": [],
                "official_sources": [],
                "layers_served": [],
                "competes_with": [],
            }
        ],
        "sector_baskets": [],
        "x_accounts": [],
        "lending_pool_catalog": {
            "source_url": "fixture:reload-empty-pools",
            "captured_at": "2026-10-25T00:00:00Z",
            "source_sha256": hashlib.sha256(catalog_bytes).hexdigest(),
            "pools": [],
        },
    }
    documents: dict[str, dict[str, JsonValue]] = {
        "registry": registry_fixture,
        "sources": {
            "version": "reload-fixture",
            "contact_email": "runtime@example.test",
            "sources": [],
        },
        "semantics": {"version": "reload-fixture", "metrics": []},
        "dependency_map": {"version": "reload-fixture", "edges": []},
        "exchange_addresses": {
            "version": "reload-fixture",
            "wallet_lists": [],
            "unavailable": {},
            "setup_required": "Synthetic reload fixture declares no wallet catalogs",
        },
    }
    for name, document in documents.items():
        (config / (name + ".yaml")).write_bytes(encode_json(document))

    thresholds = config / "thresholds.yaml"
    threshold_values = decode_json(ReloadFixtureSettings().thresholds().model_dump_json())
    assert isinstance(threshold_values, dict)

    def write_thresholds(value: float) -> None:
        thresholds.write_text(
            yaml.safe_dump({**threshold_values, "filter_cutoff": value, "routing_cutoff": 0.5})
        )

    write_thresholds(0.4)
    base = ReloadFixtureSettings(
        config_dir=config,
        data_dir=tmp_path / "data",
        backup_dir=backup,
        telegram_token="test-secret",
        telegram_alerts_chat="1",
        telegram_ops_chat="2",
    )
    original = resolve_versions(base)
    started: list[GenerationStart] = []
    loads: list[bool] = []
    stop = asyncio.Event()
    reload_event = asyncio.Event()

    class Generation:
        def __init__(self, repo: None, settings: Settings) -> None:
            self.settings = settings

        def validate(self) -> None:
            validate_runtime_settings(self.settings)

        async def prepare(self) -> None:
            self.validate()

        async def run(self, flag: asyncio.Event) -> None:
            started.append(
                GenerationStart(
                    config_version=self.settings.config_version,
                    filter_cutoff=self.settings.thresholds().filter_cutoff,
                )
            )
            await flag.wait()

        async def close(self) -> None:
            pass

    def load() -> Settings:
        loads.append(True)
        return resolve_versions(base)

    supervisor = RuntimeSupervisor(
        None, original, candidate_loader=load, factory=Generation, validator=validate_runtime_settings
    )
    task = asyncio.create_task(supervisor.run(stop, reload_event))

    async def until(predicate: Callable[[], bool]) -> None:
        async with asyncio.timeout(2):
            while not predicate():
                await asyncio.sleep(0.001)

    try:
        await until(lambda: len(started) == 1)
        write_thresholds(0.8)
        assert original.load_config("thresholds")["filter_cutoff"] == 0.4
        reload_event.set()
        await until(lambda: len(started) == 2)
        assert started[0].config_version != started[1].config_version
        assert started[1].filter_cutoff == 0.8
        assert original.code_version == supervisor.settings.code_version
        assert (base.data_dir / "config_versions" / f"{original.config_version}.json").exists()
        thresholds.write_text("filter_cutoff: [malformed\n")
        reload_event.set()
        await until(lambda: len(loads) == 2)
        await asyncio.sleep(0.01)
        assert len(started) == 2 and supervisor.settings.load_config("thresholds")["filter_cutoff"] == 0.8
        write_thresholds(0.8)
        broken_registry = supervisor.settings.load_config("registry")
        entities = broken_registry["entities"]
        assert isinstance(entities, list)
        entity = entities[0]
        assert isinstance(entity, dict)
        entity["qualified_aliases"] = [{"alias": "Generic", "requires_any": []}]
        (config / "registry.yaml").write_text(yaml.safe_dump(broken_registry))
        reload_event.set()
        await until(lambda: len(loads) == 3)
        await asyncio.sleep(0.01)
        assert len(started) == 2 and not task.done()
        assert supervisor.settings.load_config("registry") == registry_fixture
    finally:
        stop.set()
        await task


@pytest.mark.asyncio
async def test_controlled_generation_timeout_preserves_last_job_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import tank.runtime as module

    repo = MemoryRepository()
    stop = asyncio.Event()
    started = asyncio.Event()

    class Service:
        async def start(self) -> None:
            pass

        async def run(self, flag) -> None:
            await flag.wait()

        async def close(self) -> None:
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
        "inbox",
        "gateway",
    ):
        setattr(runtime, name, Service())

    async def unfinished(payload) -> None:
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
    assert preserved["run_after"] == preserved["interrupted_at"]
    assert preserved["run_after_prec"] == preserved["interrupted_at_prec"] == "COLLECTION_ONLY"


def test_doctor_only_checks_fake_subscription_auth_presence_and_internal_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import json
    from pathlib import Path

    from tank.cli import readiness_checks

    auth = tmp_path / "fake-auth.json"
    auth.write_text("fake contents must not be read by doctor")
    original_bytes = Path.read_bytes
    original_text = Path.read_text

    def read_bytes(path: Path) -> bytes:
        if path == auth:
            raise AssertionError("Doctor must not read sign-in contents")
        return original_bytes(path)

    def read_text(
        path: Path, encoding: str | None = None, errors: str | None = None, newline: str | None = None
    ) -> str:
        if path == auth:
            raise AssertionError("Doctor must not read sign-in contents")
        return original_text(path, encoding=encoding, errors=errors, newline=newline)

    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    monkeypatch.setattr(Path, "read_text", read_text)
    from tank.settings import Settings

    settings = Settings(codex_auth_path=auth, codex_gateway_key="fake-internal-secret")
    checks = readiness_checks(settings)
    indexed = {r.check: r.ok for r in checks}
    assert indexed["codex_auth_file_present"] and indexed["codex_gateway_key"]
    assert "fake-internal-secret" not in json.dumps([check.model_dump(mode="json") for check in checks])


@pytest.mark.asyncio
async def test_gateway_starts_in_same_runtime_before_sources_and_closes_after_drain() -> None:
    repo = MemoryRepository()
    stop = asyncio.Event()
    binding = asyncio.Event()
    allow_bind = asyncio.Event()
    order = []

    class Service:
        def __init__(self, name) -> None:
            self.name = name
            self.started = asyncio.Event()

        async def run(self, flag) -> None:
            order.append("run:" + self.name)
            self.started.set()
            await flag.wait()

        async def close(self) -> None:
            order.append("close:" + self.name)

    class Gateway:
        async def start(self) -> None:
            order.append("gateway:binding")
            binding.set()
            await allow_bind.wait()
            order.append("gateway:ready")

        async def close(self) -> None:
            order.append("gateway:closed")

    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.settings = SimpleNamespace(shutdown_grace_s=1)
    runtime.active_jobs = set()
    runtime.validate = lambda: None
    runtime.bootstrap = AsyncMock()
    runtime.handlers = {}
    runtime.gateway = Gateway()
    for name in ("sources", "scheduler", "market", "sessions", "pipeline", "delivery", "inbox"):
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
async def test_unfinished_dependency_defers_without_error_or_attempt_cost(
    caplog: pytest.LogCaptureFixture,
) -> None:
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
async def test_terminal_delivery_reconciliation_covers_expiry_restart_and_validation() -> None:
    from tank.delivery import DeliveryService

    repo = MemoryRepository()
    runtime = object.__new__(Runtime)
    runtime.repo = repo
    runtime.settings = ProgressPreferences(ops_progress_enabled=False, replay=False)
    runtime.delivery = DeliveryService(repo, runtime.settings, http=SimpleNamespace())
    for key, status in [("expired", "EXPIRED"), ("validation", "FAILED")]:
        job = await repo.enqueue(
            "DELIVER", {"alert_id": key, "version": 1, "channel": "telegram:alerts"}, key, max_attempts=1
        )
        await repo.put(
            "ops.jobs",
            job["job_id"],
            {**job, "status": status, "finished_at": clock_iso(), "last_error": "ValueError"},
        )
    await runtime.reconcile_delivery_failures()
    runtime._delivery_reconciled_at = None
    await runtime.reconcile_delivery_failures()
    ledger = await repo.list("out.deliveries")
    assert len(ledger) == 2 and all(row["status"] == "FAILED" and row["terminal"] for row in ledger)
    notices = await repo.list("out.ops_notices")
    assert len(notices) == 2 and all(row["kind"] == "DELIVERY_FAILED" for row in notices)
    assert len(await repo.list("ops.jobs", filters={"status": "QUEUED"})) == 2
    for status in ("FAILED", "EXPIRED"):
        for job in await repo.list("ops.jobs", filters={"type": "DELIVER", "status": status}):
            assert job["delivery_failure_recorded_at_prec"] == "COLLECTION_ONLY"


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_status", ["PARTIAL", "FAILED"])
async def test_external_capture_consumer_uses_bounded_incomplete_then_terminal_history(
    tmp_path: Path, terminal_status, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datetime import timedelta

    import httpx

    import tank.runtime as runtime_module
    from tank.analysis.truth import MarketTruthService
    from tank.market.service import MarketService
    from tank.util import iso, utcnow

    monkeypatch.setattr(runtime_module, "CAPTURE_WAIT_S", 0.02)

    repo = MemoryRepository()
    capture_started, release_capture, stop_capture = (asyncio.Event() for _ in range(3))
    requests = []

    async def external_capture_worker() -> None:
        claimed = None
        while not stop_capture.is_set():
            if claimed is None:
                pending = await repo.list("mkt.backfill_runs", filters={"status": "PENDING"})
                if pending:
                    (claimed,) = pending
                    assert claimed["event_id"] == "e" and claimed["token_id"] == "T"
                    assert claimed["anchor"] == anchor
                    await repo.put_market_batch(
                        "mkt.backfill_runs",
                        [{**claimed, "status": "RUNNING"}],
                        keys=[claimed["backfill_id"]],
                    )
                    capture_started.set()
            if claimed is not None and release_capture.is_set():
                await repo.put_market_batch(
                    "mkt.backfill_runs",
                    [
                        {
                            **claimed,
                            "status": terminal_status,
                            "result": {"instruments": [], "benchmark_klines": []}
                            if terminal_status == "PARTIAL"
                            else None,
                            "reason": "SYNTHETIC_EXTERNAL_CAPTURE_TERMINAL",
                            "finished_at": clock_iso(),
                            "finished_at_prec": "COLLECTION_ONLY",
                        }
                    ],
                    keys=[claimed["backfill_id"]],
                )
                claimed = None
            for request in await repo.list("mkt.capture_requests", filters={"status": "PENDING"}):
                assert request["kind"] == "REPAIR_WINDOWS" and request["token_id"] == "T"
                requests.append(request)
                await repo.put_market_batch(
                    "mkt.capture_requests",
                    [
                        {
                            **request,
                            "status": "COMPLETE",
                            "result": {"instruments": {}},
                            "finished_at": clock_iso(),
                            "finished_at_prec": "COLLECTION_ONLY",
                        }
                    ],
                    keys=[request["request_id"]],
                )
            await asyncio.sleep(0.001)

    now = utcnow()
    anchor = iso(now - timedelta(hours=1))
    await repo.put("evt.events", "e", {"event_id": "e", "known_to_us_at": anchor, "public_at": anchor})
    configs = {
        "registry": {"tokens": [{"token_id": "T", "symbol": "T"}], "instruments": []},
        "semantics": {"metrics": {}},
        "thresholds": {},
    }
    from tank.settings import Settings

    complete_thresholds = Settings(_env_file=None).thresholds()
    configs["thresholds"] = complete_thresholds.model_dump(mode="json")
    settings = SimpleNamespace(
        load_config=lambda name: configs[name],
        thresholds=lambda: complete_thresholds,
        configured_secrets=lambda: frozenset(),
        replay=False,
        data_dir=tmp_path,
        config_version="fixture",
        code_version="fixture",
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda req: (_ for _ in ()).throw(AssertionError("No external requests"))
        )
    ) as http:
        market = MarketService(repo, settings, http=http, versions=declared_versions_fixture())
        truth = MarketTruthService(repo, settings, market, versions=declared_versions_fixture())
        original = truth.assess
        truth.assess = AsyncMock(wraps=original)
        runtime = object.__new__(Runtime)
        runtime.repo = repo
        runtime.market = market
        runtime.truth = truth
        runtime.handlers = {
            "RUN_MARKET_TRUTH": runtime.assess_wire,
            "EVAL_CONDITIONS": __import__("tank.scheduler", fromlist=["Scheduler"])
            .Scheduler(repo, settings, market, truth, versions=declared_versions_fixture())
            .eval_conditions,
        }
        runtime.active_jobs = set()
        await market._schedule_event_backfill("e", "T", Timestamp(anchor, "EXACT", None))
        capture_task = asyncio.create_task(external_capture_worker())
        job = await repo.enqueue(
            "RUN_MARKET_TRUTH",
            {
                "event_id": "e",
                "token_id": "T",
                "cutoff": iso(now - timedelta(minutes=1)),
                "cutoff_prec": "EXACT",
            },
            "truth",
            max_attempts=2,
        )
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        try:
            await asyncio.wait_for(capture_started.wait(), 1)
            async with asyncio.timeout(3):
                while (await repo.get("ops.jobs", job["job_id"]))["status"] != "DONE":
                    await asyncio.sleep(0.01)
            first = await repo.get("ops.jobs", job["job_id"])
            assert first["attempts"] == 1
            first_assessment = await repo.get("ana.market_truth", first["result"]["mta_id"])
            assert first_assessment["data_confidence"]["class"] == "LOW"
            assert first_assessment["detail"]["capture_status"]["status"] == "INCOMPLETE"
            assert (
                first_assessment["detail"]["capture_status"]["reason"] == "BOUNDED_PREREQUISITE_WAIT_EXPIRED"
            )
            assert first_assessment["detail"]["capture_status"]["bounded_wait_s"] >= 0.02
            assert not release_capture.is_set()
            truth.assess.assert_awaited_once()

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
            assert (
                await market.capture_readiness("e", "T", anchor=Timestamp(anchor, "EXACT", None))
            ).status == "RUNNING"
            release_capture.set()
            async with asyncio.timeout(3):
                while (
                    await market.capture_readiness("e", "T", anchor=Timestamp(anchor, "EXACT", None))
                ).status != terminal_status:
                    await asyncio.sleep(0.01)
            next_job = await repo.enqueue(
                "RUN_MARKET_TRUTH",
                {"event_id": "e", "token_id": "T", "cutoff": iso(now), "cutoff_prec": "EXACT"},
                "truth-after-capture",
                max_attempts=2,
            )
            async with asyncio.timeout(3):
                while (await repo.get("ops.jobs", next_job["job_id"]))["status"] != "DONE":
                    await asyncio.sleep(0.01)
            done = await repo.get("ops.jobs", next_job["job_id"])
            done_assessment = await repo.get("ana.market_truth", done["result"]["mta_id"])
            assert done_assessment["data_confidence"]["class"] == "LOW"
            assert done_assessment["detail"]["capture_status"]["status"] == terminal_status
            assert (await repo.get("ops.jobs", job["job_id"]))["result"] == first["result"]
            assert len(requests) == 2
            for request, cutoff in zip(requests, (iso(now - timedelta(minutes=1)), iso(now)), strict=True):
                assert request["end"] == cutoff
                assert request["start"] == iso(
                    (now - timedelta(hours=1) - runtime_module.REPAIR_LOOKBACK).replace(
                        second=0, microsecond=0
                    )
                )
                assert float(request["budget_s"]) == runtime_module.REPAIR_BUDGET_S
                assert (await repo.get("mkt.capture_requests", request["request_id"]))["status"] == "COMPLETE"
            assert requests[0]["request_id"] != requests[1]["request_id"]
        finally:
            stop.set()
            release_capture.set()
            await worker
            stop_capture.set()
            await capture_task
            await market.close()


@pytest.fixture
async def transport_database(backend, postgres_template):
    """Keep durable worker claims independent from every other test's queue."""
    if backend == "memory":
        yield None
        return
    import os

    source = os.getenv("TANK_TEST_DATABASE_URL")
    if not source:
        pytest.skip("Requires isolated PostgreSQL with CREATE DATABASE permission")
    async with postgres_template.repository(source) as repository:
        await repository.close()
        yield repository.database_url


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_delivery_order_wait_requeues_without_spending_network_attempts(
    monkeypatch: pytest.MonkeyPatch, backend, transport_database
) -> None:
    from datetime import timedelta

    import httpx

    from tank.delivery import DeliveryService
    from tank.storage import PostgresRepository
    from tank.util import iso, new_id, parse_time, sha256, utcnow

    database_url = transport_database
    repo = PostgresRepository(database_url) if backend == "postgres" else MemoryRepository()
    await repo.open()
    base = utcnow() - timedelta(hours=1)
    clock = [base + timedelta(minutes=30)]
    for module in ("tank.util", "tank.storage", "tank.delivery", "tank.runtime"):
        monkeypatch.setattr(module + ".utcnow", lambda: clock[0])

    monkeypatch.setattr("tank.delivery.DELIVERY_LOCK_WAIT_S", 0.2)
    aid = new_id("alert")
    eid = new_id("event")
    settings = Settings(
        telegram_token="fixture",
        telegram_alerts_chat="1",
        telegram_ops_chat="2",
        ops_progress_enabled=False,
        replay=False,
    )
    sent_requests = []

    def send(request: httpx.Request):
        sent_requests.append(request)
        if len(sent_requests) == 1:
            return httpx.Response(503, json={"ok": False})
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 43}})

    try:
        await repo.put(
            "evt.events",
            eid,
            {
                "event_id": eid,
                "statement": "Synthetic accepted event",
                "theme_tags": [],
                "tokens_mentioned": [],
                "public_at": iso(base),
                "public_at_prec": "EXACT",
                "public_at_basis": "Synthetic fixture timestamp",
                "known_to_us_at": iso(base),
                "freshness": "FRESH",
                "event_state": "APPROVED",
                "state_verified": True,
                "certainty": "HIGH",
                "timing_confidence": "HIGH",
                "best_source_class": "PRIMARY",
                "lifecycle": "MONITORING",
            },
        )
        digest = sha256({"fixture": aid})
        await repo.put(
            "out.evidence_bundles",
            digest,
            {"bundle_id": "EVB-" + digest, "sha256": digest, "payload": {"fixture": aid}},
            immutable=True,
        )
        for version in (1, 2, 3):
            await repo.put(
                "out.alerts",
                f"{aid}:{version}",
                {
                    "alert_id": aid,
                    "version": version,
                    "event_id": eid,
                    "kind": "FULL" if version == 1 else "UPDATE",
                    "card_text": f"Immutable accepted version {version}",
                    "decisions": [],
                    "card_struct": {"event": {"thresholds": {"update_rate_limit_s": 900}}},
                    "evidence_bundle_id": digest,
                    "config_version": "fixture",
                    "code_version": "fixture",
                    "delivery_immediate": False,
                    "generated_at": iso(base + timedelta(minutes=version)),
                    "delivery_not_before": iso(base + timedelta(minutes=(version - 1) * 15)),
                },
                immutable=True,
            )
        await repo.put(
            "out.deliveries",
            new_id("delivery"),
            {
                "alert_id": aid,
                "version": 1,
                "channel": "telegram:alerts",
                "status": "SENT",
                "message_ref": "42",
                "attempt": 1,
                "at": iso(base),
            },
            immutable=True,
        )
        older = await repo.enqueue(
            "DELIVER",
            {"alert_id": aid, "version": 2, "channel": "telegram:alerts"},
            f"{aid}:2",
            run_after=iso(base + timedelta(minutes=15)),
            max_attempts=5,
        )
        claimed_older = await repo.claim_job()
        assert claimed_older["job_id"] == older["job_id"]
        current = await repo.enqueue(
            "DELIVER",
            {"alert_id": aid, "version": 3, "channel": "telegram:alerts"},
            f"{aid}:3",
            run_after=iso(clock[0]),
            max_attempts=5,
        )
        original_expiry = current["expires_at"]
        async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
            delivery = DeliveryService(repo, settings, http=http)

            async def run_one() -> None:
                stop = asyncio.Event()
                runtime = object.__new__(Runtime)
                runtime.settings = settings
                runtime.repo = repo
                runtime.delivery = delivery

                async def handle(payload):
                    try:
                        return await delivery.deliver(payload)
                    finally:
                        stop.set()

                runtime.handlers = {"DELIVER": handle}
                await asyncio.wait_for(runtime.worker(stop), 3)

            held, release = asyncio.Event(), asyncio.Event()

            async def other_worker_send() -> None:
                async with repo.lock("event:" + eid):
                    held.set()
                    await release.wait()

            owner = asyncio.create_task(other_worker_send())
            await held.wait()
            for _ in range(7):
                await run_one()
                postponed = await repo.get("ops.jobs", current["job_id"])
                assert postponed["status"] == "QUEUED" and postponed["attempts"] == 0
                assert postponed["expires_at"] == original_expiry
                assert postponed["last_error"] is None
                clock[0] = parse_time(postponed["run_after"])
            assert not sent_requests
            assert not await repo.list("out.deliveries", filters={"alert_id": aid, "version": 3})
            release.set()
            await owner

            await delivery.deliver(older["payload"])
            assert not sent_requests
            assert not await repo.list("out.deliveries", filters={"alert_id": aid, "version": 2})
            old_job = await repo.get("ops.jobs", older["job_id"])
            await repo.put("ops.jobs", older["job_id"], {**old_job, "status": "DONE"})
            await run_one()
            failed_http = await repo.get("ops.jobs", current["job_id"])
            assert failed_http["status"] == "QUEUED" and failed_http["attempts"] == 1
            assert len(sent_requests) == 1
            clock[0] = parse_time(failed_http["run_after"])
            await run_one()
            finished = await repo.get("ops.jobs", current["job_id"])
            assert finished["status"] == "DONE" and finished["attempts"] == 2
            assert finished["expires_at"] == original_expiry
            assert len(sent_requests) == 2
            await delivery.deliver(current["payload"])
            assert len(sent_requests) == 2
            ledger = await repo.list("out.deliveries", filters={"alert_id": aid, "version": 3})
            assert [row["status"] for row in ledger] == ["GENERATED", "FAILED", "GENERATED", "SENT"]
    finally:
        await repo.close()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
@pytest.mark.parametrize("lock_kind", ["event", "delivery"])
async def test_contended_delivery_lock_is_scheduling_then_sends_once(
    monkeypatch: pytest.MonkeyPatch, backend, lock_kind, transport_database
) -> None:

    import httpx

    from tank.delivery import DeliveryService
    from tank.runtime import TIMEOUTS
    from tank.storage import PostgresRepository
    from tank.util import iso, new_id, parse_time, sha256, utcnow

    database_url = transport_database
    repo = PostgresRepository(database_url) if backend == "postgres" else MemoryRepository()
    await repo.open()
    aid, eid = new_id("alert"), new_id("event")
    now = utcnow()
    held, release, acquiring = asyncio.Event(), asyncio.Event(), asyncio.Event()
    key = "event:" + eid if lock_kind == "event" else f"deliver:{aid}:1:telegram:alerts"
    settings = Settings(
        telegram_token="fixture",
        telegram_alerts_chat="1",
        telegram_ops_chat="2",
        ops_progress_enabled=False,
        replay=False,
    )
    requests = []

    async def holder() -> None:
        async with repo.lock(key):
            held.set()
            await release.wait()

    holder_task = waiter = None
    try:
        await repo.put(
            "evt.events",
            eid,
            {
                "event_id": eid,
                "statement": "Synthetic accepted event",
                "theme_tags": [],
                "public_at": iso(now),
                "public_at_prec": "EXACT",
                "public_at_basis": "Synthetic fixture",
                "known_to_us_at": iso(now),
                "freshness": "FRESH",
                "event_state": "APPROVED",
                "state_verified": True,
                "certainty": "HIGH",
                "timing_confidence": "HIGH",
                "best_source_class": "PRIMARY",
                "lifecycle": "MONITORING",
            },
        )
        digest = sha256({"fixture": aid})
        await repo.put(
            "out.evidence_bundles",
            digest,
            {"bundle_id": "EVB-" + digest, "sha256": digest, "payload": {"fixture": aid}},
            immutable=True,
        )
        alert = {
            "alert_id": aid,
            "version": 1,
            "event_id": eid,
            "kind": "FULL",
            "generated_at": iso(now),
            "card_text": "Accepted immutable snapshot",
            "card_struct": {"event": {}},
            "evidence_bundle_id": digest,
            "decisions": [],
            "config_version": "fixture",
            "code_version": "fixture",
        }
        await repo.put("out.alerts", aid + ":1", alert, immutable=True)
        job = await repo.enqueue(
            "DELIVER",
            {"alert_id": aid, "version": 1, "channel": "telegram:alerts"},
            aid + ":1",
            max_attempts=1,
        )
        holder_task = asyncio.create_task(holder())
        await asyncio.wait_for(held.wait(), 2)

        def transport(request: httpx.Request):
            requests.append(request)
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})

        async with httpx.AsyncClient(transport=httpx.MockTransport(transport), timeout=8) as http:
            delivery = DeliveryService(repo, settings, http=http)
            original_enter = delivery._enter_delivery_lock

            async def observed_enter(stack, target):
                if target == key:
                    acquiring.set()
                return await original_enter(stack, target)

            monkeypatch.setattr(delivery, "_enter_delivery_lock", observed_enter)

            waiter = asyncio.create_task(delivery.deliver(job["payload"]))
            await asyncio.wait_for(acquiring.wait(), 2)
            await asyncio.sleep(0.01)
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            assert not requests and not await repo.list("out.deliveries", filters={"alert_id": aid})

            async def run_one() -> None:
                stop = asyncio.Event()
                runtime = object.__new__(Runtime)
                runtime.settings = settings
                runtime.repo, runtime.delivery = repo, delivery

                async def handle(payload):
                    try:
                        return await delivery.deliver(payload)
                    finally:
                        stop.set()

                runtime.handlers = {"DELIVER": handle}
                await asyncio.wait_for(runtime.worker(stop), TIMEOUTS["DELIVER"] + 2)

            assert TIMEOUTS["DELIVER"] > http.timeout.read == 8
            await run_one()
            queued = await repo.get("ops.jobs", job["job_id"])
            assert queued["status"] == "QUEUED" and queued["attempts"] == 0
            assert queued["expires_at"] == job["expires_at"]
            assert queued["last_error"] is None
            assert not requests and not await repo.list("out.deliveries", filters={"alert_id": aid})
            release.set()
            await asyncio.wait_for(holder_task, 2)

            async with repo.lock("event:" + eid), repo.lock(f"deliver:{aid}:1:telegram:alerts"):
                assert await repo.get("out.alerts", aid + ":1") == alert
            await asyncio.sleep(max(0, (parse_time(queued["run_after"]) - utcnow()).total_seconds()) + 0.02)
            await run_one()
            done = await repo.get("ops.jobs", job["job_id"])
            assert done["status"] == "DONE" and done["attempts"] == 1
            assert len(requests) == 1
            ledger = await repo.list("out.deliveries", filters={"alert_id": aid})
            assert sorted(row["status"] for row in ledger) == ["GENERATED", "SENT"]
    finally:
        release.set()
        if waiter is not None and not waiter.done():
            waiter.cancel()
        await asyncio.gather(
            *(task for task in (waiter, holder_task) if task is not None), return_exceptions=True
        )
        await repo.close()


@pytest.mark.asyncio
async def test_market_truth_repairs_windows_within_budget_before_assessing() -> None:
    from datetime import timedelta

    from tank import runtime as runtime_module

    calls = []
    anchor = "2026-10-20T12:00:00Z"

    class Market:
        async def capture_readiness(
            self, event_id: str, token_id: str, *, anchor: Timestamp
        ) -> CaptureReadiness:
            return CaptureReadiness(
                event_id=event_id,
                token_id=token_id,
                status="COMPLETE",
                reason=None,
                backfill_id="capture",
                anchor=anchor.value,
            )

        async def repair_windows(self, token_id, start, end, *, budget_s=None) -> None:
            calls.append(("repair", token_id, start, end, budget_s))

    class Truth:
        async def assess_for_job(
            self, event_id: str, token_id: str, cutoff: Timestamp | None
        ) -> MarketTruthAssessmentRef:
            calls.append(("assess", token_id))
            return MarketTruthAssessmentRef(
                mta_id="MTA",
                event_id=event_id,
                token_id=token_id,
                cutoff="2026-10-20T13:00:00Z",
                cutoff_prec="EXACT",
                cutoff_prec_reason=None,
            )

    repo = MemoryRepository()
    await repo.put("evt.events", "e", {"event_id": "e", "public_at": anchor, "known_to_us_at": anchor})
    rt = object.__new__(Runtime)
    rt.repo, rt.market, rt.truth = repo, Market(), Truth()
    result = await rt.assess(
        MarketTruthJob(event_id="e", token_id="T", cutoff="2026-10-20T13:00:00Z", cutoff_prec="EXACT")
    )
    assert result.mta_id == "MTA" and [c[0] for c in calls] == ["repair", "assess"]
    _, token, start, end, budget = calls[0]
    assert token == "T" and budget == runtime_module.REPAIR_BUDGET_S
    assert end == parse_time("2026-10-20T13:00:00Z")
    assert end - start <= timedelta(hours=25) and start <= parse_time(anchor) - timedelta(hours=24)

    calls.clear()
    rt.market = Market()
    await rt.assess(MarketTruthJob(event_id="e", token_id="T"))
    assert [call[0] for call in calls] == ["repair", "assess"]
