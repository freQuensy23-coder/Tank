"""Operator CLI. Commands never send Telegram messages except `serve`."""

import asyncio
import json
import signal
import sys
from collections.abc import Awaitable
from pathlib import Path

import click
from pydantic import Field

from tank.contracts import DomainModel
from tank.contracts.configuration import DeliveryConfiguration
from tank.contracts.sources import SourceApiLimitsConfiguration, SourceConfiguration
from tank.logging import configure_logging
from tank.settings import Settings, configured_values, resolve_versions
from tank.time_precision import collection_clock
from tank.util import iso, jsonable, new_id

GIL_SWITCH_INTERVAL_S = 0.0005


def output(value):
    click.echo(json.dumps(jsonable(value), ensure_ascii=False, indent=2))


def run_db(fn, *, database_url=None, replay=False):
    async def execute():
        from tank.storage import PostgresRepository

        settings = resolve_versions(Settings())
        configure_logging(settings)
        if replay:
            if not database_url or database_url == settings.database_url:
                raise ValueError(
                    "Replay requires an explicit isolated --database-url distinct from production"
                )
            settings = settings.model_copy(update={"database_url": database_url, "replay": True})
        repo = PostgresRepository(settings.database_url)
        from tank import clickhouse_market

        clickhouse = clickhouse_market.install(repo, settings)
        await repo.open()
        try:
            return await fn(repo, settings)
        finally:
            await repo.close()
            if clickhouse is not None:
                await clickhouse.close()

    try:
        output(asyncio.run(execute()))
    except ExceptionGroup as exc:
        raise click.ClickException(
            "Operation failed with multiple service errors: "
            + ", ".join(type(error).__name__ for error in exc.exceptions)
        ) from None
    except (ValueError, RuntimeError) as exc:
        raise click.ClickException(str(exc)) from None
    except Exception as exc:
        raise click.ClickException(
            "Operation failed: " + type(exc).__name__ + "; check database connectivity and operator logs"
        ) from None


async def run_with_synchronization[T](
    runtime_work: Awaitable[None],
    synchronization_work: Awaitable[T],
    stop: asyncio.Event,
) -> None:
    """Observe the synchronizer and drain the owned runtime before reporting a failure."""
    runtime = asyncio.ensure_future(runtime_work)
    synchronization = asyncio.ensure_future(synchronization_work)
    primary: BaseException | None = None
    cancelled_sync_for_shutdown = False
    try:
        done, _ = await asyncio.wait((runtime, synchronization), return_when=asyncio.FIRST_COMPLETED)
        if synchronization in done:
            await synchronization
        await asyncio.shield(runtime)
    except BaseException as caught:
        primary = caught
    finally:
        stop.set()
        if not synchronization.done():
            cancelled_sync_for_shutdown = synchronization.cancel()
        settled = asyncio.gather(runtime, synchronization, return_exceptions=True)
        while not settled.done():
            try:
                await asyncio.shield(settled)
            except asyncio.CancelledError as cancellation:
                if primary is None:
                    primary = cancellation
        settled.result()

    failures: list[BaseException] = []
    for future in (runtime, synchronization):
        if not future.cancelled():
            failure = future.exception()
            if failure is not None and not any(failure is known for known in failures):
                failures.append(failure)
    if (
        primary is not None
        and not isinstance(primary, asyncio.CancelledError)
        and not any(primary is known for known in failures)
    ):
        failures.append(primary)
    if failures:
        if len(failures) == 1:
            raise failures[0]
        raise BaseExceptionGroup("Runtime and synchronization failed", failures)
    if isinstance(primary, asyncio.CancelledError):
        raise primary
    if runtime.cancelled() or (synchronization.cancelled() and not cancelled_sync_for_shutdown):
        raise asyncio.CancelledError("An owned service was cancelled unexpectedly")


@click.group()
def main():
    """Tank V0 operator controls."""


@main.command()
def serve():
    """Run one durable service process until SIGINT/SIGTERM."""
    sys.setswitchinterval(GIL_SWITCH_INTERVAL_S)

    async def launch(repo, settings):
        from tank import clickhouse_market, openma_sync
        from tank.runtime_composition import build_supervisor

        await repo.migrate()
        if getattr(repo, "clickhouse", None) is not None:
            for statement in clickhouse_market.ddl():
                await repo.clickhouse.execute(statement)
        stop = asyncio.Event()
        reload_requested = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        loop.add_signal_handler(signal.SIGHUP, reload_requested.set)
        await run_with_synchronization(
            build_supervisor(repo, settings).run(stop, reload_requested),
            openma_sync.keep_in_sync(settings, stop),
            stop,
        )
        return {"status": "stopped"}

    run_db(launch)


@main.group()
def db():
    """Database schema management."""


@db.command()
def migrate():
    async def command(repo, settings):
        await repo.migrate()
        if getattr(repo, "clickhouse", None) is not None:
            from tank.clickhouse_market import ddl

            for statement in ddl():
                await repo.clickhouse.execute(statement)
        return {"status": "migrated"}

    run_db(command)


@db.command("clickhouse")
def clickhouse_schema():
    """Create the ClickHouse market tables (idempotent; HLD §7.1)."""

    async def command(repo, settings):
        if getattr(repo, "clickhouse", None) is None:
            raise click.ClickException("TANK_CLICKHOUSE_URL is not set")
        from tank.clickhouse_market import ddl

        for statement in ddl():
            await repo.clickhouse.execute(statement)
        return {"status": "clickhouse schema applied", "statements": len(ddl())}

    run_db(command)


@main.group()
def review():
    """Shadow review cards, delivery and outcomes."""


def page_cursor(value):
    """--after takes the `next_after` pair printed by the previous page."""
    if value is None:
        return None
    try:
        cursor = json.loads(value)
    except ValueError:
        cursor = None
    if not (isinstance(cursor, list) and len(cursor) == 2 and all(isinstance(v, str) for v in cursor)):
        raise click.BadParameter("expects next_after of the previous page: a JSON [time, key] pair")
    return cursor


@review.command("list")
@click.option("--limit", type=click.IntRange(1, 100000), default=100)
@click.option("--event-id")
@click.option("--since")
@click.option("--until")
@click.option("--after", help="next_after of the previous page.")
def review_list(limit, event_id, since, until, after):
    """List cards and updates generated in [--since, --until)."""
    after = page_cursor(after)

    async def command(repo, settings):
        from tank.review import ReviewService

        return await ReviewService(repo, settings).list(
            start=since, end=until, limit=limit, after=after, event_id=event_id
        )

    run_db(command)


@review.command("show")
@click.argument("subject")
@click.option("--since")
@click.option("--until")
@click.option("--limit", type=click.IntRange(1, 100000), default=100)
@click.option("--after", help="next_after of the previous page.")
def review_show(subject, since, until, limit, after):
    """Open a card or event: versions, evidence, delivery ledger, realized move at the outlook horizon."""
    after = page_cursor(after)

    async def command(repo, settings):
        from tank.review import ReviewService

        return await ReviewService(repo, settings).show(
            subject, start=since, end=until, limit=limit, after=after
        )

    run_db(command)


@review.command("metrics")
@click.option("--since", required=True, help="Inclusive UTC reporting start.")
@click.option("--until", required=True, help="Exclusive UTC reporting end.")
@click.option("--checks-file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def review_metrics(since, until, checks_file):
    """Compute S1–S9 and descriptive realized outcomes from stored evidence."""
    try:
        checks = json.loads(checks_file.read_text()) if checks_file else None
    except (ValueError, OSError) as exc:
        raise click.ClickException("Invalid review checks JSON: " + type(exc).__name__) from None

    async def command(repo, settings):
        from tank.review import ReviewService

        return await ReviewService(repo, settings).report(since, until, checks=checks)

    run_db(command)


@review.command("label")
@click.argument("subject")
@click.argument(
    "label", type=click.Choice(["CATCH", "LATE", "FALSE_ALARM", "NOISE", "CORRECT_SILENCE", "MISS"])
)
@click.option(
    "--miss-reason",
    type=click.Choice(
        [
            "LATE",
            "WRONG_BENEFICIARY",
            "BAD_DATA",
            "OLD_NEWS",
            "STATE_ERROR",
            "MARKET_TRUTH_ERROR",
            "DELIVERY_FAILURE",
            "SOURCE_NOT_COVERED",
        ]
    ),
)
@click.option("--notes", default="")
@click.option("--reviewer", required=True)
@click.option(
    "--version", type=click.IntRange(1), help="Alert version; defaults to the latest generated version."
)
def review_label(subject, label, miss_reason, notes, reviewer, version):
    if label == "MISS" and miss_reason is None:
        raise click.UsageError("MISS requires --miss-reason")

    async def command(repo, settings):
        event = await repo.get("evt.events", subject)
        alerts = await repo.list("out.alerts", filters={"alert_id": subject})
        if not event and not alerts:
            raise ValueError("Unknown review subject")
        label_version = None
        if alerts:
            label_version = version if version is not None else max(a["version"] for a in alerts)
            if not any(a["version"] == label_version for a in alerts):
                raise ValueError("Unknown alert version for review label")
        elif version is not None:
            raise ValueError("--version applies to an alert subject")
        key = new_id("review")
        return await repo.put(
            "qa.reviews",
            key,
            {
                "review_id": key,
                "subject": subject,
                **({"version": label_version} if label_version is not None else {}),
                "label": label,
                "miss_reason": miss_reason,
                "notes": notes,
                "reviewer": reviewer,
                **collection_clock().fields("at"),
            },
            immutable=True,
        )

    run_db(command)


@main.command()
@click.argument("event_id")
@click.option("--token-id")
@click.option(
    "--recapture",
    type=click.Choice(
        ["PRE_24H", "PRE_4H", "PRE_1H", "POST_15M", "POST_1H", "POST_4H", "POST_24H", "POST_72H"]
    ),
)
@click.option("--reason")
def reaction(event_id, token_id, recapture, reason):
    if recapture and (not token_id or not reason or not reason.strip()):
        raise click.UsageError("--recapture requires --token-id and a nonempty --reason")

    async def command(repo, settings):
        if recapture:
            from datetime import timedelta

            from tank.util import utcnow

            async with repo.lock(f"reaction:{event_id}:{token_id}:{recapture}"):
                records = [
                    row
                    async for row in repo.iter_all(
                        "out.reaction_checkpoints",
                        filters={"event_id": event_id, "token_id": token_id, "checkpoint": recapture},
                    )
                ]
                if not records:
                    raise ValueError("Recapture requires an existing immutable checkpoint")
                revision = max(r["revision"] for r in records) + 1
                return await repo.enqueue(
                    "REACTION_CAPTURE",
                    {
                        "event_id": event_id,
                        "token_id": token_id,
                        "checkpoint": recapture,
                        "revision": revision,
                        "reason": reason,
                    },
                    f"{event_id}:{token_id}:{recapture}:{revision}",
                    priority=2,
                    max_attempts=5,
                    expires_at=iso(utcnow() + timedelta(hours=6)),
                )
        return [
            row
            async for row in repo.iter_all(
                "out.reaction_checkpoints",
                filters={"event_id": event_id, **({"token_id": token_id} if token_id else {})},
            )
        ]

    run_db(command)


@main.group()
def scout():
    """Inspect proposals and record sandbox decisions."""


@scout.command("list")
def scout_list():
    async def command(repo, settings):
        return [row async for row in repo.iter_all("scout.expansion_proposals")]

    run_db(command)


@scout.command("decide")
@click.argument("prp_id")
@click.argument("action", type=click.Choice(["APPROVE_FOR_SANDBOX", "TEST_MORE", "REJECT"]))
def scout_decide(prp_id, action):
    async def command(repo, settings):
        record = await repo.get("scout.expansion_proposals", prp_id)
        if record is None:
            raise ValueError("Unknown scout proposal")
        return await repo.put(
            "scout.expansion_proposals",
            prp_id,
            {**record, "human_action": action, **collection_clock().fields("decided_at")},
        )

    run_db(command)


@main.group()
def sources():
    """Check configured source coverage."""


@sources.command("check")
@click.option("--probe", is_flag=True, help="Probe configured feed endpoints without ingestion.")
@click.option("--accounts", is_flag=True, help="Check configured X account identities.")
def sources_check(probe, accounts):
    async def command(repo, settings):
        from tank.request_budget import RequestLedger, shared_request_budgets
        from tank.sources import SourceService

        binding = shared_request_budgets.bind(RequestLedger(repo.database_url, owner="tank-cli"))
        try:
            service = SourceService(
                repo, settings, versions=settings.require_versions(), budgets=shared_request_budgets
            )
            try:
                return await service.check(probe=probe, check_accounts=accounts)
            finally:
                await service.close()
        finally:
            await shared_request_budgets.release(binding)

    run_db(command)


@main.command()
def coverage():
    async def command(repo, settings):
        from tank.http_admission import ConfiguredHttpAdmission
        from tank.market.service import MarketService
        from tank.request_budget import RequestLedger, SharedRequestBudgets

        versions = settings.require_versions()
        budgets = SharedRequestBudgets(RequestLedger(repo.database_url, owner="tank-cli-market"))
        try:
            admission = ConfiguredHttpAdmission(
                SourceApiLimitsConfiguration.model_validate(settings.load_config("api_limits")),
                versions,
                budgets,
            )
            service = MarketService(repo, settings, versions=versions, admission=admission)
            try:
                return await service.coverage()
            finally:
                await service.close()
        finally:
            await budgets.shutdown()

    run_db(command)


@main.command()
@click.argument("fixture_id")
@click.option(
    "--database-url", required=True, help="Dedicated empty replay database, distinct from production."
)
def replay(fixture_id, database_url):
    """Replay a fixture in a fresh tank-replay session (03 §11.1)."""

    async def command(repo, settings):
        from tank.replay import replay_fixture

        return await replay_fixture(repo, settings, fixture_id)

    run_db(command, database_url=database_url, replay=True)


@main.group()
def bundle():
    """Reproduce pinned evidence bundles."""


@bundle.command("reproduce")
@click.argument("sha")
@click.option(
    "--database-url", required=True, help="Dedicated empty replay database, distinct from production."
)
@click.option(
    "--mode",
    type=click.Choice(["fresh-session", "captured-tools"]),
    default="fresh-session",
    show_default=True,
)
def bundle_reproduce(sha, database_url, mode):
    async def command(repo, settings):
        from tank.replay import reproduce_bundle

        return await reproduce_bundle(repo, settings, sha, mode=mode)

    run_db(command, database_url=database_url, replay=True)


class ReadinessCheck(DomainModel):
    check: str = Field(min_length=1)
    ok: bool
    reason: str | None = None


class ReadinessReport(DomainModel):
    ready: bool
    checks: list[ReadinessCheck]


class ReadinessOpenmaIds(DomainModel):
    agent_id: str | None
    scout_agent_id: str | None
    environment_id: str | None
    replay_environment_id: str | None


def configured(value: str | Path | None) -> bool:
    if value is None:
        return False
    text = str(value)
    return bool(text) and not any(
        word in text.upper() for word in ("REPLACE_", "NOT CONFIGURED", "UNVERSIONED")
    )


def readiness_checks(settings: Settings) -> list[ReadinessCheck]:
    """Validate readiness and return only names, booleans and exception classes."""
    import re

    checks: list[ReadinessCheck] = []
    for name in (
        "sources",
        "delivery",
        "api_limits",
        "registry",
        "semantics",
        "dependency_map",
        "thresholds",
        "openma",
    ):
        try:
            if name == "sources":
                settings.load_config_as("sources", SourceConfiguration)
            elif name == "delivery":
                settings.load_config_as("delivery", DeliveryConfiguration)
            elif name == "api_limits":
                settings.load_config_as("api_limits", SourceApiLimitsConfiguration)
            elif name == "registry":
                settings.registry()
            elif name == "semantics":
                settings.semantics()
            elif name == "dependency_map":
                settings.dependencies()
            elif name == "thresholds":
                settings.thresholds()
                checks.append(ReadinessCheck(check="calibration", ok=True))
            elif name == "openma":
                openma_config = settings.load_config("openma")
                identifiers = ReadinessOpenmaIds.model_validate(
                    {
                        field: openma_config[field]
                        for field in ("agent_id", "scout_agent_id", "environment_id", "replay_environment_id")
                        if field in openma_config
                    }
                )
                for key, value in (
                    ("agent_id", identifiers.agent_id),
                    ("scout_agent_id", identifiers.scout_agent_id),
                    ("environment_id", identifiers.environment_id),
                    ("replay_environment_id", identifiers.replay_environment_id),
                ):
                    checks.append(ReadinessCheck(check="openma_" + key, ok=configured(value)))
            checks.append(ReadinessCheck(check=name + "_syntax", ok=True))
        except Exception as exc:
            checks.append(ReadinessCheck(check=name + "_syntax", ok=False, reason=type(exc).__name__))
            if name == "thresholds":
                checks.append(ReadinessCheck(check="calibration", ok=False, reason=type(exc).__name__))
    for name, setting_value in (
        ("opencode_api_key", settings.opencode_api_key),
        ("openma_api_key", settings.openma_api_key),
        ("codex_gateway_key", settings.codex_gateway_key),
        ("etherscan_api_key", settings.etherscan_api_key),
        ("telegram_token", settings.telegram_token),
        ("telegram_alerts_chat", settings.telegram_alerts_chat),
        ("telegram_ops_chat", settings.telegram_ops_chat),
        ("backup_dir", settings.backup_dir),
        ("apify_api_token", settings.apify_api_token),
    ):
        checks.append(ReadinessCheck(check=name, ok=configured(setting_value)))
    try:
        checks.append(ReadinessCheck(check="codex_auth_file_present", ok=settings.codex_auth_path.is_file()))
    except OSError as exc:
        checks.append(ReadinessCheck(check="codex_auth_file_present", ok=False, reason=type(exc).__name__))
    checks.append(
        ReadinessCheck(
            check="user_agent_contact",
            ok=re.fullmatch(
                r"TankV0/1\.0 \([A-Za-z0-9_.+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\)",
                settings.user_agent,
            )
            is not None,
        )
    )
    return checks


@main.command()
def doctor() -> None:
    """Read-only configuration and bounded service readiness diagnosis."""

    async def diagnose() -> ReadinessReport:

        settings = Settings()
        configure_logging(settings)
        import contextlib

        import yaml

        with contextlib.suppress(OSError, ValueError, yaml.YAMLError):
            settings = settings.model_copy(update=configured_values(settings))
        checks = readiness_checks(settings)
        from psycopg import AsyncConnection

        connection = None
        try:
            async with asyncio.timeout(3):
                connection = await AsyncConnection.connect(settings.database_url, connect_timeout=2)
                cursor = await connection.execute("SELECT to_regclass('ops.jobs')")
                row = await cursor.fetchone()
                checks.append(ReadinessCheck(check="database", ok=True))
                checks.append(ReadinessCheck(check="schema", ok=row is not None and row[0] is not None))
        except Exception as exc:
            checks.append(ReadinessCheck(check="database", ok=False, reason=type(exc).__name__))
        finally:
            if connection is not None:
                await connection.close()
        return ReadinessReport(ready=all(check.ok for check in checks), checks=checks)

    result = asyncio.run(diagnose())
    output(result.model_dump(mode="json", exclude_none=True))
    if not result.ready:
        raise click.exceptions.Exit(1)


@main.command()
@click.argument("input_path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--output-dir", required=True, type=click.Path(file_okay=False, path_type=Path))
def calibrate(input_path: Path, output_dir: Path) -> None:
    """Score >=200 frozen labeled news items with Jev and propose measured cutoffs."""
    import httpx

    from tank.calibration import acquire
    from tank.settings import resolve_versions

    settings = resolve_versions(Settings())
    configure_logging(settings)
    try:
        result = asyncio.run(acquire(input_path, output_dir, settings=settings))
        output(
            result.model_dump(
                mode="json",
                include={
                    "status",
                    "counts",
                    "selected",
                    "routing_target_90_90_met",
                    "rejection_reasons",
                    "best_measured_routing",
                },
            )
        )
    except (ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from None
    except (httpx.HTTPError, KeyError, TypeError, RuntimeError) as exc:
        raise click.ClickException(
            "Calibration failed ("
            + type(exc).__name__
            + "); completed usage and failures are retained in the output directory"
        ) from None


@main.command()
@click.option(
    "--force",
    is_flag=True,
    help="Run before the configured backup hour; existing daily backups are verified.",
)
@click.option(
    "--prune", is_flag=True, help="Apply retention only after the complete backup passes verification."
)
def backup(force, prune):
    """Create and verify the canonical Tank/OpenMA/evidence recovery snapshot."""

    async def command(repo, settings):
        from tank.scheduler import Scheduler
        from tank.util import utcnow

        return await Scheduler(repo, settings, None, None, versions=settings.require_versions()).maintenance(
            utcnow(), force=force, prune=prune
        )

    run_db(command)


if __name__ == "__main__":
    main()
