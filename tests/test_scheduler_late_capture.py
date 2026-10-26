"""External historical rows + actual Go late capture → immutable reaction revisions."""

import asyncio
import gzip
import json
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from datetime import timedelta
from pathlib import Path
from typing import Never

import httpx
import pytest
from foundation_helpers import present, text_field
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from runtime_public_fixtures import public_runtime
from source_support import fixture_http_admission, fixture_versions

from tank.contracts import DomainModel
from tank.contracts.jobs import JobEnvelope
from tank.contracts.scheduler_commands import (
    ReactionCaptureCommand,
    RevisedReactionCommand,
    reaction_command_from_wire,
)
from tank.contracts.scheduler_results import (
    CapturedReaction,
    ReactionCaptureResult,
    ReactionCaptureResultDocument,
    ReactionChangeTarget,
    UnchangedReaction,
    applied_reaction_change_receipt,
)
from tank.job_repository import JobRepository
from tank.market.service import MarketService
from tank.runtime_dispatch_contracts import ClaimedWork, RunningJobClaim
from tank.scheduler import REACTION_RETRY_S, Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import Timestamp, chosen_boundary, collection_clock
from tank.util import canonical_json, iso, parse_time, utcnow


class ReactionWorkerRun(DomainModel):
    receipts: list[ReactionCaptureResult]
    auxiliary_job_ids: tuple[str, ...]


type ReactionRunner = Callable[
    [MemoryRepository, Scheduler, tuple[str, ...], Path, pytest.MonkeyPatch], Awaitable[ReactionWorkerRun]
]


async def run_claimed_reactions(
    repo: MemoryRepository,
    scheduler: Scheduler,
    job_ids: tuple[str, ...],
    directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> ReactionWorkerRun:
    async with public_runtime(repo, directory, monkeypatch) as (runtime, _):
        owned_scheduler = runtime.scheduler
        runtime.scheduler = scheduler
        stop = asyncio.Event()
        worker = asyncio.create_task(runtime.worker(stop))
        jobs = JobRepository(repo)
        receipts: list[ReactionCaptureResult] = []
        try:
            async with asyncio.timeout(5):
                while True:
                    observed = [await jobs.get(job_id) for job_id in job_ids]
                    assert all(job is not None for job in observed)
                    if all(job is not None and job.status == "DONE" for job in observed):
                        for job in observed:
                            assert (
                                job is not None
                                and job.finished_at is not None
                                and job.claim_token is not None
                            )
                            assert isinstance(job.result, dict)
                            receipts.append(ReactionCaptureResultDocument.model_validate(job.result).root)
                        return ReactionWorkerRun(receipts=receipts, auxiliary_job_ids=())
                    assert all(
                        job is not None and job.status not in {"FAILED", "EXPIRED"} for job in observed
                    )
                    await asyncio.sleep(0.005)
        finally:
            stop.set()
            try:
                await worker
            finally:
                runtime.scheduler = owned_scheduler


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.asyncio
async def test_backfill_changes_append_reaction_revision_once_and_preserve_original(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    parity_binary,
    request: pytest.FixtureRequest,
    *,
    capture_runner: ReactionRunner = run_claimed_reactions,
) -> None:
    import tank.scheduler as scheduler_module

    monkeypatch.setattr(scheduler_module, "LATE_REVISION_DEFER_S", 0)
    anchor = utcnow().replace(second=0, microsecond=0) - timedelta(hours=2)
    inst: dict[str, JsonValue] = {
        "instrument_id": "binance:spot:TUSD",
        "token_id": "test:T",
        "venue": "binance",
        "market_type": "SPOT",
        "venue_symbol": "TUSD",
        "quote_asset": "USD",
        "active": True,
        "contract_multiplier": "1",
        "tick_size": ".01",
        "lot_size": ".01",
    }

    class FixtureSettings(Settings):
        model_config = SettingsConfigDict(env_file=None)

        def load_config(self, name):
            if name == "registry":
                return {"tokens": [{"token_id": "test:T", "symbol": "T"}], "instruments": [inst]}
            if name == "semantics":
                return {"metrics": []}
            return super().load_config(name)

    settings = FixtureSettings(
        data_dir=tmp_path, replay=True, config_version="fixture", code_version="fixture"
    )

    async def go_capture(name, payload):
        path = tmp_path / (name + ".json.gz")
        with gzip.open(path, "wt") as stream:
            stream.write("\n".join(json.dumps(row) for row in payload) + "\n")
        args = [str(parity_binary), str(path)]
        process = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await process.communicate()
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
        assert process.returncode == 0, stderr.decode()
        output = json.loads(stdout)
        assert not output["errors"], output["errors"]
        return output

    def external(req: httpx.Request) -> Never:
        raise AssertionError("Tank core must not fetch exchange data: " + str(req.url))

    async def persist_capture(output, *, observed, minute) -> None:
        for row in output["rows"]:
            await repo.put_market_batch(row["collection"], [row["row"]], keys=[row["key"]])

        bars = [
            {**bar, **collection_clock(observed).fields("recorded_at")}
            for bar in output["bars"]
            if bar["minute_start"] == minute
        ]
        assert len(bars) == 1 and bars[0]["complete"] is True
        if bars:
            await repo.put_market_batch(
                "mkt.flow_bars_1m",
                bars,
                keys=[bar["instrument_id"] + ":" + bar["minute_start"] for bar in bars],
            )

    repo = MemoryRepository()
    await repo.put(
        "evt.events",
        "e",
        {
            "event_id": "e",
            "known_to_us_at": iso(anchor),
            "public_at": iso(anchor),
            "public_at_prec": "EXACT",
            "public_at_prec_reason": None,
        },
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(external)) as http:
        market = MarketService(
            repo, settings, http=http, versions=fixture_versions(), admission=fixture_http_admission()
        )
        scheduler = Scheduler(
            repo,
            settings,
            market,
            None,
            thresholds=FixtureSettings().thresholds(),
            versions=fixture_versions(),
        )
        payload: dict[str, JsonValue] = {
            "event_id": "e",
            "token_id": "test:T",
            "checkpoint": "PRE_1H",
            "anchor": {"type": "T1", "ts": iso(anchor), "ts_prec": "EXACT", "ts_prec_reason": None},
            "retro": True,
        }
        receipt = await scheduler.reaction_capture(reaction_command_from_wire(payload))
        assert isinstance(receipt, CapturedReaction)
        original = present(await repo.get("out.reaction_checkpoints", receipt.checkpoint_key))
        assert original is not None
        assert original["r_raw"] is None and original["revision"] == 1

        obligation = await market._schedule_event_backfill(
            "e", "test:T", chosen_boundary(anchor, precision="EXACT")
        )
        pending = await market.capture_readiness(
            "e", "test:T", anchor=chosen_boundary(anchor, precision="EXACT")
        )
        assert pending.status == "PENDING"
        key = pending.backfill_id
        assert key is not None
        await repo.put_market_batch(
            "mkt.backfill_runs", [{**obligation.model_dump(mode="json"), "status": "RUNNING"}], keys=[key]
        )
        observed = utcnow()

        historical: list[dict[str, JsonValue]] = []
        for minute in range(-1440, 1):
            opened = anchor + timedelta(minutes=minute)
            close = "100" if opened < anchor - timedelta(minutes=30) else "110"
            historical.append(
                {
                    "instrument_id": inst["instrument_id"],
                    "token_id": "test:T",
                    "venue": "binance",
                    "market_type": "SPOT",
                    "open_time": iso(opened),
                    "open_time_prec": "BUCKET(60s)",
                    "open_time_prec_reason": None,
                    "interval_s": 60,
                    "o": close,
                    "h": close,
                    "l": close,
                    "c": close,
                    "volume_native": "1",
                    "quote_volume": close,
                    "quote_volume_usd": close,
                    "quote_asset": "USD",
                    "quote_usd_rate": "1",
                    "complete": True,
                    **collection_clock(observed).fields("received_ts"),
                    **collection_clock(observed).fields("recorded_at"),
                }
            )
        await repo.put_market_batch(
            "mkt.klines_1m",
            historical,
            keys=[
                text_field(row, "instrument_id") + ":" + text_field(row, "open_time") for row in historical
            ],
        )

        history_bars: list[dict[str, JsonValue]] = []
        for row in historical:
            end = parse_time(text_field(row, "open_time")) + timedelta(minutes=1)
            last_trade = {
                "trade_id": "historical-" + text_field(row, "open_time"),
                "price": row["c"],
                "ts": iso(end - timedelta(microseconds=1)),
            }
            history_bars.append(
                {
                    "instrument_id": inst["instrument_id"],
                    "token_id": "test:T",
                    "venue": "binance",
                    "market_type": "SPOT",
                    "minute_start": row["open_time"],
                    "minute_start_prec": "BUCKET(60s)",
                    "minute_start_prec_reason": None,
                    "complete": True,
                    "n_buy": 1,
                    "n_sell": 0,
                    "buy_qty": "1",
                    "sell_qty": "0",
                    "buy_notional": row["c"],
                    "sell_notional": "0",
                    "quote_volume": row["c"],
                    "volume_usd": row["c"],
                    "o": row["c"],
                    "h": row["c"],
                    "l": row["c"],
                    "c": row["c"],
                    "sum_sq_qty": "1",
                    "trade_aggregation": "PER_FILL",
                    "last_trade_id": last_trade["trade_id"],
                    "last_trade": last_trade,
                    "reference_points": [{"boundary": iso(end), "complete": True, "last_trade": last_trade}],
                    **collection_clock(observed).fields("received_ts"),
                    **collection_clock(observed).fields("recorded_at"),
                }
            )
        await repo.put_market_batch(
            "mkt.flow_bars_1m",
            history_bars,
            keys=[
                text_field(row, "instrument_id") + ":" + text_field(row, "minute_start")
                for row in history_bars
            ],
        )

        await repo.put_market_batch(
            "mkt.backfill_runs",
            [
                {
                    **present(await repo.get("mkt.backfill_runs", key)),
                    **collection_clock(utcnow()).fields("finished_at"),
                    "status": "PARTIAL",
                    "reason": "ONLY_REACTION_WINDOW_CAPTURED",
                    "result": {"instruments": [], "benchmark_klines": []},
                }
            ],
            keys=[key],
        )
        state = await market.capture_readiness(
            "e", "test:T", anchor=chosen_boundary(anchor, precision="EXACT")
        )
        assert state.status in {"COMPLETE", "PARTIAL"}
        await scheduler.detect_late_data(utcnow())
        jobs = [
            JobEnvelope.model_validate(j)
            async for j in repo.iter_all("ops.jobs", filters={"type": "REACTION_CAPTURE"})
        ]
        assert jobs and all(
            isinstance(reaction_command_from_wire(job.payload), RevisedReactionCommand) for job in jobs
        )
        await run_claimed_reactions(
            repo,
            scheduler,
            tuple(job.job_id for job in jobs),
            tmp_path / "historical-runtime",
            monkeypatch,
        )
        revised = present(await repo.get("out.reaction_checkpoints", "e:test:T:PRE_1H:2"))
        assert revised["revision"] == 2 and revised["r_raw"] is not None
        assert revised["r_raw"] == pytest.approx(0.1)
        assert revised["rwp_id"] == original["rwp_id"] and text_field(revised, "rwp_id").startswith("RWP-")
        assert text_field(revised, "reason").startswith("Late capture completed:")
        assert await repo.get("out.reaction_checkpoints", "e:test:T:PRE_1H:1") == original
        await scheduler.detect_late_data(utcnow())
        await scheduler.reaction_capture(reaction_command_from_wire(jobs[0].payload))
        assert len([row async for row in repo.iter_all("out.reaction_checkpoints")]) == 2

        first_change = reaction_command_from_wire(jobs[0].payload)
        assert isinstance(first_change, RevisedReactionCommand)
        unchanged = dict(
            jobs[0].payload,
            change_id=first_change.change_id + ":same",
            reason="Late duplicate capture",
        )
        result = await scheduler.reaction_capture(reaction_command_from_wire(unchanged))
        assert isinstance(result, UnchangedReaction) and result.changed is False
        assert len([row async for row in repo.iter_all("out.reaction_checkpoints")]) == 2

        late_observed = utcnow()
        frames = [
            {"recorded_at": iso(late_observed), "instruments": [inst]},
            {
                "t": iso(late_observed),
                "rest": "depth_snapshot",
                "instrument_id": inst["instrument_id"],
                "text": json.dumps({"lastUpdateId": 0, "bids": [["109", "10"]], "asks": [["121", "10"]]}),
            },
        ]
        for trade_id, offset in enumerate((-180000, -120000, -1), start=100):
            ms = int(anchor.timestamp() * 1000) + offset
            trade = {
                "e": "aggTrade",
                "s": "TUSD",
                "E": ms,
                "a": trade_id,
                "p": "120" if offset == -1 else "110",
                "q": "1",
                "T": ms,
                "m": False,
            }
            frames.append(
                {
                    "t": iso(late_observed),
                    "endpoint": "wss://data-stream.binance.vision/stream",
                    "text": json.dumps({"stream": "tusd@aggTrade", "data": trade}),
                }
            )
        late = await go_capture("late", frames)
        assert len(late["trades"]) == 3 and late["bars"]
        late_minute = iso(anchor - timedelta(minutes=1))
        late_bar = next(bar for bar in late["bars"] if bar["minute_start"] == late_minute)
        assert late_bar["complete"] is True and late_bar["last_trade"]["price"] == "120"
        await persist_capture(late, observed=late_observed, minute=late_minute)
        await scheduler.detect_late_data(utcnow())
        previous_ids = {job.job_id for job in jobs}
        observed_jobs = [
            JobEnvelope.model_validate(job)
            async for job in repo.iter_all("ops.jobs", filters={"type": "REACTION_CAPTURE"})
        ]
        live_jobs = [job for job in observed_jobs if job.job_id not in previous_ids]
        assert len(live_jobs) == 1
        live_change = reaction_command_from_wire(live_jobs[0].payload)
        assert isinstance(live_change, RevisedReactionCommand) and "CAP-LIVE-" in live_change.change_id

        live_run = await capture_runner(
            repo,
            scheduler,
            (live_jobs[0].job_id,),
            tmp_path / "runtime",
            monkeypatch,
        )
        [live_receipt] = live_run.receipts
        assert isinstance(live_receipt, CapturedReaction)
        assert live_receipt.revision == 3 and live_receipt.data_fingerprint != revised["data_fingerprint"]
        live_record = await repo.get("out.reaction_checkpoints", live_receipt.checkpoint_key)
        assert live_record is not None and live_record["data_fingerprint"] == live_receipt.data_fingerprint
        assert live_record["r_raw"] == pytest.approx(0.2)
        assert live_record["rwp_id"] == original["rwp_id"]
        assert await repo.get("out.reaction_checkpoints", "e:test:T:PRE_1H:1") == original
        assert await repo.get("out.reaction_checkpoints", "e:test:T:PRE_1H:2") == revised

        duplicate = await go_capture("duplicate", frames)
        assert duplicate == late
        await persist_capture(duplicate, observed=late_observed, minute=late_minute)
        await scheduler.detect_late_data(utcnow())
        assert len(
            [job async for job in repo.iter_all("ops.jobs", filters={"type": "REACTION_CAPTURE"})]
        ) == len(jobs) + 1 + len(live_run.auxiliary_job_ids)
        request.node.user_properties.extend(
            [
                ("revision2_r_raw", revised["r_raw"]),
                ("revision3_r_raw", live_record["r_raw"]),
                ("late_go_bar_complete", late_bar["complete"]),
                ("late_go_last_price", late_bar["last_trade"]["price"]),
                ("normalized_trades", len(late["trades"])),
            ]
        )
        await market.close()
        await scheduler.close()


async def run_after_failed_peer(
    repo: MemoryRepository,
    scheduler: Scheduler,
    job_ids: tuple[str, ...],
    directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> ReactionWorkerRun:
    assert len(job_ids) == 1
    jobs = JobRepository(repo)
    original = await jobs.get(job_ids[0])
    assert original is not None and original.status == "QUEUED" and original.expires_at is not None
    command = reaction_command_from_wire(original.payload)
    assert isinstance(command, RevisedReactionCommand)
    source_payload = canonical_json(original.payload)
    original_expiry = Timestamp(
        original.expires_at,
        original.expires_at_prec,
        original.expires_at_prec_reason,
        original.expires_at_bucket_anchor,
    )
    first_claimed, release_first = asyncio.Event(), asyncio.Event()
    seen: list[RunningJobClaim] = []
    peer_id: str | None = None
    executing_job: ContextVar[str | None] = ContextVar("reaction_fixture_claim", default=None)
    peer_core_entered = asyncio.Event()
    capture = scheduler.reaction_capture

    async def observed_capture(
        request: ReactionCaptureCommand, *, wait_for_slot: bool = False
    ) -> ReactionCaptureResult:
        if peer_id is not None and executing_job.get() == peer_id:
            peer_core_entered.set()
            raise RuntimeError("Synthetic capture input unavailable on the peer's final attempt")
        return await capture(request, wait_for_slot=wait_for_slot)

    monkeypatch.setattr(scheduler, "reaction_capture", observed_capture)
    marker_key = "reaction-change:" + command.change_id
    assert await repo.get("ops.scheduler_checkpoints", marker_key) is None
    async with asyncio.timeout(REACTION_RETRY_S + 15):
        async with public_runtime(repo, directory / "first", monkeypatch) as (runtime, _):
            owned = runtime.scheduler
            runtime.scheduler = scheduler
            execute = runtime.execute_work_wire

            async def first_phase(work: ClaimedWork) -> JsonValue:
                if work.job.job_id == original.job_id:
                    seen.append(work.scope())
                    first_claimed.set()
                    await release_first.wait()
                elif work.job.job_id != peer_id:
                    raise AssertionError("Reaction fixture refuses unrelated jobs")
                context = executing_job.set(work.job.job_id)
                try:
                    return await execute(work)
                finally:
                    executing_job.reset(context)

            monkeypatch.setattr(runtime, "execute_work_wire", first_phase)
            stop = asyncio.Event()
            worker = asyncio.create_task(runtime.worker(stop))
            observer_error: BaseException | None = None
            try:
                await first_claimed.wait()
                peer = await jobs.enqueue(
                    "REACTION_CAPTURE",
                    original.payload,
                    "reaction-fixture-peer:" + original.job_id,
                    priority=1,
                    max_attempts=1,
                    expires_at=original_expiry,
                )
                peer_id = peer.job_id
                release_first.set()
                while True:
                    peer_state = await jobs.get(peer_id)
                    if peer_state is not None and peer_state.status == "FAILED":
                        break
                    await asyncio.sleep(0.005)
                assert peer_core_entered.is_set()
                deferred = await jobs.get(original.job_id)
                assert deferred is not None and deferred.status == "QUEUED"
                assert deferred.attempts == 0 and "result" not in deferred.model_fields_set
                assert canonical_json(deferred.payload) == source_payload
                assert deferred.expires_at == original.expires_at
                assert parse_time(deferred.run_after) > utcnow()
                assert await repo.get("ops.scheduler_checkpoints", marker_key) is None
                same = await jobs.enqueue(
                    "REACTION_CAPTURE",
                    original.payload,
                    original.idem_key,
                    priority=original.priority,
                    max_attempts=original.max_attempts,
                    expires_at=original_expiry,
                )
                assert same.job_id == original.job_id and same.status == "QUEUED"
            except BaseException as failure:
                observer_error = failure
                raise
            finally:
                stop.set()
                release_first.set()
                try:
                    if observer_error is not None:
                        worker.cancel()
                        await asyncio.gather(worker, return_exceptions=True)
                    else:
                        await worker
                finally:
                    runtime.scheduler = owned
        async with public_runtime(repo, directory / "restart", monkeypatch) as (runtime, _):
            owned = runtime.scheduler
            runtime.scheduler = scheduler
            execute = runtime.execute_work_wire

            async def restarted_phase(work: ClaimedWork) -> JsonValue:
                if work.job.job_id != original.job_id:
                    raise AssertionError("Restart fixture refuses unrelated jobs")
                assert utcnow() >= parse_time(work.job.run_after)
                seen.append(work.scope())
                context = executing_job.set(work.job.job_id)
                try:
                    return await execute(work)
                finally:
                    executing_job.reset(context)

            monkeypatch.setattr(runtime, "execute_work_wire", restarted_phase)
            stop = asyncio.Event()
            worker = asyncio.create_task(runtime.worker(stop))
            observer_error = None
            try:
                while True:
                    completed = await jobs.get(original.job_id)
                    assert completed is not None and completed.status not in {"FAILED", "EXPIRED"}
                    if completed.status == "DONE":
                        break
                    await asyncio.sleep(0.005)
            except BaseException as failure:
                observer_error = failure
                raise
            finally:
                stop.set()
                try:
                    if observer_error is not None:
                        worker.cancel()
                        await asyncio.gather(worker, return_exceptions=True)
                    else:
                        await worker
                finally:
                    runtime.scheduler = owned
    assert len(seen) == 2 and seen[0].claim_token != seen[1].claim_token
    assert seen[0].job_id == seen[1].job_id == original.job_id
    assert seen[0].attempts == seen[1].attempts == 1
    assert seen[0].recovery_count is None and seen[1].recovery_count is None
    assert seen[0].payload_revision == seen[1].payload_revision
    assert completed.finished_at is not None and isinstance(completed.result, dict)
    receipt = ReactionCaptureResultDocument.model_validate(completed.result).root
    assert isinstance(receipt, CapturedReaction) and receipt.revision == 3
    assert canonical_json(completed.payload) == source_payload and completed.expires_at == original.expires_at
    marker = await repo.get("ops.scheduler_checkpoints", marker_key)
    owner = await repo.get("ops.scheduler_checkpoints", "reaction-change-owner:" + command.change_id)
    assert marker is not None and owner is not None
    target = ReactionChangeTarget(
        change_id=command.change_id,
        event_id=command.event_id,
        token_id=command.token_id,
        checkpoint=command.checkpoint,
    )
    applied = applied_reaction_change_receipt(target, marker, owner)
    assert applied.revision == 3 and applied.changed is True
    assert applied.data_fingerprint == receipt.data_fingerprint
    repeated = await scheduler.reaction_capture(command)
    assert repeated == applied
    assert await repo.get("ops.scheduler_checkpoints", marker_key) == marker
    assert await repo.get("ops.scheduler_checkpoints", "reaction-change-owner:" + command.change_id) == owner
    assert peer_id is not None
    peer_state = await jobs.get(peer_id)
    assert peer_state is not None and peer_state.status == "FAILED" and peer_state.attempts == 1
    return ReactionWorkerRun(receipts=[receipt], auxiliary_job_ids=(peer_id,))


@pytest.mark.integration
@pytest.mark.process
@pytest.mark.asyncio
async def test_failed_edf_peer_preserves_real_change_through_policy_retry_and_runtime_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, parity_binary, request: pytest.FixtureRequest
) -> None:
    await test_backfill_changes_append_reaction_revision_once_and_preserve_original(
        tmp_path, monkeypatch, parity_binary, request, capture_runner=run_after_failed_peer
    )
