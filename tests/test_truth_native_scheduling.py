import asyncio
import threading

import pytest

from tank.analysis import truth_sql_engine


class MarshallingFailure(Exception):
    pass


class WorkerBarrier:
    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.loop_thread = threading.get_ident()
        self.entered = asyncio.Event()
        self.finished = asyncio.Event()
        self.release = threading.Event()

    def execute(self, *, result=None):
        assert threading.get_ident() != self.loop_thread
        self.loop.call_soon_threadsafe(self.entered.set)
        try:
            assert self.release.wait(2), "marshalling barrier was not released"
            if result is None:
                raise MarshallingFailure("declared synthetic marshalling failure")
            return result
        finally:
            self.loop.call_soon_threadsafe(self.finished.set)

    async def wait_until_entered(self, task):
        entered = asyncio.create_task(self.entered.wait())
        try:
            done, _ = await asyncio.wait({entered, task}, timeout=2, return_when=asyncio.FIRST_COMPLETED)
            if task in done:
                await task
            assert entered in done, "marshalling worker did not enter"
        finally:
            entered.cancel()
            await asyncio.gather(entered, return_exceptions=True)


class SentinelProvider:
    def __init__(self, rows):
        self.loop = asyncio.get_running_loop()
        self.thread = threading.get_ident()
        self.result = rows
        self.calls = []

    async def rows(self, statement, parameters):
        assert asyncio.get_running_loop() is self.loop
        assert threading.get_ident() == self.thread
        self.calls.append((statement, parameters))
        assert statement is STATEMENT
        assert parameters is PARAMETERS
        return self.result


STATEMENT = object()
PARAMETERS = object()
PAYLOAD = object()


def pipeline(monkeypatch, kind, stage, barrier, *, prepare_success=False):
    snapshot = {"cutoff": "2026-01-01T00:00:00Z"}
    config = {"controlled": "transport-only"}
    cache = {"owned-existing-bytes": {"payload": b"unchanged"}}
    requirements = [("controlled", "W4H")]
    if kind == "conditions":
        rows = [{"result": PAYLOAD, "condition_cache": "[]"}]
        constructor_name = "condition_query"
    else:
        rows = [
            {
                "result": PAYLOAD,
                "provenance_snapshot": "{}",
                "wrapped_flow_counts": [[window, 0] for window in ("W15M", "W1H", "W4H", "W24H", "EVENT")],
            }
        ]
        constructor_name = "_query"
    provider = SentinelProvider(rows)

    def constructor(actual_snapshot, actual_config_or_requirements, *args, **kwargs):
        assert actual_snapshot is snapshot
        if kind == "conditions":
            assert actual_config_or_requirements == requirements
            assert args == (config,)
            records = kwargs["cache_records"]
            assert records == [{"key": "owned-existing-bytes", "value": cache["owned-existing-bytes"]}]
            assert records[0]["value"] is not cache["owned-existing-bytes"]
            assert kwargs["cache_transport"] is True
        else:
            assert actual_config_or_requirements is config
            assert args == (truth_sql_engine._provenance_select,)
        if stage == "prepare":
            return barrier.execute(result=(STATEMENT, PARAMETERS) if prepare_success else None)
        return STATEMENT, PARAMETERS

    monkeypatch.setattr(truth_sql_engine, constructor_name, constructor)
    loads = truth_sql_engine.json.loads

    def decode(value, *args, **kwargs):
        if value is PAYLOAD:
            return barrier.execute()
        return loads(value, *args, **kwargs)

    monkeypatch.setattr(truth_sql_engine.json, "loads", decode)
    if kind == "conditions":
        coroutine = truth_sql_engine.evaluate_conditions(
            provider, snapshot, requirements, config, cache=cache
        )
    else:
        coroutine = truth_sql_engine.evaluate(provider, snapshot, config)
    return coroutine, provider, cache


@pytest.mark.parametrize("kind", ["assessment", "conditions"])
@pytest.mark.parametrize("stage", ["prepare", "decode"])
async def test_actual_native_pipeline_marshalling_yields_and_propagates_failure(monkeypatch, kind, stage):
    barrier = WorkerBarrier()
    coroutine, provider, cache = pipeline(monkeypatch, kind, stage, barrier)
    task = asyncio.create_task(coroutine)
    try:
        await barrier.wait_until_entered(task)
        heartbeat = asyncio.Event()
        asyncio.get_running_loop().call_soon(heartbeat.set)
        await asyncio.wait_for(heartbeat.wait(), 2)
        assert not task.done()
        assert len(provider.calls) == (0 if stage == "prepare" else 1)
        assert cache == {"owned-existing-bytes": {"payload": b"unchanged"}}
        barrier.release.set()
        with pytest.raises(MarshallingFailure, match="declared synthetic"):
            await task
        assert cache == {"owned-existing-bytes": {"payload": b"unchanged"}}
    finally:
        barrier.release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("kind", ["assessment", "conditions"])
@pytest.mark.parametrize("stage", ["prepare", "decode"])
async def test_actual_native_pipeline_cancellation_discards_worker_without_io_or_publication(
    monkeypatch, kind, stage
):
    barrier = WorkerBarrier()
    coroutine, provider, cache = pipeline(monkeypatch, kind, stage, barrier, prepare_success=True)
    task = asyncio.create_task(coroutine)
    try:
        await barrier.wait_until_entered(task)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(provider.calls) == (0 if stage == "prepare" else 1)
        assert cache == {"owned-existing-bytes": {"payload": b"unchanged"}}
    finally:
        barrier.release.set()
        await asyncio.gather(task, return_exceptions=True)
        if barrier.entered.is_set():
            await asyncio.wait_for(barrier.finished.wait(), 2)
    assert len(provider.calls) == (0 if stage == "prepare" else 1)
    assert cache == {"owned-existing-bytes": {"payload": b"unchanged"}}


@pytest.mark.parametrize("rows", [[], [{"result": "{}"}], [{"result": "{}", "condition_cache": "[]"}]])
async def test_condition_decode_failure_preserves_owned_cache(monkeypatch, rows):
    monkeypatch.setattr(truth_sql_engine, "condition_query", lambda *args, **kwargs: (STATEMENT, PARAMETERS))
    provider = SentinelProvider(rows)
    cache = {"owned-existing-bytes": {"payload": b"unchanged"}}
    with pytest.raises(ValueError):
        await truth_sql_engine.evaluate_conditions(provider, {}, [], cache=cache)
    assert len(provider.calls) == 1
    assert cache == {"owned-existing-bytes": {"payload": b"unchanged"}}


async def test_condition_controlled_decoder_publication_is_atomic_on_original_loop(monkeypatch):
    loop = asyncio.get_running_loop()
    loop_thread = threading.get_ident()
    observations = []
    result = object()
    replacement = {"controlled-decoder-output": object()}

    class PublicationCache(dict):
        def clear(self):
            assert threading.get_ident() == loop_thread
            assert asyncio.get_running_loop() is loop
            observations.append(("clear", tuple(self)))
            super().clear()
            loop.call_soon(lambda: observations.append(("observer", tuple(self))))

        def update(self, *args, **kwargs):
            assert threading.get_ident() == loop_thread
            assert asyncio.get_running_loop() is loop
            assert args == (replacement,)
            assert not kwargs
            observations.append(("update", tuple(self)))
            super().update(*args, **kwargs)

    def controlled_decode(rows, snapshot, requirements):
        assert threading.get_ident() != loop_thread
        assert rows is transport
        return result, replacement

    transport = object()
    provider = SentinelProvider(transport)
    cache = PublicationCache({"old-controlled-entry": object()})
    monkeypatch.setattr(truth_sql_engine, "condition_query", lambda *args, **kwargs: (STATEMENT, PARAMETERS))
    monkeypatch.setattr(truth_sql_engine, "_decode_conditions", controlled_decode)
    actual = await truth_sql_engine.evaluate_conditions(provider, {}, [], cache=cache)
    assert actual is result
    assert cache == replacement
    assert len(provider.calls) == 1
    observed = asyncio.Event()
    loop.call_soon(observed.set)
    await asyncio.wait_for(observed.wait(), 2)
    assert observations == [
        ("clear", ("old-controlled-entry",)),
        ("update", ()),
        ("observer", ("controlled-decoder-output",)),
    ]


async def test_cancelled_condition_controlled_decoder_cannot_publish_after_worker_completes(monkeypatch):
    barrier = WorkerBarrier()
    cache = {"owned-existing-bytes": {"payload": b"unchanged"}}
    replacement = {"controlled-decoder-output": object()}
    provider = SentinelProvider(PAYLOAD)

    def controlled_decode(rows, snapshot, requirements):
        assert rows is PAYLOAD
        return barrier.execute(result=(object(), replacement))

    monkeypatch.setattr(truth_sql_engine, "condition_query", lambda *args, **kwargs: (STATEMENT, PARAMETERS))
    monkeypatch.setattr(truth_sql_engine, "_decode_conditions", controlled_decode)
    task = asyncio.create_task(truth_sql_engine.evaluate_conditions(provider, {}, [], cache=cache))
    try:
        await barrier.wait_until_entered(task)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cache == {"owned-existing-bytes": {"payload": b"unchanged"}}
    finally:
        barrier.release.set()
        await asyncio.gather(task, return_exceptions=True)
        if barrier.entered.is_set():
            await asyncio.wait_for(barrier.finished.wait(), 2)
    assert len(provider.calls) == 1
    assert cache == {"owned-existing-bytes": {"payload": b"unchanged"}}


async def test_condition_cache_deepcopy_is_prepared_off_loop_before_native_io(monkeypatch):
    barrier = WorkerBarrier()

    class OwnedValue:
        def __deepcopy__(self, memo):
            return barrier.execute()

    value = OwnedValue()
    cache = {"owned-existing-value": value}
    provider = SentinelProvider(PAYLOAD)
    task = asyncio.create_task(truth_sql_engine.evaluate_conditions(provider, {}, [], cache=cache))
    try:
        await barrier.wait_until_entered(task)
        assert not provider.calls
        barrier.release.set()
        with pytest.raises(MarshallingFailure, match="declared synthetic"):
            await task
    finally:
        barrier.release.set()
        await asyncio.gather(task, return_exceptions=True)
    assert cache["owned-existing-value"] is value
    assert not provider.calls


@pytest.mark.parametrize("kind", ["assessment", "conditions"])
async def test_missing_provider_fails_before_native_marshalling(monkeypatch, kind):
    def forbidden(*args, **kwargs):
        pytest.fail("missing provider reached native marshalling")

    monkeypatch.setattr(truth_sql_engine, "_query", forbidden)
    monkeypatch.setattr(truth_sql_engine, "condition_query", forbidden)
    if kind == "conditions":
        coroutine = truth_sql_engine.evaluate_conditions(None, {}, [])
    else:
        coroutine = truth_sql_engine.evaluate(None, {})
    with pytest.raises(RuntimeError, match="repository ClickHouse native provider"):
        await coroutine
