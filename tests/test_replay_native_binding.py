import asyncio
from copy import deepcopy
from types import SimpleNamespace

import pytest

from tank.analysis import truth_native_binding, truth_native_local, truth_sql_engine
from tank.analysis.truth import MarketTruthService
from tank.analysis.truth_native_binding import bind_replay_native, native_provider
from tank.clickhouse_market import install
from tank.replay import ReplayInputs, run_captured_tools
from tank.settings import Settings
from tank.storage import MemoryRepository


class SentinelRows:
    def __init__(self):
        self.closed = 0
        self.close_started = asyncio.Event()
        self.close_release = asyncio.Event()
        self.close_release.set()
        self.close_error = None

    async def rows(self, query, params=None):
        raise AssertionError("Lifetime sentinel must never compute market truth")

    async def close(self):
        self.closed += 1
        self.close_started.set()
        await self.close_release.wait()
        if self.close_error is not None:
            raise self.close_error


@pytest.fixture
def service():
    return MarketTruthService(MemoryRepository(), SimpleNamespace(replay=True), object())


@pytest.fixture
def sentinel(monkeypatch):
    instances = []

    def create():
        provider = SentinelRows()
        instances.append(provider)
        return provider

    monkeypatch.setattr(truth_native_local, "ReplayNativeRows", create)
    return instances


@pytest.mark.parametrize("previous_none", [False, True])
async def test_binding_owns_provider_without_changing_repository(service, sentinel, previous_none):
    repo = service.repo
    if previous_none:
        service._replay_native_state = None
    async with bind_replay_native(service):
        provider = native_provider(service)
        assert provider is sentinel[0]
        assert service.repo is repo
        assert not hasattr(repo, "clickhouse")
        assert provider.closed == 0
    assert provider.closed == 1
    assert service.repo is repo
    assert ("_replay_native_state" in vars(service)) is previous_none
    assert native_provider(service) is None


async def test_binding_restores_after_body_exception(service, sentinel):
    error = ValueError("body failed")
    with pytest.raises(ValueError) as raised:
        async with bind_replay_native(service):
            raise error
    assert raised.value is error
    assert sentinel[0].closed == 1
    assert "_replay_native_state" not in vars(service)


async def test_double_binding_refuses_without_second_provider(service, sentinel):
    async with bind_replay_native(service):
        provider = native_provider(service)
        with pytest.raises(RuntimeError, match="ownership already exists"):
            async with bind_replay_native(service):
                pytest.fail("double binding admitted")
        assert native_provider(service) is provider
        assert len(sentinel) == 1
        assert provider.closed == 0
    assert provider.closed == 1


@pytest.mark.parametrize("real_service", [False, True])
async def test_binding_refuses_before_provider_construction(service, sentinel, real_service):
    selected = service if real_service else SimpleNamespace(settings=SimpleNamespace(replay=True))
    if real_service:
        service.settings.replay = False
    with pytest.raises(ValueError, match="real replay truth service"):
        async with bind_replay_native(selected):
            pytest.fail("invalid service admitted")
    assert sentinel == []


async def test_foreign_slot_is_refused_and_not_overwritten(service, sentinel):
    foreign = object()
    service._replay_native_state = foreign
    with pytest.raises(RuntimeError, match="ownership already exists"):
        async with bind_replay_native(service):
            pytest.fail("foreign ownership admitted")
    assert service._replay_native_state is foreign
    assert sentinel == []


@pytest.mark.parametrize("mutation", ["repo", "settings", "replay", "lifecycle", "token"])
async def test_resolver_refuses_mutated_ownership(service, sentinel, mutation):
    async with bind_replay_native(service):
        state = service._replay_native_state
        if mutation == "repo":
            service.repo = MemoryRepository()
        elif mutation == "settings":
            service.settings = SimpleNamespace(replay=True)
        elif mutation == "replay":
            service.settings.replay = False
        elif mutation == "lifecycle":
            state.lifecycle = "closed"
        else:
            state.token = None
        with pytest.raises(RuntimeError, match="Invalid replay native ownership"):
            native_provider(service)
    assert sentinel[0].closed == 1
    assert "_replay_native_state" not in vars(service)


async def test_resolver_refuses_owner_copied_to_another_service(service, sentinel):
    other = MarketTruthService(service.repo, service.settings, service.market)
    async with bind_replay_native(service):
        other._replay_native_state = service._replay_native_state
        with pytest.raises(RuntimeError, match="Invalid replay native ownership"):
            native_provider(other)
        assert native_provider(service) is sentinel[0]


async def test_exit_does_not_clobber_replaced_owner(service, sentinel):
    foreign = object()
    async with bind_replay_native(service):
        service._replay_native_state = foreign
    assert service._replay_native_state is foreign
    assert sentinel[0].closed == 1
    with pytest.raises(RuntimeError, match="Invalid replay native ownership"):
        native_provider(service)


def test_unbound_resolver_preserves_repository_provider(service, sentinel):
    provider = object()
    service.repo.clickhouse = provider
    assert native_provider(service) is provider
    service.settings.replay = False
    assert native_provider(service) is provider
    assert sentinel == []


@pytest.mark.parametrize("conditions", [False, True])
async def test_unbound_missing_provider_keeps_native_engine_refusal(service, sentinel, conditions):
    with pytest.raises(RuntimeError, match="repository ClickHouse native provider"):
        if conditions:
            await truth_sql_engine.evaluate_conditions(native_provider(service), {}, [])
        else:
            await truth_sql_engine.evaluate(native_provider(service), {})
    assert sentinel == []


async def test_native_close_error_is_not_hidden(service, sentinel):
    error = RuntimeError("controlled close failure")
    with pytest.raises(RuntimeError) as raised:
        async with bind_replay_native(service):
            sentinel[0].close_error = error
    assert raised.value is error
    assert "_replay_native_state" not in vars(service)


async def test_original_error_survives_close_error_with_cause(service, sentinel):
    original = ValueError("body failed")
    cleanup = RuntimeError("controlled close failure")
    with pytest.raises(ValueError) as raised:
        async with bind_replay_native(service):
            sentinel[0].close_error = cleanup
            raise original
    assert raised.value is original
    assert raised.value.__cause__ is cleanup
    assert sentinel[0].closed == 1
    assert "_replay_native_state" not in vars(service)


async def test_repeated_cancellation_waits_for_owned_close_before_restoration(service, sentinel):
    entered = asyncio.Event()
    wait = asyncio.Event()

    async def run():
        async with bind_replay_native(service):
            sentinel[0].close_release.clear()
            entered.set()
            await wait.wait()

    task = asyncio.create_task(run())
    await entered.wait()
    provider = sentinel[0]
    task.cancel("first")
    await provider.close_started.wait()
    state = service._replay_native_state
    assert state.lifecycle == "closing"
    task.cancel("second")
    assert not task.done()
    assert service._replay_native_state is state
    provider.close_release.set()
    with pytest.raises(asyncio.CancelledError) as raised:
        await task
    assert raised.value.args == ("first",)
    assert provider.closed == 1
    assert state.lifecycle == "closed"
    assert "_replay_native_state" not in vars(service)


async def test_cancellation_arriving_only_during_close_is_propagated(service, sentinel):
    release_body = asyncio.Event()
    entered = asyncio.Event()

    async def run():
        async with bind_replay_native(service):
            sentinel[0].close_release.clear()
            entered.set()
            await release_body.wait()

    task = asyncio.create_task(run())
    await entered.wait()
    release_body.set()
    provider = sentinel[0]
    await provider.close_started.wait()
    task.cancel("close cancelled")
    provider.close_release.set()
    with pytest.raises(asyncio.CancelledError, match="close cancelled"):
        await task
    assert provider.closed == 1
    assert "_replay_native_state" not in vars(service)


async def test_cleanup_preserves_first_failure_during_second_cancel():
    started = asyncio.Event()
    release = asyncio.Event()
    original = ValueError("original failure")

    async def cleanup():
        started.set()
        await release.wait()

    task = asyncio.create_task(truth_native_binding._finish_cleanup(cleanup(), failure=original))
    await started.wait()
    task.cancel()
    release.set()
    with pytest.raises(ValueError) as raised:
        await task
    assert raised.value is original


async def test_both_truth_methods_use_owned_provider_and_keep_native_errors(service, sentinel):
    async def snapshot(token_id, cutoff, **kwargs):
        return {
            "token_id": token_id,
            "cutoff": cutoff,
            "flow_bars": [],
            "klines": [],
            "books": [],
            "oi": [],
            "funding": [],
            "liquidations": [],
            "tickers": [],
            "supply": [],
            "semantics": [],
            "instruments": [],
        }

    async def refuse(query, params=None):
        raise RuntimeError("controlled native transport failure")

    service.settings = SimpleNamespace(replay=True, config_version="test", code_version="test")
    service.market = SimpleNamespace(snapshot=snapshot, condition_snapshot=snapshot)
    async with bind_replay_native(service):
        provider = sentinel[0]
        provider.rows = refuse
        with pytest.raises(RuntimeError, match="controlled native transport failure"):
            await service.assess(None, "T", "2026-09-24T12:00:00Z")
        with pytest.raises(RuntimeError, match="controlled native transport failure"):
            await service.condition_values(
                None, "T", "2026-09-24T12:00:00Z", [{"metric": "mt.move_class", "window": "W1H"}]
            )
        assert native_provider(service) is provider
        assert await service.repo.list("ana.market_truth") == []
    assert provider.closed == 1


@pytest.mark.integration
@pytest.mark.process
async def test_genuine_captured_recomputation_persists_native_result_without_repo_provider():
    from tank.util import new_id

    cutoff = "2026-09-24T12:00:00Z"
    event_id, ems_id = new_id("EVT"), new_id("EMS")
    snapshot = {
        "token_id": "T",
        "cutoff": cutoff,
        "flow_bars": [],
        "klines": [],
        "books": [],
        "oi": [],
        "funding": [],
        "liquidations": [],
        "tickers": [],
        "supply": [],
        "semantics": [],
        "instruments": [],
    }
    event = {
        "event_id": event_id,
        "event_state": "APPROVED",
        "current_ems_id": ems_id,
        "tokens_mentioned": ["T"],
        "known_to_us_at": cutoff,
    }
    records = (
        {
            "collection": "evt.events",
            "key": event_id,
            "available_at": cutoff,
            "record": event,
        },
        {
            "collection": "evt.ems_assessments",
            "key": ems_id,
            "available_at": cutoff,
            "record": {"ems_id": ems_id, "event_id": event_id, "class": "MAJOR"},
        },
        {
            "collection": "replay.market_snapshots",
            "key": "snapshot",
            "available_at": cutoff,
            "record": snapshot,
        },
    )
    inputs = ReplayInputs(
        cutoff,
        "test",
        "test",
        {"tokens": [{"token_id": "T", "symbol": "T"}]},
        {
            "thresholds": {},
            "replay_tool_calls": [
                {
                    "call_id": "native-captured-market",
                    "tool": "get_market_truth",
                    "arguments": {"event_id": event_id, "token_ids": ["T"], "cutoff": cutoff},
                    "available_at": cutoff,
                }
            ],
        },
        records,
        (),
    )
    repo = MemoryRepository()
    result = await run_captured_tools(inputs, repo)
    actual = result["tool_calls"][0]["result"]["market_truth"][0]
    stored = await repo.get("ana.market_truth", actual["mta_id"])
    assert stored == actual
    assert actual["move_class"] == "UNKNOWN"
    assert actual["token_id"] == "T"
    assert actual["event_id"] == event_id
    assert not hasattr(repo, "clickhouse")


async def test_replay_installer_stays_absent_with_production_shaped_url(service):
    settings = Settings(replay=True, clickhouse_url="https://production.invalid:8443", _env_file=None)
    assert install(service.repo, settings) is None
    assert not hasattr(service.repo, "clickhouse")
    assert native_provider(service) is None


@pytest.fixture
def synthetic_condition_capture():
    from tank.replay import ReplayMarketReader

    cutoff = "2026-09-24T12:00:00Z"
    source = {
        "token_id": "SYNTHETIC:reader-control",
        "cutoff": cutoff,
        "cutoff_prec": "EXACT",
        "cutoff_prec_reason": None,
        "books": [
            {
                "instrument_id": "z:spot:SYNTHETIC",
                "exchange_ts": cutoff,
                "mid": "1.000000000000000000000001",
            },
            {
                "instrument_id": "a:spot:SYNTHETIC",
                "exchange_ts": cutoff,
                "mid": "2.000000000000000000000002",
            },
        ],
        "flow_bars": [],
        "klines": [],
        "oi": [],
        "funding": [],
        "liquidations": [],
        "tickers": [],
        "supply": [],
        "semantics": [],
        "instruments": [],
        "capture_metadata": {"synthetic_control": True, "order": ["z", "a"]},
    }
    records = tuple(
        {
            "collection": "replay.market_snapshots",
            "key": key,
            "available_at": cutoff,
            "record": record,
        }
        for key, record in (
            ("other-token", dict(deepcopy(source), token_id="OTHER")),
            ("earlier-cutoff", dict(deepcopy(source), cutoff="2026-09-24T11:59:00Z")),
            ("selected", deepcopy(source)),
        )
    )
    inputs = ReplayInputs(cutoff, "reader-config", "reader-code", {}, {}, records, ())
    return ReplayMarketReader(inputs), source


@pytest.mark.parametrize(
    "requirements",
    [
        None,
        [],
        [{"metric": "liq.spread_ratio", "window": "W1H"}],
        [{"metric": "unsupported", "window": "W7D"}],
    ],
)
async def test_condition_reader_exact_capture_preserves_all_fields_order_and_precision(
    synthetic_condition_capture, monkeypatch, requirements
):
    reader, source = synthetic_condition_capture
    retained = deepcopy(reader.inputs._records)
    requested = []
    records = reader.inputs.records

    def observe(self, collection, *, cutoff=None):
        requested.append((collection, cutoff))
        return records(collection, cutoff=cutoff)

    monkeypatch.setattr(type(reader.inputs), "records", observe)
    anchor = {
        "type": "KNOWN_TO_US_AT",
        "ts": "2026-09-24T11:00:00Z",
        "ts_prec": "EXACT",
        "nested": {"source": ["synthetic"]},
    }
    expected = dict(deepcopy(source), anchor=deepcopy(anchor))
    before = await reader.snapshot(source["token_id"], source["cutoff"], anchor=anchor)
    actual = await reader.condition_snapshot(
        source["token_id"], source["cutoff"], anchor=anchor, requirements=requirements
    )
    after = await reader.snapshot(source["token_id"], source["cutoff"], anchor=anchor)
    assert before == actual == after == expected
    assert list(actual) == list(expected)
    assert actual is not before and actual is not after
    assert actual["anchor"] is not anchor
    assert actual["anchor"]["nested"] is not anchor["nested"]
    assert requested == [("replay.market_snapshots", source["cutoff"])] * 3
    actual["books"][0]["mid"] = "CALLER_MUTATION"
    actual["anchor"]["nested"]["source"].append("CALLER_MUTATION")
    assert anchor == expected["anchor"]
    assert reader.inputs._records == retained
    assert await reader.snapshot(source["token_id"], source["cutoff"], anchor=anchor) == expected


@pytest.mark.parametrize("boundary", ["active-phase", "global-replay"])
async def test_condition_reader_rejects_future_cutoff_before_record_access(
    synthetic_condition_capture, monkeypatch, boundary
):
    from tank.replay import ReplayError
    from tank.util import parse_time

    reader, source = synthetic_condition_capture
    if boundary == "active-phase":
        object.__setattr__(reader.inputs, "fixture_reference", {"phases": [{"phase_id": "synthetic"}]})
        reader.active_cutoff = lambda: parse_time("2026-09-24T11:59:00Z")
        cutoff = source["cutoff"]
        reason = "active reference phase"
    else:
        cutoff = "2026-09-24T12:00:00.000001Z"
        reason = "replay cutoff"

    def forbidden(*args, **kwargs):
        pytest.fail("future condition request reached retained records")

    monkeypatch.setattr(type(reader.inputs), "records", forbidden)
    with pytest.raises(ReplayError, match=reason):
        await reader.condition_snapshot(source["token_id"], cutoff, requirements=[])


@pytest.mark.parametrize("case", ["missing", "duplicate", "wrong-token", "wrong-cutoff"])
async def test_condition_reader_requires_one_exact_snapshot(synthetic_condition_capture, case):
    from tank.replay import ReplayError

    reader, source = synthetic_condition_capture
    record = deepcopy(reader.inputs._records[-1])
    if case == "missing":
        records = ()
    elif case == "duplicate":
        records = (record, deepcopy(record))
    else:
        record["record"]["token_id" if case == "wrong-token" else "cutoff"] = (
            "OTHER" if case == "wrong-token" else "2026-09-24T11:59:00Z"
        )
        records = (record,)
    object.__setattr__(reader.inputs, "_records", records)
    with pytest.raises(ReplayError, match="insufficient_fixture: missing exact market snapshot"):
        await reader.condition_snapshot(source["token_id"], source["cutoff"], requirements=[])


async def test_condition_reader_rejects_anchor_after_requested_cutoff(synthetic_condition_capture):
    from tank.replay import ReplayError

    reader, source = synthetic_condition_capture
    anchor = {"ts": "2026-09-24T12:00:00.000001Z", "ts_prec": "EXACT"}
    retained = deepcopy(reader.inputs._records)
    with pytest.raises(ReplayError, match="market anchor exceeds replay cutoff"):
        await reader.condition_snapshot(source["token_id"], source["cutoff"], anchor=anchor)
    assert reader.inputs._records == retained


@pytest.mark.parametrize("reference", [False, True])
async def test_condition_reader_preserves_missing_reference_admission_and_provenance(
    synthetic_condition_capture, reference
):
    from tank.replay import ReplayError

    reader, source = synthetic_condition_capture
    declaration = dict(
        deepcopy(source), artifact_kind="REFERENCE_MISSING_MARKET_INPUTS", missing_inputs=["oi", "books"]
    )
    record = dict(deepcopy(reader.inputs._records[-1]), record=declaration)
    object.__setattr__(reader.inputs, "_records", (record,))
    object.__setattr__(reader.inputs, "input_sha256", "synthetic-input-identity")
    if not reference:
        with pytest.raises(ReplayError, match="not a captured snapshot"):
            await reader.condition_snapshot(source["token_id"], source["cutoff"])
        return
    object.__setattr__(reader.inputs, "fixture_reference", {"fixture_id": "SYNTHETIC-REFERENCE"})
    actual = await reader.condition_snapshot(source["token_id"], source["cutoff"])
    expected = dict(
        deepcopy(declaration),
        unavailable={
            "oi": "NO_RETAINED_REFERENCE_OBSERVATIONS",
            "books": "NO_RETAINED_REFERENCE_OBSERVATIONS",
        },
        fixture_reference_provenance={
            "input_sha256": "synthetic-input-identity",
            "fixture_id": "SYNTHETIC-REFERENCE",
            "source_capture_claim": False,
            "historical_market_acceptance_claim": False,
        },
    )
    assert actual == expected
    assert reader.inputs._records == (record,)


@pytest.fixture(scope="module")
def replay_condition_original_truth():
    from test_truth_sql_conditions import original_truth

    yield from original_truth.__wrapped__()


@pytest.mark.integration
@pytest.mark.process
async def test_scheduler_condition_replays_exact_capture_through_owned_native_process(
    replay_condition_original_truth, monkeypatch
):
    from test_truth_sql_conditions import _compare_caches, _compare_value, _physical_snapshot

    from tank.replay import ReplayMarketReader
    from tank.scheduler import Scheduler

    original = replay_condition_original_truth
    captured = _physical_snapshot(minutes=15)
    cutoff = captured["cutoff"]
    event_id = "SYNTHETIC-SCHEDULER-CONDITION"
    anchor = {
        "type": "KNOWN_TO_US_AT",
        "ts": captured["anchor"]["ts"],
        "ts_prec": "EXACT",
        "ts_prec_reason": None,
    }
    captured["anchor"] = deepcopy(anchor)
    inputs = ReplayInputs(
        cutoff,
        "condition-config",
        "condition-code",
        {},
        {},
        (
            {
                "collection": "replay.market_snapshots",
                "key": "synthetic-condition-capture",
                "available_at": cutoff,
                "record": deepcopy(captured),
            },
        ),
        (),
    )
    retained = deepcopy(inputs._records)
    repo = MemoryRepository()
    await repo.put(
        "evt.events",
        event_id,
        {
            "event_id": event_id,
            "known_to_us_at": anchor["ts"],
            "known_to_us_at_prec": "EXACT",
        },
    )
    retained_repository = deepcopy(repo._data)
    settings = SimpleNamespace(
        replay=True, config_version=inputs.config_version, code_version=inputs.code_version
    )
    config = {"config_version": inputs.config_version, "code_version": inputs.code_version}
    reader = ReplayMarketReader(inputs)
    service = MarketTruthService(repo, settings, reader)
    scheduler = Scheduler(repo, settings, reader, service)
    expected_snapshot = dict(deepcopy(captured), event_id=event_id)
    expected_cache, baselines = {}, {}
    requests, engine_calls, reads = [], [], []
    real_evaluate = truth_sql_engine.evaluate_conditions
    real_records = inputs.records

    def observe_records(self, collection, *, cutoff=None):
        reads.append((collection, cutoff))
        return real_records(collection, cutoff=cutoff)

    monkeypatch.setattr(type(inputs), "records", observe_records)
    async with bind_replay_native(service):
        provider = native_provider(service)
        assert type(provider) is truth_native_local.ReplayNativeRows
        real_rows = provider.rows

        async def observe_rows(query, params=None):
            assert query.lstrip().startswith("WITH")
            assert "con_snapshot" in query and "condition_cache" in query
            assert params and "con_snapshot" in params
            requests.append((query.splitlines()[0], tuple(params)))
            return await real_rows(query, params)

        async def observe_engine(client, snapshot, requirements, config=None, *, cache=None):
            assert client is provider and client is not None
            assert snapshot == expected_snapshot
            assert config == {
                "config_version": inputs.config_version,
                "code_version": inputs.code_version,
            }
            engine_calls.append(deepcopy(requirements))
            return await real_evaluate(client, snapshot, requirements, config, cache=cache)

        monkeypatch.setattr(provider, "rows", observe_rows)
        monkeypatch.setattr(truth_sql_engine, "evaluate_conditions", observe_engine)
        first = [
            {"metric": "liq.spread_ratio", "window": "W15M"},
            {"metric": "spot.cohort.direction", "window": "W15M"},
        ]
        changed = first + [
            {"metric": "liq.depth_p1_vs_pre", "window": "W15M"},
            {"metric": "unsupported", "window": "W7D"},
        ]
        for index, requirements in enumerate((first, first, changed, changed)):
            held_requirements = deepcopy(requirements)
            expected = original.compute_condition_values(
                deepcopy(expected_snapshot),
                deepcopy(requirements),
                config=deepcopy(config),
                cache=expected_cache,
                baseline_cache=baselines,
            )
            request_count = len(requests)
            actual = await scheduler.condition_truth(event_id, "T", cutoff, requirements=requirements)
            assert len(requests) == request_count + 1
            assert engine_calls[-1] == held_requirements
            assert requirements == held_requirements
            _compare_value(actual, expected)
            _compare_caches(service._condition_cache, expected_cache, original)
            assert actual["cutoff"] == cutoff and actual["cutoff_prec"] == "EXACT"
            assert actual["_condition_evaluation"]["transient"] is True
            assert actual["_condition_evaluation"]["requires_market_truth"] == []
            assert actual["liquidity"]["spread_ratio"] == 2
            assert actual["spot"]["W15M"]["direction"] == "BUY"
            if index:
                assert all(proof["reused"] for proof in actual["_condition_evaluation"]["proofs"])
            if index >= 2:
                assert actual["_condition_evaluation"]["unavailable_metrics"] == [
                    {"metric": "unsupported", "window": "W7D", "reason": "NOT_SUPPORTED"}
                ]
            assert "mta_id" not in actual
            assert await repo.list("ana.market_truth") == []
            assert repo._data == retained_repository
            assert not hasattr(repo, "clickhouse")
            assert inputs._records == retained
        assert len(requests) == len(engine_calls) == 4
        assert reads == [("replay.market_snapshots", cutoff)] * 4
    assert native_provider(service) is None
    assert "_replay_native_state" not in vars(service)
    assert provider._closed is True and not provider._requests
