from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
from pydantic import BaseModel, ValidationError

from tank.clickhouse_market import ClickHouse
from tank.contracts import DomainModel
from tank.contracts.job_repository import JobRepository
from tank.contracts.jobs import InvalidJobClaim, JobEnvelope
from tank.contracts.repository import Collection, Repository
from tank.contracts.score import ScoreAssessment, ScoreAssessmentView, ScoreDimension, ScoreDimensions
from tank.market_codec import _prepare_records, encode, merge_point
from tank.market_storage import market_range_by_instrument, market_rows_recorded_since
from tank.models import Record
from tank.retention import BenchmarkToken, RetentionEvidence, benchmark_token_ids, protected_windows
from tank.settings import Settings
from tank.storage import ImmutableRecordError, MemoryRepository
from tank.time_precision import Timestamp, propagate_timestamp


class Observation(DomainModel):
    instrument_id: str
    observed_at: datetime
    count: int


@pytest.mark.parametrize(
    "payload",
    [
        {"instrument_id": "btc", "count": 1},
        {"instrument_id": "btc", "observed_at": datetime.now(UTC), "count": "1"},
        {"instrument_id": "btc", "observed_at": datetime.now(UTC), "count": 1, "extra": True},
    ],
)
def test_domain_rejects_missing_coerced_and_unknown_fields(
    payload: dict[str, str | int | bool | datetime],
) -> None:
    with pytest.raises(ValidationError):
        Observation.model_validate(payload)


async def test_typed_repository_roundtrips_timestamps_and_preserves_immutability() -> None:
    store = MemoryRepository()
    repository = Repository(store, Collection("qa.fixtures", Observation))
    item = Observation(instrument_id="btc", observed_at=datetime.now(UTC), count=1)
    assert await repository.get("missing") is None
    assert await repository.put("one", item, immutable=True) == item
    assert await repository.get("one") == item
    with pytest.raises(ImmutableRecordError):
        await repository.put("one", item.model_copy(update={"count": 2}), immutable=True)


async def test_typed_repository_validates_persisted_payload() -> None:
    store = MemoryRepository()
    await store.put("qa.fixtures", "one", {"instrument_id": "btc", "count": 1})
    repository = Repository(store, Collection("qa.fixtures", Observation))
    with pytest.raises(ValidationError):
        await repository.get("one")


def test_typed_configuration_reports_missing_fields(tmp_path: Path) -> None:
    (tmp_path / "observation.yaml").write_text("instrument_id: btc\ncount: 1\n")
    settings = Settings(config_dir=tmp_path, _env_file=None)
    with pytest.raises(ValidationError):
        settings.load_config_as("observation", Observation)


async def test_repository_refuses_unvalidated_constructed_entity_before_write() -> None:
    store = MemoryRepository()
    repository = Repository(store, Collection("qa.fixtures", Observation))
    entity = Observation.model_construct(instrument_id="btc", observed_at=datetime.now(UTC), count=1)
    object.__setattr__(entity, "count", "bad")
    with pytest.raises(ValueError):
        await repository.put("invalid", entity)
    assert await store.get("qa.fixtures", "invalid") is None


class Measurements(DomainModel):
    samples: list[float]


async def test_repository_refuses_nested_mutation_before_write() -> None:
    store = MemoryRepository()
    repository = Repository(store, Collection("qa.fixtures", Measurements))
    entity = Measurements(samples=[1.0])
    entity.samples.append(float("nan"))
    with pytest.raises(ValueError):
        await repository.put("invalid", entity)
    assert await store.get("qa.fixtures", "invalid") is None


@pytest.mark.parametrize(
    "field,value",
    [("complete", "false"), ("n_buy", True)],
)
def test_market_codec_rejects_coerced_scalars(field: str, value: str | bool) -> None:
    with pytest.raises(ValueError):
        encode("mkt.flow_bars_1m", {field: value})


def test_market_codec_requires_real_identity_instead_of_empty_key_parts() -> None:
    with pytest.raises(ValueError, match="Missing typed market identity"):
        _prepare_records("mkt.oi_obs", [{"instrument_id": "btc", "ts": "2026-10-24T00:00:00Z"}], None)


def test_market_codec_does_not_treat_invalid_source_metadata_as_absent() -> None:
    with pytest.raises(ValueError, match="source_observations"):
        merge_point("mkt.funding_obs", {}, {"source_observations": []})


async def test_repository_refuses_wrong_runtime_schema_before_write() -> None:
    store = MemoryRepository()
    repository = Repository[BaseModel](store, Collection[BaseModel]("qa.fixtures", Observation))
    with pytest.raises(TypeError, match="requires Observation"):
        await repository.put("invalid", Measurements(samples=[1.0]))
    assert await store.get("qa.fixtures", "invalid") is None


def test_score_projection_keeps_complete_domain_metadata_out_of_display() -> None:
    dimension = ScoreDimension(
        value=4,
        rationale="Accepted claim",
        reason=None,
        evidence=["claim-1"],
        proposed_by="LLM",
        caps_applied=["C1"],
    )
    assessment = ScoreAssessment(
        dims=ScoreDimensions(
            economic_directness=dimension,
            magnitude=dimension,
            permanence=dimension,
            distribution_expansion=dimension,
            mechanical_flows=dimension,
            surprise=dimension,
            implementation=dimension,
        )
    )
    view = ScoreAssessmentView.from_domain(assessment)
    assert view.dims.magnitude.value == 4
    assert view.dims.magnitude.rationale == "Accepted claim"
    assert "evidence" not in view.dims.magnitude.model_dump()
    assert assessment.dims.magnitude.evidence == ["claim-1"]
    assert assessment.dims.magnitude.caps_applied == ["C1"]


def test_missing_occurrence_requires_declared_reason() -> None:
    with pytest.raises(ValueError, match="explicit reason"):
        Timestamp(None, "COLLECTION_ONLY")


def test_timestamp_adapter_rejects_conflicting_value_aliases() -> None:
    with pytest.raises(ValueError, match="Conflicting timestamp value aliases"):
        propagate_timestamp(
            {"ts": {"value": "2026-10-24T00:00:00Z", "ts": "2026-10-24T01:00:00Z"}},
            "ts",
        )


async def test_typed_job_lifecycle_preserves_absent_metadata_and_real_clocks() -> None:
    store = MemoryRepository()
    jobs = JobRepository(store)
    queued = await jobs.enqueue("TEST", {}, "one")
    assert queued.status == "QUEUED"
    assert queued.finished_at is None
    claimed = await jobs.claim()
    assert isinstance(claimed, JobEnvelope)
    assert claimed.status == "RUNNING"
    await jobs.put(claimed.model_copy(update={"slot_admitted": True}))
    stored = await store.get("ops.jobs", claimed.job_id)
    assert stored is not None
    assert stored["slot_admitted"] is True
    assert "interrupted_at" not in stored
    await jobs.finish(claimed.job_id)
    finished = await jobs.get(claimed.job_id)
    assert finished is not None
    assert finished.status == "DONE"
    assert finished.finished_at is not None
    assert finished.finished_at_prec == "COLLECTION_ONLY"


async def test_typed_job_adapter_refuses_fabricated_completion_before_write() -> None:
    store = MemoryRepository()
    jobs = JobRepository(store)
    queued = await jobs.enqueue("TEST", {}, "one")
    with pytest.raises(ValueError, match="completion clock"):
        await jobs.put(queued.model_copy(update={"status": "DONE"}))
    stored = await store.get("ops.jobs", queued.job_id)
    assert stored is not None
    assert stored["status"] == "QUEUED"


async def test_clickhouse_rejects_nonfinite_json_before_http_write() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200)

    client = ClickHouse(
        "http://clickhouse.invalid",
        http=httpx.AsyncClient(transport=httpx.MockTransport(respond)),
    )
    try:
        with pytest.raises(ValueError, match="finite"):
            await client.insert("mkt.klines_1m", [{"c": float("nan")}])
    finally:
        await client.close()
    assert requests == []


def test_domain_validation_errors_omit_raw_inputs_without_losing_field_reason() -> None:
    secret = "fake-unconfigured-refresh-secret"
    with pytest.raises(ValidationError) as failure:
        Observation.model_validate(
            {"instrument_id": "btc", "observed_at": datetime.now(UTC), "count": secret}
        )
    message = str(failure.value)
    assert secret not in message
    assert "count" in message
    assert "valid integer" in message


def test_json_codec_errors_omit_raw_invalid_document() -> None:
    from tank.contracts.json import decode_json

    secret = "fake-unconfigured-refresh-secret"
    with pytest.raises(ValidationError) as failure:
        decode_json('{"token":"' + secret + '", broken}')
    assert secret not in str(failure.value)
    assert "Invalid JSON" in str(failure.value)


async def test_memory_market_reads_return_real_persisted_keys() -> None:
    store = MemoryRepository()
    row: Record = {
        "instrument_id": "A",
        "open_time": "2026-10-24T00:00:00Z",
        "open_time_prec": "EXACT",
        "recorded_at": "2026-10-24T00:01:00Z",
        "recorded_at_prec": "COLLECTION_ONLY",
        "o": "1",
        "h": "1",
        "l": "1",
        "c": "1",
        "volume_native": "1",
        "interval_s": 60,
        "complete": True,
    }
    await store.put_market_batch("mkt.klines_1m", [row], keys=["physical-key"])
    keys: dict[str, list[str]] = {}
    result = await market_range_by_instrument(
        store,
        "mkt.klines_1m",
        "open_time",
        ["A"],
        "2026-10-24T00:00:00Z",
        "2026-10-24T01:00:00Z",
        keys_out=keys,
    )
    assert len(result["A"]) == 1
    assert keys == {"A": ["physical-key"]}
    changed = await market_rows_recorded_since(
        store,
        "mkt.klines_1m",
        "open_time",
        ["A"],
        "2026-10-24T00:00:00Z",
        "2026-10-24T00:00:00Z",
        "2026-10-24T01:00:00Z",
    )
    assert [entry[2] for entry in changed] == ["physical-key"]


def test_retention_policy_uses_exact_declared_utc_anchor_window() -> None:
    evidence = RetentionEvidence(
        event_id="major",
        anchors=(
            datetime(2026, 10, 20, 12, tzinfo=timezone(timedelta(hours=2))),
            datetime(2026, 10, 21, 18, tzinfo=UTC),
        ),
        token_ids=frozenset({"token:T"}),
    )
    unknown = RetentionEvidence(event_id="unknown", anchors=(), token_ids=frozenset({"unknown:T"}))
    windows = protected_windows([evidence, unknown])
    assert len(windows) == 1
    assert windows[0].event_id == "major"
    assert windows[0].token_id == "token:T"
    assert windows[0].start == datetime(2026, 10, 19, 10, tzinfo=UTC)
    assert windows[0].end == datetime(2026, 10, 24, 18, tzinfo=UTC)


def test_retention_benchmarks_include_registered_ids_and_retained_aliases() -> None:
    selected = benchmark_token_ids(
        [
            BenchmarkToken(token_id="registered:BTC", symbol="BTC"),
            BenchmarkToken(token_id="registered:ETH", symbol="ETH"),
            BenchmarkToken(token_id="unrelated:T", symbol="T"),
        ]
    )
    assert selected == frozenset(
        {"bitcoin:BTC", "ethereum:ETH", "BTC", "ETH", "registered:BTC", "registered:ETH"}
    )


async def test_invalid_claim_rejection_preserves_unknown_provenance_and_is_once_only() -> None:
    store = MemoryRepository()
    await store.put(
        "ops.jobs",
        "legacy",
        {
            "job_id": "legacy",
            "type": "TEST",
            "status": "QUEUED",
            "priority": 1,
            "idem_key": "legacy",
            "payload": {},
            "attempts": 0,
            "max_attempts": 3,
            "run_after": "2026-10-24T00:00:00Z",
            "expires_at": None,
            "fake-private-field-key": "fake-private-value",
        },
    )
    jobs = JobRepository(store)
    invalid = await jobs.claim()
    assert isinstance(invalid, InvalidJobClaim)
    assert "fake-private-field-key" not in invalid.model_dump_json()
    assert "fake-private-value" not in invalid.model_dump_json()
    assert "extra_field" in invalid.error_fields
    assert await jobs.reject_invalid(invalid)
    assert not await jobs.reject_invalid(invalid)
    stored = await store.get("ops.jobs", "legacy")
    assert stored is not None
    assert stored["status"] == "FAILED"
    assert stored["last_error"] == "INVALID_QUEUE_ENVELOPE"
    assert stored["finished_at_prec"] == "COLLECTION_ONLY"
    assert "run_after_prec" not in stored
    assert stored["fake-private-field-key"] == "fake-private-value"
    assert await jobs.claim() is None


async def test_invalid_claim_rejection_refuses_a_changed_revision() -> None:
    store = MemoryRepository()
    await store.put(
        "ops.jobs",
        "legacy",
        {
            "job_id": "legacy",
            "type": "TEST",
            "status": "QUEUED",
            "priority": 1,
            "idem_key": "legacy",
            "payload": {},
            "attempts": 0,
            "max_attempts": 3,
            "run_after": "2026-10-24T00:00:00Z",
            "expires_at": None,
        },
    )
    jobs = JobRepository(store)
    invalid = await jobs.claim()
    assert isinstance(invalid, InvalidJobClaim)
    current = await store.get("ops.jobs", "legacy")
    assert current is not None
    current["payload"] = {"operator_revision": True}
    await store.put("ops.jobs", "legacy", current)
    assert not await jobs.reject_invalid(invalid)
    assert await store.get("ops.jobs", "legacy") == current


async def test_raw_document_store_rejects_nonfinite_json_before_write() -> None:
    store = MemoryRepository()
    with pytest.raises(ValueError, match="finite"):
        await store.put("qa.fixtures", "invalid", {"value": float("nan")})
    assert await store.get("qa.fixtures", "invalid") is None


def test_timestamp_validation_reason_does_not_echo_raw_source_value() -> None:
    secret = "fake-unconfigured-timestamp-secret"
    with pytest.raises(ValueError) as failure:
        Timestamp(secret, "EXACT")
    assert secret not in str(failure.value)
    assert "Invalid ISO timestamp" in str(failure.value)
