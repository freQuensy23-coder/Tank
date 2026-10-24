from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import BaseModel, ValidationError

from tank.contracts import DomainModel
from tank.contracts.job_repository import JobRepository
from tank.contracts.repository import Collection, Repository
from tank.contracts.score import ScoreAssessment, ScoreAssessmentView, ScoreDimension, ScoreDimensions
from tank.market_codec import _prepare_records, encode, merge_point
from tank.market_schema import MARKET_COLUMNS
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
    entity = Observation.model_construct(instrument_id="btc", observed_at=datetime.now(UTC), count="bad")
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
        _prepare_records(
            "mkt.oi_obs",
            [{"instrument_id": "btc", "ts": "2026-10-24T00:00:00Z"}],
            None,
            MARKET_COLUMNS["mkt.oi_obs"],
            False,
        )


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
    assert claimed is not None
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
