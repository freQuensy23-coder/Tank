from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from tank.contracts import DomainModel
from tank.contracts.repository import Collection, Repository
from tank.market_codec import _prepare_records, encode, merge_point
from tank.market_schema import MARKET_COLUMNS
from tank.settings import Settings
from tank.storage import ImmutableRecordError, MemoryRepository


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
