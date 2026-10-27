from datetime import UTC, datetime, timedelta

import pytest
from foundation_helpers import text_field
from test_clickhouse_market import Both
from test_clickhouse_market import both as both
from test_storage import repo as repo

from tank.contracts.registry import (
    OpenInterestSemantics,
    RegistryConfiguration,
    RegistryInstrument,
    SemanticsConfiguration,
)
from tank.market_codec import _prepare_records
from tank.market_storage import WRITE_CHUNK
from tank.models import Record
from tank.settings import Settings
from tank.storage import StorageBackend
from tank.util import iso


@pytest.fixture(scope="module")
def admitted_oi_rows() -> tuple[Record, Record]:
    """Admit synthetic software labels against real registry and metric definitions.

    Physical instrument/metric facts stay as declared in the current configuration.
    Only their opaque software identifiers change; this is not a live-capture claim.
    """
    settings = Settings(_env_file=None)
    registry = settings.registry()
    semantics = settings.semantics()
    originals = [
        item for item in registry.instruments if item.venue == "binance" and item.market_type == "PERP_LINEAR"
    ][:2]
    assert len(originals) == 2 and originals[0].venue_symbol != originals[1].venue_symbol
    instruments = [
        RegistryInstrument.model_validate(original.model_copy(update={"instrument_id": identifier}))
        for original, identifier in zip(
            originals, ("storage-fixture:instrument:extended", "storage-fixture:instrument"), strict=True
        )
    ]
    admitted_registry = RegistryConfiguration.model_validate(
        registry.model_copy(update={"instruments": instruments})
    )
    metric = next(
        item
        for item in semantics.metrics
        if isinstance(item, OpenInterestSemantics) and item.metric_key == "binance.usdm.openInterest"
    )
    metrics = [
        metric,
        OpenInterestSemantics.model_validate(
            metric.model_copy(update={"metric_key": "extended:" + metric.metric_key})
        ),
    ]
    admitted_semantics = SemanticsConfiguration.model_validate(
        semantics.model_copy(update={"metrics": metrics})
    )
    assert len(admitted_registry.instruments) == len(admitted_semantics.metrics) == 2
    rows: list[Record] = []
    for instrument, definition, quantity in zip(instruments, metrics, ("10", "20"), strict=True):
        rows.append(
            {
                "instrument_id": instrument.instrument_id,
                "token_id": instrument.token_id,
                "metric_key": definition.metric_key,
                "ts": "2026-10-18T00:00:00Z",
                "ts_prec": "EXACT",
                "ts_prec_reason": None,
                "received_ts": "2026-10-18T00:00:01Z",
                "received_ts_prec": "COLLECTION_ONLY",
                "received_ts_prec_reason": None,
                "raw_value": quantity,
                "raw_unit": definition.unit,
                "sidedness": definition.sidedness,
                "oi_native": quantity,
                "oi_usd": None,
            }
        )
    return rows[0], rows[1]


def observation_sequence(first: Record, count: int) -> list[Record]:
    start = datetime(2026, 10, 18, tzinfo=UTC)
    return [
        {
            **first,
            "ts": iso(start + timedelta(seconds=index)),
            "received_ts": iso(start + timedelta(seconds=index + 1)),
        }
        for index in range(count)
    ]


async def assert_collision_refused(repository: StorageBackend, rows: list[Record]) -> None:
    with pytest.raises(ValueError, match="Conflicting generated market storage key"):
        changed = await repository.put_market_batch("mkt.oi_obs", rows)
        stored = await repository.list("mkt.oi_obs")
        pytest.fail(
            f"Distinct admitted identities accepted: requested={len(rows)} returned={len(changed)} stored={len(stored)}"
        )
    assert await repository.list("mkt.oi_obs") == []


def test_distinct_admitted_coordinates_have_the_same_legacy_auto_key(
    admitted_oi_rows: tuple[Record, Record],
) -> None:
    first, second = admitted_oi_rows
    assert first["instrument_id"] != second["instrument_id"]
    assert first["metric_key"] != second["metric_key"]
    assert tuple(_prepare_records("mkt.oi_obs", [first], None)) == tuple(
        _prepare_records("mkt.oi_obs", [second], None)
    )


@pytest.mark.parametrize("cross_chunk", [False, True])
async def test_auto_key_batch_refuses_distinct_identities_before_effects(
    repo: StorageBackend, admitted_oi_rows: tuple[Record, Record], cross_chunk: bool
) -> None:
    first, second = admitted_oi_rows
    rows = observation_sequence(first, WRITE_CHUNK if cross_chunk else 1)
    if cross_chunk:
        rows[0]["ts"] = {
            "value": "2026-10-18T02:00:00+02:00",
            "precision": "EXACT",
            "precision_reason": None,
        }
    rows.append(second)
    await assert_collision_refused(repo, rows)


async def test_same_identity_auto_key_coalescing_still_preserves_the_last_observation(
    repo: StorageBackend, admitted_oi_rows: tuple[Record, Record]
) -> None:
    first = admitted_oi_rows[0]
    newer = {**first, "raw_value": "12", "oi_native": "12"}
    changed = await repo.put_market_batch("mkt.oi_obs", [first, newer])
    assert len(changed) == 1 and changed[0]["oi_native"] == "12"
    stored = await repo.list("mkt.oi_obs")
    assert len(stored) == 1 and stored[0]["oi_native"] == "12"


async def test_explicit_distinct_keys_admit_both_supported_coordinate_tuples(
    repo: StorageBackend, admitted_oi_rows: tuple[Record, Record]
) -> None:
    changed = await repo.put_market_batch(
        "mkt.oi_obs", admitted_oi_rows, keys=["fixture:point-one", "fixture:point-two"]
    )
    assert len(changed) == 2
    stored = await repo.list("mkt.oi_obs")
    assert {text_field(row, "instrument_id") for row in stored} == {
        text_field(row, "instrument_id") for row in admitted_oi_rows
    }


@pytest.mark.parametrize("cross_chunk", [False, True])
async def test_ordinary_auto_key_batches_retain_every_distinct_observation(
    repo: StorageBackend, admitted_oi_rows: tuple[Record, Record], cross_chunk: bool
) -> None:
    rows = observation_sequence(admitted_oi_rows[0], WRITE_CHUNK + 1 if cross_chunk else 2)
    if cross_chunk:
        rows[0]["ts"] = {
            "value": "2026-10-18T02:00:00+02:00",
            "precision": "EXACT",
            "precision_reason": None,
        }
    changed = await repo.put_market_batch("mkt.oi_obs", rows)
    assert len(changed) == len(rows)
    assert len(await repo.list("mkt.oi_obs")) == len(rows)


@pytest.mark.parametrize("usd", ["NaN", "-1", "0"])
async def test_raw_reported_usd_does_not_erase_known_native_evidence(
    repo: StorageBackend, admitted_oi_rows: tuple[Record, Record], usd: str
) -> None:
    row = {**admitted_oi_rows[0], "oi_usd": usd}
    changed = await repo.put_market_batch("mkt.oi_obs", [row])
    assert len(changed) == 1 and changed[0]["oi_native"] == "10" and changed[0]["oi_usd"] == usd
    stored = await repo.list("mkt.oi_obs")
    assert len(stored) == 1 and stored[0]["oi_native"] == "10" and stored[0]["oi_usd"] == usd


@pytest.mark.integration
@pytest.mark.parametrize("cross_chunk", [False, True])
async def test_routed_clickhouse_refuses_collision_before_any_batch_effects(
    both: Both, admitted_oi_rows: tuple[Record, Record], cross_chunk: bool
) -> None:
    _, routed, _ = both
    rows = observation_sequence(admitted_oi_rows[0], WRITE_CHUNK if cross_chunk else 1)
    rows.append(admitted_oi_rows[1])
    await assert_collision_refused(routed, rows)


@pytest.mark.integration
async def test_routed_clickhouse_preserves_ordinary_batches_and_same_identity_coalescing(
    both: Both, admitted_oi_rows: tuple[Record, Record]
) -> None:
    _, routed, _ = both
    rows = observation_sequence(admitted_oi_rows[0], WRITE_CHUNK + 1)
    assert len(await routed.put_market_batch("mkt.oi_obs", rows)) == len(rows)
    newer = {**rows[0], "raw_value": "12", "oi_native": "12"}
    changed = await routed.put_market_batch("mkt.oi_obs", [rows[0], newer])
    assert len(changed) == 1 and changed[0]["oi_native"] == "12"
    assert len(await routed.list("mkt.oi_obs")) == len(rows)


@pytest.mark.integration
async def test_routed_clickhouse_keeps_raw_usd_qualification_separate_from_native_values(
    both: Both, admitted_oi_rows: tuple[Record, Record]
) -> None:
    _, routed, _ = both
    values = ("NaN", "-1", "0")
    rows = [
        {**row, "oi_usd": usd}
        for row, usd in zip(observation_sequence(admitted_oi_rows[0], len(values)), values, strict=True)
    ]
    assert len(await routed.put_market_batch("mkt.oi_obs", rows)) == len(values)
    stored = await routed.list("mkt.oi_obs")
    assert {text_field(row, "oi_usd") for row in stored} == set(values)
    assert all(row["oi_native"] == "10" for row in stored)
