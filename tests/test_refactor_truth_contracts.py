"""Assessment jobs pass validated immutable identities instead of duplicated financial records."""

from pathlib import Path

import pytest
from pydantic import JsonValue, ValidationError

from tank.analysis.truth import MarketTruthService
from tank.analysis.truth_contracts import MarketTruthAssessmentRef
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.time_precision import Timestamp


def test_reference_rejects_missing_identity_and_undeclared_cutoff_precision() -> None:
    complete: dict[str, JsonValue] = {
        "mta_id": "MTA-1",
        "event_id": "event-1",
        "token_id": "bitcoin:BTC",
        "cutoff": "2026-06-17T04:00:00Z",
        "cutoff_prec": "EXACT",
        "cutoff_prec_reason": None,
    }
    for field in complete:
        with pytest.raises(ValidationError):
            MarketTruthAssessmentRef.from_record(
                {key: value for key, value in complete.items() if key != field}
            )
    with pytest.raises(ValidationError):
        MarketTruthAssessmentRef.from_record(complete | {"cutoff_prec": None})
    assert MarketTruthAssessmentRef.from_record(complete).mta_id == "MTA-1"


async def test_assessment_job_returns_reference_to_actual_persisted_full_record(tmp_path: Path) -> None:
    class Market:
        async def snapshot(
            self, token_id: str, cutoff: Timestamp, anchor: dict[str, JsonValue] | None = None
        ) -> dict[str, JsonValue]:
            return {"token_id": token_id, **cutoff.fields("cutoff"), "anchor": anchor}

    repo = MemoryRepository()
    await repo.open()
    await repo.put(
        "evt.events",
        "event-1",
        {"event_id": "event-1", "known_to_us_at": "2026-06-17T00:00:00Z", "known_to_us_at_prec": "EXACT"},
    )
    service = MarketTruthService(repo, Settings(data_dir=tmp_path, _env_file=None), Market())
    reference = await service.assess_for_job(
        "event-1", "bitcoin:BTC", Timestamp("2026-06-17T04:00:00Z", "EXACT")
    )
    stored = await repo.get("ana.market_truth", reference.mta_id)
    assert stored is not None
    assert MarketTruthAssessmentRef.from_record(stored) == reference
    assert reference.cutoff_prec == "EXACT"
    assert "spot" in stored and "measurement_metadata" in stored
    assert "spot" not in reference.model_dump(mode="json")
    await repo.close()
