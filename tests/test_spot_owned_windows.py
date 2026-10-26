from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tank.analysis.condition_graph_contracts import ObservedConditionComposite
from tank.analysis.condition_input_codec import SpotWindowEvidenceLookup, SpotWindowFacts
from tank.analysis.condition_inputs import SpotCalculationPolicy
from tank.time_precision import chosen_boundary, collection_clock
from tank.util import JsonInput

AT = datetime(2026, 10, 19, 12, tzinfo=UTC)


def owned_windows(row: dict[str, JsonInput]) -> SpotWindowEvidenceLookup:
    return SpotWindowEvidenceLookup(
        token_id="T",
        cutoff=chosen_boundary(AT + timedelta(minutes=1), precision="EXACT"),
        event_anchor=chosen_boundary(AT, precision="EXACT"),
        assembled_at=collection_clock(AT + timedelta(minutes=2)),
        instruments=[{"instrument_id": "A", "token_id": "T", "venue": "V", "market_type": "SPOT"}],
        flow_bars=[row],
        flow_parent_bars=[],
        flow_segments=[],
        tickers=[],
        policy=SpotCalculationPolicy(significant_flow_sigma=Decimal(2), min_spot_venues=1),
        native_ticker_volume_field="volume_native",
    )


def minute() -> dict[str, JsonInput]:
    return {
        "instrument_id": "A",
        "minute_start": "2026-10-19T12:00:00Z",
        "complete": True,
        "buy_qty": "3",
        "sell_qty": "1",
        "sum_sq_qty": "4",
        "received_ts": "2026-10-19T12:01:02Z",
    }


def test_owned_source_and_cached_graph_views_have_no_caller_mutation_alias() -> None:
    raw = minute()
    owned = owned_windows(raw)
    raw["buy_qty"] = "999"
    graph = owned.graph
    observed = graph.spots["EVENT"].composite_max
    assert isinstance(observed, ObservedConditionComposite)
    assert observed.volume == Decimal(4)
    graph.spots.clear()
    facts = owned.facts
    facts.graph.spots.clear()
    assert len(owned.graph.spots) == 5
    assert owned.source.minutes[0].buy_qty == Decimal(3)
    assert "assembled_at" not in owned.source.model_dump()
    assert owned.identity == owned.facts.source_identity


def test_named_cached_facts_refuse_copied_scope_mismatch() -> None:
    owned = owned_windows(minute())
    facts = owned.facts
    wrong = facts.source_identity.model_copy(update={"token_id": "OTHER"})
    with pytest.raises(ValidationError, match="actual source subject"):
        SpotWindowFacts.model_validate(facts.model_copy(update={"source_identity": wrong}))


def test_owned_sign_history_uses_request_clock_without_replacing_original_receipt() -> None:
    owned = owned_windows(minute())
    before = owned.snapshot.entries
    result = owned.sign_history(
        interval=owned.graph.spots["W15M"].interval,
        selected_venues=("V",),
        assembled_at=collection_clock(AT + timedelta(minutes=2)),
    )
    assert result.token_id == "T"
    assert len(result.windows) == 1
    assert result.windows[0].observations[0].value is None
    assert owned.snapshot.entries == before


def test_owned_facts_refuse_a_request_with_different_source_visibility() -> None:
    owned = owned_windows(minute())
    early = collection_clock(AT + timedelta(seconds=61))
    later = collection_clock(AT + timedelta(minutes=3))
    assert owned.matches_visibility(early) is False
    assert owned.matches_visibility(later) is True
    with pytest.raises(ValueError, match="recomputation"):
        owned.wire_spots(assembled_at=early)
    composite = owned.wire_spots(assembled_at=later)["EVENT"]["composite_max"]
    assert isinstance(composite, dict)
    assert composite["volume"] == "4"
