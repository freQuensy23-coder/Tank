"""Declared normalized funding batch and instrument scope for pure calculation tests."""

from pydantic import JsonValue

from tank.analysis.condition_funding_inputs import FundingKind


def funding_scope_fixture() -> dict[str, JsonValue]:
    return {"instrument_id": "venue:perp:T", "token_id": "T", "venue": "venue", "market_type": "PERP_LINEAR"}


def funding_record_fixture(kind: FundingKind) -> dict[str, JsonValue]:
    """The authored batch was received at 12:01; source observations remain separate."""
    row: dict[str, JsonValue] = {
        "instrument_id": "venue:perp:T",
        "kind": kind,
        "rate": "-0.001",
        "interval_h": "4",
        "rate_8h_eq": "-0.002",
        "as_of": "2026-10-16T12:00:00Z",
        "as_of_prec": "COLLECTION_ONLY",
        "received_ts": "2026-10-16T12:01:00Z",
        "received_ts_prec": "COLLECTION_ONLY",
        "source_path": "fixture:normalized-funding:first",
    }
    if kind == "SETTLED":
        row.update(
            settlement_time="2026-10-16T08:00:00Z",
            settlement_time_prec="EXACT",
            funding_time="2026-10-16T08:00:00Z",
            funding_time_prec="EXACT",
        )
    return row
