"""Authored settled history batch collected before its actual analysis assembly."""

from funding_input_fixtures import funding_record_fixture, funding_scope_fixture
from pydantic import JsonValue

from tank.analysis.funding_history import FundingHistoryPolicy
from tank.analysis.funding_rate import FundingNormalizationRequest
from tank.analysis.funding_selection import FundingCalculator, FundingWindowRequest
from tank.time_precision import Timestamp


def request() -> FundingWindowRequest:
    return FundingWindowRequest(
        start=Timestamp(value="2026-10-16T06:00:00Z", precision="EXACT", precision_reason=None),
        cutoff=Timestamp(value="2026-10-16T12:00:00Z", precision="EXACT", precision_reason=None),
        assembled_at=Timestamp(
            value="2026-10-16T12:05:00Z", precision="COLLECTION_ONLY", precision_reason=None
        ),
    )


def policy() -> FundingHistoryPolicy:
    return FundingHistoryPolicy(
        minimum_history_days=7,
        lookback_days=30,
        quantiles=(0.025, 0.975),
        normalization=FundingNormalizationRequest(mode="RECORDED_ONLY"),
    )


def rows() -> list[dict[str, JsonValue]]:
    first, last = funding_record_fixture("SETTLED"), funding_record_fixture("SETTLED")
    first.update(settlement_time="2026-10-01T08:00:00Z", funding_time="2026-10-01T08:00:00Z", rate_8h_eq="1")
    last.update(settlement_time="2026-10-09T08:00:00Z", funding_time="2026-10-09T08:00:00Z", rate_8h_eq="3")
    return [first, last, funding_record_fixture("CURRENT_ESTIMATE")]


def calculator(records: list[dict[str, JsonValue]]) -> FundingCalculator:
    return FundingCalculator(token_id="T", instruments=[funding_scope_fixture()], records=records)
