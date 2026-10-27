"""Explicit current funding normalization request and unavailable causes."""

from typing import Literal

from tank.contracts import DomainModel

type FundingNormalizationMode = Literal["RECORDED_ONLY", "CALCULATE_FROM_RAW"]
type FundingNormalizationFailure = Literal[
    "RAW_NORMALIZATION_NOT_REQUESTED",
    "RATE_NOT_CAPTURED",
    "RATE_NOT_ESTIMABLE",
    "INTERVAL_NOT_CAPTURED",
    "INTERVAL_NOT_ESTIMABLE",
    "NONPOSITIVE_INTERVAL",
    "NOT_ESTIMABLE",
]


class FundingNormalizationRequest(DomainModel):
    mode: FundingNormalizationMode
