"""Operational assembly of precisely typed baseline caches with one retention budget."""

from dataclasses import dataclass
from typing import Self

from tank.analysis.funding_history_cache import FundingHistoryCache
from tank.analysis.oi_history_cache import OIHistoryCache
from tank.analysis.statistics_contracts import (
    BaselineCache,
    BaselineRetention,
    BetaEstimate,
    BitcoinContextHistory,
    DepthBaseline,
    FundingBaseline,
    HourlyReturns,
    NativeVolumeIndex,
    OpenInterestBaseline,
    RelativeHistory,
)


@dataclass(frozen=True, slots=True)
class BaselineCaches:
    open_interest: BaselineCache[OpenInterestBaseline]
    oi_history: OIHistoryCache
    funding_history: FundingHistoryCache
    funding: BaselineCache[FundingBaseline]
    hourly_returns: BaselineCache[HourlyReturns]
    beta: BaselineCache[BetaEstimate]
    relative_history: BaselineCache[RelativeHistory]
    bitcoin_context: BaselineCache[BitcoinContextHistory]
    native_volume: BaselineCache[NativeVolumeIndex]
    depth: BaselineCache[DepthBaseline]

    @classmethod
    def with_capacity(cls, capacity: int) -> Self:
        retention = BaselineRetention(capacity)
        return cls(
            open_interest=BaselineCache("open_interest", retention),
            oi_history=OIHistoryCache(retention),
            funding_history=FundingHistoryCache(retention),
            funding=BaselineCache("funding", retention),
            hourly_returns=BaselineCache("hourly_returns", retention),
            beta=BaselineCache("beta", retention),
            relative_history=BaselineCache("relative_history", retention),
            bitcoin_context=BaselineCache("bitcoin_context", retention),
            native_volume=BaselineCache("native_volume", retention),
            depth=BaselineCache("depth", retention),
        )
