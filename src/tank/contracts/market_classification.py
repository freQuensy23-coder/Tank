"""Pure market-classification vocabulary shared across domain consumers."""

from typing import Literal

type DirectMarketMoveClass = Literal[
    "SPOT_LED_ACCUMULATION",
    "ORGANIC_MIXED",
    "DISTRIBUTION",
    "SELL_SIDE_ABSORPTION",
    "REAL_MARKET_FRAGMENTATION",
    "PRICE_MARKUP_FAKE_OI",
    "SHORT_SQUEEZE",
    "PERP_LED_CHASE",
    "LIQUIDITY_VACUUM",
]
type KnownMarketMoveClass = DirectMarketMoveClass | Literal["PASSIVE_SPOT_ABSORPTION"]
type MarketMoveClass = KnownMarketMoveClass | Literal["UNKNOWN"]
type KnownMarketDivergenceState = Literal[
    "PERP_LED_MARKUP",
    "ABSORPTION_CANDIDATE",
    "ABSORPTION_CONFIRMED",
    "LIQUIDITY_VACUUM",
    "FRAGMENTATION",
]
type MarketDivergenceState = KnownMarketDivergenceState | Literal["NONE", "UNKNOWN"]
type MarketClassificationFlag = Literal[
    "PRICE_MARKUP_OI",
    "NOT_COHORT_CONSISTENT",
    "PRE_EVENT_RUNUP",
    "FRAGMENTATION_INVESTIGATE",
]
