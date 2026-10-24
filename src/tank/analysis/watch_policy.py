"""Pure decision monitoring policy over explicit reference prices and typed predicates."""

from typing import Literal

from tank.analysis.watch_contracts import (
    WatchAll,
    WatchLeaf,
    WatchMetric,
    WatchPersistence,
    WatchProposal,
    WatchReferencePrices,
    WatchScalar,
)
from tank.util import new_id


def _expr(
    metric: WatchMetric,
    op: Literal["==", "!=", ">", ">=", "<", "<=", "in"],
    value: WatchScalar | list[WatchScalar],
    window: Literal["W15M", "W1H", "W4H", "W24H", "EVENT"] = "W1H",
    count: int = 1,
) -> WatchLeaf:
    return WatchLeaf(
        metric=metric,
        window=window,
        op=op,
        value=value,
        persist=WatchPersistence(count=count, unit="consecutive_windows"),
    )


def _condition(
    role: Literal["UPGRADE", "DOWNGRADE", "INVALIDATION"],
    text: str,
    *expressions: WatchLeaf,
) -> WatchProposal:
    if not expressions:
        raise ValueError("Watch proposal requires a predicate")
    return WatchProposal(
        cond_id=new_id("CND"),
        role=role,
        human_text=text,
        status="ARMED",
        expr=expressions[0] if len(expressions) == 1 else WatchAll(all=list(expressions)),
    )


def build_watch_conditions(
    action: str,
    move: str,
    band: str,
    prices: WatchReferencePrices,
    has_open_call: bool,
    *,
    wait_data: bool = False,
) -> list[WatchProposal]:
    anchor = prices.anchor_price
    down: WatchProposal | None = _condition(
        "DOWNGRADE",
        "Spot SELL with most volume selling for two consecutive hourly windows.",
        _expr("spot.cohort.direction", "==", "SELL", count=2),
        _expr("spot.cohort.cwb_sell", ">", 0.5, count=2),
    )
    if has_open_call:
        down = _condition(
            "DOWNGRADE",
            "Spot cohort BROAD SELL with most volume selling for two consecutive hourly windows; REDUCE.",
            _expr("spot.cohort.direction", "==", "SELL", count=2),
            _expr("spot.cohort.cwb_sell", ">", 0.5, count=2),
            _expr("spot.classification", "==", "BROAD", count=2),
        )
    up = _condition(
        "UPGRADE",
        "Spot BUY with most volume buying for two consecutive hourly windows and native OI not above normal.",
        _expr("spot.cohort.direction", "==", "BUY", count=2),
        _expr("spot.cohort.cwb_buy", ">", 0.5, count=2),
        _expr("deriv.native_oi_vs_normal", "in", ["WITHIN", "BELOW"]),
    )
    if action == "WAIT" and wait_data:
        up = _condition("UPGRADE", "Data confidence becomes HIGH; re-grade.", _expr("dc.class", "==", "HIGH"))
    elif action == "WAIT":
        up = _condition(
            "UPGRADE",
            "Data HIGH and the move class resolves to spot-led buying (aggressor side and price response checked) or organic mixed.",
            _expr("dc.class", "==", "HIGH"),
            _expr("mt.move_class", "in", ["SPOT_LED_ACCUMULATION", "ORGANIC_MIXED"]),
        )
    if action == "DONT_CHASE":
        if move == "LIQUIDITY_VACUUM":
            up = _condition(
                "UPGRADE",
                "Depth above half its pre-event level and spot BUY.",
                _expr("liq.depth_p1_vs_pre", ">", 0.5),
                _expr("spot.cohort.direction", "==", "BUY"),
            )
        elif band == "HEAVY":
            up = _condition(
                "UPGRADE",
                "Repricing returns to EARLY with spot BUY maintained.",
                _expr("price.repricing_band", "==", "EARLY"),
                _expr("spot.cohort.direction", "==", "BUY"),
            )
        else:
            up = _condition(
                "UPGRADE",
                "Spot BUY with most volume buying while funding is not above normal.",
                _expr("spot.cohort.direction", "==", "BUY"),
                _expr("spot.cohort.cwb_buy", ">", 0.5),
                _expr("deriv.funding_vs_normal", "!=", "ABOVE"),
            )
    if action == "DONT_CHASE":
        if move == "LIQUIDITY_VACUUM" or band == "HEAVY":
            down = None
        elif anchor is not None:
            down = _condition(
                "DOWNGRADE",
                "Hourly close below the anchor price; WAIT.",
                _expr("price.close_vs_level", "<", str(anchor)),
            )
        else:
            down = None
    result = [c for c in [up, down] if c is not None]
    if action == "WATCH_TO_START" and not has_open_call and anchor is not None:
        result.append(
            _condition(
                "DOWNGRADE",
                "Hourly close below the anchor price; WAIT.",
                _expr("price.close_vs_level", "<", str(anchor)),
            )
        )
    level = prices.invalidation_price
    if level is None and anchor is not None:
        entry = anchor if prices.entry_price is None else prices.entry_price
        level = min(anchor, entry)
    if level is not None:
        result.append(
            _condition(
                "INVALIDATION",
                "Hourly close below the event invalidation level.",
                _expr("price.close_vs_level", "<", str(level)),
            )
        )
    result.append(
        _condition(
            "INVALIDATION",
            "A fatal contradiction invalidates the thesis.",
            _expr("event.contradiction_fatal", "==", True),
        )
    )
    return result
