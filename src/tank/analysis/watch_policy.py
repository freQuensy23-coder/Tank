"""Pure decision monitoring policy over explicit reference prices and typed predicates."""

from typing import Literal

from tank.analysis.watch_contracts import (
    CancelTimedReview,
    ExpireTimedReview,
    FireTimedReview,
    MissingTimedReviewEvent,
    PendingTimedReview,
    TimedReviewAction,
    TimedReviewCondition,
    TimedReviewEvent,
    UnloadedExpiredReviewEvent,
    WatchAll,
    WatchLeaf,
    WatchMetric,
    WatchPersistence,
    WatchPolicyInputs,
    WatchRule,
    WatchScalar,
)
from tank.time_precision import Timestamp
from tank.util import parse_iso_time


def watch_leaf(
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


def watch_rule(
    role: Literal["UPGRADE", "DOWNGRADE", "INVALIDATION"],
    text: str,
    *expressions: WatchLeaf,
) -> WatchRule:
    if not expressions:
        raise ValueError("Watch proposal requires a predicate")
    return WatchRule(
        role=role,
        human_text=text,
        expr=expressions[0] if len(expressions) == 1 else WatchAll(all=list(expressions)),
    )


def build_watch_conditions(context: WatchPolicyInputs) -> list[WatchRule]:
    action, move, band = context.action, context.move, context.band
    prices, has_open_call, wait_data = context.prices, context.has_open_call, context.wait_data
    anchor = prices.anchor_price
    down: WatchRule | None = watch_rule(
        "DOWNGRADE",
        "Spot SELL with most volume selling for two consecutive hourly windows.",
        watch_leaf("spot.cohort.direction", "==", "SELL", count=2),
        watch_leaf("spot.cohort.cwb_sell", ">", 0.5, count=2),
    )
    if has_open_call:
        down = watch_rule(
            "DOWNGRADE",
            "Spot cohort BROAD SELL with most volume selling for two consecutive hourly windows; REDUCE.",
            watch_leaf("spot.cohort.direction", "==", "SELL", count=2),
            watch_leaf("spot.cohort.cwb_sell", ">", 0.5, count=2),
            watch_leaf("spot.classification", "==", "BROAD", count=2),
        )
    up = watch_rule(
        "UPGRADE",
        "Spot BUY with most volume buying for two consecutive hourly windows and native OI not above normal.",
        watch_leaf("spot.cohort.direction", "==", "BUY", count=2),
        watch_leaf("spot.cohort.cwb_buy", ">", 0.5, count=2),
        watch_leaf("deriv.native_oi_vs_normal", "in", ["WITHIN", "BELOW"]),
    )
    if action == "WAIT" and wait_data:
        up = watch_rule(
            "UPGRADE", "Data confidence becomes HIGH; re-grade.", watch_leaf("dc.class", "==", "HIGH")
        )
    elif action == "WAIT":
        up = watch_rule(
            "UPGRADE",
            "Data HIGH and the move class resolves to spot-led buying (aggressor side and price response checked) or organic mixed.",
            watch_leaf("dc.class", "==", "HIGH"),
            watch_leaf("mt.move_class", "in", ["SPOT_LED_ACCUMULATION", "ORGANIC_MIXED"]),
        )
    if action == "DONT_CHASE":
        if move == "LIQUIDITY_VACUUM":
            up = watch_rule(
                "UPGRADE",
                "Depth above half its pre-event level and spot BUY.",
                watch_leaf("liq.depth_p1_vs_pre", ">", 0.5),
                watch_leaf("spot.cohort.direction", "==", "BUY"),
            )
        elif band == "HEAVY":
            up = watch_rule(
                "UPGRADE",
                "Repricing returns to EARLY with spot BUY maintained.",
                watch_leaf("price.repricing_band", "==", "EARLY"),
                watch_leaf("spot.cohort.direction", "==", "BUY"),
            )
        else:
            up = watch_rule(
                "UPGRADE",
                "Spot BUY with most volume buying while funding is not above normal.",
                watch_leaf("spot.cohort.direction", "==", "BUY"),
                watch_leaf("spot.cohort.cwb_buy", ">", 0.5),
                watch_leaf("deriv.funding_vs_normal", "!=", "ABOVE"),
            )
    if action == "DONT_CHASE":
        if move == "LIQUIDITY_VACUUM" or band == "HEAVY":
            down = None
        elif anchor is not None:
            down = watch_rule(
                "DOWNGRADE",
                "Hourly close below the anchor price; WAIT.",
                watch_leaf("price.close_vs_level", "<", str(anchor)),
            )
        else:
            down = None
    result = [c for c in [up, down] if c is not None]
    if action == "WATCH_TO_START" and not has_open_call and anchor is not None:
        result.append(
            watch_rule(
                "DOWNGRADE",
                "Hourly close below the anchor price; WAIT.",
                watch_leaf("price.close_vs_level", "<", str(anchor)),
            )
        )
    level = prices.invalidation_price
    if level is None and anchor is not None:
        entry = anchor if prices.entry_price is None else prices.entry_price
        level = min(anchor, entry)
    if level is not None:
        result.append(
            watch_rule(
                "INVALIDATION",
                "Hourly close below the event invalidation level.",
                watch_leaf("price.close_vs_level", "<", str(level)),
            )
        )
    result.append(
        watch_rule(
            "INVALIDATION",
            "A fatal contradiction invalidates the thesis.",
            watch_leaf("event.contradiction_fatal", "==", True),
        )
    )
    return result


def evaluate_timed_review(
    condition: TimedReviewCondition, *, cutoff: Timestamp, event: TimedReviewEvent
) -> TimedReviewAction:
    current = TimedReviewCondition.model_validate(condition)
    clock = Timestamp.model_validate(cutoff)
    state = type(event).model_validate(event)
    if current.status != "ARMED":
        raise ValueError("Only an armed timed review can be evaluated")
    if state.event_id != current.event_id:
        raise ValueError("Timed review event state must preserve its actual event identity")
    if clock.value is None:
        raise ValueError("Timed review evaluation requires its actual cutoff")
    at = parse_iso_time(clock.value)
    if at >= parse_iso_time(current.expires_at):
        return ExpireTimedReview(status="EXPIRED", reason="EXPIRY_REACHED")
    if isinstance(state, UnloadedExpiredReviewEvent):
        raise ValueError("An unexpired review requires its actual event state")
    if isinstance(state, MissingTimedReviewEvent):
        return CancelTimedReview(status="CANCELLED", reason="EVENT_MISSING")
    if state.lifecycle in {"CLOSED", "MERGED", "INVALIDATED"}:
        return CancelTimedReview(status="CANCELLED", reason="EVENT_TERMINAL")
    if at >= parse_iso_time(current.expr.review_at):
        return FireTimedReview(status="FIRED", reason="REVIEW_DUE")
    return PendingTimedReview(status="ARMED", reason="NOT_DUE")
