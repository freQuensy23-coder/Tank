"""Pure deterministic written-rule calculations and explicit decision materialization."""

from tank.analysis.decision_contracts import (
    Action,
    ActionRule,
    Bias,
    Confidence,
    ConfidenceCaps,
    ConfidenceInput,
    ConfidenceLimitation,
    DecisionCore,
    DecisionDraft,
    DecisionEvaluation,
    DecisionInputs,
    DecisionLiquidity,
    DecisionMaterialization,
    DecisionMilestone,
    DecisionOutlook,
    DecisionRecord,
    DecisionScenarioReasons,
    DecisionScenarios,
    DecisionWatch,
    Grade,
    Horizon,
    Potential,
    TradeReason,
)
from tank.analysis.watch_contracts import (
    WatchCondition,
    WatchPolicyInputs,
    WatchProposal,
    WatchReferencePrices,
    WatchRule,
)
from tank.analysis.watch_policy import build_watch_conditions, watch_leaf, watch_rule
from tank.util import canonical_json

POSITIVE_ACTIONS = frozenset({"START", "WATCH_TO_START", "ADD", "HOLD"})
GRADES: tuple[Grade, ...] = ("A", "B", "C", "D")
BASE_TRADE_REASONS: dict[Grade, TradeReason] = {
    "A": "TRADE_BASE_A",
    "B": "TRADE_BASE_B",
    "C": "TRADE_BASE_C",
    "D": "TRADE_BASE_D",
}


def _watch(rule: WatchRule, *, milestone: DecisionMilestone | None) -> DecisionWatch:
    return DecisionWatch(role=rule.role, human_text=rule.human_text, expr=rule.expr, milestone=milestone)


def _without_event_premise(text: str | None) -> tuple[str | None, str | None]:
    if text is not None and "event" in text.casefold():
        return (None, "no event premise")
    return (text, None)


class DecisionEngine:
    def evaluate(self, inputs: DecisionInputs) -> DecisionEvaluation:
        inputs = DecisionInputs.model_validate(inputs)
        event, assessment, beneficiary, market = (
            inputs.event,
            inputs.assessment,
            inputs.beneficiary,
            inputs.market,
        )
        data, move = (market.data_confidence, market.move)
        relative = market.relative
        band, context = (relative.repricing_band, relative.market_context)
        liquidity = market.liquidity.state if isinstance(market.liquidity, DecisionLiquidity) else None
        confirmed = market.divergence.absorption_confirmed
        direction, breadth = (market.spot.direction, market.spot.breadth)
        selling = direction == "SELL" and market.spot.selling_majority is True
        positioning = market.positioning
        sync = positioning.synchronized
        funding_above = any(value == "ABOVE" for value in positioning.venue_funding)
        oi_above = sync is not None and (
            sync.native_beyond is True
            and sync.native_change is not None
            and (sync.native_change > 0)
            or sync.native_vs_normal == "ABOVE"
        )
        crowded = funding_above and oi_above
        role, capture = (beneficiary.role, beneficiary.capture_class)
        cls = assessment.classification
        shock = cls == "STRUCTURAL_SHOCK"
        fatal = event.fatal_contradiction or event.invalidated
        unresolved = event.unresolved_contradiction
        credible = (
            beneficiary.thesis_link_credible is True if role == "LOSER" else beneficiary.credible
        ) and role in {"PRIMARY", "SECONDARY", "LOSER"}
        state, verified = (event.state, event.state_verified)
        later = state in {"APPROVED", "SIGNED", "DEPLOYED", "LIVE", "MONEY_FLOWING"}
        thesis: Grade
        trade: Grade
        bias: Bias
        potential: Potential
        if fatal or not credible:
            thesis = "D"
        elif capture in {"INDIRECT", "NARRATIVE_ONLY", "UNKNOWN"}:
            thesis = "C"
        elif (
            capture == "DIRECT"
            and role in {"PRIMARY", "SECONDARY"}
            and (cls in {"MAJOR", "STRUCTURAL_SHOCK"})
            and (role == "PRIMARY" or beneficiary.exposure_score in {8, 10})
            and later
            and (verified is True)
            and (not unresolved)
        ):
            thesis = "A"
        else:
            thesis = "B"
        if (
            data == "LOW"
            or move in {"DISTRIBUTION", "LIQUIDITY_VACUUM"}
            or (role != "LOSER" and selling and (move != "PASSIVE_SPOT_ABSORPTION"))
        ):
            trade = "D"
        elif (
            move
            in {
                "PERP_LED_CHASE",
                "SHORT_SQUEEZE",
                "PRICE_MARKUP_FAKE_OI",
                "REAL_MARKET_FRAGMENTATION",
                "UNKNOWN",
            }
            or (move == "SELL_SIDE_ABSORPTION" and market.seller_exhaustion is not True)
            or band == "HEAVY"
            or crowded
        ):
            trade = "C"
        elif (
            (
                move in {"SPOT_LED_ACCUMULATION", "ORGANIC_MIXED"}
                or (move == "PASSIVE_SPOT_ABSORPTION" and confirmed)
            )
            and band in {"NEGATIVE", "UNPRICED", "EARLY"}
            and (liquidity == "NORMAL")
            and (data == "HIGH")
        ):
            trade = "A"
        else:
            trade = "B"
        raw_trade = trade
        resilience = (
            shock
            and relative.event_btc_adjusted_return is not None
            and (relative.event_btc_adjusted_return > 0)
        )
        if context in {"HEADWIND", "STRESS"} and (not resilience):
            trade = GRADES[min(3, GRADES.index(trade) + 1)]
        if data == "LOW" or (move == "UNKNOWN" and capture in {"NARRATIVE_ONLY", "UNKNOWN"} and (not shock)):
            bias = "UNKNOWN"
        elif (
            move in {"SHORT_SQUEEZE", "LIQUIDITY_VACUUM"}
            or (role != "LOSER" and (move == "DISTRIBUTION" or (direction == "SELL" and breadth == "BROAD")))
            or (role == "LOSER" and move == "SPOT_LED_ACCUMULATION")
        ):
            bias = "TWO_SIDED"
        elif role == "LOSER" or (move == "DISTRIBUTION" and direction == "SELL"):
            bias = "BEARISH"
        elif credible and band != "HEAVY" and (context != "STRESS" or shock):
            bias = "BULLISH"
        else:
            bias = "NEUTRAL"
        early = band in {"UNPRICED", "EARLY"}
        compressed = (
            isinstance(market.liquidity, DecisionLiquidity) and market.liquidity.compression == "COMPRESSED"
        )
        squeeze = oi_above and any(value == "BELOW" for value in positioning.venue_funding)
        if band == "HEAVY" or crowded:
            potential = "LOW"
        elif shock and early and (liquidity == "THIN" or compressed):
            potential = "EXPLOSIVE"
        elif shock and early or (cls == "MAJOR" and early and (liquidity == "THIN" or compressed or squeeze)):
            potential = "HIGH"
        elif cls in {"MAJOR", "STRUCTURAL_SHOCK"}:
            potential = "MEDIUM"
        else:
            potential = "LOW"
        impact = assessment.impact_start
        horizon: Horizon | None
        if move in {"SHORT_SQUEEZE", "LIQUIDITY_VACUUM"}:
            horizon, reason = ("H1", "market override")
        elif move == "PERP_LED_CHASE":
            horizon, reason = ("H4", "market override")
        elif impact == "UNKNOWN":
            horizon, reason = (None, "impact start unavailable")
        else:
            if impact == "NOW":
                horizon = "H12" if move in {"SPOT_LED_ACCUMULATION", "ORGANIC_MIXED"} else "H4"
            elif impact == "DAYS":
                horizon = "H24" if assessment.all_dependencies_within_24h is True else "D3"
            else:
                horizon = "D7"
            reason = "impact start: " + impact
        roles = inputs.triggers.roles
        action: Action
        rule: ActionRule
        if inputs.open_call is not None:
            if fatal or "INVALIDATION" in roles:
                action, rule = ("EXIT", "B1")
            elif trade == "D" or "DOWNGRADE" in roles:
                action, rule = ("REDUCE", "B2")
            elif "UPGRADE" in roles and trade == "A" and (thesis in {"A", "B"}) and (band != "HEAVY"):
                action, rule = ("ADD", "B3")
            else:
                action, rule = ("HOLD", "B4")
        elif data == "LOW":
            action, rule = ("WAIT", "A1")
        elif shock and role == "PRIMARY" and (band == "HEAVY"):
            action, rule = ("DONT_CHASE", "A0")
        elif role == "LOSER" or bias == "BEARISH":
            action, rule = ("WAIT", "A2")
        elif thesis == "D" or fatal:
            action, rule = ("WAIT", "A3")
        elif inputs.triggers.anchor_lost:
            action, rule = ("WAIT", "WATCH_ANCHOR_LOST")
        elif inputs.triggers.sell_downgrade:
            action, rule = ("WAIT", "WATCH_SELL_DOWNGRADE")
        elif trade == "A":
            if thesis in {"A", "B"}:
                action, rule = ("WATCH_TO_START", "A6") if unresolved else ("START", "A5")
            elif thesis == "C" and potential in {"HIGH", "EXPLOSIVE"}:
                action, rule = ("START", "A7")
            else:
                action, rule = ("WAIT", "A8")
        elif trade == "B":
            action, rule = ("WATCH_TO_START", "A9") if thesis in {"A", "B"} else ("WAIT", "A10")
        elif thesis in {"A", "B"} and (
            move in {"PERP_LED_CHASE", "SHORT_SQUEEZE", "PRICE_MARKUP_FAKE_OI", "LIQUIDITY_VACUUM"}
            or band == "HEAVY"
        ):
            action, rule = ("DONT_CHASE", "A11")
        else:
            action, rule = ("WAIT", "A12")
        if action == "START" and (data != "HIGH" or unresolved):
            action, rule = (
                "WATCH_TO_START",
                "START_HIGH_DATA_GUARD" if data != "HIGH" else "START_UNRESOLVED_GUARD",
            )
        references = WatchReferencePrices(
            anchor_price=relative.anchor_price,
            entry_price=relative.entry_price,
            invalidation_price=inputs.open_call.invalidation_price if inputs.open_call is not None else None,
        )
        watches = [
            _watch(watch, milestone=None)
            for watch in build_watch_conditions(
                WatchPolicyInputs(
                    action=action,
                    move=move,
                    band=band if band is not None else "UNKNOWN",
                    prices=references,
                    has_open_call=inputs.open_call is not None,
                    wait_data=rule == "A1",
                )
            )
        ]
        if event.milestone is not None:
            watches.extend(
                _watch(watch, milestone=event.milestone)
                for watch in (
                    watch_rule(
                        "UPGRADE",
                        "Milestone verified; re-score the event.",
                        watch_leaf("milestone.outcome", "==", "VERIFIED"),
                    ),
                    watch_rule(
                        "DOWNGRADE",
                        "Milestone failed or missed its window; re-score, fatal when thesis-dependent.",
                        watch_leaf("milestone.outcome", "in", ["FAILED", "MISSED"]),
                    ),
                )
            )
        certainty = event.certainty
        class_conf: Confidence = (
            "LOW"
            if move == "UNKNOWN"
            else "MEDIUM"
            if market.alternatives or market.divergence.unresolved
            else market.class_confidence
        )
        rank: dict[Confidence, int] = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
        confidence = min((data, certainty, class_conf), key=lambda value: rank[value])
        caps: tuple[tuple[ConfidenceInput, Confidence], ...] = (
            ("data", data),
            ("certainty", certainty),
            ("class", class_conf),
        )
        limiting_input = (
            None if confidence == "HIGH" else next((name for name, value in caps if value == confidence))
        )
        alternatives = market.divergence.unresolved + market.alternatives
        confidence_limiting = ConfidenceLimitation(
            input=limiting_input,
            text=None
            if limiting_input in {None, "data"}
            else f"Event certainty {certainty}: not confirmed by a HIGH-certainty source"
            if limiting_input == "certainty"
            else "Move class unresolved: no class eligible (UNKNOWN)"
            if move == "UNKNOWN"
            else "Move class has unresolved alternatives: " + ", ".join(alternatives)
            if alternatives
            else f"Move class confidence {class_conf}",
        )
        if data == "LOW":
            scenarios = DecisionScenarios(
                base="UNKNOWN: market evidence incomplete.",
                upside="UNKNOWN until class and data resolve.",
                failure="Thesis invalid if the event is denied.",
            )
        elif move == "UNKNOWN":
            complete = market.spot.complete_venues
            venues = " on complete venues" if complete is not None and complete > 0 else ""
            flow = (
                f"spot flow {direction} and {breadth}"
                if direction is not None
                and direction != "UNKNOWN"
                and (breadth is not None)
                and (breadth != "UNKNOWN")
                else "spot flow not classified"
            )
            scenarios = DecisionScenarios(
                base=f"No classifiable move yet: {flow} in the primary window{venues}.",
                upside="Upgrade if spot turns BUY and BROAD with native OI within its normal range (spot-led buying).",
                failure="Thesis invalid if the event is denied.",
            )
        elif move == "PERP_LED_CHASE":
            scenarios = DecisionScenarios(
                base="Chase risk persists while spot does not confirm.",
                upside="Move gains support if spot turns BUY and funding normalizes.",
                failure="Move unwinds if price loses the event anchor.",
            )
        elif move == "PRICE_MARKUP_FAKE_OI":
            scenarios = (
                DecisionScenarios(
                    base="USD OI rose on price markup, not new positions; spot buying carries the move.",
                    upside="Move gains support if native OI and spot volume rise with spot BUY.",
                    failure="Move unwinds if price loses the event anchor.",
                )
                if direction == "BUY"
                else DecisionScenarios(
                    base="USD OI rose on price markup, not new positions; spot does not confirm the move.",
                    upside="Move gains support if spot turns BUY with native OI rising.",
                    failure="Move unwinds if price loses the event anchor.",
                )
            )
        elif move == "SHORT_SQUEEZE":
            scenarios = DecisionScenarios(
                base="Squeeze persists while short covering dominates.",
                upside="Move extends if spot demand replaces covering.",
                failure="Move fades if covering ends without spot demand.",
            )
        elif move == "LIQUIDITY_VACUUM":
            scenarios = DecisionScenarios(
                base="Thin depth keeps price unstable.",
                upside="Move gains support if depth recovers with spot BUY.",
                failure="Move reverses if depth stays depleted and demand fades.",
            )
        elif move == "REAL_MARKET_FRAGMENTATION":
            scenarios = DecisionScenarios(
                base="Venue disagreement persists while gaps remain.",
                upside="Direction resolves if complete venues converge with spot BUY.",
                failure="Event move fades if venues converge lower.",
            )
        elif move == "PASSIVE_SPOT_ABSORPTION":
            scenarios = DecisionScenarios(
                base="Divergence persists while price resists spot SELL.",
                upside="Move strengthens if refill persists and spot turns BUY.",
                failure="Move fails if depth depletes and price loses anchor.",
            )
        elif move == "SELL_SIDE_ABSORPTION":
            scenarios = DecisionScenarios(
                base="Supply caps price while aggressive buyers meet refilling asks.",
                upside="Move extends if sellers exhaust and price responds.",
                failure="Move fades if supply persists and demand weakens.",
            )
        elif bias == "BEARISH" or move == "DISTRIBUTION":
            scenarios = DecisionScenarios(
                base="Negative thesis persists with spot SELL.",
                upside="Recovery requires spot BUY and resolved negative evidence.",
                failure="Negative thesis fails if spot demand strengthens and event reverses.",
            )
        else:
            scenarios = DecisionScenarios(
                base="Event move persists while spot supports price.",
                upside="Move extends if spot demand strengthens without crowding.",
                failure="Thesis fails on denial or loss of the event anchor.",
            )
        scenario_reasons = DecisionScenarioReasons(base=None, upside=None, failure=None)
        if not event.has_premise:
            base, base_reason = _without_event_premise(scenarios.base)
            upside, upside_reason = _without_event_premise(scenarios.upside)
            failure, failure_reason = _without_event_premise(scenarios.failure)
            scenarios = DecisionScenarios(base=base, upside=upside, failure=failure)
            scenario_reasons = DecisionScenarioReasons(
                base=base_reason, upside=upside_reason, failure=failure_reason
            )
        reasons: list[TradeReason] = [BASE_TRADE_REASONS[raw_trade]]
        if trade != raw_trade and context == "HEADWIND":
            reasons.append("CONTEXT_HEADWIND")
        elif trade != raw_trade and context == "STRESS":
            reasons.append("CONTEXT_STRESS")
        return DecisionEvaluation(
            thesis_grade=thesis,
            thesis_rule=thesis,
            trade_grade=trade,
            trade_rule=raw_trade,
            trade_reasons=reasons,
            action=action,
            action_rule=rule,
            tactical=thesis == "C" and action in POSITIVE_ACTIONS,
            investigate=move in {"UNKNOWN", "REAL_MARKET_FRAGMENTATION"},
            confidence_limiting=confidence_limiting,
            conditions=watches,
            outlook=DecisionOutlook(
                direction=bias,
                direction_rule=bias,
                potential=potential,
                potential_rule=potential,
                best_horizon=horizon,
                horizon_reason=reason,
                confidence=confidence,
                caps=ConfidenceCaps(data=data, event=certainty, market_truth=class_conf),
                confidence_limiting=confidence_limiting,
                tactical=thesis == "C" and bias == "BULLISH",
                scenarios=scenarios,
                scenario_reasons=scenario_reasons,
            ),
        )


def _core(evaluation: DecisionCore) -> DecisionCore:
    return DecisionCore(
        thesis_grade=evaluation.thesis_grade,
        thesis_rule=evaluation.thesis_rule,
        trade_grade=evaluation.trade_grade,
        trade_rule=evaluation.trade_rule,
        trade_reasons=evaluation.trade_reasons,
        action=evaluation.action,
        action_rule=evaluation.action_rule,
        tactical=evaluation.tactical,
        investigate=evaluation.investigate,
        confidence_limiting=evaluation.confidence_limiting,
        outlook=evaluation.outlook,
    )


def materialize_decision(evaluation: DecisionEvaluation, context: DecisionMaterialization) -> DecisionDraft:
    evaluation = DecisionEvaluation.model_validate(evaluation)
    context = DecisionMaterialization.model_validate(context)
    if len(evaluation.conditions) != len(context.condition_ids):
        raise ValueError("Every decision watch requires its explicit allocated identity")
    proposals: list[WatchProposal] = []
    for watch, condition_id in zip(evaluation.conditions, context.condition_ids, strict=True):
        if watch.milestone is None:
            proposal = WatchProposal(
                cond_id=condition_id,
                role=watch.role,
                human_text=watch.human_text,
                expr=watch.expr,
                status="ARMED",
            )
        else:
            proposal = WatchProposal(
                cond_id=condition_id,
                role=watch.role,
                human_text=watch.human_text,
                expr=watch.expr,
                status="ARMED",
                milestone_id=watch.milestone.reference,
            )
        proposals.append(proposal)
    return DecisionDraft(calculation=_core(evaluation), metadata=context.metadata, proposals=proposals)


def finalize_decision(
    draft: DecisionDraft,
    *,
    conditions: list[WatchCondition],
    conditions_shown: tuple[str, ...],
    outlook_id: str,
) -> DecisionRecord:
    draft = DecisionDraft.model_validate(draft)
    for proposed in draft.proposals:
        if not any(
            condition.role == proposed.role
            and canonical_json(condition.expr) == canonical_json(proposed.expr)
            for condition in conditions
        ):
            raise ValueError("Reconciliation omitted a required decision watch predicate")
    return DecisionRecord(
        calculation=draft.calculation,
        metadata=draft.metadata,
        conditions=conditions,
        conditions_shown=conditions_shown,
        outlook_id=outlook_id,
    )
