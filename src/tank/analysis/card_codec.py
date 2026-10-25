"""Validated projections of persisted analysis records for card presentation."""

from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import JsonValue

from tank.analysis.card_contracts import (
    AcknowledgedPublicationClock,
    ActiveCardDecision,
    AssessmentClass,
    CardAssessment,
    CardBeneficiary,
    CardClaim,
    CardCondition,
    CardDataConfidence,
    CardDataLimit,
    CardDecision,
    CardDerivatives,
    CardEvent,
    CardEvidenceReference,
    CardExclusion,
    CardInputs,
    CardMapping,
    CardMarket,
    CardMarketContext,
    CardOiAttribution,
    CardOiVenue,
    CardOutlook,
    CardRelative,
    CardReturns,
    CardRevenue,
    CardRevenueChartPoint,
    CardRevenuePeriod,
    CardRevenueProtocol,
    CardScenarios,
    CardSpot,
    CardSpotVenue,
    CardSubject,
    CardSynchronizedOi,
    ChartRevenueProtocol,
    EventFreshness,
    EventLifecycle,
    InactiveCardDecision,
    MarketWindow,
    NegativeEventMarker,
    PeriodRevenueProtocol,
    PublicationBaseline,
    PublicationDecisionStatus,
    PublicationEventFacts,
    PublicationGateInputs,
    PublicationNarrative,
    PublicationPolicy,
    PublicationPriorFacts,
    UnavailableRevenueProtocol,
)
from tank.analysis.decision_contracts import (
    Action,
    Bias,
    Confidence,
    ConfidenceLimitation,
    EventState,
    Grade,
    Horizon,
    MoveClass,
    Potential,
    RepricingBand,
    TradeReason,
)
from tank.analysis.statistics_contracts import DecimalBounds
from tank.contracts import DomainModel
from tank.contracts.score import ScoreAssessmentView, ScoreDimensionsView, ScoreDimensionView
from tank.time_precision import propagate_timestamp
from tank.util import canonical_json, parse_iso_time


def _field(row: Mapping[str, JsonValue], key: str) -> JsonValue:
    if key not in row:
        raise ValueError("Missing required card fact: " + key)
    return row[key]


def _record(value: JsonValue, field: str) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise ValueError("Card fact requires an object: " + field)
    return value


def _records(value: JsonValue, field: str) -> list[dict[str, JsonValue]]:
    if not isinstance(value, list):
        raise ValueError("Card fact requires an array: " + field)
    return [_record(row, field) for row in value]


def _text(value: JsonValue, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError("Card fact requires text: " + field)
    return value


def _boolean(value: JsonValue, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError("Card fact requires a boolean: " + field)
    return value


def _optional_flag(row: Mapping[str, JsonValue], field: str) -> bool | None:
    value = row.get(field)
    return _boolean(value, field) if value is not None else None


def _texts(value: JsonValue, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError("Card fact requires a text array: " + field)
    return tuple(_text(entry, field) for entry in value)


def _project[T: DomainModel](schema: type[T], fields: dict[str, JsonValue]) -> T:
    return schema.model_validate_json(canonical_json(fields))


def _labels(value: JsonValue, field: str) -> dict[str, str]:
    return {key: _text(label, field) for key, label in _record(value, field).items()}


def _claims(event: Mapping[str, JsonValue]) -> tuple[CardClaim, ...]:
    rows = _records(_field(event, "claims"), "claims")
    superseded: set[str] = set()
    for row in rows:
        if _boolean(_field(row, "accepted"), "accepted"):
            superseded.update(_texts(row.get("supersedes_claim_ids", []), "supersedes_claim_ids"))
    claims: list[CardClaim] = []
    for row in rows:
        claim_id = _text(_field(row, "claim_id"), "claim_id")
        if not _boolean(_field(row, "accepted"), "accepted") or claim_id in superseded:
            continue
        evidence = tuple(
            _project(CardEvidenceReference, {"item_id": ref.get("item_id"), "url": ref.get("url")})
            for ref in _records(_field(row, "evidence"), "evidence")
        )
        claims.append(
            CardClaim(
                claim_id=claim_id,
                subject=_text(_field(row, "subject"), "subject"),
                predicate=_text(_field(row, "predicate"), "predicate"),
                object_text=_text(_field(row, "object"), "object"),
                verified=_boolean(_field(row, "verified"), "verified"),
                verification_gap=_text(row["verification_missing"], "verification_missing")
                if row.get("verification_missing") is not None
                else None,
                evidence=evidence,
            )
        )
    return tuple(claims)


def _event(row: Mapping[str, JsonValue]) -> CardEvent:
    return CardEvent(
        statement=_text(_field(row, "statement"), "statement"),
        state=_project(CardEventState, {"value": _field(row, "event_state")}).value,
        state_verified=_boolean(_field(row, "state_verified"), "state_verified"),
        certainty=_project(CardEventCertainty, {"value": _field(row, "certainty")}).value,
        freshness=_project(CardEventFreshness, {"value": _field(row, "freshness")}).value,
        public_at=propagate_timestamp(row, "public_at"),
        known_to_us_at=propagate_timestamp(row, "known_to_us_at"),
        claims=_claims(row),
        token_labels=_labels(_field(row, "token_labels"), "token_labels"),
        evidence_labels=_labels(row.get("evidence_labels", {}), "evidence_labels"),
        has_expired_call_fact=_boolean(row.get("call_expired", False), "call_expired"),
    )


class CardEventState(DomainModel):
    value: EventState


class CardEventCertainty(DomainModel):
    value: Confidence


class CardEventFreshness(DomainModel):
    value: EventFreshness


def _assessment(row: Mapping[str, JsonValue]) -> CardAssessment:
    dims = _record(_field(row, "dims"), "dims")

    def dimension(name: str) -> ScoreDimensionView:
        value = _record(_field(dims, name), name)
        return _project(
            ScoreDimensionView,
            {
                "value": _field(value, "value"),
                "rationale": value.get("rationale"),
                "reason": value.get("reason"),
            },
        )

    scores = ScoreAssessmentView(
        dims=ScoreDimensionsView(
            economic_directness=dimension("economic_directness"),
            magnitude=dimension("magnitude"),
            permanence=dimension("permanence"),
            distribution_expansion=dimension("distribution_expansion"),
            mechanical_flows=dimension("mechanical_flows"),
            surprise=dimension("surprise"),
            implementation=dimension("implementation"),
        )
    )
    header = _project(
        CardAssessmentHeader, {"classification": _field(row, "class"), "criterion": _field(row, "criterion")}
    )
    return CardAssessment(classification=header.classification, criterion=header.criterion, scores=scores)


class CardAssessmentHeader(DomainModel):
    classification: AssessmentClass
    criterion: str


def _decision(row: Mapping[str, JsonValue]) -> CardDecision:
    token = _text(_field(row, "token_id"), "token_id")
    if _optional_flag(row, "tracking_ended") is True or _optional_flag(row, "actionable") is False:
        if (
            _optional_flag(row, "removed_from_analysis") is not True
            and _optional_flag(row, "tracking_ended") is not True
        ):
            raise ValueError("An inactive card decision requires its actual removal or expiry state")
        return InactiveCardDecision(
            kind="INACTIVE",
            token_id=token,
            reason="REMOVED_FROM_ANALYSIS"
            if _optional_flag(row, "removed_from_analysis") is True
            else "CALL_EXPIRED",
        )
    outlook = _record(_field(row, "outlook"), "outlook")
    scenarios = _record(_field(outlook, "scenarios"), "scenarios")
    reasons = _record(_field(outlook, "scenario_reasons"), "scenario_reasons")
    scenario = _project(
        CardScenarios,
        {
            "base": _field(scenarios, "base"),
            "upside": _field(scenarios, "upside"),
            "failure": _field(scenarios, "failure"),
            "base_reason": reasons.get("base"),
            "upside_reason": reasons.get("upside"),
            "failure_reason": reasons.get("failure"),
        },
    )
    projected = _project(
        CardOutlookHeader,
        {
            "direction": _field(outlook, "direction"),
            "potential": _field(outlook, "potential"),
            "horizon": _field(outlook, "best_horizon"),
            "confidence": _field(outlook, "confidence"),
            "limitation": _field(outlook, "confidence_limiting"),
        },
    )
    card_outlook = CardOutlook(
        direction=projected.direction,
        potential=projected.potential,
        horizon=projected.horizon,
        confidence=projected.confidence,
        limitation=projected.limitation,
        scenarios=scenario,
    )
    header = _project(
        CardDecisionHeader,
        {
            "action": _field(row, "action"),
            "thesis_grade": _field(row, "thesis_grade"),
            "trade_grade": _field(row, "trade_grade"),
            "raw_trade_grade": _field(row, "trade_rule"),
            "trade_reasons": _field(row, "trade_reasons"),
            "tactical": _field(row, "tactical"),
        },
    )
    conditions = tuple(
        _project(CardCondition, {"role": _field(value, "role"), "human_text": _field(value, "human_text")})
        for value in _records(_field(row, "conditions"), "conditions")
    )
    return ActiveCardDecision(
        kind="ACTIVE",
        token_id=token,
        action=header.action,
        thesis_grade=header.thesis_grade,
        trade_grade=header.trade_grade,
        raw_trade_grade=header.raw_trade_grade,
        trade_reasons=header.trade_reasons,
        tactical=header.tactical,
        outlook=card_outlook,
        conditions=conditions,
    )


class CardOutlookHeader(DomainModel):
    direction: Bias
    potential: Potential
    horizon: Horizon | None
    confidence: Confidence
    limitation: ConfidenceLimitation


class CardDecisionHeader(DomainModel):
    action: Action
    thesis_grade: Grade
    trade_grade: Grade
    raw_trade_grade: Grade
    trade_reasons: tuple[TradeReason, ...]
    tactical: bool


def _market(row: Mapping[str, JsonValue]) -> CardMarket:
    primary = _text(_field(row, "primary_window"), "primary_window")
    relative = _record(_field(row, "relative"), "relative")
    returns: dict[MarketWindow, CardReturns] = {}
    for name, value in _record(_field(relative, "windows"), "relative.windows").items():
        window = _project(CardWindowKey, {"value": name}).value
        point = _record(value, "relative.windows." + name)
        returns[window] = _project(
            CardReturns,
            {
                "raw": _field(point, "r_raw"),
                "btc_adjusted": _field(point, "r_btc_adj"),
                "btc_adjusted_reason": point.get("r_btc_adj_reason"),
            },
        )
    context_inputs = _record(_field(relative, "context_inputs"), "context_inputs")
    normal = _field(context_inputs, "return_normal_range")
    if normal is None:
        normal_bounds = None
    else:
        if not isinstance(normal, list) or len(normal) != 2:
            raise ValueError("The BTC normal range requires two actual bounds")
        normal_bounds = _project(DecimalBounds, {"lower": normal[0], "upper": normal[1]})
    context_header = _project(
        CardContextHeader,
        {
            "state": _field(relative, "market_context"),
            "btc_return_24h": _field(context_inputs, "btc_return_24h"),
        },
    )
    context = CardMarketContext(
        state=context_header.state,
        btc_return_24h=context_header.btc_return_24h,
        normal_return_range=normal_bounds,
    )
    relative_header = _project(CardRelativeHeader, {"repricing": _field(relative, "repricing_band")})
    derivatives = _record(_field(row, "derivatives"), "derivatives")
    venues: list[CardOiVenue] = []
    for venue in _records(_field(derivatives, "per_venue"), "derivatives.per_venue"):
        classification = _field(venue, "oi_class")
        if classification is None:
            continue
        coverage = _record(_field(venue, "coverage"), "derivatives.coverage")
        decomposition = _field(venue, "decomposition")
        attribution = None
        if decomposition is not None:
            measured = _record(decomposition, "decomposition")
            share = _field(measured, "pos_share")
            markup = _field(measured, "markup")
            if share is not None and markup is not None:
                attribution = _project(CardOiAttribution, {"position_share": share, "price_markup": markup})
        header = _project(
            CardOiVenueHeader,
            {
                "venue": _field(coverage, "venue"),
                "classification": classification,
                "native_change": _field(venue, "oi_native_chg_pct"),
            },
        )
        venues.append(
            CardOiVenue(
                venue=header.venue,
                classification=header.classification,
                native_change=header.native_change,
                attribution=attribution,
            )
        )
    sync_wire = _field(derivatives, "synchronized")
    synchronized = None
    if sync_wire is not None:
        sync = _record(sync_wire, "synchronized")
        synchronized = _project(
            CardSynchronizedOi,
            {
                "native_change": _field(sync, "oi_native_chg_pct"),
                "native_vs_normal": _field(sync, "native_oi_vs_normal"),
            },
        )
    derivative_window = _project(CardWindowKey, {"value": _field(derivatives, "window")}).value
    spot = _record(_field(_record(_field(row, "spot"), "spot"), primary), "spot." + primary)
    spot_rows: list[CardSpotVenue] = []
    for venue in _records(_field(spot, "venues"), "spot.venues"):
        if _boolean(_field(venue, "complete"), "complete"):
            spot_rows.append(
                _project(
                    CardSpotVenue,
                    {
                        "venue": _field(venue, "venue"),
                        "sign": _field(venue, "sign"),
                        "volume_share": _field(venue, "volume_share"),
                    },
                )
            )
    excluded = tuple(
        _project(
            CardExclusion,
            {
                "venue": _field(value, "venue"),
                "reason": _field(value, "reason"),
                "detail": value.get("detail"),
                "source_reasons": value.get("source_reasons", []),
                "missing_metrics": value.get("missing_metrics", []),
            },
        )
        for value in _records(
            _field(_record(_field(spot, "coverage"), "spot.coverage"), "excluded"), "excluded"
        )
    )
    divergence = _record(_field(row, "divergence"), "divergence")
    confidence_wire = _record(_field(row, "data_confidence"), "data_confidence")
    limiting_wire = _field(confidence_wire, "limiting")
    limiting = None
    if limiting_wire is not None:
        limiting_record = _record(limiting_wire, "data_confidence.limiting")
        limiting = _project(
            CardDataLimit,
            {"condition": _field(limiting_record, "condition"), "reason": limiting_record.get("reason")},
        )
    confidence = CardDataConfidence(
        classification=_project(CardEventCertainty, {"value": _field(confidence_wire, "class")}).value,
        limiting=limiting,
    )
    market_header = _project(
        CardMarketHeader, {"token_id": _field(row, "token_id"), "move": _field(row, "move_class")}
    )
    return CardMarket(
        token_id=market_header.token_id,
        move=market_header.move,
        absorption_confirmed=_text(_field(divergence, "state"), "divergence.state")
        in {"ABSORPTION_CONFIRMED", "CONFIRMED"},
        unresolved=_texts(_field(divergence, "unresolved"), "divergence.unresolved")
        + _texts(_field(row, "alternatives"), "alternatives"),
        confidence=confidence,
        decisive_facts=_texts(_field(row, "decisive_facts"), "decisive_facts"),
        relative=CardRelative(
            repricing=relative_header.repricing, returns_by_window=returns, context=context
        ),
        derivatives=CardDerivatives(
            window=derivative_window, venues=tuple(venues), synchronized=synchronized
        ),
        spot=CardSpot(
            window=_project(CardWindowKey, {"value": _field(spot, "window")}).value,
            complete_venues=tuple(spot_rows),
            excluded=excluded,
        ),
        holders_revenue=_revenue(_record(_field(row, "supply"), "supply")),
    )


def _revenue(supply: Mapping[str, JsonValue]) -> CardRevenue | None:
    observed = _record(_field(supply, "holders_revenue"), "holders_revenue")
    protocols: list[CardRevenueProtocol] = []
    for protocol in _records(_field(observed, "protocols"), "holders_revenue.protocols"):
        name = _text(_field(protocol, "protocol"), "protocol")
        periods = _records(_field(protocol, "periods"), "periods")
        measured = next((period for period in periods if _field(period, "value") is not None), None)
        if measured is not None:
            protocols.append(
                PeriodRevenueProtocol(
                    kind="PERIOD",
                    protocol=name,
                    unit=_project(CardUsdUnit, {"value": _field(protocol, "unit")}).value,
                    period=_project(
                        CardRevenuePeriod,
                        {
                            "value": _field(measured, "value"),
                            "provider_period": _field(measured, "provider_period"),
                        },
                    ),
                )
            )
            continue
        chart = _field(protocol, "latest_chart_point")
        if chart is not None:
            point = _record(chart, "latest_chart_point")
            protocols.append(
                ChartRevenueProtocol(
                    kind="CHART",
                    protocol=name,
                    unit=_project(CardUsdUnit, {"value": _field(protocol, "unit")}).value,
                    point=_project(
                        CardRevenueChartPoint,
                        {
                            "epoch_seconds": _field(point, "provider_timestamp"),
                            "value": _field(point, "value"),
                        },
                    ),
                )
            )
        else:
            protocols.append(
                _project(
                    UnavailableRevenueProtocol,
                    {"kind": "UNAVAILABLE", "protocol": name, "reason": _field(protocol, "reason")},
                )
            )
    if not any(isinstance(value, (PeriodRevenueProtocol, ChartRevenueProtocol)) for value in protocols):
        return None
    coverage = _record(_field(observed, "coverage"), "holders_revenue.coverage")
    day = _record(_field(coverage, "24h"), "holders_revenue.coverage.24h")
    return CardRevenue(
        protocols=tuple(protocols),
        observed_at=propagate_timestamp(observed, "as_of"),
        stale=_field(observed, "status") == "STALE",
        complete_24h=_boolean(_field(day, "complete"), "coverage.complete"),
    )


class CardWindowKey(DomainModel):
    value: MarketWindow


class CardContextHeader(DomainModel):
    state: Literal["NEUTRAL", "HEADWIND", "STRESS"] | None
    btc_return_24h: Decimal | None


class CardRelativeHeader(DomainModel):
    repricing: RepricingBand | None


class CardOiVenueHeader(DomainModel):
    venue: str
    classification: Literal["REAL_NATIVE", "MIXED", "PRICE_MARKUP"]
    native_change: Decimal | None


class CardMarketHeader(DomainModel):
    token_id: str
    move: MoveClass


class CardUsdUnit(DomainModel):
    value: Literal["USD"]


def _subject(row: Mapping[str, JsonValue], event: CardEvent) -> CardSubject:
    token_wire = _field(row, "token_id")
    token = _text(token_wire, "token_id") if token_wire is not None else None
    outside: Literal["UNREGISTERED_TOKEN", "NO_MAPPED_TOKEN"] | None = None
    evidence = _texts(_field(row, "evidence"), "beneficiary.evidence")
    current_verified = {claim.claim_id for claim in event.claims if claim.verified}
    if evidence and row.get("dependency_ref") is None:
        if token is not None and token not in event.token_labels:
            outside = "UNREGISTERED_TOKEN"
        elif token is None:
            outside = "NO_MAPPED_TOKEN"
    refs = _texts(row.get("integration_state_evidence", []), "integration_state_evidence")
    verified = any(reference in current_verified for reference in refs)
    return _project(
        CardSubject,
        {
            "token_id": token,
            "entity_id": row.get("entity_id"),
            "entity_name": row.get("entity_name"),
            "role": _field(row, "role"),
            "credible": _field(row, "credible"),
            "outside_scope": outside,
            "layer": _field(row, "layer"),
            "integration_state": row.get("integration_state"),
            "integration_verified": verified,
        },
    )


def _mapping(event: Mapping[str, JsonValue]) -> CardMapping:
    wire = _record(_field(event, "beneficiary_map"), "beneficiary_map")
    layers = _record(_field(wire, "layers"), "beneficiary_map.layers")
    fields: dict[str, JsonValue] = {}
    for name, value in layers.items():
        layer = _record(value, "beneficiary_map.layers." + name)
        fields[name] = _field(layer, "note")
    return _project(
        CardMapping,
        {
            "layer_explanations": fields,
            "no_credible_secondary_reason": _field(wire, "no_credible_secondary_reason"),
        },
    )


def card_inputs_from_records(
    event: Mapping[str, JsonValue],
    assessment: Mapping[str, JsonValue],
    beneficiaries: Sequence[Mapping[str, JsonValue]],
    decisions: Sequence[Mapping[str, JsonValue]],
    market_truths: Sequence[Mapping[str, JsonValue]],
    *,
    narrative: PublicationNarrative,
    policy: PublicationPolicy,
) -> CardInputs:
    event_view = _event(event)
    mapping_wire = _record(_field(event, "beneficiary_map"), "beneficiary_map")
    selected: list[CardBeneficiary] = []
    for row in beneficiaries:
        subject = _subject(row, event_view)
        header = _project(
            CardBeneficiaryHeader,
            {"capture": _field(row, "capture_class"), "causal_chain": _field(row, "causal_chain")},
        )
        edge_id = row.get("edge_id")
        least = edge_id is not None and edge_id == _field(mapping_wire, "least_repriced_edge")
        selected.append(
            CardBeneficiary(
                subject=subject,
                capture=header.capture,
                causal_chain=header.causal_chain,
                least_repriced=least,
            )
        )
    other: list[CardSubject] = []
    for row in _records(_field(event, "beneficiary_edges"), "beneficiary_edges"):
        if _field(row, "token_id") is None or _field(row, "tradable") is False:
            other.append(_subject(row, event_view))
    by_token: dict[str, CardDecision] = {}
    for row in decisions:
        decision = _decision(row)
        if decision.token_id in by_token:
            raise ValueError("Duplicate card decision token")
        by_token[decision.token_id] = decision
    markets: dict[str, CardMarket] = {}
    for row in market_truths:
        market = _market(row)
        if market.token_id in markets:
            raise ValueError("Duplicate card market token")
        markets[market.token_id] = market
    return CardInputs(
        narrative=narrative,
        policy=policy,
        event=event_view,
        assessment=_assessment(assessment),
        beneficiaries=tuple(selected),
        other_subjects=tuple(other),
        mapping=_mapping(event),
        decisions_by_token=by_token,
        market_by_token=markets,
    )


class CardBeneficiaryHeader(DomainModel):
    capture: Literal["DIRECT", "INDIRECT", "NARRATIVE_ONLY", "UNKNOWN"]
    causal_chain: str


def _active_fatal_ids(event: Mapping[str, JsonValue], assessment: Mapping[str, JsonValue]) -> frozenset[str]:
    fatal_ids: set[str] = set()
    for row in (event, assessment):
        for contradiction in _records(_field(row, "contradictions"), "contradictions"):
            resolved_at = contradiction.get("resolved_at")
            if resolved_at is not None:
                parse_iso_time(_text(resolved_at, "resolved_at"))
            elif _text(_field(contradiction, "type"), "contradiction.type") == "FATAL":
                fatal_ids.add(_text(_field(contradiction, "contradiction_id"), "contradiction_id"))
    return frozenset(fatal_ids)


def _publication_facts(
    event: Mapping[str, JsonValue], assessment: Mapping[str, JsonValue]
) -> PublicationEventFacts:
    fatal_ids = _active_fatal_ids(event, assessment)
    facts = _project(
        PublicationEventFactsWire,
        {
            "state": _field(event, "event_state"),
            "state_verified": _field(event, "state_verified"),
            "certainty": _field(event, "certainty"),
            "lifecycle": _field(event, "lifecycle"),
            "freshness": _field(event, "freshness"),
            "trigger_freshness": event.get("publication_trigger_freshness"),
            "stage_freshness": event.get("publication_stage_freshness"),
            "negative_marker": event.get("negative_marker"),
            "has_new_milestone_fact": event.get("milestone_resolved", False),
            "has_expired_call_fact": event.get("call_expired", False),
        },
    )
    return PublicationEventFacts(
        state=facts.state,
        state_verified=facts.state_verified,
        certainty=facts.certainty,
        lifecycle=facts.lifecycle,
        freshness=facts.freshness,
        trigger_freshness=facts.trigger_freshness,
        stage_freshness=facts.stage_freshness,
        negative_marker=facts.negative_marker,
        active_fatal_ids=frozenset(fatal_ids),
        has_new_milestone_fact=facts.has_new_milestone_fact,
        has_expired_call_fact=facts.has_expired_call_fact,
    )


def publication_baseline_from_records(
    alert: Mapping[str, JsonValue],
    decisions: Sequence[Mapping[str, JsonValue]],
    *,
    sent_delivery: Mapping[str, JsonValue] | None,
) -> PublicationBaseline:
    context = _record(_field(alert, "card_struct"), "card_struct")
    event = _record(_field(context, "event"), "card_struct.event")
    assessment = _record(_field(context, "assessment"), "card_struct.assessment")
    actions: dict[str, Action] = {}
    for decision in decisions:
        token = _text(_field(decision, "token_id"), "token_id")
        if token in actions:
            raise ValueError("Duplicate baseline decision token")
        actions[token] = _project(CardAction, {"value": _field(decision, "action")}).value
    acknowledged = None
    if sent_delivery is not None:
        if _field(sent_delivery, "status") != "SENT" or _field(sent_delivery, "channel") != "telegram:alerts":
            raise ValueError("Publication cooldown requires an acknowledged Telegram SENT delivery")
        if _field(sent_delivery, "alert_id") != _field(alert, "alert_id") or _field(
            sent_delivery, "version"
        ) != _field(alert, "version"):
            raise ValueError("Publication SENT acknowledgement belongs to a different alert revision")
        acknowledged = AcknowledgedPublicationClock(stamp=propagate_timestamp(sent_delivery, "at"))
    classification = _project(CardAssessmentClass, {"value": _field(assessment, "class")}).value
    return PublicationBaseline(
        assessment_class=classification,
        event=PublicationPriorFacts(
            state=_project(CardEventState, {"value": _field(event, "event_state")}).value,
            state_verified=_boolean(_field(event, "state_verified"), "state_verified"),
            negative_marker=_project(
                PublicationNegativeMarker, {"value": event.get("negative_marker")}
            ).value,
            active_fatal_ids=_active_fatal_ids(event, assessment),
        ),
        actions_by_token=actions,
        acknowledged_send=acknowledged,
    )


def publication_gate_inputs_from_records(
    event: Mapping[str, JsonValue],
    assessment: Mapping[str, JsonValue],
    decisions: Sequence[Mapping[str, JsonValue]],
    *,
    previous: PublicationBaseline | None,
    policy: PublicationPolicy,
    now: datetime,
) -> PublicationGateInputs:
    status: list[PublicationDecisionStatus] = []
    for row in decisions:
        status.append(
            PublicationDecisionStatus(
                token_id=_text(_field(row, "token_id"), "token_id"),
                action=_project(CardAction, {"value": _field(row, "action")}).value,
                active=_optional_flag(row, "tracking_ended") is not True
                and _optional_flag(row, "actionable") is not False,
            )
        )
    return PublicationGateInputs(
        event=_publication_facts(event, assessment),
        assessment_class=_project(CardAssessmentClass, {"value": _field(assessment, "class")}).value,
        decisions=tuple(status),
        previous=previous,
        policy=policy,
        now=now,
    )


class PublicationEventFactsWire(DomainModel):
    state: EventState
    state_verified: bool
    certainty: Confidence
    lifecycle: EventLifecycle
    freshness: EventFreshness
    trigger_freshness: EventFreshness | None
    stage_freshness: EventFreshness | None
    negative_marker: NegativeEventMarker | None
    has_new_milestone_fact: bool
    has_expired_call_fact: bool


class CardAction(DomainModel):
    value: Action


class CardAssessmentClass(DomainModel):
    value: AssessmentClass


class PublicationNegativeMarker(DomainModel):
    value: NegativeEventMarker | None
