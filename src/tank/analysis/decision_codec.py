"""Validate concrete stored decision inputs and preserve their wire representation."""

from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import Field, JsonValue, field_validator

from tank.analysis.decision_contracts import (
    ActiveDecisionTracking,
    DecisionAssessment,
    DecisionBeneficiary,
    DecisionCallContext,
    DecisionCore,
    DecisionDivergence,
    DecisionDraft,
    DecisionEvent,
    DecisionIdentity,
    DecisionInputs,
    DecisionLiquidity,
    DecisionLiquidityInput,
    DecisionMarket,
    DecisionMetadata,
    DecisionMilestone,
    DecisionOi,
    DecisionOpenCall,
    DecisionOutlook,
    DecisionPositioning,
    DecisionPrices,
    DecisionProvenance,
    DecisionRecord,
    DecisionRelative,
    DecisionScenarioReasons,
    DecisionSpot,
    DecisionStatusWriter,
    DecisionTracking,
    DecisionTriggers,
    EndedDecisionTracking,
    OutlookConditionRefs,
    ReconciledOutlook,
    UnavailableDecisionLiquidity,
)
from tank.analysis.watch_contracts import WatchAll, WatchCondition, WatchLeaf, WatchProposal
from tank.contracts import DomainModel
from tank.contracts.json import JsonDocument, decode_json
from tank.contracts.provenance import GenerationProvenance, ResolvedVersionIdentity
from tank.time_precision import Timestamp, propagate_timestamp
from tank.util import parse_iso_time


def _required(record: Mapping[str, JsonValue], key: str) -> JsonValue:
    if key not in record:
        raise ValueError(f"Missing required decision input: {key}")
    return record[key]


def _object(value: JsonValue, field: str) -> dict[str, JsonValue]:
    if not isinstance(value, dict):
        raise ValueError(f"Decision input requires an object: {field}")
    return value


def _array(value: JsonValue, field: str) -> list[JsonValue]:
    if not isinstance(value, list):
        raise ValueError(f"Decision input requires an array: {field}")
    return value


def _text(value: JsonValue, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Decision input requires nonempty text: {field}")
    return value


def _decimal(value: JsonValue | Decimal, field: str) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (Decimal, str, int, float)):
        raise ValueError(f"Decision input requires a finite decimal: {field}")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Decision input requires a finite decimal: {field}") from exc
    if not number.is_finite():
        raise ValueError(f"Decision input requires a finite decimal: {field}")
    return number


def _event(record: Mapping[str, JsonValue]) -> DecisionEvent:
    contradictions = [
        _object(value, "contradictions")
        for value in _array(_required(record, "contradictions"), "contradictions")
    ]
    for value in contradictions:
        resolved = value.get("resolved_at")
        if resolved is not None:
            parse_iso_time(_text(resolved, "contradiction.resolved_at"))
    active = [value for value in contradictions if value.get("resolved_at") is None]
    kinds = [_text(_required(value, "type"), "contradiction.type") for value in active]
    lifecycle = _text(_required(record, "lifecycle"), "lifecycle")
    if lifecycle not in {
        "DETECTED",
        "SCORED",
        "TRACKED",
        "ANALYZING",
        "MONITORING",
        "INVALIDATED",
        "CLOSED",
        "MERGED",
    }:
        raise ValueError("Unsupported decision event lifecycle")
    milestone_value = _required(record, "next_milestone")
    milestone: DecisionMilestone | None = None
    if milestone_value is not None:
        value = _object(milestone_value, "next_milestone")
        _text(_required(value, "kind"), "next_milestone.kind")
        milestone = DecisionMilestone(reference=None)
    return DecisionEvent.model_validate(
        {
            "has_premise": True,
            "state": _required(record, "event_state"),
            "state_verified": _required(record, "state_verified"),
            "invalidated": lifecycle == "INVALIDATED",
            "fatal_contradiction": "FATAL" in kinds,
            "unresolved_contradiction": "UNRESOLVED" in kinds,
            "certainty": _required(record, "certainty"),
            "milestone": milestone,
        }
    )


def _assessment(record: Mapping[str, JsonValue]) -> DecisionAssessment:
    horizon = _object(_required(record, "impact_horizon"), "impact_horizon")
    return DecisionAssessment.model_validate(
        {
            "classification": _required(record, "class"),
            "impact_start": _required(horizon, "impact_start"),
            "all_dependencies_within_24h": record.get("all_dependencies_within_24h"),
        }
    )


def _beneficiary(record: Mapping[str, JsonValue]) -> DecisionBeneficiary:
    role = _required(record, "role")
    return DecisionBeneficiary.model_validate(
        {
            "role": role,
            "capture_class": _required(record, "capture_class"),
            "credible": _required(record, "credible"),
            "thesis_link_credible": (
                _required(record, "thesis_link_credible")
                if role == "LOSER"
                else record.get("thesis_link_credible")
            ),
            "exposure_score": _required(record, "exposure_score"),
        }
    )


def _relative(record: Mapping[str, JsonValue]) -> DecisionRelative:
    anchor = _object(_required(record, "anchor"), "relative.anchor")
    windows = _object(_required(record, "windows"), "relative.windows")
    event = _object(_required(windows, "EVENT"), "relative.windows.EVENT")
    return DecisionRelative.model_validate(
        {
            "repricing_band": _required(record, "repricing_band"),
            "market_context": _required(record, "market_context"),
            "event_btc_adjusted_return": _decimal(_required(event, "r_btc_adj"), "r_btc_adj"),
            "anchor_price": _decimal(_required(anchor, "ref_price"), "anchor.ref_price"),
            "entry_price": _decimal(_required(record, "entry_ref_price"), "entry_ref_price"),
        }
    )


class _UnavailableLiquidityWire(DomainModel):
    window: Literal["EVENT"]
    start: None
    cutoff: str
    reason: Literal["INCOMPLETE"]

    @field_validator("cutoff")
    @classmethod
    def actual_cutoff(cls, value: str) -> str:
        parse_iso_time(value)
        return value


def _liquidity(record: Mapping[str, JsonValue], primary_window: str) -> DecisionLiquidityInput:
    if "liquidity_state" not in record and "vol_compression" not in record:
        unavailable = _UnavailableLiquidityWire.model_validate(
            {
                "window": _required(record, "window"),
                "start": _required(record, "start"),
                "cutoff": _required(record, "cutoff"),
                "reason": _required(record, "reason"),
            }
        )
        if unavailable.window != primary_window:
            raise ValueError("Unavailable liquidity belongs to a different analysis window")
        return UnavailableDecisionLiquidity(reason=unavailable.reason)
    compression = _object(_required(record, "vol_compression"), "vol_compression")
    return DecisionLiquidity.model_validate(
        {
            "state": _required(record, "liquidity_state"),
            "compression": _required(compression, "state"),
        }
    )


def _spot(record: Mapping[str, JsonValue]) -> DecisionSpot:
    declared = record.get("exact_majority")
    majority: JsonValue = None
    if declared is not None:
        majority = _required(_object(declared, "exact_majority"), "sell")
    else:
        breadth = _object(_required(record, "cw_breadth"), "cw_breadth")
        fraction = _required(breadth, "sell")
        exact = _decimal(fraction, "cw_breadth.sell")
        if exact is not None and not Decimal(0) <= exact <= Decimal(1):
            raise ValueError("Spot sell breadth must be a fraction between zero and one")
        if exact is not None and not (isinstance(fraction, float) and exact == Decimal("0.5")):
            majority = exact > Decimal("0.5")
    coverage = _object(_required(record, "coverage"), "spot.coverage")
    return DecisionSpot.model_validate(
        {
            "direction": _required(record, "direction"),
            "breadth": _required(record, "classification"),
            "selling_majority": majority,
            "complete_venues": _required(coverage, "n_complete"),
        }
    )


def _positioning(record: Mapping[str, JsonValue]) -> DecisionPositioning:
    windows = _object(_required(record, "windows"), "derivatives.windows")
    positioning = _object(_required(windows, "W4H"), "derivatives.windows.W4H")
    raw_sync = _required(positioning, "synchronized")
    synchronized: DecisionOi | None = None
    if raw_sync is not None:
        sync = _object(raw_sync, "synchronized")
        synchronized = DecisionOi.model_validate(
            {
                "native_beyond": _required(sync, "native_beyond"),
                "native_change": _decimal(_required(sync, "oi_native_chg_pct"), "oi_native_chg_pct"),
                "native_vs_normal": _required(sync, "native_oi_vs_normal"),
            }
        )
    per_venue = [
        _object(value, "per_venue") for value in _array(_required(positioning, "per_venue"), "per_venue")
    ]
    return DecisionPositioning.model_validate(
        {
            "synchronized": synchronized,
            "venue_funding": tuple(_required(value, "funding_vs_normal") for value in per_venue),
        }
    )


def _market(record: Mapping[str, JsonValue]) -> DecisionMarket:
    confidence = _object(_required(record, "data_confidence"), "data_confidence")
    primary = _text(_required(record, "primary_window"), "primary_window")
    if primary not in {"W15M", "W1H", "W4H", "W24H", "EVENT"}:
        raise ValueError("Unsupported decision primary window")
    spot = _object(_required(record, "spot"), "spot")
    divergence = _object(_required(record, "divergence"), "divergence")
    state = _text(_required(divergence, "state"), "divergence.state")
    return DecisionMarket.model_validate(
        {
            "data_confidence": _required(confidence, "class"),
            "class_confidence": _required(record, "class_confidence"),
            "move": _required(record, "move_class"),
            "relative": _relative(_object(_required(record, "relative"), "relative")),
            "liquidity": _liquidity(_object(_required(record, "liquidity"), "liquidity"), primary),
            "spot": _spot(_object(_required(spot, primary), "primary spot")),
            "positioning": _positioning(_object(_required(record, "derivatives"), "derivatives")),
            "divergence": DecisionDivergence.model_validate(
                {
                    "absorption_confirmed": state in {"ABSORPTION_CONFIRMED", "CONFIRMED"},
                    "unresolved": tuple(_array(_required(divergence, "unresolved"), "unresolved")),
                }
            ),
            "seller_exhaustion": record.get("seller_exhaustion"),
            "alternatives": tuple(_array(_required(record, "alternatives"), "alternatives")),
        }
    )


def _triggers(conditions: Sequence[WatchCondition]) -> DecisionTriggers:
    fired = [WatchCondition.model_validate(value) for value in conditions if value.status == "FIRED"]
    roles = frozenset(value.role for value in fired if value.role in {"UPGRADE", "DOWNGRADE", "INVALIDATION"})
    anchor_lost = any(
        value.role in {"DOWNGRADE", "INVALIDATION"}
        and isinstance(value.expr, WatchLeaf)
        and value.expr.metric == "price.close_vs_level"
        and value.expr.op == "<"
        for value in fired
    )
    sell_downgrade = any(
        value.role == "DOWNGRADE"
        and isinstance(value.expr, WatchAll)
        and any(
            isinstance(expression, WatchLeaf)
            and expression.metric == "spot.cohort.direction"
            and expression.value == "SELL"
            for expression in value.expr.all
        )
        for value in fired
    )
    return DecisionTriggers.model_validate(
        {"roles": roles, "anchor_lost": anchor_lost, "sell_downgrade": sell_downgrade}
    )


def decision_inputs_from_records(
    event: Mapping[str, JsonValue],
    assessment: Mapping[str, JsonValue],
    beneficiary: Mapping[str, JsonValue],
    market_truth: Mapping[str, JsonValue],
    *,
    open_call: DecisionCallContext | None,
    conditions: Sequence[WatchCondition],
) -> DecisionInputs:
    for value in (event, assessment, beneficiary, market_truth):
        JsonDocument.validate_value(dict(value))
    event_id = _text(_required(event, "event_id"), "event_id")
    token_id = _text(_required(beneficiary, "token_id"), "token_id")
    for value in (assessment, beneficiary, market_truth):
        if _required(value, "event_id") != event_id:
            raise ValueError("Decision input belongs to a different event")
    if _required(market_truth, "token_id") != token_id:
        raise ValueError("Decision market truth belongs to a different token")
    call: DecisionOpenCall | None = None
    if open_call is not None:
        open_call = DecisionCallContext.model_validate(open_call)
        if open_call.event_id != event_id or open_call.token_id != token_id:
            raise ValueError("Decision open call belongs to a different event or token")
        if open_call.status != "OPEN":
            raise ValueError("Decision management requires an actually open call")
        call = DecisionOpenCall(invalidation_price=open_call.invalidation_price)
    for condition in conditions:
        if condition.event_id != event_id or condition.token_id != token_id:
            raise ValueError("Decision watch belongs to a different event or token")
    return DecisionInputs(
        event=_event(event),
        assessment=_assessment(assessment),
        beneficiary=_beneficiary(beneficiary),
        market=_market(market_truth),
        open_call=call,
        triggers=_triggers(conditions),
    )


def decision_call_from_record(record: Mapping[str, JsonValue]) -> DecisionCallContext:
    return DecisionCallContext(
        call_id=_text(_required(record, "call_id"), "call_id"),
        event_id=_text(_required(record, "event_id"), "event_id"),
        token_id=_text(_required(record, "token_id"), "token_id"),
        status=_text_open_status(_required(record, "status")),
        opened_at=propagate_timestamp(record, "opened_at"),
        invalidation_price=_required_call_price(record),
    )


def _text_open_status(value: JsonValue) -> Literal["OPEN"]:
    if value != "OPEN":
        raise ValueError("Decision call context requires an actually open call")
    return "OPEN"


def _required_call_price(record: Mapping[str, JsonValue]) -> Decimal:
    value = _decimal(_required(record, "invalidation_price"), "invalidation_price")
    if value is None:
        raise ValueError("An open call requires its actual invalidation price")
    return value


class _OutlookRefsWire(DomainModel):
    upgrade: list[str]
    downgrade: list[str]

    def domain(self) -> OutlookConditionRefs:
        return OutlookConditionRefs(upgrade=tuple(self.upgrade), downgrade=tuple(self.downgrade))


class _DecisionOutlookWire(DecisionOutlook):
    conditions: _OutlookRefsWire


def _pure_outlook(value: DecisionOutlook) -> DecisionOutlook:
    return DecisionOutlook(
        direction=value.direction,
        direction_rule=value.direction_rule,
        potential=value.potential,
        potential_rule=value.potential_rule,
        best_horizon=value.best_horizon,
        horizon_reason=value.horizon_reason,
        confidence=value.confidence,
        caps=value.caps,
        confidence_limiting=value.confidence_limiting,
        tactical=value.tactical,
        scenarios=value.scenarios,
        scenario_reasons=value.scenario_reasons,
    )


class _DecisionWireCore(DecisionCore, GenerationProvenance, ResolvedVersionIdentity):
    dec_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    cutoff: str
    cutoff_prec: str | None
    cutoff_prec_reason: str | None
    cutoff_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    calculated_at: str
    calculated_at_prec: str | None
    calculated_at_prec_reason: str | None
    calculated_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    market_truth_id: str = Field(min_length=1)
    mta_id: str = Field(min_length=1)
    provenance: DecisionProvenance
    contradictions: list[str]
    thresholds_version: str = Field(min_length=1)
    management_only: bool | None = None
    actionable: bool | None = None
    removed_from_analysis: bool | None = None
    entry_ref_price: Decimal | None = None
    invalidation_price: Decimal | None = None
    tracking_ended: bool | None = None
    previous_decision_id: str | None = None
    expired_call_ids: list[str] | None = None
    tracking_recorded_at: str | None = None
    tracking_recorded_at_prec: str | None = None
    tracking_recorded_at_prec_reason: str | None = None
    tracking_recorded_at_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None
    tracking_writer: DecisionStatusWriter | None = None

    @field_validator("entry_ref_price", "invalidation_price", mode="before")
    @classmethod
    def exact_wire_price(cls, value: JsonValue | Decimal) -> Decimal | None:
        return _decimal(value, "decision price")

    @field_validator("outlook", mode="before")
    @classmethod
    def sparse_scenario_reasons(cls, value: JsonValue | DecisionOutlook) -> DecisionOutlook:
        if isinstance(value, DecisionOutlook):
            return DecisionOutlook.model_validate(value)
        payload = dict(_object(value, "outlook"))
        reasons = dict(_object(_required(payload, "scenario_reasons"), "scenario_reasons"))
        for name in ("base", "upside", "failure"):
            if name not in reasons:
                reasons[name] = None
        payload["scenario_reasons"] = reasons
        return DecisionOutlook.model_validate(payload)


class _DecisionDraftWire(_DecisionWireCore):
    conditions: list[WatchProposal]


class _DecisionRecordWire(_DecisionWireCore):
    outlook: _DecisionOutlookWire
    conditions: list[WatchCondition]
    conditions_shown: list[str]
    outlook_id: str = Field(min_length=1)

    @field_validator("outlook", mode="before")
    @classmethod
    def sparse_scenario_reasons(cls, value: JsonValue | DecisionOutlook) -> _DecisionOutlookWire:
        if isinstance(value, _DecisionOutlookWire):
            return _DecisionOutlookWire.model_validate(value)
        if isinstance(value, DecisionOutlook):
            raise ValueError("Persisted decision outlook requires actual reconciled watch references")
        payload = dict(_object(value, "outlook"))
        reasons = dict(_object(_required(payload, "scenario_reasons"), "scenario_reasons"))
        for name in ("base", "upside", "failure"):
            if name not in reasons:
                reasons[name] = None
        payload["scenario_reasons"] = reasons
        return _DecisionOutlookWire.model_validate(payload)


class _OutlookRowWire(_DecisionOutlookWire, GenerationProvenance, ResolvedVersionIdentity):
    out_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    cutoff: str
    cutoff_prec: str | None
    cutoff_prec_reason: str | None
    cutoff_bucket_anchor: Literal["START", "END", "UNSPECIFIED"] | None = None

    @field_validator("scenario_reasons", mode="before")
    @classmethod
    def sparse_available_reasons(cls, value: JsonValue | DecisionScenarioReasons) -> DecisionScenarioReasons:
        if isinstance(value, DecisionScenarioReasons):
            return DecisionScenarioReasons.model_validate(value)
        reasons = dict(_object(value, "scenario_reasons"))
        for name in ("base", "upside", "failure"):
            if name not in reasons:
                reasons[name] = None
        return DecisionScenarioReasons.model_validate(reasons)


def _wire_payload(
    model: DecisionCore
    | GenerationProvenance
    | WatchProposal
    | WatchCondition
    | _DecisionDraftWire
    | _DecisionRecordWire
    | DecisionStatusWriter
    | DecisionOutlook
    | _OutlookRowWire,
) -> dict[str, JsonValue]:
    value = decode_json(model.model_dump_json(exclude_unset=True, warnings="error"))
    return _object(value, "serialized decision record")


def _generation(wire: GenerationProvenance) -> GenerationProvenance:
    return GenerationProvenance(
        config_version=wire.config_version,
        code_version=wire.code_version,
        session_config_version=wire.session_config_version,
        session_code_version=wire.session_code_version,
        model_id=wire.model_id,
        model_id_basis=wire.model_id_basis,
        model_id_reason=wire.model_id_reason,
        prompt_version=wire.prompt_version,
        prompt_version_basis=wire.prompt_version_basis,
        prompt_version_reason=wire.prompt_version_reason,
        prompt_label=wire.prompt_label,
        prompt_binding=wire.prompt_binding,
        agent_version=wire.agent_version,
        agent_version_basis=wire.agent_version_basis,
        agent_version_reason=wire.agent_version_reason,
        session_id=wire.session_id,
    )


def _calculation(wire: _DecisionWireCore) -> DecisionCore:
    return DecisionCore(
        thesis_grade=wire.thesis_grade,
        thesis_rule=wire.thesis_rule,
        trade_grade=wire.trade_grade,
        trade_rule=wire.trade_rule,
        trade_reasons=wire.trade_reasons,
        action=wire.action,
        action_rule=wire.action_rule,
        tactical=wire.tactical,
        investigate=wire.investigate,
        confidence_limiting=wire.confidence_limiting,
        outlook=_pure_outlook(wire.outlook),
    )


def _metadata(wire: _DecisionWireCore) -> DecisionMetadata:
    if wire.mta_id != wire.market_truth_id or wire.mta_id != wire.provenance.mta_id:
        raise ValueError("Decision market truth identities disagree")
    ended = wire.tracking_ended is True
    tracking: DecisionTracking
    if ended:
        if wire.previous_decision_id is None or not wire.expired_call_ids or wire.actionable is not False:
            raise ValueError(
                "Ended tracking requires its actual predecessor, expired calls and inactive state"
            )
        if wire.previous_decision_id == wire.dec_id:
            raise ValueError("Ended tracking cannot reference itself as predecessor")
        if wire.tracking_recorded_at is None or wire.tracking_writer is None:
            raise ValueError("Ended decision tracking requires its actual status clock and writer provenance")
        tracking = EndedDecisionTracking(
            kind="ENDED",
            previous_decision_id=wire.previous_decision_id,
            expired_call_ids=tuple(wire.expired_call_ids),
            status_recorded_at=Timestamp(
                wire.tracking_recorded_at,
                wire.tracking_recorded_at_prec,
                wire.tracking_recorded_at_prec_reason,
                wire.tracking_recorded_at_bucket_anchor,
            ),
            writer=wire.tracking_writer,
        )
    else:
        if wire.previous_decision_id is not None or wire.expired_call_ids is not None:
            raise ValueError("Active tracking cannot declare expired-call lineage")
        if any(
            value is not None
            for value in (
                wire.tracking_recorded_at,
                wire.tracking_recorded_at_prec,
                wire.tracking_recorded_at_prec_reason,
                wire.tracking_recorded_at_bucket_anchor,
                wire.tracking_writer,
            )
        ):
            raise ValueError("Active tracking cannot declare ended-status clock or writer provenance")
        tracking = ActiveDecisionTracking(kind="ACTIVE")
    if wire.removed_from_analysis is True:
        if wire.management_only is not True or wire.actionable is not False:
            raise ValueError("Removed decision must be inactive and management-only")
        management = "REMOVED_FROM_ANALYSIS"
    elif wire.management_only is True:
        if wire.actionable is False and not ended:
            raise ValueError("Inactive management-only decision must declare removal")
        management = "MANAGEMENT_ONLY"
    else:
        if wire.actionable is False and not ended:
            raise ValueError("A normal decision cannot silently become inactive")
        management = "NORMAL"
    prices: DecisionPrices | None = None
    if wire.entry_ref_price is not None or wire.invalidation_price is not None:
        if wire.entry_ref_price is None or wire.invalidation_price is None:
            raise ValueError("Decision reference prices require both actual fields")
        prices = DecisionPrices(
            entry_ref_price=wire.entry_ref_price, invalidation_price=wire.invalidation_price
        )
    return DecisionMetadata.model_validate(
        {
            "identity": DecisionIdentity(
                decision_id=wire.dec_id,
                event_id=wire.event_id,
                token_id=wire.token_id,
                cutoff=Timestamp(
                    wire.cutoff, wire.cutoff_prec, wire.cutoff_prec_reason, wire.cutoff_bucket_anchor
                ),
                thresholds_version=wire.thresholds_version,
                versions=ResolvedVersionIdentity(
                    config_version=wire.config_version, code_version=wire.code_version
                ),
                generation=_generation(wire),
                assessment_id=wire.provenance.ems_id,
                market_truth_id=wire.market_truth_id,
            ),
            "calculated_at": Timestamp(
                wire.calculated_at,
                wire.calculated_at_prec,
                wire.calculated_at_prec_reason,
                wire.calculated_at_bucket_anchor,
            ),
            "management": management,
            "contradiction_ids": tuple(wire.contradictions),
            "prices": prices,
            "tracking": tracking,
        }
    )


def _base_payload(calculation: DecisionCore, metadata: DecisionMetadata) -> dict[str, JsonValue]:
    identity = metadata.identity
    payload = _wire_payload(calculation)
    payload.update(_wire_payload(identity.generation))
    payload.update(identity.cutoff.fields("cutoff"))
    payload.update(metadata.calculated_at.fields("calculated_at"))
    payload.update(
        dec_id=identity.decision_id,
        event_id=identity.event_id,
        token_id=identity.token_id,
        thresholds_version=identity.thresholds_version,
        market_truth_id=identity.market_truth_id,
        mta_id=identity.market_truth_id,
        provenance={"ems_id": identity.assessment_id, "mta_id": identity.market_truth_id},
        contradictions=[value for value in metadata.contradiction_ids],
    )
    if metadata.management in {"MANAGEMENT_ONLY", "REMOVED_FROM_ANALYSIS"}:
        payload["management_only"] = True
    if metadata.management == "REMOVED_FROM_ANALYSIS":
        payload["actionable"] = False
        payload["removed_from_analysis"] = True
    if metadata.prices is not None:
        payload["entry_ref_price"] = str(metadata.prices.entry_ref_price)
        payload["invalidation_price"] = str(metadata.prices.invalidation_price)
    if isinstance(metadata.tracking, EndedDecisionTracking):
        payload.update(metadata.tracking.status_recorded_at.fields("tracking_recorded_at"))
        payload["tracking_writer"] = _wire_payload(metadata.tracking.writer)
        payload.update(
            tracking_ended=True,
            actionable=False,
            previous_decision_id=metadata.tracking.previous_decision_id,
            expired_call_ids=[value for value in metadata.tracking.expired_call_ids],
        )
    return payload


def _sparse_reasons(payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
    outlook = _object(_required(payload, "outlook"), "outlook")
    reasons = _object(_required(outlook, "scenario_reasons"), "scenario_reasons")
    outlook["scenario_reasons"] = {name: value for name, value in reasons.items() if value is not None}
    return payload


def encode_decision_draft(draft: DecisionDraft) -> dict[str, JsonValue]:
    draft = DecisionDraft.model_validate(draft)
    payload = _base_payload(draft.calculation, draft.metadata)
    payload["conditions"] = [_wire_payload(value) for value in draft.proposals]
    wire = _DecisionDraftWire.model_validate(payload, strict=True)
    _metadata(wire)
    return _sparse_reasons(_wire_payload(wire))


def decode_decision_draft(payload: Mapping[str, JsonValue]) -> DecisionDraft:
    wire = _DecisionDraftWire.model_validate(dict(payload), strict=True)
    return DecisionDraft(calculation=_calculation(wire), metadata=_metadata(wire), proposals=wire.conditions)


def encode_decision(record: DecisionRecord) -> dict[str, JsonValue]:
    record = DecisionRecord.model_validate(record)
    payload = _base_payload(record.calculation, record.metadata)
    outlook = _wire_payload(record.calculation.outlook)
    outlook["conditions"] = JsonDocument.validate_value(
        OutlookConditionRefs.from_conditions(record.conditions).model_dump(mode="json")
    )
    payload["outlook"] = outlook
    payload["conditions"] = [_wire_payload(value) for value in record.conditions]
    payload["conditions_shown"] = [value for value in record.conditions_shown]
    payload["outlook_id"] = record.outlook_id
    wire = _DecisionRecordWire.model_validate(payload, strict=True)
    _metadata(wire)
    return _sparse_reasons(_wire_payload(wire))


def decode_decision(payload: Mapping[str, JsonValue]) -> DecisionRecord:
    wire = _DecisionRecordWire.model_validate(dict(payload), strict=True)
    if wire.outlook.conditions.domain() != OutlookConditionRefs.from_conditions(wire.conditions):
        raise ValueError("Embedded outlook watch references disagree with reconciled conditions")
    return DecisionRecord(
        calculation=_calculation(wire),
        metadata=_metadata(wire),
        conditions=wire.conditions,
        conditions_shown=tuple(wire.conditions_shown),
        outlook_id=wire.outlook_id,
    )


def encode_outlook(record: DecisionRecord) -> dict[str, JsonValue]:
    record = DecisionRecord.model_validate(record)
    metadata = record.metadata
    if isinstance(metadata.tracking, EndedDecisionTracking):
        raise ValueError("Ended tracking cannot overwrite its historical outlook row")
    identity = metadata.identity
    payload = _wire_payload(record.calculation.outlook)
    payload["conditions"] = JsonDocument.validate_value(
        OutlookConditionRefs.from_conditions(record.conditions).model_dump(mode="json")
    )
    payload.update(_wire_payload(identity.generation))
    payload.update(identity.cutoff.fields("cutoff"))
    payload.update(out_id=record.outlook_id, event_id=identity.event_id, token_id=identity.token_id)
    wire = _OutlookRowWire.model_validate(payload, strict=True)
    encoded = _wire_payload(wire)
    reasons = _object(_required(encoded, "scenario_reasons"), "scenario_reasons")
    encoded["scenario_reasons"] = {name: value for name, value in reasons.items() if value is not None}
    decode_outlook(encoded)
    return encoded


def decode_outlook(payload: Mapping[str, JsonValue]) -> ReconciledOutlook:
    wire = _OutlookRowWire.model_validate(dict(payload), strict=True)
    return ReconciledOutlook(
        outlook_id=wire.out_id,
        event_id=wire.event_id,
        token_id=wire.token_id,
        cutoff=Timestamp(wire.cutoff, wire.cutoff_prec, wire.cutoff_prec_reason, wire.cutoff_bucket_anchor),
        versions=ResolvedVersionIdentity(config_version=wire.config_version, code_version=wire.code_version),
        generation=_generation(wire),
        calculation=_pure_outlook(wire),
        conditions=wire.conditions.domain(),
    )
