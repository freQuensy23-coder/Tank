"""Pure watch lifecycle reconciliation for a materialized decision command."""

from datetime import datetime, timedelta

from pydantic import Field, model_validator

from tank.analysis.decision_contracts import DecisionDraft
from tank.analysis.watch_contracts import WatchCall, WatchCondition, WatchExpression, WatchLeaf, WatchProposal
from tank.contracts import DomainModel
from tank.contracts.provenance import GenerationProvenance
from tank.time_precision import Timestamp, collection_clock, offset_schedule
from tank.util import canonical_json, parse_iso_time


class DecisionWatchCommand(DomainModel):
    decision_id: str = Field(min_length=1)
    event_id: str = Field(min_length=1)
    token_id: str = Field(min_length=1)
    generation: GenerationProvenance
    proposals: list[WatchProposal]

    @classmethod
    def from_draft(cls, draft: DecisionDraft) -> "DecisionWatchCommand":
        draft = DecisionDraft.model_validate(draft)
        identity = draft.metadata.identity
        return cls(
            decision_id=identity.decision_id,
            event_id=identity.event_id,
            token_id=identity.token_id,
            generation=identity.generation,
            proposals=draft.proposals,
        )


class DecisionWatchPolicy(DomainModel):
    lifetime: timedelta = Field(gt=timedelta(0))
    milestone_grace: timedelta = Field(ge=timedelta(0))


class DecisionWatchReconciliation(DomainModel):
    writes: tuple[WatchCondition, ...]
    active: list[WatchCondition]
    shown: tuple[str, ...] = Field(max_length=2)

    @model_validator(mode="after")
    def unique_lifecycle(self) -> "DecisionWatchReconciliation":
        identities = [condition.cond_id for condition in self.writes]
        if len(identities) != len(set(identities)):
            raise ValueError("A watch can have only one final lifecycle update")
        if any(condition.status != "ARMED" for condition in self.active):
            raise ValueError("A decision can show only actual armed conditions")
        active = {condition.cond_id for condition in self.active}
        if len(active) != len(self.active) or not set(self.shown) <= active:
            raise ValueError("Shown watches must be unique members of the actual armed set")
        return self


def _signature(role: str, expression: WatchExpression) -> tuple[str, str]:
    return role, canonical_json(expression)


def _fired_at(condition: WatchCondition) -> datetime:
    if condition.fired_at is None:
        raise ValueError("A rearmed watch requires its predecessor's actual firing clock")
    return parse_iso_time(condition.fired_at)


def _known_stamp(stamp: Timestamp) -> str:
    if stamp.value is None:
        raise ValueError("A watch lifecycle command requires its actual clock")
    return stamp.value


def reconcile_decision_watches(
    command: DecisionWatchCommand,
    existing: list[WatchCondition],
    calls: list[WatchCall],
    *,
    now: datetime,
    policy: DecisionWatchPolicy,
    prepared_condition_ids: frozenset[str] | None,
) -> DecisionWatchReconciliation:
    command = DecisionWatchCommand.model_validate(command)
    policy = DecisionWatchPolicy.model_validate(policy)
    at = parse_iso_time(now)
    stamp = collection_clock(at)
    expiry = offset_schedule(stamp, policy.lifetime)
    identity = command
    for call in calls:
        if call.event_id != identity.event_id or call.token_id != identity.token_id or call.status != "OPEN":
            raise ValueError("Decision watch lifetime requires its actual matching open calls")
    if calls:
        latest_call = max(calls, key=lambda call: parse_iso_time(call.opened_at))
        candidate = offset_schedule(
            Timestamp(
                latest_call.opened_at,
                latest_call.opened_at_prec,
                latest_call.opened_at_prec_reason,
                latest_call.opened_at_bucket_anchor,
            ),
            policy.lifetime,
        )
        if parse_iso_time(_known_stamp(candidate)) < parse_iso_time(_known_stamp(expiry)):
            expiry = candidate
    conditions = {condition.cond_id: WatchCondition.model_validate(condition) for condition in existing}
    if len(conditions) != len(existing):
        raise ValueError("Stored watch identities must be unique")
    if any(
        condition.event_id != identity.event_id or condition.token_id != identity.token_id
        for condition in conditions.values()
    ):
        raise ValueError("Decision cannot reconcile watches from a different event or token")
    writes: dict[str, WatchCondition] = {}
    for condition in conditions.values():
        if (
            condition.status == "FIRED"
            and condition.consumed_by is None
            and (prepared_condition_ids is None or condition.cond_id in prepared_condition_ids)
        ):
            changed = WatchCondition.model_validate(
                condition.model_copy(
                    update={
                        "consumed_by": identity.decision_id,
                        **stamp.fields("consumed_at"),
                    }
                )
            )
            conditions[condition.cond_id] = changed
            writes[changed.cond_id] = changed
    active: list[WatchCondition] = []
    used: set[str] = set()
    signatures: set[tuple[str, str]] = set()
    proposal_ids = [proposal.cond_id for proposal in command.proposals]
    if len(proposal_ids) != len(set(proposal_ids)):
        raise ValueError("Allocated decision watch identities must be unique")
    for proposal in command.proposals:
        signature = _signature(proposal.role, proposal.expr)
        if signature in signatures:
            raise ValueError("A decision cannot propose the same watch predicate twice")
        signatures.add(signature)
        matches = [
            condition
            for condition in conditions.values()
            if condition.status == "ARMED"
            and parse_iso_time(condition.expires_at) > at
            and _signature(condition.role, condition.expr) == signature
        ]
        spent = [
            condition
            for condition in conditions.values()
            if condition.status == "FIRED" and _signature(condition.role, condition.expr) == signature
        ]
        if matches:
            chosen = max(matches, key=lambda condition: parse_iso_time(condition.created_at))
            condition = WatchCondition.model_validate(
                chosen.model_copy(
                    update={
                        "last_shown_by": identity.decision_id,
                    }
                )
            )
        else:
            if proposal.cond_id in conditions:
                raise ValueError("A new watch cannot overwrite an existing lifecycle identity")
            deadline = expiry
            if isinstance(proposal.expr, WatchLeaf) and proposal.expr.due_at is not None:
                milestone = offset_schedule(
                    Timestamp(
                        proposal.expr.due_at,
                        proposal.expr.due_at_prec,
                        proposal.expr.due_at_prec_reason,
                        proposal.expr.due_at_bucket_anchor,
                    ),
                    policy.milestone_grace,
                )
                if parse_iso_time(_known_stamp(milestone)) < parse_iso_time(_known_stamp(deadline)):
                    deadline = milestone
            if parse_iso_time(_known_stamp(deadline)) <= at:
                raise ValueError("A new armed watch requires an unexpired actual deadline")
            predecessor = max(spent, key=_fired_at) if spent else None
            generation = identity.generation
            condition = WatchCondition(
                cond_id=proposal.cond_id,
                event_id=identity.event_id,
                token_id=identity.token_id,
                role=proposal.role,
                human_text=proposal.human_text,
                expr=proposal.expr,
                status="ARMED",
                created_by=identity.decision_id,
                created_at=_known_stamp(stamp),
                created_at_prec=stamp.precision,
                created_at_prec_reason=stamp.precision_reason,
                created_at_bucket_anchor=stamp.bucket_anchor,
                expires_at=_known_stamp(deadline),
                expires_at_prec=deadline.precision,
                expires_at_prec_reason=deadline.precision_reason,
                expires_at_bucket_anchor=deadline.bucket_anchor,
                milestone_id=proposal.milestone_id,
                rearmed_from=None if predecessor is None else predecessor.cond_id,
                config_version=generation.config_version,
                code_version=generation.code_version,
                session_config_version=generation.session_config_version,
                session_code_version=generation.session_code_version,
                model_id=generation.model_id,
                model_id_basis=generation.model_id_basis,
                model_id_reason=generation.model_id_reason,
                prompt_version=generation.prompt_version,
                prompt_version_basis=generation.prompt_version_basis,
                prompt_version_reason=generation.prompt_version_reason,
                prompt_label=generation.prompt_label,
                prompt_binding=generation.prompt_binding,
                agent_version=generation.agent_version,
                agent_version_basis=generation.agent_version_basis,
                agent_version_reason=generation.agent_version_reason,
                session_id=generation.session_id,
            )
        used.add(condition.cond_id)
        writes[condition.cond_id] = condition
        active.append(condition)
    for condition in conditions.values():
        if (
            condition.cond_id not in used
            and condition.status == "ARMED"
            and not condition.agent_defined
            and condition.role != "AGENT"
        ):
            changed = WatchCondition.model_validate(
                condition.model_copy(
                    update={
                        "status": "CANCELLED",
                        "superseded_by": identity.decision_id,
                        **stamp.fields("cancelled_at"),
                    }
                )
            )
            writes[changed.cond_id] = changed
    return DecisionWatchReconciliation(
        writes=tuple(writes.values()),
        active=active,
        shown=tuple(condition.cond_id for condition in active[:2]),
    )
