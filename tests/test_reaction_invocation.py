"""Claim contract admission retains original wire evidence and typed aliases."""

import pytest
from pydantic import JsonValue
from test_job_claim_mutations import job_document
from test_runtime_reaction import initial_payload

from tank.contracts.jobs import JobEnvelope
from tank.runtime_reaction import ReactionJobInvocation, reaction_job_command
from tank.util import canonical_json, sha256


def declared_claim() -> dict[str, JsonValue]:
    return {
        **job_document(),
        "type": "REACTION_CAPTURE",
        "claim_token": "synthetic-contract-token",
        "payload": initial_payload() | {"_depends_on": []},
    }


def test_typed_invocation_revalidation_accepts_actual_wire_dependency_alias() -> None:
    wire = declared_claim()
    before = canonical_json(wire)
    invocation = ReactionJobInvocation.from_record(wire)
    revalidated = ReactionJobInvocation.model_validate(invocation, by_name=True)
    restored = ReactionJobInvocation.model_validate_json(
        invocation.model_dump_json(by_alias=True, exclude_unset=True)
    )
    assert revalidated == restored == invocation
    assert invocation.queue_input.depends_on == []
    assert invocation.context.original_payload == wire["payload"]
    assert invocation.context.scope.payload_revision == sha256(wire["payload"])
    assert invocation.context.scope.recovery_count is None
    assert invocation.command.model_fields_set == {
        "event_id",
        "token_id",
        "checkpoint",
        "anchor",
        "cutoff",
        "retro",
    }
    assert canonical_json(wire) == before


def test_envelope_invocation_hashes_actual_payload_and_keeps_absent_clock_facets() -> None:
    wire = declared_claim()
    invocation = ReactionJobInvocation.from_envelope(JobEnvelope.model_validate(wire))
    assert invocation.context.original_payload == wire["payload"]
    assert invocation.context.payload_sha256 == sha256(wire["payload"])
    anchor = invocation.queue_input.model_dump(mode="json", exclude_unset=True)["anchor"]
    assert isinstance(anchor, dict) and set(anchor) == {"type", "ts"}


def test_wire_dependency_field_name_is_not_an_accepted_alias() -> None:
    with pytest.raises(ValueError):
        reaction_job_command(initial_payload() | {"depends_on": []})


@pytest.mark.parametrize("missing_token", [True, False])
def test_execution_never_fabricates_an_unreported_claim_token(missing_token: bool) -> None:
    wire = declared_claim()
    if missing_token:
        wire.pop("claim_token")
    else:
        wire["claim_token"] = None
    with pytest.raises(ValueError):
        ReactionJobInvocation.from_record(wire)
