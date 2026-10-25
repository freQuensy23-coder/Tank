"""Native discovery binding, pagination and exact prompt evidence."""

from typing import Literal

import pytest
from native_session_fixtures import native_session_record
from pydantic import JsonValue, ValidationError

from tank.agents.native_sessions import (
    NativeOpeningSource,
    NativeReplayIdentity,
    NativeSessionCreate,
    NativeSessionDeployment,
    NativeSessionDiscovery,
    NativeSessionIdentity,
    NativeSessionIdentityMetadata,
    NativeSessionResolution,
    NativeStoredSessionBinding,
    native_role_for_message,
    observe_native_session,
    observe_native_session_page,
)
from tank.contracts import DomainModel
from tank.contracts.session_admission import SessionTarget
from tank.util import canonical_json, sha256


class RequestObservation(DomainModel):
    method: Literal["GET", "POST"]
    page: str | None
    idem: str | None
    creation: NativeSessionCreate | None


class ScriptedEndpoint:
    def __init__(self, responses: list[dict[str, JsonValue]]) -> None:
        self.responses = responses
        self.requests: list[RequestObservation] = []

    async def request(
        self,
        method: Literal["GET", "POST"],
        path: str,
        *,
        json: JsonValue = None,
        params: dict[str, str | int] | None = None,
        idem: str | None = None,
    ) -> dict[str, JsonValue]:
        assert path == "sessions"
        if method == "GET":
            assert json is None and idem is None and params is not None
            assert params["limit"] == 100
            page = params.get("page")
            assert page is None or isinstance(page, str)
            creation = None
        else:
            assert params is None
            page = None
            creation = NativeSessionCreate.model_validate(json)
        self.requests.append(RequestObservation(method=method, page=page, idem=idem, creation=creation))
        assert self.responses
        return self.responses.pop(0)


def deployment() -> NativeSessionDeployment:
    return NativeSessionDeployment(
        agent_id="agent", environment_id="environment", prompt_version="declared-label", effort="medium"
    )


def wanted() -> NativeSessionCreate:
    return NativeSessionCreate(
        agent="agent",
        environment_id="environment",
        metadata=NativeSessionIdentityMetadata(tank_identity="identity"),
    )


def observed(
    session_id: str,
    *,
    agent_id: str = "agent",
    environment_id: str = "environment",
    metadata: dict[str, str] | None = None,
) -> dict[str, JsonValue]:
    return native_session_record(
        session_id,
        agent_id=agent_id,
        environment_id=environment_id,
        metadata={"tank_identity": "identity"} if metadata is None else metadata,
        model_id="observed-model",
    )


@pytest.mark.asyncio
async def test_existing_binding_is_found_after_an_actual_page_boundary_without_creation() -> None:
    first: dict[str, JsonValue] = {
        "data": [observed("unrelated", metadata={"provider": "retained"})],
        "next_page": "second",
    }
    last: dict[str, JsonValue] = {"data": [observed("existing")], "next_page": None}
    endpoint = ScriptedEndpoint([first, last])
    result = await NativeSessionDiscovery(endpoint, deployment()).discover_or_create(
        wanted(), idem="create:identity"
    )
    assert result.session_id == "existing"
    assert [request.page for request in endpoint.requests] == [None, "second"]
    assert all(request.creation is None for request in endpoint.requests)
    assert result.provenance.model_id == "observed-model"
    assert result.provenance.model_id_basis == "RESOLVED_SESSION_AGENT"


@pytest.mark.asyncio
async def test_new_session_confirms_its_requested_immutable_resource_and_metadata_binding() -> None:
    endpoint = ScriptedEndpoint(
        [
            {"data": [], "next_page": None},
            observed("created", metadata={"tank_identity": "identity", "provider": "extra"}),
        ]
    )
    result = await NativeSessionDiscovery(endpoint, deployment()).discover_or_create(
        wanted(), idem="create:identity"
    )
    assert result.session_id == "created"
    assert endpoint.requests[-1].creation == wanted()
    assert endpoint.requests[-1].idem == "create:identity"
    assert result.metadata["provider"] == "extra"


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ["agent", "environment", "metadata", "replay"])
async def test_contradictory_created_binding_is_not_returned_as_a_success(defect: str) -> None:
    creation = wanted()
    if defect == "agent":
        response = observed("wrong", agent_id="other")
    elif defect == "environment":
        response = observed("wrong", environment_id="other")
    elif defect == "metadata":
        response = observed("wrong", metadata={})
    else:
        creation = NativeSessionCreate(
            agent="agent",
            environment_id="environment",
            metadata=NativeSessionIdentityMetadata(tank_identity="identity", tank_replay_run_id="run"),
        )
        response = observed("wrong")
    endpoint = ScriptedEndpoint([{"data": [], "next_page": None}, response])
    with pytest.raises(ValueError):
        await NativeSessionDiscovery(endpoint, deployment()).discover_or_create(
            creation, idem="create:identity"
        )
    assert len(endpoint.requests) == 2
    assert sum(request.method == "POST" for request in endpoint.requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("ambiguity", ["identity", "cursor"])
async def test_ambiguous_discovery_never_creates_a_replacement(ambiguity: str) -> None:
    if ambiguity == "identity":
        responses: list[dict[str, JsonValue]] = [
            {"data": [observed("one"), observed("two")], "next_page": None}
        ]
    else:
        responses = [{"data": [], "next_page": "same"}, {"data": [], "next_page": "same"}]
    endpoint = ScriptedEndpoint(responses)
    with pytest.raises(ValueError, match="repeated"):
        await NativeSessionDiscovery(endpoint, deployment()).discover_or_create(
            wanted(), idem="create:identity"
        )
    assert all(request.method == "GET" for request in endpoint.requests)


@pytest.mark.parametrize(
    "wire",
    [
        {"data": []},
        {"data": None, "next_page": None},
        {"data": [1], "next_page": None},
        {"data": [], "next_page": ""},
    ],
)
def test_required_page_facts_are_not_reconstructed(wire: dict[str, JsonValue]) -> None:
    with pytest.raises((KeyError, ValueError)):
        observe_native_session_page(wire, deployment())


def test_prompt_fingerprint_preserves_provider_extension_values_and_number_types() -> None:
    first = observed("session")
    first_agent = first["agent"]
    assert isinstance(first_agent, dict)
    first_agent["_oma"] = {"harness": "pi", "provider_extension": {"amount": 1}}
    second = observed("session")
    second_agent = second["agent"]
    assert isinstance(second_agent, dict)
    second_agent["_oma"] = {"harness": "pi", "provider_extension": {"amount": 1.0}}
    left = observe_native_session(first, deployment())
    right = observe_native_session(second, deployment())
    assert left.provenance.prompt_version is not None
    assert left.provenance.prompt_version != right.provenance.prompt_version
    assert left.provenance.prompt_label == right.provenance.prompt_label == "declared-label"


@pytest.mark.parametrize(
    "missing", ["id", "version", "model", "skills", "tools", "system", "mcp_servers", "multiagent"]
)
def test_resolved_prompt_facts_are_required_before_provenance_is_recorded(missing: str) -> None:
    wire = observed("session")
    agent = wire["agent"]
    assert isinstance(agent, dict)
    del agent[missing]
    with pytest.raises((KeyError, ValidationError)):
        observe_native_session(wire, deployment())


def test_explicit_opening_source_conflict_is_not_resolved_by_value_order() -> None:
    with pytest.raises(ValidationError, match="disagree"):
        NativeOpeningSource(item_id="one", opened_by_item="two")
    assert NativeOpeningSource(item_id="origin", opened_by_item=None).opening_item() == "origin"
    assert NativeOpeningSource(item_id=None, opened_by_item=None).opening_item() is None


def resolution(
    replay: NativeReplayIdentity | None, *, session_id: str = "session"
) -> NativeSessionResolution:
    return NativeSessionResolution(
        target=SessionTarget(session_id=session_id, event_id=None, item_id=None, proposal_id=None),
        role="ANALYST",
        replay=replay,
    )


@pytest.mark.asyncio
async def test_required_deployment_binding_refuses_before_any_native_effect() -> None:
    endpoint = ScriptedEndpoint([])
    wrong = NativeSessionCreate(
        agent="other",
        environment_id="environment",
        metadata=NativeSessionIdentityMetadata(tank_identity="identity"),
    )
    with pytest.raises(ValueError, match="configured deployment"):
        await NativeSessionDiscovery(endpoint, deployment()).discover_or_create(wrong, idem="create:identity")
    assert not endpoint.requests


def test_identity_configuration_requires_consumed_facts_but_not_an_admission_cap() -> None:
    config: dict[str, JsonValue] = {
        "agent_id": "agent",
        "environment_id": "environment",
        "prompt_version": "declared-label",
        "reasoning_effort": "medium",
    }
    assert NativeSessionDeployment.from_wire(config, role="ANALYST", replay=False) == deployment()
    del config["reasoning_effort"]
    with pytest.raises(KeyError):
        NativeSessionDeployment.from_wire(config, role="ANALYST", replay=False)


def test_cached_private_state_does_not_require_invented_replay_provenance() -> None:
    bound = NativeStoredSessionBinding.from_record({"session_id": "session"})
    bound.verify(resolution(None))
    assert bound.replay_run_id is None
    with pytest.raises(ValueError, match="requested resource"):
        bound.verify(resolution(None, session_id="different"))


@pytest.mark.parametrize("defect", ["run", "digest", "cutoff", "source"])
def test_cached_replay_binding_must_match_the_requested_recorded_evidence(defect: str) -> None:
    proof = NativeReplayIdentity(
        run_id="run",
        source_session_id="original",
        evidence_sha256="1" * 64,
        cutoff="2026-10-01T00:00:00Z",
    )
    record: dict[str, JsonValue] = {
        "session_id": "session",
        "replay_run_id": "run",
        "replay_source_session_id": "original",
        "replay_evidence_sha256": "1" * 64,
        "replay_cutoff": "2026-10-01T00:00:00Z",
    }
    NativeStoredSessionBinding.from_record(record).verify(resolution(proof))
    names = {
        "run": "replay_run_id",
        "digest": "replay_evidence_sha256",
        "cutoff": "replay_cutoff",
        "source": "replay_source_session_id",
    }
    record[names[defect]] = "2" * 64 if defect == "digest" else "different"
    bound = NativeStoredSessionBinding.from_record(record)
    with pytest.raises(ValueError):
        bound.verify(resolution(proof))
    with pytest.raises(ValueError, match="Live input"):
        bound.verify(resolution(None))


def test_replay_identity_requires_explicit_composition_and_real_recorded_metadata() -> None:
    with pytest.raises(ValueError, match="explicit replay"):
        NativeSessionResolution.from_wire(
            {"session_id": "session", "replay_run_id": "run"}, role="ANALYST", replay=False
        )
    with pytest.raises(ValidationError):
        NativeSessionResolution.from_wire({"session_id": "session"}, role="ANALYST", replay=True)
    with pytest.raises(ValidationError, match="bound cutoff"):
        NativeReplayIdentity(run_id="run", source_session_id=None, evidence_sha256="1" * 64, cutoff=None)
    live = NativeSessionIdentityMetadata.from_binding("identity", None)
    assert live.expected_fields() == {"tank_identity": "identity"}
    assert live.model_dump(mode="json", exclude_none=True) == {"tank_identity": "identity"}


@pytest.mark.parametrize("effort", ["high", None])
def test_configured_effort_is_distinct_from_reported_native_effort(effort: str | None) -> None:
    wire = observed("session")
    agent = wire["agent"]
    assert isinstance(agent, dict)
    model = agent["model"]
    assert isinstance(model, dict)
    if effort is not None:
        model["effort"] = {"type": effort}
    observation = observe_native_session(wire, deployment())
    assert deployment().effort == "medium"
    assert observation.effort == effort


@pytest.mark.parametrize("value", [None, {"type": "unsupported"}, "high"])
def test_malformed_reported_effort_cannot_become_configured_effort(value: JsonValue) -> None:
    wire = observed("session")
    agent = wire["agent"]
    assert isinstance(agent, dict)
    model = agent["model"]
    assert isinstance(model, dict)
    model["effort"] = value
    with pytest.raises(ValidationError):
        observe_native_session(wire, deployment())


@pytest.mark.parametrize("kind", ["UNKNOWN", None, 1, {"type": "NEW_ITEM"}])
def test_unknown_message_kind_cannot_select_the_analyst_deployment(kind: JsonValue) -> None:
    with pytest.raises(ValueError, match="Unsupported native"):
        native_role_for_message(kind)


@pytest.mark.parametrize(
    "kind", ["NEW_ITEM", "ITEM_ADDED", "USER_QUESTION", "CONDITION_FIRED", "CALL_EXPIRED", "MILESTONE"]
)
def test_known_event_messages_have_their_explicit_analyst_role(kind: str) -> None:
    assert native_role_for_message(kind) == "ANALYST"
    assert native_role_for_message("SCOUT_REVIEW") == "SCOUT"


@pytest.mark.parametrize("run_id", [None, "run"])
@pytest.mark.parametrize("scout", [False, True])
def test_typed_identity_marker_preserves_the_existing_canonical_wire_hash(
    run_id: str | None, scout: bool
) -> None:
    identity = NativeSessionIdentity(
        identity="item", scout=scout, model_context_version=3, replay_run_id=run_id
    )
    original: dict[str, JsonValue] = {"identity": "item", "scout": scout, "model_context_version": 3}
    if run_id is not None:
        original["replay_run_id"] = run_id
    assert canonical_json(identity.model_dump(mode="json", exclude_none=True)) == canonical_json(original)
    assert identity.marker() == sha256(original)


@pytest.mark.parametrize("extension", [None, "invalid", []])
def test_malformed_reported_prompt_extension_cannot_become_a_verified_snapshot(extension: JsonValue) -> None:
    wire = observed("session")
    agent = wire["agent"]
    assert isinstance(agent, dict)
    agent["_oma"] = extension
    with pytest.raises(ValidationError):
        observe_native_session(wire, deployment())


def test_floating_native_skill_version_retains_explicit_incomplete_prompt_provenance() -> None:
    wire = observed("session")
    agent = wire["agent"]
    assert isinstance(agent, dict)
    agent["skills"] = [{"type": "custom", "skill_id": "skill", "version": "latest"}]
    result = observe_native_session(wire, deployment())
    assert result.provenance.agent_version == "1"
    assert result.provenance.prompt_version is None
    assert result.provenance.prompt_binding is None
    assert (
        result.provenance.prompt_version_reason == "INCOMPLETE: remote resolved prompt snapshot unavailable"
    )


def test_opening_source_lookup_does_not_cross_an_undeclared_replay_scope() -> None:
    recorded = NativeStoredSessionBinding.from_record(
        {"session_id": "recorded", "replay_run_id": "previous-run"}
    )
    original = NativeStoredSessionBinding.from_record({"session_id": "original"})
    assert not recorded.matches_origin_scope(resolution(None))
    assert original.matches_origin_scope(resolution(None))
    explicit = NativeReplayIdentity(
        run_id="new-run", source_session_id="recorded", evidence_sha256=None, cutoff=None
    )
    assert recorded.matches_origin_scope(resolution(explicit))
    assert not original.matches_origin_scope(resolution(explicit))
    unreported = NativeReplayIdentity(
        run_id="new-run", source_session_id=None, evidence_sha256=None, cutoff=None
    )
    assert not recorded.matches_origin_scope(resolution(unreported))
    assert original.matches_origin_scope(resolution(unreported))


def test_declared_unrecorded_cutoff_is_bound_without_inventing_an_evidence_digest() -> None:
    replay = NativeReplayIdentity(
        run_id="run", source_session_id=None, evidence_sha256=None, cutoff="2026-10-01T00:00:00Z"
    )
    metadata = NativeSessionIdentityMetadata.from_binding("identity", replay)
    assert metadata.expected_fields() == {
        "tank_identity": "identity",
        "tank_replay_run_id": "run",
        "tank_replay_cutoff": "2026-10-01T00:00:00Z",
    }
    stored = NativeStoredSessionBinding.from_record(
        {"session_id": "session", "replay_run_id": "run", "replay_cutoff": "2026-10-01T00:00:00Z"}
    )
    stored.verify(resolution(replay))
    changed = NativeReplayIdentity(
        run_id="run", source_session_id=None, evidence_sha256=None, cutoff="2026-10-02T00:00:00Z"
    )
    with pytest.raises(ValueError, match="cutoff"):
        stored.verify(resolution(changed))
