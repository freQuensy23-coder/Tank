"""Whole supply output retains evidence presence and source-specific absence."""

from collections.abc import Mapping

import pytest
from pydantic import JsonValue
from test_condition_graph_codec import SupplyWireEvidence, condition_wire_clocks
from test_condition_graph_contracts import chain_liquidity, empty_supply_graph, unlock_schedule, wallet_flows

from tank.analysis.condition_graph_codec import encode_condition_supply
from tank.analysis.condition_graph_contracts import CapturedConditionLending, ConditionReceiptVisibility
from tank.time_precision import Timestamp


class SupplyCompositionEvidence(SupplyWireEvidence):
    def resolve_lending_observation(self, *, expected: CapturedConditionLending) -> Mapping[str, JsonValue]:
        raise AssertionError("No lending source was supplied to this fixture")


def evidence() -> SupplyCompositionEvidence:
    return SupplyCompositionEvidence(wallet_flows().flows[0], unlock_schedule().upcoming[0])


def test_supply_composition_separates_no_inputs_from_observed_potential_supply() -> None:
    graph = empty_supply_graph()
    lookup = evidence()
    absent = encode_condition_supply(graph, lookup, clocks=condition_wire_clocks())
    assert absent["supply_in_evidence"] == "UNKNOWN"
    assert absent["supply_evidence"] == {"status": "UNAVAILABLE", "reason": "NO_AVAILABLE_INPUTS"}
    assert absent["flows"] == [] and absent["unlocks_next_14d"] == []
    assert absent["unavailable"] == {
        "exchange_inventory": "NO_CAPTURED_OBSERVATION",
        "flows": "NO_CAPTURED_OBSERVATION",
        "unlocks": "NO_CAPTURED_OBSERVATION",
        "lending": "NO_CAPTURED_OBSERVATION",
        "stablecoin_chain": "SETTLEMENT_CHAIN_UNCONFIGURED",
    }
    potential = type(graph).model_validate(graph.model_copy(update={"unlocks": unlock_schedule()}))
    wire = encode_condition_supply(potential, lookup, clocks=condition_wire_clocks())
    assert wire["supply_in_evidence"] == "FALSE"
    assert wire["supply_evidence"] == {"status": "NO_POSITIVE_OBSERVATION", "available_inputs": ["UNLOCKS"]}
    assert lookup.unlock_calls == 1 and lookup.flow_calls == 0
    schedules = wire["unlocks_next_14d"]
    assert isinstance(schedules, list) and len(schedules) == 1
    assert isinstance(schedules[0], dict)
    assert schedules[0]["qty"] == "100"
    assert schedules[0]["source_note"] == {"exact_extension": ["retained"]}
    assert schedules[0]["inference"] == "POTENTIAL_SUPPLY_IS_NOT_A_SALE"


def test_late_schedule_is_not_an_available_input_or_rejoined_source() -> None:
    graph = empty_supply_graph()
    schedule = unlock_schedule()
    late = ConditionReceiptVisibility(
        received_at=Timestamp(value="2026-10-17T12:00:01Z", precision="COLLECTION_ONLY"),
        assembled_at=graph.assembled_at,
    )
    changed = type(schedule).model_validate(schedule.model_copy(update={"visibility": late}))
    graph = type(graph).model_validate(graph.model_copy(update={"unlocks": changed}))
    lookup = evidence()
    wire = encode_condition_supply(graph, lookup, clocks=condition_wire_clocks())
    assert wire["supply_in_evidence"] == "UNKNOWN" and wire["unlocks_next_14d"] == []
    unavailable = wire["unavailable"]
    assert isinstance(unavailable, dict) and unavailable["unlocks"] == "RECEIVED_AFTER_ASSEMBLY"
    assert lookup.unlock_calls == 0 and lookup.flow_calls == 0


def test_primary_chain_uses_captured_configuration_instead_of_array_position() -> None:
    graph = empty_supply_graph()
    selected = chain_liquidity()
    other = type(selected).model_validate(selected.model_copy(update={"chain": "other"}))
    policy = type(graph.settlement_chain).model_validate(
        graph.settlement_chain.model_copy(update={"chain": "chain"})
    )
    graph = type(graph).model_validate(
        graph.model_copy(update={"settlement_chain": policy, "chain_contexts": (other, selected)})
    )
    wire = encode_condition_supply(graph, evidence(), clocks=condition_wire_clocks())
    primary = wire["stablecoin_chain"]
    assert isinstance(primary, dict) and primary["chain"] == "chain"
    assert primary["supply_usd"] == "0" and primary["supply_chg_24h"] == "-10"
    assert wire["supply_in_evidence"] == "FALSE"
    assert wire["supply_evidence"] == {
        "status": "NO_POSITIVE_OBSERVATION",
        "available_inputs": ["CHAIN_LIQUIDITY"],
    }
    contexts = wire["chain_contexts"]
    assert isinstance(contexts, list) and len(contexts) == 2
    assert isinstance(contexts[0], dict) and contexts[0]["chain"] == "other"


def test_supply_composition_refuses_borrowed_clock_scope() -> None:
    clocks = condition_wire_clocks()
    wrong = type(clocks).model_validate(clocks.model_copy(update={"token_id": "OTHER"}))
    with pytest.raises(ValueError, match="another token"):
        encode_condition_supply(empty_supply_graph(), evidence(), clocks=wrong)


def test_actual_labeled_exchange_flow_preserves_its_positive_witness_and_source_extension() -> None:
    graph = empty_supply_graph()
    flows = wallet_flows()
    flow = type(flows.flows[0]).model_validate(
        flows.flows[0].model_copy(update={"classification": "TREASURY_INTERNAL"})
    )
    flows = type(flows).model_validate(flows.model_copy(update={"flows": (flow,)}))
    graph = type(graph).model_validate(graph.model_copy(update={"wallet_flows": flows}))
    lookup = SupplyCompositionEvidence(flow, unlock_schedule().upcoming[0])
    wire = encode_condition_supply(graph, lookup, clocks=condition_wire_clocks())
    assert wire["supply_in_evidence"] == "TRUE"
    assert wire["supply_evidence"] == {
        "status": "OBSERVED_POSITIVE",
        "available_inputs": ["WALLET_FLOWS"],
        "inventory": None,
        "wallet_flows": [{"source_observation_sha256": "b" * 64, "flow_index": 0}],
    }
    result = wire["flows"]
    assert isinstance(result, list) and len(result) == 1
    assert isinstance(result[0], dict)
    assert result[0]["class"] == "TREASURY_INTERNAL" and result[0]["qty"] == "10"
    assert result[0]["source_note"] == {"exact_extension": ["retained"]}
    assert lookup.flow_calls == 1
    result[0]["source_note"] = None
    assert lookup.flow_wire["source_note"] == {"exact_extension": ["retained"]}
