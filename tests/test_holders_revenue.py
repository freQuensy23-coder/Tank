"""Offline observed provider fixtures -> real truth -> frozen JSON -> deterministic card."""

import json
from copy import deepcopy
from pathlib import Path

import pytest
from card_wire_support import wire_card_inputs
from score_fixtures import unassessed_score_dimensions
from truth_support import fixture_truth_config

from tank.analysis import cards as CARDS
from tank.analysis import truth as TRUTH
from tank.analysis.card_codec import (
    card_inputs_from_records,
    card_registry_from_record,
    holders_revenue_from_record,
)
from tank.analysis.card_contracts import PublicationNarrative, PublicationPolicy
from tank.market.supply import SupplyCapture
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import iso, parse_time

END = parse_time("2026-10-19T12:00:00Z")


def captured(protocols, **extra):
    return dict(
        kind="FUNDAMENTALS",
        token_id="T",
        ts=iso(END),
        ts_prec="EXACT",
        available=True,
        identity_evidence={"match_method": "EXACT_CONTRACT_OR_GECKO_AND_SYMBOL"},
        protocols={
            slug: {
                "dailyHoldersRevenue": dict(
                    available=True,
                    source_url="https://api.llama.fi/summary/fees/" + slug + "?dataType=dailyHoldersRevenue",
                    **values,
                )
            }
            for slug, values in protocols.items()
        },
        **extra,
    )


def metric(rows):
    return TRUTH.supply_truth({"supply": rows}, END, END)["holders_revenue"]


def render(truths, tokens=None, *, narrative=None, event_claim_text: str | None = None):
    tokens = (["T"] if tokens is None else tokens)[:3]
    event, assessment, beneficiary, decision, market = wire_card_inputs()
    event.update(statement="A source reports a proposal.", token_labels={})
    if event_claim_text is not None:
        event["claims"][0]["object"] = event_claim_text
    assessment["dims"] = unassessed_score_dimensions()
    edges, decisions, markets = [], [], []
    supplied = {row["token_id"]: row for row in truths}
    for token in tokens:
        edges.append(
            dict(
                beneficiary,
                token_id=token,
                entity_name=None,
                capture_class="UNKNOWN",
                causal_chain="No mechanism evidenced",
                mechanism_evidence=[],
            )
        )
        decisions.append(dict(decision, token_id=token))
        row = supplied[token]
        if "move_class" in row:
            markets.append(row)
        else:
            markets.append(dict(market, token_id=token, supply=row["supply"]))
    inputs = card_inputs_from_records(
        event,
        assessment,
        edges,
        decisions,
        markets,
        registry=card_registry_from_record({"tokens": [{"token_id": token} for token in tokens]}),
        narrative=PublicationNarrative(
            kind="FULL",
            what="A source reports a proposal.",
            verdict="A proposal is reported.",
            why=("The source reports a proposal", "Token capture is unverified"),
        )
        if narrative is None
        else narrative,
        policy=PublicationPolicy(update_rate_limit_s=900),
    )
    return CARDS.compose_card(inputs)


@pytest.mark.parametrize("value", ["0", "3460000.123456789"])
def test_actual_value_zero_and_exact_precision_freeze_then_card(value) -> None:
    row = captured({"adapter": {"total24h": value}})
    result = TRUTH.compute_market_truth(
        {"token_id": "T", "cutoff": iso(END), "instruments": [], "supply": [row]},
        config=fixture_truth_config(),
    )
    frozen = json.loads(json.dumps(result, default=str))
    row["protocols"]["adapter"]["dailyHoldersRevenue"]["total24h"] = "999999999"
    text = render([frozen])
    assert value + " USD/24h" in text
    assert "UNKNOWN" in text and "999999999" not in text
    assert frozen["supply"]["holders_revenue"]["coverage"]["24h"]["complete"] is True
    assert "DIRECT" not in text
    assert "IMPACT" not in text and "SECONDARIES" not in text


def test_field_coverage_does_not_trust_available_complete_aggregate() -> None:
    row = captured(
        {"a": {"total24h": "11", "total30d": "30"}, "b": {"total30d": "90", "totalDataChart": [[1, "4"]]}}
    )
    row["metrics"] = {"dailyHoldersRevenue": {"available": True, "complete": True, "total24h": "11"}}
    result = metric([row])
    assert result["coverage"]["24h"] == {"observed": 1, "expected": 2, "complete": False}
    assert result["coverage"]["30d"]["complete"] is True
    text = render([{"token_id": "T", "supply": {"holders_revenue": result}}])
    assert "b 90 USD/30d" in text and "partial 24h coverage" in text
    assert "101 USD" not in text


def test_parent_children_values_are_preserved_separately_never_summed() -> None:
    result = metric([captured({"parent": {"total24h": "30"}, "child": {"total24h": "20"}})])
    text = render([{"token_id": "T", "supply": {"holders_revenue": result}}])
    assert "parent 30 USD/24h" in text and "child 20 USD/24h" in text
    assert "50 USD" not in text
    assert "PER_PROTOCOL_VALUES_NOT_SUMMED_PARENT_CHILD_SCOPE_MAY_OVERLAP" in result["inference_limits"]


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", "not-a-number", None])
def test_invalid_nonfinite_and_missing_are_unavailable(value) -> None:
    result = metric([captured({"a": {"total24h": value}})])
    assert result["protocols"][0]["periods"][0]["value"] is None
    assert not result["coverage"]["24h"]["complete"]


def test_stale_observation_is_dated_not_relabelled_current_and_future_never_shown() -> None:
    from datetime import timedelta

    stale = captured({"a": {"total24h": "7"}})
    stale["ts"] = iso(END - timedelta(seconds=21601))
    result = metric([stale, dict(captured({"a": {"total24h": "98765"}}), ts=iso(END + timedelta(seconds=1)))])
    text = render([{"token_id": "T", "supply": {"holders_revenue": result}}])
    assert "7 USD/24h" in text and "stale" in text and "2026-10-19 05:59Z" in text
    assert "98765" not in text
    future_only = metric([dict(stale, ts=iso(END + timedelta(seconds=1)))])
    assert future_only["reason"] == "ONLY_FUTURE_FUNDAMENTALS"
    assert not future_only["protocols"]


def test_unavailable_protocol_and_chart_only_are_not_zero() -> None:
    row = captured({"a": {"totalDataChart": [[1, "8"]]}})
    row["protocols"]["b"] = {
        "dailyHoldersRevenue": {"available": False, "reason": "PROVIDER_METRIC_NOT_SUPPORTED"}
    }
    result = metric([row])
    assert result["coverage"]["24h"]["observed"] == 0
    assert result["protocols"][0]["chart_available"]
    assert result["protocols"][1]["reason"] == "PROVIDER_METRIC_NOT_SUPPORTED"
    assert "0 USD" not in render([{"token_id": "T", "supply": {"holders_revenue": result}}])


def test_display_set_stays_at_three_and_excludes_other_token_revenue() -> None:
    result = metric([captured({"a": {"total24h": "3"}})])
    truths = [
        {"token_id": token, "supply": {"holders_revenue": deepcopy(result)}} for token in ["A", "B", "C", "D"]
    ]
    truths[-1]["supply"]["holders_revenue"]["protocols"][0]["periods"][0]["value"] = "88888"
    text = render(truths, ["A", "B", "C", "D"])
    capture_line = next(line for line in text.splitlines() if line.startswith("• Token capture:"))
    assert capture_line.count("DefiLlama reports holders revenue:") == 3
    assert "88888" not in capture_line


@pytest.mark.integration
async def test_real_supply_producer_to_computed_frozen_analysis(tmp_path: Path) -> None:
    repo = MemoryRepository()
    await repo.open()
    config = tmp_path / "config"
    config.mkdir()
    (config / "registry.yaml").write_text("tokens: []\ninstruments: []\n")

    class Market:
        async def _put(self, collection, key, row):
            return await repo.put(collection, key, row)

        async def _request(self, method, url, **kwargs):
            kind = kwargs["params"]["dataType"]
            return {"total24h": "0" if kind == "dailyHoldersRevenue" else "9", "total30d": "17"}

        async def _raw(self, *args) -> None:
            pass

    market = Market()
    market.repo = repo
    market.settings = Settings(
        _env_file=None, config_dir=config, data_dir=tmp_path / "data", etherscan_api_key=None
    )
    capture = SupplyCapture(market)
    token = {
        "token_id": "T",
        "defillama_protocol_slugs": ["actual-adapter"],
        "fundamentals_evidence": {"verified_metric_binding": "dailyHoldersRevenue"},
    }
    row = await capture.fundamentals(token)
    cutoff = row["ts"]
    truth = TRUTH.compute_market_truth(
        {"token_id": "T", "cutoff": cutoff, "instruments": [], "supply": [row]}, config=fixture_truth_config()
    )
    await repo.put("ana.market_truth", "frozen", json.loads(json.dumps(truth, default=str)), immutable=True)
    frozen = await repo.get("ana.market_truth", "frozen")
    assert "0 USD/24h" in render([frozen])
    assert frozen["supply"]["holders_revenue"]["protocols"][0]["source_url"].endswith(
        "dataType=dailyHoldersRevenue"
    )
    assert frozen["supply"]["holders_revenue"]["source_observation_sha256"]


def test_chart_only_observation_uses_provider_timestamp_not_invented_24h() -> None:
    row = captured(
        {
            "a": {
                "totalDataChart": [
                    [int(END.timestamp()) - 10, "0"],
                    [int(END.timestamp()) + 1, "777777"],
                    [int(END.timestamp()) - 20, "NaN"],
                ]
            }
        }
    )
    result = metric([row])
    point = result["protocols"][0]["latest_chart_point"]
    assert point["value"] == "0" and point["provider_period"] == "UNSPECIFIED"
    text = render([{"token_id": "T", "supply": {"holders_revenue": result}}])
    assert "0 USD chart point " in text and "period unknown" in text
    assert "777777" not in text and "0 USD/24h" not in text
    assert "partial 24h coverage" in text


@pytest.mark.parametrize(
    "stamp,value",
    [(True, "8"), ("NaN", "8"), ("Infinity", "8"), ("invalid", "8"), (1, True), (1, "Infinity")],
)
def test_invalid_chart_timestamp_or_value_cannot_be_a_figure(stamp, value) -> None:
    result = metric([captured({"a": {"totalDataChart": [[stamp, value]]}})])
    assert result["protocols"][0]["latest_chart_point"] is None


def test_future_capture_receipt_cannot_supply_observed_revenue() -> None:
    from datetime import timedelta

    old = captured({"a": {"total24h": "9"}})
    old["received_ts"] = iso(END)
    late = captured({"a": {"total24h": "999999"}})
    late["received_ts"] = iso(END + timedelta(seconds=1))
    result = metric([old, late])
    assert result["protocols"][0]["periods"][0]["value"] == "9"
    assert result["capture_received_at"] == iso(END)
    assert result["capture_availability"] == "KNOWN"
    assert "999999" not in render([{"token_id": "T", "supply": {"holders_revenue": result}}])


def test_provider_revenue_with_unknown_capture_never_authorizes_token_capture_claim() -> None:
    truth = {
        "token_id": "T",
        "supply": {"holders_revenue": metric([captured({"adapter": {"total24h": "123.456"}})])},
    }
    event = {"statement": "A source reports a proposal.", "event_state": "PROPOSED"}
    edges = [
        {
            "token_id": "T",
            "role": "PRIMARY",
            "capture_class": "UNKNOWN",
            "causal_chain": "No mechanism evidenced",
            "mechanism_evidence": [],
        }
    ]
    decisions = [{"token_id": "T", "action": "WAIT"}]
    context = {
        "event": event,
        "assessment": {},
        "beneficiaries": edges,
        "decisions": decisions,
        "market_truths": [truth],
    }
    before = deepcopy(context)
    card = render([truth])
    line = next(line for line in card.splitlines() if line.startswith("• Token capture:"))
    assert "T UNKNOWN" in line
    assert "DefiLlama reports holders revenue: adapter 123.456 USD/24h" in line
    assert context == before and edges[0]["capture_class"] == "UNKNOWN"
    assert "DIRECT" not in line and "INDIRECT" not in line


def test_compact_display_prefers_24h_preserving_30d_details() -> None:
    observed = metric([captured({"adapter": {"total24h": "123.456", "total30d": "8765.432"}})])
    truths = [
        {"token_id": token, "supply": {"holders_revenue": deepcopy(observed)}} for token in ["A", "B", "C"]
    ]
    card = render(truths, ["A", "B", "C"])
    assert card.count("123.456 USD/24h") == 3 and "8765.432" not in card
    assert observed["protocols"][0]["periods"][1]["value"] == "8765.432"
    line = next(line for line in card.splitlines() if line.startswith("• Token capture:"))
    assert "STALE" not in line and "PARTIAL" not in line and "receipt" not in line
    assert "coverage 24h" not in line and "not summed" not in line


def ordinary_full_card_inputs():
    return wire_card_inputs()


def test_full_ordinary_three_decision_tokens_with_observed_revenue() -> None:
    event, assessment, beneficiary, decision, truth = ordinary_full_card_inputs()
    edges, decisions, truths = [], [], []
    for token in ["A", "B", "C"]:
        edges.append(dict(deepcopy(beneficiary), token_id=token, capture_class="UNKNOWN"))
        decisions.append(dict(deepcopy(decision), token_id=token))
        truths.append(
            dict(
                deepcopy(truth),
                token_id=token,
                supply={"holders_revenue": metric([captured({"adapter": {"total24h": "123.456"}})])},
            )
        )
    card = render(truths, ["A", "B", "C"])
    assert card.count("123.456 USD/24h") == 3
    assert card.count("──") == 6
    for section in [
        "WHY IT MATTERS",
        "• What changed:",
        "• Economic effect:",
        "• Token capture:",
        "WHO:",
        "MARKET TRUTH:",
        "OUTLOOK:",
        "WHAT TO DO:",
        "WHAT CHANGES THE ACTION:",
        "CONFIDENCE:",
    ]:
        assert section in card
    assert "IMPACT:" not in card and "SECONDARIES:" not in card
    long_statement = "x" * 3500
    long_card = render(truths, ["A", "B", "C"], event_claim_text=long_statement)
    assert long_statement in long_card and len(long_card) > 5000


@pytest.mark.parametrize(
    "reason,label",
    [
        ("NO_CAPTURED_FUNDAMENTALS", "not captured"),
        ("ONLY_FUTURE_FUNDAMENTALS", "not available by cutoff"),
        ("NO_SUPPORTED_PUBLIC_FEES_ADAPTER", "provider metric unsupported"),
        ("NO_PUBLIC_FEES_ADAPTER_MATCHING_TOKEN_IDENTITY", "no verified provider match"),
        ("http error https://provider.invalid/secret?key=hidden", "incomplete provider data"),
    ],
)
def test_missing_holder_revenue_renders_bound_safe_reason(reason, label) -> None:
    frozen = {"status": "UNAVAILABLE", "protocols": [], "reason": reason}
    text = CARDS.holders_revenue_text(holders_revenue_from_record(frozen))
    assert text == "holders revenue: n/a (" + label + ")"
    assert "hidden" not in text and "http" not in text
    assert frozen["reason"] == reason


@pytest.mark.parametrize(
    "reason,label",
    [
        ("PROVIDER_METRIC_NOT_SUPPORTED", "provider metric unsupported"),
        (None, "incomplete provider data"),
        ("raw provider exception secret", "incomplete provider data"),
    ],
)
def test_missing_protocol_figure_renders_safe_n_a_reason(reason, label) -> None:
    row = captured({"adapter": {}})
    row["protocols"]["adapter"]["dailyHoldersRevenue"].update(available=False, reason=reason)
    frozen = metric([row])
    text = CARDS.holders_revenue_text(holders_revenue_from_record(frozen))
    assert "adapter n/a (" + label + ")" in text
    assert "secret" not in text
    assert frozen["protocols"][0]["reason"] == reason


def test_scalar_clock_is_observation_not_provider_period_end() -> None:
    observed = metric([captured({"adapter": {"total24h": "8"}})])
    text = CARDS.holders_revenue_text(holders_revenue_from_record(observed))
    assert "8 USD/24h" in text and "observed 2026-10-19 12:00Z" in text
    assert "as of" not in text and "as-of" not in text and "receipt" not in text


def test_fundamentals_as_of_preserves_captured_clock_precision_and_unknown_reason() -> None:
    row = captured({"a": {"total24h": "5"}})
    row.update(ts_prec="COLLECTION_ONLY", ts_prec_reason=None)
    observed = metric([row])
    assert observed["as_of"] == row["ts"]
    assert observed["as_of_prec"] == "COLLECTION_ONLY"
    assert observed["as_of_prec_reason"] is None
    row.update(ts_prec=None, ts_prec_reason="LEGACY_SOURCE_PRECISION_NOT_CAPTURED")
    unknown = metric([row])
    assert unknown["as_of"] == row["ts"]
    assert unknown["as_of_prec"] is None
    assert unknown["as_of_prec_reason"] == "LEGACY_SOURCE_PRECISION_NOT_CAPTURED"
