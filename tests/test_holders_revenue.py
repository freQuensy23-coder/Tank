"""Offline observed provider fixtures -> real truth -> frozen JSON -> deterministic card."""

import json
from copy import deepcopy

import pytest

from tank.analysis import cards as CARDS
from tank.analysis import truth as TRUTH
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


def render(truths, tokens=None):
    tokens = tokens or ["T"]
    event = {
        "statement": "A source reports a proposal.",
        "event_state": "PROPOSED",
        "thresholds": {"card_max_chars": 20000},
    }
    edges = [
        {
            "token_id": token,
            "role": "PRIMARY",
            "capture_class": "UNKNOWN",
            "causal_chain": "No mechanism evidenced",
        }
        for token in tokens
    ]
    decisions = [{"token_id": token, "action": "WAIT"} for token in tokens]
    return CARDS.compose_card(event, {}, edges, decisions, truths)


@pytest.mark.parametrize("value", ["0", "3460000.123456789"])
def test_actual_value_zero_and_exact_precision_freeze_then_card(value):
    row = captured({"adapter": {"total24h": value}})
    result = TRUTH.compute_market_truth(
        {"token_id": "T", "cutoff": iso(END), "instruments": [], "supply": [row]}
    )
    frozen = json.loads(json.dumps(result, default=str))
    row["protocols"]["adapter"]["dailyHoldersRevenue"]["total24h"] = "999999999"
    text = render([frozen])
    assert value + " USD/24h" in text
    assert "UNKNOWN" in text and "999999999" not in text
    assert frozen["supply"]["holders_revenue"]["coverage"]["24h"]["complete"] is True
    assert "DIRECT" not in text
    assert "IMPACT" not in text and "SECONDARIES" not in text


def test_field_coverage_does_not_trust_available_complete_aggregate():
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


def test_parent_children_values_are_preserved_separately_never_summed():
    result = metric([captured({"parent": {"total24h": "30"}, "child": {"total24h": "20"}})])
    text = render([{"token_id": "T", "supply": {"holders_revenue": result}}])
    assert "parent 30 USD/24h" in text and "child 20 USD/24h" in text
    assert "50 USD" not in text
    assert "PER_PROTOCOL_VALUES_NOT_SUMMED_PARENT_CHILD_SCOPE_MAY_OVERLAP" in result["inference_limits"]


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", "not-a-number", None])
def test_invalid_nonfinite_and_missing_are_unavailable(value):
    result = metric([captured({"a": {"total24h": value}})])
    assert result["protocols"][0]["periods"][0]["value"] is None
    assert not result["coverage"]["24h"]["complete"]


def test_stale_observation_is_dated_not_relabelled_current_and_future_never_shown():
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


def test_unavailable_protocol_and_chart_only_are_not_zero():
    row = captured({"a": {"totalDataChart": [[1, "8"]]}})
    row["protocols"]["b"] = {
        "dailyHoldersRevenue": {"available": False, "reason": "PROVIDER_METRIC_NOT_SUPPORTED"}
    }
    result = metric([row])
    assert result["coverage"]["24h"]["observed"] == 0
    assert result["protocols"][0]["chart_available"]
    assert result["protocols"][1]["reason"] == "PROVIDER_METRIC_NOT_SUPPORTED"
    assert "0 USD" not in render([{"token_id": "T", "supply": {"holders_revenue": result}}])


def test_display_set_stays_at_three_and_excludes_other_token_revenue():
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
async def test_real_supply_producer_to_computed_frozen_analysis(tmp_path):
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

        async def _raw(self, *args):
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
        {"token_id": "T", "cutoff": cutoff, "instruments": [], "supply": [row]}
    )
    await repo.put("ana.market_truth", "frozen", json.loads(json.dumps(truth, default=str)), immutable=True)
    frozen = await repo.get("ana.market_truth", "frozen")
    assert "0 USD/24h" in render([frozen])
    assert frozen["supply"]["holders_revenue"]["protocols"][0]["source_url"].endswith(
        "dataType=dailyHoldersRevenue"
    )
    assert frozen["supply"]["holders_revenue"]["source_observation_sha256"]


def test_chart_only_observation_uses_provider_timestamp_not_invented_24h():
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
def test_invalid_chart_timestamp_or_value_cannot_be_a_figure(stamp, value):
    result = metric([captured({"a": {"totalDataChart": [[stamp, value]]}})])
    assert result["protocols"][0]["latest_chart_point"] is None


def test_future_capture_receipt_cannot_supply_observed_revenue():
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


def test_provider_revenue_with_unknown_capture_never_authorizes_token_capture_claim():
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
    card = CARDS.compose_card(event, {}, edges, decisions, [truth])
    line = next(line for line in card.splitlines() if line.startswith("• Token capture:"))
    assert "T UNKNOWN" in line
    assert "DefiLlama reports holders revenue: adapter 123.456 USD/24h" in line
    assert context == before and edges[0]["capture_class"] == "UNKNOWN"
    assert "DIRECT" not in line and "INDIRECT" not in line


def test_compact_display_prefers_24h_preserving_30d_details_and_default_limit():
    observed = metric([captured({"adapter": {"total24h": "123.456", "total30d": "8765.432"}})])
    truths = [
        {"token_id": token, "supply": {"holders_revenue": deepcopy(observed)}} for token in ["A", "B", "C"]
    ]
    event = {"statement": "A source reports a proposal.", "event_state": "PROPOSED"}
    edges = [
        {
            "token_id": token,
            "role": "PRIMARY",
            "capture_class": "UNKNOWN",
            "causal_chain": "No mechanism evidenced",
        }
        for token in ["A", "B", "C"]
    ]
    card = CARDS.compose_card(
        event, {}, edges, [{"token_id": token, "action": "WAIT"} for token in ["A", "B", "C"]], truths
    )
    assert len(card) <= 3500
    assert card.count("123.456 USD/24h") == 3 and "8765.432" not in card
    assert observed["protocols"][0]["periods"][1]["value"] == "8765.432"
    line = next(line for line in card.splitlines() if line.startswith("• Token capture:"))
    assert "STALE" not in line and "PARTIAL" not in line and "receipt" not in line
    assert "coverage 24h" not in line and "not summed" not in line


def ordinary_full_card_inputs():
    event = {
        "statement": "Approval changes access.",
        "event_state": "APPROVED",
        "state_verified": True,
        "freshness": "FRESH",
        "public_at": "2026-10-18T12:30:00Z",
        "public_at_prec": "EXACT",
        "known_to_us_at": "2026-10-18T12:31:00Z",
        "known_to_us_at_prec": "EXACT",
        "claims": [
            {
                "subject": "World",
                "predicate": "approved",
                "object": "product",
                "accepted": True,
                "verified": True,
                "state_claim": "APPROVED",
                "evidence": [{"item_id": "source-proof", "quote": "World approved product"}],
            }
        ],
        "beneficiary_map": {"no_credible_secondary_reason": "No eligible dependency rail"},
    }
    assessment = {
        "class": "MAJOR",
        "criterion": "M2",
        "certainty": "HIGH",
        "dims": {
            key: {"value": value}
            for key, value in zip(
                [
                    "economic_directness",
                    "magnitude",
                    "permanence",
                    "distribution_expansion",
                    "mechanical_flows",
                    "surprise",
                    "implementation",
                ],
                [8, 7, 6, 5, 4, 3, 2],
                strict=True,
            )
        },
    }
    beneficiary = {
        "token_id": "world:WLD",
        "entity_name": "World",
        "role": "PRIMARY",
        "credible": True,
        "capture_class": "DIRECT",
        "causal_chain": "Required token gas",
        "mechanism_evidence": ["mechanism"],
    }
    decision = {
        "token_id": "world:WLD",
        "action": "WAIT",
        "thesis_grade": "A",
        "trade_grade": "B",
        "outlook": {
            "direction": "BULLISH",
            "potential": "MEDIUM",
            "best_horizon": "H4",
            "horizon_reason": "event window",
            "confidence": "HIGH",
            "scenarios": {
                "base": "Product remains available",
                "upside": "Deployment verified",
                "failure": "Approval reversed",
            },
        },
        "conditions": [
            {"role": "UPGRADE", "human_text": "Spot BUY persists"},
            {"role": "DOWNGRADE", "human_text": "Spot SELL persists"},
        ],
    }
    truth = {
        "token_id": "world:WLD",
        "cutoff": "2026-10-18T12:31:00Z",
        "move_class": "ORGANIC_MIXED",
        "data_confidence": {"class": "HIGH"},
        "primary_window": "W4H",
        "decisive_facts": [
            {"metric": "price", "value": ".05", "unit": "DECIMAL_RETURN", "window": "W4H"},
            {"metric": "spot.cvd", "value": "2000", "unit": "NATIVE_TOKEN", "window": "W4H"},
        ],
        "relative": {"repricing_band": "EARLY", "windows": {"EVENT": {"r_raw": ".03", "r_btc_adj": ".01"}}},
    }
    return event, assessment, beneficiary, decision, truth


def test_full_ordinary_three_decision_tokens_with_observed_revenue_fit_default_limit():
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
    card = CARDS.compose_card(
        event, assessment, edges, decisions, truths, why=["The approval is confirmed", "not estimable"]
    )
    assert len(card) <= 3500
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
    import pytest

    with pytest.raises(ValueError, match="CARD_LENGTH"):
        CARDS.compose_card(dict(event, statement="x" * 3500), assessment, edges, decisions, truths)


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
def test_missing_holder_revenue_renders_bound_safe_reason(reason, label):
    frozen = {"status": "UNAVAILABLE", "protocols": [], "reason": reason}
    text = CARDS._holders_revenue_text({"supply": {"holders_revenue": frozen}})
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
def test_missing_protocol_figure_renders_safe_n_a_reason(reason, label):
    row = captured({"adapter": {}})
    row["protocols"]["adapter"]["dailyHoldersRevenue"].update(available=False, reason=reason)
    frozen = metric([row])
    text = CARDS._holders_revenue_text({"supply": {"holders_revenue": frozen}})
    assert "adapter n/a (" + label + ")" in text
    assert "secret" not in text
    assert frozen["protocols"][0]["reason"] == reason


def test_scalar_clock_is_observation_not_provider_period_end():
    observed = metric([captured({"adapter": {"total24h": "8"}})])
    text = CARDS._holders_revenue_text({"supply": {"holders_revenue": observed}})
    assert "8 USD/24h" in text and "observed 2026-10-19 12:00Z" in text
    assert "as of" not in text and "as-of" not in text and "receipt" not in text
