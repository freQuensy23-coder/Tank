"""Render validated publication facts without allocating prose, clocks or policy."""

import html
import re
from collections.abc import Mapping
from datetime import datetime, timedelta
from decimal import Decimal
from urllib.parse import urlsplit

from tank.analysis.card_contracts import (
    ActiveCardDecision,
    BeneficiaryLayer,
    CardDataConfidence,
    CardDecision,
    CardExclusion,
    CardInputs,
    CardLengthDiagnostics,
    CardLengthSection,
    CardMapping,
    CardMarket,
    CardRevenue,
    CardSpotVenue,
    CardSubject,
    ChartRevenueProtocol,
    InactiveCardDecision,
    PeriodRevenueProtocol,
)
from tank.analysis.decisions import POSITIVE_ACTIONS
from tank.telegram_rich import score_summary
from tank.time_precision import Timestamp, bucket_duration, interval_bounds
from tank.util import parse_epoch, parse_iso_time


def token_name(token: str | None, labels: Mapping[str, str]) -> str:
    """Name a project and ticker without confusing tokens sharing a symbol."""
    if token is None:
        return "n/a (token unavailable)"
    if token in labels:
        return labels[token]
    if token.startswith("UNRESOLVED:"):
        return token.removeprefix("UNRESOLVED:") + " (unresolved)"
    project, _, ticker = token.rpartition(":")
    return project + " (" + ticker + ")" if project else token


def telegram_card_html(text: str) -> str:
    """Add visual hierarchy while escaping every agent-authored character."""
    labels = (
        "WHAT",
        "VERIFIED",
        "NOT VERIFIED",
        "WHO",
        "MARKET TRUTH",
        "OUTLOOK",
        "WHAT TO DO",
        "WHAT CHANGES THE ACTION",
        "CONFIDENCE",
    )
    rendered: list[str] = []
    for index, line in enumerate(text.splitlines()):
        escaped = html.escape(line)
        if index == 0 or line.startswith("── ") or line == "WHY IT MATTERS":
            escaped = "<b>" + escaped + "</b>"
        else:
            label = next((label for label in labels if line.startswith(label + ":")), None)
            if label is not None:
                escaped = "<b>" + label + ":</b>" + escaped[len(label) + 1 :]
        rendered.append(escaped)
    return "\n".join(rendered)


def _na(value: str | Decimal | None, reason: str) -> str:
    return str(value) if value is not None else "n/a (" + reason.replace("_", " ").lower() + ")"


def _clock(stamp: Timestamp, *, publication: bool) -> str:
    if stamp.value is None:
        return "n/a (timestamp unavailable)"
    point = parse_iso_time(stamp.value)
    if stamp.precision is None:
        return "n/a (time precision unknown)"
    if stamp.precision == "COLLECTION_ONLY":
        clock = point.strftime("%H:%MZ")
        return (
            "n/a (publication time unavailable; collected " + clock + ")"
            if publication
            else clock + " (collection)"
        )
    if stamp.precision == "EXACT":
        return point.strftime("%H:%MZ")
    lo, hi = interval_bounds(stamp)
    if lo is None or hi is None:
        return "n/a (bucket edge unknown; " + stamp.precision + ")"
    duration = bucket_duration(stamp.precision)
    if duration == timedelta(days=1) and lo.time() == lo.time().min:
        return lo.strftime("%Y-%m-%d") + " (day precision)"

    def bound(value: datetime) -> str:
        if duration >= timedelta(days=1) or lo.date() != hi.date():
            return value.strftime("%Y-%m-%d %H:%MZ")
        if duration >= timedelta(minutes=1):
            return value.strftime("%H:%MZ")
        return value.strftime("%H:%M:%S.%f").rstrip("0").rstrip(".") + "Z"

    return bound(lo) + "–" + bound(hi) + " (" + stamp.precision + ", end exclusive)"


def _percent(value: Decimal | None) -> str:
    return "n/a (not available)" if value is None else format(value * 100, ".2f") + "%"


def _unknown(confidence: CardDataConfidence) -> str:
    limiting = confidence.limiting
    if limiting is None:
        return "none recorded" if confidence.classification == "HIGH" else "n/a (limiting input unavailable)"
    labels = {
        "spot_coverage": "Spot venue coverage is incomplete",
        "derivatives_coverage": "Synchronized native OI coverage is incomplete",
        "freshness": "Some inputs are outside their observation cadence",
    }
    label = next(
        (text for key, text in labels.items() if key in limiting.condition.lower()),
        limiting.condition.replace("_", " ").capitalize(),
    )
    if limiting.reason is None or limiting.reason in {
        "INCOMPLETE",
        "NOT_AVAILABLE",
        "NOT_ESTIMABLE",
        "UNKNOWN",
    }:
        return label
    return label + ": " + limiting.reason.replace("_", " ").lower()


def _main_unknown(decision: ActiveCardDecision, truth: CardMarket) -> str:
    if decision.outlook.confidence == "HIGH":
        return _unknown(truth.confidence)
    limiting = decision.outlook.limitation
    if limiting.text is not None:
        return limiting.text
    text = _unknown(truth.confidence)
    if limiting.input == "data" or text != "none recorded":
        return text
    return "n/a (limiting input not recorded)"


def _why_content(value: str, label: str) -> str:
    return re.sub(r"^\s*" + re.escape(label) + r"\s*:\s*", "", value, flags=re.IGNORECASE)


def _exclusion_reason(excluded: CardExclusion) -> str:
    reasons = list(excluded.source_reasons)
    if excluded.detail is not None:
        reasons.insert(0, excluded.detail)
    if excluded.missing_metrics:
        reasons.append("missing " + ", ".join(name.replace("_", " ") for name in excluded.missing_metrics))
    if reasons:
        return "; ".join(value.replace("_", " ").lower() for value in reasons)
    return (
        "coverage incomplete; specific cause unavailable"
        if excluded.reason == "INCOMPLETE"
        else excluded.reason
    )


def _holders_revenue_text(revenue: CardRevenue) -> str:
    parts: list[str] = []
    scalar = False
    for protocol in revenue.protocols:
        if isinstance(protocol, PeriodRevenueProtocol):
            scalar = True
            value = str(protocol.period.value) + " " + protocol.unit + "/" + protocol.period.provider_period
        elif isinstance(protocol, ChartRevenueProtocol):
            clock = parse_epoch(protocol.point.epoch_seconds, unit="s").strftime("%Y-%m-%d %H:%MZ")
            value = (
                str(protocol.point.value)
                + " "
                + protocol.unit
                + " chart point "
                + clock
                + " (period unknown)"
            )
        else:
            reason = protocol.reason
            if reason in {"PROVIDER_METRIC_NOT_SUPPORTED", "NO_SUPPORTED_PUBLIC_FEES_ADAPTER"}:
                explanation = "provider metric unsupported"
            elif reason == "NO_PUBLIC_FEES_ADAPTER_MATCHING_TOKEN_IDENTITY":
                explanation = "no verified provider match"
            elif reason == "ONLY_FUTURE_FUNDAMENTALS":
                explanation = "not available by cutoff"
            else:
                explanation = "incomplete provider data"
            value = "n/a (" + explanation + ")"
        parts.append(protocol.protocol + " " + value)
    notes: list[str] = []
    if scalar:
        stamp = revenue.observed_at
        if stamp.value is None:
            raise ValueError("Rendered fundamentals require their actual observation clock")
        if stamp.precision in {"EXACT", "COLLECTION_ONLY"}:
            observed = parse_iso_time(stamp.value).strftime("%Y-%m-%d %H:%MZ")
            if stamp.precision == "COLLECTION_ONLY":
                observed += " (collection)"
        else:
            observed = _clock(stamp, publication=False)
        notes.append("observed " + observed)
    if revenue.stale:
        notes.append("stale")
    if not revenue.complete_24h:
        notes.append("partial 24h coverage")
    return (
        "DefiLlama reports holders revenue: "
        + "; ".join(parts)
        + (" (" + "; ".join(notes) + ")" if notes else "")
    )


def _volume_share(row: CardSpotVenue) -> Decimal:
    if row.volume_share is None:
        raise ValueError("A directional venue requires its actual volume share")
    return row.volume_share


def _market_breadth(truth: CardMarket, token: str) -> list[str]:
    lines: list[str] = []
    derivatives = truth.derivatives
    window = derivatives.window.removeprefix("W").lower()
    ticker = token.rsplit(":", 1)[-1]
    venues: list[str] = []
    for row in derivatives.venues:
        text = row.venue + " " + row.classification
        if row.native_change is not None:
            text += " " + format(row.native_change * 100, "+.2f") + "% in " + ticker
        attribution = row.attribution
        if attribution is not None:
            text += (
                " (position share "
                + format(attribution.position_share * 100, ".1f")
                + "% of the OI value change"
            )
            if attribution.price_markup != 0:
                text += "; price-markup component present"
            text += ")"
        venues.append(text)
    if venues and not any("by venue:" in fact for fact in truth.decisive_facts):
        lines.append(window + ": open interest by venue: " + "; ".join(venues))
    sync = derivatives.synchronized
    if sync is not None and sync.native_change is not None and sync.native_change < 0:
        returns = truth.relative.returns_by_window.get(derivatives.window)
        move = returns.raw if returns is not None else None
        if move is None:
            during = "; price move n/a (not available for this window)"
        elif move > 0:
            during = " while price rose " + format(move * 100, "+.2f") + "%"
        else:
            during = "; price " + format(move * 100, "+.2f") + "%"
        match sync.native_vs_normal:
            case "BELOW":
                verdict = ": positions closing"
            case "WITHIN":
                verdict = "; within its normal range"
            case _:
                verdict = "; normal range n/a (not available)"
        lines.append(
            window
            + ": open positions (synchronized native OI) "
            + format(sync.native_change * 100, "+.2f")
            + "% in "
            + ticker
            + during
            + verdict
        )
    rows = truth.spot.complete_venues
    if rows:

        def side(sign: int) -> str:
            chosen = sorted(
                (row for row in rows if row.sign == sign and row.volume_share is not None),
                key=_volume_share,
                reverse=True,
            )
            values: list[str] = []
            for row in chosen:
                if row.volume_share is None:
                    raise ValueError("A directional venue requires its actual volume share")
                values.append(row.venue + " " + format(row.volume_share * 100, ".1f") + "%")
            return ", ".join(values) if values else "none"

        lines.append(
            truth.spot.window.removeprefix("W").lower()
            + ": spot venues net buying: "
            + side(1)
            + "; net selling: "
            + side(-1)
            + " (share of spot volume)"
        )
    return lines


def _market_context(truth: CardMarket, decision: ActiveCardDecision) -> str:
    context = truth.relative.context
    if context.state is None:
        reason = "BTC normal range unavailable" if context.normal_return_range is None else "not computed"
        text = "Market context: n/a (" + reason + ")"
    else:
        text = "Market context: " + context.state
        if context.btc_return_24h is not None:
            text += " — BTC 24h " + format(context.btc_return_24h * 100, "+.2f") + "%"
            normal = context.normal_return_range
            if normal is not None:
                text += (
                    " vs normal "
                    + format(normal.lower * 100, "+.2f")
                    + "% to "
                    + format(normal.upper * 100, "+.2f")
                    + "%"
                )
    if (
        any(reason.startswith("CONTEXT_") for reason in decision.trade_reasons)
        and decision.raw_trade_grade != decision.trade_grade
    ):
        text += (
            " · Trade "
            + decision.raw_trade_grade
            + " → "
            + decision.trade_grade
            + " for market context "
            + _na(context.state, "not computed")
        )
    elif context.state in {"HEADWIND", "STRESS"} and decision.raw_trade_grade == "D":
        text += " · Trade not lowered: already D"
    return text


def _subject_name(subject: CardSubject, labels: Mapping[str, str]) -> str:
    if subject.token_id is not None:
        return token_name(subject.token_id, labels)
    if subject.entity_name is not None:
        return subject.entity_name
    if subject.entity_id is None:
        raise ValueError("A displayed subject requires its actual identity")
    return subject.entity_id


def _who(inputs: CardInputs) -> list[str]:
    mapping = inputs.mapping
    if not isinstance(mapping, CardMapping):
        raise ValueError("Full analysis requires its actual beneficiary mapping")
    edges = inputs.display_subjects()
    who: list[str] = []
    for edge in edges:
        item = edge.role.lower() + " " + _subject_name(edge, inputs.event.token_labels)
        if edge.role == "SECONDARY":
            item += (
                " ("
                + _na(edge.layer, "layer unavailable")
                + ", "
                + ("" if edge.integration_verified else "unverified ")
                + _na(edge.integration_state, "integration state unavailable")
                + ")"
            )
        if edge.outside_scope is not None:
            item += " — outside the token universe, no market block"
        who.append(item)
    if not any(edge.role == "SECONDARY" and edge.credible for edge in edges):
        reason = mapping.no_credible_secondary_reason
        generic = "No SECONDARY edge satisfies the credibility requirements in §4.7."
        if reason is not None and reason.startswith(generic):
            reason = reason[len(generic) :].strip()
        who.append("secondary none" + (" — " + reason if reason is not None else ""))
    lines = ["", "WHO: " + (" · ".join(who) if who else "n/a (no beneficiary recorded)")]
    if not any(edge.role == "SECONDARY" for edge in edges):
        names: dict[BeneficiaryLayer, str] = {
            "ISSUER_PRODUCT": "Issuer / product",
            "VENUE_DEX": "Exchanges / DEX",
            "SETTLEMENT_L1_L2": "Settlement L1 / L2",
            "LIQUIDITY_EXECUTION": "Liquidity / execution",
            "LENDING_COLLATERAL": "Lending / collateral",
            "ORACLE_DATA": "Oracles / data",
            "INTEROPERABILITY": "Interoperability",
            "CUSTODY_COMPLIANCE": "Custody / compliance",
        }
        for layer, label in names.items():
            lines.append("• " + label + ": " + mapping.layer_explanations[layer])
    return lines


def _token_block(
    inputs: CardInputs, token: str, decision: CardDecision, *, role: str, least_repriced: bool
) -> list[str]:
    name = token_name(token, inputs.event.token_labels)
    if isinstance(decision, InactiveCardDecision):
        inactive_reason = (
            "token removed from current beneficiary map"
            if decision.reason == "REMOVED_FROM_ANALYSIS"
            else "call expired"
        )
        return [name + ": No longer following: " + inactive_reason + "."]
    truth = inputs.market_by_token[token]
    move_label = (
        "Possible passive spot absorption — unconfirmed"
        if truth.move == "PASSIVE_SPOT_ABSORPTION" and not truth.absorption_confirmed
        else truth.move.replace("_", " ")
    )
    facts = truth.decisive_facts[:2]
    market = (
        "MARKET TRUTH: "
        + move_label
        + "\n"
        + ("\n".join("• " + fact for fact in facts) if facts else "Market data incomplete.")
    )
    relative = truth.relative
    event_return = relative.returns_by_window.get("EVENT")
    if (
        relative.repricing is not None
        or event_return is not None
        and (event_return.raw is not None or event_return.btc_adjusted is not None)
    ):
        raw = event_return.raw if event_return is not None else None
        adjusted = event_return.btc_adjusted if event_return is not None else None
        reason = event_return.btc_adjusted_reason if event_return is not None else None
        net = (
            " (BTC is the benchmark)"
            if reason == "NOT_APPLICABLE_BENCHMARK"
            else "; " + _percent(adjusted) + " net BTC"
        )
        market += (
            "\nPriced: "
            + _na(relative.repricing, "not available")
            + " · EVENT "
            + _percent(raw)
            + " raw"
            + net
        )
    market += "".join("\n• " + line for line in _market_breadth(truth, token))
    if truth.unresolved:
        market += "\nUnresolved: " + ", ".join(truth.unresolved)
    exclusions: dict[str, list[str]] = {}
    for entry in truth.spot.excluded:
        exclusions.setdefault(_exclusion_reason(entry), []).append(entry.venue)
    if exclusions:
        market += "\nExcluded: " + "; ".join(
            ", ".join(venues) + " — " + reason for reason, venues in exclusions.items()
        )
    outlook = decision.outlook
    if outlook.horizon is None:
        horizon = "n/a (horizon unavailable)"
    else:
        horizon = {"H1": "1h", "H4": "4h", "H12": "12h", "H24": "24h", "D3": "3d", "D7": "7d"}[
            outlook.horizon
        ]
    outlook_line = "OUTLOOK: " + outlook.direction + " · " + outlook.potential + " · best " + horizon
    scenarios = outlook.scenarios
    for label, value, reason in (
        ("Base", scenarios.base, scenarios.base_reason),
        ("Upside", scenarios.upside, scenarios.upside_reason),
        ("Failure", scenarios.failure, scenarios.failure_reason),
    ):
        outlook_line += (
            "\n• " + label + ": " + _na(value, reason if reason is not None else "scenario unavailable")
        )
    conditions: list[str] = []
    for condition_role, symbol in (("UPGRADE", "↑"), ("DOWNGRADE", "↓")):
        condition = next((value for value in decision.conditions if value.role == condition_role), None)
        text = (
            "n/a (condition unavailable)"
            if condition is None
            else condition.human_text.replace("two consecutive hourly windows", "two hourly windows")
            .replace("Spot BUY with most volume buying", "Majority-volume spot BUY")
            .replace("Spot SELL with most volume selling", "Majority-volume spot SELL")
        )
        conditions.append(symbol + " " + text)
    action = decision.action.replace("DONT_CHASE", "DON'T CHASE").replace("_", " ")
    if decision.tactical and decision.action in POSITIVE_ACTIONS:
        action += " (tactical)"
    return [
        "",
        "── " + name + " (" + role + (" · least-repriced" if least_repriced else "") + ") ──",
        "",
        market,
        "",
        outlook_line,
        "",
        _market_context(truth, decision),
        "WHAT TO DO: " + action + " — Thesis " + decision.thesis_grade + " · Trade " + decision.trade_grade,
        "WHAT CHANGES THE ACTION:\n" + "\n".join(conditions),
        "",
        "CONFIDENCE: " + outlook.confidence + " — main unknown: " + _main_unknown(decision, truth),
    ]


def compose_card(inputs: CardInputs) -> str:
    inputs = CardInputs.model_validate(inputs)
    event, assessment, narrative = inputs.event, inputs.assessment, inputs.narrative
    data_classes = [market.confidence.classification for market in inputs.market_by_token.values()]
    rank = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
    header_data = (
        min(data_classes, key=lambda value: rank[value]) if data_classes else "n/a (market data unavailable)"
    )
    verified: list[str] = []
    unverified: list[str] = []
    for claim in event.claims:
        statement = " ".join((claim.subject, claim.predicate, claim.object_text)).strip()
        sources: list[str] = []
        for entry in claim.evidence:
            label = urlsplit(entry.url).hostname if entry.url is not None else None
            if label is None and entry.item_id is not None:
                label = event.evidence_labels.get(entry.item_id)
            if label is not None and label not in sources:
                sources.append(label)
        if claim.verified:
            verified.append(
                statement + " [" + (", ".join(sources) if sources else "source in evidence bundle") + "]"
            )
        else:
            unverified.append(
                statement + " — " + _na(claim.verification_gap, "verification gap not supplied")
            )
    stage = ("" if event.state_verified else "unverified ") + event.state
    lines = [
        assessment.classification.replace("STRUCTURAL_SHOCK", "STRUCTURAL SHOCK")
        + " ("
        + assessment.criterion
        + ") — "
        + narrative.verdict,
        "WHAT: " + narrative.what,
        score_summary(assessment.scores),
        "Stage: " + stage + " · Certainty: " + event.certainty + " · Data " + header_data,
        "Fresh: "
        + event.freshness
        + " · public "
        + _clock(event.public_at, publication=True)
        + " · known "
        + _clock(event.known_to_us_at, publication=False),
        "",
        "VERIFIED: " + ("; ".join(verified) if verified else "n/a (no verified claims)"),
        "NOT VERIFIED: " + ("; ".join(unverified) if unverified else "none recorded"),
        "",
        "WHY IT MATTERS",
    ]
    capture: list[str] = []
    for beneficiary in inputs.beneficiaries:
        text = (
            token_name(beneficiary.token_id, event.token_labels)
            + " "
            + beneficiary.capture.replace("_", "-")
            + " — "
            + beneficiary.causal_chain
        )
        truth = inputs.market_by_token.get(beneficiary.token_id)
        if truth is not None and truth.holders_revenue is not None:
            text += " · " + _holders_revenue_text(truth.holders_revenue)
        capture.append(text)
    capture_text = "; ".join(capture) if capture else "UNKNOWN — n/a (token mechanism unavailable)"
    if len(narrative.why) == 3:
        capture_text += " — " + _why_content(narrative.why[2], "Token capture")
    lines += [
        "• What changed: " + _why_content(narrative.why[0], "What changed"),
        "• Economic effect: " + _why_content(narrative.why[1], "Economic effect"),
        "• Token capture: " + capture_text,
    ]
    if event.has_expired_call_fact:
        lines[-1] += " · No longer following: call expired."
    if narrative.kind == "PARTIAL":
        lines.append("Market analysis pending — token actions and outlook unavailable.")
    else:
        lines += _who(inputs)
        for beneficiary in inputs.beneficiaries:
            lines += _token_block(
                inputs,
                beneficiary.token_id,
                inputs.decisions_by_token[beneficiary.token_id],
                role=beneficiary.subject.role,
                least_repriced=beneficiary.least_repriced,
            )
    text = "\n".join(lines)
    if len(text) > inputs.policy.card_max_chars:
        diagnostics = CardLengthDiagnostics(
            actual=len(text),
            max_chars=inputs.policy.card_max_chars,
            excess=len(text) - inputs.policy.card_max_chars,
            sections=tuple(
                CardLengthSection(line=index + 1, section=line.split(":", 1)[0][:64], chars=len(line))
                for index, line in enumerate(lines)
            ),
            newline_chars=len(lines) - 1,
            editable_fields=(
                "publish_card.what / verdict / why",
                "claims.subject / predicate / object / verification_missing via explicit supersedes_claim_ids",
                "set_beneficiaries.layer_reasons / no_credible_secondary_reason / edges[].causal_chain",
                "set_assessment.assessment.impact_horizon.dependencies / time_to_materiality",
            ),
            guidance="Shorten agent-authored structured text as well as WHAT/verdict/WHY when needed; publish-only shortening may be insufficient. Preserve all facts, remaining uncertainties and required per-layer reasons. Replace claims explicitly; do not invent verification or remove evidence. No text was truncated.",
            preview=text,
        )
        raise ValueError("CARD_LENGTH: " + diagnostics.model_dump_json(by_alias=True))
    return text
