"""Template-first cards and publication policy; diagnostic wording is preserved."""

import html
import json
import re
from datetime import timedelta
from decimal import Decimal
from urllib.parse import urlsplit

from tank.analysis.decisions import POSITIVE_ACTIONS
from tank.telegram_rich import CARD_TEXT_LIMIT, score_summary
from tank.time_precision import bucket_duration, interval_bounds, propagate_timestamp
from tank.util import parse_time, utcnow


def _new_fatal(event, assessment, previous):
    prior_event = ((previous or {}).get("card_struct") or {}).get("event") or previous or {}
    prior_assessment = ((previous or {}).get("card_struct") or {}).get("assessment") or {}
    prior_fatals = [
        c
        for c in prior_event.get("contradictions", []) + prior_assessment.get("contradictions", [])
        if c.get("type") == "FATAL" and not c.get("resolved_at")
    ]

    def same_contradiction(left, right):
        identity = left.get("contradiction_id") or left.get("contra_id")
        return (
            identity == (right.get("contradiction_id") or right.get("contra_id"))
            if identity
            else left == right
        )

    return any(
        c.get("type") == "FATAL"
        and not c.get("resolved_at")
        and not any(same_contradiction(c, old_fatal) for old_fatal in prior_fatals)
        for c in event.get("contradictions", []) + assessment.get("contradictions", [])
    )


def publication_immediate(event, assessment, previous=None, decisions=None):
    """Only new safety facts or START/EXIT transitions bypass the update clock."""
    actionable = [
        d for d in decisions or [] if not d.get("tracking_ended") and d.get("actionable") is not False
    ]
    old = {d.get("token_id"): d.get("action") for d in (previous or {}).get("decisions", [])}
    return _new_fatal(event, assessment, previous) or any(
        d.get("action") in {"START", "EXIT"} and old.get(d.get("token_id")) != d.get("action")
        for d in actionable
    )


def alert_gate(event, assessment, *, previous=None, decisions=None, now=None):
    decisions = decisions or []
    cls = assessment.get("class", assessment.get("ems_class"))
    if cls not in {"MAJOR", "STRUCTURAL_SHOCK"}:
        return False, "BELOW_MAJOR"
    if event.get("freshness") == "STALE" or event.get("publication_trigger_freshness") == "STALE":
        return False, "STALE_STATE_CHANGE" if previous else "STALE"
    if previous:
        actionable = [
            d for d in decisions if not d.get("tracking_ended") and d.get("actionable") is not False
        ]
        old = {d.get("token_id"): d.get("action") for d in previous.get("decisions", [])}
        changed = any(
            d.get("action") != old.get(d.get("token_id")) and d.get("action") != "HOLD" for d in actionable
        )
        prior_event = (previous.get("card_struct") or {}).get("event") or previous
        fatal = _new_fatal(event, assessment, previous)
        state = event.get("event_state", event.get("state"))
        prior_state = prior_event.get("event_state", state)
        ladder = ["RUMOR", "ANNOUNCED", "PROPOSED", "APPROVED", "SIGNED", "DEPLOYED", "LIVE", "MONEY_FLOWING"]
        advanced = (
            event.get("publication_stage_freshness") != "STALE"
            and event.get("state_verified") is True
            and state in ladder
            and prior_state in ladder
            and ladder.index(state) > ladder.index(prior_state)
        )
        newly_verified = (
            event.get("publication_stage_freshness") != "STALE"
            and state == prior_state
            and event.get("state_verified") is True
            and prior_event.get("state_verified") is False
        )
        negative = event.get("negative_marker") is not None and event.get(
            "negative_marker"
        ) != prior_event.get("negative_marker")
        changed = (
            changed
            or fatal
            or cls != previous.get("ems_class", previous.get("class", cls))
            or advanced
            or newly_verified
            or negative
            or event.get("milestone_resolved", False)
            or event.get("call_expired", False)
        )
        if not changed:
            return False, "NO_MATERIAL_CHANGE"
        immediate = publication_immediate(event, assessment, previous, decisions)
        stamp = previous.get("published_at", previous.get("created_at"))
        if (
            not immediate
            and stamp
            and (parse_time(now) if now else utcnow()) - parse_time(stamp)
            < timedelta(seconds=_limits(event)[1])
        ):
            return False, "UPDATE_RATE_LIMIT"
        return True, "UPDATE"
    if assessment.get("certainty", event.get("certainty", "LOW")) not in {"MEDIUM", "HIGH"}:
        return False, "LOW_CERTAINTY"
    if event.get("lifecycle") == "INVALIDATED":
        return False, "INVALIDATED"
    return True, "INITIAL"


def preliminary_gate(event, assessment):
    if assessment.get("class", assessment.get("ems_class")) != "MATERIAL":
        return False, "PRELIMINARY_REQUIRES_MATERIAL"
    if event.get("lifecycle") in {"CLOSED", "MERGED", "INVALIDATED"}:
        return False, "TERMINAL_EVENT"
    if event.get("freshness") == "STALE" or event.get("publication_trigger_freshness") == "STALE":
        return False, "STALE"
    claims = event.get("claims", [])
    superseded = {
        ref
        for claim in claims
        if claim.get("accepted") is not False
        for ref in claim.get("supersedes_claim_ids", [])
    }
    if not any(
        claim.get("accepted") is not False
        and claim.get("claim_id") not in superseded
        and claim.get("quote", "").strip()
        and claim.get("evidence")
        for claim in claims
    ):
        return False, "PRELIMINARY_REQUIRES_EVIDENCE"
    return True, "PRELIMINARY"


def _na(value, reason="NOT_AVAILABLE"):
    return str(value) if value is not None else "n/a (" + str(reason).replace("_", " ").lower() + ")"


def _token_label(value):
    """The ticker alone: only for units (e.g. "-1.06M WLD"), never as identity."""
    return str(value).rsplit(":", 1)[-1]


def token_name(token, labels=None):
    """PRD hard rule 16: a token is named by its project and ticker, e.g.
    "Lighter (LIT)", so Lighter and Litentry never both read "LIT"."""
    if token is None:
        return "n/a (token unavailable)"
    token = str(token)
    if labels and token in labels:
        return labels[token]
    if token.startswith("UNRESOLVED:"):
        return token.removeprefix("UNRESOLVED:") + " (unresolved)"
    project, _, ticker = token.rpartition(":")
    return project + " (" + ticker + ")" if project else token


def telegram_card_html(text):
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
    rendered = []
    for index, line in enumerate(text.splitlines()):
        escaped = html.escape(line)
        if index == 0 or line.startswith("── ") or line == "WHY IT MATTERS":
            escaped = "<b>" + escaped + "</b>"
        else:
            label = next((label for label in labels if line.startswith(label + ":")), None)
            if label:
                escaped = "<b>" + label + ":</b>" + escaped[len(label) + 1 :]
        rendered.append(escaped)
    return "\n".join(rendered)


def _clock(record, field, *, publication=False):
    stamp = propagate_timestamp(record, field)
    if stamp.value is None:
        return "n/a (timestamp unavailable)"
    point = parse_time(stamp.value)
    if stamp.precision is None:
        return "n/a (time precision unknown)"
    if stamp.precision == "COLLECTION_ONLY":
        clock = point.strftime("%H:%MZ")
        return (
            ("n/a (publication time unavailable; collected " + clock + ")")
            if publication
            else clock + " (collection)"
        )
    if stamp.precision == "EXACT":
        return point.strftime("%H:%MZ")
    lo, hi = interval_bounds(stamp)
    if lo is None:
        return "n/a (bucket edge unknown; " + stamp.precision + ")"
    duration = bucket_duration(stamp.precision)
    if duration == timedelta(days=1) and lo.time() == lo.time().min:
        return lo.strftime("%Y-%m-%d") + " (day precision)"

    def bound(value):
        if duration >= timedelta(days=1) or lo.date() != hi.date():
            return value.strftime("%Y-%m-%d %H:%MZ")
        if duration >= timedelta(minutes=1):
            return value.strftime("%H:%MZ")
        return value.strftime("%H:%M:%S.%f").rstrip("0").rstrip(".") + "Z"

    return bound(lo) + "–" + bound(hi) + " (" + stamp.precision + ", end exclusive)"


def _percent(value):
    return _na(None) if value is None else format(Decimal(str(value)) * 100, ".2f") + "%"


def _limits(event):
    config = event.get("thresholds") or {}
    return int(config.get("card_max_chars", CARD_TEXT_LIMIT)), int(config.get("update_rate_limit_s", 900))


def _unknown(dc):
    limiting = dc.get("limiting") or {}
    if not limiting:
        return "none recorded" if dc.get("class") == "HIGH" else "n/a (limiting input unavailable)"
    labels = {
        "spot_coverage": "Spot venue coverage is incomplete",
        "derivatives_coverage": "Synchronized native OI coverage is incomplete",
        "freshness": "Some inputs are outside their observation cadence",
    }
    condition = str(limiting.get("condition", ""))
    label = next(
        (v for k, v in labels.items() if k in condition.lower()),
        condition.replace("_", " ").capitalize() or "Input incomplete",
    )
    reason = limiting.get("reason")
    if not reason or reason in {"INCOMPLETE", "NOT_AVAILABLE", "NOT_ESTIMABLE", "UNKNOWN"}:
        return label
    return label + ": " + str(reason).replace("_", " ").lower()


def _main_unknown(outlook, decision, truth):
    """§6.5: name the input that sets the confidence. "none recorded" only when
    the overall confidence is HIGH (and Data Confidence records no limit)."""
    dc = truth.get("data_confidence") or {}
    if outlook.get("confidence") == "HIGH":
        return _unknown(dc)
    limiting = outlook.get("confidence_limiting") or decision.get("confidence_limiting") or {}
    if limiting.get("text"):
        return limiting["text"]
    text = _unknown(dc)
    if limiting.get("input") == "data" or text != "none recorded":
        return text
    return "n/a (limiting input not recorded)"


def _why_content(value, label):
    return re.sub(r"^\s*" + re.escape(label) + r"\s*:\s*", "", str(value), flags=re.IGNORECASE)


def _exclusion_reason(excluded):
    """Render the recorded cause; INCOMPLETE alone is a state, not an explanation."""
    detail = excluded.get("detail")
    reasons = excluded.get("source_reasons") or []
    if isinstance(reasons, str):
        reasons = [reasons]
    missing = excluded.get("missing_metrics") or []
    if missing:
        reasons = list(reasons) + ["missing " + ", ".join(str(name).replace("_", " ") for name in missing)]
    if detail or reasons:
        return "; ".join(str(value).replace("_", " ").lower() for value in [detail, *reasons] if value)
    reason = excluded.get("reason")
    return (
        "coverage incomplete; specific cause unavailable"
        if reason == "INCOMPLETE"
        else str(reason or "specific cause unavailable")
    )


def _holders_revenue_text(truth):
    observed = (truth.get("supply") or {}).get("holders_revenue") or {}

    def missing_reason(reason, *, captured=False):
        return {
            "NO_CAPTURED_FUNDAMENTALS": "not captured",
            "ONLY_FUTURE_FUNDAMENTALS": "not available by cutoff",
            "PROVIDER_METRIC_NOT_SUPPORTED": "provider metric unsupported",
            "NO_SUPPORTED_PUBLIC_FEES_ADAPTER": "provider metric unsupported",
            "NO_PUBLIC_FEES_ADAPTER_MATCHING_TOKEN_IDENTITY": "no verified provider match",
        }.get(reason, "incomplete provider data" if captured else "not captured")

    if not observed.get("protocols"):
        return (
            "holders revenue: n/a (" + missing_reason(observed.get("reason"), captured=bool(observed)) + ")"
        )

    def clock(value):
        return parse_time(value).strftime("%Y-%m-%d %H:%MZ") if value else "n/a (time unavailable)"

    parts = []
    scalar = False
    for protocol in observed["protocols"]:
        period = next((p for p in protocol.get("periods", []) if p.get("value") is not None), None)
        if period:
            scalar = True
            value = period["value"] + " " + protocol["unit"] + "/" + period["provider_period"]
        elif protocol.get("latest_chart_point"):
            point = protocol["latest_chart_point"]
            value = (
                point["value"]
                + " "
                + protocol["unit"]
                + " chart point "
                + clock(point["provider_timestamp"])
                + " (period unknown)"
            )
        else:
            value = "n/a (" + missing_reason(protocol.get("reason"), captured=True) + ")"
        parts.append(protocol["protocol"] + " " + value)
    notes = []
    if scalar:
        notes.append("observed " + clock(observed.get("as_of")))
    if observed.get("status") == "STALE":
        notes.append("stale")
    if not (observed.get("coverage", {}).get("24h") or {}).get("complete"):
        notes.append("partial 24h coverage")
    return (
        "DefiLlama reports holders revenue: "
        + "; ".join(parts)
        + (" (" + "; ".join(notes) + ")" if notes else "")
    )


def compose_card(
    event,
    assessment,
    beneficiaries,
    decisions,
    market_truths,
    *,
    kind="FULL",
    what="",
    verdict="",
    why=None,
    unconfirmed="",
):
    if isinstance(market_truths, dict):
        market_truths = list(market_truths.values())
    dm = {d.get("token_id"): d for d in decisions}
    mm = {m.get("token_id"): m for m in market_truths}
    max_chars, _ = _limits(event)
    token_labels = event.get("token_labels") or {}
    universe = set(token_labels)

    def name(token):
        return token_name(token, token_labels)

    data_classes = [(m.get("data_confidence") or {}).get("class") for m in market_truths]
    header_data = (
        min(data_classes, key=lambda c: {"LOW": 0, "MEDIUM": 1, "HIGH": 2}.get(c, 0))
        if data_classes and all(data_classes)
        else "n/a (market data unavailable)"
    )
    claims = event.get("claims", assessment.get("claims", []))
    verified, unverified = [], []
    superseded = {
        ref
        for claim in claims
        if claim.get("accepted") is not False
        for ref in claim.get("supersedes_claim_ids", [])
    }
    for claim in claims:
        if claim.get("accepted") is False or claim.get("claim_id") in superseded:
            continue
        statement = " ".join(str(claim.get(key, "")) for key in ("subject", "predicate", "object")).strip()
        evidence = claim.get("evidence") or []
        sources = []
        for entry in evidence:
            url = entry.get("url")
            label = (urlsplit(url).hostname if url else None) or event.get("evidence_labels", {}).get(
                entry.get("item_id")
            )
            if label and label not in sources:
                sources.append(label)
        if claim.get("verified"):
            verified.append(
                (statement or "n/a (claim statement unavailable)")
                + " ["
                + (", ".join(sources) or "source in evidence bundle")
                + "]"
            )
        else:
            unverified.append(
                (statement or "n/a (claim statement unavailable)")
                + " — "
                + str(claim.get("verification_missing") or "n/a (verification gap not supplied)")
            )
    if kind == "PRELIMINARY" and unconfirmed:
        unverified.append(unconfirmed)
    scores = score_summary(assessment)
    stage = ("" if event.get("state_verified", event.get("verified")) is True else "unverified ") + _na(
        event.get("event_state", event.get("state"))
    )
    lines = [
        f"{_na(assessment.get('class', assessment.get('ems_class')), 'class unavailable').replace('STRUCTURAL_SHOCK', 'STRUCTURAL SHOCK')} ({_na(assessment.get('criterion', assessment.get('matched_criterion')), 'criterion unavailable')}) — {_na(verdict or event.get('statement'), 'neutral event statement unavailable')}",
        *([f"WHAT: {what}"] if what else []),
        scores,
        f"Stage: {stage} · Certainty: {_na(assessment.get('certainty', event.get('certainty')))} · Data {header_data}",
        f"Fresh: {_na(event.get('freshness'))} · public {_clock(event, 'public_at', publication=True)} · known {_clock(event, 'known_to_us_at')}",
        "",
        "VERIFIED: " + ("; ".join(verified) or "n/a (no verified claims)"),
        "NOT VERIFIED: " + ("; ".join(unverified) or "none recorded"),
        "",
        "WHY IT MATTERS",
    ]
    if kind == "PRELIMINARY":
        lines[0] = "PRELIMINARY · " + lines[0]
    why = list(why or [])
    changed = _why_content(why[0], "What changed") if why else "n/a (change description unavailable)"
    economic = _why_content(why[1], "Economic effect") if len(why) > 1 else "not estimable"

    def capture_text(beneficiary):
        token = beneficiary.get("token_id")
        text = (
            name(token)
            + " "
            + str(beneficiary.get("capture_class", beneficiary.get("capture", "UNKNOWN"))).replace("_", "-")
            + " — "
            + str(beneficiary.get("causal_chain", "mechanism unknown"))
        )
        truth = mm.get(token, {})
        revenue = (truth.get("supply") or {}).get("holders_revenue") or {}
        if any(
            any(period.get("value") is not None for period in protocol.get("periods", []))
            or (protocol.get("latest_chart_point") or {}).get("value") is not None
            for protocol in revenue.get("protocols", [])
        ):
            text += " · " + _holders_revenue_text(truth)
        return text

    capture = "; ".join(capture_text(b) for b in beneficiaries[:3])
    capture = capture or "UNKNOWN — n/a (token mechanism unavailable)"
    if len(why) > 2:
        capture += " — " + _why_content(why[2], "Token capture")
    lines += ["• What changed: " + changed, "• Economic effect: " + economic, "• Token capture: " + capture]
    if event.get("call_expired"):
        lines[-1] += " · No longer following: call expired."
    if kind in {"PARTIAL", "PRELIMINARY"}:
        lines.append("Market analysis pending — token actions and outlook unavailable.")
    else:
        mapping = event.get("beneficiary_map") or {}

        named_edges = beneficiaries + [
            edge
            for edge in event.get("beneficiary_edges", [])
            if edge.get("token_id") is None or edge.get("tradable") is False
        ]

        def outside_universe(edge):

            return bool(
                universe
                and edge.get("token_id")
                and edge["token_id"] not in universe
                and edge.get("evidence")
                and not edge.get("dependency_ref")
            )

        def identity(edge):
            return edge.get("token_id") or edge.get("entity_id") or edge.get("entity_name")

        primary_identities = {identity(edge) for edge in named_edges if edge.get("role") == "PRIMARY"}
        primary_entities = {edge.get("entity_id") for edge in named_edges if edge.get("role") == "PRIMARY"}
        edges = [
            edge
            for edge in named_edges
            if edge.get("role") in {"PRIMARY", "LOSER"}
            or edge.get("role") == "SECONDARY"
            and (edge.get("credible") or outside_universe(edge))
            and identity(edge) not in primary_identities
            and (not edge.get("entity_id") or edge.get("entity_id") not in primary_entities)
        ]
        who = []
        shown = set()
        for edge in edges:
            if identity(edge) in shown:
                continue
            shown.add(identity(edge))
            role = _na(edge.get("role"), "role unavailable").lower()
            label = (
                name(edge["token_id"])
                if edge.get("token_id")
                else edge.get("entity_name") or edge.get("entity_id") or "n/a (entity unnamed)"
            )
            item = role + " " + label
            if edge.get("role") == "SECONDARY":
                item += (
                    " ("
                    + _na(edge.get("layer"), "layer unavailable")
                    + ", "
                    + _integration_stage_label(edge, event, claims)
                    + ")"
                )
            if outside_universe(edge):
                item += " — outside the token universe, no market block"
            who.append(item)
        if not any(edge.get("role") == "SECONDARY" and edge.get("credible") for edge in edges):
            reason = mapping.get("no_credible_secondary_reason")
            generic = "No SECONDARY edge satisfies the credibility requirements in §4.7."
            if reason and reason.startswith(generic):
                reason = reason[len(generic) :].strip()
            who.append("secondary none" + (" — " + reason if reason else ""))
        lines += ["", "WHO: " + (" · ".join(who) or "n/a (no beneficiary recorded)")]
        if not any(edge.get("role") == "SECONDARY" for edge in edges):
            layer_names = {
                "ISSUER_PRODUCT": "Issuer / product",
                "VENUE_DEX": "Exchanges / DEX",
                "SETTLEMENT_L1_L2": "Settlement L1 / L2",
                "LIQUIDITY_EXECUTION": "Liquidity / execution",
                "LENDING_COLLATERAL": "Lending / collateral",
                "ORACLE_DATA": "Oracles / data",
                "INTEROPERABILITY": "Interoperability",
                "CUSTODY_COMPLIANCE": "Custody / compliance",
            }
            aliases = {"SETTLEMENT_L1_L2": "SETTLEMENT", "ORACLE_DATA": "ORACLE"}
            layers = mapping.get("layers", {})
            for layer, label in layer_names.items():
                result = layers.get(layer) or layers.get(aliases.get(layer)) or {}
                explanation = (
                    result.get("note") or result.get("reason") or "n/a (layer assessment not supplied)"
                )
                lines.append("• " + label + ": " + explanation)
        for beneficiary in beneficiaries[:3]:
            token = beneficiary.get("token_id")
            decision = dm.get(token)
            if decision is None:
                raise ValueError("MISSING_DECISION: " + str(token))
            if decision.get("tracking_ended") or decision.get("actionable") is False:
                lines.append(
                    name(token)
                    + (
                        ": No longer following: token removed from current beneficiary map."
                        if decision.get("removed_from_analysis")
                        else ": No longer following: call expired."
                    )
                )
                continue
            if token not in mm:
                raise ValueError("MISSING_MARKET_TRUTH: " + str(token))
            if not decision.get("action"):
                raise ValueError("MISSING_DECISION_ACTION: " + str(token))
            truth = mm[token]
            outlook = decision.get("outlook") or {}
            relative = truth.get("relative") or {}
            facts = []
            for fact in truth.get("decisive_facts", [])[:2]:
                if isinstance(fact, dict):
                    unit = fact.get("unit")
                    display_unit = _token_label(token) if unit == "NATIVE_TOKEN" else _na(unit)
                    measured = (
                        _percent(fact.get("value"))
                        if unit in {"DECIMAL_RETURN", "DECIMAL_FRACTION", "RATE"}
                        else (
                            _na(fact.get("value"), fact.get("reason", "NOT_AVAILABLE")) + " " + display_unit
                        )
                    )
                    window = (
                        str(fact["window"]).removeprefix("W").lower()
                        if fact.get("window")
                        else "n/a (window unavailable)"
                    )
                    metric = {"spot.cvd": "spot net flow"}.get(fact.get("metric"), fact.get("metric", ""))
                    facts.append(window + ": " + metric.replace("_", " ") + " " + measured)
                else:
                    facts.append(str(fact))
            event_return = (relative.get("windows") or {}).get("EVENT") or {}
            move = truth.get("move_class") or "UNKNOWN"
            divergence = truth.get("divergence") or {}
            move_label = (
                "Possible passive spot absorption — unconfirmed"
                if move == "PASSIVE_SPOT_ABSORPTION"
                and divergence.get("state") not in {"ABSORPTION_CONFIRMED", "CONFIRMED"}
                and truth.get("absorption_confirmed") is not True
                else str(move).replace("_", " ")
            )
            market = "MARKET TRUTH: " + move_label
            market += "\n" + ("\n".join("• " + fact for fact in facts) or "Market data incomplete.")
            if relative.get("repricing_band") is not None or any(
                event_return.get(k) is not None for k in ("r_raw", "r_btc_adj")
            ):
                net = (
                    " (BTC is the benchmark)"
                    if event_return.get("r_btc_adj_reason") == "NOT_APPLICABLE_BENCHMARK"
                    else f"; {_percent(event_return.get('r_btc_adj'))} net BTC"
                )
                market += f"\nPriced: {_na(relative.get('repricing_band'))} · EVENT {_percent(event_return.get('r_raw'))} raw{net}"
            market += "".join("\n• " + line for line in _market_breadth(truth, token))
            alternatives = (truth.get("divergence") or {}).get("unresolved", []) + truth.get(
                "alternatives", []
            )
            if alternatives:
                market += "\nUnresolved: " + ", ".join(alternatives)
            spot = (truth.get("spot") or {}).get(truth.get("primary_window", "W4H"), {})
            excluded = (spot.get("coverage") or {}).get("excluded") or []
            if excluded:
                reasons = {}
                for entry in excluded:
                    reasons.setdefault(_exclusion_reason(entry), []).append(str(entry.get("venue")))
                market += "\nExcluded: " + "; ".join(
                    ", ".join(venues) + " — " + reason for reason, venues in reasons.items()
                )
            horizon = {"H1": "1h", "H4": "4h", "H12": "12h", "H24": "24h", "D3": "3d", "D7": "7d"}.get(
                outlook.get("best_horizon"), "n/a (horizon unavailable)"
            )
            scenarios = outlook.get("scenarios") or {}
            outlook_line = f"OUTLOOK: {_na(outlook.get('direction'), 'bias unavailable')} · {_na(outlook.get('potential'), 'potential unavailable')} · best {horizon}"
            reasons = outlook.get("scenario_reasons") or {}
            outlook_line += "".join(
                "\n• "
                + name.capitalize()
                + ": "
                + _na(scenarios.get(name), reasons.get(name, "scenario unavailable"))
                for name in ("base", "upside", "failure")
            )
            conditions = []
            for role, symbol in [("UPGRADE", "↑"), ("DOWNGRADE", "↓")]:
                condition = next((c for c in decision.get("conditions", []) if c.get("role") == role), {})
                conditions.append(
                    symbol
                    + " "
                    + condition.get("human_text", "n/a (condition unavailable)")
                    .replace("two consecutive hourly windows", "two hourly windows")
                    .replace("Spot BUY with most volume buying", "Majority-volume spot BUY")
                    .replace("Spot SELL with most volume selling", "Majority-volume spot SELL")
                )
            lines += [
                "",
                "── "
                + name(token)
                + " ("
                + _na(beneficiary.get("role"), "role unavailable")
                + (
                    " · least-repriced"
                    if beneficiary.get("edge_id")
                    and beneficiary.get("edge_id") == mapping.get("least_repriced_edge")
                    else ""
                )
                + ") ──",
                "",
                market,
                "",
                outlook_line,
                "",
                _market_context(truth, decision),
                "WHAT TO DO: "
                + str(decision["action"]).replace("DONT_CHASE", "DON'T CHASE").replace("_", " ")
                + (
                    " (tactical)"
                    if decision.get("tactical") and decision["action"] in POSITIVE_ACTIONS
                    else ""
                )
                + f" — Thesis {_na(decision.get('thesis_grade'), 'grade unavailable')} · Trade {_na(decision.get('trade_grade'), 'grade unavailable')}",
                "WHAT CHANGES THE ACTION:\n" + "\n".join(conditions),
                "",
                f"CONFIDENCE: {_na(outlook.get('confidence'), 'confidence unavailable')} — main unknown: "
                + _main_unknown(outlook, decision, truth),
            ]
    text = "\n".join(lines)
    if len(text) > max_chars:
        diagnostics = {
            "actual": len(text),
            "max": max_chars,
            "excess": len(text) - max_chars,
            "sections": [
                {"line": i + 1, "section": line.split(":", 1)[0][:64], "chars": len(line)}
                for i, line in enumerate(lines)
            ],
            "newline_chars": len(lines) - 1,
            "editable_fields": [
                "publish_card.what / verdict / why",
                "claims.subject / predicate / object / verification_missing via explicit supersedes_claim_ids",
                "set_beneficiaries.layer_reasons / no_credible_secondary_reason / edges[].causal_chain",
                "set_assessment.assessment.impact_horizon.dependencies / time_to_materiality",
            ],
            "guidance": (
                "Shorten agent-authored structured text as well as WHAT/verdict/WHY when needed; "
                "publish-only shortening may be insufficient. Preserve all facts, remaining "
                "uncertainties and required per-layer reasons. Replace claims explicitly; "
                "do not invent verification or remove evidence. No text was truncated."
            ),
            "preview": text,
        }
        raise ValueError("CARD_LENGTH: " + json.dumps(diagnostics, ensure_ascii=False))
    return text


def _market_breadth(truth, token):
    """Per-venue OI class with its price-markup share, and spot venue breadth,
    each labelled with venue and window (01 hard rules 1 and 3, L1, L2, L8)."""
    lines = []
    derivatives = truth.get("derivatives") or {}
    window = str(derivatives.get("window") or truth.get("primary_window") or "").removeprefix("W").lower()
    venues = []
    for row in derivatives.get("per_venue") or []:
        if not isinstance(row, dict) or not row.get("oi_class"):
            continue
        venue = (row.get("coverage") or {}).get("venue") or row.get("venue") or "n/a (venue unknown)"
        text = venue + " " + str(row["oi_class"])
        if row.get("oi_native_chg_pct") is not None:
            text += (
                " "
                + format(Decimal(str(row["oi_native_chg_pct"])) * 100, "+.2f")
                + "% in "
                + _token_label(token)
            )
        share = (row.get("decomposition") or {}).get("pos_share")
        if (row.get("decomposition") or {}).get("markup") is not None and share is not None:
            text += (
                " (position share "
                + format(Decimal(str(share)) * 100, ".1f")
                + "% of the OI value change; price-markup component present)"
            )
        venues.append(text)

    covered = any("by venue:" in str(fact) for fact in truth.get("decisive_facts") or [])
    if venues and window and not covered:
        lines.append(window + ": open interest by venue: " + "; ".join(venues))
    sync = derivatives.get("synchronized") or {}
    change = sync.get("oi_native_chg_pct")
    if change is not None and Decimal(str(change)) < 0 and window:
        price = ((truth.get("relative") or {}).get("windows") or {}).get(
            derivatives.get("window") or ""
        ) or {}
        move = price.get("r_raw")
        if move is None:
            during = "; price move n/a (not available for this window)"
        elif Decimal(str(move)) > 0:
            during = " while price rose " + format(Decimal(str(move)) * 100, "+.2f") + "%"
        else:
            during = "; price " + format(Decimal(str(move)) * 100, "+.2f") + "%"

        verdict = {
            "BELOW": ": positions closing",
            "WITHIN": "; within its normal range",
        }.get(sync.get("native_oi_vs_normal"), "; normal range n/a (not available)")
        lines.append(
            window
            + ": open positions (synchronized native OI) "
            + format(Decimal(str(change)) * 100, "+.2f")
            + "% in "
            + _token_label(token)
            + during
            + verdict
        )
    spot = (truth.get("spot") or {}).get(truth.get("primary_window", "W4H")) or {}
    rows = [row for row in spot.get("venues") or [] if isinstance(row, dict) and row.get("complete")]
    if rows:
        spot_window = str(spot.get("window") or truth.get("primary_window") or "").removeprefix("W").lower()

        def side(sign):
            chosen = [r for r in rows if r.get("sign") == sign and r.get("volume_share") is not None]
            return (
                ", ".join(
                    str(r.get("venue")) + " " + format(Decimal(str(r["volume_share"])) * 100, ".1f") + "%"
                    for r in sorted(chosen, key=lambda r: -r["volume_share"])
                )
                or "none"
            )

        lines.append(
            spot_window
            + ": spot venues net buying: "
            + side(1)
            + "; net selling: "
            + side(-1)
            + " (share of spot volume)"
        )
    return lines


def _market_context(truth, decision):
    """03 §5.7 market context with its BTC input, and the Trade Grade notch (§4.9)."""
    relative = truth.get("relative") or {}
    context = relative.get("market_context")
    inputs = relative.get("context_inputs") or {}
    btc, normal = inputs.get("btc_return_24h"), inputs.get("return_normal_range")
    if context is None:
        text = (
            "Market context: n/a (" + ("BTC normal range unavailable" if not normal else "not computed") + ")"
        )
    else:
        text = "Market context: " + str(context)
        if btc is not None:
            text += " — BTC 24h " + format(Decimal(str(btc)) * 100, "+.2f") + "%"
            if normal and len(normal) == 2 and None not in normal:
                text += (
                    " vs normal "
                    + format(Decimal(str(normal[0])) * 100, "+.2f")
                    + "% to "
                    + format(Decimal(str(normal[1])) * 100, "+.2f")
                    + "%"
                )
    raw, final = decision.get("trade_rule"), decision.get("trade_grade")
    if any(str(r).startswith("CONTEXT_") for r in decision.get("trade_reasons") or []) and raw != final:
        text += " · Trade " + str(raw) + " → " + str(final) + " for market context " + str(context)
    elif context in {"HEADWIND", "STRESS"} and raw == "D":
        text += " · Trade not lowered: already D"
    return text


def _integration_stage_label(subject, event, claims):
    """Display the agent's recorded integration state and verification flags."""
    state = subject.get("integration_state") or (
        event.get("event_state", "UNKNOWN") if subject.get("role") == "PRIMARY" else "UNKNOWN"
    )
    refs = subject.get("integration_state_evidence", subject.get("evidence", []))
    verified = any(claim.get("claim_id") in refs and claim.get("verified") is True for claim in claims)
    if subject.get("role") == "PRIMARY" and not subject.get("integration_state"):
        verified = event.get("state_verified") is True
    return ("" if verified else "unverified ") + str(state)
