"""Template-first cards and publication policy; diagnostic wording is preserved."""

import re
from datetime import timedelta
from decimal import Decimal

from tank.util import parse_time, utcnow


def alert_gate(event, assessment, *, previous=None, decisions=None, now=None):
    decisions = decisions or []
    cls = assessment.get("class", assessment.get("ems_class"))
    if previous:
        actionable = [
            d for d in decisions if not d.get("tracking_ended") and d.get("actionable") is not False
        ]
        old = {d.get("token_id"): d.get("action") for d in previous.get("decisions", [])}
        changed = any(
            d.get("action") != old.get(d.get("token_id")) and d.get("action") != "HOLD" for d in actionable
        )
        fatal = any(
            c.get("type") == "FATAL" and not c.get("resolved_at")
            for c in event.get("contradictions", []) + assessment.get("contradictions", [])
        )
        state = event.get("event_state", event.get("state"))
        prior_event = (previous.get("card_struct") or {}).get("event") or previous
        prior_state = prior_event.get("event_state", state)
        ladder = ["RUMOR", "ANNOUNCED", "PROPOSED", "APPROVED", "SIGNED", "DEPLOYED", "LIVE", "MONEY_FLOWING"]
        advanced = (
            state in ladder and prior_state in ladder and ladder.index(state) > ladder.index(prior_state)
        )
        newly_verified = (
            state == prior_state
            and event.get("state_verified") is True
            and prior_event.get("state_verified") is False
        )
        negative = event.get("negative_marker") is not None and event.get(
            "negative_marker"
        ) != prior_event.get("negative_marker")
        stale = event.get("freshness") == "STALE"
        safety = (
            fatal
            or event.get("call_expired", False)
            or any(d.get("action") == "EXIT" and old.get(d.get("token_id")) != "EXIT" for d in actionable)
        )
        if stale and not safety:
            return False, "STALE_STATE_CHANGE"
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
        immediate = fatal or any(d.get("action") in {"START", "EXIT"} for d in actionable)
        stamp = previous.get("published_at", previous.get("created_at"))
        if (
            not immediate
            and stamp
            and (parse_time(now) if now else utcnow()) - parse_time(stamp)
            < timedelta(seconds=_limits(event)[1])
        ):
            return False, "UPDATE_RATE_LIMIT"
        return True, "UPDATE_SAFETY_OVERRIDE" if event.get("freshness") == "STALE" or event.get(
            "lifecycle"
        ) == "INVALIDATED" else "UPDATE"
    if cls not in {"MAJOR", "STRUCTURAL_SHOCK"}:
        return False, "BELOW_MAJOR"
    if assessment.get("certainty", event.get("certainty", "LOW")) not in {"MEDIUM", "HIGH"}:
        return False, "LOW_CERTAINTY"
    if event.get("freshness") == "STALE":
        return False, "STALE"
    if event.get("lifecycle") == "INVALIDATED":
        return False, "INVALIDATED"
    return True, "INITIAL"


def _na(value, reason="NOT_AVAILABLE"):
    return str(value) if value is not None else "n/a (" + reason + ")"


def _clock(value):
    if isinstance(value, dict):
        value = value.get("value")
    return parse_time(value).strftime("%H:%MZ") if value else "n/a (timestamp unavailable)"


def _percent(value):
    return _na(None) if value is None else format(Decimal(str(value)) * 100, ".2f") + "%"


def _limits(event):
    config = event.get("thresholds") or {}
    return int(config.get("card_max_chars", 3500)), int(config.get("update_rate_limit_s", 900))


def _unknown(dc):
    limiting = dc.get("limiting") or {}
    if not limiting:
        return "none recorded" if dc.get("class") == "HIGH" else "n/a (limiting input unavailable)"
    labels = {
        "semantics": "Metric meaning is not fully verified",
        "spot_coverage": "Spot venue coverage is incomplete",
        "derivatives_coverage": "Synchronized native OI coverage is incomplete",
        "freshness": "Some inputs are outside their observation cadence",
    }
    condition = str(limiting.get("condition", ""))
    label = next(
        (v for k, v in labels.items() if k in condition.lower()),
        condition.replace("_", " ").capitalize() or "Input incomplete",
    )
    return label + ": " + str(limiting.get("reason", "reason unavailable"))


def compose_card(
    event, assessment, beneficiaries, decisions, market_truths, *, kind="FULL", verdict="", why=None
):
    if isinstance(market_truths, dict):
        market_truths = list(market_truths.values())
    dm = {d.get("token_id"): d for d in decisions}
    mm = {m.get("token_id"): m for m in market_truths}
    max_chars, _ = _limits(event)
    data_classes = [(m.get("data_confidence") or {}).get("class", "LOW") for m in market_truths]
    header_data = (
        min(data_classes, key=lambda c: {"LOW": 0, "MEDIUM": 1, "HIGH": 2}.get(c, 0))
        if data_classes
        else "UNKNOWN"
    )
    claims = event.get("claims", assessment.get("claims", []))
    verified = []
    unverified = []
    for c in claims:
        if c.get("accepted") is False:
            continue
        statement = " ".join(str(c.get(key, "")) for key in ("subject", "predicate", "object")).strip()
        evidence = c.get("evidence") or []
        sources = ", ".join(str(e.get("url", e.get("item_id", "n/a (missing item)"))) for e in evidence)
        if c.get("verified"):
            verified.append(
                (statement or "n/a (claim statement unavailable)")
                + " ["
                + (sources or "n/a (evidence missing)")
                + "]"
            )
        else:
            unverified.append(
                (statement or "n/a (claim statement unavailable)")
                + " — "
                + str(c.get("verification_missing", "HIGH-certainty primary confirmation missing"))
            )
    lines = [
        f"{assessment.get('class', assessment.get('ems_class', 'UNKNOWN'))} ({assessment.get('criterion', assessment.get('matched_criterion', 'n/a'))}) — {verdict}",
        f"Stage: {_na(event.get('event_state', event.get('state')))} · Certainty: {_na(assessment.get('certainty', event.get('certainty')))} · Fresh: {_na(event.get('freshness'))} (public {_clock(event.get('public_at'))}, known {_clock(event.get('known_to_us_at'))}) · Data {header_data}",
        "VERIFIED: "
        + ("; ".join(verified) or "n/a (no verified claims)")
        + " · NOT VERIFIED: "
        + ("; ".join(unverified) or "—"),
        "WHY IT MATTERS",
    ]
    if event.get("call_expired"):
        lines.append("No longer following: call expired.")
    dims = assessment.get("dims", {})
    labels = [
        ("D", "economic_directness"),
        ("M", "magnitude"),
        ("P", "permanence"),
        ("X", "distribution_expansion"),
        ("F", "mechanical_flows"),
        ("S", "surprise"),
        ("I", "implementation"),
    ]
    lines.insert(
        1,
        "Scores "
        + " ".join(
            label
            + _na((dims.get(key) or {}).get("value"), (dims.get(key) or {}).get("reason") or "NOT_AVAILABLE")
            for label, key in labels
        ),
    )
    lines += ["• " + str(w) for w in (why or ["Economic effect: not estimable"])[:3]]
    lines.append(
        "Token capture: "
        + "; ".join(
            str(b.get("token_id"))
            + " "
            + str(b.get("capture_class", b.get("capture", "UNKNOWN")))
            + " — "
            + str(b.get("causal_chain", "n/a (mechanism unavailable)"))
            for b in beneficiaries[:3]
        )
    )
    impact = assessment.get("impact_horizon") or {}
    lines.append(
        "IMPACT: start "
        + _na(impact.get("impact_start"))
        + " · ramp "
        + _na(impact.get("ramp_shape"))
        + " · dependencies "
        + (", ".join(impact.get("dependencies") or []) or "none recorded")
        + " · timing "
        + _na(impact.get("timing_confidence"))
        + " · materiality "
        + _na(impact.get("time_to_materiality"))
    )
    if kind == "PARTIAL":
        lines = [line for line in lines if line != "Token capture: "]
        lines.append("Market analysis pending — token actions and outlook unavailable.")
        text = "\n".join(lines)
        if len(text) > max_chars:
            raise ValueError("CARD_LENGTH: partial card exceeds 3500 characters")
        return text
    mapping = event.get("beneficiary_map") or {}
    if not any(b.get("role") == "SECONDARY" and b.get("credible") for b in beneficiaries):
        lines.append(
            "SECONDARIES: " + _na(mapping.get("no_credible_secondary_reason"), "no secondary assessment")
        )
        for layer, detail in mapping.get("layers", {}).items():
            if detail.get("status") in {"NONE", "UNKNOWN"}:
                lines.append(
                    str(layer)
                    + ": "
                    + detail["status"]
                    + " — "
                    + _na(detail.get("note"), "reason unavailable")
                )
    lines.append(
        "WHO: "
        + " · ".join(
            str(b.get("entity_name", b.get("entity_id", b.get("token_id", "UNKNOWN"))))
            + " "
            + str(b.get("role"))
            + (
                " ("
                + str(b.get("layer", "UNKNOWN"))
                + ", "
                + str(b.get("integration_state") or event.get("event_state", "UNKNOWN"))
                + ")"
                if b.get("role") == "SECONDARY"
                else ""
            )
            for b in event.get("beneficiary_edges", beneficiaries)
        )
    )
    for b in beneficiaries[:3]:
        token = b.get("token_id")
        if dm.get(token, {}).get("tracking_ended") or dm.get(token, {}).get("actionable") is False:
            lines.append(
                str(token)
                + (
                    ": No longer following: token removed from current beneficiary map."
                    if dm[token].get("removed_from_analysis")
                    else ": No longer following: call expired."
                )
            )
            continue
        if token not in dm and token in mm:
            raise ValueError("MISSING_DECISION: " + str(token))
        if token not in mm or token not in dm:
            continue
        d = dm.get(token, {})
        m = mm.get(token, {})
        o = d.get("outlook", {})
        r = m.get("relative") or {}
        dc = m.get("data_confidence") or {}
        facts = []
        for f in m.get("decisive_facts", [])[:2]:
            if isinstance(f, dict):
                unit = f.get("unit")
                measured = (
                    _percent(f.get("value"))
                    if unit in {"DECIMAL_RETURN", "DECIMAL_FRACTION", "RATE"}
                    else _na(f.get("value"), f.get("reason", "NOT_AVAILABLE")) + " " + _na(unit)
                )
                facts.append(measured + " / " + _na(f.get("window")) + " " + f.get("metric", ""))
            else:
                facts.append(str(f))
        event_return = (r.get("windows") or {}).get("EVENT") or {}
        alt = (m.get("divergence") or {}).get("unresolved", []) + m.get("alternatives", [])
        lines += [
            f"── {token} ({b.get('role', 'UNKNOWN')}"
            + (
                " · least-repriced"
                if b.get("edge_id") and b.get("edge_id") == mapping.get("least_repriced_edge")
                else ""
            )
            + ") ──",
            f"MARKET TRUTH: {m.get('move_class', 'UNKNOWN')} (analysis cutoff {_na(m.get('cutoff'))}) — "
            + ("; ".join(facts) or "n/a (market facts unavailable)")
            + f"; priced: {r.get('repricing_band', 'UNKNOWN')} (EVENT {_percent(event_return.get('r_raw'))} raw; {_percent(event_return.get('r_btc_adj'))} net BTC)",
            "Unresolved alternatives: " + (", ".join(alt) or "—"),
            f"OUTLOOK: {o.get('direction', 'UNKNOWN')} · {o.get('potential', 'UNKNOWN')} · best { {'H1': '1h', 'H4': '4h', 'H12': '12h', 'H24': '24h', 'D3': '3d', 'D7': '7d'}.get(o.get('best_horizon'), 'n/a') } ({o.get('horizon_reason', 'unknown')})",
        ]
        spot = (m.get("spot") or {}).get(m.get("primary_window", "W4H"), {})
        excluded = (spot.get("coverage") or {}).get("excluded") or []
        if excluded:
            lines.append(
                "Excluded: "
                + "; ".join(
                    str(x.get("venue")) + " — " + str(x.get("reason", "reason unavailable")) for x in excluded
                )
            )
        lines += [name + ": " + str(text) for name, text in o.get("scenarios", {}).items()]
        if d.get("tracking_ended") or d.get("actionable") is False:
            lines.append("No longer following: call expired.")
        else:
            lines += [
                f"WHAT TO DO: {d.get('action', 'WAIT')}"
                + (" (tactical)" if d.get("tactical") else "")
                + f" — Thesis {d.get('thesis_grade', 'n/a')} · Trade {d.get('trade_grade', 'n/a')}"
            ]
            lines.append("WHAT CHANGES THE ACTION:")
            for role, symbol in [("UPGRADE", "↑"), ("DOWNGRADE", "↓")]:
                condition = next((c for c in d.get("conditions", []) if c.get("role") == role), {})
                lines.append(
                    symbol
                    + " "
                    + condition.get("human_text", "n/a (condition unavailable)")
                    .replace("two consecutive hourly windows", "two hourly windows")
                    .replace("Spot BUY with most volume buying", "Majority-volume spot BUY")
                    .replace("Spot SELL with most volume selling", "Majority-volume spot SELL")
                )
        lines.append(f"CONFIDENCE: {o.get('confidence', 'LOW')} — main unknown: " + _unknown(dc))
    text = "\n".join(lines)
    if len(text) > max_chars:
        raise ValueError("CARD_LENGTH: card exceeds 3500 characters; shorten verdict, facts or WHY")
    return text


def lint_card(text, context):
    errors = []

    def fail(rule, sentence):
        errors.append({"rule": rule, "sentence": sentence})

    max_chars, _ = _limits(context.get("event", {}))
    if len(text) > max_chars:
        fail("LENGTH", "Card exceeds 3500 characters")
    if text.count("──") > 6:
        fail("TOKENS", "More than three token blocks")
    event = context.get("event", {})
    bs = context.get("beneficiaries", [])
    mts = context.get("market_truths", [])
    if isinstance(mts, dict):
        mts = list(mts.values())
    # Each block is checked against its own token; header prose must be valid for all shown tokens.
    current = mts
    for line in text.splitlines():
        if line.startswith("──"):
            current = [m for m in mts if str(m.get("token_id")) in line] or mts
        lower = line.lower()
        diagnostic = line.startswith("Unresolved alternatives:")
        for m in current or [{}]:
            spot = (m.get("spot") or {}).get(m.get("primary_window", "W4H"), {})
            broad = spot.get("classification") == "BROAD" and (spot.get("coverage") or {}).get(
                "n_complete", 0
            ) >= int((event.get("thresholds") or {}).get("min_spot_venues", 2))
            if (
                re.search(r"\b(market-wide|broad|buying|selling)\b", lower)
                and not broad
                and not line.startswith(("↑", "↓"))
            ):
                venues = [v.get("venue", "").lower() for v in spot.get("venues", [])]
                if not any(v and v in lower for v in venues):
                    fail("L1", line)
            deriv = m.get("derivatives") or {}
            deriv = deriv if isinstance(deriv, dict) else {}
            if (
                re.search(r"\b(leverage|new positions?|oi build|open positions)\b", lower)
                and (deriv.get("synchronized") or {}).get("oi_class") != "REAL_NATIVE"
            ):
                fail("L2", line)
            if (
                re.search(r"\b(accumulation|accumulating)\b", lower)
                and m.get("move_class") != "SPOT_LED_ACCUMULATION"
                and not diagnostic
                and not line.startswith("↑")
            ):
                fail("L11", line)
            div = m.get("divergence") or {}
            trigger = div.get("triggered", div.get("trigger", False)) or (
                spot.get("direction") == "SELL" and div.get("state", "UNKNOWN") != "UNKNOWN"
            )
            if trigger and not diagnostic:
                if re.search(r"\b(weak|weakness|bearish)\b", lower):
                    fail("L12", line)
                if (
                    re.search(r"\b(absorption|absorbed)\b", lower)
                    and div.get("state") not in {"ABSORPTION_CONFIRMED", "CONFIRMED"}
                    and "PASSIVE_SPOT_ABSORPTION" not in line
                ):
                    fail("L12", line)
            if re.search(r"\b(trend|increasing|decreasing)\b", lower) and not all(
                spot.get("cohort_consistent", {}).values()
            ):
                fail("L6", line)
            if (
                "market-wide" in lower
                and "liquidation" in lower
                and any(v.get("liq_coverage") != "COMPLETE" for v in deriv.get("per_venue", []))
            ):
                fail("L5", line)
        state = event.get("event_state", event.get("state", "UNKNOWN"))
        verified = event.get("state_verified", event.get("verified", False))
        if (
            not line.startswith(("Stage:", "VERIFIED:", "↑", "↓"))
            and not lower.startswith("not verified:")
            and "unverified" not in lower
            and re.search(r"\b(live|launched|adopted|integrated|approved)\b", lower)
        ):
            required_live = bool(re.search(r"\b(live|launched|adopted|integrated)\b", lower))
            if not verified or state not in (
                {"LIVE", "MONEY_FLOWING"}
                if required_live
                else {"APPROVED", "SIGNED", "DEPLOYED", "LIVE", "MONEY_FLOWING"}
            ):
                fail("L3", line)
        if re.search(r"\b(revenue to holders|buyback|burn|value accrual)\b", lower) and not any(
            b.get("capture_class", b.get("capture")) in {"DIRECT", "INDIRECT"} and b.get("mechanism_evidence")
            for b in bs
        ):
            fail("L4", line)
        if re.search(r"(volume|tvl).*(capture|accrual)", lower):
            fail("L4", line)
        if re.search(r"\b(chance|probability|odds)\b|%\s*likely", lower):
            fail("L7", line)
        if re.match(r"(base|upside|failure):", lower) and re.search(r"\d+\s*[-–]\s*\d+", line):
            fail("L7", line)
        if (
            re.search(r"\b(new|fresh|just|breaking)\b", lower)
            and event.get("freshness") not in {"FRESH", "NORMAL"}
            and not line.startswith("Stage:")
        ):
            fail("L10", line)
        if re.search(r"n/a.*\b0\b", lower):
            fail("L9", line)
        if line.startswith("IMPACT:") and not re.search(
            r"(?<![\w])\d+(?:\.\d+)?(?![\w])",
            re.sub(r"\d+\s*(?:hours?|days?|weeks?|months?|years?|[hmds])\b", "", line, flags=re.I),
        ):
            continue
        numeric_prose = re.sub(r"§\s*\d+(?:\.\d+)*(?:[–-]\d+(?:\.\d+)*)?", "", line)
        if re.search(r"(?<![\w])\d+(?:\.\d+)?", numeric_prose) and not line.startswith(
            ("Stage:", "Scores", "──", "↑", "↓", "OUTLOOK:", "VERIFIED:", "WHO:", "Token capture:")
        ):
            if not re.search(
                r"\b(?:\d+\s*[hmds]|EVENT|W1H|W4H|W24H|hour|event)\b", line, re.I
            ) or not re.search(r"%|\b(?:USD|bps|tokens?|WLD|BTC|ETH)\b|\$", line):
                fail("L8", line)
    return list({(e["rule"], e["sentence"]): e for e in errors}.values())
