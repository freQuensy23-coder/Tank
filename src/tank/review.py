"""Offline shadow review. Outcomes describe frozen calls; they never calibrate rules."""

from datetime import timedelta
from decimal import Decimal, localcontext
from statistics import median

from tank.util import iso, parse_time, sha256, utcnow

HOURS = {"H1": 1, "H4": 4, "H12": 12, "H24": 24, "D3": 72, "D7": 168}
CHECKS = {
    "state_overstated",
    "usd_oi_new_positions",
    "accumulation_without_checks",
    "secondary_exists",
    "secondary_accepted",
    "reproducible_verdict",
}


def price_return(endpoint, entry):
    """Keep monetary subtraction and division exact before statistical conversion."""
    endpoint, entry = Decimal(str(endpoint)), Decimal(str(entry))
    with localcontext() as context:
        context.prec = max(len(endpoint.as_tuple().digits), len(entry.as_tuple().digits), 40) + 40
        return (endpoint - entry) / entry


def public_bounds(event):
    """Represent source precision without interpreting legacy timestamps as exact."""
    public = event.get("public_at")
    precision = event.get("public_at_prec", event.get("public_at_precision"))
    if isinstance(public, dict):
        precision = public.get("precision", precision)
        public = public.get("value")
    precision = precision or "UNKNOWN"
    if not public:
        return None, None, precision
    point = parse_time(public)
    if precision == "EXACT":
        return point, point, precision
    if precision == "BUCKET_1H":
        lower = point.replace(minute=0, second=0, microsecond=0)
        return lower, lower + timedelta(hours=1), precision
    if precision == "BUCKET_1D":
        lower = point.replace(hour=0, minute=0, second=0, microsecond=0)
        return lower, lower + timedelta(days=1), precision
    return None, None, "UNKNOWN"


def validate_checks(checks):
    if checks is None:
        return []
    if not isinstance(checks, list):
        raise ValueError("checks must be a list")  # noqa: TRY004 - CLI input validation
    seen = set()
    for row in checks:
        if not isinstance(row, dict):
            raise ValueError("each check must be an object")  # noqa: TRY004 - CLI input validation
        event_check = isinstance(row.get("event_id"), str) and "alert_id" not in row and "version" not in row
        alert_check = (
            isinstance(row.get("alert_id"), str) and type(row.get("version")) is int and "event_id" not in row
        )
        if not (event_check or alert_check):
            raise ValueError("each check requires event_id OR alert_id and integer version")
        if event_check and set(row) & (CHECKS - {"secondary_exists", "secondary_accepted"}):
            raise ValueError("event checks support secondary audit fields only")
        key = (row.get("event_id", row.get("alert_id")), row.get("version"))
        if key in seen:
            raise ValueError("duplicate alert version checks")
        seen.add(key)
        if set(row) - CHECKS - {"alert_id", "event_id", "version", "reviewer", "reason"} or any(
            type(row[k]) is not bool for k in CHECKS & row.keys()
        ):
            raise ValueError("unknown check or non-boolean value")
        if not row.get("reviewer") or not row.get("reason"):
            raise ValueError("structured checks require reviewer and reason")
    return checks


class ReviewService:
    def __init__(self, repo, settings=None):
        self.repo, self.settings = repo, settings

    async def rows(self, table, field, start=None, end=None, filters=None):
        return [r async for r in self.repo.iter_range(table, field, start=start, end=end, filters=filters)]

    async def list(self, start=None, end=None, limit=100, after=None, event_id=None):
        if not 1 <= limit <= 10000:
            raise ValueError("limit must be 1..10000")
        return await self.repo.list_range(
            "out.alerts",
            "generated_at",
            start=start,
            end=end,
            filters={"event_id": event_id} if event_id else None,
            limit=limit,
            after=after,
        )

    async def evidence(self, alert):
        digest = alert.get("evidence_bundle_id")
        row = await self.repo.get("out.evidence_bundles", digest) if digest else None
        payload = row.get("payload") if row else None
        valid = isinstance(payload, dict) and sha256(payload) == digest and row.get("sha256") == digest
        return {
            "valid": valid,
            "reason": None if valid else "MISSING_OR_HASH_MISMATCH",
            "payload": payload if valid else None,
        }

    async def show(self, subject, *, now=None, start=None, end=None, limit=100, after=None):
        if not 1 <= limit <= 10000:
            raise ValueError("limit must be 1..10000")
        event = await self.repo.get("evt.events", subject)
        filters = {"event_id": subject} if event else {"alert_id": subject}
        alerts = await self.repo.list_range(
            "out.alerts", "generated_at", start=start, end=end, filters=filters, limit=limit + 1, after=after
        )
        result = []
        for alert in alerts[:limit]:
            evidence = await self.evidence(alert)
            delivery = await self.rows(
                "out.deliveries", "at", filters={"alert_id": alert["alert_id"], "version": alert["version"]}
            )
            outcomes = []
            if evidence["valid"]:
                for decision in evidence["payload"].get("decisions", []):
                    truth = next(
                        (
                            m
                            for m in evidence["payload"].get("market_truth", [])
                            if m.get("mta_id")
                            == decision.get("mta_id", decision.get("provenance", {}).get("mta_id"))
                        ),
                        {},
                    )
                    outcomes.append(await self.outcome(alert, decision, truth, now=now))
            result.append({"alert": alert, "evidence": evidence, "delivery": delivery, "outcomes": outcomes})
        return {
            "subject": subject,
            "versions": result,
            "reviews": await self.repo.list("qa.reviews", filters={"subject": subject}, limit=10000),
            "pagination": {"limit": limit, "returned": len(result), "has_more": len(alerts) > limit},
        }

    async def horizon_series(self, instrument, start, end, now, *, extrema=True):
        """Exact full minutes plus enumerated raw trades for partial boundaries."""
        floor_start = start.replace(second=0, microsecond=0)
        floor_end = end.replace(second=0, microsecond=0)
        ceil_start = floor_start + timedelta(minutes=1) if start != floor_start else start
        bars = await self.rows(
            "mkt.klines_1m", "open_time", iso(ceil_start), iso(floor_end), {"instrument_id": instrument}
        )
        fields = ("h", "l", "c") if extrema else ("c",)
        complete = [
            b
            for b in bars
            if b.get("complete") is True
            and all(b.get(k) is not None for k in fields)
            and (not b.get("received_ts") or parse_time(b["received_ts"]) <= now)
        ]
        expected = max(0, int((floor_end - ceil_start).total_seconds() / 60))
        exact = len(complete) == expected and len({b["open_time"] for b in complete}) == expected
        segments = []
        if start != floor_start:
            segments.append((start, min(ceil_start, end)))
        if end != floor_end and floor_end >= ceil_start:
            segments.append((max(start, floor_end), end))
        prices = []
        evidence = list(complete)
        proofs = []
        final_trades = []
        for lo, hi in segments:
            minute = lo.replace(second=0, microsecond=0)
            all_trades = await self.rows(
                "mkt.trades",
                "ts",
                iso(minute),
                iso(minute + timedelta(minutes=1)),
                {"instrument_id": instrument},
            )
            all_trades = [
                r for r in all_trades if not r.get("received_ts") or parse_time(r["received_ts"]) <= now
            ]
            unique = {(r.get("trade_id"), r.get("ts")): r for r in all_trades}
            flow = await self.rows(
                "mkt.flow_bars_1m",
                "minute_start",
                iso(minute),
                iso(minute + timedelta(minutes=1)),
                {"instrument_id": instrument},
            )
            proven = any(
                b.get("complete") is True
                and b.get("n_buy") is not None
                and b.get("n_sell") is not None
                and len(unique) == int(b["n_buy"]) + int(b["n_sell"])
                for b in flow
            )
            if not proven:
                coverage = await self.repo.list(
                    "qa.reviews",
                    filters={"subject": "MARKET_CAPTURE_COVERAGE", "instrument_id": instrument},
                    limit=100000,
                )
                proven = any(
                    r.get("complete") is True
                    and parse_time(r["start"]) <= minute
                    and parse_time(r["end"]) >= minute + timedelta(minutes=1)
                    and parse_time(r["at"]) <= now
                    and r.get("minute_trade_counts", {}).get(iso(minute)) == len(unique)
                    for r in coverage
                )
            selected = [r for r in unique.values() if lo <= parse_time(r["ts"]) < hi]
            selected.sort(key=lambda r: (r["ts"], str(r.get("trade_id"))))
            usable = all(
                r.get("trade_id") is not None and r.get("price", r.get("p")) is not None for r in selected
            )
            exact = exact and proven and usable
            prices.extend(
                Decimal(str(r.get("price", r.get("p"))))
                for r in selected
                if r.get("price", r.get("p")) is not None
            )
            evidence.extend(selected)
            proofs.append(
                {
                    "start": iso(lo),
                    "end": iso(hi),
                    "complete": proven and usable,
                    "trade_count": len(selected),
                }
            )
            if hi == end:
                final_trades = selected
        complete.sort(key=lambda b: b["open_time"])
        endpoint = (
            Decimal(str(final_trades[-1].get("price", final_trades[-1].get("p"))))
            if final_trades
            else Decimal(str(complete[-1]["c"]))
            if complete
            else None
        )
        highs = [Decimal(str(b["h"])) for b in complete] + prices if extrema else []
        lows = [Decimal(str(b["l"])) for b in complete] + prices if extrema else []
        return {
            "endpoint": endpoint,
            "high": max(highs) if highs else None,
            "low": min(lows) if lows else None,
            "evidence_hash": sha256(evidence),
            "coverage": {
                "expected_minutes": expected,
                "complete_minutes": len(complete),
                "exact": bool(exact and endpoint is not None),
                "partial_boundary": bool(segments),
                "boundary_segments": proofs,
            },
        }

    async def outcome(self, alert, decision, truth, *, now=None):
        now = parse_time(now) if now else utcnow()
        start = parse_time(decision.get("cutoff") or alert["generated_at"])
        horizon = decision.get("outlook", {}).get("best_horizon")
        result = {
            "dec_id": decision.get("dec_id"),
            "token_id": decision.get("token_id"),
            "horizon": horizon,
            "start": iso(start),
            "realized_return": None,
            "mfe": None,
            "mae": None,
            "excursion_basis": "RAW_PRICE_RETURN_HIGH_LOW",
            "btc_adjusted_return": None,
            "direction_correct": None,
        }
        if horizon not in HOURS:
            return dict(result, status="UNAVAILABLE", reason="FROZEN_HORIZON_MISSING")
        end = start + timedelta(hours=HOURS[horizon])
        result["end"] = iso(end)
        if now < end:
            fired = await self.rows(
                "ana.watch_conditions",
                "fired_at",
                iso(start),
                iso(now),
                {"event_id": alert.get("event_id"), "token_id": decision.get("token_id")},
            )
            result["conditions_fired"] = [
                {"cond_id": r["cond_id"], "fired_at": r["fired_at"]} for r in fired if r.get("fired_at")
            ]
            return dict(result, status="PENDING", reason="HORIZON_NOT_REACHED")
        relative = truth.get("relative", {})
        entry = relative.get("entry_ref_price")
        instrument = relative.get("ref_venue")
        series = await self.horizon_series(instrument, start, end, now) if instrument else None
        result["coverage"] = (
            series["coverage"] if series else {"exact": False, "reason": "FROZEN_REFERENCE_MISSING"}
        )
        result["market_evidence_hash"] = series["evidence_hash"] if series else None
        if (
            not series
            or not series["coverage"]["exact"]
            or not entry
            or series["high"] is None
            or series["low"] is None
        ):
            result.update(status="UNAVAILABLE", reason="PARTIAL_BOUNDARY_OR_INCOMPLETE_MARKET_DATA")
        else:
            entry = Decimal(str(entry))
            raw_return_decimal = price_return(series["endpoint"], entry)
            result.update(
                status="COMPLETED",
                reason=None,
                realized_return=float(raw_return_decimal),
                mfe=float(price_return(series["high"], entry)),
                mae=float(price_return(series["low"], entry)),
            )
        conditions = await self.rows(
            "ana.watch_conditions",
            "fired_at",
            iso(start),
            iso(end),
            {"event_id": alert["event_id"], "token_id": decision.get("token_id")},
        )
        result["conditions_fired"] = [
            {"cond_id": r["cond_id"], "fired_at": r["fired_at"]} for r in conditions if r.get("fired_at")
        ]
        # A horizon-specific pre-call normal range is mandatory. Never borrow EVENT or refit beta.
        window = relative.get("windows", {}).get(
            {"H1": "W1H", "H4": "W4H", "H12": "W12H", "H24": "W24H", "D3": "W3D", "D7": "W7D"}[horizon], {}
        )
        nr = (
            relative.get("normal_ranges", {})
            .get(
                {"H1": "W1H", "H4": "W4H", "H12": "W12H", "H24": "W24H", "D3": "W3D", "D7": "W7D"}[horizon],
                {},
            )
            .get("value", window.get("normal_range"))
        )
        beta = relative.get("beta_btc")
        result["direction_reason"] = "FROZEN_BTC_ENDPOINTS_OR_HORIZON_NORMAL_RANGE_UNAVAILABLE"
        btc = relative.get("outcome_btc_reference", {})
        if (
            result["realized_return"] is not None
            and nr is not None
            and beta is not None
            and btc.get("instrument_id")
            and btc.get("entry_ref_price")
        ):
            benchmark = await self.horizon_series(btc["instrument_id"], start, end, now, extrema=False)
            result["btc_coverage"] = benchmark["coverage"]
            result["btc_market_evidence_hash"] = benchmark["evidence_hash"]
            if benchmark["coverage"]["exact"]:
                with localcontext() as context:
                    context.prec = 80
                    adjusted = raw_return_decimal - Decimal(str(beta)) * price_return(
                        benchmark["endpoint"], btc["entry_ref_price"]
                    )
                bias = decision.get("outlook", {}).get("direction")
                realized = (
                    "BULLISH"
                    if adjusted > Decimal(str(nr[1]))
                    else "BEARISH"
                    if adjusted < Decimal(str(nr[0]))
                    else "NEUTRAL"
                )
                result.update(
                    btc_adjusted_return=float(adjusted),
                    direction_correct=bias == realized if bias in ("BULLISH", "BEARISH", "NEUTRAL") else None,
                    direction_reason=None,
                    frozen_beta=beta,
                    frozen_normal_range=nr,
                )
        if result["status"] == "COMPLETED":
            digest = sha256(result)
            key = sha256([alert["alert_id"], alert["version"], decision.get("dec_id"), digest])
            async with self.repo.lock(
                "outcome:" + sha256([alert["alert_id"], alert["version"], decision.get("dec_id")])
            ):
                old = await self.repo.list(
                    "qa.outcomes",
                    filters={
                        "alert_id": alert["alert_id"],
                        "version": alert["version"],
                        "dec_id": decision.get("dec_id"),
                    },
                    limit=10000,
                )
                existing = next((r for r in old if r.get("input_hash") == digest), None)
                record = existing or dict(
                    result,
                    outcome_id=key,
                    alert_id=alert["alert_id"],
                    version=alert["version"],
                    computed_at=iso(now),
                    revision=max([r.get("revision", 0) for r in old], default=0) + 1,
                    input_hash=digest,
                    config_version=alert.get("config_version"),
                    code_version=alert.get("code_version"),
                )
                if not existing:
                    await self.repo.put("qa.outcomes", key, record, immutable=True)
                return record

        return result

    async def report(self, start, end, *, checks=None, now=None):
        if not start or not end or parse_time(start) >= parse_time(end):
            raise ValueError("a nonempty explicit period is required")
        audit_rows = validate_checks(checks)
        checked = {(r["alert_id"], r["version"]): r for r in audit_rows if "alert_id" in r}
        event_checks = {r["event_id"]: r for r in audit_rows if "event_id" in r}
        alerts = await self.rows("out.alerts", "generated_at", start, end)
        labels = await self.rows("qa.reviews", "at", end=iso(now or utcnow()))
        latest = {}
        for r in sorted(labels, key=lambda r: r["at"]):
            latest[(r["subject"], r.get("version") or 1)] = r
        metrics = {}

        def rate(name, values, total, target, *, count=False):
            known = [v for v in values if v is not None]
            numerator = sum(known)
            value = (numerator if count else numerator / len(known)) if known else None
            verified = total > 0 and len(known) == total
            metrics[name] = {
                "value": value,
                "numerator": numerator if known else None,
                "denominator": len(known),
                "eligible": total,
                "coverage": len(known) / total if total else None,
                "status": ("PASS" if target(value) else "FAIL") if verified else "UNVERIFIED",
            }

        first = []
        sent = []
        stale = []
        low = []
        noise = []
        causes = []
        reproducible = []
        major_events = {}
        public_precision = {"EXACT": 0, "BUCKET_1H": 0, "BUCKET_1D": 0, "UNKNOWN": 0}
        stale_precision = {"EXACT": 0, "BUCKET_1H": 0, "BUCKET_1D": 0, "UNKNOWN": 0}
        semantic = {k: [] for k in ("S3", "S4", "S5")}
        for alert in alerts:
            ev = await self.evidence(alert)
            payload = ev["payload"] or {}
            event = payload.get("event", {})
            ems = payload.get("ems", {})
            key = (alert["alert_id"], alert["version"])
            check = checked.get(key, {})
            known = event.get("known_to_us_at")
            lower, upper, precision = public_bounds(event)
            stale_precision[precision] += 1
            if known and lower is not None:
                minimum = (parse_time(known) - upper).total_seconds()
                maximum = (parse_time(known) - lower).total_seconds()
                stale.append(True if minimum > 172800 else False if maximum <= 172800 else None)
            else:
                stale.append(None)
            deliveries = await self.rows(
                "out.deliveries", "at", filters={"alert_id": alert["alert_id"], "version": alert["version"]}
            )
            success = [d for d in deliveries if d.get("status") in ("SENT", "DELIVERED")]
            sent.append(
                (min(parse_time(d["at"]) for d in success) - parse_time(known)).total_seconds()
                if success and known
                else None
            )
            category = ems.get("class")
            low.append(category in ("LOW", "MATERIAL") if category else None)
            label_row = latest.get((alert["alert_id"], alert["version"]), {})
            label = label_row.get("label")
            if category in ("MAJOR", "STRUCTURAL_SHOCK", "STRUCTURAL SHOCK"):
                noise.append(label == "NOISE" if label else None)
                major_events.setdefault(alert["event_id"], []).append(check)
            for name, field in [
                ("S3", "state_overstated"),
                ("S4", "usd_oi_new_positions"),
                ("S5", "accumulation_without_checks"),
            ]:
                semantic[name].append(check.get(field))
            truths = payload.get("market_truth", [])
            decision_ids = set(alert.get("decisions", []))
            decisions = [d for d in payload.get("decisions", []) if d.get("dec_id") in decision_ids]
            causes.extend([None] * len(decision_ids - {d.get("dec_id") for d in decisions}))
            for decision in decisions:
                truth = next((t for t in truths if t.get("token_id") == decision.get("token_id")), None)
                causes.append(
                    bool(
                        truth.get("move_class")
                        and truth["move_class"] != "UNKNOWN"
                        and truth.get("decisive_facts")
                    )
                    if truth
                    else None
                )
            reproducible.append(
                bool(ev["valid"] and deliveries and check["reproducible_verdict"])
                if "reproducible_verdict" in check
                else None
            )
        # S1 public-to-known uses events (one per fact), not repeated card versions.
        events = await self.rows("evt.events", "known_to_us_at", start, end)
        for event in events:
            ems = (
                await self.repo.get("evt.ems_assessments", event.get("current_ems_id"))
                if event.get("current_ems_id")
                else None
            )
            if ems and ems.get("class") in ("MAJOR", "STRUCTURAL_SHOCK", "STRUCTURAL SHOCK"):
                major_events.setdefault(event["event_id"], [])
            observations = await self.repo.list(
                "evt.event_observations", filters={"event_id": event["event_id"]}, limit=100000
            )
            sources = []
            for observation in observations:
                item = await self.repo.get("ingest.source_items", observation.get("item_id"))
                if item:
                    sources.append(item.get("source_id"))
            if not sources or any(s in {f"S{i:02d}" for i in range(1, 25)} for s in sources):
                lower, upper, precision = public_bounds(event)
                public_precision[precision] += 1
                first.append(
                    (parse_time(event["known_to_us_at"]) - lower).total_seconds()
                    if sources and precision == "EXACT" and lower is not None
                    else None
                )

        def latency(values, target):
            known = [v for v in values if v is not None and v >= 0]
            verified = bool(values) and len(known) == len(values)
            return {
                "seconds": median(known) if known else None,
                "denominator": len(known),
                "eligible": len(values),
                "coverage": len(known) / len(values) if values else None,
                "status": ("PASS" if median(known) <= target else "FAIL") if verified else "UNVERIFIED",
            }

        metrics["S1"] = {"public_to_known": latency(first, 120), "known_to_sent": latency(sent, 480)}
        metrics["S1"]["public_to_known"]["precision_counts"] = public_precision
        rate("S2", stale, len(alerts), lambda v: v == 0, count=True)
        metrics["S2"]["precision_counts"] = stale_precision
        for k, v in semantic.items():
            rate(k, v, len(alerts), lambda n: n == 0, count=True)
        rate("S6", causes, len(causes), lambda v: v == 1)
        secondary = []
        for event_id, reviews in major_events.items():
            if event_id in event_checks:
                reviews = [event_checks[event_id]]
            existence = [r.get("secondary_exists") for r in reviews if "secondary_exists" in r]
            if not existence:
                secondary.append(None)
            elif any(existence):
                accepted = [r.get("secondary_accepted") for r in reviews if r.get("secondary_exists") is True]
                secondary.append(any(accepted) if all(v is not None for v in accepted) else None)
        rate("S7", secondary, len(secondary), lambda v: v >= 0.7)
        rate("S8_low_material", low, len(alerts), lambda v: v == 0, count=True)
        rate("S8_major_noise", noise, len(noise), lambda v: v <= 0.2)
        rate("S9", reproducible, len(alerts), lambda v: v == 1)
        return {
            "period": {"start": iso(parse_time(start)), "end": iso(parse_time(end))},
            "metrics": metrics,
            "cards": len(alerts),
            "shadow_acceptance": "UNVERIFIED",
            "note": "Descriptive ledger report; offline tests do not establish 14 days of live shadow operation.",
        }
