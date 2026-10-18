"""Durable timer creation, watch evaluation and reaction checkpoints."""

import asyncio
import logging
from datetime import timedelta
from decimal import Decimal

from tank.util import iso, new_id, parse_time, utcnow

log = logging.getLogger(__name__)
CHECKPOINTS = {
    "PRE_24H": -86400,
    "PRE_4H": -14400,
    "PRE_1H": -3600,
    "POST_15M": 900,
    "POST_1H": 3600,
    "POST_4H": 14400,
    "POST_24H": 86400,
    "POST_72H": 259200,
}


def field(record, path):
    value = record
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def compare(actual, op, target):
    if actual is None:
        return False
    if op in (">", ">=", "<", "<="):
        a, b = Decimal(str(actual)), Decimal(str(target))
        return {">": a > b, ">=": a >= b, "<": a < b, "<=": a <= b}[op]
    if op == "==":
        return actual == target
    if op == "!=":
        return actual != target
    if op == "in":
        return actual in target
    raise ValueError(f"Unsupported watch operator: {op}")


class Scheduler:
    def __init__(self, repo, settings, market, truth):
        self.repo, self.settings, self.market, self.truth = repo, settings, market, truth
        self.background = {}
        self.closing = False

    def threshold(self, name, fixture_default):
        if not hasattr(self.settings, "load_config"):
            return fixture_default  # Explicit lightweight injected test settings only.
        thresholds = self.settings.load_config("thresholds")
        if name not in thresholds:
            # Incomplete injected test configuration; production validates required keys.
            from tank.settings import Settings

            if isinstance(self.settings, Settings):
                raise ValueError("Missing scheduler threshold: " + name)
            return fixture_default
        return thresholds[name]

    def launch(self, name, operation):
        if self.closing:
            return
        previous = self.background.get(name)
        if previous is not None and not previous.done():
            return

        async def guarded():
            try:
                await operation()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("%s maintenance failed; next tick will retry", name)

        self.background[name] = asyncio.create_task(guarded(), name="scheduler-" + name)

    async def close(self):
        self.closing = True
        for task in self.background.values():
            task.cancel()
        await asyncio.gather(*self.background.values(), return_exceptions=True)
        self.background.clear()

    async def run(self, stop):
        try:
            while not stop.is_set():
                try:
                    await self.tick()
                except Exception:
                    log.exception("Scheduler tick failed")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=30)
                except TimeoutError:
                    pass
        finally:
            await self.close()

    async def tick(self, now=None):
        now = now or utcnow()
        bucket = iso(now.replace(second=0, microsecond=0))
        await self.repo.enqueue(
            "EVAL_CONDITIONS",
            {"cutoff": bucket},
            bucket,
            priority=2,
            expires_at=iso(now + timedelta(minutes=5)),
            max_attempts=2,
        )
        async for event in self.active_events(now):
            milestone = event.get("next_milestone")
            if (
                milestone
                and milestone.get("due_at")
                and milestone.get("status") not in {"CONFIRMED", "VERIFIED", "FAILED", "MISSED"}
                and parse_time(milestone["due_at"]) <= now
            ):
                due = iso(milestone["due_at"])
                kind = milestone.get("kind", milestone.get("milestone"))
                expires = milestone.get("expires_at") or iso(parse_time(due) + timedelta(hours=2))
                if parse_time(expires) > now:
                    await self.repo.enqueue(
                        "MILESTONE_CHECK",
                        {
                            "event_id": event["event_id"],
                            "milestone": kind,
                            "due_at": due,
                            "source_id": milestone.get("source_id"),
                        },
                        f"{event['event_id']}:{kind}:{due}",
                        priority=1,
                        expires_at=expires,
                    )
            assessment = await self.latest_assessment(event)
            if not assessment:
                continue
            if assessment.get("class") not in ("MAJOR", "STRUCTURAL_SHOCK"):
                continue
            anchor = self.anchor(event)
            tokens = await self.analysis_tokens(event)
            for token in tokens:
                for label, offset in CHECKPOINTS.items():
                    due = parse_time(anchor["ts"]) + timedelta(seconds=max(0, offset))
                    existing = await self.repo.list(
                        "out.reaction_checkpoints",
                        filters={"event_id": event["event_id"], "token_id": token, "checkpoint": label},
                        limit=1,
                    )
                    if existing:
                        continue
                    await self.repo.enqueue(
                        "REACTION_CAPTURE",
                        {
                            "event_id": event["event_id"],
                            "token_id": token,
                            "checkpoint": label,
                            "anchor": anchor,
                            "cutoff": iso(parse_time(anchor["ts"]) + timedelta(seconds=offset)),
                            "retro": due < now,
                        },
                        f"{event['event_id']}:{token}:{label}",
                        priority=2,
                        run_after=iso(due),
                        expires_at=iso(max(due, now) + timedelta(hours=6)),
                        max_attempts=5,
                    )
        await self.expire_calls(now)
        await self.demote(now)
        await self.lifecycle(now)
        self.launch("scout", lambda: self.detect_scout_hooks(now))
        if self.market is not None:
            self.launch("late-reactions", lambda: self.detect_late_data(now))
        heartbeat_hour, heartbeat_minute = map(int, str(self.threshold("heartbeat_utc", "08:00")).split(":"))
        if (now.hour, now.minute) >= (heartbeat_hour, heartbeat_minute):
            await self.heartbeat(now)
        self.launch("backup", lambda: self.maintenance(now))
        self.launch("partitions", lambda: self.partition_maintenance(now))

    async def partition_maintenance(self, now):
        marker = await self.repo.get("ops.scheduler_checkpoints", "partitions")
        if marker and now - parse_time(marker["at"]) < timedelta(hours=1):
            return
        await self.repo.maintain_partitions()
        await self.repo.put(
            "ops.scheduler_checkpoints",
            "partitions",
            {"checkpoint_id": "partitions", "at": iso(now), "state": {}},
        )

    async def active_events(self, now, *, states=None, include_recent_terminal=True):
        states = states or ("DETECTED", "SCORED", "TRACKED", "ANALYZING", "ALERTED", "MONITORING", "COOLDOWN")
        seen = set()
        for state in states:
            async for event in self.repo.iter_all("evt.events", filters={"lifecycle": state}):
                if event["event_id"] not in seen:
                    seen.add(event["event_id"])
                    yield event
        if include_recent_terminal:
            async for event in self.repo.iter_range(
                "evt.events",
                "known_to_us_at",
                start=now - timedelta(hours=72),
                end=now + timedelta(microseconds=1),
            ):
                if event["event_id"] not in seen and event.get("lifecycle") != "MERGED":
                    seen.add(event["event_id"])
                    yield event

    async def analysis_tokens(self, event):
        latest = None
        async for row in self.repo.iter_all("evt.beneficiary_maps", filters={"event_id": event["event_id"]}):
            if latest is None or row.get("map_version", 0) > latest.get("map_version", 0):
                latest = row
        if latest is not None:
            tokens = set(latest.get("analysis_set", []))
        else:
            edges = [
                row
                async for row in self.repo.iter_all(
                    "evt.beneficiary_edges", filters={"event_id": event["event_id"]}
                )
            ]
            version = max((row.get("map_version", 0) for row in edges), default=0)
            tokens = set(event.get("analysis_set", [])) | {
                row["token_id"]
                for row in edges
                if row.get("token_id") and row.get("map_version", 0) == version
            }
        return tokens | {"bitcoin:BTC", "ethereum:ETH"}

    async def latest_assessment(self, event):
        if event.get("current_ems_id"):
            current = await self.repo.get("evt.ems_assessments", event["current_ems_id"])
            if current is not None:
                return current
        latest = None
        async for row in self.repo.iter_all("evt.ems_assessments", filters={"event_id": event["event_id"]}):
            if latest is None or row.get("version", 0) > latest.get("version", 0):
                latest = row
        return latest

    async def expire_calls(self, now):
        async for call in self.repo.iter_range(
            "ana.calls",
            "opened_at",
            end=now - timedelta(days=self.threshold("call_expiry_days", 7)) + timedelta(microseconds=1),
            filters={"status": "OPEN"},
        ):
            if parse_time(call["opened_at"]) + timedelta(days=self.threshold("call_expiry_days", 7)) <= now:
                async with self.repo.lock("event:" + call["event_id"]):
                    event = await self.repo.get("evt.events", call["event_id"])
                    if not event:
                        raise ValueError("Expiring call references a missing event")
                    expired_ids = sorted(set(event.get("call_expired_ids", [])) | {call["call_id"]})
                    await self.repo.put(
                        "evt.events",
                        call["event_id"],
                        {**event, "call_expired": True, "call_expired_ids": expired_ids},
                    )
                    await self.repo.put(
                        "ana.calls",
                        call["call_id"],
                        {**call, "status": "CLOSED", "closed_by": "EXPIRED", "closed_at": iso(now)},
                    )
                    await self.message("CALL_EXPIRED", call["event_id"], call["call_id"], call=call)

    @staticmethod
    def anchor(event):
        from tank.market.capture import event_anchor

        return event_anchor(event)

    async def message(self, kind, event_id, key, **extra):
        await self.repo.enqueue(
            "AGENT_MESSAGE",
            {"kind": kind, "event_id": event_id, **extra},
            f"{event_id}:{kind}:{key}",
            priority=1,
            expires_at=iso(utcnow() + timedelta(days=1)),
        )

    async def eval_conditions(self, payload):
        cutoff = payload.get("cutoff") or iso()
        cache = {}
        async for cond in self.repo.iter_all("ana.watch_conditions", filters={"status": "ARMED"}):
            async with self.repo.lock("condition:" + cond["cond_id"]):
                cond = await self.repo.get("ana.watch_conditions", cond["cond_id"])
                if cond["status"] != "ARMED":
                    continue
                if not cond.get("expires_at"):
                    log.error(
                        "Watch condition %s has no expiry; requires decision reconciliation", cond["cond_id"]
                    )
                    continue
                if parse_time(cond["expires_at"]) <= parse_time(cutoff):
                    await self.repo.put(
                        "ana.watch_conditions", cond["cond_id"], {**cond, "status": "EXPIRED"}
                    )
                    continue
                expr = cond["expr"]
                if "milestone" in expr:
                    continue
                event = await self.repo.get("evt.events", cond["event_id"])
                contradictions = [
                    row
                    async for row in self.repo.iter_all(
                        "ana.contradictions", filters={"event_id": cond["event_id"], "type": "FATAL"}
                    )
                ]
                event = {
                    **event,
                    "contradiction_fatal": any(
                        r.get("type") == "FATAL" and not r.get("resolved_at") for r in contradictions
                    ),
                }

                def window_seconds(expression):
                    window = expression.get("window", "W1H").lstrip("W").lower()
                    return int(window[:-1]) * {"m": 60, "h": 3600, "d": 86400}[window[-1]]

                def persistence_windows(expression):
                    if "all" in expression:
                        return [n for child in expression["all"] for n in persistence_windows(child)]
                    if (
                        not expression["metric"].startswith(("event.", "milestone."))
                        and expression.get("persist", {}).get("count", 1) > 1
                    ):
                        return [window_seconds(expression)]
                    return []

                periods = persistence_windows(expr)
                # Market persistence uses actual non-overlapping, completed window endpoints.
                # Instant event predicates never inherit this clock or its memoization.
                market_cutoff = cutoff
                if periods:
                    from datetime import datetime, timezone

                    seconds = max(periods)
                    endpoint = int(parse_time(cutoff).timestamp()) // seconds * seconds
                    market_cutoff = iso(datetime.fromtimestamp(endpoint, timezone.utc))
                counters = dict(cond.get("counters", {}))

                async def evaluate(expr, path="root"):
                    if "all" in expr:
                        results = [
                            await evaluate(child, path + str(i)) for i, child in enumerate(expr["all"])
                        ]
                        return all(results)
                    metric = expr["metric"]
                    market_metric = not metric.startswith(("event.", "milestone."))
                    required = expr.get("persist", {}).get("count", 1)
                    evaluation_cutoff = market_cutoff if market_metric else cutoff
                    seconds = window_seconds(expr)
                    window_id = int(parse_time(evaluation_cutoff).timestamp()) // seconds
                    previous = counters.get(path, {})
                    if market_metric and required > 1:
                        created = cond.get("created_at")
                        if created and parse_time(evaluation_cutoff) <= parse_time(created):
                            return False
                        if previous.get("window") == window_id:
                            return previous.get("count", 0) >= required
                    truth = {}
                    if market_metric:
                        cache_key = (cond["event_id"], cond["token_id"], evaluation_cutoff)
                        if cache_key not in cache:
                            cache[cache_key] = await self.truth.assess(*cache_key)
                        truth = cache[cache_key]
                    value = metric_value(truth, event, metric, expr.get("window", "W1H"))
                    matched = compare(value, expr["op"], expr["value"])
                    if required == 1 or not market_metric:
                        counters[path] = {
                            "window": window_id,
                            "count": int(matched),
                            "value": value,
                            "cutoff": evaluation_cutoff,
                        }
                        return matched
                    count = (
                        (previous.get("count", 0) if previous.get("window") == window_id - 1 else 0) + 1
                        if matched
                        else 0
                    )
                    counters[path] = {
                        "window": window_id,
                        "count": count,
                        "value": value,
                        "cutoff": evaluation_cutoff,
                    }
                    return count >= required

                fired = await evaluate(expr)
                updated = {**cond, "counters": counters, "last_evaluated_at": cutoff}
                if fired:
                    updated.update(status="FIRED", fired_at=cutoff)
                    async with self.repo.transaction():
                        await self.repo.put("ana.watch_conditions", cond["cond_id"], updated)
                        await self.message(
                            "CONDITION_FIRED",
                            cond["event_id"],
                            cond["cond_id"],
                            condition=updated,
                            token_id=cond["token_id"],
                        )
                else:
                    await self.repo.put("ana.watch_conditions", cond["cond_id"], updated)

    async def milestone_check(self, payload):
        event = await self.repo.get("evt.events", payload["event_id"])
        if event is None:
            raise ValueError("Milestone event does not exist")
        matches = [
            row
            for row in event.get("milestone_resolutions", [])
            if row.get("kind", row.get("milestone")) == payload["milestone"]
            and (not payload.get("due_at") or row.get("due_at") == payload["due_at"])
        ]
        resolution = matches[-1] if matches else None
        # Timer expiry is not evidence of an outcome; only accepted primary quotes resolve it.
        await self.message(
            "MILESTONE",
            payload["event_id"],
            str(payload["milestone"]) + ":" + str(payload.get("due_at", "")),
            milestone=payload["milestone"],
            outcome=resolution.get("outcome") if resolution else None,
            quotes=resolution.get("quotes", []) if resolution else [],
            evidence=resolution.get("evidence", []) if resolution else [],
            verification_required=resolution is None,
            instruction="Verify outcome against the scheduled official source; retain a quote. Mark a missed window explicitly."
            if resolution is None
            else "Accepted primary evidence resolved the scheduled milestone.",
        )

    async def reaction_capture(self, payload):
        lock = f"reaction:{payload['event_id']}:{payload['token_id']}:{payload['checkpoint']}"
        async with self.repo.lock(lock):
            return await self._reaction_capture(payload)

    async def detect_late_data(self, now):
        marker = await self.repo.get("ops.scheduler_checkpoints", "reaction-capture-changes")
        since = marker["at"] if marker else None
        async for change in self.market.capture_changes(
            since=since, end=iso(now + timedelta(microseconds=1))
        ):
            changed_at = change.get("finished_at") or change["observed_at"]
            change_id = change.get("change_id") or change["backfill_id"] + ":" + changed_at
            source_id = change.get("backfill_id") or change_id
            latest = {}
            async for row in self.repo.iter_all(
                "out.reaction_checkpoints",
                filters={"event_id": change["event_id"], "token_id": change["token_id"]},
            ):
                label = row["checkpoint"]
                if label not in latest or row.get("revision", 1) > latest[label].get("revision", 1):
                    latest[label] = row
            for label, row in latest.items():
                if parse_time(changed_at) < parse_time(row["captured_at"]):
                    continue
                if parse_time(change["anchor"]) != parse_time(row["anchor"]["ts"]):
                    continue
                key = f"{change_id}:{label}"
                await self.repo.enqueue(
                    "REACTION_CAPTURE",
                    {
                        "event_id": change["event_id"],
                        "token_id": change["token_id"],
                        "checkpoint": label,
                        "automatic_revision": True,
                        "change_id": key,
                        "reason": "Late capture completed: "
                        + source_id
                        + " ("
                        + change.get("status", change.get("kind", "CAPTURE_CHANGE"))
                        + ")",
                    },
                    "reaction-late:" + key,
                    priority=2,
                    max_attempts=5,
                    expires_at=iso(now + timedelta(hours=6)),
                )
        await self.repo.put(
            "ops.scheduler_checkpoints",
            "reaction-capture-changes",
            {"checkpoint_id": "reaction-capture-changes", "at": iso(now), "state": {}},
        )

    async def reaction_pack_id(self, event_id, token_id):
        key = f"reaction-pack:{event_id}:{token_id}"
        async with self.repo.lock(key):
            row = await self.repo.get("ops.scheduler_checkpoints", key)
            if row is None:
                row = {
                    "checkpoint_id": key,
                    "at": iso(),
                    "state": {"rwp_id": new_id("RWP"), "event_id": event_id, "token_id": token_id},
                }
                await self.repo.put("ops.scheduler_checkpoints", key, row)
            return row["state"]["rwp_id"]

    async def _reaction_capture(self, payload):
        payload = dict(payload)
        change_marker = (
            "reaction-change:" + payload["change_id"] if payload.get("automatic_revision") else None
        )
        if change_marker:
            applied = await self.repo.get("ops.scheduler_checkpoints", change_marker)
            if applied:
                return applied["state"]
            latest_auto = None
            async for row in self.repo.iter_all(
                "out.reaction_checkpoints",
                filters={
                    "event_id": payload["event_id"],
                    "token_id": payload["token_id"],
                    "checkpoint": payload["checkpoint"],
                },
            ):
                if latest_auto is None or row.get("revision", 1) > latest_auto.get("revision", 1):
                    latest_auto = row
            if latest_auto is None:
                raise ValueError("Automatic revision requires a captured checkpoint")
            payload["revision"] = latest_auto["revision"] + 1
        revision = int(payload.get("revision", 1))
        if revision < 1:
            raise ValueError("Reaction revision must be positive")
        key = f"{payload['event_id']}:{payload['token_id']}:{payload['checkpoint']}:{revision}"
        existing = await self.repo.get("out.reaction_checkpoints", key)
        if existing:
            if revision > 1 and payload.get("reason") != existing.get("reason"):
                raise ValueError("Immutable reaction revision already exists with another reason")
            return existing
        latest = None
        async for previous in self.repo.iter_all(
            "out.reaction_checkpoints",
            filters={
                "event_id": payload["event_id"],
                "token_id": payload["token_id"],
                "checkpoint": payload["checkpoint"],
            },
        ):
            if latest is None or previous["revision"] > latest["revision"]:
                latest = previous
        if revision > 1:
            if not str(payload.get("reason", "")).strip():
                raise ValueError("Late-data reaction revision requires an explicit reason")
            if not latest or revision != latest["revision"] + 1:
                raise ValueError(
                    "Reaction revisions must append the next revision after an existing checkpoint"
                )
            payload.update(anchor=latest["anchor"], retro=latest.get("retro", False))
        captured_at = iso()
        from tank.analysis.truth import compute_market_truth

        origin = parse_time(payload["anchor"]["ts"])
        offset = CHECKPOINTS[payload["checkpoint"]]
        start = origin + timedelta(seconds=offset) if offset < 0 else origin
        end = origin if offset < 0 else origin + timedelta(seconds=offset)
        snapshot = await self.market.snapshot(
            payload["token_id"], iso(end), anchor={"ts": iso(start), "type": payload["anchor"]["type"]}
        )
        snapshot.update(event_id=payload["event_id"], token_id=payload["token_id"])
        knobs = self.settings.load_config("thresholds") if hasattr(self.settings, "load_config") else {}
        truth = await asyncio.to_thread(
            compute_market_truth,
            snapshot,
            config={
                **knobs,
                "config_version": self.settings.config_version,
                "code_version": self.settings.code_version,
            },
        )
        relative = truth.get("relative", {})
        event_window = relative.get("windows", {}).get("EVENT", {})
        spot = truth.get("spot", {}).get("EVENT", {})
        deriv = truth.get("derivatives", {}).get("windows", {}).get("EVENT", {})
        sync = deriv.get("synchronized") or {}
        checkpoint_data = {
            "r_raw": event_window.get("r_raw"),
            "r_btc_adj": event_window.get("r_btc_adj"),
            "pctile": event_window.get("pctile"),
            "spot_cvd": (spot.get("composite_max") or {}).get("cvd"),
            "spot_method": "TRADES" if spot.get("venues") else "NONE",
            "spot_coverage": spot.get("coverage"),
            "oi_native_chg_pct": sync.get("oi_native_chg_pct"),
            "oi_class": sync.get("oi_class"),
            "funding": deriv.get("per_venue", []),
            "liquidations": deriv.get("liquidations"),
            "window_start": iso(start),
            "window_end": iso(end),
        }
        from tank.util import sha256

        fingerprint = sha256(checkpoint_data)
        if (
            change_marker
            and latest
            and fingerprint
            == (
                latest.get("data_fingerprint") or sha256({name: latest.get(name) for name in checkpoint_data})
            )
        ):
            state = {"changed": False, "revision": latest["revision"], "data_fingerprint": fingerprint}
            await self.repo.put(
                "ops.scheduler_checkpoints",
                change_marker,
                {"checkpoint_id": change_marker, "at": iso(), "state": state},
            )
            return state
        record = {
            "data_fingerprint": fingerprint,
            "rwp_id": await self.reaction_pack_id(payload["event_id"], payload["token_id"]),
            **payload,
            "captured_at": captured_at,
            "revision": revision,
            "supersedes_revision": revision - 1 if revision > 1 else None,
            "market_truth_id": truth.get("mta_id"),
            "market_truth": truth,
            **checkpoint_data,
            "config_version": self.settings.config_version,
            "code_version": self.settings.code_version,
        }
        async with self.repo.transaction():
            await self.repo.put("out.reaction_checkpoints", key, record, immutable=True)
            if change_marker:
                await self.repo.put(
                    "ops.scheduler_checkpoints",
                    change_marker,
                    {
                        "checkpoint_id": change_marker,
                        "at": iso(),
                        "state": {"changed": True, "revision": revision, "data_fingerprint": fingerprint},
                    },
                )
        return record

    async def scout_proposal(self, hook, concept, evidence, explanation, now):
        """Record an observation, never change the canonical taxonomy or thresholds."""
        from tank.util import sha256

        evidence = sorted(set(evidence))
        if not evidence:
            return None
        async with self.repo.lock("scout:auto:" + sha256(concept.casefold())):
            matching = [
                p
                async for p in self.repo.iter_all("scout.expansion_proposals", filters={"hook": hook})
                if p["concept_name"].casefold() == concept.casefold()
            ]
            known = {ref for p in matching for ref in p.get("evidence", [])}
            if not set(evidence) - known:
                return matching[-1] if matching else None
            existing = next((p for p in matching if not p.get("human_action")), None)
            proposal_id = existing["prp_id"] if existing else new_id("prp")
            proposal = existing or {
                "prp_id": proposal_id,
                "hook": hook,
                "concept_name": concept,
                "definition": explanation,
                "why_existing_fails": explanation,
                "mechanism": "Observed detector evidence requires adversarial review before any canonical change.",
                "overlap": "Existing categories must be considered first by the scout reviewer.",
                "recommendation": "SUBCATEGORY",
                "confidence": "LOW",
                "occurrences": 0,
                "evidence": [],
                "created_at": iso(now),
                "created_by": "scheduler",
                "config_version": self.settings.config_version,
                "code_version": self.settings.code_version,
            }
            proposal.update(
                occurrences=proposal["occurrences"] + 1,
                evidence=sorted(set(proposal["evidence"]) | set(evidence)),
                last_observed_at=iso(now),
            )
            await self.repo.put("scout.expansion_proposals", proposal_id, proposal)
            await self.repo.enqueue(
                "SCOUT_REVIEW",
                {"prp_id": proposal_id},
                f"{proposal_id}:{proposal['occurrences']}",
                priority=4,
                max_attempts=2,
                expires_at=iso(now + timedelta(days=7)),
            )
            return proposal

    async def detect_scout_hooks(self, now):
        """Bound MTA reads by observation time; resume metadata changes durably."""
        from collections import defaultdict

        from tank.util import sha256

        end = now + timedelta(microseconds=1)
        boundary = now - timedelta(hours=24)
        checkpoint = await self.repo.get("ops.scheduler_checkpoints", "scout_hooks")
        since = parse_time(checkpoint["at"]) if checkpoint else None
        recent = {}
        pairs = defaultdict(set)

        def ref(row):
            return row.get("mta_id") or "truth:" + sha256(row)

        async for truth in self.repo.iter_range(
            "ana.market_truth", "cutoff", start=boundary + timedelta(microseconds=1), end=end, batch_size=100
        ):
            token = truth["token_id"]
            state = recent.setdefault(token, {"all_unknown": True, "evidence": []})
            state["all_unknown"] = (
                state["all_unknown"]
                and truth.get("move_class") == "UNKNOWN"
                and (truth.get("data_confidence") or {}).get("class") in {"MEDIUM", "HIGH"}
            )
            state["evidence"].append(ref(truth))
            if truth.get("event_id"):
                pairs[truth["event_id"]].add(token)
        for token, state in recent.items():
            baseline = await self.repo.latest_before(
                "ana.market_truth",
                "cutoff",
                boundary + timedelta(microseconds=1),
                filters={"token_id": token},
            )
            if (
                baseline
                and state["all_unknown"]
                and baseline.get("move_class") == "UNKNOWN"
                and (baseline.get("data_confidence") or {}).get("class") in {"MEDIUM", "HIGH"}
            ):
                await self.scout_proposal(
                    "MARKET_TRUTH_STATE",
                    "MARKET_TRUTH_STATE:" + token,
                    [ref(baseline), *state["evidence"]],
                    "Every available token analysis across the preceding 24 hours is UNKNOWN despite non-LOW Data Confidence.",
                    now,
                )
        changed = set(pairs)
        if since is not None:
            for collection, time_field in [
                ("evt.ems_assessments", "cutoff"),
                ("ana.contradictions", "detected_at"),
            ]:
                async for row in self.repo.iter_range(
                    collection, time_field, start=since, end=end, batch_size=100
                ):
                    if row.get("event_id"):
                        changed.add(row["event_id"])
        registered = {r["source_id"] for r in self.settings.load_config("sources")["sources"]}
        seen = set()

        async def candidates():
            async for event in self.active_events(now):
                if event["event_id"] not in seen:
                    seen.add(event["event_id"])
                    yield event
            if since is None:
                # One uncapped metadata bootstrap; never load historical MTA payloads.
                stream = self.repo.iter_all("evt.events")
            else:
                stream = self.repo.iter_range("evt.events", "known_to_us_at", start=since, end=end)
            async for event in stream:
                if event["event_id"] not in seen:
                    seen.add(event["event_id"])
                    yield event
            for event_id in changed - seen:
                event = await self.repo.get("evt.events", event_id)
                if event is not None:
                    yield event

        async for event in candidates():
            event_id = event["event_id"]
            contradictions = [
                c
                async for c in self.repo.iter_all("ana.contradictions", filters={"event_id": event_id})
                if not c.get("resolved_at")
                and c.get("type") in {"UNRESOLVED", "DIAGNOSTIC"}
                and (not c.get("detected_at") or parse_time(c["detected_at"]) <= now)
            ]
            tokens = set(pairs.get(event_id, set())) | {
                c["token_id"] for c in contradictions if c.get("token_id")
            }
            tokens.update(t for t in event.get("tokens_mentioned", []) if isinstance(t, str))
            async for edge in self.repo.iter_all("evt.beneficiary_edges", filters={"event_id": event_id}):
                if edge.get("token_id"):
                    tokens.add(edge["token_id"])
            for token in tokens:
                relevant = [c for c in contradictions if c.get("token_id") in {None, token}]
                last_two = await self.repo.list_latest(
                    "ana.market_truth",
                    "cutoff",
                    filters={"event_id": event_id, "token_id": token},
                    end=end,
                    limit=2,
                )
                persistent = len(last_two) == 2 and all(
                    (r.get("divergence") or {}).get("state") == "UNKNOWN" for r in last_two
                )
                evidence = [c["contradiction_id"] for c in relevant] if len(relevant) > 1 else []
                if persistent:
                    evidence += [ref(r) for r in last_two]
                if evidence:
                    await self.scout_proposal(
                        "ALTERNATIVE_EXPLANATION",
                        f"ALTERNATIVE_EXPLANATION:{event_id}:{token}",
                        evidence,
                        "More than one unresolved/diagnostic contradiction or repeated consecutive UNKNOWN divergence requires an alternative-explanation review.",
                        now,
                    )
            assessment = await self.latest_assessment(event)
            major = assessment and assessment.get("class") in {"MAJOR", "STRUCTURAL_SHOCK"}
            evidence = []
            public, known = event.get("public_at"), event.get("known_to_us_at")
            if major and public and known and parse_time(known) - parse_time(public) > timedelta(hours=1):
                evidence.append(event_id)
            observations = self.repo.iter_range(
                "evt.event_observations", "observed_at", filters={"event_id": event_id}, end=end
            )
            first = None
            first_time = None
            primary_refs = []
            async for observation in observations:
                item = await self.repo.get("ingest.source_items", observation["item_id"])
                if item is None:
                    continue
                if first is None:
                    first = item
                    first_time = (
                        item.get("published_at") or observation.get("observed_at") or item.get("observed_at")
                    )
                if (
                    item.get("source_class") == "PRIMARY"
                    and item.get("source_id") in registered
                    and item.get("published_at")
                ):
                    primary_refs.append((item["item_id"], item["published_at"]))
            if first and first.get("source_class") in {"SECONDARY", "SOCIAL_LEAD"} and first_time:
                for item_id, published_at in primary_refs:
                    if parse_time(published_at) < parse_time(first_time):
                        evidence += [first["item_id"], item_id]
            if evidence:
                await self.scout_proposal(
                    "SOURCE_OPPORTUNITY",
                    "SOURCE_OPPORTUNITY:" + event_id,
                    evidence,
                    "A MAJOR+ event arrived more than one hour after publication or a secondary/social observation preceded discovery of an earlier registered primary publication.",
                    now,
                )
        await self.repo.put(
            "ops.scheduler_checkpoints",
            "scout_hooks",
            {
                "checkpoint_id": "scout_hooks",
                "at": iso(now),
                "state": {"criteria": "TDD10"},
                "config_version": self.settings.config_version,
                "code_version": self.settings.code_version,
            },
        )

    async def demote(self, now):
        hot = [row async for row in self.repo.iter_all("mkt.hot_tokens")]
        if hot:
            await self.market.refresh_hot_expiry(now=now)
        for row in hot:
            protected = await self.market.hot_protected(row["event_id"], row["token_id"], now=now)
            checkpoints = await self.repo.list(
                "out.reaction_checkpoints",
                filters={"event_id": row["event_id"], "token_id": row["token_id"], "checkpoint": "POST_72H"},
                limit=1,
            )
            if checkpoints and not protected:
                await self.market.demote(row["token_id"], row["event_id"])
        hot = [row async for row in self.repo.iter_all("mkt.hot_tokens")]
        tokens = {r["token_id"] for r in hot}
        for row in sorted(hot, key=lambda r: (r.get("priority", 0), r["since"])):
            if len(tokens) <= self.threshold("hot_token_cap", 15):
                break
            if row["token_id"] in tokens:
                await self.market.demote(row["token_id"])
                tokens.remove(row["token_id"])

    async def heartbeat(self, now):
        notice_id = f"heartbeat_{now.date()}"
        if await self.repo.get("out.ops_notices", notice_id):
            return
        end = now + timedelta(microseconds=1)
        count = 0
        async for row in self.repo.iter_range(
            "out.deliveries", "at", start=now - timedelta(days=1), end=end, filters={"status": "SENT"}
        ):
            count += bool(row.get("alert_id"))
        hot_tokens = {row["token_id"] async for row in self.repo.iter_all("mkt.hot_tokens")}
        lag = 0
        backlog = {"queued": 0, "running": 0}
        analysis_kinds = {
            "FILTER_ITEM",
            "ROUTE_ITEM",
            "AGENT_MESSAGE",
            "ESCALATE_TOKENS",
            "RUN_MARKET_TRUTH",
            "EVAL_CONDITIONS",
            "MILESTONE_CHECK",
            "REACTION_CAPTURE",
            "SCOUT_REVIEW",
        }
        for status in ("QUEUED", "RUNNING"):
            async for row in self.repo.iter_all("ops.jobs", filters={"status": status}):
                backlog[status.lower()] += row["type"] in analysis_kinds
                if status == "QUEUED":
                    lag = max(lag, max(0, (now - parse_time(row["run_after"])).total_seconds()))
        spend, unknown = Decimal(0), 0
        async for row in self.repo.iter_range(
            "ops.llm_usage", "at", start=now.replace(hour=0, minute=0, second=0, microsecond=0), end=end
        ):
            if row.get("cost_usd") is None:
                unknown += 1
            else:
                spend += Decimal(str(row["cost_usd"]))
        spending_label = (
            f"known subtotal ${spend}; unknown cost records={unknown}" if unknown else f"${spend}"
        )
        async with self.repo.transaction():
            await self.repo.put(
                "out.ops_notices",
                notice_id,
                {
                    "notice_id": notice_id,
                    "kind": "HEARTBEAT",
                    "text": f"Tank 24h: alerts={count}; hot tokens={len(hot_tokens)}; queue lag={lag:.0f}s; analysis backlog queued={backlog['queued']}, running={backlog['running']}; LLM spending={spending_label}",
                    "created_at": iso(now),
                    "created_by": "scheduler",
                    "analysis_backlog": backlog,
                    "queue_lag_s": lag,
                },
                immutable=True,
            )
            await self.repo.enqueue(
                "DELIVER",
                {"notice_id": notice_id, "channel": "telegram:ops"},
                notice_id,
                priority=0,
                expires_at=iso(now + timedelta(days=1)),
                max_attempts=5,
            )

    async def maintenance(self, now, *, force=False, prune=True):
        import os
        import shutil
        from pathlib import Path
        from urllib.parse import unquote, urlsplit

        backup_dir = getattr(self.settings, "backup_dir", None)
        if backup_dir is None:
            raise ValueError("TANK_BACKUP_DIR must point to the operator external backup disk")
        if not force and now.hour < getattr(self.settings, "backup_hour_utc", 2):
            return {"status": "deferred", "reason": "before configured backup hour"}
        target = Path(backup_dir)
        if not target.is_dir():
            raise ValueError("Configured external backup directory is unavailable")
        final = target / f"tank-{now.date()}"
        if (final / "COMPLETE").exists():
            await asyncio.to_thread(verify_backup, final, protected=protected_evidence_paths(self.settings))
            if prune:
                await self.retention(now, final)
            return {"status": "verified", "path": str(final), "retention": prune}
        staging = target / f".tank-{now.date()}.partial"
        if staging.exists():
            await asyncio.to_thread(shutil.rmtree, staging)
        staging.mkdir(exist_ok=True)
        import json

        async def dump_database(url, name):
            connection = urlsplit(url)
            env = os.environ.copy()
            for key, value in {
                "PGHOST": connection.hostname,
                "PGPORT": connection.port,
                "PGUSER": unquote(connection.username or ""),
                "PGPASSWORD": unquote(connection.password or ""),
                "PGDATABASE": unquote(connection.path.lstrip("/")),
            }.items():
                if value:
                    env[key] = str(value)
                else:
                    env.pop(key, None)
            proc = await asyncio.create_subprocess_exec(
                "pg_dump",
                "--format=custom",
                "--file",
                str(staging / name),
                env=env,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                async with asyncio.timeout(600):
                    await proc.communicate()
            except (asyncio.CancelledError, TimeoutError):
                proc.terminate()
                await proc.wait()
                raise
            if proc.returncode:
                raise RuntimeError(f"Nightly pg_dump failed with status {proc.returncode}")

        await dump_database(self.settings.database_url, "tank.dump")
        await dump_database(getattr(self.settings, "openma_database_url", "postgresql:///oma"), "oma.dump")
        data_root = Path(self.settings.data_dir).resolve()
        if target.resolve().is_relative_to(data_root):
            raise ValueError("Backup destination must be outside live data directory")
        roots = {"data": data_root, "config": Path(self.settings.config_dir).resolve()}
        logs_root = Path(getattr(self.settings, "logs_dir", None) or "logs").resolve()
        if logs_root.is_dir():
            roots["logs"] = logs_root
        protected = protected_evidence_paths(self.settings)
        auth_parent = protected.get("subscription sign-in directory")
        if auth_parent and any(root.is_relative_to(auth_parent) for root in roots.values()):
            raise ValueError(
                "Subscription sign-in requires a dedicated directory outside whole evidence roots"
            )
        import threading

        cancelled = threading.Event()

        def copy_evidence():
            for label, root in roots.items():
                if not root.is_dir():
                    raise ValueError(f"Required backup input is unavailable: {label}")
                destination = staging / label
                for source in sorted(root.rglob("*")):
                    if cancelled.is_set():
                        raise RuntimeError("Backup interrupted before completion")
                    if is_protected_evidence_path(source, protected):
                        continue
                    if source.is_symlink():
                        raise ValueError("Backup evidence roots must not contain symlinks")
                    if source.is_file():
                        output = destination / source.relative_to(root)
                        output.parent.mkdir(parents=True, exist_ok=True)
                        with source.open("rb") as source_file, output.open("wb") as destination_file:
                            while chunk := source_file.read(1024 * 1024):
                                if cancelled.is_set():
                                    raise RuntimeError("Backup interrupted before completion")
                                destination_file.write(chunk)
                        shutil.copystat(source, output)
            files = {
                path.relative_to(staging).as_posix(): file_digest(path, cancelled=cancelled)
                for path in sorted(staging.rglob("*"))
                if path.is_file() and path.name not in {"manifest.json", "COMPLETE"}
            }
            manifest = {
                "version": 1,
                "created_at": iso(now),
                "reference_base": str(Path.cwd()),
                "roots": {label: str(root) for label, root in roots.items()},
                "files": files,
                "database_dumps": ["tank.dump", "oma.dump"],
                "excluded": sorted(protected),
                "restore": "Restore data/config/logs to the recorded roots or resolve historical file references with resolve_backup_reference. Restore tank/oma dumps into isolated databases; preserve OpenMA root secrets separately.",
            }
            (staging / "manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2))
            verify_backup(staging, require_complete=False, protected=protected, cancelled=cancelled)
            return manifest

        copy_task = asyncio.create_task(asyncio.to_thread(copy_evidence))
        try:
            manifest = await asyncio.shield(copy_task)
        except asyncio.CancelledError:
            cancelled.set()
            await asyncio.gather(copy_task, return_exceptions=True)
            raise
        # Source references must all resolve in the copied data snapshot before it is declared complete.
        async for item in self.repo.iter_all("ingest.source_items"):
            for name, reference in item.items():
                if (
                    reference
                    and isinstance(reference, str)
                    and (name.endswith("_raw_ref") or name in {"raw_ref", "raw_path"})
                ):
                    resolve_backup_reference(staging, reference, manifest=manifest)
        (staging / "COMPLETE").write_text(iso(now) + "\n")
        if final.exists():
            raise RuntimeError("Incomplete backup destination already exists")
        staging.rename(final)
        if prune:
            await self.retention(now, final)
        return {"status": "created", "path": str(final), "retention": prune}

    async def retention(self, now, backup):
        from pathlib import Path

        marker = Path(backup) / "RETENTION_COMPLETE"
        if marker.exists():
            return []
        await self.repo.prune_market(iso(now - timedelta(days=self.threshold("base_retention_days", 90))))
        await self.repo.prune_jobs(iso(now - timedelta(days=self.threshold("jobs_retention_days", 30))))
        removed = await self.prune_raw_files(now, backup)
        marker.write_text(iso(now) + "\n")
        return removed

    async def prune_raw_files(self, now, backup):
        """Prune only verified-backed-up market files with known times and identities."""
        import json
        from pathlib import Path

        manifest = await asyncio.to_thread(
            verify_backup, backup, protected=protected_evidence_paths(self.settings)
        )
        windows = await self.repo.preserved_market_windows()
        instruments = {
            r["instrument_id"]: r["token_id"] for r in self.settings.load_config("registry")["instruments"]
        }
        data_root = Path(self.settings.data_dir).resolve()
        raw_root, archive_root = data_root / "market/raw", data_root / "market/binance"
        protected = protected_evidence_paths(self.settings)
        removed = []
        async for row in self.repo.iter_range(
            "mkt.raw_archives",
            "received_ts",
            end=now
            - timedelta(
                days=min(
                    self.threshold("hot_raw_retention_days", 30),
                    self.threshold("archive_retention_days", 180),
                )
            ),
        ):
            original_path = Path(row["path"])
            path = original_path.resolve()
            if (
                is_protected_evidence_path(path, protected)
                or original_path.is_symlink()
                or not path.is_file()
            ):
                continue
            archive = path.is_relative_to(archive_root)
            if not archive and not path.is_relative_to(raw_root):
                continue
            received = row.get("received_ts")
            if not received or parse_time(received) >= now - timedelta(
                days=self.threshold("archive_retention_days", 180)
                if archive
                else self.threshold("hot_raw_retention_days", 30)
            ):
                continue
            token = row.get("token_id") or instruments.get(row.get("instrument_id"))
            if not token:
                continue
            start, end = await asyncio.to_thread(raw_time_range, path, row, now)
            if (
                start is None
                or end is None
                or end
                >= now
                - timedelta(
                    days=self.threshold("archive_retention_days", 180)
                    if archive
                    else self.threshold("hot_raw_retention_days", 30)
                )
            ):
                continue
            # MAJOR+ windows permanently retain every instrument on overlapping UTC days.
            if any(start.date() <= high.date() and end.date() >= low.date() for _, low, high in windows):
                continue
            saved = resolve_backup_reference(backup, str(path), manifest=manifest)
            digest = await asyncio.to_thread(file_digest, path)
            if await asyncio.to_thread(file_digest, saved) != digest:
                continue
            # Audit tombstone is written before removal; immutable DB metadata remains recoverable.
            tombstone = {
                "archive_id": row["archive_id"],
                "path": str(path),
                "sha256": digest,
                "removed_at": iso(now),
                "backup": str(Path(backup).resolve()),
                "backup_relative": saved.relative_to(Path(backup).resolve()).as_posix(),
            }
            audit = data_root / "retention" / "removed-market-files.jsonl"
            audit.parent.mkdir(parents=True, exist_ok=True)
            with audit.open("a") as handle:
                handle.write(json.dumps(tombstone, sort_keys=True) + "\n")
            path.unlink()
            checksum = path.with_suffix(path.suffix + ".CHECKSUM")
            if archive and checksum.is_file():
                try:
                    resolve_backup_reference(backup, str(checksum), manifest=manifest)
                except ValueError:
                    pass
                else:
                    checksum.unlink()
            removed.append(tombstone)
        return removed

    async def lifecycle(self, now):
        async for event in self.active_events(
            now, states=("TRACKED", "COOLDOWN", "MONITORING", "ALERTED"), include_recent_terminal=False
        ):
            state = event.get("lifecycle")
            event_id = event["event_id"]
            observations = await self.repo.list_latest(
                "evt.event_observations",
                "observed_at",
                filters={"event_id": event_id},
                end=now + timedelta(microseconds=1),
                limit=1,
            )
            last = max(
                [event.get("known_to_us_at", iso(now))]
                + [o["observed_at"] for o in observations if o.get("observed_at")]
            )
            if state == "ALERTED":
                await self.repo.put(
                    "evt.events", event_id, {**event, "lifecycle": "MONITORING", "monitoring_at": iso(now)}
                )
            elif (
                state == "TRACKED"
                and not event.get("next_milestone")
                and now - parse_time(last) >= timedelta(days=self.threshold("event_close_days", 7))
            ):
                await self.repo.put(
                    "evt.events", event_id, {**event, "lifecycle": "CLOSED", "closed_at": iso(now)}
                )
            elif state == "COOLDOWN" and now - parse_time(event.get("cooldown_at", last)) >= timedelta(
                days=self.threshold("event_close_days", 7)
            ):
                await self.repo.put(
                    "evt.events", event_id, {**event, "lifecycle": "CLOSED", "closed_at": iso(now)}
                )
            elif state == "MONITORING":
                calls = await self.repo.list(
                    "ana.calls", filters={"event_id": event_id, "status": "OPEN"}, limit=1
                )
                armed = await self.repo.list(
                    "ana.watch_conditions", filters={"event_id": event_id, "status": "ARMED"}, limit=1
                )
                checkpoints = [
                    row
                    async for row in self.repo.iter_all(
                        "out.reaction_checkpoints", filters={"event_id": event_id, "checkpoint": "POST_72H"}
                    )
                ]
                targets = await self.analysis_tokens(event)
                captured = {r["token_id"] for r in checkpoints}
                if targets <= captured and not calls and not armed:
                    await self.repo.put(
                        "evt.events", event_id, {**event, "lifecycle": "COOLDOWN", "cooldown_at": iso(now)}
                    )


def metric_value(truth, event, metric, window):
    spot = truth.get("spot", {}).get(window, {})
    derivative_pack = truth.get("derivatives", {})
    deriv = (
        derivative_pack.get("windows", {}).get(window, {})
        if "windows" in derivative_pack
        else derivative_pack
    )
    sync = deriv.get("synchronized") or {}
    values = {
        "event.state": event.get("event_state"),
        "event.contradiction_fatal": event.get("contradiction_fatal"),
        "milestone.outcome": (event.get("next_milestone") or {}).get("outcome"),
        "spot.cohort.direction": spot.get("direction"),
        "spot.cohort.cwb_buy": spot.get("cw_breadth", {}).get("buy"),
        "spot.cohort.cwb_sell": spot.get("cw_breadth", {}).get("sell"),
        "spot.classification": spot.get("classification"),
        "deriv.native_oi_chg_pct": sync.get("oi_native_chg_pct"),
        "deriv.native_oi_vs_normal": sync.get("native_oi_vs_normal"),
        "deriv.oi_class": sync.get("oi_class"),
        "deriv.funding_vs_normal": funding_state(deriv),
        "mt.move_class": truth.get("move_class"),
        "mt.divergence_state": truth.get("divergence", {}).get("state"),
        "dc.class": truth.get("data_confidence", {}).get("class"),
        "price.repricing_band": truth.get("relative", {}).get("repricing_band"),
        "price.close_vs_level": truth.get("relative", {}).get("close_1h_price"),
        "price.vs_anchor_pct": truth.get("relative", {}).get("windows", {}).get("EVENT", {}).get("r_raw"),
        "liq.depth_p1_vs_pre": depth_ratio(truth.get("liquidity", {})),
        "liq.spread_ratio": truth.get("liquidity", {}).get("spread_ratio"),
    }
    return values.get(metric, field(truth, metric))


def depth_ratio(liquidity):
    current, pre = liquidity.get("aggregate_depth_p1_usd"), liquidity.get("pre_event_depth_p1_usd")
    return float(current) / float(pre) if current is not None and pre else None


def funding_state(derivatives):
    values = [r.get("funding_vs_normal") for r in derivatives.get("per_venue", [])]
    if not values or any(v is None for v in values):
        return None
    return "ABOVE" if "ABOVE" in values else "BELOW" if all(v == "BELOW" for v in values) else "WITHIN"


def file_digest(path, *, cancelled=None):
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            if cancelled is not None and cancelled.is_set():
                raise RuntimeError("Backup interrupted before completion")
            digest.update(chunk)
    return digest.hexdigest()


def verify_backup(directory, *, require_complete=True, protected=None, cancelled=None):
    """Verify the relative-path recovery manifest before restoring evidence."""
    import json
    from pathlib import Path

    root = Path(directory).resolve()
    if require_complete and not (root / "COMPLETE").is_file():
        raise ValueError("Backup is incomplete")
    manifest = json.loads((root / "manifest.json").read_text())
    if protected:
        for relative in manifest["files"]:
            parts = Path(relative).parts
            if parts and parts[0] in manifest.get("roots", {}):
                original = Path(manifest["roots"][parts[0]]).joinpath(*parts[1:])
                if is_protected_evidence_path(original, protected):
                    raise ValueError(
                        "Backup contains a protected credential path and cannot be used as ordinary evidence"
                    )
    for relative, digest in manifest["files"].items():
        path = (root / relative).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise ValueError("Backup file is missing or outside backup root")
        if file_digest(path, cancelled=cancelled) != digest:
            raise ValueError("Backup file checksum mismatch")
    if not all(name in manifest["files"] for name in ("tank.dump", "oma.dump")):
        raise ValueError("Backup requires both Tank and OpenMA database dumps")
    return manifest


def resolve_backup_reference(directory, reference, *, manifest=None):
    """Map an original absolute/relative evidence path into a relocated backup."""
    import json
    from pathlib import Path

    root = Path(directory).resolve()
    manifest = manifest or json.loads((root / "manifest.json").read_text())
    original = Path(reference)
    if not original.is_absolute():
        original = Path(manifest["reference_base"]) / original
    original = original.resolve()
    for label, source_root in manifest["roots"].items():
        source = Path(source_root)
        if original.is_relative_to(source):
            relative = (Path(label) / original.relative_to(source)).as_posix()
            path = (root / relative).resolve()
            if relative not in manifest["files"] or not path.is_file():
                raise ValueError("Evidence reference is missing from backup")
            return path
    raise ValueError("Evidence reference is outside backed-up roots")


def raw_time_range(path, row, now):
    """Unknown historical ranges are retained, never guessed from download age."""
    import json
    import re
    from datetime import datetime, timezone

    if path.suffix == ".zip":
        match = re.search(r"(20\d\d-\d\d(?:-\d\d)?)\.zip$", path.name)
        if not match:
            return None, None
        value = match.group(1)
        start = parse_time(value + "-01" if len(value) == 7 else value)
        if len(value) == 7:
            end = (
                datetime(start.year + 1, 1, 1, tzinfo=timezone.utc)
                if start.month == 12
                else datetime(start.year, start.month + 1, 1, tzinfo=timezone.utc)
            )
        else:
            end = start + timedelta(days=1)
        return start, end
    if path.suffix != ".parquet":
        return None, None
    import pyarrow.parquet as pq

    stamps = []

    def add(value):
        try:
            stamp = parse_time(value)
            if parse_time("2009-01-01") <= stamp <= now + timedelta(days=1):
                stamps.append(stamp)
        except (ValueError, TypeError, OverflowError, OSError):
            pass

    def walk(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key in {
                    "ts",
                    "timestamp",
                    "time",
                    "T",
                    "E",
                    "fundingTime",
                    "openTime",
                    "closeTime",
                    "startTime",
                    "endTime",
                    "t",
                }:
                    add(child)
                if isinstance(child, (list, dict)):
                    walk(child)
        elif isinstance(value, list):
            if value and not isinstance(value[0], (dict, list)):
                add(value[0])
            for child in value:
                if isinstance(child, (dict, list)):
                    walk(child)

    try:
        for raw in pq.read_table(path).to_pylist():
            walk(json.loads(raw["payload_json"]))
    except Exception:
        return None, None
    if not stamps:
        if row.get("kind") not in {"websocket", "book_snapshot", "independent_rest_book", "ticker"}:
            return None, None
        stamps = [parse_time(row["received_ts"])]
    return min(stamps), max(stamps)


def protected_evidence_paths(settings):
    """Credentials and browser state are never ordinary evidence/retention inputs."""
    from pathlib import Path

    paths = {}
    for field, label in [
        ("x_profile_dir", "operator browser profile"),
        ("x_storage_state_path", "operator X storage state"),
        ("secrets_dir", "Docker secrets directory"),
    ]:
        value = getattr(settings, field, None)
        if value is not None:
            paths[label] = Path(value).resolve()
    auth = getattr(settings, "codex_auth_path", None)
    if auth is not None:
        paths["subscription sign-in directory"] = Path(auth).resolve().parent
    return paths


def is_protected_evidence_path(path, protected):
    from pathlib import Path

    resolved = Path(path).resolve()
    return any(resolved.is_relative_to(root) for root in protected.values())
