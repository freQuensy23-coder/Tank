"""Durable timer creation, watch evaluation and reaction checkpoints."""

import asyncio
import logging
from datetime import timedelta
from decimal import Decimal, localcontext

from tank.time_precision import chosen_boundary, collection_clock, offset_schedule, propagate_timestamp
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
    if isinstance(actual, dict) and actual.get("kind") == "EXACT_STRICT_MAJORITY":
        # Truth preserves the exact > 1/2 result before ratio-to-float display conversion.
        if op == ">" and Decimal(str(target)) == Decimal("0.5"):
            return actual["strict_majority"] is True
        actual = actual.get("value")
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
        self._verified_backups = {}
        self._raw_retention_lock = asyncio.Lock()

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

        task = asyncio.create_task(guarded(), name="scheduler-" + name)
        self.background[name] = task

        def completed(finished):
            # Dynamic event/token work must not retain completed task frames.
            # An older callback must never remove its same-name replacement.
            if self.background.get(name) is finished:
                self.background.pop(name)

        task.add_done_callback(completed)

    async def close(self):
        self.closing = True
        tasks = tuple(self.background.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
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
        bucket_stamp = chosen_boundary(now.replace(second=0, microsecond=0), precision="BUCKET(1m)")
        bucket = bucket_stamp.value
        await self.repo.enqueue(
            "EVAL_CONDITIONS",
            {**bucket_stamp.fields("cutoff")},
            bucket,
            priority=2,
            expires_at=offset_schedule(collection_clock(now), timedelta(minutes=5)),
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
                due_stamp = propagate_timestamp(milestone, "due_at")
                due = due_stamp.value
                kind = milestone.get("kind", milestone.get("milestone"))
                expiry_stamp = (
                    propagate_timestamp(milestone, "expires_at")
                    if milestone.get("expires_at")
                    else offset_schedule(due_stamp, timedelta(hours=2))
                )
                expires = expiry_stamp.value
                if parse_time(expires) > now:
                    await self.repo.enqueue(
                        "MILESTONE_CHECK",
                        {
                            "event_id": event["event_id"],
                            "milestone": kind,
                            **due_stamp.fields("due_at"),
                            "source_id": milestone.get("source_id"),
                        },
                        f"{event['event_id']}:{kind}:{due}",
                        priority=1,
                        expires_at=expiry_stamp,
                    )
            if milestone and milestone.get("due_at"):
                await self.expire_milestone(event["event_id"], now)
            assessment = await self.latest_assessment(event)
            if not assessment:
                continue
            if assessment.get("class") not in ("MAJOR", "STRUCTURAL_SHOCK"):
                continue
            await self.ensure_reactions(event, now=now)
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
        self.launch("raw-retention", lambda: self.raw_retention(now))
        self.launch("partitions", lambda: self.partition_maintenance(now))

    async def ensure_reactions(self, event, *, now=None):
        """Open the pack outside the analyst transaction; only POST windows are jobs.

        PRE windows start as soon as the MAJOR+ assessment commits. Their immutable
        rows are the restart checkpoint: a tick resumes any missing capture, while
        a named task prevents overlapping attempts during normal operation.
        """
        now = now or utcnow()
        assessment = await self.latest_assessment(event)
        if not assessment or assessment.get("class") not in {"MAJOR", "STRUCTURAL_SHOCK"}:
            return
        anchor = self.anchor(event)
        anchor_stamp = propagate_timestamp(anchor, "ts", precision_aliases=("precision",))
        tokens = await self.analysis_tokens(event)
        captured = {
            (row["token_id"], row["checkpoint"])
            async for row in self.repo.iter_metadata(
                "out.reaction_checkpoints",
                ["token_id", "checkpoint"],
                filters={"event_id": event["event_id"]},
            )
        }
        gaps = {
            (row["state"]["token_id"], row["state"]["checkpoint"])
            async for row in self.repo.iter_all(
                "ops.scheduler_checkpoints",
                filters={"event_id": event["event_id"], "kind": "REACTION_CAPTURE_GAP"},
            )
        }
        jobs = {
            row["idem_key"]: row
            async for row in self.repo.iter_metadata(
                "ops.jobs",
                ["job_id", "idem_key", "status", "expires_at", "expires_at_prec", "expires_at_prec_reason"],
                filters={"type": "REACTION_CAPTURE"},
            )
            if row["idem_key"].startswith(event["event_id"] + ":")
        }
        for token in sorted(tokens):
            missing_pre = []
            for label, offset in CHECKPOINTS.items():
                if (token, label) in captured or (token, label) in gaps:
                    continue
                payload = {
                    "event_id": event["event_id"],
                    "token_id": token,
                    "checkpoint": label,
                    "anchor": anchor,
                    **offset_schedule(anchor_stamp, timedelta(seconds=offset)).fields("cutoff"),
                    "retro": offset < 0 or parse_time(anchor_stamp.value) + timedelta(seconds=offset) < now,
                }
                if offset < 0:
                    missing_pre.append(payload)
                    continue
                due_stamp = offset_schedule(anchor_stamp, timedelta(seconds=offset))
                due = parse_time(due_stamp.value)
                key = f"{event['event_id']}:{token}:{label}"
                job = jobs.get(key)
                run_stamp = due_stamp if due >= now else collection_clock(now)
                expiry_stamp = (
                    propagate_timestamp(job, "expires_at")
                    if job and job.get("expires_at")
                    else offset_schedule(run_stamp, timedelta(hours=6))
                )
                terminal = job and job["status"] in {"FAILED", "EXPIRED"}
                expired = bool(job) and job["status"] != "RUNNING" and parse_time(expiry_stamp.value) <= now
                if terminal or expired:
                    await self.record_capture_gap(
                        event["event_id"], token, label, anchor, expiry_stamp, now, job
                    )
                elif not job:
                    await self.repo.enqueue(
                        "REACTION_CAPTURE",
                        payload,
                        key,
                        priority=2,
                        run_after=run_stamp,
                        expires_at=expiry_stamp,
                        max_attempts=5,
                    )
            if missing_pre and self.market is not None:

                async def capture_pre(payloads=missing_pre):
                    for payload in payloads:
                        await self.reaction_capture(payload)

                self.launch(f"reaction-pre:{event['event_id']}:{token}", capture_pre)

    async def expire_milestone(self, event_id, now):
        """A missed verification window is a timer fact, never a source outcome."""
        async with self.repo.lock("event:" + event_id), self.repo.transaction():
            event = await self.repo.get("evt.events", event_id)
            milestone = (event or {}).get("next_milestone")
            if (
                not milestone
                or not milestone.get("due_at")
                or milestone.get("status") in {"CONFIRMED", "VERIFIED", "FAILED", "MISSED"}
            ):
                return
            due = propagate_timestamp(milestone, "due_at")
            expiry = (
                propagate_timestamp(milestone, "expires_at")
                if milestone.get("expires_at")
                else offset_schedule(due, timedelta(hours=2))
            )
            if parse_time(expiry.value) > now:
                return
            missed = {**milestone, "status": "MISSED", **collection_clock(now).fields("missed_at")}
            await self.repo.put("evt.events", event_id, {**event, "next_milestone": missed})
            await self.message(
                "MILESTONE",
                event_id,
                "missed:" + str(milestone.get("kind", milestone.get("milestone"))) + ":" + due.value,
                milestone=missed,
                verification_required=True,
                instruction="Milestone verification window elapsed without accepted resolution. Re-score; verify source outcome. Timer expiry is not evidence that the source milestone failed.",
            )

    async def record_capture_gap(self, event_id, token_id, label, anchor, expiry, now, job):
        key = f"reaction-gap:{event_id}:{token_id}:{label}"
        if await self.repo.get("ops.scheduler_checkpoints", key):
            return
        await self.repo.put(
            "ops.scheduler_checkpoints",
            key,
            {
                "checkpoint_id": key,
                "event_id": event_id,
                "kind": "REACTION_CAPTURE_GAP",
                **collection_clock(now).fields("at"),
                "state": {
                    "event_id": event_id,
                    "token_id": token_id,
                    "checkpoint": label,
                    "anchor": anchor,
                    **expiry.fields("expires_at"),
                    "status": job["status"] if job and job["status"] in {"FAILED", "EXPIRED"} else "EXPIRED",
                    "job_status": job.get("status") if job else None,
                    "job_id": job.get("job_id") if job else None,
                    "reason": "Checkpoint window elapsed"
                    if not job
                    else "Capture job terminal: " + job["status"],
                    "captured": False,
                },
            },
        )
        log.error("Reaction capture gap: %s; +72h lifecycle capture requirement remains unsatisfied", key)

    async def raw_retention(self, now):
        """Run raw-cache TTL once per UTC day, independently of external backup."""
        now = parse_time(now)
        async with self._raw_retention_lock:
            marker = await self.repo.get("ops.scheduler_checkpoints", "raw-retention")
            if marker and parse_time(marker["at"]).date() == now.date():
                return []
            removed = await self.prune_raw_files(now, None)
            await self.repo.put(
                "ops.scheduler_checkpoints",
                "raw-retention",
                {
                    "checkpoint_id": "raw-retention",
                    **collection_clock(now).fields("at"),
                    "state": {"removed_count": len(removed)},
                },
            )
            return removed

    async def partition_maintenance(self, now):
        marker = await self.repo.get("ops.scheduler_checkpoints", "partitions")
        if marker and now - parse_time(marker["at"]) < timedelta(hours=1):
            return
        await self.repo.maintain_partitions()
        await self.repo.put(
            "ops.scheduler_checkpoints",
            "partitions",
            {"checkpoint_id": "partitions", **collection_clock(now).fields("at"), "state": {}},
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
                        {
                            **call,
                            "status": "CLOSED",
                            "closed_by": "EXPIRED",
                            **collection_clock(now).fields("closed_at"),
                        },
                    )
                    await self.message("CALL_EXPIRED", call["event_id"], call["call_id"], call=call)

    @staticmethod
    def anchor(event):
        from tank.market.capture import event_anchor

        anchor = event_anchor(event)
        selected = {"T0": "event_time", "T1": "public_at", "KNOWN_TO_US_AT": "known_to_us_at"}[anchor["type"]]
        return {**anchor, **propagate_timestamp(event, selected).fields("ts")}

    async def message(self, kind, event_id, key, **extra):
        await self.repo.enqueue(
            "AGENT_MESSAGE",
            {"kind": kind, "event_id": event_id, **extra},
            f"{event_id}:{kind}:{key}",
            priority=1,
            expires_at=offset_schedule(collection_clock(), timedelta(days=1)),
        )

    @staticmethod
    def condition_requirements(expression):
        if "all" in expression:
            return [item for child in expression["all"] for item in Scheduler.condition_requirements(child)]
        metric = expression["metric"]
        if metric.startswith(("event.", "milestone.")):
            return []
        return [{"metric": metric, "window": expression.get("window", "W1H")}]

    async def condition_truth(self, event_id, token_id, cutoff, requirements):
        evaluator = getattr(self.truth, "condition_values", None)
        if not callable(evaluator):
            from tank.analysis.truth import MarketTruthService

            if isinstance(self.truth, MarketTruthService):
                raise RuntimeError("Selective condition evaluator is required")
            # Explicit injected test doubles retain their small fixture API.
            return await self.truth.assess(event_id, token_id, cutoff)
        truth = await evaluator(event_id, token_id, cutoff, requirements=requirements)
        # TDD §3.8 reserves RUN_MARKET_TRUTH for get_market_truth. Minute
        # checks remain transient; unavailable metrics cannot create snapshots
        # or trigger a condition from stale/guessed values.
        return truth

    async def eval_conditions(self, payload):
        cutoff_stamp = propagate_timestamp(payload, "cutoff") if payload.get("cutoff") else collection_clock()
        cutoff = cutoff_stamp.value
        cache = {}
        conditions = [
            cond async for cond in self.repo.iter_all("ana.watch_conditions", filters={"status": "ARMED"})
        ]
        requirement_sets = {}
        for condition in conditions:
            key = (condition["event_id"], condition["token_id"])
            requirement_sets.setdefault(key, set()).update(
                (item["metric"], item["window"]) for item in self.condition_requirements(condition["expr"])
            )
        for cond in conditions:
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
                        r.get("type") == "FATAL"
                        and not r.get("resolved_at")
                        and r.get("token_id") in {None, cond["token_id"]}
                        for r in contradictions
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
                market_cutoff_stamp = cutoff_stamp
                if periods:
                    from datetime import datetime, timezone

                    seconds = max(periods)
                    endpoint = int(parse_time(cutoff).timestamp()) // seconds * seconds
                    market_cutoff_stamp = chosen_boundary(
                        datetime.fromtimestamp(endpoint, timezone.utc), precision="EXACT"
                    )
                counters = dict(cond.get("counters", {}))
                unobserved, leaf_matches = [], []

                async def evaluate(expr, path="root"):
                    if "all" in expr:
                        results = [
                            await evaluate(child, path + str(i)) for i, child in enumerate(expr["all"])
                        ]
                        return all(results)
                    metric = expr["metric"]
                    market_metric = not metric.startswith(("event.", "milestone."))
                    required = expr.get("persist", {}).get("count", 1)
                    evaluation_stamp = market_cutoff_stamp if market_metric else cutoff_stamp
                    evaluation_cutoff = evaluation_stamp.value
                    seconds = window_seconds(expr)
                    window_id = int(parse_time(evaluation_cutoff).timestamp()) // seconds
                    previous = counters.get(path, {})
                    if market_metric and required > 1:
                        created = cond.get("created_at")
                        if created and parse_time(evaluation_cutoff) <= parse_time(created):
                            unobserved.append(path)
                            return False
                    truth = {}
                    if market_metric:
                        cache_key = (cond["event_id"], cond["token_id"], evaluation_stamp)
                        if cache_key not in cache:
                            requirements = requirement_sets[(cond["event_id"], cond["token_id"])] | {
                                (item["metric"], item["window"])
                                for item in self.condition_requirements(cond["expr"])
                            }
                            cache[cache_key] = await self.condition_truth(
                                *cache_key,
                                requirements=[
                                    {"metric": metric, "window": window}
                                    for metric, window in sorted(requirements)
                                ],
                            )
                        truth = cache[cache_key]
                    value = metric_value(truth, event, metric, expr.get("window", "W1H"))
                    matched = compare(value, expr["op"], expr["value"])
                    leaf_matches.append(matched)
                    proof = next(
                        (
                            row
                            for row in truth.get("_condition_evaluation", {}).get("proofs", [])
                            if row.get("metric") == metric
                            and row.get("window", "W1H") == expr.get("window", "W1H")
                        ),
                        {},
                    )
                    availability = {
                        "data_available": value is not None,
                        "unavailable_reason": (proof.get("reason") or "INCOMPLETE")
                        if value is None
                        else None,
                    }
                    if required == 1 or not market_metric:
                        counters[path] = {
                            "window": window_id,
                            "count": int(matched),
                            "value": value,
                            **availability,
                            **evaluation_stamp.fields("cutoff"),
                        }
                        return matched
                    # Retain only explicitly evaluated non-overlapping windows.
                    # Re-evaluate retained endpoints through the selective reader:
                    # changed dependency heads can complete/correct a closed hour.
                    # Replacing a window never increments that same hour twice.
                    history = {row["window"]: dict(row) for row in previous.get("history", [])}
                    if not history and previous.get("window") is not None:
                        history[previous["window"]] = dict(previous)
                    history[window_id] = {
                        "window": window_id,
                        "matched": matched,
                        "value": value,
                        **availability,
                        **evaluation_stamp.fields("cutoff"),
                        "dependency_key": truth.get("_condition_evaluation", {}).get("request_key"),
                    }
                    history = {
                        key: row for key, row in history.items() if window_id - required < key <= window_id
                    }
                    for old_window, old in list(history.items()):
                        if old_window == window_id:
                            continue
                        old_stamp = propagate_timestamp(old, "cutoff")
                        if not old_stamp.value:
                            continue
                        old_key = (cond["event_id"], cond["token_id"], old_stamp)
                        if old_key not in cache:
                            requirements = requirement_sets[(cond["event_id"], cond["token_id"])]
                            cache[old_key] = await self.condition_truth(
                                *old_key,
                                requirements=[
                                    {"metric": name, "window": window}
                                    for name, window in sorted(requirements)
                                ],
                            )
                        old_truth = cache[old_key]
                        old_value = metric_value(old_truth, event, metric, expr.get("window", "W1H"))
                        old_proof = next(
                            (
                                row
                                for row in old_truth.get("_condition_evaluation", {}).get("proofs", [])
                                if row.get("metric") == metric
                                and row.get("window", "W1H") == expr.get("window", "W1H")
                            ),
                            {},
                        )
                        history[old_window] = {
                            "window": old_window,
                            "value": old_value,
                            "matched": compare(old_value, expr["op"], expr["value"]),
                            "data_available": old_value is not None,
                            "unavailable_reason": (old_proof.get("reason") or "INCOMPLETE")
                            if old_value is None
                            else None,
                            **old_stamp.fields("cutoff"),
                            "dependency_key": old_truth.get("_condition_evaluation", {}).get("request_key"),
                        }
                    count = 0
                    for endpoint in range(window_id, window_id - required, -1):
                        if endpoint not in history or not history[endpoint].get("matched"):
                            break
                        count += 1
                    counters[path] = {
                        **history[window_id],
                        "count": count,
                        "history": [history[key] for key in sorted(history)],
                    }
                    return count >= required

                fired = await evaluate(expr)
                updated = {**cond, "counters": counters, **cutoff_stamp.fields("last_evaluated_at")}
                if cond.get("rearmed_from") and not cond.get("cleared_at"):
                    # A re-armed watch fires on an edge, not a level: it must be
                    # seen false on available data once before it can fire again,
                    # so a lasting state never re-fires every window (W1).
                    # Cleared means the predicate itself is false in the current
                    # window, not merely that persistence is not yet reached.
                    available = all(row.get("data_available", True) for row in counters.values())
                    if available and not unobserved and leaf_matches and not all(leaf_matches):
                        updated.update(cutoff_stamp.fields("cleared_at"))
                    fired = False
                if fired:
                    updated.update(status="FIRED", **cutoff_stamp.fields("fired_at"))
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
        while True:
            async with self.repo.lock(lock):
                plan = await self._reaction_plan(payload)
                if plan["complete"]:
                    return plan["result"]
            # Capturing market buffers and computing history must not keep a
            # database transaction (and its market row locks) open for minutes.
            measured = await self._measure_reaction(plan["payload"])
            async with self.repo.lock(lock):
                current = await self._reaction_plan(payload)
                if current["complete"]:
                    return current["result"]
                if current["key"] != plan["key"] or current["payload"] != plan["payload"]:
                    continue  # Another revision committed; measure its frozen anchor again.
                return await self._commit_reaction(current, measured)

    async def detect_late_data(self, now):
        marker = await self.repo.get("ops.scheduler_checkpoints", "reaction-capture-changes")
        since = marker["at"] if marker else None
        # One queued late revision per checkpoint is enough: it measures with
        # all data present when it runs (03 §8.6), so later changes coalesce.
        queued = {
            (job["payload"].get("event_id"), job["payload"].get("token_id"), job["payload"].get("checkpoint"))
            async for job in self.repo.iter_all(
                "ops.jobs", filters={"type": "REACTION_CAPTURE", "status": "QUEUED"}
            )
            if job["payload"].get("automatic_revision")
        }
        async for change in self.market.capture_changes(
            since=since, end=iso(now + timedelta(microseconds=1))
        ):
            changed_at = change.get("finished_at") or change["observed_at"]
            change_id = change.get("change_id") or change["backfill_id"] + ":" + changed_at
            source_id = change.get("backfill_id") or change_id
            latest = {}
            async for row in self.repo.iter_metadata(
                "out.reaction_checkpoints",
                ["checkpoint", "revision", "captured_at", "anchor"],
                filters={"event_id": change["event_id"], "token_id": change["token_id"]},
            ):
                label = row["checkpoint"]
                if label not in latest or (row.get("revision") or 1) > (latest[label].get("revision") or 1):
                    latest[label] = row
            for label, row in latest.items():
                if parse_time(changed_at) < parse_time(row["captured_at"]):
                    continue
                if parse_time(change["anchor"]) != parse_time(row["anchor"]["ts"]):
                    continue
                target = (change["event_id"], change["token_id"], label)
                if target in queued:
                    continue
                queued.add(target)
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
                    expires_at=offset_schedule(collection_clock(now), timedelta(hours=6)),
                )
        await self.repo.put(
            "ops.scheduler_checkpoints",
            "reaction-capture-changes",
            {"checkpoint_id": "reaction-capture-changes", **collection_clock(now).fields("at"), "state": {}},
        )

    async def reaction_pack_id(self, event_id, token_id):
        key = f"reaction-pack:{event_id}:{token_id}"
        async with self.repo.lock(key):
            row = await self.repo.get("ops.scheduler_checkpoints", key)
            if row is None:
                row = {
                    "checkpoint_id": key,
                    **collection_clock().fields("at"),
                    "state": {"rwp_id": new_id("RWP"), "event_id": event_id, "token_id": token_id},
                }
                await self.repo.put("ops.scheduler_checkpoints", key, row)
            return row["state"]["rwp_id"]

    async def _reaction_plan(self, payload):
        payload = dict(payload)
        change_marker = (
            "reaction-change:" + payload["change_id"] if payload.get("automatic_revision") else None
        )
        if change_marker:
            applied = await self.repo.get("ops.scheduler_checkpoints", change_marker)
            if applied:
                return {"complete": True, "result": applied["state"]}
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
            return {"complete": True, "result": existing}
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
        return {
            "complete": False,
            "payload": payload,
            "change_marker": change_marker,
            "revision": revision,
            "key": key,
            "latest": latest,
        }

    async def _measure_reaction(self, payload):
        payload = dict(payload)
        captured_stamp = collection_clock()
        from tank.analysis.truth import compute_market_truth

        anchor_stamp = propagate_timestamp(payload["anchor"], "ts", precision_aliases=("precision",))
        payload["anchor"] = {**payload["anchor"], **anchor_stamp.fields("ts")}
        origin = parse_time(anchor_stamp.value)
        offset = CHECKPOINTS[payload["checkpoint"]]
        start = origin + timedelta(seconds=offset) if offset < 0 else origin
        end = origin if offset < 0 else origin + timedelta(seconds=offset)
        start_stamp = offset_schedule(anchor_stamp, start - origin)
        end_stamp = offset_schedule(anchor_stamp, end - origin)
        snapshot = await self.market.snapshot(
            payload["token_id"],
            end_stamp,
            anchor={**start_stamp.fields("ts"), "type": payload["anchor"]["type"]},
        )
        snapshot.update(
            event_id=payload["event_id"], token_id=payload["token_id"], **end_stamp.fields("cutoff")
        )
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
            # 01 FR-R-01: open interest in token units at each checkpoint.
            "oi_native_start": sync.get("oi_native_start"),
            "oi_native_end": sync.get("oi_native_end"),
            "oi_venues": sync.get("venues"),
            "oi_class": sync.get("oi_class"),
            "funding": deriv.get("per_venue", []),
            "liquidations": deriv.get("liquidations"),
            **start_stamp.fields("window_start"),
            **end_stamp.fields("window_end"),
        }
        return {
            "payload": payload,
            "captured_stamp": captured_stamp,
            "checkpoint_data": checkpoint_data,
            "truth": truth,
        }

    async def _commit_reaction(self, plan, measured):
        payload, change_marker = measured["payload"], plan["change_marker"]
        revision, key, latest = plan["revision"], plan["key"], plan["latest"]
        captured_stamp = measured["captured_stamp"]
        checkpoint_data, truth = measured["checkpoint_data"], measured["truth"]
        from tank.util import sha256

        fingerprint_data = {
            name: value
            for name, value in checkpoint_data.items()
            if not name.endswith(("_prec", "_prec_reason"))
        }
        fingerprint = sha256(fingerprint_data)
        if (
            change_marker
            and latest
            and fingerprint
            == (
                latest.get("data_fingerprint")
                or sha256({name: latest.get(name) for name in fingerprint_data})
            )
        ):
            state = {"changed": False, "revision": latest["revision"], "data_fingerprint": fingerprint}
            await self.repo.put(
                "ops.scheduler_checkpoints",
                change_marker,
                {"checkpoint_id": change_marker, **collection_clock().fields("at"), "state": state},
            )
            return state
        record = {
            "data_fingerprint": fingerprint,
            "rwp_id": await self.reaction_pack_id(payload["event_id"], payload["token_id"]),
            **payload,
            **captured_stamp.fields("captured_at"),
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
                        **collection_clock().fields("at"),
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
                **collection_clock(now).fields("created_at"),
                "created_by": "scheduler",
                "config_version": self.settings.config_version,
                "code_version": self.settings.code_version,
            }
            proposal.update(
                occurrences=proposal["occurrences"] + 1,
                evidence=sorted(set(proposal["evidence"]) | set(evidence)),
                **collection_clock(now).fields("last_observed_at"),
            )
            await self.repo.put("scout.expansion_proposals", proposal_id, proposal)
            await self.repo.enqueue(
                "SCOUT_REVIEW",
                {"prp_id": proposal_id},
                f"{proposal_id}:{proposal['occurrences']}",
                priority=4,
                max_attempts=2,
                expires_at=offset_schedule(collection_clock(now), timedelta(days=7)),
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
                **collection_clock(now).fields("at"),
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
                    **collection_clock(now).fields("created_at"),
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
                expires_at=offset_schedule(collection_clock(now), timedelta(days=1)),
                max_attempts=5,
            )

    @staticmethod
    def backup_identity(directory, day):
        from pathlib import Path

        directory = Path(directory).resolve()

        def identity(name):
            try:
                stat = (directory / name).stat()
            except FileNotFoundError:
                return None
            return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)

        return (str(directory), day, identity("manifest.json"), identity("COMPLETE"))

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
        cache_key = self.backup_identity(final, now.date())
        self._verified_backups = {
            key: manifest for key, manifest in self._verified_backups.items() if key[1] == now.date()
        }
        if (final / "COMPLETE").exists():
            if force or cache_key not in self._verified_backups:
                self._verified_backups[cache_key] = await asyncio.to_thread(
                    verify_backup, final, protected=protected_evidence_paths(self.settings)
                )
            if prune:
                await self.retention(now, final)
            await self.prune_snapshots(target)
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
        # HLD §§5,8: nightly pg_dump plus evidence bundles, not the live
        # market cache, raw source history, configuration tree or service logs.
        roots = {"bundles": data_root / "bundles"}
        protected = protected_evidence_paths(self.settings)
        import threading

        cancelled = threading.Event()

        def copy_evidence():
            import hashlib

            files = {
                name: file_digest(staging / name, cancelled=cancelled) for name in ("tank.dump", "oma.dump")
            }
            for label, root in roots.items():
                if not root.exists():
                    continue  # No evidence bundles have been published yet.
                if root.is_symlink() or not root.is_dir():
                    raise ValueError(f"Required backup input is not a directory: {label}")
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
                        digest = hashlib.sha256()
                        with source.open("rb") as source_file, output.open("wb") as destination_file:
                            while chunk := source_file.read(1024 * 1024):
                                if cancelled.is_set():
                                    raise RuntimeError("Backup interrupted before completion")
                                destination_file.write(chunk)
                                digest.update(chunk)
                        shutil.copystat(source, output)
                        files[output.relative_to(staging).as_posix()] = digest.hexdigest()
            manifest = {
                "version": 1,
                **collection_clock(now).fields("created_at"),
                "reference_base": str(Path.cwd()),
                "roots": {label: str(root) for label, root in roots.items()},
                "files": files,
                "database_dumps": ["tank.dump", "oma.dump"],
                "scope": "pg_dump and bundles/ (HLD §§5,8)",
                "excluded": sorted(protected),
                "restore": "Restore bundles to the recorded root or resolve historical file references with resolve_backup_reference. Restore tank/oma dumps into isolated databases; preserve OpenMA root secrets separately.",
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
        (staging / "COMPLETE").write_text(iso(now) + "\n")
        (staging / "COMPLETE.json").write_text(json.dumps(collection_clock(now).fields("completed_at")))
        if final.exists():
            raise RuntimeError("Incomplete backup destination already exists")
        staging.rename(final)
        self._verified_backups[self.backup_identity(final, now.date())] = manifest
        if prune:
            await self.retention(now, final)
        await self.prune_snapshots(target)
        return {"status": "created", "path": str(final), "retention": prune}

    async def prune_snapshots(self, target):
        """Keep the newest verified snapshots only (lead ruling 2026-10-20, thresholds)."""
        from tank.backup import prune_backup_snapshots

        removed = await asyncio.to_thread(
            prune_backup_snapshots, target, int(self.threshold("backup_snapshots_kept", 3))
        )
        if removed:
            log.info("Removed old backup snapshots: %s", ", ".join(removed))
        return removed

    async def retention(self, now, backup):
        from pathlib import Path

        marker = Path(backup) / "RETENTION_COMPLETE"
        if marker.exists():
            return []
        import json

        market_cutoff = offset_schedule(
            collection_clock(now), -timedelta(days=self.threshold("base_retention_days", 90))
        )
        jobs_cutoff = offset_schedule(
            collection_clock(now), -timedelta(days=self.threshold("jobs_retention_days", 30))
        )
        await self.repo.prune_market(market_cutoff.value)
        await self.repo.prune_jobs(jobs_cutoff.value)
        removed = await self.raw_retention(now)
        marker.with_suffix(".json").write_text(
            json.dumps(
                {
                    **collection_clock(now).fields("completed_at"),
                    **market_cutoff.fields("market_cutoff"),
                    **jobs_cutoff.fields("jobs_cutoff"),
                }
            )
        )
        marker.write_text(iso(now) + "\n")
        return removed

    async def prune_raw_files(self, now, backup):
        """Expire market caches by TTL; pg_dump/bundles do not contain raw caches."""
        import json
        import os
        from pathlib import Path

        data_root = Path(self.settings.data_dir).resolve()
        protected = protected_evidence_paths(self.settings)
        raw_roots = [data_root / p for p in ("raw/market_frames", "raw/trades", "raw/books", "market/raw")]
        archive_roots = [data_root / p for p in ("archive/binance", "market/binance")]
        hot_cutoff = now - timedelta(days=self.threshold("hot_raw_retention_days", 30))
        archive_cutoff = now - timedelta(days=self.threshold("archive_retention_days", 180))
        windows = await self.repo.preserved_market_windows()
        instruments = {
            r["instrument_id"]: r["token_id"] for r in self.settings.load_config("registry")["instruments"]
        }

        def safe(value):
            path = Path(value).absolute()
            if not path.is_relative_to(data_root) or is_protected_evidence_path(path, protected):
                return False
            return not any(p.is_symlink() for p in (path, *path.parents) if p.is_relative_to(data_root))

        def family(row):
            integrity = row.get("integrity") or {}
            canonical = integrity.get("canonical_parquet_path") or row.get("journal_path") or row["path"]
            # A book manifest's parquet_path is the public hardlink, while path is immutable.
            if row.get("kind") == "BOOKS":
                canonical = row.get("parquet_path") or canonical
            return str(Path(canonical).absolute())

        # New receipts for an old hour keep its whole dependency family current.
        recent = set()
        async for row in self.repo.iter_range(
            "mkt.raw_archives", "received_ts", start=min(hot_cutoff, archive_cutoff)
        ):
            cutoff = (
                archive_cutoff
                if any(Path(row["path"]).absolute().is_relative_to(root) for root in archive_roots)
                else hot_cutoff
            )
            if parse_time(row["received_ts"]) >= cutoff:
                recent.add(family(row))
        groups = {}
        async for row in self.repo.iter_range(
            "mkt.raw_archives", "received_ts", end=max(hot_cutoff, archive_cutoff)
        ):
            groups.setdefault(family(row), []).append(row)

        # Bundle payload metadata is already in PostgreSQL; never scan/hash backup files.
        pinned = set()

        def references(value):
            if isinstance(value, dict):
                for child in value.values():
                    references(child)
            elif isinstance(value, list):
                for child in value:
                    references(child)
            elif isinstance(value, str) and value.startswith(str(data_root) + os.sep):
                pinned.add(str(Path(value).absolute()))

        async for bundle in self.repo.iter_all("out.evidence_bundles"):
            references(bundle.get("payload", {}))

        removed = []
        for key, rows in groups.items():
            if key in recent:
                continue
            paths = set()
            valid = True
            for row in rows:
                original = Path(row["path"]).absolute()
                if not safe(original):
                    valid = False
                    break
                archive = any(original.is_relative_to(root) for root in archive_roots)
                if not archive and not any(original.is_relative_to(root) for root in raw_roots):
                    valid = False
                    break
                cutoff = archive_cutoff if archive else hot_cutoff
                if not row.get("received_ts") or parse_time(row["received_ts"]) >= cutoff:
                    valid = False
                    break
                if not (row.get("token_id") or instruments.get(row.get("instrument_id"))):
                    valid = False
                    break
                if row.get("kind") in {"NORMALIZED_TRADES", "BOOKS"} and row.get("hour_start"):
                    start = parse_time(row["hour_start"])
                    end = start + timedelta(hours=1)
                else:
                    start, end = await asyncio.to_thread(raw_time_range, original, row, now)
                if start is None or end is None or end > cutoff:
                    valid = False
                    break
                # Keep every venue/instrument on an overlapping MAJOR+ UTC day.
                if any(start.date() <= high.date() and end.date() >= low.date() for _, low, high in windows):
                    valid = False
                    break
                paths.add(original)
                for value in (
                    row.get("journal_path"),
                    row.get("parquet_path"),
                    (row.get("integrity") or {}).get("index_path"),
                ):
                    if value:
                        paths.add(Path(value).absolute())
                if archive:
                    paths.add(original.with_suffix(original.suffix + ".CHECKSUM"))
            if not valid:
                continue
            canonical = Path(key)
            if any(canonical.is_relative_to(root) for root in raw_roots):
                stem = canonical.stem
                owned_parquet = {Path(r["path"]).absolute() for r in rows}
                versions = set()
                for directory in (".versions", ".batches"):
                    versions.update((canonical.parent / directory).glob(stem + ".*.parquet"))
                batch_digests = {
                    digest
                    for row in rows
                    for digest in (
                        (row.get("integrity") or {}).get("contained_batch_sha256", [])
                        + (row.get("integrity") or {}).get("required_batch_sha256", [])
                    )
                }
                owned_parquet.update(
                    path
                    for path in versions
                    if path.name in {stem + "." + digest + ".parquet" for digest in batch_digests}
                )
                if versions - owned_parquet:
                    continue  # Uncommitted/unknown output is not an expired manifest.
                if (
                    canonical.suffix == ".parquet"
                    and canonical.is_file()
                    and not any(
                        path.is_file() and path.stat().st_ino == canonical.stat().st_ino
                        for path in owned_parquet
                    )
                ):
                    continue  # A newer publication has not reached this metadata scan.
                paths.add(canonical)
                journal = canonical.with_suffix(".jsonl")
                if safe(journal) and journal.is_file():
                    try:
                        # Inspect only the last record, never hash/read the whole hour.
                        with journal.open("rb") as handle:
                            handle.seek(0, os.SEEK_END)
                            cursor, tail = handle.tell(), b""
                            while cursor:
                                count = min(cursor, 65536)
                                cursor -= count
                                handle.seek(cursor)
                                tail = handle.read(count) + tail
                                if b"\n" in tail.rstrip(b"\n"):
                                    break
                        last = json.loads(tail.rstrip(b"\n").rsplit(b"\n", 1)[-1])
                        if parse_time(last["received_ts"]) >= hot_cutoff:
                            continue
                    except (OSError, ValueError, KeyError, TypeError):
                        continue
                paths.add(journal)
                indexes = set(canonical.parent.glob(stem + ".index*.json"))
                if any(not safe(path) for path in indexes):
                    continue
                try:
                    if any(
                        parse_time(json.loads(path.read_text())["received_ts"]) >= hot_cutoff
                        for path in indexes
                    ):
                        continue
                except (OSError, ValueError, KeyError, TypeError):
                    continue  # Incomplete index provenance is not deletion authority.
                paths.update(indexes)
                paths.update(versions)
            # Manifest metadata may name only this raw hour/cache family, never
            # arbitrary permanent/source files elsewhere in the data volume.
            parents = {canonical.parent, canonical.parent / ".versions", canonical.parent / ".batches"}
            if any(
                not safe(path)
                or path.parent not in parents
                or not (
                    path.name == canonical.name
                    or path.name.startswith((canonical.stem + ".", canonical.stem + "-"))
                )
                or not any(path.is_relative_to(root) for root in raw_roots + archive_roots)
                or any(path.is_relative_to(Path(reference)) for reference in pinned)
                for path in paths
            ):
                continue
            files = sorted((p for p in paths if p.is_file()), key=str)
            if not files:
                continue
            identities = {p: p.stat() for p in files}
            row = next((r for r in reversed(rows) if r.get("status") == "SEALED"), rows[-1])
            tombstone = {
                "archive_id": row["archive_id"],
                "archive_ids": [r["archive_id"] for r in rows],
                "path": row["path"],
                "sha256": row.get("sha256"),
                "checksum_basis": "RECORDED_MANIFEST_NOT_REHASHED",
                "files": [str(p) for p in files],
                "reason": "MARKET_RAW_TTL",
                "hot_raw_retention_days": self.threshold("hot_raw_retention_days", 30),
                "archive_retention_days": self.threshold("archive_retention_days", 180),
                **collection_clock(now).fields("removed_at"),
            }
            audit = data_root / "retention" / "removed-market-files.jsonl"
            if not safe(audit):
                continue
            audit.parent.mkdir(parents=True, exist_ok=True)

            def record(status):
                with audit.open("a") as handle:
                    handle.write(json.dumps(dict(tombstone, status=status), sort_keys=True) + "\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                descriptor = os.open(audit.parent, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)

            for path in files:
                before, current = identities[path], path.lstat()
                if not safe(path) or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                    current.st_dev,
                    current.st_ino,
                    current.st_size,
                    current.st_mtime_ns,
                ):
                    raise ValueError("Raw retention target changed during removal")
            record("REMOVAL_INTENT")
            for path in files:
                path.unlink()
            for parent in {path.parent for path in files}:
                descriptor = os.open(parent, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            record("REMOVAL_COMPLETE")
            removed.append(tombstone)
        return removed

    async def lifecycle(self, now):
        async for candidate in self.active_events(
            now, states=("TRACKED", "COOLDOWN", "MONITORING", "ALERTED"), include_recent_terminal=False
        ):
            event_id = candidate["event_id"]
            # The candidate scan is only discovery. Re-read state and all
            # transition predicates under the same lock used by event tools.
            async with self.repo.lock("event:" + event_id), self.repo.transaction():
                event = await self.repo.get("evt.events", event_id)
                if event and event.get("lifecycle") in {"TRACKED", "COOLDOWN", "MONITORING", "ALERTED"}:
                    await self._advance_lifecycle(event, now)

    async def _advance_lifecycle(self, event, now):
        state = event["lifecycle"]
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
                "evt.events",
                event_id,
                {**event, "lifecycle": "MONITORING", **collection_clock(now).fields("monitoring_at")},
            )
        elif (
            state == "TRACKED"
            and (
                not event.get("next_milestone")
                or event["next_milestone"].get("status") in {"CONFIRMED", "VERIFIED", "FAILED", "MISSED"}
            )
            and now - parse_time(last) >= timedelta(days=self.threshold("event_close_days", 7))
        ):
            await self.repo.put(
                "evt.events",
                event_id,
                {**event, "lifecycle": "CLOSED", **collection_clock(now).fields("closed_at")},
            )
        elif state == "COOLDOWN" and now - parse_time(event.get("cooldown_at", last)) >= timedelta(
            days=self.threshold("event_close_days", 7)
        ):
            await self.repo.put(
                "evt.events",
                event_id,
                {**event, "lifecycle": "CLOSED", **collection_clock(now).fields("closed_at")},
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
                    "evt.events",
                    event_id,
                    {**event, "lifecycle": "COOLDOWN", **collection_clock(now).fields("cooldown_at")},
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

    def cohort_share(side):
        value = spot.get("cw_breadth", {}).get(side)
        exact = spot.get("exact_majority", {})
        if side in exact:
            return {"kind": "EXACT_STRICT_MAJORITY", "value": value, "strict_majority": exact[side]}
        return value

    values = {
        "event.state": event.get("event_state"),
        "event.contradiction_fatal": event.get("contradiction_fatal"),
        "milestone.outcome": (event.get("next_milestone") or {}).get("outcome"),
        "spot.cohort.direction": spot.get("direction"),
        "spot.cohort.cwb_buy": cohort_share("buy"),
        "spot.cohort.cwb_sell": cohort_share("sell"),
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
    if current is None or pre is None:
        return None
    current, pre = Decimal(str(current)), Decimal(str(pre))
    if not pre:
        return None
    with localcontext() as context:
        context.prec = max(
            128,
            len(current.as_tuple().digits)
            + len(pre.as_tuple().digits)
            + abs(current.adjusted() - pre.adjusted())
            + 32,
        )
        return current / pre


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
    if path.suffix not in {".parquet", ".jsonl"}:
        return None, None

    low = high = None

    def add(value):
        nonlocal low, high
        try:
            stamp = parse_time(value)
            if parse_time("2009-01-01") <= stamp <= now + timedelta(days=1):
                low = stamp if low is None else min(low, stamp)
                high = stamp if high is None else max(high, stamp)
        except (ValueError, TypeError, OverflowError, OSError):
            pass

    def walk(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key in {
                    "ts",
                    "timestamp",
                    "exchange_ts",
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
        if path.suffix == ".jsonl":
            with path.open() as handle:
                for line in handle:
                    raw = json.loads(line)
                    walk(json.loads(raw["payload_json"]) if "payload_json" in raw else raw)
        else:
            import pyarrow.parquet as pq

            for batch in pq.ParquetFile(path).iter_batches(batch_size=10000):
                for raw in batch.to_pylist():
                    walk(json.loads(raw["payload_json"]) if "payload_json" in raw else raw)
    except Exception:
        return None, None
    if low is None:
        if row.get("kind") not in {"websocket", "book_snapshot", "independent_rest_book", "ticker"}:
            return None, None
        low = high = parse_time(row["received_ts"])
    return low, high


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
