"""Validated domain tools exposed to OpenMA. External material is evidence, never instructions."""

import asyncio
import copy
import os
from contextlib import AsyncExitStack, suppress
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlparse

import httpx
import zstandard

from tank.analysis.identity import (
    entity_identity,
    resolve_tokens,
    token_identities,
)
from tank.analysis.identity import (
    resolve_entity as resolve_entity,
)
from tank.analysis.identity import (
    resolve_token as resolve_token,
)
from tank.analysis.reaction_scheduling import AssessmentFreeDiagnosticScheduling, ReactionScheduling
from tank.analysis.truth_contracts import MarketTruthAssessmentRef
from tank.analysis.watch_contracts import WatchCondition
from tank.contracts.provenance import ResolvedVersionIdentity
from tank.contracts.source_repository import SourceExtractionRepository
from tank.contracts.tool_schema import SCOPED_TOOLS as SCOPED_TOOLS
from tank.contracts.tool_schema import validate_schema as validate_schema
from tank.ops_progress import event_progress, item_progress, tool_progress
from tank.sources.capture_fetch import capture_fetch, is_exchange_url
from tank.time_precision import (
    Timestamp,
    chosen_boundary,
    collection_clock,
    interval_bounds,
    offset_schedule,
    propagate_timestamp,
)
from tank.util import canonical_json, clock_iso, iso, new_id, parse_time, sha256, utcnow

STATES = ["RUMOR", "PROPOSED", "APPROVED", "SIGNED", "DEPLOYED", "LIVE", "MONEY_FLOWING"]
INTEGRATION = [
    "COMPATIBLE",
    "IN_DEVELOPMENT",
    "BUILT",
    "SUBMITTED",
    "APPROVED",
    "LIVE",
    "MATERIAL_USAGE",
    "ECONOMIC_FLOW",
]
LAYERS = [
    "ISSUER_PRODUCT",
    "VENUE_DEX",
    "SETTLEMENT_L1_L2",
    "LIQUIDITY_EXECUTION",
    "LENDING_COLLATERAL",
    "ORACLE_DATA",
    "INTEROPERABILITY",
    "CUSTODY_COMPLIANCE",
]
IMPLEMENTATION = dict(zip(STATES, [0, 4, 6, 6, 7, 8, 10], strict=False))
M1_VENUES = frozenset({"binance", "coinbase", "okx", "bybit", "kraken", "robinhood"})


HOST_BOUND_FIELDS = {
    **{tool: ("event_id",) for tool in SCOPED_TOOLS},
    "add_observation": ("event_id", "item_id"),
    "record_event": ("item_id",),
    "get_item": ("item_id",),
    "get_event": ("event_id",),
    "record_review": ("proposal_id",),
}


def domain_schema(settings, tool):
    """A tool's one input shape: its public schema plus the records the host binds itself."""
    skill = "tank-scout-reviewer" if tool == "record_review" else "tank-event-analyst"
    relative = f"skills/{skill}/tools.json"
    if not settings.has_config(relative):
        return None
    schema = next(
        (
            copy.deepcopy(row["input_schema"])
            for row in settings.read_config_json(relative)
            if row["name"] == tool
        ),
        None,
    )
    if schema is None:
        return None
    for field in HOST_BOUND_FIELDS.get(tool, ()):
        schema.setdefault("properties", {})[field] = {"type": "string"}
        if field not in schema.setdefault("required", []):
            schema["required"].append(field)
    return schema


TOOL_RESULT_MAX_BYTES = 40_000
READ_TOOLS = frozenset(
    {
        "get_item",
        "search_tank_items",
        "list_open_events",
        "get_event",
        "get_registry",
        "get_token_facts",
        "web_search",
        "fetch_page",
    }
)
AGENT_WATCH_METRICS = frozenset(
    {
        "event.state",
        "event.contradiction_fatal",
        "spot.cohort.direction",
        "spot.cohort.cwb_buy",
        "spot.cohort.cwb_sell",
        "spot.classification",
        "deriv.native_oi_chg_pct",
        "deriv.native_oi_vs_normal",
        "deriv.oi_class",
        "deriv.funding_vs_normal",
        "mt.move_class",
        "mt.divergence_state",
        "dc.class",
        "price.repricing_band",
        "price.close_vs_level",
        "price.vs_anchor_pct",
        "liq.depth_p1_vs_pre",
        "liq.spread_ratio",
    }
)
MAX_AGENT_WATCHES = 10
CATEGORICAL_WATCH_METRICS = frozenset(
    {
        "event.state",
        "event.contradiction_fatal",
        "milestone.outcome",
        "spot.cohort.direction",
        "spot.classification",
        "deriv.native_oi_vs_normal",
        "deriv.oi_class",
        "deriv.funding_vs_normal",
        "mt.move_class",
        "mt.divergence_state",
        "dc.class",
        "price.repricing_band",
    }
)
SERIES_KEEP = 7


def compact_series(value):
    """Long arrays (daily charts since 2011, balances, pools) become their
    count and newest items; scalars and short structures are kept as stored."""
    if isinstance(value, dict):
        return {key: compact_series(item) for key, item in value.items()}
    if isinstance(value, list):
        if len(value) > 2 * SERIES_KEEP:
            return {"count": len(value), "last": [compact_series(item) for item in value[-SERIES_KEEP:]]}
        return [compact_series(item) for item in value]
    return value


def fit_result(result, max_bytes=TOOL_RESULT_MAX_BYTES):
    """Bound a read tool's result: first long lists, then long strings, and
    say so. The stored evidence itself is never shortened."""
    size = len(canonical_json(result).encode())
    if size <= max_bytes:
        return result
    fitted = compact_series(result)
    limit = 4000
    while len(canonical_json(fitted).encode()) > max_bytes and limit >= 250:

        def clip(value, limit=limit):
            if isinstance(value, dict):
                return {key: clip(item) for key, item in value.items()}
            if isinstance(value, list):
                return [clip(item) for item in value]
            if isinstance(value, str) and len(value) > limit:
                return value[:limit] + f" [... {len(value) - limit} more characters stored]"
            return value

        fitted = clip(fitted)
        limit //= 2
    note = {
        "original_bytes": size,
        "note": "Result shortened to fit the context: long series show their count and newest items, "
        "long texts their start. The full records stay stored (get_item / get_event for one record).",
    }
    if len(canonical_json(fitted).encode()) > max_bytes:
        return {"_truncated": note, "keys": sorted(result) if isinstance(result, dict) else None}
    if isinstance(fitted, dict):
        return {**fitted, "_truncated": note}
    return {"items": fitted, "_truncated": note}


UNLOCK_ROWS_SHOWN = 14
TICKER_MAX_AGE_S = 3600
UNLOCK_WINDOW_DAYS = 14


def summarize_unlocks(rows, cutoff):
    """Per label, the unlocks within 14 days before and after the cutoff
    (03 §5.6: unlocks within 14 days are shown; potential supply, never a
    sale): totals in token units, counts, and the nearest tranche each side."""
    window = timedelta(days=UNLOCK_WINDOW_DAYS)
    labels = {}
    for row in rows:
        label = labels.setdefault(
            str(row.get("label") or "unlabelled"),
            {
                "label": str(row.get("label") or "unlabelled"),
                "kind": row.get("kind"),
                "count": 0,
                "past_14d": {"count": 0, "qty": Decimal(0)},
                "upcoming_14d": {"count": 0, "qty": Decimal(0)},
                "nearest_past": None,
                "nearest_upcoming": None,
                "qty_missing": 0,
            },
        )
        label["count"] += 1
        if not row.get("date"):
            continue
        date = parse_time(row["date"])
        qty = row.get("qty")
        side = "past" if date < cutoff else "upcoming"
        if abs(date - cutoff) <= window:
            bucket = label[side + "_14d"]
            bucket["count"] += 1
            if qty is None:
                label["qty_missing"] += 1
            else:
                bucket["qty"] += Decimal(str(qty))
        nearest = label["nearest_" + side]
        if nearest is None or abs(date - cutoff) < abs(parse_time(nearest["date"]) - cutoff):
            label["nearest_" + side] = {"date": iso(date), "qty": None if qty is None else str(qty)}
    for label in labels.values():
        for side in ("past_14d", "upcoming_14d"):
            label[side]["qty"] = str(label[side]["qty"])
    return {
        "analysis_cutoff": iso(cutoff),
        "window_days": UNLOCK_WINDOW_DAYS,
        "count": len(rows),
        "rows_shown": min(len(rows), UNLOCK_ROWS_SHOWN),
        "by_label": sorted(labels.values(), key=lambda label: label["label"]),
        "note": "Unlocks are potential supply, never a sale.",
    }


def dedupe_request_log(calls, truth_hashes):
    """A get_market_truth result repeats Market Truth rows that the bundle
    already holds in full under market_truth; an exact copy (same sha256) is
    kept once and referenced here. Anything not identical stays as stored."""
    known = set(truth_hashes)
    out = []
    for call in calls:
        result = call.get("result")
        if (
            call.get("tool") == "get_market_truth"
            and isinstance(result, dict)
            and isinstance(result.get("market_truth"), list)
        ):
            entries = []
            for entry in result["market_truth"]:
                digest = sha256(entry)
                entries.append(
                    {"same_as_bundle_market_truth": entry.get("mta_id"), "sha256": digest}
                    if digest in known and isinstance(entry, dict)
                    else entry
                )
            call = {**call, "result": {**result, "market_truth": entries}}
        out.append(call)
    return out


def write_evidence_bundle(bundle, data_dir):
    """Freeze the bundle file durably; runs in a thread (pure CPU and blocking
    file I/O: canonical JSON, zstd, fsync), never on the event loop."""
    for _k, v in bundle.items():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            v.sort(key=canonical_json)
    digest = sha256(bundle)
    bundle_path = Path(data_dir).resolve() / "bundles" / (digest + ".json.zst")
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    encoded = canonical_json(bundle).encode("utf-8")
    if not bundle_path.exists():
        temporary = bundle_path.with_name(bundle_path.name + "." + new_id("bundle") + ".tmp")
        try:
            with temporary.open("xb") as output:
                output.write(zstandard.ZstdCompressor(level=3).compress(encoded))
                output.flush()
                os.fsync(output.fileno())
            with suppress(FileExistsError):
                os.link(temporary, bundle_path)
        finally:
            temporary.unlink(missing_ok=True)
    if sha256(zstandard.ZstdDecompressor().decompress(bundle_path.read_bytes())) != digest:
        raise ValueError("Immutable evidence bundle file hash mismatch")

    with bundle_path.open("rb") as published:
        os.fsync(published.fileno())
    directory = bundle_path.parent
    volume_root = Path(data_dir).resolve()
    while True:
        descriptor = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        if directory == volume_root.parent:
            break
        directory = directory.parent
    return digest, bundle_path


def known_at(row):
    """When a stored fact became known. A late-receipt historical reconstruction
    (lead rulings F-02, F-04) is known from the end of its schedule_as_of bucket,
    the rule replay uses to admit it; other rows from their observation time."""
    if row.get("evidence_mode") == "HISTORICAL_RECONSTRUCTION" and row.get("schedule_as_of"):
        _, end = interval_bounds(propagate_timestamp(row, "schedule_as_of"))
        return end if end is not None else parse_time(row.get("observed_at") or row["schedule_as_of"])
    stamp = row.get("observed_at") or row.get("captured_at")
    return parse_time(stamp) if stamp else None


def normalized(s):
    return " ".join(str(s).split())


def freshness(age):
    for hours, grade in [(1, "FRESH"), (4, "NORMAL"), (8, "MEDIUM"), (24, "POOR"), (48, "OLD")]:
        if age <= hours * 3600:
            return grade
    return "STALE"


class EventToolService:
    TOOLS = {
        "get_item",
        "search_tank_items",
        "list_open_events",
        "get_event",
        "get_registry",
        "get_token_facts",
        "web_search",
        "fetch_page",
        "record_event",
        "add_observation",
        "merge_into",
        "set_assessment",
        "set_beneficiaries",
        "get_market_truth",
        "get_decision",
        "publish_card",
        "schedule_milestone",
        "propose_pattern",
        "record_review",
        "set_watch_condition",
    }

    def __init__(
        self,
        repo,
        settings,
        market_truth,
        decision_engine,
        *,
        versions: ResolvedVersionIdentity,
        scheduler: ReactionScheduling,
        http=None,
        search=None,
        reference_clock=None,
    ):
        self.repo, self.settings, self.truth, self.engine = repo, settings, market_truth, decision_engine
        self.versions = versions
        self.source_extraction = SourceExtractionRepository(repo)
        self.http = http
        self.search_service = search
        if not isinstance(scheduler, ReactionScheduling):
            raise TypeError("Event tools require a reaction scheduling port")
        self.scheduler = (
            AssessmentFreeDiagnosticScheduling.model_validate(scheduler)
            if isinstance(scheduler, AssessmentFreeDiagnosticScheduling)
            else scheduler
        )
        if reference_clock is not None:
            from tank.fixture_reference import TrustedFixtureClock

            if type(reference_clock) is not TrustedFixtureClock or not settings.replay:
                raise ValueError("Trusted fixture clock requires isolated fixture replay mode")
        self.reference_clock = reference_clock

    async def _complete_source(self, item_id: str) -> None:
        completed = await self.source_extraction.complete(item_id)
        if completed is None:
            raise ValueError("Source item disappeared before accepted extraction completion")

    def item_known_at(self, item):
        return (
            self.reference_clock.known_at(item)
            if self.reference_clock is not None
            else propagate_timestamp(item, "observed_at")
        )

    def item_reference_fields(self, item):
        if self.reference_clock is None:
            return {}
        return {
            **self.item_known_at(item).fields("simulated_available_at"),
            "fixture_reference_provenance": self.reference_clock.provenance(item),
        }

    def provenance(self, ctx):
        return {
            k: v
            for k, v in dict(
                config_version=self.versions.config_version,
                code_version=self.versions.code_version,
                session_config_version=ctx.get("session_config_version", ctx.get("config_version")),
                session_code_version=ctx.get("session_code_version", ctx.get("code_version")),
                model_id=ctx.get("model_id"),
                model_id_basis=ctx.get("model_id_basis"),
                model_id_reason=ctx.get("model_id_reason"),
                prompt_version=ctx.get("prompt_version"),
                prompt_version_basis=ctx.get("prompt_version_basis"),
                prompt_version_reason=ctx.get("prompt_version_reason"),
                prompt_label=ctx.get("prompt_label"),
                prompt_binding=copy.deepcopy(ctx.get("prompt_binding")),
                agent_version=ctx.get("agent_version"),
                agent_version_basis=ctx.get("agent_version_basis"),
                agent_version_reason=ctx.get("agent_version_reason"),
                session_id=ctx.get("session_id"),
            ).items()
        }

    async def dispatch(self, tool, arguments, context):
        if tool == "set_assessment" and isinstance(self.scheduler, AssessmentFreeDiagnosticScheduling):
            self.scheduler.refuse_assessment()
        if tool not in self.TOOLS:
            raise ValueError(f"Unknown Tank tool: {tool}")
        if not isinstance(arguments, dict):
            raise ValueError("Tool arguments must be an object")
        context = dict(context or {})
        if context.get("session_id"):
            stored_session = await self.repo.get("ops.agent_sessions", context["session_id"])
            if stored_session:
                context["event_id"] = stored_session.get("event_id")
                active = next(
                    (
                        row
                        for row in stored_session.get("message_log", [])
                        if row.get("message_id") == stored_session.get("active_message_id")
                    ),
                    {},
                )
                trigger = active.get("payload") or stored_session.get("last_payload") or {}
                context["trigger_kind"] = trigger.get("kind")
                context["trigger_item_id"] = trigger.get("item_id")
        analyst = context.get("role") == "analyst" or (
            context.get("session_id") and context.get("role") not in {"system", "scout"}
        )
        if analyst:
            bound = context.get("event_id")
            if tool in SCOPED_TOOLS and (not bound or arguments.get("event_id") != bound):
                return {
                    "accepted": False,
                    "reason": "EVENT_SCOPE",
                    "error": "Analyst mutations must target the session bound event; record_event first for an unbound session",
                }
            if tool == "record_event" and bound:
                return {
                    "accepted": False,
                    "reason": "EVENT_ALREADY_BOUND",
                    "event_id": bound,
                    "error": "Use add_observation for the session event",
                }
            if tool == "merge_into" and not bound:
                return {
                    "accepted": False,
                    "reason": "EVENT_SCOPE",
                    "error": "Merge requires a bound source event",
                }
        if (context or {}).get("role") == "scout" and tool not in {
            "web_search",
            "search_tank_items",
            "record_review",
        }:
            raise ValueError("Scout cannot mutate event state")
        if tool == "set_beneficiaries":
            novel = [
                edge
                for edge in arguments.get("edges", [])
                if isinstance(edge, dict) and edge.get("layer") not in LAYERS
            ]
            if novel:
                claims = {
                    row["claim_id"]: row for row in await self.rows("evt.claims", arguments.get("event_id"))
                }
                proposals = []
                for edge in novel:
                    evidence = [
                        ref
                        for ref in edge.get("evidence", [])
                        if ref in claims and claims[ref].get("accepted")
                    ]
                    if not evidence:
                        raise ValueError("Novel beneficiary layer requires accepted evidence")
                    proposal = await self.propose_pattern(
                        {
                            "hook": "BENEFICIARY_LAYER",
                            "subject": "BENEFICIARY_LAYER:" + str(edge.get("layer")),
                            "evidence": [{"claim_id": ref} for ref in evidence],
                        },
                        dict(context or {}),
                    )
                    proposals.append(proposal["prp_id"])
                return {
                    "accepted": False,
                    "reason": "Noncanonical beneficiary layer requires scout review",
                    "proposal_ids": proposals,
                }
        schema = domain_schema(self.settings, tool)
        if schema is not None:
            validate_schema(arguments, schema)
        ctx = dict(context or {})
        event_id = arguments.get("event_id") or ctx.get("event_id")
        if tool == "get_decision" and not ctx.get("_decision_market_refresh_completed"):
            ctx["_decision_prepared_condition_ids"] = await self.refresh_fired_condition_truth(arguments, ctx)
            if ctx.get("_decision_market_refresh_only"):
                return {
                    "market_refresh_completed": True,
                    "prepared_condition_ids": ctx["_decision_prepared_condition_ids"],
                }
        if tool in {
            "record_event",
            "add_observation",
            "merge_into",
            "set_assessment",
            "set_beneficiaries",
            "publish_card",
            "schedule_milestone",
            "get_decision",
            "set_watch_condition",
        }:
            async with AsyncExitStack() as locks:
                if tool == "record_event" and ctx.get("session_id"):
                    await locks.enter_async_context(self.repo.lock("session:" + ctx["session_id"]))
                if tool in {"record_event", "add_observation"}:
                    await locks.enter_async_context(self.repo.lock("ingest-dedup"))
                await locks.enter_async_context(self.repo.transaction())
                async with self.repo.lock(
                    "event:" + str(event_id or ctx.get("session_id") or arguments.get("item_id"))
                ):
                    if tool == "record_event" and analyst and ctx.get("session_id"):
                        current_session = await self.repo.get("ops.agent_sessions", ctx["session_id"])
                        if current_session and current_session.get("event_id"):
                            return {
                                "accepted": False,
                                "reason": "EVENT_ALREADY_BOUND",
                                "event_id": current_session["event_id"],
                                "error": "Use add_observation for the session event",
                            }
                    result = await getattr(self, tool)(arguments, ctx)
                    await tool_progress(self.repo, self.settings, tool, arguments, ctx, result)

            if tool == "set_assessment":
                await self.scheduler.ensure_event_reactions(event_id)
            return fit_result(result) if tool in READ_TOOLS else result
        result = await getattr(self, tool)(arguments, ctx)
        await tool_progress(self.repo, self.settings, tool, arguments, ctx, result)
        return fit_result(result) if tool in READ_TOOLS else result

    async def rows(self, table, event_id=None, *, filters=None):
        query = dict(filters or {})
        if event_id is not None:
            query["event_id"] = event_id
        return [row async for row in self.repo.iter_all(table, filters=query or None)]

    async def latest_record(self, table, event_id, token_id):
        return await self.repo.latest_before(
            table,
            "cutoff",
            iso(utcnow() + timedelta(microseconds=1)),
            filters={"event_id": event_id, "token_id": token_id},
        )

    async def first_alert(self, event_id, kind=None):
        filters = {"event_id": event_id}
        if kind:
            filters["kind"] = kind
        async for row in self.repo.iter_range("out.alerts", "generated_at", filters=filters, batch_size=1):
            return row
        return None

    async def require(self, table, key):
        row = await self.repo.get(table, key)
        if row is None:
            raise ValueError(f"Missing {table}: {key}")
        return row

    async def get_item(self, a, c):
        return self.source_item_view(await self.require("ingest.source_items", a["item_id"]))

    def source_item_view(self, item):
        """Expose the exact clock used by V7 without rewriting capture evidence."""
        if self.reference_clock is None:
            return item
        known = self.item_known_at(item)
        return {
            **copy.deepcopy(item),
            **self.item_reference_fields(item),
            "freshness_reference": {
                "value": known.value,
                "precision": known.precision,
                "clock_domain": "FIXTURE_SIMULATED_AS_OF",
                "basis": "V7 measures public_at age at this declared item availability; observed_at remains the actual capture receipt.",
            },
            "analysis_cutoff": iso(utcnow()),
        }

    async def search_tank_items(self, a, c):
        days = a.get("days", 90)
        if not isinstance(days, (int, float)) or not 0 < days <= 90:
            raise ValueError("days must be >0 and <=90")
        now = utcnow()
        cutoff = now - timedelta(days=days)
        sources = a.get("source_ids", [])
        terms = a["query"].casefold().split()
        if self.reference_clock is not None:
            rows = []

            for item_id in self.reference_clock.item_ids:
                item = await self.repo.get("ingest.source_items", item_id)
                if item is None:
                    continue
                known = parse_time(self.item_known_at(item).value)
                if (
                    cutoff <= known <= now
                    and (not sources or item["source_id"] in sources)
                    and all(
                        t in (item.get("title", "") + " " + item.get("text", "")).casefold() for t in terms
                    )
                ):
                    rows.append((known, item_id, item))
            return {"items": [self.source_item_view(row[2]) for row in sorted(rows, reverse=True)[:20]]}
        rows = [
            r
            async for r in self.repo.iter_range(
                "ingest.source_items", "observed_at", start=iso(cutoff), end=iso(now)
            )
        ]
        return {
            "items": sorted(
                [
                    r
                    for r in rows
                    if parse_time(r["observed_at"]) >= cutoff
                    and (not sources or r["source_id"] in sources)
                    and all(t in (r.get("title", "") + " " + r.get("text", "")).casefold() for t in terms)
                ],
                key=lambda r: r["observed_at"],
                reverse=True,
            )[:20]
        }

    def identities(self, a):
        """The registry identities a token_id or entity_id filter covers: the token or entity and its pair."""
        tokens = self.settings.load_config("registry").get("tokens", [])
        wanted = set()
        if a.get("token_id"):
            wanted |= {
                a["token_id"],
                *(row.get("entity_id") for row in tokens if row["token_id"] == a["token_id"]),
            }
        if a.get("entity_id"):
            wanted |= {
                a["entity_id"],
                *(row["token_id"] for row in tokens if row.get("entity_id") == a["entity_id"]),
            }
        return wanted - {None}

    async def list_open_events(self, a, c):
        wanted = self.identities(a)

        statuses = {}
        for session in await self.rows("ops.agent_sessions"):
            if session.get("event_id"):
                statuses.setdefault(session["event_id"], set()).add(session.get("status"))
        failed = {eid for eid, seen in statuses.items() if seen == {"FAILED"}}
        sessions = {}
        for session in await self.rows("ops.agent_sessions"):
            if session.get("event_id"):
                sessions.setdefault(session["event_id"], []).append(
                    {"session_id": session.get("session_id"), "status": session.get("status")}
                )
        fields = (
            "event_id",
            "statement",
            "entities",
            "tokens_mentioned",
            "event_state",
            "state_verified",
            "lifecycle",
            "certainty",
            "freshness",
            "known_to_us_at",
            "public_at",
        )
        return {
            "events": [
                {**{k: r.get(k) for k in fields}, "sessions": sessions.get(r["event_id"], [])}
                for r in await self.rows("evt.events")
                if r["lifecycle"] not in {"CLOSED", "MERGED", "INVALIDATED"}
                and r["event_id"] not in failed
                and (
                    not wanted
                    or wanted
                    & {
                        *(e.get("entity_id") for e in r.get("entities", []) if isinstance(e, dict)),
                        *r.get("tokens_mentioned", []),
                    }
                )
            ]
        }

    async def get_event(self, a, c):
        eid = a["event_id"]
        event = await self.require("evt.events", eid)
        return {
            "event": event,
            **{
                table.split(".")[-1]: await self.rows(table, eid)
                for table in [
                    "evt.event_observations",
                    "evt.claims",
                    "evt.state_changes",
                    "evt.ems_assessments",
                    "evt.beneficiary_maps",
                    "evt.beneficiary_edges",
                ]
            },
        }

    async def get_registry(self, a, c):
        wanted = self.identities(a)
        registry = self.settings.load_config("registry")
        return {
            name: [
                r
                for r in registry.get(name, [])
                if not wanted or wanted & {r.get("entity_id"), r.get("token_id")}
            ]
            for name in ["entities", "tokens", "instruments", "x_accounts"]
        } | {"dependency_map": self.settings.load_config("dependency_map")}

    async def covered_daily_volume(self, token, cutoff):
        """03 §4.2: the base of M for access and distribution effects is the
        token's covered daily volume. Per covered spot venue, the venue's 24 h
        volume (native and USD) from the latest stored 24 h ticker at or before
        the cutoff; a venue without a fresh, complete figure is excluded with
        its reason and is not part of the total."""
        registry = self.settings.load_config("registry")
        instruments = [
            row
            for row in registry.get("instruments", [])
            if row.get("token_id") == token and row.get("market_type") == "SPOT" and row.get("active", True)
        ]
        venues, excluded, usd_missing = [], [], []
        total_native, total_usd = Decimal(0), Decimal(0)
        for instrument in instruments:
            iid = instrument["instrument_id"]
            row = await self.repo.latest_before(
                "mkt.tickers_24h",
                "ts",
                iso(cutoff + timedelta(microseconds=1)),
                filters={"instrument_id": iid},
            )
            if row is None:
                excluded.append({"instrument_id": iid, "reason": "NO_STORED_24H_TICKER"})
                continue
            age = (cutoff - parse_time(row["ts"])).total_seconds()
            if age > TICKER_MAX_AGE_S:
                excluded.append({"instrument_id": iid, "reason": "TICKER_STALE", "ticker_ts": row["ts"]})
                continue
            if row.get("volume_native") is None:
                excluded.append({"instrument_id": iid, "reason": "VOLUME_INCOMPLETE", "ticker_ts": row["ts"]})
                continue
            native = Decimal(str(row["volume_native"]))
            usd = None if row.get("volume_usd") is None else Decimal(str(row["volume_usd"]))
            total_native += native
            if usd is None:
                usd_missing.append(
                    {"instrument_id": iid, "reason": row.get("usd_reason") or "USD_UNAVAILABLE"}
                )
            else:
                total_usd += usd
            venues.append(
                {
                    "instrument_id": iid,
                    "venue": instrument.get("venue"),
                    "volume_native": str(native),
                    "volume_usd": None if usd is None else str(usd),
                    "usd_reason": None if usd is not None else row.get("usd_reason") or "USD_UNAVAILABLE",
                    "ticker_ts": row["ts"],
                    "age_s": round(age, 1),
                }
            )
        return {
            "window": "trailing 24 h at the venue's latest 24 h ticker up to the analysis cutoff",
            "analysis_cutoff": iso(cutoff),
            "total_native": str(total_native) if venues else None,
            "total_usd": str(total_usd) if venues and len(usd_missing) < len(venues) else None,
            "total_usd_excludes": usd_missing,
            "unit_native": token.rpartition(":")[2],
            "venues": venues,
            "excluded": excluded,
            "reason": None if venues else "NO_COVERED_SPOT_VOLUME",
            "market_cap": None,
            "market_cap_reason": "NOT_STORED: Tank stores no circulating supply or market cap for the token",
        }

    async def get_token_facts(self, a, c):
        """Stored fundamentals, supply and unlocks known at the analysis cutoff
        (03 §3.2). Unlocks dated before the cutoff stay visible, labelled PAST,
        so the agent can weigh a recent unlock; Tank draws no conclusion."""
        token = a["token_id"]
        registry = await self.get_registry({"token_id": token}, c)
        cutoff = utcnow()

        def known(row):
            at = known_at(row)
            return at is None or at <= cutoff

        unlocks = []
        for row in sorted(
            (r for r in await self.rows("mkt.unlocks", filters={"token_id": token}) if known(r)),
            key=lambda r: str(r.get("date") or ""),
        ):
            date = row.get("date")
            unlocks.append(
                dict(
                    row,
                    relative_to_cutoff=None
                    if not date
                    else "PAST"
                    if parse_time(date) < cutoff
                    else "UPCOMING",
                )
            )
        unlock_summary = summarize_unlocks(unlocks, cutoff)

        dated = [row for row in unlocks if row.get("date")]
        dated.sort(key=lambda row: abs((parse_time(row["date"]) - cutoff).total_seconds()))
        nearest = sorted(dated[:UNLOCK_ROWS_SHOWN], key=lambda row: str(row["date"]))
        unlocks = (
            nearest
            + [row for row in unlocks if not row.get("date")][: max(0, UNLOCK_ROWS_SHOWN - len(nearest))]
        )

        latest = {}
        for row in await self.rows("mkt.supply_obs", filters={"token_id": token}):
            if not known(row):
                continue
            key = (row.get("kind"), row.get("chain"), row.get("instrument_id"), row.get("fee_scope"))
            if key not in latest or str(row.get("ts") or "") > str(latest[key].get("ts") or ""):
                latest[key] = row
        chains = sorted(
            await self.rows("evt.capture_chains", filters={"token_id": token}),
            key=lambda r: (str(r.get("event_id")), int(r.get("version") or 0)),
        )
        newest_chain = {}
        for chain in chains:
            newest_chain[chain.get("event_id")] = chain
        return {
            "registry": registry,
            "analysis_cutoff": iso(cutoff),
            "supply_obs": [
                compact_series(row) for row in sorted(latest.values(), key=lambda r: str(r.get("kind")))
            ],
            "covered_daily_volume": await self.covered_daily_volume(token, cutoff),
            "unlock_summary": unlock_summary,
            "unlocks": unlocks,
            "capture_chains": [compact_series(row) for row in list(newest_chain.values())[-5:]],
        }

    def validate_evidence_url(self, url):
        parsed = urlparse(url)
        secret_names = {
            "apikey",
            "api_key",
            "access_token",
            "authorization",
            "client_secret",
            "api-token",
            "auth_token",
        }
        if (
            parsed.username
            or parsed.password
            or any(
                name.casefold() in secret_names for name, _ in parse_qsl(parsed.query, keep_blank_values=True)
            )
        ):
            raise ValueError("Credential-bearing URLs cannot be stored as evidence")
        configured = [
            getattr(self.settings, name, None)
            for name in [
                "etherscan_api_key",
                "opencode_api_key",
                "openma_api_key",
                "telegram_token",
                "codex_gateway_key",
            ]
        ]
        decoded = unquote(url)
        if any(value and value in decoded for value in configured):
            raise ValueError("Credential-bearing URLs cannot be stored as evidence")

    def validate_evidence_bytes(self, data):
        configured = [
            getattr(self.settings, name, None)
            for name in [
                "etherscan_api_key",
                "opencode_api_key",
                "openma_api_key",
                "telegram_token",
                "codex_gateway_key",
            ]
        ]
        if any(value and len(value) >= 8 and value.encode("utf-8") in data for value in configured):
            raise ValueError("Credential-bearing content cannot be stored as evidence")

    def persist_web_raw(self, url, original, media_type, context, evidence_kind="FETCHED_PAGE"):
        from tank.sources.raw_store import write_raw_item

        clock = collection_clock()
        digest = sha256(original)
        iid = (
            "web-"
            + sha256(
                {
                    "url": url,
                    "raw_sha256": digest,
                    "media_type": media_type,
                    "evidence_kind": evidence_kind,
                    "session": context.get("session_id"),
                }
            )[:32]
        )
        raw, saved_digest = write_raw_item(self.settings.data_dir, "WEB", iid, clock.value, original)
        return {
            "item_id": iid,
            "raw_ref": raw,
            "raw_sha256": saved_digest,
            "clock": clock,
            "evidence_kind": evidence_kind,
        }

    async def save_web(
        self,
        url,
        text,
        title,
        c,
        source_class="SECONDARY",
        raw_bytes=None,
        media_type="text/plain",
        raw_capture=None,
        evidence_kind="FETCHED_PAGE",
    ):
        self.validate_evidence_url(url)
        original = raw_bytes if raw_bytes is not None else text.encode("utf-8")
        self.validate_evidence_bytes(original)
        if evidence_kind not in {"FETCHED_PAGE", "SEARCH_RESULT"}:
            raise ValueError("Unknown web evidence origin")
        capture = raw_capture or self.persist_web_raw(url, original, media_type, c, evidence_kind)
        digest = capture["raw_sha256"]
        iid = capture["item_id"]
        raw = capture["raw_ref"]
        if digest != sha256(original) or capture["evidence_kind"] != evidence_kind:
            raise ValueError("Captured raw evidence hash mismatch")
        row = dict(
            item_id=iid,
            source_id="WEB",
            feed="web:search" if evidence_kind == "SEARCH_RESULT" else "web:page",
            published_prec="COLLECTION_ONLY",
            relevance={"decision": "ALWAYS", "reason": "AGENT_REQUESTED_EVIDENCE"},
            external_id=iid,
            raw_ref=str(raw),
            raw_sha256=digest,
            raw_media_type=media_type,
            evidence_kind=evidence_kind,
            raw_size_bytes=len(original),
            readable_text_sha256=sha256(text),
            content_hash=sha256(normalized(text)),
            url=url,
            title=title,
            text=text,
            **capture["clock"].fields("observed_at"),
            published_at=None,
            published_at_precision="COLLECTION_ONLY",
            source_class=source_class,
            extraction_status="PENDING",
            **self.provenance(c),
        )
        existing = await self.repo.get("ingest.source_items", iid)
        if existing:
            return existing
        await self.repo.put("ingest.source_items", iid, row)
        await item_progress(
            self.repo,
            self.settings,
            iid,
            "EVIDENCE_FETCHED",
            "Источник запрошен агентом; Jev не требуется",
            event_id=c.get("event_id"),
        )
        return row

    async def web_search(self, a, c):
        from tank.search import DuckDuckGoSearchService, SearchError

        if self.search_service is None:
            self.search_service = DuckDuckGoSearchService(user_agent=self.settings.user_agent)

        async def count_attempt(attempt):
            key = new_id("search-usage")
            await self.repo.put(
                "ops.llm_usage",
                key,
                dict(
                    usage_id=key,
                    call="WEB_SEARCH",
                    provider="duckduckgo",
                    session_id=c.get("session_id"),
                    event_id=c.get("event_id"),
                    call_id=c.get("call_id"),
                    attempt=attempt,
                    web_searches=1,
                    usage={"web_searches": 1},
                    cost_usd="0",
                    billing_basis="FREE",
                    **collection_clock().fields("at"),
                    model="duckduckgo",
                    effort="none",
                    input_tokens=0,
                    cache_read_tokens=0,
                    cache_write_tokens=0,
                    output_tokens=0,
                    stop_reason="SEARCH_ATTEMPT",
                    query_sha256=sha256(a["query"]),
                    **collection_clock().fields("created_at"),
                    **{key: value for key, value in self.provenance(c).items() if key not in {"session_id"}},
                ),
                immutable=True,
            )

        try:
            response = await self.search_service.search(a["query"], on_attempt=count_attempt)
        except SearchError as error:
            return {"items": [], "search": error.as_dict()}
        items = []
        for result in response.results:
            item = await self.save_web(
                result.href,
                result.body,
                result.title,
                c,
                raw_bytes=canonical_json(result.model_dump(mode="json")).encode("utf-8"),
                media_type="application/json",
                evidence_kind="SEARCH_RESULT",
            )
            items.append(item)
        return {
            "items": items,
            "provider": "duckduckgo",
            "attempts": response.attempts,
            "billing_basis": "FREE",
        }

    async def fetch_page(self, a, c):
        self.validate_evidence_url(a["url"])
        if urlparse(a["url"]).scheme != "https":
            raise ValueError("Only HTTPS evidence fetches are allowed")
        limit = min(20, float(a.get("timeout_s") or 20))
        if is_exchange_url(a["url"]):
            response = await capture_fetch(self.repo, "GET", a["url"], budget_s=limit)
            response.raise_for_status()
            if len(response.content) > 2 * 1024 * 1024:
                raise ValueError("Page exceeds 2 MB")
            chunks, media_type = [response.content], response.headers.get("content-type")
        else:
            chunks, media_type = await self._direct_page(a["url"], limit)
        original = b"".join(chunks)
        self.validate_evidence_bytes(original)
        return await self._store_page(a, c, original, media_type or "application/octet-stream")

    async def _direct_page(self, url, limit):
        async with (
            asyncio.timeout(limit),
            (
                httpx.AsyncClient(timeout=limit, follow_redirects=False)
                if self.http is None
                else _borrow(self.http)
            ) as client,
            client.stream("GET", url, headers={"User-Agent": self.settings.user_agent}) as r,
        ):
            r.raise_for_status()
            chunks = []
            size = 0
            async for chunk in r.aiter_bytes():
                size += len(chunk)
                if size > 2 * 1024 * 1024:
                    raise ValueError("Page exceeds 2 MB")
                chunks.append(chunk)
            return chunks, r.headers.get("content-type")

    async def _store_page(self, a, c, original, media_type):

        capture = self.persist_web_raw(a["url"], original, media_type, c)
        from selectolax.parser import HTMLParser

        html = original.decode("utf-8", errors="replace")
        tree = HTMLParser(html)
        for node in tree.css("script,style,noscript"):
            node.decompose()
        return await self.save_web(
            a["url"],
            tree.text(separator=" ", strip=True),
            "Fetched page",
            c,
            raw_bytes=original,
            media_type=media_type,
            raw_capture=capture,
        )

    def validate_time(self, x, item):

        if self.reference_clock is not None:
            self.item_known_at(item)
        public = x["public_at"]
        return iso(public["value"] if isinstance(public, dict) else public)

    async def validate_claims(self, claims, item, c, certainty, obs_id, eid):
        """Persist the analyst's assertions; V1/V2/V5/V7 are not host classifiers."""
        accepted = []
        prior_claims = {row["claim_id"]: row for row in await self.rows("evt.claims", eid)}
        for raw in claims:
            supersedes = raw.get("supersedes_claim_ids", [])
            if not isinstance(supersedes, list) or any(not isinstance(ref, str) for ref in supersedes):
                raise ValueError("supersedes_claim_ids must be claim ID strings")
            if len(supersedes) != len(set(supersedes)):
                raise ValueError("Duplicate superseded claim ID")
            if any(ref == raw.get("claim_id") or ref not in prior_claims for ref in supersedes):
                raise ValueError("Superseded claims must already exist in the same event")
            claim = copy.deepcopy(raw)
            quote = claim.get("quote")
            evidence = claim.get("evidence") or (
                [{"item_id": item["item_id"], "quote": quote}] if quote else []
            )
            if claim.get("type") not in {
                "FACT",
                "STATE",
                "NUMBER",
                "RELATION",
                "TIMESTAMP",
                "MECHANISM",
                "MILESTONE",
            }:
                raise ValueError("Invalid claim type")
            verified = claim.get("verified", False)
            if not isinstance(verified, bool):
                raise TypeError("Claim verified must be Boolean")
            claim.update(
                claim_id=new_id("clm"),
                obs_id=obs_id,
                event_id=eid,
                evidence=copy.deepcopy(evidence),
                verified=verified,
                accepted=True,
                **self.provenance(c),
            )
            await self.repo.put("evt.claims", claim["claim_id"], claim)
            accepted.append(claim)
        return accepted, []

    async def record_event(self, a, c):
        x = a.get("extraction", a)
        required = {
            "item_id",
            "is_event",
            "statement",
            "certainty",
            "certainty_reason",
            "entities",
            "token_ids",
            "event_time",
            "public_at",
            "freshness",
            "claims",
            "next_milestone",
            "checked_sources",
        }
        if required - set(x):
            raise ValueError("Missing extraction fields: " + ",".join(sorted(required - set(x))))
        item = await self.require("ingest.source_items", x["item_id"])
        self.validate_time(x, item)
        if x["certainty"] not in {"LOW", "MEDIUM", "HIGH"} or not x["certainty_reason"]:
            raise ValueError("certainty and reason required")
        if not x["is_event"]:
            item["extraction_status"] = "SKIPPED"
            item["extraction_decision"] = "NO_EVENT"
            item["extraction_reason"] = x.get("no_event_reason") or ""
            await self.repo.put("ingest.source_items", item["item_id"], item)
            await item_progress(
                self.repo, self.settings, item["item_id"], "AGENT_REJECTED", item["extraction_reason"]
            )
            return {"is_event": False}
        eid = new_id("evt")
        obs = new_id("obs")
        registry = self.settings.load_config("registry")
        names = [str(e["name"]) for e in x["entities"] if e.get("name") is not None]
        context = " ".join([x["statement"], *names, *[str(t) for t in x.get("token_names", [])]])
        entities = [
            {
                "entity_id": entity_identity(registry, e.get("entity_id"), e.get("name"), context),
                "role": e["role"],
            }
            for e in x["entities"]
        ]
        tokens_mentioned = token_identities(registry, x["token_ids"], x.get("token_names", []), context)
        milestone = x.get("next_milestone")
        if milestone and not await self.registered_source(milestone.get("source_id")):
            raise ValueError("next_milestone.source_id must be a registered source_id")
        claims, dropped = await self.validate_claims(x["claims"], item, c, x["certainty"], obs, eid)
        primary_fact_basis = [claim["claim_id"] for claim in claims if claim["verified"]]
        certainty = x["certainty"]
        certainty_reason = x["certainty_reason"]
        event_time = copy.deepcopy(x["event_time"])
        event_time_stamp = propagate_timestamp({"event_time": event_time}, "event_time")
        event_time.update(
            precision=event_time_stamp.precision, precision_reason=event_time_stamp.precision_reason
        )
        event = dict(
            event_id=eid,
            statement=x["statement"],
            entities=entities,
            tokens_mentioned=tokens_mentioned,
            theme_tags=x.get("theme_tags", []),
            event_time=event_time,
            **propagate_timestamp({"public_at": x["public_at"]}, "public_at").fields("public_at"),
            event_time_prec=event_time_stamp.precision,
            event_time_prec_reason=event_time_stamp.precision_reason,
            public_at_basis=x["public_at"]["basis"],
            **self.item_known_at(item).fields("known_to_us_at"),
            **self.item_reference_fields(item),
            freshness=x["freshness"],
            event_state="RUMOR",
            state_verified=False,
            certainty=certainty,
            certainty_reason=certainty_reason,
            certainty_evidence_claims=primary_fact_basis,
            checked_sources=copy.deepcopy(x["checked_sources"]),
            timing_confidence=x.get("timing_confidence")
            or (
                "HIGH"
                if x["public_at"]["precision"] == "EXACT"
                else "LOW"
                if x["public_at"]["precision"] in {None, "COLLECTION_ONLY"}
                else "MEDIUM"
            ),
            best_source_class=item["source_class"],
            next_milestone=x["next_milestone"],
            lifecycle="DETECTED",
            **self.provenance(c),
        )
        await self.apply_states(event, claims, c)
        await self.repo.put("evt.events", eid, event)
        await self.repo.put(
            "evt.event_observations",
            obs,
            dict(
                obs_id=obs,
                event_id=eid,
                item_id=item["item_id"],
                link_type="ORIGIN",
                **propagate_timestamp({"public_at": x["public_at"]}, "public_at").fields("public_at"),
                freshness=x["freshness"],
                match_method="AGENT",
                **propagate_timestamp(item, "observed_at").fields("observed_at"),
                **self.item_reference_fields(item),
                **self.provenance(c),
            ),
        )
        await self._complete_source(item["item_id"])
        await item_progress(
            self.repo, self.settings, item["item_id"], "EVENT_RECORDED", x["statement"], event_id=eid
        )
        if c.get("session_id"):
            async with self.repo.lock("session:" + c["session_id"]):
                session = await self.repo.get("ops.agent_sessions", c["session_id"])
                if session:
                    session["event_id"] = eid
                    await self.repo.put("ops.agent_sessions", c["session_id"], session)
        if claims and item.get("source_id") == "S24" and item.get("end"):
            await self.schedule_milestone(
                {
                    "event_id": eid,
                    "milestone": "Snapshot vote ends",
                    **propagate_timestamp(item, "end").fields("due_at"),
                    "source_id": "S24",
                },
                c,
            )
        elif milestone and all(milestone.get(key) for key in ["kind", "due_at", "source_id"]):
            await self.schedule_milestone(
                {
                    "event_id": eid,
                    "milestone": milestone["kind"],
                    "due_at": milestone["due_at"],
                    "source_id": milestone["source_id"],
                },
                c,
            )
        return {"event_id": eid, "accepted_claims": claims, "dropped_claims": dropped}

    async def approval_rejection_targets(self, event, rejection):
        dependency = rejection.get("approval_dependency")
        if dependency is None:
            return []
        if dependency.get("scope") not in {"EVENT", "TOKEN"} or not dependency.get("claim_ids"):
            raise ValueError("Approval rejection requires an explicit evidenced thesis dependency")
        token_ids = dependency.get("token_ids", [])
        if dependency["scope"] == "EVENT" and token_ids:
            raise ValueError("Event-wide dependency must not name token-only targets")
        if dependency["scope"] == "TOKEN" and not token_ids:
            raise ValueError("Token dependency requires token targets")
        registry = self.settings.load_config("registry")
        tokens = {row["token_id"]: row for row in registry.get("tokens", [])}
        if any(token not in tokens for token in token_ids):
            raise ValueError("Approval dependency token is not registered")
        claims = []
        for claim_id in dependency["claim_ids"]:
            claim = await self.require("evt.claims", claim_id)
            if (
                claim.get("event_id") != event["event_id"]
                or not claim.get("accepted")
                or claim.get("type") != "RELATION"
                or not claim.get("evidence")
            ):
                raise ValueError(
                    "Thesis dependency must cite accepted causal relation claims from this event"
                )
            claims.append(claim)

        evidence = [claim["claim_id"] for claim in claims]
        return (
            [(token_id, evidence) for token_id in token_ids]
            if dependency["scope"] == "TOKEN"
            else [(None, evidence)]
        )

    async def apply_states(self, event, claims, c):
        old = event["event_state"]
        states = [r["state_claim"] for r in claims if r.get("state_claim") in STATES]
        event["event_state"] = max([old, *states], key=STATES.index)
        event["state_verified"] = (
            event.get("state_verified", False)
            if old == event["event_state"]
            else any(r["verified"] and r["state_claim"] == event["event_state"] for r in claims)
        )
        if any(r["verified"] and r.get("state_claim") == event["event_state"] for r in claims):
            event["state_verified"] = True
        if old != event["event_state"]:
            sid = new_id("state")
            await self.repo.put(
                "evt.state_changes",
                sid,
                dict(
                    state_change_id=sid,
                    event_id=event["event_id"],
                    ladder="EVENT",
                    from_state=old,
                    to_state=event["event_state"],
                    evidence=[r["claim_id"] for r in claims if r.get("state_claim") == event["event_state"]],
                    **collection_clock().fields("changed_at"),
                    **self.provenance(c),
                ),
                immutable=True,
            )
        for claim in claims:
            marker = claim.get("state_claim")
            if marker not in {"CANCELLED", "REVERSED", "REJECTED", "PAUSED"}:
                continue
            event["negative_marker"] = marker
            if marker == "REJECTED":
                for token_id, dependency_claims in await self.approval_rejection_targets(event, claim):
                    await self.contradiction(
                        event["event_id"],
                        "FATAL",
                        "Approval rejected: " + claim["object"],
                        [claim["claim_id"], *dependency_claims],
                        c,
                        token_id=token_id,
                    )
            else:
                await self.contradiction(
                    event["event_id"],
                    "UNRESOLVED" if marker == "PAUSED" else "FATAL",
                    marker,
                    [claim["claim_id"]],
                    c,
                )

    async def contradiction(self, eid, kind, description, evidence, c, *, token_id=None):
        key = new_id("ctr")
        row = dict(
            contradiction_id=key,
            event_id=eid,
            token_id=token_id,
            type=kind,
            description=description,
            evidence=evidence,
            **collection_clock().fields("detected_at"),
            **self.provenance(c),
        )
        await self.repo.put("ana.contradictions", key, row)
        return row

    async def add_observation(self, a, c):
        event = await self.require("evt.events", a["event_id"])
        item = await self.require("ingest.source_items", a["item_id"])
        link = a.get("link", a.get("link_type"))
        if link not in {"CORROBORATION", "STATE_UPDATE", "CONTRADICTION", "DENIAL"}:
            raise ValueError("Invalid observation link")
        if "public_at" not in a or "freshness" not in a:
            raise ValueError("V7 observation public_at and freshness required")
        self.validate_time(a, item)
        obs = new_id("obs")
        certainty = a.get("certainty", event["certainty"])
        claims, dropped = await self.validate_claims(a["claims"], item, c, certainty, obs, event["event_id"])
        verified_claims = [claim for claim in claims if claim["verified"]]
        if "timing_confidence" in a:
            event["timing_confidence"] = a["timing_confidence"]
        if "certainty" in a:
            if certainty not in {"LOW", "MEDIUM", "HIGH"}:
                raise ValueError("Invalid certainty")
            event["certainty"] = certainty
            if "certainty_reason" in a:
                event["certainty_reason"] = a["certainty_reason"]
        before = event["event_state"]
        await self.apply_states(event, claims, c)
        contradictions = []
        if link == "DENIAL":
            if not any(r["verified"] for r in claims):
                raise ValueError("DENIAL requires accepted primary evidence")
            event["lifecycle"] = "INVALIDATED"
            contradictions = [
                await self.contradiction(
                    event["event_id"], "FATAL", "Official denial", [r["claim_id"] for r in claims], c
                )
            ]
        elif link == "CONTRADICTION" and claims:
            contradictions = [
                await self.contradiction(
                    event["event_id"],
                    "UNRESOLVED",
                    "Contradicting observation",
                    [r["claim_id"] for r in claims],
                    c,
                )
            ]
        milestone = event.get("next_milestone")
        explicit = a.get("milestone_outcome")
        state_confirmation = event["event_state"] != before or link == "DENIAL"
        matching_source = bool(milestone and item["source_id"] == milestone["source_id"])
        if milestone and item.get("source_id") == "WEB" and item.get("evidence_kind") != "SEARCH_RESULT":
            source = next(
                (
                    source
                    for source in self.settings.load_config("sources")["sources"]
                    if source["source_id"] == milestone["source_id"]
                ),
                {},
            )
            host = (urlparse(item.get("url", "")).hostname or "").lower()
            matching_source = any(
                host == (urlparse(feed.get("official_index_url", feed.get("url", ""))).hostname or "").lower()
                for feed in source.get("feeds", [])
            )
        if (
            milestone
            and verified_claims
            and (state_confirmation or explicit)
            and matching_source
            and parse_time(self.item_known_at(item).value)
            >= parse_time(milestone["due_at"]) - timedelta(minutes=5)
        ):
            quotes = [entry["quote"] for claim in verified_claims for entry in claim["evidence"]]
            resolution = dict(
                milestone,
                resolution_id=new_id("milestone"),
                status="CONFIRMED",
                **collection_clock().fields("confirmed_at"),
                outcome=explicit["outcome"]
                if explicit
                else "Official denial"
                if link == "DENIAL"
                else event["event_state"],
                quotes=[explicit["quote"]] if explicit else quotes,
                evidence=[claim["claim_id"] for claim in verified_claims],
            )
            event.setdefault("milestone_resolutions", []).append(resolution)
            event.update(
                next_milestone=None,
                milestone_resolved=True,
                milestone_resolution_id=resolution["resolution_id"],
            )
            for escalation in await self.rows("ops.source_escalations", event["event_id"]):
                if escalation["source_id"] == milestone["source_id"] and escalation.get(
                    "trigger", ""
                ).startswith("MILESTONE:"):
                    escalation.update(
                        status="CONFIRMED",
                        **collection_clock().fields("confirmed_at"),
                        evidence=resolution["evidence"],
                    )
                    await self.repo.put("ops.source_escalations", escalation["escalation_id"], escalation)
        if claims and link != "CORROBORATION" and event["lifecycle"] != "INVALIDATED":
            event["lifecycle"] = "SCORED"
        await self.repo.put("evt.events", event["event_id"], event)
        await self.repo.put(
            "evt.event_observations",
            obs,
            dict(
                obs_id=obs,
                event_id=event["event_id"],
                item_id=item["item_id"],
                link_type=link,
                **propagate_timestamp({"public_at": a["public_at"]}, "public_at").fields("public_at"),
                freshness=a["freshness"],
                match_method="AGENT",
                **propagate_timestamp(item, "observed_at").fields("observed_at"),
                **self.item_reference_fields(item),
                **self.provenance(c),
            ),
        )
        await self._complete_source(item["item_id"])
        await item_progress(
            self.repo, self.settings, item["item_id"], "OBSERVATION_ADDED", link, event_id=event["event_id"]
        )
        return {
            "event_state": event["event_state"],
            "accepted_claims": claims,
            "dropped_claims": dropped,
            "contradictions": contradictions,
            "link_type": link,
        }

    async def merge_into(self, a, c):
        source = await self.require("evt.events", c["event_id"])
        target = await self.require("evt.events", a["event_id"])
        if source["event_id"] == target["event_id"]:
            return target
        if target.get("lifecycle") in {"CLOSED", "MERGED", "INVALIDATED"}:
            raise ValueError("Merge target must be an open event")
        if parse_time(target["known_to_us_at"]) > parse_time(source["known_to_us_at"]):
            raise ValueError("Merge target must be the earlier event")
        async with self.repo.lock("event:" + target["event_id"]):
            earlier = min((target, source), key=lambda row: parse_time(row["known_to_us_at"]))
            target.update(propagate_timestamp(earlier, "known_to_us_at").fields("known_to_us_at"))
            for table, key in [("evt.event_observations", "obs_id"), ("evt.claims", "claim_id")]:
                for row in await self.rows(table, source["event_id"]):
                    row["event_id"] = target["event_id"]
                    await self.repo.put(table, row[key], row)
                    if key == "obs_id":
                        await self.repo.enqueue(
                            "AGENT_MESSAGE",
                            {
                                "kind": "ITEM_ADDED",
                                "event_id": target["event_id"],
                                "item_id": row["item_id"],
                                "session_id": target.get("session_id"),
                            },
                            "merge:" + row["obs_id"],
                        )
            source.update(lifecycle="MERGED", merged_into=target["event_id"])
            await self.repo.put("evt.events", source["event_id"], source)
            await self.repo.put("evt.events", target["event_id"], target)
        return target

    async def validate_impact_horizon(self, horizon, event, accepted):

        dated = []
        for proof in horizon.get("timing_evidence", []):
            claim = accepted.get(proof["claim_id"], {})
            if claim.get("type") not in {"TIMESTAMP", "MILESTONE"} or not claim.get("verified"):
                raise ValueError("Timing evidence requires an accepted verified dated milestone")
            dated.append(parse_time(proof["due_at"]))
        if event["event_state"] == "PROPOSED" and horizon.get("impact_start") in {"NOW", "DAYS"}:
            if horizon["impact_start"] == "DAYS" and any(
                utcnow() <= due < utcnow() + timedelta(weeks=1) for due in dated
            ):
                horizon["ramp_shape"] = "CONDITIONAL"
                if not horizon.get("dependencies"):
                    raise ValueError("Proposed dated decision requires an explicit dependency")
            else:
                horizon["impact_start"] = "WEEKS"
                horizon["timing_cap_reason"] = "PROPOSED_WITHOUT_DATED_DECISION_WITHIN_DAYS"
        if horizon.get("impact_start") == "NOW" and STATES.index(event["event_state"]) < STATES.index("LIVE"):
            proof = horizon.get("immediate_market_access") or {}
            claim = accepted.get(proof.get("claim_id"), {})
            effective = parse_time(proof["effective_at"]) if proof.get("effective_at") else None
            if (
                claim.get("type") not in {"STATE", "FACT"}
                or not claim.get("verified")
                or effective is None
                or effective > utcnow()
            ):
                raise ValueError("NOW requires LIVE or evidenced immediately effective market access")
        if horizon.get("ramp_shape") == "CONDITIONAL" and not horizon.get("dependencies"):
            raise ValueError("Conditional ramp requires dependencies")

    async def set_assessment(self, a, c):
        eid = a["event_id"]
        event = await self.require("evt.events", eid)
        x = copy.deepcopy(a["assessment"])
        claims = await self.rows("evt.claims", eid)
        accepted = {r["claim_id"]: r for r in claims if r["accepted"]}
        criterion = x["criterion"]
        criterion_claim_id = x.get("criterion_claim_id")
        criterion_claim = accepted.get(criterion_claim_id) if criterion_claim_id else None
        if criterion_claim_id and criterion_claim is None:
            raise ValueError("criterion_claim_id must be an accepted claim of this event")
        quote = (criterion_claim or {}).get("quote") or next(
            (
                entry.get("quote")
                for entry in (criterion_claim or {}).get("evidence", [])
                if entry.get("quote")
            ),
            "",
        )
        x["criterion_quote"] = quote
        klass = x["class"]
        caps = []
        if klass not in {"LOW", "MATERIAL", "MAJOR", "STRUCTURAL_SHOCK"}:
            raise ValueError("Invalid EMS class")
        if criterion not in {
            "G1",
            "G2",
            "G3",
            "G4",
            "G5",
            "G6",
            "M1",
            "M2",
            "M3",
            "M4",
            "M5",
            "M6",
            "M7",
            "MATERIAL",
            "LOW",
        }:
            raise ValueError("Invalid class criterion")
        criterion_proof = (
            [criterion_claim_id]
            if criterion_claim and criterion_claim.get("verified") is True and quote.strip()
            else []
        )
        gates = x.get("structural_gates")
        if gates is None:
            gates = (
                [{"gate": criterion, "satisfied": bool(criterion_proof), "evidence": criterion_proof}]
                if criterion in {"G1", "G2", "G3", "G4", "G5", "G6"}
                else []
            )
            x["structural_gates"] = gates
        for gate in gates:
            if any(ref not in accepted for ref in gate.get("evidence", [])):
                raise ValueError("Structural gate references unaccepted evidence")
            if gate.get("satisfied") and not gate.get("evidence"):
                raise ValueError("Satisfied structural gate requires accepted evidence")

        evidence_ok = any(
            gate.get("gate") == criterion
            and gate.get("satisfied") is True
            and bool(set(gate.get("evidence", [])) & set(criterion_proof))
            for gate in gates
        )
        if klass == "STRUCTURAL_SHOCK" and (
            event["certainty"] != "HIGH"
            or not event["state_verified"]
            or STATES.index(event["event_state"]) < 2
            or not criterion.startswith("G")
            or not evidence_ok
        ):
            klass = "MAJOR"
            caps.append("STRUCTURAL_GATE")
        if klass == "MAJOR" and (
            event["certainty"] == "LOW"
            or criterion_claim is None
            or not quote.strip()
            or criterion not in {"M1", "M2", "M3", "M4", "M5", "M6", "M7", "G1", "G2", "G3", "G4", "G5", "G6"}
        ):
            klass = "MATERIAL"
            caps.append("MAJOR_EVIDENCE")
        if criterion == "M1":
            venue = x.get("criterion_venue_id")
            if not isinstance(venue, str) or not venue.strip():
                raise ValueError("M1 requires criterion_venue_id, the registry entity_id of the venue")
            if not any(
                row["entity_id"] == venue for row in self.settings.load_config("registry").get("entities", [])
            ):
                raise ValueError(
                    "criterion_venue_id is not a registry entity; M1 counts only Binance, Coinbase, OKX, Bybit, "
                    "Kraken and Robinhood, so a listing elsewhere is not M1"
                )
            if venue not in M1_VENUES:
                klass = "MATERIAL"
                caps.append("M1_VENUE")
        if (
            event["event_state"] == "PROPOSED"
            and criterion not in {"M4", "M5"}
            and klass in {"MAJOR", "STRUCTURAL_SHOCK"}
        ):
            klass = "MATERIAL"
            caps.append("PROPOSED")
        universe = {r["token_id"] for r in self.settings.load_config("registry")["tokens"]}
        current_maps = await self.rows("evt.beneficiary_maps", eid)
        current_map = max(current_maps, key=lambda row: int(row["map_version"])) if current_maps else None
        current_edges = [
            edge
            for edge in await self.rows("evt.beneficiary_edges", eid)
            if current_map and int(edge["map_version"]) == int(current_map["map_version"])
        ]
        registry = self.settings.load_config("registry")
        concerned = set(
            resolve_tokens(registry, event.get("tokens_mentioned", []), event.get("statement") or "")
        ) | {edge.get("token_id") for edge in current_edges}
        if not concerned.intersection(universe) and klass != "LOW":
            klass = "LOW"
            caps.append("NO_UNIVERSE_CONCERN")
        if event["certainty"] == "LOW" and klass in {"MAJOR", "STRUCTURAL_SHOCK"}:
            klass = "MATERIAL"
            caps.append("LOW_CERTAINTY")
        dims = x.get("dims", {})
        state_proof = [
            claim["claim_id"]
            for claim in accepted.values()
            if claim.get("state_claim") == event["event_state"]
        ]
        if isinstance(dims.get("implementation"), dict):
            dims["implementation"].update(
                value=IMPLEMENTATION[event["event_state"]],
                evidence=state_proof,
                reason=None,
            )
        for name in [
            "economic_directness",
            "magnitude",
            "permanence",
            "distribution_expansion",
            "mechanical_flows",
            "surprise",
            "implementation",
        ]:
            d = dims.get(name)
            if not isinstance(d, dict):
                raise ValueError("Every dimension requires value, rationale and evidence")
            if d.get("value") is not None and (
                not isinstance(d["value"], (int, float)) or not 0 <= d["value"] <= 10
            ):
                raise ValueError("Dimension outside 0..10")
            if not d.get("rationale") or any(ref not in accepted for ref in d.get("evidence", [])):
                raise ValueError("Dimension evidence/rationale invalid")
            if name != "implementation" and d.get("value") is not None and not d.get("evidence"):
                raise ValueError("Numeric dimension requires accepted claim evidence: " + name)
            if d.get("value") is None and not d.get("reason"):
                raise ValueError("Missing score requires reason")
        horizon = x.get("impact_horizon", {})
        await self.validate_impact_horizon(horizon, event, accepted)
        for chain in x.pop("capture_chains", []):
            score = chain.get("capture_score")
            mechanism = chain.get("token_mechanism", {})
            applied = []
            primary = any(accepted.get(ref, {}).get("verified") for ref in mechanism.get("evidence", []))
            if score is not None:
                if all(
                    not isinstance(chain.get(step), dict) or chain[step].get("value") is None
                    for step in ["eligible_activity", "effective_fees", "protocol_capture"]
                ) and not any(ref in accepted for ref in mechanism.get("evidence", [])):
                    score = min(score, 3)
                    applied.append("C1")
                if not primary:
                    score = min(score, 4)
                    applied.append("C2")
                if not mechanism.get("active"):
                    score = min(score, 5)
                    applied.append("C3")
                fees = chain.get("effective_fees")
                fees_value = fees.get("value", fees.get("number")) if isinstance(fees, dict) else fees
                if fees_value is None:
                    score = min(score, 7)
                    applied.append("C4")
            chain.update(
                event_id=eid,
                version=max(
                    (
                        int(r["version"])
                        for r in await self.rows("evt.capture_chains", eid)
                        if r["token_id"] == chain["token_id"]
                    ),
                    default=0,
                )
                + 1,
                capture_score=score,
                capture_class="UNKNOWN"
                if score is None
                else "DIRECT"
                if score >= 7 and primary and mechanism.get("active")
                else "INDIRECT"
                if score >= 4
                else "NARRATIVE_ONLY",
                conditional=not mechanism.get("active"),
                caps_applied=applied,
                **self.provenance(c),
            )
            await self.repo.put("evt.capture_chains", f"{eid}:{chain['token_id']}:{chain['version']}", chain)
        ems = new_id("ems")
        x.update(
            ems_id=ems,
            event_id=eid,
            version=max((int(r["version"]) for r in await self.rows("evt.ems_assessments", eid)), default=0)
            + 1,
            **collection_clock().fields("cutoff"),
            dims=dims,
            **{"class": klass},
            class_reasons=x.get("class_reasons", []) + caps,
            **self.provenance(c),
        )
        await self.repo.put("evt.ems_assessments", ems, x)
        event.update(
            current_ems_id=ems,
            lifecycle="INVALIDATED"
            if event["lifecycle"] == "INVALIDATED"
            else "ANALYZING"
            if klass in {"MAJOR", "STRUCTURAL_SHOCK"}
            else "TRACKED",
        )
        await self.repo.put("evt.events", eid, event)
        if klass in {"MAJOR", "STRUCTURAL_SHOCK"}:
            entity_ids = {entry["entity_id"] for entry in event["entities"]}
            registry = self.settings.load_config("registry")
            entity_ids.update(
                token.get("entity_id") for token in registry["tokens"] if token["token_id"] in concerned
            )
            source_ids = {
                sid
                for entity in registry["entities"]
                if entity["entity_id"] in entity_ids
                for sid in entity.get("official_sources", [])
            }
            for sid in source_ids:
                await self.escalate_source(sid, eid, "MAJOR", utcnow(), utcnow() + timedelta(hours=2), c)
            if klass == "STRUCTURAL_SHOCK":
                deadline_stamp = offset_schedule(
                    propagate_timestamp(event, "known_to_us_at"), timedelta(minutes=3)
                )
                x.update(partial_required=True, **deadline_stamp.fields("first_card_deadline"))
                event.update(deadline_stamp.fields("first_card_deadline"))
                await self.repo.put("evt.events", eid, event)
        await event_progress(
            self.repo,
            self.settings,
            eid,
            "ASSESSED",
            f"Класс: {klass}; " + "; ".join(str(reason) for reason in x.get("class_reasons", [])),
            key=ems,
        )
        return x

    async def set_beneficiaries(self, a, c):
        eid = a["event_id"]
        event = await self.require("evt.events", eid)
        version = (
            max((int(r["map_version"]) for r in await self.rows("evt.beneficiary_maps", eid)), default=0) + 1
        )
        claims = {r["claim_id"]: r for r in await self.rows("evt.claims", eid)}
        registry = self.settings.load_config("registry")
        tokens = {t["token_id"] for t in registry["tokens"]}
        chains = await self.rows("evt.capture_chains", eid)
        edges = []
        for raw in a["edges"]:
            e = copy.deepcopy(raw)
            context = " ".join(str(e.get(k) or "") for k in ("entity_name", "token_name"))
            e["entity_id"] = entity_identity(registry, e.get("entity_id"), e.get("entity_name"), context)
            if e.get("token_name") is not None:
                if e.get("token_id") is not None:
                    raise ValueError(
                        "Name an edge token by its registry token_id or, outside the registry, by token_name"
                    )
                e["token_id"] = token_identities(registry, [], [e["token_name"]], context)[0]
            if (
                isinstance(e.get("exposure_score"), bool)
                or not isinstance(e.get("exposure_score"), (int, float))
                or not 0 <= e["exposure_score"] <= 10
            ):
                raise ValueError("Exposure score must be numeric 0..10")
            if e["layer"] not in LAYERS or e["role"] not in {"PRIMARY", "SECONDARY", "COMPETITOR", "LOSER"}:
                raise ValueError("Invalid layer or beneficiary role")
            if not e.get("evidence") or any(
                ref not in claims or not claims[ref]["accepted"] for ref in e["evidence"]
            ):
                raise ValueError("Beneficiary requires accepted evidence")
            if not e.get("causal_chain"):
                raise ValueError("Beneficiary requires causal/share-loss mechanism")
            chain = max(
                (r for r in chains if r["token_id"] == e.get("token_id")),
                key=lambda r: int(r["version"]),
                default={},
            )
            score = chain.get("capture_score")
            state = e.get("integration_state", "COMPATIBLE")
            support = [claims[r] for r in e["evidence"]]
            stage_support = []
            for claim in support:
                claimed_state = claim.get("state_claim")
                if claimed_state not in INTEGRATION:
                    continue
                supported_state = claimed_state
                if INTEGRATION.index(supported_state) > INTEGRATION.index("BUILT") and claim.get(
                    "directionality"
                ) not in {"COUNTERPARTY_ADOPTION", "BOTH"}:
                    supported_state = "BUILT"
                stage_support.append((supported_state, claim["claim_id"]))
            state = max((stage for stage, _ in stage_support), key=INTEGRATION.index, default="COMPATIBLE")
            stage_evidence = [claim_id for stage, claim_id in stage_support if stage == state]

            exposure = e["exposure_score"]
            if 6 <= exposure < 8:
                likely = (
                    STATES.index(event["event_state"]) >= STATES.index("APPROVED")
                    if e["role"] == "PRIMARY"
                    else INTEGRATION.index(state) >= INTEGRATION.index("BUILT")
                )
                if not likely:
                    raise ValueError("Exposure at least 6 requires applicable likely-rail state evidence")
            if 8 <= exposure < 10:
                live = (
                    STATES.index(event["event_state"]) >= STATES.index("LIVE")
                    if e["role"] == "PRIMARY"
                    else INTEGRATION.index(state) >= INTEGRATION.index("LIVE")
                )
                if not live:
                    raise ValueError("Exposure at least 8 below 10 requires applicable LIVE evidence")
            if exposure == 10 and not any(
                claim.get("directionality") in {"COUNTERPARTY_ADOPTION", "BOTH"} and claim.get("evidence")
                for claim in support
            ):
                raise ValueError(
                    "Exposure 10 requires accepted counterparty evidence for the sole-rail basis"
                )

            e["tradable"] = e.get("token_id") in tokens and any(
                i["token_id"] == e.get("token_id") for i in registry["instruments"]
            )
            mature = (
                STATES.index(event["event_state"]) >= STATES.index("APPROVED")
                if e["role"] == "PRIMARY"
                else INTEGRATION.index(state) >= 2
            )
            negative = e.get("negative_thesis")
            negative_proof = []
            if negative is not None:
                if e["role"] != "LOSER" or e.get("direction") != "NEGATIVE":
                    raise ValueError("Negative thesis evidence applies only to a negative LOSER edge")
                if (
                    not negative.get("mechanism")
                    or not negative.get("quote")
                    or not negative.get("claim_ids")
                ):
                    raise ValueError(
                        "Negative thesis requires its causal mechanism and quoted claim evidence"
                    )
                for claim_id in negative["claim_ids"]:
                    if (
                        claim_id not in e["evidence"]
                        or claim_id not in claims
                        or not claims[claim_id].get("accepted")
                        or claims[claim_id].get("type") != "RELATION"
                    ):
                        raise ValueError(
                            "Negative thesis must cite accepted causal relation evidence for this edge"
                        )
                    negative_proof.append(claim_id)
            link_quality = (
                score is not None
                and score >= 4
                and e["exposure_score"] >= 6
                and mature
                and e["edge_certainty"] in {"MEDIUM", "HIGH"}
                and e["tradable"]
            )
            e.update(
                integration_state_evidence=stage_evidence,
                thesis_link_credible=bool(
                    e["role"] == "LOSER"
                    and e.get("direction") == "NEGATIVE"
                    and negative_proof
                    and link_quality
                ),
                negative_mechanism_evidence=negative_proof,
                edge_id=new_id("edge"),
                event_id=eid,
                map_version=version,
                integration_state=None if e["role"] == "PRIMARY" else state,
                capture_ref=[eid, e.get("token_id"), chain.get("version")],
                capture_score=score,
                capture_class=chain.get("capture_class", "UNKNOWN"),
                capture=chain.get("capture_class", "UNKNOWN"),
                mechanism_evidence=chain.get("token_mechanism", {}).get("evidence", []),
                credible=e["role"] in {"PRIMARY", "SECONDARY"}
                and score is not None
                and score >= 4
                and e["exposure_score"] >= 6
                and mature
                and e["edge_certainty"] in {"MEDIUM", "HIGH"}
                and e["tradable"],
                **self.provenance(c),
            )
            await self.repo.put("evt.beneficiary_edges", e["edge_id"], e)
            edges.append(e)
        primary = [e for e in edges if e["role"] == "PRIMARY" and e["tradable"]]
        secondary = sorted(
            [e for e in edges if e["role"] == "SECONDARY" and e["credible"]],
            key=lambda e: e["exposure_score"] * e["capture_score"],
            reverse=True,
        )[:4]
        losers = sorted(
            [e for e in edges if e["role"] == "LOSER" and e["tradable"]],
            key=lambda e: e["exposure_score"],
            reverse=True,
        )[:2]

        context_entities = {e["entity_id"] for e in event["entities"]}
        dependency = self.settings.load_config("dependency_map")
        for dep in dependency.get("edges", []):
            if (
                dep.get("entity_id") not in context_entities
                and dep.get("token_id") not in event["tokens_mentioned"]
            ):
                continue
            for layer in dep.get("layers_served", []):
                if layer not in LAYERS or any(
                    e["entity_id"] == dep["entity_id"] and e["layer"] == layer for e in edges
                ):
                    continue
                e = dict(
                    edge_id=new_id("edge"),
                    event_id=eid,
                    map_version=version,
                    entity_id=dep["entity_id"],
                    token_id=dep.get("token_id"),
                    layer=layer,
                    role="SECONDARY",
                    direction="POSITIVE",
                    causal_chain=dep.get("description", "Registry compatibility candidate"),
                    integration_state="COMPATIBLE",
                    directionality="SOURCE_SIDE_SUPPORT",
                    exposure_score=4,
                    capture_score=None,
                    capture_class="UNKNOWN",
                    tradable=dep.get("token_id") in tokens
                    and any(i["token_id"] == dep.get("token_id") for i in registry["instruments"]),
                    edge_certainty="LOW",
                    credible=False,
                    credibility_reasons=[
                        "Dependency-map compatibility alone does not establish adoption or capture"
                    ],
                    evidence=[],
                    dependency_ref=sha256(dep),
                    **self.provenance(c),
                )
                await self.repo.put("evt.beneficiary_edges", e["edge_id"], e)
                edges.append(e)
        notes = a.get("layer_reasons", {})
        layers = {}
        for layer in LAYERS:
            found = [e["edge_id"] for e in edges if e["layer"] == layer]
            credible = [edge for edge in edges if edge["layer"] == layer and edge.get("credible")]
            credible_secondary = any(edge["role"] == "SECONDARY" for edge in credible)
            note = notes.get(layer) or ""
            if not credible_secondary and not note.strip():
                raise ValueError(f"Layer {layer} requires NONE/UNKNOWN reason for no credible SECONDARY")
            layers[layer] = {
                "status": "FOUND" if credible else a.get("layer_statuses", {}).get(layer, "UNKNOWN"),
                "edges": found,
                "note": note,
            }
        result = dict(
            event_id=eid,
            map_version=version,
            **collection_clock().fields("cutoff"),
            layers=layers,
            no_credible_secondary_reason=None
            if secondary
            else a.get("no_credible_secondary_reason")
            or "No SECONDARY edge satisfies the credibility requirements in §4.7.",
            analysis_set=list(dict.fromkeys(e["token_id"] for e in primary + secondary + losers)),
            **self.provenance(c),
        )
        await self.repo.put("evt.beneficiary_maps", f"{eid}:{version}", result)
        return result

    async def managed_tokens(self, event_id):
        calls = await self.rows("ana.calls", event_id)
        active = {row["token_id"] for row in calls if row.get("status") == "OPEN"}
        event = await self.repo.get("evt.events", event_id) or {}
        pending = set(event.get("call_expired_ids", []))
        active.update(row["token_id"] for row in calls if row["call_id"] in pending)
        first_full = await self.first_alert(event_id, "FULL")
        shown = set(first_full.get("shown_tokens", []) if first_full else [])
        return active | shown

    async def refresh_fired_condition_truth(self, a, c):
        """Refresh actual fired-condition inputs before entering mutation locks."""
        eid = a["event_id"]
        token = a["token_id"]
        fired = [
            row
            for row in await self.rows(
                "ana.watch_conditions", eid, filters={"token_id": token, "status": "FIRED"}
            )
            if not row.get("consumed_by") and row.get("fired_at")
        ]
        prepared_ids = [row["cond_id"] for row in fired]
        if not fired:
            return prepared_ids
        latest = await self.latest_record("ana.market_truth", eid, token)
        cutoff = parse_time(latest["cutoff"]) if latest and latest.get("cutoff") else None
        from tank.scheduler import Scheduler

        fired = [
            row
            for row in fired
            if (cutoff is None or parse_time(row["fired_at"]) > cutoff)
            and Scheduler.condition_requirements(row["expr"])
        ]
        if not fired:
            return prepared_ids
        refresh_key = sha256(sorted((row["cond_id"], row["fired_at"]) for row in fired))
        await self.get_market_truth(
            {"event_id": eid, "token_ids": [token]}, c, refresh_key="condition:" + refresh_key
        )
        return prepared_ids

    async def get_market_truth(self, a, c, *, refresh_key=None):

        named = {"SECOND": "BUCKET(1s)", "MINUTE": "BUCKET(1m)", "HOUR": "BUCKET(1h)"}
        normalized_prec = None
        if a.get("cutoff_prec") in named:
            normalized_prec = {"from": a["cutoff_prec"], "to": named[a["cutoff_prec"]]}
            a = dict(a, cutoff_prec=named[a["cutoff_prec"]])
        eid = a["event_id"]
        event = await self.require("evt.events", eid)
        assessment = await self.require("evt.ems_assessments", event["current_ems_id"])
        managed = await self.managed_tokens(eid)
        if assessment["class"] not in {"MAJOR", "STRUCTURAL_SHOCK"} and not set(a["token_ids"]) <= managed:
            raise ValueError("Market escalation requires MAJOR+ or existing call/shown-token management")
        maps = await self.rows("evt.beneficiary_maps", eid)
        allowed = set(
            max(maps, key=lambda r: int(r["map_version"]))["analysis_set"]
            if maps
            else event["tokens_mentioned"]
        )
        allowed |= await self.managed_tokens(eid)
        if not set(a["token_ids"]) <= allowed:
            raise ValueError("token_ids outside analysis set")
        cutoff = iso(a["cutoff"]) if a.get("cutoff") else iso(utcnow().replace(second=0, microsecond=0))
        cutoff_stamp = chosen_boundary(
            cutoff,
            precision=a.get("cutoff_prec", "EXACT" if a.get("cutoff") else "BUCKET(1m)"),
            bucket_size=(a["cutoff_bucket_size_s"], "s") if a.get("cutoff_bucket_size_s") else None,
        )
        cutoff_minute = iso(parse_time(cutoff).replace(second=0, microsecond=0))
        async with self.repo.transaction():
            jobs = []
            for token in a["token_ids"]:
                escalation = await self.repo.enqueue(
                    "ESCALATE_TOKENS",
                    {"event_id": eid, "token_id": token, "tokens": [token]},
                    f"{token}:{eid}",
                    priority=0,
                    max_attempts=3,
                )
                jobs.append(
                    await self.repo.enqueue(
                        "RUN_MARKET_TRUTH",
                        {
                            "event_id": eid,
                            "token_id": token,
                            **cutoff_stamp.fields("cutoff"),
                            "_depends_on": [escalation["job_id"]],
                        },
                        f"{eid}:{token}:{refresh_key or cutoff_minute}",
                        priority=1,
                        max_attempts=2,
                        expires_at=offset_schedule(collection_clock(), timedelta(minutes=30)),
                    )
                )
        truths = []
        for job in jobs:
            deadline = asyncio.get_running_loop().time() + 90 * 15
            while True:
                current = await self.require("ops.jobs", job["job_id"])
                if current["status"] == "DONE":
                    result = current.get("result")
                    if result is None:
                        raise ValueError("Market truth job completed without persisted result")
                    reference = MarketTruthAssessmentRef.from_record(result)
                    if reference.event_id != eid or reference.token_id != job["payload"]["token_id"]:
                        raise ValueError("Market truth job reference does not match the requested assessment")
                    stored_truth = await self.require("ana.market_truth", reference.mta_id)
                    if MarketTruthAssessmentRef.from_record(stored_truth) != reference:
                        raise ValueError("Market truth job reference does not match its persisted assessment")
                    truths.append(stored_truth)
                    break
                if current["status"] in {"FAILED", "EXPIRED"}:
                    raise ValueError(
                        "Market truth job is terminally failed or expired; inspect its durable job record"
                    )
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError("Market truth jobs are pending; retry this call with the same call_id")
                await asyncio.sleep(0.25)
        if maps:
            mapping = max(maps, key=lambda r: int(r["map_version"]))
            edges = [
                e
                for e in await self.rows("evt.beneficiary_edges", eid)
                if int(e["map_version"]) == int(mapping["map_version"])
            ]
            ranks = {"NEGATIVE": 0, "UNPRICED": 1, "EARLY": 2, "SUBSTANTIAL": 3, "HEAVY": 4}
            candidates = []

            latest = {truth["token_id"]: truth for truth in truths}
            for edge in edges:
                token = edge.get("token_id")
                if edge.get("credible") and token and token not in latest:
                    stored = await self.latest_record("ana.market_truth", eid, token)
                    if stored:
                        latest[token] = stored
            for edge in edges:
                mt = latest.get(edge.get("token_id"))
                if edge.get("credible") and mt and mt.get("liquidity", {}).get("liquidity_state") == "NORMAL":
                    relative = mt.get("relative", {})
                    band = relative.get("repricing_band")
                    pct = (relative.get("windows", {}).get("EVENT") or {}).get("pctile")
                    if band in ranks and pct is not None:
                        candidates.append((ranks[band], float(pct), -edge["exposure_score"], edge["edge_id"]))
            mapping["least_repriced_edge"] = min(candidates)[-1] if candidates else None
            mapping["least_repriced_reason"] = (
                "Lowest repricing band, event percentile, then highest exposure among credible NORMAL-liquidity beneficiaries"
                if candidates
                else "No credible beneficiary has complete repricing and NORMAL liquidity evidence"
            )
            await self.repo.put("evt.beneficiary_maps", f"{eid}:{mapping['map_version']}", mapping)
        return {
            "market_truth": truths,
            **({"cutoff_prec_normalized": normalized_prec} if normalized_prec else {}),
            **cutoff_stamp.fields("requested_cutoff"),
            **chosen_boundary(cutoff_minute, precision="BUCKET(1m)").fields("cutoff_minute"),
        }

    async def get_decision(self, a, c):
        eid = a["event_id"]
        token = a["token_id"]
        event = await self.require("evt.events", eid)
        event["contradictions"] = [
            row for row in await self.rows("ana.contradictions", eid) if row.get("token_id") in {None, token}
        ]
        assessment = await self.require("evt.ems_assessments", event["current_ems_id"])
        edges = [r for r in await self.rows("evt.beneficiary_edges", eid) if r.get("token_id") == token]
        latest_truth = await self.latest_record("ana.market_truth", eid, token)
        maps = await self.rows("evt.beneficiary_maps", eid)
        current_version = max((int(m["map_version"]) for m in maps), default=None)
        historical_edges = edges
        edges = [e for e in edges if int(e["map_version"]) == current_version]
        management_only = not edges and token in await self.managed_tokens(eid)
        if management_only and historical_edges:
            edges = [max(historical_edges, key=lambda row: int(row["map_version"]))]
        if not edges or latest_truth is None:
            current = max(maps, key=lambda m: int(m["map_version"])) if maps else {}
            raise ValueError(
                f"Decision for {token!r} requires a current-map beneficiary edge and Market Truth "
                f"({'no edge' if not edges else 'no Market Truth'}); use a registry token_id from the "
                f"analysis set: {current.get('analysis_set', [])}"
            )
        calls = [
            r for r in await self.rows("ana.calls", eid) if r["token_id"] == token and r["status"] == "OPEN"
        ]
        conditions = [r for r in await self.rows("ana.watch_conditions", eid) if r["token_id"] == token]
        expired_ids = set(event.get("call_expired_ids", []))
        expired_calls = [
            row
            for row in await self.rows("ana.calls", eid)
            if row["token_id"] == token
            and row.get("closed_by") == "EXPIRED"
            and (not expired_ids or row["call_id"] in expired_ids)
        ]
        accepted_observations = {
            row["obs_id"] for row in await self.rows("evt.claims", eid) if row.get("accepted")
        }
        material_observations = [
            row
            for row in await self.rows("evt.event_observations", eid)
            if row.get("link_type") in {"STATE_UPDATE", "DENIAL", "CONTRADICTION"}
            and row["obs_id"] in accepted_observations
        ]
        latest_expiry = max(
            (parse_time(row["closed_at"]) for row in expired_calls if row.get("closed_at")), default=None
        )
        new_fact_after_expiry = latest_expiry is not None and any(
            parse_time(row["observed_at"]) > latest_expiry for row in material_observations
        )
        if expired_calls and not calls and not new_fact_after_expiry:
            frozen = await self.latest_record("ana.decisions", eid, token)
            if not frozen:
                raise ValueError("Expired call requires its frozen decision")
            result = copy.deepcopy(frozen)
            result.update(
                dec_id=new_id("dec"),
                **collection_clock().fields("cutoff"),
                tracking_ended=True,
                actionable=False,
                conditions=[],
                conditions_shown=[],
                previous_decision_id=frozen["dec_id"],
                expired_call_ids=[row["call_id"] for row in expired_calls],
                **self.provenance(c),
            )
            await self.reconcile_conditions(result, conditions, [], c)
            await self.repo.put("ana.decisions", result["dec_id"], result)
            return result
        result = self.engine.evaluate(
            event,
            assessment,
            max(
                edges,
                key=lambda e: (
                    e.get("role") == "PRIMARY",
                    e.get("credible", False),
                    e.get("exposure_score", 0),
                ),
            ),
            latest_truth,
            open_call=max(calls, key=lambda r: parse_time(r["opened_at"])) if calls else None,
            conditions=[
                condition
                for condition in conditions
                if condition.get("status") == "FIRED"
                and not condition.get("consumed_by")
                and (
                    c.get("_decision_prepared_condition_ids") is None
                    or condition["cond_id"] in c["_decision_prepared_condition_ids"]
                )
            ],
        )
        if management_only:
            result["management_only"] = True
            if not calls:
                result["actionable"] = False
                result["removed_from_analysis"] = True

        result.update(
            dec_id=result.get("dec_id", new_id("dec")),
            event_id=eid,
            token_id=token,
            market_truth_id=latest_truth["mta_id"],
            mta_id=latest_truth["mta_id"],
            trade_reasons=result.get(
                "trade_reasons", [result["trade_rule"]] if result.get("trade_rule") else []
            ),
            contradictions=[
                row["contradiction_id"]
                for row in event.get("contradictions", [])
                if not row.get("resolved_at")
            ],
            **propagate_timestamp(latest_truth, "cutoff").fields("cutoff"),
            **collection_clock().fields("calculated_at"),
            **self.provenance(c),
        )
        relative = latest_truth.get("relative", {})
        anchor = (relative.get("anchor") or {}).get("ref_price")
        entry = relative.get("entry_ref_price", anchor)
        if entry is not None and anchor is not None:
            result.update(
                entry_ref_price=str(entry),
                invalidation_price=str(min(Decimal(str(anchor)), Decimal(str(entry)))),
            )
        await self.reconcile_conditions(result, conditions, calls, c)
        if isinstance(result.get("outlook"), dict):
            outlook = dict(
                result["outlook"],
                out_id=new_id("out"),
                event_id=eid,
                token_id=token,
                **propagate_timestamp(result, "cutoff").fields("cutoff"),
                **self.provenance(c),
            )
            await self.repo.put("ana.outlooks", outlook["out_id"], outlook)
            result["outlook_id"] = outlook["out_id"]
        await self.repo.put("ana.decisions", result["dec_id"], result)
        return result

    async def reconcile_conditions(self, result, existing, calls, context):
        """Keep one durable watch per expression; firings are consumed exactly once."""
        now = utcnow()
        expiry_stamp = offset_schedule(collection_clock(now), timedelta(days=7))
        if calls:
            latest_call = max(calls, key=lambda row: parse_time(row["opened_at"]))
            call_expiry = offset_schedule(propagate_timestamp(latest_call, "opened_at"), timedelta(days=7))
            expiry_stamp = min((expiry_stamp, call_expiry), key=lambda stamp: parse_time(stamp.value))
        expiry = parse_time(expiry_stamp.value)
        for old in existing:
            if (
                old.get("status") == "FIRED"
                and not old.get("consumed_by")
                and (
                    context.get("_decision_prepared_condition_ids") is None
                    or old["cond_id"] in context["_decision_prepared_condition_ids"]
                )
            ):
                old.update(consumed_by=result["dec_id"], **collection_clock(now).fields("consumed_at"))
                await self.repo.put("ana.watch_conditions", old["cond_id"], old)
        reconciled = []
        used = set()
        for proposed in result.get("conditions", []):
            signature = canonical_json({"role": proposed["role"], "expr": proposed["expr"]})

            matches = [
                old
                for old in existing
                if old.get("status") == "ARMED"
                and parse_time(old.get("expires_at", iso(expiry))) > now
                and canonical_json({"role": old["role"], "expr": old["expr"]}) == signature
            ]
            spent = [
                old
                for old in existing
                if old.get("status") == "FIRED"
                and canonical_json({"role": old["role"], "expr": old["expr"]}) == signature
            ]
            if matches:
                condition = max(matches, key=lambda old: parse_time(old.get("created_at", iso(now))))
                if "created_at" not in condition:
                    condition.update(collection_clock(now).fields("created_at"))
                if "expires_at" not in condition:
                    condition.update(expiry_stamp.fields("expires_at"))
                condition["last_shown_by"] = result["dec_id"]
            else:
                due = proposed.get("expr", {}).get("due_at")
                deadline_stamp = expiry_stamp
                if due:
                    milestone_expiry = offset_schedule(
                        propagate_timestamp(proposed.get("expr", {}), "due_at"), timedelta(hours=1)
                    )
                    deadline_stamp = min(
                        (expiry_stamp, milestone_expiry), key=lambda stamp: parse_time(stamp.value)
                    )
                condition = dict(
                    proposed,
                    cond_id=proposed.get("cond_id", new_id("cond")),
                    event_id=result["event_id"],
                    token_id=result["token_id"],
                    created_by=result["dec_id"],
                    **collection_clock(now).fields("created_at"),
                    **deadline_stamp.fields("expires_at"),
                    **self.provenance(context),
                )
                if spent:
                    predecessor = max(spent, key=lambda old: parse_time(old.get("fired_at", iso(now))))
                    condition["rearmed_from"] = predecessor["cond_id"]
            used.add(condition["cond_id"])
            reconciled.append(condition)
            await self.repo.put(
                "ana.watch_conditions",
                condition["cond_id"],
                WatchCondition.model_validate(condition).model_dump(mode="json", exclude_unset=True),
            )
        for old in existing:
            if old["cond_id"] not in used and old.get("status") == "ARMED" and not old.get("agent_defined"):
                old.update(
                    status="CANCELLED",
                    **collection_clock(now).fields("cancelled_at"),
                    superseded_by=result["dec_id"],
                )
                await self.repo.put("ana.watch_conditions", old["cond_id"], old)
        result["conditions"] = reconciled
        result["conditions_shown"] = [condition["cond_id"] for condition in reconciled][:2]
        if isinstance(result.get("outlook"), dict):
            result["outlook"]["conditions"] = {
                role.lower(): [condition["cond_id"] for condition in reconciled if condition["role"] == role]
                for role in ["UPGRADE", "DOWNGRADE"]
            }

    async def set_watch_condition(self, a, c):
        """Arm an agent monitoring rule (role AGENT) or disarm any ARMED rule of the event."""
        eid = a["event_id"]
        await self.require("evt.events", eid)
        now = utcnow()
        if a["action"] == "DISARM":
            condition = await self.repo.get("ana.watch_conditions", a.get("cond_id") or "")
            if condition is None or condition.get("event_id") != eid:
                raise ValueError("cond_id does not name a watch condition of this event")
            if condition.get("status") != "ARMED":
                return {"accepted": False, "reason": "NOT_ARMED", "status": condition.get("status")}
            condition.update(
                status="CANCELLED",
                cancelled_by="AGENT",
                cancel_provenance=self.provenance(c),
                **collection_clock(now).fields("cancelled_at"),
            )
            await self.repo.put(
                "ana.watch_conditions",
                condition["cond_id"],
                WatchCondition.model_validate(condition).model_dump(mode="json", exclude_unset=True),
            )
            return {"accepted": True, "cond_id": condition["cond_id"], "status": "CANCELLED"}
        token = a.get("token_id")
        if not isinstance(token, str):
            raise ValueError("ARM requires token_id, a registry token key")
        registry = self.settings.load_config("registry") if hasattr(self.settings, "load_config") else None
        if registry is not None and token not in {row["token_id"] for row in registry.get("tokens", [])}:
            raise ValueError("ARM token_id is not a registry token key")
        leaves = []
        for leaf in a.get("conditions") or []:
            if leaf["metric"] not in AGENT_WATCH_METRICS:
                raise ValueError(
                    "metric is not evaluated by the watch scheduler; use one of "
                    + ", ".join(sorted(AGENT_WATCH_METRICS))
                )
            if leaf["metric"].startswith(("event.", "milestone.")) and leaf.get("persist_count", 1) > 1:
                raise ValueError(
                    "event.* and milestone.* conditions fire on the first match; omit persist_count"
                )
            if leaf["op"] == "in" and not isinstance(leaf["value"], list):
                raise ValueError("op in requires a list value")
            if leaf["op"] in (">", ">=", "<", "<=") and leaf["metric"] in CATEGORICAL_WATCH_METRICS:
                raise ValueError(f"{leaf['metric']} is categorical; use ==, != or in")
            if leaf["op"] in (">", ">=", "<", "<="):
                try:
                    number = Decimal(str(leaf["value"]))
                except InvalidOperation:
                    number = None
                if isinstance(leaf["value"], bool) or number is None or not number.is_finite():
                    raise ValueError(f"op {leaf['op']} requires a numeric value")
            leaves.append(
                {
                    "metric": leaf["metric"],
                    "window": leaf.get("window", "W1H"),
                    "op": leaf["op"],
                    "value": leaf["value"],
                    "persist": {"count": leaf.get("persist_count", 1), "unit": "consecutive_windows"},
                }
            )
        if not leaves:
            raise ValueError("ARM requires at least one condition")
        market_windows = {
            leaf["window"] for leaf in leaves if not leaf["metric"].startswith(("event.", "milestone."))
        }
        if len(market_windows) > 1:
            raise ValueError("All market conditions of one rule must use the same window")
        expr = leaves[0] if len(leaves) == 1 else {"all": leaves}
        active = [
            row
            for row in await self.rows("ana.watch_conditions", eid, filters={"status": "ARMED"})
            if row.get("agent_defined")
        ]
        signature = canonical_json({"token_id": token, "expr": expr})
        for row in active:
            if canonical_json({"token_id": row["token_id"], "expr": row["expr"]}) == signature:
                return {"accepted": True, "condition": row, "existing": True}
        if len(active) >= MAX_AGENT_WATCHES:
            raise ValueError(
                f"At most {MAX_AGENT_WATCHES} agent rules may be ARMED per event; DISARM one first"
            )
        condition = {
            "cond_id": new_id("CND"),
            "event_id": eid,
            "token_id": token,
            "role": "AGENT",
            "human_text": a.get("text") or "",
            "expr": expr,
            "status": "ARMED",
            "agent_defined": True,
            "created_by": "AGENT",
            **collection_clock(now).fields("created_at"),
            **offset_schedule(
                collection_clock(now), timedelta(hours=int(a.get("expires_in_hours", 72)))
            ).fields("expires_at"),
            **self.provenance(c),
        }
        await self.repo.put(
            "ana.watch_conditions",
            condition["cond_id"],
            WatchCondition.model_validate(condition).model_dump(mode="json", exclude_unset=True),
        )
        return {"accepted": True, "condition": condition}

    async def publish_card(self, a, c):
        from tank.analysis.cards import (
            alert_gate,
            compose_card,
            publication_immediate,
        )
        from tank.telegram_rich import CARD_TEXT_LIMIT, card_rich_message

        eid = a["event_id"]
        event = await self.require("evt.events", eid)
        assessment = await self.require("evt.ems_assessments", event["current_ems_id"])
        publication_state = {
            "contract_version": 1,
            "event_id": eid,
            "kind": a.get("kind"),
            "ems_id": assessment["ems_id"],
            "ems_class": assessment["class"],
            "lifecycle": event["lifecycle"],
            "freshness": event.get("freshness"),
            "certainty": event.get("certainty"),
            "map_version": None,
            "analysis_set": [],
            "decision_ids": [],
            "market_truth_ids": [],
            "previous_alert_id": None,
            "previous_alert_version": None,
        }

        def publication_outcome(result):
            reason = result.get("reason") or ("GENERATED" if result.get("published") else "UNKNOWN")
            suppressed = reason in {
                "TERMINAL_EVENT",
                "FIRST_CARD_ALREADY_GENERATED",
                "BELOW_MAJOR",
                "LOW_CERTAINTY",
                "STALE",
                "INVALIDATED",
                "NO_MATERIAL_CHANGE",
                "STALE_STATE_CHANGE",
            }
            receipt = copy.deepcopy(publication_state)
            receipt.update(
                disposition="PUBLISHED"
                if result.get("published")
                else "SUPPRESSED"
                if suppressed
                else "REFUSED",
                reason=reason,
                alert_id=result.get("alert_id"),
                alert_version=result.get("version"),
            )
            return dict(result, publication_receipt=receipt)

        if event["lifecycle"] in {"CLOSED", "MERGED"}:
            return publication_outcome({"published": False, "reason": "TERMINAL_EVENT"})
        event["thresholds"] = self.settings.load_config("thresholds")
        newest = await self.repo.list_latest("out.alerts", "generated_at", filters={"event_id": eid}, limit=1)
        previous = newest[0] if newest else None
        initial_full = await self.first_alert(eid, "FULL")
        active_maps = await self.rows("evt.beneficiary_maps", eid)
        if active_maps:
            active_map = max(active_maps, key=lambda row: int(row["map_version"]))
            publication_state.update(
                map_version=active_map["map_version"], analysis_set=list(active_map["analysis_set"])
            )
        publication_state.update(
            newest_generated_alert_id=previous.get("alert_id") if previous else None,
            newest_generated_alert_version=previous.get("version") if previous else None,
        )
        active_tokens = (
            set(max(active_maps, key=lambda row: int(row["map_version"]))["analysis_set"])
            if active_maps
            else set(event.get("tokens_mentioned", []))
        )
        active_tokens |= await self.managed_tokens(eid)
        decisions = []
        for token in sorted(active_tokens):
            decision = await self.latest_record("ana.decisions", eid, token)
            if decision:
                decisions.append(decision)
        publication_state.update(
            decision_ids=[row["dec_id"] for row in decisions],
            market_truth_ids=sorted(
                {
                    row.get("market_truth_id") or row.get("mta_id")
                    for row in decisions
                    if row.get("market_truth_id") or row.get("mta_id")
                }
            ),
        )
        event["claims"] = await self.rows("evt.claims", eid)
        event["contradictions"] = await self.rows("ana.contradictions", eid)
        observations = await self.rows("evt.event_observations", eid)
        observations_by_id = {row["obs_id"]: row for row in observations}
        stage_origins = [
            observations_by_id[claim["obs_id"]].get("freshness")
            for claim in event["claims"]
            if claim.get("accepted") is True
            and claim.get("state_claim") == event.get("event_state")
            and claim.get("obs_id") in observations_by_id
        ]
        if stage_origins and all(grade == "STALE" for grade in stage_origins):
            event["publication_stage_freshness"] = "STALE"
        if c.get("trigger_kind") in {None, "NEW_ITEM", "ITEM_ADDED"}:
            trigger_item = c.get("trigger_item_id") or c.get("item_id")
            candidates = [
                row for row in observations if not trigger_item or row.get("item_id") == trigger_item
            ]
            if candidates:
                trigger = max(candidates, key=lambda row: row["obs_id"])
                event["publication_trigger_freshness"] = trigger.get("freshness")
                publication_state.update(
                    trigger_item_id=trigger.get("item_id"), trigger_freshness=trigger.get("freshness")
                )
        last_sent = (
            await self.repo.latest_before(
                "out.deliveries",
                "at",
                iso(utcnow() + timedelta(microseconds=1)),
                filters={"alert_id": previous["alert_id"], "channel": "telegram:alerts", "status": "SENT"},
            )
            if previous
            else None
        )
        comparison = (
            await self.repo.get("out.alerts", f"{last_sent['alert_id']}:{last_sent['version']}")
            if last_sent
            else None
        )
        gate_previous = None
        if comparison or previous:
            baseline = comparison or previous
            publication_state.update(
                previous_alert_id=baseline["alert_id"], previous_alert_version=baseline["version"]
            )
            gate_previous = dict(
                baseline,
                decisions=[await self.require("ana.decisions", ref) for ref in baseline["decisions"]],
                **propagate_timestamp(last_sent or {}, "at").fields("published_at"),
                ems_class=baseline["card_struct"]["assessment"]["class"],
                event_state=baseline["card_struct"]["event"]["event_state"],
                state_verified=baseline["card_struct"]["event"].get("state_verified"),
                negative_marker=baseline["card_struct"]["event"].get("negative_marker"),
            )
        kind = a["kind"]
        if kind == "PARTIAL" and previous:
            return publication_outcome({"published": False, "reason": "FIRST_CARD_ALREADY_GENERATED"})
        completing_partial = kind == "FULL" and previous and previous["kind"] == "PARTIAL"
        pinned_for_gate = initial_full.get("shown_tokens", []) if initial_full else []
        gate_decisions = (
            [row for row in decisions if row["token_id"] in pinned_for_gate]
            if a["kind"] == "UPDATE" and pinned_for_gate
            else decisions
        )
        ok, reason = alert_gate(
            event,
            assessment,
            previous=None if completing_partial else gate_previous,
            decisions=gate_decisions,
        )
        publication_state["gate_reason"] = reason
        rate_limited = not ok and reason == "UPDATE_RATE_LIMIT"
        if not ok and not rate_limited:
            if event["lifecycle"] == "ANALYZING" and reason in {
                "STALE",
                "STALE_STATE_CHANGE",
                "NO_MATERIAL_CHANGE",
            }:
                stored = await self.require("evt.events", eid)
                stored.update(lifecycle="MONITORING", **collection_clock().fields("monitoring_at"))
                await self.repo.put("evt.events", eid, stored)
                publication_state["lifecycle"] = stored["lifecycle"]
            return publication_outcome({"published": False, "reason": reason})
        immediate = publication_immediate(event, assessment, previous=gate_previous, decisions=gate_decisions)
        delivery_stamp = collection_clock()
        if rate_limited:
            delivery_stamp = offset_schedule(
                propagate_timestamp(last_sent, "at"),
                timedelta(seconds=event["thresholds"].get("update_rate_limit_s", 900)),
            )
        delivery_not_before = delivery_stamp.value
        kind = a["kind"]
        if kind not in {"FULL", "PARTIAL", "UPDATE"}:
            raise ValueError("Invalid alert kind")
        if kind == "UPDATE" and not previous:
            raise ValueError("UPDATE requires a published original")
        if kind == "PARTIAL" and assessment["class"] != "STRUCTURAL_SHOCK":
            raise ValueError("PARTIAL requires STRUCTURAL_SHOCK")

        fetched_edges = await self.rows("evt.beneficiary_edges", eid)
        beneficiaries = list(fetched_edges)
        current_maps = active_maps
        if current_maps:
            event["beneficiary_map"] = max(current_maps, key=lambda row: int(row["map_version"]))
        map_version = max((int(m["map_version"]) for m in current_maps), default=None)
        beneficiaries = [e for e in beneficiaries if int(e["map_version"]) == map_version]
        event["beneficiary_edges"] = copy.deepcopy(beneficiaries)
        analysis_tokens = set(event.get("beneficiary_map", {}).get("analysis_set", []))
        decision_tokens = {row["token_id"] for row in decisions}
        beneficiaries = [
            edge for edge in beneficiaries if edge.get("token_id") in analysis_tokens & decision_tokens
        ]
        maps = active_maps
        least = max(maps, key=lambda r: int(r["map_version"])).get("least_repriced_edge") if maps else None
        all_edges = fetched_edges
        pinned = initial_full.get("shown_tokens", []) if initial_full else []
        if kind == "UPDATE" and pinned:
            beneficiaries = []
            for token in pinned:
                candidates = [edge for edge in all_edges if edge.get("token_id") == token]
                if candidates:
                    beneficiaries.append(max(candidates, key=lambda row: int(row["map_version"])))
        elif kind == "PARTIAL":
            beneficiaries = []
        else:
            primaries = sorted(
                [edge for edge in beneficiaries if edge["role"] == "PRIMARY" and edge.get("tradable")],
                key=lambda row: -row.get("exposure_score", 0),
            )
            secondaries = sorted(
                [edge for edge in beneficiaries if edge["role"] == "SECONDARY" and edge.get("credible")],
                key=lambda row: (row["edge_id"] != least, -row.get("exposure_score", 0)),
            )
            remaining = sorted(
                [
                    edge
                    for edge in beneficiaries
                    if edge["role"] in {"SECONDARY", "LOSER"}
                    and edge.get("tradable")
                    and (edge["role"] == "LOSER" or edge.get("credible"))
                ],
                key=lambda row: -row.get("exposure_score", 0),
            )
            selected = primaries[:1] + secondaries[:1]
            used = {edge.get("token_id") for edge in selected}
            selected += next(([edge] for edge in remaining if edge.get("token_id") not in used), [])
            beneficiaries = selected[:3]
        truth_by_token = {}
        for token in sorted(active_tokens):
            decision = next((row for row in decisions if row["token_id"] == token), None)
            truth = (
                await self.repo.get("ana.market_truth", decision["market_truth_id"])
                if decision and decision.get("market_truth_id")
                else await self.latest_record("ana.market_truth", eid, token)
            )
            if truth:
                truth_by_token[token] = truth
        truths = list(truth_by_token.values())
        if kind == "FULL":
            required = (
                set(max(current_maps, key=lambda row: int(row["map_version"]))["analysis_set"])
                if current_maps
                else set()
            )
            decision_tokens = {decision["token_id"] for decision in decisions}
            missing_truth = required - set(truth_by_token)
            missing_decision = required - decision_tokens
            if not current_maps or missing_truth or missing_decision:
                return publication_outcome(
                    {
                        "published": False,
                        "reason": "ANALYSIS_INCOMPLETE",
                        "missing_market_truth": sorted(missing_truth),
                        "missing_decisions": sorted(missing_decision),
                    }
                )

        truth_hashes = await asyncio.to_thread(lambda: [sha256(truth) for truth in truths])
        ctx = {
            "event": event,
            "assessment": assessment,
            "beneficiaries": beneficiaries,
            "decisions": decisions,
            "market_truths": [
                {"mta_id": truth.get("mta_id"), "token_id": truth.get("token_id"), "sha256": digest}
                for truth, digest in zip(truths, truth_hashes, strict=True)
            ],
        }
        what = a.get("what", "")
        if what:
            ctx["what"] = what
        verdict = a.get("verdict", "")
        why = a.get("why", [])
        if kind != "PARTIAL" and len(why) not in {2, 3}:
            raise ValueError("FULL/UPDATE require 2–3 WHY bullets")
        evidence_labels = {}
        source_items_read = {}
        for claim in event.get("claims", []):
            for evidence in claim.get("evidence", []):
                item_id = evidence.get("item_id")
                if not item_id or item_id in evidence_labels:
                    continue
                item = await self.repo.get("ingest.source_items", item_id)
                source_items_read[item_id] = item
                if item:
                    source_id = item.get("source_id")
                    source = await self.repo.get("reg.sources", source_id) if source_id else None
                    evidence_labels[item_id] = (source or {}).get("name") or source_id or "source"
        event["evidence_labels"] = evidence_labels

        event["token_labels"] = {
            row["token_id"]: str(row.get("project") or row.get("name") or row["token_id"])
            + " ("
            + str(row.get("symbol") or row["token_id"].rpartition(":")[2])
            + ")"
            for row in self.settings.load_config("registry").get("tokens", [])
        }
        text = compose_card(
            event,
            assessment,
            beneficiaries,
            decisions,
            truths,
            kind=kind,
            what=what,
            verdict=verdict,
            why=why,
        )
        rich_message = card_rich_message(
            text, assessment, max_chars=event["thresholds"].get("card_max_chars", CARD_TEXT_LIMIT)
        )
        previous_failed = previous and bool(
            await self.repo.list_latest(
                "out.deliveries",
                "at",
                filters={
                    "alert_id": previous["alert_id"],
                    "version": previous["version"],
                    "status": "FAILED",
                    "terminal": True,
                },
                limit=1,
            )
        )
        if (
            previous
            and last_sent
            and not previous_failed
            and previous["version"] > last_sent["version"]
            and previous["card_text"] == text
            and previous["card_struct"] == ctx
        ):
            return publication_outcome(
                {
                    "published": True,
                    "alert_id": previous["alert_id"],
                    "version": previous["version"],
                    "reason": "IDENTICAL_PENDING",
                    "delivery_not_before": previous.get("delivery_not_before"),
                }
            )

        delivery_not_before = delivery_stamp.value
        cutoff = clock_iso()

        claims = list(event["claims"])
        observations = list(observations)
        ids = {o["item_id"] for o in observations} | {e["item_id"] for r in claims for e in r["evidence"]}
        bundle = {
            **collection_clock(cutoff).fields("cutoff"),
            "event": event,
            "source_items": [
                source_items_read.get(i) or await self.require("ingest.source_items", i) for i in sorted(ids)
            ],
            "observations": observations,
            "claims": claims,
            "state_changes": await self.rows("evt.state_changes", eid),
            "ems": assessment,
            "capture_chains": await self.rows("evt.capture_chains", eid),
            "beneficiary_map": list(active_maps),
            "beneficiary_edges": all_edges,
            "market_truth": truths,
            "outlooks": [
                (await self.require("ana.outlooks", decision["outlook_id"]))
                if decision.get("outlook_id")
                else decision["outlook"]
                for decision in decisions
                if decision.get("outlook_id") or isinstance(decision.get("outlook"), dict)
            ],
            "decisions": decisions,
            "contradictions": list(event["contradictions"]),
            "data_confidence": [m["data_confidence"] for m in truths],
            "unresolved_fields": [],
            "config_hash": self.versions.config_version,
            "analysis_inputs_hash": sha256(
                {
                    name: self.settings.load_config(name)
                    for name in ["registry", "thresholds", "semantics", "dependency_map"]
                }
            ),
            "code_version": self.versions.code_version,
            "model_ids": [],
            "rich_message": rich_message,
            "producing_session_provenance": self.provenance(c),
            "agent_version": c.get("agent_version"),
            "skill_version": c.get("skill_version"),
            "session_id": c.get("session_id"),
            "tool_call_ids": [c["call_id"]] if c.get("call_id") else [],
            "request_log": [],
        }
        if what:
            bundle["what"] = what
        calls_by_key = {}
        if c.get("session_id"):
            for row in await self.rows("ops.tool_calls", filters={"session_id": c["session_id"]}):
                calls_by_key[(row.get("session_id"), row.get("call_id"))] = row

        async for meta in self.repo.iter_metadata(
            "ops.tool_calls", ["session_id", "call_id"], filters={"event_id": eid}
        ):
            key = (meta.get("session_id"), meta.get("call_id"))
            if key in calls_by_key:
                continue
            for row in await self.rows("ops.tool_calls", filters={"event_id": eid, "call_id": key[1]}):
                calls_by_key[(row.get("session_id"), row.get("call_id"))] = row
        calls = list(calls_by_key.values())
        producing_records = [
            event,
            assessment,
            *claims,
            *bundle["source_items"],
            *bundle["observations"],
            *bundle["state_changes"],
            *bundle["capture_chains"],
            *bundle["beneficiary_map"],
            *bundle["beneficiary_edges"],
            *bundle["market_truth"],
            *bundle["outlooks"],
            *bundle["decisions"],
        ]
        producing_sessions = {
            record.get("session_id") for record in producing_records if record.get("session_id")
        }
        if c.get("session_id"):
            producing_sessions.add(c["session_id"])
        sessions = [
            row
            for sid in sorted(producing_sessions)
            if (row := await self.repo.get("ops.agent_sessions", sid)) is not None
        ]
        producers = producing_records + sessions + [c]
        bundle["model_ids"] = sorted({row["model_id"] for row in producers if row.get("model_id")})
        bundle["prompt_versions"] = sorted(
            {row["prompt_version"] for row in producers if row.get("prompt_version")}
        )
        bundle["agent_versions"] = sorted(
            {row["agent_version"] for row in producers if row.get("agent_version")}
        )
        bundle["config_versions"] = sorted(
            {row["config_version"] for row in producers if row.get("config_version")}
        )
        bundle["code_versions"] = sorted(
            {row["code_version"] for row in producers if row.get("code_version")}
        )
        if bundle["agent_version"] is None and len(bundle["agent_versions"]) == 1:
            bundle["agent_version"] = bundle["agent_versions"][0]
        if not bundle["model_ids"]:
            bundle["unresolved_fields"].append(
                {"field": "model_ids", "reason": "No producing record or session declares a model"}
            )
        if bundle["skill_version"] is None:
            bundle["unresolved_fields"].append(
                {
                    "field": "skill_version",
                    "reason": "Producing session did not expose a deployed skill version",
                }
            )
        if bundle["agent_version"] is None:
            bundle["unresolved_fields"].append(
                {
                    "field": "agent_version",
                    "reason": "Producing session did not expose a deployed agent version",
                }
            )
        bundle["request_log"] = await asyncio.to_thread(dedupe_request_log, calls, truth_hashes)
        bundle["tool_call_ids"] = sorted(
            set([r["call_id"] for r in calls if r.get("call_id")] + bundle["tool_call_ids"])
        )

        digest, bundle_path = await asyncio.to_thread(write_evidence_bundle, bundle, self.settings.data_dir)
        await self.repo.put(
            "out.evidence_bundles",
            digest,
            {"bundle_id": "EVB-" + digest, "sha256": digest, "path": str(bundle_path), "payload": bundle},
            immutable=True,
        )
        aid = previous["alert_id"] if previous else new_id("alert")
        version = previous["version"] + 1 if previous else 1

        generated = collection_clock()
        if not rate_limited:
            delivery_stamp = generated
            delivery_not_before = delivery_stamp.value
        alert = dict(
            alert_id=aid,
            version=version,
            kind=kind,
            **delivery_stamp.fields("delivery_not_before"),
            delivery_immediate=immediate,
            shown_tokens=[edge["token_id"] for edge in beneficiaries],
            expired_call_ids=event.get("call_expired_ids", []) if event.get("call_expired") else [],
            milestone_resolution_id=event.get("milestone_resolution_id")
            if event.get("milestone_resolved")
            else None,
            terminal_correction=kind == "UPDATE"
            and event["lifecycle"] == "INVALIDATED"
            and any(r["type"] == "FATAL" for r in event["contradictions"]),
            event_id=eid,
            **generated.fields("generated_at"),
            card_text=text,
            rich_message=rich_message,
            card_struct=ctx,
            decisions=[
                d["dec_id"]
                for d in decisions
                if d["token_id"] in {edge["token_id"] for edge in beneficiaries}
            ],
            evidence_bundle_id=digest,
            supersedes_version=version - 1 if previous else None,
            **self.provenance(c),
        )
        await self.repo.put("out.alerts", f"{aid}:{version}", alert, immutable=True)
        delivery = new_id("delivery")
        await self.repo.put(
            "out.deliveries",
            delivery,
            {
                "delivery_id": delivery,
                "alert_id": aid,
                "version": version,
                "channel": "telegram:alerts",
                "status": "GENERATED",
                "attempt": 0,
                **generated.fields("at"),
            },
            immutable=True,
        )
        await self.repo.enqueue(
            "DELIVER",
            {"alert_id": aid, "version": version, "channel": "telegram:alerts"},
            f"{aid}:{version}:telegram:alerts",
            priority=0,
            run_after=delivery_stamp,
            expires_at=offset_schedule(delivery_stamp, timedelta(days=1)),
            max_attempts=5,
        )
        return publication_outcome(
            {
                "published": True,
                "alert_id": aid,
                "version": version,
                "evidence_bundle_id": digest,
                "delivery_not_before": delivery_not_before,
                "delivery_status": "QUEUED",
                "reason": "QUEUED_RATE_LIMIT" if rate_limited else "GENERATED",
            }
        )

    async def escalate_source(self, source_id, event_id, trigger, starts_at, expires_at, context):
        key = f"{source_id}:{event_id}:{trigger}"
        row = dict(
            escalation_id=key,
            source_id=source_id,
            event_id=event_id,
            trigger=trigger,
            status="ACTIVE",
            **(starts_at if isinstance(starts_at, Timestamp) else collection_clock(starts_at)).fields(
                "starts_at"
            ),
            **(expires_at if isinstance(expires_at, Timestamp) else collection_clock(expires_at)).fields(
                "expires_at"
            ),
            interval_s=15,
            **self.provenance(context),
        )
        return await self.repo.put("ops.source_escalations", key, row)

    async def registered_source(self, source_id):
        return isinstance(source_id, str) and (
            await self.repo.get("reg.sources", source_id) is not None
            or any(row["source_id"] == source_id for row in self.settings.load_config("sources")["sources"])
        )

    async def schedule_milestone(self, a, c):
        event = await self.require("evt.events", a["event_id"])
        due_stamp = propagate_timestamp(a, "due_at", missing_reason="CALLER_DID_NOT_DECLARE_DUE_PRECISION")
        due = due_stamp.value
        source_id = a["source_id"]
        if not await self.registered_source(source_id):
            raise ValueError("Milestone source_id must be a registered source")
        event["next_milestone"] = {
            "kind": a["milestone"],
            **due_stamp.fields("due_at"),
            "source_id": source_id,
        }
        await self.repo.put("evt.events", event["event_id"], event)
        job = await self.repo.enqueue(
            "MILESTONE_CHECK",
            {
                "event_id": event["event_id"],
                "milestone": a["milestone"],
                **due_stamp.fields("due_at"),
                "source_id": source_id,
            },
            f"{event['event_id']}:{a['milestone']}:{due}",
            run_after=due_stamp,
            expires_at=offset_schedule(
                max((due_stamp, collection_clock()), key=lambda stamp: parse_time(stamp.value)),
                timedelta(hours=2),
            ),
        )
        await self.escalate_source(
            source_id,
            event["event_id"],
            "MILESTONE:" + a["milestone"] + ":" + due,
            offset_schedule(due_stamp, -timedelta(minutes=5)),
            offset_schedule(due_stamp, timedelta(hours=2)),
            c,
        )
        return {"job_id": job["job_id"]}

    async def record_review(self, a, c):
        if c.get("role") != "scout" or c.get("proposal_id") != a["proposal_id"]:
            raise ValueError("Scout review requires its proposal session")
        row = await self.require("scout.expansion_proposals", a["proposal_id"])
        scores = a.get("scores", {})
        if set(scores) != {"overlap", "observability", "reusability", "decision_value"} or any(
            isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 10
            for value in scores.values()
        ):
            raise ValueError("Scout review requires four numeric scores 0..10")
        if a.get("recommendation") not in {"SUBCATEGORY", "TEST_MORE", "NEW_CATEGORY", "REJECT"}:
            raise ValueError("Invalid scout recommendation")
        row["recommendation"] = a["recommendation"]
        duplicate = a.get("duplicate_of")
        if duplicate:
            if row.get("duplicate_of") not in {None, duplicate}:
                raise ValueError("Duplicate target is already recorded")
            if duplicate == row["prp_id"]:
                raise ValueError("Proposal cannot duplicate itself")
            target = await self.require("scout.expansion_proposals", duplicate)
            if target.get("human_action"):
                raise ValueError("Duplicate target must be open")
            if not row.get("duplicate_of"):
                target["occurrences"] += row["occurrences"]
                target["evidence"] = list(dict.fromkeys(target["evidence"] + row["evidence"]))
                await self.repo.put("scout.expansion_proposals", duplicate, target)
            row["duplicate_of"] = duplicate
        row["adversarial_review"] = {
            **collection_clock().fields("reviewed_at"),
            "scores": scores,
            "recommendation": a["recommendation"],
            "duplicate_of": duplicate,
            "verdict": a["verdict"],
            "reason": a["reason"],
            "evidence": evidence_ids(a["evidence"]),
            **self.provenance(c),
        }
        await self.repo.put("scout.expansion_proposals", row["prp_id"], row)
        return row

    async def propose_pattern(self, a, c):
        subject = a["subject"]
        name = subject if isinstance(subject, str) else subject["concept_name"]
        async with self.repo.lock("scout:" + sha256(name.casefold())):
            return await self._propose_pattern(a, c)

    async def _propose_pattern(self, a, c):
        if a["hook"] not in {
            "BENEFICIARY_LAYER",
            "MARKET_TRUTH_STATE",
            "SOURCE_OPPORTUNITY",
            "ALTERNATIVE_EXPLANATION",
        }:
            raise ValueError("Unknown scout hook")
        if not a.get("evidence"):
            raise ValueError("Scout evidence required")
        subject = a["subject"]
        subject = {"concept_name": subject} if isinstance(subject, str) else subject
        existing = next(
            (
                p
                for p in await self.rows("scout.expansion_proposals")
                if p["concept_name"].casefold() == subject["concept_name"].casefold()
                and not p.get("human_action")
            ),
            None,
        )
        if existing and existing.get("duplicate_of"):
            existing = await self.require("scout.expansion_proposals", existing["duplicate_of"])
        row = existing or dict(
            prp_id=new_id("prp"),
            hook=a["hook"],
            **subject,
            occurrences=0,
            evidence=[],
            recommendation="SUBCATEGORY",
            confidence="LOW",
            **collection_clock().fields("created_at"),
            **self.provenance(c),
        )
        row["occurrences"] += 1
        row["evidence"] += evidence_ids(a["evidence"])
        await self.repo.put("scout.expansion_proposals", row["prp_id"], row)
        await self.repo.enqueue(
            "SCOUT_REVIEW",
            {"proposal_id": row["prp_id"]},
            f"{row['prp_id']}:{row['occurrences']}",
            priority=4,
        )
        return row


class _borrow:
    def __init__(self, client):
        self.client = client

    async def __aenter__(self):
        return self.client

    async def __aexit__(self, *args):
        return False


def evidence_ids(evidence):
    """The stored form of pattern evidence: the cited claim, event and source document IDs."""
    return [
        entry[key]
        for entry in evidence
        for key in ("claim_id", "event_id", "item_id")
        if isinstance(entry, dict) and key in entry
    ]
