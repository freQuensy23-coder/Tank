"""Live cycle of conditions, held updates and Telegram chains (tank-events).

Runs the real Scheduler loop, the real Runtime job worker, the real
EventToolService (tools, alert gate, cards) and the real DeliveryService on a
test PostgreSQL database with the real clock and the real 15-minute hold.
Only three things are stand-ins: Telegram (a local transport that records each
sendMessage and never reaches the network), the agent (a script that follows
the 03 §3.2 playbook through the same tool calls) and market data (scenario
values given to MarketTruth's interface).

Scenario (03 §6.6, §7.2, §7.4; 01 FR-A-04):
  1. NEW_ITEM: record_event with tokens ["WLD"] (V3 must store world:WLD),
     set_assessment MAJOR, set_beneficiaries, get_market_truth, get_decision,
     publish_card FULL. Data Confidence LOW -> WAIT (A1). Card sent.
  2. Data Confidence turns HIGH. The scheduler fires the WAIT(data) upgrade,
     CONDITION_FIRED -> get_decision START -> UPDATE sent at once (immediate),
     reply to the card; the call opens; the fired watch is re-armed (W1).
  3. ITEM_ADDED of STALE old news advancing the stage -> publish refused,
     nothing sent.
  4. MILESTONE due -> resolved -> UPDATE accepted but held by the 15-min limit.
  5. ITEM_ADDED official denial -> FATAL -> EXIT UPDATE sent at once, the call
     closes. The held milestone version is older and must never be sent (W2).
  6. Wait past the held version's slot; it is dropped.

Usage: TANK_TEST_DATABASE_URL=... python live_cycle.py OUTDIR [--memory --hold 60]
"""

import argparse
import asyncio
import json
import os
import sys
import tempfile
import time
from datetime import timedelta
from pathlib import Path

import httpx

from tank.analysis.decisions import DecisionEngine
from tank.analysis.truth import compute_market_truth
from tank.delivery import DeliveryService
from tank.event_tools import LAYERS, EventToolService
from tank.runtime import Runtime
from tank.scheduler import Scheduler
from tank.settings import Settings
from tank.storage import MemoryRepository, PostgresRepository
from tank.util import iso, new_id, sha256, utcnow

TOKEN = "world:WLD"
LOG = []


def log(kind, **data):
    row = {"t": iso(), "kind": kind, **data}
    LOG.append(row)
    print(json.dumps(row, default=str), flush=True)


class FakeTelegram:
    """Records sendMessage bodies; answers like the Bot API. No network."""

    def __init__(self):
        self.sent = []

    def __call__(self, request):
        body = json.loads(request.content)
        message_id = 1000 + len(self.sent) + 1
        reply = (body.get("reply_parameters") or {}).get("message_id")
        self.sent.append(
            {
                "at": iso(),
                "message_id": message_id,
                "chat": body["chat_id"],
                "reply_to": reply,
                "text": body["text"],
            }
        )
        log(
            "telegram.sendMessage",
            message_id=message_id,
            chat=body["chat_id"],
            reply_to=reply,
            first_line=body["text"].splitlines()[0][:120],
        )
        result = {"message_id": message_id}
        if reply:
            result["reply_to_message"] = {"message_id": reply}
        return httpx.Response(200, json={"ok": True, "result": result})


class ScenarioTruth:
    """MarketTruth interface with scenario values (no market collectors)."""

    def __init__(self, repo):
        self.repo = repo
        self.data_class = "LOW"

    def values(self, event_id, token_id, cutoff):
        stamp = cutoff.value if hasattr(cutoff, "value") else (cutoff or iso())
        row = compute_market_truth({"token_id": token_id, "event_id": event_id, "cutoff": stamp})
        row.pop("measurement_metadata", None)
        row.update(
            mta_id=new_id("mta"),
            move_class="SPOT_LED_ACCUMULATION",
            data_confidence={
                "class": self.data_class,
                "limiting": {}
                if self.data_class == "HIGH"
                else {"condition": "spot_coverage", "reason": "SCENARIO"},
            },
            relative={
                "repricing_band": "EARLY",
                "market_context": "NEUTRAL",
                "anchor": {"ref_price": "10"},
                "entry_ref_price": "10.4",
                "close_1h_price": "10.5",
                "windows": {"EVENT": {"r_raw": "0.04", "r_btc_adj": "0.03", "pctile": 0.4}},
            },
            liquidity={"liquidity_state": "NORMAL"},
            config_version="live-cycle",
            code_version="live-cycle",
        )
        return row

    async def condition_values(self, event_id, token_id, cutoff, requirements):
        return self.values(event_id, token_id, cutoff)

    async def assess(self, event_id, token_id, cutoff=None):
        row = self.values(event_id, token_id, cutoff)
        await self.repo.put("ana.market_truth", row["mta_id"], row)
        return row


class Idle:
    async def run(self, stop):
        await stop.wait()

    async def close(self):
        pass


class StubMarket(Idle):
    async def escalate(self, event_id, tokens):
        pass

    async def capture_readiness(self, event_id, token_id, anchor=None):
        return {"status": "COMPLETE"}

    async def wait_capture(self, *a, **k):
        pass


class StubGateway:
    async def preflight(self):
        pass

    async def start(self):
        pass

    async def close(self):
        pass


class StubPipeline:
    async def filter_item(self, payload):
        raise RuntimeError("not used")

    route_item = filter_item


class ScriptedAgent(Idle):
    """Follows the 03 §3.2 playbook with Tank's tools; prose is fixed text."""

    def __init__(self, repo, tools, work):
        self.repo, self.tools, self.work = repo, tools, work
        self.receipts = []

    async def call(self, tool, args, context):
        # The scripted stand-in declares itself as the producing model (provenance).
        context = {
            "model_id": "scripted-agent-live-cycle",
            "prompt_version": "tank-event-analyst baseline-2.5",
            "role": "system",
            **context,
        }
        result = await self.tools.dispatch(tool, args, context)
        if tool == "publish_card":
            receipt = {
                k: result.get(k)
                for k in ("published", "reason", "alert_id", "version", "delivery_not_before")
            }
            self.receipts.append({"trigger": context.get("trigger_kind"), **receipt})
            log("publish_card", trigger=context.get("trigger_kind"), **receipt)
        elif tool == "get_decision":
            log(
                "get_decision",
                trigger=context.get("trigger_kind"),
                action=result["action"],
                rule=result["action_rule"],
                tactical=result.get("tactical"),
                conditions=[
                    {
                        "role": c["role"],
                        "status": c.get("status"),
                        "rearmed_from": c.get("rearmed_from"),
                        "text": c.get("human_text"),
                    }
                    for c in result.get("conditions", [])
                ],
            )
        return result

    async def item(self, text, source_id="S01", public_at=None):
        item_id = new_id("item")
        now = iso()
        raw = self.work / (item_id + ".txt")
        raw.write_text(text)
        await self.repo.put(
            "ingest.source_items",
            item_id,
            {
                "item_id": item_id,
                "source_id": source_id,
                "feed": "live-cycle:official",
                "external_id": item_id,
                "published_at": public_at or now,
                "published_prec": "EXACT",
                "observed_at": now,
                "raw_ref": str(raw),
                "raw_sha256": sha256(text),
                "content_hash": sha256(text),
                "source_class": "PRIMARY",
                "text": text,
                "title": text,
                "extraction_status": "PENDING",
                "relevance": {"decision": "ALWAYS"},
                "public_at": public_at or now,
            },
        )
        return item_id

    @staticmethod
    def public(value):
        return {"value": value, "precision": "EXACT", "basis": "Source publication time", "quote": None}

    def claim(self, quote, state):
        return dict(
            verified=True,
            type="STATE",
            subject="World",
            predicate="ships",
            object="World App payments",
            state_claim=state,
            directionality="BOTH",
            evidence_class="PRIMARY_FACT",
            quote=quote,
        )

    async def new_item(self, milestone_due):
        quote = "World App payments are now live for all users"
        item_id = await self.item(quote)
        ctx = {"trigger_kind": "NEW_ITEM", "trigger_item_id": item_id}
        now = iso()
        recorded = await self.call(
            "record_event",
            dict(
                item_id=item_id,
                is_event=True,
                statement="World launches payments in World App",
                certainty="HIGH",
                certainty_reason="Official World announcement",
                entities=[{"name": "World App", "entity_id": None, "role": "ACTOR"}],
                tokens=["WLD"],
                event_time={"value": now, "precision": "EXACT", "quote": None},
                public_at={"value": now, "precision": "EXACT", "basis": "Official post time", "quote": None},
                freshness="FRESH",
                claims=[self.claim(quote, "LIVE")],
                next_milestone={"kind": "Regional rollout", "due_at": iso(milestone_due), "source_id": "S01"},
                checked_sources=["S01"],
            ),
            ctx,
        )
        eid = recorded["event_id"]
        event = await self.repo.get("evt.events", eid)
        log(
            "record_event",
            event_id=eid,
            tokens_mentioned=event["tokens_mentioned"],
            entities=event["entities"],
            state=event["event_state"],
        )
        proof = recorded["accepted_claims"][0]["claim_id"]
        dims = {
            name: {
                "value": None,
                "reason": "NOT_ESTIMABLE",
                "rationale": "Not estimable",
                "evidence": [],
                "proposed_by": "LLM",
                "caps_applied": [],
            }
            for name in [
                "economic_directness",
                "magnitude",
                "permanence",
                "distribution_expansion",
                "mechanical_flows",
                "surprise",
                "implementation",
            ]
        }
        step = {
            "value": "1000",
            "unit": "USD",
            "window": "P1D",
            "observed_at": now,
            "precision": "DAY",
            "coverage": 1,
            "reason": None,
        }
        unknown = dict(step, value=None, coverage=None, reason="NOT_ESTIMABLE")
        assessed = await self.call(
            "set_assessment",
            {
                "event_id": eid,
                "assessment": {
                    "class": "MAJOR",
                    "criterion": "M3",
                    "criterion_quote": quote,
                    "dims": dims,
                    "impact_horizon": {
                        "impact_start": "NOW",
                        "ramp_shape": "STEP",
                        "dependencies": [],
                        "time_to_materiality": "Now",
                        "timing_confidence": "HIGH",
                    },
                    "capture_chains": [
                        {
                            "token_id": TOKEN,
                            "capture_score": 8,
                            "headline_activity": step,
                            "eligible_activity": step,
                            "effective_fees": step,
                            "protocol_capture": step,
                            "token_flow_annual_usd": unknown,
                            "token_flow_pct_mcap": unknown,
                            "token_mechanism": {"type": "GAS", "active": True, "evidence": [proof]},
                        }
                    ],
                },
            },
            ctx,
        )
        log("set_assessment", final_class=assessed.get("class"), caps=assessed.get("caps"))
        await self.call(
            "set_beneficiaries",
            {
                "event_id": eid,
                "edges": [
                    {
                        "entity_id": "world",
                        "token_id": "WLD",
                        "role": "PRIMARY",
                        "layer": "ISSUER_PRODUCT",
                        "direction": "POSITIVE",
                        "causal_chain": "Payments use WLD",
                        "directionality": "BOTH",
                        "exposure_score": 8,
                        "edge_certainty": "HIGH",
                        "evidence": [proof],
                    }
                ],
                "layer_reasons": {layer: "No supported intermediary" for layer in LAYERS},
                "layer_statuses": {layer: "NONE" for layer in LAYERS},
                "no_credible_secondary_reason": "No adopted intermediary",
            },
            ctx,
        )
        await self.call("get_market_truth", {"event_id": eid, "tokens": [TOKEN]}, ctx)
        await self.call("get_decision", {"event_id": eid, "token": TOKEN}, ctx)
        await self.call(
            "publish_card",
            {
                "event_id": eid,
                "kind": "FULL",
                "verdict": "World launches payments in World App",
                "why": ["Payments are live in World App", "not estimable"],
            },
            ctx,
        )
        return eid

    async def send_message(self, payload):
        kind, eid = payload["kind"], payload["event_id"]
        ctx = {"trigger_kind": kind, "trigger_item_id": payload.get("item_id")}
        log("agent.message", message=kind, condition=(payload.get("condition") or {}).get("human_text"))
        if kind == "ITEM_ADDED":
            item = await self.repo.get("ingest.source_items", payload["item_id"])
            if payload.get("scenario") == "stale":
                await self.call(
                    "add_observation",
                    {
                        "event_id": eid,
                        "item_id": item["item_id"],
                        "link": "STATE_UPDATE",
                        "public_at": self.public(item["public_at"]),
                        "freshness": "STALE",
                        "claims": [self.claim(item["text"], "MONEY_FLOWING")],
                    },
                    ctx,
                )
            else:
                await self.call(
                    "add_observation",
                    {
                        "event_id": eid,
                        "item_id": item["item_id"],
                        "link": "DENIAL",
                        "public_at": self.public(item["public_at"]),
                        "freshness": "FRESH",
                        "claims": [dict(self.claim(item["text"], None), type="FACT", predicate="denies")],
                    },
                    ctx,
                )
        if kind == "MILESTONE":
            text = "World App payments roll out in the region as scheduled"
            item_id = await self.item(text)
            await self.call(
                "add_observation",
                {
                    "event_id": eid,
                    "item_id": item_id,
                    "link": "STATE_UPDATE",
                    "public_at": self.public(iso()),
                    "freshness": "FRESH",
                    "claims": [self.claim(text, "LIVE")],
                    "milestone_outcome": {"outcome": "VERIFIED", "quote": text},
                },
                ctx,
            )
            ctx["trigger_item_id"] = item_id
        await self.call("get_decision", {"event_id": eid, "token": TOKEN}, ctx)
        await self.call(
            "publish_card",
            {
                "event_id": eid,
                "kind": "UPDATE",
                "verdict": "World launches payments in World App",
                "why": ["What changed: " + kind.lower(), "not estimable"],
            },
            ctx,
        )


async def wait_for(predicate, timeout, label):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            log("reached", step=label)
            return value
        await asyncio.sleep(2)
    raise TimeoutError(label)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("out")
    parser.add_argument("--memory", action="store_true")
    parser.add_argument("--hold", type=int, default=None, help="dry runs only: shorter update hold (s)")
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="tank-events-live-"))
    (work / "secrets").mkdir()
    settings = Settings(
        data_dir=work / "data",
        config_dir=Path(__file__).resolve().parents[3] / "config",
        secrets_dir=work / "secrets",
        telegram_token="fake-live-cycle",
        telegram_alerts_chat="-100111",
        telegram_ops_chat="-100222",
        config_version="live-cycle",
        code_version="ws/tank-events",
    )
    if args.hold is not None:
        load = settings.load_config

        def load_config(name):
            data = load(name)
            return dict(data, update_rate_limit_s=args.hold) if name == "thresholds" else data

        object.__setattr__(settings, "load_config", load_config)
    hold = settings.load_config("thresholds").get("update_rate_limit_s", 900)
    log("start", backend="memory" if args.memory else "postgres", hold_s=hold)
    repo = MemoryRepository() if args.memory else PostgresRepository(os.environ["TANK_TEST_DATABASE_URL"])
    await repo.open()
    if not args.memory:
        await repo.migrate()
    telegram = FakeTelegram()
    truth = ScenarioTruth(repo)
    tools = EventToolService(repo, settings, truth, DecisionEngine(settings))
    agent = ScriptedAgent(repo, tools, work)
    http = httpx.AsyncClient(transport=httpx.MockTransport(telegram))
    scheduler = Scheduler(repo, settings, None, truth)
    runtime = Runtime(
        repo,
        settings,
        sources=Idle(),
        market=StubMarket(),
        truth=truth,
        pipeline=StubPipeline(),
        sessions=agent,
        event_tools=tools,
        delivery=DeliveryService(repo, settings, http=http),
        scheduler=scheduler,
        gateway=StubGateway(),
    )
    await repo.recover_jobs()
    stop = asyncio.Event()
    tasks = [asyncio.create_task(scheduler.run(stop))] + [
        asyncio.create_task(runtime.worker(stop, n)) for n in range(4)
    ]
    eid = None

    def alerts_sent():
        return [m for m in telegram.sent if m["chat"] == "-100111"]

    try:
        t0 = utcnow()
        milestone_due = t0 + timedelta(minutes=5)
        eid = await agent.new_item(milestone_due)
        await wait_for(lambda: len(alerts_sent()) >= 1, 120, "FULL card sent")
        await asyncio.sleep(30)
        truth.data_class = "HIGH"
        log("scenario", step="Data Confidence HIGH")
        await wait_for(lambda: len(alerts_sent()) >= 2, 240, "START update sent")
        start_sent = utcnow()
        calls = await repo.list("ana.calls", filters={"event_id": eid})
        log(
            "calls",
            calls=[{k: c.get(k) for k in ("status", "entry_ref_price", "invalidation_price")} for c in calls],
        )
        stale_id = await agent.item(
            "World App payments were live last week", public_at=iso(t0 - timedelta(days=3))
        )
        await repo.enqueue(
            "AGENT_MESSAGE",
            {"kind": "ITEM_ADDED", "event_id": eid, "item_id": stale_id, "scenario": "stale"},
            "stale:" + stale_id,
            priority=1,
        )
        await wait_for(
            lambda: any(r["trigger"] == "ITEM_ADDED" for r in agent.receipts), 120, "STALE update handled"
        )
        await wait_for(
            lambda: any(r["trigger"] == "MILESTONE" for r in agent.receipts), 600, "MILESTONE update handled"
        )
        await asyncio.sleep(60)
        before = len(alerts_sent())
        denial_id = await agent.item("World denies that World App payments launched")
        await repo.enqueue(
            "AGENT_MESSAGE",
            {"kind": "ITEM_ADDED", "event_id": eid, "item_id": denial_id, "scenario": "denial"},
            "denial:" + denial_id,
            priority=1,
        )
        await wait_for(
            lambda: any("EXIT" in m["text"] for m in alerts_sent()[before:]), 240, "EXIT update sent"
        )
        exit_sent = utcnow()
        # Past the milestone version's slot (START + hold) and the slot of any
        # update held after EXIT (EXIT + hold).
        held_until = max(start_sent, exit_sent) + timedelta(seconds=hold + 90)
        log("scenario", step="waiting past the held slot", until=iso(held_until))
        while utcnow() < held_until:
            await asyncio.sleep(5)
    finally:
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        evidence = {"log": LOG, "telegram": telegram.sent, "receipts": agent.receipts}
        if eid:
            evidence["alerts"] = [
                {
                    k: a.get(k)
                    for k in (
                        "alert_id",
                        "version",
                        "kind",
                        "delivery_not_before",
                        "delivery_immediate",
                        "supersedes_version",
                        "card_text",
                    )
                }
                for a in sorted(
                    await repo.list("out.alerts", filters={"event_id": eid}), key=lambda a: a["version"]
                )
            ]
            ids = {a["alert_id"] for a in evidence["alerts"]}
            evidence["deliveries"] = sorted(
                [
                    {
                        k: d.get(k)
                        for k in (
                            "alert_id",
                            "version",
                            "status",
                            "attempt",
                            "at",
                            "message_ref",
                            "request_reply_to_message_ref",
                        )
                    }
                    for i in ids
                    for d in await repo.list("out.deliveries", filters={"alert_id": i})
                ],
                key=lambda d: d["at"],
            )
            evidence["deliver_jobs"] = [
                {k: j.get(k) for k in ("status", "run_after", "payload", "attempts")}
                for j in await repo.list("ops.jobs", filters={"type": "DELIVER"})
            ]
            evidence["watch_conditions"] = [
                {
                    k: c.get(k)
                    for k in (
                        "cond_id",
                        "role",
                        "status",
                        "rearmed_from",
                        "consumed_by",
                        "created_at",
                        "fired_at",
                        "human_text",
                    )
                }
                for c in await repo.list("ana.watch_conditions", filters={"event_id": eid})
            ]
            evidence["calls"] = await repo.list("ana.calls", filters={"event_id": eid})
            evidence["event"] = await repo.get("evt.events", eid)
        (out / "evidence.json").write_text(json.dumps(evidence, indent=2, default=str))
        await http.aclose()
        await repo.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
