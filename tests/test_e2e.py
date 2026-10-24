"""Synthetic offline service integration; no historical acceptance claim."""

import json
from datetime import UTC
from pathlib import Path

import httpx
import pytest
from delivery_support import delivery_job
from score_fixtures import unassessed_score_dimensions
from source_support import source_configuration, source_definition

from tank.agents.pipeline import Pipeline
from tank.agents.sessions import SessionService
from tank.analysis.cards import compose_card
from tank.analysis.decisions import DecisionEngine
from tank.delivery import DeliveryError, DeliveryService
from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.sources import SourceService
from tank.storage import ImmutableRecordError, MemoryRepository
from tank.telegram_inbox import TelegramInbox
from tank.telegram_rich import rich_message_text

pytestmark = [pytest.mark.integration]


def _native_session(session_id, agent_id="synthetic"):
    """Native OpenMA session response with its resolved, versioned agent bytes."""
    return {
        "id": session_id,
        "agent": {
            "id": agent_id,
            "version": "synthetic-agent-v1",
            "system": "Synthetic offline analyst fixture instructions",
            "skills": [{"type": "custom", "skill_id": "synthetic-analyst", "version": "1"}],
            "tools": [],
            "model": {"id": "gpt-6.1-sol"},
        },
    }


def _fixture_api_limits():
    from pathlib import Path

    import yaml

    return yaml.safe_load((Path(__file__).resolve().parents[1] / "config" / "api_limits.yaml").read_text())


def _fixture_thresholds():
    from pathlib import Path

    import yaml

    thresholds = yaml.safe_load(
        (Path(__file__).resolve().parents[1] / "config" / "thresholds.yaml").read_text()
    )
    thresholds.update(filter_cutoff=0.5, routing_cutoff=0.5)
    return thresholds


def _fake_gateway_settings(tmp_path: Path):
    """Explicit fake credentials: never consult operator Codex authentication."""
    import base64

    def encode(value):
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")

    token = (
        encode({"alg": "HS256", "typ": "JWT"})
        + "."
        + encode({"exp": 4102444800, "sub": "FAKE-test-subscription"})
        + ".FAKE-signature"
    )
    path = tmp_path / "FAKE-codex-auth.json"
    path.write_text(
        json.dumps(
            {
                "auth_mode": "chatgpt",
                "tokens": {
                    "access_token": token,
                    "refresh_token": "FAKE-refresh-token",
                    "id_token": token,
                    "account_id": "FAKE-account",
                },
                "last_refresh": "2026-10-17T00:00:00Z",
            }
        )
    )
    path.chmod(0o600)
    return {
        "_env_file": None,
        "openma_api_key": "FAKE-openma-key",
        "codex_auth_path": path,
        "codex_gateway_key": "FAKE-internal-gateway-key",
        "codex_gateway_host": "127.0.0.1",
        "codex_gateway_port": 0,
    }


async def test_source_jev_sse_validated_read_restart_and_delivery(tmp_path: Path) -> None:
    config = {
        "sources": source_configuration({"sources": [{"source_id": "S30", "feeds": []}]}),
        "api_limits": _fixture_api_limits(),
        "exchange_addresses": {"version": "synthetic-fixture", "wallet_lists": []},
        "registry": {"tokens": [{"token_id": "T", "symbol": "T"}], "entities": []},
        "thresholds": _fixture_thresholds(),
        "openma": {"agent_id": "synthetic-agent", "environment_id": "synthetic-offline"},
    }

    class TestSettings(Settings):
        def load_config(self, name):
            result = config[name.removesuffix(".yaml")]
            if name.removesuffix(".yaml") == "openma":
                result = dict(
                    result,
                    prompt_version="synthetic-prompt-v1",
                    agent_version="synthetic-agent-v1",
                    reasoning_effort="medium",
                )
            return result

        def read_config_bytes(self, name):
            import yaml

            if name.removesuffix(".yaml") in config:
                return yaml.safe_dump(self.load_config(name)).encode()
            return super().read_config_bytes(name)

    settings = TestSettings(
        **_fake_gateway_settings(tmp_path),
        data_dir=tmp_path,
        jev_provider="openrouter",
        openrouter_api_key="mock",
        telegram_token="mock",
        telegram_alerts_chat="mock",
    )
    repo = MemoryRepository()
    sent = []
    event_posts = []
    failures = [True]
    tool_event = {"id": "tool1", "type": "agent.custom_tool_use", "name": "get_item", "input": {}}

    def transport(request: httpx.Request):
        path = request.url.path
        if request.url.host == "opencode.ai":
            body = json.loads(request.content)
            key = next(iter(body["questions"]))
            return httpx.Response(
                200,
                json={
                    "model": "jev-1.13-free",
                    "answers": {key: {"type": "noul", "noul": 0.99}},
                    "usage": {"input_tokens": 20},
                },
            )
        if path.endswith("/sessions"):
            return httpx.Response(
                200,
                json={"data": []} if request.method == "GET" else _native_session("s1", "synthetic-agent"),
            )
        if request.method == "GET" and path.endswith("/sessions/s1"):
            return httpx.Response(200, json=_native_session("s1", "synthetic-agent"))
        if path.endswith("/events/stream"):
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                text="id: 1\nevent: message\ndata: " + json.dumps(tool_event) + "\n\n",
            )
        if path.endswith("/events"):
            event_posts.append(json.loads(request.content))
            return httpx.Response(200, json={})
        if request.url.host == "api.telegram.org":
            sent.append(json.loads(request.content))
            if failures.pop() if failures else False:
                return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 1}})
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 100 + len(sent)}})
        raise AssertionError(str(request.url))

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
        source = SourceService(repo, settings, http=http)
        item_id, seen = "item-e2e-one", "2026-10-17T00:00:01Z"
        raw, sha = source.raw(
            b"Synthetic integration item", source_id="S30", item_id=item_id, observed_at=seen
        )
        item = await source.ingest(
            source_definition({"source_id": "S30"}),
            {
                "item_id": item_id,
                "raw_ref": raw,
                "raw_sha256": sha,
                "observed_at": seen,
                "external_id": "one",
                "title": "Synthetic",
                "text": "Synthetic crypto T approval",
                "published_at": "2026-10-17T00:00:00Z",
            },
        )
        tool_event["input"] = {}
        pipeline = Pipeline(repo, settings, http=http)
        await pipeline.filter_item({"item_id": item["item_id"]})
        routed_origin = await pipeline.route_item({"item_id": item["item_id"]})
        toolservice = EventToolService(repo, settings, None, DecisionEngine(settings), http=http)
        session = SessionService(repo, settings, toolservice.dispatch, http=http)

        session.start_stream = lambda sid: None
        await session.send_message(routed_origin["routing_message"])
        await session.stream("s1")
        stored_read = await repo.get("ops.tool_calls", "s1:tool1")
        assert stored_read["result"].get("item_id") == item["item_id"], stored_read
        resumed = SessionService(repo, settings, toolservice.dispatch, http=http)
        before = len(event_posts)
        await resumed.stream("s1")
        assert len(event_posts) == before

        event = {"event_id": "e", "event_state": "APPROVED", "state_verified": True, "freshness": "FRESH"}
        assessment = {
            "dims": unassessed_score_dimensions(),
            "class": "MAJOR",
            "certainty": "HIGH",
            "impact_horizon": {
                "impact_start": "UNKNOWN",
                "ramp_shape": "STEP",
                "dependencies": [],
                "time_to_materiality": "not estimable",
                "timing_confidence": "LOW",
            },
        }
        edge = {"token_id": "T", "role": "PRIMARY", "credible": True, "capture_class": "DIRECT"}
        market = {
            "token_id": "T",
            "move_class": "UNKNOWN",
            "data_confidence": {"class": "LOW"},
            "relative": {},
            "liquidity": {},
            "spot": {},
            "derivatives": {},
        }
        decision = DecisionEngine(settings).evaluate(event, assessment, edge, market)
        assert decision["action"] == "WAIT"
        card = compose_card(
            event, assessment, [edge], [decision], [market], verdict="Synthetic integration test."
        )
        alert = {"alert_id": "a", "version": 1, "event_id": "e", "card_text": card, "decisions": []}
        await repo.put("out.alerts", "a:1", alert, immutable=True)
        with pytest.raises(ImmutableRecordError):
            await repo.put("out.alerts", "a:1", dict(alert, card_text="changed"), immutable=True)
        deliver = DeliveryService(repo, settings, http=http)
        with pytest.raises(DeliveryError):
            await deliver.deliver(delivery_job({"alert_id": "a", "version": 1}))
        await deliver.deliver(delivery_job({"alert_id": "a", "version": 1}))
        await DeliveryService(repo, settings, http=http).deliver(
            delivery_job({"alert_id": "a", "version": 1})
        )
        assert len(sent) == 2
        await repo.put("out.alerts", "a:2", dict(alert, version=2), immutable=True)
        await deliver.deliver(delivery_job({"alert_id": "a", "version": 2}))
        assert sent[-1]["reply_parameters"]["message_id"] == 102
        await session.close()
        await resumed.close()


@pytest.fixture
async def isolated_e2e_database(postgres_template):
    import os

    source = os.environ.get("TANK_E2E_DATABASE_URL")
    if not source:
        pytest.skip("requires disposable PostgreSQL16 admin test connection")
    async with postgres_template.repository(source) as repo:
        yield repo


@pytest.mark.skipif(
    not __import__("os").environ.get("TANK_E2E_DATABASE_URL"),
    reason="set disposable PostgreSQL16 TANK_E2E_DATABASE_URL",
)
@pytest.mark.parametrize("complete_market", [False, True])
@pytest.mark.postgres
async def test_full_sse_pipeline_postgres(
    tmp_path: Path, complete_market, monkeypatch: pytest.MonkeyPatch, isolated_e2e_database
) -> None:
    """Whole synthetic call graph on PostgreSQL, including validated publication."""
    import importlib
    from datetime import datetime
    from functools import cache

    from tank.analysis.truth import MarketTruthService
    from tank.event_tools import LAYERS
    from tank.storage import PostgresRepository
    from tank.util import iso, new_id

    clock = datetime.now(UTC).replace(second=0, microsecond=0)
    for name in (
        "tank.util",
        "tank.storage",
        "tank.sources",
        "tank.agents.pipeline",
        "tank.agents.sessions",
        "tank.event_tools",
        "tank.analysis.cards",
        "tank.analysis.truth",
        "tank.delivery",
    ):
        module = importlib.import_module(name)
        if hasattr(module, "utcnow"):
            monkeypatch.setattr(module, "utcnow", lambda: clock)

    config = {
        "sources": source_configuration(
            {
                "sources": [
                    {
                        "source_id": "S30",
                        "source_class": "PRIMARY",
                        "mode": "REST",
                        "feeds": [{"url": "https://synthetic.invalid/feed"}],
                    }
                ]
            }
        ),
        "api_limits": _fixture_api_limits(),
        "exchange_addresses": {"version": "synthetic-fixture", "wallet_lists": []},
        "registry": {
            "tokens": [{"token_id": "T", "symbol": "T"}],
            "entities": [
                {"entity_id": "binance", "name": "Binance"},
                {"entity_id": "synthetic-venue", "name": "Synthetic venue"},
            ],
            "instruments": [{"instrument_id": "mock:spot:T", "token_id": "T"}],
        },
        "thresholds": _fixture_thresholds(),
        "openma": {"agent_id": "synthetic", "environment_id": "synthetic-offline"},
        "semantics": {},
        "dependency_map": {},
    }

    class TestSettings(Settings):
        def load_config(self, name):
            result = config[name.removesuffix(".yaml")]
            if name.removesuffix(".yaml") == "openma":
                result = dict(
                    result,
                    prompt_version="synthetic-prompt-v1",
                    agent_version="synthetic-agent-v1",
                    reasoning_effort="medium",
                )
            return result

        def read_config_bytes(self, name):
            import yaml

            if name.removesuffix(".yaml") in config:
                return yaml.safe_dump(self.load_config(name)).encode()
            return super().read_config_bytes(name)

    settings = TestSettings(
        **_fake_gateway_settings(tmp_path),
        data_dir=tmp_path,
        database_url=isolated_e2e_database.database_url,
        jev_provider="openrouter",
        openrouter_api_key="mock",
        telegram_token="mock",
        telegram_alerts_chat="mock",
    )
    repo = isolated_e2e_database
    sid = new_id("synthetic-session")
    feed_guid = sid
    denying = False
    active = []
    posted = []
    sends = []

    def transport(req):
        path = req.url.path
        if req.url.host == "synthetic.invalid":
            rss = f'<rss version="2.0"><channel><title>Synthetic</title><item><guid>{feed_guid}</guid><title>Synthetic listing approval</title><description>{text}</description><pubDate>{now}</pubDate><link>https://synthetic.invalid/{sid}</link></item></channel></rss>'
            return httpx.Response(200, content=rss.encode(), headers={"content-type": "application/rss+xml"})
        if req.url.host == "opencode.ai":
            return httpx.Response(
                200,
                json={
                    "model": "jev-1.13-free",
                    "answers": {
                        next(iter(json.loads(req.content)["questions"])): {
                            "type": "noul",
                            "noul": 0.99
                            if "relevant" in json.loads(req.content)["questions"]
                            else (
                                0.99
                                if denying
                                and sid
                                in str(json.loads(req.content)["state"].get("event", {}).get("statement", ""))
                                else 0.01
                            ),
                        }
                    },
                    "usage": {"input_tokens": 20},
                },
            )
        if path.endswith("/sessions"):
            return httpx.Response(200, json={"data": []} if req.method == "GET" else _native_session(sid))
        if req.method == "GET" and path.endswith("/sessions/" + sid):
            return httpx.Response(200, json=_native_session(sid))
        if path.endswith("/events/stream"):
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                text="".join(
                    "id: " + str(i + 1) + "\ndata: " + json.dumps(e) + "\n\n" for i, e in enumerate(active)
                ),
            )
        if path.endswith("/events"):
            posted.append(json.loads(req.content))
            return httpx.Response(200, json={})
        if req.url.host == "api.telegram.org":
            sends.append(json.loads(req.content))
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 900 + len(sends)}})
        raise AssertionError(str(req.url))

    @cache
    def canonical_snapshot():
        from test_truth_start_fixture import synthetic_start_snapshot

        return synthetic_start_snapshot()

    class CapturedMarket:
        async def snapshot(self, token_id, cutoff, anchor=None):
            if complete_market:
                from tank.util import parse_time

                synthetic = canonical_snapshot()
                delta = parse_time(cutoff) - parse_time(synthetic["cutoff"])

                def shift(value):
                    if isinstance(value, list):
                        return [shift(v) for v in value]
                    if isinstance(value, dict):
                        return {k: shift(v) for k, v in value.items()}
                    if isinstance(value, str) and value.startswith("2026-"):
                        return iso(parse_time(value) + delta)
                    return value

                synthetic = shift(synthetic)
                synthetic.update(cutoff=cutoff, anchor=anchor)
                return synthetic

            return {
                "token_id": token_id,
                "cutoff": cutoff,
                "anchor": anchor,
                "flow_bars": [],
                "klines": [],
                "books": [],
                "oi": [],
                "funding": [],
                "liquidations": [],
                "tickers": [],
                "supply": [],
                "semantics": [],
                "instruments": [],
            }

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
            source = SourceService(repo, settings, http=http)
            text = f"Binance approved the synthetic $T listing {sid}."
            if complete_market:
                text += " The synthetic fee burn is now live and fees accrued are burned."
            now = iso()
            polled = await source.poll_once("S30")
            assert polled["status"] == "HEALTHY" and polled["items"] == 1, polled
            item = (await repo.list("ingest.source_items", filters={"external_id": sid}))[0]
            sha = item["raw_sha256"]
            pipeline = Pipeline(repo, settings, http=http)
            await pipeline.filter_item({"item_id": item["item_id"]})
            routed_origin = await pipeline.route_item({"item_id": item["item_id"]})
            truth_service = MarketTruthService(repo, settings, CapturedMarket())
            tools = EventToolService(
                repo,
                settings,
                truth_service,
                DecisionEngine(settings),
                http=http,
            )
            session = SessionService(repo, settings, tools.dispatch, http=http)
            session.start_stream = lambda sid: None
            await session.send_message(routed_origin["routing_message"])
            count = 0

            async def invoke(name, args):
                nonlocal count
                if name == "record_event":
                    args = args.get("extraction", args)
                    args["public_at"]["basis"] = "synthetic source timestamp"
                    for captured_claim in args["claims"]:
                        captured_claim.update(
                            subject="Binance",
                            predicate="approved",
                            object="T listing",
                            directionality="NONE",
                            evidence_class="PRIMARY_FACT",
                        )
                if name == "set_assessment" and complete_market:
                    metric = {
                        "value": 1000,
                        "unit": "USD",
                        "window": "W24H",
                        "observed_at": now,
                        "precision": "EXACT",
                        "coverage": 1,
                        "reason": None,
                    }
                    args["assessment"]["capture_chains"] = [
                        {
                            "token_id": "T",
                            "capture_score": 8,
                            "token_mechanism": {"type": "BURN", "active": True, "evidence": [claim]},
                            **{
                                k: dict(metric)
                                for k in [
                                    "headline_activity",
                                    "eligible_activity",
                                    "effective_fees",
                                    "protocol_capture",
                                    "token_flow_annual_usd",
                                    "token_flow_pct_mcap",
                                ]
                            },
                        }
                    ]
                from model_fixture import model_arguments

                model_args = model_arguments(name, args)
                count += 1
                call_id = f"{sid}-full-{count}"
                active[:] = [
                    {"id": call_id, "type": "agent.custom_tool_use", "name": name, "input": model_args}
                ]
                if name == "get_market_truth":
                    import asyncio

                    async def market_jobs() -> None:
                        while True:
                            for job in await repo.list(
                                "ops.jobs", filters={"status": "QUEUED"}, limit=100000
                            ):
                                if job["payload"].get("event_id") != args["event_id"]:
                                    continue
                                if job["type"] not in {"ESCALATE_TOKENS", "RUN_MARKET_TRUTH"}:
                                    continue
                                result = None
                                if job["type"] == "RUN_MARKET_TRUTH":
                                    p = job["payload"]
                                    result = await truth_service.assess(
                                        p["event_id"], p["token_id"], p["cutoff"]
                                    )
                                await repo.put(
                                    "ops.jobs", job["job_id"], dict(job, result=result, status="DONE")
                                )
                            await asyncio.sleep(0.01)

                    worker = asyncio.create_task(market_jobs())
                    try:
                        await session.stream(sid)
                    finally:
                        worker.cancel()
                        worker_results = await asyncio.gather(worker, return_exceptions=True)
                        if isinstance(worker_results[0], Exception):
                            raise worker_results[0]
                else:
                    await session.stream(sid)
                row = await repo.get("ops.tool_calls", sid + ":" + call_id)
                assert row["status"] == "DONE" and row.get("returned_at"), row.get("result")
                return row["result"]

            extracted = await invoke(
                "record_event",
                {
                    "extraction": {
                        "item_id": item["item_id"],
                        "is_event": True,
                        "statement": text,
                        "certainty": "HIGH",
                        "certainty_reason": "Synthetic primary capture",
                        "entities": [],
                        "token_ids": ["T"],
                        "event_time": {
                            "value": iso(
                                __import__("datetime").datetime.fromisoformat(now)
                                - __import__("datetime").timedelta(hours=4)
                            )
                            if complete_market
                            else iso(
                                __import__("datetime").datetime.fromisoformat(now)
                                - __import__("datetime").timedelta(minutes=1)
                            ),
                            "precision": "EXACT",
                            "quote": text,
                        },
                        "public_at": {"value": now, "precision": "EXACT", "quote": text},
                        "freshness": "FRESH",
                        "claims": [
                            {
                                "verified": True,
                                "type": "STATE",
                                "state_claim": "LIVE" if complete_market else "APPROVED",
                                "quote": text,
                            }
                        ],
                        "next_milestone": None,
                        "checked_sources": [],
                    }
                },
            )
            eid = extracted["event_id"]
            claim = extracted["accepted_claims"][0]["claim_id"]
            dims = {
                name: {
                    "value": None,
                    "reason": "NOT_ESTIMABLE",
                    "rationale": "Synthetic data does not estimate economics",
                    "evidence": [claim],
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
            assessed = await invoke(
                "set_assessment",
                {
                    "event_id": eid,
                    "assessment": {
                        "class": "MAJOR",
                        "criterion": "M1",
                        "criterion_venue_id": "binance",
                        "criterion_claim_id": claim,
                        "dims": dims,
                        "impact_horizon": {
                            "impact_start": "UNKNOWN",
                            "ramp_shape": "STEP",
                            "dependencies": [],
                            "time_to_materiality": "not estimable",
                            "timing_confidence": "LOW",
                        },
                    },
                },
            )
            assert assessed["class"] == "MAJOR"
            await invoke(
                "set_beneficiaries",
                {
                    "event_id": eid,
                    "edges": [
                        {
                            "token_id": "T",
                            "layer": "VENUE_DEX",
                            "role": "PRIMARY",
                            "entity_id": "synthetic-venue",
                            "direction": "POSITIVE",
                            "directionality": "NONE",
                            "evidence": [claim],
                            "causal_chain": "Synthetic listing affects market access",
                            "exposure_score": 6,
                            "edge_certainty": "HIGH",
                        }
                    ],
                    "layer_reasons": {layer: "No other synthetic evidence" for layer in LAYERS},
                },
            )
            mt = await invoke("get_market_truth", {"event_id": eid, "token_ids": ["T"]})
            assert mt["market_truth"][0]["move_class"] == (
                "SPOT_LED_ACCUMULATION" if complete_market else "UNKNOWN"
            ), mt
            persisted_truth = (await repo.list("ana.market_truth", filters={"event_id": eid}))[0]
            assert all(isinstance(fact, str) for fact in persisted_truth["decisive_facts"])
            if complete_market:
                assert persisted_truth["decisive_fact_details"]
                assert any(
                    "EVENT" in fact and "NATIVE_TOKEN" in fact for fact in persisted_truth["decisive_facts"]
                )
            decision = await invoke("get_decision", {"event_id": eid, "token_id": "T"})
            assert decision["action"] == ("START" if complete_market else "WAIT"), decision
            what = "Binance approved listing T, allowing the exchange to add trading access for this token."
            published = await invoke(
                "publish_card",
                {
                    "event_id": eid,
                    "kind": "FULL",
                    "what": what,
                    "verdict": "The synthetic listing was approved.",
                    "why": ["Primary synthetic evidence records approval.", "Market inputs are unavailable."],
                },
            )
            assert published["published"], published
            alerts = await repo.list("out.alerts", filters={"event_id": eid})
            assert "linter" not in alerts[0]
            frozen = alerts[0]["card_text"]
            assert frozen.splitlines()[1] == "WHAT: " + what
            assert alerts[0]["card_struct"]["what"] == what
            if complete_market:
                assert "EVENT" in frozen and "NATIVE_TOKEN" in frozen
            bundle = await repo.get("out.evidence_bundles", published["evidence_bundle_id"])
            assert bundle["payload"]["source_items"][0]["raw_sha256"] == sha
            assert bundle["payload"]["what"] == what
            await session.close()
            await repo.close()

            repo = PostgresRepository(settings.database_url)
            await repo.open()
            delivery = DeliveryService(repo, settings, http=http)
            payload = {"alert_id": published["alert_id"], "version": published["version"]}
            await delivery.deliver(delivery_job(payload))
            await delivery.deliver(delivery_job(payload))
            assert (
                len(sends) == 1
                and (await repo.list("out.alerts", filters={"event_id": eid}))[0]["card_text"] == frozen
            )
            assert rich_message_text(sends[0]["rich_message"]).splitlines()[1] == "WHAT: " + what
            tools = EventToolService(
                repo,
                settings,
                MarketTruthService(repo, settings, CapturedMarket()),
                DecisionEngine(settings),
                http=http,
            )
            resumed = SessionService(repo, settings, tools.dispatch, http=http)
            resumed.start_stream = lambda sid: None
            session = resumed
            before = len(posted)
            await resumed.stream(sid)
            assert len(posted) == before
            if complete_market:
                calls = await repo.list("ana.calls", filters={"event_id": eid})
                assert len(calls) == 1 and calls[0]["status"] == "OPEN"
                denying = True

                clock += __import__("datetime").timedelta(seconds=1)
                feed_guid = sid + "-denial"
                text = (
                    f"Binance cancelled the synthetic $T listing {sid} and reversed its synthetic fee burn."
                )
                now = iso()
                source = SourceService(repo, settings, http=http)
                assert (await source.poll_once("S30"))["status"] == "HEALTHY"
                denial_item = (await repo.list("ingest.source_items", filters={"external_id": feed_guid}))[0]
                pipeline = Pipeline(repo, settings, http=http)
                await pipeline.filter_item({"item_id": denial_item["item_id"]})
                routed = await pipeline.route_item({"item_id": denial_item["item_id"]})
                assert routed["routing_message"]["kind"] == "ITEM_ADDED", routed
                assert routed["route_event_id"] == eid, routed
                await session.send_message(routed["routing_message"])
                await invoke(
                    "add_observation",
                    {
                        "certainty_reason": "Synthetic agent confirms the official denial",
                        "event_id": eid,
                        "link": "DENIAL",
                        "certainty": "HIGH",
                        "public_at": {
                            "value": now,
                            "precision": "EXACT",
                            "basis": "synthetic source timestamp",
                            "quote": text,
                        },
                        "freshness": "FRESH",
                        "claims": [
                            {
                                "verified": True,
                                "type": "STATE",
                                "subject": "Binance",
                                "predicate": "cancelled",
                                "object": "T listing",
                                "state_claim": "CANCELLED",
                                "directionality": "NONE",
                                "evidence_class": "PRIMARY_FACT",
                                "quote": text,
                            }
                        ],
                    },
                )
                exited = await invoke("get_decision", {"event_id": eid, "token_id": "T"})
                assert exited["action"] == "EXIT", exited
                correction_what = "Binance cancelled the listing of T, withdrawing the planned access to trading on its exchange."
                correction = await invoke(
                    "publish_card",
                    {
                        "event_id": eid,
                        "kind": "UPDATE",
                        "what": correction_what,
                        "verdict": "The synthetic listing was officially cancelled.",
                        "why": [
                            "Primary synthetic evidence records cancellation.",
                            "The open synthetic thesis is invalidated.",
                        ],
                    },
                )
                assert correction["published"], correction
                await delivery.deliver(
                    delivery_job({"alert_id": correction["alert_id"], "version": correction["version"]})
                )
                assert len(sends) == 2 and sends[-1]["reply_parameters"]["message_id"] == 901
                assert (
                    rich_message_text(sends[-1]["rich_message"]).splitlines()[1] == "WHAT: " + correction_what
                )
                previous = await repo.get(
                    "out.alerts", published["alert_id"] + ":" + str(published["version"])
                )
                assert previous["card_struct"]["what"] == what
                current = await repo.get(
                    "out.alerts", correction["alert_id"] + ":" + str(correction["version"])
                )
                assert current["card_struct"]["what"] == correction_what
                calls = await repo.list("ana.calls", filters={"event_id": eid})
                assert len(calls) == 1 and calls[0]["status"] == "CLOSED"

                before = len(posted)
                await session.stream(sid)
                await delivery.deliver(
                    delivery_job({"alert_id": correction["alert_id"], "version": correction["version"]})
                )
                assert len(posted) == before and len(sends) == 2
                assert len(await repo.list("ana.calls", filters={"event_id": eid})) == 1
            await resumed.close()
    finally:
        await repo.close()


@pytest.mark.skipif(
    not __import__("os").environ.get("TANK_E2E_DATABASE_URL"),
    reason="requires disposable PostgreSQL16 admin connection",
)
@pytest.mark.postgres
async def test_runtime_autonomous_source_to_telegram(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, isolated_e2e_database
) -> None:
    """Only external wire inputs are fed; Runtime's eight workers perform every stage."""
    import asyncio
    import importlib

    from tank.analysis.truth import MarketTruthService
    from tank.bootstrap import sync_registry
    from tank.codex_gateway import CodexGateway
    from tank.event_tools import LAYERS
    from tank.runtime import Runtime
    from tank.runtime_composition import validate_runtime_settings
    from tank.scheduler import Scheduler
    from tank.util import iso

    repo = isolated_e2e_database

    async with repo.transaction():
        clock = (await (await repo._conn().execute("SELECT clock_timestamp() AS clock")).fetchone())["clock"]
    backup_clock = clock.replace(hour=12, minute=0, second=0, microsecond=0)
    for name in (
        "tank.util",
        "tank.storage",
        "tank.sources",
        "tank.agents.pipeline",
        "tank.agents.sessions",
        "tank.event_tools",
        "tank.analysis.cards",
        "tank.analysis.truth",
        "tank.runtime",
        "tank.scheduler",
        "tank.delivery",
    ):
        module = importlib.import_module(name)
        if hasattr(module, "utcnow"):
            monkeypatch.setattr(module, "utcnow", lambda: clock)
    config = {
        "sources": source_configuration(
            {
                "sources": [
                    {
                        "source_id": "S30",
                        "source_class": "PRIMARY",
                        "mode": "REST",
                        "interval_s": 60,
                        "feeds": [{"url": "https://synthetic.invalid/autonomous-feed"}],
                    }
                ]
            }
        ),
        "api_limits": _fixture_api_limits(),
        "exchange_addresses": {"version": "synthetic-fixture", "wallet_lists": []},
        "registry": {
            "tokens": [{"token_id": "T", "symbol": "T"}],
            "entities": [
                {"entity_id": "binance", "name": "Binance"},
                {"entity_id": "synthetic-venue", "name": "Synthetic venue"},
            ],
            "instruments": [{"instrument_id": "mock:spot:T", "token_id": "T"}],
        },
        "thresholds": _fixture_thresholds(),
        "openma": {"agent_id": "synthetic-offline-agent", "environment_id": "synthetic-offline-environment"},
        "semantics": {"metrics": []},
        "dependency_map": {},
    }

    class TestSettings(Settings):
        def load_config(self, name):
            result = config[name.removesuffix(".yaml")]
            if name.removesuffix(".yaml") == "openma":
                result = dict(
                    result,
                    prompt_version="synthetic-prompt-v1",
                    agent_version="synthetic-agent-v1",
                    reasoning_effort="medium",
                )
            return result

        def read_config_bytes(self, name):
            import yaml

            if name.removesuffix(".yaml") in config:
                return yaml.safe_dump(self.load_config(name)).encode()
            return super().read_config_bytes(name)

    settings = TestSettings(
        **_fake_gateway_settings(tmp_path),
        data_dir=tmp_path / "data",
        database_url=repo.database_url,
        jev_provider="openrouter",
        openrouter_api_key="mock",
        telegram_token="mock",
        telegram_alerts_chat="mock-alerts",
        telegram_ops_chat="mock-ops",
        backup_dir=tmp_path / "backups",
        backup_hour_utc=23,
        config_version="synthetic-integration-v1",
        code_version="synthetic-test",
        shutdown_grace_s=3,
    )
    assert backup_clock.hour < settings.backup_hour_utc, "Synthetic backup clock must precede its window"
    settings.backup_dir.mkdir()
    settings.data_dir.mkdir()
    backup_results = []

    class FixtureScheduler(Scheduler):
        async def maintenance(self, now, **kwargs):

            result = await super().maintenance(backup_clock, **kwargs)
            backup_results.append((now, result))
            return result

    text = "Binance approved the synthetic autonomous $T listing."
    timestamp = iso(clock - __import__("datetime").timedelta(minutes=1))
    queue = asyncio.Queue()
    delivered = asyncio.Event()
    stop = asyncio.Event()
    wire = []
    state = {}
    stage = 0

    class SSE(httpx.AsyncByteStream):
        async def __aiter__(self):
            while True:
                event = await queue.get()
                yield (
                    "id: "
                    + str(event["seq"])
                    + "\ndata: "
                    + json.dumps({"seq": event["seq"], "event": event["body"]})
                    + "\n\n"
                ).encode()

        async def aclose(self) -> None:
            pass

    async def emit(name, arguments) -> None:
        nonlocal stage
        stage += 1
        arguments = {k: v for k, v in arguments.items() if k not in {"event_id", "item_id"}}
        body = {
            "id": f"autonomous-tool-{stage}",
            "type": "agent.custom_tool_use",
            "name": name,
            "input": arguments,
        }
        wire.append(body)
        await queue.put({"seq": stage, "body": body})

    async def transport(request: httpx.Request):
        nonlocal stage
        path = request.url.path
        if request.url.host == "synthetic.invalid":
            rss = f'<rss version="2.0"><channel><title>Synthetic</title><item><guid>autonomous-input</guid><title>Autonomous synthetic listing</title><description>{text}</description><pubDate>{timestamp}</pubDate><link>https://synthetic.invalid/autonomous-input</link></item></channel></rss>'
            return httpx.Response(200, content=rss.encode(), headers={"content-type": "application/rss+xml"})
        if request.url.host == "opencode.ai":
            body = json.loads(request.content)
            question = next(iter(body["questions"]))
            assert body["model"] == "jev-1.13-free"
            return httpx.Response(
                200,
                json={
                    "model": "jev-1.13-free",
                    "answers": {question: {"type": "noul", "noul": 0.99}},
                    "usage": {"input_tokens": 20},
                },
            )
        if path.endswith("/sessions"):
            return httpx.Response(
                200, json={"data": []} if request.method == "GET" else _native_session("autonomous-session")
            )
        if request.method == "GET" and path.endswith("/sessions/autonomous-session"):
            return httpx.Response(200, json=_native_session("autonomous-session"))
        if path.endswith("/events/stream"):
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=SSE())
        if path.endswith("/events"):
            if request.method == "GET":
                return httpx.Response(200, json={"data": []})
            event = json.loads(request.content)["events"][0]
            if event["type"] == "user.message":
                payload = json.loads(event["content"][0]["text"])
                assert payload["item"]["text"]
                assert "item_id" not in payload["item"]
                await emit(
                    "record_event",
                    {
                        "is_event": True,
                        "statement": text,
                        "certainty": "HIGH",
                        "certainty_reason": "Synthetic primary source capture",
                        "entities": [],
                        "token_ids": ["T"],
                        "event_time": {"value": timestamp, "precision": "EXACT", "quote": text},
                        "public_at": {
                            "value": timestamp,
                            "precision": "EXACT",
                            "basis": "Synthetic source timestamp",
                            "quote": text,
                        },
                        "freshness": "FRESH",
                        "claims": [
                            {
                                "verified": True,
                                "type": "STATE",
                                "subject": "Binance",
                                "predicate": "approved",
                                "object": "T listing",
                                "state_claim": "APPROVED",
                                "directionality": "NONE",
                                "evidence_class": "PRIMARY_FACT",
                                "quote": text,
                            }
                        ],
                        "next_milestone": None,
                        "checked_sources": [],
                    },
                )
            elif event["type"] == "user.custom_tool_result":
                result = json.loads(event["content"][0]["text"])
                if stage == 1:
                    assert "event_id" not in result and "claim_id" not in json.dumps(result)
                    state["claim"] = next(
                        row["claim_id"] for row in await repo.list("evt.claims") if row.get("quote") == text
                    )
                    dims = {
                        name: {
                            "value": None,
                            "reason": "NOT_ESTIMABLE",
                            "rationale": "No synthetic economic estimate",
                            "evidence": [state["claim"]],
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
                    await emit(
                        "set_assessment",
                        {
                            "assessment": {
                                "class": "MAJOR",
                                "criterion": "M1",
                                "criterion_venue_id": "binance",
                                "criterion_claim_id": state["claim"],
                                "dims": dims,
                                "impact_horizon": {
                                    "impact_start": "UNKNOWN",
                                    "ramp_shape": "STEP",
                                    "dependencies": [],
                                    "time_to_materiality": "not estimable",
                                    "timing_confidence": "LOW",
                                },
                            },
                        },
                    )
                elif stage == 2:
                    assert result["class"] == "MAJOR"
                    await emit(
                        "set_beneficiaries",
                        {
                            "edges": [
                                {
                                    "entity_id": "synthetic-venue",
                                    "token_id": "T",
                                    "role": "PRIMARY",
                                    "layer": "VENUE_DEX",
                                    "direction": "POSITIVE",
                                    "directionality": "NONE",
                                    "causal_chain": "Synthetic listing affects access",
                                    "exposure_score": 6,
                                    "edge_certainty": "HIGH",
                                    "evidence": [state["claim"]],
                                }
                            ],
                            "layer_reasons": {layer: "No other synthetic evidence" for layer in LAYERS},
                        },
                    )
                elif stage == 3:
                    await emit("get_market_truth", {"token_ids": ["T"]})
                elif stage == 4:
                    assert result["market_truth"][0]["move_class"] == "UNKNOWN"
                    await emit("get_decision", {"token_id": "T"})
                elif stage == 5:
                    assert result["action"] == "WAIT"
                    await emit(
                        "publish_card",
                        {
                            "kind": "FULL",
                            "what": "The synthetic listing was approved.",
                            "verdict": "The synthetic listing was approved.",
                            "why": [
                                "Primary synthetic evidence records approval.",
                                "Market inputs are unavailable.",
                            ],
                        },
                    )
                elif stage == 6:
                    assert result["published"], result
                    await queue.put(
                        {
                            "seq": 7,
                            "body": {
                                "id": "autonomous-end-turn",
                                "type": "session.status_idle",
                                "stop_reason": {"type": "end_turn"},
                            },
                        }
                    )
            return httpx.Response(200, json={})
        if request.url.host == "api.telegram.org":
            if request.url.path.endswith("/getUpdates"):
                return httpx.Response(200, json={"ok": True, "result": []})
            if json.loads(request.content)["chat_id"] == "mock-alerts":
                delivered.set()
            else:
                assert json.loads(request.content)["chat_id"] == "mock-ops"
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 1701}})
        raise AssertionError(f"Uncaptured external request: {request.method} {request.url}")

    class CapturedMarket:
        """External captured-data adapter; no domain rule or queue processing here."""

        def __init__(self, http) -> None:
            from tank.market.service import MarketService

            self.escalations = []
            self.actual_capture_reader = MarketService(repo, settings, http=http)

        async def run(self, event) -> None:
            await event.wait()

        async def escalate(self, event_id, tokens) -> None:
            from tank.market.capture import event_anchor
            from tank.util import sha256

            self.escalations.append((event_id, tokens))
            anchor = event_anchor(await repo.get("evt.events", event_id)).value
            for token in tokens:
                key = "BF-" + sha256({"event_id": event_id, "token_id": token, "anchor": iso(anchor)})
                await repo.put(
                    "mkt.backfill_runs",
                    key,
                    {
                        "backfill_id": key,
                        "event_id": event_id,
                        "token_id": token,
                        "anchor": iso(anchor),
                        "started_at": iso(),
                        "finished_at": iso(),
                        "status": "FAILED",
                        "reason": "SYNTHETIC_CAPTURE_INPUTS_UNAVAILABLE",
                        "result": None,
                        "config_version": settings.config_version,
                        "code_version": settings.code_version,
                    },
                )

        async def capture_readiness(self, event_id, token_id, *, anchor=None):
            return await self.actual_capture_reader.capture_readiness(event_id, token_id, anchor=anchor)

        async def capture_changes(self, since=None, end=None):
            async for change in self.actual_capture_reader.capture_changes(since=since, end=end):
                yield change

        async def demote(self, token_id, event_id=None) -> None:
            pass

        async def snapshot(self, token_id, cutoff, anchor=None):
            from tank.time_precision import Timestamp, chosen_boundary

            boundary = cutoff if isinstance(cutoff, Timestamp) else chosen_boundary(cutoff, precision="EXACT")
            runs = await repo.list("mkt.backfill_runs", filters={"token_id": token_id})
            latest = max(runs, key=lambda row: row["started_at"]) if runs else None
            return {
                "backfill_runs": runs,
                "capture_status": {
                    "status": latest["status"] if latest else "NOT_REQUESTED",
                    "reason": latest["reason"] if latest else "NO_DURABLE_CAPTURE_REQUEST",
                    "backfill_id": latest["backfill_id"] if latest else None,
                    "observed_at": iso(),
                    "bounded_wait_s": 0,
                    "incomplete_windows_excluded": True,
                },
                "token_id": token_id,
                **boundary.fields("cutoff"),
                "anchor": anchor,
                "flow_bars": [],
                "klines": [],
                "books": [],
                "oi": [],
                "funding": [],
                "liquidations": [],
                "tickers": [],
                "supply": [],
                "semantics": [],
                "instruments": [],
            }

        async def close(self) -> None:
            await self.actual_capture_reader.close()

    runtime_task = None
    import logging
    import sys
    import traceback

    runtime_errors = []

    class ErrorCapture(logging.Filter):
        def filter(self, record):
            if record.levelno >= logging.ERROR and len(runtime_errors) < 3:
                runtime_errors.append("".join(traceback.format_exception(sys.exception())))
            return True

    error_capture = ErrorCapture()
    logging.getLogger("tank.runtime").addFilter(error_capture)
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as http:
            market = CapturedMarket(http)
            truth = MarketTruthService(repo, settings, market)
            sources = SourceService(repo, settings, http=http)
            pipeline = Pipeline(repo, settings, http=http)
            tools = EventToolService(repo, settings, truth, DecisionEngine(settings), http=http)
            sessions = SessionService(repo, settings, tools.dispatch, http=http)
            delivery = DeliveryService(repo, settings, http=http)
            gateway = CodexGateway(settings)
            inbox = TelegramInbox(repo, settings, http=http)
            scheduler = FixtureScheduler(repo, settings, market, truth)
            sessions.telegram = inbox
            tools.scheduler = scheduler

            async def bootstrap_registry() -> None:
                await sync_registry(repo, settings)

            def validate_generation() -> None:
                validate_runtime_settings(settings)

            runtime = Runtime(
                repo,
                settings,
                sources=sources,
                market=market,
                truth=truth,
                pipeline=pipeline,
                sessions=sessions,
                delivery=delivery,
                inbox=inbox,
                scheduler=scheduler,
                gateway=gateway,
                bootstrap=bootstrap_registry,
                validate=validate_generation,
            )
            runtime_task = asyncio.create_task(runtime.run(stop))
            try:
                async with asyncio.timeout(15):
                    while not delivered.is_set():
                        if runtime_errors:
                            pytest.fail("\n".join(runtime_errors))
                        if runtime_task.done():
                            await runtime_task
                            pytest.fail("Runtime ended before delivery")
                        await asyncio.sleep(0.01)
            except TimeoutError:
                pytest.fail(
                    str(
                        {
                            "runtime_errors": runtime_errors,
                            "runtime_done": runtime_task.done(),
                            "runtime_error": str(runtime_task.exception()) if runtime_task.done() else None,
                            "wire": [row["name"] for row in wire],
                            "jobs": [
                                {key: row.get(key) for key in ("type", "status", "last_error", "attempts")}
                                for row in await repo.list("ops.jobs", limit=100)
                            ],
                            "sources": [
                                {key: row.get(key) for key in ("source_id", "status", "error")}
                                for row in await repo.list("ops.source_checkpoints")
                            ],
                            "sessions": [
                                {key: row.get(key) for key in ("session_id", "status", "runtime_status")}
                                for row in await repo.list("ops.agent_sessions")
                            ],
                        }
                    )
                )
            assert gateway.bound_port is not None and gateway.bound_port > 0
            assert gateway.settings.codex_auth_path == settings.codex_auth_path
            assert settings.codex_auth_path.name == "FAKE-codex-auth.json"

            async with asyncio.timeout(3):
                while not await repo.list("out.deliveries", filters={"status": "SENT"}):
                    await asyncio.sleep(0.01)
            stop.set()
            await asyncio.wait_for(runtime_task, 12)
            assert backup_results and all(result["status"] == "deferred" for _, result in backup_results)
            assert [event["name"] for event in wire] == [
                "record_event",
                "set_assessment",
                "set_beneficiaries",
                "get_market_truth",
                "get_decision",
                "publish_card",
            ]
            jobs = await repo.list("ops.jobs", limit=100000)
            assert not runtime_errors, "\n".join(runtime_errors)
            assert not [job for job in jobs if job.get("last_error")], jobs
            assert {
                "FILTER_ITEM",
                "ROUTE_ITEM",
                "AGENT_MESSAGE",
                "ESCALATE_TOKENS",
                "RUN_MARKET_TRUTH",
                "DELIVER",
            } <= {job["type"] for job in jobs if job["status"] == "DONE"}
            alerts = await repo.list("out.alerts")
            assert len(alerts) == 1 and "linter" not in alerts[0]
            assert (await repo.list("ops.agent_sessions"))[0]["runtime_status"] == "IDLE"
            state["event_id"] = (await repo.get("ops.agent_sessions", "autonomous-session"))["event_id"]
            assert market.escalations == [(state["event_id"], ["T"])]
            captures = await repo.list("mkt.backfill_runs", filters={"event_id": state["event_id"]})
            assert len(captures) == 1 and captures[0]["status"] == "FAILED"
            truth_rows = await repo.list("ana.market_truth", filters={"event_id": state["event_id"]})
            assert truth_rows[0]["data_confidence"]["class"] == "LOW"
            assert (
                truth_rows[0]["data_confidence"]["capture_readiness"]["capture_status"]["status"] == "FAILED"
            )
            assert (await repo.list("ana.decisions"))[0]["action"] == "WAIT"
            assert (
                len(
                    await repo.list(
                        "out.deliveries", filters={"status": "SENT", "channel": "telegram:alerts"}
                    )
                )
                == 1
            )
            assert not [record for record in caplog.records if record.levelno >= 40], caplog.text
    finally:
        stop.set()
        if runtime_task and not runtime_task.done():
            runtime_task.cancel()
            await asyncio.gather(runtime_task, return_exceptions=True)
        await repo.close()
        logging.getLogger("tank.runtime").removeFilter(error_capture)
