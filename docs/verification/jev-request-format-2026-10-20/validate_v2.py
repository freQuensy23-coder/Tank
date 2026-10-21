"""Compare the jev-v1 and jev-v2 request formats on the same real pairs from the V0-B packet.

Both formats are scored on the current Jev binding (OpenCode); the earlier OpenRouter jev-v1 scores
(cache in V1_USAGE_DIR) choose the pairs and are reported for reference.

Usage: validate_v2.py PACKET V1_USAGE_DIR OUT_DIR [--score]
Without --score it only lists the pairs and counts the requests (no network).
"""

import asyncio
import json
import random
import sys
import time
from pathlib import Path

import httpx

from tank.agents.jev import JevBinding, classify
from tank.agents.pipeline import Pipeline
from tank.calibration import _CachedConfig, load_packet
from tank.settings import Settings, resolve_versions
from tank.storage import MemoryRepository
from tank.util import iso, sha256

V1_FILTER = "Is this text about crypto - crypto assets, crypto exchanges, crypto regulation or enforcement, blockchain networks, or one of these projects: Bitcoin, Ethereum, Uniswap, PancakeSwap, Aerodrome, Curve, 1inch, 0x, CoW Protocol, Hyperliquid, Lighter, Aave, Morpho, Euler, Ondo, LayerZero, Quant, Avalanche, NEAR, Plasma, Ethena, World (Worldcoin)? Whether it reports anything new or important does not matter here. General finance, technology or politics that does not touch crypto does not count."
V1_ROUTE = "Does the item report the same real-world event as the event described, or a new development of it - a confirmation, denial, stage change, new detail or new party? Another event about the same project does not count."


def v1_state(item):
    return {
        "source": item.get("source") or item.get("source_name") or item.get("source_id"),
        "author": item.get("author")
        or item.get("source")
        or item.get("source_name")
        or item.get("source_id"),
        "text": "\n\n".join(
            dict.fromkeys(str(v) for v in (item.get("title"), item.get("text") or item.get("body")) if v)
        ),
        "quoted": item.get("quoted"),
    }


def v1_routing_state(item, event):
    return {
        **v1_state(item),
        "event": {
            "statement": event.get("statement"),
            "entities": event.get("entities", []),
            "tokens": event.get("tokens_mentioned", event.get("tokens", [])),
            "stage": event.get("event_state", event.get("stage")),
            "known_to_us_at": event.get("known_to_us_at"),
        },
    }


def v1_questions(name):
    return {
        name: {
            "type": "noul",
            "instructions": V1_FILTER if name == "relevant" else V1_ROUTE,
            "criteria": {
                "true": "About crypto" if name == "relevant" else "Same event or a development of it",
                "false": "Not about crypto" if name == "relevant" else "A different event",
            },
        }
    }


def v1_score(usage, item, name, state):
    questions = {
        name: {
            "type": "noul",
            "instructions": V1_FILTER if name == "relevant" else V1_ROUTE,
            "criteria": {
                "true": "About crypto" if name == "relevant" else "Same event or a development of it",
                "false": "Not about crypto" if name == "relevant" else "A different event",
            },
        }
    }
    old = JevBinding(
        provider="openrouter", base_url="https://openrouter.ai/api", request_model="typesafe/jev-1.13"
    )
    key = sha256(
        {
            "item_id": item["item_id"],
            "model": old.request_model,
            "provider": old.provider,
            "base_url": old.base_url,
            "prompt_version": "jev-v1",
            "questions": questions,
            "state_fingerprint": sha256(state),
        }
    )
    f = usage / (key + ".json")
    return json.loads(f.read_text())["score"] if f.exists() else None


async def main():
    packet_path, usage, out = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
    rows, events, window = load_packet(json.load(open(packet_path)))
    settings = _CachedConfig(resolve_versions(Settings()))
    sessions, pairs, filters = {}, [], []
    for row in rows:
        item = row["source_item"]
        if row["routed"]:
            repo = MemoryRepository()
            for e, last in sessions.items():
                if last >= row["at"] - window:
                    await repo.put("evt.events", e, {**events[e], "lifecycle": "OPEN"})
                    await repo.put(
                        "ops.agent_sessions", e, {"session_id": e, "event_id": e, "last_item_at": iso(last)}
                    )
            sel = Pipeline(repo, settings, http=None)
            for ev in await sel._candidate_events(sel.identities(item), iso(row["at"] - window), row["at"]):
                v1 = v1_score(usage, item, "same_event", v1_routing_state(item, events[ev["event_id"]]))
                pairs.append(
                    {
                        "item_id": row["item_id"],
                        "event_id": ev["event_id"],
                        "same": ev["event_id"] == row["expected_event_id"],
                        "v1": v1,
                    }
                )
            if row.get("event_id"):
                sessions[row["event_id"]] = row["at"]
        filters.append(
            {
                "item_id": row["item_id"],
                "relevant": row["relevant"],
                "filtered": row["filtered"],
                "v1": v1_score(usage, item, "relevant", v1_state(item)),
            }
        )
    rnd = random.Random(20261020)
    positives = [p for p in pairs if p["same"] and p["v1"] is not None]
    hard = [p for p in pairs if not p["same"] and p["v1"] is not None and p["v1"] >= 0.5]
    easy = rnd.sample([p for p in pairs if not p["same"] and p["v1"] is not None and p["v1"] < 0.5], 20)
    fsel = rnd.sample(
        [f for f in filters if f["filtered"] and f["relevant"] and f["v1"] is not None], 20
    ) + rnd.sample([f for f in filters if f["filtered"] and not f["relevant"] and f["v1"] is not None], 20)
    chosen = positives + hard + easy
    print(
        "route pairs: positives",
        len(positives),
        "hard negatives (v1>=0.5)",
        len(hard),
        "easy negatives",
        len(easy),
        "| filter items",
        len(fsel),
        "| requests (v1 + v2 formats)",
        2 * (len(chosen) + len(fsel)),
    )
    if "--score" not in sys.argv:
        return
    by_id = {r["item_id"]: r for r in rows}
    out.mkdir(parents=True, exist_ok=True)
    cache_path = out / "responses.jsonl"
    cache = {}
    if cache_path.exists():
        for line in cache_path.read_text().splitlines():
            row = json.loads(line)
            cache[row["key"]] = row
    log = {"requests": 0, "status": {}, "latency_s": [], "retries_429": 0}
    async with httpx.AsyncClient(timeout=60) as http:
        sem = asyncio.Semaphore(4)

        async def ask(fmt, name, item, state, questions):
            key = sha256({"fmt": fmt, "item": item["item_id"], "state": state, "questions": questions})
            if key in cache:
                return cache[key]["score"]
            async with sem:
                for attempt in range(6):
                    started = time.monotonic()
                    try:
                        data = await classify(http, api_key=None, state=state, questions=questions)
                    except httpx.HTTPStatusError as error:
                        code = error.response.status_code
                        log["status"][code] = log["status"].get(code, 0) + 1
                        if code == 429:
                            log["retries_429"] += 1
                            await asyncio.sleep(2**attempt)
                            continue
                        raise
                    log["requests"] += 1
                    log["status"][200] = log["status"].get(200, 0) + 1
                    log["latency_s"].append(round(time.monotonic() - started, 2))
                    score = data["answers"][name]["noul"]
                    row = {
                        "key": key,
                        "fmt": fmt,
                        "item_id": item["item_id"],
                        "name": name,
                        "score": score,
                        "model": data["model"],
                        "usage": data["usage"],
                    }
                    cache[key] = row
                    with cache_path.open("a") as stream:
                        stream.write(json.dumps(row) + "\n")
                    return score
                raise RuntimeError("rate limited after retries")

        pipe = Pipeline(None, settings, http=None)

        async def both(p, name):
            item = by_id[p["item_id"]]["source_item"]
            if name == "relevant":
                s1, s2 = v1_state(item), pipe.state(item)
            else:
                s1, s2 = (
                    v1_routing_state(item, events[p["event_id"]]),
                    pipe.routing_state(item, events[p["event_id"]]),
                )
            p["v1_openrouter"] = p.pop("v1", p.get("v1_openrouter"))
            p["v1"] = await ask("v1", name, item, s1, v1_questions(name))
            p["v2"] = await ask("v2", name, item, s2, pipe.score_identity(item, name, s2)[1])

        await asyncio.gather(*(both(p, "same_event") for p in chosen), *(both(f, "relevant") for f in fsel))
    (out / "pairs.json").write_text(json.dumps({"route": chosen, "filter": fsel}, indent=1))
    lat = sorted(log["latency_s"]) or [0]
    log["latency_p50_s"], log["latency_max_s"] = lat[len(lat) // 2], lat[-1]
    del log["latency_s"]
    (out / "provider-log.json").write_text(json.dumps(log, indent=1))
    print("provider", json.dumps(log))
    for version in ("v1_openrouter", "v1", "v2"):
        pos = [p[version] for p in positives]
        neg = [p[version] for p in hard + easy]
        auc = sum((a > b) + 0.5 * (a == b) for a in pos for b in neg) / (len(pos) * len(neg))
        line = [
            version,
            "route: mean same",
            round(sum(pos) / len(pos), 3),
            "mean different",
            round(sum(neg) / len(neg), 3),
            "separation (P[same > different])",
            round(auc, 3),
        ]
        for cut in (0.5, 0.7):
            line += [
                f"| at {cut}: same passed",
                sum(s >= cut for s in pos),
                "/",
                len(pos),
                "different passed",
                sum(s >= cut for s in neg),
                "/",
                len(neg),
            ]
        print(*line)
        rel = [f[version] for f in fsel if f["relevant"]]
        irr = [f[version] for f in fsel if not f["relevant"]]
        print(
            version,
            "filter at 0.5: crypto passed",
            sum(s >= 0.5 for s in rel),
            "/",
            len(rel),
            "non-crypto passed",
            sum(s >= 0.5 for s in irr),
            "/",
            len(irr),
        )


asyncio.run(main())
