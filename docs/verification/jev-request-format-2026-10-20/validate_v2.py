"""Compare jev-v1 (cached scores) and jev-v2 on the same real pairs from the V0-B packet.

Usage: validate_v2.py PACKET V1_USAGE_DIR OUT_DIR [--score]
Without --score it only lists the pairs and counts the v2 requests (no network).
"""
import asyncio
import json
import random
import sys
from pathlib import Path

import httpx

from tank.agents.jev import BINDING
from tank.agents.pipeline import Pipeline
from tank.calibration import _CachedConfig, _Ledger, load_packet
from tank.settings import Settings, resolve_versions
from tank.storage import MemoryRepository
from tank.util import iso, sha256

V1_FILTER = "Is this text about crypto - crypto assets, crypto exchanges, crypto regulation or enforcement, blockchain networks, or one of these projects: Bitcoin, Ethereum, Uniswap, PancakeSwap, Aerodrome, Curve, 1inch, 0x, CoW Protocol, Hyperliquid, Lighter, Aave, Morpho, Euler, Ondo, LayerZero, Quant, Avalanche, NEAR, Plasma, Ethena, World (Worldcoin)? Whether it reports anything new or important does not matter here. General finance, technology or politics that does not touch crypto does not count."
V1_ROUTE = "Does the item report the same real-world event as the event described, or a new development of it - a confirmation, denial, stage change, new detail or new party? Another event about the same project does not count."


def v1_state(item):
    return {"source": item.get("source") or item.get("source_name") or item.get("source_id"),
            "author": item.get("author") or item.get("source") or item.get("source_name") or item.get("source_id"),
            "text": "\n\n".join(dict.fromkeys(str(v) for v in (item.get("title"), item.get("text") or item.get("body")) if v)),
            "quoted": item.get("quoted")}


def v1_routing_state(item, event):
    return {**v1_state(item), "event": {"statement": event.get("statement"), "entities": event.get("entities", []),
            "tokens": event.get("tokens_mentioned", event.get("tokens", [])),
            "stage": event.get("event_state", event.get("stage")), "known_to_us_at": event.get("known_to_us_at")}}


def v1_score(usage, item, name, state):
    questions = {name: {"type": "noul", "instructions": V1_FILTER if name == "relevant" else V1_ROUTE,
                        "criteria": {"true": "About crypto" if name == "relevant" else "Same event or a development of it",
                                     "false": "Not about crypto" if name == "relevant" else "A different event"}}}
    key = sha256({"item_id": item["item_id"], "model": BINDING.request_model, "provider": BINDING.provider,
                  "base_url": BINDING.base_url, "prompt_version": "jev-v1", "questions": questions,
                  "state_fingerprint": sha256(state)})
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
                    await repo.put("ops.agent_sessions", e, {"session_id": e, "event_id": e, "last_item_at": iso(last)})
            sel = Pipeline(repo, settings, http=None)
            for ev in await sel._candidate_events(sel.identities(item), iso(row["at"] - window), row["at"]):
                v1 = v1_score(usage, item, "same_event", v1_routing_state(item, events[ev["event_id"]]))
                pairs.append({"item_id": row["item_id"], "event_id": ev["event_id"], "same": ev["event_id"] == row["expected_event_id"], "v1": v1})
            if row.get("event_id"):
                sessions[row["event_id"]] = row["at"]
        filters.append({"item_id": row["item_id"], "relevant": row["relevant"], "filtered": row["filtered"],
                        "v1": v1_score(usage, item, "relevant", v1_state(item))})
    rnd = random.Random(20261020)
    positives = [p for p in pairs if p["same"] and p["v1"] is not None]
    hard = [p for p in pairs if not p["same"] and p["v1"] is not None and p["v1"] >= 0.5]
    easy = rnd.sample([p for p in pairs if not p["same"] and p["v1"] is not None and p["v1"] < 0.5], 20)
    fsel = rnd.sample([f for f in filters if f["filtered"] and f["relevant"] and f["v1"] is not None], 20) + \
        rnd.sample([f for f in filters if f["filtered"] and not f["relevant"] and f["v1"] is not None], 20)
    chosen = positives + hard + easy
    print("route pairs: positives", len(positives), "hard negatives (v1>=0.5)", len(hard), "easy negatives", len(easy),
          "| filter items", len(fsel), "| v2 requests", len(chosen) + len(fsel))
    if "--score" not in sys.argv:
        return
    by_id = {r["item_id"]: r for r in rows}
    async with httpx.AsyncClient(timeout=30) as http:
        pipe = Pipeline(_Ledger(out / "usage"), settings, http=http)
        sem = asyncio.Semaphore(8)

        async def v2(p, name):
            item = by_id[p["item_id"]]["source_item"]
            state = pipe.state(item) if name == "relevant" else pipe.routing_state(item, events[p["event_id"]])
            async with sem:
                p["v2"] = (await pipe._score_record(item, name, state))["score"]

        await asyncio.gather(*(v2(p, "same_event") for p in chosen), *(v2(f, "relevant") for f in fsel))
    out.mkdir(parents=True, exist_ok=True)
    (out / "pairs.json").write_text(json.dumps({"route": chosen, "filter": fsel}, indent=1))
    for version in ("v1", "v2"):
        pos = [p[version] for p in positives]; neg = [p[version] for p in hard + easy]
        auc = sum((a > b) + 0.5 * (a == b) for a in pos for b in neg) / (len(pos) * len(neg))
        print(version, "route: mean same", round(sum(pos) / len(pos), 3), "mean different", round(sum(neg) / len(neg), 3),
              "pairwise separation", round(auc, 3), "| at 0.5: same passed", sum(s >= 0.5 for s in pos), "/", len(pos),
              "different passed", sum(s >= 0.5 for s in neg), "/", len(neg))
        rel = [f[version] for f in fsel if f["relevant"]]; irr = [f[version] for f in fsel if not f["relevant"]]
        print(version, "filter at 0.5: crypto passed", sum(s >= 0.5 for s in rel), "/", len(rel),
              "non-crypto passed", sum(s >= 0.5 for s in irr), "/", len(irr))


asyncio.run(main())
