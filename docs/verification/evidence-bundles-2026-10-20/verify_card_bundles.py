"""Check that every card version is frozen with its evidence (01 §7.3, 03 §11, HLD §2 rule 8).

Read-only. For each out.alerts version:
1. the evidence bundle row exists, and sha256(canonical payload) equals the alert's
   evidence_bundle_id and the row's sha256;
2. the bundle file `bundles/{sha}.json.zst`, when present under --data-dir, decompresses
   to the same hash;
3. the bundle holds what 01 §7.3 lists: source items and quotes, market data per venue
   and window, exclusions with reasons, scores and their inputs, and settings and
   analysis versions;
4. the card text is reproduced by tank.analysis.cards.compose_card from the frozen
   inputs (card_struct), with the verdict and WHY of the publish_card call recorded in
   the bundle's request_log;
5. the delivery ledger has rows for the version.
Usage: TANK_DATABASE_URL=... python verify_card_bundles.py [--data-dir DIR] > report.json
"""

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path

import zstandard

from tank.analysis.cards import compose_card
from tank.storage import PostgresRepository
from tank.util import sha256


def walk(value, key):
    """Every value stored under `key` anywhere in a nested structure."""
    if isinstance(value, dict):
        for k, v in value.items():
            if k == key:
                yield v
            yield from walk(v, key)
    elif isinstance(value, list):
        for v in value:
            yield from walk(v, key)


def contents(payload):
    items = payload.get("source_items") or []
    item_ids = {i.get("item_id") for i in items}
    quotes = [
        e
        for c in payload.get("claims") or payload.get("event", {}).get("claims") or []
        for e in c.get("evidence", [])
    ]
    truths = payload.get("market_truth") or []
    decisions = payload.get("decisions") or []
    exclusions = [x for k in ("excluded", "exclusions", "excluded_venues") for x in walk(truths, k)]
    return {
        "source_items": len(items),
        "source_items_raw_sha256": all(i.get("raw_sha256") for i in items) and bool(items),
        "quotes": len(quotes),
        "quotes_point_to_frozen_items": bool(quotes) and all(q.get("item_id") in item_ids for q in quotes),
        "market_truth": len(truths),
        "market_truth_per_decision": all(
            any(t.get("mta_id") == d.get("mta_id") for t in truths) for d in decisions
        ),
        "market_windows": sorted({t.get("primary_window") for t in truths if t.get("primary_window")}),
        "venue_detail": all(t.get("spot") is not None and t.get("derivatives") is not None for t in truths),
        "exclusion_fields": len(exclusions),
        "scores": {
            "ems": bool(payload.get("ems")),
            "outlooks": len(payload.get("outlooks") or []),
            "decisions": len(decisions),
            "grades_with_rules": all(
                d.get("thesis_grade") and d.get("thesis_rule") and d.get("trade_rule") for d in decisions
            ),
        },
        "versions": {
            "config_versions": payload.get("config_versions"),
            "code_versions": payload.get("code_versions"),
            "agent_version": payload.get("agent_version"),
            "prompt_versions": payload.get("prompt_versions"),
            "model_ids": payload.get("model_ids"),
            "thresholds_versions": sorted({str(d.get("thresholds_version")) for d in decisions}),
        },
        "unresolved_fields": payload.get("unresolved_fields"),
    }


def reproduce(alert, payload):
    struct = alert.get("card_struct") or {}
    calls = [
        r.get("arguments") or {}
        for r in payload.get("request_log") or []
        if r.get("tool") == "publish_card" and (r.get("arguments") or {}).get("kind", "FULL") == alert["kind"]
    ]
    for args in calls:
        text = compose_card(
            struct.get("event"),
            struct.get("assessment"),
            struct.get("beneficiaries"),
            struct.get("decisions"),
            struct.get("market_truths"),
            kind=alert["kind"],
            verdict=args.get("verdict", ""),
            why=args.get("why"),
        )
        if text == alert["card_text"]:
            return {"reproduced": True, "publish_card_calls": len(calls)}
    return {"reproduced": False, "publish_card_calls": len(calls)}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path)
    options = parser.parse_args()
    repo = await PostgresRepository(os.environ["TANK_DATABASE_URL"]).open()
    report = []
    try:
        async for alert in repo.iter_all("out.alerts"):
            digest = alert.get("evidence_bundle_id")
            row = await repo.get("out.evidence_bundles", digest) if digest else None
            payload = (row or {}).get("payload")
            entry = {
                "alert_id": alert["alert_id"],
                "version": alert["version"],
                "kind": alert["kind"],
                "bundle": digest,
                "bundle_row": row is not None,
                "hash_valid": bool(payload) and sha256(payload) == digest == row.get("sha256"),
            }
            if options.data_dir and digest:
                path = options.data_dir / "bundles" / (digest + ".json.zst")
                if path.is_file():
                    body = zstandard.ZstdDecompressor().stream_reader(path.open("rb")).read()
                    entry["file_hash_valid"] = hashlib.sha256(body).hexdigest() == digest
                else:
                    entry["file_hash_valid"] = None
            if payload:
                entry["contents"] = contents(payload)
                entry["card"] = reproduce(alert, payload)
            entry["deliveries"] = len(
                await repo.list(
                    "out.deliveries", filters={"alert_id": alert["alert_id"], "version": alert["version"]}
                )
            )
            c = entry.get("contents", {})
            entry["frozen_with_evidence"] = bool(
                entry["hash_valid"]
                and entry.get("file_hash_valid") is not False
                and c.get("source_items_raw_sha256")
                and c.get("quotes_point_to_frozen_items")
                and c.get("market_truth_per_decision")
                and c.get("venue_detail")
                and c.get("scores", {}).get("ems")
                and c.get("scores", {}).get("grades_with_rules")
                and c.get("versions", {}).get("config_versions")
                and c.get("versions", {}).get("code_versions")
                and entry.get("card", {}).get("reproduced")
                and entry["deliveries"] > 0
            )
            report.append(entry)
    finally:
        await repo.close()
    print(
        json.dumps(
            {
                "cards": len(report),
                "all_frozen_with_evidence": bool(report) and all(r["frozen_with_evidence"] for r in report),
                "versions": report,
            },
            indent=2,
            default=str,
        )
    )


asyncio.run(main())
