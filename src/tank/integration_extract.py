"""Bounded live component probe: actual captured items -> OpenMA -> validated tools.

This is intentionally separate from `serve`: it makes no routing decisions and
runs no delivery worker. Provision its isolated database and sandbox DB route
before using it. The normal analyst prompt and actual tools remain unchanged.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import shutil
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import httpx
import zstandard

from tank.agents.security import redact_tool_arguments
from tank.agents.sessions import SessionService
from tank.analysis.decisions import DecisionEngine
from tank.analysis.truth import MarketTruthService
from tank.bootstrap import sync_registry
from tank.codex_gateway import CodexGateway
from tank.event_tools import EventToolService
from tank.logging import configure_logging
from tank.market.service import MarketService
from tank.settings import Settings, resolve_versions
from tank.sources.legacy import needs_source_metadata_repair, normalize_legacy_source_record
from tank.storage import PostgresRepository
from tank.util import iso, sha256


def captured_items(path, item_ids):
    if not 1 <= len(item_ids) <= 3 or len(set(item_ids)) != len(item_ids):
        raise ValueError("Approve one to three distinct real initial items")
    rows = {row["item_id"]: row for row in map(json.loads, Path(path).read_text().splitlines())}
    selected = []
    for item_id in item_ids:
        row = rows[item_id]
        if any(not row.get(field) for field in ("source_id", "content_hash", "raw_sha256", "observed_at")):
            raise ValueError("Captured item lacks source/hash/observation provenance")
        if "published_at" not in row:
            raise ValueError("Preserve explicit original publication timestamp or null")
        selected.append(row)
    return selected


def stage_raw_items(items, output, payload_root=None, *, sources=None):
    staged, relocations = [], []
    raw_dir = Path(output) / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    for original in items:
        source = Path(original.get("raw_ref") or "__missing_capture__")
        if not source.is_file() and payload_root is not None:
            matches = list(Path(payload_root).rglob(source.name))
            if len(matches) == 1:
                source = matches[0]
        if not source.is_file():
            raise ValueError("Actual source raw payload is unavailable before model calls")
        compressed = source.read_bytes()
        payload = (
            zstandard.ZstdDecompressor().decompress(compressed) if source.suffix == ".zst" else compressed
        )
        if hashlib.sha256(payload).hexdigest() != original["raw_sha256"]:
            raise ValueError("Actual raw payload hash differs from captured SourceItem")
        compressed_hash = hashlib.sha256(compressed).hexdigest()
        target = raw_dir / (compressed_hash + (".zst" if source.suffix == ".zst" else ".bin"))
        if target.exists() and target.read_bytes() != compressed:
            raise ValueError("Content-addressed raw staging collision")
        if not target.exists():
            shutil.copyfile(source, target)
        row = {**original, "original_raw_ref": original["raw_ref"], "raw_ref": str(target.resolve())}
        if needs_source_metadata_repair(row):
            feeds = [
                feed
                for source in sources or []
                if source.get("source_id") == row["source_id"]
                for feed in source.get("feeds", [])
                if feed.get("url") == row.get("feed")
            ]
            if len(feeds) != 1:
                raise ValueError("Legacy source metadata requires one matching configured feed")
            row = normalize_legacy_source_record(
                row,
                payload,
                feed=feeds[0],
                raw_ref=str(target.resolve()),
                raw_sha256=original["raw_sha256"],
            )
            if row["source_metadata_repair"]["outcome"] != "VERIFIED":
                raise ValueError("Legacy source publication metadata is unverified before model calls")
        staged.append(row)
        relocations.append(
            {
                "item_id": row["item_id"],
                "original_raw_ref": original["raw_ref"],
                "staged_raw_ref": row["raw_ref"],
                "raw_sha256": row["raw_sha256"],
                "compressed_sha256": compressed_hash,
            }
        )
    return staged, relocations


def session_status(response):
    data = response.get("data")
    return (data if isinstance(data, dict) else response).get("status")


def isolated_database(settings, expected_database):
    actual = urlparse(settings.database_url).path.lstrip("/")
    if not expected_database.startswith("tank_integration_") or actual != expected_database:
        raise ValueError("Require the explicitly provisioned tank_integration_* database")
    return actual


def probe_settings(settings, expected_database):
    if (
        not expected_database.startswith("tank_integration_")
        or not expected_database.replace("_", "").isalnum()
    ):
        raise ValueError("Require an explicitly named tank_integration_* database")
    parsed = urlparse(settings.database_url)
    if parsed.scheme not in {"postgresql", "postgres"}:
        raise ValueError("Integration probe requires PostgreSQL")
    return settings.model_copy(
        update={"database_url": urlunparse(parsed._replace(path="/" + expected_database))}
    )


async def remote_log(sessions, sid):
    events, page, seen = [], None, set()
    while True:
        params = {"limit": 100, "order": "asc"}
        if page:
            params["page"] = page
        response = await sessions.request("GET", f"sessions/{sid}/events", params=params)
        events.extend(response.get("data", []))
        page = response.get("next_page")
        if not page:
            return events
        if page in seen:
            raise ValueError("OpenMA repeated an event page token")
        seen.add(page)


async def completed_extraction(repo, calls, item_id):
    """Require a returned successful extraction and its matching durable state."""
    for call in calls:
        if (
            call.get("tool") != "record_event"
            or call.get("status") != "DONE"
            or not call.get("returned_at")
            or call.get("is_error")
        ):
            continue
        arguments = call.get("arguments", {})
        extraction = arguments.get("extraction", arguments)
        result = call.get("result")
        if (
            extraction.get("item_id") != item_id
            or not isinstance(result, dict)
            or result.get("error")
            or result.get("accepted") is False
            or result.get("ok") is False
        ):
            continue
        item = await repo.get("ingest.source_items", item_id)
        if not item:
            continue
        if result.get("is_event") is False:
            if extraction.get("is_event") is False and item.get("extraction_status") == "SKIPPED":
                return call
            continue
        event_id = result.get("event_id")
        if (
            not event_id
            or extraction.get("is_event") is not True
            or item.get("extraction_status") != "DONE"
            or not await repo.get("evt.events", event_id)
        ):
            continue
        async for observation in repo.iter_all(
            "evt.event_observations", filters={"event_id": event_id, "item_id": item_id}
        ):
            if observation.get("link_type") == "ORIGIN":
                return call
    return None


async def interrupt_owned(repo, sessions, item_ids):
    """Reconcile sessions persisted before a failed send_message returned its ID."""
    outcomes = []
    for item_id in item_ids:
        async for mapping in repo.iter_all("ops.agent_sessions", filters={"item_id": item_id}):
            sid = mapping["session_id"]
            try:
                remote = await sessions.request("GET", f"sessions/{sid}")
                if session_status(remote) not in {"idle", "terminated"}:
                    await sessions.request(
                        "POST", f"sessions/{sid}/events", json={"events": [{"type": "user.interrupt"}]}
                    )
                    for _ in range(30):
                        remote = await sessions.request("GET", f"sessions/{sid}")
                        if session_status(remote) in {"idle", "terminated"}:
                            break
                        await asyncio.sleep(1)
                await sessions.request("POST", f"sessions/{sid}/archive", json={})
                outcomes.append(
                    {"session_id": sid, "remote_status": session_status(remote), "archived": True}
                )
            except (httpx.HTTPError, OSError, ValueError) as error:
                outcomes.append({"session_id": sid, "interrupt_error_type": type(error).__name__})
    return outcomes


async def extract(
    raw_path, item_ids, output_dir, expected_database, *, settings=None, timeout=240, payload_root=None
):
    if not 1 <= timeout <= 600:
        raise ValueError("Bounded probe timeout must be between 1 and 600 seconds")
    settings = resolve_versions(probe_settings(settings or Settings(), expected_database))
    isolated_database(settings, expected_database)
    items = captured_items(raw_path, item_ids)
    configure_logging(settings)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    original_hashes = {item["item_id"]: sha256(item) for item in items}
    items, relocations = stage_raw_items(
        items, output, payload_root, sources=settings.load_config("sources").get("sources", [])
    )
    manifest = {
        "scope": "Actual component integration; extraction only; deliberately interrupted after returned record_event",
        "started_at": iso(),
        "database": expected_database,
        "item_ids": item_ids,
        "raw_relocations": relocations,
        "source_metadata_repairs": {
            item["item_id"]: item["source_metadata_repair"]
            for item in items
            if item.get("source_metadata_repair")
        },
        "original_source_item_hashes": original_hashes,
        "input_integrity_scope": "Exact selected SourceItem packet hashes and decompressed/raw payload hashes verified; content_hash retained from source capture, not recomputed from parsed fields (canonical ingest calculation is inline).",
        "source_item_hashes": {item["item_id"]: sha256(item) for item in items},
        "code_version": settings.code_version,
        "config_version": settings.config_version,
        "environment_id": settings.load_config("openma")["environment_id"],
        "routing": "Not invoked; no cutoff supplied",
        "delivery": "No delivery worker is started",
        "sessions": [],
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    repo = await PostgresRepository(settings.database_url).open()
    gateway = CodexGateway(settings)
    market = MarketService(repo, settings)
    truth = MarketTruthService(repo, settings, market)
    tools = EventToolService(repo, settings, truth, DecisionEngine(settings))
    sessions = SessionService(repo, settings, tools.dispatch)
    active_sid = None
    owned_items = set()
    try:
        await repo.migrate()
        manifest["registry_sync"] = await sync_registry(repo, settings)
        await gateway.start()
        for item in items:
            prior = await repo.get("ingest.source_items", item["item_id"])
            if prior is not None:
                raise ValueError("Use a fresh integration database; approved item already exists")
            await repo.put("ingest.source_items", item["item_id"], item)
            owned_items.add(item["item_id"])
            deadline = asyncio.get_running_loop().time() + timeout
            session = await asyncio.wait_for(
                sessions.send_message({"kind": "NEW_ITEM", "item_id": item["item_id"]}), timeout=timeout
            )
            active_sid = session["session_id"]
            extraction = None
            while asyncio.get_running_loop().time() < deadline:
                calls = [
                    row async for row in repo.iter_all("ops.tool_calls", filters={"session_id": active_sid})
                ]
                extraction = await completed_extraction(repo, calls, item["item_id"])
                if extraction:
                    break
                current = await repo.get("ops.agent_sessions", active_sid)
                if current and current.get("status") == "FAILED":
                    break  # Ordinary session retry has already exhausted; preserve the terminal failure promptly.
                await asyncio.sleep(0.5)
            remote = await sessions.request("GET", f"sessions/{active_sid}")
            if session_status(remote) not in {"idle", "terminated"}:
                await sessions.request(
                    "POST", f"sessions/{active_sid}/events", json={"events": [{"type": "user.interrupt"}]}
                )
            for _ in range(30):
                remote = await sessions.request("GET", f"sessions/{active_sid}")
                if session_status(remote) in {"idle", "terminated"}:
                    break
                await asyncio.sleep(1)
            result = {
                "item_id": item["item_id"],
                "session_id": active_sid,
                "extraction_completed": extraction is not None,
                "interrupt_remote_status": session_status(remote) if remote else None,
                "remote_events": await remote_log(sessions, active_sid),
                "source_item": await repo.get("ingest.source_items", item["item_id"]),
                "session": await repo.get("ops.agent_sessions", active_sid),
                "tool_calls": [
                    row async for row in repo.iter_all("ops.tool_calls", filters={"session_id": active_sid})
                ],
                "llm_usage": [
                    row async for row in repo.iter_all("ops.llm_usage", filters={"session_id": active_sid})
                ],
                "event": None,
            }
            if result["session"].get("event_id"):
                result["event"] = await repo.get("evt.events", result["session"]["event_id"])
                result["observations"] = [
                    row
                    async for row in repo.iter_all(
                        "evt.event_observations", filters={"event_id": result["session"]["event_id"]}
                    )
                ]
            safe, refusal = redact_tool_arguments(result, settings)
            if refusal:
                raise ValueError("Credential in integration evidence refused")
            (output / (active_sid + ".json")).write_text(json.dumps(safe, indent=2) + "\n")
            manifest["sessions"].append(
                {
                    "session_id": active_sid,
                    "item_id": item["item_id"],
                    "extraction_completed": extraction is not None,
                    "event_id": result["session"].get("event_id"),
                }
            )
            (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
            active_sid = None
            if extraction is None:
                raise TimeoutError(
                    "No actual returned record_event before terminal failure or probe deadline; evidence preserved"
                )
        manifest["completed_at"] = iso()
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        return manifest
    finally:
        manifest["cleanup_sessions"] = await interrupt_owned(repo, sessions, sorted(owned_items))
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        await sessions.close()
        await market.close()
        await gateway.close()
        await repo.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw_items")
    parser.add_argument("output")
    parser.add_argument("--item-id", action="append", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--timeout", type=int, default=240)
    parser.add_argument("--payload-root", default=None)
    args = parser.parse_args()
    result = asyncio.run(
        extract(
            args.raw_items,
            args.item_id,
            args.output,
            args.database,
            timeout=args.timeout,
            payload_root=args.payload_root,
        )
    )
    print(json.dumps({"sessions": result["sessions"], "completed_at": result["completed_at"]}))


if __name__ == "__main__":
    main()
