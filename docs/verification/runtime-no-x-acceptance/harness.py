#!/usr/bin/env python3
"""Review helper, not a public Tank mode. Default validates a plan offline.

The explicit run action starts the actual eight-worker RuntimeSupervisor and can
make model, source, market and Telegram calls. This file has not been run live.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path

TABLES = (
    "ingest.source_items",
    "ops.source_checkpoints",
    "ops.source_dedup",
    "ops.jobs",
    "ops.agent_sessions",
    "ops.tool_calls",
    "evt.events",
    "evt.event_observations",
    "evt.ems_assessments",
    "evt.beneficiary_maps",
    "evt.beneficiary_edges",
    "ana.market_truth",
    "ana.decisions",
    "out.alerts",
    "out.deliveries",
)
SEED_TABLES = ("ingest.source_items", "ops.source_checkpoints", "ops.source_dedup")
CHATS = {"alerts": "-1004408952606", "ops": "-1004481434974"}


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def source_tree_digest(root):
    entries = [(str(path.relative_to(root)), digest(path)) for path in sorted(Path(root).rglob("*.py"))]
    return hashlib.sha256(canonical(entries).encode()).hexdigest()


def check_plan(path):
    if not __debug__:
        raise RuntimeError("Run preparation with assertions enabled (no python -O)")
    plan = json.loads(Path(path).read_text())
    assert plan["scope"] == "REAL_RUNTIME_NO_X_BOUNDED_ACCEPTANCE"
    assert digest(__file__) == plan["harness_sha256"]
    assert (
        source_tree_digest(Path(__file__).resolve().parents[3] / "src/tank") == plan["runtime_source_sha256"]
    )
    assert plan["workers"] == 8 and plan["x_enabled"] is False
    assert plan["database_name"] and plan["database_name"] != "tank"
    assert plan["required_job_types"] == ["FILTER_ITEM", "ROUTE_ITEM"]
    assert plan["telegram_chats"] == CHATS
    assert plan["source_ids"] and "S29" not in plan["source_ids"]
    for field in (
        "initial_run_s",
        "restart_run_s",
        "collector_interval_s",
        "max_new_items",
        "cleanup_wait_s",
    ):
        assert isinstance(plan[field], int) and plan[field] > 0
    for field in ("calibration", "seed", "source_config_diff", "deployment"):
        evidence = plan[field]
        assert digest(evidence["path"]) == evidence["sha256"], f"{field} evidence digest mismatch"
    assert plan["calibration"]["decision"] == "ACCEPTED"
    assert plan["seed"]["origin"] == "PRESERVED_ACTUAL_CAPTURE"
    assert plan["seed"]["import_tables"] == list(SEED_TABLES)
    assert plan["seed"]["excluded_state"] == ["evt", "ana", "out", "jobs", "sessions", "model_outputs"]
    assert plan["target_strategy"] in ("NATURAL_NEW_ITEM", "PREDECLARED_ISOLATED_FIRST_OBSERVATION")
    if plan["target_strategy"] == "PREDECLARED_ISOLATED_FIRST_OBSERVATION":
        assert plan["predeclared_item"]["original_published_at"]
        assert plan["predeclared_item"]["original_raw_sha256"]
        assert plan["predeclared_item"]["excluded_baseline_item_id"]
    return plan


async def snapshot(repo):
    return {table: [row async for row in repo.iter_all(table)] for table in TABLES}


def seed_hash(state):
    # Exact imported rows, including original publisher/observation metadata.
    selected = {table: sorted(state[table], key=canonical) for table in SEED_TABLES}
    return hashlib.sha256(canonical(selected).encode()).hexdigest()


def raw_proof(item, *, raw_root=None, protected=None):
    """Verify decompressed source bytes only inside the declared evidence root."""
    from tank.scheduler import is_protected_evidence_path

    reference = item.get("raw_ref", item.get("raw_path"))
    expected = item.get("raw_sha256")
    if not reference or not expected or raw_root is None:
        return {"verified": False, "reason": "MISSING_RAW_REFERENCE_HASH_OR_ROOT"}
    source = Path(reference)
    path = source.resolve()
    if (
        source.is_symlink()
        or not path.is_relative_to(Path(raw_root).resolve())
        or is_protected_evidence_path(path, protected or {})
    ):
        return {"verified": False, "reason": "UNSAFE_RAW_PATH"}
    try:
        compressed = path.read_bytes()
        if path.suffix == ".zst":
            import zstandard

            payload = zstandard.ZstdDecompressor().decompress(compressed)
        elif path.suffix in {".gz", ".gzip"}:
            import gzip

            payload = gzip.decompress(compressed)
        else:
            payload = compressed
        actual = hashlib.sha256(payload).hexdigest()
        return {
            "verified": actual == expected,
            "reason": None if actual == expected else "RAW_HASH_MISMATCH",
            "raw_sha256": actual,
            "compressed_sha256": hashlib.sha256(compressed).hexdigest(),
        }
    except Exception as exc:
        return {"verified": False, "reason": "RAW_UNREADABLE", "error_type": type(exc).__name__}


def accepted_event_call(call, item, state, event_id):
    """Positive-event branch of integration_extract.completed_extraction."""
    arguments = call.get("arguments", {})
    extraction = arguments.get("extraction", arguments) if isinstance(arguments, dict) else {}
    result = call.get("result")
    return bool(
        call.get("tool", call.get("tool_name")) == "record_event"
        and call.get("status") == "DONE"
        and call.get("returned_at")
        and not call.get("is_error")
        and isinstance(result, dict)
        and "error" not in result
        and result.get("accepted") is not False
        and result.get("ok") is not False
        and result.get("is_event") is not False
        and isinstance(extraction, dict)
        and extraction.get("is_event") is True
        and extraction.get("item_id") == item["item_id"]
        and result.get("event_id") == event_id
        and item.get("extraction_status") == "DONE"
        and any(row.get("event_id") == event_id for row in state["evt.events"])
        and any(
            row.get("event_id") == event_id
            and row.get("item_id") == item["item_id"]
            and row.get("link_type") == "ORIGIN"
            for row in state["evt.event_observations"]
        )
    )


def publisher_latency(item):
    from tank.time_precision import interval_bounds, propagate_timestamp
    from tank.util import parse_time

    stamp = propagate_timestamp(item, "published_at")
    low, high = interval_bounds(stamp)
    result = {
        **stamp.fields("published_at"),
        "observed_at": item.get("observed_at"),
        "observed_at_prec": item.get("observed_at_prec", item.get("observed_at_precision")),
        "value_s": None,
        "min_s": None,
        "max_s": None,
    }
    if low is None or not item.get("observed_at"):
        result["reason"] = "PUBLICATION_TIME_NOT_ESTIMABLE"
        return result
    observed = parse_time(item["observed_at"])
    result.update(min_s=(observed - high).total_seconds(), max_s=(observed - low).total_seconds())
    if stamp.precision == "EXACT":
        result["value_s"] = result["min_s"]
    return result


def link_report(before, after, *, raw_root=None, protected=None):
    """Mechanical receipt linkage. No inferred eligibility or fabricated outcomes."""
    from tank.util import parse_time

    old_items = {row["item_id"] for row in before["ingest.source_items"]}
    new_items = [row for row in after["ingest.source_items"] if row["item_id"] not in old_items]
    tools = [
        row
        for row in after["ops.tool_calls"]
        if row.get("status") == "DONE"
        and row.get("returned_at")
        and not row.get("is_error")
        and isinstance(row.get("result"), dict)
        and "error" not in row["result"]
        and row["result"].get("accepted") is not False
        and row["result"].get("ok") is not False
    ]
    chains = []
    for item in new_items:
        item_id = item["item_id"]
        observations = [row for row in after["evt.event_observations"] if row.get("item_id") == item_id]
        raw = raw_proof(item, raw_root=raw_root, protected=protected)
        jobs = [row for row in after["ops.jobs"] if row.get("payload", {}).get("item_id") == item_id]
        filter_jobs = [
            row for row in jobs if row.get("type") == "FILTER_ITEM" and row.get("status") == "DONE"
        ]
        relevance = item.get("relevance", {}).get("decision")
        always = relevance == "ALWAYS"
        filtered_source = item.get("source_id") in {"S30", "S31", "S32", "S33", "S34"} or (
            item.get("source_id") == "S25" and item.get("subtype") == "commits"
        )
        filter_complete = (always and not filtered_source) or (bool(filter_jobs) and relevance == "SEND")
        filter_evidence = {
            "source_policy": "FILTER_REQUIRED" if filtered_source or not always else "ALWAYS",
            "relevance_decision": relevance,
            "filter_job_receipts": filter_jobs,
            "satisfied": filter_complete,
        }
        for observation in observations:
            event_id = observation["event_id"]
            sessions = {
                row["session_id"] for row in after["ops.agent_sessions"] if row.get("event_id") == event_id
            }
            records = [
                row
                for row in tools
                if row.get("session_id") in sessions and accepted_event_call(row, item, after, event_id)
            ]
            cards = [row for row in after["out.alerts"] if row.get("event_id") == event_id]
            for card in cards:
                decisions = [
                    row
                    for row in after["ana.decisions"]
                    if row.get("dec_id") in card.get("decisions", []) and row.get("event_id") == event_id
                ]
                truths = [
                    row
                    for row in after["ana.market_truth"]
                    if row.get("mta_id")
                    in {decision.get("market_truth_id", decision.get("mta_id")) for decision in decisions}
                    and row.get("event_id") == event_id
                ]
                sent = [
                    row
                    for row in after["out.deliveries"]
                    if row.get("alert_id") == card["alert_id"]
                    and row.get("version") == card.get("version")
                    and row.get("status") == "SENT"
                    and row.get("channel") == "telegram:alerts"
                    and row.get("message_ref")
                ]
                publishes = [
                    row
                    for row in tools
                    if row.get("session_id") in sessions
                    and row.get("tool", row.get("tool_name")) == "publish_card"
                    and row["result"].get("published") is True
                    and row["result"].get("alert_id") == card["alert_id"]
                    and row["result"].get("version") == card.get("version")
                ]

                def seconds(end, start):
                    return (parse_time(end) - parse_time(start)).total_seconds() if end and start else None

                chains.append(
                    {
                        "item_id": item_id,
                        "source_id": item.get("source_id"),
                        "event_id": event_id,
                        "raw_ref": item.get("raw_ref", item.get("raw_path")),
                        "raw_proof": raw,
                        "job_receipts": jobs,
                        "filter_evidence": filter_evidence,
                        "record_event_receipts": records,
                        "truth_versions": truths,
                        "decision_versions": decisions,
                        "publish_card_receipts": publishes,
                        "card": card,
                        "sent_receipts": sent,
                        "reply_receipts": [
                            {
                                "message_ref": row.get("message_ref"),
                                "requested_target": row.get("request_reply_to_message_ref"),
                                "returned_target": row.get("response_reply_to_message_ref"),
                                "returned_target_recorded": row.get("response_reply_to_message_ref")
                                is not None,
                            }
                            for row in sent
                        ],
                        "complete_receipt_chain": bool(
                            card.get("kind") == "FULL"
                            and raw["verified"]
                            and filter_complete
                            and any(
                                job.get("type") == "ROUTE_ITEM" and job.get("status") == "DONE"
                                for job in jobs
                            )
                            and records
                            and truths
                            and decisions
                            and {row["dec_id"] for row in decisions} == set(card.get("decisions", []))
                            and {row["mta_id"] for row in truths}
                            == {
                                decision.get("market_truth_id", decision.get("mta_id"))
                                for decision in decisions
                            }
                            and publishes
                            and sent
                        ),
                        "latency_s": {
                            "observed_to_accepted_record_event": [
                                seconds(
                                    row.get("completed_at", row.get("answered_at")), item.get("observed_at")
                                )
                                for row in records
                            ],
                            "publisher_to_observed": publisher_latency(item),
                            "observed_to_card": seconds(card.get("generated_at"), item.get("observed_at")),
                            "observed_to_sent": [
                                seconds(row.get("at"), item.get("observed_at")) for row in sent
                            ],
                            "generated_to_sent": [
                                seconds(row.get("at"), card.get("generated_at")) for row in sent
                            ],
                        },
                    }
                )
    new_ids = {item["item_id"] for item in new_items}
    return {
        "new_item_job_receipts": [
            row for row in after["ops.jobs"] if row.get("payload", {}).get("item_id") in new_ids
        ],
        "new_items": len(new_items),
        "chains": chains,
        "complete_chains": sum(bool(chain["complete_receipt_chain"]) for chain in chains),
    }


def safe_evidence(value, settings, secrets=()):
    from tank.agents.security import redact_tool_arguments
    from tank.logging import redact_text

    safe, refusal = redact_tool_arguments(value, settings)
    rendered = json.dumps(safe, indent=2, default=str)
    scrubbed = redact_text(rendered, secrets)
    if refusal or scrubbed != rendered:
        raise ValueError("Credential-bearing acceptance evidence refused")
    return scrubbed


def duplicates(state):
    receipts = {}
    for row in state["out.deliveries"]:
        if row.get("status") == "SENT" and row.get("alert_id"):
            key = (row["alert_id"], row.get("version"), row.get("channel"))
            receipts.setdefault(key, []).append(row.get("message_ref"))
    return [
        {"alert_id": key[0], "version": key[1], "channel": key[2], "message_refs": refs}
        for key, refs in receipts.items()
        if len(refs) > 1
    ]


async def session_catalog(service):
    """Read the ordinary paginated OpenMA catalog, not a guessed native API."""
    rows, page, seen = [], None, set()
    while True:
        params = {"limit": 100}
        if page:
            params["page"] = page
        listing = await service.request("GET", "sessions", params=params)
        rows.extend(
            {
                "id": row.get("id"),
                "status": row.get("status"),
                "tank_identity": row.get("metadata", {}).get("tank_identity"),
            }
            for row in listing.get("data", [])
        )
        page = listing.get("next_page")
        if not page:
            return rows
        if page in seen:
            raise ValueError("Repeated OpenMA catalog page")
        seen.add(page)


async def cleanup_owned(repo, settings, before, initial_catalog, *, wait_s):
    """Test-only remote resource disposal AFTER final snapshots, never local mutations."""
    from tank.agents.sessions import SessionService
    from tank.integration_extract import session_status
    from tank.util import sha256

    service = SessionService(repo, settings, None)
    receipts = []
    result = {
        "sessions": receipts,
        "native_catalog_status": "PENDING_CONFIGURATION_HANDOFF",
        "native_catalog_note": "OpenMA session catalog is not the BoxLite native catalog. Configuration must attest owned QA native sessions stopped; do not touch unrelated sessions.",
    }
    try:
        after = await snapshot(repo)
        baseline_ids = {row["session_id"] for row in (before or {}).get("ops.agent_sessions", [])}
        remote_before_ids = {row["id"] for row in initial_catalog}
        new_items = {row["item_id"] for row in after["ingest.source_items"]} - {
            row["item_id"] for row in (before or {}).get("ingest.source_items", [])
        }
        identities = {(item_id, False) for item_id in new_items}
        identities.update(
            (row["event_id"], False)
            for row in after["evt.events"]
            if any(
                obs.get("event_id") == row["event_id"] and obs.get("item_id") in new_items
                for obs in after["evt.event_observations"]
            )
        )
        identities.update(
            (row["payload"]["prp_id"], True)
            for row in after["ops.jobs"]
            if row.get("type") == "SCOUT_REVIEW" and row.get("payload", {}).get("prp_id")
        )
        markers = {sha256({"identity": identity, "scout": scout}) for identity, scout in identities}
        # A response can be lost before local session persistence: exact existing
        # production metadata markers reconcile only this QA run's new identities.
        try:
            catalog = await session_catalog(service)
        except Exception as exc:
            result["catalog_error_type"] = type(exc).__name__
            catalog = []
        owned = {
            row["session_id"]
            for row in after["ops.agent_sessions"]
            if row["session_id"] not in baseline_ids and row["session_id"] not in remote_before_ids
        }
        owned.update(
            row["id"]
            for row in catalog
            if row["id"] not in remote_before_ids and row.get("tank_identity") in markers
        )
        result["reused_preexisting_session_ids"] = sorted(
            {
                row["session_id"]
                for row in after["ops.agent_sessions"]
                if row["session_id"] in remote_before_ids and row["session_id"] not in baseline_ids
            }
        )
        result["owned_session_ids"] = sorted(owned)
        for sid in sorted(owned):
            receipt = {
                "session_id": sid,
                "ownership": "NEW_QA_LOCAL_MAPPING_OR_EXACT_TANK_IDENTITY",
                "requests": [],
            }
            receipts.append(receipt)
            try:
                remote = await service.request("GET", f"sessions/{sid}")
                receipt["initial_remote_status"] = session_status(remote)
                if session_status(remote) not in {"idle", "terminated"}:
                    response = await service.request(
                        "POST", f"sessions/{sid}/events", json={"events": [{"type": "user.interrupt"}]}
                    )
                    receipt["requests"].append(
                        {"method": "POST", "path": f"sessions/{sid}/events", "response": response}
                    )
                    deadline = asyncio.get_running_loop().time() + wait_s
                    while asyncio.get_running_loop().time() < deadline:
                        remote = await service.request("GET", f"sessions/{sid}")
                        if session_status(remote) in {"idle", "terminated"}:
                            break
                        await asyncio.sleep(1)
                response = await service.request("POST", f"sessions/{sid}/archive", json={})
                receipt["requests"].append(
                    {"method": "POST", "path": f"sessions/{sid}/archive", "response": response}
                )
                receipt["archive_acknowledged"] = True
                remote = await service.request("GET", f"sessions/{sid}")
                receipt["final_remote_status"] = session_status(remote)
            except Exception as exc:
                receipt.update(archive_acknowledged=False, error_type=type(exc).__name__)
        result["openma_catalog_after_cleanup"] = await session_catalog(service)
    except Exception as exc:
        result["cleanup_error_type"] = type(exc).__name__
    finally:
        await service.close()
    return result


async def run(plan, output):
    from urllib.parse import unquote, urlsplit

    import tank
    from tank.logging import configure_logging
    from tank.runtime import RuntimeSupervisor
    from tank.scheduler import protected_evidence_paths
    from tank.settings import Settings, resolve_versions
    from tank.storage import PostgresRepository
    from tank.util import iso

    assert source_tree_digest(Path(tank.__file__).resolve().parent) == plan["runtime_source_sha256"], (
        "Imported package differs from frozen runtime source"
    )
    settings = resolve_versions(Settings(_env_file=None))
    configure_logging(settings)
    assert settings.workers == 8 and settings.x_enabled is False
    assert unquote(urlsplit(settings.database_url).path.lstrip("/")) == plan["database_name"]
    assert str(settings.telegram_alerts_chat) == CHATS["alerts"]
    assert str(settings.telegram_ops_chat) == CHATS["ops"]
    assert {row["source_id"] for row in settings.load_config("sources")["sources"]} == set(plan["source_ids"])
    thresholds = settings.load_config("thresholds")
    for field in ("filter_cutoff", "routing_cutoff"):
        assert thresholds[field] == plan["calibration"][field] and thresholds[field] is not None
    assert settings.config_version == plan["config_version"] and settings.code_version == plan["code_version"]
    repo = PostgresRepository(settings.database_url)
    await repo.open()
    output.mkdir(parents=True, exist_ok=False)
    secrets = tuple(
        str(getattr(settings, name))
        for name in (
            "openma_api_key",
            "typesafe_api_key",
            "openrouter_api_key",
            "telegram_token",
            "codex_gateway_key",
            "etherscan_api_key",
            "database_url",
            "openma_database_url",
        )
        if getattr(settings, name, None)
    )

    def save(name, value):
        (output / name).write_text(safe_evidence(value, settings, secrets))

    report = {
        "status": "INCOMPLETE",
        "plan_sha256": hashlib.sha256(canonical(plan).encode()).hexdigest(),
        "harness_sha256": digest(__file__),
        "runtime_source_sha256": plan["runtime_source_sha256"],
        "started_at": iso(),
        "config_version": settings.config_version,
        "code_version": settings.code_version,
        "limitations": [
            "Finite declared source scope; no-X.",
            "A run bound is not a specification latency gate.",
            "Historical isolated first-observation cases cannot establish live-news latency.",
        ],
    }
    protected = protected_evidence_paths(settings)
    before = None
    initial_catalog = None
    try:
        before = await snapshot(repo)
        assert seed_hash(before) == plan["seed"]["baseline_sha256"], (
            "Actual seed differs from frozen baseline"
        )
        assert all(not before[table] for table in TABLES if table not in SEED_TABLES), (
            "Acceptance database contains derived/model/output state"
        )
        assert all(
            raw_proof(row, raw_root=settings.data_dir, protected=protected)["verified"]
            for row in before["ingest.source_items"]
        ), "Seed raw byte/hash verification failed"
        seed_items = {row["item_id"] for row in before["ingest.source_items"]}
        assert all(row.get("item_id") in seed_items for row in before["ops.source_dedup"]), (
            "Dangling source dedup entry"
        )
        assert {row["source_id"] for row in before["ingest.source_items"]} == set(plan["source_ids"]), (
            "Seed does not cover declared source scope"
        )
        save("before.json", before)
        from tank.agents.sessions import SessionService

        catalog_service = SessionService(repo, settings, None)
        try:
            initial_catalog = await session_catalog(catalog_service)
            save("openma-catalog-before.json", initial_catalog)
        finally:
            await catalog_service.close()
        phases = []
        for label, duration in (("initial", plan["initial_run_s"]), ("restart", plan["restart_run_s"])):
            stop, reload_requested = asyncio.Event(), asyncio.Event()
            supervisor = RuntimeSupervisor(repo, settings)
            task = asyncio.create_task(supervisor.run(stop, reload_requested))
            phase = {"phase": label, "started_at": iso(), "duration_bound_s": duration}
            deadline = asyncio.get_running_loop().time() + duration
            try:
                while asyncio.get_running_loop().time() < deadline:
                    if task.done():
                        await task
                        raise RuntimeError("Runtime stopped before the declared bound")
                    state = await snapshot(repo)
                    if (
                        link_report(before, state, raw_root=settings.data_dir, protected=protected)[
                            "new_items"
                        ]
                        > plan["max_new_items"]
                    ):
                        raise RuntimeError("Declared new-item safety bound exceeded; preserve queued work")
                    await asyncio.sleep(
                        min(
                            plan["collector_interval_s"], max(0, deadline - asyncio.get_running_loop().time())
                        )
                    )
            finally:
                stop.set()
                await task  # Ordinary runtime drain/recovery, no direct job/session mutations.
                phase["stopped_at"] = iso()
                phases.append(phase)
                state = await snapshot(repo)
                save(label + ".json", state)
                save(
                    label + "-chains.json",
                    link_report(before, state, raw_root=settings.data_dir, protected=protected),
                )
        after = await snapshot(repo)
        chains = link_report(before, after, raw_root=settings.data_dir, protected=protected)
        duplicate_sends = duplicates(after)
        completed_kinds = {
            row["type"] for row in chains["new_item_job_receipts"] if row.get("status") == "DONE"
        }
        report.update(
            completed_required_job_types=sorted(completed_kinds.intersection(plan["required_job_types"])),
            phases=phases,
            complete_chains=chains["complete_chains"],
            duplicate_sends=duplicate_sends,
            status="RECEIPTS_COMPLETE_REVIEW_REQUIRED"
            if chains["complete_chains"]
            and not duplicate_sends
            and set(plan["required_job_types"]).issubset(completed_kinds)
            else "FAIL_OR_INCOMPLETE",
        )
        save("chains.json", chains)
    except BaseException as exc:
        report.update(status="FAILED_OR_INTERRUPTED", error_type=type(exc).__name__)
        raise
    finally:
        # All acceptance snapshots above precede this explicit QA-only cleanup.
        # No cleanup runs between generations, and no accepted local rows change.
        try:
            try:
                save("final-before-cleanup.json", await snapshot(repo))
            except Exception as exc:
                report.update(status="FAILED_EVIDENCE_CAPTURE", final_evidence_error_type=type(exc).__name__)
            if initial_catalog is not None:
                cleanup = await cleanup_owned(
                    repo, settings, before, initial_catalog, wait_s=plan["cleanup_wait_s"]
                )
                save("cleanup.json", cleanup)
                report["cleanup"] = cleanup
                if (
                    cleanup.get("cleanup_error_type")
                    or cleanup.get("catalog_error_type")
                    or cleanup.get("reused_preexisting_session_ids")
                    or any(not row.get("archive_acknowledged") for row in cleanup["sessions"])
                ):
                    report["status"] = "FAILED_CLEANUP_OR_ISOLATION"
            else:
                report["cleanup"] = {"status": "NO_RUNTIME_STARTED_INITIAL_CATALOG_NOT_CAPTURED"}
        finally:
            report["finished_at"] = iso()
            try:
                save("report.json", report)
            finally:
                await repo.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "run"], nargs="?", default="prepare")
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    plan = check_plan(args.plan)
    if args.action == "prepare":
        print(
            json.dumps(
                {
                    "status": "PLAN_VALIDATED_OFFLINE_NOT_EXECUTED",
                    "source_ids": plan["source_ids"],
                    "workers": 8,
                }
            )
        )
    else:
        if args.output is None:
            parser.error("run requires a new --output directory")
        try:
            asyncio.run(run(plan, args.output))
        except BaseException as exc:
            print(json.dumps({"status": "RUN_FAILED_OR_INTERRUPTED", "error_type": type(exc).__name__}))
            raise SystemExit(1) from None


if __name__ == "__main__":
    main()
