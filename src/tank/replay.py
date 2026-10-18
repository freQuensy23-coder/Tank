"""Offline replay boundaries. Sealed evaluator data never enters ReplayInputs.

Runners must use supplied readers and HTTP transport, never production adapters.
A packet without captured agent/tool inputs cannot establish historical acceptance.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from tank.util import canonical_json, parse_time


class ReplayError(ValueError):
    pass


def digest(value):
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


# Market reconstruction separates exchange observation time from later collection.
# Source/agent evidence never receives this exception.
def _market_record(row):
    return row.get("collection") in {
        "replay.market_snapshots",
        "mkt.flow_bars_1m",
        "mkt.oi_obs",
        "mkt.funding_obs",
        "mkt.liq_obs",
        "mkt.klines_1m",
        "mkt.book_summaries",
        "mkt.tickers_24h",
        "mkt.supply_obs",
        "mkt.supply",
        "mkt.trades",
    }


def _semantic_end(value, fixed_interval=None):
    """Latest actual semantic observation, including the entire bar bucket."""
    times = []
    if isinstance(value, list):
        for entry in value:
            times.extend(_semantic_end(entry, fixed_interval))
    elif isinstance(value, dict):
        for key, entry in value.items():
            if (
                key
                in {
                    "ts",
                    "exchange_ts",
                    "observed_at",
                    "as_of",
                    "cutoff",
                    "funding_time",
                    "settlement_time",
                    "close_time",
                    "bucket_end",
                    "window_end",
                }
                and entry is not None
                and not isinstance(entry, (dict, list))
            ):
                times.append(parse_time(entry))
            if isinstance(entry, (dict, list)):
                times.extend(_semantic_end(entry, fixed_interval))
        if value.get("minute_start") is not None:
            times.append(parse_time(value["minute_start"]) + timedelta(seconds=60))
        if value.get("open_time") is not None:
            # Unknown bucket width cannot establish a pre-cutoff close/high.
            if (
                not (value.get("interval_s") or fixed_interval)
                or float(value.get("interval_s") or fixed_interval) <= 0
            ):
                raise ReplayError("market bar requires explicit interval_s")
            times.append(
                parse_time(value["open_time"])
                + timedelta(seconds=float(value.get("interval_s") or fixed_interval))
            )
    return times


def _receipt_times(value):
    if isinstance(value, list):
        return [time for entry in value for time in _receipt_times(entry)]
    if not isinstance(value, dict):
        return []
    result = []
    for key, entry in value.items():
        if (
            key in {"received_ts", "received_at", "available_at", "collected_at"}
            and entry is not None
            and not isinstance(entry, (dict, list))
        ):
            result.append(parse_time(entry))
        elif isinstance(entry, (dict, list)):
            result.extend(_receipt_times(entry))
    return result


@dataclass(frozen=True)
class ReplayInputs:
    cutoff: str
    config_version: str
    code_version: str
    registry: dict
    config: dict
    _records: tuple
    _tool_results: tuple
    assembled_at: str | None = None

    @property
    def evidence_mode(self):
        late = any(
            _market_record(row)
            and max([parse_time(row["available_at"]), *_receipt_times(row["record"])])
            > parse_time(self.cutoff)
            for row in self._records
        )
        return "HISTORICAL_RECONSTRUCTION" if late else "AS_OF_CAPTURE"

    def record_visible(self, row, bound):
        if _market_record(row):
            times = _semantic_end(row["record"], 60 if row.get("collection") == "mkt.klines_1m" else None)
            return bool(times) and max(times) <= bound
        return parse_time(row["available_at"]) <= bound

    def records(self, collection, *, cutoff=None):
        bound = parse_time(cutoff or self.cutoff)
        if bound > parse_time(self.cutoff):
            raise ReplayError("query exceeds replay cutoff")
        return [
            copy.deepcopy(row["record"])
            for row in self._records
            if row["collection"] == collection and self.record_visible(row, bound)
        ]

    def tool_result(self, call_id, *, cutoff=None):
        bound = parse_time(cutoff or self.cutoff)
        if bound > parse_time(self.cutoff):
            raise ReplayError("tool query exceeds replay cutoff")
        matches = [
            row["result"]
            for row in self._tool_results
            if row.get("call_id") == call_id and parse_time(row["available_at"]) <= bound
        ]
        if not matches or len({digest(value) for value in matches}) != 1:
            raise ReplayError(f"No unique captured pre-cutoff result for tool call {call_id}")
        return copy.deepcopy(matches[0])

    async def offline_http(self, request):
        raise ReplayError(f"Network forbidden during replay: {request.method} {request.url}")


class ReplayClock:
    def __init__(self, start, cutoff):
        self.current, self.cutoff = parse_time(start), parse_time(cutoff)
        if self.current > self.cutoff:
            raise ReplayError("clock starts after cutoff")

    def advance(self, timestamp):
        value = parse_time(timestamp)
        if not self.current <= value <= self.cutoff:
            raise ReplayError("clock must advance monotonically within cutoff")
        self.current = value
        return self.current

    def now(self):
        return self.current


def load_inputs(path, settings):
    packet = json.loads(Path(path).read_text())
    if any(key in packet for key in ("outcomes", "sealed_outcomes", "expected_outputs")):
        raise ReplayError("sealed evaluator data must be stored outside the input packet")

    def private_map_present(value):
        if isinstance(value, dict):
            return any(
                key in {"name_map", "masking_map", "unmask_map", "private_map"} or private_map_present(entry)
                for key, entry in value.items()
            )
        if isinstance(value, list):
            return any(private_map_present(entry) for entry in value)
        return False

    if private_map_present(packet):
        raise ReplayError("private name masking maps must remain outside pipeline inputs")
    for name in ("config_version", "code_version"):
        if packet[name] != getattr(settings, name):
            raise ReplayError(f"{name} pin mismatch")
    for name in ("registry", "config", "records", "tool_results", "cutoff"):
        if name not in packet:
            raise ReplayError(f"missing replay input {name}")
    cutoff = parse_time(packet["cutoff"])
    assembled = parse_time(packet["assembled_at"]) if packet.get("assembled_at") else None
    for row in packet["tool_results"]:
        if parse_time(row["available_at"]) > cutoff:
            raise ReplayError("packet contains post-cutoff observation")
    for row in packet["records"]:
        available = parse_time(row["available_at"])
        if _market_record(row):
            semantic = _semantic_end(row["record"], 60 if row.get("collection") == "mkt.klines_1m" else None)
            if not semantic:
                raise ReplayError("market record lacks semantic observation time")
            if max(semantic) > cutoff:
                raise ReplayError("post-cutoff market observation or bar bucket end")
            receipts = [available, *_receipt_times(row["record"])]
            if max(receipts) > cutoff and (assembled is None or max(receipts) > assembled):
                raise ReplayError("late market capture requires explicit assembled_at covering receipt")
        elif available > cutoff:
            raise ReplayError("packet contains post-cutoff observation")
    observation_keys = {
        "observed_at",
        "known_to_us_at",
        "fetched_at",
        "created_at",
        "updated_at",
        "received_ts",
        "published_at",
        "public_at",
        "available_at",
        "received_at",
        "collected_at",
        "exchange_ts",
        "minute_start",
        "open_time",
        "ts",
        "as_of",
        "funding_time",
        "cutoff",
    }

    def validate_observations(value):
        if isinstance(value, list):
            for entry in value:
                validate_observations(entry)
        elif isinstance(value, dict):
            for key, entry in value.items():
                if (
                    key in observation_keys
                    and entry is not None
                    and not isinstance(entry, (dict, list))
                    and parse_time(entry) > parse_time(packet["cutoff"])
                ):
                    raise ReplayError(f"post-cutoff observation field: {key}")
                if isinstance(entry, (dict, list)):
                    validate_observations(entry)

    for row in packet["records"]:
        if not _market_record(row):
            validate_observations(row["record"])
    for row in packet["tool_results"]:
        validate_observations(row.get("result"))
    expected = packet.get("input_sha256")
    unhashed = {k: v for k, v in packet.items() if k != "input_sha256"}
    if expected and expected != digest(unhashed):
        raise ReplayError("input bundle digest mismatch")
    return ReplayInputs(
        packet["cutoff"],
        packet["config_version"],
        packet["code_version"],
        copy.deepcopy(packet["registry"]),
        copy.deepcopy(packet["config"]),
        tuple(copy.deepcopy(packet["records"])),
        tuple(copy.deepcopy(packet["tool_results"])),
        packet.get("assembled_at"),
    )


def freeze(path, value):
    """Exclusive creation; identical reruns are accepted, conflicting runs fail."""
    path = Path(path)
    content = canonical_json(value) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x") as handle:
            handle.write(content)
    except FileExistsError:
        if path.read_text() != content:
            raise ReplayError(f"immutable replay artifact differs: {path}")
    return path


async def replay_bundle(bundle_path, repo, settings, *, runner=None, output_dir=None):
    """Run an explicitly supplied offline pipeline and freeze output before evaluation.

    No default synthetic interpretation is substituted for the actual agent pipeline.
    The caller must supply a runner using captured model responses/tool results.
    """
    inputs = load_inputs(bundle_path, settings)
    # A reused repository could expose rows absent from the packet through domain
    # tools. Refuse it before importing anything, including apparently harmless
    # registry/session/queue state. This invariant is stronger than URL matching.
    from tank.models import TABLE_COLUMNS

    for collection in TABLE_COLUMNS:
        if await repo.list(collection, limit=1):
            raise ReplayError(f"replay requires an empty isolated repository: {collection}")
    if runner is None:
        runner = run_captured_tools
    output = await runner(inputs, repo)
    result = {
        "mode": "CAPTURED_TOOL_REEXECUTION",
        "performance_evidence": False,
        "evidence_mode": inputs.evidence_mode,
        "assembled_at": inputs.assembled_at,
        "config_version": inputs.config_version,
        "code_version": inputs.code_version,
        "cutoff": inputs.cutoff,
        "input_sha256": digest(json.loads(Path(bundle_path).read_text())),
        "output": output,
        "output_sha256": digest(output),
    }
    if output_dir:
        freeze(Path(output_dir) / "outputs.json", result)
        freeze(Path(output_dir) / "inputs.json", json.loads(Path(bundle_path).read_text()))
    return result


def evaluate_frozen(output_path, sealed_path):
    """Evaluator-only entry. Call after replay has returned and frozen its output."""
    output = json.loads(Path(output_path).read_text())
    if digest(output["output"]) != output["output_sha256"]:
        raise ReplayError("frozen output integrity failure")
    sealed = json.loads(Path(sealed_path).read_text())
    checks = []
    for assertion in sealed["assertions"]:
        value = output["output"]
        for component in assertion["path"]:
            value = value[component]
        checks.append({"path": assertion["path"], "passed": value == assertion["equals"]})
    return {"passed": all(c["passed"] for c in checks), "checks": checks}


class ReplayMarketReader:
    """Exact-cutoff normalized capture reader; no REST/network fallback."""

    def __init__(self, inputs):
        self.inputs = inputs

    async def snapshot(self, token_id, cutoff, anchor=None):
        if parse_time(cutoff) > parse_time(self.inputs.cutoff):
            raise ReplayError("market request exceeds replay cutoff")
        matches = [
            row
            for row in self.inputs.records("replay.market_snapshots", cutoff=cutoff)
            if row["token_id"] == token_id and parse_time(row["cutoff"]) == parse_time(cutoff)
        ]
        if len(matches) != 1:
            raise ReplayError("insufficient_fixture: missing exact market snapshot")
        result = matches[0]
        if anchor is not None:
            if parse_time(anchor["ts"]) > parse_time(cutoff):
                raise ReplayError("market anchor exceeds replay cutoff")
            result["anchor"] = copy.deepcopy(anchor)
        return result


async def run_captured_tools(inputs, repo):
    """Execute captured analyst tool calls through production validation and rules."""
    from contextlib import ExitStack
    from unittest.mock import patch

    import httpx

    from tank import event_tools, storage, util
    from tank.analysis import cards, decisions, truth
    from tank.analysis.decisions import DecisionEngine
    from tank.event_tools import EventToolService
    from tank.settings import Settings

    calls = inputs.config.get("replay_tool_calls")
    if not calls:
        raise ReplayError("insufficient_fixture: missing captured analyst tool calls")

    class ReplaySettings(Settings):
        def load_config(self, name):
            key = name.removesuffix(".yaml")
            return copy.deepcopy(inputs.registry if key == "registry" else inputs.config.get(key, {}))

    import tempfile

    empty_secrets = tempfile.TemporaryDirectory(prefix="tank-replay-empty-secrets-")
    settings = ReplaySettings(
        replay=True,
        config_version=inputs.config_version,
        code_version=inputs.code_version,
        _env_file=None,
        secrets_dir=Path(empty_secrets.name),
        openma_api_key=None,
        telegram_token=None,
        typesafe_api_key=None,
        tavily_api_key=None,
        etherscan_api_key=None,
        codex_gateway_key=None,
    )

    counter = 0
    original_new_id = util.new_id

    def stable_id(prefix):
        nonlocal counter
        counter += 1
        return original_new_id(prefix, timestamp=clock.now(), entropy=counter)

    async def captured_http(request):
        for row in inputs._tool_results:
            if (
                row.get("method", "GET") == request.method
                and row.get("url") == str(request.url)
                and parse_time(row["available_at"]) <= clock.now()
            ):
                return httpx.Response(row.get("status_code", 200), json=row["result"])
        raise ReplayError(f"Uncaptured replay network request: {request.method} {request.url}")

    clock = ReplayClock(min(c["available_at"] for c in calls), inputs.cutoff)
    results = []
    identities = {}

    def remap(value):
        if isinstance(value, str):
            return identities.get(value, value)
        if isinstance(value, list):
            return [remap(v) for v in value]
        if isinstance(value, dict):
            return {k: remap(v) for k, v in value.items()}
        return value

    def remember_ids(captured, actual, key=""):
        if isinstance(captured, dict) and isinstance(actual, dict):
            for name in captured.keys() & actual.keys():
                remember_ids(captured[name], actual[name], name)
        elif isinstance(captured, list) and isinstance(actual, list):
            for before, after in zip(captured, actual):
                remember_ids(before, after, key)
        elif key.endswith("_id") and isinstance(captured, str) and isinstance(actual, str):
            if captured in identities and identities[captured] != actual:
                raise ReplayError("captured identity maps to conflicting replay identities")
            identities[captured] = actual

    # The isolated CLI owns the process. These patches must never overlap live work.
    with ExitStack() as stack:
        stack.callback(empty_secrets.cleanup)
        original_iso = util.iso

        def replay_iso(value=None):
            return original_iso(clock.now() if value is None else value)

        for module in (event_tools, decisions, cards, storage, truth, util):
            if hasattr(module, "new_id"):
                stack.enter_context(patch.object(module, "new_id", stable_id))
            if hasattr(module, "utcnow"):
                stack.enter_context(patch.object(module, "utcnow", clock.now))
            if hasattr(module, "iso"):
                stack.enter_context(patch.object(module, "iso", replay_iso))
        async with httpx.AsyncClient(transport=httpx.MockTransport(captured_http)) as http:
            service = EventToolService(
                repo,
                settings,
                truth.MarketTruthService(repo, settings, ReplayMarketReader(inputs)),
                DecisionEngine(settings),
                http=http,
            )
            seeded = set()
            for call in calls:
                clock.advance(call["available_at"])
                for index, row in enumerate(inputs._records):
                    if index in seeded or row["collection"] == "replay.market_snapshots":
                        continue
                    if inputs.record_visible(row, clock.now()):
                        await repo.put(row["collection"], row["key"], copy.deepcopy(row["record"]))
                        seeded.add(index)
                call = remap(call)
                context = copy.deepcopy(call.get("context", {}))
                context["call_id"] = call["call_id"]
                if call["tool"] in {"web_search", "fetch_page"}:
                    # DDG and page readers are external observations. Never run
                    # their clients during offline transcript reproduction.
                    captures = [
                        row
                        for row in inputs._tool_results
                        if row.get("call_id") == call["call_id"]
                        and parse_time(row["available_at"]) <= clock.now()
                    ]
                    if not captures or any(
                        row.get("tool") != call["tool"] or remap(row.get("arguments")) != call["arguments"]
                        for row in captures
                    ):
                        raise ReplayError(
                            "insufficient_fixture: no exact captured pre-cutoff external tool arguments"
                        )
                    result = inputs.tool_result(call["call_id"], cutoff=util.iso(clock.now()))
                elif call["tool"] == "get_market_truth":
                    import asyncio

                    async def complete_market_jobs(call=call):
                        while True:
                            for job in await repo.list(
                                "ops.jobs", filters={"status": "QUEUED"}, limit=100000
                            ):
                                if job["payload"].get("event_id") != call["arguments"]["event_id"]:
                                    continue
                                if job["type"] not in {"ESCALATE_TOKENS", "RUN_MARKET_TRUTH"}:
                                    continue
                                if job.get("run_after") and parse_time(job["run_after"]) > clock.now():
                                    continue
                                dependencies = [
                                    await repo.get("ops.jobs", key)
                                    for key in job["payload"].get("_depends_on", [])
                                ]
                                if any(row is None or row.get("status") != "DONE" for row in dependencies):
                                    continue
                                result = None
                                if job["type"] == "RUN_MARKET_TRUTH":
                                    payload = job["payload"]
                                    result = await service.truth.assess(
                                        payload["event_id"], payload["token_id"], payload["cutoff"]
                                    )
                                await repo.put(
                                    "ops.jobs", job["job_id"], dict(job, result=result, status="DONE")
                                )
                            await asyncio.sleep(0.01)

                    worker = asyncio.create_task(complete_market_jobs())
                    try:
                        result = await asyncio.wait_for(
                            service.dispatch(call["tool"], copy.deepcopy(call["arguments"]), context), 5
                        )
                    finally:
                        worker.cancel()
                        completed = await asyncio.gather(worker, return_exceptions=True)
                        if completed and isinstance(completed[0], Exception):
                            raise completed[0]
                else:
                    result = await service.dispatch(call["tool"], copy.deepcopy(call["arguments"]), context)
                captured = next(
                    (r for r in inputs._tool_results if r.get("call_id") == call["call_id"]), None
                )
                if captured:
                    if parse_time(captured["available_at"]) > clock.now():
                        raise ReplayError("captured tool result was unavailable at call time")
                    remember_ids(captured["result"], result)
                results.append({"call_id": call["call_id"], "tool": call["tool"], "result": result})
    return {"tool_calls": results}


async def replay_fixture(repo, settings, fixture_id):
    if not __import__("re").fullmatch(r"F-\d{2}", fixture_id):
        raise ReplayError("invalid fixture identifier")
    path = settings.data_dir / "replay" / "inputs" / f"{fixture_id}.json"
    if not path.is_file():
        raise ReplayError(f"insufficient_fixture: no captured packet for {fixture_id}")
    bundle_id = digest(json.loads(path.read_text()))
    return await replay_bundle(
        path, repo, settings, output_dir=settings.data_dir / "replay" / "bundles" / bundle_id
    )


async def reproduce_bundle(repo, settings, sha):
    if not __import__("re").fullmatch(r"[a-f0-9]{64}", sha):
        raise ReplayError("invalid bundle digest")
    directory = settings.data_dir / "replay" / "bundles" / sha
    source = directory / "inputs.json"
    expected = json.loads((directory / "outputs.json").read_text())
    if digest(json.loads(source.read_text())) != sha:
        raise ReplayError("bundle input integrity failure")
    actual = await replay_bundle(source, repo, settings)
    if actual != expected:
        raise ReplayError("bundle reproduction differs from immutable output")
    return {"reproduced": True, "input_sha256": sha, "output_sha256": actual["output_sha256"]}


def prepare_masked_packet(source_path, masked_path, private_map_path, replacements):
    """Authoring step only: keep the reversible name map outside replay inputs.

    Original captures are never overwritten. Distinct aliases are required so
    the private map is reversible. Masked historical runs remain behavior checks.
    """
    import re

    source_path, masked_path, private_map_path = map(Path, (source_path, masked_path, private_map_path))
    if source_path.resolve() == masked_path.resolve():
        raise ReplayError("masking must preserve the original input packet")
    if private_map_path.resolve().is_relative_to(masked_path.parent.resolve()):
        raise ReplayError("private masking map must be outside the replay input directory")
    if (
        not isinstance(replacements, dict)
        or not replacements
        or any(
            not isinstance(k, str) or not k or not isinstance(v, str) or not v
            for k, v in replacements.items()
        )
    ):
        raise ReplayError("masking requires explicit nonempty names and aliases")
    if len({key.casefold() for key in replacements}) != len(replacements) or len(
        set(replacements.values())
    ) != len(replacements):
        raise ReplayError("name masking map must be one-to-one and case-unambiguous")
    if any(value.casefold() in {key.casefold() for key in replacements} for value in replacements.values()):
        raise ReplayError("masked aliases cannot themselves be original names")
    packet = json.loads(source_path.read_text())
    lookup = {key.casefold(): value for key, value in replacements.items()}
    pattern = re.compile(
        r"(?<!\w)("
        + "|".join(re.escape(k) for k in sorted(replacements, key=len, reverse=True))
        + r")(?!\w)",
        re.IGNORECASE,
    )

    def mask(value):
        if isinstance(value, str):
            return pattern.sub(lambda match: lookup[match.group().casefold()], value)
        if isinstance(value, list):
            return [mask(v) for v in value]
        if isinstance(value, dict):
            result = {mask(k): mask(v) for k, v in value.items()}
            if len(result) != len(value):
                raise ReplayError("masking creates colliding object keys")
            return result
        return value

    masked = mask({k: v for k, v in packet.items() if k != "input_sha256"})
    # Pins identify executable code/config versions; masking is a separate input
    # transformation and must not rewrite these deployment identities.
    for key in ("config_version", "code_version", "cutoff"):
        masked[key] = packet[key]
    private = {"schema_version": 1, "source_sha256": digest(packet), "replacements": replacements}
    masked["masking"] = {"applied": True, "map_sha256": digest(private), "performance_evidence": False}
    masked["input_sha256"] = digest(masked)
    freeze(private_map_path, private)
    freeze(masked_path, masked)
    return {
        "masked_path": str(masked_path),
        "private_map_path": str(private_map_path),
        "masked_sha256": digest(masked),
        "performance_evidence": False,
    }


async def replay_agent_session(
    bundle_path,
    repo,
    settings,
    event_tools,
    *,
    http,
    initial_message,
    replay_run_id=None,
    timeout=30,
    output_path=None,
):
    """Run a fresh tank-replay turn against explicit offline external adapters.

    The caller supplies the control-plane transport (a captured transcript in
    tests) and EventToolService with a captured market-data reader. Domain tools
    execute production code; SessionService reuses captures only for external
    web reads. No Telegram delivery worker runs. A live VM/model turn requires an
    actually provisioned isolated OpenMA environment; this function does not
    silently replace that with a deterministic interpretation.
    """
    import asyncio
    from contextlib import ExitStack
    from unittest.mock import patch

    from tank import event_tools as tool_module
    from tank import storage, util
    from tank.agents import sessions as session_module
    from tank.agents.sessions import SessionService
    from tank.analysis import cards, truth
    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import MarketTruthService
    from tank.event_tools import EventToolService
    from tank.models import TABLE_COLUMNS

    inputs = load_inputs(bundle_path, settings)
    if not settings.replay or not isinstance(event_tools, EventToolService) or event_tools.repo is not repo:
        raise ReplayError("fresh replay requires the real EventToolService on an isolated replay repository")
    if (
        event_tools.settings is not settings
        or not isinstance(event_tools.truth, MarketTruthService)
        or event_tools.truth.repo is not repo
        or event_tools.truth.settings is not settings
        or not isinstance(event_tools.engine, DecisionEngine)
    ):
        raise ReplayError("fresh replay must rerun the real pinned MarketTruthService and DecisionEngine")
    if http is None:
        raise ReplayError("fresh replay requires an explicitly supplied offline control-plane transport")
    if initial_message.get("kind") not in {"NEW_ITEM", "ITEM_ADDED"} or initial_message.get("session_id"):
        raise ReplayError("fresh replay requires an initial item message without an original session id")
    if not settings.load_config("openma").get("replay_environment_id"):
        raise ReplayError("fresh replay requires a registered tank-replay environment")
    if settings.load_config("registry") != inputs.registry:
        raise ReplayError("replay registry differs from the captured snapshot")
    for name in ("thresholds", "semantics", "dependency_map", "openma"):
        if name in inputs.config and settings.load_config(name) != inputs.config[name]:
            raise ReplayError(f"replay configuration differs from captured snapshot: {name}")
    for collection in TABLE_COLUMNS:
        if await repo.list(collection, limit=1):
            raise ReplayError(f"replay requires an empty isolated repository: {collection}")
    for row in sorted(inputs._records, key=lambda row: parse_time(row["available_at"])):
        if row["collection"] != "replay.market_snapshots":
            await repo.put(row["collection"], row["key"], copy.deepcopy(row["record"]))
    if not await repo.get("ingest.source_items", initial_message["item_id"]):
        raise ReplayError("insufficient_fixture: initial source item is absent")
    original_sessions = [
        row["record"]
        for row in inputs._records
        if row["collection"] == "ops.agent_sessions"
        and (
            row["record"].get("event_id") == initial_message.get("event_id")
            if initial_message.get("event_id")
            else row["record"].get("item_id") == initial_message["item_id"]
        )
    ]
    source_id = initial_message.get("replay_source_session_id") or inputs.config.get(
        "replay_source_session_id"
    )
    if source_id is None and len(original_sessions) == 1:
        source_id = original_sessions[0]["session_id"]
    if source_id:
        initial_message = dict(initial_message, replay_source_session_id=source_id)
    run_id = replay_run_id or util.new_id("RPL")
    service = SessionService(repo, settings, event_tools.dispatch, http=http)
    worker = None
    sid = None
    initial_decisions = {r["dec_id"] for r in await repo.list("ana.decisions", limit=100000)}
    initial_alerts = {(r["alert_id"], r["version"]) for r in await repo.list("out.alerts", limit=100000)}

    async def market_jobs():
        while True:
            session = await repo.get("ops.agent_sessions", sid)
            eid = (session or {}).get("event_id")
            for job in await repo.list("ops.jobs", filters={"status": "QUEUED"}, limit=100000):
                if (
                    not eid
                    or job["payload"].get("event_id") != eid
                    or job["type"] not in {"ESCALATE_TOKENS", "RUN_MARKET_TRUTH"}
                ):
                    continue
                if job.get("run_after") and parse_time(job["run_after"]) > parse_time(inputs.cutoff):
                    continue
                dependencies = [
                    await repo.get("ops.jobs", key) for key in job["payload"].get("_depends_on", [])
                ]
                if any(row is None or row.get("status") != "DONE" for row in dependencies):
                    continue
                result = None
                if job["type"] == "RUN_MARKET_TRUTH":
                    payload = job["payload"]
                    if parse_time(payload["cutoff"]) > parse_time(inputs.cutoff):
                        raise ReplayError("market request exceeds replay cutoff")
                    result = await event_tools.truth.assess(eid, payload["token_id"], payload["cutoff"])
                await repo.put("ops.jobs", job["job_id"], dict(job, result=result, status="DONE"))
            await asyncio.sleep(0.01)

    original_iso = util.iso

    def replay_iso(value=None):
        return original_iso(inputs.cutoff if value is None else value)

    with ExitStack() as stack:
        for module in (util, storage, tool_module, session_module, cards, truth):
            if hasattr(module, "utcnow"):
                stack.enter_context(patch.object(module, "utcnow", lambda: parse_time(inputs.cutoff)))
            if hasattr(module, "iso"):
                stack.enter_context(patch.object(module, "iso", replay_iso))
        try:
            session = await service.send_message(dict(initial_message, replay_run_id=run_id))
            sid = session["session_id"]
            worker = asyncio.create_task(market_jobs())
            async with asyncio.timeout(timeout):
                while True:
                    if worker.done():
                        await worker
                    current = await repo.get("ops.agent_sessions", sid)
                    if current.get("status") in {"FAILED", "ENDED"}:
                        raise ReplayError("replay agent turn failed before an idle completion")
                    calls = await repo.list("ops.tool_calls", filters={"session_id": sid}, limit=100000)
                    if any(call.get("status") in {"ERROR", "REFUSED"} for call in calls):
                        raise ReplayError("replay requires missing or invalid captured tool inputs")
                    if current.get("runtime_status") == "IDLE" and all(
                        call.get("returned_at") for call in calls
                    ):
                        break
                    await asyncio.sleep(0.01)
            decisions = [
                r
                for r in await repo.list("ana.decisions", limit=100000)
                if r["dec_id"] not in initial_decisions
            ]
            alerts = [
                r
                for r in await repo.list("out.alerts", limit=100000)
                if (r["alert_id"], r["version"]) not in initial_alerts
            ]
            report = {
                "replay_run_id": run_id,
                "session_id": sid,
                "mode": "FRESH_OPENMA_SESSION",
                "cutoff": inputs.cutoff,
                "config_version": inputs.config_version,
                "code_version": inputs.code_version,
                "input_sha256": digest(json.loads(Path(bundle_path).read_text())),
                "output": {"decisions": decisions, "alerts": alerts},
                "performance_evidence": False,
                "evidence_mode": inputs.evidence_mode,
                "assembled_at": inputs.assembled_at,
            }
            report["output_sha256"] = digest(report["output"])
            if output_path:
                freeze(output_path, report)
            return report
        finally:
            if worker:
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
            await service.close()
