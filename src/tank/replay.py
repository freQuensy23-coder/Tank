"""Offline replay boundaries. Sealed evaluator data never enters ReplayInputs.

External data uses captured readers; native agent analysis may differ on a fresh
subscription-model turn. Sandbox commands run again against the isolated database.
Historical replay requires original messages/tool evidence. Explicit first-reference
fixtures use declared authored messages; their generated delivery intents remain
QUEUED without a historical receipt, transport, or delivery acceptance claim.
"""

import copy
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path

from tank.market_schema import MARKET_COLUMNS
from tank.time_precision import Timestamp, interval_bounds, propagate_timestamp
from tank.util import canonical_json, iso, parse_time


class ReplayError(ValueError):
    pass


class ReplayDivergence(ReplayError):
    """Original causal references do not have one compatible fresh counterpart."""


class ReplayEvidenceUnavailable(ReplayError):
    """A fresh reference-session lookup has no eligible recorded answer."""


def reference_evidence_gaps(calls):
    """Report missing answers from durable tool errors, including recovered calls."""
    gaps = []
    for call in calls:
        result = call.get("result")
        error = result.get("error") if isinstance(result, dict) else None
        if isinstance(error, dict) and error.get("type") == "ReplayEvidenceUnavailable":
            gaps.append(
                {
                    "call_id": call["call_id"],
                    "tool": call["tool"],
                    "requested_at": call.get("requested_at"),
                    "reason": error.get("reason"),
                    "origin": "LOCAL_REPLAY_DIAGNOSTIC",
                }
            )
    return gaps


def digest(value):
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def missing_capture_error(call):
    """A broken captured transcript still ends replay immediately.

    Pending ledger rows have no result, and ordinary domain refusals carry a
    string error. Both must remain available to the normal agent continuation.
    A reference session's ReplayEvidenceUnavailable is also a tool error, not
    a fabricated successful lookup or a fatal transcript mismatch.
    """
    result = call.get("result")
    if not isinstance(result, dict):
        return False
    error = result.get("error")
    if not isinstance(error, dict):
        return False
    reason = error.get("reason")
    return error.get("type") == "ReplayError" and isinstance(reason, str) and "insufficient_fixture" in reason


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
    }


def _late_unlock(row, cutoff):
    """An unlock row captured after the cutoff (lead rulings F-02, F-04).

    Unlock schedules are public history: a later capture may enter as a historical
    reconstruction when the schedule holding the tranche was known by the cutoff
    (schedule_as_of <= cutoff; the tranche itself may fall before or after it) and
    every receipt is <= assembled_at. Calendars captured before the cutoff keep their
    own availability.
    """
    if row.get("collection") != "mkt.unlocks":
        return False
    receipts = [parse_time(row["available_at"]), *_receipt_times(row["record"])]
    return max(receipts) > parse_time(cutoff)


def _unlock_time(row):
    """When the schedule holding this tranche was known: the end of its declared bucket."""
    if row["record"].get("schedule_as_of") is None:
        raise ReplayError("late unlock capture requires schedule_as_of")
    _, known = interval_bounds(propagate_timestamp(row["record"], "schedule_as_of"))
    if known is None:
        raise ReplayError("late unlock schedule_as_of requires EXACT or BUCKET(size) precision")
    return [known]


def _market_timestamp(value, key):
    raw = value.get(key)
    if isinstance(raw, dict) and "value" in raw and "ts" in raw:
        selected = parse_time(raw["value"]) if raw["value"] is not None else None
        alias = parse_time(raw["ts"]) if raw["ts"] is not None else None
        if selected != alias:
            raise ReplayError("conflicting market timestamp values")
    return propagate_timestamp(value, key)


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
                    "boundary",
                    "start",
                    "end",
                }
                and entry is not None
            ):
                stamp = _market_timestamp(value, key)

                if stamp.value is not None and not (
                    key == "observed_at" and stamp.precision == "COLLECTION_ONLY"
                ):
                    times.append(parse_time(stamp.value))
            elif isinstance(entry, (dict, list)):
                times.extend(_semantic_end(entry, fixed_interval))
        if value.get("minute_start") is not None:
            times.append(parse_time(_market_timestamp(value, "minute_start").value) + timedelta(seconds=60))
        if value.get("open_time") is not None:
            if (
                not (value.get("interval_s") or fixed_interval)
                or float(value.get("interval_s") or fixed_interval) <= 0
            ):
                raise ReplayError("market bar requires explicit interval_s")
            times.append(
                parse_time(_market_timestamp(value, "open_time").value)
                + timedelta(seconds=float(value.get("interval_s") or fixed_interval))
            )
    return times


def _point_at_boundary(value, bound):
    """Point observations at an exclusive endpoint are absent; completed bars survive."""
    if isinstance(value, list):
        return any(_point_at_boundary(entry, bound) for entry in value)
    if not isinstance(value, dict):
        return False
    is_bar = value.get("open_time") is not None or value.get("minute_start") is not None
    point_fields = {"ts", "exchange_ts", "as_of", "funding_time", "settlement_time", "observed_at"}
    if not is_bar:
        for field in point_fields:
            if value.get(field) is not None:
                stamp = _market_timestamp(value, field)
                if (
                    stamp.value is not None
                    and stamp.precision != "COLLECTION_ONLY"
                    and parse_time(stamp.value) >= bound
                ):
                    return True
    return any(
        _point_at_boundary(entry, bound) for entry in value.values() if isinstance(entry, (dict, list))
    )


def _receipt_times(value):
    if isinstance(value, list):
        return [time for entry in value for time in _receipt_times(entry)]
    if not isinstance(value, dict):
        return []
    result = []
    for key, entry in value.items():
        if key in {"received_ts", "received_at", "available_at", "collected_at", "observed_at"}:
            stamp = _market_timestamp(value, key)
            if stamp.value is not None and (key != "observed_at" or stamp.precision == "COLLECTION_ONLY"):
                result.append(parse_time(stamp.value))
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
    fixture_reference: dict | None = None
    input_sha256: str | None = None

    def available_at(self, row):
        from tank.fixture_reference import availability

        return availability(row, self.fixture_reference)

    @property
    def evidence_mode(self):
        if self.fixture_reference:
            return "FIXTURE_REFERENCE_RECONSTRUCTION"
        late = any(
            (_market_record(row) or _late_unlock(row, self.cutoff))
            and max([parse_time(row["available_at"]), *_receipt_times(row["record"])])
            > parse_time(self.cutoff)
            for row in self._records
        )
        return "HISTORICAL_RECONSTRUCTION" if late else "AS_OF_CAPTURE"

    def exclusive_boundary(self, bound):
        return bool(
            self.fixture_reference
            and self.fixture_reference.get("cutoff_policy") == "UTC_MINUTE_END_EXCLUSIVE"
            and parse_time(self.cutoff) == bound
        )

    def reference_capture(self, tool, arguments, bound):
        """Select eligible declared responses without revealing future phase inputs."""
        from tank.agents.sessions import CapturedToolResult

        phased = bool(self.fixture_reference.get("phases"))
        matches = [
            capture
            for capture in self._tool_results
            if capture.get("tool") == tool and capture.get("arguments") == arguments
        ]
        if phased or self.exclusive_boundary(bound):
            matches = [
                capture
                for capture in matches
                if (
                    parse_time(self.available_at(capture)) < bound
                    if self.exclusive_boundary(bound)
                    else parse_time(self.available_at(capture)) <= bound
                )
                and (capture.get("valid_until") is None or bound < parse_time(capture["valid_until"]))
            ]
        signatures = {CapturedToolResult.from_record(capture).signature() for capture in matches}
        if not matches:
            raise ReplayEvidenceUnavailable(
                "REPLAY_EVIDENCE_UNAVAILABLE: no eligible recorded response for this request. "
                "External network is disabled. This is missing replay evidence, not an empty search "
                "or a live source failure; do not infer that confirmation is absent."
            )
        if len(signatures) != 1:
            raise ReplayError("insufficient_fixture: no unambiguous eligible fixture page capture")
        return min(matches, key=lambda row: parse_time(self.available_at(row)))

    def record_visible(self, row, bound):
        if self.fixture_reference and row["collection"] == "ingest.source_items":
            available = parse_time(self.available_at(row))
            return available < bound if self.exclusive_boundary(bound) else available <= bound
        late_unlock = _late_unlock(row, self.cutoff)
        if _market_record(row) or late_unlock:
            times = (
                _unlock_time(row)
                if late_unlock
                else _semantic_end(row["record"], 60 if row.get("collection") == "mkt.klines_1m" else None)
            )
            if not times or max(times) > bound:
                return False
            if self.exclusive_boundary(bound) and _point_at_boundary(row["record"], bound):
                return False
            receipts = [parse_time(row["available_at"]), *_receipt_times(row["record"])]

            historical = (
                max(receipts) > parse_time(self.cutoff)
                and self.assembled_at is not None
                and max(receipts) <= parse_time(self.assembled_at)
            )
            return historical or max(receipts) <= bound
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
        self.selected = start if isinstance(start, Timestamp) else propagate_timestamp({"at": start}, "at")
        self.current, self.cutoff = parse_time(self.selected.value), parse_time(cutoff)
        if self.current > self.cutoff:
            raise ReplayError("clock starts after cutoff")

    def advance(self, timestamp):
        selected = (
            timestamp if isinstance(timestamp, Timestamp) else propagate_timestamp({"at": timestamp}, "at")
        )
        value = parse_time(selected.value)
        if not self.current <= value <= self.cutoff:
            raise ReplayError("clock must advance monotonically within cutoff")
        if value != self.current or isinstance(timestamp, Timestamp):
            self.selected = selected
        self.current = value
        return self.current

    def timestamp(self):
        return Timestamp(iso(self.current), self.selected.precision, self.selected.precision_reason)

    def now(self):
        return self.current


def load_inputs(path, settings):
    packet = json.loads(Path(path).read_text())
    if packet.get("fixture_status") == "INSUFFICIENT_FIXTURE":
        missing = packet.get("missing")
        if not isinstance(missing, list) or not all(isinstance(reason, str) for reason in missing):
            raise ReplayError("insufficient_fixture: invalid missing-evidence catalog entry")
        raise ReplayError(
            "insufficient_fixture: " + str(packet.get("fixture_id", "unknown")) + ": " + "; ".join(missing)
        )
    if any(key in packet for key in ("outcomes", "sealed_outcomes", "expected_outputs")):
        raise ReplayError("sealed evaluator data must be stored outside the input packet")

    def private_map_present(value):
        if isinstance(value, dict):
            if "replacements" in value and any(k in value for k in ("source_sha256", "original_packet_path")):
                return True
            return any(
                key
                in {
                    "name_map",
                    "masking_map",
                    "unmask_map",
                    "private_map",
                    "original_packet_path",
                    "original_registry_path",
                    "original_raw_paths",
                    "source_token_identity",
                }
                or private_map_present(entry)
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
    from tank.fixture_reference import validate as validate_fixture_reference

    try:
        fixture_reference = validate_fixture_reference(
            packet,
            _market_record,
            private_mask_path=getattr(settings, "replay_private_mask_path", None),
            input_path=path,
        )
    except (ValueError, KeyError, TypeError, OSError) as error:
        raise ReplayError("insufficient_fixture: " + str(error)) from error
    cutoff = parse_time(packet["cutoff"])
    assembled = parse_time(packet["assembled_at"]) if packet.get("assembled_at") else None
    for row in packet["tool_results"]:
        if parse_time(row["available_at"]) > cutoff and not fixture_reference:
            raise ReplayError("packet contains post-cutoff observation")
    for row in packet["records"]:
        available = parse_time(row["available_at"])
        late_unlock = _late_unlock(row, packet["cutoff"])
        if late_unlock and row.get("evidence_mode") != "HISTORICAL_RECONSTRUCTION":
            raise ReplayError("late unlock capture must be declared HISTORICAL_RECONSTRUCTION")
        if _market_record(row) or late_unlock:
            semantic = (
                _unlock_time(row)
                if late_unlock
                else _semantic_end(row["record"], 60 if row.get("collection") == "mkt.klines_1m" else None)
            )
            if not semantic:
                raise ReplayError("market record lacks semantic observation time")
            if max(semantic) > cutoff:
                raise ReplayError("post-cutoff market observation or bar bucket end")
            receipts = [available, *_receipt_times(row["record"])]
            if max(receipts) > cutoff and (assembled is None or max(receipts) > assembled):
                raise ReplayError("late market capture requires explicit assembled_at covering receipt")
        elif available > cutoff and not fixture_reference:
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

    def validate_observations(value, *, reconstructed_source=False):
        if isinstance(value, list):
            for entry in value:
                validate_observations(entry, reconstructed_source=reconstructed_source)
        elif isinstance(value, dict):
            for key, entry in value.items():
                if (
                    key in observation_keys
                    and not (
                        reconstructed_source
                        and key
                        in {
                            "observed_at",
                            "acquired_at",
                            "fetched_at",
                            "received_ts",
                            "received_at",
                            "collected_at",
                            "available_at",
                        }
                    )
                    and entry is not None
                    and not isinstance(entry, (dict, list))
                    and parse_time(entry) > parse_time(packet["cutoff"])
                ):
                    raise ReplayError(f"post-cutoff observation field: {key}")
                if isinstance(entry, (dict, list)):
                    validate_observations(entry, reconstructed_source=reconstructed_source)

    for row in packet["records"]:
        if not _market_record(row) and not _late_unlock(row, packet["cutoff"]):
            validate_observations(row["record"], reconstructed_source=bool(fixture_reference))
    for row in packet["tool_results"]:
        validate_observations(row.get("result"), reconstructed_source=bool(fixture_reference))
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
        fixture_reference=copy.deepcopy(fixture_reference),
        input_sha256=expected or digest(unhashed),
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
            raise ReplayError(f"immutable replay artifact differs: {path}") from None
    return path


async def replay_bundle(bundle_path, repo, settings, *, runner=None, output_dir=None):
    """Run an explicitly supplied offline pipeline and freeze output before evaluation.

    This lower-level helper is the captured-tool diagnostic. Operator commands
    use run_operational_replay for a fresh OpenMA session with original messages.
    Custom runners are useful for offline tests; none imply native acceptance.
    """
    inputs = load_inputs(bundle_path, settings)

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
    if output.get("status") not in (None, "COMPLETED"):
        raise ReplayError("Incomplete or divergent replay output is not a completed acceptance run")
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

    def __init__(self, inputs, evidence_capture=None):
        self.inputs = inputs
        self.evidence_capture = evidence_capture
        self.active_cutoff = None

    async def snapshot(self, token_id, cutoff, anchor=None):
        if (
            getattr(self.inputs, "fixture_reference", None)
            and self.inputs.fixture_reference.get("phases")
            and self.active_cutoff is not None
            and parse_time(cutoff) > self.active_cutoff()
        ):
            raise ReplayError("market request exceeds the active reference phase")
        if parse_time(cutoff) > parse_time(self.inputs.cutoff):
            raise ReplayError("market request exceeds replay cutoff")
        matches = [
            row
            for row in self.inputs.records("replay.market_snapshots", cutoff=cutoff)
            if row["token_id"] == token_id and parse_time(row["cutoff"]) == parse_time(cutoff)
        ]
        if len(matches) != 1:
            raise ReplayError("insufficient_fixture: missing exact market snapshot")
        result = copy.deepcopy(matches[0])
        if result.get("artifact_kind") == "REFERENCE_MISSING_MARKET_INPUTS":
            if not self.inputs.fixture_reference:
                raise ReplayError(
                    "insufficient_fixture: reference missing-market declaration is not a captured snapshot"
                )
            result["unavailable"] = {
                field: "NO_RETAINED_REFERENCE_OBSERVATIONS" for field in result["missing_inputs"]
            }
            result["fixture_reference_provenance"] = {
                "input_sha256": self.inputs.input_sha256,
                "fixture_id": self.inputs.fixture_reference["fixture_id"],
                "source_capture_claim": False,
                "historical_market_acceptance_claim": False,
            }
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

    settings = ReplaySettings(
        replay=True,
        config_version=inputs.config_version,
        code_version=inputs.code_version,
        _env_file=None,
        tank_db_password=None,
        oma_db_password=None,
        apify_api_token=None,
        openma_api_key=None,
        openrouter_api_key=None,
        telegram_token=None,
        tavily_api_key=None,
        etherscan_api_key=None,
        codex_gateway_key=None,
    )

    from tank.replay_evidence import seal_replay_evidence

    evidence_directory = tempfile.TemporaryDirectory(prefix="tank-replay-diagnostic-evidence-")
    evidence = seal_replay_evidence(
        inputs,
        "RPL-diagnostic",
        Path(evidence_directory.name).resolve() / "RPL-diagnostic",
        input_sha256=digest({"records": inputs._records, "cutoff": inputs.cutoff}),
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
            for before, after in zip(captured, actual, strict=False):
                remember_ids(before, after, key)
        elif key.endswith("_id") and isinstance(captured, str) and isinstance(actual, str):
            if captured in identities and identities[captured] != actual:
                raise ReplayError("captured identity maps to conflicting replay identities")
            identities[captured] = actual

    with ExitStack() as stack:
        stack.callback(evidence_directory.cleanup)
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
                truth.MarketTruthService(repo, settings, ReplayMarketReader(inputs, evidence)),
                DecisionEngine(settings),
                http=http,
            )
            seeded = set()
            for call in calls:
                clock.advance(call["available_at"])
                market_batches = {}
                for index, row in enumerate(inputs._records):
                    projected = row["record"]
                    revision_identity = (index, digest(projected))
                    if revision_identity in seeded or row["collection"] == "replay.market_snapshots":
                        continue
                    if inputs.record_visible(row, clock.now()):
                        collection = row["collection"]
                        if collection in MARKET_COLUMNS:
                            batch = market_batches.setdefault(collection, [])
                            batch.append((revision_identity, row))
                            if len(batch) == 500:
                                await repo.put_market_batch(
                                    collection,
                                    [copy.deepcopy(entry["record"]) for _, entry in batch],
                                    keys=[entry["key"] for _, entry in batch],
                                )
                                seeded.update(identity for identity, _ in batch)
                                batch.clear()
                        else:
                            record = copy.deepcopy(projected)
                            await repo.put(collection, row["key"], record)
                            seeded.add(revision_identity)
                for collection, batch in market_batches.items():
                    if batch:
                        await repo.put_market_batch(
                            collection,
                            [copy.deepcopy(entry["record"]) for _, entry in batch],
                            keys=[entry["key"] for _, entry in batch],
                        )
                        seeded.update(identity for identity, _ in batch)
                call = remap(call)
                context = copy.deepcopy(call.get("context", {}))
                context["call_id"] = call["call_id"]
                if call["tool"] in {"web_search", "fetch_page"}:
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
                elif call["tool"] in {"get_market_truth", "get_decision"}:
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
    directory = settings.data_dir / "replay" / "bundles" / bundle_id

    result = await run_operational_replay(path, repo, settings)
    freeze(directory / "outputs.json", result)
    freeze(directory / "inputs.json", json.loads(path.read_text()))
    return result


def semantic_verdict_output(output):
    """Compare complete structured verdicts with declared identity substitutions.

    Only run/session provenance and rendered actor prose are omitted. Generated
    domain IDs are alpha-renamed, preserving every reference/equality relation;
    source item/token/evidence identities, rules, grades, gates, windows, values,
    config/code versions and all otherwise unknown fields remain comparable.
    """
    generated = {
        "dec_id",
        "event_id",
        "ems_id",
        "mta_id",
        "alert_id",
        "edge_id",
        "outlook_id",
        "claim_id",
        "bundle_id",
        "evidence_bundle_id",
        "call_id",
        "cond_id",
        "out_id",
        "observation_id",
        "obs_id",
        "state_change_id",
    }
    omitted = {
        "session_id",
        "replay_run_id",
        "session_config_version",
        "session_code_version",
        "card_text",
        "removed_prose",
    }
    identities = {}
    counters = {}

    def collect(value):
        if isinstance(value, dict):
            for key in sorted(value):
                entry = value[key]
                if key in generated and isinstance(entry, str) and entry not in identities:
                    counters[key] = counters.get(key, 0) + 1
                    identities[entry] = f"@{key}:{counters[key]}"
                if key not in omitted:
                    collect(entry)
        elif isinstance(value, list):
            for entry in value:
                collect(entry)

    def normalize(value):
        if isinstance(value, dict):
            return {key: normalize(entry) for key, entry in sorted(value.items()) if key not in omitted}
        if isinstance(value, list):
            return [normalize(entry) for entry in value]
        return identities.get(value, value) if isinstance(value, str) else value

    collect(output)
    return normalize(output)


async def reproduce_bundle(repo, settings, sha, *, mode="fresh-session"):
    if not __import__("re").fullmatch(r"[a-f0-9]{64}", sha):
        raise ReplayError("invalid bundle digest")
    directory = settings.data_dir / "replay" / "bundles" / sha
    source = directory / "inputs.json"
    expected = json.loads((directory / "outputs.json").read_text())
    if digest(json.loads(source.read_text())) != sha:
        raise ReplayError("bundle input integrity failure")
    if mode == "captured-tools":
        if expected.get("mode") != "CAPTURED_TOOL_REEXECUTION":
            raise ReplayError("Diagnostic reproduction requires a diagnostic bundle")
        actual = await replay_bundle(source, repo, settings)
    elif mode == "fresh-session":
        if expected.get("mode") != "FRESH_OPENMA_SESSION":
            raise ReplayError(
                "Full reproduction requires a fresh-session bundle; use explicit diagnostic mode for captured tools"
            )
        actual = await run_operational_replay(source, repo, settings)
    else:
        raise ReplayError("Unknown replay mode")

    matches = actual["output_sha256"] == expected["output_sha256"]
    if mode == "captured-tools":
        if not matches:
            raise ReplayError("bundle reproduction differs from immutable output")
        return {"reproduced": True, "input_sha256": sha, "output_sha256": actual["output_sha256"]}

    semantic_expected = semantic_verdict_output(expected["output"])
    semantic_actual = semantic_verdict_output(actual["output"])
    semantic_match = actual.get("status", "COMPLETED") == "COMPLETED" and semantic_expected == semantic_actual
    actual["semantic_verdict_sha256"] = digest(semantic_actual)
    actual["reference_semantic_verdict_sha256"] = digest(semantic_expected)
    actual["semantic_verdict_match"] = semantic_match
    destination = directory / "reproductions" / (actual["replay_run_id"] + ".json")
    freeze(destination, actual)
    return {
        "reproduced": semantic_match,
        "raw_output_match": matches,
        "semantic_verdict_match": semantic_match,
        "mode": "FRESH_OPENMA_SESSION",
        "analysis_may_differ": True,
        "input_sha256": sha,
        "reference_output_sha256": expected["output_sha256"],
        "output_sha256": actual["output_sha256"],
        "session_id": actual["session_id"],
        "report_path": str(destination),
    }


def original_replay_messages(inputs):
    """Use explicit captured messages or the original durable message log.

    Tool-call order cannot reconstruct analyst messages, so never infer NEW_ITEM
    from a tool's arguments or fabricate a request when the capture is absent.
    """
    explicit = inputs.config.get("replay_messages")
    if explicit is not None:
        return copy.deepcopy(explicit)
    source_id = inputs.config.get("replay_source_session_id")
    sessions = [
        row["record"]
        for row in inputs._records
        if row["collection"] == "ops.agent_sessions"
        and row["record"].get("message_log")
        and (not source_id or row["record"].get("session_id") == source_id)
    ]
    if len(sessions) != 1:
        raise ReplayError(
            "insufficient_fixture: missing original replay_messages or ambiguous original session"
        )
    session = sessions[0]
    messages = []
    for entry in session["message_log"]:
        if not entry.get("sent_at") or not isinstance(entry.get("payload"), dict):
            raise ReplayError("insufficient_fixture: incomplete original message log")
        if parse_time(entry["sent_at"]) > parse_time(inputs.cutoff):
            continue
        payload = copy.deepcopy(entry["payload"])
        for field in ("session_id", "replay_run_id"):
            payload.pop(field, None)
        payload["replay_source_session_id"] = session["session_id"]
        payload.update(propagate_timestamp(entry, "sent_at").fields("replay_at"))
        messages.append(payload)
    return messages


def bind_replay_result_lineage(original, actual, substitutions):
    """Bind identities emitted by the corresponding real tool execution.

    Economic values are never copied from the capture. Condition identities
    require the same role and expression, not their old mutable snapshot.
    """
    if not isinstance(original, dict) or not isinstance(actual, dict):
        return
    for key in ("event_id", "dec_id", "ems_id", "mta_id", "alert_id", "outlook_id", "obs_id"):
        if isinstance(original.get(key), str) and isinstance(actual.get(key), str):
            substitutions[original[key]] = actual[key]
    old_conditions, fresh_conditions = original.get("conditions"), actual.get("conditions")
    if isinstance(old_conditions, list) and isinstance(fresh_conditions, list):
        for old in old_conditions:
            if not isinstance(old, dict) or not old.get("cond_id"):
                continue
            matches = [
                fresh
                for fresh in fresh_conditions
                if isinstance(fresh, dict)
                and fresh.get("role") == old.get("role")
                and fresh.get("expr") == old.get("expr")
            ]
            if len(matches) == 1 and matches[0].get("cond_id"):
                substitutions[old["cond_id"]] = matches[0]["cond_id"]


async def resolve_replay_message_references(message, inputs, repo, substitutions, *, event_scope=None):
    """Resolve recorded timer references by exact logical lineage, never old state.

    Lifecycle/status fields are not object identity. Embedded old call/condition
    snapshots are replaced by the actual fresh row after its immutable identity
    fields match. Missing or ambiguous counterparts stop the run explicitly.
    """
    specs = {
        "claim_id": (
            "evt.claims",
            "claim_id",
            (
                "event_id",
                "type",
                "subject",
                "predicate",
                "object",
                "fact",
                "state_claim",
                "certainty",
                "evidence",
                "verified",
                "accepted",
                "directionality",
                "public_at",
                "public_at_prec",
            ),
        ),
        "call_id": (
            "ana.calls",
            "call_id",
            (
                "event_id",
                "token_id",
                "opened_at",
                "opened_by_version",
                "entry_ref_price",
                "invalidation_price",
                "expires_at",
            ),
        ),
        "cond_id": (
            "ana.watch_conditions",
            "cond_id",
            ("event_id", "token_id", "role", "expr", "created_at", "expires_at"),
        ),
        "condition_id": (
            "ana.watch_conditions",
            "cond_id",
            ("event_id", "token_id", "role", "expr", "created_at", "expires_at"),
        ),
        "dec_id": (
            "ana.decisions",
            "dec_id",
            ("event_id", "token_id", "cutoff", "action", "action_rule", "thesis_grade", "trade_grade"),
        ),
        "alert_id": (
            "out.alerts",
            "alert_id",
            ("event_id", "version", "kind", "generated_at", "shown_tokens"),
        ),
    }
    aliases = {"created_by": "dec_id", "last_shown_by": "dec_id", "opened_by_alert": "alert_id"}
    resolved = {}

    def mapped(value):
        return substitutions.get(value, value) if isinstance(value, str) else value

    async def counterpart(field, identifier, embedded=None):
        field = aliases.get(field, field)
        collection, id_field, identity_fields = specs[field]
        key = (collection, identifier)
        if key in resolved:
            return resolved[key]
        originals = [
            row["record"]
            for row in inputs._records
            if row["collection"] == collection and row["record"].get(id_field) == identifier
        ]
        if embedded is not None:
            originals.append(embedded)

        bound = substitutions.get(identifier)
        if bound and originals:
            causal = [
                row
                async for row in repo.iter_all(collection)
                if row.get(id_field) == bound and (not event_scope or row.get("event_id") == event_scope)
            ]
            if len(causal) == 1:
                fresh = causal[0]
                purpose_fields = ("event_id", "token_id", "role", "expr")
                if any(
                    all(mapped(old.get(name)) == fresh.get(name) for name in purpose_fields)
                    for old in originals
                ):
                    resolved[key] = fresh
                    return fresh
                raise ReplayDivergence("Fresh causal counterpart has a different structural purpose")
        signatures = {
            canonical_json({name: mapped(row.get(name)) for name in identity_fields}) for row in originals
        }
        if len(signatures) != 1:
            raise ReplayError("insufficient_fixture: missing or ambiguous original timer lineage")
        signature = next(iter(signatures))
        matches = [
            row
            async for row in repo.iter_all(collection)
            if (not event_scope or row.get("event_id") == event_scope)
            and canonical_json({name: row.get(name) for name in identity_fields}) == signature
        ]
        if len(matches) != 1:
            raise ReplayDivergence("Replay timer lineage has no unique fresh counterpart")
        fresh = matches[0]
        substitutions[identifier] = fresh[id_field]
        resolved[key] = fresh
        return fresh

    async def visit(value, field=None):
        if isinstance(value, dict):
            if field in {"call", "condition"}:
                id_field = "call_id" if field == "call" else "cond_id"
                if not value.get(id_field):
                    raise ReplayError("Recorded timer object lacks its identity")
                return copy.deepcopy(await counterpart(id_field, value[id_field], value))
            return {key: await visit(entry, key) for key, entry in value.items()}
        if isinstance(value, list):
            return [await visit(entry, field) for entry in value]
        if isinstance(value, str) and field == "evidence":
            captured_claim = any(
                row["collection"] == "evt.claims" and row["record"].get("claim_id") == value
                for row in inputs._records
            )
            if captured_claim:
                return (await counterpart("claim_id", value))["claim_id"]
            if await repo.get("ingest.source_items", value) is None:
                raise ReplayError("Replay evidence reference has no captured claim or source identity")
            return value
        if isinstance(value, str) and (field in specs or field in aliases):
            actual_field = aliases.get(field, field)
            fresh = await counterpart(field, value)
            return fresh[specs[actual_field][1]]
        selected = mapped(value)
        if field == "event_id" and event_scope and selected != event_scope:
            raise ReplayDivergence("Replay message event reference does not resolve to its fresh session")
        return selected

    return await visit(message)


def replay_message_timestamp(inputs, message):
    """Select one clock anchor from original messages or earliest source revision."""
    if message.get("replay_at"):
        return propagate_timestamp(message, "replay_at")
    candidates = [
        (index, row)
        for index, row in enumerate(inputs._records)
        if row["collection"] == "ingest.source_items"
        and row["record"].get("item_id") == message.get("item_id")
    ]
    if not candidates:
        raise ReplayError("insufficient_fixture: replay source item or timestamp is absent")
    _, source = min(candidates, key=lambda entry: (parse_time(entry[1]["available_at"]), entry[0]))
    record = source["record"]
    if record.get("observed_at"):
        return propagate_timestamp(record, "observed_at")
    return propagate_timestamp(source, "available_at")


async def run_operational_replay(bundle_path, repo, settings, *, http=None):
    """Compose the actual agent/session path; no captured-tool fallback.

    Only the OpenMA control plane is contacted. External tool reads use the
    captured packet and market calculations execute against its exact snapshots.
    The registered tank-replay environment supplies the isolated offline sandbox.
    """
    import httpx

    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import MarketTruthService
    from tank.event_tools import EventToolService
    from tank.util import new_id

    inputs = load_inputs(bundle_path, settings)
    messages = original_replay_messages(inputs)
    if not isinstance(messages, list) or not messages:
        raise ReplayError("insufficient_fixture: missing original replay_messages")
    if any(
        not isinstance(message, dict)
        or message.get("kind")
        not in {"NEW_ITEM", "ITEM_ADDED", "CONDITION", "CONDITION_FIRED", "MILESTONE", "CALL_EXPIRED"}
        or (message.get("kind") in {"NEW_ITEM", "ITEM_ADDED"} and not message.get("item_id"))
        or message.get("session_id")
        for message in messages
    ):
        raise ReplayError("Replay messages must preserve original item messages without remote session IDs")
    if not settings.replay:
        raise ReplayError("Operational replay requires isolated replay settings")
    from tank.replay_evidence import ReplayEvidenceView

    run_id = new_id("RPL")
    first_timestamp = replay_message_timestamp(inputs, messages[0])
    evidence_view = ReplayEvidenceView(
        inputs,
        run_id,
        settings.data_dir / "replay" / "evidence" / run_id,
        settings.data_dir / "replay" / "private-evidence" / run_id,
        input_sha256=hashlib.sha256(Path(bundle_path).read_bytes()).hexdigest(),
    )
    evidence = evidence_view.advance(first_timestamp)
    truth = MarketTruthService(repo, settings, ReplayMarketReader(inputs, evidence))
    from tank.fixture_reference import TrustedFixtureClock

    tools = EventToolService(
        repo,
        settings,
        truth,
        DecisionEngine(settings),
        reference_clock=TrustedFixtureClock(inputs) if inputs.fixture_reference else None,
    )
    client = http or httpx.AsyncClient(timeout=httpx.Timeout(30, read=None))
    attempt_path = settings.data_dir / "replay" / "attempts" / (run_id + ".json")
    try:
        return await replay_agent_session(
            bundle_path,
            repo,
            settings,
            tools,
            http=client,
            initial_message=messages[0],
            subsequent_messages=messages[1:],
            replay_run_id=run_id,
            output_path=attempt_path,
            evidence_capture=evidence,
            evidence_view=evidence_view,
            timeout=inputs.config.get("replay_timeout_seconds", 480),
        )
    finally:
        if http is None:
            await client.aclose()


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
    subsequent_messages=(),
    replay_run_id=None,
    timeout=30,
    output_path=None,
    evidence_capture=None,
    evidence_view=None,
):
    """Run a fresh tank-replay turn against explicit offline external adapters.

    The caller supplies the control-plane transport (a captured transcript in
    tests, the real OpenMA API operationally) and a captured market-data reader. Domain tools
    execute production code; SessionService reuses captures only for external
    web reads. Historical delivery transitions use captured receipts only. First-reference
    fixtures retain generated delivery intents QUEUED and export delivery NOT_RUN;
    no transport, SENT state, message reference, or delivery clock is fabricated.
    A live VM/model turn requires an
    actually provisioned isolated OpenMA environment; this function does not
    silently replace that with a deterministic interpretation.
    """
    import asyncio
    from contextlib import ExitStack
    from unittest.mock import patch

    import httpx

    from tank import delivery as delivery_module
    from tank import event_tools as tool_module
    from tank import scheduler as scheduler_module
    from tank import storage, util
    from tank.agents import sessions as session_module
    from tank.agents.sessions import CapturedToolResult, SessionService
    from tank.analysis import cards, truth
    from tank.analysis.decisions import DecisionEngine
    from tank.analysis.truth import MarketTruthService
    from tank.delivery import DeliveryError, DeliveryService
    from tank.event_tools import EventToolService
    from tank.models import TABLE_COLUMNS
    from tank.scheduler import Scheduler

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
    from tank.fixture_reference import TrustedFixtureClock

    if inputs.fixture_reference:
        if (
            type(event_tools.reference_clock) is not TrustedFixtureClock
            or event_tools.reference_clock.input_sha256 != inputs.input_sha256
        ):
            raise ReplayError("fixture reference replay requires its exact trusted preregistered clock")
    elif event_tools.reference_clock is not None:
        raise ReplayError("ordinary replay cannot use a fixture reference clock")
    if http is None:
        raise ReplayError("fresh replay requires an explicitly supplied control-plane transport")
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
    timeline = [
        replay_message_timestamp(inputs, message) for message in [initial_message, *subsequent_messages]
    ]
    if any(parse_time(right.value) < parse_time(left.value) for left, right in pairwise(timeline)):
        raise ReplayError("Original replay messages are not chronological")
    clock = ReplayClock(timeline[0], inputs.cutoff)
    if inputs.fixture_reference and inputs.fixture_reference.get("phases"):
        if (
            output_path
            and evidence_view
            and Path(output_path).resolve().is_relative_to(evidence_view.destination.resolve())
        ):
            raise ReplayError("Reference phase checkpoints must remain outside guest evidence")
        event_tools.truth.market.active_cutoff = clock.now

    async def advance_clock(timestamp):
        clock.advance(timestamp)
        if evidence_view:
            current = evidence_view.advance(clock.timestamp())
            if active_session_id:
                await service.request(
                    "POST",
                    f"sessions/{active_session_id}",
                    json={
                        "metadata": {
                            "tank_replay_run_id": run_id,
                            "tank_replay_evidence_sha256": current["manifest_sha256"],
                            "tank_replay_cutoff": current["manifest"]["cutoff"],
                        }
                    },
                )
                mapped = await repo.get("ops.agent_sessions", active_session_id)
                mapped.update(
                    replay_evidence_sha256=current["manifest_sha256"],
                    replay_cutoff=current["manifest"]["cutoff"],
                )
                await repo.put("ops.agent_sessions", active_session_id, mapped)

    active_session_id = None
    imported = set()

    async def import_prefix(*, baseline=False):
        market_batches = {}
        for revision, row in sorted(
            enumerate(inputs._records),
            key=lambda entry: (parse_time(inputs.available_at(entry[1])), entry[0]),
        ):
            projected = row["record"]
            identity = (revision, digest(projected))
            if identity in imported or row["collection"] in {
                "replay.market_snapshots",
                "ops.tool_calls",
            }:
                continue

            if not baseline and not (
                row["collection"].startswith("ingest.")
                or _market_record(row)
                or _late_unlock(row, inputs.cutoff)
            ):
                continue
            if inputs.record_visible(row, clock.now()):
                collection = row["collection"]
                if collection in MARKET_COLUMNS:
                    batch = market_batches.setdefault(collection, [])
                    batch.append((identity, row))
                    if len(batch) == 500:
                        await repo.put_market_batch(
                            collection,
                            [copy.deepcopy(entry["record"]) for _, entry in batch],
                            keys=[entry["key"] for _, entry in batch],
                        )
                        imported.update(identity for identity, _ in batch)
                        batch.clear()
                else:
                    record = copy.deepcopy(projected)
                    if collection == "ingest.source_items":
                        fresh = await repo.get(collection, row["key"])
                        for field in (
                            "event_id",
                            "session_id",
                            "extraction_status",
                            "route_event_id",
                            "route",
                            "routing_message",
                            "routing_scores",
                            "status",
                            "relevance",
                            "relevance_decision",
                            "relevance_p",
                        ):
                            record.pop(field, None)
                            if fresh and field in fresh:
                                record[field] = fresh[field]
                        if not fresh or not (
                            "relevance" in fresh or fresh.get("relevance_decision") is not None
                        ):
                            from tank.sources.policy import initial_relevance

                            record["relevance"] = initial_relevance(
                                record.get("source_id"), record.get("subtype"), record.get("author_role")
                            )
                        if fresh:
                            for field in ("observed_at", "published_at", "public_at", "fetched_at"):
                                if field in record and record[field] != fresh.get(field):
                                    record.update(propagate_timestamp(record, field).fields(field))
                            record = {**fresh, **record}
                        if "extraction_status" not in record:
                            record["extraction_status"] = "PENDING"
                    await repo.put(collection, row["key"], record)
                    imported.add(identity)
        for collection, batch in market_batches.items():
            if batch:
                await repo.put_market_batch(
                    collection,
                    [copy.deepcopy(entry["record"]) for _, entry in batch],
                    keys=[entry["key"] for _, entry in batch],
                )
                imported.update(identity for identity, _ in batch)

    await import_prefix(baseline=True)
    if not await repo.get("ingest.source_items", initial_message["item_id"]):
        raise ReplayError("insufficient_fixture: initial source item is absent")
    original_sessions = [
        row["record"]
        for row in inputs._records
        if row["collection"] == "ops.agent_sessions"
        and (
            row["record"].get("event_id") == initial_message.get("event_id")
            if initial_message.get("event_id")
            else row["record"].get("opened_by_item", row["record"].get("item_id"))
            == initial_message["item_id"]
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
    capture_receipts = []
    identity_substitutions = {}
    causal_tool_receipts = []
    consumed_source_calls = set()

    def mapped_capture(value):
        if isinstance(value, dict):
            return {key: mapped_capture(entry) for key, entry in value.items()}
        if isinstance(value, list):
            return [mapped_capture(entry) for entry in value]
        return identity_substitutions.get(value, value) if isinstance(value, str) else value

    def corresponding_tool_call(session, tool, arguments):
        candidates = [
            row
            for row in inputs._records
            if row["collection"] == "ops.tool_calls"
            and row["key"] not in consumed_source_calls
            and row["record"].get("session_id") == session.get("replay_source_session_id")
            and row["record"].get("tool") == tool
            and row["record"].get("status") == "DONE"
            and mapped_capture(row["record"].get("arguments", row["record"].get("input"))) == arguments
        ]

        return (
            min(
                candidates,
                key=lambda row: parse_time(row["record"].get("requested_at") or row["available_at"]),
            )
            if candidates
            else None
        )

    class CapturedExternalSessionService(SessionService):
        def clock_timestamp(self):
            return clock.timestamp()

        async def invoke_tool(self, session, tool, arguments, call_id, **internal):
            if tool not in {"web_search", "fetch_page"}:
                resolved = internal.setdefault("resolved_arguments", {})
                actual = await super().invoke_tool(session, tool, arguments, call_id, **internal)
                if internal.get("decision_market_refresh_only"):
                    return actual
                source = corresponding_tool_call(session, tool, resolved or arguments)
                if source is not None:
                    consumed_source_calls.add(source["key"])
                    before = dict(identity_substitutions)
                    bind_replay_result_lineage(source["record"].get("result"), actual, identity_substitutions)
                    causal_tool_receipts.append(
                        {
                            "tool": tool,
                            "source_call_id": source["record"].get("call_id"),
                            "fresh_tool_use_id": call_id,
                            "policy": "Exact arguments and chronological occurrence",
                            "identity_bindings": {
                                key: value
                                for key, value in identity_substitutions.items()
                                if before.get(key) != value
                            },
                        }
                    )
                return actual
            original = session.get("replay_source_session_id")
            if inputs.fixture_reference:
                capture = inputs.reference_capture(tool, arguments, clock.now())
                selected_at = propagate_timestamp({"at": inputs.available_at(capture)}, "at")
                if parse_time(selected_at.value) > clock.now():
                    await advance_clock(selected_at)
                await import_prefix()
                capture_receipts.append(
                    {
                        "tool": tool,
                        "origin": capture["origin"],
                        "source_item_id": capture.get("source_item_id"),
                        "actual_available_at": capture["available_at"],
                        "simulated_available_at": inputs.available_at(capture),
                        "status": capture["status"],
                        "is_error": CapturedToolResult.from_record(capture).is_error,
                    }
                )
                return CapturedToolResult.from_record(capture)
            matches = [
                row
                for row in inputs._records
                if row["collection"] == "ops.tool_calls"
                and row["record"].get("session_id") == original
                and row["record"].get("tool") == tool
                and row["record"].get("status") in {"DONE", "ERROR", "REFUSED"}
                and row["record"].get("arguments", row["record"].get("input")) == arguments
            ]
            results = {CapturedToolResult.from_record(row["record"]).signature() for row in matches}
            if not original or len(results) != 1:
                raise ReplayError(
                    "insufficient_fixture: one unambiguous original external tool result required"
                )
            capture = min(matches, key=lambda row: parse_time(row["available_at"]))

            capture_timestamp = propagate_timestamp(capture, "available_at")
            if parse_time(capture_timestamp.value) > clock.now():
                await advance_clock(capture_timestamp)
            await import_prefix()
            capture_receipts.append(
                {
                    "tool": tool,
                    "source_session_id": original,
                    "source_call_id": capture["record"].get("call_id"),
                    "available_at": capture["available_at"],
                    "status": capture["record"]["status"],
                    "is_error": CapturedToolResult.from_record(capture["record"]).is_error,
                }
            )
            return CapturedToolResult.from_record(capture["record"])

    service = CapturedExternalSessionService(repo, settings, event_tools.dispatch, http=http)
    delivery_receipts = []
    active_delivery_receipt = None

    async def captured_telegram(request):
        if (
            request.method != "POST"
            or request.url.host != "api.telegram.org"
            or active_delivery_receipt is None
        ):
            raise ReplayError("Uncaptured replay delivery transport request")
        return httpx.Response(
            200, json={"ok": True, "result": {"message_id": int(active_delivery_receipt["message_ref"])}}
        )

    delivery_http = httpx.AsyncClient(transport=httpx.MockTransport(captured_telegram))
    delivery_settings = settings.model_copy(
        update={
            "telegram_token": "REPLAY_CAPTURED_RECEIPT_ONLY",
            "telegram_alerts_chat": "replay-captured-alerts",
            "telegram_ops_chat": "replay-captured-ops",
        }
    )
    delivery = DeliveryService(repo, delivery_settings, http=delivery_http)

    async def replay_delivery(payload):
        nonlocal active_delivery_receipt
        if payload.get("notice_id"):
            raise ReplayDivergence("Captured replay of ops notices is not an analyst card transition")
        fresh = [
            row
            async for row in repo.iter_all(
                "out.alerts", filters={"alert_id": payload["alert_id"], "version": payload["version"]}
            )
        ]
        if len(fresh) != 1:
            raise ReplayDivergence("Replay delivery has no unique fresh alert")
        fresh = fresh[0]
        original_session = next(
            (
                row["record"]
                for row in inputs._records
                if row["collection"] == "ops.agent_sessions" and row["record"].get("session_id") == source_id
            ),
            {},
        )
        old_event = original_session.get("event_id") or initial_message.get("event_id")
        original_alerts = [
            row["record"]
            for row in inputs._records
            if row["collection"] == "out.alerts"
            and row["record"].get("event_id") == old_event
            and row["record"].get("version") == fresh["version"]
            and row["record"].get("kind") == fresh["kind"]
        ]
        if len(original_alerts) != 1:
            raise ReplayDivergence("Replay delivery has no unique original publication lineage")
        original_alert = original_alerts[0]
        receipts = [
            row["record"]
            for row in inputs._records
            if row["collection"] == "out.deliveries"
            and row["record"].get("alert_id") == original_alert["alert_id"]
            and row["record"].get("version") == original_alert["version"]
            and row["record"].get("channel") == payload.get("channel", "telegram:alerts")
            and row["record"].get("status") == "SENT"
        ]
        if len(receipts) != 1 or not receipts[0].get("message_ref"):
            raise ReplayError("insufficient_fixture: one captured SENT delivery receipt required")
        receipt = receipts[0]
        sent = propagate_timestamp(receipt, "sent_at" if receipt.get("sent_at") else "at")
        if sent.value is None:
            raise ReplayError("insufficient_fixture: captured delivery timestamp required")
        if parse_time(sent.value) > clock.now():
            await advance_clock(sent)
            await import_prefix()
        active_delivery_receipt = receipt
        try:
            outcome = await delivery.deliver(payload)
        finally:
            active_delivery_receipt = None
        if isinstance(outcome, DeliveryError):
            raise ReplayDivergence("Fresh delivery code rejected or deferred the recorded SENT transition")
        identity_substitutions[original_alert["alert_id"]] = fresh["alert_id"]
        call_bindings = {}
        async for call in repo.iter_all(
            "ana.calls", filters={"opened_by_alert": fresh["alert_id"], "opened_by_version": fresh["version"]}
        ):
            originals = {
                row["record"]["call_id"]
                for row in inputs._records
                if row["collection"] == "ana.calls"
                and row["record"].get("opened_by_alert") == original_alert["alert_id"]
                and row["record"].get("opened_by_version") == original_alert["version"]
                and row["record"].get("token_id") == call["token_id"]
            }
            if len(originals) == 1:
                old = next(iter(originals))
                identity_substitutions[old] = call["call_id"]
                call_bindings[old] = call["call_id"]
        delivery_receipts.append(
            {
                "original_alert_id": original_alert["alert_id"],
                "fresh_alert_id": fresh["alert_id"],
                "version": fresh["version"],
                "message_ref": receipt["message_ref"],
                **sent.fields("captured_sent_at"),
                "call_bindings": call_bindings,
                "transport": "CAPTURED_RECEIPT_NO_NETWORK",
            }
        )
        return {"captured_delivery": True}

    scheduler = Scheduler(repo, settings, event_tools.truth.market, event_tools.truth)
    timer_receipts = []
    selected_timer_timestamp = None

    async def rerun_timer(message, original):
        nonlocal selected_timer_timestamp
        kind = message.get("kind")
        if kind not in {"CONDITION", "CONDITION_FIRED", "CALL_EXPIRED", "MILESTONE"}:
            return message, None
        trigger = replay_message_timestamp(inputs, original)
        before = {row["job_id"] async for row in repo.iter_all("ops.jobs", filters={"type": "AGENT_MESSAGE"})}
        selected_timer_timestamp = trigger
        try:
            if kind == "CALL_EXPIRED":
                await scheduler.expire_calls(parse_time(trigger.value))
                identity = (message.get("call") or {}).get("call_id")
                canonical_kind = kind
            elif kind in {"CONDITION", "CONDITION_FIRED"}:
                if parse_time(trigger.value) < clock.now():
                    raise ReplayDivergence(
                        "Original condition trigger predates the fresh available-state clock"
                    )
                await scheduler.eval_conditions(trigger.fields("cutoff"))
                identity = (message.get("condition") or {}).get("cond_id")
                canonical_kind = "CONDITION_FIRED"
            else:
                await scheduler.milestone_check(message)
                identity, canonical_kind = message.get("milestone"), kind
        finally:
            selected_timer_timestamp = None
        generated = [
            row
            async for row in repo.iter_all("ops.jobs", filters={"type": "AGENT_MESSAGE"})
            if row["job_id"] not in before
            and row["payload"].get("kind") == canonical_kind
            and row["payload"].get("event_id") == message.get("event_id")
            and (
                (row["payload"].get("call") or {}).get("call_id") == identity
                if canonical_kind == "CALL_EXPIRED"
                else (row["payload"].get("condition") or {}).get("cond_id") == identity
                if canonical_kind == "CONDITION_FIRED"
                else row["payload"].get("milestone") == identity
            )
        ]
        if len(generated) != 1:
            raise ReplayDivergence(
                "Recorded timer predicate does not fire for the actual fresh object at its original trigger"
            )
        job = generated[0]
        actual = job["payload"]
        if kind == "MILESTONE" and any(
            message.get(field) != actual.get(field)
            for field in ("outcome", "quotes", "evidence", "verification_required")
        ):
            raise ReplayDivergence("Fresh milestone evidence/outcome differs from the recorded trigger")
        controls = {key: value for key, value in message.items() if key.startswith("replay_")}
        timer_receipts.append(
            {
                "original_kind": kind,
                "actual_kind": canonical_kind,
                "job_id": job["job_id"],
                **trigger.fields("original_trigger_at"),
                "payload_sha256": digest(actual),
                "predicate": "RERUN_PRODUCTION_SCHEDULER",
            }
        )
        return dict(actual, **controls), job

    pending_messages = iter(subsequent_messages)
    divergence = None
    accepted_messages = [copy.deepcopy(initial_message)]
    phase_checkpoints = []

    async def checkpoint_phase(execution, calls):
        phases = (inputs.fixture_reference or {}).get("phases", [])
        phase = next(
            (phase for phase in phases if max(phase["message_indexes"]) == len(accepted_messages) - 1), None
        )
        if not phase or any(row["phase_id"] == phase["phase_id"] for row in phase_checkpoints):
            return

        follower = service.tasks.get(sid)
        if follower is not None and not follower.done():
            follower.cancel()
            await asyncio.gather(follower, return_exceptions=True)
        collections = (
            "ingest.source_items",
            "evt.events",
            "evt.claims",
            "evt.ems_assessments",
            "evt.beneficiary_maps",
            "evt.beneficiary_edges",
            "ana.market_truth",
            "ana.decisions",
            "out.alerts",
            "out.evidence_bundles",
        )
        state = {name: [row async for row in repo.iter_all(name)] for name in collections}
        snapshot = {
            "phase_id": phase["phase_id"],
            "at": phase["at"],
            "at_prec": phase["at_prec"],
            "actual_checkpoint_at": datetime.now(UTC).isoformat(),
            "actual_checkpoint_at_prec": "COLLECTION_ONLY",
            "session_id": sid,
            "execution_barrier": copy.deepcopy(execution),
            "callbacks": [
                {
                    "call_id": call["call_id"],
                    "ordinal": call.get("native_turn_call_index"),
                    "returned_at": call.get("returned_at"),
                    "sha256": digest(call),
                }
                for call in calls
            ],
            "state": state,
            "session_workflow": copy.deepcopy(
                (await repo.get("ops.agent_sessions", sid)).get("verified_source_completion")
            ),
            "delivery_intents": [row async for row in repo.iter_all("ops.jobs", filters={"type": "DELIVER"})],
            "external_capture_gaps": reference_evidence_gaps(calls),
            "requested_external_captures_complete": not reference_evidence_gaps(calls),
            "evidence_manifest_sha256": evidence_view.receipts[-1]["manifest_sha256"]
            if evidence_view and evidence_view.receipts
            else None,
        }
        snapshot["sha256"] = digest(snapshot)
        if output_path:
            freeze(Path(output_path).parent / (run_id + "-phase-" + phase["phase_id"] + ".json"), snapshot)
        phase_checkpoints.append(snapshot)

    worker = None
    sid = None
    session_ids = []
    execution_barriers = {}
    report = None
    failure = None
    initial_decisions = {r["dec_id"] async for r in repo.iter_all("ana.decisions")}
    initial_alerts = {(r["alert_id"], r["version"]) async for r in repo.iter_all("out.alerts")}

    async def market_jobs():
        while True:
            session = await repo.get("ops.agent_sessions", sid)
            eid = (session or {}).get("event_id")
            async for job in repo.iter_all("ops.jobs", filters={"status": "QUEUED"}):
                if job["type"] == "DELIVER":
                    if inputs.fixture_reference:
                        continue

                    outcome = await replay_delivery(job["payload"])
                    await repo.put("ops.jobs", job["job_id"], dict(job, result=outcome, status="DONE"))
                    continue
                if (
                    not eid
                    or job["payload"].get("event_id") != eid
                    or job["type"] not in {"ESCALATE_TOKENS", "RUN_MARKET_TRUTH"}
                ):
                    continue
                if job.get("run_after") and parse_time(job["run_after"]) > clock.now():
                    continue
                dependencies = [
                    await repo.get("ops.jobs", key) for key in job["payload"].get("_depends_on", [])
                ]
                if any(row is None or row.get("status") != "DONE" for row in dependencies):
                    continue
                result = None
                if job["type"] == "RUN_MARKET_TRUTH":
                    payload = job["payload"]
                    if parse_time(payload["cutoff"]) > clock.now():
                        raise ReplayError("market request exceeds replay clock")
                    result = await event_tools.truth.assess(eid, payload["token_id"], payload["cutoff"])
                await repo.put("ops.jobs", job["job_id"], dict(job, result=result, status="DONE"))
            await asyncio.sleep(0.01)

    original_iso = util.iso

    def replay_iso(value=None):
        return original_iso(clock.now() if value is None else value)

    with ExitStack() as stack:
        for module in (
            util,
            storage,
            tool_module,
            session_module,
            cards,
            truth,
            delivery_module,
            scheduler_module,
        ):
            if hasattr(module, "utcnow"):
                stack.enter_context(patch.object(module, "utcnow", clock.now))
            if hasattr(module, "iso"):
                stack.enter_context(patch.object(module, "iso", replay_iso))
        stack.enter_context(
            patch.object(delivery_module, "collection_clock", lambda value=None, **kwargs: clock.timestamp())
        )
        stack.enter_context(
            patch.object(
                scheduler_module,
                "collection_clock",
                lambda value=None, **kwargs: selected_timer_timestamp or clock.timestamp(),
            )
        )
        stack.enter_context(
            patch.object(
                scheduler_module,
                "utcnow",
                lambda: (
                    parse_time(selected_timer_timestamp.value) if selected_timer_timestamp else clock.now()
                ),
            )
        )
        try:
            deadline = asyncio.get_running_loop().time() + timeout
            async with asyncio.timeout_at(deadline):
                session = await service.send_message(
                    dict(
                        initial_message,
                        replay_run_id=run_id,
                        **(
                            {
                                "replay_evidence_sha256": evidence_capture["manifest_sha256"],
                                "replay_cutoff": evidence_capture["manifest"]["cutoff"],
                            }
                            if evidence_capture
                            else {}
                        ),
                    )
                )
            sid = session["session_id"]
            session_ids.append(sid)
            active_session_id = sid
            worker = asyncio.create_task(market_jobs())
            async with asyncio.timeout_at(deadline):
                while True:
                    if worker.done():
                        try:
                            await worker
                        except ReplayDivergence as exc:
                            divergence = {
                                "reason": str(exc),
                                "stage": "CAPTURED_DELIVERY",
                                "policy": "No original future state imported",
                            }
                            break
                    current = await repo.get("ops.agent_sessions", sid)
                    if current.get("status") == "FAILED":
                        raise ReplayError("replay agent turn failed before a settled completion")
                    if current.get("status") == "ENDED" and any(
                        message.get("turn_status") != "DONE" for message in current.get("message_log", [])
                    ):
                        raise ReplayError("replay agent session terminated before a settled completion")
                    calls = [
                        row async for row in repo.iter_all("ops.tool_calls", filters={"session_id": sid})
                    ]
                    if any(missing_capture_error(call) for call in calls):
                        raise ReplayError("insufficient_fixture: required replay tool capture is absent")

                    queued_deliveries = [
                        row
                        async for row in repo.iter_all(
                            "ops.jobs", filters={"status": "QUEUED", "type": "DELIVER"}
                        )
                    ]
                    execution = await service.completed_turn(sid)

                    if (
                        execution["complete"]
                        and (inputs.fixture_reference or not queued_deliveries)
                        and all(call.get("returned_at") for call in calls)
                    ):
                        execution_barriers[sid] = copy.deepcopy(execution)
                        await checkpoint_phase(execution, calls)
                        message = next(pending_messages, None)
                        if message is None:
                            incomplete = []
                            for accepted_message in accepted_messages:
                                if accepted_message.get("kind") in {"NEW_ITEM", "ITEM_ADDED"}:
                                    source = await repo.get(
                                        "ingest.source_items", accepted_message["item_id"]
                                    )
                                    if not source or source.get("extraction_status") not in {
                                        "DONE",
                                        "SKIPPED",
                                    }:
                                        incomplete.append(accepted_message["item_id"])
                            if incomplete:
                                raise ReplayError(
                                    "incomplete_replay: source extraction did not finish: "
                                    + ", ".join(sorted(set(incomplete)))
                                )
                            break
                        if message.get("session_id") or message.get("kind") not in {
                            "NEW_ITEM",
                            "ITEM_ADDED",
                            "CONDITION",
                            "CONDITION_FIRED",
                            "MILESTONE",
                            "CALL_EXPIRED",
                        }:
                            raise ReplayError("Invalid subsequent replay message")
                        scheduled_at = replay_message_timestamp(inputs, message)
                        if parse_time(scheduled_at.value) >= clock.now():
                            await advance_clock(scheduled_at)
                        await import_prefix()
                        original_session = next(
                            (
                                row["record"]
                                for row in inputs._records
                                if row["collection"] == "ops.agent_sessions"
                                and row["record"].get("session_id") == source_id
                            ),
                            {},
                        )
                        if original_session.get("event_id") and current.get("event_id"):
                            identity_substitutions[original_session["event_id"]] = current["event_id"]
                        try:
                            resolved_message = await resolve_replay_message_references(
                                message,
                                inputs,
                                repo,
                                identity_substitutions,
                                event_scope=None
                                if message["kind"] == "NEW_ITEM"
                                else current.get("event_id"),
                            )
                            resolved_message, timer_job = await rerun_timer(resolved_message, message)
                        except ReplayDivergence as exc:
                            divergence = {
                                "reason": str(exc),
                                "original_message": copy.deepcopy(message),
                                "policy": "No old snapshot imported; original trigger time retained; no clock movement to fresh expiry",
                            }
                            break
                        next_payload = dict(resolved_message, replay_run_id=run_id)
                        if message["kind"] == "NEW_ITEM":
                            source_id = message.get("replay_source_session_id")
                            current_evidence = (
                                evidence_view.advance(clock.timestamp())
                                if evidence_view
                                else evidence_capture
                            )
                            if current_evidence:
                                next_payload.update(
                                    replay_evidence_sha256=current_evidence["manifest_sha256"],
                                    replay_cutoff=current_evidence["manifest"]["cutoff"],
                                )
                        else:
                            next_payload["session_id"] = sid
                        if source_id:
                            next_payload["replay_source_session_id"] = source_id
                        next_session = await service.send_message(next_payload)
                        sid = next_session["session_id"]
                        active_session_id = sid
                        if sid not in session_ids:
                            session_ids.append(sid)
                        accepted_messages.append(copy.deepcopy(message))
                        if timer_job:
                            await repo.put(
                                "ops.jobs",
                                timer_job["job_id"],
                                dict(timer_job, status="DONE", result={"replay_session_id": sid}),
                            )
                    await asyncio.sleep(0.01)
            calls = [
                row async for row in repo.iter_all("ops.tool_calls") if row.get("session_id") in session_ids
            ]
            decisions = [
                r async for r in repo.iter_all("ana.decisions") if r["dec_id"] not in initial_decisions
            ]
            alerts = [
                r
                async for r in repo.iter_all("out.alerts")
                if (r["alert_id"], r["version"]) not in initial_alerts
            ]
            verdict_inputs = []
            for decision in decisions:
                provenance = decision.get("provenance", {})
                verdict_inputs.append(
                    {
                        "dec_id": decision["dec_id"],
                        "event": await repo.get("evt.events", decision["event_id"]),
                        "assessment": await repo.get("evt.ems_assessments", provenance["ems_id"])
                        if provenance.get("ems_id")
                        else None,
                        "market_truth": await repo.get("ana.market_truth", provenance["mta_id"])
                        if provenance.get("mta_id")
                        else None,
                        "claims": [
                            row
                            async for row in repo.iter_all(
                                "evt.claims", filters={"event_id": decision["event_id"]}
                            )
                        ],
                    }
                )
            report = {
                "replay_run_id": run_id,
                "replay_evidence": evidence_capture,
                "guest_prefix_receipts": evidence_view.receipts if evidence_view else [],
                "session_id": sid,
                "session_ids": session_ids,
                "mode": "FRESH_OPENMA_SESSION",
                "evidence_mode": inputs.evidence_mode,
                "fixture_reference": copy.deepcopy(inputs.fixture_reference),
                "historical_live_performance_claim": False if inputs.fixture_reference else None,
                "status": "DIVERGED" if divergence else "COMPLETED",
                "divergence": divergence,
                "messages": accepted_messages,
                "phase_checkpoints": phase_checkpoints,
                "external_capture_receipts": capture_receipts,
                "identity_substitutions": identity_substitutions,
                "causal_tool_receipts": causal_tool_receipts,
                "delivery_receipts": delivery_receipts,
                "timer_receipts": timer_receipts,
                "execution_barrier": execution,
                "execution_barriers": execution_barriers,
                "tool_error_receipts": [
                    {
                        "call_id": call["call_id"],
                        "tool": call["tool"],
                        "status": call["status"],
                        "error": call.get("result", {}).get("error"),
                    }
                    for call in calls
                    if call.get("is_error")
                ],
                "cutoff": inputs.cutoff,
                "config_version": inputs.config_version,
                "code_version": inputs.code_version,
                "input_sha256": digest(json.loads(Path(bundle_path).read_text())),
                "output": {
                    "decisions": decisions,
                    "alerts": alerts,
                    "verdict_inputs": verdict_inputs,
                    "events": [row async for row in repo.iter_all("evt.events")],
                    "observations": [row async for row in repo.iter_all("evt.event_observations")],
                    "claims": [row async for row in repo.iter_all("evt.claims")],
                    "state_changes": [row async for row in repo.iter_all("evt.state_changes")],
                    "assessments": [row async for row in repo.iter_all("evt.ems_assessments")],
                    "beneficiary_maps": [row async for row in repo.iter_all("evt.beneficiary_maps")],
                    "beneficiary_edges": [row async for row in repo.iter_all("evt.beneficiary_edges")],
                    "market_truth": [row async for row in repo.iter_all("ana.market_truth")],
                    "calls": [row async for row in repo.iter_all("ana.calls")],
                    "conditions": [row async for row in repo.iter_all("ana.watch_conditions")],
                },
                "performance_evidence": False,
                "assembled_at": inputs.assembled_at,
            }
            if inputs.fixture_reference:
                report.update(
                    delivery_status="NOT_RUN",
                    delivery_reason="FIRST_REFERENCE_NO_CAPTURED_DELIVERY",
                    delivery_intents=sorted(
                        [
                            copy.deepcopy(job)
                            async for job in repo.iter_all("ops.jobs", filters={"type": "DELIVER"})
                        ],
                        key=lambda job: job["job_id"],
                    ),
                )
            report["output_sha256"] = digest(report["output"])
            return report
        except BaseException as exc:
            failure = {
                "error_type": type(exc).__name__,
                "reason": str(exc)
                if isinstance(exc, ReplayError)
                else "Replay completion deadline expired"
                if isinstance(exc, TimeoutError)
                else "Replay caller cancelled execution"
                if isinstance(exc, asyncio.CancelledError)
                else "Execution failed; inspect the audited session/tool records",
            }
            raise
        finally:
            if worker:
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)

            await service.stop_followers()

            owned = [
                row async for row in repo.iter_all("ops.agent_sessions", filters={"replay_run_id": run_id})
            ]
            cleanup = []
            for row in owned:
                receipt = {"session_id": row["session_id"], "archived": False}
                if failure is not None:
                    try:
                        receipt["local_finalization"] = await service.finalize_replay_abort(
                            row["session_id"], run_id, failure
                        )
                    except Exception as exc:
                        receipt["local_finalization_error_type"] = type(exc).__name__
                try:
                    await service.request("POST", f"sessions/{row['session_id']}/archive")
                    receipt["archived"] = True
                except Exception as exc:
                    receipt["error_type"] = type(exc).__name__
                cleanup.append(receipt)
            await service.close()
            await delivery_http.aclose()
            await scheduler.close()
            if report is None and failure is not None:
                report = {
                    "mode": "FRESH_OPENMA_SESSION",
                    "status": "FAILED",
                    "replay_run_id": run_id,
                    "session_id": sid,
                    "failure": failure,
                    "messages": accepted_messages,
                    "input_sha256": digest(json.loads(Path(bundle_path).read_text())),
                    "config_version": inputs.config_version,
                    "code_version": inputs.code_version,
                    "cutoff": inputs.cutoff,
                    "output": None,
                    "output_sha256": digest(None),
                    "phase_checkpoints": phase_checkpoints,
                    "partial_output_not_collected": True,
                    "performance_evidence": False,
                }
            if report is not None:
                report["cleanup_sessions"] = cleanup
                if inputs.fixture_reference:
                    recorded_calls = [
                        row
                        async for row in repo.iter_all("ops.tool_calls")
                        if row.get("session_id") in session_ids
                    ]
                    gaps = reference_evidence_gaps(recorded_calls)
                    report["external_capture_gaps"] = gaps
                    report["requested_external_captures_complete"] = (
                        not gaps and report["status"] == "COMPLETED"
                    )
                if output_path:
                    freeze(output_path, report)
