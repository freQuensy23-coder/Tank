"""Explicit first fixture reconstruction, never a historical live-run claim."""

import base64
import json
import re
from datetime import UTC, date, datetime, timedelta
from importlib.resources import files
from pathlib import Path

from tank.fixture_reference_code import CURRENT_PROJECTION_CODE, ProjectionCodeEvidence
from tank.fixture_reference_sources import FILESYSTEM_RETAINED_SOURCES, RetainedSourceReader
from tank.time_precision import Timestamp, interval_bounds, propagate_timestamp
from tank.util import canonical_json, parse_time, sha256

MODE = "FIXTURE_REFERENCE_RECONSTRUCTION"


def retained_source_payload(
    raw_ref: str | Path, *, raw_sources: RetainedSourceReader = FILESYSTEM_RETAINED_SOURCES
) -> bytes:
    return raw_sources.read_payload(str(raw_ref))


def validate(
    packet,
    market_record,
    *,
    private_mask_path=None,
    input_path=None,
    raw_sources: RetainedSourceReader = FILESYSTEM_RETAINED_SOURCES,
    projection_code: ProjectionCodeEvidence = CURRENT_PROJECTION_CODE,
):
    """Validate integrity of frozen authored evidence; source derivation is independently reviewed.

    Hash agreement alone does not prove text was extracted from raw bytes or a
    provider version was public by a historical date. The preregistered packet
    builder and independent source review establish that semantic binding.
    """
    reference = packet.get("fixture_reference")
    if packet.get("replay_mode") != MODE:
        if any(
            row.get("record", {}).get("artifact_kind") == "REFERENCE_MISSING_MARKET_INPUTS"
            for row in packet.get("records", [])
        ):
            raise ValueError("Missing-market declarations require explicit fixture reference mode")
        if reference or any("simulated_available_at" in row for row in packet.get("records", [])):
            raise ValueError("Fixture reconstruction fields require the explicit reconstruction mode")
        return None
    if not isinstance(reference, dict) or reference.get("schema") != "tank.fixture-reference.v1":
        raise ValueError("Explicit fixture reference schema required")
    if (
        reference.get("historical_live_run") is not False
        or not isinstance(reference.get("fixture_id"), str)
        or not reference["fixture_id"]
    ):
        raise ValueError("Fixture reference must declare nonhistorical authored origin")
    if not packet.get("input_sha256") or not reference.get("preregistered_at"):
        raise ValueError("First fixture requires preregistration timestamp and immutable packet hash")
    parse_time(reference["preregistered_at"])
    if not packet.get("assembled_at"):
        raise ValueError("Fixture reconstruction requires actual assembled_at")
    assembled, cutoff = parse_time(packet["assembled_at"]), parse_time(packet["cutoff"])
    phases = reference.get("phases")
    initial_cutoff = parse_time(reference["cutoff_instant"]) if phases else cutoff
    if phases:
        _synthetic_manifest(reference)
        if reference["fixture_id"] != "F-24" or reference.get("cutoff_policy") != "RELATIVE_DECLARED":
            raise ValueError("Phased reference is limited to the declared synthetic F24 timeline")
        if not isinstance(phases, list) or len(phases) != 2:
            raise ValueError("F24 requires exactly two declared input phases")
        if any(set(phase) != {"phase_id", "at", "at_prec", "message_indexes"} for phase in phases):
            raise ValueError("Reference phases contain input coordinates only")
        if any(
            not isinstance(phase["phase_id"], str)
            or not re.fullmatch(r"[A-Za-z0-9_-]+", phase["phase_id"])
            or phase["at_prec"] != "EXACT"
            for phase in phases
        ):
            raise ValueError("Reference phases require named exact authored coordinates")
        if [phase["phase_id"] for phase in phases] != ["initial", "confirmation"]:
            raise ValueError("F24 phases must name initial and confirmation coordinates")
        if (
            phases[0]["phase_id"] == phases[1]["phase_id"]
            or not initial_cutoff == parse_time(phases[0]["at"]) < parse_time(phases[1]["at"]) == cutoff
        ):
            raise ValueError("Reference phases must retain the initial boundary and ordered final horizon")
    spec = reference.get("cutoff_spec")
    if not isinstance(spec, str) or not spec:
        raise ValueError("Fixture requires its preserved cutoff literal")
    if reference.get("cutoff_timezone") != "UTC":
        raise ValueError("Fixture cutoff timezone must explicitly retain the specified UTC domain")
    if reference.get("cutoff_policy") == "UTC_DAY_END_EXCLUSIVE":
        day = date.fromisoformat(reference["cutoff_day"])
        parsed_day = None
        for pattern in ("%Y-%m-%d", "%d %b %Y"):
            try:
                parsed_day = datetime.strptime(spec, pattern).date()
                break
            except ValueError:
                continue
        if parsed_day != day:
            raise ValueError("Canonical cutoff_day differs from original fixture literal")
        expected_day = (day + timedelta(days=1)).isoformat() + "T00:00:00Z"
        if packet["cutoff"] != expected_day:
            raise ValueError(
                "Day fixture boundary must be the whole named UTC day ending next midnight exclusive"
            )
    elif reference.get("cutoff_policy") == "EXACT":
        exact = reference.get("cutoff_instant")
        parsed_exact = None
        try:
            if "T" in spec and re.search(r"T[0-9]{2}:[0-9]{2}:[0-9]{2}", spec):
                parsed_exact = parse_time(spec)
        except (ValueError, TypeError):
            pass
        if parsed_exact is None:
            for pattern in ("%d %b %Y %H:%M:%S", "%d %b%Y %H:%M:%S"):
                try:
                    parsed_exact = datetime.strptime(spec, pattern).replace(tzinfo=UTC)
                    break
                except ValueError:
                    continue
        if parsed_exact is None:
            raise ValueError(
                "Minute-only fixture cutoff is insufficient without an explicit documented interval interpretation"
            )
        if packet["cutoff"] != exact or parse_time(exact) != parsed_exact:
            raise ValueError("Exact fixture canonical instant must match the preserved original UTC literal")
    elif reference.get("cutoff_policy") in {"MINUTE_START", "UTC_MINUTE_END_EXCLUSIVE"}:
        parsed_minute = None
        minute_spec = spec.removesuffix(" UTC") if isinstance(spec, str) else spec
        for pattern in ("%d %b %Y %H:%M", "%d %b%Y %H:%M", "%Y-%m-%dT%H:%MZ"):
            try:
                parsed_minute = datetime.strptime(minute_spec, pattern).replace(tzinfo=UTC)
                break
            except ValueError:
                continue
        end_exclusive = reference["cutoff_policy"] == "UTC_MINUTE_END_EXCLUSIVE"
        expected_minute = (
            interval_bounds(Timestamp(parsed_minute.isoformat(), "BUCKET(1m)", bucket_anchor="START"))[1]
            if parsed_minute is not None and end_exclusive
            else parsed_minute
        )
        basis = "WHOLE_NAMED_MINUTE_END_EXCLUSIVE" if end_exclusive else "CALLER_SELECTED_INTERVAL_START"
        if (
            parsed_minute is None
            or cutoff != expected_minute
            or reference.get("cutoff_instant") != packet["cutoff"]
            or reference.get("original_call_precision") != "BUCKET(1m)"
            or reference.get("chosen_boundary_basis") != basis
        ):
            raise ValueError(
                "Minute cutoff requires an explicit selected interval start with original precision"
            )
    elif reference.get("cutoff_policy") == "RELATIVE_DECLARED":
        _, upper = interval_bounds(propagate_timestamp(reference, "relative_anchor"))
        if (
            reference.get("relative_anchor_origin") != "DECLARED_SYNTHETIC_COORDINATE"
            or reference.get("offset_seconds") != 60
            or spec not in {"post + 1 min", "quote + 1 min"}
            or upper is None
            or initial_cutoff != parse_time(reference["relative_anchor"]) + timedelta(seconds=60)
            or parse_time(reference["cutoff_instant"]) != initial_cutoff
        ):
            raise ValueError(
                "Relative fixture requires a declared synthetic anchor and exact authored offset"
            )
        _synthetic_manifest(reference)
    else:
        raise ValueError("Fixture must explicitly select its evaluation boundary policy")
    messages = packet["config"].get("replay_messages")
    if not messages or any(
        message.get("origin") != "FIXTURE_AUTHORED_REFERENCE"
        or message.get("historical") is not False
        or message.get("kind") not in {"NEW_ITEM", "ITEM_ADDED"}
        or not message.get("replay_at")
        or message.get("replay_source_session_id")
        for message in messages
    ):
        raise ValueError("First fixture messages must be explicitly authored nonhistorical item messages")
    if messages[0]["kind"] != "NEW_ITEM":
        raise ValueError("First fixture must start with authored NEW_ITEM")
    if phases:
        indexes = [index for phase in phases for index in phase["message_indexes"]]
        if any(type(index) is not int for index in indexes) or sorted(indexes) != list(range(len(messages))):
            raise ValueError("Every phased message must be assigned exactly once")
        for phase in phases:
            if not phase["message_indexes"] or any(
                parse_time(messages[index]["replay_at"]) != parse_time(phase["at"])
                for index in phase["message_indexes"]
            ):
                raise ValueError("Phased message must match its declared coordinate")
        if (
            phases[0]["message_indexes"] != [0]
            or phases[1]["message_indexes"] != [1]
            or len(messages) != 2
            or messages[1]["kind"] != "ITEM_ADDED"
            or messages[0]["item_id"] == messages[1]["item_id"]
        ):
            raise ValueError("F24 requires an initial source then a distinct later confirmation item")
    mask_proof = (
        _host_identity_mask(packet, private_mask_path, input_path, raw_sources=raw_sources)
        if packet.get("masking", {}).get("applied")
        and (
            reference["fixture_id"] in {"F-24", "F-25", "F-26", "F-27"}
            or any(
                row.get("as_of_evidence", {}).get("derivation", {}).get("kind") == "SNAPSHOT_SIGNED_IPFS_V1"
                for row in packet["records"]
                if row["collection"] == "ingest.source_items"
            )
        )
        else None
    )
    sources = {}
    for row in packet["records"]:
        if row["collection"] == "ingest.source_items":
            record = row["record"]
            _reject_analysis_seed(record)
            if not record.get("observed_at"):
                raise ValueError("Fixture source requires its actual observed_at collection clock")
            if parse_time(row["available_at"]) < parse_time(record["observed_at"]):
                raise ValueError("Fixture actual availability cannot precede actual source observation")
            if any(
                record.get(key)
                for key in (
                    "event_id",
                    "session_id",
                    "route_event_id",
                    "route",
                    "routing_scores",
                    "routing_message",
                    "body_recovery",
                    "source_metadata_repair",
                )
            ):
                raise ValueError("First fixture cannot seed analysis or later repair state")
            if record.get("extraction_status", "PENDING") != "PENDING":
                raise ValueError("First fixture source cannot seed an extraction outcome")
            if (record.get("relevance") or {}).get("decision") not in (None, "ALWAYS"):
                raise ValueError("First fixture source cannot seed classifier outcomes")
            proof = row.get("as_of_evidence") or {}
            if proof.get("kind") not in {
                "IMMUTABLE_ORIGINAL_PUBLICATION",
                "VERSIONED_AS_OF_SNAPSHOT",
                "DECLARED_SYNTHETIC_SOURCE",
                "RETROSPECTIVE_BEHAVIOR_REFERENCE",
            }:
                raise ValueError("Fixture source requires immutable or versioned as-of body evidence")
            if not all(
                isinstance(proof.get(field), str) and proof[field] for field in ("version_id", "source_url")
            ):
                raise ValueError("Fixture source requires original publication version and URL")
            if proof["kind"] == "DECLARED_SYNTHETIC_SOURCE":
                case = _synthetic_manifest(reference)
                if not proof.get("source_url", "").startswith("fixture://"):
                    raise ValueError("Declared synthetic source must not claim an acquired real account URL")
                if (proof.get("derivation") or {}).get("kind") != "DECLARED_UTF8_SOURCE_V1":
                    raise ValueError("Synthetic source requires exact authored UTF8 byte derivation")
                leads = (
                    [mask_proof["transform"](lead) for lead in case.get("source_leads", [])]
                    if mask_proof
                    else case.get("source_leads", [])
                )
                if any(quote not in record["text"] for quote in leads):
                    raise ValueError("Synthetic source omits the authoritative declared lead quote")
            if proof["kind"] == "VERSIONED_AS_OF_SNAPSHOT":
                _, snapshot_upper = interval_bounds(propagate_timestamp(proof, "snapshot_as_of"))
                if snapshot_upper is None or snapshot_upper > cutoff:
                    raise ValueError("Versioned fixture snapshot must prove its as-of occurrence bounds")
            pair = proof.get("raw_ref"), proof.get("raw_sha256")
            declared = [
                (record.get(a), record.get(b))
                for a, b in (
                    ("raw_ref", "raw_sha256"),
                    ("detail_raw_ref", "detail_raw_sha256"),
                    ("discovery_raw_ref", "discovery_raw_sha256"),
                )
            ]
            if not all(pair) or pair not in declared:
                raise ValueError("Fixture as-of raw evidence must be declared by its source record")
            if sha256(retained_source_payload(pair[0], raw_sources=raw_sources)) != pair[1]:
                raise ValueError("Fixture retained original raw evidence checksum mismatch")
            if not isinstance(record.get("text"), str) or not record["text"]:
                raise ValueError("Fixture requires original source body, not a semantic event seed")
            if sha256(record["text"].encode()) != proof.get("body_sha256"):
                raise ValueError("Fixture original body checksum mismatch")
            raw_view = retained_source_payload(pair[0], raw_sources=raw_sources)
            projection = proof.get("projection_provenance")
            if projection:
                original = retained_source_payload(projection["original_raw_ref"], raw_sources=raw_sources)
                if sha256(original) != projection["original_raw_sha256"]:
                    raise ValueError("Fixture host-only original projection checksum mismatch")
                projection_code.verify(projection["projection_module_sha256"])
                descriptor = proof.get("derivation") or {}
                if descriptor.get("kind") != "SOURCE_ORIGINAL_FIELD_PROJECTION_V1":
                    raise ValueError("Fixture projection requires a declared original-field selector")
                if (
                    project_original_source(descriptor["source_kind"], original, descriptor["selector"])
                    != raw_view
                ):
                    raise ValueError(
                        "Fixture selected source bytes differ from host-only original projection"
                    )
            elif (proof.get("derivation") or {}).get("kind") == "SOURCE_ORIGINAL_FIELD_PROJECTION_V1":
                raise ValueError("Fixture selected source requires host-only original projection proof")
            verify_derivation(
                record,
                proof,
                raw_view,
                signed_original=mask_proof["signed_originals"].get(row["key"]) if mask_proof else None,
            )
            public_lower, public_upper = interval_bounds(propagate_timestamp(record, "published_at"))
            if public_lower is None or public_upper is None:
                raise ValueError("Fixture original publication requires explicit occurrence bounds")
            simulated = parse_time(row["simulated_available_at"])
            retrospective_schedule = (
                proof["kind"] == "RETROSPECTIVE_BEHAVIOR_REFERENCE"
                and row.get("simulated_availability_basis") == "FIXTURE_CASE_REFERENCE_SCHEDULE"
                and reference["fixture_id"] in {"F-08", "F-23"}
                and propagate_timestamp(record, "published_at").precision == "BUCKET(1d)"
                and public_lower <= simulated <= public_upper
            )
            if (simulated < public_upper and not retrospective_schedule) or simulated > cutoff:
                raise ValueError(
                    "Fixture source schedule must follow the whole publication precision interval"
                )
            if (
                row.get("simulated_availability_basis") != "CONSERVATIVE_PUBLICATION_UPPER_BOUND"
                and not retrospective_schedule
            ):
                raise ValueError(
                    "Fixture schedule must explicitly distinguish simulated from actual availability"
                )
            _verify_receipts(record, assembled)
            if parse_time(row["available_at"]) > assembled:
                raise ValueError("Fixture actual source availability exceeds assembled_at")
            if record["item_id"] in sources:
                raise ValueError(
                    "First fixture must identify each immutable source version with a unique item ID"
                )
            sources[record["item_id"]] = row
        elif row["collection"] == "mkt.unlocks":
            record = row["record"]
            _reject_analysis_seed(record)
            observed = record.get("observed_at")
            if not observed or parse_time(row["available_at"]) < parse_time(observed):
                raise ValueError("Unlock calendar availability cannot precede its actual observation")
            if row.get("simulated_available_at") is not None:
                raise ValueError("Unlock calendars require actual availability, not simulated publication")
            _verify_receipts(record, assembled)
        elif row["record"].get("artifact_kind") == "REFERENCE_MISSING_MARKET_INPUTS":
            validate_missing_market(row, packet, reference)
        elif not market_record(row):
            raise ValueError("First fixture cannot seed event, verdict, session, call or delivery state")
    for message in messages:
        source = sources.get(message.get("item_id"))
        if source is None or parse_time(message["replay_at"]) < parse_time(source["simulated_available_at"]):
            raise ValueError("Authored fixture message precedes its source availability")
        if parse_time(message["replay_at"]) > cutoff:
            raise ValueError("Authored fixture message exceeds evaluation boundary")
    if phases:
        later = sources[messages[1]["item_id"]]
        if (
            later["as_of_evidence"]["kind"] != "DECLARED_SYNTHETIC_SOURCE"
            or later["record"].get("source_id") != "S13"
            or parse_time(later["simulated_available_at"]) != parse_time(phases[1]["at"])
        ):
            raise ValueError("Later F24 phase requires its separately declared synthetic S13 source")
    if phases:
        for capture in packet["tool_results"]:
            if (
                capture.get("declared_response_kind")
                in {"EMPTY_OFFICIAL_FEED_LOOKUP", "EMPTY_OFFICIAL_SEARCH_LOOKUP"}
                and capture.get("valid_until") != phases[1]["at"]
            ):
                raise ValueError("Initial empty F24 lookup must expire at the confirmation phase")
        for index, left in enumerate(packet["tool_results"]):
            for right in packet["tool_results"][index + 1 :]:
                if (left.get("tool"), left.get("arguments")) == (right.get("tool"), right.get("arguments")):
                    first, second = sorted(
                        (left, right), key=lambda row: parse_time(row["simulated_available_at"])
                    )
                    if first.get("valid_until") is None or parse_time(first["valid_until"]) > parse_time(
                        second["simulated_available_at"]
                    ):
                        raise ValueError(
                            "Repeated phased capture arguments require nonoverlapping validity intervals"
                        )
    for capture in packet["tool_results"]:
        if (
            phases
            and capture.get("valid_until") is not None
            and (
                parse_time(capture["valid_until"]) <= parse_time(capture["simulated_available_at"])
                or parse_time(capture["valid_until"]) > cutoff
            )
        ):
            raise ValueError("Fixture capture validity must be a bounded half-open interval")
        if capture.get("origin") == "DECLARED_SYNTHETIC_TOOL_RESPONSE":
            _synthetic_manifest(reference)
            lookup = {
                "EMPTY_OFFICIAL_FEED_LOOKUP": ("fetch_page", "url"),
                "EMPTY_OFFICIAL_SEARCH_LOOKUP": ("web_search", "query"),
            }.get(capture.get("declared_response_kind"))
            arguments = capture.get("arguments") or {}
            if (
                reference["fixture_id"] != "F-24"
                or lookup is None
                or capture.get("tool") != lookup[0]
                or not isinstance(arguments, dict)
                or set(arguments) != {lookup[1]}
                or not isinstance(arguments[lookup[1]], str)
                or not arguments[lookup[1]].strip()
                or capture.get("result") != {"items": []}
                or capture.get("status") != "DONE"
                or capture.get("is_error") is not False
                or capture.get("http_acquisition_claim") is not False
            ):
                raise ValueError("Synthetic official lookup must be an exact declared empty response")
            authored = parse_time(capture["actual_authored_at"])
            if authored > assembled or parse_time(capture["available_at"]) < authored:
                raise ValueError("Synthetic response requires actual authoring/availability receipts")
            if (
                parse_time(capture["available_at"]) > assembled
                or parse_time(capture["simulated_available_at"]) > cutoff
            ):
                raise ValueError("Synthetic response exceeds authored assembly or chosen phase boundary")
            continue
        if capture.get("origin") != "FIXTURE_ACQUIRED_REFERENCE" or capture.get("tool") != "fetch_page":
            raise ValueError("First fixture external capture must be an explicitly acquired immutable page")
        source = sources.get(capture.get("source_item_id"))
        if source is None or capture.get("arguments") != {"url": source["as_of_evidence"]["source_url"]}:
            raise ValueError("Fixture external capture must refer to its exact acquired source URL")
        if capture.get("status") == "DONE":
            if canonical_json(capture.get("result")) != canonical_json(source["record"]):
                raise ValueError("Fixture page capture must preserve the acquired source record exactly")
        elif capture.get("status") not in {"ERROR", "REFUSED"} or not capture.get("is_error"):
            raise ValueError("Fixture external capture outcome metadata is incomplete")
        elif (
            set(capture.get("result") or {}) != {"ok", "error"}
            or capture["result"]["ok"] is not False
            or not isinstance(capture["result"]["error"], dict)
            or set(capture["result"]["error"]) - {"type", "reason", "category"}
        ):
            raise ValueError("Fixture external failure cannot seed semantic result state")
        if (
            not parse_time(source["simulated_available_at"])
            <= parse_time(capture["simulated_available_at"])
            <= cutoff
        ):
            raise ValueError("Fixture external capture precedes its source or exceeds cutoff")
        if parse_time(capture["available_at"]) > assembled:
            raise ValueError("Fixture capture actual availability exceeds assembled_at")
    return reference


MISSING_MARKET_FIELDS = (
    "flow_bars",
    "flow_segments",
    "reference_points",
    "books",
    "oi",
    "funding",
    "klines",
    "liquidations",
    "tickers",
)


def validate_missing_market(row, packet, reference):
    """A reference-only absence declaration, never an observed zero or complete capture."""
    raw = files("tank").joinpath("resources/fixture-input-authority-v1.json").read_bytes()
    authority = json.loads(raw)
    if (
        reference.get("authoritative_input_sha256") != sha256(raw)
        or reference.get("authoritative_manifest_sha256") != authority["original_manifest_sha256"]
        or reference["fixture_id"] not in {case["fixture_id"] for case in authority["cases"]}
    ):
        raise ValueError("Missing market declaration requires exact input authority provenance")
    record = row["record"]
    allowed = {
        "artifact_kind",
        "token_id",
        "cutoff",
        "source_capture_claim",
        "actual_declaration_at",
        "actual_declaration_at_prec",
        "actual_declaration_at_prec_reason",
        "missing_inputs",
        *MISSING_MARKET_FIELDS,
    }
    if (
        row["collection"] != "replay.market_snapshots"
        or set(record) - allowed
        or record.get("source_capture_claim") is not False
        or any(record.get(field) != [] for field in MISSING_MARKET_FIELDS)
        or not record.get("missing_inputs")
        or not all(isinstance(reason, str) and reason for reason in record["missing_inputs"])
        or record.get("actual_declaration_at_prec") != "COLLECTION_ONLY"
        or parse_time(record["cutoff"]) != parse_time(packet["cutoff"])
        or parse_time(row["simulated_available_at"]) != parse_time(record["cutoff"])
    ):
        raise ValueError(
            "Missing market declaration cannot contain observations, computed truth or semantic seeds"
        )
    declared = parse_time(record["actual_declaration_at"])
    if not declared <= parse_time(row["available_at"]) <= parse_time(packet["assembled_at"]):
        raise ValueError("Missing market declaration requires actual collection and assembly clocks")
    proof = row["missing_market_provenance"]
    inventory_raw = Path(proof["inventory_ref"]).read_bytes()
    if sha256(inventory_raw) != proof["inventory_sha256"]:
        raise ValueError("Missing market retained inventory checksum mismatch")
    inventory = json.loads(inventory_raw)
    if (
        inventory.get("token_id") != record["token_id"]
        or inventory.get("retained_observation_count") != 0
        or inventory.get("missing_inputs") != record["missing_inputs"]
        or parse_time(inventory["actual_checked_at"]) > declared
    ):
        raise ValueError("Missing market declaration differs from its retained inventory")
    instrument_ids = {
        entry["instrument_id"]
        for entry in packet["registry"].get("instruments", [])
        if entry.get("token_id") == record["token_id"]
    }
    for other in packet["records"]:
        if other is row or not (
            other["collection"].startswith("mkt.") or other["collection"] == "replay.market_snapshots"
        ):
            continue
        if (
            other["record"].get("token_id") == record["token_id"]
            or other["record"].get("instrument_id") in instrument_ids
        ):
            raise ValueError("Missing market declaration cannot hide retained market observations")


def _synthetic_manifest(reference):
    raw = files("tank").joinpath("resources/fixture-input-authority-v1.json").read_bytes()
    authority = json.loads(raw)
    if reference.get("authoritative_input_sha256") != sha256(raw):
        raise ValueError("Synthetic fixture requires the exact packaged input authority hash")
    if reference.get("authoritative_manifest_sha256") != authority["original_manifest_sha256"]:
        raise ValueError("Synthetic fixture requires original authoritative definition provenance")
    matches = [case for case in authority["cases"] if case["fixture_id"] == reference["fixture_id"]]
    if len(matches) != 1 or matches[0].get("synthetic") is not True:
        raise ValueError("Source is not an authoritative declared synthetic fixture")
    if reference.get("cutoff_spec") != matches[0]["cutoff_spec"]:
        raise ValueError("Synthetic cutoff literal differs from its authoritative fixture")
    return matches[0]


def project_original_source(kind, original, selector):
    """Select only preregistered authored fields; full transport stays host-only."""
    from tank.sources import parsers

    if kind == "WEB_DATED_CORE_FACTS_V1":
        if not isinstance(selector, dict) or set(selector) != {"start_literal", "end_literal"}:
            raise ValueError("Dated source projection requires exact literal excerpt boundaries")
        text = parsers.text(original)
        start, end = selector["start_literal"], selector["end_literal"]
        if text.count(start) != 1 or text.count(end) != 1:
            raise ValueError("Dated source excerpt boundaries are not unique")
        a, b = text.index(start), text.index(end) + len(end)
        if a >= b:
            raise ValueError("Dated source excerpt order is invalid")
        result = {"text": text[a:b]}
    else:
        obj = json.loads(original)
        if kind == "FEDERAL_REGISTER_JSON_ITEM_V1":
            if not isinstance(selector, dict) or set(selector) != {"document_number"}:
                raise ValueError("Federal Register projection requires its document number")
            matches = [r for r in obj["results"] if r["document_number"] == selector["document_number"]]
            if len(matches) != 1:
                raise ValueError("Federal Register document selection must be unique")
            fields = (
                "document_number",
                "title",
                "type",
                "abstract",
                "excerpts",
                "html_url",
                "pdf_url",
                "publication_date",
                "agencies",
            )
            result = {"results": [{k: matches[0][k] for k in fields if k in matches[0]}]}
        elif kind == "DISCOURSE_FIRST_POST_V1" and selector == "/post_stream/posts/0":
            post = obj["post_stream"]["posts"][0]
            fields = ("cooked", "created_at", "updated_at", "version", "last_edited_at", "username")
            result = {"post_stream": {"posts": [{k: post[k] for k in fields if k in post}]}}
        elif kind == "DISCOURSE_TOPIC_POST_V1":
            if (
                not isinstance(selector, dict)
                or set(selector) != {"post_number"}
                or type(selector["post_number"]) is not int
            ):
                raise ValueError("Discourse topic post projection requires an explicit integer post_number")
            matches = [
                p for p in obj["post_stream"]["posts"] if p.get("post_number") == selector["post_number"]
            ]
            if len(matches) != 1:
                raise ValueError("Discourse topic post selection must be unique")
            fields = (
                "id",
                "post_number",
                "cooked",
                "created_at",
                "updated_at",
                "version",
                "last_edited_at",
                "username",
            )
            result = {
                "topic_id": obj.get("id"),
                "post_stream": {"posts": [{k: matches[0][k] for k in fields if k in matches[0]}]},
            }
        elif kind == "BINANCE_CMS_ORIGINAL_V1" and selector == "/data":
            fields = ("code", "title", "body", "publishDate", "version", "lastUpdateTime")
            result = {"data": {k: obj["data"][k] for k in fields if k in obj["data"]}}
        elif kind == "BYBIT_LIST_SELECTED_V1" and re.fullmatch(r"/result/list/[0-9]+", str(selector)):
            article = obj["result"]["list"][int(selector.rsplit("/", 1)[1])]
            fields = (
                "title",
                "description",
                "url",
                "dateTimestamp",
                "startDateTimestamp",
                "endDateTimestamp",
                "publishTime",
            )
            result = {"result": {"list": [{k: article[k] for k in fields if k in article}]}}
        else:
            raise ValueError("Unsupported original source projection selector")
    return canonical_json(result).encode()


def verify_derivation(record, proof, raw, *, signed_original=None):
    """Reproduce declared source-body selectors; historical publication review remains separate."""
    if signed_original is not None:
        original_record, original_proof, original_raw = signed_original
        verify_derivation(original_record, original_proof, original_raw)
        if (
            proof.get("version_id") != original_proof.get("version_id")
            or proof.get("derivation", {}).get("kind") != "SNAPSHOT_SIGNED_IPFS_V1"
        ):
            raise ValueError("Signed identity view changed original CID or source kind")
    descriptor = proof.get("derivation")
    if descriptor is None:
        return
    from tank.sources import parsers

    kind = descriptor.get("kind")
    if kind == "SOURCE_ORIGINAL_FIELD_PROJECTION_V1":
        kind = descriptor["source_kind"]
    module_hash = descriptor.get("module_sha256")
    if module_hash and module_hash != sha256(Path(parsers.__file__).read_bytes()):
        raise ValueError("Fixture source derivation parser bytes differ from the preregistered version")
    if kind == "DECLARED_UTF8_SOURCE_V1":
        derived = raw.decode("utf-8")
    elif kind == "GOVINFO_HTML_TANK_TEXT_V1":
        if not module_hash:
            raise ValueError("GovInfo text derivation requires its exact parser module hash")
        derived = parsers.text(raw)
    else:
        try:
            payload = json.loads(raw)
            if kind == "FEDERAL_REGISTER_JSON_ITEM_V1":
                if not module_hash or record.get("source_id") != "S03":
                    raise ValueError("Federal Register derivation requires S03 and the exact parser version")
                matches = [
                    r for r in parsers.json_items(raw, "S03") if r["external_id"] == record["external_id"]
                ]
                if len(matches) != 1:
                    raise ValueError("Federal Register derivation requires one matching document")
                item = matches[0]
                if any(
                    item.get(k) != record.get(k)
                    for k in ("title", "url", "document_type", "pdf_url", "agencies")
                ):
                    raise ValueError("Federal Register source metadata differs from the retained API item")
                if interval_bounds(propagate_timestamp(item, "published_at")) != interval_bounds(
                    propagate_timestamp(record, "published_at")
                ):
                    raise ValueError("Federal Register publication date or precision differs")
                derived = item["text"]
            elif kind == "WEB_DATED_CORE_FACTS_V1":
                derived = payload["text"]
            elif kind == "BYBIT_LIST_SELECTED_V1":
                article = payload["result"]["list"][0]
                published = article.get("publishTime", article.get("dateTimestamp"))
                if published is None:
                    raise ValueError("Bybit selected announcement lacks original publication clock")
                published = (
                    datetime.fromtimestamp(published / 1000, UTC)
                    if isinstance(published, (int, float))
                    else parse_time(published)
                )
                lo, hi = interval_bounds(propagate_timestamp(record, "published_at"))
                if lo is None or hi is None or not (lo <= published <= hi):
                    raise ValueError("Bybit selected original publication clock mismatch")
                derived = article["title"] + "\n" + article["description"]
            elif kind == "SNAPSHOT_SIGNED_IPFS_V1":
                cid = proof["version_id"]
                if not cid.startswith("b"):
                    raise ValueError("Snapshot original requires raw SHA256 CIDv1")
                decoded = base64.b32decode(cid[1:].upper() + "=" * ((-len(cid[1:])) % 8))
                if decoded[:4] != bytes.fromhex("01551220") or decoded[4:].hex() != sha256(
                    signed_original[2] if signed_original is not None else raw
                ):
                    raise ValueError("Snapshot signed source CID does not match retained bytes")
                message = payload["data"]["message"]
                if payload["hash"] != proof["proposal_id"]:
                    raise ValueError("Snapshot signed proposal identity mismatch")
                if datetime.fromtimestamp(message["timestamp"], UTC) != parse_time(record["published_at"]):
                    raise ValueError("Snapshot signed publication timestamp mismatch")
                derived = message["body"]
            elif kind == "SNAPSHOT_ORIGINAL_PROPOSAL_V1":
                proposal = payload["data"]["proposal"]
                if proposal["ipfs"] != proof["version_id"]:
                    raise ValueError("Snapshot original content version mismatch")
                derived = proposal["body"]
            elif kind == "BINANCE_CMS_ORIGINAL_V1":
                article = payload["data"]
                if str(article["version"]) != "1" or article["lastUpdateTime"] != 0:
                    raise ValueError("Binance original publication version is unproven")
                published = datetime.fromtimestamp(article["publishDate"] / 1000, UTC)
                if (
                    published != parse_time(record["published_at"])
                    or propagate_timestamp(record, "published_at").precision != "EXACT"
                ):
                    raise ValueError("Binance original publication clock mismatch")
                derived = parsers.binance_body(article["body"])
            elif kind == "DISCOURSE_TOPIC_POST_V1":
                posts = payload["post_stream"]["posts"]
                if len(posts) != 1:
                    raise ValueError("Discourse topic post view must carry exactly one post")
                post = posts[0]
                if post.get("post_number") != (descriptor.get("selector") or {}).get("post_number"):
                    raise ValueError("Discourse topic post number differs from the declared selector")
                if (
                    post["version"] != 1
                    or post["created_at"] != post["updated_at"]
                    or post.get("last_edited_at") is not None
                ):
                    raise ValueError("Discourse topic post version is unproven")
                if (
                    parse_time(post["created_at"]) != parse_time(record["published_at"])
                    or propagate_timestamp(record, "published_at").precision != "EXACT"
                ):
                    raise ValueError("Discourse topic post publication clock mismatch")
                derived = parsers.text(post["cooked"])
            elif kind == "DISCOURSE_FIRST_POST_V1":
                post = payload["post_stream"]["posts"][0]
                if (
                    post["version"] != 1
                    or post["created_at"] != post["updated_at"]
                    or post.get("last_edited_at") is not None
                ):
                    raise ValueError("Discourse original first-post version is unproven")
                if (
                    parse_time(post["created_at"]) != parse_time(record["published_at"])
                    or propagate_timestamp(record, "published_at").precision != "EXACT"
                ):
                    raise ValueError("Discourse original publication clock mismatch")
                derived = parsers.text(post["cooked"])
            else:
                raise ValueError("Unsupported fixture source derivation")
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("Fixture source derivation lacks declared original provider fields") from exc
    if derived != record["text"]:
        raise ValueError("Fixture body does not derive from the declared retained source bytes")


def _reject_analysis_seed(value):
    forbidden = {
        "event_id",
        "session_id",
        "route_event_id",
        "routing_message",
        "routing_scores",
        "ems_id",
        "mta_id",
        "dec_id",
        "alert_id",
        "claim_id",
        "expected_action",
        "expected_grade",
        "expected_outputs",
        "ground_truth",
        "labels",
        "outcomes",
        "tank_is_event",
        "same_event_group",
        "corrected_is_repeat",
        "relation_scorable",
        "filter_score",
        "beneficiary_map",
        "claims",
        "decisions",
        "alerts",
    }
    if isinstance(value, dict):
        if forbidden.intersection(value):
            raise ValueError("First fixture source cannot contain nested analysis/evaluator seed fields")
        for entry in value.values():
            _reject_analysis_seed(entry)
    elif isinstance(value, list):
        for entry in value:
            _reject_analysis_seed(entry)


def _verify_receipts(value, assembled):
    if isinstance(value, dict):
        for key, entry in value.items():
            if (
                key
                in {
                    "observed_at",
                    "acquired_at",
                    "fetched_at",
                    "received_at",
                    "received_ts",
                    "collected_at",
                    "available_at",
                }
                and entry is not None
            ):
                if parse_time(entry) > assembled:
                    raise ValueError("Fixture actual source receipt exceeds assembled_at")
            elif isinstance(entry, (dict, list)):
                _verify_receipts(entry, assembled)
    elif isinstance(value, list):
        for entry in value:
            _verify_receipts(entry, assembled)


def availability(row, reference):
    return (
        row["simulated_available_at"]
        if reference and row.get("simulated_available_at")
        else row["available_at"]
    )


class TrustedFixtureClock:
    """Immutable harness context; never constructed from model tool arguments."""

    __slots__ = ("_entries", "_fixture_id", "_input_sha256", "_sealed")

    def __init__(self, inputs):
        if not inputs.fixture_reference or not inputs.input_sha256:
            raise ValueError("Trusted fixture clock requires validated preregistered reference inputs")
        object.__setattr__(self, "_fixture_id", inputs.fixture_reference["fixture_id"])
        object.__setattr__(self, "_input_sha256", inputs.input_sha256)
        entries = []
        for row in inputs._records:
            if row["collection"] != "ingest.source_items":
                continue
            record, proof = row["record"], row["as_of_evidence"]
            entries.append(
                (
                    record["item_id"],
                    proof["body_sha256"],
                    proof["raw_sha256"],
                    row["simulated_available_at"],
                    propagate_timestamp(record, "observed_at"),
                    proof["kind"],
                    proof["version_id"],
                    proof["source_url"],
                    tuple(
                        (field, propagate_timestamp(record, field))
                        for field in (
                            "observed_at",
                            "acquired_at",
                            "received_at",
                            "received_ts",
                            "fetched_at",
                            "collected_at",
                        )
                        if record.get(field) is not None
                    ),
                    propagate_timestamp(row, "available_at"),
                    record.get("source_id"),
                    record.get("external_id"),
                    propagate_timestamp(record, "published_at"),
                )
            )
        object.__setattr__(self, "_entries", tuple(entries))
        object.__setattr__(self, "_sealed", True)

    def __setattr__(self, name, value):
        raise AttributeError("Trusted fixture clock is immutable")

    @property
    def input_sha256(self):
        return self._input_sha256

    @property
    def item_ids(self):
        """Only sealed reference sources, including ones not yet admitted."""
        return tuple(dict.fromkeys(entry[0] for entry in self._entries))

    def _entry(self, item):
        matches = [entry for entry in self._entries if entry[0] == item.get("item_id")]
        if len(matches) != 1:
            raise ValueError("Source item has no unique sealed fixture clock binding")
        entry = matches[0]
        hashes = {item.get(field) for field in ("raw_sha256", "detail_raw_sha256", "discovery_raw_sha256")}
        if (
            sha256(str(item.get("text", "")).encode()) != entry[1]
            or entry[2] not in hashes
            or item.get("source_id") != entry[10]
            or item.get("external_id") != entry[11]
            or propagate_timestamp(item, "observed_at") != entry[4]
            or propagate_timestamp(item, "published_at") != entry[12]
        ):
            raise ValueError("Source item body/raw/clock identity differs from sealed fixture clock binding")
        return entry

    def known_at(self, item):
        from tank.time_precision import chosen_boundary

        return chosen_boundary(self._entry(item)[3], precision="EXACT")

    def provenance(self, item):
        entry = self._entry(item)
        return {
            "clock_domain": "FIXTURE_SIMULATED_AS_OF",
            "fixture_id": self._fixture_id,
            "preregistered_input_sha256": self._input_sha256,
            **entry[4].fields("actual_observed_at"),
            **self.known_at(item).fields("simulated_known_to_us_at"),
            "source_body_sha256": entry[1],
            "source_raw_sha256": entry[2],
            "source_binding_verification": "PREREGISTERED_BUILDER_AND_INDEPENDENT_SOURCE_REVIEW",
            "evidence_kind": entry[5],
            "source_version_id": entry[6],
            "source_url": entry[7],
            "actual_source_clocks": {field: timestamp.fields(field) for field, timestamp in entry[8]},
            **entry[9].fields("actual_available_at"),
            "historical_live_performance_claim": False,
        }


def _host_identity_mask(
    packet, private_path, input_path, *, raw_sources: RetainedSourceReader = FILESYSTEM_RETAINED_SOURCES
):
    """Validate a private identity derivative; never export the map or originals."""
    if not (packet.get("masking") or {}).get("applied"):
        return None
    if private_path is None:
        raise ValueError("Masked source fixture requires a host-only identity map")
    input_root = Path(input_path).resolve().parent
    private_path = Path(private_path).resolve(strict=True)
    if private_path.is_relative_to(input_root):
        raise ValueError("Private identity map must remain outside replay inputs")
    private = json.loads(private_path.read_text())
    if sha256(canonical_json(private).encode()) != packet["masking"].get("map_sha256"):
        raise ValueError("Private identity map checksum mismatch")
    paths = [
        Path(private[k]).resolve(strict=True) for k in ("original_packet_path", "original_registry_path")
    ]
    if any(p.is_relative_to(input_root) for p in paths):
        raise ValueError("Unmasked authority bindings must remain outside replay inputs")
    original_path, registry_path = paths
    if original_path.suffix == ".zst":
        import zstandard

        with zstandard.ZstdDecompressor().stream_reader(original_path.open("rb")) as reader:
            original = json.load(reader)
    else:
        original = json.loads(original_path.read_text())
    if sha256(canonical_json(original).encode()) != private.get("source_sha256"):
        raise ValueError("Original packet checksum mismatch")
    if registry_path.suffix in {".yaml", ".yml"}:
        import yaml

        registry = yaml.safe_load(registry_path.read_text())
    else:
        registry = json.loads(registry_path.read_text())
    if original.get("registry") != registry:
        raise ValueError("Original registry differs from its canonical host snapshot")
    if original.get("masking", {}).get("applied"):
        raise ValueError("Original authority packet must be unmasked")

    for field in ("fixture_reference", "cutoff", "assembled_at", "replay_mode"):
        if packet.get(field) != original.get(field):
            raise ValueError("Masking changed authority or original temporal coordinates")
    allowed = set()
    identity_fields = {
        "name",
        "names",
        "aliases",
        "symbol",
        "entity_id",
        "token_id",
        "handle",
        "official_accounts",
        "venue_symbol",
        "instrument_id",
    }

    def identities(value):
        if isinstance(value, dict):
            for key, entry in value.items():
                if key in identity_fields:
                    values = entry if isinstance(entry, list) else [entry]
                    allowed.update(x for x in values if isinstance(x, str) and x)
                identities(entry)
        elif isinstance(value, list):
            for entry in value:
                identities(entry)

    identities(registry)
    token = private.get("source_token_identity")
    if token is not None:
        if set(token) != {
            "item_id",
            "body_sha256",
            "symbol",
            "chain",
            "contract_address",
        }:
            raise ValueError("Unsupported source token identity binding")
        if any(not isinstance(token.get(k), str) or not token[k] for k in token):
            raise ValueError("Source token identity fields must be explicit strings")
        matches = [
            row
            for row in original.get("records", [])
            if row.get("collection") == "ingest.source_items"
            and row["record"].get("item_id") == token["item_id"]
        ]
        if (
            len(matches) != 1
            or token["chain"] != "Ethereum"
            or matches[0].get("as_of_evidence", {}).get("derivation", {}).get("kind")
            != "SNAPSHOT_SIGNED_IPFS_V1"
        ):
            raise ValueError("Unknown signed source token identity")
        record = matches[0]["record"]
        address = token["contract_address"]
        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", address):
            raise ValueError("Invalid source token contract")
        declaration = f"Token Address: [{address}](https://etherscan.io/address/{address})"
        if (
            sha256(record["text"].encode()) != token["body_sha256"]
            or declaration not in record["text"]
            or not re.search(r"(?<!\w)" + re.escape(token["symbol"]) + r"(?!\w)", record.get("title", ""))
        ):
            raise ValueError("Source token identity lacks its original structured address field")
        allowed.add(token["symbol"])
    replacements = private.get("replacements")
    if (
        not isinstance(replacements, dict)
        or (
            not replacements
            and not (
                packet["fixture_reference"]["fixture_id"] == "F-25"
                and private.get("already_anonymous") is True
                and packet["masking"].get("already_anonymous") is True
            )
        )
        or any(
            not isinstance(k, str)
            or not isinstance(v, str)
            or not k
            or not v
            or k.casefold() not in {x.casefold() for x in allowed}
            for k, v in replacements.items()
        )
    ):
        raise ValueError("Masking may substitute only canonical registry identities")
    keys, values = list(replacements), list(replacements.values())
    sensitive = private.get("case_sensitive_names", [])
    if not isinstance(sensitive, list) or any(k not in replacements for k in sensitive):
        raise ValueError("Invalid case-sensitive identity declaration")

    groups: dict[str, list[str]] = {}
    for key in keys:
        groups.setdefault(key.casefold(), []).append(key)
    if any(len(group) > 1 and any(k not in sensitive for k in group) for group in groups.values()) or len(
        {x.casefold() for x in values}
    ) != len(values):
        raise ValueError("Identity map is not one-to-one")
    if {x.casefold() for x in keys} & {x.casefold() for x in values}:
        raise ValueError("Masked aliases overlap original identities")
    if any(not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.:/-]{0,95}", v) for v in values):
        raise ValueError("Invalid opaque identity alias")

    def transform(text, inverse=False):
        if not replacements:
            return text
        mapping = {v: k for k, v in replacements.items()} if inverse else replacements
        exact = {replacements[k] if inverse else k for k in sensitive}
        parts = sorted(mapping, key=len, reverse=True)
        branches = [re.escape(k) if k in exact else "(?i:" + re.escape(k) + ")" for k in parts]
        pattern = re.compile(r"(?<!\w)(?:" + "|".join(branches) + r")(?!\w)")

        def replace(match):
            key = next(
                k for k in parts if (k == match[0] if k in exact else k.casefold() == match[0].casefold())
            )
            return mapping[key]

        return pattern.sub(replace, text)

    sources = {
        row["key"]: row for row in original.get("records", []) if row["collection"] == "ingest.source_items"
    }
    actual = [row for row in packet.get("records", []) if row["collection"] == "ingest.source_items"]
    if set(sources) != {row["key"] for row in actual}:
        raise ValueError("Masking changed original source identities")
    signed_originals = {}
    mutable = {
        "raw_ref",
        "raw_path",
        "raw_sha256",
        "detail_raw_ref",
        "detail_raw_sha256",
        "discovery_raw_ref",
        "discovery_raw_sha256",
        "content_hash",
    }

    def transform_tree(value, inverse=False):
        if isinstance(value, str):
            return transform(value, inverse)
        if isinstance(value, list):
            return [transform_tree(v, inverse) for v in value]
        if isinstance(value, dict):
            result = {transform(k, inverse): transform_tree(v, inverse) for k, v in value.items()}
            if len(result) != len(value):
                raise ValueError("Identity substitution collides JSON keys")
            return result
        return value

    for row in actual:
        before = sources[row["key"]]
        proof, masked_proof = before.get("as_of_evidence") or {}, row.get("as_of_evidence") or {}
        signed = proof.get("derivation", {}).get("kind") == "SNAPSHOT_SIGNED_IPFS_V1"
        kind = "SNAPSHOT_SIGNED_IPFS_V1" if signed else "DECLARED_UTF8_SOURCE_V1"
        if (
            proof.get("derivation", {}).get("kind") != kind
            or masked_proof.get("derivation", {}).get("kind") != kind
            or not signed
            and packet["fixture_reference"]["fixture_id"] not in {"F-24", "F-25", "F-26", "F-27"}
        ):
            raise ValueError("Unsupported identity derivative source kind")
        original_raw_path = Path(
            (private.get("original_raw_paths") or {}).get(proof.get("raw_sha256"), proof.get("raw_ref", ""))
        ).resolve(strict=True)
        if original_raw_path.is_relative_to(input_root):
            raise ValueError("Unmasked raw binding must remain outside replay inputs")
        raw = retained_source_payload(original_raw_path, raw_sources=raw_sources)
        masked_raw = retained_source_payload(masked_proof["raw_ref"], raw_sources=raw_sources)
        if sha256(raw) != proof.get("raw_sha256") or sha256(masked_raw) != masked_proof.get("raw_sha256"):
            raise ValueError("Identity derivative raw checksum mismatch")
        if signed:
            original_json, masked_json = json.loads(raw), json.loads(masked_raw)
            if (
                transform_tree(original_json) != masked_json
                or transform_tree(masked_json, True) != original_json
            ):
                raise ValueError("Signed JSON identity derivative is not exactly reversible")
        elif (
            transform(raw.decode()).encode() != masked_raw
            or transform(masked_raw.decode(), True).encode() != raw
        ):
            raise ValueError("Synthetic raw identity transformation is not exactly reversible")

        expected = transform_tree(before["record"])
        original_record, record = before["record"], row["record"]
        for field in mutable:
            expected.pop(field, None)
        if expected != {k: v for k, v in record.items() if k not in mutable}:
            raise ValueError("Masked source metadata differs beyond mechanical identities")
        for field in ("text", "title", "author", "quoted"):
            old = original_record.get(field)
            if isinstance(old, str) and transform(record[field], True) != old:
                raise ValueError("Source content identity substitution is not reversible")
        if original_record.get("raw_path") != record.get("raw_path") and not (
            original_record.get("raw_path") == proof.get("raw_ref")
            and record.get("raw_path") == masked_proof.get("raw_ref")
        ):
            raise ValueError("Undeclared source raw path replacement")
        for prefix in ("", "detail_", "discovery_"):
            fields = (prefix + "raw_ref", prefix + "raw_sha256")
            old_pair = tuple(original_record.get(k) for k in fields)
            pair = tuple(record.get(k) for k in fields)
            if old_pair != pair and not (
                old_pair == (proof.get("raw_ref"), proof.get("raw_sha256"))
                and pair == (masked_proof.get("raw_ref"), masked_proof.get("raw_sha256"))
            ):
                raise ValueError("Undeclared source raw-reference replacement")
        if "content_hash" in original_record:
            expected_hash = sha256(
                " ".join(
                    (
                        record.get("title", "") + " " + record["text"] + " " + (record.get("quoted") or "")
                    ).split()
                )
            )
            if all(record.get(k) == original_record.get(k) for k in ("title", "text", "quoted")):
                expected_hash = original_record["content_hash"]
            if record.get("content_hash") != expected_hash:
                raise ValueError("Masked content hash mismatch")
        excluded_proof = {"raw_ref", "raw_sha256", "body_sha256"}
        if {k: v for k, v in masked_proof.items() if k not in excluded_proof} != transform_tree(
            {k: v for k, v in proof.items() if k not in excluded_proof}
        ):
            raise ValueError("Identity masking changed publication provenance")
        excluded_row = {"record", "as_of_evidence"}
        if {k: v for k, v in row.items() if k not in excluded_row} != {
            k: v for k, v in before.items() if k not in excluded_row
        }:
            raise ValueError("Identity masking changed source scheduling or coordinates")
        if signed:
            if token is not None and token["contract_address"] not in masked_raw.decode():
                raise ValueError("Masking changed structured token contract identity")
            from tank.fixture_reference import verify_derivation

            verify_derivation(original_record, proof, raw)
            signed_originals[row["key"]] = (original_record, proof, raw)
        elif raw != original_record["text"].encode():
            raise ValueError("Original synthetic raw differs from original text")
    return {"transform": transform, "signed_originals": signed_originals}


def _masked_synthetic_leads(packet, case, private_path, input_path):
    context = _host_identity_mask(packet, private_path, input_path)
    return (
        [context["transform"](lead) for lead in case.get("source_leads", [])]
        if context
        else case.get("source_leads", [])
    )
