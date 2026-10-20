"""TDD §11 fixture acceptance, independent of the fixture-building implementation.

Read-only: never repairs captures, changes cutoffs, invents outcomes, or runs a
model. Prepared inputs and a completed acceptance run are separate obligations.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

MARKET_CASES = {f"F-{n:02}" for n in range(11, 17)}
MARKET_REQUIRED = {f"F-{n:02}" for n in range(1, 17)} | {"F-24", "F-26"}
OUTCOME_FIELDS = {"sealed_outcomes", "expected_outputs", "expected_verdict", "assertion_spec"}


def specification_cases(root):
    text = (Path(root) / "03_Tank_V0_Technical_Design.md").read_text()
    rows = re.findall(r"^\| (F-\d{2}) \| (.*?) \| (.*?) \| (.*?) \|$", text, re.MULTILINE)
    return {
        name: {"case_description": case, "cutoff_spec": cutoff, "assertion_spec": assertion}
        for name, case, cutoff, assertion in rows
    }


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def read_bytes(path):
    data = path.read_bytes()
    if path.suffix == ".zst":
        import zstandard

        try:
            with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)) as reader:
                data = reader.read()
        except zstandard.ZstdError as error:
            raise ValueError("invalid compressed fixture") from error
    return data


def read_json(path):
    return json.loads(read_bytes(path))


def resolve(root, value):
    if str(value).startswith("/data/"):
        return root / str(value).lstrip("/")
    path = Path(value)
    return path if path.is_absolute() else root / path


def time(value):
    return datetime.fromisoformat(value).astimezone(UTC)


def cutoff_errors(case, packet):
    """Verify the declared interpretation of an imprecise original cutoff.

    A whole-minute reference requires explicit bucket precision and an exclusive
    boundary. It is a behavior reconstruction, not a recovered exact call time.

    A date-only specification names a day, not an invented exact publication
    instant. Its exclusive query bound may be the following midnight only when
    that policy and the original day are both declared. F24 keeps its first
    assertion checkpoint separate from its later confirmation phase.
    """
    literal = case["cutoff_spec"].removesuffix(" UTC")
    reference = packet.get("fixture_reference", {})
    try:
        actual = time(packet["cutoff"])
        if literal in {"post + 1 min", "quote + 1 min"}:
            expected = time(reference["relative_anchor"]) + timedelta(minutes=1)
            initial = time(reference.get("cutoff_instant", packet["cutoff"]))
            phases = reference.get("phases", [])
            if initial != expected:
                return ["initial cutoff differs from the declared post/quote + 1 minute"]
            if actual != initial and not (
                case["fixture_id"] == "F-24"
                and len(phases) == 2
                and time(phases[0]["at"]) == initial
                and time(phases[1]["at"]) == actual > initial
            ):
                return ["later horizon replaced the original assertion cutoff"]
            return []
        try:
            expected = datetime.strptime(literal, "%d %b %Y %H:%M").replace(tzinfo=UTC)
        except ValueError:
            day = datetime.strptime(literal, "%d %b %Y").replace(tzinfo=UTC)
            if not (
                reference.get("cutoff_policy") == "UTC_DAY_END_EXCLUSIVE"
                and reference.get("cutoff_day") == day.date().isoformat()
                and actual == day + timedelta(days=1)
            ):
                return ["date-only cutoff lacks its original day and explicit exclusive boundary"]
            return []
        if reference.get("cutoff_policy") == "UTC_MINUTE_END_EXCLUSIVE":
            expected += timedelta(minutes=1)
            if not (
                reference.get("original_call_precision") == "BUCKET(1m)"
                and reference.get("chosen_boundary_basis") == "WHOLE_NAMED_MINUTE_END_EXCLUSIVE"
                and time(reference["cutoff_instant"]) == expected
                and reference.get("cutoff_spec") == case["cutoff_spec"]
            ):
                return ["minute reference lacks its original literal, precision or exclusive boundary"]
        if actual != expected:
            return [f"cutoff moved: {actual.isoformat()} != {expected.isoformat()}"]
    except (ValueError, KeyError, TypeError, AttributeError):
        return ["missing or invalid original cutoff"]
    return []


def forbidden_fields(value):
    if isinstance(value, dict):
        return [key for key in value if key in OUTCOME_FIELDS] + [
            key for child in value.values() for key in forbidden_fields(child)
        ]
    if isinstance(value, list):
        return [key for child in value for key in forbidden_fields(child)]
    return []


COMPARISONS = {"equals", "not_equals", "in", "not_in", "lte", "gte", "length_equals", "satisfies", "some"}


def valid_assertion(assertion):
    """A small data-only predicate language for literal spec sets and bounds."""
    if not isinstance(assertion, dict):
        return False
    if "when" in assertion and not valid_assertion(assertion["when"]):
        return False
    logical = set(assertion) & {"all", "any"}
    if logical:
        if len(logical) != 1 or set(assertion) - logical - {"when"}:
            return False
        children = assertion[next(iter(logical))]
        return isinstance(children, list) and bool(children) and all(map(valid_assertion, children))
    operators = set(assertion) & COMPARISONS
    path = assertion.get("path")
    if (
        len(operators) != 1
        or set(assertion) - operators - {"path", "when"}
        or not isinstance(path, list)
        or not path
        or any(type(part) not in (str, int) or isinstance(part, int) and part < 0 for part in path)
    ):
        return False
    operator = next(iter(operators))
    expected = assertion[operator]
    if operator in {"satisfies", "some"}:
        return valid_assertion(expected)
    if operator in {"in", "not_in"}:
        return isinstance(expected, list) and bool(expected)
    if operator in {"lte", "gte"}:
        return type(expected) in (int, float)
    if operator == "length_equals":
        return type(expected) is int and expected >= 0
    return True


def assertion_passes(output, assertion):
    """Evaluate only frozen output; an absent condition cannot waive a check."""
    if not valid_assertion(assertion):
        raise ValueError("invalid sealed assertion")
    return _assertion_result(output, assertion) is True


def _assertion_result(output, assertion):
    if "when" in assertion:
        condition = _assertion_result(output, assertion["when"])
        if condition is None:
            return None
        if condition is False:
            return True
    for logical in ("all", "any"):
        if logical in assertion:
            checks = [_assertion_result(output, child) for child in assertion[logical]]
            decisive = logical != "all"
            if any(check is decisive for check in checks):
                return decisive
            return None if any(check is None for check in checks) else not decisive
    values = [output]
    for part in assertion["path"]:
        following = []
        for value in values:
            if part == "*":
                if not isinstance(value, list) or not value:
                    return None
                following.extend(value)
            else:
                if not (
                    isinstance(value, dict)
                    and isinstance(part, str)
                    and part in value
                    or isinstance(value, list)
                    and type(part) is int
                    and part < len(value)
                ):
                    return None
                following.append(value[part])
        values = following
    operator = next(iter(set(assertion) & COMPARISONS))
    expected = assertion[operator]

    def compare(value):
        if operator == "some":
            return isinstance(value, list) and any(assertion_passes(item, expected) for item in value)
        if operator == "satisfies":
            return assertion_passes(value, expected)
        if operator == "equals":
            return type(value) is type(expected) and value == expected
        if operator == "not_equals":
            return not (type(value) is type(expected) and value == expected)
        if operator in {"in", "not_in"}:
            found = any(type(value) is type(item) and value == item for item in expected)
            return found if operator == "in" else not found
        if operator in {"lte", "gte"}:
            return type(value) in (int, float) and (
                value <= expected if operator == "lte" else value >= expected
            )
        return isinstance(value, (str, list, dict)) and len(value) == expected

    return bool(values) and all(map(compare, values))


def pinned_file(root, reference, label, errors):
    if not isinstance(reference, dict) or not reference.get("path") or not reference.get("sha256"):
        errors.append(f"{label}: missing file reference and SHA256")
        return None, None
    path = resolve(root, reference["path"])
    if not path.is_file():
        errors.append(f"{label}: missing file {reference['path']}")
        return None, path
    if hashlib.sha256(path.read_bytes()).hexdigest() != reference["sha256"]:
        errors.append(f"{label}: file hash mismatch")
        return None, path
    try:
        value = read_json(path)
        if not isinstance(value, dict):
            raise TypeError("expected object")
        return value, path
    except (ValueError, TypeError, OSError):
        errors.append(f"{label}: invalid JSON")
        return None, path


def already_anonymous(root, case, packet, private, sources, raw_sources):
    if not (
        case["fixture_id"] == "F-25"
        and private.get("already_anonymous") is True
        and packet.get("masking", {}).get("already_anonymous") is True
        and len(sources) == len(raw_sources) == 1
    ):
        return False
    path = root / "src/tank/resources/fixture-input-authority-v1.json"
    if not path.is_file():
        return False
    content = path.read_bytes()
    if (
        packet.get("fixture_reference", {}).get("authoritative_input_sha256")
        != hashlib.sha256(content).hexdigest()
    ):
        return False
    authority = next((row for row in json.loads(content)["cases"] if row["fixture_id"] == "F-25"), {})
    leads = authority.get("source_leads", [])
    source = sources[0]
    return (
        len(leads) == 1
        and source.get("declared_synthetic") is True
        and source.get("source_id") == "S29"
        and source.get("author") == source.get("author_label") == "DECLARED_SYNTHETIC_FOUNDER"
        and all(source.get(key) == leads[0] for key in ("text", "title", "body"))
        and raw_sources[0] == leads[0]
    )


def check_ready(root, case):
    root = Path(root).resolve()
    errors = []
    name = case["fixture_id"]
    input_ref = case.get("acceptance_input")
    path = (
        resolve(root, input_ref["path"])
        if isinstance(input_ref, dict) and input_ref.get("path")
        else (root / "data/replay/inputs" / f"{name}.json")
    )
    if not path.is_file():
        return ["missing runnable input packet"]
    if (
        isinstance(input_ref, dict)
        and input_ref.get("sha256") != hashlib.sha256(path.read_bytes()).hexdigest()
    ):
        errors.append("acceptance input file hash mismatch")
    try:
        packet = read_json(path)
    except (ValueError, TypeError, OSError):
        return ["invalid input packet"]
    if not isinstance(packet, dict) or packet.get("fixture_status") == "INSUFFICIENT_FIXTURE":
        return errors + ["input is a placeholder, not a replay packet"]
    required = {"registry", "config", "records", "tool_results", "code_version", "config_version", "cutoff"}
    errors += [f"missing input field {key}" for key in sorted(required - packet.keys())]
    if errors:
        return errors
    records = packet["records"]
    if not isinstance(records, list):
        return ["records must be an array"]
    if not packet["registry"] or not packet["config"]:
        errors.append("empty registry/config snapshot")
    if any(packet[key] in (None, "", "unversioned") for key in ("code_version", "config_version")):
        errors.append("unversioned executable/configuration")
    input_hash = digest({k: v for k, v in packet.items() if k != "input_sha256"})
    if packet.get("input_sha256") != input_hash:
        errors.append("missing or incorrect input digest")
    # Replay outputs bind the whole input envelope, including its internal
    # payload checksum. Keep that identity distinct from the payload checksum.
    input_identity = digest(packet)
    sources = [r.get("record", {}) for r in records if r.get("collection") == "ingest.source_items"]
    if name not in MARKET_CASES and not sources:
        errors.append("missing source items")
    if any(not r.get("text") or not r.get("observed_at") or not r.get("raw_sha256") for r in sources):
        errors.append("source item lacks text, original observation time or raw provenance")
    raw_sources = []
    for source in sources:
        value = source.get("raw_path") or source.get("raw_ref")
        raw_path = resolve(root, value) if isinstance(value, str) else None
        if not raw_path or not raw_path.resolve().is_relative_to(root):
            errors.append("source raw bytes are outside the fixture workspace or missing")
            continue
        if raw_path.name in {".env", "auth.json"} or "secrets" in raw_path.parts:
            errors.append("credential path cannot be a fixture source")
            continue
        if not raw_path.is_file():
            errors.append("source raw file is missing")
            continue
        try:
            raw = read_bytes(raw_path)
        except ValueError:
            errors.append("source raw file is corrupt")
            continue
        if hashlib.sha256(raw).hexdigest() != source.get("raw_sha256"):
            errors.append("source raw bytes do not match their retained hash")
        raw_sources.append(raw.decode("utf-8", errors="replace"))
    snapshots = [r.get("record", {}) for r in records if r.get("collection") == "replay.market_snapshots"]
    observations = ("oi", "flow_bars", "klines", "books", "funding", "reference_points")
    if name in MARKET_REQUIRED and not any(
        r.get("artifact_kind") != "REFERENCE_MISSING_MARKET_INPUTS"
        and any(r.get(key) for key in observations)
        for r in snapshots
    ):
        errors.append("missing populated market reader snapshot; a missing-data declaration is not a capture")
    errors.extend(cutoff_errors(case, packet))
    if forbidden_fields(packet):
        errors.append("expected outcomes/assertions leaked into pipeline inputs")
    masking = packet.get("masking", {})
    if masking.get("applied") is not True:
        errors.append("identities have not been masked")
    private, private_path = pinned_file(root, case.get("private_name_map"), "private name map", errors)
    if private_path and private_path.resolve().is_relative_to(path.parent.resolve()):
        errors.append("private name map is inside pipeline input directory")
    if private:
        replacements = private.get("replacements", {})
        anonymous = not replacements and already_anonymous(root, case, packet, private, sources, raw_sources)
        if (
            not isinstance(replacements, dict)
            or not replacements
            and not anonymous
            or any(
                not isinstance(k, str) or not k or not isinstance(v, str) or not v
                for k, v in replacements.items()
            )
            or len(set(replacements.values())) != len(replacements)
        ):
            errors.append("name map is empty or not reversible")
        elif masking.get("map_sha256") != digest(private):
            errors.append("input does not bind the private name map")
        else:
            case_sensitive = private.get("case_sensitive_names", [])
            if not (
                isinstance(case_sensitive, list)
                and all(isinstance(name, str) and name in replacements for name in case_sensitive)
            ):
                errors.append("invalid case-sensitive identity scope")
                case_sensitive = []
            names = re.compile(
                r"(?<!\w)(?:"
                + "|".join(
                    re.escape(k) if k in case_sensitive else "(?i:" + re.escape(k) + ")"
                    for k in sorted(replacements, key=len, reverse=True)
                )
                + r")(?!\w)",
            )
            if replacements and (
                names.search(canonical(packet)) or any(names.search(raw) for raw in raw_sources)
            ):
                errors.append("original identities remain in pipeline-visible input")
    sealed, sealed_path = pinned_file(root, case.get("sealed_outcomes"), "sealed outcomes", errors)
    if sealed_path and sealed_path.resolve().is_relative_to(path.parent.resolve()):
        errors.append("sealed outcomes are inside pipeline input directory")
    if sealed:
        if sealed.get("fixture_id") != name or sealed.get("input_sha256") != input_identity:
            errors.append("sealed outcomes are not bound to this exact fixture input")
        if sealed.get("assertion_spec") != case["assertion_spec"]:
            errors.append("sealed requirements differ from TDD §11.3")
        manual = sealed.get("manual_predicates", [])
        if not isinstance(manual, list) or any(
            not isinstance(row, dict)
            or not isinstance(row.get("requirement"), str)
            or not row["requirement"].strip()
            for row in manual
        ):
            errors.append("sealed manual requirements are invalid")
        assertions = sealed.get("assertions")
        if not isinstance(assertions, list) or not assertions:
            errors.append("sealed outcome has no executable assertions")
        elif not all(map(valid_assertion, assertions)):
            errors.append("sealed assertions are invalid")
        try:
            time(sealed["sealed_at"])
        except (KeyError, TypeError, ValueError):
            errors.append("sealed outcomes lack a declared pre-run sealing time")
    return errors


def check_completed(root, case):
    errors = check_ready(root, case)
    run, _ = pinned_file(Path(root), case.get("acceptance_run"), "acceptance run", errors)
    if not run:
        return errors
    output, _ = pinned_file(Path(root), run.get("frozen_output"), "frozen output", errors)
    if output:
        if output.get("status") != "COMPLETED" or output.get("output_sha256") != digest(output.get("output")):
            errors.append("acceptance output is incomplete or not integrity-bound")
        sealed, _ = pinned_file(Path(root), case.get("sealed_outcomes"), "sealed outcomes", errors)
        if sealed:
            if output.get("input_sha256") != sealed.get("input_sha256"):
                errors.append("acceptance used a different input generation")
            try:
                if time(sealed["sealed_at"]) > time(run["started_at"]):
                    errors.append("outcomes were sealed after the run started")
            except (KeyError, TypeError, ValueError):
                errors.append("cannot verify outcomes were sealed before the run")
            for assertion in sealed.get("assertions", []):
                try:
                    passed = assertion_passes(output.get("output"), assertion)
                except ValueError:
                    errors.append("invalid sealed assertion")
                    continue
                if not passed:
                    errors.append(f"assertion failed: {canonical(assertion)}")
            requirements = sealed.get("manual_predicates", [])
            if requirements:
                review, _ = pinned_file(
                    Path(root), run.get("manual_review"), "required manual review", errors
                )
                if review:
                    if (
                        review.get("input_sha256") != sealed.get("input_sha256")
                        or review.get("frozen_output_sha256") != run["frozen_output"]["sha256"]
                    ):
                        errors.append("manual review is not bound to this input and output")
                    checks = review.get("checks")
                    if not isinstance(checks, list) or len(checks) != len(requirements):
                        errors.append("manual review does not cover every sealed requirement")
                    else:
                        for requirement, check in zip(requirements, checks, strict=True):
                            if not isinstance(check, dict):
                                errors.append("invalid manual review check")
                                continue
                            if check.get("requirement") != requirement.get("requirement"):
                                errors.append("manual review changed a sealed requirement")
                            if check.get("passed") is not True:
                                errors.append("required manual clause failed: " + requirement["requirement"])
                            if not isinstance(check.get("reason"), str) or not check["reason"].strip():
                                errors.append("manual review lacks reasoning")
                            evidence = check.get("evidence")
                            if not isinstance(evidence, list) or not evidence:
                                errors.append("manual review lacks retained evidence")
                            else:
                                for reference in evidence:
                                    pinned_file(Path(root), reference, "manual review evidence", errors)
    return errors
