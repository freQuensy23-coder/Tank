import asyncio
import contextlib
import hashlib
import itertools
import json
import math
import os
import re
import tempfile
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx

from tank.clickhouse_market import ClickHouse

_SAFE_PHRASES = (
    "Unknown identifier",
    "Unknown expression or function identifier",
    "Function with name",
    "does not exist",
    "Unknown function",
    "Syntax error",
    "Expected one of",
    "Illegal type",
    "Nested type",
    "inside Nullable",
    "There is no supertype",
    "Cannot convert",
    "Cannot parse",
    "Division by zero",
    "Decimal math overflow",
    "Too large",
    "Too deep",
    "AST",
    "scale",
    "precision",
    "native financial range",
    "UNKNOWN_IDENTIFIER",
    "UNKNOWN_FUNCTION",
    "SYNTAX_ERROR",
    "ILLEGAL_TYPE_OF_ARGUMENT",
    "NO_COMMON_TYPE",
    "NUMBER_OF_ARGUMENTS_DOESNT_MATCH",
    "BAD_ARGUMENTS",
    "Query tree is too big",
    "TYPE_MISMATCH",
    "DECIMAL_OVERFLOW",
    "TOO_LARGE_QUERY",
    "TOO_DEEP_AST",
    "TOO_BIG_AST",
    "CANNOT_PARSE_TEXT",
    "CANNOT_PARSE_NUMBER",
    "ATTEMPT_TO_READ_AFTER_EOF",
    "URL too long",
    "URL component",
    "too long",
    "UNKNOWN_SETTING",
    "UNSUPPORTED_METHOD",
    "URI",
    "Request",
    "Too Long",
    "Bad Gateway",
    "Service Unavailable",
    "Server Error",
    "Field value too long",
    "Query parameter",
    "Substitution",
    "not set",
    "constant",
    "cannot be parsed",
    "must be the same",
    "accumulator",
    "cannot be inside Nullable",
    "Cannot convert NULL value to non-Nullable type",
    "Conversion functions with postfix",
    "should take String argument",
    "cannot be converted",
    "cannot be Nullable",
    "array aggregation function cannot be performed on type",
)

_SAFE_NATIVE_FUNCTIONS = frozenset(
    {
        "_CAST",
        "CAST",
        "accurateCast",
        "accurateCastOrNull",
        "plus",
        "minus",
        "multiply",
        "divide",
        "intDiv",
        "modulo",
        "positiveModulo",
        "negate",
        "equals",
        "notEquals",
        "less",
        "lessOrEquals",
        "greater",
        "greaterOrEquals",
        "and",
        "or",
        "not",
        "in",
        "notIn",
        "isNull",
        "isNotNull",
        "ifNull",
        "nullIf",
        "assumeNotNull",
        "coalesce",
        "if",
        "multiIf",
        "array",
        "arrayElement",
        "arrayElementOrNull",
        "arrayMap",
        "arrayFilter",
        "arrayFold",
        "arraySum",
        "arrayMin",
        "arrayMax",
        "arraySort",
        "tuple",
        "tupleElement",
        "toNullable",
        "toFloat64",
        "toFloat64OrNull",
        "toFloat64OrZero",
    }
)

_SAFE_WORDS = frozenset(
    [
        "maximum",
        "minimum",
        "parse",
        "parser",
        "depth",
        "recursion",
        "exceeded",
        "limit",
        "setting",
        "settings",
        "unexpected",
        "argument",
        "arguments",
        "lambda",
        "parameters",
        "parameter",
        "requires",
        "expression",
        "expressions",
        "constant",
        "constants",
        "fold",
        "subquery",
        "subqueries",
        "type",
        "types",
        "regex",
        "regexp",
        "pattern",
        "regular",
        "invalid",
        "empty",
        "supported",
        "unsupported",
        "query",
        "named",
        "alias",
        "aliases",
        "conflict",
        "conflicting",
        "duplicate",
        "multiple",
        "definition",
        "defined",
        "already",
        "different",
        "differ",
        "text",
        "format",
        "function",
        "functions",
        "aggregate",
        "unknown",
        "found",
        "name",
        "names",
        "expected",
        "too",
        "many",
        "size",
        "range",
        "length",
        "must",
        "equal",
        "same",
        "number",
        "not",
        "allowed",
        "allow",
        "cannot",
        "disabled",
        "enabled",
        "enable",
        "disable",
        "nested",
        "identifier",
        "identifiers",
        "one",
        "two",
        "three",
        "max_parser_depth",
        "max_ast_depth",
        "max_expanded_ast_elements",
        "max_query_size",
        "http_max_field_value_size",
    ]
)


def failure_receipt(error, nodeid, query):
    message = str(error)
    status = re.match(r"ClickHouse (\d{3}):", message)
    code = re.search(r"\bCode: (\d+)\b", message)
    complete_message = message
    message = re.split(r"(?i)\b(?:while processing|in scope|stack trace)\b", message)[0]
    message_tokens = {word.casefold() for word in re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", message)}
    diagnostics = sorted(name for name in _SAFE_NATIVE_FUNCTIONS if name.casefold() in message_tokens)
    if "array aggregation function cannot be performed on type" in message.casefold():
        native_calls = {
            word.casefold() for word in re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", complete_message)
        }
        diagnostics.extend(sorted(name for name in _SAFE_NATIVE_FUNCTIONS if name.casefold() in native_calls))
    diagnostics.extend(
        phrase
        for phrase in _SAFE_PHRASES
        if re.search(r"(?<!\w)" + re.escape(phrase) + r"(?!\w)", message, re.IGNORECASE)
    )
    diagnostics.extend(
        dict.fromkeys(
            word.lower() for word in re.findall(r"\b[A-Za-z_]+\b", message) if word.lower() in _SAFE_WORDS
        )
    )
    identifiers = set(re.findall(r"\b(?:spot|deriv|rel|lq|sup|res)_[A-Za-z0-9_]+\b", query))
    identifiers.update(
        {
            "UInt8",
            "UInt32",
            "UInt64",
            "Int8",
            "Int32",
            "Int64",
            "Float64",
            "Decimal",
            "Decimal256",
            "Nothing",
            "String",
            "Nullable",
            "Array",
            "Tuple",
            "Bool",
        }
    )
    diagnostics.extend(sorted(name for name in identifiers if name.casefold() in message_tokens))
    diagnostics.extend(re.findall(r"\bposition [0-9]{1,8}\b", message))
    safe = " | ".join(diagnostics)[:512] or "[redacted]"
    return {
        "exception_class": type(error).__name__[:128],
        "http_status": int(status[1]) if status else None,
        "native_error_code": int(code[1]) if code else None,
        "server_message": safe,
        "nodeid": nodeid[:512],
        "query_digest": hashlib.sha256(query.encode()).hexdigest(),
    }


def retain_first_failure(directory, receipt):
    _retain_first_receipt(directory, receipt, "truth-sql-failure-")


def retain_liquidity_phase(directory, nodeid, phase, *, status="PASS"):
    if status not in {"STARTED", "PASS"}:
        raise ValueError("unexpected liquidity qualification status")
    if phase not in {
        "volume_population",
        "volume_coverage",
        "volume_totals",
        "tier_values",
        "venue_stats",
        "venue_depth",
        "venue_precision",
        "venue_walk_t1",
        "venue_walk_t2",
        "venue_walk_t3",
        "venue_output",
        "burst_output",
        "state",
    }:
        raise ValueError("unexpected liquidity qualification phase")
    _retain_first_receipt(
        directory,
        {"nodeid": (nodeid + "::" + phase)[:512], "phase": phase, "status": status},
        "truth-sql-liquidity-phase-" + status.lower() + "-",
    )


def retain_transport_qualification(directory, nodeid, checks):
    domain_parameters = {
        "spot_snapshot",
        "deriv_snapshot",
        "deriv_config",
        "rel_snapshot",
        "lq_snapshot",
        "lq_config",
        "lq_spot",
        "lq_relative",
        "sup_snapshot",
        "sup_config",
        "sup_spots",
        "sup_derivatives",
        "sup_liquidity",
        "sup_relative",
        "sup_supply_preimages",
        "res_flow_snapshot",
        "res_snapshot",
        "res_config",
        "res_spots",
        "res_derivatives",
        "res_liquidity",
        "res_supply",
        "res_relative",
        "res_confidence",
    }
    if set(checks) not in ({"ascii", "unicode", "empty"}, domain_parameters):
        raise ValueError("unexpected transport qualification population")
    for check in checks.values():
        if set(check) != {"bytes", "characters", "sha256"}:
            raise ValueError("unexpected transport qualification fields")
        if any(type(check[name]) is not int or check[name] < 0 for name in ("bytes", "characters")):
            raise ValueError("invalid transport qualification length")
        if not isinstance(check["sha256"], str) or re.fullmatch("[a-f0-9]{64}", check["sha256"]) is None:
            raise ValueError("invalid transport qualification digest")
    _retain_first_receipt(
        directory,
        {"nodeid": nodeid[:512], "status": "PASS", "checks": checks},
        "truth-sql-transport-qualified-",
    )


def _retain_first_receipt(directory, receipt, prefix):
    directory = Path(directory)
    if directory.is_symlink():
        raise ValueError("SQL failure evidence directory must not be a symlink")
    directory.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(receipt["nodeid"].encode()).hexdigest()
    destination = directory / f"{prefix}{digest}.json"
    if destination.is_symlink():
        raise ValueError("SQL failure evidence destination must not be a symlink")
    descriptor, temporary = tempfile.mkstemp(prefix=".truth-sql-failure-", dir=directory)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(receipt, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        with contextlib.suppress(FileExistsError):
            os.link(temporary, destination)
    finally:
        os.unlink(temporary)


_LIQUIDITY_BASELINE = (
    "tests/test_truth_sql_liquidity.py::test_native_liquidity_complete_original_output[no_event_anchor]"
)
_LIQUIDITY_PHASE_CASE = (
    "tests/test_truth_sql_liquidity.py::test_native_liquidity_absence_relational_phase_contracts"
)
_LIQUIDITY_SELECTIONS = {
    "volume_population": "SELECT length(lq_volume_flows) AS value FROM lq_volume_population",
    "volume_coverage": "SELECT length(lq_covered_buckets) AS value FROM lq_volume_coverage",
    "volume_totals": "SELECT arrayMap(x -> toString(x), lq_volumes) AS value FROM lq_volume_totals",
    "tier_values": "SELECT arrayMap(x -> toString(x), lq_tiers) AS value FROM lq_tier_values",
    "venue_stats": "SELECT arraySort(groupArray(tuple(venue, pre_spread))) AS value FROM lq_venue_stats",
    "venue_depth": (
        "SELECT arraySort(groupArray(tuple(venue, toString(bid), toString(ask), "
        "toString(current_total), spread))) AS value FROM lq_venue_stats"
    ),
    "venue_precision": (
        "SELECT arraySort(groupArray(tuple(venue, lq_precision(current.3)))) AS value FROM lq_venue_stats"
    ),
    **{
        "venue_walk_t" + str(index): (
            f"SELECT arraySort(groupArray(tuple(venue, lq_walk_book(current.3, lq_tiers[{index}], 'ask')))) "
            "AS value FROM lq_venue_stats CROSS JOIN lq_tier_values"
        )
        for index in (1, 2, 3)
    },
    "venue_output": (
        "SELECT arraySort(x -> JSONExtractString(x, 'venue'), groupArray(venue_json)) AS value FROM lq_venues"
    ),
    "burst_output": "SELECT observations AS value FROM lq_observed",
    "state": "SELECT toString(lq_net) AS value FROM lq_state",
}
_SUMMARY_FIELDS = (
    "read_rows",
    "read_bytes",
    "written_rows",
    "written_bytes",
    "total_rows_to_read",
    "result_rows",
    "result_bytes",
    "elapsed_ns",
)
_PROCESS_FIELDS = (
    "elapsed",
    "read_rows",
    "read_bytes",
    "memory_usage",
    "peak_memory_usage",
    "is_cancelled",
    "is_all_data_sent",
)
_PROFILE_EVENTS = (
    "FunctionExecute",
    "ScalarSubqueriesGlobalCacheHit",
    "ScalarSubqueriesLocalCacheHit",
    "ScalarSubqueriesCacheMiss",
)
_OBSERVER_INSTANCES = itertools.count()


def _liquidity_observation(nodeid, query):
    if nodeid == _LIQUIDITY_BASELINE:
        if hashlib.sha256(query.encode()).hexdigest() == (
            "0fab638e168c4c8f0f6b601b05e4cf2f8a00d65ebf429cae18c606da8b56c897"
        ):
            return "baseline"
        return None
    for phase, selection in _LIQUIDITY_SELECTIONS.items():
        if (
            nodeid == _LIQUIDITY_PHASE_CASE + "::" + phase
            and query.startswith("WITH \n{lq_snapshot:String} AS lq_snapshot,")
            and query.endswith(" " + selection)
        ):
            return phase
    return None


def _bounded_number(value, *, integer=True, text=False, maximum=(1 << 64) - 1):
    if text and type(value) is str and re.fullmatch(r"[0-9]{1,20}", value):
        value = int(value)
    if integer and type(value) is not int:
        raise ValueError("invalid integer metric")
    if not integer and type(value) not in {int, float}:
        raise ValueError("invalid numeric metric")
    if not math.isfinite(value) or not 0 <= value <= maximum:
        raise ValueError("metric outside admitted range")
    return value


def _metric_projection(values, names, *, text=False):
    if type(values) is not dict:
        raise ValueError("invalid metric object")
    result = {}
    for name in names:
        value = values.get(name)
        result[name + "_present"] = int(name in values)
        if name in values:
            maximum = (1 << 64) - 1
            if name == "elapsed":
                maximum = 86400
            elif name in {"is_cancelled", "is_all_data_sent"}:
                maximum = 1
            result[name] = _bounded_number(value, integer=name != "elapsed", text=text, maximum=maximum)
    return result


def _unique_json(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate metric key")
        result[key] = value
    return result


def _summary_metrics(response):
    raw = response.headers.get("X-ClickHouse-Summary")
    if raw is None:
        return {"status": "absent"}
    try:
        if len(raw) > 16384:
            raise ValueError("oversized summary")
        return {
            "status": "observed",
            **_metric_projection(json.loads(raw, object_pairs_hook=_unique_json), _SUMMARY_FIELDS, text=True),
        }
    except (ValueError, TypeError, OverflowError):
        return {"status": "invalid"}


def _own_query_sql():
    return (
        "SELECT "
        + ", ".join(_PROCESS_FIELDS)
        + ", "
        + ", ".join(
            f"mapContains(ProfileEvents, '{name}') AS {name}_present, ProfileEvents['{name}'] AS {name}"
            for name in _PROFILE_EVENTS
        )
        + " FROM system.processes WHERE query_id = {observer_query_id:String} LIMIT 1 FORMAT JSONEachRow"
    )


class EvidenceClickHouse(ClickHouse):
    def __init__(self, url, nodeid, *, http=None):
        if urlparse(url).hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("native SQL qualification requires a loopback service")
        super().__init__(url, http=http)
        self.nodeid = nodeid
        self._evidence_query = None
        self._observation = None
        self._original_http_post = self.http.post
        self._observe_ci = os.environ.get("GITHUB_ACTIONS") == "true" and url == os.environ.get(
            "CLICKHOUSE_TEST_URL"
        )
        self._observer_identity = ":".join(
            [
                os.environ.get(name, "")
                for name in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_JOB", "PYTEST_XDIST_WORKER")
            ]
            + [str(os.getpid()), str(next(_OBSERVER_INSTANCES))]
        )
        self._request_count = 0
        self.http.event_hooks["response"].append(self._response_evidence)

    async def _response_evidence(self, response):
        if self._observation is not None:
            self._observation["headers_offset_ns"] = time.monotonic_ns() - self._observation["start"]
            self._observation["summary"] = _summary_metrics(response)
        if response.status_code == 200 or self._evidence_query is None:
            return
        await response.aread()
        with contextlib.suppress(OSError, ValueError):
            retain_first_failure(
                Path(__file__).resolve().parents[1] / ".test-results",
                failure_receipt(
                    RuntimeError(f"ClickHouse {response.status_code}: {response.text}"),
                    self.nodeid,
                    self._evidence_query,
                ),
            )

    async def _observed_http_post(self, url, **kwargs):
        observation = self._observation
        kwargs["headers"] = dict(kwargs["headers"]) | {"X-ClickHouse-Query-Id": observation["query_id"]}
        observation["http_start_offset_ns"] = time.monotonic_ns() - observation["start"]
        try:
            return await self._original_http_post(url, **kwargs)
        finally:
            observation["http_end_offset_ns"] = time.monotonic_ns() - observation["start"]

    async def _post(self, query, params):
        body = await super()._post(query, params)
        if self._observation is not None:
            self._observation["decode_start_offset_ns"] = time.monotonic_ns() - self._observation["start"]
        return body

    async def _own_query_snapshot(self, query_id):
        try:
            async with asyncio.timeout(5):
                response = await self._original_http_post(
                    self.base,
                    params={
                        "param_observer_query_id": query_id,
                        "output_format_json_quote_64bit_integers": "0",
                    },
                    content=_own_query_sql().encode(),
                    headers={"X-ClickHouse-User": self.user, "X-ClickHouse-Key": self.password},
                    timeout=httpx.Timeout(4, connect=1),
                    follow_redirects=False,
                )
                if response.status_code != 200:
                    return {"status": "http_error", "http_status": response.status_code}
                if len(response.content) > 16384:
                    return {"status": "invalid"}
                rows = [
                    json.loads(line, object_pairs_hook=_unique_json) for line in response.content.splitlines()
                ]
                if not rows:
                    return {"status": "absent"}
                if len(rows) != 1:
                    return {"status": "invalid"}
                row = rows[0]
                metrics = _metric_projection(row, _PROCESS_FIELDS)
                for name in _PROFILE_EVENTS:
                    present = _bounded_number(row[name + "_present"], maximum=1)
                    metrics[name + "_present"] = present
                    value = _bounded_number(row[name])
                    if present:
                        metrics[name] = value
                return {"status": "observed", **metrics}
        except (Exception, asyncio.CancelledError) as error:
            safe_classes = {
                httpx.ReadTimeout,
                httpx.ConnectTimeout,
                TimeoutError,
                ValueError,
                KeyError,
                asyncio.CancelledError,
            }
            return {
                "status": "error",
                "exception_class": type(error).__name__ if type(error) in safe_classes else "other",
            }

    async def rows(self, query, params=None):
        phase = _liquidity_observation(self.nodeid, query) if self._observe_ci else None
        observation = None
        if phase is not None:
            self._request_count += 1
            query_id = hashlib.sha256(
                f"{self._observer_identity}:{self.nodeid}:{self._request_count}".encode()
            ).hexdigest()
            observation = {
                "start": time.monotonic_ns(),
                "query_id": query_id,
                "summary": {"status": "no_response"},
                "outcome": "interrupted",
            }
            self._observation = observation
            self.http.post = self._observed_http_post
        self._evidence_query = query
        timeout = False
        try:
            rows = await super().rows(query, params)
            if observation is not None:
                observation["outcome"] = "decoded"
            return rows
        except Exception as error:
            if observation is not None:
                timeout = isinstance(error, httpx.ReadTimeout)
                observation["outcome"] = "read_timeout" if timeout else "exception"
            with contextlib.suppress(OSError, ValueError):
                retain_first_failure(
                    Path(__file__).resolve().parents[1] / ".test-results",
                    failure_receipt(error, self.nodeid, query),
                )
            raise
        finally:
            end = time.monotonic_ns()
            self._evidence_query = None
            self._observation = None
            if observation is not None:
                self.http.post = self._original_http_post
                clocks = {"rows_end_offset_ns": end - observation["start"]}
                for name in (
                    "http_start_offset_ns",
                    "headers_offset_ns",
                    "http_end_offset_ns",
                    "decode_start_offset_ns",
                ):
                    if name in observation:
                        clocks[name] = observation[name]
                snapshot = {"status": "not_requested"}
                diagnostic_start = time.monotonic_ns()
                if timeout:
                    snapshot = await self._own_query_snapshot(observation["query_id"])
                with contextlib.suppress(Exception):
                    clocks = {
                        name: _bounded_number(value, maximum=86400 * 1_000_000_000)
                        for name, value in clocks.items()
                    }
                    _retain_first_receipt(
                        Path(__file__).resolve().parents[1] / ".test-results",
                        {
                            "nodeid": self.nodeid,
                            "phase": phase,
                            "query_digest": hashlib.sha256(query.encode()).hexdigest(),
                            "query_id": observation["query_id"],
                            "outcome": observation["outcome"],
                            "clock": "monotonic_ns_offsets_from_rows_entry_inclusive_nonadditive",
                            "clocks": clocks,
                            "summary": observation["summary"],
                            "own_query": snapshot,
                            "diagnostic_wall_ns": _bounded_number(time.monotonic_ns() - diagnostic_start),
                        },
                        "truth-sql-native-" + observation["query_id"] + "-",
                    )
