"""Reference vectors for tank-capture's Python-compatible primitives.

CPython and Tank's own helpers are the reference: decimal arithmetic and rendering,
json.dumps as Tank hashes it, declared-unit tank.util.parse_epoch/iso and tank.time_precision.
Writes capture/testdata/pyvectors/{decimal,json,time}.json.gz.
Run: uv run python scripts/capture/gen_pyvectors.py
"""

import gzip
import json
import random
from decimal import Context, Decimal, DivisionByZero, InvalidOperation, localcontext
from pathlib import Path
from typing import Literal

from pydantic import JsonValue

from tank.contracts import DomainModel
from tank.time_precision import propagate_timestamp
from tank.util import EpochUnit, iso, parse_epoch, parse_utc_time

type Operation = Literal["add", "sub", "mul", "div"]

OUT = Path(__file__).resolve().parents[2] / "capture" / "testdata" / "pyvectors"


class PrecisionFixture(DomainModel):
    record: dict[str, JsonValue]
    field: str
    aliases: list[str]


class EpochPrecisionFixture(PrecisionFixture):
    unit: EpochUnit


def write(name: str, payload: JsonValue) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n"
    (OUT / name).write_bytes(gzip.compress(text.encode(), mtime=0))


def literal(rng: random.Random) -> str:
    kind = rng.random()
    sign = "-" if rng.random() < 0.15 else ""
    if kind < 0.1:
        return sign + rng.choice(["0", "0.0", "0.00000000", "0E-8", "0E+3", "00", "0.000"])
    if kind < 0.2:
        digits = "".join(rng.choice("0123456789") for _ in range(rng.randint(1, 40)))
        return sign + digits + "E" + str(rng.randint(-30, 30))
    if kind < 0.3:
        return (
            sign
            + "0."
            + "0" * rng.randint(0, 12)
            + "".join(rng.choice("123456789") for _ in range(rng.randint(1, 8)))
        )
    whole = "".join(rng.choice("0123456789") for _ in range(rng.randint(1, 20))).lstrip("0") or "0"
    frac = "".join(rng.choice("0123456789") for _ in range(rng.randint(0, 20)))
    return sign + whole + ("." + frac if frac or rng.random() < 0.2 else "")


def run(op: Operation, a: Decimal, b: Decimal, prec: int) -> list[JsonValue] | None:
    with localcontext(Context(prec=prec)):
        try:
            if op == "add":
                value = a + b
            elif op == "sub":
                value = a - b
            elif op == "mul":
                value = a * b
            else:
                value = a / b
        except (DivisionByZero, InvalidOperation):
            return None
        return [str(value), value.as_tuple().exponent]


def decimals(rng: random.Random) -> None:
    literals = sorted({literal(rng) for _ in range(700)}) + [
        "1e-05",
        "1.0E+2",
        "0.000000100",
        "0.0000001",
        "100000",
        "1E+2",
        "  2.50 ",
        "1_000.5",
        "123456789012345678.123456789012345678",
        "12345.123456789012345678",
        ".5",
        "5.",
        "+7",
        "-0",
    ]
    rendered: list[JsonValue] = [
        {"text": text, "str": str(Decimal(text)), "f": format(Decimal(text), "f")} for text in literals
    ]
    ops: list[JsonValue] = []
    operations: list[Operation] = ["add", "sub", "mul", "div"]
    for _ in range(2500):
        a, b = Decimal(rng.choice(literals)), Decimal(rng.choice(literals))
        op = rng.choice(operations)
        for prec in (28, 100):
            ops.append({"op": op, "a": str(a), "b": str(b), "prec": prec, "result": run(op, a, b, prec)})

    for _ in range(400):
        q = Decimal(rng.choice(["0.0001", "0.00012", "1.5", "3", "0.123456789", "25000", "0.00000300"]))
        p = Decimal(rng.choice(["83520.4", "0.000007", "1.0002", "2.5", "0.0000001", "64000"]))
        for op, a, b in (("mul", q, q), ("mul", q, p), ("div", q * p, q + q), ("div", p + p, Decimal(2))):
            ops.append({"op": op, "a": str(a), "b": str(b), "prec": 100, "result": run(op, a, b, 100)})
    write("decimal.json.gz", {"literals": rendered, "ops": ops})


def text(rng: random.Random) -> str:
    pool = 'abcXYZ019 _-:./"\\\n\t\r\b\f\x01\x1f\x7féü€漢😀 '
    return "".join(rng.choice(pool) for _ in range(rng.randint(0, 12)))


def value(rng: random.Random, depth: int = 0) -> JsonValue:
    kind = rng.random()
    if depth > 3 or kind < 0.35:
        return rng.choice(
            [None, True, False, rng.randint(-(10**12), 10**12), text(rng), str(Decimal(literal(rng)))]
        )
    if kind < 0.65:
        return [value(rng, depth + 1) for _ in range(rng.randint(0, 4))]
    return {text(rng): value(rng, depth + 1) for _ in range(rng.randint(0, 5))}


def jsons(rng: random.Random) -> None:
    cases: list[JsonValue] = []
    for _ in range(600):
        item = value(rng)
        cases.append(
            {
                "value": item,
                "ascii": json.dumps(item, sort_keys=True, separators=(",", ":")),
                "utf8": json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
            }
        )
    floats = [0.1, 1.0, 28800.0, 1e16, 1e-05, 0.0001, 123456789012345.6, 1.5e300, -2.75, 1e22, 3.0e-7]
    floats += [rng.uniform(-1e6, 1e6) for _ in range(100)] + [rng.random() * 10 ** rng.randint(-8, 18)]
    write("json.json.gz", {"values": cases, "floats": [[f, repr(f)] for f in floats]})


def times(rng: random.Random) -> None:
    inputs = [
        "2026-10-21T09:00:00Z",
        "2026-10-21T09:00:00.123456Z",
        "2026-10-21T09:00:00.123456789Z",
        "2026-10-21T09:00:00.1Z",
        "2026-10-21T09:00:00+00:00",
        "2026-10-21T11:30:00+02:30",
        "2026-10-21T09:00:00",
        "2026-10-21 09:00:00.5",
        "2026-10-21",
    ]
    epoch_strings: list[tuple[str, EpochUnit]] = [
        ("1781654400123", "ms"),
        ("1790249583.872436", "s"),
        ("1781654400", "s"),
    ]
    numbers: list[tuple[int, EpochUnit]] = [
        (1781654400123, "ms"),
        (1781654400123456, "us"),
        (1790249583, "s"),
        (1792574749213, "ms"),
    ]
    numbers += [(rng.randint(1_600_000_000_000, 1_900_000_000_000), "ms") for _ in range(200)]
    numbers += [(rng.randint(1_600_000_000_000_000, 1_900_000_000_000_000), "us") for _ in range(100)]
    decimals_ = ["1790249583.872436", "1790249585.0128748", "1688669448.3138"]
    decimals_ += [
        f"{rng.randint(1_600_000_000, 1_900_000_000)}.{rng.randint(0, 9999999):07d}" for _ in range(200)
    ]
    floats = [1781654400.1234567] + [rng.uniform(1.6e9, 1.9e9) for _ in range(100)]
    cases: list[JsonValue] = [{"kind": "str", "input": s, "iso": iso(parse_utc_time(s))} for s in inputs]
    cases += [{"kind": "str", "input": s, "iso": iso(parse_epoch(s, unit=unit))} for s, unit in epoch_strings]
    cases += [{"kind": "int", "input": n, "iso": iso(parse_epoch(n, unit=unit))} for n, unit in numbers]
    cases += [
        {"kind": "decimal", "input": d, "iso": iso(parse_epoch(Decimal(d), unit="s"))} for d in decimals_
    ]
    cases += [{"kind": "float", "input": f, "iso": iso(parse_epoch(f, unit="s"))} for f in floats]
    precisions: list[JsonValue] = []
    precision_inputs: list[PrecisionFixture] = [
        PrecisionFixture(record={"ts": "2026-10-21T09:00:00Z", "ts_prec": "EXACT"}, field="ts", aliases=[]),
        PrecisionFixture(record={"ts": None, "ts_prec": None}, field="ts", aliases=[]),
        PrecisionFixture(record={"ts": None, "ts_prec": "COLLECTION_ONLY"}, field="ts", aliases=[]),
        EpochPrecisionFixture(
            record={"ts": 1781654400123, "ts_prec": None}, field="ts", aliases=[], unit="ms"
        ),
        PrecisionFixture(
            record={"ts": "2026-10-21T09:00:00Z", "ts_prec": "BUCKET(300s)", "ts_bucket_size_s": 300},
            field="ts",
            aliases=[],
        ),
        PrecisionFixture(
            record={
                "ts": "2026-10-21T09:00:00Z",
                "ts_prec": "BUCKET(300s)",
                "timestamp_precision": "BUCKET(300s)",
            },
            field="ts",
            aliases=["timestamp_precision"],
        ),
        PrecisionFixture(
            record={"ts": "2026-10-21T09:00:00Z", "ts_prec": "BUCKET(300s)", "ts_bucket_anchor": "END"},
            field="ts",
            aliases=[],
        ),
        PrecisionFixture(
            record={"ts": "2026-10-21T09:00:00Z", "ts_prec": "BUCKET(5m)", "ts_bucket_anchor": None},
            field="ts",
            aliases=[],
        ),
        PrecisionFixture(
            record={"minute_start": "2026-10-21T09:00:00Z", "minute_start_prec": "BUCKET(60s)"},
            field="minute_start",
            aliases=[],
        ),
        PrecisionFixture(
            record={"as_of": None, "as_of_prec": None, "as_of_prec_reason": "SOURCE_CLOCK_UNAVAILABLE"},
            field="as_of",
            aliases=[],
        ),
        PrecisionFixture(record={"x": "2026-10-21T09:00:00Z", "x_prec": "HOUR"}, field="x", aliases=[]),
        PrecisionFixture(record={"x": "2026-10-21T09:00:00Z", "x_precision": "EXACT"}, field="x", aliases=[]),
    ]
    for case in precision_inputs:
        normalized = dict(case.record)
        if isinstance(case, EpochPrecisionFixture):
            raw = normalized[case.field]
            if isinstance(raw, bool) or not isinstance(raw, (str, int, float)):
                raise ValueError("Epoch precision fixture requires a numeric scalar")
            normalized[case.field] = iso(parse_epoch(raw, unit=case.unit))
        stamp = propagate_timestamp(normalized, case.field, precision_aliases=case.aliases)
        fields: dict[str, JsonValue] = {name: value for name, value in stamp.fields(case.field).items()}
        precisions.append(
            {
                "record": case.record,
                "field": case.field,
                "aliases": [alias for alias in case.aliases],
                "fields": fields,
            }
        )
    write(
        "time.json.gz",
        {"times": cases, "precisions": precisions, "parse_check": iso(parse_epoch(0, unit="s"))},
    )


def main() -> None:
    rng = random.Random(20261021)
    decimals(rng)
    jsons(rng)
    times(rng)


if __name__ == "__main__":
    main()
