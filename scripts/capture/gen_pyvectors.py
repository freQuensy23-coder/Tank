"""Reference vectors for tank-capture's Python-compatible primitives.

CPython and Tank's own helpers are the reference: decimal arithmetic and rendering,
json.dumps as Tank hashes it, tank.util.parse_time/iso and tank.time_precision.
Writes capture/testdata/pyvectors/{decimal,json,time}.json.gz.
Run: uv run python scripts/capture/gen_pyvectors.py
"""

import gzip
import json
import random
from decimal import Context, Decimal, DivisionByZero, InvalidOperation, localcontext
from pathlib import Path

from tank.time_precision import propagate_timestamp
from tank.util import iso, parse_time

OUT = Path(__file__).resolve().parents[2] / "capture" / "testdata" / "pyvectors"


def write(name, payload) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n"
    (OUT / name).write_bytes(gzip.compress(text.encode(), mtime=0))


def literal(rng):
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


def run(op, a, b, prec):
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


def decimals(rng) -> None:
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
    rendered = [
        {"text": text, "str": str(Decimal(text)), "f": format(Decimal(text), "f")} for text in literals
    ]
    ops = []
    for _ in range(2500):
        a, b = Decimal(rng.choice(literals)), Decimal(rng.choice(literals))
        op = rng.choice(["add", "sub", "mul", "div"])
        for prec in (28, 100):
            ops.append({"op": op, "a": str(a), "b": str(b), "prec": prec, "result": run(op, a, b, prec)})

    for _ in range(400):
        q = Decimal(rng.choice(["0.0001", "0.00012", "1.5", "3", "0.123456789", "25000", "0.00000300"]))
        p = Decimal(rng.choice(["83520.4", "0.000007", "1.0002", "2.5", "0.0000001", "64000"]))
        for op, a, b in (("mul", q, q), ("mul", q, p), ("div", q * p, q + q), ("div", p + p, Decimal(2))):
            ops.append({"op": op, "a": str(a), "b": str(b), "prec": 100, "result": run(op, a, b, 100)})
    write("decimal.json.gz", {"literals": rendered, "ops": ops})


def text(rng):
    pool = 'abcXYZ019 _-:./"\\\n\t\r\b\f\x01\x1f\x7féü€漢😀 '
    return "".join(rng.choice(pool) for _ in range(rng.randint(0, 12)))


def value(rng, depth=0):
    kind = rng.random()
    if depth > 3 or kind < 0.35:
        return rng.choice(
            [None, True, False, rng.randint(-(10**12), 10**12), text(rng), str(Decimal(literal(rng)))]
        )
    if kind < 0.65:
        return [value(rng, depth + 1) for _ in range(rng.randint(0, 4))]
    return {text(rng): value(rng, depth + 1) for _ in range(rng.randint(0, 5))}


def jsons(rng) -> None:
    cases = []
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


def times(rng) -> None:
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
        "1781654400123",
        "1790249583.872436",
        "1781654400",
    ]
    numbers = [1781654400123, 1781654400123456, 1790249583, 1792574749213]
    numbers += [rng.randint(1_600_000_000_000, 1_900_000_000_000) for _ in range(200)]
    numbers += [rng.randint(1_600_000_000_000_000, 1_900_000_000_000_000) for _ in range(100)]
    decimals_ = ["1790249583.872436", "1790249585.0128748", "1688669448.3138"]
    decimals_ += [
        f"{rng.randint(1_600_000_000, 1_900_000_000)}.{rng.randint(0, 9999999):07d}" for _ in range(200)
    ]
    floats = [1781654400.1234567] + [rng.uniform(1.6e9, 1.9e9) for _ in range(100)]
    cases = [{"kind": "str", "input": s, "iso": iso(s)} for s in inputs]
    cases += [{"kind": "int", "input": n, "iso": iso(n)} for n in numbers]
    cases += [{"kind": "decimal", "input": d, "iso": iso(Decimal(d))} for d in decimals_]
    cases += [{"kind": "float", "input": f, "iso": iso(f)} for f in floats]
    precisions = []
    for record, field, aliases in [
        ({"ts": "2026-10-21T09:00:00Z", "ts_prec": "EXACT"}, "ts", []),
        ({"ts": None, "ts_prec": None}, "ts", []),
        ({"ts": None, "ts_prec": "COLLECTION_ONLY"}, "ts", []),
        ({"ts": 1781654400123, "ts_prec": None}, "ts", []),
        ({"ts": "2026-10-21T09:00:00Z", "ts_prec": "BUCKET(300s)", "ts_bucket_size_s": 300}, "ts", []),
        (
            {"ts": "2026-10-21T09:00:00Z", "ts_prec": "BUCKET(300s)", "timestamp_precision": "BUCKET(300s)"},
            "ts",
            ["timestamp_precision"],
        ),
        ({"ts": "2026-10-21T09:00:00Z", "ts_prec": "BUCKET(300s)", "ts_bucket_anchor": "END"}, "ts", []),
        ({"ts": "2026-10-21T09:00:00Z", "ts_prec": "BUCKET(5m)", "ts_bucket_anchor": None}, "ts", []),
        ({"minute_start": "2026-10-21T09:00:00Z", "minute_start_prec": "BUCKET(60s)"}, "minute_start", []),
        ({"as_of": None, "as_of_prec": None, "as_of_prec_reason": "SOURCE_CLOCK_UNAVAILABLE"}, "as_of", []),
        ({"x": "2026-10-21T09:00:00Z", "x_prec": "HOUR"}, "x", []),
        ({"x": "2026-10-21T09:00:00Z", "x_precision": "EXACT"}, "x", []),
    ]:
        stamp = propagate_timestamp(record, field, precision_aliases=aliases)
        precisions.append(
            {"record": record, "field": field, "aliases": aliases, "fields": stamp.fields(field)}
        )
    write("time.json.gz", {"times": cases, "precisions": precisions, "parse_check": iso(parse_time(0))})


def main() -> None:
    rng = random.Random(20261021)
    decimals(rng)
    jsons(rng)
    times(rng)


if __name__ == "__main__":
    main()
