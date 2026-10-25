"""Parity harness: tank-capture's Go ports against the goldens of the retired Python capture.

Every recorded fixture runs through `capture-parity` (capture/cmd/capture-parity):
capture/testdata/frames/*.jsonl.gz (real public WebSocket frames) and
capture/testdata/rest/*.json.gz (REST poll cycles, as `capture-parity rest FIXTURE --api-policy config/api_limits.yaml`). The
full output must equal the golden the Python reference produced before it was removed
(scripts/capture/goldens, scripts/capture/gen_goldens.py): normalized trades, typed rows,
flow bars, semantic fingerprints, stream watermarks and errors.

  --go BINARY   run the Go ports and diff their full outputs against the goldens
  --check       the goldens still hash to the committed capture/testdata/parity digests
  --only NAME   one fixture (golden name prefix)

Run: uv run python scripts/capture/parity.py --check --go capture/bin/capture-parity
"""

import argparse
import gzip
import hashlib
import json
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

from pydantic import Field, JsonValue

from tank.contracts import DomainModel
from tank.util import canonical_json

ROOT = Path(__file__).resolve().parents[2]
FRAMES = ROOT / "capture" / "testdata" / "frames"
REST = ROOT / "capture" / "testdata" / "rest"
DIGESTS = ROOT / "capture" / "testdata" / "parity"
GOLDENS = Path(__file__).resolve().parent / "goldens"


class FixtureCase(DomainModel):
    name: str = Field(min_length=1)
    arguments: tuple[str, ...]


class ParityRow(DomainModel):
    collection: str = Field(min_length=1)
    key: str = Field(min_length=1)
    row: dict[str, JsonValue]


class ParityCounts(DomainModel):
    trades: int
    rows: int
    bars: int
    errors: int
    fingerprints: int
    watermarks: int


class ParityOutput(DomainModel):
    trades: list[dict[str, JsonValue]]
    rows: list[ParityRow]
    bars: list[dict[str, JsonValue]]
    errors: list[list[JsonValue]]
    fingerprints: dict[str, str]
    watermarks: dict[str, JsonValue]

    def counts(self) -> ParityCounts:
        return ParityCounts(
            trades=len(self.trades),
            rows=len(self.rows),
            bars=len(self.bars),
            errors=len(self.errors),
            fingerprints=len(self.fingerprints),
            watermarks=len(self.watermarks),
        )


class RowDigest(DomainModel):
    collection: str
    key: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ParityDigests(DomainModel):
    trades: list[str]
    rows: list[RowDigest]
    bars: list[str]
    fingerprints: dict[str, str]
    watermarks: dict[str, JsonValue]
    errors: list[list[JsonValue]]


class Arguments(DomainModel):
    check: bool
    go: Path | None
    only: str | None = Field(min_length=1)


def fixtures() -> list[FixtureCase]:
    tracked = subprocess.check_output(
        ["git", "-C", str(ROOT), "ls-files", "-z", "--", "capture/testdata/parity"]
    ).decode()
    expected = {Path(name).name for name in tracked.split("\0") if name and name.endswith(".json.gz")}
    if not expected:
        raise ValueError("Tracked parity fixture inventory must not be empty")
    cases = [
        FixtureCase(name=path.name.removesuffix(".jsonl.gz") + ".json.gz", arguments=(str(path),))
        for path in sorted(FRAMES.glob("*.jsonl.gz"))
    ] + [
        FixtureCase(
            name="rest-" + path.name,
            arguments=("rest", str(path), "--api-policy", str(ROOT / "config" / "api_limits.yaml")),
        )
        for path in sorted(REST.glob("*.json.gz"))
    ]
    actual = {case.name for case in cases}
    if actual != expected or len(cases) != len(expected):
        raise ValueError(
            f"Parity fixture inventory differs: missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )
    for name in expected:
        if not (GOLDENS / name).is_file() or not (DIGESTS / name).is_file():
            raise ValueError(f"Missing retained parity evidence: {name}")
    return cases


def load[T: DomainModel](path: Path, schema: type[T]) -> T:
    return schema.model_validate_json(gzip.decompress(path.read_bytes()))


def digest(value: JsonValue) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def digests(output: ParityOutput) -> ParityDigests:
    return ParityDigests(
        trades=[digest(trade) for trade in output.trades],
        rows=[
            RowDigest(collection=row.collection, key=row.key, sha256=digest(row.row)) for row in output.rows
        ],
        bars=[digest(bar) for bar in output.bars],
        fingerprints=output.fingerprints,
        watermarks=output.watermarks,
        errors=output.errors,
    )


def diagnostic(value: JsonValue | ParityRow) -> str:
    if isinstance(value, ParityRow):
        return value.model_dump_json()
    return json.dumps(value, sort_keys=True)


def compare_sequence[T: JsonValue | ParityRow](
    section: str, left: Sequence[T], right: Sequence[T]
) -> str | None:
    if len(left) != len(right):
        return f"{section}: {len(left)} golden vs {len(right)} go"
    for index, (expected, observed) in enumerate(zip(left, right, strict=True)):
        if expected != observed:
            return f"{section}[{index}]:\ngolden {diagnostic(expected)}\ngo     {diagnostic(observed)}"
    return None


def compare(golden: ParityOutput, observed: ParityOutput) -> str | None:
    for mismatch in (
        compare_sequence("trades", golden.trades, observed.trades),
        compare_sequence("rows", golden.rows, observed.rows),
        compare_sequence("bars", golden.bars, observed.bars),
        compare_sequence("errors", golden.errors, observed.errors),
    ):
        if mismatch is not None:
            return mismatch
    if golden.fingerprints != observed.fingerprints:
        return "fingerprints differ"
    if golden.watermarks != observed.watermarks:
        return "watermarks differ"
    return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--go", type=Path)
    parser.add_argument("--only")
    args = Arguments.model_validate(vars(parser.parse_args()))
    if not args.check and args.go is None:
        parser.error("use --check or --go to select actual verification")
    cases = fixtures()
    if args.only is not None:
        cases = [case for case in cases if case.name.startswith(args.only)]
        if not cases:
            parser.error("fixture selector did not match any retained case")
    failures = 0
    for case in cases:
        golden = load(GOLDENS / case.name, ParityOutput)
        failed = False
        if args.check and digests(golden) != load(DIGESTS / case.name, ParityDigests):
            failed = True
            print("GOLDEN DRIFT", case.name, file=sys.stderr)
        if args.go is not None:
            result = subprocess.run([str(args.go), *case.arguments], check=True, capture_output=True)
            mismatch = compare(golden, ParityOutput.model_validate_json(result.stdout))
            if mismatch is not None:
                failed = True
                print("GO MISMATCH", case.name, mismatch, file=sys.stderr)
        failures += failed
        print(case.name, golden.counts(), "FAILED" if failed else "ok")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
