"""Parity harness: tank-capture's Go ports against the goldens of the retired Python capture.

Every recorded fixture runs through `capture-parity` (capture/cmd/capture-parity):
capture/testdata/frames/*.jsonl.gz (real public WebSocket frames) and
capture/testdata/rest/*.json.gz (REST poll cycles, as `capture-parity rest FIXTURE`). The
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
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FRAMES = ROOT / "capture" / "testdata" / "frames"
REST = ROOT / "capture" / "testdata" / "rest"
DIGESTS = ROOT / "capture" / "testdata" / "parity"
GOLDENS = Path(__file__).resolve().parent / "goldens"


def fixtures():
    """(golden name, capture-parity arguments) for every recorded fixture."""
    frames = [
        (path.name.removesuffix(".jsonl.gz") + ".json.gz", [str(path)])
        for path in sorted(FRAMES.glob("*.jsonl.gz"))
    ]
    rest = [("rest-" + path.name, ["rest", str(path)]) for path in sorted(REST.glob("*.json.gz"))]
    return frames + rest


def load(path):
    return json.loads(gzip.decompress(path.read_bytes()))


def digest(value):
    from tank.util import canonical_json

    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def digests(out):
    return {
        "trades": [digest(t) for t in out["trades"]],
        "rows": [
            {"collection": r["collection"], "key": r["key"], "sha256": digest(r["row"])} for r in out["rows"]
        ],
        "bars": [digest(b) for b in out["bars"]],
        "fingerprints": out["fingerprints"],
        "watermarks": out["watermarks"],
        "errors": out["errors"],
    }


def compare(golden, go):
    for section in ("trades", "rows", "bars", "errors"):
        left, right = golden[section], go.get(section, [])
        if len(left) != len(right):
            return f"{section}: {len(left)} golden vs {len(right)} go"
        for i, (a, b) in enumerate(zip(left, right, strict=True)):
            if a != b:
                return f"{section}[{i}]:\ngolden {json.dumps(a, sort_keys=True)}\ngo     {json.dumps(b, sort_keys=True)}"
    for section in ("fingerprints", "watermarks"):
        if golden[section] != go.get(section):
            return f"{section} differ"
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--go")
    parser.add_argument("--only")
    args = parser.parse_args()
    failures = 0
    for name, arguments in fixtures():
        if args.only and not name.startswith(args.only):
            continue
        golden = load(GOLDENS / name)
        failed = False
        if args.check and json.loads(json.dumps(digests(golden))) != load(DIGESTS / name):
            failed = True
            print("GOLDEN DRIFT", name, file=sys.stderr)
        if args.go:
            go = json.loads(subprocess.run([args.go, *arguments], check=True, capture_output=True).stdout)
            mismatch = compare(golden, go)
            if mismatch:
                failed = True
                print("GO MISMATCH", name, mismatch, file=sys.stderr)
        failures += failed
        print(name, {k: len(v) for k, v in golden.items()}, "FAILED" if failed else "ok")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
