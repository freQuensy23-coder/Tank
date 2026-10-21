"""Golden outputs of the retired Python market capture, for tank-capture's parity checks.

The core no longer captures market data: tank-capture (capture/) is the only writer. Its
parity harness compares the Go ports with these goldens, the full outputs the Python
reference (tank.market streams, normalize, OrderBook, FlowAccumulator, MarketService
pollers) produced for every recorded fixture before that code was deleted:

  scripts/capture/goldens/<fixture>.json.gz       WebSocket frames (capture/testdata/frames)
  scripts/capture/goldens/rest-<fixture>.json.gz  REST poll cycles (capture/testdata/rest)

This script runs only at the commit that still has the Python reference: the commit that
added it. To regenerate (e.g. for a new fixture), check that commit out in a worktree and
run `uv run python scripts/capture/gen_goldens.py` there, then copy the goldens back.
"""

import asyncio
import gzip
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import parity  # noqa: E402
import rest_parity  # noqa: E402

GOLDENS = HERE / "goldens"


def write(name, out):
    GOLDENS.mkdir(parents=True, exist_ok=True)
    text = json.dumps(json.loads(json.dumps(out)), sort_keys=True, separators=(",", ":")) + "\n"
    (GOLDENS / name).write_bytes(gzip.compress(text.encode(), mtime=0))


def main():
    for fixture in parity.fixtures():
        write(fixture.name.removesuffix(".jsonl.gz") + ".json.gz", asyncio.run(parity.run(fixture)))
        print(fixture.name)
    for fixture in sorted(rest_parity.REST.glob("*.json.gz")):
        write("rest-" + fixture.name, asyncio.run(rest_parity.run(fixture)))
        print("rest", fixture.name)


if __name__ == "__main__":
    main()
