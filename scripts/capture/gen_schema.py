"""Export tank.market_schema to capture/internal/schema/market_schema.json.

tank-capture embeds this file, so its writer encodes exactly the typed market columns
the Python service writes; tests/test_capture_parity.py fails when the file is stale.
Run: uv run python scripts/capture/gen_schema.py [--check]
"""

import json
import sys
from pathlib import Path

from tank.market_schema import (
    MARKET_ALIASES,
    MARKET_COLUMNS,
    MARKET_IDENTITIES,
    MARKET_REQUIRED,
)

OUT = Path(__file__).resolve().parents[2] / "capture" / "internal" / "schema" / "market_schema.json"


def render():
    payload = {
        "columns": {
            name: [[column, kind] for column, kind in columns.items()]
            for name, columns in MARKET_COLUMNS.items()
        },
        "aliases": MARKET_ALIASES,
        "identities": {name: list(identity) for name, identity in MARKET_IDENTITIES.items()},
        "required": MARKET_REQUIRED,
    }
    return json.dumps(payload, indent=1, sort_keys=True) + "\n"


if __name__ == "__main__":
    text = render()
    if "--check" in sys.argv:
        sys.exit(0 if OUT.read_text() == text else 1)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text)
