"""Resolve container database credentials without putting secrets in image/config."""

import os
import sys
from pathlib import Path
from urllib.parse import quote


def read(name):
    path = Path("/run/secrets") / name
    value = path.read_text().strip()
    if not value:
        raise RuntimeError(f"Empty required secret: {name}")
    return value


def main():
    if "TANK_DATABASE_URL" not in os.environ:
        os.environ["TANK_DATABASE_URL"] = (
            "postgresql://tank:" + quote(read("tank_db_password"), safe="") + "@postgres:5432/tank"
        )
    if "TANK_OPENMA_DATABASE_URL" not in os.environ:
        os.environ["TANK_OPENMA_DATABASE_URL"] = (
            "postgresql://oma:" + quote(read("oma_db_password"), safe="") + "@postgres:5432/oma"
        )
    os.execvp(sys.argv[1], sys.argv[1:])


if __name__ == "__main__":
    main()
