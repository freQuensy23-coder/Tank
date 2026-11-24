"""Retention of nightly backup snapshots.

The spec fixes no count (HLD §8: nightly pg_dump + bundles/). Lead ruling 2026-10-20:
keep the last `backup_snapshots_kept` verified snapshots (3 while the backup shares
the production disk, 7 once it has its own disk).
"""

import os
import re
import shutil
from os import PathLike
from pathlib import Path
from urllib.parse import unquote, urlsplit

SNAPSHOT = re.compile(r"tank-\d{4}-\d{2}-\d{2}")


def dump_environment(url: str, database: str, socket: Path | None) -> dict[str, str]:
    if socket is not None:
        return {
            "PATH": os.environ.get("PATH", os.defpath),
            "PGHOST": str(socket),
            "PGUSER": "tank_backup",
            "PGDATABASE": database,
        }
    connection = urlsplit(url)
    env = os.environ.copy()
    for key, value in {
        "PGHOST": connection.hostname,
        "PGPORT": connection.port,
        "PGUSER": unquote(connection.username or ""),
        "PGPASSWORD": unquote(connection.password or ""),
        "PGDATABASE": unquote(connection.path.lstrip("/")),
    }.items():
        if value:
            env[key] = str(value)
        else:
            env.pop(key, None)
    return env


def prune_backup_snapshots(target: str | PathLike[str], keep: int) -> list[str]:
    """Remove complete `tank-YYYY-MM-DD` snapshots older than the newest `keep`.

    Only directories that `Scheduler.maintenance` completed (`COMPLETE` present) count
    and are removed; partial staging and anything else in the directory is left alone.
    """
    if type(keep) is not int or keep < 1:
        raise ValueError("backup_snapshots_kept must be a positive integer")
    complete = sorted(
        path
        for path in Path(target).iterdir()
        if path.is_dir()
        and not path.is_symlink()
        and SNAPSHOT.fullmatch(path.name)
        and (path / "COMPLETE").is_file()
    )
    removed = complete[:-keep]
    for path in removed:
        shutil.rmtree(path)
    return [path.name for path in removed]
