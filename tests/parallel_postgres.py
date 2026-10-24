"""Opt-in xdist routing for precreated, isolated PostgreSQL test databases.

Use PYTHONPATH=tests pytest -p parallel_postgres -n auto --dist loadfile.
Each configured DSN must contain one literal {worker} in its tank_test dbname.
The controller preserves templates; workers only replace their own dbname.
This plugin never connects, creates databases, or loads credential files.
"""

import os
import re
from collections.abc import Mapping, MutableMapping, Sequence

import pytest
from psycopg import ProgrammingError
from psycopg.conninfo import conninfo_to_dict, make_conninfo

DATABASE_VARIABLES = (
    "TANK_TEST_DATABASE_URL",
    "TANK_E2E_DATABASE_URL",
    "TANK_SOURCE_TEST_DATABASE_URL",
)
_WORKER = re.compile(r"gw[0-9]+")
_DATABASE = re.compile(r"tank_test[A-Za-z0-9_]*\{worker\}[A-Za-z0-9_]*")


def resolve_test_databases(environment: Mapping[str, str], worker_id: str | None) -> dict[str, str]:
    """Validate every configured route before returning any environment updates."""
    if worker_id is not None and _WORKER.fullmatch(worker_id) is None:
        raise ValueError("worker ID must match gw[0-9]+")
    updates = {}
    for variable in DATABASE_VARIABLES:
        value = environment.get(variable)
        if not value:
            continue
        try:
            fields = conninfo_to_dict(value)
        except (ProgrammingError, ValueError):
            raise ValueError(f"{variable}: invalid libpq DSN") from None
        database = fields.get("dbname")
        if not isinstance(database, str):
            raise ValueError(f"{variable} requires a database name")
        if _DATABASE.fullmatch(database) is None:
            raise ValueError(f"{variable}: expected tank_test dbname with one {{worker}}")
        for field, content in fields.items():
            if field != "dbname" and isinstance(content, str) and ("{" in content or "}" in content):
                raise ValueError(f"{variable}: placeholders allowed only in dbname")
        if worker_id is None:
            updates[variable] = value
        else:
            updates[variable] = make_conninfo(value, dbname=database.replace("{worker}", worker_id))
    return updates


def configure_environment(environment: MutableMapping[str, str], worker_id: str | None) -> None:
    """Do not partially rewrite the environment if any configured DSN is unsafe."""
    updates = resolve_test_databases(environment, worker_id)
    environment.update(updates)


def parallel_requested(args: Sequence[str]) -> bool:
    """Only the controller checks -n; worker invocations may omit that option."""
    workers = None
    for index, arg in enumerate(args):
        if arg in {"-n", "--numprocesses"}:
            workers = args[index + 1] if index + 1 < len(args) else ""
        elif arg.startswith("--numprocesses="):
            workers = arg.partition("=")[2]
        elif arg.startswith("-n") and len(arg) > 2:
            workers = arg[2:]
    return workers == "auto" or bool(workers and re.fullmatch(r"[1-9][0-9]*", workers))


@pytest.hookimpl(tryfirst=True)
def pytest_load_initial_conftests(
    early_config: pytest.Config, parser: pytest.Parser, args: list[str]
) -> None:
    """Run before conftest reads or validates inherited test database routes."""
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if worker_id is None and not parallel_requested(args):
        raise pytest.UsageError("parallel_postgres requires -n auto or a positive worker count")
    try:
        configure_environment(os.environ, worker_id)
    except ValueError as error:
        raise pytest.UsageError(f"parallel_postgres: {error}") from None
