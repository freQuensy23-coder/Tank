"""Provision only the disposable PostgreSQL 16 service used by this CI job."""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from xdist.plugin import pytest_xdist_auto_num_workers


def main() -> None:
    fields = conninfo_to_dict(os.environ["CI_POSTGRES_DSN"])
    if fields.get("dbname") != "tank_test_ci_admin" or fields.get("host") != "127.0.0.1":
        raise ValueError("CI setup requires the loopback tank_test_ci_admin database")
    # Use the actual pinned xdist hook, so -n auto and precreated routes agree.
    workers = pytest_xdist_auto_num_workers(SimpleNamespace(option=SimpleNamespace(numprocesses="auto")))
    if workers < 1:
        raise ValueError("xdist must have at least one worker")
    with psycopg.connect(**fields, autocommit=True) as connection:
        version = connection.info.server_version
        if not 160000 <= version < 170000:
            raise ValueError("CI requires PostgreSQL 16")
        connection.execute(
            "CREATE ROLE tank_agent LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
            "PASSWORD 'tank-ci-agent-disposable'"
        )
        connection.execute("ALTER ROLE tank_agent SET default_transaction_read_only = on")
        routes = {
            "TANK_TEST_DATABASE_URL": "tank_test_ci_main_{worker}",
            "TANK_E2E_DATABASE_URL": "tank_test_ci_e2e_{worker}",
            "TANK_SOURCE_TEST_DATABASE_URL": "tank_test_ci_source_{worker}",
        }
        for template in routes.values():
            for index in range(workers):
                database = template.replace("{worker}", f"gw{index}")
                connection.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    with Path(os.environ["GITHUB_ENV"]).open("a") as output:
        for variable, template in routes.items():
            value = make_conninfo(**{**fields, "dbname": template})
            if "\n" in value or "\r" in value:
                raise ValueError("CI database routes must fit one environment line")
            output.write(f"{variable}={value}\n")
    print(json.dumps({"postgres_major": 16, "workers": workers, "databases": workers * 3}))


if __name__ == "__main__":
    main()
