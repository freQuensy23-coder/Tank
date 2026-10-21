"""Provision only the disposable PostgreSQL 16 service used by this CI job."""

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import psycopg
from postgres_helpers import prepare_schema_template
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from xdist.plugin import pytest_xdist_auto_num_workers


def main() -> None:
    fields = conninfo_to_dict(os.environ["CI_POSTGRES_DSN"])
    if fields.get("dbname") != "tank_test_ci_admin" or fields.get("host") != "127.0.0.1":
        raise ValueError("CI setup requires the loopback tank_test_ci_admin database")

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
    seed_name = f"tank_test_ci_schema_{uuid4().hex}"
    schema_template = asyncio.run(prepare_schema_template(make_conninfo(**fields), seed_name))
    if schema_template != seed_name:
        raise ValueError("schema preparation returned an unexpected database name")
    with Path(os.environ["GITHUB_ENV"]).open("a") as output:
        for variable, template in routes.items():
            value = make_conninfo(**{**fields, "dbname": template})
            if "\n" in value or "\r" in value:
                raise ValueError("CI database routes must fit one environment line")
            output.write(f"{variable}={value}\n")
        output.write(f"TANK_CI_SCHEMA_TEMPLATE={schema_template}\n")
    print(
        json.dumps(
            {
                "postgres_major": 16,
                "workers": workers,
                "worker_databases": workers * 3,
                "schema_template": schema_template,
            }
        )
    )


if __name__ == "__main__":
    main()
