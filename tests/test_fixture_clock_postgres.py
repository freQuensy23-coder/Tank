"""Exercise the historical clock view and source search through actual storage."""

import os
import uuid
from pathlib import Path
from runpy import run_path

import pytest

from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.storage import PostgresRepository


async def test_reference_clock_view_and_search_with_actual_postgres(tmp_path, monkeypatch):
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict, make_conninfo

    dsn = os.getenv("TANK_TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Requires isolated PostgreSQL QA connection")
    name = "tank_reference_clock_" + uuid.uuid4().hex[:16]
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    repo = None
    created = False
    try:
        await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        created = True
        params = conninfo_to_dict(dsn)
        params["dbname"] = name
        repo = await PostgresRepository(make_conninfo(**params)).open()
        await repo.migrate()
        service = EventToolService(repo, Settings(data_dir=tmp_path), None, None)
        case = run_path(str(Path(__file__).with_name("test_event_tools.py")))[
            "test_trusted_reference_clock_changes_logical_age_only"
        ]
        await case(service, tmp_path, monkeypatch)
    finally:
        if repo is not None:
            await repo.close()
        try:
            if created:
                await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        finally:
            await admin.close()
