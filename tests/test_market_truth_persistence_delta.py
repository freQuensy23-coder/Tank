import json
import os
from copy import deepcopy

import pytest
from psycopg.types.json import Jsonb

from tank.storage import ImmutableRecordError

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


@pytest.mark.parametrize("intervening_write", [False, True])
async def test_record_replacement_transfers_only_changed_document_fields(
    postgres_template, monkeypatch, intervening_write
):
    async with (
        postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo,
        repo.lock("record-replacement-test"),
    ):
        original = {
            "token_id": "T",
            "symbol": "T",
            "retained": {"items": ["x" * 50000]},
            "remove_me": "old",
            "numeric": 1,
            "boolean": 0,
            "nested": {"values": [1, 0]},
            "null_to_missing": None,
        }
        await repo.put("reg.tokens", "T", original)
        updated = dict(
            original, symbol="U", numeric=1.0, boolean=False, nested={"values": [1.0, False]}, added_null=None
        )
        updated.pop("remove_me")
        updated.pop("null_to_missing")
        sent = []
        intervened = []
        revised = []
        connect = repo._conn

        class Connection:
            def __init__(self, connection):
                self.connection = connection

            def __getattr__(self, name):
                return getattr(self.connection, name)

            async def execute(self, query, params=None, **kwargs):
                text = query.as_string(self.connection) if hasattr(query, "as_string") else query
                if intervening_write and not revised and text.startswith("SELECT document, immutable,"):
                    await self.connection.execute(
                        "UPDATE reg.tokens SET document=document WHERE storage_key=%s", ("T",)
                    )
                    revised.append(True)
                if intervening_write and not intervened and text.startswith('UPDATE "reg"."tokens" SET'):
                    await self.connection.execute(
                        "UPDATE reg.tokens SET document = document || %s WHERE storage_key=%s",
                        (Jsonb({"intervening_only": True}), "T"),
                    )
                    intervened.append(True)
                sent.extend(deepcopy(value.obj) for value in params or () if isinstance(value, Jsonb))
                return await self.connection.execute(query, params, **kwargs)

        monkeypatch.setattr(repo, "_conn", lambda: Connection(connect()))
        returned = await repo.put("reg.tokens", "T", updated)
        stored = await repo.get("reg.tokens", "T")
        assert stored == returned == updated
        assert type(stored["numeric"]) is float and type(stored["boolean"]) is bool
        assert type(stored["nested"]["values"][0]) is float
        assert type(stored["nested"]["values"][1]) is bool
        assert stored["symbol"] == "U" and "remove_me" not in stored and "null_to_missing" not in stored
        assert "added_null" in stored and stored["added_null"] is None
        assert sent
        if intervening_write:
            assert intervened and "intervening_only" not in stored
            assert max(len(json.dumps(value)) for value in sent) > 50000
        else:
            assert max(len(json.dumps(value)) for value in sent) < 1000
        returned["retained"]["items"].clear()
        assert (await repo.get("reg.tokens", "T"))["retained"] == original["retained"]
        await repo.put("reg.tokens", "T", updated, immutable=True)
        with pytest.raises(ImmutableRecordError):
            await repo.put("reg.tokens", "T", dict(updated, symbol="V"))
