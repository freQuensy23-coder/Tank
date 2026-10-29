import json
import os
from copy import deepcopy

import pytest
from psycopg.types.json import Jsonb

from tank.storage import ImmutableRecordError

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


async def test_record_replacement_transfers_only_changed_document_fields(postgres_template, monkeypatch):
    async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo:
        original = {
            "token_id": "T",
            "symbol": "T",
            "retained": {"items": ["x" * 50000]},
            "remove_me": "old",
            "numeric": 1,
            "boolean": 0,
        }
        await repo.put("reg.tokens", "T", original)
        updated = dict(original, symbol="U", numeric=1.0, boolean=False)
        updated.pop("remove_me")
        sent = []
        connect = repo._conn

        class Connection:
            def __init__(self, connection):
                self.connection = connection

            def __getattr__(self, name):
                return getattr(self.connection, name)

            async def execute(self, query, params=None, **kwargs):
                sent.extend(deepcopy(value.obj) for value in params or () if isinstance(value, Jsonb))
                return await self.connection.execute(query, params, **kwargs)

        monkeypatch.setattr(repo, "_conn", lambda: Connection(connect()))
        returned = await repo.put("reg.tokens", "T", updated)
        stored = await repo.get("reg.tokens", "T")
        assert stored == returned == updated
        assert type(stored["numeric"]) is float and type(stored["boolean"]) is bool
        assert stored["symbol"] == "U" and "remove_me" not in stored
        assert sent and max(len(json.dumps(value)) for value in sent) < 1000
        returned["retained"]["items"].clear()
        assert (await repo.get("reg.tokens", "T"))["retained"] == original["retained"]
        await repo.put("reg.tokens", "T", updated, immutable=True)
        with pytest.raises(ImmutableRecordError):
            await repo.put("reg.tokens", "T", dict(updated, symbol="V"))
