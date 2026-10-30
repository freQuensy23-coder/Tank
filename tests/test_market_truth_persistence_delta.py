import json
import os
from copy import deepcopy
from enum import Enum

import pytest
from psycopg.types.json import Jsonb

from tank import storage
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

        copies = []
        original_copy = storage.copy.deepcopy

        def observed_copy(value, memo=None):
            if type(value) is dict and value.get("token_id") == "T":
                copies.append(value)
            return original_copy(value, memo)

        monkeypatch.setattr(repo, "_conn", lambda: Connection(connect()))
        with monkeypatch.context() as patch:
            patch.setattr(storage.copy, "deepcopy", observed_copy)
            returned = await repo.put("reg.tokens", "T", updated)
        assert not copies
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


@pytest.mark.parametrize("depth", [2, 80])
async def test_new_json_record_is_detached_without_a_second_copy(postgres_template, monkeypatch, depth):
    leaf = [{"v": [1.0, False]}]
    nested = leaf
    for _ in range(depth):
        nested = {"nested": nested}
    record = {"token_id": "COPY", "symbol": "COPY", "copy_probe": True, "nested": nested}
    expected = deepcopy(record)
    original_copy, copies = storage.copy.deepcopy, []

    def observed_copy(value, memo=None):
        if type(value) is dict and value.get("copy_probe") is True:
            copies.append(value)
        return original_copy(value, memo)

    async with (
        postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo,
        repo.lock("record-copy-test"),
    ):
        with monkeypatch.context() as patch:
            patch.setattr(storage.copy, "deepcopy", observed_copy)
            returned = await repo.put("reg.tokens", "COPY", record)
        assert len(copies) == (1 if depth >= 64 else 0)
        leaf[0]["v"].append("input")
        assert returned == expected
        returned["nested"].clear()
        assert await repo.get("reg.tokens", "COPY") == expected


async def test_record_copy_fallback_preserves_conversion_and_mutable_enum_callbacks(postgres_template):
    calls = []

    class Meta(type):
        def __eq__(cls, other):
            calls.append("class")
            return type.__eq__(cls, other)

        __hash__ = type.__hash__

    class Model(metaclass=Meta):
        def model_dump(self, *, mode):
            calls.append("model")
            return {"mode": mode}

    class Mutable(list):
        def __deepcopy__(self, memo):
            calls.append("copy")
            return Mutable(self)

    class Status(Enum):
        VALUE = Mutable(["kept"])

    class Key:
        def __str__(self):
            calls.append("key")
            return "extra"

    record = {"token_id": "CUSTOM", "symbol": "CUSTOM", "model": Model(), Key(): 7, "enum": Status.VALUE}
    expected = deepcopy(storage.jsonable(record))
    expected_calls = list(calls)
    assert [value for value in calls if value != "class"] == ["model", "key", "copy"]
    calls.clear()
    async with (
        postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo,
        repo.lock("record-copy-fallback-test"),
    ):
        returned = await repo.put("reg.tokens", "CUSTOM", record)
        assert calls == expected_calls and returned == expected
        assert type(returned["enum"]) is Mutable
        calls.clear()
        returned = await repo.put("reg.tokens", "CUSTOM", record)
        assert calls == expected_calls and returned == expected
        Status.VALUE.value.append("input")
        assert returned["enum"] == ["kept"]
        returned["enum"].clear()
        stored = await repo.get("reg.tokens", "CUSTOM")
        assert stored["enum"] == ["kept"] and stored["model"] == {"mode": "python"}
        assert stored["extra"] == 7


@pytest.mark.parametrize("drift", [False, True])
@pytest.mark.parametrize("result_kind", ["object", "null", "missing"])
async def test_jsonb_projections_are_rebuilt_without_resending_unchanged_payloads(
    postgres_template, monkeypatch, drift, result_kind
):
    async with (
        postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as repo,
        repo.lock("projection-delta-test"),
    ):
        job = await repo.enqueue(
            "RUN_MARKET_TRUTH",
            {"retained": ["x" * 50000], "typed": [1.0, False]},
            "projection-delta-test",
            priority=1,
        )
        job["result"] = {"retained": ["y" * 30000]}
        await repo.put("ops.jobs", job["job_id"], job)
        updated = dict(job, priority=2)
        if result_kind == "null":
            updated["result"] = None
        elif result_kind == "missing":
            updated.pop("result")
        if drift:
            await repo._conn().execute(
                "UPDATE ops.jobs SET payload=%s, result=%s WHERE storage_key=%s",
                (Jsonb({"drift": True}), Jsonb({"drift": True}), job["job_id"]),
            )
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

        with monkeypatch.context() as patch:
            patch.setattr(repo, "_conn", lambda: Connection(connect()))
            returned = await repo.put("ops.jobs", job["job_id"], updated)
        assert sent and max(len(json.dumps(value)) for value in sent) < 1000
        stored = await repo.get("ops.jobs", job["job_id"])
        typed = await (
            await repo._conn().execute(
                "SELECT payload, result, priority, result IS NULL AS result_is_sql_null "
                "FROM ops.jobs WHERE storage_key=%s",
                (job["job_id"],),
            )
        ).fetchone()
        assert returned == stored == updated
        assert typed["payload"] == updated["payload"]
        assert typed["result"] == updated.get("result")
        assert typed["result_is_sql_null"] == (result_kind != "object")
        assert typed["priority"] == updated["priority"]
        assert type(typed["payload"]["typed"][0]) is float
        assert type(typed["payload"]["typed"][1]) is bool
