"""Fresh import exercises actual SQL policy constraints before any remote allocation."""

import copy
import json
import os
from contextlib import AsyncExitStack

import httpx
import pytest

from tank.replay import run_operational_replay
from tank.settings import Settings
from tank.storage import MemoryRepository
from tank.util import sha256


@pytest.mark.integration
@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_fresh_import_rederives_policy_without_old_classifier_outputs(
    tmp_path, backend, postgres_template
):
    async with AsyncExitStack() as resources:
        if backend == "postgres":
            url = os.getenv("TANK_TEST_DATABASE_URL")
            if not url:
                pytest.skip("Explicit disposable PostgreSQL administrator DSN required")
            repo = await resources.enter_async_context(postgres_template.repository(url))
        else:
            repo = MemoryRepository()
            resources.push_async_callback(repo.close)
        at = "2026-09-21T15:02:00Z"
        cfg = {
            "registry": {},
            "thresholds": {},
            "semantics": {},
            "dependency_map": {},
            "openma": {
                "agent_id": "fixture-agent",
                "environment_id": "live",
                "replay_environment_id": "replay",
            },
        }

        class Config(Settings):
            def load_config(self, name):
                return copy.deepcopy(cfg.get(name, {}))

        settings = Config(
            replay=True,
            data_dir=tmp_path / "runtime",
            config_version="test",
            code_version="test",
            _env_file=None,
        )
        rows = []
        for sid, subtype, role, old in [
            ("S24", None, None, "ALWAYS"),
            ("S30", None, None, "SEND"),
            ("S25", "commits", None, "SKIP"),
            ("S29", None, "PERSON", "SEND"),
            ("S29", None, "ORGANIZATION", "ALWAYS"),
            ("WEB", None, None, "SEND"),
        ]:
            iid = sid + "-" + (role or subtype or "item")
            source = {
                "item_id": iid,
                "source_id": sid,
                "subtype": subtype,
                "author_role": role,
                "source_class": "PRIMARY",
                "feed": "https://fixture.invalid/retained",
                "text": "Retained source",
                "content_hash": sha256("Retained source"),
                "external_id": iid,
                "raw_ref": "retained-fixture",
                "raw_sha256": sha256(b"retained"),
                "published_at": at,
                "published_at_prec": "EXACT",
                "observed_at": at,
                "observed_at_prec": "COLLECTION_ONLY",
                "relevance": {"decision": old, "p_relevant": 0.987, "model": "ORIGINAL-MODEL-SENTINEL"},
                "relevance_decision": old,
                "relevance_p": 0.654,
                "extraction_status": "DONE",
            }
            rows.append(
                {"collection": "ingest.source_items", "key": iid, "available_at": at, "record": source}
            )
        cfg["replay_messages"] = [{"kind": "NEW_ITEM", "item_id": rows[0]["key"], "replay_at": at}]
        packet = {
            "cutoff": at,
            "config_version": "test",
            "code_version": "test",
            "registry": {},
            "config": cfg,
            "records": rows,
            "tool_results": [],
        }
        path = tmp_path / "inputs.json"
        path.write_text(json.dumps(packet))

        class StopBeforeNative(Exception):
            pass

        requests = []

        def stop(request):
            requests.append(request.method)
            raise StopBeforeNative()

        async with httpx.AsyncClient(transport=httpx.MockTransport(stop)) as http:
            with pytest.raises(StopBeforeNative):
                await run_operational_replay(path, repo, settings, http=http)
        assert requests == ["GET"]
        for row in rows:
            stored = await repo.get("ingest.source_items", row["key"])
            assert stored["extraction_status"] == "PENDING"
            assert "relevance_decision" not in stored and "relevance_p" not in stored
            assert "ORIGINAL-MODEL-SENTINEL" not in json.dumps(stored)
            if row["key"] in {"S24-item", "S29-ORGANIZATION"}:
                assert stored["relevance"] == {"decision": "ALWAYS"}
            else:
                assert stored["relevance"]["decision"] is None
                assert stored["relevance"]["pending_reason"]
        assert await repo.list("ops.agent_sessions") == []
