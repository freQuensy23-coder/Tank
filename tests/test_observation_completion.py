"""A routed item completes atomically with its accepted observation."""

from pathlib import Path

import pytest
from scheduling_support import RecordingReactionScheduling
from source_support import fixture_http_admission, fixture_versions
from test_session_binding_concurrency import fixture_repo

from tank.analysis.page_access import LivePageAccess
from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.util import clock_iso, new_id, sha256


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
@pytest.mark.parametrize("fail_item_write", [False, True])
async def test_observation_and_source_completion_are_atomic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, backend, fail_item_write, postgres_template
) -> None:
    async with fixture_repo(backend, postgres_template) as repo:
        service = EventToolService(
            repo,
            Settings(data_dir=tmp_path),
            None,
            None,
            versions=fixture_versions(),
            scheduler=RecordingReactionScheduling(),
            pages=LivePageAccess(fixture_http_admission()),
        )
        first, second, stamp = new_id("item"), new_id("item"), clock_iso()
        quote = "Product announced"
        for item_id in (first, second):
            await repo.put(
                "ingest.source_items",
                item_id,
                dict(
                    item_id=item_id,
                    source_id="S01",
                    feed="fixture:official",
                    external_id=item_id,
                    source_class="PRIMARY",
                    text=quote,
                    title=quote,
                    observed_at=stamp,
                    observed_at_prec="COLLECTION_ONLY",
                    published_at=stamp,
                    published_prec="EXACT",
                    raw_ref=str(tmp_path / (item_id + ".txt")),
                    raw_sha256=sha256(quote),
                    content_hash=sha256(quote),
                    relevance={"decision": "ALWAYS"},
                    extraction_status="PENDING",
                ),
            )
        public_at = dict(value=stamp, precision="EXACT", basis="Fixture publication", quote=None)
        origin = await service.dispatch(
            "record_event",
            dict(
                item_id=first,
                is_event=True,
                statement=quote,
                certainty="MEDIUM",
                certainty_reason="Synthetic fixture",
                entities=[],
                token_ids=[],
                event_time=dict(value=None, precision="UNKNOWN", quote=None),
                public_at=public_at,
                freshness="FRESH",
                claims=[],
                next_milestone=None,
                checked_sources=[],
            ),
            {},
        )
        before = await repo.get("evt.events", origin["event_id"])
        original_put = repo.put

        async def put(table, key, row, **kwargs):
            if table == "ingest.source_items" and key == second and fail_item_write:
                raise RuntimeError("Injected source completion write failure")
            return await original_put(table, key, row, **kwargs)

        monkeypatch.setattr(repo, "put", put)
        arguments = dict(
            event_id=origin["event_id"],
            item_id=second,
            link="CORROBORATION",
            claims=[],
            public_at=public_at,
            freshness="FRESH",
        )
        if fail_item_write:
            with pytest.raises(RuntimeError, match="source completion write failure"):
                await service.dispatch("add_observation", arguments, {})
        else:
            result = await service.dispatch("add_observation", arguments, {})
            assert result["link_type"] == "CORROBORATION"
        item = await repo.get("ingest.source_items", second)
        observations = await repo.list("evt.event_observations", filters={"item_id": second})
        assert item["extraction_status"] == ("PENDING" if fail_item_write else "DONE")
        assert len(observations) == (0 if fail_item_write else 1)
        if fail_item_write:
            assert await repo.get("evt.events", origin["event_id"]) == before
        else:
            assert observations[0]["link_type"] == "CORROBORATION"
        assert not await repo.list("out.alerts", filters={"event_id": origin["event_id"]})
