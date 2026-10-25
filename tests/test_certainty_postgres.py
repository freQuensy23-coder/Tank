"""Actual PostgreSQL certainty contract control; no service, native agent or model."""

import os
from pathlib import Path

import pytest
from source_support import fixture_versions

from tank.event_tools import EventToolService
from tank.settings import Settings
from tank.util import clock_iso, new_id, sha256

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


async def test_primary_paraphrase_persists_high_in_postgres(tmp_path: Path, postgres_template) -> None:
    url = os.getenv("TANK_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Requires isolated TANK_TEST_DATABASE_URL")
    async with postgres_template.repository(url) as repo:
        item_id, event_id = new_id("item"), None
        service = EventToolService(repo, Settings(data_dir=tmp_path), None, None, versions=fixture_versions())
        stamp = clock_iso()
        quote = "The SEC announced approval of the application."
        (tmp_path / "source.txt").write_text(quote)
        await repo.put(
            "ingest.source_items",
            item_id,
            dict(
                item_id=item_id,
                source_id="S01",
                source_class="PRIMARY",
                text=quote,
                observed_at=stamp,
                feed="fixture:official",
                external_id=item_id,
                published_prec="COLLECTION_ONLY",
                raw_ref=str(tmp_path / "source.txt"),
                raw_sha256=sha256(quote),
                content_hash=sha256(quote),
                relevance={"decision": "ALWAYS"},
                extraction_status="PENDING",
            ),
        )
        extraction = dict(
            item_id=item_id,
            is_event=True,
            statement="SEC approved the application.",
            certainty="HIGH",
            certainty_reason="Primary regulator confirms its decision",
            entities=[dict(entity_id="sec", role="REGULATOR")],
            token_ids=[],
            event_time=dict(value=None, precision="UNKNOWN", quote=None),
            public_at=dict(value=stamp, precision="EXACT", basis="Primary publication", quote=None),
            freshness="FRESH",
            next_milestone=None,
            checked_sources=[],
            claims=[
                dict(
                    verified=True,
                    type="STATE",
                    subject="SEC",
                    predicate="approved",
                    object="the application",
                    state_claim="APPROVED",
                    directionality="BOTH",
                    evidence_class="PRIMARY_FACT",
                    quote=quote,
                )
            ],
        )
        created = await service.dispatch("record_event", extraction, {})
        event_id = created["event_id"]
        event = await repo.get("evt.events", event_id)
        assert event["certainty"] == "HIGH" and event["state_verified"]
        assert event["certainty_evidence_claims"] == [created["accepted_claims"][0]["claim_id"]]
