from pathlib import Path
from typing import Literal

import httpx
import pytest
import zstandard
from pydantic import Field, JsonValue
from pydantic_settings import SettingsConfigDict
from source_support import fixture_request_budgets, fixture_versions
from test_storage import repo as repo

from tank.contracts import DomainModel
from tank.contracts.sources import SourceDefinition
from tank.settings import Settings
from tank.sources import SourceService
from tank.storage import StorageBackend
from tank.util import sha256

FIXTURES = Path(__file__).parent / "fixtures/source-legacy-pending"


class OldProducerWitness(DomainModel):
    producer_commit: Literal["d17a24f80b98d0286861701f693a3b1b7dcb3e01"]
    source: SourceDefinition
    checkpoint: dict[str, JsonValue]
    index_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class DetailSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@pytest.mark.parametrize(
    "fixture_name,fixture_sha256,publication",
    [
        (
            "producer-d17.json",
            "21e12ed7ae785490290513368d9a3e527c25eb75a53562c2053df861b7dba001",
            "2026-10-01T12:00:00Z",
        ),
        (
            "producer-d17.json",
            "21e12ed7ae785490290513368d9a3e527c25eb75a53562c2053df861b7dba001",
            "2026-10-01",
        ),
        ("producer-d17-known.json", "bcb2b21ffbaac9bafce06041db9cd2d08968c943bc79293a281633f12890c176", None),
    ],
)
async def test_actual_old_pending_publication_uses_only_the_available_detail_evidence(
    repo: StorageBackend, tmp_path: Path, fixture_name: str, fixture_sha256: str, publication: str | None
) -> None:
    fixture = FIXTURES / fixture_name
    original = fixture.read_bytes()
    assert sha256(original) == fixture_sha256
    witness = OldProducerWitness.model_validate_json(original)
    await repo.put("ops.source_checkpoints", "S01:0", witness.checkpoint)
    calls: list[str] = []
    payload = (
        '<main><h1>Actual release</h1><div class="field--name-body"><p>'
        "Bitcoin actual authored release body.</p></div></main>"
        + (
            '<meta property="article:published_time" content="' + publication + '">'
            if publication is not None
            else ""
        )
    ).encode()

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        assert str(request.url) == "https://source.example/article"
        return httpx.Response(200, content=payload)

    budgets = fixture_request_budgets()
    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
            service = SourceService(
                repo,
                DetailSettings(data_dir=tmp_path, ops_progress_enabled=False),
                http=http,
                budgets=budgets,
                versions=fixture_versions(),
            )
            service.sources = {witness.source.source_id: witness.source}
            try:
                restored = await service.poller.pending(witness.source, "S01:0", witness.checkpoint)
                assert len(restored.batches) == 1
                old_rows = restored.batches[0].storage_record()["rows"]
                assert isinstance(old_rows, list) and len(old_rows) == 1
                old = old_rows[0]
                assert isinstance(old, dict) and "pending_phase" not in old
                if publication is not None:
                    assert old["published_prec_reason"] == "INCOMPLETE" and old["published_at"] is None
                else:
                    assert old["published_at"] == "2026-10-16T12:00:00Z"
                completed = await service.poller.drain(witness.source, 0, force=True)
                assert completed.items == 1 and completed.errors == []
                assert (await service.poller.drain(witness.source, 0, force=True)).items == 0
            finally:
                await service.close()
        assert calls == ["https://source.example/article"]
        [stored] = await repo.list("ingest.source_items")
        assert stored["item_id"] == old["item_id"] and stored["observed_at"] == old["observed_at"]
        assert stored["discovery_config_version"] == old["discovery_config_version"]
        assert stored["discovery_code_version"] == old["discovery_code_version"]
        if publication is not None:
            assert stored["published_at"] == (publication if "T" in publication else "2026-10-01T00:00:00Z")
            assert stored["published_at_precision"] == ("EXACT" if "T" in publication else "BUCKET(1d)")
            assert stored["published_prec_reason"] is None and stored.get("published_at_reason") is None
            assert stored.get("published_at_missing_reason") is None
            assert stored["published_at_basis"] == "ARTICLE_PUBLICATION_META"
            assert stored.get("published_at_bucket_size_s") == (None if "T" in publication else 86400)
        else:
            for field in (
                "published_at",
                "published_prec",
                "published_at_precision",
                "published_prec_reason",
                "published_at_reason",
                "published_at_missing_reason",
                "published_at_basis",
                "published_at_bucket_size_s",
                "published_at_bucket_anchor",
            ):
                assert stored.get(field) == old.get(field)
        assert stored["raw_sha256"] == sha256(payload)
        raw_ref = stored["raw_ref"]
        assert isinstance(raw_ref, str)
        assert zstandard.ZstdDecompressor().decompress(Path(raw_ref).read_bytes()) == payload
        final = await repo.get("ops.source_checkpoints", "S01:0")
        assert final is not None and final["pending_batches"] == [] and final["etag"] == '"actual-index"'
        assert fixture.read_bytes() == original
    finally:
        await budgets.shutdown()
