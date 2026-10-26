"""Notice reconciliation must admit the actual stored delivery authority."""

from pathlib import Path

import httpx
import pytest
from pydantic import JsonValue
from source_support import fixture_versions, source_definition
from test_storage import repo as repo
from test_telegram_preflight import PreflightSettings

from tank.contracts.source_items import ArticleDiscovery, CapturedItem, CaptureEvidence, SourceItem
from tank.ops_progress import DisabledSourceProgress
from tank.sources.ingestion import SourceIngestion
from tank.sources.raw_store import RawItemStorage, write_raw_item
from tank.storage import MemoryRepository, StorageBackend
from tank.telegram_inbox import ANSWER_CHANNEL, TelegramInbox, question_id_for
from tank.time_precision import collection_clock
from tank.util import clock_iso


async def invalid_notice_receipt(repo: StorageBackend, tmp_path: Path, fault: str) -> None:

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Invalid retained receipt cannot authorize a new POST")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(repo, PreflightSettings(data_dir=tmp_path, telegram_token=None), http=http)
        await inbox.store(
            {
                "chat": {"id": 42, "type": "private"},
                "message_id": 17,
                "date": 1800000000,
                "text": "Actual input",
            },
            1,
        )
        row = await repo.get("ops.telegram_messages", "42:17")
        assert row is not None
        receipt: dict[str, JsonValue] = {
            "question_id": question_id_for("42", 17),
            "channel": ANSWER_CHANNEL,
            "purpose": "NOTICE",
            "chat_id": "42",
            "attempt": 1,
            "status": "SENT",
            "message_ref": "101",
            "text": "Actual notice",
            **collection_clock().fields("at"),
        }
        if fault == "foreign-chat":
            receipt["chat_id"] = "99"
        elif fault == "missing-message-ref":
            receipt.pop("message_ref")
        elif fault == "zero-message-ref":
            receipt["message_ref"] = "0"
        elif fault == "missing-attempt":
            receipt.pop("attempt")
        elif fault == "missing-purpose":
            receipt.pop("purpose")
        else:
            receipt["purpose"] = "UNATTESTED"
        await repo.put("out.deliveries", "malformed-notice", receipt, immutable=True)
        before = await repo.list("out.deliveries")
        with pytest.raises(ValueError):
            await inbox.notice(row, "Actual notice", "IGNORED", reason="NO_REFERENCE")
    assert await repo.get("ops.telegram_messages", "42:17") == row
    assert await repo.list("out.deliveries") == before


@pytest.mark.parametrize(
    "fault", ["foreign-chat", "missing-message-ref", "zero-message-ref", "missing-purpose", "unknown-purpose"]
)
async def test_malformed_notice_receipt_cannot_settle_the_actual_input(
    repo: StorageBackend, tmp_path: Path, fault: str
) -> None:
    await invalid_notice_receipt(repo, tmp_path, fault)


async def test_unreported_notice_attempt_is_not_fabricated_for_an_unconstrained_old_row(
    tmp_path: Path,
) -> None:
    await invalid_notice_receipt(MemoryRepository(), tmp_path, "missing-attempt")


@pytest.mark.parametrize("uncertain", [False, True])
async def test_actual_notice_phase_reconciles_without_new_token_or_post(
    repo: StorageBackend, tmp_path: Path, uncertain: bool
) -> None:
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if uncertain:
            raise httpx.ReadTimeout("Synthetic unavailable receipt", request=request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 101, "date": 1800000001}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        active = TelegramInbox(
            repo, PreflightSettings(data_dir=tmp_path, telegram_token="synthetic-private-token"), http=http
        )
        await active.store(
            {
                "chat": {"id": 42, "type": "private"},
                "message_id": 17,
                "date": 1800000000,
                "text": "Actual input",
            },
            1,
        )
        row = await repo.get("ops.telegram_messages", "42:17")
        assert row is not None
        if uncertain:
            with pytest.raises(httpx.ReadTimeout):
                await active.notice(row, "Actual notice", "IGNORED", reason="NO_REFERENCE")
        else:
            await active.notice(row, "Actual notice", "IGNORED", reason="NO_REFERENCE")
        before = await repo.list("out.deliveries")
        inactive = TelegramInbox(repo, PreflightSettings(data_dir=tmp_path, telegram_token=None), http=http)
        await inactive.notice(row, "Actual notice", "IGNORED", reason="NO_REFERENCE")
        assert await repo.list("out.deliveries") == before
    assert calls == 1
    current = await repo.get("ops.telegram_messages", "42:17")
    assert current is not None and current["status"] == "IGNORED"
    if uncertain:
        assert {receipt["status"] for receipt in before} == {"GENERATED"}
    else:
        assert {receipt["status"] for receipt in before} == {"GENERATED", "SENT"}


async def ingest_notice_material(repo: StorageBackend, tmp_path: Path) -> SourceItem:
    discovery = ArticleDiscovery(
        source_id="S01",
        external_id="actual-item",
        url="https://example.test/notice-material",
        title="Synthetic notice material",
        text="An authored source item for the notice subject boundary",
        published_at="2026-10-01T12:00:00Z",
        published_prec="EXACT",
    )
    observed = clock_iso()
    raw_ref, raw_sha256 = write_raw_item(
        tmp_path, "S01", "actual-item", observed, discovery.model_dump_json(exclude_unset=True).encode()
    )
    versions = fixture_versions()
    captured = CapturedItem(
        discovery=discovery,
        capture=CaptureEvidence(
            item_id="actual-item",
            feed="https://example.test/feed",
            observed_at=observed,
            raw_ref=raw_ref,
            raw_sha256=raw_sha256,
            discovery_config_version=versions.config_version,
            discovery_code_version=versions.code_version,
        ),
    )
    ingestion = SourceIngestion(repo, versions, {}, RawItemStorage(tmp_path), DisabledSourceProgress())
    source = source_definition(
        {
            "source_id": "S01",
            "name": "Synthetic notice source",
            "feeds": [{"url": "https://example.test/feed", "kind": "rss"}],
        }
    )
    return await ingestion.ingest(source, captured)


@pytest.mark.parametrize("subject", ["foreign", "absent", "null"])
async def test_real_ops_no_session_cannot_settle_from_another_material_receipt(
    repo: StorageBackend, tmp_path: Path, subject: str
) -> None:
    material = await ingest_notice_material(repo, tmp_path)
    source_before = material.storage_record()
    raw_path = Path(material.raw_ref)
    raw_before = raw_path.read_bytes()
    jobs_before = await repo.list("ops.jobs")

    def refuse(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Foreign material receipt cannot authorize HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(refuse)) as http:
        inbox = TelegramInbox(repo, PreflightSettings(data_dir=tmp_path, telegram_token=None), http=http)
        await inbox.store(
            {
                "chat": {"id": 42, "type": "private"},
                "message_id": 17,
                "date": 1800000000,
                "text": "Why this material?",
            },
            1,
        )
        row = await repo.get("ops.telegram_messages", "42:17")
        assert row is not None
        receipt: dict[str, JsonValue] = {
            "question_id": question_id_for("42", 17),
            "channel": ANSWER_CHANNEL,
            "purpose": "NOTICE",
            "chat_id": "42",
            "attempt": 1,
            "status": "SENT",
            "message_ref": "101",
            "item_id": "foreign-item",
            **collection_clock().fields("at"),
        }
        if subject == "absent":
            receipt.pop("item_id")
        elif subject == "null":
            receipt["item_id"] = None
        await repo.put("out.deliveries", "foreign-material", receipt, immutable=True)
        before = await repo.list("out.deliveries")
        with pytest.raises(ValueError):
            await inbox.ops_question(row, {"kind": "OPS_ITEM", "item_id": "actual-item", "text": None})
    assert await repo.get("ops.telegram_messages", "42:17") == row
    assert await repo.list("out.deliveries") == before
    assert await repo.list("ops.jobs") == jobs_before
    assert await repo.get("ingest.source_items", material.item_id) == source_before
    assert raw_path.read_bytes() == raw_before


async def test_real_ops_no_session_reuses_its_matching_material_receipt_without_new_http(
    repo: StorageBackend, tmp_path: Path
) -> None:
    source = (await ingest_notice_material(repo, tmp_path)).storage_record()
    calls = 0

    def respond(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 101, "date": 1800000001}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        active = TelegramInbox(
            repo, PreflightSettings(data_dir=tmp_path, telegram_token="synthetic-private-token"), http=http
        )
        await active.store(
            {
                "chat": {"id": 42, "type": "private"},
                "message_id": 17,
                "date": 1800000000,
                "text": "Why this material?",
            },
            1,
        )
        row = await repo.get("ops.telegram_messages", "42:17")
        assert row is not None
        target: dict[str, JsonValue] = {"kind": "OPS_ITEM", "item_id": "actual-item", "text": None}
        await active.ops_question(row, target)
        before = await repo.list("out.deliveries")
        assert {delivery["status"] for delivery in before} == {"GENERATED", "SENT"}
        assert all(delivery["item_id"] == "actual-item" for delivery in before)
        inactive = TelegramInbox(repo, PreflightSettings(data_dir=tmp_path, telegram_token=None), http=http)
        await inactive.ops_question(row, target)
        assert await repo.list("out.deliveries") == before
    assert calls == 1
    current = await repo.get("ops.telegram_messages", "42:17")
    assert current is not None and current["status"] == "NO_SESSION"
    assert current["item_id"] == "actual-item" and current["reason"] == "NOT_YET_RECEIVED"
    assert await repo.get("ingest.source_items", "actual-item") == source
