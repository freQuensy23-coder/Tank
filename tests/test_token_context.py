"""Regression tests for token research snapshots and human-gated updates."""

from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from tank.storage import MemoryRepository
from tank.telegram_inbox import TelegramInbox
from tank.token_context import (
    TokenContextService,
    check_markdown,
    diff_svg,
    filename,
    revision,
)


def settings():
    return SimpleNamespace(
        telegram_ops_chat="-200",
        telegram_alerts_chat="-100",
        telegram_token="fake",
        replay=False,
        load_config=lambda _: {
            "tokens": [{"token_id": "uniswap:UNI", "symbol": "UNI"}],
        },
    )


@pytest.mark.asyncio
async def test_token_context_empty_snapshot_and_import():
    repo = MemoryRepository()
    service = TokenContextService(repo, settings())
    current = await service.read("uniswap:UNI")
    assert current["markdown"] == ""
    assert current["revision"] == revision("")
    assert (await service.snapshot()) == {filename("uniswap:UNI"): ""}
    imported = await service.import_text("uniswap:UNI", "## Fees\nOld facts.\n")
    assert imported["changed"]
    assert (await service.read("uniswap:UNI"))["markdown"] == "## Fees\nOld facts.\n"
    assert (await service.import_text("uniswap:UNI", "## Fees\nOld facts.\n"))["changed"] is False


@pytest.mark.asyncio
async def test_edit_replaces_stale_facts_only_after_approval():
    repo = MemoryRepository()
    service = TokenContextService(repo, settings())
    await service.import_text("uniswap:UNI", "## Fees\nOld.\n")
    await repo.put("evt.events", "EVT-1", {"event_id": "EVT-1", "tokens_mentioned": ["uniswap:UNI"]})
    await repo.put(
        "evt.claims",
        "CLM-1",
        {"claim_id": "CLM-1", "event_id": "EVT-1", "accepted": True, "verified": True},
    )
    context = {"role": "analyst", "session_id": "session-1", "event_id": "EVT-1"}
    arguments = {
        "token_id": "uniswap:UNI",
        "updated_markdown": "## Fees\nCurrent.\n",
        "reason": "Verified event changed the protocol fee switch.",
        "claim_ids": ["CLM-1"],
    }
    result = await service.propose(arguments, context)
    assert result["status"] == "PENDING_HUMAN_APPROVAL"
    assert (await service.read("uniswap:UNI"))["markdown"] == "## Fees\nOld.\n"
    proposal = (await repo.list("reg.token_context_proposals"))[0]
    with pytest.raises(ValueError, match="proposal message"):
        await service.decide(
            proposal["proposal_id"], "approve", chat_id="-200", message_id="100", user_id="5"
        )
    await repo.put(
        "reg.token_context_proposals",
        proposal["proposal_id"],
        {**proposal, "ops_chat_id": "-200", "ops_message_id": "100"},
    )
    applied = await service.decide(
        proposal["proposal_id"], "agree", chat_id="-200", message_id="100", user_id="5"
    )
    assert applied["status"] == "APPROVED"
    assert (await service.read("uniswap:UNI"))["markdown"] == "## Fees\nCurrent.\n"
    with pytest.raises(ValueError, match="already been decided"):
        await service.decide(
            proposal["proposal_id"], "approve", chat_id="-200", message_id="100", user_id="5"
        )


@pytest.mark.asyncio
async def test_stale_proposal_is_rejected_without_partial_write():
    repo = MemoryRepository()
    service = TokenContextService(repo, settings())
    await service.import_text("uniswap:UNI", "## Fees\nOld.\n")
    await repo.put("evt.events", "EVT-1", {"event_id": "EVT-1", "tokens_mentioned": ["uniswap:UNI"]})
    await repo.put(
        "evt.claims", "CLM-1", {"claim_id": "CLM-1", "event_id": "EVT-1", "accepted": True, "verified": True}
    )
    context = {"role": "analyst", "session_id": "session-1", "event_id": "EVT-1"}
    kwargs = {"token_id": "uniswap:UNI", "reason": "Verified event changed the fees.", "claim_ids": ["CLM-1"]}
    await service.propose({**kwargs, "updated_markdown": "## Fees\nNew A.\n"}, context)
    await service.propose({**kwargs, "updated_markdown": "## Fees\nNew B.\n"}, context)
    proposals = await repo.list("reg.token_context_proposals")
    for index, proposal in enumerate(proposals):
        await repo.put(
            "reg.token_context_proposals",
            proposal["proposal_id"],
            {**proposal, "ops_chat_id": "-200", "ops_message_id": str(index)},
        )
    await service.decide(proposals[0]["proposal_id"], "approve", chat_id="-200", message_id="0", user_id="5")
    with pytest.raises(ValueError, match="Stale"):
        await service.decide(
            proposals[1]["proposal_id"], "approve", chat_id="-200", message_id="1", user_id="5"
        )
    assert (await service.read("uniswap:UNI"))["markdown"] == "## Fees\nNew A.\n"
    assert (await repo.get("reg.token_context_proposals", proposals[1]["proposal_id"]))["status"] == "PENDING"


@pytest.mark.asyncio
async def test_no_unverified_or_extra_sections_in_automatic_edits():
    repo = MemoryRepository()
    service = TokenContextService(repo, settings())
    await service.import_text("uniswap:UNI", "## Fees\nOld.\n")
    await repo.put("evt.events", "EVT-1", {"event_id": "EVT-1", "tokens_mentioned": ["uniswap:UNI"]})
    await repo.put(
        "evt.claims", "CLM-1", {"claim_id": "CLM-1", "event_id": "EVT-1", "accepted": True, "verified": False}
    )
    context = {"role": "analyst", "session_id": "session-1", "event_id": "EVT-1"}
    arguments = {
        "token_id": "uniswap:UNI",
        "updated_markdown": "## Fees\nUpdated.\n",
        "reason": "Verified fees changed.",
        "claim_ids": ["CLM-1"],
    }
    with pytest.raises(ValueError, match="accepted and verified"):
        await service.propose(arguments, context)
    await repo.put(
        "evt.claims", "CLM-1", {"claim_id": "CLM-1", "event_id": "EVT-1", "accepted": True, "verified": True}
    )
    with pytest.raises(ValueError, match="sections"):
        await service.propose(
            {**arguments, "updated_markdown": "## Fees\nUpdated.\n\n## New topic\nNew content.\n"},
            context,
        )
    with pytest.raises(ValueError, match="replace"):
        await service.propose(
            {**arguments, "updated_markdown": "## Fees\nOld.\nExtra."},
            context,
        )
    with pytest.raises(ValueError, match="follow-up"):
        await service.propose({**arguments, "user_requested_expansion": True}, context)


@pytest.mark.asyncio
async def test_callback_requires_ops_administrator():
    repo = MemoryRepository()
    service = TokenContextService(repo, settings())
    await service.import_text("uniswap:UNI", "## Fees\nOld.\n")
    await repo.put("evt.events", "EVT-1", {"event_id": "EVT-1", "tokens_mentioned": ["uniswap:UNI"]})
    await repo.put(
        "evt.claims", "CLM-1", {"claim_id": "CLM-1", "event_id": "EVT-1", "accepted": True, "verified": True}
    )
    await service.propose(
        {
            "token_id": "uniswap:UNI",
            "updated_markdown": "## Fees\nNew.\n",
            "reason": "Fees were changed by the protocol.",
            "claim_ids": ["CLM-1"],
        },
        {"role": "analyst", "session_id": "session-1", "event_id": "EVT-1"},
    )
    proposal = (await repo.list("reg.token_context_proposals"))[0]
    await repo.put(
        "reg.token_context_proposals",
        proposal["proposal_id"],
        {**proposal, "ops_chat_id": "-200", "ops_message_id": "7"},
    )
    inbox = TelegramInbox(repo, settings(), http=SimpleNamespace())
    replies = []

    async def fake_call(method, payload):
        replies.append((method, payload))
        if method == "getChatMember":
            return {"status": "member"}
        return True

    inbox.call = fake_call
    await inbox.token_context_callback(
        {
            "id": "cb",
            "data": "tc:approve:" + proposal["proposal_id"],
            "from": {"id": 5},
            "message": {"chat": {"id": -200}, "message_id": 7},
        }
    )
    assert (await service.read("uniswap:UNI"))["markdown"] == "## Fees\nOld.\n"
    assert any(method == "answerCallbackQuery" and args.get("show_alert") for method, args in replies)


def test_diff_svg_and_markdown_validation():
    svg = diff_svg("uniswap:UNI", "Old", "Current")
    assert "#ffebe9" in svg
    assert "#dafbe1" in svg
    assert "<svg" in svg
    assert check_markdown("a\r\nb") == "a\nb"


def test_initial_migration_embeds_every_fact_sheet_without_google_auth():
    root = Path(__file__).resolve().parents[1]
    migration = (root / "src/tank/migrations/011_token_context.sql").read_text()
    registry = yaml.safe_load((root / "config/registry.yaml").read_text())
    tokens = [row["token_id"] for row in registry["tokens"]]

    assert len(tokens) == 22
    assert migration.count("https://docs.google.com/document/d/") == 22
    assert migration.count("$tank_seed_") == 44
    assert migration.count("Executive summary") >= 22
    assert migration.count("Verification notes") >= 22
    for token_id in tokens:
        assert f"    ('{token_id}'," in migration
    assert "ON CONFLICT (storage_key) DO NOTHING" in migration
    assert "sha256(convert_to(markdown, 'UTF8'))" in migration
    assert "GOOGLE_DRIVE_ACCESS_TOKEN" not in migration
    assert "seed-drive" not in (root / "src/tank/cli.py").read_text()
    assert "import_drive_folder" not in (root / "src/tank/token_context.py").read_text()


@pytest.mark.asyncio
async def test_token_context_openma_mounts_read_only_snapshot():
    import json

    import httpx

    from tank.agents.sessions import SessionService

    repo = MemoryRepository()
    context_settings = settings()
    context_settings.token_context_enabled = True
    context_settings.openma_url = "http://openma:8787"
    context_settings.openma_api_key = None
    context_settings.read_config_bytes = lambda name: b"test"
    mounted = []

    def handler(request):
        if request.url.path == "/v1/files":
            assert request.method == "POST"
            assert b"uniswap:UNI.md" in request.content
            mounted.append("upload")
            return httpx.Response(200, json={"id": "file-uniswap"})
        if request.url.path == "/v1/sessions/abc/resources":
            assert request.method == "POST"
            body = json.loads(request.content)
            assert body == {
                "type": "file",
                "file_id": "file-uniswap",
                "mount_path": filename("uniswap:UNI"),
            }
            mounted.append("mount")
            return httpx.Response(200, json={"id": "resource-uniswap"})
        raise AssertionError("Unexpected OpenMA request")

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        await SessionService(repo, context_settings, lambda *_: None, http=http).mount_token_context("abc")
    finally:
        await http.aclose()
    assert mounted == ["upload", "mount"]
