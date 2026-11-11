"""Regression tests for token research snapshots and human-gated updates."""

import hashlib
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
    assert check_markdown("a\r\nb") == "a\r\nb"


def test_initial_migration_embeds_every_fact_sheet_without_google_auth():
    root = Path(__file__).resolve().parents[1]
    migration = (root / "src/tank/migrations/012_token_context.sql").read_text()
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


def openma_settings(tokens):
    context_settings = settings()
    context_settings.token_context_enabled = True
    context_settings.openma_url = "http://openma:8787"
    context_settings.openma_api_key = None
    context_settings.read_config_bytes = lambda name: b"test"
    context_settings.load_config = lambda _: {
        "tokens": [{"token_id": token_id, "symbol": token_id.split(":")[-1]} for token_id in tokens]
    }
    return context_settings


@pytest.mark.asyncio
async def test_token_context_openma_mounts_read_only_snapshot():
    import json

    import httpx

    from tank.agents.sessions import SessionService

    repo = MemoryRepository()
    context_settings = openma_settings(["uniswap:UNI"])
    mounted = []

    def handler(request):
        if request.url.path == "/v1/files":
            if "files-api-2025-04-14" not in request.headers["anthropic-beta"].split(","):
                return httpx.Response(
                    400,
                    json={"error": {"message": "Missing required anthropic-beta flag: files-api-2025-04-14"}},
                )
            assert request.method == "POST"
            assert b"uniswap:UNI.md" in request.content
            mounted.append("upload")
            return httpx.Response(200, json={"id": "file-uniswap"})
        if request.url.path == "/v1/sessions/abc/resources" and request.method == "GET":
            return httpx.Response(200, json={"data": [], "next_page": None})
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


@pytest.mark.asyncio
async def test_token_context_mount_retry_skips_paths_that_are_already_mounted():
    import json

    import httpx

    from tank.agents.sessions import SessionService

    tokens = ["uniswap:UNI", "aave:AAVE", "curve:CRV"]
    context_settings = openma_settings(tokens)
    resources, uploads, outage = [], [], {"left": 1}

    def handler(request):
        if request.url.path == "/v1/files":
            if "files-api-2025-04-14" not in request.headers["anthropic-beta"].split(","):
                return httpx.Response(
                    400,
                    json={"error": {"message": "Missing required anthropic-beta flag: files-api-2025-04-14"}},
                )
            uploads.append(request.content)
            return httpx.Response(200, json={"id": f"file-{len(uploads)}"})
        if request.method == "GET":
            return httpx.Response(200, json={"data": resources, "next_page": None})
        body = json.loads(request.content)
        if len(resources) == 1 and outage["left"]:
            outage["left"] -= 1
            return httpx.Response(503, json={"error": {"message": "unavailable"}})
        if any(row["mount_path"] == body["mount_path"] for row in resources):
            return httpx.Response(400, json={"error": {"message": "mount path is already in use"}})
        resources.append(body)
        return httpx.Response(200, json={"id": f"resource-{len(resources)}"})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        service = SessionService(MemoryRepository(), context_settings, lambda *_: None, http=http)
        with pytest.raises(httpx.HTTPStatusError):
            await service.mount_token_context("abc")
        assert [row["mount_path"] for row in resources] == [filename(tokens[0])]
        await service.mount_token_context("abc")
        await service.mount_token_context("abc")
    finally:
        await http.aclose()
    assert [row["mount_path"] for row in resources] == [filename(token) for token in tokens]
    assert len(uploads) == 4


def test_migration_seed_is_byte_identical_to_google_docs_plain_text_exports():
    root = Path(__file__).resolve().parents[1]
    migration = (root / "src/tank/migrations/012_token_context.sql").read_bytes().decode("utf-8")
    expected = {
        "1INCH": "55f20f18c07b39917992becc06d4b25d538f9f5c297b4f1a654303daff5dd9b0",
        "AAVE": "b26949611baef644b7d38b4929d97d3b46109df3395209fc8fe18b234792ca3d",
        "AERO": "59bda2aed8cd579ccfce570e3d190c3759a9a579b1bbde6ca5184ad2dda8b267",
        "AVAX": "30ce6d28f999bfa44c16baf76297fc08ac6ef9f3145f02204bc1a06c7b9d6b48",
        "BTC": "0159b1cc7647c7645e9bb4fc75aa23b7bc66cf642d0454062750c5b6c429e824",
        "CAKE": "2fb1e94b68c10089fa7283677cf56e2a60ec8ee9221a81a7f467db1b711f08fd",
        "COW": "c8d967e5c6898e515eca30a4c36d3a10e2a4173990c5b1fd1e6fca2cb801747e",
        "CRV": "923baf5a4244cf5fbf514cb48d4f8f52f3e93877b2343c004c6274bbbc99f4f2",
        "ENA": "7b0ed4921edd52e60e779761af2eb825530410cd88238b45318faf8170d4a6f8",
        "ETH": "1639abf95efc539617f415c0b9b04bb73308bd7bdfb0147a4164081a8a83a1f3",
        "EUL": "e42f8a34a971d4b95ece394e71b44a3a7570966baf259f040cf7fc4b897fe75c",
        "HYPE": "788ef10d430e1b450e02bd86e42633898e67223fdfbfcf638eb27e6c3f71759d",
        "LIT": "800177c68c10c97c480a642ffd801de352551ea5dfbf251afe4cf55b7f7455c9",
        "MORPHO": "07edd15388aa02988dedb1f51dcc3da4448c338fdc200285603d78d666c71d4b",
        "NEAR": "4dccc6d76754a42592f211d61f4f6a6f22ce4071503d075336f4b679a6ea6cc5",
        "ONDO": "f1c54c06b72b3765b989b3a2b8c88a1759bb9f4e1f17480bdc8c5b81e33fc418",
        "QNT": "dbf8315a33cfb1866ce8114b8d3786eff67b4ffb45baf20aab2961ee4ce40b20",
        "UNI": "07c42427e48ef1b3965db9a787440b64dfbf6786a4ea8bdb66f6f8d8c5d2f06d",
        "WLD": "a8796e9c0c631e57dc430bb8c830f44b78f484cc508984c9465c13995f01e9ce",
        "XPL": "59781dc8b649a76655c814f32591fabf09ef70dbd3e636ca4fd738bda06f21f7",
        "ZRO": "7d0ba7bb6bf09659962cef4ee3fc0f200a67b7ef86dc4cbe3b9d823a2b83738b",
        "ZRX": "f0575dfe5726dada61bb503aa3df63b0c6290fc9b0851d5eda98f46a4f69629f",
    }
    registry = yaml.safe_load((root / "config/registry.yaml").read_text())
    assert set(expected) == {row["symbol"] for row in registry["tokens"]}
    for symbol, source_sha in expected.items():
        tag = f"$tank_seed_{symbol}_20261110$"
        assert migration.count(tag) == 2
        payload = migration.split(tag)[1].encode("utf-8")
        assert payload.startswith(b"\xef\xbb\xbf")
        assert b"\r\n" in payload
        assert hashlib.sha256(payload).hexdigest() == source_sha

    storage = (root / "src/tank/storage.py").read_text()
    assert 'path.read_bytes().decode("utf-8")' in storage


async def pending_proposal(repo, service):
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
    return proposal


def callback_updates(proposal):
    return [
        {
            "update_id": 10,
            "callback_query": {
                "id": "cb",
                "data": "tc:approve:" + proposal["proposal_id"],
                "from": {"id": 5},
                "message": {"chat": {"id": -200}, "message_id": 7},
            },
        },
        {"update_id": 11, "message": {"chat": {"id": -200}, "message_id": 8, "date": 1, "text": "hi"}},
    ]


@pytest.mark.asyncio
async def test_refused_callback_answer_keeps_the_decision_and_the_poll_cursor_moving():
    repo = MemoryRepository()
    service = TokenContextService(repo, settings())
    proposal = await pending_proposal(repo, service)
    inbox = TelegramInbox(repo, settings(), http=SimpleNamespace())
    calls = []

    async def fake_call(method, payload):
        calls.append(method)
        if method == "getUpdates":
            return callback_updates(proposal)
        if method == "getChatMember":
            return {"status": "administrator"}
        if method == "answerCallbackQuery":
            raise RuntimeError("Telegram answerCallbackQuery rejected (HTTP 400)")
        return True

    inbox.call = fake_call
    assert await inbox.poll() == 2
    assert (await service.read("uniswap:UNI"))["markdown"] == "## Fees\nNew.\n"
    assert "editMessageReplyMarkup" in calls
    assert (await repo.get("ops.source_checkpoints", "telegram:bot-updates"))["cursor"] == "12"
    assert await repo.get("ops.telegram_messages", "-200:8")


@pytest.mark.asyncio
async def test_failing_callback_is_skipped_instead_of_blocking_every_later_poll():
    repo = MemoryRepository()
    service = TokenContextService(repo, settings())
    proposal = await pending_proposal(repo, service)
    inbox = TelegramInbox(repo, settings(), http=SimpleNamespace())

    async def fake_call(method, payload):
        if method == "getUpdates":
            return callback_updates(proposal)
        if method == "getChatMember":
            raise RuntimeError("Telegram getChatMember rejected (HTTP 502)")
        return True

    inbox.call = fake_call
    assert await inbox.poll() == 2
    assert (await service.read("uniswap:UNI"))["markdown"] == "## Fees\nOld.\n"
    assert (await repo.get("ops.source_checkpoints", "telegram:bot-updates"))["cursor"] == "12"
    assert await repo.get("ops.telegram_messages", "-200:8")
    assert (await repo.get("reg.token_context_proposals", proposal["proposal_id"]))["status"] == "PENDING"


def seeded_document(symbol):
    root = Path(__file__).resolve().parents[1]
    migration = (root / "src/tank/migrations/012_token_context.sql").read_bytes().decode("utf-8")
    return migration.split(f"$tank_seed_{symbol}_20261110$")[1]


async def proposing_service(markdown):
    repo = MemoryRepository()
    service = TokenContextService(repo, settings())
    await service.import_text("uniswap:UNI", markdown)
    await repo.put("evt.events", "EVT-1", {"event_id": "EVT-1", "tokens_mentioned": ["uniswap:UNI"]})
    await repo.put(
        "evt.claims", "CLM-1", {"claim_id": "CLM-1", "event_id": "EVT-1", "accepted": True, "verified": True}
    )

    async def propose(updated, **extra):
        return await service.propose(
            {
                "token_id": "uniswap:UNI",
                "updated_markdown": updated,
                "reason": "Verified event changed the facts.",
                "claim_ids": ["CLM-1"],
                **extra,
            },
            {"role": "analyst", "session_id": "session-1", "event_id": "EVT-1", **extra.pop("context", {})},
        )

    return service, propose


def shown_text(rows, marker):
    text = ""
    for row_marker, line in rows:
        if row_marker == marker:
            text += line[2:] if line.startswith("  ") and text else line[1:]
    return text


def test_diff_image_shows_the_whole_change_of_a_long_real_paragraph():
    from tank.token_context import DIFF_MAX_ROWS, diff_rows

    old = seeded_document("UNI")
    paragraph = max(old.splitlines(), key=len)
    assert len(paragraph) > 1000
    new_paragraph = paragraph[:500] + "REPLACED" + paragraph[508:]
    new = old.replace(paragraph, new_paragraph)
    rows = diff_rows("uniswap:UNI", old, new)
    assert shown_text(rows, "-") == paragraph
    assert shown_text(rows, "+") == new_paragraph
    assert len(rows) < DIFF_MAX_ROWS
    svg = diff_svg("uniswap:UNI", old, new)
    assert svg.count("<text") == len(rows) + 2
    assert "…" not in svg


def test_diff_image_does_not_drop_lines_of_a_long_change():
    from tank.token_context import diff_rows

    old = seeded_document("UNI")
    block = "".join(f"\r\nAppended fact {index}: " + "detail " * 30 for index in range(82))
    rows = diff_rows("uniswap:UNI", old, old + block)
    assert sum(1 for marker, _ in rows if marker == "+") >= 82
    assert "Appended fact 81:" in shown_text(rows, "+")
    assert "additional diff lines" not in diff_svg("uniswap:UNI", old, old + block)


@pytest.mark.asyncio
async def test_automatic_edit_guard_holds_on_the_real_plain_text_documents():
    old = seeded_document("UNI")
    assert not any(line.startswith("#") for line in old.splitlines())
    lines = old.splitlines()
    index = max(range(len(lines)), key=lambda number: len(lines[number]))
    service, propose = await proposing_service(old)

    def with_line(replacement):
        return old.replace(lines[index], replacement, 1)

    edited = lines[index][:500] + "REPLACED" + lines[index][508:]
    with pytest.raises(ValueError, match="sections"):
        await propose(with_line(edited) + "\r\nBrand new section\r\n" + "Body.\r\n" * 80)
    with pytest.raises(ValueError, match="replace"):
        await propose(with_line(edited + "\r\n" + "Appended.\r\n" * 80))
    with pytest.raises(ValueError, match="at most"):
        await propose(with_line(lines[index] + " extra" * 400))
    title = next(line for line in lines if line == "Executive summary")
    with pytest.raises(ValueError, match="sections"):
        await propose(old.replace(title, "Brand new section", 1))
    with pytest.raises(ValueError, match="sections"):
        await propose(old.replace(lines[index], "Brand new section", 1))
    assert (await service.read("uniswap:UNI"))["markdown"] == old
    assert await propose(with_line(edited)) == {
        "status": "PENDING_HUMAN_APPROVAL",
        "token_id": "uniswap:UNI",
        "applied": False,
    }


@pytest.mark.asyncio
async def test_proposal_too_long_for_the_ops_image_is_refused():
    old = seeded_document("UNI")
    service, propose = await proposing_service(old)
    huge = "".join(f"\r\nSection {index}\r\n" + "x " * 400 for index in range(60))
    with pytest.raises(ValueError, match="too long"):
        await propose(old + huge, user_requested_expansion=True, context={"trigger_kind": "USER_QUESTION"})
    assert not await service.repo.list("reg.token_context_proposals")
