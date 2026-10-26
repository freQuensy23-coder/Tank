import copy
import json
from types import SimpleNamespace

import httpx
import pytest

from tank.delivery import DeliveryError, DeliveryService
from tank.storage import MemoryRepository
from tank.telegram_rich import card_rich_message, message_text, rich_message_text, score_summary, score_table


def assessment():
    return {
        "dims": {
            "economic_directness": {"value": 8, "rationale": "Fees reach UNI burn."},
            "magnitude": {"value": None, "reason": "NOT_ESTIMABLE", "rationale": "Volume is unknown."},
            "permanence": {"value": 6, "rationale": "Durable architecture."},
            "distribution_expansion": {"value": 0, "rationale": "No new access."},
            "mechanical_flows": {"value": None, "reason": "NOT_SUPPORTED"},
            "surprise": {"value": None, "rationale": "Expectations were not assessed."},
            "implementation": {"value": 8, "rationale": "Live launch."},
        }
    }


def test_native_table_preserves_zero_unknown_and_explanations_as_literal_text():
    data = assessment()
    markup = '<a href="https://example.test">claim & proof</a> | **bold** 🦄'
    data["dims"]["economic_directness"]["rationale"] = markup
    message = card_rich_message(
        f"MAJOR — <test>\n{score_summary(data)}\nStage: LIVE\n\nWHY IT MATTERS\nVERIFIED: <claim>",
        data,
    )
    table = message["blocks"][1]
    assert table["type"] == "table"
    assert table["is_compact"] and table["is_bordered"] and table["is_striped"]
    assert len(table["cells"]) == 8
    assert all(
        cell["align"] in {"left", "center", "right"} and cell["valign"] == "top"
        for row in table["cells"]
        for cell in row
    )
    assert [cell["text"] for cell in table["cells"][0]] == ["Dimension", "Score / 10", "Reason"]
    assert table["cells"][1][2]["text"] == markup
    assert table["cells"][2][1]["text"] == "Not assessed"
    assert table["cells"][4][1]["text"] == "0"
    assert table["cells"][5][2]["text"] == "not supported"
    text = rich_message_text(message)
    assert markup in text and "<claim>" in text and "<test>" in text
    assert "Scores D8" not in text
    assert "Economic directness | 8 | " in text
    assert "Durable architecture." in text
    assert set(message) == {"blocks"}


def test_rich_size_check_counts_the_added_rationales_against_telegram_limits_only():
    data = assessment()
    data["dims"]["economic_directness"]["rationale"] = "x" * 5000
    message = card_rich_message(f"MAJOR\n{score_summary(data)}", data)
    assert len(rich_message_text(message)) > 5000
    data["dims"]["economic_directness"]["rationale"] = "x" * 32769
    with pytest.raises(ValueError, match="CARD_LENGTH: .*Telegram maximum 32768"):
        card_rich_message(f"MAJOR\n{score_summary(data)}", data)
    with pytest.raises(ValueError, match="500 blocks"):
        card_rich_message("\n".join(["line", score_summary({}), *(["line"] * 501)]), {})


def test_rich_text_reader_handles_nested_blocks_and_formatting():
    message = {
        "rich_message": {
            "blocks": [
                {"type": "heading", "text": {"type": "bold", "text": "UNI"}},
                {
                    "type": "details",
                    "summary": "Details",
                    "blocks": [
                        {
                            "type": "paragraph",
                            "text": [
                                "Value ",
                                {"type": "italic", "text": ["8", "/10"]},
                                {"type": "custom_emoji", "alternative_text": "🦄"},
                            ],
                        }
                    ],
                },
                score_table(assessment()),
                {
                    "type": "list",
                    "items": [{"label": "1.", "blocks": [{"type": "paragraph", "text": "Item"}]}],
                },
            ]
        }
    }
    text = message_text(message)
    assert message_text({**message, "text": "fallback summary"}) == text
    assert text.startswith("UNI\nDetails\nValue 8/10🦄")
    assert "Distribution expansion | 0 | No new access." in text
    assert text.endswith("1. Item")
    assert message_text({"text": "ordinary"}) == "ordinary"
    assert message_text({"caption": "caption"}) == "caption"
    assert message_text({}) == ""


@pytest.mark.asyncio
async def test_rich_delivery_retries_frozen_payload_and_threads_update_once():
    repo = MemoryRepository()
    what = "Uniswap now uses external trading venues for swaps in its app & API."
    frozen = card_rich_message(
        f"MAJOR\nWHAT: {what}\n{score_summary(assessment())}\nStage: LIVE", assessment()
    )
    for version in (1, 2):
        await repo.put(
            "out.alerts",
            f"a:{version}",
            {
                "alert_id": "a",
                "version": version,
                "event_id": "e",
                "kind": "FULL" if version == 1 else "UPDATE",
                "card_text": "legacy representation",
                "rich_message": copy.deepcopy(frozen),
                "delivery_immediate": True,
                "decisions": [],
            },
            immutable=True,
        )
    requests = []

    def send(request):
        body = json.loads(request.content)
        requests.append((request.url.path, body))
        assert request.url.path.endswith("/sendRichMessage")
        assert "text" not in body and "parse_mode" not in body and "link_preview_options" not in body
        if len(requests) == 1:
            return httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 3}})
        result = {"message_id": 40 + len(requests), "date": 1792678057}
        if body.get("reply_parameters"):
            result["reply_to_message"] = body["reply_parameters"]
        return httpx.Response(200, json={"ok": True, "result": result})

    settings = SimpleNamespace(telegram_token="test", telegram_alerts_chat="1", telegram_ops_chat="2")
    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        service = DeliveryService(repo, settings, http=http)
        with pytest.raises(DeliveryError):
            await service.deliver({"alert_id": "a", "version": 1})
        await service.deliver({"alert_id": "a", "version": 1})
        await service.deliver({"alert_id": "a", "version": 1})
        await service.deliver({"alert_id": "a", "version": 2})
    assert len(requests) == 3
    assert all(body["rich_message"] == frozen for _, body in requests)
    assert all(
        rich_message_text(body["rich_message"]).splitlines()[1] == "WHAT: " + what for _, body in requests
    )
    assert requests[-1][1]["reply_parameters"] == {"message_id": 42}
    sent = await repo.list("out.deliveries", filters={"status": "SENT"})
    assert len(sent) == 2
    assert sent[-1]["response_reply_to_message_ref"] == "42"
    assert (await repo.get("out.alerts", "a:1"))["rich_message"] == frozen


def test_multiline_headline_and_reason_keep_the_table_at_the_canonical_summary():
    data = assessment()
    data["dims"]["magnitude"]["rationale"] = "Unknown volume.\nNo estimate."
    text = "MAJOR\nHeadline continuation\n" + score_summary(data) + "\nStage: LIVE"
    result = card_rich_message(text, data)
    assert result["blocks"][1]["text"] == "Headline continuation"
    assert result["blocks"][2]["type"] == "table"
    assert result["blocks"][2]["cells"][2][2]["text"] == "Unknown volume.\nNo estimate."
    assert result["blocks"][3]["text"] == "Stage: LIVE"
