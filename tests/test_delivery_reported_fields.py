"""Known source and destination facts retain their actual semantic types."""

import json
from typing import Literal

import httpx
import pytest
from delivery_support import delivery_job
from pydantic import JsonValue
from pydantic_settings import SettingsConfigDict
from test_ops_card_schedule import seed

from tank.contracts.delivery import OpsSourceItem
from tank.contracts.source_checkpoints import VenueInstrument
from tank.delivery import DeliveryService
from tank.ops_cards import source_summary
from tank.settings import Settings
from tank.sources.instruments import evidence
from tank.storage import MemoryRepository
from tank.time_precision import collection_clock


class ReportedFieldSettings(Settings):
    model_config = SettingsConfigDict(env_file=None)


@pytest.mark.parametrize(
    "field", ["symbol", "venue", "market_type", "status", "base", "quote", "contract_type"]
)
@pytest.mark.parametrize("value", [True, 42, 0.5])
def test_instrument_presentation_refuses_numeric_or_boolean_text_facts(field: str, value: JsonValue) -> None:
    before: dict[str, JsonValue] = {"symbol": "X-USDT", "venue": "okx", "status": "preopen"}
    after: dict[str, JsonValue] = {**before, "status": "live", field: value}
    item = OpsSourceItem(
        item_id="instrument",
        source_id="S14",
        title="Instrument",
        text=json.dumps({"before": before, "after": after}),
    )
    with pytest.raises(ValueError):
        source_summary(item)


def test_actual_instrument_evidence_market_field_reaches_presentation() -> None:
    previous = VenueInstrument(
        acquisition_market_type="SPOT",
        symbol="X-USDT",
        venue="okx",
        status="preopen",
        base="X",
        quote="USDT",
        contract_type=None,
    )
    following = VenueInstrument.model_validate(previous.model_copy(update={"status": "live"}))
    raw = json.dumps({"before": evidence(previous), "after": evidence(following)})
    item = OpsSourceItem(item_id="instrument", source_id="S14", title="Instrument", text=raw)
    title, summary = source_summary(item)
    assert title == "OKX · X-USDT · спот"
    assert "до открытия торгов → торги открыты" in summary
    assert item.text == raw


def test_unrelated_source_json_is_opaque_source_text() -> None:
    raw = json.dumps({"before": {"symbol": 42}, "after": {"symbol": True}})
    item = OpsSourceItem(item_id="unrelated", source_id="S29", title="Actual source", text=raw)
    title, summary = source_summary(item)
    assert title == "Actual source"
    assert '"before"' in summary and '"symbol": 42' in summary
    assert item.text == raw


@pytest.mark.parametrize("route", ["ALERT", "PROGRESS"])
@pytest.mark.parametrize("credential", ["TOKEN", "CHAT"])
async def test_whitespace_destination_credentials_refuse_before_delivery_intent(
    route: Literal["ALERT", "PROGRESS"], credential: Literal["TOKEN", "CHAT"]
) -> None:
    repo = MemoryRepository()
    if route == "PROGRESS":
        await seed(repo, collection_clock())
        job = delivery_job({"notice_id": "n"})
    else:
        publication: dict[str, JsonValue] = {
            "alert_id": "a",
            "event_id": "e",
            "version": 1,
            "card_text": "Actual card",
            "decisions": [],
            **collection_clock().fields("generated_at"),
        }
        await repo.put("out.alerts", "a:1", publication, immutable=True)
        job = delivery_job({"alert_id": "a", "version": 1})
    requests: list[httpx.Request] = []

    def send(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 42}})

    settings = ReportedFieldSettings(
        telegram_token=" " if credential == "TOKEN" else "synthetic",
        telegram_alerts_chat=" " if credential == "CHAT" else "10",
        telegram_ops_chat=" " if credential == "CHAT" else "10",
        ops_progress_enabled=True,
        ops_progress_delay_s=0,
        replay=False,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(send)) as http:
        with pytest.raises(ValueError):
            await DeliveryService(repo, settings, http=http).deliver(job)
    assert requests == []
    assert await repo.list("out.deliveries") == []
    assert await repo.list("out.ops_cards") == []


def test_instrument_market_aliases_cannot_select_contradictory_markets() -> None:
    raw = json.dumps(
        {
            "before": {"symbol": "X-USDT", "market_type": "SPOT", "status": "preopen"},
            "after": {
                "symbol": "X-USDT",
                "market_type": "SPOT",
                "acquisition_market_type": "PERP_LINEAR",
                "status": "live",
            },
        }
    )
    item = OpsSourceItem(item_id="instrument", source_id="S14", title="Instrument", text=raw)
    with pytest.raises(ValueError):
        source_summary(item)
    assert item.text == raw
