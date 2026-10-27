"""Mixed reported contracts do not turn an acquisition category into physical kind."""

from typing import Literal

import pytest
from pydantic import JsonValue
from source_support import source_feed

from tank.contracts.delivery import OpsSourceItem
from tank.contracts.json import encode_json
from tank.contracts.source_checkpoints import BybitContractObservation, VenueInstrument
from tank.ops_cards import source_summary
from tank.sources.instrument_adapters import parse_instrument_page
from tank.sources.instruments import InstrumentPage, instrument_changes
from tank.time_precision import collection_clock

VARIANTS: tuple[tuple[str, str, str, Literal["PERPETUAL", "DATED_FUTURE"]], ...] = (
    ("AAAUSDT", "LinearPerpetual", "USDT", "PERPETUAL"),
    ("AAAUSDT_FUT", "LinearFutures", "USDT", "DATED_FUTURE"),
    ("AAAUSD", "InversePerpetual", "AAA", "PERPETUAL"),
    ("AAAUSD_FUT", "InverseFutures", "AAA", "DATED_FUTURE"),
)


def provider_wire(changed: str | None) -> dict[str, JsonValue]:
    rows: list[JsonValue] = [
        {
            "symbol": symbol,
            "baseCoin": "AAA",
            "quoteCoin": "USD" if settlement == "AAA" else settlement,
            "status": "Settling" if symbol == changed else "Trading",
            "contractType": code,
            "settleCoin": settlement,
        }
        for symbol, code, settlement, _ in VARIANTS
    ]
    return {"retCode": 0, "result": {"category": "linear", "list": rows, "nextPageCursor": ""}}


@pytest.mark.parametrize("symbol,code,settlement,kind", VARIANTS)
def test_mixed_bybit_s14_evidence_keeps_acquisition_and_reported_kind_separate(
    symbol: str, code: str, settlement: str, kind: Literal["PERPETUAL", "DATED_FUTURE"]
) -> None:
    feed = source_feed(
        {
            "kind": "instruments",
            "url": "https://api.bybit.example/v5/market/instruments-info?category=linear",
            "venue": "bybit",
            "market_type": "PERP_LINEAR",
        }
    )
    baseline_payload = encode_json(provider_wire(None))
    changed_payload = encode_json(provider_wire(symbol))
    baseline = parse_instrument_page(baseline_payload, feed).snapshot
    changed = parse_instrument_page(changed_payload, feed).snapshot
    assert set(baseline.root) == set(changed.root) == {row[0] for row in VARIANTS}
    observed = changed.root[symbol]
    assert isinstance(observed, VenueInstrument)
    assert isinstance(observed.reported_contract, BybitContractObservation)
    assert observed.acquisition_market_type == "PERP_LINEAR"
    assert observed.reported_contract.kind == kind
    assert observed.reported_contract.contract_type == code
    assert observed.reported_contract.settle_coin == settlement
    stamp = collection_clock()
    assert stamp.value is not None
    events, _ = instrument_changes(
        changed,
        baseline,
        feed,
        [InstrumentPage(payload=changed_payload, observed_at=stamp.value)],
        {value: 0 for value in changed.root},
        stamp.value,
    )
    assert len(events) == 1
    emitted = events[0]
    assert emitted.payload == changed_payload
    assert emitted.discovery.source_id == "S14"
    assert emitted.discovery.change == "STATE_UPDATE"
    item = OpsSourceItem(
        item_id="catalog-" + symbol,
        source_id=emitted.discovery.source_id,
        title=emitted.discovery.title,
        text=emitted.discovery.text,
    )
    title, _ = source_summary(item)
    assert title.startswith("BYBIT · ")
    assert title.endswith(" · рынок деривативов")
    assert "бессрочный" not in title
    assert baseline.root[symbol].status == "Trading"
    assert changed.root[symbol].status == "Settling"
