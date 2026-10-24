"""Cross-language decoding of unchanged actual Go producer outputs."""

from pathlib import Path

from pydantic import JsonValue

from tank.contracts.json import decode_json
from tank.market.capture_contracts import BackfillResult, RepairResult
from tank.time_precision import Timestamp

FIXTURES = Path(__file__).parent / "fixtures" / "capture_lifecycle"


def peer_record(name: str) -> dict[str, JsonValue]:
    wire = decode_json((FIXTURES / name).read_bytes())
    assert isinstance(wire, dict)
    return wire


def peer_text(wire: dict[str, JsonValue], key: str) -> str:
    value = wire[key]
    assert isinstance(value, str)
    return value


def test_actual_go_partial_backfill_preserves_available_flow_and_missing_klines() -> None:
    wire = peer_record("backfill-partial-go.json")
    result = BackfillResult.model_validate(wire["result"])
    assert wire["status"] == "PARTIAL"
    assert wire["reason"] == "INCOMPLETE_VENUE_WINDOWS_EXCLUDED"
    assert result.benchmark_klines == []
    (instrument,) = result.instruments
    assert instrument.instrument_id == "coinbase:spot:TEST"
    assert instrument.complete is False
    assert instrument.windows.W24H.flow.complete_minutes == 1440
    assert instrument.windows.W24H.flow.complete is True
    assert instrument.windows.W24H.klines.complete_minutes == 0
    assert instrument.windows.W24H.klines.missing_or_incomplete_minutes == 1440
    assert instrument.windows.W24H.klines.complete is False
    assert instrument.pre24_reference is not None
    assert instrument.pre24_reference.available is True
    assert instrument.pre24_reference.trade_id == "180"
    assert (
        Timestamp(peer_text(wire, "started_at"), peer_text(wire, "started_at_prec"), None).precision
        == "COLLECTION_ONLY"
    )


def test_actual_go_complete_repair_reports_thirty_repaired_minutes() -> None:
    wire = peer_record("repair-complete-go.json")
    result = RepairResult.model_validate(wire["result"])
    assert wire["status"] == "COMPLETE"
    counts = result.instruments["coinbase:spot:TEST"]
    assert counts.incomplete_before == 30 and counts.incomplete_after == 0
    assert wire["reason"] is None
    assert wire["started_at_prec"] == wire["finished_at_prec"] == "COLLECTION_ONLY"


def test_actual_go_expired_repair_preserves_unexecuted_result() -> None:
    wire = peer_record("repair-expired-go.json")
    assert wire["status"] == "EXPIRED"
    assert wire["reason"] == "REQUEST_DEADLINE_PASSED"
    assert "result" not in wire or wire["result"] is None
    assert wire["finished_at_prec"] == "COLLECTION_ONLY"
