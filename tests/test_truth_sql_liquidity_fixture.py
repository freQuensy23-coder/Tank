import json
from datetime import datetime

from test_truth_sql_liquidity import _packet

from tank.analysis.truth_sql_liquidity import _transport
from tank.util import canonical_json, parse_time


def test_liquidity_packet_retains_frozen_datetime_anchor_and_original_wire_input():
    snapshot, spot, relative = _packet()
    anchor = snapshot["_event_anchor"]
    assert isinstance(anchor, datetime)
    assert anchor == parse_time("2026-10-16T11:45:00Z")
    assert anchor == parse_time(snapshot["_start"])
    expected = dict(snapshot)
    expected["_event_anchor"] = "2026-10-16T11:45:00Z"
    encoded = canonical_json(_transport(snapshot))
    assert encoded == json.dumps(
        expected, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
    assert snapshot["_event_anchor"] is anchor
    assert len(snapshot["books"]) == 4
    assert len(snapshot["flow_bars"]) == 30
    assert [venue["venue"] for venue in spot["venues"]] == ["A", "B"]
    assert relative == {"windows": {"W15M": {"r_raw": 0.005, "beyond_normal": False}}}
