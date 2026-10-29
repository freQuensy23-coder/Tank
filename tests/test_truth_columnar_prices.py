from array import array
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Rounded, localcontext

import pytest
from test_truth_start_fixture import compact_start_snapshot

from tank.analysis import truth
from tank.util import parse_time


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def micros(value):
    delta = value - EPOCH
    return delta.days * 86400000000 + delta.seconds * 1000000 + delta.microseconds


def price_packet():
    first = datetime(2026, 10, 1, tzinfo=UTC)
    times = [first + timedelta(hours=index) for index in range(6)]
    return {
        "cutoff": times[-2],
        "klines": [
            {
                "instrument_id": "A:spot:T",
                "token_id": "T",
                "market_type": "SPOT",
                "complete": True,
                "close_time": at,
                "c": value,
            }
            for at, value in ((times[3], "91"), (times[0], "92"), (times[3], "93"))
        ],
        "stat_price_series": {
            "A:spot:T": {
                "token_id": "T",
                "time_basis": "CLOSE_TIME",
                "times_us": array("q", map(micros, times)),
                "values": array("d", [101, 102, 103, 104, 105, 106]),
            }
        },
    }


def test_projected_statistical_prices_keep_columns_without_float_reconversion(monkeypatch):
    packet = price_packet()
    expected = truth._price_groups(packet)
    before = deepcopy(packet)
    original = truth.number

    def reject_reconversion(value):
        assert type(value) is not float, "native statistical prices must remain columnar"
        return original(value)

    monkeypatch.setattr(truth, "number", reject_reconversion)
    token = truth._ASSESSMENT_INPUTS.set({})
    try:
        actual = truth._prices(packet)
        assert list(actual) == list(expected)
        for iid, values in expected.items():
            assert list(actual[iid]) == list(values)
            assert [(at, value.hex()) for at, value in actual[iid].items()] == [
                (at, value.hex()) for at, value in values.items()
            ]
        assert packet == before
        packet["stat_price_series"]["A:spot:T"]["values"][0] = 9999
        assert dict(actual["A:spot:T"]) == expected["A:spot:T"]
    finally:
        truth._ASSESSMENT_INPUTS.reset(token)


@pytest.mark.parametrize("microseconds", [0, 1])
def test_columnar_relative_result_matches_legacy_order_and_context(microseconds):
    packet = compact_start_snapshot()
    identities = {row["instrument_id"]: row.get("token_id") for row in packet["klines"]}
    packet["stat_price_series"] = {
        iid: {
            "token_id": identities[iid],
            "time_basis": "CLOSE_TIME",
            "times_us": array("q", [micros(at) + microseconds for at in sorted(values)]),
            "values": array("d", [values[at] for at in sorted(values)]),
        }
        for iid, values in truth._price_groups(packet).items()
    }
    end = parse_time(packet["cutoff"])
    start = end - timedelta(hours=4) + timedelta(seconds=17)
    before = deepcopy(packet)

    def compute(inputs):
        token = truth._ASSESSMENT_INPUTS.set(inputs)
        try:
            with localcontext() as context:
                context.prec = 128
                context.clear_flags()
                context.flags[Rounded] = True
                result = truth.relative_truth(packet, start, end)
                return result, context.prec, dict(context.flags)
        finally:
            truth._ASSESSMENT_INPUTS.reset(token)

    assert compute({}) == compute(None)
    assert packet == before


@pytest.mark.parametrize(
    "case", ["duplicate", "mismatch", "nonfinite", "callback", "custom_id", "custom_field"]
)
def test_unsupported_price_columns_retain_legacy_conversion_and_flags(case):
    packet = price_packet()
    projected = packet["stat_price_series"]["A:spot:T"]
    calls = []

    class Price:
        def __float__(self):
            calls.append("price")
            return 107.0

    class Key(str):
        def __hash__(self):
            calls.append("hash")
            return str.__hash__(self)

        def __eq__(self, other):
            calls.append("equal")
            return str.__eq__(self, other)

    if case == "duplicate":
        projected["times_us"][1] = projected["times_us"][0]
    elif case == "mismatch":
        projected["values"].pop()
    elif case == "nonfinite":
        projected["values"][1] = float("nan")
    elif case == "custom_id":
        packet["stat_price_series"] = {Key("A:spot:T"): projected}
    elif case == "custom_field":
        values = projected.pop("times_us")
        projected[Key("times_us")] = values
    else:
        projected["values"] = [Price(), 102.0, 103.0, 104.0, 105.0, 106.0]

    def compute(inputs):
        calls.clear()
        token = truth._ASSESSMENT_INPUTS.set(inputs)
        try:
            with localcontext() as context:
                context.prec = 128
                context.clear_flags()
                context.flags[Rounded] = True
                result = {iid: dict(values) for iid, values in truth._prices(packet).items()}
                return result, list(calls), context.prec, dict(context.flags)
        finally:
            truth._ASSESSMENT_INPUTS.reset(token)

    assert compute({}) == compute(None)
