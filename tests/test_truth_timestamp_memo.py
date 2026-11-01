from datetime import UTC, datetime, timedelta
from functools import lru_cache

import pytest

from tank import util


def test_assessment_timestamp_memo_prevents_reparse_after_global_lru_eviction(monkeypatch):
    parse = util._parse_text_time.__wrapped__
    parsed = []

    @lru_cache(maxsize=2)
    def bounded(value):
        parsed.append(value)
        return parse(value)

    monkeypatch.setattr(util, "_parse_text_time", bounded)
    values = [datetime(2026, 10, 28, tzinfo=UTC) + timedelta(seconds=index) for index in range(3)]
    texts = [value.isoformat() for value in values]
    memo = {}
    token = util._TIME_PARSE_MEMO.set(memo)
    try:
        for _ in range(2):
            assert [util.parse_time(value) for value in texts] == values
        assert parsed == texts
        assert len(memo) == 3
        for _ in range(2):
            with pytest.raises(ValueError):
                util.parse_time("invalid-timestamp")
        assert "invalid-timestamp" not in memo
    finally:
        util._TIME_PARSE_MEMO.reset(token)


def test_assessment_timestamp_memo_is_bounded_and_preserves_all_results():
    start, memo = datetime(2026, 10, 28, tzinfo=UTC), {}
    token = util._TIME_PARSE_MEMO.set(memo)
    try:
        for index in range(65540):
            expected = start + timedelta(seconds=index)
            assert util.parse_time(expected.isoformat()) == expected
        assert len(memo) == 65536
    finally:
        util._TIME_PARSE_MEMO.reset(token)


def test_assessment_timestamp_memo_keeps_custom_string_behavior():
    calls = []

    class Text(str):
        def __str__(self):
            calls.append(1)
            return f"2026-10-28T00:00:0{len(calls)}Z"

    memo = {}
    token = util._TIME_PARSE_MEMO.set(memo)
    try:
        value = Text("2026-10-28T00:00:00Z")
        assert util.parse_time(value).second == 1
        assert util.parse_time(value).second == 2
        assert len(calls) == 2 and not memo
    finally:
        util._TIME_PARSE_MEMO.reset(token)
