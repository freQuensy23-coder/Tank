import gc
import weakref
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Rounded, localcontext

import pytest

from tank.analysis import truth


def row_retainers(roots, rows):
    row_ids = {id(row) for row in rows}
    pending, visited, retainers = list(roots), set(), []
    while pending:
        value = pending.pop()
        identity = id(value)
        if identity in visited or identity in row_ids or type(value) not in (dict, list, tuple):
            continue
        visited.add(identity)
        references = gc.get_referents(value)
        if any(id(reference) in row_ids for reference in references):
            retainers.append(value)
        pending.extend(references)
    return retainers


def test_assessment_row_memos_share_one_retention_owner():
    base = datetime(2026, 10, 28, tzinfo=UTC)
    moments = [base + timedelta(minutes=index) for index in range(128)]
    rows = []
    for index, moment in enumerate(moments):
        row = {
            "ts": moment.isoformat(),
            "received_ts": (moment + timedelta(seconds=10)).isoformat(),
            "metadata": {"source": "captured"},
        }
        if index % 2:
            row.update(ts_prec="BUCKET(60s)", ts_bucket_size_s=60, ts_bucket_anchor="UNSPECIFIED")
        rows.append(row)
    original = deepcopy(rows)
    inputs, times = {}, {}
    input_token = truth._ASSESSMENT_INPUTS.set(inputs)
    time_token = truth._TIMES.set(times)
    try:
        with localcontext() as context:
            context.prec = 128
            context.clear_flags()
            context.flags[Rounded] = True
            before = dict(context.flags)
            for minutes in (32, 97, 32):
                assembled = base + timedelta(minutes=minutes)
                snapshot = {"assembled_at": assembled.isoformat()}
                for index, (row, moment) in enumerate(zip(rows, moments, strict=True)):
                    assert truth._time(row) == moment
                    assert truth._available(row, snapshot) is (moment + timedelta(seconds=10) <= assembled)
                    assert truth.oi_temporally_admissible(row) is (index % 2 == 0)
            assert context.prec == 128 and dict(context.flags) == before
        assert rows == original
        assert len(row_retainers((inputs, times), rows)) <= 1
    finally:
        truth._TIMES.reset(time_token)
        truth._ASSESSMENT_INPUTS.reset(input_token)


@pytest.mark.parametrize("assessment", [False, True])
def test_time_memo_retains_original_row_until_its_scope_ends(assessment):
    class Row(dict):
        pass

    inputs, times = {}, {}
    input_token = truth._ASSESSMENT_INPUTS.set(inputs if assessment else None)
    time_token = truth._TIMES.set(times)
    try:
        row = Row(ts="2026-10-28T00:00:00+00:00")
        reference = weakref.ref(row)
        assert truth._time(row) == datetime(2026, 10, 28, tzinfo=UTC)
        del row
        assert reference() is not None
    finally:
        truth._TIMES.reset(time_token)
        truth._ASSESSMENT_INPUTS.reset(input_token)
    del inputs, times
    assert reference() is None


def test_invalid_received_time_does_not_publish_a_row_owner():
    row = {"ts": "2026-10-28T00:00:00+00:00", "received_ts": "invalid-time"}
    inputs, times = {}, {}
    input_token = truth._ASSESSMENT_INPUTS.set(inputs)
    time_token = truth._TIMES.set(times)
    try:
        with pytest.raises(ValueError):
            truth._available(row, {"assembled_at": "2026-10-28T00:00:00+00:00"})
        assert row_retainers((inputs, times), [row]) == []
    finally:
        truth._TIMES.reset(time_token)
        truth._ASSESSMENT_INPUTS.reset(input_token)
