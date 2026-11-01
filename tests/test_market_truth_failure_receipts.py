import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import market_truth_request_worker as worker
import pytest
from test_market_truth_request import receive


@pytest.mark.parametrize("depth", [0, 24])
def test_worker_failure_retains_bounded_root_and_transport_frames_without_error_text(depth):
    def fail(remaining):
        if remaining:
            return fail(remaining - 1)
        raise ValueError("private request text must stay local")

    with pytest.raises(ValueError) as caught:
        fail(depth)
    frames, trace = [], caught.value.__traceback__
    while trace:
        frames.append([Path(trace.tb_frame.f_code.co_filename).name, trace.tb_lineno])
        trace = trace.tb_next
    actual = worker.failure_receipt(caught.value)
    expected = frames if len(frames) <= 16 else frames[:8] + frames[-8:]
    assert actual == {
        "failure": "ValueError",
        "frames": expected,
        "frames_omitted": max(0, len(frames) - 16),
    }
    assert "private request text" not in json.dumps(actual)


async def test_worker_receive_preserves_complete_failure_receipt_as_json():
    receipt = {"failure": "RemoteProtocolError", "frames": [["transport.py", index] for index in range(16)]}
    reader = asyncio.StreamReader()
    reader.feed_data(json.dumps(receipt).encode() + b"\n")
    reader.feed_eof()
    with pytest.raises(AssertionError) as caught:
        await receive(SimpleNamespace(stdout=reader))
    assert caught.value.args == (json.dumps(receipt, sort_keys=True),)


def test_return_boundary_failure_retains_safe_peer_diagnostics_without_error_text():
    try:
        raise ValueError("private peer error text")
    except ValueError as error:
        peer = worker.failure_receipt(error)
    actual = worker.failure_receipt(worker.ReturnBoundaryError([peer]))
    assert actual["failure"] == "ReturnBoundaryError"
    assert actual["return_boundary_failures"] == [peer]
    assert "private peer error text" not in json.dumps(actual)
