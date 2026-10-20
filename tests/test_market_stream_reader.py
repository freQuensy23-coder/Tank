"""The stream reader keeps draining the socket so keepalive pongs are always read."""

import asyncio

import pytest

from tank.market import streams


class Socket:
    def __init__(self, frames, error=None):
        self.frames, self.error = list(frames), error

    async def recv(self):
        if self.frames:
            return self.frames.pop(0)
        await asyncio.sleep(0)
        raise self.error or ConnectionError("closed")


@pytest.mark.asyncio
async def test_reader_moves_every_frame_and_hands_over_the_closing_error():
    inbox = asyncio.Queue()
    await streams.drain_socket(Socket(["a", "b", "c"], error=RuntimeError("1011")), inbox)
    items = [inbox.get_nowait() for _ in range(inbox.qsize())]
    assert items[:3] == ["a", "b", "c"]
    assert isinstance(items[3], RuntimeError) and str(items[3]) == "1011"


@pytest.mark.asyncio
async def test_reader_backlog_beyond_limit_is_an_explicit_overload(monkeypatch):
    monkeypatch.setattr(streams, "STREAM_BACKLOG_FRAMES", 5)
    inbox = asyncio.Queue()
    await streams.drain_socket(Socket(str(i) for i in range(10)), inbox)
    items = [inbox.get_nowait() for _ in range(inbox.qsize())]
    assert items[:6] == [str(i) for i in range(6)]
    assert isinstance(items[-1], streams.SemanticError) and "STREAM_BACKLOG_EXCEEDED" in str(items[-1])
