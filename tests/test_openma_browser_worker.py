"""Deterministic regression checks for the real sandbox worker's request budget."""

import base64
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def transport(monkeypatch, tmp_path):
    path = Path(__file__).resolve().parents[1] / "deploy/openma_browser/browser-worker.py"
    spec = importlib.util.spec_from_file_location("tank_browser_worker", path)
    worker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(worker)
    clock = SimpleNamespace(now=0.0)
    options = SimpleNamespace(ready_at=18.0, exit_code=None, response_delay=0.0, chunks=[b'{"ok":true}\n'])
    attempts, sockets, spawned = [], [], []
    monkeypatch.setattr(worker.time, "monotonic", lambda: clock.now)
    monkeypatch.setattr(worker.time, "sleep", lambda seconds: setattr(clock, "now", clock.now + seconds))
    monkeypatch.setattr(worker, "paths", lambda state: (tmp_path / "rpc.sock", tmp_path / "start.lock"))

    class Socket:
        def __init__(self, *_args):
            self.timeouts = []
            self.chunks = list(options.chunks)
            sockets.append(self)

        def settimeout(self, timeout):
            self.timeout = timeout
            self.timeouts.append(timeout)

        def connect(self, _path):
            attempts.append(clock.now)
            if clock.now < options.ready_at:
                raise FileNotFoundError

        def sendall(self, data):
            options.written = json.loads(data)
            assert options.written["op"] == "open"

        def recv(self, _size):
            elapsed = min(self.timeout, options.response_delay)
            clock.now += elapsed
            if options.response_delay >= self.timeout:
                raise TimeoutError("socket response deadline")
            return self.chunks.pop(0) if self.chunks else b""

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.close()

    monkeypatch.setattr(worker.socket, "socket", Socket)

    def spawn(*args, **kwargs):
        spawned.append((args, kwargs))
        return SimpleNamespace(poll=lambda: options.exit_code)

    monkeypatch.setattr(worker.subprocess, "Popen", spawn)
    encoded = base64.b64encode(json.dumps({"version": 1, "op": "open"}).encode()).decode()
    return SimpleNamespace(
        worker=worker,
        clock=clock,
        options=options,
        attempts=attempts,
        sockets=sockets,
        spawned=spawned,
        call=lambda: worker.request(tmp_path, encoded),
    )


def test_healthy_microvm_startup_over_fifteen_seconds_uses_remaining_rpc_budget(transport, capsys):
    transport.call()
    assert capsys.readouterr().out == transport.worker.REPLY_PREFIX + '{"ok":true}\n'
    assert transport.clock.now >= 18
    assert len(transport.spawned) == 1
    assert transport.sockets[-1].timeouts[-1] == pytest.approx(80 - transport.clock.now)


def test_slow_response_does_not_receive_a_second_full_timeout(transport):
    transport.options.response_delay = 65
    with pytest.raises(TimeoutError):
        transport.call()
    assert transport.clock.now == pytest.approx(80)


def test_partial_response_chunks_share_the_same_absolute_deadline(transport):
    transport.options.ready_at = 0
    transport.options.response_delay = 30
    transport.options.chunks = [b'{"ok":', b"true", b"}\n"]
    with pytest.raises(TimeoutError):
        transport.call()
    assert transport.clock.now == pytest.approx(80)


def test_startup_lock_wait_is_bounded_by_the_request_budget(transport, monkeypatch):
    def locked(*_args):
        raise BlockingIOError

    monkeypatch.setattr(transport.worker.fcntl, "flock", locked)
    with pytest.raises(TimeoutError, match="startup lock"):
        transport.call()
    assert transport.clock.now == pytest.approx(75)
    assert transport.spawned == []


def test_dead_startup_process_reports_exit_without_waiting_out_budget(transport):
    transport.options.exit_code = 7
    with pytest.raises(RuntimeError, match=r"exited during startup \(exit 7\)"):
        transport.call()
    assert transport.clock.now == 0


def test_daemon_receives_the_original_absolute_budget_after_slow_startup(transport):
    transport.call()
    assert transport.clock.now >= 18
    assert transport.options.written["deadline"] == 75


def test_response_grace_never_extends_the_daemon_operation_budget(transport, capsys):
    transport.options.response_delay = 60
    transport.options.chunks = [b'{"version":1,"ok":false,"error":"worker recycled"}\n']
    transport.call()
    assert transport.options.written["deadline"] == 75
    assert transport.clock.now == pytest.approx(78)
    assert '"error":"worker recycled"' in capsys.readouterr().out
    assert transport.worker.REQUEST_TIMEOUT + transport.worker.RESPONSE_GRACE < 90
