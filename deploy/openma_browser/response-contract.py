"""Real Unix-socket regression for bounded receipt after operation expiry.

Synthetic daemon operation/cleanup timing; no browser, VM, credentials or source edits.
"""

import base64
import importlib.util
import json
import socket
import sys
import tempfile
import threading
import time
from pathlib import Path

spec = importlib.util.spec_from_file_location("worker", sys.argv[1])
worker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(worker)
worker.REQUEST_TIMEOUT = 0.15
worker.RESPONSE_GRACE = 0.08


def exercise(delays, chunks):
    state = Path(tempfile.mkdtemp(prefix="tank-response-contract-"))
    sockpath, lockpath = worker.paths(state)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(sockpath))
    server.listen(1)
    observed = {}
    failures = []

    def serve():
        try:
            with server.accept()[0] as peer:
                request = json.loads(peer.recv(4096))
                observed["operation_deadline"] = request["deadline"]
                time.sleep(max(0, request["deadline"] - time.monotonic()))
                observed["operation_expired"] = True
                for delay, chunk in zip(delays, chunks, strict=True):
                    time.sleep(delay)
                    try:
                        peer.sendall(chunk)
                    except BrokenPipeError:
                        observed["client_closed"] = True
                        break
        except Exception as error:  # noqa: BLE001 - propagate fixture thread failures to the caller.
            failures.append(error)

    thread = threading.Thread(target=serve)
    thread.start()
    start = time.monotonic()
    try:
        worker.request(state, base64.b64encode(b'{"version":1,"op":"open","args":{}}').decode())
        observed["result"] = "reply"
    except TimeoutError:
        observed["result"] = "bounded timeout"
    observed["elapsed"] = time.monotonic() - start
    thread.join(timeout=2)
    assert not thread.is_alive() and not failures, failures
    server.close()
    sockpath.unlink()
    lockpath.unlink()
    sockpath.parent.rmdir()
    state.rmdir()
    assert observed["operation_expired"]
    assert 0.14 <= observed["operation_deadline"] - start <= 0.17
    return observed


reply = b'{"version":1,"ok":false,"error":"worker recycled"}\n'
received = exercise([0.03], [reply])
assert received["result"] == "reply" and received["elapsed"] < 0.23, received
slow = exercise([0.15], [reply])
assert slow["result"] == "bounded timeout" and 0.21 <= slow["elapsed"] < 0.3, slow
partial = exercise([0.02, 0.04, 0.06], [b'{"version":1,', b'"ok":false,', b'"error":"late"}\n'])
assert partial["result"] == "bounded timeout" and 0.21 <= partial["elapsed"] < 0.3, partial
print(
    json.dumps(
        {
            "structured_expiry_reply": received,
            "cleanup_exceeds_grace": slow,
            "partial_chunks_share_absolute_grace": partial,
            "operation_budget_unchanged": True,
            "resends": 0,
        }
    )
)
