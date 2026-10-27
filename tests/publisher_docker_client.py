"""Tiny stdlib client; actual command/state validation lives in the owned backend."""

import os
import sys
from pathlib import Path

from publisher_stand_protocol import (
    CommandReply,
    CommandRequest,
    ProtocolError,
    ReadyReply,
    ReadyRequest,
    command,
    exchange,
    read_endpoint,
)


def main() -> int:
    try:
        endpoint = read_endpoint(Path(os.environ["TANK_PUBLISHER_DOCKER_STAND"]))
        ready = exchange(endpoint, ReadyRequest(endpoint.nonce))
        if not isinstance(ready, ReadyReply):
            raise ProtocolError("Actual owned backend readiness required")
        reply = exchange(endpoint, CommandRequest(endpoint.nonce, command(sys.argv[1]), tuple(sys.argv[2:])))
        if not isinstance(reply, CommandReply):
            raise ProtocolError("Stand command reply required")
        sys.stdout.write(reply.stdout)
        sys.stderr.write(reply.stderr)
        return reply.code
    except (OSError, ValueError, KeyError, IndexError):
        print("Owned Docker stand transport refused", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
