"""VM-only bounded regular-file I/O; no subprocesses or host filesystem access."""

import base64
import errno
import hashlib
import json
import os
import stat
import sys
from pathlib import Path

LIMIT = 512 * 1024 * 1024
CHUNK = 64 * 1024


def line(value):
    sys.stdout.write(json.dumps(value, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def main():
    operation, path = sys.argv[1:3]
    header = {"protocol": "tank-file-io", "version": 1, "operation": operation}
    opened = False
    try:
        if operation == "read":
            with os.fdopen(os.open(path, os.O_RDONLY | os.O_NONBLOCK), "rb", buffering=0) as source:
                info = os.fstat(source.fileno())
                if not stat.S_ISREG(info.st_mode):
                    raise OSError(errno.EINVAL, "regular_file_required")
                if info.st_size > LIMIT:
                    raise OSError(errno.EFBIG, "file_too_large")
                line({**header, "ok": True, "size": info.st_size})
                opened = True
                total = 0
                digest = hashlib.sha256()
                while True:
                    chunk = source.read(min(CHUNK, LIMIT - total + 1))
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > LIMIT or total > info.st_size:
                        raise OSError(errno.EFBIG, "file_changed_or_too_large")
                    digest.update(chunk)
                    sys.stdout.write("D " + base64.b64encode(chunk).decode("ascii") + "\n")
                    sys.stdout.flush()
                    if sys.stdin.buffer.read(2) != b"A\n":
                        raise OSError(errno.EIO, "missing_read_ack")
                if total != info.st_size:
                    raise OSError(errno.EIO, "file_changed")
                line({"done": True, "bytes": total, "sha256": digest.hexdigest()})
        elif operation == "write":
            size = int(sys.argv[3])
            if not 0 <= size <= LIMIT:
                raise OSError(errno.EFBIG, "file_too_large")

            Path(path).parent.mkdir(parents=True, exist_ok=True)
            with os.fdopen(
                os.open(path, os.O_WRONLY | os.O_CREAT | os.O_NONBLOCK, 0o666), "wb", buffering=0
            ) as destination:
                if not stat.S_ISREG(os.fstat(destination.fileno()).st_mode):
                    raise OSError(errno.EINVAL, "regular_file_required")
                os.ftruncate(destination.fileno(), 0)
                line({**header, "ok": True, "size": size})
                opened = True
                total = 0
                digest = hashlib.sha256()
                while total < size:
                    chunk = sys.stdin.buffer.read(min(CHUNK, size - total))
                    if not chunk:
                        raise OSError(errno.EIO, "truncated_input")
                    view = memoryview(chunk)
                    while view:
                        written = destination.write(view)
                        if not written:
                            raise OSError(errno.EIO, "short_write")
                        view = view[written:]
                    digest.update(chunk)
                    total += len(chunk)
                    line({"ack": total})
                if sys.stdin.buffer.read(1):
                    raise OSError(errno.EFBIG, "unexpected_input")
                line({"done": True, "bytes": total, "sha256": digest.hexdigest()})
        else:
            raise OSError(errno.EINVAL, "invalid_operation")
        return 0
    except OSError as error:
        line(({"done": False} if opened else {**header, "ok": False}) | {"errno": error.errno or errno.EIO})
        return 1
    except (ValueError, IndexError):
        line({**header, "ok": False, "errno": errno.EINVAL})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
