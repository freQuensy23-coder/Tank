"""Read the single host credential from an explicitly owned private file."""

import os
import re
import stat
from pathlib import Path

from pydantic import SecretStr


class PublisherCredentialError(RuntimeError):
    pass


def host_token(path: Path, *, owner_uid: int) -> SecretStr:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
        metadata = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != owner_uid
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise PublisherCredentialError("Publisher credential file must be owned and private")
        values: list[SecretStr] = []
        for line in stream:
            match = re.fullmatch(r"\s*(?:export\s+)?APIFY_API_TOKEN\s*=\s*(.*?)\s*", line.rstrip("\n"))
            if match is None:
                continue
            value = match[1]
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            if re.fullmatch(r"[A-Za-z0-9_-]+", value) is None:
                raise PublisherCredentialError("Unsupported publisher credential value")
            values.append(SecretStr(value))
    if len(values) != 1:
        raise PublisherCredentialError("Host file must define publisher credential exactly once")
    return values[0]
