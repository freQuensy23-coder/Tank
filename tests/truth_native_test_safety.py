import os
import re
from contextlib import asynccontextmanager
from urllib.parse import urlsplit

NATIVE_URL_REFUSAL = "native SQL qualification requires an explicit loopback HTTP(S) service"


def require_loopback_native_url() -> str:
    url = os.environ.get("CLICKHOUSE_TEST_URL")
    if not url:
        raise ValueError(NATIVE_URL_REFUSAL) from None
    valid = False
    if not any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in url):
        try:
            parsed = urlsplit(url)
            authority = parsed.netloc
            userinfo, separator, host = authority.rpartition("@")
            if not separator:
                host = authority
            valid = (
                parsed.scheme in {"http", "https"}
                and parsed.hostname in {"localhost", "127.0.0.1", "::1"}
                and re.fullmatch(r"(?:localhost|127\.0\.0\.1|\[::1\])(?::[0-9]+)?", host, re.IGNORECASE) is not None
                and (parsed.port is None or 1 <= parsed.port <= 65535)
                and not parsed.fragment
                and "\\" not in url
                and re.search(r"%(?![0-9a-fA-F]{2})", url) is None
                and (
                    not separator
                    or (
                        authority.count("@") == 1
                        and bool(userinfo.split(":", 1)[0])
                        and re.fullmatch(r"[A-Za-z0-9._~!$&'()*+,;=:%-]+", userinfo) is not None
                    )
                )
            )
        except ValueError:
            valid = False
    if not valid:
        raise ValueError(NATIVE_URL_REFUSAL) from None
    return url


@asynccontextmanager
async def owned_native_repository(template, dsn):
    require_loopback_native_url()
    async with template.repository(dsn) as repository:
        yield repository
