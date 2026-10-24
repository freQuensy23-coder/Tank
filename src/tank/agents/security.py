"""Keep credentials out of durable tool arguments and evidence URLs."""

import re
from re import Match
from urllib.parse import parse_qsl, quote, quote_plus, urlencode, urlsplit, urlunsplit

from pydantic import JsonValue

_REDACTED = "[REDACTED]"
_AUTH_KEYS = {"api_key", "apikey", "access_token", "password", "secret", "auth", "authorization"}


def redact_tool_arguments(
    arguments: dict[str, JsonValue], secrets: frozenset[str]
) -> tuple[dict[str, JsonValue], str | None]:
    """Copy JSON tool input, redacting credentials supplied by the settings boundary."""
    found = False

    def redact(value: JsonValue) -> JsonValue:
        nonlocal found
        if isinstance(value, dict):
            children: dict[str, JsonValue] = {}
            for key, child in value.items():
                if str(key).casefold() in _AUTH_KEYS and child not in (None, ""):
                    children[key] = _REDACTED
                    found = True
                else:
                    children[key] = redact(child)
            return children
        if isinstance(value, list):
            return [redact(child) for child in value]
        if not isinstance(value, str):
            return value

        def hide_userinfo(match: Match[str]) -> str:
            nonlocal found
            found = True
            return match.group(1) + _REDACTED + "@"

        clean = re.sub(r"([a-zA-Z][a-zA-Z0-9+.-]*://)[^/\s?#@]+@", hide_userinfo, value)
        try:
            parts = urlsplit(clean)
            if parts.scheme.casefold() in {"http", "https"} and parts.netloc:
                pairs = parse_qsl(parts.query, keep_blank_values=True)
                if any(key.casefold() in _AUTH_KEYS | {"token"} for key, _ in pairs):
                    pairs = [
                        (key, _REDACTED if key.casefold() in _AUTH_KEYS | {"token"} else child)
                        for key, child in pairs
                    ]
                    clean = urlunsplit(parts._replace(query=urlencode(pairs)))
                    found = True
        except ValueError:
            pass
        secret_forms = {
            form for secret in secrets for form in (secret, quote(secret, safe=""), quote_plus(secret))
        }
        for secret in sorted(secret_forms, key=len, reverse=True):
            if secret in clean:
                clean = clean.replace(secret, _REDACTED)
                found = True
        return clean

    sanitized: dict[str, JsonValue] = {}
    for key, value in arguments.items():
        if key.casefold() in _AUTH_KEYS and value not in (None, ""):
            sanitized[key] = _REDACTED
            found = True
        else:
            sanitized[key] = redact(value)
    return (
        sanitized,
        "Tool arguments contain credentials; provide public evidence without authentication."
        if found
        else None,
    )
