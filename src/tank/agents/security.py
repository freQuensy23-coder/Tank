"""Keep credentials out of durable tool arguments and evidence URLs."""

import copy
import re
from urllib.parse import parse_qsl, quote, quote_plus, unquote, urlencode, urlsplit, urlunsplit

_REDACTED = "[REDACTED]"
_AUTH_KEYS = {"api_key", "apikey", "access_token", "password", "secret", "auth", "authorization"}
_SECRET_SETTING = re.compile(r"(?:api_?key|password|secret|_token|_key)$", re.IGNORECASE)


def redact_tool_arguments(arguments, settings):
    """Return a detached JSON-shaped copy and a safe refusal reason if sensitive.

    A plain domain field named ``token`` is valid. It is credential-bearing only
    in a URL query, where evidence-fetch URLs must not carry authentication.
    Only configured scalar secret values are inspected; no auth files are read.
    """
    configured = vars(settings) if hasattr(settings, "__dict__") else {}
    secrets = {
        value
        for key, value in configured.items()
        if _SECRET_SETTING.search(key) and isinstance(value, str) and value
    }

    for key, value in configured.items():
        if key.endswith("_url") and isinstance(value, str):
            try:
                parsed = urlsplit(value)
                if parsed.password:
                    secrets.add(unquote(parsed.password))
            except ValueError:
                pass
    found = False

    def redact(value):
        nonlocal found
        if isinstance(value, dict):
            clean = {}
            for key, child in value.items():
                if str(key).casefold() in _AUTH_KEYS and child not in (None, ""):
                    clean[key] = _REDACTED
                    found = True
                else:
                    clean[key] = redact(child)
            return clean
        if isinstance(value, list):
            return [redact(child) for child in value]
        if not isinstance(value, str):
            return value

        def hide_userinfo(match):
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

    sanitized = redact(copy.deepcopy(arguments))
    return (
        sanitized,
        "Tool arguments contain credentials; provide public evidence without authentication."
        if found
        else None,
    )
