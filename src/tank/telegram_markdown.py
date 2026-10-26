"""Agent Markdown rendered as Telegram message entities.

Entities carry explicit offsets instead of markup, so Telegram never has to parse anything and
cannot reject a message for malformed formatting. Markup that does not form a complete span
(an unclosed ``**``, a lone backtick, ``snake_case``) stays in the text as written.
"""

import re

ESCAPABLE = set("\\`*_~[]()#>-|!.+")
LINK_SCHEMES = ("http://", "https://", "tg://")
FENCE = re.compile(r"^\s*(`{3,}|~{3,})\s*([\w+#.-]*)\s*$")
HEADING = re.compile(r"^\s{0,3}#{1,6}\s+(.*?)\s*#*\s*$")
BULLET = re.compile(r"^(\s*)[-*+]\s+(.*)$")
QUOTE = re.compile(r"^\s{0,3}>\s?(.*)$")
PAIRS = (("**", "bold"), ("__", "bold"), ("~~", "strikethrough"), ("*", "italic"), ("_", "italic"))


def utf16_length(text):
    return len(text.encode("utf-16-le")) // 2


def _closing(line, start, marker):
    """Where a span opened at start with marker closes on this line, or -1."""
    size = len(marker)
    j = start + size
    while True:
        j = line.find(marker, j)
        if j < 0:
            return -1
        if size == 1 and (line[j - 1 : j] == marker or line[j + 1 : j + 2] == marker):
            j += 1
            while line[j : j + 1] == marker:
                j += 1
            continue
        word_inside = marker[0] == "_" and line[j + size : j + size + 1].isalnum()
        if j > start + size and not line[j - 1].isspace() and not word_inside:
            return j
        j += 1


def _inline(line):
    """Plain text and code-point entities (type, start, end, extra) of one line of inline Markdown."""
    out, entities, i = [], [], 0

    def emit(text, kind=None, extra=None, nested=()):
        start = sum(map(len, out))
        out.append(text)
        if kind:
            entities.append((kind, start, start + len(text), extra))
        entities.extend((k, s + start, e + start, x) for k, s, e, x in nested)

    while i < len(line):
        char = line[i]
        if char == "\\" and line[i + 1 : i + 2] in ESCAPABLE:
            emit(line[i + 1])
            i += 2
            continue
        if char == "`":
            run = len(line[i:]) - len(line[i:].lstrip("`"))
            end = line.find("`" * run, i + run)
            if end > i + run:
                code = line[i + run : end]
                if code.strip():
                    emit(code.strip() if code.startswith(" ") and code.endswith(" ") else code, "code")
                    i = end + run
                    continue
            emit(line[i : i + run])
            i += run
            continue
        if char == "[":
            close = line.find("](", i + 1)
            end = line.find(")", close + 2) if close > i + 1 else -1
            url = line[close + 2 : end].strip() if end > 0 else ""
            if end > 0 and url.startswith(LINK_SCHEMES) and " " not in url and "[" not in line[i + 1 : close]:
                text, nested = _inline(line[i + 1 : close])
                if text:
                    emit(text, "text_link", {"url": url}, nested)
                    i = end + 1
                    continue
        matched = False
        for marker, kind in PAIRS:
            if not line.startswith(marker, i):
                continue
            after = line[i + len(marker) : i + len(marker) + 1]
            if not after or after.isspace() or after == marker[0]:
                break
            if marker[0] == "_" and i > 0 and line[i - 1].isalnum():
                break
            end = _closing(line, i, marker)
            if end < 0:
                break
            text, nested = _inline(line[i + len(marker) : end])
            if text:
                emit(text, kind, None, nested)
                i = end + len(marker)
                matched = True
            break
        if matched:
            continue
        run = 1
        if char in "*_~":
            while line[i + run : i + run + 1] == char:
                run += 1
        emit(line[i : i + run])
        i += run
    return "".join(out), entities


def render(markdown):
    """Telegram text and entities for agent Markdown; text without markup gets no entities."""
    out, entities = [], []
    length = 0

    def add(text, found=()):
        nonlocal length
        out.append(text)
        entities.extend((k, s + length, e + length, x) for k, s, e, x in found)
        length += len(text)

    lines = markdown.replace("\r\n", "\n").split("\n")
    quote_start = None
    i = 0
    while i < len(lines):
        line = lines[i]
        fence = FENCE.match(line)
        if fence:
            close = next(
                (
                    j
                    for j in range(i + 1, len(lines))
                    if lines[j].strip().startswith(fence[1]) and not lines[j].strip().strip(fence[1][0])
                ),
                None,
            )
            if close is not None:
                body = "\n".join(lines[i + 1 : close])
                if body.strip():
                    extra = {"language": fence[2]} if fence[2] else None
                    add(body, [("pre", 0, len(body), extra)])
                i = close + 1
                if i < len(lines):
                    add("\n")
                continue
        quote = QUOTE.match(line)
        if quote and quote_start is None:
            quote_start = length
        elif not quote and quote_start is not None:
            if length - 1 > quote_start:
                entities.append(("blockquote", quote_start, length - 1, None))
            quote_start = None
        if quote:
            line = quote[1]
        heading = HEADING.match(line)
        bullet = BULLET.match(line)
        if heading:
            text, found = _inline(heading[1])
            add(text, [*found, ("bold", 0, len(text), None)] if text else found)
        elif bullet:
            text, found = _inline(bullet[2])
            prefix = bullet[1] + "• "
            add(prefix + text, [(k, s + len(prefix), e + len(prefix), x) for k, s, e, x in found])
        else:
            add(*_inline(line))
        i += 1
        if i < len(lines):
            add("\n")
    if quote_start is not None and length > quote_start:
        entities.append(("blockquote", quote_start, length, None))
    return _finish("".join(out), entities)


def _finish(text, entities):
    """Strip the text as Telegram would and convert offsets to UTF-16 code units."""
    lead = len(text) - len(text.lstrip())
    text = text.strip()
    result = []
    for kind, start, end, extra in entities:
        start, end = max(start - lead, 0), min(end - lead, len(text))
        while kind != "pre" and start < end and text[start].isspace():
            start += 1
        while kind != "pre" and end > start and text[end - 1].isspace():
            end -= 1
        if end <= start:
            continue
        offset = utf16_length(text[:start])
        entity = {"type": kind, "offset": offset, "length": utf16_length(text[start:end])}
        result.append({**entity, **(extra or {})})
    result.sort(key=lambda entity: (entity["offset"], -entity["length"]))
    return text, result
