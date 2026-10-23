"""Native Telegram Rich Messages and their readable text representation."""

SCORE_DIMENSIONS = (
    ("Economic directness", "economic_directness"),
    ("Magnitude", "magnitude"),
    ("Permanence", "permanence"),
    ("Distribution expansion", "distribution_expansion"),
    ("Mechanical flows", "mechanical_flows"),
    ("Surprise", "surprise"),
    ("Implementation", "implementation"),
)
RICH_TEXT_LIMIT = 32768
CARD_TEXT_LIMIT = 5000
CARD_LABELS = (
    "WHAT",
    "VERIFIED",
    "NOT VERIFIED",
    "WHO",
    "MARKET TRUTH",
    "OUTLOOK",
    "WHAT TO DO",
    "WHAT CHANGES THE ACTION",
    "CONFIDENCE",
)


def rich_text(value):
    """Read strings and nested Telegram RichText without interpreting markup."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "".join(rich_text(part) for part in value)
    if isinstance(value, dict):
        for key in ("text", "alternative_text", "expression"):
            if key in value:
                return rich_text(value[key])
    return ""


def rich_message_text(message):
    """Flatten native blocks for forwarded-message matching and agent context."""
    lines = []
    for block in message.get("blocks") or []:
        if block.get("type") == "table":
            lines.extend(
                " | ".join(rich_text(cell.get("text")) for cell in row) for row in block.get("cells") or []
            )
        elif block.get("type") == "list":
            for item in block.get("items") or []:
                label = item.get("label") or ""
                lines.append((label + " " + rich_message_text(item)).strip())
        elif block.get("blocks") is not None:
            if block.get("summary"):
                lines.append(rich_text(block["summary"]))
            lines.append(rich_message_text(block))
        else:
            lines.append(rich_text(block.get("text") or block.get("expression")))
        caption = block.get("caption")
        if caption:
            lines.append(rich_text(caption))
    return "\n".join(line for line in lines if line)


def message_text(message):
    return (
        rich_message_text(message.get("rich_message") or {})
        or message.get("text")
        or message.get("caption")
        or ""
    )


def score_summary(assessment):
    parts = []
    for code, (_, key) in zip("DMPXFSI", SCORE_DIMENSIONS, strict=True):
        dimension = (assessment.get("dims") or {}).get(key) or {}
        value = dimension.get("value")
        reason = (
            dimension.get("rationale")
            or str(dimension.get("reason") or "estimate not supplied").replace("_", " ").lower()
        )
        parts.append(code + str(value) if value is not None else code + " n/a (" + reason + ")")
    return "Scores " + " · ".join(parts)


def score_table(assessment):
    cells = [
        [
            {"text": label, "is_header": True, "align": "left", "valign": "top"}
            for label in ("Dimension", "Score / 10", "Reason")
        ]
    ]
    for label, key in SCORE_DIMENSIONS:
        dimension = (assessment.get("dims") or {}).get(key) or {}
        value = dimension.get("value")
        reason = (
            dimension.get("rationale")
            or str(dimension.get("reason") or "Explanation not supplied").replace("_", " ").lower()
        )
        cells.append(
            [
                {"text": label, "align": "left", "valign": "top"},
                {"text": "Not assessed" if value is None else str(value), "align": "center", "valign": "top"},
                {"text": reason, "align": "left", "valign": "top"},
            ]
        )
    return {
        "type": "table",
        "is_bordered": True,
        "is_striped": True,
        "is_compact": True,
        "cells": cells,
    }


def card_rich_message(text, assessment, *, max_chars=CARD_TEXT_LIMIT):
    """Freeze the card with one native score table; all agent text stays literal."""
    marker = "\n" + score_summary(assessment)
    before, found, after = text.partition(marker)
    if not found or after and not after.startswith("\n"):
        raise ValueError("CARD_LAYOUT: canonical score summary is missing")
    blocks = []
    for section, part in enumerate((before, after)):
        if section:
            blocks.append(score_table(assessment))
        for index, line in enumerate(part.splitlines()):
            if not line:
                continue
            content = line
            if (section == 0 and index == 0) or line.startswith("── ") or line == "WHY IT MATTERS":
                content = {"type": "bold", "text": line}
            elif label := next((label for label in CARD_LABELS if line.startswith(label + ":")), None):
                content = [{"type": "bold", "text": label + ":"}, line[len(label) + 1 :]]
            blocks.append({"type": "paragraph", "text": content})
    message = {"blocks": blocks}
    length = len(rich_message_text(message))
    limit = min(max_chars, RICH_TEXT_LIMIT)
    block_count = len(blocks) + sum(len(block.get("cells", [])) for block in blocks)
    if length > limit or block_count > 500:
        raise ValueError(
            f"CARD_LENGTH: rich message has {length} characters and {block_count} blocks; "
            f"maximum {limit} characters and 500 blocks"
        )
    return message
