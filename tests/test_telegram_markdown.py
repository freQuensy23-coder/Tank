import pytest

from tank.telegram_markdown import render, utf16_length


def spans(text, entities):
    encoded = text.encode("utf-16-le")
    return [
        (e["type"], encoded[e["offset"] * 2 : (e["offset"] + e["length"]) * 2].decode("utf-16-le"))
        for e in entities
    ]


def test_agent_markdown_becomes_entities_without_markup():
    text, entities = render(
        "It’s an automatic **“do we have enough data yet?” check**—not a bullish signal.\n\n"
        "Think of it as **the camera becoming clearer** 🦄 and *calm*, see `cfg` or [docs](https://ex.test)."
    )
    assert "**" not in text and "`" not in text and "](" not in text
    assert spans(text, entities) == [
        ("bold", "“do we have enough data yet?” check"),
        ("bold", "the camera becoming clearer"),
        ("italic", "calm"),
        ("code", "cfg"),
        ("text_link", "docs"),
    ]
    assert entities[-1]["url"] == "https://ex.test"


def test_blocks_render_headings_lists_quotes_and_fenced_code():
    text, entities = render("# Verdict\n- keep **watching**\n> data gap\n\n```python\nx = 1\n```\nend")
    assert text == "Verdict\n• keep watching\ndata gap\n\nx = 1\nend"
    assert spans(text, entities) == [
        ("bold", "Verdict"),
        ("bold", "watching"),
        ("blockquote", "data gap"),
        ("pre", "x = 1"),
    ]
    assert entities[-1]["language"] == "python"


@pytest.mark.parametrize(
    "source",
    [
        "unclosed **bold and `tick",
        "snake_case_name and 2 * 3 * 4",
        "```\nunclosed fence",
        "[label](javascript:alert) and ** spaced **",
        "***",
    ],
)
def test_malformed_markdown_stays_literal(source):
    assert render(source) == (source, [])


def test_nested_spans_and_offsets_count_utf16_units():
    text, entities = render("🦄🦄 **a *b* c** \\*literal\\*")
    assert text == "🦄🦄 a b c *literal*"
    assert spans(text, entities) == [("bold", "a b c"), ("italic", "b")]
    assert entities[0]["offset"] == utf16_length("🦄🦄 ")
