import pytest
import telegramify_markdown

from tank.telegram_inbox import MESSAGE_LIMIT, formatted_chunks


def entity_text(text, entity):
    encoded = text.encode("utf-16-le")
    start = entity["offset"] * 2
    end = (entity["offset"] + entity["length"]) * 2
    assert 0 <= start < end <= len(encoded)
    encoded[:start].decode("utf-16-le")
    encoded[end:].decode("utf-16-le")
    return encoded[start:end].decode("utf-16-le")


@pytest.mark.parametrize(
    "markdown",
    [
        "$NEAR and $ETH",
        "Watch $NEAR and $ETH before trading.",
        "🚀 $NEAR and $ETH; fees stay $5 to $10.",
    ],
)
def test_paired_tickers_remain_literal_instead_of_becoming_math(markdown):
    parts = formatted_chunks(markdown)

    assert "".join(text for text, _ in parts) == markdown


@pytest.mark.parametrize(
    "markdown",
    [
        "Before.\n\n<div>important</div>\n\nAfter.",
        "Before <span>important</span> after.",
        "Before <b>important</b> and <i>still important</i> after.",
    ],
    ids=["html-block-between-paragraphs", "inline-span", "inline-formatting-tags"],
)
def test_html_is_preserved_as_literal_answer_text(markdown):
    parts = formatted_chunks(markdown)

    assert "".join(text for text, _ in parts) == markdown


@pytest.mark.parametrize(
    ("markdown", "expected"),
    [
        ("# Outlook", "Outlook"),
        ("## Market outlook", "Market outlook"),
        ("### Risks", "Risks"),
        ("# C#", "C#"),
    ],
)
def test_heading_preserves_visible_words_without_inventing_emoji(markdown, expected):
    [(text, _)] = formatted_chunks(markdown)

    assert text == expected


@pytest.mark.parametrize(
    "markdown",
    [
        "```\nunclosed fence",
        "```python\nvalue = '**literal**'",
        "Before.\n\n```python\nvalue = '$NEAR and $ETH'",
    ],
)
def test_unclosed_fence_remains_literal_including_its_opening_marker(markdown):
    parts = formatted_chunks(markdown)

    assert "".join(text for text, _ in parts) == markdown


def test_link_with_balanced_parentheses_preserves_the_complete_destination():
    [(text, entities)] = formatted_chunks("🚀 [docs](https://example.org/a_(b))")

    assert text == "🚀 docs"
    assert entities == [
        {"type": "text_link", "offset": 3, "length": 4, "url": "https://example.org/a_(b)"}
    ]
    assert entity_text(text, entities[0]) == "docs"


def test_bold_containing_inline_code_preserves_literal_asterisks_in_code():
    [(text, entities)] = formatted_chunks("**Use `a**b` here**")

    assert text == "Use a**b here"
    assert [entity_text(text, entity) for entity in entities if entity["type"] == "code"] == [
        "a**b"
    ]
    bold = [entity_text(text, entity) for entity in entities if entity["type"] == "bold"]
    assert any("Use" in span for span in bold)
    assert any("here" in span for span in bold)


def test_long_fenced_code_preserves_literal_content_bytes_across_chunks():
    content = (
        "  value = '**literal**'; ticker = '$NEAR'; html = '<div>🚀</div>'  \n\n" * 160
        + "last = 'C# and a**b'"
    )
    parts = formatted_chunks("```python\n" + content + "\n```")

    assert len(parts) > 1
    assert "".join(text for text, _ in parts).encode("utf-8") == content.encode("utf-8")
    for text, entities in parts:
        assert 0 < len(text.encode("utf-16-le")) // 2 <= MESSAGE_LIMIT
        assert entities
        assert all(entity["type"] == "pre" for entity in entities)
        assert "".join(entity_text(text, entity) for entity in entities) == text
        assert all(entity.get("language") == "python" for entity in entities)


@pytest.mark.parametrize("converted", ["", " \n\t"])
def test_empty_conversion_falls_back_to_the_original_nonempty_answer(monkeypatch, converted):
    markdown = "**Keep** $NEAR and $ETH; <div>important</div>."
    monkeypatch.setattr(telegramify_markdown, "convert", lambda *args, **kwargs: (converted, []))

    assert formatted_chunks(markdown) == [(markdown, [])]


def test_entity_serialization_failure_falls_back_to_the_original_answer(monkeypatch):
    class BrokenEntity:
        def to_dict(self):
            raise ValueError("entity serialization failed")

    monkeypatch.setattr(
        telegramify_markdown,
        "split_entities",
        lambda text, entities, limit: [(text, [BrokenEntity()])],
    )
    markdown = "**Keep** watching 🚀."

    assert formatted_chunks(markdown) == [(markdown, [])]
