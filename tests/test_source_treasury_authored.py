import hashlib
import json
from pathlib import Path

import pytest
import zstandard
from selectolax.parser import HTMLParser

from tank.sources import parsers

ROOT = Path(__file__).parent / "fixtures/treasury-authored"


def retained():
    payload = zstandard.ZstdDecompressor().decompress((ROOT / "release.html.zst").read_bytes())
    assert hashlib.sha256(payload).hexdigest() == json.loads((ROOT / "receipt.json").read_text())["sha256"]
    return payload


def test_treasury_release_excludes_navigation_and_uses_authored_title() -> None:
    result = parsers.article(retained(), source_id="S07")
    assert result["title"] == "Treasury International Capital Data for August 2026"
    assert "today released Treasury International Capital" in result["text"]
    assert "Treasury Hunt" not in result["text"]
    assert "GENERAL INFORMATION" not in result["text"]
    assert result["published_at"] == "2026-10-16T20:00:00Z"


@pytest.mark.parametrize("change", ["missing", "ambiguous"])
def test_treasury_missing_authored_body_never_falls_back_to_menu(change) -> None:
    tree = HTMLParser(retained())
    node = tree.css_first("main article[about]")
    if change == "missing":
        node.decompose()
    else:
        tree = HTMLParser(tree.html.replace(node.html, node.html + node.html))
    with pytest.raises(ValueError, match="Treasury release"):
        parsers.article(tree.html, source_id="S07")
