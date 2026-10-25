"""Fixture clock admission must precede generated evidence."""

from datetime import datetime
from pathlib import Path

import pytest
from bootstrap_support import bootstrap_fixture


def test_naive_fixture_clock_cannot_create_catalog_evidence(tmp_path: Path) -> None:
    directory = tmp_path / "registry"
    with pytest.raises(ValueError, match="aware observation clock"):
        bootstrap_fixture(directory, datetime(2026, 10, 25))
    assert not directory.exists()
