"""Live public-source integration over the registry accounts; touches no personal X or Apify storage."""

import copy
from pathlib import Path

import pytest
import yaml
from direct_fetch import fetch_snapshot

REGISTRY = Path(__file__).resolve().parents[4] / "config" / "registry.yaml"


@pytest.mark.integration
async def test_live_registry_snapshot_and_application_cursor_round_trip():
    handles = [row["handle"] for row in yaml.safe_load(REGISTRY.read_text())["x_accounts"]]
    request = {"handles": handles, "concurrency": 6, "requestId": "direct-live-first"}
    first = await fetch_snapshot(request)
    assert first["health"] == "healthy", first["failedAccounts"]
    assert first["stats"]["accountsRead"] == len(handles)
    assert len(first["nextCursors"]) == len(handles)
    assert len({post["id"] for post in first["posts"]}) == len(first["posts"]) > 0
    assert all(
        isinstance(post["id"], str) and post["url"].startswith("https://x.com/") for post in first["posts"]
    )
    request["cursors"] = copy.deepcopy(first["nextCursors"])
    second = await fetch_snapshot(request)
    assert second["health"] == "healthy", second["failedAccounts"]
    assert second["stats"]["accountsRead"] == len(handles)
    assert second["posts"]
    assert all(
        int(second["nextCursors"][handle]) >= int(post_id) for handle, post_id in request["cursors"].items()
    )
