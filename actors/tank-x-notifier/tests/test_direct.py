"""Contract and failure tests for application-owned state."""

import copy
from unittest.mock import AsyncMock, patch

import direct_fetch
import main
import public_profile
import pytest

POST = {
    "id": "2113055268879663154",
    "author": "example",
    "text": "A public post",
    "publishedTs": 1792626609.426,
    "publishedAt": "2026-10-21T23:50:09.426000+00:00",
    "url": "https://x.com/example/status/2113055268879663154",
    "source": "x-public-profile",
}


def collection(posts, accounts):
    failures = [row for row in accounts if row["status"] == "failed"]
    gaps = [row["handle"] for row in accounts if row.get("historyGap")]
    return posts, {
        "accountResults": accounts,
        "failedAccounts": failures,
        "coverageGaps": gaps,
        "error": "PUBLIC_SOURCE_UNAVAILABLE" if len(failures) == len(accounts) else None,
        "watchedAccounts": len(accounts),
        "accountsRead": len(accounts) - len(failures),
        "wireBytes": sum(row.get("wireBytes", 0) for row in accounts),
    }


def healthy(handle="example", post_id=POST["id"]):
    return {
        "handle": handle,
        "status": "ok",
        "scanTs": 1792626639.3,
        "posts": 1,
        "wireBytes": 200,
        "lastMaxId": post_id,
        "historyGap": False,
    }


@pytest.mark.parametrize(
    "bad",
    [
        None,
        [],
        {},
        {"handles": []},
        {"handles": "example"},
        {"handles": ["bad/name"]},
        {"handles": [1]},
        {"handles": ["example"], "concurrency": True},
        {"handles": ["example"], "concurrency": 0},
        {"handles": ["example"], "concurrency": 9},
        {"handles": ["example"], "mode": "monitor"},
        {"handles": ["example"], "webhookUrl": "https://example.com"},
        {"handles": ["example"], "stateStoreName": "old-state"},
        {"handles": ["example"], "cookies": []},
        {"handles": ["example"], "cursors": {"example": 2113055268879663154}},
        {"handles": ["example"], "cursors": {"example": "0"}},
        {"handles": ["example"], "cursors": {"example": "broken"}},
        {"handles": ["example"], "cursors": {"other": "123"}},
        {"handles": ["example"], "cursors": {"Example": "123", "example": "124"}},
        {"handles": ["example"], "requestId": "a" * 129},
    ],
)
async def test_invalid_request_never_reads_x(bad):
    with patch.object(direct_fetch, "collect", new_callable=AsyncMock) as collect:
        with pytest.raises((TypeError, ValueError)):
            await direct_fetch.fetch_snapshot(bad)
        collect.assert_not_awaited()


def test_input_normalization_preserves_request_and_rejects_numeric_ids():
    config = direct_fetch.validate_input(
        {
            "handles": [" @Example ", "EXAMPLE"],
            "cursors": {"@EXAMPLE": POST["id"]},
            "requestId": "app-call-1",
        }
    )
    assert config == {
        "handles": ["example"],
        "concurrency": 6,
        "cursors": {"example": POST["id"]},
        "requestId": "app-call-1",
    }


async def test_repeated_fetches_return_snapshot_without_hidden_deduplication():
    request = {"handles": ["example"], "cursors": {"example": "2113054001977602107"}}
    original = copy.deepcopy(request)
    with patch.object(
        direct_fetch,
        "collect",
        new_callable=AsyncMock,
        return_value=collection([POST], [healthy()]),
    ) as collect:
        first = await direct_fetch.fetch_snapshot(request)
        second = await direct_fetch.fetch_snapshot(request)
    assert first["posts"] == second["posts"] == [POST]
    assert first["nextCursors"] == {"example": POST["id"]}
    assert first["health"] == "healthy" and first["ok"]
    assert request == original
    assert collect.await_args.args[0]["_accountState"] == {"example": {"lastMaxId": "2113054001977602107"}}


async def test_partial_failure_returns_good_posts_and_keeps_failed_cursor():
    accounts = [
        healthy(),
        {"handle": "other", "status": "failed", "error": "PUBLIC_PROFILE_HTTP_503"},
    ]
    request = {"handles": ["example", "other"], "cursors": {"other": "100"}}
    with patch.object(
        direct_fetch,
        "collect",
        new_callable=AsyncMock,
        return_value=collection([POST], accounts),
    ):
        response = await direct_fetch.fetch_snapshot(request)
    assert response["posts"] == [POST]
    assert response["health"] == "degraded" and not response["ok"]
    assert response["nextCursors"] == {"other": "100", "example": POST["id"]}
    assert response["stats"]["accountsRead"] == 1
    assert response["failedAccounts"][0]["handle"] == "other"


async def test_complete_source_failure_is_explicit_not_an_empty_healthy_scan():
    failed = [{"handle": "example", "status": "failed", "error": "PUBLIC_PROFILE_HTTP_403"}]
    with patch.object(
        direct_fetch,
        "collect",
        new_callable=AsyncMock,
        return_value=collection([], failed),
    ):
        response = await direct_fetch.fetch_snapshot({"handles": ["example"], "cursors": {"example": "100"}})
    assert response["health"] == "failed" and not response["ok"]
    assert response["error"] == "PUBLIC_SOURCE_UNAVAILABLE"
    assert response["nextCursors"] == {"example": "100"}
    assert response["posts"] == []


async def test_caller_cursor_detects_lost_overlap_with_real_collection_logic():
    with patch.object(public_profile, "fetch_profile", return_value=([POST], 200)):
        response = await direct_fetch.fetch_snapshot({"handles": ["example"], "cursors": {"example": "100"}})
    assert response["coverageGaps"] == ["example"]
    assert response["health"] == "degraded" and response["posts"] == [POST]


async def test_stale_public_page_does_not_advance_application_cursor():
    with patch.object(public_profile, "fetch_profile", return_value=([POST], 200)):
        response = await direct_fetch.fetch_snapshot(
            {"handles": ["example"], "cursors": {"example": "2113055268879663155"}}
        )
    assert response["health"] == "failed"
    assert response["failedAccounts"][0]["error"] == "PUBLIC_PROFILE_STALE_SAMPLE"
    assert response["nextCursors"]["example"] == "2113055268879663155"
    assert response["posts"] == []


class FakeActor:
    def __init__(self, config):
        self.config = config
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    async def get_input(self):
        self.calls.append(("get_input",))
        return self.config

    async def set_value(self, key, value):
        self.calls.append(("set_value", key, value))


@pytest.mark.parametrize("source_failure", [False, True])
async def test_apify_adapter_only_reads_input_and_writes_one_output(source_failure):
    actor = FakeActor({"handles": ["example"]})
    accounts = (
        [{"handle": "example", "status": "failed", "error": "PUBLIC_PROFILE_HTTP_503"}]
        if source_failure
        else [healthy()]
    )
    with (
        patch.object(main, "Actor", actor),
        patch.object(
            direct_fetch,
            "collect",
            new_callable=AsyncMock,
            return_value=collection([] if source_failure else [POST], accounts),
        ),
    ):
        await main.main()
    assert len(actor.calls) == 2
    assert actor.calls[0] == ("get_input",)
    assert actor.calls[1][0:2] == ("set_value", "OUTPUT")
    assert actor.calls[1][2]["health"] == ("failed" if source_failure else "healthy")
    assert actor.calls[1][2]["posts"] == ([] if source_failure else [POST])
