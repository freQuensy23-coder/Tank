"""S29 runs the Apify X actor; responses follow the shape of a real run-sync call (23 Oct 2026)."""

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal

import httpx
import pydantic
import pytest
import zstandard
from pydantic import JsonValue
from source_support import fixture_http_admission, fixture_request_budgets, fixture_versions
from test_sources import settings

from tank.contracts.json import decode_json
from tank.contracts.source_items import SocialSourceItem, parse_feed_source_item
from tank.contracts.sources import SourceAccount
from tank.sources import SourceService
from tank.sources.xsensor import AccountFailure, XSensor
from tank.storage import MemoryRepository

URL = "https://api.apify.com/v2/actors/actor/run-sync?build=direct&memory=256&timeout=110"


def post(status_id: str, author: str = "ethereum", text: str = "Text") -> dict[str, JsonValue]:
    return {
        "id": status_id,
        "author": author,
        "text": text,
        "publishedTs": 1792598834.0,
        "publishedAt": "2026-10-21T16:07:14+00:00",
        "url": f"https://x.com/{author}/status/{status_id}",
        "source": "x-public-profile",
    }


def account(handle: str, *, gap: bool = False) -> dict[str, JsonValue]:
    return {"handle": handle, "status": "ok", "scanTs": 1.0, "posts": 5, "wireBytes": 1, "historyGap": gap}


def failed(handle: str) -> dict[str, JsonValue]:
    return {"handle": handle, "status": "failed", "scanTs": 1.0, "error": "PUBLIC_PROFILE_HTTP_404"}


def run(
    posts: Sequence[dict[str, JsonValue]],
    accounts: Sequence[dict[str, JsonValue]],
    cursors: Mapping[str, str],
    *,
    health: Literal["healthy", "degraded", "failed"] = "healthy",
    error: str | None = None,
) -> dict[str, JsonValue]:
    return {
        "schemaVersion": 1,
        "mode": "fetch",
        "requestId": None,
        "source": "x-public-profile",
        "checkedAt": "2026-10-23T13:49:11.467537+00:00",
        "ok": health == "healthy",
        "health": health,
        "posts": [entry for entry in posts],
        "accounts": [entry for entry in accounts],
        "nextCursors": {handle: cursor for handle, cursor in cursors.items()},
        "failedAccounts": [a for a in accounts if a["status"] == "failed"],
        "coverageGaps": [],
        "error": error,
        "stats": {"durationSeconds": 4.1},
    }


class Actor:
    """The run-sync endpoint: records requests and answers with the queued responses."""

    def __init__(self, *responses: tuple[int, dict[str, JsonValue]]) -> None:
        self.responses = list(responses)
        self.requests: list[httpx.Request] = []
        self.sent: list[bytes] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        status, body = self.responses.pop(0)
        self.sent.append(json.dumps(body).encode())
        return httpx.Response(status, content=self.sent[-1])

    def body(self, index: int) -> dict[str, JsonValue]:
        value = decode_json(self.requests[index].content)
        if not isinstance(value, dict):
            raise AssertionError("Actor HTTP request must be a JSON object")
        return value


@pytest.mark.asyncio
async def test_first_run_is_a_baseline_then_only_posts_above_the_cursor_are_new() -> None:
    actor = Actor(
        (
            201,
            run(
                [post("100"), post("90", "AyaMiyagotchi")],
                [account("ethereum"), account("AyaMiyagotchi")],
                {"ethereum": "100", "ayamiyagotchi": "90"},
            ),
        ),
        (
            201,
            run(
                [post("105", text="New"), post("100"), post("90", "AyaMiyagotchi")],
                [account("ethereum"), account("AyaMiyagotchi", gap=True), failed("gone")],
                {"ethereum": "105", "ayamiyagotchi": "90"},
                health="degraded",
            ),
        ),
    )
    sensor = XSensor(
        httpx.AsyncClient(transport=httpx.MockTransport(actor)),
        "FAKE-token",
        admission=fixture_http_admission(),
    )

    first = await sensor.posts(URL, ["ethereum", "AyaMiyagotchi"], {})
    assert first.rows == [] and first.cursors == {"ethereum": "100", "ayamiyagotchi": "90"}
    assert actor.body(0) == {"handles": ["ethereum", "AyaMiyagotchi"]}
    assert actor.requests[0].headers["authorization"] == "Bearer FAKE-token"

    second = await sensor.posts(URL, ["ethereum", "AyaMiyagotchi", "gone"], {**first.cursors, "removed": "1"})
    assert actor.body(1)["cursors"] == {"ethereum": "100", "ayamiyagotchi": "90"}
    assert [row.discovery.external_id for row in second.rows] == ["105"]
    assert (
        second.rows[0].discovery.text == "New"
        and second.rows[0].discovery.published_at == "2026-10-21T16:07:14Z"
    )
    assert second.rows[0].payload == actor.sent[1]
    assert second.cursors == {"ethereum": "105", "ayamiyagotchi": "90", "removed": "1"}
    assert second.failures == [AccountFailure(account="gone", reason="PUBLIC_PROFILE_HTTP_404")]
    assert second.gaps == ["AyaMiyagotchi"]


@pytest.mark.asyncio
async def test_a_failed_or_unreadable_run_raises() -> None:
    actor = Actor(
        (201, run([], [failed("ethereum")], {}, health="failed", error="PUBLIC_SOURCE_UNAVAILABLE")),
        (400, {"error": {"type": "run-failed", "message": "Actor run did not succeed"}}),
        (201, {"health": "healthy", "posts": [], "accounts": []}),
    )
    sensor = XSensor(
        httpx.AsyncClient(transport=httpx.MockTransport(actor)),
        "FAKE-token",
        admission=fixture_http_admission(),
    )
    with pytest.raises(RuntimeError, match="PUBLIC_SOURCE_UNAVAILABLE"):
        await sensor.posts(URL, ["ethereum"], {"ethereum": "1"})
    with pytest.raises(RuntimeError, match="Apify X run request failed: Client error '400 Bad Request'"):
        await sensor.posts(URL, ["ethereum"], {"ethereum": "1"})
    with pytest.raises(pydantic.ValidationError):
        await sensor.posts(URL, ["ethereum"], {"ethereum": "1"})
    degraded = Actor(
        (201, run([], [account("ethereum")], {"ethereum": "1"}, health="degraded", error="COVERAGE"))
    )
    scan = await XSensor(
        httpx.AsyncClient(transport=httpx.MockTransport(degraded)),
        "FAKE-token",
        admission=fixture_http_admission(),
    ).posts(URL, ["ethereum"], {"ethereum": "1"})
    assert scan.failures == [AccountFailure(account=None, reason="COVERAGE")]
    partial = Actor((201, run([post("5", "kept")], [account("kept")], {})))
    scan = await XSensor(
        httpx.AsyncClient(transport=httpx.MockTransport(partial)),
        "FAKE-token",
        admission=fixture_http_admission(),
    ).posts(URL, ["kept", "dropped"], {})
    assert scan.failures == [
        AccountFailure(account="kept", reason="NO_CURSOR"),
        AccountFailure(account="dropped", reason="MISSING_FROM_RUN"),
    ]
    token = "FAKE secret\n"

    def illegal_header(request: httpx.Request) -> httpx.Response:
        raise httpx.LocalProtocolError("Illegal header value " + repr(("Bearer " + token).encode()))

    with pytest.raises(RuntimeError) as leaked:
        await XSensor(
            httpx.AsyncClient(transport=httpx.MockTransport(illegal_header)),
            token,
            admission=fixture_http_admission(),
        ).posts(URL, ["a"], {})
    assert "FAKE" not in str(leaked.value) and "secret" not in str(leaked.value)
    assert leaked.value.__cause__ is None
    with pytest.raises(ValueError, match="APIFY_API_TOKEN"):
        await XSensor(httpx.AsyncClient(), None, admission=fixture_http_admission()).posts(
            URL, ["ethereum"], {}
        )


@pytest.mark.asyncio
async def test_s29_poll_stores_new_posts_and_moves_cursors_only_after_them(tmp_path: Path) -> None:
    source = {"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": URL}]}
    config = settings(tmp_path, [source])
    config.apify_api_token = "FAKE-token"
    accounts = [account("Official_Org"), account("other_account")]
    actor = Actor(
        (
            201,
            run(
                [post("100", "Official_Org"), post("99", "other_account")],
                accounts,
                {"official_org": "100", "other_account": "99"},
            ),
        ),
        (
            201,
            run(
                [
                    post("105", "Official_Org", "Upgrade vote"),
                    post("100", "Official_Org"),
                    post("102", "other_account", "Market view"),
                ],
                accounts,
                {"official_org": "105", "other_account": "102"},
            ),
        ),
        (
            201,
            run(
                [],
                [failed("Official_Org"), failed("other_account")],
                {},
                health="failed",
                error="PUBLIC_SOURCE_UNAVAILABLE",
            ),
        ),
    )
    repo = MemoryRepository()
    svc = SourceService(
        repo,
        config,
        http=httpx.AsyncClient(transport=httpx.MockTransport(actor)),
        versions=fixture_versions(),
        budgets=fixture_request_budgets(),
    )
    svc.accounts = {
        "official_org": SourceAccount(
            handle="Official_Org", role="ORGANIZATION", entity_id="uniswap", token_id=None
        ),
        "other_account": SourceAccount(
            handle="@other_account", role="OTHER", entity_id="other", token_id=None
        ),
    }

    async def items() -> dict[str, SocialSourceItem]:
        found: dict[str, SocialSourceItem] = {}
        for record in await repo.list("ingest.source_items"):
            item = parse_feed_source_item(record)
            if not isinstance(item, SocialSourceItem):
                raise AssertionError("S29 producer stored another source variant")
            found[item.external_id] = item
        return found

    async def checkpoint() -> dict[str, JsonValue]:
        row = await repo.get("ops.source_checkpoints", "S29:0")
        if row is None:
            raise AssertionError("S29 checkpoint was not persisted")
        return row

    result = await svc.poll_once("S29")
    assert result.status == "HEALTHY" and result.items == 0 and not await items()
    assert (await checkpoint())["account_cursors"] == {"official_org": "100", "other_account": "99"}
    assert actor.body(0) == {"handles": ["Official_Org", "other_account"]}

    result = await svc.poll_once("S29")
    assert result.status == "HEALTHY" and result.items == 2 and sorted(await items()) == ["102", "105"]
    stored = await items()
    official, other = stored["105"], stored["102"]
    assert official.author_role == "ORGANIZATION" and official.monitored_accounts == ["Official_Org"]
    assert other.author_role == "OTHER" and other.deferrable
    raw = zstandard.ZstdDecompressor().decompress(Path(official.raw_ref).read_bytes())
    decoded = decode_json(raw)
    assert isinstance(decoded, dict)
    assert decoded["nextCursors"] == {"official_org": "105", "other_account": "102"}
    assert [(job["type"], job["priority"]) for job in await repo.list("ops.jobs")] == [
        ("ROUTE_ITEM", 1),
        ("FILTER_ITEM", 2),
    ]
    assert (await checkpoint())["account_cursors"] == {"official_org": "105", "other_account": "102"}

    result = await svc.poll_once("S29")
    state = await checkpoint()
    error = state["acquisition_error"]
    assert isinstance(error, str)
    assert result.status == "DEGRADED" and "PUBLIC_SOURCE_UNAVAILABLE" in error
    assert state["account_cursors"] == {"official_org": "105", "other_account": "102"}


@pytest.mark.parametrize("missing", ["posts", "historyGap"])
def test_successful_account_cannot_default_required_read_metadata(
    missing: Literal["posts", "historyGap"],
) -> None:
    from tank.sources.xsensor import ApifyRun

    report = account("ethereum")
    del report[missing]
    with pytest.raises(pydantic.ValidationError):
        ApifyRun.model_validate(run([], [report], {"ethereum": "100"}))


@pytest.mark.parametrize(
    ("field", "value"),
    [("url", "https://x.com/another/status/105"), ("publishedAt", "2026-10-21T16:07:14")],
)
def test_post_identity_and_instant_require_explicit_wire_evidence(
    field: Literal["url", "publishedAt"], value: str
) -> None:
    from tank.sources.xsensor import ApifyPost

    payload = post("105")
    payload[field] = value
    with pytest.raises(pydantic.ValidationError):
        ApifyPost.model_validate(payload)


def test_failed_and_regressing_accounts_never_advance_their_cursor() -> None:
    from tank.sources.xsensor import ApifyRun, scan

    partial = ApifyRun.model_validate(
        run(
            [post("105"), post("205", "other")],
            [failed("ethereum"), account("other")],
            {"ethereum": "999", "other": "205"},
            health="degraded",
        )
    )
    observed = scan(
        partial, ["ethereum", "other"], {"ethereum": "100", "other": "200"}, b"retained partial response"
    )
    assert [row.discovery.external_id for row in observed.rows] == ["105", "205"]
    assert observed.cursors == {"ethereum": "100", "other": "205"}
    assert observed.failures == [AccountFailure(account="ethereum", reason="PUBLIC_PROFILE_HTTP_404")]
    regressed = scan(
        ApifyRun.model_validate(run([post("105")], [account("ethereum")], {"ethereum": "90"})),
        ["ethereum"],
        {"ethereum": "100"},
        b"retained regressing response",
    )
    assert regressed.cursors == {"ethereum": "100"}
    assert regressed.failures == [AccountFailure(account="ethereum", reason="CURSOR_REGRESSION")]


def test_actor_result_cannot_expand_the_requested_account_scope() -> None:
    from tank.sources.xsensor import ApifyRun, scan

    report = ApifyRun.model_validate(
        run(
            [post("105"), post("500", "retired")],
            [account("ethereum"), account("retired")],
            {"ethereum": "105", "retired": "500"},
        )
    )
    observed = scan(report, ["ethereum"], {"ethereum": "100", "retired": "200"}, b"retained response")
    assert [row.discovery.author for row in observed.rows] == ["ethereum"]
    assert observed.cursors == {"ethereum": "105", "retired": "200"}
    assert observed.failures == [AccountFailure(account=None, reason="UNREQUESTED_ACCOUNT_RESULT")]


@pytest.mark.asyncio
async def test_credential_echo_is_rejected_before_it_can_become_raw_source_evidence() -> None:
    token = "synthetic-sensitive-token"
    actor = Actor((201, run([], [failed("ethereum")], {}, health="failed", error="Provider echoed " + token)))
    async with httpx.AsyncClient(transport=httpx.MockTransport(actor)) as http:
        with pytest.raises(ValueError, match="credential material") as failure:
            await XSensor(http, token, admission=fixture_http_admission()).posts(
                URL, ["ethereum"], {"ethereum": "100"}
            )
    assert token not in str(failure.value)


@pytest.mark.asyncio
async def test_configured_founder_role_survives_real_source_capture_and_domain_decode(tmp_path: Path) -> None:
    from tank.contracts.source_items import SourceItemDocument
    from tank.settings import Settings

    configuration = Settings(
        data_dir=tmp_path, apify_api_token="synthetic-actor-token", ops_progress_enabled=False
    )
    registry = configuration.load_config("registry")
    accounts = registry["x_accounts"]
    assert isinstance(accounts, list)
    configured = next(row for row in accounts if isinstance(row, dict) and row["handle"] == "sama")
    assert isinstance(configured, dict)
    actor = Actor(
        (201, run([post("100", "sama")], [account("sama")], {"sama": "100"})),
        (201, run([post("105", "sama")], [account("sama")], {"sama": "105"})),
    )
    repo = MemoryRepository()
    async with httpx.AsyncClient(transport=httpx.MockTransport(actor)) as http:
        service = SourceService(
            repo, configuration, http=http, versions=fixture_versions(), budgets=fixture_request_budgets()
        )
        declared_account = SourceAccount.model_validate(configured)
        assert declared_account.official_authorities is not None
        assert {authority.entity_id for authority in declared_account.official_authorities} == {
            "world",
            "openai",
        }
        service.accounts = {"sama": declared_account}
        assert (await service.poll_once("S29")).items == 0
        assert (await service.poll_once("S29")).items == 1
    [record] = await repo.list("ingest.source_items")
    item = SourceItemDocument.model_validate(record).root
    assert isinstance(item, SocialSourceItem)
    assert item.author_role == configured["role"] == "FOUNDER_C_LEVEL"
    assert item.original_author_role == "FOUNDER_C_LEVEL"
    assert item.relevance.decision is None
    assert item.author_organization == configured["entity_id"]


@pytest.mark.asyncio
async def test_actor_never_starts_with_an_unbound_budget_scope() -> None:
    from tank.http_admission import ConfiguredHttpAdmission
    from tank.request_budget import RequestBudgetScope

    admitted = fixture_http_admission()
    admission = ConfiguredHttpAdmission(admitted.policy, admitted.versions, RequestBudgetScope())
    calls: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        sensor = XSensor(http, "FAKE-token", admission=admission)
        with pytest.raises(RuntimeError, match="has not been bound"):
            await sensor.posts(URL, ["ethereum"], {})
    assert not calls


@pytest.mark.asyncio
async def test_actor_redirect_cannot_send_authenticated_run_to_another_host() -> None:
    calls: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.host)
        return httpx.Response(307, headers={"Location": "https://other.invalid/run"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        sensor = XSensor(http, "FAKE-token", admission=fixture_http_admission())
        with pytest.raises(ValueError, match="declared HTTPS Apify"):
            await sensor.posts(URL, ["ethereum"], {})
    assert calls == ["api.apify.com"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 404, 503])
async def test_public_account_probe_waits_on_observed_block_and_keeps_authority_unverified(
    tmp_path: Path,
    status: int,
) -> None:
    from datetime import timedelta

    from source_support import FixtureBudgetClock

    from tank.contracts.sources import AccountProbeResponse
    from tank.request_budget import MemoryBudgetBackend

    clock = FixtureBudgetClock()
    engine = fixture_request_budgets(clock=clock)
    assert isinstance(engine.backend, MemoryBudgetBackend)
    calls: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(status, json={"author_name": "Synthetic public profile"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = SourceService(
            MemoryRepository(),
            settings(tmp_path, []),
            http=http,
            versions=fixture_versions(),
            budgets=engine,
        )
        service.accounts = {
            "ethereum": SourceAccount(handle="ethereum", role="ORGANIZATION", entity_id=None, token_id=None)
        }
        await service.http_admission.block(
            http.build_request("GET", "https://publish.twitter.com/oembed"),
            clock.now() + timedelta(seconds=9),
            "observed Retry-After",
        )
        [result] = await service._account_checks()
    assert isinstance(result, AccountProbeResponse)
    assert result.exists is None and result.identity_status == "REQUIRES_OWNER_EVIDENCE"
    assert result.http_status == status
    assert calls[0].url.params["url"] == "https://x.com/ethereum"
    assert clock.monotonic() == 9
    assert not engine.backend.windows


def test_failed_actor_run_needs_nonblank_run_or_account_evidence() -> None:
    from tank.sources.xsensor import ApifyRun, scan

    with pytest.raises(pydantic.ValidationError, match="requires its error evidence"):
        ApifyRun.model_validate(run([], [], {}, health="failed", error=" "))
    report = ApifyRun.model_validate(run([], [failed("ethereum")], {}, health="failed", error=" "))
    with pytest.raises(RuntimeError, match="PUBLIC_PROFILE_HTTP_404"):
        scan(report, ["ethereum"], {"ethereum": "100"}, b"retained failure evidence")
