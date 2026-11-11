import asyncio
import html
import json
from types import SimpleNamespace

import httpx
import pytest
from test_session_binding_concurrency import fixture_repo
from test_sources import RSS
from test_sources import settings as source_settings
from test_xsensor import URL, account, failed, post, run

from tank import ops_entry_errors
from tank.delivery import DeliveryService
from tank.ops_entry_errors import CAPTION_LIMIT, MESSAGE_LIMIT, report_entry_errors, units
from tank.sources import SourceService
from tank.storage import MemoryRepository

FEED_URL = "https://example.org/rss"
SOURCE = {"source_id": "S30", "name": "Fixture blog", "feeds": [{"url": FEED_URL, "kind": "rss"}]}


def broken(*titles):
    bad = b"".join(
        b"<item><title>"
        + title.encode()
        + b"</title><description>Body of "
        + title.encode()
        + b"</description></item>"
        for title in titles
    )
    return RSS.replace(b"<item><guid>", bad + b"<item><guid>")


async def entry_notices(repo):
    return await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERRORS"})


async def entry_jobs(repo):
    return [
        job
        for job in await repo.list("ops.jobs", filters={"type": "DELIVER"})
        if job["payload"]["notice_id"].startswith("entry_errors_")
    ]


async def poll(repo, tmp_path, payloads, *, enabled=True, replay=False, source=SOURCE):
    settings = source_settings(tmp_path, [source])
    settings.ops_progress_enabled = enabled
    settings.replay = replay
    feed = iter(payloads)

    def handle(request):
        return httpx.Response(200, content=next(feed))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        service = SourceService(repo, settings, http=http)
        results = [await service.poll_once(source["source_id"]) for _ in payloads]
    return results


async def test_first_poll_reports_unparsed_entry_once_with_identity_and_hashes(tmp_path):
    repo = MemoryRepository()
    results = await poll(repo, tmp_path, [broken("Bad entry without identity")])
    assert results[0]["items"] == 1
    [notice] = await entry_notices(repo)
    assert len(await entry_jobs(repo)) == 1
    checkpoint = await repo.get("ops.source_checkpoints", "S30:0")
    [error] = checkpoint["entry_errors"]
    assert error["entry"]["title"] == "Bad entry without identity"
    text = notice["text"]
    assert text.startswith("TANK OPS · Записи источника не разобраны")
    assert "Источник: S30 · Fixture blog" in text and "Лента: " + FEED_URL in text
    assert "Запись без идентификатора и ссылки (ENTRY_WITHOUT_ID_OR_URL)" in text
    assert "«Bad entry without identity»" in text and "Фрагмент: Body of Bad entry" in text
    assert error["entry_sha256"] in text and checkpoint["payload_sha256"] in text
    assert "Обнаружено: " in text and notice["seen_at"] == checkpoint["fetched_at"]


async def test_repeat_polls_do_not_report_same_entry_and_new_entry_is_reported_alone(tmp_path):
    repo = MemoryRepository()
    await poll(repo, tmp_path, [broken("Stuck entry"), broken("Stuck entry"), broken("Stuck entry")])
    assert len(await entry_notices(repo)) == 1
    await poll(
        repo, tmp_path, [broken("Stuck entry", "Second broken"), broken("Second broken", "Stuck entry")]
    )
    notices = await entry_notices(repo)
    assert len(notices) == 2 and len(await entry_jobs(repo)) == 2
    latest = next(row for row in notices if "Second broken" in row["text"])
    assert "Stuck entry" not in latest["text"] and len(latest["entry_errors"]) == 1


async def test_burst_of_broken_entries_is_one_message_clipped_to_telegram_limit(tmp_path):
    repo = MemoryRepository()
    titles = [f"Broken entry {n} " + "\U0001f680" * 150 for n in range(60)]
    await poll(repo, tmp_path, [broken(*titles)])
    [notice] = await entry_notices(repo)
    assert len(notice["entry_errors"]) == 60 and len(await entry_jobs(repo)) == 1
    assert units(html.escape(notice["text"])) <= MESSAGE_LIMIT
    assert "Новых записей с ошибкой: 60" in notice["text"]
    assert (
        "полный список: out.ops_notices " + notice["notice_id"] in notice["text"] and "document" not in notice
    )


@pytest.mark.parametrize("enabled,replay", [(False, False), (True, True)])
async def test_disabled_or_replay_reports_nothing(tmp_path, enabled, replay):
    repo = MemoryRepository()
    await poll(repo, tmp_path, [broken("Bad")], enabled=enabled, replay=replay)
    assert await entry_notices(repo) == [] and await entry_jobs(repo) == []
    checkpoint = await repo.get("ops.source_checkpoints", "S30:0")
    assert checkpoint["entry_errors"]


async def test_restart_and_concurrent_reports_are_idempotent(tmp_path):
    repo = MemoryRepository()
    await poll(repo, tmp_path, [broken("Bad")])
    await poll(repo, tmp_path, [broken("Bad")])
    assert len(await entry_notices(repo)) == 1
    settings = SimpleNamespace(ops_progress_enabled=True)
    errors = [{"reason": "PENDING_PROPOSAL_UNAVAILABLE", "proposal_id": "0xabc", "payload_sha256": "p"}]
    source = {"source_id": "S24", "name": "Snapshot"}
    feed = {"url": "https://hub.snapshot.org/graphql"}
    with pytest.raises(ValueError, match="rollback"):
        async with repo.transaction():
            await report_entry_errors(repo, settings, source, feed, errors, "2026-10-24T12:00:00Z")
            raise ValueError("rollback")
    assert len(await entry_notices(repo)) == 1
    ids = await asyncio.gather(
        *(report_entry_errors(repo, settings, source, feed, errors, "2026-10-24T12:00:00Z") for _ in range(4))
    )
    assert len([notice_id for notice_id in ids if notice_id]) == 1
    assert len(await entry_notices(repo)) == 2 and len(await entry_jobs(repo)) == 2
    [snapshot] = [row for row in await entry_notices(repo) if row["source_id"] == "S24"]
    assert "предложение 0xabc" in snapshot["text"]


async def test_publisher_json_entry_error_is_reported_and_delivered_to_ops(tmp_path):
    repo = MemoryRepository()
    source = {
        "source_id": "S03",
        "name": "Federal Register",
        "feeds": [{"url": "https://example.org/index", "kind": "json"}],
    }
    data = {
        "results": [
            {"title": "Malformed original without identifier", "abstract": "Rule text"},
            {
                "document_number": "good",
                "title": "Valid release",
                "abstract": "Full visible body",
                "html_url": "https://example.org/good",
                "publication_date": "2026-10-18",
            },
        ],
        "next_page_url": None,
    }
    payload = json.dumps(data).encode()
    results = await poll(repo, tmp_path, [payload, payload], source=source)
    assert [result["items"] for result in results] == [1, 0]
    [notice] = await entry_notices(repo)
    assert "INVALID_PUBLISHER_ENTRY" in notice["text"]
    assert "«Malformed original without identifier»" in notice["text"]
    requests = []

    def respond(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 7, "date": 1}})

    settings = SimpleNamespace(
        ops_progress_enabled=True,
        telegram_token="test",
        telegram_alerts_chat="alerts",
        telegram_ops_chat="ops",
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        delivery = DeliveryService(repo, settings, http=http)
        [job] = await entry_jobs(repo)
        await delivery.deliver(job["payload"])
        await delivery.deliver(job["payload"])
    assert len(requests) == 1 and requests[0]["chat_id"] == "ops"
    assert requests[0]["text"] == html.escape(notice["text"]) and "document" not in notice


async def test_x_account_failures_are_keyed_by_account():
    repo = MemoryRepository()
    settings = SimpleNamespace(ops_progress_enabled=True)
    source = {"source_id": "S29", "name": "X"}
    feed = {"url": "apify://x"}
    first = [
        {"account": "alice", "reason": "MISSING_FROM_RUN"},
        {"account": "bob", "reason": "MISSING_FROM_RUN"},
        {"account": None, "reason": "RUN_DEGRADED"},
    ]
    await report_entry_errors(repo, settings, source, feed, first, "2026-10-24T12:00:00Z")
    [notice] = await entry_notices(repo)
    assert len(notice["entry_errors"]) == 3
    assert "аккаунт @alice" in notice["text"] and "аккаунт @bob" in notice["text"]
    assert "весь запуск сбора X" in notice["text"]
    again = first + [{"account": "carol", "reason": "MISSING_FROM_RUN"}]
    await report_entry_errors(repo, settings, source, feed, again, "2026-10-24T12:05:00Z")
    latest = [row for row in await entry_notices(repo) if row["notice_id"] != notice["notice_id"]]
    assert [row["entry_errors"] for row in latest] == [[{"account": "carol", "reason": "MISSING_FROM_RUN"}]]


async def test_failed_x_run_request_is_reported_once_with_its_response(tmp_path):
    repo = MemoryRepository()
    source = {"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": URL}]}
    settings = source_settings(tmp_path, [source])
    settings.ops_progress_enabled = True
    settings.apify_api_token = "FAKE-token"
    apify_error = {"error": {"type": "run-failed", "message": "Actor run timed out (token=FAKE-token)"}}
    answers = [
        (400, apify_error),
        (400, apify_error),
        (201, run([], [failed("gone")], {}, health="degraded")),
    ]

    def respond(request):
        status, body = answers.pop(0)
        return httpx.Response(
            status, content=json.dumps(body).encode(), headers={"content-type": "application/json"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = SourceService(repo, settings, http=http)
        service.accounts = {"gone": {"handle": "gone", "role": "OTHER", "entity_id": "gone"}}
        await service.poll_once("S29")
        await service.poll_once("S29")
        checkpoint = await repo.get("ops.source_checkpoints", "S29:0")
        assert checkpoint["status"] == "DEGRADED" and checkpoint["reads"] == {"ok": 0, "failed": 2}
        [notice] = await entry_notices(repo)
        assert len(await entry_jobs(repo)) == 1
        [error] = notice["entry_errors"]
        assert error["account"] is None and error["reason"] == "APIFY_RUN_HTTP_ERROR"
        assert error["response"]["status"] == 400
        assert json.loads(error["response"]["body"]) == {
            "error": {"type": "run-failed", "message": "Actor run timed out (token=[REDACTED])"}
        }
        text = notice["text"]
        assert "Запуск сбора X в Apify завершился HTTP-ошибкой (APIFY_RUN_HTTP_ERROR)" in text
        assert "весь запуск сбора X" in text and "Ответ: HTTP 400 Bad Request" in text
        body = error["response"]["body"]
        assert f"Заголовки:\ncontent-type: application/json\ncontent-length: {len(body)}\n" in text
        assert text.endswith(f"Тело ответа ({len(body)} байт):\n{body}") and "token=[REDACTED]" in body
        assert "document" not in notice
        assert "FAKE-token" not in json.dumps(notice, default=str)
        await service.poll_once("S29")
    [account_notice] = [row for row in await entry_notices(repo) if row["notice_id"] != notice["notice_id"]]
    assert account_notice["entry_errors"] == [{"account": "gone", "reason": "PUBLIC_PROFILE_HTTP_404"}]
    assert "аккаунт @gone" in account_notice["text"] and "Ответ:" not in account_notice["text"]


@pytest.mark.parametrize(
    ("statuses", "reported"),
    [([400, 400], [400]), ([400, 201, 400], [400, 400]), ([400, 401], [400, 401])],
)
async def test_failed_x_run_is_reported_once_per_failure_episode_and_status(tmp_path, statuses, reported):
    repo = MemoryRepository()
    source = {"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": URL}]}
    settings = source_settings(tmp_path, [source])
    settings.ops_progress_enabled = True
    settings.apify_api_token = "FAKE-token"
    answers = list(statuses)

    def respond(request):
        status = answers.pop(0)
        body = run([], [account("ethereum")], {"ethereum": "1"}) if status == 201 else {"error": "failed"}
        return httpx.Response(status, content=json.dumps(body).encode())

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = SourceService(repo, settings, http=http)
        service.accounts = {"ethereum": {"handle": "ethereum", "role": "OTHER", "entity_id": "ethereum"}}
        for _ in statuses:
            await service.poll_once("S29")
    errors = [error for row in await entry_notices(repo) for error in row["entry_errors"]]
    assert sorted(error["response"]["status"] for error in errors) == reported
    assert len(await entry_jobs(repo)) == len(reported)


OPS = SimpleNamespace(
    ops_progress_enabled=True, telegram_token="test", telegram_alerts_chat="alerts", telegram_ops_chat="ops"
)


async def deliver_twice(repo, notice_id):
    """Deliver the notice's job twice through the real sender; the requests Telegram received."""
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 7, "date": 1}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        delivery = DeliveryService(repo, OPS, http=http)
        [job] = [job for job in await entry_jobs(repo) if job["payload"]["notice_id"] == notice_id]
        await delivery.deliver(job["payload"])
        await delivery.deliver(job["payload"])
    return requests


def x_evidence(url, statuses, body, reason="PUBLIC_PROFILE_HTTP_"):
    """The worker's record of its requests for one page, as the actor returns it."""
    attempts = [
        {
            "attempt": number,
            "at": f"2026-11-10T10:00:0{number}+00:00",
            "status": status,
            "error": f"{reason}{status}",
            "retryable": True,
            "response": {
                "status": status,
                "headers": [
                    ["content-type", "text/html"],
                    ["set-cookie", "[REDACTED]; Path=/"],
                    ["set-cookie", "[REDACTED]; Secure"],
                    ["x-response-time", str(100 + number)],
                ],
                "body": body,
                "bodyEncoding": "utf-8",
                "bodyBytes": len(body.encode()),
                "bodyTruncated": False,
                "omittedWireBytes": 0,
                "readError": None,
                "decodeError": None,
                "redacted": True,
            },
        }
        for number, status in enumerate(statuses, 1)
    ]
    return {
        "request": {"method": "GET", "url": url, "body": None},
        "attempts": attempts,
        "attemptCount": len(attempts),
        "outcome": "exhausted",
    }


async def test_long_x_run_response_is_sent_whole_as_a_document_with_a_caption():
    repo = MemoryRepository()
    body = json.dumps({"error": {"message": '"&<>' * 5000 + " END-OF-BODY"}})
    headers = {f"x-apify-header-{number}": f"value {number} <&>" for number in range(40)}
    response = {
        "status": 400,
        "reason_phrase": "Bad Request",
        "headers": headers,
        "body": body,
        "body_bytes": len(body),
    }
    error = {"account": None, "reason": "APIFY_RUN_HTTP_ERROR", "error": "Client error", "response": response}
    notice_id = await report_entry_errors(
        repo, OPS, {"source_id": "S29", "name": "X"}, {"url": URL}, [error], "2026-11-08T17:40:00Z"
    )
    notice = await repo.get("out.ops_notices", notice_id)
    document = notice["document"]
    assert document["filename"] == notice_id + ".txt"
    assert document["text"].endswith(f"Тело ответа ({len(body)} байт):\n{body}")
    assert "Ответ: HTTP 400 Bad Request\nЗаголовки:\nx-apify-header-0: value 0 <&>\n" in document["text"]
    assert all(f"{name}: {value}\n" in document["text"] for name, value in headers.items())
    caption = notice["text"]
    assert units(html.escape(caption)) <= CAPTION_LIMIT
    assert "Ответ: HTTP 400 Bad Request" in caption and "в приложенном файле" in caption
    assert "END-OF-BODY" not in caption and "… и ещё" not in caption
    [request] = await deliver_twice(repo, notice_id)
    assert request.url.path == "/bottest/sendDocument"
    assert request.headers["content-type"].startswith("multipart/form-data")
    assert f'name="document"; filename="{notice_id}.txt"'.encode() in request.content
    assert document["text"].encode() in request.content
    assert (
        html.escape(caption).encode() in request.content and b'name="chat_id"\r\n\r\nops' in request.content
    )
    [sent] = await repo.list("out.deliveries", filters={"status": "SENT"})
    assert sent["message_ref"] == "7"


async def test_worker_http_error_is_reported_once_per_account_with_every_attempt_in_full():
    repo = MemoryRepository()
    source = {"source_id": "S29", "name": "X"}
    feed = {"url": URL}
    body = "<html><body>BODY-START " + "Access denied & blocked. " * 1500 + "BODY-END</body></html>"
    blocked = {
        "account": "blocked",
        "reason": "PUBLIC_PROFILE_HTTP_403",
        "diagnostics": x_evidence("https://x.com/blocked", [403, 403, 403], body),
    }
    notice_id = await report_entry_errors(repo, OPS, source, feed, [blocked], "2026-11-10T10:00:10Z")
    notice = await repo.get("out.ops_notices", notice_id)
    text = notice["document"]["text"]
    assert "Запрос к X: GET https://x.com/blocked\nПопыток: 3; итог: exhausted\n" in text
    for number in (1, 2, 3):
        assert (
            f"Попытка {number} · 2026-11-10T10:00:0{number}+00:00 · PUBLIC_PROFILE_HTTP_403\n"
            "Ответ: HTTP 403\n"
            "Заголовки:\n"
            "content-type: text/html\n"
            "set-cookie: [REDACTED]; Path=/\n"
            "set-cookie: [REDACTED]; Secure\n"
            f"x-response-time: {100 + number}\n"
            f"Тело ответа ({len(body)} байт, учётные данные скрыты):\n{body}"
        ) in text
    caption = notice["text"]
    assert units(html.escape(caption)) <= CAPTION_LIMIT and "BODY-START" not in caption
    assert "1. PUBLICPROFILEHTTP403\nЗапись: аккаунт @blocked\n" in caption
    assert caption.endswith(
        "Запрос к X: GET https://x.com/blocked\nПопыток: 3; итог: exhausted\nОтветы: 403, 403, 403"
    )
    [request] = await deliver_twice(repo, notice_id)
    assert request.url.path == "/bottest/sendDocument" and request.content.count(body.encode()) == 3
    later = {
        "account": "Blocked",
        "reason": "PUBLIC_PROFILE_HTTP_429",
        "diagnostics": x_evidence("https://x.com/blocked", [429, 429, 429], "Rate limit exceeded"),
    }
    assert await report_entry_errors(repo, OPS, source, feed, [later], "2026-11-10T10:05:00Z") is None
    assert len(await entry_notices(repo)) == 1 and len(await entry_jobs(repo)) == 1


async def test_short_worker_http_error_is_one_message_with_the_whole_response():
    repo = MemoryRepository()
    gone = {
        "account": "gone",
        "reason": "PUBLIC_PROFILE_HTTP_404",
        "diagnostics": x_evidence(
            "https://x.com/gone", [404, 404, 404], '{"errors": [{"code": 34}]} <not found>'
        ),
    }
    notice_id = await report_entry_errors(
        repo, OPS, {"source_id": "S29", "name": "X"}, {"url": URL}, [gone], "2026-11-10T10:00:10Z"
    )
    notice = await repo.get("out.ops_notices", notice_id)
    assert "document" not in notice and units(html.escape(notice["text"])) <= MESSAGE_LIMIT
    assert notice["text"].count("x-response-time: ") == 3
    assert (
        notice["text"].count(
            'Тело ответа (38 байт, учётные данные скрыты):\n{"errors": [{"code": 34}]} <not found>'
        )
        == 3
    )
    [request] = await deliver_twice(repo, notice_id)
    assert request.url.path == "/bottest/sendMessage"
    assert json.loads(request.content)["text"] == html.escape(notice["text"])


async def test_referenced_post_http_error_is_reported_once_per_referenced_post():
    repo = MemoryRepository()
    source = {"source_id": "S29", "name": "X"}
    feed = {"url": URL}

    def unread(post_id, inside, status=404):
        url = f"https://x.com/i/status/{post_id}"
        return {
            "account": "alice",
            "reason": f"PUBLIC_POST_HTTP_{status}",
            "referenced_post": {"relation": "replied_to", "id": post_id, "url": url, "in": inside},
            "diagnostics": x_evidence(url, [status] * 3, "Not Found", "PUBLIC_POST_HTTP_"),
        }

    account_failure = {"account": "alice", "reason": "MISSING_FROM_RUN"}
    first = [account_failure, unread("40", "https://x.com/alice/status/105")]
    notice = await repo.get(
        "out.ops_notices", await report_entry_errors(repo, OPS, source, feed, first, "2026-11-10T10:00:10Z")
    )
    assert len(notice["entry_errors"]) == 2 and "document" not in notice
    assert (
        "2. PUBLICPOSTHTTP404\n"
        "Запись: аккаунт @alice · пост, на который дан ответ https://x.com/i/status/40 "
        "в посте https://x.com/alice/status/105\n"
        "Запрос к X: GET https://x.com/i/status/40\n"
        "Попыток: 3; итог: exhausted\n"
        "Попытка 1 · 2026-11-10T10:00:01+00:00 · PUBLIC_POST_HTTP_404\n"
        "Ответ: HTTP 404\n"
    ) in notice["text"]
    again = [
        unread("40", "https://x.com/alice/status/106", 403),
        unread("41", "https://x.com/alice/status/106"),
    ]
    second = await repo.get(
        "out.ops_notices", await report_entry_errors(repo, OPS, source, feed, again, "2026-11-10T10:05:00Z")
    )
    assert [error["referenced_post"]["id"] for error in second["entry_errors"]] == ["41"]
    assert await report_entry_errors(repo, OPS, source, feed, again, "2026-11-10T10:10:00Z") is None


async def test_s29_poll_reports_worker_http_errors_and_keeps_them_out_of_the_checkpoint(tmp_path):
    repo = MemoryRepository()
    source = {"source_id": "S29", "name": "X", "feeds": [{"kind": "x", "url": URL}]}
    settings = source_settings(tmp_path, [source])
    settings.ops_progress_enabled = True
    settings.apify_api_token = "FAKE-token"
    profile = x_evidence("https://x.com/blocked", [403, 403, 403], "PROFILE-BODY " * 400)
    reply = x_evidence("https://x.com/i/status/40", [404, 404, 404], "POST-BODY", "PUBLIC_POST_HTTP_")
    reference = {
        "relation": "replied_to",
        "id": "40",
        "author": None,
        "url": "https://x.com/i/status/40",
        "publishedAt": None,
        "text": None,
        "error": "PUBLIC_POST_HTTP_404",
        "diagnostics": reply,
    }
    accounts = [
        account("alice"),
        {**failed("blocked"), "error": "PUBLIC_PROFILE_HTTP_403", "diagnostics": profile},
    ]
    degraded = run(
        [{**post("105", "alice"), "referencedPosts": [reference]}],
        accounts,
        {"alice": "105"},
        health="degraded",
    )
    answers = [
        run([post("100", "alice")], [account("alice"), account("blocked")], {"alice": "100", "blocked": "7"})
    ]
    answers += [degraded, degraded]

    def respond(request):
        return httpx.Response(201, content=json.dumps(answers.pop(0)).encode())

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        service = SourceService(repo, settings, http=http)
        service.accounts = {
            handle: {"handle": handle, "role": "OTHER", "entity_id": handle}
            for handle in ("alice", "blocked")
        }
        for _ in range(3):
            await service.poll_once("S29")
    [notice] = await entry_notices(repo)
    assert [error["reason"] for error in notice["entry_errors"]] == [
        "PUBLIC_PROFILE_HTTP_403",
        "PUBLIC_POST_HTTP_404",
    ]
    text = notice["document"]["text"]
    assert text.count("PROFILE-BODY " * 400) == 3 and text.count("\nPOST-BODY") == 3
    assert (
        "пост, на который дан ответ https://x.com/i/status/40 в посте https://x.com/alice/status/105" in text
    )
    assert len(await entry_jobs(repo)) == 1
    checkpoint = await repo.get("ops.source_checkpoints", "S29:0")
    assert checkpoint["entry_errors"] == [{"account": "blocked", "reason": "PUBLIC_PROFILE_HTTP_403"}]
    assert "ops_entry_errors" not in checkpoint and "PROFILE-BODY" not in json.dumps(checkpoint)
    [item] = await repo.list("ingest.source_items")
    assert "[text unavailable: PUBLIC_POST_HTTP_404]" in item["quoted"] and "POST-BODY" not in json.dumps(
        item
    )


@pytest.mark.parametrize("second", ["broken", "recovered"])
async def test_failed_report_does_not_fail_acquisition_and_is_retried_next_poll(
    tmp_path, monkeypatch, second
):
    repo = MemoryRepository()
    report = ops_entry_errors.report_entry_errors
    calls = []

    async def flaky(*args):
        calls.append(args)
        if len(calls) == 1:
            raise RuntimeError("database unavailable")
        return await report(*args)

    monkeypatch.setattr("tank.sources.polling.report_entry_errors", flaky)
    results = await poll(repo, tmp_path, [broken("Bad"), broken("Bad") if second == "broken" else RSS])
    assert [result["items"] for result in results] == [1, 0]
    [notice] = await entry_notices(repo)
    assert "«Bad»" in notice["text"] and len(await entry_jobs(repo)) == 1
    checkpoint = await repo.get("ops.source_checkpoints", "S30:0")
    assert checkpoint["entry_error_reports"] == [] and not checkpoint.get("acquisition_error")
    assert bool(checkpoint["entry_errors"]) == (second == "broken")
    await poll(repo, tmp_path, [RSS])
    assert len(await entry_notices(repo)) == 1


@pytest.mark.parametrize(
    "backend", ["memory", pytest.param("postgres", marks=[pytest.mark.integration, pytest.mark.postgres])]
)
async def test_entry_error_notices_persist_and_deduplicate_in_storage(backend, postgres_template):
    async with fixture_repo(backend, postgres_template) as repo:
        settings = SimpleNamespace(ops_progress_enabled=True)
        source = {"source_id": "S30", "name": "Fixture blog"}
        errors = [{"reason": "ENTRY_WITHOUT_ID_OR_URL", "entry_sha256": "e" * 64, "payload_sha256": "p" * 64}]
        ids = await asyncio.gather(
            *(
                report_entry_errors(repo, settings, source, {"url": FEED_URL}, errors, "2026-10-24T12:00:00Z")
                for _ in range(3)
            )
        )
        assert len([notice_id for notice_id in ids if notice_id]) == 1
        assert len(await entry_notices(repo)) == 1
        assert len(await repo.list("out.ops_notices", filters={"kind": "ENTRY_ERROR"})) == 1
        assert len(await entry_jobs(repo)) == 1
