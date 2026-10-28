import hashlib
import json
import os
import re
import tarfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from urllib.parse import quote, unquote, urlsplit
from xml.etree import ElementTree

import httpx
import pytest
import zstandard
from selectolax.parser import HTMLParser

from tank.agents.pipeline import Pipeline
from tank.event_tools import resolve_entity
from tank.settings import Settings
from tank.sources import SourceService, parsers
from tank.sources.organization import resolve_author_organization
from tank.storage import MemoryRepository
from tank.util import parse_time

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/github_coverage"
ATOM = "{http://www.w3.org/2005/Atom}"
WORLD_FEED = "https://github.com/worldcoin/world-id-protocol/commits/main.atom"
BILLING_SHA = "111f5675534dbe317cf91bc7707bc5897889e72c"
BILLING_ID = "tag:github.com,2008:Grit::Commit/" + BILLING_SHA
BILLING_TITLE = "feat: initial billing contracts (#1067)"
WORLD_SHA256 = "3bcdab309dd3cf42b8097f3075b7ad1bfa8d6741cc069337c3f5e07d3f127b81"


def feed_key(url):
    parsed = urlsplit(url)
    assert parsed.scheme == "https" and parsed.netloc == "github.com", url
    assert not parsed.query and not parsed.fragment, url
    parts = parsed.path.strip("/").split("/")
    assert len(parts) >= 3 and all(parts), url
    assert parts[2] in {"commits", "releases.atom"}, url
    if parts[2] == "commits":
        assert len(parts) >= 4 and parts[-1].endswith(".atom"), url
    else:
        assert len(parts) == 3, url
    return parts[0].casefold(), parts[1].casefold(), unquote("/".join(parts[2:]))


def selected_repositories(projects):
    return [
        (project, repository)
        for project in projects
        for repository in project["repositories"]
        if repository["decision"] in {"ADD", "ALREADY_COVERED"}
    ]


def selected_commit_url(repository):
    proposed = repository["proposed_feed_url"]
    if proposed:
        return proposed
    urls = [url for url in repository["existing_feed_urls"] if feed_key(url)[2].startswith("commits/")]
    assert len(urls) == 1, repository["full_name"]
    return urls[0]


@pytest.fixture
def world_atom_http_server():
    payload = (FIXTURES / "world-id-protocol-main.atom").read_bytes()
    requests = []
    stopped = threading.Event()
    request_path = urlsplit(WORLD_FEED).path

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def do_GET(self):
            requests.append({"path": self.path, "authorization": self.headers.get("Authorization")})
            if self.path != request_path:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/atom+xml")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True

    def serve():
        try:
            server.serve_forever(poll_interval=0.01)
        finally:
            stopped.set()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield SimpleNamespace(
            url=f"http://127.0.0.1:{server.server_port}{request_path}",
            path=request_path,
            payload=payload,
            requests=requests,
        )
    finally:
        shutdown = threading.Thread(target=server.shutdown, daemon=True)
        shutdown.start()
        assert stopped.wait(timeout=3), "Atom HTTP fixture did not stop within its shutdown grace"
        shutdown.join(timeout=3)
        server.server_close()
        thread.join(timeout=3)
        assert not thread.is_alive() and not shutdown.is_alive()


@pytest.fixture(scope="module")
def github_coverage():
    settings = Settings()
    registry = settings.load_config("registry")
    source = next(s for s in settings.load_config("sources")["sources"] if s["source_id"] == "S25")
    projects = json.loads((ROOT / "docs/github_coverage.json").read_text())["projects"]
    baseline = json.loads((FIXTURES / "baseline-s25.json").read_text())
    return SimpleNamespace(source=source, registry=registry, projects=projects, baseline=baseline)


def test_github_feeds_preserve_checkpoint_indexes_and_source_policy(github_coverage):
    source, baseline = github_coverage.source, github_coverage.baseline
    assert source["feeds"][: len(baseline["feeds"])] == baseline["feeds"]
    assert {key: value for key, value in source.items() if key != "feeds"} == {
        key: value for key, value in baseline.items() if key != "feeds"
    }
    keys = [feed_key(feed["url"]) for feed in source["feeds"]]
    assert len(keys) == len(set(keys))
    for feed in source["feeds"][len(baseline["feeds"]) :]:
        assert feed["kind"] == "atom" and feed["subtype"] == "commits", feed
        assert feed["interval_s"] == source["interval_s"], feed
        assert isinstance(feed.get("author_organization"), str) and feed["author_organization"].strip(), feed


def test_all_registry_protocols_have_exact_catalog_and_verified_commit_feed_coverage(github_coverage):
    source, registry, projects = github_coverage.source, github_coverage.registry, github_coverage.projects
    protocols = {row["entity_id"]: row for row in registry["entities"] if row["kind"] == "PROTOCOL"}
    assert len(protocols) == 22
    assert len(projects) == len(protocols)
    assert {project["entity_id"] for project in projects} == set(protocols)
    configured = {feed_key(feed["url"]): feed for feed in source["feeds"]}
    selected = selected_repositories(projects)
    selected_keys = [feed_key(selected_commit_url(repository)) for _, repository in selected]
    assert len(selected_keys) == len(set(selected_keys))
    assert set(selected_keys) == {
        feed_key(feed["url"]) for feed in source["feeds"] if feed["subtype"] == "commits"
    }
    added_keys = {
        feed_key(selected_commit_url(repository))
        for _, repository in selected
        if repository["decision"] == "ADD"
    }
    assert added_keys == {
        feed_key(feed["url"]) for feed in source["feeds"][len(github_coverage.baseline["feeds"]) :]
    }
    for project in projects:
        entity_id = project["entity_id"]
        assert project["protocol_name"] and project["token_id"] in protocols[entity_id]["tokens"], entity_id
        assert "S25" in protocols[entity_id]["official_sources"], entity_id
        assert any(candidate["entity_id"] == entity_id for candidate, _ in selected), entity_id
        organization_rows = project["official_organizations"]
        organizations = {row["login"].casefold() for row in organization_rows}
        assert organizations, entity_id
        for organization in project["official_organizations"]:
            assert organization["evidence_urls"], (entity_id, organization)
        for repository in project["repositories"]:
            assert repository["decision"] in {"ADD", "ALREADY_COVERED", "EXCLUDE", "GAP"}, repository
            if repository["decision"] not in {"ADD", "ALREADY_COVERED"}:
                continue
            full_name = repository["full_name"]
            assert re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", full_name), full_name
            assert full_name.split("/")[0].casefold() in organizations, full_name
            branch = repository["default_branch"]
            assert isinstance(branch, str) and branch.strip(), full_name
            expected_url = f"https://github.com/{full_name}/commits/{quote(branch, safe='/')}.atom"
            key = feed_key(selected_commit_url(repository))
            assert key == feed_key(expected_url), full_name
            configured_feed = configured[key]
            assert configured_feed["kind"] == "atom", full_name
            assert configured_feed["subtype"] == "commits", full_name
            evidence = repository["feed_evidence"]
            assert evidence["status"] == 200 and evidence["entry_count"] > 0, full_name
            assert re.fullmatch(r"[0-9a-f]{40}", evidence["latest_sha"]), full_name
            assert re.fullmatch(r"[0-9a-f]{64}", evidence["sha256"]), full_name
            assert parse_time(evidence["updated_at"]).tzinfo is not None, full_name
            assert evidence["archive_member"] == full_name.replace("/", "__") + ".atom", full_name


@pytest.mark.integration
def test_retained_atom_archive_matches_catalog_and_real_routing_inputs(github_coverage, record_property):
    source, registry, projects = github_coverage.source, github_coverage.registry, github_coverage.projects
    feeds = {feed_key(feed["url"]): feed for feed in source["feeds"]}
    settings = SimpleNamespace(load_config=lambda name: registry)
    pipeline = Pipeline(MemoryRepository(), settings, http=object())
    selected = selected_repositories(projects)
    expected_members = [repository["feed_evidence"]["archive_member"] for _, repository in selected]
    assert len(expected_members) == len(set(expected_members))
    covered_entities = set()
    entry_count = 0
    with tarfile.open(FIXTURES / "feeds.tar.gz", "r:gz") as archive:
        members = archive.getmembers()
        assert all(member.isfile() for member in members)
        assert len(members) == len(expected_members)
        assert {member.name for member in members} == set(expected_members)
        payloads = {}
        for info in members:
            member = archive.extractfile(info)
            assert member is not None, info.name
            with member:
                payloads[info.name] = member.read()
        for project, repository in selected:
            evidence = repository["feed_evidence"]
            payload = payloads[evidence["archive_member"]]
            assert hashlib.sha256(payload).hexdigest() == evidence["sha256"], repository["full_name"]
            root = ElementTree.fromstring(payload)
            assert root.tag == ATOM + "feed", repository["full_name"]
            entries = root.findall(ATOM + "entry")
            assert len(entries) == evidence["entry_count"], repository["full_name"]
            entry_count += len(entries)
            latest_id = entries[0].findtext(ATOM + "id")
            assert latest_id is not None
            assert latest_id.rsplit("/", 1)[-1] == evidence["latest_sha"]
            assert parse_time(entries[0].findtext(ATOM + "updated")) == parse_time(evidence["updated_at"])
            feed = feeds[feed_key(selected_commit_url(repository))]
            rows = parsers.feeds(payload, "S25", feed)
            assert not rows.errors and len(rows) == len(entries), repository["full_name"]
            assert {row["external_id"] for row in rows} == {
                entry.findtext(ATOM + "id") for entry in entries
            }, repository["full_name"]
            expected_repository = tuple(part.casefold() for part in repository["full_name"].split("/"))
            for row in rows:
                location = urlsplit(row["url"])
                assert location.scheme == "https" and location.netloc == "github.com", row
                actual_repository = tuple(part.casefold() for part in location.path.split("/")[1:3])
                assert actual_repository == expected_repository
                assert row["subtype"] == "commits", row
            assert parse_time(rows[0]["source_updated_at"]) == parse_time(evidence["updated_at"])
            assert rows[0]["source_updated_at_precision"] == "EXACT"
            item = dict(
                rows[0],
                source_id="S25",
                source=source["name"],
                author=source["name"],
                feed=feed["url"],
            )
            item["author_organization"] = resolve_author_organization(source, item)
            identities = pipeline.identities(item)
            assert {project["entity_id"], project["token_id"]} <= identities, repository["full_name"]
            view = Pipeline.item_view(item)
            assert view["source"] == source["name"]
            assert "author_organization" not in view
            assert "author" not in view
            assert view == Pipeline.state(item)["item"]
            assert view == Pipeline.item_view(
                {key: value for key, value in item.items() if key != "author_organization"}
            )
            covered_entities.add(project["entity_id"])
    assert covered_entities == {
        entity["entity_id"] for entity in registry["entities"] if entity["kind"] == "PROTOCOL"
    }
    record_property("selected_repository_count", len(selected))
    record_property("archive_member_count", len(members))
    record_property("retained_atom_entry_count", entry_count)
    record_property("covered_protocol_count", len(covered_entities))
    record_property("archive_path", str(FIXTURES / "feeds.tar.gz"))


@pytest.mark.parametrize(
    "entity_id,alias,short_name,qualifier",
    [
        ("lighter", "Lighter.xyz", "Lighter", "LIT"),
        ("plasma", "Plasma chain", "Plasma", "XPL"),
        ("world", "World Network", "World", "Network"),
    ],
)
def test_short_protocol_names_need_real_publisher_context_for_routing(
    github_coverage, entity_id, alias, short_name, qualifier
):
    source, registry = github_coverage.source, github_coverage.registry
    project = next(project for project in github_coverage.projects if project["entity_id"] == entity_id)
    repository = next(row for row in project["repositories"] if row["decision"] == "ADD")
    feed = next(
        feed for feed in source["feeds"] if feed_key(feed["url"]) == feed_key(selected_commit_url(repository))
    )
    item = {
        "source_id": "S25",
        "source": source["name"],
        "author": source["name"],
        "feed": feed["url"],
        "title": short_name,
    }
    settings = SimpleNamespace(load_config=lambda name: registry)
    pipeline = Pipeline(MemoryRepository(), settings, http=object())
    assert entity_id not in pipeline.identities(item)
    before = Pipeline.item_view(item)
    item["author_organization"] = resolve_author_organization(source, item)
    assert entity_id in pipeline.identities(item)
    assert resolve_entity(registry, alias) == entity_id
    assert resolve_entity(registry, short_name, context=qualifier) == entity_id
    assert Pipeline.item_view(item) == before
    assert Pipeline.state(item)["item"] == before
    if entity_id in {"lighter", "plasma"}:
        assert resolve_entity(registry, short_name) == "UNRESOLVED:" + short_name


@pytest.mark.integration
@pytest.mark.postgres
async def test_world_billing_atom_poll_persists_real_filter_admission_idempotently(
    tmp_path, postgres_template, world_atom_http_server, record_property
):
    database_url = os.getenv("TANK_SOURCE_TEST_DATABASE_URL")
    assert database_url, "Requires the existing isolated CI source PostgreSQL fixture"
    settings = Settings(data_dir=tmp_path, user_agent="Tank offline GitHub regression")
    source = next(s for s in settings.load_config("sources")["sources"] if s["source_id"] == "S25")
    feed = next(feed for feed in source["feeds"] if feed_key(feed["url"]) == feed_key(WORLD_FEED))
    stand = world_atom_http_server
    test_feed = dict(feed, url=stand.url)
    isolated_source = dict(source, feeds=[test_feed])
    assert feed["url"] == WORLD_FEED
    assert {key: value for key, value in test_feed.items() if key != "url"} == {
        key: value for key, value in feed.items() if key != "url"
    }
    assert urlsplit(test_feed["url"]).path == urlsplit(feed["url"]).path
    payload = stand.payload
    assert hashlib.sha256(payload).hexdigest() == WORLD_SHA256
    root = ElementTree.fromstring(payload)
    assert root.findtext(ATOM + "updated") == "2026-10-28T12:05:22Z"
    self_link = root.find(ATOM + "link[@rel='self']")
    assert self_link is not None and self_link.get("href") == WORLD_FEED
    entries = root.findall(ATOM + "entry")
    assert entries[0].findtext(ATOM + "id") == BILLING_ID
    assert entries[0].findtext(ATOM + "updated") == "2026-10-28T12:05:22Z"
    external_ids = {entry.findtext(ATOM + "id") for entry in entries}
    assert len(external_ids) == len(entries)
    billing_entry = next(entry for entry in entries if entry.findtext(ATOM + "id") == BILLING_ID)
    billing_lines = [
        line.strip()
        for line in HTMLParser(billing_entry.findtext(ATOM + "content")).text().splitlines()
        if line.strip()
    ]
    assert billing_lines[0] == BILLING_TITLE
    assert len(billing_lines) > 1 and all(line.startswith("Co-authored-by:") for line in billing_lines[1:])
    expected_body = " ".join(billing_lines)
    object.__setattr__(
        settings,
        "load_config",
        lambda name: (
            {"sources": [isolated_source]} if name == "sources" else Settings.load_config(settings, name)
        ),
    )
    async with (
        postgres_template.repository(database_url) as repo,
        httpx.AsyncClient(trust_env=False, timeout=10) as http,
    ):
        service = SourceService(repo, settings, http=http)
        try:
            started = perf_counter()
            first = await service.poll_once("S25")
            first_poll_seconds = perf_counter() - started
            assert first["status"] == "HEALTHY" and first["errors"] == [], first
            rows = await repo.list("ingest.source_items", filters={"source_id": "S25"})
            assert first["items"] == len(entries) == len(rows)
            assert {row["external_id"] for row in rows} == external_ids
            billing = next(row for row in rows if row["external_id"] == BILLING_ID)
            assert billing["title"] == BILLING_TITLE and billing["text"] == expected_body
            billing_url = "https://github.com/worldcoin/world-id-protocol/commit/" + BILLING_SHA
            assert billing["url"] == billing_url
            assert billing["source_id"] == "S25" and billing["feed"] == test_feed["url"]
            assert billing["source"] == "GitHub" and billing["author"] == "GitHub"
            assert billing["author_organization"] == feed["author_organization"]
            assert {"world", "world:WLD"} <= Pipeline(repo, settings, http=http).identities(billing)
            assert billing["subtype"] == "commits" and billing["source_class"] == "TECHNICAL_EVIDENCE"
            assert billing["source_updated_at"] == "2026-10-28T12:05:22Z"
            assert billing["source_updated_at_precision"] == "EXACT"
            assert billing["published_at"] is None
            assert billing["relevance"] == {"decision": None, "pending_reason": "AWAITING_FILTER"}
            assert billing["extraction_status"] == "PENDING" and billing["deferrable"] is True
            jobs = await repo.list("ops.jobs", filters={"type": "FILTER_ITEM"})
            assert len(jobs) == len(rows)
            assert {job["idem_key"] for job in jobs} == {row["item_id"] for row in rows}
            assert all(job["payload"] == {"item_id": job["idem_key"]} for job in jobs)
            assert all(job["status"] == "QUEUED" and job["priority"] == 2 for job in jobs)
            assert await repo.list("ops.jobs", filters={"type": "ROUTE_ITEM"}) == []
            assert await repo.list("ops.llm_usage") == []
            assert await repo.list("evt.events") == []
            assert await repo.list("out.alerts") == []
            raw_before = {path: path.read_bytes() for path in tmp_path.rglob("*.zst")}
            assert set(raw_before) == {Path(row["raw_ref"]) for row in rows}
            for row in rows:
                assert row["raw_sha256"] == WORLD_SHA256
                assert row["observation_count"] == 1
                compressed = raw_before[Path(row["raw_ref"])]
                assert zstandard.ZstdDecompressor().decompress(compressed) == payload
            async with repo.transaction():
                projected = await (
                    await repo._conn().execute(
                        "SELECT source_id,external_id,relevance_decision,body,raw_path,raw_sha256 "
                        "FROM ingest.source_items WHERE item_id=%s",
                        (billing["item_id"],),
                    )
                ).fetchone()
                queued = await (
                    await repo._conn().execute(
                        "SELECT type,status,priority,idem_key,document->'payload' AS payload FROM ops.jobs "
                        "WHERE document->'payload'->>'item_id'=%s",
                        (billing["item_id"],),
                    )
                ).fetchall()
            assert projected["source_id"] == "S25" and projected["external_id"] == BILLING_ID
            assert projected["body"] == expected_body and projected["relevance_decision"] is None
            assert projected["raw_path"] == billing["raw_ref"]
            assert projected["raw_sha256"] == WORLD_SHA256
            assert len(queued) == 1
            assert queued[0] == {
                "type": "FILTER_ITEM",
                "status": "QUEUED",
                "priority": 2,
                "idem_key": billing["item_id"],
                "payload": {"item_id": billing["item_id"]},
            }
            started = perf_counter()
            second = await service.poll_once("S25")
            second_poll_seconds = perf_counter() - started
            assert second == {"source_id": "S25", "status": "HEALTHY", "items": 0, "errors": []}
            assert stand.requests == [{"path": stand.path, "authorization": None}] * 2
            after_rows = await repo.list("ingest.source_items")
            after_jobs = await repo.list("ops.jobs", filters={"type": "FILTER_ITEM"})
            assert sorted(after_rows, key=lambda row: row["item_id"]) == sorted(
                rows, key=lambda row: row["item_id"]
            )
            assert sorted(after_jobs, key=lambda job: job["job_id"]) == sorted(
                jobs, key=lambda job: job["job_id"]
            )
            assert {path: path.read_bytes() for path in tmp_path.rglob("*.zst")} == raw_before
            checkpoint = await repo.get("ops.source_checkpoints", "S25:0")
            assert checkpoint["http_status"] == 200 and checkpoint["payload_sha256"] == WORLD_SHA256
            assert checkpoint["pending_batches"] == []
            view = Pipeline.state(billing)["item"]
            assert view["title"] == BILLING_TITLE and view["source"] == "GitHub"
            assert "author_organization" not in view and "World Network" not in json.dumps(view)
            for name, value in {
                "boundary": "retained-public-atom->real-http->postgres-ingest->queued-FILTER_ITEM",
                "canonical_configured_feed": feed["url"],
                "test_http_endpoint": test_feed["url"],
                "request_path": stand.path,
                "http_request_count": len(stand.requests),
                "authorization_header_count": sum(r["authorization"] is not None for r in stand.requests),
                "fixture_path": str(FIXTURES / "world-id-protocol-main.atom"),
                "payload_bytes": len(payload),
                "payload_sha256": WORLD_SHA256,
                "atom_entry_count": len(entries),
                "ingest_item_count": len(rows),
                "filter_admission_count": len(jobs),
                "raw_file_count": len(raw_before),
                "billing_external_id": BILLING_ID,
                "billing_source_updated_at": billing["source_updated_at"],
                "second_poll_new_item_count": second["items"],
                "first_poll_seconds": first_poll_seconds,
                "second_poll_seconds": second_poll_seconds,
                "classifier_executed": False,
                "telegram_delivery_executed": False,
            }.items():
                record_property(name, value)
        finally:
            await service.close()
