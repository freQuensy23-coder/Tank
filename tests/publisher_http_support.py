"""Actual loopback HTTP serialization for the typed synthetic publisher provider."""

import json
import ssl
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Literal
from urllib.parse import parse_qs, urlsplit

from publisher_state_support import ProviderState

from tank.contracts import DomainModel
from tank.deployment.api_contracts import (
    ActorSnapshot,
    ActorTagUpdate,
    BuildObservation,
    CompletedBuildSnapshot,
    HistoricalBuildSnapshot,
    ProviderErrorDetail,
    ProviderErrorEnvelope,
    UploadedVersion,
    VersionUpload,
)
from tank.deployment.apify_api import ApifyEndpoint, Method
from tank.deployment.contracts import BuildID
from tank.deployment.identities import CommitSHA
from tank.deployment.journal import PublisherJournal

type Reply = (
    ActorSnapshot | BuildObservation | CompletedBuildSnapshot | UploadedVersion | HistoricalBuildSnapshot
)
type LegacyFault = Literal["source", "version", "build_number", "status", "actor"]
type WireFault = Literal["ACTOR_IDENTITY", "UPLOAD_FAILURE", "BUILD_TAG_LENGTH"] | LegacyFault


class ObservedRequest(DomainModel):
    method: Method
    path: str


@dataclass(frozen=True)
class PublisherHTTPPeer:
    endpoint: ApifyEndpoint
    requests: list[ObservedRequest]
    server_port: int


@contextmanager
def serve_peer(
    provider: ProviderState,
    journal: PublisherJournal,
    selected_sha: CommitSHA,
    *,
    port: int,
    context: ssl.SSLContext | None,
    after_tag_update: Callable[[], bool] | None,
    after_build_start: Callable[[], bool] | None,
    after_build_snapshot: Callable[[BuildID], bool] | None,
    wire_fault: WireFault | None,
) -> Iterator[PublisherHTTPPeer]:
    requests: list[ObservedRequest] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.respond()

        def do_POST(self) -> None:
            self.respond()

        def do_PUT(self) -> None:
            self.respond()

        def respond(self) -> None:
            if self.headers.get("Authorization") != "Bearer synthetic-publisher-image-token":
                self.send_error(403)
                return
            requests.append(ObservedRequest.model_validate({"method": self.command, "path": self.path}))
            path = urlsplit(self.path)
            query = parse_qs(path.query)
            length = self.headers.get("Content-Length")
            body = b"" if length is None else self.rfile.read(int(length))
            actor = "/v2/actors/rbf5CQKQgKNVl4SfO"
            reply: Reply
            if self.command == "GET" and path.path == actor:
                reply = provider.actor()
            elif self.command == "PUT" and path.path == actor:
                update = ActorTagUpdate.model_validate_json(body)
                transaction = journal.read_transaction()
                assert transaction is not None and transaction.activation_attempted
                try:
                    provider.update_direct_tag(update)
                except RuntimeError:
                    if provider.fault != "TAG_UNCERTAIN":
                        raise
                    self.close_connection = True
                    return
                if after_tag_update is not None and after_tag_update():
                    self.close_connection = True
                    return
                reply = provider.actor()
            elif self.command in {"POST", "PUT"} and path.path in {
                actor + "/versions",
                actor + "/versions/0.3",
            }:
                if wire_fault == "UPLOAD_FAILURE":
                    self.send_error(503)
                    return
                provider.upload_version(
                    VersionUpload.model_validate_json(body), replace=self.command == "PUT"
                )
                reply = provider.uploaded_version()
            elif self.command == "GET" and path.path == actor + "/versions/0.3":
                reply = provider.uploaded_version()
            elif self.command == "POST" and path.path == actor + "/builds":
                if wire_fault == "BUILD_TAG_LENGTH":
                    assert len(query["tag"]) == 1 and len(query["tag"][0]) > 30
                    failure = ProviderErrorEnvelope(
                        error=ProviderErrorDetail(
                            type="invalid-input", message="tag must be at most 30 characters long"
                        )
                    )
                    document = failure.model_dump_json().encode()
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(document)))
                    self.end_headers()
                    self.wfile.write(document)
                    return
                assert query == {"version": ["0.3"], "tag": ["tank-" + selected_sha[:12]]}
                reply = provider.start_build(selected_sha)
                if after_build_start is not None and after_build_start():
                    self.close_connection = True
                    return
            elif self.command == "GET" and path.path == "/v2/actor-builds/c99YRMlKXube5V0gV":
                reply = provider.historical_build()
            elif self.command == "GET" and path.path.startswith("/v2/actor-builds/"):
                build_id = path.path.removeprefix("/v2/actor-builds/")
                reply = provider.wait_build(build_id) if query else provider.completed_build(build_id)
                if not query and after_build_snapshot is not None and after_build_snapshot(build_id):
                    self.close_connection = True
                    return
            else:
                self.send_error(404)
                return
            document = json.dumps({"data": reply.model_dump(mode="json")}).encode()
            if wire_fault == "ACTOR_IDENTITY" and isinstance(reply, CompletedBuildSnapshot):
                document = document.replace(b'"actId": "rbf5CQKQgKNVl4SfO"', b'"actId": "unrelated"')
            if isinstance(reply, HistoricalBuildSnapshot):
                match wire_fault:
                    case "source":
                        document = document.replace(
                            b'"content": "unverified fixture"', b'"content": "changed"'
                        )
                    case "version":
                        document = document.replace(b'"versionNumber": "0.2"', b'"versionNumber": "9.9"')
                    case "build_number":
                        document = document.replace(b'"buildNumber": "0.2.2"', b'"buildNumber": "0.2.3"')
                    case "status":
                        document = document.replace(b'"status": "SUCCEEDED"', b'"status": "FAILED"')
                    case "actor":
                        document = document.replace(b'"actId": "rbf5CQKQgKNVl4SfO"', b'"actId": "unrelated"')
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(document)))
            self.end_headers()
            self.wfile.write(document)

        def log_message(self, format: str, *args: str) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    if context is not None:
        server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        endpoint = (
            "https://api.apify.com/v2" if context is not None else f"http://127.0.0.1:{server.server_port}/v2"
        )
        yield PublisherHTTPPeer(ApifyEndpoint(base_url=endpoint), requests, server.server_port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@contextmanager
def publisher_http_peer(
    provider: ProviderState, journal: PublisherJournal, selected_sha: CommitSHA
) -> Iterator[PublisherHTTPPeer]:
    with serve_peer(
        provider,
        journal,
        selected_sha,
        port=0,
        context=None,
        after_tag_update=None,
        after_build_start=None,
        after_build_snapshot=None,
        wire_fault=None,
    ) as peer:
        yield peer


@contextmanager
def publisher_https_peer(
    provider: ProviderState,
    journal: PublisherJournal,
    selected_sha: CommitSHA,
    *,
    certificate: Path,
    private_key: Path,
    after_tag_update: Callable[[], bool] | None,
    after_build_start: Callable[[], bool] | None,
    after_build_snapshot: Callable[[BuildID], bool] | None,
) -> Iterator[PublisherHTTPPeer]:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, private_key)
    with serve_peer(
        provider,
        journal,
        selected_sha,
        port=443,
        context=context,
        after_tag_update=after_tag_update,
        after_build_start=after_build_start,
        after_build_snapshot=after_build_snapshot,
        wire_fault=None,
    ) as peer:
        yield peer


@contextmanager
def publisher_wrong_actor_peer(
    provider: ProviderState,
    journal: PublisherJournal,
    selected_sha: CommitSHA,
) -> Iterator[PublisherHTTPPeer]:
    with serve_peer(
        provider,
        journal,
        selected_sha,
        port=0,
        context=None,
        after_tag_update=None,
        after_build_start=None,
        after_build_snapshot=None,
        wire_fault="ACTOR_IDENTITY",
    ) as peer:
        yield peer


@contextmanager
def publisher_upload_failure_peer(
    provider: ProviderState,
    journal: PublisherJournal,
    selected_sha: CommitSHA,
) -> Iterator[PublisherHTTPPeer]:
    with serve_peer(
        provider,
        journal,
        selected_sha,
        port=0,
        context=None,
        after_tag_update=None,
        after_build_start=None,
        after_build_snapshot=None,
        wire_fault="UPLOAD_FAILURE",
    ) as peer:
        yield peer


@contextmanager
def publisher_legacy_fault_peer(
    provider: ProviderState,
    journal: PublisherJournal,
    selected_sha: CommitSHA,
    fault: LegacyFault,
) -> Iterator[PublisherHTTPPeer]:
    with serve_peer(
        provider,
        journal,
        selected_sha,
        port=0,
        context=None,
        after_tag_update=None,
        after_build_start=None,
        after_build_snapshot=None,
        wire_fault=fault,
    ) as peer:
        yield peer


@contextmanager
def publisher_tag_limit_peer(
    provider: ProviderState,
    journal: PublisherJournal,
    selected_sha: CommitSHA,
) -> Iterator[PublisherHTTPPeer]:
    with serve_peer(
        provider,
        journal,
        selected_sha,
        port=0,
        context=None,
        after_tag_update=None,
        after_build_start=None,
        after_build_snapshot=None,
        wire_fault="BUILD_TAG_LENGTH",
    ) as peer:
        yield peer
