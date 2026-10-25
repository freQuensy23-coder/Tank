import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from pydantic import SecretStr, ValidationError

from tank.deployment.api_contracts import ReportedAPIReason, UnreportedAPIReason
from tank.deployment.apify_api import ApifyAPI, ApifyAPIError, ApifyEndpoint, ApifyHTTPFailure

pytestmark = pytest.mark.integration


@dataclass(frozen=True)
class RequestObservation:
    method: str
    path: str
    authorization: str | None
    body: bytes


@dataclass
class ApiPeer:
    endpoint: ApifyEndpoint
    received: list[RequestObservation] = field(default_factory=list)


@contextmanager
def api_peer(document: bytes, *, status: int, location: str | None) -> Iterator[ApiPeer]:
    observed: list[RequestObservation] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.respond()

        def do_POST(self) -> None:
            self.respond()

        def do_PUT(self) -> None:
            self.respond()

        def respond(self) -> None:
            length = self.headers.get("Content-Length")
            body = b"" if length is None else self.rfile.read(int(length))
            observed.append(
                RequestObservation(self.command, self.path, self.headers.get("Authorization"), body)
            )
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(document)))
            if location is not None:
                self.send_header("Location", location)
            self.end_headers()
            self.wfile.write(document)

        def log_message(self, format: str, *args: str) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield ApiPeer(
            endpoint=ApifyEndpoint(base_url=f"http://127.0.0.1:{server.server_port}/v2"),
            received=observed,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def client(peer: ApiPeer) -> ApifyAPI:
    return ApifyAPI(SecretStr("synthetic-publisher-token"), peer.endpoint)


def test_actual_actor_read_projects_required_facts_and_keeps_token_out_of_url() -> None:
    document = (
        b'{"data":{"id":"rbf5CQKQgKNVl4SfO","taggedBuilds":{"direct":{"buildId":"AAAAAAAAAAAAAAAAA"}},'
        b'"versions":[{"versionNumber":"0.3"}],"unconsumedProviderField":{"opaque":true}},"statistics":{}}'
    )
    with api_peer(document, status=200, location=None) as peer:
        actor = client(peer).actor()
        assert actor.taggedBuilds.direct.buildId == "A" * 17
        assert [version.versionNumber for version in actor.versions] == ["0.3"]
        assert len(peer.received) == 1
        request = peer.received[0]
        assert request.path == "/v2/actors/rbf5CQKQgKNVl4SfO"
        assert request.authorization == "Bearer synthetic-publisher-token"
        assert request.method == "GET"
        assert request.body == b""


@pytest.mark.parametrize(
    "document",
    [
        b'{"data":null}',
        b'{"data":[]}',
        b'{"data":{"id":"rbf5CQKQgKNVl4SfO","versions":[]}}',
        b'{"data":{"id":"wrong-actor","taggedBuilds":{"direct":{"buildId":"AAAAAAAAAAAAAAAAA"}},"versions":[]}}',
        b'{"data":{"id":"rbf5CQKQgKNVl4SfO","taggedBuilds":{"direct":{"buildId":"AAAAAAAAAAAAAAAAA"}}}}',
    ],
)
def test_actual_actor_read_refuses_missing_or_wrong_consumed_facts(document: bytes) -> None:
    with api_peer(document, status=200, location=None) as peer:
        with pytest.raises((ApifyAPIError, ValidationError)):
            client(peer).actor()
        assert len(peer.received) == 1


def test_actual_build_snapshot_decodes_text_binary_and_folder_records() -> None:
    document = (
        b'{"data":{"id":"AAAAAAAAAAAAAAAAA","actId":"rbf5CQKQgKNVl4SfO","status":"SUCCEEDED",'
        b'"actVersion":{"sourceType":"SOURCE_FILES","sourceFiles":['
        b'{"name":"folder","folder":true},'
        b'{"name":"main.py","format":"TEXT","content":"print(1)\\n"},'
        b'{"name":"binary","format":"BASE64","content":"/wA="}]}}}'
    )
    with api_peer(document, status=200, location=None) as peer:
        build = client(peer).completed_build("A" * 17)
        assert build.actVersion.decoded() == {"main.py": b"print(1)\n", "binary": b"\xff\x00"}
        assert peer.received[0].path == "/v2/actor-builds/" + "A" * 17


def test_actual_build_lookup_refuses_a_different_valid_identity() -> None:
    document = (
        b'{"data":{"id":"BBBBBBBBBBBBBBBBB","actId":"rbf5CQKQgKNVl4SfO","status":"SUCCEEDED",'
        b'"actVersion":{"sourceType":"SOURCE_FILES","sourceFiles":[]}}}'
    )
    with api_peer(document, status=200, location=None) as peer:
        with pytest.raises(ApifyAPIError, match="identity mismatch"):
            client(peer).completed_build("A" * 17)
        assert peer.received[0].path.endswith("A" * 17)


def test_real_redirect_never_forwards_credentials_or_contacts_destination() -> None:
    with (
        api_peer(b'{"data":{}}', status=200, location=None) as destination,
        api_peer(b"", status=302, location=destination.endpoint.base_url) as origin,
    ):
        with pytest.raises(ApifyAPIError, match="HTTP 302"):
            client(origin).actor()
        assert len(origin.received) == 1
        assert destination.received == []


def test_http_error_preserves_provider_reason_without_exposing_token() -> None:
    with api_peer(
        b'{"error":{"type":"invalid-input","message":"synthetic-publisher-token request refused"}}',
        status=400,
        location=None,
    ) as peer:
        with pytest.raises(ApifyHTTPFailure, match="HTTP 400: invalid-input") as caught:
            client(peer).actor()
        assert caught.value.status_code == 400
        assert isinstance(caught.value.diagnostic, ReportedAPIReason)
        assert "synthetic-publisher-token" not in str(caught.value)
        assert "[token] request refused" in str(caught.value)


def test_actual_build_tag_fits_provider_limit_with_approved_sha_prefix() -> None:
    with api_peer(b'{"data":{"id":"AAAAAAAAAAAAAAAAA","status":"READY"}}', status=201, location=None) as peer:
        observed = client(peer).start_build("b" * 40)
        assert observed.id == "A" * 17
        assert peer.received[0].path == "/v2/actors/rbf5CQKQgKNVl4SfO/builds?version=0.3&tag=tank-" + "b" * 12
        assert peer.received[0].method == "POST"


@pytest.mark.parametrize("document", [b"not JSON", b'{"error":{"message":1}}'])
def test_malformed_provider_reason_preserves_http_failure_without_coercing_details(document: bytes) -> None:
    with api_peer(document, status=403, location=None) as peer:
        with pytest.raises(ApifyHTTPFailure) as caught:
            client(peer).actor()
        assert caught.value.status_code == 403
        assert isinstance(caught.value.diagnostic, UnreportedAPIReason)
        assert caught.value.diagnostic.reason == "INVALID_JSON_OR_FIELDS"
        assert str(caught.value) == "Apify GET failed with HTTP 403"


def test_provider_reason_is_redacted_before_its_display_bound() -> None:
    document = b'{"error":{"type":"invalid-input","message":"synthetic-publisher-token ' + b"x" * 600 + b'"}}'
    with api_peer(document, status=400, location=None) as peer:
        with pytest.raises(ApifyHTTPFailure) as caught:
            client(peer).actor()
        diagnostic = caught.value.diagnostic
        assert isinstance(diagnostic, ReportedAPIReason)
        assert len(diagnostic.text) == 300
        assert diagnostic.text.startswith("invalid-input [token] ")
        assert "synthetic-publisher-token" not in str(caught.value)


@pytest.mark.parametrize(
    "entries",
    [
        b'[{"name":"main.py","folder":0,"format":"TEXT","content":"x"}]',
        b'[{"name":"main.py","folder":1,"format":"TEXT","content":"x"}]',
        b'[{"name":"../main.py","format":"TEXT","content":"x"}]',
        b'[{"name":"main.py","format":"BASE64","content":"%%%"}]',
        b'[{"name":"main.py","format":"UNKNOWN","content":"x"}]',
        b'[{"name":"bad\\u0000name","format":"TEXT","content":"x"}]',
        b'[{"name":"main.py","format":"TEXT","content":"one"},{"name":"main.py","format":"TEXT","content":"two"}]',
    ],
)
def test_actual_snapshot_refuses_malformed_file_facts_before_digesting(entries: bytes) -> None:
    document = (
        b'{"data":{"id":"AAAAAAAAAAAAAAAAA","actId":"rbf5CQKQgKNVl4SfO","status":"SUCCEEDED",'
        b'"actVersion":{"sourceType":"SOURCE_FILES","sourceFiles":' + entries + b"}}}"
    )
    with api_peer(document, status=200, location=None) as peer:
        with pytest.raises(ValueError):
            client(peer).completed_build("A" * 17).actVersion.decoded()
        assert len(peer.received) == 1
