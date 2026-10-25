import httpx
import pytest
from pydantic import ValidationError
from source_support import fixture_http_admission

from tank.analysis.page_access import LivePageAccess, read_live_page
from tank.contracts.page_evidence import LivePageProvenance, PageRead, RecordedBodyProvenance
from tank.time_precision import Timestamp, collection_clock
from tank.util import sha256


async def test_live_page_retains_actual_binary_digest_and_borrowed_client_lifetime() -> None:
    body = b"\xff\x00binary"

    def response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body)

    def guard(request: httpx.Request) -> None:
        assert request.url.host == "example.org"

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        read = await read_live_page(
            LivePageAccess(fixture_http_admission()),
            "https://example.org/page",
            client=client,
            timeout_s=1.0,
            user_agent="Tank test",
            guard=guard,
        )
        assert not client.is_closed
    assert read.body == body and read.body_sha256 == sha256(body)
    assert read.media_type is None
    assert read.provenance.content_type_header is None
    assert isinstance(read.provenance, LivePageProvenance)
    assert read.provenance.received_at.precision == "COLLECTION_ONLY"


def test_page_bytes_and_original_body_claim_must_match_actual_digest() -> None:
    body = b"actual"
    with pytest.raises(ValidationError, match="actual read bytes"):
        PageRead(
            body=body,
            body_sha256="0" * 64,
            media_type=None,
            provenance=LivePageProvenance(
                mode="LIVE_RESPONSE", received_at=collection_clock(), content_type_header=None
            ),
        )
    with pytest.raises(ValidationError, match="captured digest"):
        PageRead(
            body=body,
            body_sha256=sha256(body),
            media_type=None,
            provenance=RecordedBodyProvenance(
                mode="VERIFIED_RECORDED_BODY",
                input_sha256="a" * 64,
                record_id="capture-1",
                captured_body_sha256="0" * 64,
                content_type_header=None,
            ),
        )


def test_live_page_receipt_cannot_claim_an_uncaptured_collection_instant() -> None:
    with pytest.raises(ValidationError, match="actual collection instant"):
        LivePageProvenance(
            mode="LIVE_RESPONSE",
            received_at=Timestamp(value=None, precision="COLLECTION_ONLY", precision_reason="NOT_CAPTURED"),
            content_type_header=None,
        )


async def test_live_page_error_does_not_expose_outgoing_credentials() -> None:
    credential = "synthetic-private-api-key"

    def response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, content=b"unavailable")

    def guard(request: httpx.Request) -> None:
        assert request.url.host == "api.etherscan.io"

    async with httpx.AsyncClient(
        params={"apikey": credential}, transport=httpx.MockTransport(response)
    ) as client:
        with pytest.raises(httpx.HTTPStatusError, match="HTTP 503") as failure:
            await read_live_page(
                LivePageAccess(fixture_http_admission()),
                "https://api.etherscan.io/v2/api?chainid=1",
                client=client,
                timeout_s=1.0,
                user_agent="Tank test",
                guard=guard,
            )
        assert not client.is_closed
    assert credential not in str(failure.value)
