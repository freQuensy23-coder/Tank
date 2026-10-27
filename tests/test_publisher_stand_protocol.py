"""Pure wire admission preserves zero and refuses ambiguous identities/fields."""

import json

import pytest
from publisher_stand_protocol import (
    ChildExit,
    CommandReply,
    CommandRequest,
    ProtocolError,
    ReadyRequest,
    ShutdownReply,
    ShutdownRequest,
    decode_reply,
    decode_request,
    encode_reply,
    encode_request,
)


def test_named_request_phases_preserve_actual_empty_arguments_without_unused_fields() -> None:
    for request in (
        CommandRequest("a" * 32, "inspect", ("", "actual")),
        ReadyRequest("a" * 32),
        ShutdownRequest("a" * 32),
    ):
        assert decode_request(encode_request(request)) == request
    with pytest.raises(ProtocolError):
        decode_request(json.dumps({"kind": "READY", "nonce": "a" * 32, "arguments": []}).encode())


@pytest.mark.parametrize(
    "raw",
    [
        b'{"kind":"READY","kind":"SHUTDOWN","nonce":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}',
        b'{"kind":"READY"}',
        b'{"kind":"OTHER","nonce":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}',
        b'{"kind":"READY","nonce":false}',
        b'{"kind":"COMMAND","nonce":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","command":"inspect","arguments":[null]}',
        b'{"kind":"READY","nonce":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}{}',
    ],
)
def test_ambiguous_missing_unknown_and_malformed_requests_refuse(raw: bytes) -> None:
    with pytest.raises((ProtocolError, ValueError)):
        decode_request(raw)


def test_command_zero_status_and_opaque_stdout_are_not_truthiness_defaults() -> None:
    actual = CommandReply(123, 0, "worker completed\n", "")
    assert decode_reply(encode_reply(actual)) == actual
    for bad in (False, None, -1, 256, "0"):
        with pytest.raises(ProtocolError):
            decode_reply(
                json.dumps(
                    {"kind": "COMMAND", "process_id": 123, "code": bad, "stdout": "", "stderr": ""}
                ).encode()
            )


def test_drain_receipt_binds_container_identity_without_inventing_pid_epochs() -> None:
    actual = ShutdownReply(10, (ChildExit("a" * 64, 123, -15), ChildExit("b" * 64, 123, 0)))
    assert decode_reply(encode_reply(actual)) == actual
    with pytest.raises(ProtocolError):
        decode_reply(encode_reply(ShutdownReply(10, (actual.children[0], actual.children[0]))))


@pytest.mark.parametrize(
    "raw",
    [
        b"{}",
        b'{"kind":"READY","process_id":true}',
        b'{"kind":"READY","process_id":0}',
        b'{"kind":"READY","process_id":1,"unused":null}',
    ],
)
def test_server_identity_is_required_and_not_filled(raw: bytes) -> None:
    with pytest.raises(ProtocolError):
        decode_reply(raw)
