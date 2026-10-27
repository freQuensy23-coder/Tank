"""Real native guard controls use newly created synthetic files only."""

import array
import builtins
import io
import os
import socket
from pathlib import Path
from typing import Literal

import conftest as policy
import pytest


class ChangingPath(os.PathLike[str]):
    def __init__(self, admitted: Path, later: Path) -> None:
        self.admitted = admitted
        self.later = later
        self.calls = 0

    def __fspath__(self) -> str:
        self.calls += 1
        return str(self.admitted if self.calls == 1 else self.later)


@pytest.mark.parametrize("library", ["BUILTIN", "IO"])
def test_native_open_uses_the_one_admitted_pathlike_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, library: Literal["BUILTIN", "IO"]
) -> None:
    public = tmp_path / "public.txt"
    protected = tmp_path / "synthetic-credential.txt"
    public.write_text("synthetic public")
    protected.write_text("synthetic protected")
    monkeypatch.setattr(policy, "_PROTECTED_CREDENTIAL_FILES", {protected})
    monkeypatch.setattr(policy, "_PROTECTED_CREDENTIAL_ROOTS", ())
    path = ChangingPath(public, protected)
    opener = policy._open if library == "BUILTIN" else policy._io_open
    with opener(path, "r", encoding="utf-8") as stream:
        assert stream.read() == "synthetic public"
    assert path.calls == 1


@pytest.mark.parametrize("library", ["BUILTIN", "IO"])
def test_protected_first_pathlike_identity_is_refused_before_native_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, library: Literal["BUILTIN", "IO"]
) -> None:
    public = tmp_path / "public.txt"
    protected = tmp_path / "synthetic-credential.txt"
    public.write_text("synthetic public")
    protected.write_text("synthetic protected")
    monkeypatch.setattr(policy, "_PROTECTED_CREDENTIAL_FILES", {protected})
    monkeypatch.setattr(policy, "_PROTECTED_CREDENTIAL_ROOTS", ())
    path = ChangingPath(protected, public)
    opener = policy._open if library == "BUILTIN" else policy._io_open
    with pytest.raises(policy.OfflineCredentialError), opener(path, "r", encoding="utf-8"):
        pass
    assert path.calls == 1


@pytest.mark.parametrize("library", ["BUILTIN", "IO"])
def test_binary_unbuffered_native_open(tmp_path: Path, library: Literal["BUILTIN", "IO"]) -> None:
    path = tmp_path / "payload.bin"
    path.write_bytes(b"synthetic\x00\xff")
    opener = builtins.open if library == "BUILTIN" else io.open
    with opener(path, "rb", buffering=0) as stream:
        assert stream.read() == b"synthetic\x00\xff"


@pytest.mark.parametrize("library", ["BUILTIN", "IO"])
@pytest.mark.parametrize("bytes_path", [False, True])
def test_custom_native_opener_receives_admitted_path_and_flags(
    tmp_path: Path, library: Literal["BUILTIN", "IO"], bytes_path: bool
) -> None:
    path = tmp_path / "payload.bin"
    path.write_bytes(b"synthetic")
    filename = os.fsencode(path) if bytes_path else str(path)
    calls: list[tuple[str | bytes, int]] = []

    def acquire(name: str | bytes, flags: int) -> int:
        calls.append((name, flags))
        return os.open(name, flags)

    opener = builtins.open if library == "BUILTIN" else io.open
    with opener(filename, "rb", opener=acquire) as stream:
        assert stream.read() == b"synthetic"
    assert len(calls) == 1
    assert calls[0][0] == filename
    assert calls[0][1] & os.O_ACCMODE == os.O_RDONLY


@pytest.mark.parametrize("library", ["BUILTIN", "IO"])
def test_native_descriptor_and_closefd_false_are_preserved(
    tmp_path: Path, library: Literal["BUILTIN", "IO"]
) -> None:
    path = tmp_path / "payload.bin"
    path.write_bytes(b"synthetic")
    descriptor = os.open(path, os.O_RDONLY)
    opener = builtins.open if library == "BUILTIN" else io.open
    try:
        with opener(descriptor, "rb", closefd=False) as stream:
            assert stream.read() == b"synthetic"
        assert os.fstat(descriptor).st_ino == path.stat().st_ino
    finally:
        os.close(descriptor)


@pytest.mark.parametrize("library", ["BUILTIN", "IO"])
def test_native_text_encoding_errors_and_newline_forwarding(
    tmp_path: Path, library: Literal["BUILTIN", "IO"]
) -> None:
    path = tmp_path / "payload.txt"
    path.write_bytes(b"first\r\nsecond\xff")
    opener = builtins.open if library == "BUILTIN" else io.open
    with opener(path, "r", encoding="ascii", errors="replace", newline="") as stream:
        assert stream.read() == "first\r\nsecond\ufffd"
    with pytest.raises(ValueError), opener(path, "rb", encoding="utf-8"):
        pass


@pytest.mark.parametrize("family", [socket.AF_INET, socket.AF_INET6])
def test_real_native_connect_ex_address_families(family: socket.AddressFamily) -> None:
    host = "127.0.0.1" if family == socket.AF_INET else "::1"
    with socket.socket(family, socket.SOCK_STREAM) as listener:
        listener.bind((host, 0))
        listener.listen(1)
        listener.settimeout(1)
        with socket.socket(family, socket.SOCK_STREAM) as client:
            assert client.connect_ex(listener.getsockname()) == 0
            connection, _ = listener.accept()
            connection.close()


def test_real_native_sendto_overloads_preserve_zero_flags() -> None:
    with (
        socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver,
        socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender,
    ):
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(1)
        address = receiver.getsockname()
        assert sender.sendto(b"first", address) == 5
        assert receiver.recvfrom(32)[0] == b"first"
        assert sender.sendto(b"second", 0, address) == 6
        assert receiver.recvfrom(32)[0] == b"second"


def test_real_native_sendmsg_connected_and_explicit_destinations() -> None:
    with (
        socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver,
        socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender,
    ):
        receiver.bind(("127.0.0.1", 0))
        receiver.settimeout(1)
        address = receiver.getsockname()
        assert sender.sendmsg([b"first"], [], 0, address) == 5
        assert receiver.recvfrom(32)[0] == b"first"
        sender.connect(address)
        assert sender.sendmsg([memoryview(b"second")]) == 6
        assert receiver.recvfrom(32)[0] == b"second"


def test_real_native_unix_ancillary_descriptor_transfer(tmp_path: Path) -> None:
    path = tmp_path / "payload.txt"
    path.write_text("synthetic")
    descriptor = os.open(path, os.O_RDONLY)
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
    received = array.array("i")
    right.settimeout(1)
    try:
        rights = array.array("i", [descriptor])
        assert left.sendmsg([b"fd"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)]) == 2
        data, ancillary, flags, _ = right.recvmsg(2, socket.CMSG_SPACE(rights.itemsize))
        assert data == b"fd" and flags & socket.MSG_CTRUNC == 0
        assert len(ancillary) == 1
        level, kind, raw = ancillary[0]
        assert (level, kind) == (socket.SOL_SOCKET, socket.SCM_RIGHTS)
        received.frombytes(raw)
        assert len(received) == 1
        assert os.fstat(received[0]).st_ino == os.fstat(descriptor).st_ino
    finally:
        for duplicate in received:
            os.close(duplicate)
        os.close(descriptor)
        left.close()
        right.close()


def test_declared_byte_hostname_preserves_exact_resolver_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[policy.Host | None, str | bytes | int | None, int, int, int, int]] = []

    def resolve(
        host: policy.Host | None,
        port: str | bytes | int | None,
        family: int = socket.AF_UNSPEC,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[policy.ResolverResult]:
        calls.append((host, port, family, type, proto, flags))
        return [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "db.test.invalid", ("192.0.2.10", 5432))
        ]

    monkeypatch.setattr(policy, "_ALLOWED_DATABASE_HOSTS", {"db.test.invalid"})
    monkeypatch.setattr(policy, "_ALLOWED_RESOLVED_IPS", set())
    monkeypatch.setattr(policy, "_ORIGINAL_GETADDRINFO", resolve)
    actual = socket.getaddrinfo(
        b"db.test.invalid",
        b"5432",
        socket.AF_INET,
        socket.SOCK_STREAM,
        socket.IPPROTO_TCP,
        socket.AI_CANONNAME,
    )
    assert calls == [
        (
            b"db.test.invalid",
            b"5432",
            socket.AF_INET,
            socket.SOCK_STREAM,
            socket.IPPROTO_TCP,
            socket.AI_CANONNAME,
        )
    ]
    assert actual[0][4] == ("192.0.2.10", 5432)
    addresses = policy._ALLOWED_RESOLVED_IPS
    assert addresses == {"192.0.2.10"}
    with socket.socket() as sock:
        policy.check_address(sock, ("192.0.2.10", 5432))
    with pytest.raises(policy.OfflineNetworkError):
        socket.getaddrinfo("unconfigured.test.invalid", 5432)
    assert len(calls) == 1


@pytest.mark.parametrize("library", ["BUILTIN", "IO"])
@pytest.mark.parametrize("variant", ["STR", "BYTES", "MODE"])
def test_native_primitive_spelling_cannot_hide_protected_wire_value(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    library: Literal["BUILTIN", "IO"],
    variant: Literal["STR", "BYTES", "MODE"],
) -> None:
    public = tmp_path / "public.txt"
    protected = tmp_path / "synthetic-credential.txt"
    public.write_text("synthetic public")
    protected.write_text("synthetic protected")
    monkeypatch.setattr(policy, "_PROTECTED_CREDENTIAL_FILES", {protected})
    monkeypatch.setattr(policy, "_PROTECTED_CREDENTIAL_ROOTS", ())

    class MisleadingPath(str):
        def __str__(self) -> str:
            return str(public)

    class MisleadingBytes(bytes):
        def decode(self, encoding: str = "utf-8", errors: str = "strict") -> str:
            return str(public)

    class MisleadingMode(str):
        def __contains__(self, value: object) -> bool:
            return False

    file: policy.FileReference
    mode = "r"
    if variant == "STR":
        file = MisleadingPath(str(protected))
    elif variant == "BYTES":
        file = MisleadingBytes(os.fsencode(protected))
    else:
        file = protected
        mode = MisleadingMode("r")
    opener = builtins.open if library == "BUILTIN" else io.open
    with pytest.raises(policy.OfflineCredentialError), opener(file, mode):
        pass
