"""Small strict stdlib wire contract for one private owned process stand."""

import json
import os
import re
import socket
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, NoReturn

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
type Command = Literal["create", "start", "inspect", "rm"]
MAX_PACKET = 262144


class ProtocolError(ValueError):
    pass


def unique(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for name, value in pairs:
        if name in result:
            raise ProtocolError("Duplicate stand packet field")
        result[name] = value
    return result


def invalid_constant(value: str) -> NoReturn:
    raise ProtocolError("Non-JSON stand number")


def packet(raw: bytes) -> dict[str, JsonValue]:
    if not raw or len(raw) > MAX_PACKET:
        raise ProtocolError("Stand packet size refused")
    value: JsonValue = json.loads(
        raw.decode("utf-8"), object_pairs_hook=unique, parse_constant=invalid_constant
    )
    if not isinstance(value, dict):
        raise ProtocolError("Stand packet requires an object")
    return value


def text(value: JsonValue) -> str:
    if not isinstance(value, str) or "\x00" in value:
        raise ProtocolError("Stand field requires actual text")
    value.encode("utf-8")
    return value


def integer(value: JsonValue) -> int:
    if type(value) is not int:
        raise ProtocolError("Stand field requires an actual integer")
    return value


def command(value: str) -> Command:
    match value:
        case "create":
            return "create"
        case "start":
            return "start"
        case "inspect":
            return "inspect"
        case "rm":
            return "rm"
        case _:
            raise ProtocolError("Unsupported Docker stand command")


@dataclass(frozen=True)
class Endpoint:
    root: Path
    socket_path: Path
    process_id: int
    nonce: str


@dataclass(frozen=True)
class CommandRequest:
    nonce: str
    command: Command
    arguments: tuple[str, ...]


@dataclass(frozen=True)
class ReadyRequest:
    nonce: str


@dataclass(frozen=True)
class ShutdownRequest:
    nonce: str


type Request = CommandRequest | ReadyRequest | ShutdownRequest


@dataclass(frozen=True)
class CommandReply:
    process_id: int
    code: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class ReadyReply:
    process_id: int


@dataclass(frozen=True)
class ChildExit:
    container_id: str
    process_id: int
    returncode: int


@dataclass(frozen=True)
class ShutdownReply:
    process_id: int
    children: tuple[ChildExit, ...]


type Reply = CommandReply | ReadyReply | ShutdownReply


def encode_request(request: Request) -> bytes:
    value: dict[str, JsonValue] = {"nonce": request.nonce}
    if isinstance(request, CommandRequest):
        value.update(kind="COMMAND", command=request.command, arguments=list(request.arguments))
    else:
        value["kind"] = "READY" if isinstance(request, ReadyRequest) else "SHUTDOWN"
    return json.dumps(value).encode()


def decode_request(raw: bytes) -> Request:
    value = packet(raw)
    kind = value.get("kind")
    expected = {"kind", "nonce", "command", "arguments"} if kind == "COMMAND" else {"kind", "nonce"}
    if set(value) != expected:
        raise ProtocolError("Stand request fields refused")
    nonce = text(value["nonce"])
    if re.fullmatch("[0-9a-f]{32}", nonce) is None:
        raise ProtocolError("Stand request nonce refused")
    if kind == "READY":
        return ReadyRequest(nonce)
    if kind == "SHUTDOWN":
        return ShutdownRequest(nonce)
    if kind != "COMMAND" or not isinstance(value["arguments"], list):
        raise ProtocolError("Stand request phase refused")
    return CommandRequest(
        nonce, command(text(value["command"])), tuple(text(arg) for arg in value["arguments"])
    )


def encode_reply(reply: Reply) -> bytes:
    value: dict[str, JsonValue] = {"process_id": reply.process_id}
    if isinstance(reply, CommandReply):
        value.update(kind="COMMAND", code=reply.code, stdout=reply.stdout, stderr=reply.stderr)
    elif isinstance(reply, ReadyReply):
        value["kind"] = "READY"
    else:
        value["kind"] = "DRAINED"
        value["children"] = [
            {
                "container_id": child.container_id,
                "process_id": child.process_id,
                "returncode": child.returncode,
            }
            for child in reply.children
        ]
    return json.dumps(value).encode()


def decode_reply(raw: bytes) -> Reply:
    value = packet(raw)
    if "process_id" not in value:
        raise ProtocolError("Stand server identity is missing")
    process_id = integer(value["process_id"])
    if process_id <= 0:
        raise ProtocolError("Stand server identity refused")
    kind = value.get("kind")
    if kind == "READY" and set(value) == {"kind", "process_id"}:
        return ReadyReply(process_id)
    if kind == "COMMAND" and set(value) == {"kind", "process_id", "code", "stdout", "stderr"}:
        code = integer(value["code"])
        if not 0 <= code <= 255:
            raise ProtocolError("Stand command status refused")
        return CommandReply(process_id, code, text(value["stdout"]), text(value["stderr"]))
    if kind == "DRAINED" and set(value) == {"kind", "process_id", "children"}:
        rows = value["children"]
        if not isinstance(rows, list):
            raise ProtocolError("Stand drain evidence refused")
        children: list[ChildExit] = []
        for row in rows:
            if not isinstance(row, dict) or set(row) != {"container_id", "process_id", "returncode"}:
                raise ProtocolError("Stand child evidence refused")
            child = ChildExit(
                text(row["container_id"]), integer(row["process_id"]), integer(row["returncode"])
            )
            if re.fullmatch("[0-9a-f]{64}", child.container_id) is None or child.process_id <= 0:
                raise ProtocolError("Stand child identity refused")
            children.append(child)
        if len({child.container_id for child in children}) != len(children):
            raise ProtocolError("Stand child identities repeated")
        return ShutdownReply(process_id, tuple(children))
    raise ProtocolError("Stand reply fields refused")


def write_endpoint(endpoint: Endpoint) -> None:
    if endpoint.process_id != os.getpid():
        raise ProtocolError("Only the actual server can publish its endpoint")
    value = {
        "schema_version": "publisher-stand-v1",
        "root": str(endpoint.root),
        "socket_path": str(endpoint.socket_path),
        "process_id": endpoint.process_id,
        "nonce": endpoint.nonce,
    }
    pending = endpoint.root / ("endpoint.pending." + endpoint.nonce)
    descriptor = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(json.dumps(value))
        stream.flush()
        os.fsync(stream.fileno())
    os.link(pending, endpoint.root / "endpoint.json", follow_symlinks=False)
    pending.unlink()


def read_endpoint(root: Path) -> Endpoint:
    descriptor = os.open(root / "endpoint.json", os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise ProtocolError("Stand endpoint ownership refused")
        value = packet(os.read(descriptor, MAX_PACKET + 1))
    finally:
        os.close(descriptor)
    if (
        set(value) != {"schema_version", "root", "socket_path", "process_id", "nonce"}
        or value["schema_version"] != "publisher-stand-v1"
    ):
        raise ProtocolError("Stand endpoint fields refused")
    endpoint = Endpoint(
        Path(text(value["root"])),
        Path(text(value["socket_path"])),
        integer(value["process_id"]),
        text(value["nonce"]),
    )
    if (
        endpoint.root != root
        or endpoint.process_id <= 0
        or re.fullmatch("[0-9a-f]{32}", endpoint.nonce) is None
    ):
        raise ProtocolError("Stand endpoint scope refused")
    parent = endpoint.socket_path.parent
    if (
        not endpoint.socket_path.is_absolute()
        or parent.is_symlink()
        or parent.stat().st_uid != os.geteuid()
        or stat.S_IMODE(parent.stat().st_mode) != 0o700
    ):
        raise ProtocolError("Stand socket directory refused")
    actual = endpoint.socket_path.lstat()
    if (
        not stat.S_ISSOCK(actual.st_mode)
        or actual.st_uid != os.geteuid()
        or stat.S_IMODE(actual.st_mode) != 0o600
    ):
        raise ProtocolError("Stand socket ownership refused")
    return endpoint


def exchange(endpoint: Endpoint, request: Request, *, timeout: float | None = None) -> Reply:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(timeout)
        connection.connect(str(endpoint.socket_path))
        connection.sendall(encode_request(request))
        connection.shutdown(socket.SHUT_WR)
        with connection.makefile("rb") as stream:
            reply = decode_reply(stream.read(MAX_PACKET + 1))
    if reply.process_id != endpoint.process_id:
        raise ProtocolError("Stand response belongs to another process")
    return reply
