"""Credential-free, offline-by-default pytest boundary. Never imported by Tank."""

import atexit
import builtins
import io
import ipaddress
import os
import re
import socket
import sys
import tempfile
from collections.abc import AsyncIterator, Buffer, Callable, Iterable, Mapping, MutableMapping
from pathlib import Path
from typing import IO, TYPE_CHECKING, overload
from urllib.parse import urlsplit

import pytest
import pytest_asyncio

if TYPE_CHECKING:
    from postgres_helpers import PostgresTemplate

pytest_plugins = (
    "test_market_service",
    "test_storage_typed_market",
    "test_capture_parity",
)

TEST_DATABASE_VARIABLES = frozenset(
    {"TANK_TEST_DATABASE_URL", "TANK_E2E_DATABASE_URL", "TANK_SOURCE_TEST_DATABASE_URL"}
)
TEST_CLICKHOUSE = "CLICKHOUSE_TEST_URL"


def scrub_inherited_environment(environment: MutableMapping[str, str]) -> None:
    """Inspect names only; discard operator configuration without reading values."""
    for name in tuple(environment):
        upper = name.upper()
        if name in TEST_DATABASE_VARIABLES | {TEST_CLICKHOUSE}:
            continue
        if (
            upper in {"NETRC", "DBUS_SESSION_BUS_ADDRESS", "NPM_CONFIG_USERCONFIG", "CURL_HOME"}
            or upper.startswith(
                (
                    "TANK_",
                    "PG",
                    "OPENAI_",
                    "ANTHROPIC_",
                    "CODEX_",
                    "APIFY_",
                    "ACTOR_",
                    "AWS_",
                    "AZURE_",
                    "GOOGLE_",
                    "GCP_",
                    "GIT_",
                    "KRB5",
                    "DOCKER_",
                )
            )
            or re.search(
                r"(?:TOKEN|PASSWORD|PASSFILE|SECRET|CREDENTIAL|API_KEY|ACCESS_KEY|PRIVATE_KEY|AUTH|ASKPASS|SSH_AGENT|KEYCHAIN|KEYLOG|PROXY)",
                upper,
            )
        ):
            del environment[name]


def test_database_hosts(environment: Mapping[str, str]) -> set[str]:
    hosts = set()
    for name in (*TEST_DATABASE_VARIABLES, TEST_CLICKHOUSE):
        value = environment.get(name)
        if not value:
            continue
        if "://" in value:
            host = urlsplit(value).hostname
            if host:
                hosts.add(host.casefold().rstrip("."))
        else:
            from psycopg.conninfo import conninfo_to_dict

            configured = conninfo_to_dict(value).get("host")
            if configured is None:
                continue
            if not isinstance(configured, str):
                raise pytest.UsageError(f"{name} requires a text PostgreSQL host")
            for host in configured.split(","):
                if host and not host.startswith("/"):
                    hosts.add(host.casefold().rstrip("."))
    return hosts


def check_test_databases(environment: Mapping[str, str]) -> None:
    """Tests truncate and drop; they run only on dedicated tank_test_* databases.

    Each workstream uses its own database, e.g. tank_test_<session>, created on the
    shared cluster; the production databases tank, oma and tank_replay are refused.
    """
    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    for name in sorted(TEST_DATABASE_VARIABLES):
        value = environment.get(name)
        if not value:
            continue
        try:
            database = conninfo_to_dict(value).get("dbname")
        except psycopg.Error:
            raise pytest.UsageError(f"{name} is not a valid PostgreSQL URL") from None
        if not isinstance(database, str) or not database.startswith("tank_test"):
            raise pytest.UsageError(f"{name} must name a dedicated tank_test_* database, not {database!r}")


check_test_databases(os.environ)
scrub_inherited_environment(os.environ)


_operator_home = Path.home()
_OFFLINE_HOME = Path(tempfile.mkdtemp(prefix="tank-pytest-offline-home-"))
os.environ.update(
    HOME=str(_OFFLINE_HOME),
    CODEX_HOME=str(_OFFLINE_HOME / ".codex"),
    XDG_CONFIG_HOME=str(_OFFLINE_HOME / "config"),
    XDG_CACHE_HOME=str(_OFFLINE_HOME / "cache"),
    XDG_DATA_HOME=str(_OFFLINE_HOME / "data"),
    GIT_CONFIG_GLOBAL=os.devnull,
    GIT_CONFIG_NOSYSTEM="1",
)


def _cleanup_home() -> None:
    import shutil

    shutil.rmtree(_OFFLINE_HOME, ignore_errors=True)


atexit.register(_cleanup_home)
_ALLOWED_DATABASE_HOSTS = test_database_hosts(os.environ)
_ALLOWED_RESOLVED_IPS: set[str] = set()
_ORIGINAL_CONNECT = socket.socket.connect
_ORIGINAL_CONNECT_EX = socket.socket.connect_ex
_ORIGINAL_GETADDRINFO = socket.getaddrinfo
_ORIGINAL_SENDTO = socket.socket.sendto
_ORIGINAL_SENDMSG = socket.socket.sendmsg if hasattr(socket.socket, "sendmsg") else None


class OfflineNetworkError(RuntimeError):
    pass


class OfflineCredentialError(RuntimeError):
    pass


_ORIGINAL_OPEN = builtins.open
_ORIGINAL_IO_OPEN = io.open
_PROTECTED_CREDENTIAL_ROOTS = (
    Path("/run/secrets"),
    Path("/codex-auth"),
    _operator_home / ".aws",
    _operator_home / ".azure",
    _operator_home / ".ssh",
)
_PROTECTED_CREDENTIAL_FILES = {
    _operator_home / ".codex" / "auth.json",
    _operator_home / ".netrc",
    _operator_home / ".pgpass",
    _operator_home / ".config" / "gcloud" / "application_default_credentials.json",
    Path.cwd() / ".env",
}


type FileReference = int | str | bytes | os.PathLike[str] | os.PathLike[bytes]
type NativeFile = int | str | bytes
type FileOpener = Callable[[str | bytes, int], int]
type NativeStream = IO[str] | IO[bytes]
type Host = str | bytes
type IPv4Address = tuple[Host, int]
type IPv6FlowAddress = tuple[Host, int, int]
type IPv6Address = tuple[Host, int, int, int]
type SocketAddress = str | Buffer | IPv4Address | IPv6FlowAddress | IPv6Address
type ResolverAddress = tuple[str, int] | tuple[str, int, int, int] | tuple[int, bytes]
type ResolverResult = tuple[socket.AddressFamily, socket.SocketKind, int, str, ResolverAddress]
type AncillaryData = tuple[int, int, Buffer]


def _native_file(file: FileReference) -> NativeFile:
    if isinstance(file, int):
        return file
    value = os.fspath(file)
    return str.__str__(value) if isinstance(value, str) else bytes.__bytes__(value)


def _check_credential_file(file: FileReference, mode: str) -> None:
    if isinstance(file, int):
        return
    if "r" not in mode and "+" not in mode:
        return
    path = Path(os.fsdecode(file)).resolve(strict=False)
    if any(path == protected.resolve(strict=False) for protected in _PROTECTED_CREDENTIAL_FILES):
        raise OfflineCredentialError("pytest offline policy blocked an operator credential path")
    parents = frozenset(path.parents)
    for root in _PROTECTED_CREDENTIAL_ROOTS:
        resolved_root = root.resolve(strict=False)
        if path == resolved_root or resolved_root in parents:
            raise OfflineCredentialError("pytest offline policy blocked an operator credential path")


def _open(
    file: FileReference,
    mode: str = "r",
    buffering: int = -1,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    closefd: bool = True,
    opener: FileOpener | None = None,
) -> NativeStream:
    admitted = _native_file(file)
    mode = str.__str__(mode)
    _check_credential_file(admitted, mode)
    return _ORIGINAL_OPEN(admitted, mode, buffering, encoding, errors, newline, closefd, opener)


def _io_open(
    file: FileReference,
    mode: str = "r",
    buffering: int = -1,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    closefd: bool = True,
    opener: FileOpener | None = None,
) -> NativeStream:
    admitted = _native_file(file)
    mode = str.__str__(mode)
    _check_credential_file(admitted, mode)
    return _ORIGINAL_IO_OPEN(admitted, mode, buffering, encoding, errors, newline, closefd, opener)


def _host_text(host: Host) -> str:
    return bytes.decode(host, "ascii") if isinstance(host, bytes) else str.__str__(host)


def permitted_host(host: Host) -> bool:
    normalized = _host_text(host).casefold().rstrip(".")
    if normalized in {"localhost", ""} | _ALLOWED_DATABASE_HOSTS | _ALLOWED_RESOLVED_IPS:
        return True
    try:
        return ipaddress.ip_address(normalized.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def check_address(sock: socket.socket, address: SocketAddress) -> None:
    if sock.family == socket.AF_UNIX:
        return
    if sock.family in (socket.AF_INET, socket.AF_INET6) and isinstance(address, tuple):
        host = tuple.__getitem__(address, 0)
        if isinstance(host, (str, bytes)) and permitted_host(host):
            return
    raise OfflineNetworkError("pytest offline policy blocked a non-allowlisted socket destination")


def _connect(sock: socket.socket, address: SocketAddress) -> None:
    check_address(sock, address)
    _ORIGINAL_CONNECT(sock, address)


def _connect_ex(sock: socket.socket, address: SocketAddress) -> int:
    check_address(sock, address)
    return _ORIGINAL_CONNECT_EX(sock, address)


@overload
def _sendto(sock: socket.socket, data: Buffer, address: SocketAddress, /) -> int: ...


@overload
def _sendto(sock: socket.socket, data: Buffer, flags: int, address: SocketAddress, /) -> int: ...


def _sendto(sock: socket.socket, data: Buffer, *args: SocketAddress | int) -> int:
    if not args:
        raise TypeError("sendto requires a destination")
    destination = args[-1]
    if isinstance(destination, int):
        raise TypeError("sendto requires a socket address")
    check_address(sock, destination)
    if len(args) == 1:
        return _ORIGINAL_SENDTO(sock, data, destination)
    if len(args) == 2 and isinstance(args[0], int):
        return _ORIGINAL_SENDTO(sock, data, args[0], destination)
    raise TypeError("sendto requires address or flags and address")


def _sendmsg(
    sock: socket.socket,
    buffers: Iterable[Buffer],
    ancdata: Iterable[AncillaryData] = (),
    flags: int = 0,
    address: SocketAddress | None = None,
) -> int:
    original = _ORIGINAL_SENDMSG
    if original is None:
        raise OfflineNetworkError("native sendmsg is unavailable")
    if address is not None:
        check_address(sock, address)
    elif sock.family in (socket.AF_INET, socket.AF_INET6):
        check_address(sock, sock.getpeername())
    return original(sock, buffers, ancdata, flags, address)


def _getaddrinfo(
    host: Host | None,
    port: str | bytes | int | None,
    family: int = socket.AF_UNSPEC,
    type: int = 0,
    proto: int = 0,
    flags: int = 0,
) -> list[ResolverResult]:
    if host is not None and not permitted_host(host):
        raise OfflineNetworkError("pytest offline policy blocked external DNS resolution")
    results = _ORIGINAL_GETADDRINFO(host, port, family, type, proto, flags)
    if host is not None and _host_text(host).casefold().rstrip(".") in _ALLOWED_DATABASE_HOSTS:
        for result in results:
            address = result[4][0]
            if isinstance(address, str):
                _ALLOWED_RESOLVED_IPS.add(address.split("%", 1)[0])
    return results


_boundary_patch = pytest.MonkeyPatch()
_boundary_patch.setattr(builtins, "open", _open)
_boundary_patch.setattr(io, "open", _io_open)
_boundary_patch.setattr(socket.socket, "connect", _connect)
_boundary_patch.setattr(socket.socket, "connect_ex", _connect_ex)
_boundary_patch.setattr(socket, "getaddrinfo", _getaddrinfo)
_boundary_patch.setattr(socket.socket, "sendto", _sendto)
if _ORIGINAL_SENDMSG is not None:
    _boundary_patch.setattr(socket.socket, "sendmsg", _sendmsg)


@pytest.fixture(autouse=True)
def offline_credentials(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Synthetic default locations; explicit constructor/fake env values still win."""
    from pydantic_settings.sources import DotEnvSettingsSource

    synthetic_home = tmp_path / "offline-home"
    synthetic_home.mkdir()
    monkeypatch.setenv("HOME", str(synthetic_home))
    monkeypatch.setenv("CODEX_HOME", str(synthetic_home / ".codex"))
    monkeypatch.setattr(sys.modules[__name__], "_OFFLINE_HOME", synthetic_home)

    monkeypatch.setattr(DotEnvSettingsSource, "_read_env_files", lambda self: {})
    docker_endpoint = os.environ.get("OPENMA_SQL_TEST_DOCKER_HOST")
    if docker_endpoint:
        if not docker_endpoint.startswith("unix://"):
            raise RuntimeError("offline Docker SQL probes require an explicit local unix endpoint")
        monkeypatch.setenv("DOCKER_HOST", docker_endpoint)
        monkeypatch.setenv("DOCKER_CONFIG", str(tmp_path / "FAKE-empty-docker-config"))


@pytest.fixture(autouse=True)
def _fresh_quote_usd_history(monkeypatch: pytest.MonkeyPatch) -> None:
    """The stablecoin USD samples are process state; no test inherits another's."""
    from tank.market import normalize

    monkeypatch.setattr(normalize, "QUOTE_USD_HISTORY", {})


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--postgres-schema-template",
        metavar="tank_test_NAME",
        help="Clone each migrated test database from this externally prepared, sealed schema seed",
    )


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def postgres_template(request: pytest.FixtureRequest) -> AsyncIterator["PostgresTemplate"]:
    """Create databases lazily; memory-only runs never inspect database settings."""
    from postgres_helpers import PostgresTemplate

    schema_template = request.config.getoption("postgres_schema_template")
    if schema_template is not None and not isinstance(schema_template, str):
        raise pytest.UsageError("postgres schema template must be a database name")
    owner = PostgresTemplate(schema_template=schema_template)
    try:
        yield owner
    finally:
        await owner.close()
