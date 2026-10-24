"""Typed standard-library worker for the qualified Python 3.12 native diagnostic guest."""

import json
import os
import socket
import ssl
import struct
import subprocess
import time
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

type JsonValue = str | bool | int | None | list[JsonValue] | dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class DnsReply:
    rcode: int
    answers: int

    def __post_init__(self) -> None:
        if type(self.rcode) is not int or self.rcode != 0:
            raise ValueError("DNS reply requires a successful response code")
        if type(self.answers) is not int or not 1 <= self.answers <= 65535:
            raise ValueError("DNS reply requires at least one answer")


@dataclass(frozen=True, slots=True)
class TlsReply:
    peer: str
    version: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.peer, str)
            or not self.peer
            or not isinstance(self.version, str)
            or not self.version
        ):
            raise ValueError("TLS reply requires peer and negotiated version")


type ProbeValue = str | bool | int | list[str] | DnsReply | TlsReply


def value_json(value: ProbeValue) -> JsonValue:
    if isinstance(value, DnsReply):
        return {"rcode": value.rcode, "answers": value.answers}
    if isinstance(value, TlsReply):
        return {"peer": value.peer, "version": value.version}
    if isinstance(value, list):
        if any(not isinstance(item, str) for item in value):
            raise TypeError("Probe lists must contain only strings")
        return list(value)
    if type(value) in (str, bool, int):
        return value
    raise TypeError("Unsupported probe value")


@dataclass(frozen=True, slots=True)
class ProbeSuccess:
    value: ProbeValue
    ms: int

    def __post_init__(self) -> None:
        value_json(self.value)
        if type(self.ms) is not int or self.ms < 0:
            raise ValueError("Probe elapsed milliseconds must be a nonnegative integer")

    def json(self) -> dict[str, JsonValue]:
        return {"ok": True, "value": value_json(self.value), "ms": self.ms}


@dataclass(frozen=True, slots=True)
class ProbeFailure:
    error: str
    ms: int

    def __post_init__(self) -> None:
        if not isinstance(self.error, str) or not self.error or len(self.error) > 240:
            raise ValueError("Probe error requires bounded nonempty text")
        if type(self.ms) is not int or self.ms < 0:
            raise ValueError("Probe elapsed milliseconds must be a nonnegative integer")

    def json(self) -> dict[str, JsonValue]:
        return {"ok": False, "error": self.error, "ms": self.ms}


type ProbeAttempt = ProbeSuccess | ProbeFailure


def attempt_json(result: ProbeAttempt) -> dict[str, JsonValue]:
    if not isinstance(result, ProbeSuccess | ProbeFailure):
        raise TypeError("Probe report fields require success or failure records")
    return result.json()


def attempt(operation: Callable[[], ProbeValue]) -> ProbeAttempt:
    started = time.monotonic()
    try:
        value = operation()
        return ProbeSuccess(value, round((time.monotonic() - started) * 1000))
    except Exception as error:
        return ProbeFailure(
            (type(error).__name__ + ": " + str(error))[:240], round((time.monotonic() - started) * 1000)
        )


def dns_reply(data: bytes) -> DnsReply:
    if len(data) < 12:
        raise ValueError("DNS response header is truncated")
    rcode = data[3] & 15
    answers = struct.unpack(">H", data[6:8])[0]
    if rcode != 0 or answers == 0:
        raise RuntimeError(f"rcode={rcode} answers={answers}")
    return DnsReply(rcode, answers)


def udp_dns(server: str, name: str) -> DnsReply:
    query = (
        struct.pack(">HHHHHH", 0x5441, 0x0100, 1, 0, 0, 0)
        + b"".join(bytes([len(part)]) + part.encode() for part in name.split("."))
        + b"\0"
        + struct.pack(">HH", 1, 1)
    )
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as connection:
        connection.settimeout(4)
        connection.sendto(query, (server, 53))
        data, _ = connection.recvfrom(1500)
    return dns_reply(data)


def peer_address(address: tuple[str, int] | tuple[str, int, int, int]) -> str:
    host = address[0]
    if not isinstance(host, str) or not host:
        raise RuntimeError("TCP connection has no peer address")
    return host


def tls(host: str, port: int, sni: str) -> TlsReply:
    with (
        socket.create_connection((host, port), timeout=6) as raw,
        ssl.create_default_context().wrap_socket(raw, server_hostname=sni) as connection,
    ):
        version = connection.version()
        if version is None:
            raise RuntimeError("TLS connection has no negotiated version")
        return TlsReply(peer_address(connection.getpeername()), version)


def https(target: str) -> int:
    with urllib.request.urlopen(target, timeout=12) as response:
        status = response.status
    if type(status) is not int or not 100 <= status <= 599:
        raise RuntimeError("HTTP response has no valid status")
    return status


def psql(dburl: str, query: str) -> str:
    process = subprocess.run(
        ["psql", dburl, "-v", "ON_ERROR_STOP=1", "-Atc", query],
        capture_output=True,
        text=True,
        timeout=25,
        check=False,
    )
    if process.returncode:
        error = process.stderr.strip()
        raise RuntimeError(error.splitlines()[-1] if error else f"exit {process.returncode}")
    return process.stdout.strip()


def nameservers(path: Path) -> list[str]:
    result: list[str] = []
    for line in path.read_text().splitlines():
        if line.startswith("nameserver"):
            fields = line.split()
            if len(fields) < 2:
                raise ValueError("Configured nameserver has no address")
            result.append(fields[1])
    return result


def configured_dns(servers: list[str]) -> DnsReply:
    if not servers:
        raise RuntimeError("No configured nameserver")
    return udp_dns(servers[0], "example.com")


def resolve_host(host: str) -> list[str]:
    addresses: set[str] = set()
    for result in socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP):
        address = result[4]
        if not isinstance(address, tuple) or not isinstance(address[0], str):
            raise RuntimeError("DNS resolution returned an invalid socket address")
        addresses.add(address[0])
    return sorted(addresses)


def database_peer(host: str) -> str:
    with socket.create_connection((host, 5432), timeout=6) as connection:
        return peer_address(connection.getpeername())


@dataclass(frozen=True, slots=True)
class ProbeReport:
    machine: ProbeAttempt
    kernel: ProbeAttempt
    hypervisor_cpu_flag: ProbeAttempt
    nameservers: list[str]
    dns_example: ProbeAttempt
    dns_openrouter: ProbeAttempt
    dns_etherscan: ProbeAttempt
    dns_postgres: ProbeAttempt
    configured_dns: ProbeAttempt
    public_dns: ProbeAttempt
    tls_cloudflare: ProbeAttempt
    tls_google: ProbeAttempt
    https_example: ProbeAttempt
    https_openrouter: ProbeAttempt
    db_tcp: ProbeAttempt
    db_select: ProbeAttempt
    db_write_refused: ProbeAttempt
    db_cross_database: ProbeAttempt

    def json(self) -> dict[str, JsonValue]:
        if not isinstance(self.nameservers, list) or any(
            not isinstance(server, str) for server in self.nameservers
        ):
            raise TypeError("Configured nameservers must be strings")
        return {
            "machine": attempt_json(self.machine),
            "kernel": attempt_json(self.kernel),
            "hypervisor_cpu_flag": attempt_json(self.hypervisor_cpu_flag),
            "nameservers": list(self.nameservers),
            "dns_getaddrinfo_example.com": attempt_json(self.dns_example),
            "dns_getaddrinfo_openrouter.ai": attempt_json(self.dns_openrouter),
            "dns_getaddrinfo_api.etherscan.io": attempt_json(self.dns_etherscan),
            "dns_getaddrinfo_postgres": attempt_json(self.dns_postgres),
            "dns_udp_configured": attempt_json(self.configured_dns),
            "dns_udp_public_1.1.1.1": attempt_json(self.public_dns),
            "tls_direct_ip_1.1.1.1": attempt_json(self.tls_cloudflare),
            "tls_direct_ip_8.8.8.8": attempt_json(self.tls_google),
            "https_example.com": attempt_json(self.https_example),
            "https_openrouter_models": attempt_json(self.https_openrouter),
            "db_tcp": attempt_json(self.db_tcp),
            "db_select": attempt_json(self.db_select),
            "db_write_refused": attempt_json(self.db_write_refused),
            "db_cross_database": attempt_json(self.db_cross_database),
        }


def collect() -> ProbeReport:
    servers = nameservers(Path("/etc/resolv.conf"))
    return ProbeReport(
        machine=attempt(lambda: os.uname().machine),
        kernel=attempt(lambda: os.uname().release),
        hypervisor_cpu_flag=attempt(lambda: "hypervisor" in Path("/proc/cpuinfo").read_text()),
        nameservers=servers,
        dns_example=attempt(lambda: resolve_host("example.com")),
        dns_openrouter=attempt(lambda: resolve_host("openrouter.ai")),
        dns_etherscan=attempt(lambda: resolve_host("api.etherscan.io")),
        dns_postgres=attempt(lambda: resolve_host("postgres")),
        configured_dns=attempt(lambda: configured_dns(servers)),
        public_dns=attempt(lambda: udp_dns("1.1.1.1", "example.com")),
        tls_cloudflare=attempt(lambda: tls("1.1.1.1", 443, "one.one.one.one")),
        tls_google=attempt(lambda: tls("8.8.8.8", 443, "dns.google")),
        https_example=attempt(lambda: https("https://example.com/")),
        https_openrouter=attempt(lambda: https("https://openrouter.ai/api/v1/models")),
        db_tcp=attempt(lambda: database_peer(os.environ["TANK_NATIVE_DB_IPV4"])),
        db_select=attempt(
            lambda: psql(os.environ["TANK_DB_URL"], "select current_user || ' ' || current_database()")
        ),
        db_write_refused=attempt(
            lambda: psql(os.environ["TANK_DB_URL"], "create table tank_native_probe(x int)")
        ),
        db_cross_database=attempt(lambda: psql(os.environ["TANK_CROSS_DB_URL"], "select current_database()")),
    )


def main() -> None:
    print("TANKPROBE" + json.dumps(collect().json(), allow_nan=False))


if __name__ == "__main__":
    main()
