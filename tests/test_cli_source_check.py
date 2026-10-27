"""Actual source checks reach the CLI's stable administrative JSON boundary."""

import asyncio
import os
import ssl
from datetime import UTC, datetime
from ipaddress import IPv4Address
from pathlib import Path

import httpx
import pytest
from click.testing import CliRunner
from configuration_support import captured_fixture_settings
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from postgres_helpers import PostgresTemplate
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool
from pydantic import JsonValue
from source_support import source_configuration

import tank.cli as cli
import tank.sources as sources
from tank.contracts.json import decode_unique_json
from tank.contracts.source_checks import SavedSourceHealth, SourceCheckResult, SourceProbeSuccess
from tank.market_codec import StoredRow
from tank.settings import Settings
from tank.sources import SourceService

type CliSourcePool = AsyncConnectionPool[AsyncConnection[StoredRow]]


@pytest.mark.integration
@pytest.mark.postgres
@pytest.mark.parametrize("probe", [False, True])
async def test_actual_source_check_cli_preserves_flat_wire_and_drains_owned_resources(
    postgres_template: PostgresTemplate,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    probe: bool,
) -> None:
    private_key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Authored CLI loopback")])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime(2020, 1, 1, tzinfo=UTC))
        .not_valid_after(datetime(2040, 1, 1, tzinfo=UTC))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(IPv4Address("127.0.0.1"))]), critical=False
        )
        .sign(private_key, hashes.SHA256())
    )
    certificate_path = tmp_path / "loopback.pem"
    key_path = tmp_path / "loopback-key.pem"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        private_key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )
    )
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(certificate_path, key_path)

    def trusted_context() -> ssl.SSLContext:
        return ssl.create_default_context(cafile=certificate_path)

    monkeypatch.setattr(sources, "tls_context", trusted_context)
    payload = b'<rss version="2.0"><channel><title>Authored empty feed</title></channel></rss>'
    requests: list[bytes] = []

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            requests.append(request.split(b"\r\n", 1)[0])
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/rss+xml\r\nContent-Length: "
                + str(len(payload)).encode()
                + b"\r\nConnection: close\r\n\r\n"
                + payload
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    async with await asyncio.start_server(serve, "127.0.0.1", 0, ssl=server_context) as server:
        assert server.sockets
        address = server.sockets[0].getsockname()
        assert isinstance(address, tuple)
        port = address[1]
        assert isinstance(port, int) and not isinstance(port, bool) and port > 0
        url = f"https://127.0.0.1:{port}/feed"
        async with postgres_template.repository(os.environ["TANK_TEST_DATABASE_URL"]) as fixture_repo:
            checkpoint: dict[str, JsonValue] = {
                "source_id": "S30",
                "status": "HEALTHY",
                "retained_extension": {"known_zero": 0, "missing": None},
            }
            await fixture_repo.put("ops.source_checkpoints", "S30:0", checkpoint)
            settings = captured_fixture_settings(
                Settings(
                    _env_file=None,
                    database_url=fixture_repo.database_url,
                    clickhouse_url=None,
                    data_dir=tmp_path,
                    ops_progress_enabled=False,
                ),
                {
                    "sources": source_configuration(
                        {
                            "sources": [
                                {
                                    "source_id": "S30",
                                    "name": "Authored CLI source",
                                    "feeds": [{"url": url, "kind": "rss"}],
                                }
                            ]
                        }
                    ),
                    "registry": {"x_accounts": []},
                },
                tmp_path / "captured-source-config",
            )
            pools: list[CliSourcePool] = []
            clients: list[httpx.AsyncClient] = []
            actual_results: list[SourceCheckResult] = []
            invocations: list[tuple[bool, bool]] = []
            real_wait = AsyncConnectionPool.wait
            real_check = SourceService.check
            real_close = SourceService.close

            def configured() -> Settings:
                return settings

            async def waited(pool: CliSourcePool) -> None:
                await real_wait(pool)
                pools.append(pool)

            async def checked(
                service: SourceService, *, probe: bool = False, check_accounts: bool = False
            ) -> list[SourceCheckResult]:
                invocations.append((probe, check_accounts))
                results = await real_check(service, probe=probe, check_accounts=check_accounts)
                actual_results.extend(results)
                return results

            async def closed(service: SourceService) -> None:
                assert service.owns_http is True
                client = service.http
                assert isinstance(client, httpx.AsyncClient)
                await real_close(service)
                clients.append(client)

            monkeypatch.setattr(cli, "Settings", configured)
            monkeypatch.setattr(AsyncConnectionPool, "wait", waited)
            monkeypatch.setattr(SourceService, "check", checked)
            monkeypatch.setattr(SourceService, "close", closed)
            arguments = ["sources", "check", "--probe"] if probe else ["sources", "check"]
            result = await asyncio.to_thread(CliRunner().invoke, cli.main, arguments)
            assert result.exit_code == 0
            assert invocations == [(probe, False)]
            assert len(pools) == len(clients) == len(actual_results) == 1
            assert pools[0].closed and clients[0].is_closed
            records = decode_unique_json(result.stdout)
            assert isinstance(records, list) and len(records) == 1
            record = records[0]
            assert isinstance(record, dict)
            assert record["source_id"] == "S30"
            if probe:
                assert isinstance(actual_results[0], SourceProbeSuccess)
                assert record["status"] == "PASS_EMPTY" and record["item_count"] == 0
                assert record["url"] == url and record["http_status"] == 200
                assert record["response_bytes"] == len(payload)
                assert record["config_version"] == settings.require_versions().config_version
                assert requests and all(request.startswith(b"GET /feed ") for request in requests)
                assert "context" not in record and "response" not in record
            else:
                assert isinstance(actual_results[0], SavedSourceHealth)
                assert records == [
                    {"source_id": "S30", "configured": True, "states": [checkpoint], "status": "HEALTHY"}
                ]
                assert requests == []
            assert await fixture_repo.get("ops.source_checkpoints", "S30:0") == checkpoint
            assert await fixture_repo.list("ingest.source_items") == []
            assert await fixture_repo.list("ops.jobs") == []
