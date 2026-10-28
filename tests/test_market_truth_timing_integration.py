import asyncio
import json
import logging
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from tank import clickhouse_market as chm
from tank import logging as telemetry
from tank.analysis.truth import compute_market_truth

pytestmark = pytest.mark.integration


async def test_http_decode_and_real_compute_share_trace_without_payloads(caplog):
    caplog.set_level(logging.INFO, logger="tank.market_timing")
    requests = []
    tasks = set()
    payload = b'{"instrument_id":"test:spot:T","open_time":"2026-10-28T11:59:00Z","c":"2","complete":true}\n'

    async def serve(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            headers = head.decode().split("\r\n")
            length = int(next(line.split(":", 1)[1] for line in headers if line.lower().startswith("content-length:")))
            await reader.readexactly(length)
            requests.append(parse_qs(urlsplit(headers[0].split()[1]).query))
            writer.write(
                b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: "
                + str(len(payload)).encode()
                + b"\r\n\r\n"
                + payload
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        async with httpx.AsyncClient() as http:
            client = chm.ClickHouse(f"http://127.0.0.1:{port}", password="private-password", http=http)
            with telemetry.timing_span("integration", root=True):
                rows = await client.rows("SELECT private_sql", {"private_param": ("private-value", "String")})
                decoded = await chm._decode("mkt.klines_1m", rows)
                result = await asyncio.to_thread(
                    compute_market_truth,
                    {"token_id": "test:T", "cutoff": "2026-10-28T12:00:00Z", "klines": decoded},
                )
    finally:
        server.close()
        if tasks:
            await asyncio.gather(*tasks)
        await server.wait_closed()
    assert result["cutoff"] == "2026-10-28T12:00:00Z"
    assert result["data_confidence"]["class"] == "LOW"
    records = [json.loads(r.getMessage()) for r in caplog.records if r.name == "tank.market_timing"]
    ended = [r for r in records if r["event"] == "stage_end"]
    assert len({r["trace_id"] for r in records}) == 1
    assert {"clickhouse.http", "clickhouse.json_decode", "clickhouse.typed_decode", "compute"} <= {
        r["stage"] for r in ended
    }
    http_span = next(r for r in ended if r["stage"] == "clickhouse.http")
    assert requests[0]["query_id"] == [http_span["query_id"]]
    assert http_span["bytes"] == len(payload)
    assert next(r for r in ended if r["stage"] == "clickhouse.json_decode")["rows"] == 1
    assert "private-" not in "\n".join(r.getMessage() for r in caplog.records if r.name == "tank.market_timing")
