"""Lifetime-only synthetic factories; no financial SQL success is represented."""

import os
from contextlib import asynccontextmanager

import pytest
from truth_native_test_safety import NATIVE_URL_REFUSAL, owned_native_repository, require_loopback_native_url


class LifetimeTemplate:
    def __init__(self, calls):
        self.calls = calls

    def repository(self, dsn):
        self.calls.append(("repository_factory", dsn))
        client = LifetimeClient(os.environ["CLICKHOUSE_TEST_URL"], self.calls)

        @asynccontextmanager
        async def lifetime():
            client.execute_ddl()
            try:
                yield client
            finally:
                self.calls.append(("close",))

        return lifetime()


class LifetimeClient:
    def __init__(self, url, calls):
        self.url = url
        self.calls = calls
        calls.append(("client_factory", url))

    def execute_ddl(self):
        self.calls.append(("ddl",))


@pytest.mark.parametrize("url", [
    None, "", " ",
    "http://remote.example:8123", "https://192.0.2.10:8443", "http://[2001:db8::1]:8123",
    "ftp://localhost:8123", "localhost:8123", "//localhost:8123", "http:///localhost:8123",
    "http://localhost.remote.example:8123", "http://127.0.0.1.remote.example:8123",
    "http://localhost@remote.example:8123", "http://127.0.0.1@192.0.2.10:8123",
    "http://localhost.:8123", "http://127.1:8123", "http://2130706433:8123",
    "http://[::ffff:127.0.0.1]:8123", "http://%6cocalhost:8123",
    "http://localhost:invalid", "http://localhost:", "http://localhost:0", "http://localhost:65536",
    "http://[::1", "http://[::1]remote.example:8123", "http://::1:8123",
    "http://:secret@localhost:8123", "http://user@@localhost:8123", "http://user:%zz@localhost:8123",
    "http://user[bad]:secret@localhost:8123", "http://localhost\\@remote.example:8123",
    "http://local\nhost:8123", " http://localhost:8123", "http://localhost:8123/#remote",
])
async def test_refused_native_endpoint_precedes_all_lifetime_factories(monkeypatch, url):
    if url is None:
        monkeypatch.delenv("CLICKHOUSE_TEST_URL", raising=False)
    else:
        monkeypatch.setenv("CLICKHOUSE_TEST_URL", url)
    calls = []
    template = LifetimeTemplate(calls)
    with pytest.raises(ValueError) as failure:
        async with owned_native_repository(template, "unchanged-dedicated-test-dsn"):
            pytest.fail("refused endpoint entered repository")
    assert str(failure.value) == NATIVE_URL_REFUSAL
    assert failure.value.__cause__ is None
    assert calls == []


@pytest.mark.parametrize("url", [
    "http://localhost:8123", "https://127.0.0.1:8443", "http://[::1]:8123",
    "http://localhost", "https://LOCALHOST:8443",
    "http://synthetic-user:synthetic%40password@localhost:8123/original?key=unchanged",
    "https://synthetic-user:synthetic%3Apassword@[::1]:8443/original",
])
async def test_loopback_native_endpoint_preserves_original_url_and_lifetime(monkeypatch, url):
    monkeypatch.setenv("CLICKHOUSE_TEST_URL", url)
    assert require_loopback_native_url() == url
    calls = []
    async with owned_native_repository(LifetimeTemplate(calls), "unchanged-dedicated-test-dsn") as client:
        assert client.url == url
        assert calls == [
            ("repository_factory", "unchanged-dedicated-test-dsn"),
            ("client_factory", url), ("ddl",),
        ]
    assert calls == [
        ("repository_factory", "unchanged-dedicated-test-dsn"),
        ("client_factory", url), ("ddl",), ("close",),
    ]
