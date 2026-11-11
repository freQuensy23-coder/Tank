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
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import pytest_asyncio

pytest_plugins = (
    "test_market_service",
    "test_storage_typed_market",
    "test_capture_parity",
)

TEST_DATABASE_VARIABLES = frozenset(
    {"TANK_TEST_DATABASE_URL", "TANK_E2E_DATABASE_URL", "TANK_SOURCE_TEST_DATABASE_URL"}
)
TEST_CLICKHOUSE = "CLICKHOUSE_TEST_URL"


def scrub_inherited_environment(environment):
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


def test_database_hosts(environment):
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

            for host in conninfo_to_dict(value).get("host", "").split(","):
                if host and not host.startswith("/"):
                    hosts.add(host.casefold().rstrip("."))
    return hosts


def check_test_databases(environment):
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
            database = conninfo_to_dict(value).get("dbname") or ""
        except psycopg.Error:
            raise pytest.UsageError(f"{name} is not a valid PostgreSQL URL") from None
        if not database.startswith("tank_test"):
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


def _cleanup_home():
    import shutil

    shutil.rmtree(_OFFLINE_HOME, ignore_errors=True)


atexit.register(_cleanup_home)
_ALLOWED_DATABASE_HOSTS = test_database_hosts(os.environ)
_ALLOWED_RESOLVED_IPS = set()
_ORIGINAL_CONNECT = socket.socket.connect
_ORIGINAL_CONNECT_EX = socket.socket.connect_ex
_ORIGINAL_GETADDRINFO = socket.getaddrinfo
_ORIGINAL_SENDTO = socket.socket.sendto
_ORIGINAL_SENDMSG = getattr(socket.socket, "sendmsg", None)


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


def _check_credential_file(file, mode):
    if isinstance(file, int) or not isinstance(file, (str, bytes, os.PathLike)):
        return
    if "r" not in mode and "+" not in mode:
        return
    path = Path(os.fsdecode(file)).resolve(strict=False)
    if path in {p.resolve(strict=False) for p in _PROTECTED_CREDENTIAL_FILES} or any(
        path == root.resolve(strict=False) or root.resolve(strict=False) in path.parents
        for root in _PROTECTED_CREDENTIAL_ROOTS
    ):
        raise OfflineCredentialError("pytest offline policy blocked an operator credential path")


def _open(file, mode="r", *args, **kwargs):
    _check_credential_file(file, mode)
    return _ORIGINAL_OPEN(file, mode, *args, **kwargs)


def _io_open(file, mode="r", *args, **kwargs):
    _check_credential_file(file, mode)
    return _ORIGINAL_IO_OPEN(file, mode, *args, **kwargs)


builtins.open = _open
io.open = _io_open


def permitted_host(host):
    if isinstance(host, bytes):
        host = host.decode("ascii")
    normalized = str(host).casefold().rstrip(".")
    if normalized in {"localhost", ""} | _ALLOWED_DATABASE_HOSTS | _ALLOWED_RESOLVED_IPS:
        return True
    try:
        return ipaddress.ip_address(normalized.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def check_address(sock, address):
    if sock.family == socket.AF_UNIX:
        return
    if (
        sock.family in (socket.AF_INET, socket.AF_INET6)
        and isinstance(address, tuple)
        and permitted_host(address[0])
    ):
        return
    raise OfflineNetworkError("pytest offline policy blocked a non-allowlisted socket destination")


def _connect(sock, address):
    check_address(sock, address)
    return _ORIGINAL_CONNECT(sock, address)


def _connect_ex(sock, address):
    check_address(sock, address)
    return _ORIGINAL_CONNECT_EX(sock, address)


def _sendto(sock, data, *args):
    if not args:
        raise TypeError("sendto requires a destination")
    check_address(sock, args[-1])
    return _ORIGINAL_SENDTO(sock, data, *args)


def _sendmsg(sock, buffers, ancdata=(), flags=0, address=None):
    if address is not None:
        check_address(sock, address)
    elif sock.family in (socket.AF_INET, socket.AF_INET6):
        check_address(sock, sock.getpeername())
    return _ORIGINAL_SENDMSG(sock, buffers, ancdata, flags, address)


def _getaddrinfo(host, *args, **kwargs):
    if host is not None and not permitted_host(host):
        raise OfflineNetworkError("pytest offline policy blocked external DNS resolution")
    results = _ORIGINAL_GETADDRINFO(host, *args, **kwargs)
    if host is not None and str(host).casefold().rstrip(".") in _ALLOWED_DATABASE_HOSTS:
        _ALLOWED_RESOLVED_IPS.update(result[4][0].split("%", 1)[0] for result in results)
    return results


socket.socket.connect = _connect
socket.socket.connect_ex = _connect_ex
socket.getaddrinfo = _getaddrinfo
socket.socket.sendto = _sendto
if _ORIGINAL_SENDMSG is not None:
    socket.socket.sendmsg = _sendmsg


@pytest.fixture(autouse=True)
def offline_credentials(monkeypatch, tmp_path):
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
def _fresh_quote_usd_history(monkeypatch):
    """The stablecoin USD samples are process state; no test inherits another's."""
    from tank.market import normalize

    monkeypatch.setattr(normalize, "QUOTE_USD_HISTORY", {})


def pytest_addoption(parser):
    parser.addoption(
        "--postgres-schema-template",
        metavar="tank_test_NAME",
        help="Clone each migrated test database from this externally prepared, sealed schema seed",
    )


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def postgres_template(request):
    """Create databases lazily; memory-only runs never inspect database settings."""
    from postgres_helpers import PostgresTemplate

    owner = PostgresTemplate(schema_template=request.config.getoption("postgres_schema_template"))
    try:
        yield owner
    finally:
        await owner.close()


@pytest_asyncio.fixture
async def market_clickhouse():
    """A ClickHouse database of its own with the mkt tables (CLICKHOUSE_TEST_URL), dropped afterwards."""
    import uuid

    from tank import clickhouse_market

    url = os.environ.get(TEST_CLICKHOUSE)
    if not url:
        pytest.fail("CLICKHOUSE_TEST_URL must point to an isolated ClickHouse test server")
    ch = clickhouse_market.ClickHouse(url, database="mkt_test_" + uuid.uuid4().hex)
    await clickhouse_market.create_database(ch)
    try:
        yield ch
    finally:
        try:
            await clickhouse_market.drop_database(ch)
        finally:
            await ch.close()


@pytest.fixture(autouse=True, scope="session")
def flow_evidence_from_bars():
    """A Market Truth calculation of a snapshot that has bars (`flow_bars`) and no `flow_sql` gets the records the
    trade-flow statements would return for those bars (tests/flow_helpers.py), and one that has no `liquidity_sql`
    those of the order-book statements for its books and bars (tests/liquidity_helpers.py) and one that has no
    `supply_sql` those of the supply statements for its supply rows (tests/supply_helpers.py), and one that has no
    `series_sql` those of the series statements for its rows (tests/series_helpers.py), so that a test can describe
    the market minute by minute. Session scoped: module-scoped baselines calculate too."""
    from flow_helpers import with_flow
    from liquidity_helpers import with_liquidity
    from series_helpers import with_series
    from supply_helpers import with_supply

    from tank.analysis import truth

    compute, condition = truth._compute_market_truth, truth.compute_condition_values

    def assessed(snapshot, **kwargs):
        snapshot = snapshot if "flow_sql" in snapshot else with_flow(snapshot)
        if "liquidity_sql" not in snapshot:
            snapshot = with_liquidity(snapshot, config=kwargs.get("config"))
        if "supply_sql" not in snapshot:
            snapshot = with_supply(snapshot, config=kwargs.get("config"))
        if "series_sql" not in snapshot:
            snapshot = with_series(snapshot, config=kwargs.get("config"))
        return compute(snapshot, **kwargs)

    def evaluated(snapshot, requirements, **kwargs):
        snapshot = snapshot if "flow_sql" in snapshot else with_flow(snapshot)
        windows, state = truth.condition_liquidity_windows(snapshot, requirements)
        if windows and "liquidity_sql" not in snapshot:
            snapshot = with_liquidity(snapshot, windows=windows, state=state, config=kwargs.get("config"))
        windows = truth.condition_supply_windows(snapshot, requirements)
        if windows and "supply_sql" not in snapshot:
            snapshot = with_supply(snapshot, windows=windows, config=kwargs.get("config"))
        needs = truth.condition_series_needs(snapshot, requirements)
        if any(needs.values()) and "series_sql" not in snapshot:
            snapshot = with_series(snapshot, config=kwargs.get("config"), **needs)
        return condition(snapshot, requirements, **kwargs)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(truth, "_compute_market_truth", assessed)
        patch.setattr(truth, "compute_condition_values", evaluated)
        yield


@pytest.fixture
def relative_statements(monkeypatch):
    """Stand-in for the ClickHouse statements of the price-relative, volume, open-interest, funding, trade-flow and
    order-book, supply and series rules in tests that run a Market Truth service without a ClickHouse or PostgreSQL
    server: the snapshot keeps the `relative_sql`, the `volume_sql`, the `oi_sql`, the `funding_sql`, the `flow_sql`,
    the `liquidity_sql`, the `supply_sql` and the `series_sql` it carries, `evidence`, `volumes`, `oi`, `funding`,
    `flow`, `liquidity`, `supply` and `series` replace them when set, and a snapshot without one has no such evidence
    (the flow, liquidity, supply and series evidence of its rows, when it has any). The statements themselves are
    tested against a real server in tests/test_truth_relative_sql.py, tests/test_truth_volume_sql.py,
    tests/test_truth_oi_sql.py, tests/test_truth_funding_sql.py, tests/test_truth_flow_sql.py,
    tests/test_truth_liquidity_sql.py, tests/test_truth_supply_sql.py and tests/test_truth_series_sql.py."""
    from types import SimpleNamespace

    from flow_helpers import flow_evidence
    from liquidity_helpers import liquidity_evidence
    from series_helpers import series_evidence
    from supply_helpers import supply_evidence

    from tank.analysis import truth, truth_sql

    stand_in = SimpleNamespace(
        evidence=None,
        volumes=None,
        oi=None,
        funding=None,
        flow=None,
        liquidity=None,
        supply=None,
        series=None,
    )

    async def attach_series(repo, snapshot, config, *, tickers=True, health=True, lead_lag=True, **kwargs):
        evidence = stand_in.series or snapshot.get("series_sql")
        if evidence is None:
            evidence = series_evidence(
                snapshot, tickers=tickers, health=health, lead_lag=lead_lag, config=config
            )
        return dict(snapshot, series_sql=evidence)

    monkeypatch.setattr(truth, "with_series", attach_series)

    async def attach_supply(repo, snapshot, config, *, windows=None, **kwargs):
        evidence = stand_in.supply or snapshot.get("supply_sql")
        if evidence is None:
            evidence = supply_evidence(snapshot, windows=windows, config=config)
        return dict(snapshot, supply_sql=evidence)

    monkeypatch.setattr(truth, "with_supply", attach_supply)

    async def attach_liquidity(repo, snapshot, config, *, windows=None, state=True, **kwargs):
        evidence = stand_in.liquidity or snapshot.get("liquidity_sql")
        if evidence is None:
            evidence = liquidity_evidence(snapshot, windows=windows, state=state, config=config)
        return dict(snapshot, liquidity_sql=evidence)

    monkeypatch.setattr(truth, "with_liquidity", attach_liquidity)

    async def attach_flow(repo, snapshot, config, **kwargs):
        evidence = stand_in.flow or snapshot.get("flow_sql") or flow_evidence(snapshot)
        return dict(snapshot, flow_sql=evidence)

    monkeypatch.setattr(truth, "with_flow", attach_flow)

    async def attach(repo, snapshot, config, **kwargs):
        evidence = stand_in.evidence or snapshot.get("relative_sql") or truth_sql.NO_RELATIVE
        return dict(snapshot, relative_sql=evidence)

    async def attach_volumes(repo, snapshot, config, **kwargs):
        evidence = stand_in.volumes or snapshot.get("volume_sql") or truth_sql.NO_VOLUME
        return dict(snapshot, volume_sql=evidence)

    async def attach_oi(repo, snapshot, config, **kwargs):
        evidence = stand_in.oi or snapshot.get("oi_sql") or truth_sql.NO_OI
        return dict(snapshot, oi_sql=evidence)

    monkeypatch.setattr(truth, "with_relative", attach)
    monkeypatch.setattr(truth, "with_volumes", attach_volumes)

    async def attach_funding(repo, snapshot, config, **kwargs):
        evidence = stand_in.funding or snapshot.get("funding_sql") or truth_sql.NO_FUNDING
        return dict(snapshot, funding_sql=evidence)

    monkeypatch.setattr(truth, "with_oi", attach_oi)
    monkeypatch.setattr(truth, "with_funding", attach_funding)
    return stand_in
