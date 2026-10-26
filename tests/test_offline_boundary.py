"""The boundary tests never attempt an actual external connection."""

import socket
from pathlib import Path

import conftest as policy
import pytest

from tank.settings import Settings


def test_operator_environment_is_removed_by_name() -> None:
    environment = {
        "TANK_TELEGRAM_TOKEN": "FAKE-operator",
        "OPENAI_API_KEY": "FAKE-operator",
        "PGPASSWORD": "FAKE-operator",
        "HTTPS_PROXY": "FAKE-operator",
        "SSH_AUTH_SOCK": "FAKE-agent-socket",
        "DOCKER_HOST": "FAKE-remote-daemon",
        "APIFY_IS_AT_HOME": "1",
        "APIFY_LOCAL_STORAGE_DIR": "/FAKE-operator/storage",
        "APIFY_INPUT_KEY": "FAKE-operator-input",
        "ACTOR_RUN_ID": "FAKE-operator-run",
        "ACTOR_EVENTS_WEBSOCKET_URL": "wss://FAKE-operator.invalid/events",
        "ACTOR_STORAGE_DIR": "/FAKE-operator/actor-storage",
        "NETRC": "FAKE-netrc-path",
        "TANK_TEST_DATABASE_URL": "postgresql://fake:fake@localhost/test",
        "PATH": "/fake/bin",
    }
    policy.scrub_inherited_environment(environment)
    assert set(environment) == {"TANK_TEST_DATABASE_URL", "PATH"}


def test_operator_dotenv_is_never_opened(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / ".env"
    path.write_text("TANK_TELEGRAM_TOKEN=FAKE-dotenv-must-not-be-loaded\n")
    assert Settings(_env_file=path).telegram_token is None
    monkeypatch.setenv("TANK_TELEGRAM_TOKEN", "FAKE-explicit-test-environment")
    assert Settings(_env_file=path).telegram_token == "FAKE-explicit-test-environment"


def test_default_secret_and_auth_locations_are_synthetic(tmp_path: Path) -> None:
    settings = Settings()
    assert settings.codex_auth_path.is_relative_to(tmp_path)
    assert settings.apify_api_token is None
    assert settings.tank_db_password is None
    assert settings.telegram_token is None
    assert settings.openma_api_key is None
    assert settings.codex_gateway_key is None


def test_external_connect_is_rejected_before_native_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden_native(*args) -> None:
        raise AssertionError("native external socket must never be called")

    monkeypatch.setattr(policy, "_ORIGINAL_CONNECT", forbidden_native)
    monkeypatch.setattr(policy, "_ORIGINAL_CONNECT_EX", forbidden_native)
    with socket.socket() as sock:
        with pytest.raises(policy.OfflineNetworkError):
            sock.connect(("203.0.113.10", 443))
        with pytest.raises(policy.OfflineNetworkError):
            sock.connect_ex(("203.0.113.10", 443))


def test_external_dns_is_rejected_before_resolver(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden_native(*args, **kwargs) -> None:
        raise AssertionError("native external DNS must never be called")

    monkeypatch.setattr(policy, "_ORIGINAL_GETADDRINFO", forbidden_native)
    with pytest.raises(policy.OfflineNetworkError):
        socket.getaddrinfo("offline-boundary.invalid", 443)


def test_loopback_and_unix_remain_available() -> None:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        with socket.create_connection(listener.getsockname(), timeout=1):
            connection, _ = listener.accept()
            connection.close()
    left, right = socket.socketpair()
    left.close()
    right.close()


def test_explicit_database_host_allowlist_without_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    assert policy.test_database_hosts(
        {"TANK_TEST_DATABASE_URL": "postgresql://fake:fake@db.test.invalid/test"}
    ) == {"db.test.invalid"}
    monkeypatch.setattr(policy, "_ALLOWED_DATABASE_HOSTS", {"db.test.invalid"})
    with socket.socket() as sock:
        policy.check_address(sock, ("db.test.invalid", 5432))


def test_external_udp_is_rejected_before_native_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden_native(*args) -> None:
        raise AssertionError("native external UDP must never be called")

    monkeypatch.setattr(policy, "_ORIGINAL_SENDTO", forbidden_native)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock, pytest.raises(policy.OfflineNetworkError):
        sock.sendto(b"synthetic", ("203.0.113.10", 53))


def test_home_lookup_is_synthetic(monkeypatch: pytest.MonkeyPatch) -> None:
    from pathlib import Path

    assert Path.home() == policy._OFFLINE_HOME
    assert str(policy._OFFLINE_HOME) in __import__("os").environ["CODEX_HOME"]


@pytest.mark.skipif(not hasattr(socket.socket, "sendmsg"), reason="platform has no sendmsg")
def test_external_sendmsg_is_rejected_before_native_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden_native(*args) -> None:
        raise AssertionError("native external sendmsg must never be called")

    monkeypatch.setattr(policy, "_ORIGINAL_SENDMSG", forbidden_native)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock, pytest.raises(policy.OfflineNetworkError):
        sock.sendmsg([b"synthetic"], [], 0, ("203.0.113.10", 53))


def test_known_operator_auth_path_is_blocked_before_file_open(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden_native(*args, **kwargs) -> None:
        raise AssertionError("operator file must never be opened")

    monkeypatch.setattr(policy, "_ORIGINAL_OPEN", forbidden_native)
    with pytest.raises(policy.OfflineCredentialError), open("/codex-auth/auth.json"):
        pass


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://postgres:FAKE@172.30.60.2/tank",
        "postgresql://tank:FAKE@postgres:5432/oma",
        "host=172.30.60.2 dbname=tank_replay user=postgres",
        "postgresql://postgres:FAKE@172.30.60.2/",
    ],
)
def test_production_databases_are_refused(url) -> None:
    with pytest.raises(pytest.UsageError):
        policy.check_test_databases({"TANK_TEST_DATABASE_URL": url})


def test_dedicated_test_database_is_accepted() -> None:
    policy.check_test_databases(
        {
            "TANK_TEST_DATABASE_URL": "postgresql://postgres:FAKE@172.30.60.2/tank_test_tank-ops",
            "TANK_E2E_DATABASE_URL": "host=172.30.60.2 dbname=tank_test_e2e user=postgres",
        }
    )


@pytest.mark.parametrize("protected_file", [False, True])
def test_credential_alias_is_refused_before_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, protected_file: bool
) -> None:
    target = tmp_path / "credentials"
    target.mkdir()
    secret = target / "auth.json"
    secret.write_text("synthetic")
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    monkeypatch.setattr(policy, "_PROTECTED_CREDENTIAL_FILES", {secret} if protected_file else set())
    monkeypatch.setattr(policy, "_PROTECTED_CREDENTIAL_ROOTS", () if protected_file else (target,))

    def forbidden_native(*args: object, **kwargs: object) -> None:
        raise AssertionError("protected alias reached native open")

    monkeypatch.setattr(policy, "_ORIGINAL_OPEN", forbidden_native)
    monkeypatch.setattr(policy, "_ORIGINAL_IO_OPEN", forbidden_native)
    with pytest.raises(policy.OfflineCredentialError):
        policy._open(alias / "auth.json")
    with pytest.raises(policy.OfflineCredentialError):
        policy._io_open(alias / "auth.json")


@pytest.mark.parametrize("protected_file", [False, True])
def test_protected_ancestor_retarget_is_observed_on_each_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, protected_file: bool
) -> None:
    old = tmp_path / "old"
    new = tmp_path / "new"
    old.mkdir()
    new.mkdir()
    ancestor = tmp_path / "ancestor"
    ancestor.symlink_to(old, target_is_directory=True)
    protected = ancestor / "auth.json" if protected_file else ancestor
    monkeypatch.setattr(policy, "_PROTECTED_CREDENTIAL_FILES", {protected} if protected_file else set())
    monkeypatch.setattr(policy, "_PROTECTED_CREDENTIAL_ROOTS", () if protected_file else (protected,))
    policy._check_credential_file(new / "auth.json", "r")
    ancestor.unlink()
    ancestor.symlink_to(new, target_is_directory=True)
    with pytest.raises(policy.OfflineCredentialError):
        policy._check_credential_file(new / "auth.json", "r")
    policy._check_credential_file(old / "auth.json", "r")


def test_late_created_protected_symlink_is_observed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "target"
    target.mkdir()
    protected = tmp_path / "not-yet-created"
    monkeypatch.setattr(policy, "_PROTECTED_CREDENTIAL_FILES", set())
    monkeypatch.setattr(policy, "_PROTECTED_CREDENTIAL_ROOTS", (protected,))
    policy._check_credential_file(target / "auth.json", "r")
    protected.symlink_to(target, target_is_directory=True)
    with pytest.raises(policy.OfflineCredentialError):
        policy._check_credential_file(target / "auth.json", "r")
