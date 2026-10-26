import os
from pathlib import Path

import pytest

from tank.deployment.credentials import PublisherCredentialError, host_token


def private_file(path: Path, body: str) -> Path:
    path.write_text(body)
    path.chmod(0o600)
    return path


@pytest.mark.parametrize(
    "assignment",
    [
        "APIFY_API_TOKEN=actual-token",
        "export APIFY_API_TOKEN='actual-token'",
        'APIFY_API_TOKEN="actual-token"',
    ],
)
def test_actual_private_file_selects_only_declared_host_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, assignment: str
) -> None:
    monkeypatch.setenv("APIFY_API_TOKEN", "environment-must-not-select")
    path = private_file(tmp_path / ".env", "OTHER=unrelated\n" + assignment + "\n")
    token = host_token(path, owner_uid=os.geteuid())
    assert token.get_secret_value() == "actual-token"
    assert "actual-token" not in repr(token)


@pytest.mark.parametrize(
    "body",
    [
        "OTHER=value\n",
        "APIFY_API_TOKEN=one\nAPIFY_API_TOKEN=two\n",
        "APIFY_API_TOKEN=\n",
        "APIFY_API_TOKEN='unsafe value'\n",
        "APIFY_API_TOKEN=$(execute)\n",
    ],
)
def test_invalid_or_ambiguous_host_credential_is_refused_without_echo(tmp_path: Path, body: str) -> None:
    path = private_file(tmp_path / ".env", body)
    with pytest.raises(PublisherCredentialError) as failure:
        host_token(path, owner_uid=os.geteuid())
    assert body.strip() not in str(failure.value)


def test_symlink_wrong_owner_and_open_permissions_are_refused(tmp_path: Path) -> None:
    path = private_file(tmp_path / ".env", "APIFY_API_TOKEN=private-token\n")
    link = tmp_path / "linked"
    link.symlink_to(path)
    with pytest.raises(OSError):
        host_token(link, owner_uid=os.geteuid())
    with pytest.raises(PublisherCredentialError):
        host_token(path, owner_uid=os.geteuid() + 1)
    path.chmod(0o644)
    with pytest.raises(PublisherCredentialError):
        host_token(path, owner_uid=os.geteuid())


def test_nonregular_file_is_refused_without_blocking(tmp_path: Path) -> None:
    path = tmp_path / "pipe"
    os.mkfifo(path, 0o600)
    with pytest.raises(PublisherCredentialError):
        host_token(path, owner_uid=os.geteuid())
