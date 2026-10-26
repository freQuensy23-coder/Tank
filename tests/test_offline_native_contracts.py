"""Real native guard controls use newly created synthetic files only."""

import os
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
