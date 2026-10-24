from pathlib import Path

import pytest

from tank.backup import prune_backup_snapshots


def snapshot(root: Path, name: str, complete: bool = True) -> Path:
    path = root / name
    path.mkdir()
    (path / "tank.dump").write_bytes(b"dump")
    if complete:
        (path / "COMPLETE").write_text("done\n")
    return path


def test_keeps_newest_complete_snapshots_only(tmp_path: Path) -> None:
    tmp_path = tmp_path / "backup"
    tmp_path.mkdir()
    for day in ("2026-10-17", "2026-10-18", "2026-10-19", "2026-10-20"):
        snapshot(tmp_path, f"tank-{day}")
    snapshot(tmp_path, "tank-2026-10-16", complete=False)
    (tmp_path / ".tank-2026-10-21.partial").mkdir()
    (tmp_path / "recovered-20261020").mkdir()
    assert prune_backup_snapshots(tmp_path, 3) == ["tank-2026-10-17"]
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        ".tank-2026-10-21.partial",
        "recovered-20261020",
        "tank-2026-10-16",
        "tank-2026-10-18",
        "tank-2026-10-19",
        "tank-2026-10-20",
    ]
    assert prune_backup_snapshots(tmp_path, 3) == []


@pytest.mark.parametrize("keep", [0, -1, True, False])
def test_keep_must_be_positive_integer(tmp_path: Path, keep: int) -> None:
    snapshot(tmp_path, "tank-2026-10-20")
    with pytest.raises(ValueError):
        prune_backup_snapshots(tmp_path, keep)
    assert (tmp_path / "tank-2026-10-20").is_dir()
