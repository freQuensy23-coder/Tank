import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

STAGE = Path(__file__).parents[1] / "deploy/openma_tool_kind"
SPEC = importlib.util.spec_from_file_location("tool_kind_overlay", STAGE / "prepare_overlay.py")
overlay = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(overlay)


def sha(value):
    return hashlib.sha256(value.encode()).hexdigest()


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    assets, root = tmp_path / "assets", tmp_path / "tree"
    assets.mkdir()
    root.mkdir()
    rows, replacements = [], {}
    for name, before, after in [("a.ts", "old-a", "new-a"), ("nested/b.ts", "old-b", "new-b")]:
        path = root / name
        path.parent.mkdir(exist_ok=True)
        path.write_text(before)
        rows.append({"path": name, "before_sha256": sha(before), "after_sha256": sha(after)})
        replacements[name] = [{"before": before, "after": after}]
    (assets / "source-guards.json").write_text(json.dumps(rows))
    (assets / "replacements.json").write_text(json.dumps(replacements))
    (assets / "manifest.json").write_text(
        json.dumps(
            {
                "assets_sha256": {
                    name: hashlib.sha256((assets / name).read_bytes()).hexdigest()
                    for name in ["source-guards.json", "replacements.json"]
                }
            }
        )
    )
    monkeypatch.setattr(overlay, "BASE", assets)
    return root, assets




def test_apply_is_exact_and_repeat_is_refused(prepared):
    root, _ = prepared
    assert overlay.apply(root) == {"a.ts": sha("new-a"), "nested/b.ts": sha("new-b")}
    with pytest.raises(ValueError, match="source guard mismatch"):
        overlay.apply(root)


def test_second_guard_failure_does_not_mutate_first_target(prepared):
    root, _ = prepared
    (root / "nested/b.ts").write_text("changed")
    with pytest.raises(ValueError, match="source guard mismatch"):
        overlay.apply(root)
    assert (root / "a.ts").read_text() == "old-a"


def test_asset_tampering_is_refused_before_mutation(prepared):
    root, assets = prepared
    (assets / "replacements.json").write_text("{}")
    with pytest.raises(ValueError, match="asset hash mismatch"):
        overlay.apply(root)
    assert (root / "a.ts").read_text() == "old-a"


@pytest.mark.parametrize("dangling", [False, True])
def test_symlink_target_is_refused(prepared, tmp_path, dangling):
    root, _ = prepared
    outside = tmp_path / "outside.ts"
    if not dangling:
        outside.write_text("old-a")
    (root / "a.ts").unlink()
    (root / "a.ts").symlink_to(outside)
    with pytest.raises(ValueError, match="symlink"):
        overlay.apply(root)
    if not dangling:
        assert outside.read_text() == "old-a"


def test_symlink_parent_is_refused_before_any_mutation(prepared, tmp_path):
    root, _ = prepared
    (root / "nested/b.ts").unlink()
    (root / "nested").rmdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "b.ts").write_text("old-b")
    (root / "nested").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        overlay.apply(root)
    assert (root / "a.ts").read_text() == "old-a"
    assert (outside / "b.ts").read_text() == "old-b"
