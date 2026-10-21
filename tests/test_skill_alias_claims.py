import copy
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
SPEC = importlib.util.spec_from_file_location(
    "skill_alias_claims", ROOT / "deploy/openma_skill_aliases/claim-skill-paths.py"
)
claims = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(claims)


def binding():
    return {"name": "tank-data", "skill_id": "skill-one", "version": "123", "archive_sha256": "a" * 64}


def test_claim_is_durable_before_files_and_same_binding_restarts(tmp_path):
    plan = binding()
    claims.apply([plan], "check", tmp_path)
    assert not (tmp_path / "home").exists()
    claims.apply([plan], "claim", tmp_path)
    base = tmp_path / "home/user/.skills"
    assert json.loads((base / ".openma-bindings/tank-data.json").read_text()) == plan
    partial = base / "tank-data/SKILL.md"
    partial.write_text("partial prior preparation")
    claims.apply([plan], "check", tmp_path)
    claims.apply([plan], "claim", tmp_path)
    assert partial.read_text() == "partial prior preparation"


@pytest.mark.parametrize(
    "key,value", [("skill_id", "other"), ("version", "124"), ("archive_sha256", "b" * 64)]
)
def test_rebinding_refused_without_touching_alias_bytes(tmp_path, key, value):
    plan = binding()
    claims.apply([plan], "claim", tmp_path)
    file = tmp_path / "home/user/.skills/tank-data/SKILL.md"
    file.write_bytes(b"original skill bytes")
    with pytest.raises(ValueError, match="binding mismatch"):
        claims.apply([{**plan, key: value}], "claim", tmp_path)
    assert file.read_bytes() == b"original skill bytes"


@pytest.mark.parametrize("same", [True, False])
def test_concurrent_exclusive_receipt_claim_is_revalidated(tmp_path, monkeypatch, same):
    plan = binding()

    def raced_link(source, destination, **kwargs):
        competitor = plan if same else {**plan, "version": "unrelated"}
        Path(destination).write_text(json.dumps(competitor))
        raise FileExistsError("concurrent claim")

    monkeypatch.setattr(claims.os, "link", raced_link)
    if same:
        claims.apply([plan], "claim", tmp_path)
        assert (tmp_path / "home/user/.skills/tank-data").is_dir()
    else:
        with pytest.raises(ValueError, match="binding mismatch"):
            claims.apply([plan], "claim", tmp_path)
        assert not (tmp_path / "home/user/.skills/tank-data").exists()


def test_unbound_existing_directory_and_nested_symlinks_fail_closed(tmp_path):
    alias = tmp_path / "home/user/.skills/tank-data"
    alias.mkdir(parents=True)
    with pytest.raises(ValueError, match="Unbound"):
        claims.apply([binding()], "claim", tmp_path)
    alias.rmdir()
    claims.apply([binding()], "claim", tmp_path)
    outside = tmp_path / "other"
    outside.mkdir()
    (alias / "references").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="non-plain"):
        claims.apply([binding()], "claim", tmp_path)
    assert not list(outside.iterdir())


def test_all_bindings_checked_before_any_claim_and_invalid_sizes_rejected(tmp_path):
    good, bad = binding(), {**binding(), "name": "../escape"}
    with pytest.raises(ValueError, match="name"):
        claims.apply([good, bad], "claim", tmp_path)
    assert not (tmp_path / "home").exists()
    with pytest.raises(ValueError, match="Duplicate"):
        claims.apply([good, copy.deepcopy(good)], "claim", tmp_path)
    with pytest.raises(ValueError, match="identity"):
        claims.apply([{**good, "version": "x" * 192}], "claim", tmp_path)
    with pytest.raises(ValueError, match="bounds"):
        claims.apply([good] * 1000, "claim", tmp_path)


def test_corrupt_receipt_is_never_treated_as_absent(tmp_path):
    claims.apply([binding()], "claim", tmp_path)
    receipt = tmp_path / "home/user/.skills/.openma-bindings/tank-data.json"
    receipt.write_text("{partial")
    with pytest.raises(ValueError):
        claims.apply([binding()], "claim", tmp_path)


def test_guarded_overlay_refuses_unreviewed_materializer(tmp_path):
    spec = importlib.util.spec_from_file_location(
        "alias_overlay", ROOT / "deploy/openma_skill_aliases/prepare_overlay.py"
    )
    overlay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(overlay)
    target = tmp_path / overlay.TARGET
    target.parent.mkdir(parents=True)
    target.write_text("changed input")
    with pytest.raises(ValueError, match="source guard"):
        overlay.apply(tmp_path)
    assert target.read_text() == "changed input"
    assert not target.with_name("node-managed-stable-skills.ts").exists()


def test_failed_atomic_publication_leaves_no_partial_receipt_and_retries(tmp_path, monkeypatch):
    original = claims.os.link

    def fail(*args, **kwargs):
        raise OSError("interrupted publication")

    monkeypatch.setattr(claims.os, "link", fail)
    with pytest.raises(OSError, match="interrupted"):
        claims.apply([binding()], "claim", tmp_path)
    receipts = tmp_path / "home/user/.skills/.openma-bindings"
    assert not list(receipts.iterdir())
    assert not (tmp_path / "home/user/.skills/tank-data").exists()
    monkeypatch.setattr(claims.os, "link", original)
    claims.apply([binding()], "claim", tmp_path)
    assert json.loads((receipts / "tank-data.json").read_text()) == binding()


def test_overlay_refuses_hash_matching_target_symlink(tmp_path, monkeypatch):
    import hashlib

    spec = importlib.util.spec_from_file_location(
        "alias_symlink_guard", ROOT / "deploy/openma_skill_aliases/prepare_overlay.py"
    )
    overlay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(overlay)
    outside = tmp_path / "outside"
    outside.write_text("matching bytes")
    monkeypatch.setattr(overlay, "INPUT_SHA256", hashlib.sha256(outside.read_bytes()).hexdigest())
    root = tmp_path / "prepared"
    target = root / overlay.TARGET
    target.parent.mkdir(parents=True)
    target.symlink_to(outside)
    with pytest.raises(ValueError, match="plain scoped"):
        overlay.apply(root)
    assert outside.read_text() == "matching bytes"


def test_overlay_refuses_dangling_generated_asset_symlink(tmp_path, monkeypatch):
    import hashlib

    spec = importlib.util.spec_from_file_location(
        "alias_new_asset_guard", ROOT / "deploy/openma_skill_aliases/prepare_overlay.py"
    )
    overlay = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(overlay)
    root = tmp_path / "prepared"
    target = root / overlay.TARGET
    target.parent.mkdir(parents=True)
    target.write_text("matching bytes")
    monkeypatch.setattr(overlay, "INPUT_SHA256", hashlib.sha256(target.read_bytes()).hexdigest())
    assets = tmp_path / "assets"
    assets.mkdir()
    (assets / "manifest.json").write_text('{"assets_sha256":{}}')
    monkeypatch.setattr(overlay, "BASE", assets)
    target.with_name("node-managed-stable-skills.ts").symlink_to(tmp_path / "absent")
    with pytest.raises(ValueError, match="already exists"):
        overlay.apply(root)
    assert target.read_text() == "matching bytes"
    assert not (tmp_path / "absent").exists()
