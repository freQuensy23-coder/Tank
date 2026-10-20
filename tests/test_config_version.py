import os
import shutil
import subprocess
import sys
from pathlib import Path

from tank.settings import Settings, resolve_versions


def git(root, *arguments):
    return subprocess.check_output(["git", "-C", str(root), *arguments], text=True).strip()


def repository(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    git(root, "init", "-q")
    (root / ".gitignore").write_text("data/\nconfig/ignored.yaml\n")
    (root / "README.md").write_text("Outside configuration\n")
    config = root / "config"
    config.mkdir()
    (config / "rules.yaml").write_text("value: 1\n")
    git(root, "add", ".")
    git(root, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "Baseline")
    return root, config, git(root, "rev-parse", "HEAD")


def test_ignored_snapshot_root_has_content_identity_and_no_clean_commit_alias(tmp_path):
    root, config, commit = repository(tmp_path)
    snapshot = root / "data/snapshot/config"
    snapshot.mkdir(parents=True)
    shutil.copyfile(config / "rules.yaml", snapshot / "rules.yaml")
    settings = Settings(config_dir=snapshot, data_dir=tmp_path / "runtime", _env_file=None)
    assert git(snapshot, "status", "--porcelain", "--", ".") == ""
    first = resolve_versions(settings)
    assert first.config_version.startswith(commit + "-dirty.")
    (snapshot / "rules.yaml").write_text("value: 2\n")
    second = resolve_versions(settings)
    assert second.config_version != first.config_version
    assert len(list((settings.data_dir / "config_versions").glob("*.json"))) == 2


def test_ignored_extra_payload_is_not_hidden_by_clean_status(tmp_path):
    root, config, commit = repository(tmp_path)
    settings = Settings(config_dir=config, data_dir=tmp_path / "runtime", _env_file=None)
    assert resolve_versions(settings).config_version == commit
    (config / "ignored.yaml").write_text("hidden_rule: true\n")
    assert git(config, "status", "--porcelain", "--", ".") == ""
    assert resolve_versions(settings).config_version.startswith(commit + "-dirty.")


def test_assume_unchanged_cannot_hide_modified_loaded_bytes(tmp_path):
    root, config, commit = repository(tmp_path)
    git(root, "update-index", "--assume-unchanged", "config/rules.yaml")
    (config / "rules.yaml").write_text("value: 3\n")
    assert git(config, "status", "--porcelain", "--", ".") == ""
    assert resolve_versions(
        Settings(config_dir=config, data_dir=tmp_path / "runtime", _env_file=None)
    ).config_version.startswith(commit + "-dirty.")


def test_prepare_config_uses_same_exact_head_proof_for_ignored_snapshot(tmp_path):
    import runpy
    from pathlib import Path

    prepare_config = runpy.run_path(str(Path(__file__).resolve().parents[1] / "deploy/prepare_config.py"))[
        "prepare_config"
    ]

    root, config, commit = repository(tmp_path)
    snapshot = root / "data/snapshot/config"
    snapshot.mkdir(parents=True)
    shutil.copyfile(config / "rules.yaml", snapshot / "rules.yaml")
    manifest = prepare_config(snapshot)
    assert manifest["git_commit"] == commit
    assert manifest["git_dirty"] is True
    version = resolve_versions(
        Settings(config_dir=snapshot, data_dir=tmp_path / "runtime", _env_file=None)
    ).config_version
    assert version == commit + "-dirty." + manifest["content_sha256"]


def test_exact_git_payload_handles_nested_root_and_unusual_filename(tmp_path):
    root, config, _ = repository(tmp_path)
    (config / "rules [1]\t.json").write_text('{"literal":true}')
    git(root, "add", "config")
    git(
        root,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.invalid",
        "commit",
        "-qm",
        "Literal filename",
    )
    commit = git(root, "rev-parse", "HEAD")
    (root / "README.md").write_text("Unrelated dirty file outside config\n")
    settings = Settings(config_dir=config, data_dir=tmp_path / "runtime", _env_file=None)
    assert resolve_versions(settings).config_version == commit


def test_packaged_json_resource_changes_code_version_in_cold_process(tmp_path):
    package_root = tmp_path / "package"
    copied_package = package_root / "tank"
    shutil.copytree(Path(__file__).resolve().parents[1] / "src/tank", copied_package)
    resource = copied_package / "resources/fixture-input-authority-v1.json"
    assert resource.is_file()
    config = tmp_path / "config"
    config.mkdir()
    (config / "rules.yaml").write_text("value: 1\n")
    program = (
        "import sys; from pathlib import Path; "
        "from tank.settings import Settings, resolve_versions; "
        "print(resolve_versions(Settings(config_dir=Path(sys.argv[1]), "
        "data_dir=Path(sys.argv[2]), _env_file=None)).code_version)"
    )
    environment = dict(os.environ, PYTHONPATH=str(package_root))

    def cold_version():
        return subprocess.check_output(
            [sys.executable, "-c", program, str(config), str(tmp_path / "runtime")],
            env=environment,
            cwd=tmp_path,
            text=True,
        ).strip()

    original = cold_version()
    assert cold_version() == original
    resource.write_bytes(resource.read_bytes() + b"\n")
    assert cold_version() != original
