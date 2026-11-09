import base64
import importlib.util
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("apify_worker", ROOT / "scripts/deploy/apify_worker.py")
assert SPEC is not None and SPEC.loader is not None
apify_worker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(apify_worker)

SHA = "a" * 40
OLD = "OldBuild000000001"
NEW = "NewBuild000000002"


def worker_dir(tmp_path, main="print('v2')\n"):
    root = tmp_path / "x-reader"
    (root / ".actor").mkdir(parents=True)
    (root / "main.py").write_text(main)
    (root / "Dockerfile").write_text("FROM apify/actor-python:3.13\n")
    (root / ".actor/actor.json").write_text('{"actorSpecification": 1}\n')
    (root / "README.md").write_text("documentation only\n")
    return root


def source(files):
    return [
        {"name": name, "format": "BASE64", "content": base64.b64encode(body).decode()}
        for name, body in sorted(files.items())
    ]


class FakeApi:
    """Apify actor, version and build endpoints as the worker publisher uses them."""

    def __init__(self, active_files, build_status="SUCCEEDED", built_files=None, retag_during_build=False):
        self.tagged = OLD
        self.builds = {
            OLD: {"id": OLD, "status": "SUCCEEDED", "actVersion": {"sourceFiles": source(active_files)}}
        }
        self.version = None
        self.build_status = build_status
        self.built_files = built_files
        self.retag_during_build = retag_during_build
        self.calls = []

    def request(self, method, path, body=None):
        self.calls.append((method, path))
        if method == "GET" and path == "/actors/" + apify_worker.ACTOR_ID:
            return {"taggedBuilds": {"direct": {"buildId": self.tagged}}}
        if method == "PUT" and path == "/actors/" + apify_worker.ACTOR_ID:
            self.tagged = body["taggedBuilds"]["direct"]["buildId"]
            return {}
        if method == "PUT" and path.endswith("/versions/" + apify_worker.VERSION):
            self.version = body
            return body
        if method == "POST" and "/builds?" in path:
            files = self.built_files
            snapshot = source(files) if files is not None else self.version["sourceFiles"]
            self.builds[NEW] = {"id": NEW, "status": "RUNNING", "actVersion": {"sourceFiles": snapshot}}
            return dict(self.builds[NEW])
        if method == "GET" and path.startswith("/actor-builds/"):
            build_id = path.removeprefix("/actor-builds/").split("?")[0]
            if build_id == NEW:
                self.builds[NEW]["status"] = self.build_status
                if self.retag_during_build:
                    self.tagged = "Someone0000000003"
            return dict(self.builds[build_id])
        raise AssertionError(f"unexpected {method} {path}")


def test_unchanged_worker_source_publishes_nothing(tmp_path):
    root = worker_dir(tmp_path)
    files = apify_worker.local_files(root)
    api = FakeApi({**files, "README.md": b"older docs", "tank-release.json": b"{}"})
    assert apify_worker.Worker(api).publish(root, SHA) == (OLD, OLD)
    assert [call for call in api.calls if call[0] != "GET"] == []


def test_changed_worker_is_built_then_activated(tmp_path):
    root = worker_dir(tmp_path)
    api = FakeApi({**apify_worker.local_files(root), "main.py": b"print('v1')\n"})
    assert apify_worker.Worker(api).publish(root, SHA) == (OLD, NEW)
    assert api.tagged == NEW
    uploaded = {entry["name"]: base64.b64decode(entry["content"]) for entry in api.version["sourceFiles"]}
    assert uploaded == apify_worker.local_files(root)
    assert api.version["buildTag"] is None and api.version["versionNumber"] == apify_worker.VERSION
    assert ("POST", f"/actors/{apify_worker.ACTOR_ID}/builds?version=0.3&tag=tank-{SHA[:12]}") in api.calls
    writes = [call for call in api.calls if call[0] != "GET"]
    assert writes[-1] == ("PUT", "/actors/" + apify_worker.ACTOR_ID)


@pytest.mark.parametrize(
    "api_options,message",
    [
        ({"build_status": "FAILED"}, "ended FAILED"),
        ({"built_files": {"main.py": b"other"}}, "built source differs"),
        ({"retag_during_build": True}, "tag changed while building"),
    ],
)
def test_failed_or_foreign_build_never_moves_the_direct_tag(tmp_path, api_options, message):
    root = worker_dir(tmp_path)
    api = FakeApi({"main.py": b"print('v1')\n"}, **api_options)
    with pytest.raises(apify_worker.PublishError, match=message):
        apify_worker.Worker(api).publish(root, SHA)
    assert ("PUT", "/actors/" + apify_worker.ACTOR_ID) not in api.calls


def test_build_deadline_stops_waiting(tmp_path):
    root = worker_dir(tmp_path)
    api = FakeApi({"main.py": b"print('v1')\n"}, build_status="RUNNING")
    ticks = iter(range(0, 10_000, 300))
    with pytest.raises(apify_worker.PublishError, match="ended RUNNING"):
        apify_worker.Worker(api, clock=lambda: next(ticks)).publish(root, SHA)
    assert api.tagged == OLD


def test_restore_points_direct_back_and_verifies(tmp_path):
    api = FakeApi({})
    api.tagged = NEW
    apify_worker.Worker(api).tag(OLD)
    assert api.tagged == OLD


def test_main_prints_build_ids_and_never_the_token(tmp_path, monkeypatch, capsys):
    root = worker_dir(tmp_path)
    env = tmp_path / ".env"
    env.write_text("POSTGRES_PASSWORD=x\nAPIFY_API_TOKEN='apify_api_secretvalue'\n")
    api = FakeApi({"main.py": b"print('v1')\n"})
    tokens = []

    def fake_api(token):
        tokens.append(token)
        return api

    monkeypatch.setattr(apify_worker, "Api", fake_api)
    assert apify_worker.main(["publish", str(root), SHA], env_file=env) == 0
    out, err = capsys.readouterr()
    assert out.split() == [OLD, NEW]
    assert tokens == ["apify_api_secretvalue"] and "secretvalue" not in out + err
    assert apify_worker.main(["publish", str(root), "bad"], env_file=env) == 1
    assert "invalid release SHA" in capsys.readouterr().err


def test_publisher_targets_the_actor_s29_runs():
    sources = yaml.safe_load((ROOT / "config/sources.yaml").read_text())
    urls = re.findall(r"https://api\.apify\.com/v2/actors/([A-Za-z0-9]+)/run-sync\?build=(\w+)", str(sources))
    assert urls == [(apify_worker.ACTOR_ID, apify_worker.TAG)]
