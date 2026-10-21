import ast
import copy
import runpy
from pathlib import Path

import pytest

CI_ROOT = Path(__file__).resolve().parents[1] / "scripts/ci"
COMMENT_POLICY = runpy.run_path(str(CI_ROOT / "no_comments.py"))
DEPLOY_POLICY = runpy.run_path(str(CI_ROOT / "deploy_production.py"))
files = COMMENT_POLICY["files"]
go_comments = COMMENT_POLICY["go_comments"]
python_comments = COMMENT_POLICY["python_comments"]
qualified_sha = DEPLOY_POLICY["qualified_sha"]
target = DEPLOY_POLICY["target"]


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ('value = "# literal"\n', []),
        ('"""# documentation"""\n', []),
        ("#!/usr/bin/env python3\nvalue = 1\n", []),
        ("value = 1 # ordinary comment\n", [1]),
        ("\n#!/usr/bin/env python3\n", [2]),
    ],
)
def test_python_comment_tokens(tmp_path: Path, source: str, expected: list[int]) -> None:
    path = tmp_path / "example.py"
    path.write_text(source)
    assert python_comments(path) == expected


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ('package p\nvar url = "https://example.org/a/*b*/"\n', []),
        ("package p\nvar raw = `first\n// literal\n/* literal */`\n", []),
        ("package p\n// ordinary\nvar x = 1 /* ordinary */\n", [2, 3]),
        ("//go:build linux\n// +build linux\n\npackage p\n//go:embed asset.json\nvar raw string\n", []),
        ("package p\n//line generated.go:42\n/*line generated.go:43*/var x = 1\n", []),
        ("package p\nvar x = 1 //go:embed invalid\n", [2]),
        ('package p\n/*#include <stdlib.h>*/\nimport "C"\n//export Callback\nfunc Callback() {}\n', []),
        ('package p\n// ordinary\n\nimport "C"\n', [2]),
        ("package p\nvar raw = `first\nsecond`\n// ordinary\n", [4]),
    ],
)
def test_go_comment_lexer(tmp_path: Path, source: str, expected: list[int]) -> None:
    path = tmp_path / "example.go"
    path.write_text(source)
    assert go_comments(path) == expected


def test_immutable_history_and_upstream_are_excluded(tmp_path: Path) -> None:
    for name in (
        "deploy/history/old.py",
        "deploy/upstream/base.go",
        "deploy/vendor/copy.go",
        "capture/testdata/input.go",
        "src/active.py",
    ):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# retained\n" if path.suffix == ".py" else "// retained\n")
    assert list(files([str(tmp_path)])) == [tmp_path / "src/active.py"]


@pytest.mark.parametrize("value", ["main", "a" * 39, "A" * 40, "a" * 40 + "; id", "$(id)"])
def test_deployment_ref_rejects_shell_and_partial_values(value: str) -> None:
    with pytest.raises(ValueError, match="full lowercase"):
        qualified_sha(value, "b" * 40)


def test_deployment_uses_full_default_sha_and_safe_target() -> None:
    assert qualified_sha("", "b" * 40) == "b" * 40
    assert target("tank-deploy", "openclaw.example.org") == "tank-deploy@openclaw.example.org"
    for user, host in (("-oProxyCommand=id", "example.org"), ("tank", "example.org; id"), ("tank", "$(id)")):
        with pytest.raises(ValueError, match="Invalid production SSH"):
            target(user, host)


def deployment_decisions() -> dict:
    tree = ast.parse(DEPLOY_POLICY["REMOTE"])
    functions = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in {"compose_images", "containers_healthy"}
    ]
    namespace = {}
    exec(compile(ast.Module(body=functions, type_ignores=[]), "remote-decisions", "exec"), namespace)
    return namespace


def test_deployment_requires_existing_production_project_and_service_images() -> None:
    images = deployment_decisions()["compose_images"]
    configuration = {
        "name": "tank",
        "services": {
            "tank": {"image": "tank-v0:persistent-no-x"},
            "tank-capture": {"image": "tank-capture:local"},
        },
    }
    assert images(configuration) == {"tank": "tank-v0:persistent-no-x", "tank-capture": "tank-capture:local"}
    wrong_project = copy.deepcopy(configuration)
    wrong_project["name"] = "tank-v0"
    with pytest.raises(ValueError, match="production project tank"):
        images(wrong_project)
    missing_capture = copy.deepcopy(configuration)
    del missing_capture["services"]["tank-capture"]
    with pytest.raises(ValueError, match="tank-capture"):
        images(missing_capture)


def test_deployment_waits_for_expected_image_and_capture_health() -> None:
    healthy = deployment_decisions()["containers_healthy"]
    images = {"tank": ("source", "tag", "sha256:core"), "tank-capture": ("source", "tag", "sha256:capture")}
    states = {
        "tank": {"Image": "sha256:core", "State": {"Running": True}},
        "tank-capture": {
            "Image": "sha256:capture",
            "State": {"Running": True, "Health": {"Status": "healthy"}},
        },
    }
    assert healthy(states, images)
    for service, section, key, value in [
        ("tank", None, "Image", "sha256:old"),
        ("tank", "State", "Running", False),
        ("tank-capture", "State", "Health", {"Status": "starting"}),
        ("tank-capture", None, "Image", "sha256:old"),
    ]:
        changed = copy.deepcopy(states)
        state = changed[service][section] if section else changed[service]
        state[key] = value
        assert not healthy(changed, images)
    assert not healthy({"tank": states["tank"]}, images)
