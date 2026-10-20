"""Exercise source/output/symlink guards using a supplied pinned source file."""

import argparse
import hashlib
import importlib.util
import json
import tempfile
from pathlib import Path

BASE = Path(__file__).resolve().parent


def check(prepared):
    spec = importlib.util.spec_from_file_location("tank_network_stage", BASE / "prepare_overlay.py")
    stage = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(stage)
    original = (prepared / stage.TARGET).read_bytes()
    assert hashlib.sha256(original).hexdigest() == stage.INPUT_SHA256
    passed = []
    with tempfile.TemporaryDirectory(prefix="tank-native-network-guards-") as directory:
        root = Path(directory)

        def fixture(name):
            tree = root / name
            path = tree / stage.TARGET
            path.parent.mkdir(parents=True)
            path.write_bytes(original)
            return tree, path

        def refuses(tree):
            try:
                stage.apply(tree)
            except ValueError:
                return
            raise AssertionError("Unsafe stage application accepted")

        tree, path = fixture("success")
        stage.apply(tree)
        manifest = json.loads((BASE / "manifest.json").read_text())
        assert hashlib.sha256(path.read_bytes()).hexdigest() == manifest["patched_sha256"]
        passed.append("exact_input_and_output")
        before = path.read_bytes()
        refuses(tree)
        assert path.read_bytes() == before
        passed.append("repeat_refused_without_mutation")
        tree, path = fixture("changed")
        path.write_bytes(original + b"\n")
        refuses(tree)
        assert path.read_bytes() == original + b"\n"
        passed.append("changed_input_refused_without_mutation")
        for broken in [False, True]:
            tree, path = fixture("symlink-" + str(broken))
            outside = root / ("outside-" + str(broken))
            if not broken:
                outside.write_bytes(original)
            path.unlink()
            path.symlink_to(outside)
            refuses(tree)
            assert path.is_symlink()
            if not broken:
                assert outside.read_bytes() == original
            passed.append("broken_symlink_refused" if broken else "matching_symlink_refused")
        tree, path = fixture("output-mismatch")
        fake_assets = root / "altered-assets"
        fake_assets.mkdir()
        (fake_assets / "manifest.json").write_text(json.dumps({"patched_sha256": "0" * 64}))
        stage.BASE = fake_assets
        refuses(tree)
        assert path.read_bytes() == original
        passed.append("output_guard_refused_without_mutation")
    return {"status": "PASS", "checks": passed, "count": len(passed), "native": "NOT_RUN"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("prepared_source", type=Path)
    print(json.dumps(check(parser.parse_args().prepared_source), indent=2))
