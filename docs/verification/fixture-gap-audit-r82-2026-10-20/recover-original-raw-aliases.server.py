"""Exact-byte original raw alias recovery; no semantic/source field changes."""

import hashlib
import importlib.util
import json
import shutil
from pathlib import Path

import zstandard

ROOT = Path("/srv/tank")
HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("prep", HERE / "prepare-market-packets.server-v2.py")
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)


def raw_sha(path):
    raw = path.read_bytes()
    if raw.startswith(b"\x28\xb5\x2f\xfd"):
        with path.open("rb") as source, zstandard.ZstdDecompressor().stream_reader(source) as reader:
            return hashlib.file_digest(reader, "sha256").hexdigest()
    return hashlib.sha256(raw).hexdigest()


def main():
    cases = {c["fixture_id"]: c for c in helper.read_json(HERE / "portfolio.candidate.json")["fixtures"]}
    contexts = helper.read_json(HERE / "news-private-reference-audit.json")["current_contexts"]
    rows = []
    for context in contexts:
        if not context["runtime_host_private_proof_required_by_current_validator"]:
            continue
        fid = context["fixture_id"]
        private = helper.read_json(cases[fid]["private_name_map"]["path"])
        for expected, declared in private.get("original_raw_paths", {}).items():
            target = helper.host_path(declared)
            row = {
                "fixture_id": fid,
                "declared_original_raw_path": declared,
                "expected_raw_sha256": expected,
                "existed": target.is_file(),
                "restored": False,
            }
            if target.is_file():
                row["raw_sha256_match"] = raw_sha(target) == expected
                assert row["raw_sha256_match"]
            else:
                matches = []
                for candidate in (ROOT / "data/replay").rglob("*" + expected + "*"):
                    if (
                        candidate.is_file()
                        and candidate != target
                        and candidate.suffix in {".json", ".txt", ".zst"}
                        and raw_sha(candidate) == expected
                    ):
                        matches.append(candidate)
                row["exact_equivalents"] = [str(p.relative_to(ROOT)) for p in matches]
                if matches:
                    source = sorted(matches, key=lambda p: len(str(p)))[0]
                    assert target.is_relative_to(ROOT / "data/replay/private"), str(target)
                    assert shutil.disk_usage(ROOT).free >= 8 * 1024**3
                    missing = []
                    ancestor = target.parent
                    while not ancestor.exists():
                        missing.append(ancestor)
                        ancestor = ancestor.parent
                    for folder in reversed(missing):
                        folder.mkdir(mode=0o700)
                    with source.open("rb") as src, target.open("xb") as dest:
                        shutil.copyfileobj(src, dest)
                    target.chmod(0o600)
                    assert raw_sha(target) == expected
                    assert (
                        hashlib.sha256(source.read_bytes()).hexdigest()
                        == hashlib.sha256(target.read_bytes()).hexdigest()
                    )
                    row.update(
                        restored=True,
                        raw_sha256_match=True,
                        source=str(source.relative_to(ROOT)),
                        file_sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
                        bytes=target.stat().st_size,
                    )
                else:
                    row["raw_sha256_match"] = False
            rows.append(row)
    report = {
        "scope": "EXACT_ORIGINAL_RAW_HOST_ALIAS_RECOVERY_NOT_PARENT_RECONSTRUCTION",
        "bindings": rows,
        "restored": sum(x["restored"] for x in rows),
        "unresolved": sum(not x["raw_sha256_match"] for x in rows),
        "source_fields_rewritten": False,
        "native_admission_run": False,
    }
    (HERE / "original-raw-alias-recovery.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
