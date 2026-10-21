"""Bounded deterministic inverse masking; only exact sealed-parent hash permits recovery."""

import hashlib
import importlib.util
import json
import re
import shutil
from pathlib import Path

import zstandard

from tank.replay import digest

ROOT = Path("/srv/tank")
HERE = Path(__file__).resolve().parent
OUT = HERE / "inverse-mask-recovery"
spec = importlib.util.spec_from_file_location("helper", HERE / "prepare-market-packets.server-v2.py")
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
BASE = {
    "F-01": "data/replay/acceptance-F01-signed-market-r39-prepared/inputs/input.json.zst",
    "F-17": "data/replay/acceptance-r36-prepared/F-17-vote-metadata-v6/inputs/input.json.zst",
    "F-24": "data/replay/acceptance-F24-reporter-context-r41/inputs/input.json.zst",
}


def inverse(packet, private, original_rows):
    mapping = {v: k for k, v in private["replacements"].items()}
    sensitive = {private["replacements"][k] for k in private.get("case_sensitive_names", [])}
    parts = sorted(mapping, key=len, reverse=True)
    pattern = re.compile(
        r"(?<!\w)(?:"
        + "|".join(re.escape(k) if k in sensitive else "(?i:" + re.escape(k) + ")" for k in parts)
        + r")(?!\w)"
    )

    def replace(match):
        key = next(
            k for k in parts if (k == match[0] if k in sensitive else k.casefold() == match[0].casefold())
        )
        return mapping[key]

    def tree(value):
        if isinstance(value, str):
            return pattern.sub(replace, value)
        if isinstance(value, list):
            return [tree(x) for x in value]
        if isinstance(value, dict):
            out = {tree(k): tree(v) for k, v in value.items()}
            assert len(out) == len(value), "INVERSE_KEY_COLLISION"
            return out
        return value

    candidate = tree({k: v for k, v in packet.items() if k not in {"masking", "input_sha256"}})
    for row in private.get("identifier_replacements", []):
        target = candidate
        for key in row["path"][:-1]:
            target = target[key]
        key = row["path"][-1]
        assert target[key] in {row["original"], row["masked"]}, ("IDENTIFIER_NOT_REVERSIBLE", row["path"])
        target[key] = row["original"]
    for row in candidate["records"]:
        if row["collection"] != "ingest.source_items":
            continue
        original = original_rows.get(row["key"])
        if original is None:
            raise ValueError("No authentic retained original source wrapper for " + row["key"])
        proof = row.get("as_of_evidence", {})
        before = original["as_of_evidence"]
        original_sha = before["raw_sha256"]
        if original_sha not in private.get("original_raw_paths", {}):
            raise ValueError("Original source hash not in sealed private raw binding")
        for field in ["raw_ref", "raw_sha256", "body_sha256"]:
            proof[field] = before[field]
        for field in [
            "raw_ref",
            "raw_path",
            "raw_sha256",
            "content_hash",
            "detail_raw_ref",
            "detail_raw_sha256",
            "discovery_raw_ref",
            "discovery_raw_sha256",
        ]:
            if field in row["record"] and field in original["record"]:
                row["record"][field] = original["record"][field]
    candidate["input_sha256"] = digest(candidate)
    return candidate


def main():
    assert shutil.disk_usage(ROOT).free >= 8 * 1024**3
    OUT.mkdir(exist_ok=False)
    verified = []
    for line in (HERE / "manual-and-inverse-original-sha256.txt").read_text().splitlines():
        h, path = line.split(None, 1)
        p = ROOT / path
        assert hashlib.sha256(p.read_bytes()).hexdigest() == h
        verified.append({"path": path, "sha256": h, "bytes": p.stat().st_size})
    cases = {c["fixture_id"]: c for c in helper.read_json(HERE / "portfolio.candidate.json")["fixtures"]}
    results = []
    for fid, base_path in BASE.items():
        case = cases[fid]
        private = helper.read_json(case["private_name_map"]["path"])
        source_path = (
            private.get("prior_original_packet", {}).get("path") or "data/replay/inputs/" + fid + ".json"
        )
        unmasked = helper.read_json(source_path)
        originals = {
            row["key"]: row for row in unmasked["records"] if row["collection"] == "ingest.source_items"
        }
        attempts = []
        matched = None
        for label, path in [
            ("CURRENT_ACCEPTANCE_PACKET", case["acceptance_input"]["path"]),
            ("AUTHENTIC_ORIGINAL_MASKED_GENERATION", base_path),
        ]:
            packet = helper.read_json(path)
            try:
                candidate = inverse(packet, private, originals)
                actual = helper.stream_digest(candidate)
                exact = actual == private["source_sha256"]
                attempts.append(
                    {
                        "source": label,
                        "path": path,
                        "source_file_sha256": helper.sha(path),
                        "expected_canonical_parent_sha256": private["source_sha256"],
                        "actual_canonical_parent_sha256": actual,
                        "exact_match": exact,
                        "candidate_pins": {
                            k: candidate.get(k) for k in ["code_version", "config_version", "assembled_at"]
                        },
                    }
                )
                if exact:
                    matched = candidate
                    break
            except (ValueError, KeyError, TypeError, AssertionError) as error:
                attempts.append({"source": label, "path": path, "exact_match": False, "reason": str(error)})
            print(fid, label, attempts[-1].get("exact_match"), flush=True)
        row = {
            "fixture_id": fid,
            "attempts": attempts,
            "parent_recovered": matched is not None,
            "original_source_wrapper_authority": source_path,
            "no_expected_financial_fields_or_new_clocks_invented": True,
        }
        if matched is not None:
            path = OUT / (fid + "-original.json.zst")
            with path.open("xb") as dest:
                with zstandard.ZstdCompressor(level=6).stream_writer(dest, closefd=False) as writer:
                    for chunk in json.JSONEncoder(sort_keys=True, separators=(",", ":")).iterencode(matched):
                        writer.write(chunk.encode())
            reread = helper.read_json(path)
            assert helper.stream_digest(reread) == private["source_sha256"]
            path.chmod(0o600)
            row["recovered_path"] = str(path.relative_to(ROOT))
            row["recovered_file_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        results.append(row)
        del matched
    receipt = {
        "scope": "DETERMINISTIC_HASH_VERIFIED_PARENT_RECOVERY_ONLY",
        "cases": results,
        "native_admission_run": False,
        "source_fields_rewritten": False,
        "transfer_sha_verified": verified,
        "failed_inverse_candidates_not_admitted": True,
    }
    (OUT / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    OUT.chmod(0o700)
    print(json.dumps({"recovered": sum(x["parent_recovered"] for x in results), "cases": results}, indent=2))


if __name__ == "__main__":
    main()
