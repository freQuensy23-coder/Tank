"""Re-assemble masked §11 acceptance packets from retained unmasked parents (tank-acceptance).

    PYTHONPATH=/srv/tank/src /srv/tank/.venv/bin/python assemble_packets.py \
        --fixtures F-17,F-24 --generation r91 [--from-generation r90] \
        [--code-version X --config-version Y] [--add-replacements '{"F-17": {"tether": "issuer-t"}}'] [--replace]

Per fixture: start from <from>/<F>/private/{original-packet.json.zst,map.json}; refresh config sections
from the current /srv/tank/config snapshot (as Settings.load_config reads it); keep replay_messages and
the other fixture-owned config keys and the registry snapshot; pin code/config versions (resolve_versions
in this process, or explicit); mask with the exact-case transform used by fixture_reference admission;
fail on any word-bounded, case-insensitive source-side name/slug left in the masked envelope; re-seal the
predecessor's predicates unchanged against the new input; then run check_ready (temp manifest entry),
fixture_reference.validate and load_inputs with the target pins. Writes only under
data/replay/acceptance-<generation>/. Re-running with identical inputs only re-verifies.
"""
import argparse, copy, hashlib, io, json, re, shutil, sys, tempfile
from datetime import UTC, datetime
from pathlib import Path

import yaml
import zstandard

ROOT = Path("/srv/tank")
sys.path.insert(0, str(ROOT / "tests"))
from tank import fixture_reference as fr  # noqa: E402
from tank.replay import _market_record, digest, load_inputs  # noqa: E402
from tank.settings import Settings, resolve_versions  # noqa: E402
from tank.util import canonical_json  # noqa: E402
import fixture_acceptance as fa  # noqa: E402

SECTIONS = ("dependency_map", "openma", "semantics", "sources", "thresholds")
REPLAY = ROOT / "data/replay"
LOG_SCRATCH = Path("/srv/tank-work/logs/tank-acceptance/assemble-scratch")
FIXTURE_DIR = re.compile(r"acceptance-[A-Za-z0-9][A-Za-z0-9_.-]*/F-\d{2}")


class AssemblyError(Exception):
    pass


def safe_remove(path):
    """Delete only strictly inside one data/replay/acceptance-<generation>/F-xx/ dir or LOG_SCRATCH.

    Never the generation root, never a fixture dir itself, never anything reached through a symlink."""
    target = Path(path)
    resolved = target.resolve()
    if target.is_symlink():
        raise AssemblyError(f"refusing to delete symlink {target}")
    allowed = False
    if resolved.is_relative_to(LOG_SCRATCH.resolve()) and resolved != LOG_SCRATCH.resolve():
        allowed = True
    elif resolved.is_relative_to(REPLAY.resolve()):
        relative = resolved.relative_to(REPLAY.resolve()).as_posix()
        head = "/".join(relative.split("/")[:2])
        allowed = bool(FIXTURE_DIR.fullmatch(head)) and relative != head
    if not allowed:
        raise AssemblyError(f"refusing to delete outside a fixture output dir or own scratch: {resolved}")
    if resolved.is_dir():
        shutil.rmtree(resolved)
    elif resolved.exists():
        resolved.unlink()


def now():
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_bytes(path):
    data = Path(path).read_bytes()
    return zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)).read() if data[:4] == b"\x28\xb5\x2f\xfd" else data


def read_json(path):
    return json.loads(read_bytes(path))


def write_new(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(data)
    return sha(path)


def zjson(value):
    return zstandard.ZstdCompressor(level=12).compress((canonical_json(value) + "\n").encode())


def transform_pair(replacements, sensitive):
    """Exact-case substitution, identical to fixture_reference._host_identity_mask.transform."""

    def build(inverse):
        mapping = {v: k for k, v in replacements.items()} if inverse else dict(replacements)
        exact = {replacements[k] if inverse else k for k in sensitive}
        parts = sorted(mapping, key=len, reverse=True)
        pattern = re.compile(r"(?<!\w)(?:" + "|".join(
            re.escape(k) if k in exact else "(?i:" + re.escape(k) + ")" for k in parts) + r")(?!\w)")

        def text(value):
            return pattern.sub(lambda m: mapping[next(
                k for k in parts if (k == m[0] if k in exact else k.casefold() == m[0].casefold()))], value)

        def tree(value):
            if isinstance(value, str):
                return text(value)
            if isinstance(value, list):
                return [tree(v) for v in value]
            if isinstance(value, dict):
                out = {text(k): tree(v) for k, v in value.items()}
                if len(out) != len(value):
                    raise AssemblyError("identity substitution collides JSON keys")
                return out
            return value

        return text, tree

    return build(False), build(True)


def leak_scan(masked, replacements):
    base = {p for k in replacements for p in [k, *k.split(":")] if p}
    names = sorted(base | {n.replace(" ", sep) for n in base if " " in n for sep in ("-", "_")}, key=len, reverse=True)
    bounded = re.compile(r"(?<!\w)(?:" + "|".join(map(re.escape, names)) + r")(?!\w)", re.I)
    loose = re.compile("|".join(map(re.escape, names)), re.I)
    hits, warnings = {}, set()

    def walk(value, path):
        if isinstance(value, dict):
            for k, v in value.items():
                walk(k, path)
                walk(v, path + [k])
        elif isinstance(value, list):
            for v in value:
                walk(v, path)
        elif isinstance(value, str):
            where = "/".join(map(str, path[:2]))
            for m in bounded.finditer(value):
                hits.setdefault(f"{where}:{m[0]}", value[max(0, m.start() - 30):m.end() + 30])
            warnings.update(f"{where}:{m[0].casefold()}" for m in loose.finditer(value))

    walk({k: v for k, v in masked.items() if k != "masking"}, [])
    return hits, sorted(warnings)


def _capture(config_dir):
    from tank.settings import _CONFIG_SUFFIXES, _capture_tree
    return _capture_tree(Path(config_dir), _CONFIG_SUFFIXES)


def content_hash(record):
    return hashlib.sha256(" ".join(
        (record.get("title", "") + " " + record["text"] + " " + (record.get("quoted") or "")).split()).encode()).hexdigest()


def source_dir(generation, fixture):
    for name in (f"acceptance-{generation}", f"acceptance-{generation}-tank-acceptance"):
        if (REPLAY / name / fixture / "private/original-packet.json.zst").is_file():
            return REPLAY / name / fixture
    raise AssemblyError(f"{fixture}: no retained parent under acceptance-{generation}[-tank-acceptance]")


def resolved_settings(config_dir):
    """resolve_versions on one config dir; its config_versions record goes to own scratch, then removed."""
    LOG_SCRATCH.mkdir(parents=True, exist_ok=True)
    scratch = Path(tempfile.mkdtemp(prefix="settings-", dir=LOG_SCRATCH))
    try:
        return resolve_versions(Settings(_env_file=None, data_dir=scratch, config_dir=Path(config_dir)))
    finally:
        safe_remove(scratch)


def runtime_config(config_dir):
    settings = resolved_settings(config_dir)
    return {name: settings.load_config(name) for name in SECTIONS}, settings.code_version


def write_overlay(out, config_dir, masked_sections, masked_registry):
    """Replay config = deployed config with registry + sections replaced by their masked views.

    The fresh replay requires settings.load_config(registry/thresholds/semantics/dependency_map/openma)
    to equal the packet; the masked packet therefore needs this overlay as the replay process's /config."""
    overlay = out / "replay-config"
    shutil.copytree(config_dir, overlay, ignore=shutil.ignore_patterns("__pycache__"), symlinks=False)
    for name, value in {**masked_sections, "registry": masked_registry}.items():
        text = yaml.safe_dump(value, sort_keys=False, allow_unicode=True, width=1 << 20)
        if yaml.safe_load(text) != value:
            raise AssemblyError(f"masked {name}.yaml does not round-trip")
        (overlay / f"{name}.yaml").write_text(text)
    return overlay


def assemble(fixture, args, sections, code_version, out_root):
    src = source_dir(args.from_generation, fixture)
    out = out_root / fixture
    rel = out.relative_to(ROOT).as_posix()
    parent_path, map_path, seal_path = (src / "private/original-packet.json.zst", src / "private/map.json",
                                        src / "sealed/outcomes.json")
    added = args.add_replacements.get(fixture, {})
    build_key = digest({"parent": sha(parent_path), "map": sha(map_path), "seal": sha(seal_path), "added": added,
                        "code_version": code_version, "config_version": args.config_version,
                        "config": digest({k: v.hex() for k, v in _capture(args.config_dir).items()})})
    receipt_path = out / "private/assembly.json"
    if out.exists() and any(out.iterdir()):
        if receipt_path.is_file() and read_json(receipt_path).get("build_key") == build_key:
            return verify(fixture, rel, out, reused=True)
        if not args.replace:
            raise AssemblyError(f"{fixture}: {rel} exists with a different build key; pass --replace")
        for child in out.iterdir():
            safe_remove(child)

    parent = read_json(parent_path)
    old_map, old_seal = read_json(map_path), read_json(seal_path)
    if (parent.get("masking") or {}).get("applied"):
        raise AssemblyError(f"{fixture}: retained parent is masked")
    if digest(parent) != old_map["source_sha256"]:
        raise AssemblyError(f"{fixture}: retained parent does not match its private map")
    replacements = {**old_map["replacements"], **added}
    sensitive = list(dict.fromkeys([*old_map.get("case_sensitive_names", []), *added]))
    (fwd_text, fwd), (_, inv) = transform_pair(replacements, sensitive)
    overlay = write_overlay(out, args.config_dir, fwd(sections), fwd(parent["registry"]))
    config_version = args.config_version or resolved_settings(overlay).config_version
    parent = {k: v for k, v in parent.items() if k != "input_sha256"}
    parent["config"] = {**{k: v for k, v in parent["config"].items() if k not in SECTIONS}, **copy.deepcopy(sections)}
    parent.update(code_version=code_version, config_version=config_version, assembled_at=now())
    parent["input_sha256"] = digest(parent)
    masked = fwd({k: v for k, v in parent.items() if k != "input_sha256"})
    for key in ("config_version", "code_version", "cutoff"):
        masked[key] = parent[key]
    for key in ("fixture_reference", "assembled_at", "replay_mode"):
        if masked.get(key) != parent.get(key):
            raise AssemblyError(f"{fixture}: masking changed {key}")
    pinned = ("input_sha256", "config_version", "code_version")
    if inv({k: v for k, v in masked.items() if k not in pinned}) != {k: v for k, v in parent.items() if k not in pinned}:
        raise AssemblyError(f"{fixture}: masked envelope is not exactly reversible")
    hits, warnings = leak_scan(masked, replacements)
    if hits:
        raise AssemblyError(f"{fixture}: original identities remain in masked envelope: {dict(list(hits.items())[:8])}")

    # Masked raw derivatives of each source item; original raw stays at its retained host-only path.
    raws, files = {}, {}
    for prow, mrow in zip(parent["records"], masked["records"]):
        if prow["collection"] != "ingest.source_items":
            continue
        proof, mproof = prow["as_of_evidence"], mrow["as_of_evidence"]
        original = Path(old_map.get("original_raw_paths", {}).get(proof["raw_sha256"], proof["raw_ref"]))
        raw = read_bytes(original)
        if hashlib.sha256(raw).hexdigest() != proof["raw_sha256"]:
            raise AssemblyError(f"{fixture}: retained original raw checksum mismatch {original}")
        if proof["raw_sha256"] not in raws:
            signed = proof.get("derivation", {}).get("kind") == "SNAPSHOT_SIGNED_IPFS_V1"
            view = fwd(json.loads(raw)) if signed else None
            data = None
            if signed:  # keep byte-identical prior derivative when it is the same JSON view
                for prior in (src / "inputs/raw").glob("*.zst"):
                    candidate = read_bytes(prior)
                    try:
                        if json.loads(candidate) == view:
                            data = candidate
                            break
                    except ValueError:
                        continue
            data = data if data is not None else (canonical_json(view).encode() if signed else fwd_text(raw.decode()).encode())
            digest_ = hashlib.sha256(data).hexdigest()
            path = out / "inputs/raw" / f"{digest_}.{'json' if signed else 'txt'}.zst"
            raws[proof["raw_sha256"]] = (str(path), digest_, data)
        path, digest_, data = raws[proof["raw_sha256"]]
        files[path] = data
        record = mrow["record"]
        for prefix in ("", "detail_", "discovery_"):
            if prow["record"].get(prefix + "raw_sha256") == proof["raw_sha256"]:
                record[prefix + "raw_ref"], record[prefix + "raw_sha256"] = path, digest_
        if prow["record"].get("raw_path") == proof["raw_ref"]:
            record["raw_path"] = path
        if "content_hash" in prow["record"] and any(record.get(k) != prow["record"].get(k) for k in ("title", "text", "quoted")):
            record["content_hash"] = content_hash(record)
        mproof.update(raw_ref=path, raw_sha256=digest_, body_sha256=hashlib.sha256(record["text"].encode()).hexdigest())
        for ptool, mtool in zip(parent["tool_results"], masked["tool_results"]):
            if ptool.get("source_item_id") == prow["record"].get("item_id") and ptool.get("result") == prow["record"]:
                mtool["result"] = copy.deepcopy(record)

    private = {k: v for k, v in old_map.items() if k not in ("predecessor_private_map", "generation")}
    private.update(
        generation=args.generation, source_sha256=digest(parent), replacements=replacements,
        case_sensitive_names=sensitive,
        original_packet_path=str(out / "private/original-packet.json.zst"),
        original_registry_path=str(out / "private/original-registry.json"),
        predecessor_private_map={"path": map_path.relative_to(ROOT).as_posix(), "sha256": sha(map_path)})
    if added:
        private["added_exact_case_replacements"] = sorted(added)
    masked["masking"] = {**{k: v for k, v in (read_json(src / "inputs/input.json.zst").get("masking") or {}).items()},
                         "applied": True, "map_sha256": digest(private), "performance_evidence": False}
    masked["input_sha256"] = digest({k: v for k, v in masked.items() if k != "input_sha256"})

    spec = fa.specification_cases(ROOT)[fixture]["assertion_spec"]
    if old_seal["assertion_spec"] != spec:
        raise AssemblyError(f"{fixture}: predecessor seal differs from TDD §11.3")
    sealed = {
        "fixture_id": fixture, "input_sha256": digest(masked), "assertion_spec": spec, "sealed_at": now(),
        "assertions": old_seal["assertions"], "manual_predicates": old_seal.get("manual_predicates", []),
        **{k: old_seal[k] for k in ("automated_coverage", "source", "no_historical_performance_claim") if k in old_seal},
        "generation": args.generation,
        "predecessor_seal": {"path": seal_path.relative_to(ROOT).as_posix(), "sha256": sha(seal_path)},
    }
    write_new(out / "private/original-packet.json.zst", zjson(parent))
    write_new(out / "private/original-registry.json", Path(old_map["original_registry_path"]).read_bytes())
    for path, data in files.items():
        if not Path(path).exists():
            write_new(Path(path), zstandard.ZstdCompressor(level=12).compress(data))
    write_new(out / "private/map.json", (canonical_json(private) + "\n").encode())
    write_new(out / "inputs/input.json.zst", zjson(masked))
    write_new(out / "sealed/outcomes.json", (json.dumps(sealed, indent=1, ensure_ascii=False) + "\n").encode())
    write_new(receipt_path, (json.dumps({
        "build_key": build_key, "assembled_at": parent["assembled_at"], "source": src.relative_to(ROOT).as_posix(),
        "code_version": code_version, "config_version": config_version, "added_replacements": added,
        "config_dir": str(args.config_dir),
        "leak_scan_technical_context_warnings": warnings}, indent=1) + "\n").encode())
    return verify(fixture, rel, out, reused=False)


def verify(fixture, rel, out, reused):
    manifest = {c["fixture_id"]: c for c in json.loads((ROOT / "tests/fixtures/manifest.json").read_text())["fixtures"]}
    case = {k: v for k, v in manifest[fixture].items() if k not in ("acceptance_run", "acceptance_history", "execution_attempts")}
    for key, name in (("acceptance_input", "inputs/input.json.zst"), ("private_name_map", "private/map.json"),
                      ("sealed_outcomes", "sealed/outcomes.json")):
        case[key] = {"path": f"{rel}/{name}", "sha256": sha(out / name)}
    case.update(readiness="PREPARED_NATIVE_PENDING", missing=[])
    packet = read_json(out / "inputs/input.json.zst")
    private = read_json(out / "private/map.json")
    hits, _ = leak_scan(packet, private["replacements"])
    result = {"reused_existing_build": reused, "check_ready": fa.check_ready(ROOT, case), "leak_hits": sorted(hits)}
    try:
        fr.validate(packet, _market_record, private_mask_path=out / "private/map.json", input_path=out / "inputs/input.json.zst")
        result["fixture_reference_validate"] = "ADMITTED"
    except Exception as error:
        result["fixture_reference_validate"] = f"REJECTED {type(error).__name__}: {error}"
    scratch = Path(tempfile.mkdtemp(prefix=".verify-", dir=out))
    try:
        plain = scratch / "input.json"
        plain.write_text(canonical_json(packet))
        pins = type("Pins", (), {"code_version": packet["code_version"], "config_version": packet["config_version"],
                                 "replay_private_mask_path": out / "private/map.json"})
        load_inputs(plain, pins)
        result["load_inputs"] = "ADMITTED"
    except Exception as error:
        result["load_inputs"] = f"REJECTED {type(error).__name__}: {error}"
    finally:
        safe_remove(scratch)
    overlay = out / "replay-config"
    replay_settings = resolved_settings(overlay)
    mismatched = [name for name in ("thresholds", "semantics", "dependency_map", "openma")
                  if name in packet["config"] and replay_settings.load_config(name) != packet["config"][name]]
    if replay_settings.load_config("registry") != packet["registry"]:
        mismatched.append("registry")
    result["replay_config_matches_packet"] = not mismatched or mismatched
    result["replay_config_version_on_host"] = replay_settings.config_version
    result["pins"] = {"code_version": packet["code_version"], "config_version": packet["config_version"]}
    private_paths = sorted({str(Path(p).parent) for p in [private["original_packet_path"], *private.get("original_raw_paths", {}).values()]})
    result["container"] = {
        "TANK_CONFIG_PATH": str(overlay),
        "TANK_REPLAY_PRIVATE_MASK_PATH": str(out / "private/map.json"),
        "packet": f"zstd -dc {out / 'inputs/input.json.zst'} > <tank-data volume>/replay/inputs/{fixture}.json",
        "bind_same_path_read_only": [p for p in sorted({str(out), *private_paths})
                                     if not any(p != q and Path(p).is_relative_to(q) for q in {str(out), *private_paths})],
    }
    result["sealed_input_sha256"] = digest(packet)
    result["manifest_entry"] = {k: case[k] for k in ("acceptance_input", "private_name_map", "sealed_outcomes", "readiness", "missing")}
    result["ready"] = (not result["check_ready"] and not hits and result["fixture_reference_validate"] == "ADMITTED"
                       and result["load_inputs"] == "ADMITTED" and result["replay_config_matches_packet"] is True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--fixtures", required=True)
    parser.add_argument("--generation", required=True)
    parser.add_argument("--from-generation", default="r90")
    parser.add_argument("--config-dir", type=Path, default=ROOT / "config",
                        help="deployed config export (e.g. docker compose exec -T tank tar -C /config -cf - . | tar -xf - -C DIR)")
    parser.add_argument("--code-version", help="deployed process code_version (default: this process)")
    parser.add_argument("--config-version", help="override; normally computed per fixture over its masked replay-config overlay")
    parser.add_argument("--add-replacements", type=json.loads, default={},
                        help='JSON {"F-17": {"tether": "issuer-t"}}: extra exact-case registry slugs')
    parser.add_argument("--replace", action="store_true", help="rebuild a fixture dir with a different build key")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", args.generation):
        parser.error("invalid generation name")
    out_root = REPLAY / f"acceptance-{args.generation}"
    out_root.mkdir(parents=True, exist_ok=True)
    sections, own_code_version = runtime_config(args.config_dir)
    code_version = args.code_version or own_code_version
    results, failed = {}, False
    for fixture in [f.strip() for f in args.fixtures.split(",") if f.strip()]:
        try:
            results[fixture] = assemble(fixture, args, sections, code_version, out_root)
            failed |= not results[fixture]["ready"]
        except AssemblyError as error:
            results[fixture] = {"ready": False, "error": str(error)}
            failed = True
    print(json.dumps(results, indent=1))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
