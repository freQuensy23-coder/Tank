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

import argparse
import copy
import hashlib
import io
import json
import re
import shutil
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import yaml
import zstandard

ROOT = Path("/srv/tank")
sys.path.insert(0, str(ROOT / "tests"))
import fixture_acceptance as fa

from tank import fixture_reference as fr
from tank.replay import _market_record, digest, load_inputs
from tank.settings import Settings, resolve_versions
from tank.util import canonical_json

SECTIONS = ("dependency_map", "openma", "semantics", "sources", "thresholds")
REPLAY = ROOT / "data/replay"
LOG_SCRATCH = Path("/srv/tank-work/logs/tank-acceptance/assemble-scratch")
FIXTURE_DIR = re.compile(r"acceptance-[A-Za-z0-9][A-Za-z0-9_.-]*/F-\d{2}")


class AssemblyError(Exception):
    pass


def safe_remove(path) -> None:
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
    return (
        zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)).read()
        if data[:4] == b"\x28\xb5\x2f\xfd"
        else data
    )


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
        if not replacements:
            return (lambda value: value), (lambda value: copy.deepcopy(value))
        mapping = {v: k for k, v in replacements.items()} if inverse else dict(replacements)
        exact = {replacements[k] if inverse else k for k in sensitive}
        parts = sorted(mapping, key=len, reverse=True)
        pattern = re.compile(
            r"(?<!\w)(?:"
            + "|".join(re.escape(k) if k in exact else "(?i:" + re.escape(k) + ")" for k in parts)
            + r")(?!\w)"
        )

        def text(value):
            return pattern.sub(
                lambda m: mapping[
                    next(k for k in parts if (k == m[0] if k in exact else k.casefold() == m[0].casefold()))
                ],
                value,
            )

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


TOKEN_ID = re.compile(r"[a-z0-9][a-z0-9._-]*:[A-Za-z0-9._-]+")


def leak_scan(masked, replacements, venues=frozenset(), exceptions=None):
    """Word-bounded, case-insensitive scan for source-side names/slugs (fails), with two exemptions
    reported as warnings: lowercase technical venue ids (first segment of registry instrument ids,
    preserved by every map's declared scope) and unbounded substrings (e.g. 'aavedao.eth')."""
    base = set()
    for key in replacements:
        base.add(key)
        if TOKEN_ID.fullmatch(key):
            base.update(key.split(":"))
    if not base:
        return {}, []
    names = sorted(
        base | {n.replace(" ", sep) for n in base if " " in n for sep in ("-", "_")}, key=len, reverse=True
    )
    bounded = re.compile(r"(?<!\w)(?:" + "|".join(map(re.escape, names)) + r")(?!\w)", re.I)
    loose = re.compile("|".join(map(re.escape, names)), re.I)
    hits, warnings = {}, set()

    def walk(value, path) -> None:
        if isinstance(value, dict):
            for k, v in value.items():
                walk(k, path)
                walk(v, path + [k])
        elif isinstance(value, list):
            for v in value:
                walk(v, path)
        elif isinstance(value, str):
            where = "/".join(map(str, path[:2]))
            spans = set()
            allowed = []
            if exceptions and path and path[-1] in exceptions["fields"]:
                allowed = [
                    x.span()
                    for tok in [*exceptions["tokens"], *exceptions.get("masked_forms", [])]
                    for x in re.finditer(re.escape(tok), value)
                ]
            for m in bounded.finditer(value):
                spans.add(m.span())
                if any(a <= m.start() and m.end() <= b for a, b in allowed):
                    warnings.add(f"{where}:{m[0]} (declared technical-context exception)")
                elif m[0] in venues:
                    warnings.add(f"{where}:{m[0]} (technical venue id)")
                else:
                    hits.setdefault(f"{where}:{m[0]}", value[max(0, m.start() - 30) : m.end() + 30])
            warnings.update(
                f"{where}:{m[0].casefold()}" for m in loose.finditer(value) if m.span() not in spans
            )

    walk({k: v for k, v in masked.items() if k != "masking"}, [])
    return hits, sorted(warnings)[:200]


def registry_venues(registry):
    return frozenset(
        str(i.get("instrument_id", "")).split(":")[0]
        for i in registry.get("instruments", [])
        if isinstance(i, dict) and ":" in str(i.get("instrument_id", ""))
    )


def _capture(config_dir):
    from tank.settings import _CONFIG_SUFFIXES, _capture_tree

    return _capture_tree(Path(config_dir), _CONFIG_SUFFIXES)


def content_hash(record):
    return hashlib.sha256(
        " ".join(
            (record.get("title", "") + " " + record["text"] + " " + (record.get("quoted") or "")).split()
        ).encode()
    ).hexdigest()


ASSEMBLER_REVISION = 13
SLUG_FIELDS = {"entity_id", "name"}
HOST_MASK_FIXTURES = {"F-24", "F-25", "F-26", "F-27"}


def project(value):
    """Evaluator-only /data/... paths -> host repository paths (the runtime resolves literal paths)."""
    text = str(value)
    return str(ROOT / text.lstrip("/")) if text.startswith("/data/") else text


def resolve_path(value):
    path = Path(project(value))
    return path if path.is_absolute() else ROOT / path


def manifest_cases():
    return {
        c["fixture_id"]: c
        for c in json.loads((ROOT / "tests/fixtures/manifest.json").read_text())["fixtures"]
    }


def source_spec(fixture, args):
    """Retained unmasked parent + private map + predecessor seal + prior masked input for one fixture."""
    if args.from_manifest:
        case = manifest_cases()[fixture]
        refs = {}
        for key in ("private_name_map", "sealed_outcomes", "acceptance_input"):
            ref = case.get(key) or {}
            path = resolve_path(ref.get("path", ""))
            if not ref.get("path") or not path.is_file() or sha(path) != ref.get("sha256"):
                raise AssemblyError(f"{fixture}: manifest {key} missing or hash mismatch")
            refs[key] = path
        if fixture in args.map_override:
            refs["private_name_map"] = resolve_path(args.map_override[fixture])
        old_map = read_json(refs["private_name_map"])
        spec = {
            "map": refs["private_name_map"],
            "seal": refs["sealed_outcomes"],
            "prior_input": refs["acceptance_input"],
            "parent": resolve_path(old_map["original_packet_path"]),
            "source": "manifest",
        }
    else:
        src = None
        for name in (
            f"acceptance-{args.from_generation}",
            f"acceptance-{args.from_generation}-tank-acceptance",
        ):
            if (REPLAY / name / fixture / "private/original-packet.json.zst").is_file():
                src = REPLAY / name / fixture
                break
        if src is None:
            raise AssemblyError(
                f"{fixture}: no retained parent under acceptance-{args.from_generation}[-tank-acceptance]"
            )
        old_map = read_json(src / "private/map.json")
        spec = {
            "map": src / "private/map.json",
            "seal": src / "sealed/outcomes.json",
            "prior_input": src / "inputs/input.json.zst",
            "parent": src / "private/original-packet.json.zst",
            "source": src.relative_to(ROOT).as_posix(),
        }
    if not spec["parent"].is_file():
        raise AssemblyError(f"{fixture}: retained unmasked parent missing: {spec['parent']}")
    spec["old_map"] = old_map
    registry = old_map.get("original_registry_path")
    spec["registry"] = resolve_path(registry) if registry else None
    return spec


def apply_slugs(parent_value, masked_value, slugs, fwd_text) -> None:
    """Declared identifier slugs (identity fields holding exactly the slug) -> declared masked slug."""
    if isinstance(parent_value, dict):
        for key, value in parent_value.items():
            mkey = fwd_text(key)
            if key in SLUG_FIELDS and isinstance(value, str) and value in slugs:
                masked_value[mkey] = slugs[value]
            elif key == "names" and isinstance(value, list):
                for i, name in enumerate(value):
                    if isinstance(name, str) and name in slugs:
                        masked_value[mkey][i] = slugs[name]
                    else:
                        apply_slugs(name, masked_value[mkey][i], slugs, fwd_text)
            else:
                apply_slugs(value, masked_value[mkey], slugs, fwd_text)
    elif isinstance(parent_value, list):
        for a, b in zip(parent_value, masked_value, strict=False):
            apply_slugs(a, b, slugs, fwd_text)


def mask_bytes(raw, fwd, fwd_text):
    """Identity-only derivative of retained bytes: JSON string tokens when JSON, else UTF-8 text."""
    try:
        return canonical_json(fwd(json.loads(raw))).encode(), "json"
    except (ValueError, UnicodeDecodeError):
        return fwd_text(raw.decode("utf-8")).encode(), "txt"


def _norm(value):
    return re.sub(r"[-_\s]+", " ", str(value).casefold()).strip()


def _pipeline_text(packet):
    parts = []
    for row in packet.get("records", []):
        if row.get("collection") == "ingest.source_items":
            parts += [str(row["record"].get(k) or "") for k in ("title", "text", "body", "author", "quoted")]
    parts += [json.dumps(t.get("result"), ensure_ascii=False) for t in packet.get("tool_results", [])]
    parts.append(json.dumps((packet.get("config") or {}).get("replay_messages"), ensure_ascii=False))
    return "\n".join(parts)


def entity_families(registry, replacements, sensitive, parent_text, host_mask, venues=frozenset()):
    """One real entity -> one placeholder family (coordinator rule after the r119 F-08/F-06 findings).

    The display name (primary) keeps or receives a root placeholder; explicit keys of names prefixed by
    the primary are dropped so the primary's word replacement yields "<root> Network"; lowercase/slug
    variants of the primary follow the root (non-host-mask only: host admission needs casefold-unique
    aliases); token symbol keys that would swallow a differently-cased display name become exact-case;
    registry aliases of the entity left without any key get "<root><sep>AliasN". Registry names equal to
    the primary are masked to the root by registry_family_pass. Token role: a symbol matches exactly,
    a token_id or exchange ticker case-insensitively."""
    sep = "_" if host_mask else " "
    repl, sens = dict(replacements), list(dict.fromkeys(sensitive))
    symbols, token_forms = set(), set()
    for token in registry.get("tokens", []):
        sym, tid = token.get("symbol"), token.get("token_id")
        if sym:
            symbols.add(str(sym))
            token_forms.update(
                f"{sym}{x}".casefold() for x in ("USDT", "-USD", "/USD", "-USDT", "-USDT-SWAP")
            )
        if tid:
            token_forms.add(str(tid).casefold())

    def token_role(key):
        return key in symbols or key.casefold() in token_forms

    used_numbers = {int(m.group(1)) for v in repl.values() for m in [re.fullmatch(r"Name(\d+)", v)] if m}
    families, log = [], []

    def new_root():
        n = 1
        while n in used_numbers:
            n += 1
        used_numbers.add(n)
        return f"Name{n:02d}"

    for index, entity in enumerate(registry.get("entities", [])):
        ids = [
            x
            for x in [
                entity.get("entity_id"),
                entity.get("name"),
                *(entity.get("names") or []),
                *(entity.get("aliases") or []),
            ]
            if isinstance(x, str) and x
        ]
        idn = {_norm(x) for x in ids}
        ekeys = [
            k
            for k in repl
            if not token_role(k) and (_norm(k) in idn or any(_norm(k).startswith(n + " ") for n in idn))
        ]
        forms = sorted(
            {
                m.group(0)
                for x in ids
                for m in re.finditer(r"(?<!\w)" + re.escape(x) + r"(?!\w)", parent_text, re.I)
                if not m.group(0).islower() and not token_role(m.group(0))
            },
            key=len,
        )
        swallowed = [
            k
            for k in repl
            if k in symbols and k not in sens and any(f.casefold() == k.casefold() and f != k for f in forms)
        ]
        if not ekeys and not swallowed:
            continue
        canonical = _norm((entity.get("names") or [entity.get("entity_id")])[0])
        candidates = [k for k in ekeys if not k.islower()] + [f for f in forms if f not in repl]
        primary = next((c for c in sorted(candidates, key=len) if _norm(c) == canonical), None) or (
            sorted(candidates, key=len)[0] if candidates else None
        )
        if primary is None:
            continue
        changes = {}

        def put(key, value) -> None:
            old = repl.get(key)
            if value is None:
                if key in repl:
                    del repl[key]
                    changes[key] = [old, None]
                    if key in sens:
                        sens.remove(key)
            elif old != value:
                repl[key] = value
                changes[key] = [old, value]

        newly_added = primary not in repl
        if not newly_added:
            root = repl[primary]
        else:
            prefixed = sorted((k for k in ekeys if k.startswith(primary + " ")), key=len)
            root = repl[prefixed[0]] if prefixed else new_root()
            put(primary, root)
        if primary not in sens and (
            newly_added or any(k != primary and k.casefold() == primary.casefold() for k in repl)
        ):
            sens.append(primary)
        for k in swallowed:
            sens.append(k)
        if swallowed and not host_mask:
            low = primary.lower()
            if low not in repl and re.search(
                r"(?<!\w)" + re.escape(low) + r"(?!\w)",
                parent_text + json.dumps(registry, ensure_ascii=False),
            ):
                put(low, root.lower())
                sens.append(low)
        for k in [
            k for k in list(repl) if k != primary and k.startswith(primary + " ") and not token_role(k)
        ]:
            put(k, None)
        if not host_mask:
            taken = set(repl.values())
            for k in [
                k
                for k in list(repl)
                if k.islower()
                and not token_role(k)
                and (_norm(k) == _norm(primary) or _norm(k).startswith(_norm(primary) + " "))
            ]:
                rest = _norm(k)[len(_norm(primary)) :].strip()
                value = root.lower() + ("-" + rest.replace(" ", "-") if rest else "")
                if value != repl[k] and value not in taken:
                    taken.discard(repl[k])
                    put(k, value)
                    taken.add(value)
        (fwd_text, _), _ = transform_pair(repl, sens)
        alias_n = 0
        for alias in [*(entity.get("aliases") or []), *(entity.get("names") or [])]:
            if (
                not isinstance(alias, str)
                or _norm(alias) == _norm(primary)
                or alias in venues
                or alias.islower()
                or fwd_text(alias) != alias
                or token_role(alias)
            ):
                continue
            alias_n += 1
            value = f"{root}{sep}Alias{alias_n}"
            while value in repl.values():
                alias_n += 1
                value = f"{root}{sep}Alias{alias_n}"
            put(alias, value)
            sens.append(alias)
            (fwd_text, _), _ = transform_pair(repl, sens)
        sens = [k for k in dict.fromkeys(sens) if k in repl]
        families.append(
            {"index": index, "entity_id": entity.get("entity_id"), "primary": primary, "root": root}
        )
        if changes or swallowed:
            log.append(
                {
                    "entity_id": entity.get("entity_id"),
                    "primary": primary,
                    "root": root,
                    "key_changes": changes,
                    "made_exact_case": swallowed,
                }
            )
    return repl, sens, families, log


def registry_family_pass(parent_registry, masked_registry, families) -> None:
    """Registry names/aliases equal to an entity's primary name carry its root placeholder (not the slug)."""
    pents, ments = parent_registry.get("entities", []), masked_registry.get("entities", [])
    for fam in families:
        pe, me = pents[fam["index"]], ments[fam["index"]]
        for field in ("names", "aliases"):
            for j, value in enumerate(pe.get(field) or []):
                if isinstance(value, str) and _norm(value) == _norm(fam["primary"]):
                    me[field][j] = fam["root"]


def family_gate(parent_registry, masked, families, replacements, venues=frozenset()):
    """Fail when an entity placeholder used in pipeline-visible text is not among that entity's masked
    registry name/names/aliases, or an original alias of a mapped entity survives in the masked registry."""
    errors, text = [], _pipeline_text(masked)
    ments, pents = masked["registry"].get("entities", []), parent_registry.get("entities", [])
    symbols = {str(t["symbol"]) for t in parent_registry.get("tokens", []) if t.get("symbol")}
    token_forms = {
        str(t["token_id"]).casefold() for t in parent_registry.get("tokens", []) if t.get("token_id")
    }
    token_forms |= {
        f"{x}{y}".casefold() for x in symbols for y in ("USDT", "-USD", "/USD", "-USDT", "-USDT-SWAP")
    }
    for fam in families:
        me, pe = ments[fam["index"]], pents[fam["index"]]
        known = {
            str(v).casefold()
            for v in [me.get("name"), *(me.get("names") or []), *(me.get("aliases") or [])]
            if v
        }
        known |= {str(r.get("alias", "")).casefold() for r in me.get("qualified_aliases") or []}
        ids = {
            _norm(x)
            for x in [
                pe.get("entity_id"),
                pe.get("name"),
                *(pe.get("names") or []),
                *(pe.get("aliases") or []),
            ]
            if x
        }
        keys = [
            k
            for k in replacements
            if (k not in symbols and k.casefold() not in token_forms)
            and (_norm(k) in ids or any(_norm(k).startswith(n + " ") for n in ids))
        ]
        for k in keys:
            value = replacements[k]
            if value.islower() and re.search(r"[-_]", value):
                continue
            if re.search(r"(?<!\w)" + re.escape(value) + r"(?!\w)", text) and value.casefold() not in known:
                errors.append(
                    f"{fam['entity_id']}: placeholder {value!r} (for {k!r}) is used in pipeline text but not in the masked registry entity"
                )
        for field in ("names", "aliases"):
            for orig, now in zip(pe.get(field) or [], me.get(field) or [], strict=False):
                if (
                    isinstance(orig, str)
                    and orig == now
                    and not orig.islower()
                    and orig not in venues
                    and len(orig) > 1
                ):
                    errors.append(f"{fam['entity_id']}: registry {field} {orig!r} left unmasked")
    return errors


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


def write_overlay(
    out, config_dir, masked_sections, masked_registry, mask_tree, replacements, venues, exceptions=None
):
    """Replay config = deployed config with registry + sections replaced by their masked views.

    The fresh replay requires settings.load_config(registry/thresholds/semantics/dependency_map/openma)
    to equal the packet; the masked packet therefore needs this overlay as the replay process's /config."""
    overlay = out / "replay-config"
    shutil.copytree(config_dir, overlay, ignore=shutil.ignore_patterns("__pycache__"), symlinks=False)
    for name, value in {**masked_sections, "registry": masked_registry}.items():
        if yaml.safe_load((overlay / f"{name}.yaml").read_bytes()) == value:
            continue
        text = yaml.safe_dump(value, sort_keys=False, allow_unicode=True, width=1 << 20)
        if yaml.safe_load(text) != value:
            raise AssemblyError(f"masked {name}.yaml does not round-trip")
        (overlay / f"{name}.yaml").write_text(text)
    fundamentals = overlay / "fundamentals_evidence.json"
    if fundamentals.is_file():
        masked = mask_tree(json.loads(fundamentals.read_text()))
        hits, _ = leak_scan(masked, replacements, venues, exceptions)
        if hits:
            raise AssemblyError(
                f"fundamentals_evidence.json keeps original identities: {dict(list(hits.items())[:6])}"
            )
        fundamentals.write_text(json.dumps(masked, indent=1, ensure_ascii=False) + "\n")
    return overlay


def assemble(fixture, args, sections, code_version, out_root):
    spec = source_spec(fixture, args)
    out = out_root / fixture
    rel = out.relative_to(ROOT).as_posix()
    old_map = spec["old_map"]
    added = args.add_replacements.get(fixture, {})
    build_key = digest(
        {
            "rev": ASSEMBLER_REVISION,
            "source": spec["source"],
            "parent": sha(spec["parent"]),
            "inverse_snapshots": fixture in args.inverse_snapshots,
            "note": args.note.get(fixture),
            "map_override": args.map_override.get(fixture),
            "replay_timeout": [args.replay_timeout_base, args.replay_timeout_per_message],
            "exceptions": args.scan_exceptions.get(fixture),
            "map": sha(spec["map"]),
            "seal": sha(spec["seal"]),
            "added": added,
            "code_version": code_version,
            "config_version": args.config_version,
            "config": digest({k: v.hex() for k, v in _capture(args.config_dir).items()}),
            "module": sha(fr.__file__),
        }
    )
    receipt_path = out / "private/assembly.json"
    if out.exists() and any(out.iterdir()):
        if receipt_path.is_file() and read_json(receipt_path).get("build_key") == build_key:
            return verify(fixture, rel, out, reused=True)
        if not args.replace:
            raise AssemblyError(f"{fixture}: {rel} exists with a different build key; pass --replace")
        for child in out.iterdir():
            safe_remove(child)

    parent = read_json(spec["parent"])
    old_seal = read_json(spec["seal"])
    prior = read_json(spec["prior_input"])
    if (parent.get("masking") or {}).get("applied"):
        raise AssemblyError(f"{fixture}: retained parent is masked")
    if digest(parent) != old_map["source_sha256"]:
        raise AssemblyError(
            f"{fixture}: retained unmasked parent missing (file {spec['parent']} does not match map source_sha256)"
        )
    registry_bytes = (
        spec["registry"].read_bytes()
        if spec["registry"]
        else (canonical_json(parent["registry"]) + "\n").encode()
    )
    registry_value = (
        yaml.safe_load(registry_bytes)
        if spec["registry"] and spec["registry"].suffix in {".yaml", ".yml"}
        else json.loads(registry_bytes)
    )
    if registry_value != parent["registry"]:
        raise AssemblyError(f"{fixture}: retained registry differs from the parent registry")
    module_sha = sha(fr.__file__)

    raw_paths = {h: project(p) for h, p in (old_map.get("original_raw_paths") or {}).items()}
    recovered = {}
    recovery_log = []
    staged = set()

    for raw_sha, raw_path in list(raw_paths.items()):
        if Path(raw_path).is_file():
            data = read_bytes(raw_path)
            if hashlib.sha256(data).hexdigest() != raw_sha:
                raise AssemblyError(f"{fixture}: retained original raw checksum mismatch {raw_path}")
            copy_path = str(out / "private/raw" / f"{raw_sha}.zst")
            recovered[copy_path] = data
            staged.add(copy_path)
            raw_paths[raw_sha] = copy_path
    (old_fwd_text, _), (old_inv_text, _) = transform_pair(
        old_map.get("replacements", {}), old_map.get("case_sensitive_names", [])
    )
    prior_rows = {r["key"]: r for r in prior["records"] if r["collection"] == "ingest.source_items"}

    def recover_by_inverse(want_sha, masked_ref, label):
        """Authentic original only if the inverse hashes to the recorded sha256 AND re-masks byte-exactly."""
        masked_path = resolve_path(masked_ref)
        if not masked_path.is_file():
            raise AssemblyError(
                f"{fixture}: {label} missing and its masked derivative is missing too: {masked_ref}"
            )
        masked_bytes = read_bytes(masked_path)
        try:
            candidate = old_inv_text(masked_bytes.decode("utf-8")).encode()
        except UnicodeDecodeError:
            raise AssemblyError(f"{fixture}: {label} missing; masked derivative is not UTF-8, no inverse")
        if (
            hashlib.sha256(candidate).hexdigest() != want_sha
            or old_fwd_text(candidate.decode()).encode() != masked_bytes
        ):
            raise AssemblyError(
                f"{fixture}: {label} missing; inverse masking of {masked_ref} does not hash to {want_sha} "
                "or does not re-mask byte-exactly (nothing written)"
            )
        path = str(out / "private/recovered" / f"{want_sha}.zst")
        recovered[path] = candidate
        recovery_log.append(
            {
                "sha256": want_sha,
                "label": label,
                "from_masked": str(masked_path),
                "status": "recovered by inverse masking, sha256-verified",
            }
        )
        return path

    for row in parent["records"]:
        if row["collection"] != "ingest.source_items":
            continue
        record, proof = row["record"], row["as_of_evidence"]
        before = copy.deepcopy(record)
        for key in ("raw_ref", "raw_path", "detail_raw_ref", "discovery_raw_ref"):
            if isinstance(record.get(key), str):
                record[key] = project(record[key])
        proof["raw_ref"] = project(proof["raw_ref"])
        present = raw_paths.get(proof["raw_sha256"], proof["raw_ref"])
        if not (present in recovered or Path(present).is_file()):
            text = record.get("text", "").encode()
            if (
                proof.get("derivation", {}).get("kind") == "DECLARED_UTF8_SOURCE_V1"
                and hashlib.sha256(text).hexdigest() == proof["raw_sha256"]
            ):
                path = str(out / "private/raw" / f"{proof['raw_sha256']}.txt.zst")
                recovered[path] = text
                for key in ("raw_ref", "raw_path"):
                    if record.get(key) == proof["raw_ref"]:
                        record[key] = path
                raw_paths.pop(proof["raw_sha256"], None)
                proof["raw_ref"] = path
            else:
                prior_proof = (prior_rows.get(row["key"]) or {}).get("as_of_evidence") or {}
                path = recover_by_inverse(
                    proof["raw_sha256"],
                    prior_proof.get("raw_ref", ""),
                    f"unmasked raw view {proof['raw_sha256'][:12]}",
                )
                for key in ("raw_ref", "raw_path"):
                    if record.get(key) == proof["raw_ref"]:
                        record[key] = path
                raw_paths.pop(proof["raw_sha256"], None)
                proof["raw_ref"] = path
        projection = proof.get("projection_provenance")
        if projection:
            projection["original_raw_ref"] = project(projection["original_raw_ref"])
            if not Path(projection["original_raw_ref"]).is_file():
                prior_projection = ((prior_rows.get(row["key"]) or {}).get("as_of_evidence") or {}).get(
                    "projection_provenance"
                ) or {}
                projection["original_raw_ref"] = recover_by_inverse(
                    projection["original_raw_sha256"],
                    prior_projection.get("original_raw_ref", ""),
                    f"host-only projection original {projection['original_raw_sha256'][:12]}",
                )
            original = (
                recovered[projection["original_raw_ref"]]
                if projection["original_raw_ref"] in recovered
                else read_bytes(projection["original_raw_ref"])
            )
            descriptor = proof["derivation"]
            if hashlib.sha256(original).hexdigest() != projection[
                "original_raw_sha256"
            ] or fr.project_original_source(descriptor["source_kind"], original, descriptor["selector"]) != (
                recovered[proof["raw_ref"]] if proof["raw_ref"] in recovered else read_bytes(proof["raw_ref"])
            ):
                raise AssemblyError(
                    f"{fixture}: retained host-only projection no longer reproduces its raw view"
                )
            projection["projection_module_sha256"] = module_sha
        for tool in parent["tool_results"]:
            if tool.get("result") == before:
                tool["result"] = copy.deepcopy(record)

    replacements = {**old_map.get("replacements", {}), **added}
    sensitive = list(dict.fromkeys([*old_map.get("case_sensitive_names", []), *added]))
    slugs = {r["original"]: r["masked"] for r in old_map.get("identifier_replacements", [])}
    (fwd_text, fwd0), (_, inv) = transform_pair(replacements, sensitive)

    exceptions = args.scan_exceptions.get(fixture)
    if exceptions:
        exceptions = {
            **exceptions,
            "masked_forms": sorted({fwd_text(t) for t in exceptions["tokens"]} - set(exceptions["tokens"])),
        }

    def fwd(value):
        masked_value = fwd0(value)
        if slugs:
            apply_slugs(value, masked_value, slugs, fwd_text)
        return masked_value

    if fixture in args.inverse_snapshots:
        inverse_slugs = {v: k for k, v in slugs.items()}

        def restore(value):
            value = copy.deepcopy(value)

            def walk(node) -> None:
                if isinstance(node, dict):
                    for key, entry in node.items():
                        if key in SLUG_FIELDS and isinstance(entry, str) and entry in inverse_slugs:
                            node[key] = inverse_slugs[entry]
                        elif key == "names" and isinstance(entry, list):
                            node[key] = [inverse_slugs.get(n, n) if isinstance(n, str) else n for n in entry]
                        else:
                            walk(entry)
                elif isinstance(node, list):
                    for entry in node:
                        walk(entry)

            walk(value)
            return inv(value)

        prior_snaps = [r for r in prior["records"] if r["collection"] == "replay.market_snapshots"]
        own = [i for i, r in enumerate(parent["records"]) if r["collection"] == "replay.market_snapshots"]
        if len(prior_snaps) != len(own):
            raise AssemblyError(f"{fixture}: passing input and parent hold different snapshot counts")
        supplement_fields = set(args.supplement_fields)
        ambiguous_aliases = {v for k, v in replacements.items() if k not in sensitive}
        inverse_report = []
        for index, masked_row in zip(own, prior_snaps, strict=False):
            restored = restore(masked_row)
            mine = parent["records"][index]

            for key, value in mine["record"].items():
                if key in masked_row["record"] and canonical_json(fwd(value)) == canonical_json(
                    masked_row["record"][key]
                ):
                    restored["record"][key] = copy.deepcopy(value)
            ambiguous = sum(
                len(
                    re.findall(
                        r"(?<!\w)" + re.escape(alias) + r"(?!\w)", canonical_json(masked_row["record"].get(k))
                    )
                )
                for k in supplement_fields
                for alias in ambiguous_aliases
                if k in masked_row["record"]
            )
            inverse_report.append(
                {"key": masked_row["key"], "case_ambiguous_alias_occurrences_in_supplement": ambiguous}
            )
            if canonical_json(fwd(restored)) != canonical_json(masked_row):
                raise AssemblyError(
                    f"{fixture}: inverse snapshot {masked_row['key']} does not re-mask to the passing snapshot"
                )
            drift = sorted(
                k
                for k in set(mine["record"]) | set(restored["record"])
                if k not in supplement_fields and mine["record"].get(k) != restored["record"].get(k)
            )
            drift += sorted(
                f"row.{k}"
                for k in set(mine) | set(restored)
                if k not in ("record", "available_at", "simulated_available_at")
                and mine.get(k) != restored.get(k)
            )
            if drift:
                raise AssemblyError(
                    f"{fixture}: inverse snapshot disagrees with the parent snapshot outside the supplement: {drift[:8]}"
                )
            parent["records"][index] = restored

    venues = registry_venues(parent["registry"])
    host_mask_family = fixture in HOST_MASK_FIXTURES or any(
        (r.get("as_of_evidence") or {}).get("derivation", {}).get("kind") == "SNAPSHOT_SIGNED_IPFS_V1"
        for r in parent["records"]
        if r["collection"] == "ingest.source_items"
    )
    replacements, sensitive, families, family_log = entity_families(
        parent["registry"], replacements, sensitive, _pipeline_text(parent), host_mask_family, venues
    )
    (fwd_text, fwd0), (_, inv) = transform_pair(replacements, sensitive)
    if exceptions:
        exceptions = {
            **exceptions,
            "masked_forms": sorted({fwd_text(t) for t in exceptions["tokens"]} - set(exceptions["tokens"])),
        }

    def fwd(value):
        masked_value = fwd0(value)
        if slugs:
            apply_slugs(value, masked_value, slugs, fwd_text)
        if isinstance(value, dict):
            if isinstance(value.get("entities"), list):
                registry_family_pass(value, masked_value, families)
            elif isinstance(value.get("registry"), dict):
                registry_family_pass(value["registry"], masked_value["registry"], families)
        return masked_value

    scan_names = {**replacements, **{k: v for k, v in slugs.items()}}
    slug_text = (
        re.compile(
            r"(?<!\w)(?:" + "|".join(map(re.escape, sorted(slugs, key=len, reverse=True))) + r")(?!\w)"
        )
        if slugs
        else None
    )

    def fund_tree(value):
        """Fundamentals are tool-returned protocol data: also mask declared protocol slugs by word."""
        masked_value = fwd(value)
        if not slug_text:
            return masked_value

        def sub(node):
            if isinstance(node, str):
                return slug_text.sub(lambda m: slugs[m[0]], node)
            if isinstance(node, list):
                return [sub(v) for v in node]
            if isinstance(node, dict):
                return {sub(k): sub(v) for k, v in node.items()}
            return node

        return sub(masked_value)

    overlay = write_overlay(
        out,
        args.config_dir,
        fwd(sections),
        fwd(parent["registry"]),
        fund_tree,
        scan_names,
        venues,
        exceptions,
    )
    config_version = args.config_version or resolved_settings(overlay).config_version
    parent = {k: v for k, v in parent.items() if k != "input_sha256"}
    parent["config"] = {
        **{k: v for k, v in parent["config"].items() if k not in SECTIONS},
        **copy.deepcopy(sections),
    }

    messages = len(parent["config"].get("replay_messages") or [])
    parent["config"]["replay_timeout_seconds"] = (
        args.replay_timeout_base + args.replay_timeout_per_message * max(messages - 1, 0)
    )
    parent.update(code_version=code_version, config_version=config_version, assembled_at=now())
    parent["input_sha256"] = digest(parent)
    masked = fwd({k: v for k, v in parent.items() if k != "input_sha256"})
    for key in ("config_version", "code_version", "cutoff"):
        masked[key] = parent[key]
    for key in ("fixture_reference", "assembled_at", "replay_mode"):
        if masked.get(key) != parent.get(key):
            raise AssemblyError(f"{fixture}: masking changed {key}")
    host_mask = fixture in HOST_MASK_FIXTURES or any(
        (r.get("as_of_evidence") or {}).get("derivation", {}).get("kind") == "SNAPSHOT_SIGNED_IPFS_V1"
        for r in parent["records"]
        if r["collection"] == "ingest.source_items"
    )
    pinned = ("input_sha256", "config_version", "code_version", "registry")
    reversible = not slugs and inv({k: v for k, v in masked.items() if k not in pinned}) == {
        k: v for k, v in parent.items() if k not in pinned
    }
    if host_mask and not reversible and not slugs:
        raise AssemblyError(
            f"{fixture}: masked envelope is not exactly reversible (host identity admission requires it)"
        )
    hits, warnings = leak_scan(masked, scan_names, venues, exceptions)
    if hits:
        raise AssemblyError(
            f"{fixture}: original identities remain in masked envelope: {dict(list(hits.items())[:8])}"
        )
    family_errors = family_gate(parent["registry"], masked, families, replacements, venues)
    if family_errors:
        raise AssemblyError(f"{fixture}: placeholder family gate: {family_errors[:6]}")

    files, derived = {}, {}

    def derive(raw, kind=None):
        data, ext = (
            (canonical_json(fwd(json.loads(raw))).encode(), "json")
            if kind == "json"
            else mask_bytes(raw, fwd, fwd_text)
        )
        digest_ = hashlib.sha256(data).hexdigest()
        return data, digest_, ext

    for prow, mrow in zip(parent["records"], masked["records"], strict=False):
        if prow["collection"] != "ingest.source_items":
            continue
        precord, record = prow["record"], mrow["record"]
        proof, mproof = prow["as_of_evidence"], mrow["as_of_evidence"]
        kind = proof.get("derivation", {}).get("kind")
        projection = proof.get("projection_provenance")
        original_path = raw_paths.get(proof["raw_sha256"], proof["raw_ref"])
        raw = recovered[original_path] if original_path in recovered else read_bytes(original_path)
        if hashlib.sha256(raw).hexdigest() != proof["raw_sha256"]:
            raise AssemblyError(f"{fixture}: retained original raw checksum mismatch {original_path}")
        if projection:
            full = (
                recovered[projection["original_raw_ref"]]
                if projection["original_raw_ref"] in recovered
                else read_bytes(projection["original_raw_ref"])
            )
            mfull, mfull_sha, ext = derive(full)
            full_path = out / "host-only" / f"{mfull_sha}.{ext}.zst"
            files[str(full_path)] = mfull
            descriptor = mproof["derivation"]
            try:
                view = fr.project_original_source(descriptor["source_kind"], mfull, descriptor["selector"])
            except ValueError as error:
                raise AssemblyError(
                    f"{fixture}: masked projection of {prow['key']} fails under current code: {error}"
                )
            mproof["projection_provenance"] = {
                "original_raw_ref": str(full_path),
                "original_raw_sha256": mfull_sha,
                "projection_module_sha256": module_sha,
            }
            ext = "json"
        elif kind == "DECLARED_UTF8_SOURCE_V1":
            view, ext = fwd_text(raw.decode()).encode(), "txt"
        else:
            view, _, ext = derive(raw, "json" if kind == "SNAPSHOT_SIGNED_IPFS_V1" else None)
        view_sha = derived.setdefault(proof["raw_sha256"], hashlib.sha256(view).hexdigest())
        path = str(out / "inputs/raw" / f"{view_sha}.{ext}.zst")
        files[path] = view
        for prefix in ("", "detail_", "discovery_"):
            pair = (precord.get(prefix + "raw_ref"), precord.get(prefix + "raw_sha256"))
            if pair == (proof["raw_ref"], proof["raw_sha256"]):
                record[prefix + "raw_ref"], record[prefix + "raw_sha256"] = path, view_sha
            elif all(pair):
                extra = read_bytes(raw_paths.get(pair[1], pair[0]))
                data, extra_sha, extra_ext = derive(extra)
                extra_path = str(out / "inputs/raw" / f"{extra_sha}.{extra_ext}.zst")
                files[extra_path] = data
                record[prefix + "raw_ref"], record[prefix + "raw_sha256"] = extra_path, extra_sha
        if precord.get("raw_path") == proof["raw_ref"]:
            record["raw_path"] = path
        if "content_hash" in precord and any(
            record.get(k) != precord.get(k) for k in ("title", "text", "quoted")
        ):
            record["content_hash"] = content_hash(record)
        mproof.update(
            raw_ref=path, raw_sha256=view_sha, body_sha256=hashlib.sha256(record["text"].encode()).hexdigest()
        )
        try:
            fr.verify_derivation(record, mproof, view) if kind != "SNAPSHOT_SIGNED_IPFS_V1" else None
        except ValueError as error:
            raise AssemblyError(
                f"{fixture}: masked source {prow['key']} no longer derives from its masked raw: {error}"
            )
        for ptool, mtool in zip(parent["tool_results"], masked["tool_results"], strict=False):
            if ptool.get("source_item_id") == precord.get("item_id") and ptool.get("result") == precord:
                mtool["result"] = copy.deepcopy(record)

    private = {k: v for k, v in old_map.items() if k not in ("predecessor_private_map", "generation")}
    private.update(
        generation=args.generation,
        source_sha256=digest(parent),
        replacements=replacements,
        case_sensitive_names=sensitive,
        original_packet_path=str(out / "private/original-packet.json.zst"),
        original_registry_path=str(out / "private/original-registry.json"),
        predecessor_private_map={
            "path": spec["map"].relative_to(ROOT).as_posix(),
            "sha256": sha(spec["map"]),
        },
    )
    private.pop("original_raw_paths", None)
    if raw_paths:
        private["original_raw_paths"] = raw_paths
    if exceptions:
        if not exceptions.get("basis"):
            raise AssemblyError(f"{fixture}: a declared scan exception needs its ruling text in 'basis'")
        private["declared_technical_context_exceptions"] = exceptions
    if added:
        private["added_exact_case_replacements"] = sorted(added)
    if fixture in args.note:
        private["generation_note"] = args.note[fixture]
    private["placeholder_families"] = {
        "rule": "one real entity -> one placeholder family; registry names equal to the display name carry the root; gate: every entity placeholder in pipeline text is a registry name/alias of that entity",
        "families": families,
        "changes": family_log,
    }
    if recovery_log:
        private["recovered_originals"] = recovery_log
    if fixture in args.inverse_snapshots:
        private["inverse_restored_snapshots"] = {
            "basis": "unmasked supplement capture not retained; parent snapshot = authentic parent fields + exact inverse of the passing masked supplement fields; re-mask reproduces the passing snapshot (canonical JSON)",
            "rows": inverse_report,
        }
    masked["masking"] = {
        **(prior.get("masking") or {}),
        "applied": True,
        "map_sha256": digest(private),
        "performance_evidence": False,
    }
    masked["input_sha256"] = digest({k: v for k, v in masked.items() if k != "input_sha256"})

    spec_text = fa.specification_cases(ROOT)[fixture]["assertion_spec"]
    if old_seal["assertion_spec"] != spec_text:
        raise AssemblyError(f"{fixture}: predecessor seal differs from TDD §11.3")
    sealed = {
        "fixture_id": fixture,
        "input_sha256": digest(masked),
        "assertion_spec": spec_text,
        "sealed_at": now(),
        "assertions": old_seal["assertions"],
        "manual_predicates": old_seal.get("manual_predicates", []),
        **{
            k: old_seal[k]
            for k in ("automated_coverage", "source", "no_historical_performance_claim")
            if k in old_seal
        },
        "generation": args.generation,
        "predecessor_seal": {"path": spec["seal"].relative_to(ROOT).as_posix(), "sha256": sha(spec["seal"])},
        **({"generation_note": args.note[fixture]} if fixture in args.note else {}),
    }
    for path, data in recovered.items():
        write_new(Path(path), zstandard.ZstdCompressor(level=12).compress(data))
    write_new(out / "private/original-packet.json.zst", zjson(parent))
    write_new(out / "private/original-registry.json", registry_bytes)
    for path, data in files.items():
        if not Path(path).exists():
            write_new(Path(path), zstandard.ZstdCompressor(level=12).compress(data))
    write_new(out / "private/map.json", (canonical_json(private) + "\n").encode())
    write_new(out / "inputs/input.json.zst", zjson(masked))
    write_new(
        out / "sealed/outcomes.json", (json.dumps(sealed, indent=1, ensure_ascii=False) + "\n").encode()
    )
    write_new(
        receipt_path,
        (
            json.dumps(
                {
                    "build_key": build_key,
                    "assembled_at": parent["assembled_at"],
                    "source": spec["source"],
                    "parent": str(spec["parent"]),
                    "code_version": code_version,
                    "config_version": config_version,
                    "added_replacements": added,
                    "config_dir": str(args.config_dir),
                    "exactly_reversible": reversible,
                    "fundamentals_evidence_masked": (overlay / "fundamentals_evidence.json").is_file(),
                    "generation_note": args.note.get(fixture),
                    "inverse_restored_snapshots": fixture in args.inverse_snapshots,
                    "declared_technical_context_exceptions": exceptions,
                    "replay_timeout_seconds": parent["config"]["replay_timeout_seconds"],
                    "placeholder_family_changes": family_log,
                    "recovered_originals": recovery_log
                    + [
                        {
                            "path": p,
                            "status": "declared UTF-8 raw recovered from record text, sha256-verified",
                        }
                        for p in recovered
                        if p not in staged and not any(p.endswith(r["sha256"] + ".zst") for r in recovery_log)
                    ],
                    "staged_original_raw_copies": sorted(staged),
                    "leak_scan_technical_context_warnings": warnings,
                },
                indent=1,
            )
            + "\n"
        ).encode(),
    )
    return verify(fixture, rel, out, reused=False)


def verify(fixture, rel, out, reused):
    manifest = {
        c["fixture_id"]: c
        for c in json.loads((ROOT / "tests/fixtures/manifest.json").read_text())["fixtures"]
    }
    case = {
        k: v
        for k, v in manifest[fixture].items()
        if k not in ("acceptance_run", "acceptance_history", "execution_attempts")
    }
    for key, name in (
        ("acceptance_input", "inputs/input.json.zst"),
        ("private_name_map", "private/map.json"),
        ("sealed_outcomes", "sealed/outcomes.json"),
    ):
        case[key] = {"path": f"{rel}/{name}", "sha256": sha(out / name)}
    case.update(readiness="PREPARED_NATIVE_PENDING", missing=[])
    packet = read_json(out / "inputs/input.json.zst")
    private = read_json(out / "private/map.json")
    parent_registry = read_json(out / "private/original-packet.json.zst")["registry"]
    hits, _ = leak_scan(
        packet,
        {
            **private["replacements"],
            **{r["original"]: r["masked"] for r in private.get("identifier_replacements", [])},
        },
        registry_venues(parent_registry),
        private.get("declared_technical_context_exceptions"),
    )
    result = {
        "reused_existing_build": reused,
        "check_ready": fa.check_ready(ROOT, case),
        "leak_hits": sorted(hits),
    }
    try:
        fr.validate(
            packet,
            _market_record,
            private_mask_path=out / "private/map.json",
            input_path=out / "inputs/input.json.zst",
        )
        result["fixture_reference_validate"] = "ADMITTED"
    except Exception as error:
        result["fixture_reference_validate"] = f"REJECTED {type(error).__name__}: {error}"
    scratch = Path(tempfile.mkdtemp(prefix=".verify-", dir=out))
    try:
        plain = scratch / "input.json"
        plain.write_text(canonical_json(packet))
        pins = type(
            "Pins",
            (),
            {
                "code_version": packet["code_version"],
                "config_version": packet["config_version"],
                "replay_private_mask_path": out / "private/map.json",
            },
        )
        load_inputs(plain, pins)
        result["load_inputs"] = "ADMITTED"
    except Exception as error:
        result["load_inputs"] = f"REJECTED {type(error).__name__}: {error}"
    finally:
        safe_remove(scratch)
    overlay = out / "replay-config"
    replay_settings = resolved_settings(overlay)
    mismatched = [
        name
        for name in ("thresholds", "semantics", "dependency_map", "openma")
        if name in packet["config"] and replay_settings.load_config(name) != packet["config"][name]
    ]
    if replay_settings.load_config("registry") != packet["registry"]:
        mismatched.append("registry")
    result["replay_config_matches_packet"] = mismatched if mismatched else True
    result["replay_config_version_on_host"] = replay_settings.config_version
    result["pins"] = {"code_version": packet["code_version"], "config_version": packet["config_version"]}
    private_paths = sorted(
        {
            str(Path(p).parent)
            for p in [private["original_packet_path"], *private.get("original_raw_paths", {}).values()]
        }
    )
    result["container"] = {
        "TANK_CONFIG_PATH": str(overlay),
        "TANK_REPLAY_PRIVATE_MASK_PATH": str(out / "private/map.json"),
        "packet": f"zstd -dc {out / 'inputs/input.json.zst'} > <tank-data volume>/replay/inputs/{fixture}.json",
        "bind_same_path_read_only": [
            p
            for p in sorted({str(out), *private_paths})
            if not any(p != q and Path(p).is_relative_to(q) for q in {str(out), *private_paths})
        ],
    }
    result["sealed_input_sha256"] = digest(packet)
    result["manifest_entry"] = {
        k: case[k]
        for k in ("acceptance_input", "private_name_map", "sealed_outcomes", "readiness", "missing")
    }
    result["ready"] = (
        not result["check_ready"]
        and not hits
        and result["fixture_reference_validate"] == "ADMITTED"
        and result["load_inputs"] == "ADMITTED"
        and result["replay_config_matches_packet"] is True
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--fixtures", required=True)
    parser.add_argument("--generation", required=True)
    parser.add_argument("--from-generation", default="r90")
    parser.add_argument(
        "--from-manifest",
        action="store_true",
        help="take parent/map/seal/prior input from the manifest entry (acceptance_input, private_name_map)",
    )
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=ROOT / "config",
        help="deployed config export (e.g. docker compose exec -T tank tar -C /config -cf - . | tar -xf - -C DIR)",
    )
    parser.add_argument("--code-version", help="deployed process code_version (default: this process)")
    parser.add_argument(
        "--config-version",
        help="override; normally computed per fixture over its masked replay-config overlay",
    )
    parser.add_argument(
        "--add-replacements",
        type=json.loads,
        default={},
        help='JSON {"F-17": {"tether": "issuer-t"}}: extra exact-case registry slugs',
    )
    parser.add_argument(
        "--replace", action="store_true", help="rebuild a fixture dir with a different build key"
    )
    parser.add_argument(
        "--replay-timeout-base",
        type=int,
        default=2700,
        help="config.replay_timeout_seconds = base + per_message * (replay_messages - 1)",
    )
    parser.add_argument("--replay-timeout-per-message", type=int, default=900)
    parser.add_argument(
        "--scan-exceptions",
        type=json.loads,
        default={},
        help='JSON {"F-26": {"tokens": [...], "fields": [...], "basis": "ruling"}}: tokens the leak scan tolerates in those fields only',
    )
    parser.add_argument(
        "--map-override",
        type=json.loads,
        default={},
        help='JSON {"F-xx": "path/to/map.json"} (manifest mode)',
    )
    parser.add_argument(
        "--note", type=json.loads, default={}, help='JSON {"F-xx": "text"} recorded in map, seal and receipt'
    )
    parser.add_argument(
        "--inverse-snapshots",
        type=lambda v: {x.strip() for x in v.split(",") if x.strip()},
        default=set(),
        help="fixtures whose market snapshots are restored by gated exact inverse of the passing input",
    )
    parser.add_argument(
        "--supplement-fields",
        type=lambda v: [x for x in v.split(",") if x],
        default=[
            "available_at",
            "assembled_at",
            "flow_bars",
            "missing_inputs",
            "provenance",
            "reference_points",
            "sector_baskets",
        ],
        help="snapshot fields a supplement may add/change (inverse-snapshots gate)",
    )
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
        except (AssemblyError, FileNotFoundError, ValueError) as error:
            results[fixture] = {"ready": False, "error": f"{type(error).__name__}: {error}"}
            failed = True
    print(json.dumps(results, indent=1))
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
